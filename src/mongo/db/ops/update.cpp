//@file update.cpp

/**
 *    Copyright (C) 2008 10gen Inc.
 *    Copyright (C) 2013 Tokutek Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "pch.h"

#include "mongo/db/ops/update.h"

#include <cstring>  // for memcpy

#include "mongo/bson/mutable/damage_vector.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/index_set.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/ops/update_driver.h"
#include "mongo/db/ops/update_internal.h"
#include "mongo/db/pagefault.h"
#include "mongo/db/pdfile.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/query_runner.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/record.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/server_parameters.h"
#include "mongo/platform/unordered_set.h"

//#define DEBUGUPDATE(x) cout << x << endl;
#define DEBUGUPDATE(x)

namespace mongo {

    MONGO_EXPORT_SERVER_PARAMETER( newUpdateFrameworkEnabled, bool, false );

    bool isNewUpdateFrameworkEnabled() {
        return newUpdateFrameworkEnabled;
    }

    bool toggleNewUpdateFrameworkEnabled() {
        return newUpdateFrameworkEnabled = !newUpdateFrameworkEnabled;
    }

    void checkNoMods( BSONObj o ) {
        BSONObjIterator i( o );
        while( i.moreWithEOO() ) {
            BSONElement e = i.next();
            if ( e.eoo() )
                break;
            uassert( 10154 ,  "Modifiers and non-modifiers cannot be mixed", e.fieldName()[ 0 ] != '$' );
        }
        // no clustering secondary keys
        return false;
    }

    static void checkTooLarge(const BSONObj& newObj) {
        uassert( 12522 , "$ operator made object too large" , newObj.objsize() <= BSONObjMaxUserSize );
    }

    /**
     * return a BSONObj with the _id field of the doc passed in. If no _id and multi, error.
     */
    BSONObj makeOplogEntryQuery(const BSONObj doc, bool multi) {
        BSONObjBuilder idPattern;
        BSONElement id;
        // NOTE: If the matching object lacks an id, we'll log
        // with the original pattern.  This isn't replay-safe.
        // It might make sense to suppress the log instead
        // if there's no id.
        if ( doc.getObjectID( id ) ) {
           idPattern.append( id );
           return idPattern.obj();
        }
        else {
           uassert( 10157, "multi-update requires all modified objects to have an _id" , ! multi );
           return doc;
        }
    }

    /* note: this is only (as-is) called for

             - not multi
             - not mods is indexed
             - not upsert
    */
    static UpdateResult _updateById(bool isOperatorUpdate,
                                    int idIdxNo,
                                    ModSet* mods,
                                    NamespaceDetails* d,
                                    NamespaceDetailsTransient *nsdt,
                                    bool su,
                                    const char* ns,
                                    const BSONObj& updateobj,
                                    BSONObj patternOrig,
                                    bool logop,
                                    OpDebug& debug,
                                    bool fromMigrate = false) {

        DiskLoc loc;
        {
            IndexDetails& i = d->idx(idIdxNo);
            BSONObj key = i.getKeyFromQuery( patternOrig );
            loc = QueryRunner::fastFindSingle(i, key);
            if( loc.isNull() ) {
                // no upsert support in _updateById yet, so we are done.
                return UpdateResult( 0 , 0 , 0 , BSONObj() );
            }
        }
        cl->notifyOfWriteOp();
    }

    static void checkNoMods(const BSONObj &obj) {
        for (BSONObjIterator i(obj); i.more(); ) {
            const BSONElement &e = i.next();
            uassert(10154, "Modifiers and non-modifiers cannot be mixed", e.fieldName()[0] != '$');
        }
    }

    static void checkTooLarge(const BSONObj &obj) {
        uassert(12522, "$ operator made object too large", obj.objsize() <= BSONObjMaxUserSize);
    }

    ExportedServerParameter<bool> _fastupdatesParameter(
            ServerParameterSet::getGlobal(), "fastupdates", &cmdLine.fastupdates, true, true);
    ExportedServerParameter<bool> _fastupdatesIgnoreErrorsParameter(
            ServerParameterSet::getGlobal(), "fastupdatesIgnoreErrors", &cmdLine.fastupdatesIgnoreErrors, true, true);

    static Counter64 fastupdatesErrors;
    static ServerStatusMetricField<Counter64> fastupdatesIgnoredErrorsDisplay("fastupdates.errors", &fastupdatesErrors);

    // Apply an update message supplied by a collection to
    // some row in an in IndexDetails (for fast ydb updates).
    //
    class ApplyUpdateMessage : public storage::UpdateCallback {
        // @param pkQuery - the pk with field names, for proper default obj construction
        //                  in mods.createNewFromQuery().
        BSONObj applyMods(const BSONObj &oldObj, const BSONObj &msg) {
            try {
                // The update message is simply an update object, supplied by the user.
                ModSet mods(msg);
                auto_ptr<ModSetState> mss = mods.prepare(oldObj, false);
                const BSONObj newObj = mss->createNewFromMods();
                checkTooLarge(newObj);
                return newObj;
            } catch (const std::exception &ex) {
                // Applying an update message in this fashion _always_ ignores errors.
                // That is the risk you take when using --fastupdates.
                //
                // We will print such errors to the server's error log no more than once per 5 seconds.
                if (!cmdLine.fastupdatesIgnoreErrors && _loggingTimer.millisReset() > 5000) {
                    problem() << "* Failed to apply \"--fastupdate\" updateobj message! "
                                 "This means an update operation that appeared successful actually failed." << endl;
                    problem() << "* It probably should not be happening in production. To ignore these errors, "
                                 "set the server parameter fastupdatesIgnoreErrors=true" << endl;
                    problem() << "*    doc: " << oldObj << endl;
                    problem() << "*    updateobj: " << msg << endl;
                    problem() << "*    exception: " << ex.what() << endl;
                }
                fastupdatesErrors.increment(1);
                return oldObj;
            }
        }
    private:
        Timer _loggingTimer;
    } _storageUpdateCallback; // installed as the ydb update callback in db.cpp via set_update_callback

    static void updateUsingMods(const char *ns, Collection *cl, const BSONObj &pk, const BSONObj &obj,
                                const BSONObj &updateobj, shared_ptr<ModSet> mods, MatchDetails* details,
                                const bool fromMigrate) {
        ModSet *useMods = mods.get();
        auto_ptr<ModSet> mymodset;
        bool hasDynamicArray = mods->hasDynamicArray();
        if (details->hasElemMatchKey() && hasDynamicArray) {
            useMods = mods->fixDynamicArray(details->elemMatchKey());
            mymodset.reset(useMods);
        }
        auto_ptr<ModSetState> mss = useMods->prepare(obj, false /* not an insertion */);
        BSONObj newObj = mss->createNewFromMods();
        checkTooLarge(newObj);
        bool modsAreIndexed = useMods->isIndexed() > 0;
        bool forceFullUpdate = hasDynamicArray || !cl->updateObjectModsOk();
        // adding cl->indexBuildInProgress() as a check below due to #1085
        // This is a little heavyweight, as we whould be able to have modsAreIndexed
        // take hot indexes into account. Unfortunately, that code right now is not
        // factored cleanly enough to do nicely, so we just do the heavyweight check
        // here. Hope to get this properly fixed soon.
        uint64_t flags = (modsAreIndexed || cl->indexBuildInProgress()) ? 
            0 : 
            Collection::KEYS_UNAFFECTED_HINT;
        updateOneObject(cl, pk, obj, newObj,
            forceFullUpdate ? BSONObj() : updateobj, // if we have a dynamic array, force it to do a full overwrite
            fromMigrate, flags);

        // must happen after updateOneObject
        if (forceFullUpdate) {
            OplogHelpers::logUpdate(ns, pk, obj, newObj, fromMigrate);
        }
        else {
            OplogHelpers::logUpdateModsWithRow(ns, pk, obj, updateobj, fromMigrate);
        }
    }

    static void updateNoMods(const char *ns, Collection *cl, const BSONObj &pk, const BSONObj &obj, BSONObj &updateobj,
                             const bool fromMigrate) {
        // This is incredibly un-intiutive, but it takes a const BSONObj
        // and modifies it in-place if a timestamp needs to be set.
        BSONElementManipulator::lookForTimestamps(updateobj);
        checkNoMods(updateobj);
        updateOneObject(cl, pk, obj, updateobj, BSONObj(), fromMigrate, 0);
        // must happen after updateOneObject
        OplogHelpers::logUpdate(ns, pk, obj, updateobj, fromMigrate);
    }

    static UpdateResult upsertAndLog(Collection *cl, const BSONObj &patternOrig,
                                     const BSONObj &updateobj, const bool isOperatorUpdate,
                                     ModSet *mods, bool fromMigrate) {
        const string &ns = cl->ns();
        uassert(16893, str::stream() << "Cannot upsert a collection under-going bulk load: " << ns,
                       ns != cc().bulkLoadNS());

        BSONObj newObj = updateobj;
        if (isOperatorUpdate) {
            newObj = mods->createNewFromQuery(patternOrig);
            cc().curop()->debug().fastmodinsert = true;
        } else {
            cc().curop()->debug().upsert = true;
        }

        checkNoMods(newObj);
        insertOneObject(cl, newObj);
        OplogHelpers::logInsert(ns.c_str(), newObj, fromMigrate);
        return UpdateResult(0, isOperatorUpdate, 1, newObj);
    }

    static UpdateResult updateByPK(const char *ns, Collection *cl,
                            const BSONObj &pk, const BSONObj &patternOrig,
                            const BSONObj &updateobj,
                            const bool upsert,
                            const bool fromMigrate,
                            uint64_t flags) {
        // Create a mod set for $ style updates.
        shared_ptr<ModSet> mods;
        const bool isOperatorUpdate = updateobj.firstElementFieldName()[0] == '$';
        if (isOperatorUpdate) {
            mods.reset(new ModSet(updateobj, cl->indexKeys()));
        }

        BSONObj obj;
        ResultDetails queryResult;
        if (mods && mods->hasDynamicArray()) {
            queryResult.matchDetails.requestElemMatchKey();
        }

        const bool found = queryByPKHack(cl, pk, patternOrig, obj, &queryResult);
        if (!found) {
            if (!upsert) {
                return UpdateResult(0, 0, 0, BSONObj());
            }
            return upsertAndLog(cl, patternOrig, updateobj, isOperatorUpdate, mods.get(), fromMigrate);
        }

        if (isOperatorUpdate) {
            updateUsingMods(ns, cl, pk, obj, updateobj, mods, &queryResult.matchDetails, fromMigrate);
        } else {
            // replace-style update
            BSONObj copy = updateobj.copy();
            updateNoMods(ns, cl, pk, obj, copy, fromMigrate);
        }
        return UpdateResult(1, isOperatorUpdate, 1, BSONObj());
    }

    BSONObj invertUpdateMods(const BSONObj &updateobj) {
        BSONObjBuilder b(updateobj.objsize());
        for (BSONObjIterator i(updateobj); i.more(); ) {
            const BSONElement &e = i.next();
            verify(str::equals(e.fieldName(), "$inc"));
            BSONObjBuilder inc(b.subobjStart("$inc"));
            for (BSONObjIterator o(e.Obj()); o.more(); ) {
                const BSONElement &fieldToInc = o.next();
                verify(fieldToInc.isNumber());
                const long long invertedValue = -fieldToInc.numberLong();
                inc.append(fieldToInc.fieldName(), invertedValue);
            }
            inc.done();
        }
        return b.obj();
    }

    static UpdateResult _updateObjects(const char *ns,
                                       const BSONObj &updateobj,
                                       const BSONObj &patternOrig,
                                       const bool upsert, const bool multi,
                                       const bool fromMigrate) {
        TOKULOG(2) << "update: " << ns
                   << " update: " << updateobj
                   << " query: " << patternOrig
                   << " upsert: " << upsert << " multi: " << multi << endl;

        Collection *cl = getOrCreateCollection(ns, true);

        // Fast-path for simple primary key updates.
        //
        // - We don't do it for capped collections since  their documents may not grow,
        // and the fast path doesn't know if docs grow until the update message is applied.
        // - We don't do it if multi=true because semantically we're not supposed to, if
        // the update ends up being a replace-style upsert. See jstests/update_multi6.js
        if (!multi && !cl->isCapped()) {
            const BSONObj pk = cl->getSimplePKFromQuery(patternOrig);
            if (!pk.isEmpty()) {
                return updateByPK(ns, cl, pk, patternOrig, updateobj,
                                  upsert, fromMigrate, 0);
            }
        }

        // Run a regular update using the query optimizer.

        set<BSONObj> seenObjects;
        MatchDetails details;
        shared_ptr<ModSet> mods;

        const bool isOperatorUpdate = updateobj.firstElementFieldName()[0] == '$';
        if (isOperatorUpdate) {
            mods.reset(new ModSet(updateobj, cl->indexKeys()));
            if (mods->hasDynamicArray()) {
                details.requestElemMatchKey();
            }
        }

        int numModded = 0;
        cc().curop()->debug().nscanned = 0;
        for (shared_ptr<Cursor> c = getOptimizedCursor(ns, patternOrig); c->ok(); ) {
            cc().curop()->debug().nscanned++;
            BSONObj currPK = c->currPK();
            if (c->getsetdup(currPK)) {
                c->advance();
                continue;
            }
            if (!c->currentMatches(&details)) {
                c->advance();
                continue;
            }

            BSONObj currentObj = c->current();
            if (!isOperatorUpdate) {
                // replace-style update only affects a single matching document
                uassert(10158, "multi update only works with $ operators", !multi);
                BSONObj copy = updateobj.copy();
                updateNoMods(ns, cl, currPK, currentObj, copy, fromMigrate);
                return UpdateResult(1, 0, 1, BSONObj());
            }

            // operator-style updates may affect many documents
            if (multi) {
                // Advance past the document to be modified - SERVER-5198,
                // First, get owned copies of currPK/currObj, which live in the cursor.
                currPK = currPK.getOwned();
                currentObj = currentObj.getOwned();
                while (c->ok() && currPK == c->currPK()) {
                    c->advance();
                }

                BSONObj pattern = patternOrig;

                if ( logop ) {
                    BSONObj js = BSONObj::make(r);
                    BSONObj idQuery = makeOplogEntryQuery(js, multi);
                    pattern = idQuery;
                }

                /* look for $inc etc.  note as listed here, all fields to inc must be this type, you can't set some
                    regular ones at the moment. */
                if ( isOperatorUpdate ) {

                    if ( multi ) {
                        // go to next record in case this one moves
                        c->advance();

                        // Update operations are deduped for cursors that implement their own
                        // deduplication.  In particular, some geo cursors are excluded.
                        if ( autoDedup ) {

                            if ( seenObjects.count( loc ) ) {
                                continue;
                            }

                            // SERVER-5198 Advance past the document to be modified, provided
                            // deduplication is enabled, but see SERVER-5725.
                            while( c->ok() && loc == c->currLoc() ) {
                                c->advance();
                            }
                        }
                    }

                    const BSONObj& onDisk = loc.obj();

                    ModSet* useMods = mods.get();

                    auto_ptr<ModSet> mymodset;
                    if ( details.hasElemMatchKey() && mods->hasDynamicArray() ) {
                        useMods = mods->fixDynamicArray( details.elemMatchKey() );
                        mymodset.reset( useMods );
                    }

                    auto_ptr<ModSetState> mss = useMods->prepare( onDisk,
                                                                  false /* not an insertion */ );

                    bool willAdvanceCursor = multi && c->ok() && ( modsIsIndexed || ! mss->canApplyInPlace() );

                    if ( willAdvanceCursor ) {
                        if ( cc.get() ) {
                            cc->setDoingDeletes( true );
                        }
                        c->prepareToTouchEarlierIterate();
                    }

                    // If we've made it this far, "ns" must contain a valid collection name, and so
                    // is of the form "db.collection".  Therefore, the following expression must
                    // always be valid.  "system.users" updates must never be done in place, in
                    // order to ensure that they are validated inside DataFileMgr::updateRecord(.).
                    bool isSystemUsersMod = nsToCollectionSubstring(ns) == "system.users";

                    BSONObj newObj;
                    if ( !mss->isUpdateIndexed() && mss->canApplyInPlace() && !isSystemUsersMod ) {
                        mss->applyModsInPlace( true );// const_cast<BSONObj&>(onDisk) );

                        DEBUGUPDATE( "\t\t\t doing in place update" );
                        if ( !multi )
                            debug.fastmod = true;

                        if ( modsIsIndexed ) {
                            seenObjects.insert( loc );
                        }
                        newObj = loc.obj();
                        d->paddingFits();
                    }
                    else {
                        newObj = mss->createNewFromMods();
                        checkTooLarge(newObj);
                        DiskLoc newLoc = theDataFileMgr.updateRecord(ns,
                                                                     d,
                                                                     nsdt,
                                                                     r,
                                                                     loc,
                                                                     newObj.objdata(),
                                                                     newObj.objsize(),
                                                                     debug);

                        if ( newLoc != loc || modsIsIndexed ){
                            // log() << "Moved obj " << newLoc.obj()["_id"] << " from " << loc << " to " << newLoc << endl;
                            // object moved, need to make sure we don' get again
                            seenObjects.insert( newLoc );
                        }

                    }

                    if ( logop ) {
                        DEV verify( mods->size() );
                        BSONObj logObj = mss->getOpLogRewrite();
                        DEBUGUPDATE( "\t rewrite update: " << logObj );

                        // It is possible that the entire mod set was a no-op over this
                        // document.  We would have an empty log record in that case. If we
                        // call logOp, with an empty record, that would be replicated as "clear
                        // this record", which is not what we want. Therefore, to get a no-op
                        // in the replica, we simply don't log.
                        if ( logObj.nFields() ) {
                            logOp("u", ns, logObj , &pattern, 0, fromMigrate, &newObj );
                        }
                    }
                    numModded++;
                    if ( ! multi )
                        return UpdateResult( 1 , 1 , numModded , BSONObj() );
                    if ( willAdvanceCursor )
                        c->recoverFromTouchingEarlierIterate();

                    getDur().commitIfNeeded();

                    continue;
                } else {
                    seenObjects.insert(currPK);
                }
            }

            updateUsingMods(ns, cl, currPK, currentObj, updateobj, mods, &details, fromMigrate);
            numModded++;

            if (!multi) {
                break;
            }
        }

        if (numModded) {
            // We've modified something, so we're done.
            return UpdateResult(1, 1, numModded, BSONObj());
        }
        if (!upsert) {
            // We haven't modified anything, but we're not trying to upsert, so we're done.
            return UpdateResult(0, isOperatorUpdate, numModded, BSONObj());
        }

        if (!isOperatorUpdate) {
            uassert(10159, "multi update only works with $ operators", !multi);
        }
        // Upsert a new object
        return upsertAndLog(cl, patternOrig, updateobj, isOperatorUpdate, mods.get(), fromMigrate);
    }

    UpdateResult _updateObjectsNEW( bool su,
                                    const char* ns,
                                    const BSONObj& updateobj,
                                    const BSONObj& patternOrig,
                                    bool upsert,
                                    bool multi,
                                    bool logop ,
                                    OpDebug& debug,
                                    RemoveSaver* rs,
                                    bool fromMigrate,
                                    const QueryPlanSelectionPolicy& planPolicy,
                                    bool forReplication ) {

        // TODO
        // + Separate UpdateParser from UpdateRunner (the latter should be "stage-y")
        //   + All the yield and deduplicate logic would move to the query stage
        //     portion of it
        //
        // + Replication related
        //   + fast path for update for query by _id
        //   + support for relaxing viable path constraint in replication
        //
        // + Field Management
        //   + Force all upsert to contain _id
        //   + Prevent changes to immutable fields (_id, and those mentioned by sharding)
        //
        // + Yiedling related
        //   + $atomic support (or better, support proper yielding if not)
        //   + page fault support

        debug.updateobj = updateobj;

        NamespaceDetails* d = nsdetails( ns );
        NamespaceDetailsTransient* nsdt = &NamespaceDetailsTransient::get( ns );

        UpdateDriver::Options opts;
        opts.multi = multi;
        opts.upsert = upsert;
        opts.logOp = logop;
        opts.modOptions = forReplication ? ModifierInterface::Options::fromRepl():
                                           ModifierInterface::Options::normal();
        UpdateDriver driver( opts );
        // TODO: This copies the index keys, but we may not actually need to.
        Status status = driver.parse( nsdt->indexKeys(), updateobj );
        if ( !status.isOK() ) {
            uasserted( 16840, status.reason() );
        }

        shared_ptr<Cursor> cursor = getOptimizedCursor( ns, patternOrig, BSONObj(), planPolicy );

        // If the update was marked with '$isolated' (a.k.a '$atomic'), we are not allowed to
        // yield while evaluating the update loop below.
        //
        // TODO: Old code checks this repeatedly within the update loop. Is that necessary? It seems
        // that once atomic should be always atomic.
        const bool canYield =
            cursor->ok() &&
            cursor->matcher() &&
            cursor->matcher()->docMatcher().atomic();

        // The 'cursor' the optimizer gave us may contain query plans that generate duplicate
        // diskloc's. We set up here the mechanims that will prevent us from processing those
        // twice if we see them. We also set up a 'ClientCursor' so that we can support
        // yielding.
        //
        // TODO: Is it valid to call this on a non-ok cursor?
        const bool dedupHere = cursor->autoDedup();

        //
        // We'll start assuming we have one or more documents for this update. (Othwerwise,
        // we'll fallback to upserting.)
        //

        // We record that this will not be an upsert, in case a mod doesn't want to be applied
        // when in strict update mode.
        driver.setContext( ModifierInterface::ExecInfo::UPDATE_CONTEXT );

        // Let's fetch each of them and pipe them through the update expression, making sure to
        // keep track of the necessary stats. Recall that we'll be pulling documents out of
        // cursors and some of them do not deduplicate the entries they generate. We have
        // deduping logic in here, too -- for now.
        unordered_set<DiskLoc, DiskLoc::Hasher> seenLocs;
        int numUpdated = 0;
        debug.nscanned = 0;

        Client& client = cc();

        mutablebson::Document doc;

        // If we are going to be yielding, we will need a ClientCursor scoped to this loop. We
        // only loop as long as the underlying cursor is OK.
        for ( auto_ptr<ClientCursor> clientCursor; cursor->ok(); ) {

            // If we haven't constructed a ClientCursor, and if the client allows us to throw
            // page faults, and if we are referring to a location that is likely not in
            // physical memory, then throw a PageFaultException. The entire operation will be
            // restarted.
            if ( clientCursor.get() == NULL &&
                 client.allowedToThrowPageFaultException() &&
                 !cursor->currLoc().isNull() &&
                 !cursor->currLoc().rec()->likelyInPhysicalMemory() ) {
                // We should never throw a PFE if we have already updated items.
                dassert(numUpdated == 0);
                throw PageFaultException( cursor->currLoc().rec() );
            }

            if ( !canYield && debug.nscanned != 0 ) {

                // We are permitted to yield. To do so we need a ClientCursor, so create one
                // now if we have not yet done so.
                if ( !clientCursor.get() )
                    clientCursor.reset(
                        new ClientCursor( QueryOption_NoCursorTimeout, cursor, ns ) );

                // Ask the client cursor to yield. We get two bits of state back: whether or not
                // we yielded, and whether or not we correctly recovered from yielding.
                bool yielded = false;
                const bool recovered = clientCursor->yieldSometimes(
                    ClientCursor::WillNeed, &yielded );

                // If we couldn't recover from the yield, or if the cursor died while we were
                // yielded, get out of the update loop right away. We don't need to reset
                // 'clientCursor' since we are leaving the scope.
                if ( !recovered || !cursor->ok() )
                    break;

                if ( yielded ) {
                    // Details about our namespace may have changed while we were yielded, so
                    // we re-acquire them here. If we can't do so, escape the update
                    // loop. Otherwise, refresh the driver so that it knows about what is
                    // currently indexed.
                    d = nsdetails( ns );
                    if ( !d )
                        break;
                    nsdt = &NamespaceDetailsTransient::get( ns );

                    // TODO: This copies the index keys, but it may not need to do so.
                    driver.refreshIndexKeys( nsdt->indexKeys() );
                }

            }

            // Let's fetch the next candidate object for this update.
            Record* r = cursor->_current();
            DiskLoc loc = cursor->currLoc();
            const BSONObj oldObj = loc.obj();

            // We count how many documents we scanned even though we may skip those that are
            // deemed duplicated. The final 'numUpdated' and 'nscanned' numbers may differ for
            // that reason.
            debug.nscanned++;

            // Skips this document if it:
            // a) doesn't match the query portion of the update
            // b) was deemed duplicate by the underlying cursor machinery
            //
            // Now, if we are going to update the document,
            // c) we don't want to do so while the cursor is at it, as that may invalidate
            // the cursor. So, we advance to next document, before issuing the update.
            MatchDetails matchDetails;
            matchDetails.requestElemMatchKey();
            if ( !cursor->currentMatches( &matchDetails ) ) {
                // a)
                cursor->advance();
                continue;
            }
            else if ( cursor->getsetdup( loc ) && dedupHere ) {
                // b)
                cursor->advance();
                continue;
            }
            else if (driver.dollarModMode() && multi) {
                // c)
                cursor->advance();
                if ( dedupHere ) {
                    if ( seenLocs.count( loc ) ) {
                        continue;
                    }
                }

                // There are certain kind of cursors that hold multiple pointers to data
                // underneath. $or cursors is one example. In a $or cursor, it may be the case
                // that when we did the last advance(), we finished consuming documents from
                // one of $or child and started consuming the next one. In that case, it is
                // possible that the last document of the previous child is the same as the
                // first document of the next (see SERVER-5198 and jstests/orp.js).
                //
                // So we advance the cursor here until we see a new diskloc.
                //
                // Note that we won't be yielding, and we may not do so for a while if we find
                // a particularly duplicated sequence of loc's. That is highly unlikely,
                // though.  (See SERVER-5725, if curious, but "stage" based $or will make that
                // ticket moot).
                while( cursor->ok() && loc == cursor->currLoc() ) {
                    cursor->advance();
                }
            }

            // For some (unfortunate) historical reasons, not all cursors would be valid after
            // a write simply because we advanced them to a document not affected by the write.
            // To protect in those cases, not only we engaged in the advance() logic above, but
            // we also tell the cursor we're about to write a document that we've just seen.
            // prepareToTouchEarlierIterate() requires calling later
            // recoverFromTouchingEarlierIterate(), so we make a note here to do so.
            bool touchPreviousDoc = multi && cursor->ok();
            if ( touchPreviousDoc ) {
                if ( clientCursor.get() )
                    clientCursor->setDoingDeletes( true );
                cursor->prepareToTouchEarlierIterate();
            }

            // Ask the driver to apply the mods. It may be that the driver can apply those "in
            // place", that is, some values of the old document just get adjusted without any
            // change to the binary layout on the bson layer. It may be that a whole new
            // document is needed to accomodate the new bson layout of the resulting document.
            doc.reset( oldObj, mutablebson::Document::kInPlaceEnabled );
            BSONObj logObj;
            StringData matchedField = matchDetails.hasElemMatchKey() ?
                                                    matchDetails.elemMatchKey():
                                                    StringData();
            status = driver.update( matchedField, &doc, &logObj );
            if ( !status.isOK() ) {
                uasserted( 16837, status.reason() );
            }

            // If the driver applied the mods in place, we can ask the mutable for what
            // changed. We call those changes "damages". :) We use the damages to inform the
            // journal what was changed, and then apply them to the original document
            // ourselves. If, however, the driver applied the mods out of place, we ask it to
            // generate a new, modified document for us. In that case, the file manager will
            // take care of the journaling details for us.
            //
            // This code flow is admittedly odd. But, right now, journaling is baked in the file
            // manager. And if we aren't using the file manager, we have to do jounaling
            // ourselves.
            bool objectWasChanged = false;
            BSONObj newObj;
            const char* source = NULL;
            mutablebson::DamageVector damages;
            bool inPlace = doc.getInPlaceUpdates(&damages, &source);
            if ( inPlace && !damages.empty() && !driver.modsAffectIndices() ) {
                d->paddingFits();

                // All updates were in place. Apply them via durability and writing pointer.
                mutablebson::DamageVector::const_iterator where = damages.begin();
                const mutablebson::DamageVector::const_iterator end = damages.end();
                for( ; where != end; ++where ) {
                    const char* sourcePtr = source + where->sourceOffset;
                    void* targetPtr = getDur().writingPtr(
                        const_cast<char*>(oldObj.objdata()) + where->targetOffset,
                        where->size);
                    std::memcpy(targetPtr, sourcePtr, where->size);
                }
                newObj = oldObj;
                debug.fastmod = true;

                objectWasChanged = true;
            }
            else {

                // The updates were not in place. Apply them through the file manager.
                newObj = doc.getObject();
                DiskLoc newLoc = theDataFileMgr.updateRecord(ns,
                                                             d,
                                                             nsdt,
                                                             r,
                                                             loc,
                                                             newObj.objdata(),
                                                             newObj.objsize(),
                                                             debug);

                // If we've moved this object to a new location, make sure we don't apply
                // that update again if our traversal picks the objecta again.
                //
                // We also take note that the diskloc if the updates are affecting indices.
                // Chances are that we're traversing one of them and they may be multi key and
                // therefore duplicate disklocs.
                if ( newLoc != loc || driver.modsAffectIndices()  ) {
                    seenLocs.insert( newLoc );
                }

                objectWasChanged = true;
            }

            // Log Obj
            if ( logop ) {
                if ( !logObj.isEmpty() ) {
                    BSONObj idQuery = driver.makeOplogEntryQuery(newObj, multi);
                    logOp("u", ns, logObj , &idQuery, 0, fromMigrate, &newObj);
                }
            }

            // If we applied any in-place updates, or asked the DataFileMgr to write for us,
            // then count this as an update.
            if (objectWasChanged)
                numUpdated++;

            if (!multi) {
                break;
            }

            // If we used the cursor mechanism that prepares an earlier seen document for a
            // write we need to tell such mechanisms that the write is over.
            if ( touchPreviousDoc ) {
                cursor->recoverFromTouchingEarlierIterate();
            }

            getDur().commitIfNeeded();

        }

        if (numUpdated > 0) {
            return UpdateResult( true /* updated existing object(s) */,
                                 driver.dollarModMode() /* $mod or obj replacement */,
                                 numUpdated /* # of docments update */,
                                 BSONObj() );
        }
        else if (numUpdated == 0 && !upsert) {
            return UpdateResult( false /* no object updated */,
                                 driver.dollarModMode() /* $mod or obj replacement */,
                                 0 /* no updates */,
                                 BSONObj() );
        }

        //
        // We haven't succeeded updating any existing document but upserts are allowed.
        //

        // If this is a $mod base update, we need to generate a document by examining the
        // query and the mods. Otherwise, we can use the object replacement sent by the user
        // update command that was parsed by the driver before.
        BSONObj oldObj;
        if ( *updateobj.firstElementFieldName() == '$' ) {
            if ( !driver.createFromQuery( patternOrig, &oldObj ) ) {
                uasserted( 16835, "cannot create object to update" );
            }
            debug.fastmodinsert = true;
        }
        else {
            // Copy the _id
            if (patternOrig.hasElement("_id")) {
                oldObj = patternOrig.getField("_id").wrap();
            }
            debug.upsert = true;
        }

        // Since this is an upsert, we will be oplogging it as an insert. We don't
        // need the driver's help to build the oplog record, then. We also set the
        // context of the update driver to an "upsert". Some mods may only work in that
        // context (e.g. $setOnInsert).
        driver.setLogOp( false );
        driver.setContext( ModifierInterface::ExecInfo::INSERT_CONTEXT );

        doc.reset( oldObj, mutablebson::Document::kInPlaceDisabled );
        status = driver.update( StringData(), &doc, NULL /* no oplog record */);
        if ( !status.isOK() ) {
            uasserted( 16836, status.reason() );
        }
        BSONObj newObj = doc.getObject();

        theDataFileMgr.insertWithObjMod( ns, newObj, false, su );

        if ( logop ) {
            logOp( "i", ns, newObj, 0, 0, fromMigrate, &newObj );
        }

        return UpdateResult( false /* updated a non existing document */,
                             driver.dollarModMode() /* $mod or obj replacement? */,
                             1 /* count of updated documents */,
                             newObj /* object that was upserted */ );
    }

    UpdateResult updateObjects( const char* ns,
                                const BSONObj& updateobj,
                                const BSONObj& patternOrig,
                                bool upsert,
                                bool multi,
                                bool logop ,
                                OpDebug& debug,
                                bool fromMigrate,
                                const QueryPlanSelectionPolicy& planPolicy ) {

        validateUpdate( ns , updateobj , patternOrig );

        if ( isNewUpdateFrameworkEnabled() ) {

            UpdateResult ur = _updateObjectsNEW(false, ns, updateobj, patternOrig,
                                                upsert, multi, logop,
                                                debug, NULL, fromMigrate, planPolicy );
            debug.nupdated = ur.num;
            return ur;
        }
        else {

            UpdateResult ur = _updateObjects(false, ns, updateobj, patternOrig,
                                             upsert, multi, logop,
                                             debug, NULL, fromMigrate, planPolicy );
            debug.nupdated = ur.num;
            return ur;
        }
    }

    UpdateResult updateObjectsForReplication( const char* ns,
                                              const BSONObj& updateobj,
                                              const BSONObj& patternOrig,
                                              bool upsert,
                                              bool multi,
                                              bool logop ,
                                              OpDebug& debug,
                                              bool fromMigrate,
                                              const QueryPlanSelectionPolicy& planPolicy ) {

        validateUpdate( ns , updateobj , patternOrig );

        if ( isNewUpdateFrameworkEnabled() ) {

            UpdateResult ur = _updateObjectsNEW(false,
                                                ns,
                                                updateobj,
                                                patternOrig,
                                                upsert,
                                                multi,
                                                logop,
                                                debug,
                                                NULL /* no remove saver */,
                                                fromMigrate,
                                                planPolicy,
                                                true /* for replication */ );
            debug.nupdated = ur.num;
            return ur;

        }
        else {

            UpdateResult ur = _updateObjects(false,
                                             ns,
                                             updateobj,
                                             patternOrig,
                                             upsert,
                                             multi,
                                             logop,
                                             debug,
                                             NULL /* no remove saver */,
                                             fromMigrate,
                                             planPolicy,
                                             true /* for replication */ );
            debug.nupdated = ur.num;
            return ur;

        }
    }

    BSONObj applyUpdateOperators( const BSONObj& from, const BSONObj& operators ) {
        if ( isNewUpdateFrameworkEnabled() ) {
            UpdateDriver::Options opts;
            opts.multi = false;
            opts.upsert = false;
            UpdateDriver driver( opts );
            Status status = driver.parse( IndexPathSet(), operators );
            if ( !status.isOK() ) {
                uasserted( 16838, status.reason() );
            }

            mutablebson::Document doc( from, mutablebson::Document::kInPlaceDisabled );
            status = driver.update( StringData(), &doc, NULL /* not oplogging */ );
            if ( !status.isOK() ) {
                uasserted( 16839, status.reason() );
            }

            return doc.getObject();
        }
        else {
            ModSet mods( operators );
            return mods.prepare( from, false /* not an insertion */ )->createNewFromMods();
        }
    }

}  // namespace mongo
