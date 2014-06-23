/**
*    Copyright (C) 2010 10gen Inc.
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
#include "../commands.h"
#include "rs.h"
#include "multicmd.h"

namespace mongo {

    bool shouldVeto(const uint32_t id, const int config, string& errmsg) {
        const Member* primary = theReplSet->box.getPrimary();
        const Member* hopeful = theReplSet->findById(id);
        const Member *highestPriority = theReplSet->getMostElectable();
    
        if( !hopeful ) {
            errmsg = str::stream() << "replSet couldn't find member with id " << id;
            return true;
        }
        else if( theReplSet->config().version > config ) {
            errmsg = str::stream() << "replSet member " << id << " is not yet aware its cfg version " << config << " is stale";
            return true;
        }
        else if( theReplSet->isPrimary() )
        {
            // hbinfo is not updated, so we have to check the primary's last GTID separately
            errmsg = str::stream() << "I am already primary, " << hopeful->fullName() <<
                " can try again once I've stepped down";
            return true;
        }
        else if( primary ) 
        {
            // other members might be aware of more up-to-date nodes
            errmsg = str::stream() << hopeful->fullName() << " is trying to elect itself but " <<
                primary->fullName() << " is already primary and more up-to-date";
            return true;
        }
        else if( highestPriority && highestPriority->config().priority > hopeful->config().priority) {
            errmsg = str::stream() << hopeful->fullName() << " has lower priority than " << highestPriority->fullName();
            return true;
        }
    
        if (!theReplSet->isElectable(id)) {
            errmsg = str::stream() << "I don't think " << hopeful->fullName() <<
                " is electable";
            return true;
        }
    
        return false;
    }

    /** the first cmd called by a node seeking election and it's a basic sanity 
        test: do any of the nodes it can reach know that it can't be the primary?
        */
    class CmdReplSetFresh : public ReplSetCommand {
    public:
        CmdReplSetFresh() : ReplSetCommand("replSetFresh") { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetFresh);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
    private:


        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) ) {
                return false;
            }
            
            GTIDManager* gtidMgr = theReplSet->gtidManager.get();
            if( cmdObj["set"].String() != theReplSet->name() ) {
                errmsg = "wrong repl set name";
                return false;
            }
            string who = cmdObj["who"].String();
            int cfgver = cmdObj["cfgver"].Int();
            uint32_t id = cmdObj["id"].Int();
            GTID remoteGTID = getGTIDFromBSON("GTID", cmdObj);
            GTID ourGTID = gtidMgr->getLiveState();

            bool weAreFresher = false;
            // check not only our own GTID, but any other member we can reach
            if (GTID::cmp(remoteGTID, ourGTID) < 0 ||
                     GTID::cmp(remoteGTID, theReplSet->lastOtherGTID()) < 0) {                
                log() << "we are fresher! remoteGTID" << remoteGTID.toString() << " ourGTID " << ourGTID.toString() << " lastOther " << theReplSet->lastOtherGTID() << " " << rsLog;
                weAreFresher = true;
            }
            addGTIDToBSON("GTID", ourGTID, result);
            result.append("fresher", weAreFresher);
            bool veto = shouldVeto(id, cfgver, errmsg);
            // have this check here because once we get to the second phase of the election,
            // we don't want this to be a reason for an election failure
            if (!veto) {
                if (!theReplSet->isElectable(id)) {
                    errmsg = str::stream() << "I don't think " << theReplSet->findById(id)->fullName() <<
                        " is electable";
                    veto = true;
                }
            }
            result.append("veto", veto);
            if (veto) {
                result.append("errmsg", errmsg);
            }
            // stands for "highest known primary"
            uint64_t highestKnownPrimaryToUse = std::max(gtidMgr->getHighestKnownPrimary(),
                theReplSet->getHighestKnownPrimaryAcrossSet());
            result.append("hkp", highestKnownPrimaryToUse);

            return true;
        }
    } cmdReplSetFresh;

    class CmdReplSetElect : public ReplSetCommand {
    public:
        CmdReplSetElect() : ReplSetCommand("replSetElect") { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetElect);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
    private:
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) )
                return false;
            theReplSet->elect.electCmdReceived(cmdObj, &result);
            return true;
        }
    } cmdReplSetElect;

    int Consensus::totalVotes() const {
        static int complain = 0;
        int vTot = rs._self->config().votes;
        for( Member *m = rs.head(); m; m=m->next() )
            vTot += m->config().votes;
        if( vTot % 2 == 0 && vTot && complain++ == 0 )
            log() << "replSet " /*buildbot! warning */ "total number of votes is even - add arbiter or give one member an extra vote" << rsLog;
        return vTot;
    }

    bool Consensus::aMajoritySeemsToBeUp() const {
        int vUp = rs._self->config().votes;
        for( Member *m = rs.head(); m; m=m->next() )
            vUp += m->hbinfo().up() ? m->config().votes : 0;
        return vUp * 2 > totalVotes();
    }

    bool Consensus::shouldRelinquish() const {
        GTID ourLiveState = theReplSet->gtidManager->getLiveState();
        uint64_t ourHighestKnownPrimary = theReplSet->gtidManager->getHighestKnownPrimary();
        int vUp = rs._self->config().votes;
        for( Member *m = rs.head(); m; m=m->next() ) {
            if (m->hbinfo().up()) {
                if (GTID::cmp(ourLiveState, m->hbinfo().gtid) < 0) {
                    log() << "our GTID is" << ourLiveState.toString() << \
                        ", " << m->fullName() << " has GTID " << m->hbinfo().gtid.toString() << \
                        ", relinquishing primary" << rsLog;
                    return true;
                }
                uint64_t otherHighestKnownPrimary = m->hbinfo().highestKnownPrimaryInSet;
                if (ourHighestKnownPrimary < otherHighestKnownPrimary) {
                    log() << "our highestKnownPrimary " << ourHighestKnownPrimary << \
                        ", " << m->fullName() << " has highestKnownPrimary " << otherHighestKnownPrimary << \
                        ", relinquishing primary" << rsLog;
                    return true;
                }
                vUp += m->config().votes;
            }
        }

        // the manager will handle calling stepdown if another node should be
        // primary due to priority
        if (!( vUp * 2 > totalVotes())) {
            log() << "can't see a majority of the set, relinquishing primary" << rsLog;
            return true;
        }

        return false;
    }

    unsigned Consensus::yea(unsigned memberId) {
        return rs._self->config().votes;
    }

    /* todo: threading **************** !!!!!!!!!!!!!!!! */
    void Consensus::electCmdReceived(BSONObj cmd, BSONObjBuilder* _b) {
        BSONObjBuilder& b = *_b;
        DEV log() << "replSet received elect msg " << cmd.toString() << rsLog;
        else LOG(2) << "replSet received elect msg " << cmd.toString() << rsLog;
        string set = cmd["set"].String();
        unsigned whoid = cmd["whoid"].Int();
        int cfgver = cmd["cfgver"].Int();
        OID round = cmd["round"].OID();
        int myver = rs.config().version;

        const Member* hopeful = rs.findById(whoid);

        int vote = 0;
        string errmsg;
        if( set != rs.name() ) {
            log() << "replSet error received an elect request for '" << set << "' but our set name is '" << rs.name() << "'" << rsLog;
        }
        else if( myver < cfgver ) {
            // we are stale.  don't vote
        }
        else if (shouldVeto(whoid, cfgver, errmsg)) {
            log() << "Election vetoed with: " << errmsg << rsLog;
            vote = -10000;
        }
        else {
            GTIDManager* gtidMgr = theReplSet->gtidManager.get();
            bool voteYes;
            if (cmd["primaryToUse"].ok()) {
                GTID remoteGTID = getGTIDFromBSON("gtid", cmd);
                gtidMgr->acceptPossiblePrimary(cmd["primaryToUse"].numberLong(), remoteGTID);
            }
            else {
                // it's 1.5 machine, with the older protocol
                voteYes = true;
            }
            if (voteYes) {
                vote = yea(whoid);
                dassert( hopeful->id() == whoid );
                log() << "replSet info voting yea for " <<  hopeful->fullName() << " (" << whoid << ')' << rsLog;
            }
            else {
                log() << "Due to bad possible primary, replSet did NOT vote yea for " <<  hopeful->fullName() << " (" << whoid << ')' << rsLog;
            }
        }

        b.append("vote", vote);
        b.append("round", round);
    }

    void ReplSetImpl::_getTargets(list<Target>& L, int& configVersion) {
        configVersion = config().version;
        for( Member *m = head(); m; m=m->next() )
            if( m->hbinfo().maybeUp() )
                L.push_back( Target(m->fullName()) );
    }

    /* config version is returned as it is ok to use this unlocked.  BUT, if unlocked, you would need
       to check later that the config didn't change. */
    void ReplSetImpl::getTargets(list<Target>& L, int& configVersion) {
        if( lockedByMe() ) {
            _getTargets(L, configVersion);
            return;
        }
        lock lk(this);
        _getTargets(L, configVersion);
    }

    /* Do we have the newest data of them all?
       @param allUp - set to true if all members are up.  Only set if true returned.
       @return true if we are freshest.  Note we may tie.
    */
    bool Consensus::weAreFreshest(bool& allUp, int& nTies, uint64_t& highestKnownPrimary) {
        nTies = 0;
        GTID ourGTID = rs.gtidManager->getLiveState();
        highestKnownPrimary = rs.gtidManager->getHighestKnownPrimary();
        BSONObjBuilder cmdBuilder;
        cmdBuilder.append("replSetFresh", 1);
        cmdBuilder.append("set", rs.name());
        addGTIDToBSON("GTID", ourGTID, cmdBuilder);
        cmdBuilder.append("who", rs._self->fullName());
        cmdBuilder.append("cfgver", rs._cfg->version);
        cmdBuilder.append("id", rs._self->id());
        BSONObj cmd = cmdBuilder.done();

        list<Target> L;
        int ver;
        /* the following queries arbiters, even though they are never fresh.  wonder if that makes sense.
           it doesn't, but it could, if they "know" what freshness it one day.  so consider removing
           arbiters from getTargets() here.  although getTargets is used elsewhere for elections; there
           arbiters are certainly targets - so a "includeArbs" bool would be necessary if we want to make
           not fetching them herein happen.
           */
        rs.getTargets(L, ver);
        multiCommand(cmd, L);
        int nok = 0;
        allUp = true;
        for( list<Target>::iterator i = L.begin(); i != L.end(); i++ ) {
            if( i->ok ) {
                nok++;
                if( i->result["fresher"].trueValue() ) {
                    log() << "not electing self, we are not freshest" << rsLog;
                    return false;
                }
                GTID remoteGTID = getGTIDFromBSON("GTID", i->result);
                if( GTID::cmp(remoteGTID, ourGTID) == 0 ) {
                    nTies++;
                }
                verify( GTID::cmp(remoteGTID, ourGTID) <= 0 );

                if( i->result["veto"].trueValue() ) {
                    BSONElement msg = i->result["errmsg"];
                    if (!msg.eoo()) {
                        log() << "not electing self, " << i->toHost << " would veto with '" <<
                            msg.String() << "'" << rsLog;
                    }
                    else {
                        log() << "not electing self, " << i->toHost << " would veto" << rsLog;
                    }
                    return false;
                }
                // 1.5 members won't be sending this
                if ( i->result["hkp"].ok()) {
                    uint64_t memHighestKnownPrimary = i->result["hkp"].numberLong();
                    if (memHighestKnownPrimary > highestKnownPrimary) {
                        highestKnownPrimary = memHighestKnownPrimary;
                    }
                }
            }
            else {
                DEV log() << "replSet freshest returns " << i->result.toString() << rsLog;
                allUp = false;
            }
        }
        LOG(1) << "replSet dev we are freshest of up nodes, nok:" << nok << " nTies:" << nTies << rsLog;
        // <= as this may change while we are working...
        verify( GTID::cmp(ourGTID, rs.gtidManager->getLiveState()) <= 0 );
        return true;
    }

    extern time_t started;

    void Consensus::multiCommand(BSONObj cmd, list<Target>& L) {
        verify( !rs.lockedByMe() );
        mongo::multiCommand(cmd, L);
    }

    void Consensus::_electSelf() {
        if( time(0) < steppedDown ) {
            return;
        }
        
        bool allUp;
        int nTies;
        uint64_t highestKnownPrimary = 0;
        if( !weAreFreshest(allUp, nTies, highestKnownPrimary) ) {
            return;
        }

        rs.sethbmsg("",9);

        if( !allUp && time(0) - started < 60 * 5 ) {
            /* the idea here is that if a bunch of nodes bounce all at once, we don't want to drop data
               if we don't have to -- we'd rather be offline and wait a little longer instead
               todo: make this configurable.
               */
            rs.sethbmsg("not electing self, not all members up and we have been up less than 5 minutes");
            return;
        }

        Member& me = *rs._self;

        if( nTies ) {
            /* tie?  we then randomly sleep to try to not collide on our voting. */
            /* todo: smarter. */
            if( me.id() == 0 || sleptLast ) {
                // would be fine for one node not to sleep
                // todo: biggest / highest priority nodes should be the ones that get to not sleep
            }
            else {
                verify( !rs.lockedByMe() ); // bad to go to sleep locked
                unsigned ms = ((unsigned) rand()) % 1000 + 50;
                DEV log() << "replSet tie " << nTies << " sleeping a little " << ms << "ms" << rsLog;
                sleptLast = true;
                sleepmillis(ms);
                throw RetryAfterSleepException();
            }
        }
        sleptLast = false;

        time_t start = time(0);
        unsigned meid = me.id();
        int tally = yea( meid );
        bool success = false;
        try {
            log() << "replSet info electSelf " << meid << rsLog;
            uint64_t primaryToUse = highestKnownPrimary+1;
            BSONObjBuilder b;
            b.append("replSetElect", 1);
            b.append("set", rs.name());
            b.append("who", me.fullName());
            b.append("whoid", me.hbinfo().id());
            b.append("cfgver", rs._cfg->version);
            b.append("round", OID::gen());
            b.append("primaryToUse", primaryToUse);
            b.append("gtid", theReplSet->gtidManager->getLiveState());
            BSONObj electCmd = b.obj();

            int configVersion;
            list<Target> L;
            rs.getTargets(L, configVersion);
            multiCommand(electCmd, L);

            {
                for( list<Target>::iterator i = L.begin(); i != L.end(); i++ ) {
                    DEV log() << "replSet elect res: " << i->result.toString() << rsLog;
                    if( i->ok ) {
                        int v = i->result["vote"].Int();
                        tally += v;
                    }
                }
                if( tally*2 <= totalVotes() ) {
                    log() << "replSet couldn't elect self, only received " << tally << " votes" << rsLog;
                }
                else if( time(0) - start > 30 ) {
                    // defensive; should never happen as we have timeouts on connection and operation for our conn
                    log() << "replSet too much time passed during our election, ignoring result" << rsLog;
                }
                else if( configVersion != rs.config().version ) {
                    log() << "replSet config version changed during our election, ignoring result" << rsLog;
                }
                else if (!theReplSet->gtidManager->acceptPossiblePrimary(primaryToUse, theReplSet->gtidManager->getLiveState())) {
                    log() << "Could not accept " << primaryToUse << " as a primary GTID value, another election likely ssnuck in"<< rsLog;
                }
                else {
                    /* succeeded. */
                    LOG(1) << "replSet election succeeded, assuming primary role" << rsLog;
                    theReplSet->handleHighestKnownPrimaryOfMember(primaryToUse);
                    success = rs.assumePrimary(primaryToUse);
                    if (!success) {
                        log() << "tried to assume primary and failed" << rsLog;
                    }
                }
            }
        }
        catch( std::exception& ) {
            throw;
        }
    }

    void Consensus::electSelf() {
        verify( !rs.lockedByMe() );
        verify( !rs.myConfig().arbiterOnly );
        verify( rs.myConfig().slaveDelay == 0 );
        try {
            _electSelf();
        }
        catch(RetryAfterSleepException&) {
            throw;
        }
        catch(DBException& e) {
            log() << "replSet warning caught unexpected exception in electSelf() " << e.toString() << rsLog;
        }
        catch(...) {
            log() << "replSet warning caught unexpected exception in electSelf()" << rsLog;
        }
    }

}
