/**
*    Copyright (C) 2012 10gen Inc.
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

#include "mongo/db/auth/authz_manager_external_state.h"

#include "mongo/base/status.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/security_key.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespacestring.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    AuthzManagerExternalState::AuthzManagerExternalState() {}
    AuthzManagerExternalState::~AuthzManagerExternalState() {}

    Status AuthzManagerExternalState::getPrivilegeDocument(const UserName& userName,
                                                           int authzVersion,
                                                           BSONObj* result) {
        if (userName == internalSecurity.user->getName()) {
            return Status(ErrorCodes::InternalError,
                          "Requested privilege document for the internal user");
        }

        StringData dbname = userName.getDB();

        if (dbname == StringData("$external", StringData::LiteralTag()) ||
            dbname == AuthorizationManager::SERVER_RESOURCE_NAME ||
            dbname == AuthorizationManager::CLUSTER_RESOURCE_NAME) {
            return Status(ErrorCodes::UserNotFound,
                          mongoutils::str::stream() << "No privilege documents stored in the " <<
                          dbname << " user source.");
        }

        if (!NamespaceString::validDBName(dbname)) {
            return Status(ErrorCodes::BadValue, "Bad database name \"" + dbname + "\"");
        }

        if (dbname == StringData("local", StringData::LiteralTag()) &&
            userName.getUser() == internalSecurity.user) {

            if (internalSecurity.pwd.empty()) {
                return Status(ErrorCodes::UserNotFound,
                              "key file must be used to log in with internal user",
                              15889);
            }
            *result = BSON(AuthorizationManager::USER_NAME_FIELD_NAME <<
                           internalSecurity.user <<
                           AuthorizationManager::PASSWORD_FIELD_NAME <<
                           internalSecurity.pwd).getOwned();
            return Status::OK();
        }

        std::string usersNamespace = dbname + ".system.users";

        BSONObj userBSONObj;
        Status status = findOne(
                AuthorizationManager::usersCollectionNamespace,
                BSONObj(),
                &userBSONObj);
        // If the status is NoMatchingDocument, there are no privilege documents.
        // If it's OK, there are.  Otherwise, we were unable to complete the query,
        // so best to assume that there _are_ privilege documents.  This might happen
        // if the node contaning the users collection becomes transiently unavailable.
        // See SERVER-12616, for example.
        return status != ErrorCodes::NoMatchingDocument;
    }


    Status AuthzManagerExternalState::insertPrivilegeDocument(const string& dbname,
                                                              const BSONObj& userObj,
                                                              const BSONObj& writeConcern) {
        Status status = insert(NamespaceString("admin.system.users"), userObj, writeConcern);
        if (status.isOK()) {
            return status;
        }
        if (status.code() == ErrorCodes::DuplicateKey) {
            std::string name = userObj[AuthorizationManager::USER_NAME_FIELD_NAME].String();
            std::string source = userObj[AuthorizationManager::USER_DB_FIELD_NAME].String();
            return Status(ErrorCodes::DuplicateKey,
                          mongoutils::str::stream() << "User \"" << name << "@" << source <<
                                  "\" already exists");
        }
        else {
            queryBuilder.append(AuthorizationManager::USER_SOURCE_FIELD_NAME,
                                userName.getDB());
        }

        bool found = _findUser(usersNamespace, queryBuilder.obj(), &userBSONObj);
        if (!found) {
            return Status(ErrorCodes::UserNotFound,
                          mongoutils::str::stream() << "auth: couldn't find user " <<
                          userName.toString() << ", " << usersNamespace,
                          0);
        }

        *result = userBSONObj.getOwned();
        return Status::OK();
    }

    bool AuthzManagerExternalState::hasAnyPrivilegeDocuments() {
        std::string usersNamespace = "admin.system.users";

        BSONObj userBSONObj;
        BSONObj query;
        return _findUser(usersNamespace, query, &userBSONObj);
    }

}  // namespace mongo
