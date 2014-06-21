// @file rs_exception.h

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

#pragma once

#include "mongo/pch.h"

#include "mongo/util/assert_util.h"

namespace mongo {

    class RetryAfterSleepException : public std::exception {
    public:
        const char * what() const throw () { return "RetryAfterSleepException"; }
    };

    class RollbackOplogException : public DBException {
      public:
        RollbackOplogException() : DBException("Failed to rollback oplog operation", 0) {}
        RollbackOplogException(const string &s) : DBException("Failed to rollback oplog operation: " + s, 0) {}
    };
}
