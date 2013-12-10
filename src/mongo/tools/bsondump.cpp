// restore.cpp

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
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects
*    for all of the code used other than as permitted herein. If you modify
*    file(s) with this exception, you may extend this exception to your
*    version of the file(s), but you are not obligated to do so. If you do not
*    wish to do so, delete this exception statement from your version. If you
*    delete this exception statement from all source files in the program,
*    then also delete it in the license file.
*/

#include "../pch.h"
#include "mongo/base/initializer.h"
#include "mongo/client/dbclientcursor.h"
#include "../util/text.h"
#include "tool.h"

#include <boost/program_options.hpp>

#include <fcntl.h>

using namespace mongo;

namespace po = boost::program_options;

class BSONDump : public BSONTool {

    enum OutputType { JSON , DEBUG } _type;

public:

    BSONDump() : BSONTool( "bsondump", NONE ) {
        add_options()
        ("type" , po::value<string>()->default_value("json") , "type of output: json,debug" )
        ;
        add_hidden_options()
        ("file" , po::value<string>() , ".bson file" )
        ;
        addPositionArg( "file" , 1 );
        _noconnection = true;
    }

    virtual void printExtraHelp(ostream& out) {
        out << "Display BSON objects in a data file.\n" << endl;
        out << "usage: " << _name << " [options] <bson filename>" << endl;
    }

    virtual int doRun() {
        {
            string t = getParam( "type" );
            if ( t == "json" )
                _type = JSON;
            else if ( t == "debug" )
                _type = DEBUG;
            else {
                cerr << "bad type: " << t << endl;
                return 1;
            }
        }

        boost::filesystem::path root = getParam( "file" );
        if ( root == "" ) {
            printExtraHelp(cout);
            return 1;
        }

        processFile( root );
        return 0;
    }

    bool debug( const BSONObj& o , int depth=0) {
        string prefix = "";
        for ( int i=0; i<depth; i++ ) {
            prefix += "\t\t\t";
        }

        int read = 4;

        try {
            cout << prefix << "--- new object ---\n";
            cout << prefix << "\t size : " << o.objsize() << "\n";

            // Note: this will recursively check each level of the bson and will also be called by
            // this function at each level. While inefficient, it shouldn't effect correctness.
            const Status status = validateBSON(o.objdata(), o.objsize());
            if (!status.isOK()) {
                cout << prefix << "\t OBJECT IS INVALID: " << status.reason() << '\n'
                     << prefix << "\t attempting to print as much as possible" << endl;
            }
            
            BSONObjIterator i(o);
            while ( i.more() ) {
                // This call verifies it is safe to call size() and fieldName() but doesn't check
                // whether the element extends past the end of the object. That is done below.
                BSONElement e = i.next(/*checkEnd=*/true);

                cout << prefix << "\t\t " << e.fieldName() << "\n"
                     << prefix << "\t\t\t type:" << setw(3) << e.type() << " size: " << e.size()
                     << endl;

                if ( ( read + e.size() ) > o.objsize() ) {
                    cout << prefix << " SIZE DOES NOT WORK" << endl;
                    return false;
                }
                read += e.size();
                try {
                    if ( e.isABSONObj() ) {
                        if ( ! debug( e.Obj() , depth + 1 ) ) {
                            //return false;
                            cout << prefix << "\t\t\t BAD BAD BAD" << endl;
                            
                            if ( e.size() < 1000 ) {
                                cout << "---\n" << e.Obj().hexDump() << "\n---" << endl;
                            }
                        }
                    }
                    else if ( e.type() == String && ! isValidUTF8( e.valuestr() ) ) {
                        cout << prefix << "\t\t\t" << "bad utf8 String!" << endl;
                    }
                    else if ( logger::globalLogDomain()->shouldLog(logger::LogSeverity::Debug(1)) ) {
                        cout << prefix << "\t\t\t" << e << endl;
                    }
                }
                catch ( std::exception& e ) {
                    cout << prefix << "\t\t\t bad value: " << e.what() << endl;
                }
            }
        }
        catch ( std::exception& e ) {
            cout << prefix << "\tbad\t" << e.what() << endl;
            cout << "----\n" << o.hexDump() << "\n---" << endl;
        }
        return true;
    }

    virtual void gotObject( const BSONObj& o ) {
        switch ( _type ) {
        case JSON:
            cout << o.jsonString( TenGen ) << endl;
            break;
        case DEBUG:
            debug(o);
            break;
        default:
            cerr << "bad type? : " << _type << endl;
        }
    }
};

int main( int argc , char ** argv, char **envp ) {
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    BSONDump dump;
    return dump.main( argc , argv );
}
