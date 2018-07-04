#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "SQLParser.h"
#include "sqlhelper.h"
#include "db_cxx.h"

using namespace std;
void initializeDBenv(char *DBenv);

int main(int argc, char *argv[]) {
    //initialize environment
    initializeDBenv(argv[1]);
    //loop to get user inputs
    
    while(true){
        string SQLinput;
        cout << "endl";
        cout << "SQL>";
        getline(cin, SQLinput);
        //if quit, quit
        if(SQLinput == "quit"){
            break;
        }
        //not inputs
        if (SQLinput.length() < 1){
            continue;
        }
        //hyrise parser
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(SQLinput);
        //check if hyrise function is Valid, continue loop if not
        if (!result->isValid()) {
            cout << "Invalid SQL statment";
            continue;
        }
        //print out sqlstatment
        else{
            for (int i = 0 ; i< result->size(); i++){
                //hsql::SQLStatement* statement = result->getStatement(i);
                cout<< result->getStatement(i);
            }

        }
        //reset
        

    }
}


void initializeDBenv(char *DBenv){
    cout << "(Sql5300: running with database environment at"<<DBenv<<")";

    std::string envHome(DBenv);
    DbEnv *myEnv = new DbEnv(0U);

    try {
        myEnv->open(DBenv, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch(DbException &e) {
        std::cerr << "Error opening database environment: "
                  << envHome << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
    } catch(std::exception &e) {
        std::cerr << "Error opening database environment: "
                  << envHome << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
    }

}
