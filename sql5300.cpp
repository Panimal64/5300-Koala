#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
using namespace hsql;

void initializeDBenv(char *DBenv);
string execute(const SQLStatement *query);


//set db environement 
void initializeDBenv(char *DBenv){

    cout << "(sql5300: running with database environment at"<<DBenv<< ")"<<endl;

    DbEnv myEnv(0U);
    myEnv.set_message_stream(&cout);
    myEnv.set_error_stream(&cerr);

    //attempt to create environment 
    try {
        myEnv.open(DBenv, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch(DbException &e) {
        std::cerr << e.what() << std::endl;
    }

}


string createTable(const CreateStatement *stmt) {
    string ret("CREATE TABLE ");
    if (stmt->type != CreateStatement::kTable )
        return ret + "...";
    if (stmt->ifNotExists)
        ret += "IF NOT EXISTS ";
    ret += string(stmt->tableName) + " (";
    bool doComma = false;
    for (ColumnDefinition *col : *stmt->columns) {
        if(doComma)
            ret += ", ";
        ret += columnDefinitionToString(col);
        doComma = true;
    }
    ret += ")";
    return ret;
}

string execute(const SQLStatement *stmt) {
    //check the sql statment for type , TODO INSERTION 
    switch (stmt->type()) {
    // case kStmtSelect:
    //     return selectTable((const SelectStatement*) stmt);
    case kStmtCreate:
        return createTable((const CreateStatement*) stmt);
    default:
        return "No Implementation";
    }
} 


int main(int argc, char **argv) {

    if(argc != 2){
        cerr<< "Error in Database Path for cpsc5300"<< endl;
        return 1;
    }
    //initialize environment
    initializeDBenv(argv[1]);
    //loop to get user inputs
    
    //SQL Shell
    while(true){
        string SQLinput;
        cout << endl;
        cout << "SQL>";
        getline(cin, SQLinput);
        //not inputs
        if (SQLinput.length() == 0){
            continue;
        }
        //if quit, quit
        if(SQLinput == "quit"){
            break;
        }
        //hyrise parser
        SQLParserResult *result = SQLParser::parseSQLString(SQLinput);
        //check if hyrise function is Valid, continue loop if not
        if (!result->isValid()) {
            cout << "Invalid SQL statment"<<endl;
            continue;
        }
        //print out sqlstatment
        else{
            for (int i = 0 ; i< result->size(); i++){
                //hsql::SQLStatement* statement = result->getStatement(i);
                cout<< result->getStatement(i)<<endl;
            }

        }

    }
    return 0;
}


