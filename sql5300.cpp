#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "heap_storage.h"
using namespace std;
using namespace hsql;

DbEnv*  _DB_ENV;

void initializeDBenv(char *DBenv);
string execute(const SQLStatement *query);
string selectTable(const SelectStatement *query);
string expressionToString(const Expr *expr);
string opToString(const Expr *expr);
string tableRefInfoToString(const TableRef *table);

string tableRefInfoToString(const TableRef *table) {
    string tableString;
    string aliasAS = "";
    if (table->alias != NULL){
        aliasAS =" AS ";
    }
    //for joins
    string joinStr= "";
    string leftTable=""; //for left side
    string rightTable=""; //for right table
    string  joinCond =""; //join cond
   
    switch (table->type) {
        case kTableSelect:
        tableString += ""; 
        break;
        case kTableName: 
        {
            tableString += table->name;
            tableString +=aliasAS + table->alias;
            break;
        }
            
        case kTableJoin:
            leftTable = tableRefInfoToString(table->join->left);
            rightTable = tableRefInfoToString(table->join->right);
            
            if (table->join->condition != NULL)
            {   
                 joinCond = "ON " + expressionToString(table->join->condition);
            }
           
            switch (table->join->type) {
                case kJoinCross:
                 {
                    joinStr= " CROSS JOIN ";
                    break;
                }
                case kJoinInner:
                {
                    joinStr= " JOIN ";
                    break;
                }
                  
                case kJoinOuter:
                 {
                    joinStr= " OUTER JOIN ";
                    break;
                }
                case kJoinLeftOuter:
                 {
                    joinStr= " LEFT OUTER JOIN ";
                    break;
                }
                case kJoinLeft:
                    joinStr= " LEFT JOIN ";
                    break;
                case kJoinRightOuter:
                 {
                    joinStr= " RIGHT OUTER JOIN ";
                    break;
                }
                case kJoinRight:
                    joinStr= " RIGHT JOIN ";
                    break;
                case kJoinNatural:
                    joinStr= " NATURAL JOIN ";
                    break;
            }

            tableString +=leftTable + joinStr + rightTable+ joinCond ;
            break;
        case kTableCrossProduct:
        {
            bool doComma = false;
            for (TableRef* tbl : *table->list) {
                if (doComma)
                    joinStr += ", ";
                joinStr += tableRefInfoToString(tbl);
                doComma = true;
             }
    

            break;
        }   
    }
    return tableString;
}


string opToString(const Expr *expr){
    if (expr == NULL){
    return "null";
    }

    string ret;

    if (expr->opType == Expr::NOT){
    ret += "NOT ";
    }
    //LHS of the operator
    ret += expressionToString(expr->expr) + " ";
    
    switch (expr->opType){
    case Expr::SIMPLE_OP:
        ret += expr->opChar;
        break;
    case Expr::AND:
        ret += "AND";
        break;
    case Expr::OR:
        ret+= "OR";
        break;
    default:
        break; //good style, handles if not is used
    }
    
    //RHS of the operator in the case of anything but NOT
    if(expr->expr2 != NULL){
    ret += " " + expressionToString(expr->expr2);
    }
    return ret;
}


//translate AST expression into string
string expressionToString(const Expr *expr){
    string ret;
    switch(expr->type){
    case kExprStar:
        ret += "*";
        break;
    case kExprColumnRef:
        if (expr->table != NULL){
        ret += string(expr->table) + ".";
        }
        break;
    case kExprLiteralString:
        ret += expr -> name;
        break;
    case kExprLiteralFloat:
        ret += to_string(expr->fval);
        break;
    case kExprLiteralInt:
        ret += to_string(expr->ival);
        break;
    case kExprFunctionRef:
        ret += string(expr->name) + "?" + expr->expr->name;
        break;
    case kExprOperator:
        ret += opToString(expr);
        break;
    default:
        ret += "???"; //missing expr type
        break;
    }
    if (expr->alias != NULL){
    ret += string(" AS ") + expr->alias;
    }
    return ret;
}
    


//execute select function:
string selectTable(const SelectStatement *stmt){
    string ret("SELECT ");
   
    bool doComma = false;
    for (Expr* expr : *stmt->selectList){
    if(doComma){
        ret += ", ";
    }
    ret += expressionToString(expr);
    doComma = true;
    }
    ret += " FROM " + tableRefInfoToString(stmt->fromTable);
    if (stmt->whereClause != NULL){
    ret += " WHERE " + expressionToString(stmt->whereClause);
    }
    
    return ret;
}

string columnDefinitionToString(const ColumnDefinition *col){
    string ret(col->name);
    switch (col->type){
    case ColumnDefinition::DOUBLE:
        ret += " DOUBLE";
        break;
    case ColumnDefinition::INT:
        ret += " INT";
        break;
    case ColumnDefinition::TEXT:
        ret += " TEXT";
        break;
    default:
    ret += " ...";
    break;
    }
    return ret;
}

//set db environement, not working atm 
void initializeDBenv(char *DBenv){


cout << "(sql5300: running with database environment at"<<DBenv<< ")"<<endl;

    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    try {
        env.open(DBenv, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException& exc) {
        cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }
    _DB_ENV = &env;
    

}

//create table using passed sql statement
string createTable(const CreateStatement *stmt) {

    string sql_statement("CREATE TABLE ");

    if (stmt->type != CreateStatement::kTable )
        return sql_statement= "Invalid create statement";

    //if sql table does not exist,then append
    if (stmt->ifNotExists)
        sql_statement += "IF NOT EXISTS ";

    if(stmt->tableName)
    sql_statement += string(stmt->tableName);

    //start of param
    sql_statement += "(";

    bool doComma = false;
    for (ColumnDefinition *col : *stmt->columns) {
        if(doComma)
            sql_statement += ", ";
        sql_statement += columnDefinitionToString(col);
        doComma = true;
    }

    //end of param
    sql_statement += ")";
    return sql_statement;
}

string execute(const SQLStatement *stmt) {
    //check the sql statment for type , TODO INSERTION 
    switch (stmt->type()) {
    case kStmtSelect:
         return selectTable((const SelectStatement*) stmt);
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
    //initializeDBenv(argv[1]);
    //loop to get user inputs
    char *myenv = argv[1];
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    try {
        env.open(myenv, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException& exc) {
        cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }
    _DB_ENV = &env;

    //SQL Shell
    while(true){
        string SQLinput;
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
        if (SQLinput == "test") {
            cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
            continue;
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
            for (uint i = 0 ; i< result->size(); i++){
                //hsql::SQLStatement* statement = result->getStatement(i);
                cout << execute(result->getStatement(i)) << endl;
            }

        }

    }
    return EXIT_SUCCESS;
}

