/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Gregory Deresinski, Dave Pannu with assistance from Professor Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */

/**
Additional notes:  All "FIXME" statements from MS4 prep file have been left intact.
The code has been implemented to work but the statments remain to allow the next group
to find the implemented code easier by searching for 'Fixme'.
*/
#include "SQLExec.h"
#include "EvalPlan.h"
#include <iostream>

using namespace std;
using namespace hsql;

Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

/**
Allow results of query to print
*/
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

/**
Destructor to minimize memory leaks
*/
QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}

/**
Execute correct statement type
*/
QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();
    if (SQLExec::indices == nullptr)
        SQLExec::indices = new Indices();


    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            // porting from Milestone5_prep    
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);                
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

// porting from Milestone5_prep
QueryResult *SQLExec::insert(const InsertStatement *statement) {
    Identifier table_name = statement->tableName;                  // get table name from insert statement     
    DbRelation& table = SQLExec::tables->get_table(table_name);    // use table name to obtain table from SQLExec::tables

    ColumnNames column_names;
    ColumnAttributes column_attributes;
    SQLExec::tables->get_columns(table_name, column_names, column_attributes);     // get column info

    ValueDict row;                  // construct row to do table insert
	Handles i_handles;              // handles to store the row
    int indices_n = 0;              // counter for index
    bool eligible = true;           // flag to indicate whether it is eligible to execute INSERT

    // check if the number of values matches the column number of table or the specific table does exist
    if ((statement->values->size() != column_names.size()) || column_names.empty())
        eligible = false;

    if (statement->columns != NULL) {                              // if column info in INSERT command exists 
        for ( uint i = 0; i < statement->columns->size(); i++) {   // scan all the columns of table
            Identifier column_name = statement->columns->at(i);    // abstract the column name from INSERT command
            if (find(column_names.begin(), column_names.end(), column_name) == column_names.end()) 
                eligible = false;           // specific column of INSERT command can not find a match in table's columns
        }
    }

    if (eligible) {                                             // eligible to execute INSERT
        for( uint i = 0; i < statement->values->size(); i++) {  // scan all the values of INSERT command
            Identifier column_name = column_names.at(i);        // abstract the column name from table
            Expr *expr = statement->values->at(i);              // abstract the type of expression of INSERT command 
		    switch (expr->type) {
			    case kExprLiteralString:                        // string type
				    row[column_name] = Value(expr->name);
                    break;
                case kExprLiteralInt:                           // integer type
				    row[column_name] = Value(expr->ival);
                    break;
                default:                                        // others' types are unrecognized
                    throw SQLExecError("Unrecognized column type");
		    }
        }
        i_handles.push_back(table.insert(&row));                // insert row to table        
    }
    else    // not eligible to execute INSERT
        throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
    // add indices
	for (auto const& index_name: SQLExec::indices->get_index_names(table_name)) {
		DbIndex& index = SQLExec::indices->get_index(table_name, index_name); // get the DbIndex using table name/index name
        indices_n++;                // count for number of index
		for (auto const &handle: i_handles)
			index.insert(handle);   // insert record into index
	}

    return new QueryResult("successfully inserted " + to_string(i_handles.size()) + 
                            " row into " + table_name + " and " + to_string(indices_n) + " indices");  // FIXME MILESTONE5
}

QueryResult *SQLExec::del(const DeleteStatement *statement) {
    try {
        Identifier table_name = statement->tableName;                           // get the table name from DELETE command
        DbRelation& table = SQLExec::tables->get_table(table_name);             // get the corresponding table
        EvalPlan *plan = new EvalPlan(table);                                   // create a new TableScan plan 
        if (statement->expr != nullptr)                                         // if SELECT ... FROM ... WHERE
            plan = new EvalPlan(get_where_conjunction(statement->expr), plan);  // create a new plan for SELECT

        EvalPlan *optimized = plan->optimize();                                 // optimize the plan
        EvalPipeline pipeline = optimized->pipeline();                          // pipeline gets handles

        auto index_names = SQLExec::indices->get_index_names(table_name);       // get the corresponding index names
        Handles *handles = pipeline.second;                                     // assign record ID to handles

        int indices_n = index_names.size();                                     // count for number of indices
        int row_n = 0;                                                          // counter of number of del rows

        for (auto const& handle : *handles) {                                   // scan all the del rows
            for (auto const& index_name : index_names) {                        // scan all the corresponding indices
                DbIndex &index = SQLExec::indices->get_index(table_name, index_name);   // get index table 
                index.del(handle);                                              // delete row's index
            }
        }

        for (auto const& handle : *handles) {                                   // scan all the del rows
            table.del(handle);                                                  // delete row in the table
            row_n++;                                                            // count for number of del rows
        }

        delete handles;

        string suffix = "successfully deleted " + to_string(row_n) + " rows";   // construct the del message
        if (indices_n >0)                                                       // if index exists, show index info
            suffix += " from " + to_string(indices_n) + " indices";

        return new QueryResult(suffix);
        
    } catch (DbException& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

ValueDict *SQLExec::get_where_conjunction(const Expr *whereClause) {
    ValueDict* rows = new ValueDict();                                      // construct a return ValueDict
    if (whereClause->type == kExprOperator) {                               // if expression type is kExprOperator
        if (whereClause->opType == Expr::AND) {                             // if opType is AND, ex: id = 1 AND data = "one"
            ValueDict* sub_row = get_where_conjunction(whereClause->expr);  // recursively invoke for expr
            rows->insert(sub_row->begin(), sub_row->end());                 // merge expr's ValueDict
            sub_row = get_where_conjunction(whereClause->expr2);            // recursively invoke for expr2
            rows->insert(sub_row->begin(), sub_row->end());                 // merge expr2's ValueDict
        }
        else if (whereClause->opType == Expr::SIMPLE_OP) {                  // if opType is SIMPLE_OP, ex id = 1
            Identifier column_name = whereClause->expr->name;               // store the key from expr->name
            Value value;
            switch (whereClause->expr2->type) {
                case kExprLiteralString:                                    // if value is String
                    value = Value(whereClause->expr2->name);
                    break;
                case kExprLiteralInt:                                       // if value is INT
                    value = Value(whereClause->expr2->ival);
                    break;
                default:
                    throw DbRelationError("Unrecognized value!");
                    break;
            }
            (*rows)[column_name] = value;                                   // insert key and value into ValueDict
        }
        else
            throw DbRelationError("Unsupport opType!");
    }
    else 
        throw DbRelationError("Unsupport type!");

    return rows;
}

QueryResult *SQLExec::select(const SelectStatement *statement) {
    try {
        DbRelation& table = SQLExec::tables->get_table(statement->fromTable->name);     // get the table from SELECT command    
        EvalPlan *plan = new EvalPlan(table);                                           // create a new TableScan plan

        ColumnNames* column_names = new ColumnNames;                                    // construct new ColumnNames
	    ColumnAttributes* column_attributes = new ColumnAttributes;                     // construct new ColumnAttributes

        if (statement->whereClause != nullptr)                                          // if whereClause is not nullptr
            plan = new EvalPlan(get_where_conjunction(statement->whereClause), plan);   // create a new plan for SELECT

        if (statement->selectList->at(0)->type == kExprStar) {                          // do "SELECT * from ...""
            *column_names = table.get_column_names();                                   // get the column names of the table  
            plan = new EvalPlan(EvalPlan::ProjectAll, plan);                            // create a new plan for ProjectAll
        }
        else {
            for (auto const column : *statement->selectList)                            // scan all the columns after SELECT
                column_names->push_back(column->name);                                  // store columns which are prepared for projection

            plan = new EvalPlan(column_names, plan);                                    // create a new plan for Project particular column(s)
        }
    
        EvalPlan *optimized = plan->optimize();                                         // attempt to get the best equivalent evaluation plan
	    ValueDicts *rows = optimized->evaluate();                                       // evaluate the plan
	    column_attributes = table.get_column_attributes(*column_names);                 // get the attributes info using column names


    return new QueryResult(column_names, column_attributes, rows, "Successfully returned " + to_string(rows->size()) + " rows.");

    } catch (DbException& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/**
Obtain the type of column and it's attributes and data type
*/
void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

/**
Create statement for SQL, currently limited to Create Table & Create Index
*/
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch(statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

/**
Create Table SQL statement
Use case: CREATE TABLE shotablename(columnname columnattribute, ..., columnname columnattribute)
*/
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation& table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (exception& e) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception& e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}


/**FIXME-DONE
Create index on a specific table using columns
Use: CREATE INDEX indexname ON tablename(columnname)
*/
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    
    
    Identifier table_name = statement->tableName;
    Identifier index_name = statement->indexName;
    Identifier index_type = statement->indexType;
    ColumnNames* column_names = new ColumnNames;
    ColumnAttributes* column_attributes = new ColumnAttributes;
    ValueDict row;
    Handles* index_handles = new Handles;
    vector<char*>* index_columns = statement->indexColumns;


    //Determine if BTREE or HASH
    if (index_type == "BTREE"){
        row["is_unique"] = true;
    }
    else{
        row["is_unique"] = false;
    }

    //Check to see if column exists in table
    tables->get_columns(table_name, *column_names,*column_attributes);
    for(auto const& column : *index_columns){
        bool check = false;
        string col = column;
        for(auto const& tblCol : *column_names) {
            if(tblCol == col)
                check = true;
        }
        if(!check)
            throw SQLExecError("COLUMN NAME " + col + " IS NOT IN TABLE " + table_name);
    }

    //Insert row form index columns into _indices
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(index_type);
    
    int seq_count = 0;
    for (char* index_col : *index_columns) {
        row["column_name"] = Value(index_col);
        row["seq_in_index"] = ++seq_count;
        Handle index_handle = SQLExec::indices->insert(&row);
        index_handles->push_back(index_handle);
    }

    DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
    index.create();

    
    return new QueryResult("Created Index " + index_name);
}

// DROP index or table based on statement specifics
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch(statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}
 
/**
SQL drop table statement
USE: DROP TABLE tablename
*/
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation& table = SQLExec::tables->get_table(table_name);



    /* FIXME-DONE: drop any indices! */
    IndexNames index_name = SQLExec::indices->get_index_names(table_name);
    for(auto const& index_names : index_name){
        DbIndex& to_drop = indices->get_index(table_name, index_names);
        to_drop.drop();
    }
    Handles* handle = indices->select(&where);
    for (auto const& handles : *handle){
        indices->del(handles);
    }
    delete handle;
    /* End FIXME */


    // remove from _columns schema
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* handles = columns.select(&where);
    for (auto const& handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); // expect only one row from select

    return new QueryResult(string("dropped ") + table_name);
}

/**
FIXME-DONE  Drop specified index for a table
Use: DROP INDEX indexname FROM tablename
*/
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    ValueDict search;
    search["table_name"] = table_name;
    search["index_name"] = index_name;
    Handles* handle = SQLExec::indices->select(&search);
    for(auto const& handles : *handle)
        indices->del(handles);

    delete handle;
    return new QueryResult("DROPPED INDEX " + index_name);
}

//Show table column or index based on statement specifics
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

/**
//FIXME-DONE
Based off show_columns provided by professor Lundeen.  
Shows columns in index
*/
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = SQLExec::indices->select(&where);

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::indices->project(handle, column_names); 
        rows->push_back(row);
    }                                                   
    string result = "successfully returned " + to_string(rows->size()) + " rows";
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, result);
}

/**
Show statement for tables
*/
QueryResult *SQLExec::show_tables() {
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles* handles = SQLExec::tables->select();
    u_long n = handles->size() - 2;

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME)
            rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}
/**
Show statement for columns
*/
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

