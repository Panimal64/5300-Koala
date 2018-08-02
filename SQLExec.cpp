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
    return new QueryResult("INSERT statement not yet implemented");  // FIXME
}

QueryResult *SQLExec::del(const DeleteStatement *statement) {
    return new QueryResult("DELETE statement not yet implemented");  // FIXME
}

QueryResult *SQLExec::select(const SelectStatement *statement) {
    return new QueryResult("SELECT statement not yet implemented");  // FIXME
}

/**
Obtain the type of colum and it's attributes and data type
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

