#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include "heap_storage.h"
using namespace std;

typedef u_int16_t u16;

// from klundeen
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// from klundeen
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

//given the id of a record we try to get the header of the any record with that id
//if it doesn't exist we reutrn null, if it does we pull the data from the
//block and return it in a block pointer
Dbt* SlottedPage::get(RecordID record_id){
    u16 size, loc;
    get_header(size, loc, record_id);
    if (!loc){
	return NULL;
    }
    Dbt* result = new Dbt(this->address(loc), size);
    return result;
}

//this updates an existing record by getting the old record's information, and comparing
//its size to the new size of the incoming data.  If the new block is bigger we check
//if there is room, slide the block left (to make room) and copy the data into it.  
//Else we copy the data into the block and slide the block right (to fill the gap).
//Then we update the headers as appropriate.
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError){
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = data.get_size();
    if (new_size > size){
	u16 extra = new_size - size;
		if(has_room(extra)){
		    slide(loc + new_size, loc + size);//sliding left to add room
		    memcpy(this->address(loc-extra), data.get_data(), new_size);
		}
		else{
		    throw DbBlockNoRoomError("Block is full!");
		}
    }
    else{
	memcpy(this->address(loc), data.get_data(), new_size);
	slide(loc + new_size, loc + size);//sliding right to leave room behind
    }
    get_header(size, loc, record_id);
    put_header(record_id, size, loc);
}

//Deletes a block by setting its size and location to 0, we call slide to 
//fix the end_free value and collapse the data that was there.
void SlottedPage::del(RecordID record_id){
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);

}

//RecordIDs is a vector that is typedef'd in storage_engine.h. So to return
//a pointer of it we will create a new pointer to a a vector, go through the
//number of records we have saved in the object variable.  And we push back
//the number of any that exist into the vector.  Then simply return that vector
//----Maybe make sure we release this memory?---
RecordIDs* SlottedPage::ids(void){
    RecordIDs* record_ids = new RecordIDs();
    for (int i = 1; i < this->num_records; i++){
		u16 size, loc;
		get_header(size, loc, i);
		if (loc != 0){
		    record_ids->push_back(i); //make sure this is right
		}
    }
    return record_ids;
}

//get the header information for a given id.
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id){
    loc = get_n(id*4);
    size = get_n(id*4 + 2);
}


//from klundeen
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

//returns a bool stating if the size given is small enough to fit in the remaining space
//this is done by subtracting from the end_free location in the block (last free byte),
//the number of bytes taken up by the header (including the new entry)
bool SlottedPage::has_room(u_int16_t size){
    u16 free = this->end_free - ((this->num_records + 1) * 4);
    return size <= free;
}

//Slides the data in a block left or right as neccessary to allow for more room in the
//page (or to take up more room). Accomplished by finding the shift and copying the
//information contained in the block that needs moving into the new location. Headers
//are then adjusted to account for the change.
void SlottedPage::slide(u_int16_t start, u_int16_t end){
    u16 shift = end - start;
    if (shift == 0){
	return;
    }
    memcpy(this->address(end_free + 1), this->address(end_free + 1 + shift), shift);

    //adjust newheaders
    u16 size, loc;
    RecordIDs* allIds = ids();//will the memory here be released? be wary of this statement
    for (unsigned int i = 0; i < allIds->size(); i++){
	RecordID id = allIds->at(i);
	get_header(size, loc, id);
	if (loc <= start){
	    loc += shift;
	    put_header(id, size, loc);
	}
    }
    this->end_free += shift;
    put_header();
}


//from klundeen
// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}
//from klundeen
// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}
//from klundeen
// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}


/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
 */
//HeapFile::HeapFile(std::string name) : DbFile(name), last(0), closed(true), db(_DB_ENV, 0) {
//	this->dbfilename = this->name + ".db";
//}

//create file
void HeapFile::create(void){
	db_open(DB_CREATE|DB_EXCL);
	get_new();
}
//delete file
void HeapFile::drop(void){
	close();
	db.remove(this->dbfilename.c_str(), nullptr,0); //filename, flags
}
//open file
void HeapFile::open(void){
	if(this->closed == false){
		db_open();
	}
	
}
//closefile
void HeapFile::close(void){
	this->db.close(0); //pass in flags, check later
	this->closed = true;

}
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);
}
//write block into file
void HeapFile::put(DbBlock* block){
	BlockID block_id = block->get_block_id();
	Dbt key(&block_id, sizeof(block_id));
	this->db.put(nullptr,&key, block->get_block(),0);
	
}
//returns the id  of block for updates
//push a block id number to BLOCK ID
BlockIDs* HeapFile::block_ids(){
	BlockIDs* ids = new BlockIDs(); 
	for(unsigned int block = 1; block <= this->last; block++){
		ids->push_back(block); 
	}
	return ids;
}
//return last bloc id, not sure if needed
// u_int32_t HeapFile::get_last_block_id() {
// 	return this->last;
// }
//open db 

void HeapFile::db_open(uint flags){
	if(this->closed == true){
		return;
	}
	this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO,flags,0);
	//this->last=db.get(DB_LAST); //get last key/data pair of db Check if works
	this->closed = false;
}
/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */


// HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes ){

// }

void HeapTable::create(){
	this->file.create();
}
void HeapTable::create_if_not_exists(){
	try{
		this->open();
	}
	catch (DbException& e){
		this->create();
	}
}
void HeapTable::drop(){
	file.drop();
}
void HeapTable::open(){
	file.open();
}
void HeapTable::close(){
	file.close();
}
//insert row into db page -> block
Handle HeapTable::insert(const ValueDict* row){
	this->open();
	validate(row);
	return this->append(row);
	
}
//Updates data on a page
void HeapTable::update(const Handle handle, const ValueDict* new_values){
	validate(new_values);

	Dbt* data = marshal(new_values);
	BlockID block = handle.first;
	RecordID record = handle.second;

	SlottedPage* page= this->file.get(block);
	page->put(record, *data);
	this->file.put(page);

}
	   
//DELETE from a specified table with specific handle
void HeapTable::del(const Handle handle){
	BlockID block = handle.first;
	RecordID record = handle.second;
	SlottedPage* page = file.get(block);
	page->del(record);

}

//from klundeen
//Used for SELECTNG a handle with specific rows
Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

//I'm not sure if this will get every single column, is that okay?
ValueDict* HeapTable::project(Handle handle){
	BlockID block = handle.first;
	RecordID record = handle.second;
	SlottedPage* page = file.get(block);
	Dbt* data= page->get(record);
	ValueDict* value = this->unmarshal(data);
	return value;
}
	   
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names){
    BlockID block = handle.first;
    RecordID record = handle.second;
    SlottedPage* page = file.get(block);
    Dbt* data = page->get(record);
    ValueDict* value = unmarshal(data);
    ValueDict* result;
    if (column_names == NULL){
		return value;
    }
    else{
		result = new ValueDict();
		for (auto const &column : *column_names){
		    ValueDict::const_iterator field = value->find(column);
		    result->insert(pair<Identifier,Value>(field->first, field->second));
		}
    }
    delete page;
    delete data;
    return result;

}
// =======
// 	BlockID block = handle.first;
// 	RecordID record = handle.second;
// 	SlottedPage* block = file.get(block_id);
// 	Dbt* data= block->get(record);
// 	ValueDict* value = unmarshal(dbt);
// 	ValueDict* ret;
// 	for (auto const& column_name: *column_names) {
// 		*ret[column_name]= *value[column_name];
// 	}
// 	return ret;
// }
ValueDict* HeapTable::validate(const ValueDict* row){
    ValueDict* wholeRow = new ValueDict;
    for (auto const& column : this->column_names){
		ValueDict::const_iterator field = row->find(column);
	    if (field == row->end()){
			throw DbRelationError("Failed Validation");
	    }
	    else{
			wholeRow->insert(pair<Identifier, Value>(field->first, field->second));
	    }
	    
  	}
    return wholeRow;
}	   

Handle HeapTable::append(const ValueDict* row){
    Dbt* data = this->marshal(row);
    SlottedPage* block = this->file.get(this->file.get_last_block_id());
    RecordID record;
    try{
	record = block->add(data);
    }
    catch (DbBlockNoRoomError){
	block = this->file.get_new();
	record = block->add(data);
    }
    this->file.put(block);
    return Handle(this->file.get_last_block_id(), record);
}

//from klundeen
// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}


ValueDict* HeapTable::unmarshal(Dbt* data){
	ValueDict *row = new ValueDict();
	uint offset = 0;
    uint col_num = 0;
    char *bytes = (char*)data->get_data();
    Value value;

    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        value.data_type = ca.get_data_type();
        
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n= *(int32_t*) (bytes + offset); 
            offset += sizeof(int32_t);
            (*row)[column_name] = value;
        }
     	else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint totalBytes = *(u16*) (bytes + offset);
            offset += sizeof(u16);
        	char* stringBuffer = new char[totalBytes];
            memcpy(stringBuffer,bytes+offset, totalBytes); // copy source into destination buffer of size
            value.s = string(stringBuffer);
            offset += totalBytes;
            (*row)[column_name] = value;
    	} 
    	else {
	            throw DbRelationError("Only know how to marshal INT and TEXT");
    	}
    }

	return row;
}


