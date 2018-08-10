#include "btree.h"
using namespace std;

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
        : DbIndex(relation, name, key_columns, unique),
          closed(true),
          stat(nullptr),
          root(nullptr),
          file(relation.get_table_name() + "-" + name),
          key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
	// FIXME - what else?!
	build_key_profile();
}

BTreeIndex::~BTreeIndex() {
	// FIXME - free up stuff
	delete(this->stat);
	delete(this->root);
	drop();
}

// Create the index.
void BTreeIndex::create() {
	// FIXME
	this->file.create();
	this->stat = new BTreeStat(this->file, this->STAT, this->STAT + 1, this->key_profile);
	this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
	this->closed = false;
	// now build the index! -- add every row from relation into index
	Handles* handles = this->relation.select();
	for (auto const& handle : *handles) {
		this->insert(handle);
	}
}

// Drop the index.
void BTreeIndex::drop() {
	// FIXME
	this->file.drop();
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
	// FIXME
	if (this->closed) {
		this->file.open();
		this->stat = new BTreeStat(this->file, this->STAT, this->key_profile);

		if (this->stat->get_height() == 1)
			this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, false);
		else
			this->root = new BTreeInterior(this->file, this->stat->get_root_id(), this->key_profile, false);

		this->closed = false;
	}
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {
	// FIXME
	this->file.close();
	this->stat = nullptr;
	this->root = nullptr;
	this->closed = true;
}

// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
	// FIXME
	KeyValue* _tKey = this->tkey(key_dict);
	return this->_lookup(this->root, this->stat->get_height(), _tKey);
}

Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue* key) const {
	// FIXME
	Handles* handles = new Handles();
	Handle handle;
	if (height == 1) {
		handle = ((BTreeLeaf*)node)->find_eq(key);
		if (handle.first)
			handles->push_back(handle);
		return handles;
	}
	else {
		return _lookup(((BTreeInterior*)node)->find(key, this->stat->get_height()), 
													this->stat->get_height() - 1, key);
	}
}

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
    // FIXME
}

// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle) {
	// FIXME
	KeyValue* _tKey = this->tkey(this->relation.project(handle, &key_columns));
	Insertion split_root = _insert(this->root, this->stat->get_height(), _tKey, handle);

	if (!BTreeNode::insertion_is_none(split_root)) {
		BTreeInterior* root = new BTreeInterior(this->file, 0, this->key_profile, true);
		root->set_first(this->root->get_id());
		root->insert(&split_root.second, split_root.first);
		root->save();
		this->stat->set_root_id(root->get_id());
		this->stat->set_height(this->stat->get_height() + 1);
		this->stat->save();
		this->root = root;
	}
}

Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle) {
	// FIXME
	Insertion insertion;
	if (height == 1)
		insertion = ((BTreeLeaf*)node)->insert(key, handle);
	else {
		Insertion new_kid = _insert(((BTreeInterior*)node)->find(key, height), height - 1, key, handle);
		if (!BTreeNode::insertion_is_none(new_kid))
			insertion = ((BTreeInterior*)node)->insert(&new_kid.second, new_kid.first);
	}
	return insertion;
}

void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME
}

// Transform a key dictionary into a tuple in the correct order.
KeyValue *BTreeIndex::tkey(const ValueDict *key) const {
	// FIXME
	KeyValue *keyValue = new KeyValue();
	for (auto const& column_name : key_columns) {
		Value value;
		ValueDict::const_iterator column = key->find(column_name);
		if (column != key->end()) {
			value = column->second;
			keyValue->push_back(value);
		}
		else
			throw DbRelationError("can not find the specific key");
	}
	return keyValue;
}

// build key profile
// figure out the data types of each key component and encode them in self.key_profile, 
// a list of int/str classes.
void BTreeIndex::build_key_profile() {
	// FIXME
	ColumnNames column_names = this->relation.get_column_names();
	ColumnAttributes column_attributes = this->relation.get_column_attributes();
	for (auto const& key_column_name : key_columns) {
		for (uint i = 0; i < column_names.size(); i++) {
			if (key_column_name == column_names[i]) {
				this->key_profile.push_back(column_attributes[i].get_data_type());
			}
		}
	}
}

bool test_btree() {
	// FIXME
	cout << "run test_btree" << endl;
	bool result = false;
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::INT);
	column_attributes.push_back(ca);

	HeapTable testTable("foo", column_names, column_attributes);
	testTable.create();

	ValueDict row1, row2;
	row1["a"] = Value(12);
	row1["b"] = Value(99);
	row2["a"] = Value(88);
	row2["b"] = Value(101);
	testTable.insert(&row1);
	testTable.insert(&row2);

	ColumnNames test_column_names;
	test_column_names.push_back("a");
	ValueDict row;
	for (uint i = 1; i < 1000; i++) {
		row["a"] = Value(i + 100);
		row["b"] = Value((-1)*i);
		testTable.insert(&row);
	}

	DbIndex* index = new BTreeIndex(testTable, "fooindex", test_column_names, true);
	index->create();

	ValueDict test_row1, test_row2, test_row3, test_row4;
	result = false;
	test_row1["a"] = Value(12);
	Handles* handles1 = index->lookup(&test_row1);
	if (handles1->empty()) {
		result = false;
	}
	else {
		for (auto const& handle : *handles1) {
			ValueDict* result_row = testTable.project(handle);
			if ((*result_row)["a"] == test_row1["a"]) {
				result = true;
				cout << "First test passes - Found!!!" << endl;
				break;
			}
			delete result_row;
		}
	}
	delete handles1;

	result = false;
	test_row2["a"] = Value(88);
	Handles* handles2 = index->lookup(&test_row2);
	if (handles2->empty()) {
		result = false;
	}
	else {
		for (auto const& handle : *handles2) {
			ValueDict* result_row = testTable.project(handle);
			if ((*result_row)["a"] == test_row2["a"]) {
				result = true;
				cout << "Second test passes - Found!!!" << endl;
				break;
			}
			delete result_row;
		}
	}
	delete handles2;

	result = false;
	test_row3["a"] = Value(6);
	Handles* handles3 = index->lookup(&test_row3);
	if (handles3->empty()) {
		cout << "Thrid test passes - Not Found!!!" << endl;
		result = true;
	}
	else {
		for (auto const& handle : *handles3) {
			ValueDict* result_row = testTable.project(handle);
			if ((*result_row)["a"] == test_row3["a"]) {
				result = false;
				break;
			}
			delete result_row;
		}
	}
	delete handles3;

	result = false;
	for (uint j = 1; j < 1000; j++) {
		test_row4["a"] = Value(j + 100);
		test_row4["b"] = Value((-1)*j);
		Handles* handles4 = index->lookup(&test_row4);
		if (handles4->empty()) {
			result = false;
		}
		else {
			for (auto const& handle : *handles4) {
				ValueDict* result_row = testTable.project(handle);
				if ((*result_row)["a"] == test_row4["a"]) {
					result = true;
					break;
				}
				delete result_row;
			}
		}
		delete handles4;
	}
	cout << "Fourth test passes - Found!!! (1000 rows)" << endl;
	delete index;

	return result;
}
