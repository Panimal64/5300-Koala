#include "btree.h"
using namespace std;

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
        : DbIndex(relation, name, key_columns, unique),
          closed(true),
          stat(nullptr),
          root(nullptr),
          file(relation.get_table_name() + "-" + name),
          key_profile() {
	//cout << "BTreeIndex::BTreeIndex" << endl;
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
	// FIXME - what else?!
	build_key_profile();
}

BTreeIndex::~BTreeIndex() {
	// FIXME - free up stuff
	//cout << "BTreeIndex::~BTreeIndex" << endl;
	delete(this->stat);
	delete(this->root);
	drop();
}

// Create the index.
void BTreeIndex::create() {
	// FIXME
	//cout << "BTreeIndex::create" << endl;
	// Create the index
	this->file.create();
	//cout << "BTreeIndex, new BTreeStat" << endl;
	this->stat = new BTreeStat(this->file, this->STAT, this->STAT + 1, this->key_profile);
	//cout << "BTreeIndex, new BTreeLeaf" << endl;
	this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
	//cout << "BTreeIndex, this->closed = false" << endl;
	this->closed = false;
	//cout << "BTreeIndex, this->relation.select" << endl;
	// now build the index! -- add every row from relation into index
	Handles* handles = this->relation.select();
	//cout << "BTreeIndex, scan handles" << endl;
	for (auto const& handle : *handles) {
		//cout << "BTreeIndex, this->insert" << endl;
		this->insert(handle);
	}
}

// Drop the index.
void BTreeIndex::drop() {
	// FIXME
	//cout << "BTreeIndex::drop" << endl;
	this->file.drop();
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
	// FIXME
	//cout << "BTreeIndex::open" << endl;
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
	//cout << "BTreeIndex::close" << endl;
	this->file.close();
	this->stat = nullptr;
	this->root = nullptr;
	this->closed = true;
}

// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
	// FIXME
	//cout << "BTreeIndex::lookup" << endl;
	KeyValue* _tKey = this->tkey(key_dict);
	//cout << "BTreeIndex, this->tkey" << endl;
	return this->_lookup(this->root, this->stat->get_height(), _tKey);
}

Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue* key) const {
	// FIXME
	//cout << "BTreeIndex::_lookup" << endl;
	Handles* handles = new Handles();
	Handle handle;

	if (height == 1) {
		//cout << "BTreeIndex, (BTreeLeaf)find_eq" << endl;
		//try {
			handle = ((BTreeLeaf*)node)->find_eq(key);
			//cout << "BTreeIndex, After (BTreeLeaf)find_eq" << endl;
			if (handle.first)
				handles->push_back(handle);
			//cout << "BTreeIndex, After push_back" << endl;
		//} catch (DbRelationError& e) {
		//	throw DbRelationError("DbRelationError: " + string(e.what()));
		//}

		return handles;
	}
	else {
		//cout << "BTreeIndex, (BTreeInterior)find" << endl;
		return _lookup(((BTreeInterior*)node)->find(key, this->stat->get_height()), 
													this->stat->get_height() - 1, key);
	}
}

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
    // FIXME
}

void BTreeIndex::insert(Handle handle) {
  // FIXME
  //cout << "BTreeIndex::insert" << endl;
  cout << handle.first << " " << handle.second << " ";
  KeyValue* _tKey = tkey(relation.project(handle, &key_columns));
  cout << "Key Found "; 
  Insertion split_root = _insert(root, stat->get_height(), _tKey, handle);
  cout << "Insertion Complete" << endl;
  // cout << handle.first << " " << handle.second << endl;
  cout << "BTreeIndex, split_root.first = " << to_string(split_root.first) << " " << BTreeNode::insertion_is_none(split_root) << endl;
  if(!BTreeNode::insertion_is_none(split_root)) {
    cout << "BTreeIndex, split_root.first NOT 0" << endl;
    BTreeInterior* newRoot = new BTreeInterior(file, stat->get_root_id() + 1, key_profile, true);
    newRoot->set_first(stat->get_root_id());
    cout << stat->get_root_id() << endl;
    newRoot->insert(&split_root.second, split_root.first);
    newRoot->save();
    stat->set_root_id(newRoot->get_id());
    stat->set_height(stat->get_height() + 1);
    stat->save();
    root = newRoot;
  }
}

Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle) {
  // FIXME
  cout << "BTreeIndex, _insert, height = " << to_string(height) << endl;
  cout << "BTreeIndex, _insert, key Value = " << to_string(key->at(0).n) << endl;
  Insertion insertion;
  if (height == 1) {
    insertion = ((BTreeLeaf*)node)->insert(key, handle);
    cout << "BTreeIndex, _insert, BTreeLeaf->insert, BlockID = " << to_string(insertion.first) << endl;
    //((BTreeLeaf*)node)->save();
  }
  else {
    Insertion new_kid = _insert(((BTreeInterior*)node)->find(key, height), height - 1, key, handle);
    if (!BTreeNode::insertion_is_none(new_kid)) {
      insertion = ((BTreeInterior*)node)->insert(&new_kid.second, new_kid.first);
      cout << "BTreeIndex, _insert, BTreeInterior->insert, BlockID = " << to_string(insertion.first) << endl;
      //((BTreeInterior*)node)->save();
    }
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
	//cout << "BTreeIndex::tkey" << endl;
	KeyValue *keyValue = new KeyValue();
	for (auto const& column_name : key_columns) {
		Value value;
		ValueDict::const_iterator column = key->find(column_name);
		if (column != key->end()) {
			value = column->second;
			//cout << "tkey = " << value.n << endl;
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

  ValueDict row1, row2, row;
  row1["a"] = Value(12);
  row1["b"] = Value(99);
  row2["a"] = Value(88);
  row2["b"] = Value(101);
  testTable.insert(&row1);
  testTable.insert(&row2);

  for (uint i = 1; i < 1000; i++) {
    row["a"] = Value(i + 100);
    row["b"] = Value((-1)*i);
    testTable.insert(&row);
  }

  ColumnNames test_column_names;
  test_column_names.push_back("a");
  DbIndex* index = new BTreeIndex(testTable, "fooindex", test_column_names, true);
  index->create();

  //cout << "HeapTable create index - simple" << endl;
  ValueDict test_row1, test_row2, test_row3, test_row4;

  cout << "HeapTable compare 1 - simple" << endl;
  test_row1["a"] = Value(12);
  for (auto const& handle : *index->lookup(&test_row1)) {
    ValueDict* result_row = testTable.project(handle);
    if ((*result_row)["a"] == test_row1["a"]) {
      result = true;
      cout << "First test passes - Found!!!" << endl;
      break;
    }
  }

  cout << "HeapTable compare 2 - simple" << endl;
  result = false;
  test_row2["a"] = Value(88);
  Handles* handles2 = index->lookup(&test_row2);
  for (auto const& handle : *handles2) {
    ValueDict* result_row = testTable.project(handle);
    if ((*result_row)["a"] == test_row2["a"]) {
      result = true;
      cout << "Second test passes - Found!!!" << endl;
      break;
    }
  }

  cout << "HeapTable compare 3 - simple" << endl;
  test_row3["a"] = Value(6);
  Handles* handles3 = index->lookup(&test_row3);
  if (handles3->empty())
    cout << "HeapTable compare 3 - Not Found!!!" << endl;

  else {
    for (auto const& handle : *handles3) {
      //cout << "HeapTable compare 3 - before project" << endl;
      ValueDict* result_row = testTable.project(handle);
      //cout << "HeapTable compare 3 - after project" << endl;
      if ((*result_row)["a"] == test_row3["a"]) {
        cout << "HeapTable compare 3 - Found!!!" << endl;
        result = false;
        break;
      }
    }
  }
  cout << "Thrid test passes" << endl;

  return result;
}
