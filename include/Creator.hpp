//
// Copyright [2021] <pan_fera>
//

#ifndef INCLUDE_CREATOR_HPP_
#define INCLUDE_CREATOR_HPP_

#include <string>
#include <iomanip>
#include <iostream>
#include <rocksdb/db.h>
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include <vector>

using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyOptions;
using rocksdb::ColumnFamilyHandle;

class Creator {
 public:
  ~Creator();

  void create_new_random_db(const std::string& path_to_new_db);

  inline int get_column_families_names_size();

  inline int get_values_size();

  inline void set_values(const std::string& value);
 private:
  rocksdb::DB* _db_ptr;
  std::vector<std::string> _column_families_names;
  std::vector<ColumnFamilyHandle*> _handles;
  std::vector<std::string> _values;
};

#endif  // INCLUDE_CREATOR_HPP_
