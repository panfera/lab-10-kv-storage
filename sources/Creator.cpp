//
// Copyright [2021] <pan_fera>
//

#include "Creator.hpp"
#define max_rows 30
#define max_columns 7
inline int random_int(const int& from, const int& to) {
  return from + std::rand() % to;
}

inline void Creator::set_values(const std::string& value) {
  _values.push_back(value);
}

inline int Creator::get_column_families_names_size() {
  return static_cast<int>(_column_families_names.size());
}

inline int Creator::get_values_size() {
  return static_cast<int>(_values.size());
}

inline void create_random_values(Creator* creator) {
  for (size_t i = 1;
       i <= static_cast<size_t>(random_int(
                creator->get_column_families_names_size(), max_rows));
       ++i) {
    creator->set_values(std::to_string(random_int(0, 30)));
  }
}

inline std::vector<int> count_num_in_each_column(Creator* creator) {
  int rand = 0;
  int sum =
      creator->get_values_size() - creator->get_column_families_names_size();
  std::vector<int> out;

  if (creator->get_column_families_names_size() > 1) {
    for (size_t i = 0;
         i < static_cast<size_t>(creator->get_column_families_names_size());
         ++i) {
      sum = sum - rand;
      if (sum > 0) {
        rand =
            (i !=
             static_cast<size_t>(creator->get_column_families_names_size()) - 1)
                ? random_int(1, sum)
                : sum;
        out.push_back(rand);
      } else {
        out.push_back(0);
      }
      ++out[i];
    }
  } else {
    out.push_back(creator->get_values_size());
  }
  return out;
}

void str_of_separator(std::stringstream& ss, const size_t& num_c) {
  for (size_t i = 0; i < num_c; ++i) {
    if (i != num_c - 1) {
      ss << std ::right << std::setfill('-') << "|" << std::setw(18);
    } else {
      ss << std ::right << std::setfill('-') << "|" << std::setw(20);
    }
  }
  ss << "|\n " << std::setfill(' ');
}

void Creator::create_new_random_db(const std::string& path_to_new_db) {
  std::srand(static_cast<unsigned int>(time(nullptr)));
  _db_ptr = nullptr;
  rocksdb::WriteOptions write_options;
  write_options.sync = true;
  rocksdb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;

  rocksdb::Status status = rocksdb::DB::Open(options, path_to_new_db, &_db_ptr);
  if (!status.ok())
    throw std::runtime_error("BD::Open fail " + status.ToString());

  for (int i = 0; i < random_int(2, max_columns); ++i) {
    _column_families_names.push_back("column_family_" + std::to_string(i + 1));
  }

  status = _db_ptr->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(),
                                         _column_families_names, &_handles);
  if (!status.ok())
    throw std::runtime_error("BD::Create_Columns fail " + status.ToString());

  create_random_values(this);

  std::vector<int> num_in_each_column = count_num_in_each_column(this);

  std::cout << _values.size() << std::endl;
  int count = 0;
  for (size_t i = 0; i < _column_families_names.size(); ++i) {
    for (size_t j = 0; j < static_cast<size_t>(num_in_each_column[i]);
         ++j, ++count) {
      status = _db_ptr->Put(write_options, _handles[i],
                            "key" + std::to_string(count + 1), _values[count]);
      if (!status.ok()) throw std::runtime_error("BD::Put fail");
    }
  }

  std::stringstream ss;
  for (auto& column_family_name : _column_families_names) {
    ss << std ::left << " | " << std::setw(15) << column_family_name;
  }
  ss << " |\n ";

  str_of_separator(ss, _column_families_names.size());

  int max_len_col = 0;
  for (int& i : num_in_each_column)
    max_len_col = (i > max_len_col) ? i : max_len_col;

  int count_in_line;
  std::string temp;
  for (int i = 0; i < max_len_col; ++i) {
    count_in_line = i;
    for (size_t j = 0; j < _column_families_names.size(); ++j) {
      if (num_in_each_column[j] > i) {
        status = _db_ptr->Get(rocksdb::ReadOptions(), _handles[j],
                              "key" + std::to_string(count_in_line + 1), &temp);
        if (!status.ok())
          throw std::runtime_error(
              "BD::Get(" + std::to_string(count_in_line + 1) + ") fail");
        ss << std ::left << "| " << std::setw(16)
           << "key" + std::to_string(count_in_line + 1) + " : " + temp;
      } else {
        ss << std ::left << "| ";
        for (int k = 0; k < 16; ++k) {
          ss << " ";
        }
      }
      count_in_line = count_in_line + num_in_each_column[j];
    }
    ss << "|\n ";
    str_of_separator(ss, _column_families_names.size());
  }
  std::cout << ss.str() << std::endl;
  std::cout << "values: " << _values.size() << std::endl;
  std::cout << "columns: " << _column_families_names.size() << std::endl;
}

Creator::~Creator() {
  rocksdb::Status status;
  for (auto handle : _handles) {
    _db_ptr->DestroyColumnFamilyHandle(handle);
  }
  if (_db_ptr) {
    _db_ptr->Close();
    delete _db_ptr;
  }
}
