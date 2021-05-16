//
// Copyright [2021] <pan_fera>
//

#include "PersistentStorage.hpp"

#include <iomanip>
#include <iostream>
#include <string>
#include "PicoSHA2/picosha2.h"

using rocksdb::DB;
using rocksdb::Status;
using rocksdb::DBOptions;
using rocksdb::Iterator;
using rocksdb::ColumnFamilyHandle;

std::atomic<size_t> count = 0;

PersistentStorage::PersistentStorage(
    const std::string& path_to_db_from, const std::string& path_to_db_to,
    logging::trivial::severity_level& log_level, size_t& thread_count)
    : _log_level(log_level), _thread_count(thread_count) {
  logging::add_console_log(
      std::cout, keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
      keywords::auto_flush = true,
      keywords::filter = logging::trivial::severity == _log_level);
  logging::add_common_attributes();

  _db_from = nullptr;
  _db_to = nullptr;

  rocksdb::WriteOptions write_options;
  write_options.sync = true;
  rocksdb::Options options;
  options.create_if_missing = false;

  Status s_from, s_to;

  s_from = DB::ListColumnFamilies(DBOptions(), path_to_db_from, &_names);
  if (!s_from.ok()) {
    BOOST_LOG_TRIVIAL(error)
        << "DB::ListColumnFamilies input failed: " << s_from.ToString();
    throw std::runtime_error("DB::ListColumnFamilies input failed: " +
                             s_from.ToString());
  }

  for (auto& i : _names) {
    _descriptors.emplace_back(i, rocksdb::ColumnFamilyOptions());
  }

  // s_from = DB::OpenForReadOnly(options, path_to_db_from, &_db_from);
  s_from = DB::OpenForReadOnly(options, path_to_db_from, _descriptors,
                               &_handles_from, &_db_from);
  if (!s_from.ok()) {
    BOOST_LOG_TRIVIAL(error)
        << "DB::OpenForReadOnly input failed: " << s_from.ToString();
    throw std::runtime_error("DB::OpenForReadOnly input failed: " +
                             s_from.ToString());
  }

  options.create_if_missing = true;

  s_to = rocksdb::DB::Open(options, path_to_db_to, &_db_to);
  if (!s_to.ok()) {
    BOOST_LOG_TRIVIAL(error) << "DB::Open output failed: " << s_to.ToString();
    throw std::runtime_error("DB::Open output failed: " + s_to.ToString());
  }

  _names.erase(_names.begin());

  s_to = _db_to->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(), _names,
                                      &_handles_to);

  if (!s_to.ok()) {
    BOOST_LOG_TRIVIAL(error)
        << "DB::CreateColumnFamilies output failed: " << s_to.ToString();
    throw std::runtime_error("DB::CreateColumnFamilies output failed: " +
                             s_to.ToString());
  }

  BOOST_LOG_TRIVIAL(info) << "DB::OpenForReadOnly input and DB::Open output";
}

PersistentStorage::~PersistentStorage() {
  for (auto handle : _handles_from) {
    _db_from->DestroyColumnFamilyHandle(handle);
  }
  if (_db_from) {
    _db_from->Close();
    delete _db_from;
  }

  for (auto handle : _handles_to) {
    _db_to->DestroyColumnFamilyHandle(handle);
  }

  if (_db_to) {
    _db_to->Close();
    delete _db_to;
  }
}

inline void PersistentStorage::read_handle(
    Iterator* it, ColumnFamilyHandle* i) {
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string hash = picosha2::hash256_hex_string(it->key().ToString() +
                                                    it->value().ToString());
    Element tmp{it->key().ToString(), it->value().ToString(), i, hash};
    _queue_elements.push(std::move(tmp));
  }
  ++count;
  BOOST_LOG_TRIVIAL(info) << "Read :" << i->GetName();
  if (it) delete it;
}
void PersistentStorage::read_db(std::shared_ptr<ThreadPool> ptr) {
  for (const auto& i : _handles_from) {
    Iterator* it = _db_from->NewIterator(rocksdb::ReadOptions(), i);
    ptr->enqueue(read_handle, it, i);
  }

  BOOST_LOG_TRIVIAL(info) << "Read DB";
}
void PersistentStorage::write(Element tmp) {
  rocksdb::WriteOptions write_options;
  write_options.sync = true;
  Status status;
  status = _db_to->Put(write_options, tmp._family, tmp._key, tmp._hash);
  if (!status.ok())
    BOOST_LOG_TRIVIAL(warning) << "DB::Open failed " << status.ToString();
  BOOST_LOG_TRIVIAL(info) << "Write DB key:" << tmp._key;
  _queue_elements.pop();
}
void PersistentStorage::write_db(std::shared_ptr<ThreadPool> ptr) {
  while (count != _handles_from.size() || _queue_elements.counter != 0) {
    if (!_queue_elements.empty()) {
      for (size_t i = 0; i < _queue_elements.size(); ++i) {
        Element tmp = _queue_elements.front();
        ptr->enqueue(write, tmp);
      }
    }
  }
}

void PersistentStorage::print(std::ostream& out) {
  for (const auto iter : _handles_from) {
    out << "family " + iter->GetName() << std::endl;
    rocksdb::Iterator* it = _db_from->NewIterator(rocksdb::ReadOptions(), iter);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      out << it->key().ToString() << " : " << it->value().ToString()
          << std::endl;
    }
    if (!it->status().ok())
      BOOST_LOG_TRIVIAL(warning)
          << "DB::Open failed " << it->status().ToString();
    delete it;
    out << std::endl;
  }

  std::cout << "/////////////" << std::endl;
  for (const auto iter : _handles_to) {
    out << "family " + iter->GetName() << std::endl;
    rocksdb::Iterator* it = _db_to->NewIterator(rocksdb::ReadOptions(), iter);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      out << it->key().ToString() << " : " << it->value().ToString()
          << std::endl;
    }
    if (!it->status().ok())
      BOOST_LOG_TRIVIAL(warning)
          << "DB::Open failed " << it->status().ToString();
    delete it;
    out << std::endl;
  }
}

std::ostream& operator<<(std::ostream& out, PersistentStorage& storage) {
  std::shared_ptr<ThreadPool> ptr =
      std::make_shared<ThreadPool>(storage._thread_count);
  storage.read_db(ptr);
  storage.write_db(ptr);
  storage.print(out);
  return out;
}
