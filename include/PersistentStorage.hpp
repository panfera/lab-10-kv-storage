//
// Copyright [2021] <pan_fera>
//

#ifndef INCLUDE_PERSISTENTSTORAGE_HPP_
#define INCLUDE_PERSISTENTSTORAGE_HPP_

#include <ThreadPool.h>
#include <rocksdb/db.h>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/common.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/expressions/keyword.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <vector>
#include <string>
#include <memory>
#include "Queue.hpp"

namespace logging = boost::log;
namespace keywords = boost::log::keywords;

struct Element {
  std::string _key;
  std::string _value;
  rocksdb::ColumnFamilyHandle* _family;
  std::string _hash;
};
class PersistentStorage {
 public:
  PersistentStorage(const std::string& path_to_db_from,
                    const std::string& path_to_db_to,
                    logging::trivial::severity_level& log_level,
                    size_t& thread_count);
  PersistentStorage(PersistentStorage const&) = delete;
  void operator=(PersistentStorage const&) = delete;

  friend std::ostream& operator<<(std::ostream& out,
                                  PersistentStorage& storage);

  void read_db(std::shared_ptr<ThreadPool> ptr);
  void write_db(std::shared_ptr<ThreadPool> ptr);
  void print(std::ostream& out);
  inline static void read_handle(rocksdb::Iterator* it,
                                 rocksdb::ColumnFamilyHandle* i);
  inline static void write(Element tmp);
  ~PersistentStorage();

  logging::trivial::severity_level _log_level;
  size_t _thread_count;

 private:
  rocksdb::DB* _db_from;
  inline static rocksdb::DB* _db_to;

  inline static Queue<Element> _queue_elements;
  std::vector<std::string> _names;
  std::vector<rocksdb::ColumnFamilyDescriptor> _descriptors;
  std::vector<rocksdb::ColumnFamilyHandle*> _handles_from;
  std::vector<rocksdb::ColumnFamilyHandle*> _handles_to;
};

#endif  // INCLUDE_PERSISTENTSTORAGE_HPP_
