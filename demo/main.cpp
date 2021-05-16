//
// Copyright [2021] <pan_fera>
//

#include <ThreadPool.h>
#include <boost/program_options.hpp>
#include <iostream>
#include "PersistentStorage.hpp"

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
  po::options_description desc("Options");
  desc.add_options()("log-level", po::value<std::string>(),
                     "= \"info\"|\"warning\"|\"error\"\n= default: \"error\"")(
      "thread_count", po::value<size_t>(),
      "= \n= default: count of logical core")(
      "output", po::value<std::string>(),
      "= <path/to/output/storage.db>\n= default: "
      "<path/to/input/_storage.db>")("help", "= produce help message");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);

  if (vm.count("help") || argc == 1) {
    std::cout << "Usage: ./demo [options] <path/to/input/storage.db>\n";
    std::cout << desc << std::endl;
    return 0;
  }

  logging::trivial::severity_level log_level;
  size_t thread_count;
  std::string output{};
  std::string input{};

  thread_count = (vm.count("thread_count"))
                     ? vm["thread_count"].as<size_t>()
                     : std::thread::hardware_concurrency();

  log_level = logging::trivial::error;
  if (vm.count("log-level")) {
    if (vm["log-level"].as<std::string>() == "info")
      log_level = logging::trivial::info;
    else if (vm["log-level"].as<std::string>() == "warning")
      log_level = logging::trivial::warning;
  }

  output =
      (vm.count("output")) ? vm["output"].as<std::string>() : "_storage.db";
  input = argv[argc - 1];

  PersistentStorage a(input, output, log_level, thread_count);

  std::cout << a;

  return 0;
}

//#include "Creator.hpp"
// Creator a;
// a.create_new_random_db("test.db");