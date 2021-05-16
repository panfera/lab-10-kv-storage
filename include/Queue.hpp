//
// Copyright [2021] <pan_fera>
//

#ifndef INCLUDE_QUEUE_HPP_
#define INCLUDE_QUEUE_HPP_

#include <iostream>
#include <mutex>
#include <queue>
#include <string>

template <typename T>
class Queue {
 public:
  Queue() { counter = 0; }
  void push(T&& obj) {
    std::lock_guard<std::mutex> lock(_mut);
    _queue.push(obj);
    ++counter;
  }

  T front() {
    std::lock_guard<std::mutex> lock(_mut);
    T _tmp = _queue.front();
    _queue.pop();
    return _tmp;
  }

  void pop() {
    std::lock_guard<std::mutex> lock(_mut);
    --counter;
  }

  bool empty() {
    std::lock_guard<std::mutex> lock(_mut);
    return _queue.empty();
  }

  size_t size() {
    std::lock_guard<std::mutex> lock(_mut);
    return _queue.size();
  }
  size_t counter;
  std::queue<T> _queue;

 private:
  std::mutex _mut;
};

#endif  // INCLUDE_QUEUE_HPP_
