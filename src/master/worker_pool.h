#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace gfs {

class WorkerPool {
 public:
  explicit WorkerPool(size_t threads);
  ~WorkerPool();

  void Post(std::function<void()> task);
  void Stop();

 private:
  void Run();

  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> tasks_;
  std::vector<std::thread> threads_;
  bool stopping_ = false;
};

}  // namespace gfs
