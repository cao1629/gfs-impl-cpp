#include "master/worker_pool.h"

namespace gfs {

WorkerPool::WorkerPool(size_t threads) {
  for (size_t i = 0; i < threads; ++i) threads_.emplace_back([this] { run(); });
}

WorkerPool::~WorkerPool() { stop(); }

void WorkerPool::post(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return;
    tasks_.push_back(std::move(task));
  }
  cv_.notify_one();
}

void WorkerPool::stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return;
    stopping_ = true;
  }
  cv_.notify_all();
  for (auto& t : threads_) {
    if (t.joinable()) t.join();
  }
}

void WorkerPool::run() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
      if (stopping_ && tasks_.empty()) return;
      task = std::move(tasks_.front());
      tasks_.pop_front();
    }
    task();
  }
}

}
