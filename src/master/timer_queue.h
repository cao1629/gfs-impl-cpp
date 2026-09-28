#pragma once

#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <thread>

#include "common/clock.h"
#include "common/config.h"
#include "master/worker_pool.h"

namespace gfs {

class TimerQueue {
 public:
  explicit TimerQueue(WorkerPool& pool);
  ~TimerQueue();

  void At(TimePoint when, std::function<void()> fn);
  void After(Millis delay, std::function<void()> fn);
  void Stop();

 private:
  void Run();

  WorkerPool& pool_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::multimap<TimePoint, std::function<void()>> timers_;
  bool stopping_ = false;
  std::thread thread_;
};

class PeriodicTask {
 public:
  PeriodicTask(Millis interval, std::function<void()> fn);
  ~PeriodicTask();

  void Stop();

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool stopping_ = false;
  std::thread thread_;
};

}  // namespace gfs
