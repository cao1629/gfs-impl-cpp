#include "master/timer_queue.h"

namespace gfs {

TimerQueue::TimerQueue(WorkerPool& pool)
    : pool_(pool), thread_([this] { Run(); }) {}

TimerQueue::~TimerQueue() { Stop(); }

void TimerQueue::At(TimePoint when, std::function<void()> fn) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    timers_.emplace(when, std::move(fn));
  }
  cv_.notify_one();
}

void TimerQueue::After(Millis delay, std::function<void()> fn) {
  At(Now() + delay, std::move(fn));
}

void TimerQueue::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return;
    stopping_ = true;
  }
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
}

void TimerQueue::Run() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stopping_) {
    if (timers_.empty()) {
      cv_.wait(lock);
      continue;
    }
    auto first = timers_.begin();
    if (first->first > Now()) {
      cv_.wait_until(lock, first->first);
      continue;
    }
    auto fn = std::move(first->second);
    timers_.erase(first);
    lock.unlock();
    pool_.Post(std::move(fn));
    lock.lock();
  }
}

PeriodicTask::PeriodicTask(Millis interval, std::function<void()> fn)
    : thread_([this, interval, fn = std::move(fn)] {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!stopping_) {
          if (cv_.wait_for(lock, interval, [this] { return stopping_; }))
            return;
          lock.unlock();
          fn();
          lock.lock();
        }
      }) {}

PeriodicTask::~PeriodicTask() { Stop(); }

void PeriodicTask::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return;
    stopping_ = true;
  }
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
}

}  // namespace gfs
