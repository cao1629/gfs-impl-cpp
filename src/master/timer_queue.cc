#include "master/timer_queue.h"

namespace gfs {

TimerQueue::TimerQueue(WorkerPool& pool) : pool_(pool), thread_([this] { run(); }) {}

TimerQueue::~TimerQueue() { stop(); }

void TimerQueue::at(TimePoint when, std::function<void()> fn) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    timers_.emplace(when, std::move(fn));
  }
  cv_.notify_one();
}

void TimerQueue::after(Millis delay, std::function<void()> fn) {
  at(now() + delay, std::move(fn));
}

void TimerQueue::stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return;
    stopping_ = true;
  }
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
}

void TimerQueue::run() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!stopping_) {
    if (timers_.empty()) {
      cv_.wait(lock);
      continue;
    }
    auto first = timers_.begin();
    if (first->first > now()) {
      cv_.wait_until(lock, first->first);
      continue;
    }
    auto fn = std::move(first->second);
    timers_.erase(first);
    lock.unlock();
    pool_.post(std::move(fn));
    lock.lock();
  }
}

PeriodicTask::PeriodicTask(Millis interval, std::function<void()> fn)
    : thread_([this, interval, fn = std::move(fn)] {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!stopping_) {
          if (cv_.wait_for(lock, interval, [this] { return stopping_; })) return;
          lock.unlock();
          fn();
          lock.lock();
        }
      }) {}

PeriodicTask::~PeriodicTask() { stop(); }

void PeriodicTask::stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return;
    stopping_ = true;
  }
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
}

}
