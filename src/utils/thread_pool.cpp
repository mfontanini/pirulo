#include "utils/thread_pool.h"

using std::mutex;
using std::lock_guard;
using std::unique_lock;
using std::move;

namespace pirulo {

ThreadPool::ThreadPool(size_t thread_count)
: ThreadPool(thread_count, 0) {

}

ThreadPool::ThreadPool(size_t thread_count, size_t maximum_tasks)
: maximum_tasks_(maximum_tasks) {
    for (size_t i = 0; i < thread_count; ++i) {
        threads_.emplace_back(&ThreadPool::process, this);
    }
}

ThreadPool::~ThreadPool() {
    stop();
}

bool ThreadPool::add_task(Task task) {
    lock_guard<mutex> _(tasks_mutex_);
    if (maximum_tasks_ > 0 && tasks_.size() >= maximum_tasks_) {
        return false;
    }
    tasks_.push(move(task));
    tasks_condition_.notify_one();
    return true;
}

void ThreadPool::stop() {
    {
        // Wake all threads
        running_ = false;
        lock_guard<mutex> _(tasks_mutex_);
        tasks_condition_.notify_all();
    }
    for (auto& thread : threads_) {
        thread.join();
    }
    threads_.clear();
}

void ThreadPool::wait_for_tasks() {
    while (running_) {
        unique_lock<mutex> lock(tasks_mutex_);
        if (tasks_.empty()) {
            return;
        }
        no_tasks_condition_.wait(lock);
    }
}

void ThreadPool::process() {
    while (running_) {
        unique_lock<mutex> lock(tasks_mutex_);

        // Either spurious wake up or someone called ThreadPool::stop
        while (running_ && tasks_.empty()) {
            tasks_condition_.wait(lock);
        }
        if (!running_) {
            continue;
        }

        Task task = move(tasks_.front());
        tasks_.pop();

        if (tasks_.empty()) {
            no_tasks_condition_.notify_all();
        }

        // Release lock and execute task
        lock.unlock();
        task();
    }
}

} // pirulo
