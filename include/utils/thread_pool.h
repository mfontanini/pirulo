#pragma once

#include <vector>
#include <thread>
#include <functional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace pirulo {

class ThreadPool {
public:
    using Task = std::function<void()>;

    ThreadPool(size_t thread_count);
    ThreadPool(size_t thread_count, size_t maximum_tasks);
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ~ThreadPool();

    // Returns true iff the task was successfully added. Adding a task will only fail
    // iff a maximum task limit has been set and the limit has been reached
    bool add_task(Task task);
    void stop();
    void wait_for_tasks();
private:
    void process();

    std::vector<std::thread> threads_;
    std::queue<Task> tasks_;
    std::mutex tasks_mutex_;
    std::condition_variable tasks_condition_;
    std::condition_variable no_tasks_condition_;
    const size_t maximum_tasks_;
    std::atomic<bool> running_{true};
};

} // pirulo
