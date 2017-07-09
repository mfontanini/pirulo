#pragma once

#include <functional>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <thread>
#include <atomic>
#include <deque>

namespace pirulo {

class TaskScheduler {
public:
    using Task = std::function<void()>;
    using TaskId = size_t;
    using Duration = std::chrono::milliseconds;

    TaskScheduler();
    TaskScheduler(const TaskScheduler&) = delete;
    TaskScheduler& operator=(const TaskScheduler&) = delete;
    ~TaskScheduler();

    TaskId add_task(Task task, Duration maximum_offset);
    void remove_task(TaskId id);
    void set_priority(TaskId id, double priority);
    void set_minimum_reschedule_time(Duration value);

private:
    using ClockType = std::chrono::steady_clock;
    struct TaskExecutionInstance {
        TaskId task_id;
        ClockType::time_point scheduled_at;
        ClockType::time_point scheduled_for;
    };
    using TaskQueue = std::deque<TaskExecutionInstance>;
    struct TaskMetadata {
        Task task;
        TaskQueue::iterator queue_iterator;
        Duration maximum_offset;
        double priority;
    };
    using TaskMap = std::unordered_map<TaskId, TaskMetadata>;

    static Duration get_schedule_delta(const TaskMetadata& meta);
    void stop();
    TaskQueue::iterator schedule_task(TaskId task_id, const TaskMetadata& meta);
    void remove_scheduled_instance(TaskId task_id);
    void process_tasks();

    TaskMap tasks_;
    TaskId current_task_id_{0};
    std::thread process_thread_;
    Duration minimum_reschedule_ = std::chrono::seconds(10);
    TaskQueue tasks_queue_;
    mutable std::mutex tasks_mutex_;
    std::condition_variable tasks_condition_;
    std::atomic<bool> running_{true};
};

} // pirulo
