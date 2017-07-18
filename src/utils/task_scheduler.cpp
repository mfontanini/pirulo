#include <algorithm>
#include "utils/task_scheduler.h"
#include "exceptions.h"
#include "detail/logging.h"

using std::lock_guard;
using std::unique_lock;
using std::mutex;
using std::thread;
using std::move;
using std::find_if;
using std::upper_bound;
using std::min;
using std::max;

using std::chrono::seconds;

namespace pirulo {

PIRULO_CREATE_LOGGER("p.scheduler");

static const double MINIMUM_PRIORITY = 0.1;

TaskScheduler::TaskScheduler() {
    process_thread_ = thread([&] {
        process_tasks();
    });
}

TaskScheduler::~TaskScheduler() {
    stop();
}

TaskScheduler::TaskId TaskScheduler::add_task(Task task, Duration maximum_offset) {
    lock_guard<mutex> _(tasks_mutex_);
    TaskId task_id = current_task_id_++;

    // Construct the new task and insert it
    TaskMetadata task_meta{ move(task), tasks_queue_.end(), ClockType::now(),
                            maximum_offset, 1.0 };
    auto iter = tasks_.emplace(task_id, move(task_meta)).first;

    // Schedule and update the stored iterator
    iter->second.queue_iterator = schedule_task(task_id, iter->second);
    return task_id;
}

void TaskScheduler::remove_task(TaskId id) {
    lock_guard<mutex> _(tasks_mutex_);
    auto iter = tasks_.find(id);
    if (iter == tasks_.end()) {
        return;
    }
    remove_scheduled_instance(id);
    tasks_.erase(iter);
}

void TaskScheduler::set_priority(TaskId id, double priority) {
    // Don't let this go too low
    priority = max(priority, MINIMUM_PRIORITY);

    lock_guard<mutex> _(tasks_mutex_);
    auto iter = tasks_.find(id);
    if (iter == tasks_.end()) {
        throw Exception("Task not found");
    }
    const auto now = ClockType::now();
    TaskMetadata& meta = iter->second; 
    const double priority_diff = priority - meta.priority;

    // Update the priority and the pariority set time to now
    meta.priority = priority;
    meta.last_priority_set_time = now;
    
    // Re-schedule this task if either:
    // * The prioity value is higher (meaning the priority is lower)
    // * The priority is lower but the task is already scheduled for more than our minimum
    // re-schedule time ahead
    const TaskExecutionInstance& current_instance = *meta.queue_iterator;
    if (priority_diff < 0 || now + minimum_reschedule_ < current_instance.scheduled_for) {
        remove_scheduled_instance(id);
        meta.queue_iterator = schedule_task(id, meta);
    }
}

void TaskScheduler::set_minimum_reschedule_time(Duration value) {
    minimum_reschedule_ = value;
}

void TaskScheduler::stop() {
    {
        lock_guard<mutex> _(tasks_mutex_);
        running_ = false;
        tasks_condition_.notify_all();
    }

    process_thread_.join();
}

TaskScheduler::Duration TaskScheduler::get_schedule_delta(const TaskMetadata& meta) {
    const size_t modifier = meta.maximum_offset.count() * meta.priority;;
    return Duration(modifier);
}

TaskScheduler::TaskQueue::iterator
TaskScheduler::schedule_task(TaskId task_id, const TaskMetadata& meta) {
    // Get the execution offset
    const auto now = ClockType::now();
    const auto schedule_for = now + get_schedule_delta(meta);
    TaskExecutionInstance instance{task_id, now, schedule_for};

    // Find the task that's schedule after this one
    const auto comparer = [](const TaskExecutionInstance& lhs, const TaskExecutionInstance& rhs) {
        return lhs.scheduled_for < rhs.scheduled_for;
    };
    auto position = upper_bound(tasks_queue_.begin(), tasks_queue_.end(), instance, comparer);
    return tasks_queue_.insert(position, move(instance));
}

void TaskScheduler::remove_scheduled_instance(TaskId task_id) {
    const auto comparer = [&](const TaskExecutionInstance& element) {
        return element.task_id == task_id;
    };
    tasks_queue_.erase(find_if(tasks_queue_.begin(), tasks_queue_.end(), comparer));
}

void TaskScheduler::process_tasks() {
    while (running_) {
        unique_lock<mutex> lock(tasks_mutex_);
        auto now = ClockType::now();
        while (running_) {
            auto now = ClockType::now();
            // Some random wake up time by default
            auto wake_up_time = now + seconds(10);
            if (!tasks_queue_.empty()) {
                wake_up_time = tasks_queue_.front().scheduled_for;
            }
            // If we should be awake already, stop loooping
            if (wake_up_time <= now) {
                break;
            }
            tasks_condition_.wait_until(lock, wake_up_time);
        }
        // Make sure there's actually something to process
        if (tasks_queue_.empty()) {
            continue;
        }
        TaskExecutionInstance instance = move(tasks_queue_.front());
        tasks_queue_.pop_front();
        // Refresh the current wake_up_time
        now = ClockType::now();

        // Before leaving the critical section, re-schedule the task
        TaskMetadata& meta = tasks_.at(instance.task_id);
        instance.scheduled_at = now;
        instance.scheduled_for = now + get_schedule_delta(meta);
        // If we're past our priority adjustment offset, this means the priority hasn't changed
        // in a while. Update it accordingly
        if (meta.last_priority_set_time + priority_adjustment_offset_ < now) {
            const double new_priority = min(1.0, meta.priority * 2.0);
            if (new_priority != meta.priority) {
                meta.priority = new_priority;
                LOG4CXX_TRACE(logger, "Set priority for task " << instance.task_id << " to "
                              << meta.priority);
            }
        }
        meta.queue_iterator = schedule_task(instance.task_id, meta);

        // Execute the task outside of the critical section
        const Task task = meta.task;
        lock.unlock();
        task();
    }
}

} // pirulo
