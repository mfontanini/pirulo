#pragma once

#include <map>
#include <chrono>
#include <functional>
#include <mutex>

namespace pirulo {

template <typename T>
class Observer {
public:
    using ObserverCallback = std::function<void(const T&)>;

    Observer(std::chrono::milliseconds cool_down_time);

    void observe(const T& object, ObserverCallback callback);
    void notify(const T& object);

private:
    using ClockType = std::chrono::steady_clock;
    struct ObservedContext {
        std::vector<ObserverCallback> observers;
        ClockType::time_point last_observe_time;
    };
    using ObservedObjectsMap = std::map<T, ObservedContext>;

    ObservedObjectsMap observed_objects_;
    std::chrono::milliseconds cool_down_time_;
    mutable std::mutex observed_objects_mutex_;
};

template <typename T>
Observer<T>::Observer(std::chrono::milliseconds cool_down_time)
: cool_down_time_(cool_down_time) {

}

template <typename T>
void Observer<T>::observe(const T& object, ObserverCallback callback) {
    std::lock_guard<std::mutex> _(observed_objects_mutex_);
    observed_objects_[object].observers.emplace_back(std::move(callback));
}

template <typename T>
void Observer<T>::notify(const T& object) {
    std::unique_lock<std::mutex> lock(observed_objects_mutex_);
    auto iter = observed_objects_.find(object);
    if (iter == observed_objects_.end()) {
        return;
    }
    auto now = ClockType::now();
    // If we're still in cooldown phase, then don't trigger any callbacks
    if (iter->second.last_observe_time + cool_down_time_ > now) {
        return;
    }
    iter->second.last_observe_time = now;

    // Get the observers and release the lock
    const std::vector<ObserverCallback> observers = iter->second.observers;
    lock.unlock();

    for (const ObserverCallback& callback : observers) {
        callback(object);
    }
}

} // pirulo
