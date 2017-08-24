#pragma once

#include "utils/thread_pool.h"
#include "utils/observer.h"

namespace pirulo {

template <typename T, typename... Args>
class AsyncObserver {
public:
    using ObserverCallback = typename Observer<T, Args...>::ObserverCallback;

    AsyncObserver(ThreadPool& pool);
    AsyncObserver(ThreadPool& pool, std::chrono::milliseconds cool_down_time);

    void observe(const T& object, const ObserverCallback& callback);
    void notify(const T& object, const Args&... args);
private:
    Observer<T, Args...> observer_;
    ThreadPool& pool_;
};

template <typename T, typename... Args>
AsyncObserver<T, Args...>::AsyncObserver(ThreadPool& pool)
: pool_(pool) {

}

template <typename T, typename... Args>
AsyncObserver<T, Args...>::AsyncObserver(ThreadPool& pool,
                                         std::chrono::milliseconds cool_down_time)
: observer_(cool_down_time), pool_(pool) {

}

template <typename T, typename... Args>
void AsyncObserver<T, Args...>::observe(const T& object, const ObserverCallback& callback) {
    observer_.observe(object, [&, callback](const T& object, const Args&... args) {
        auto wrapped_callback = std::bind(callback, object, args...);
        pool_.add_task([wrapped_callback]() {
            wrapped_callback();
        });
    });
}

template <typename T, typename... Args>
void AsyncObserver<T, Args...>::notify(const T& object, const Args&... args) {
    observer_.notify(object, args...);
}

} // pirulo
