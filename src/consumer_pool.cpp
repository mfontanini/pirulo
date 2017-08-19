#include "consumer_pool.h"

using std::unique_lock;
using std::mutex;

using cppkafka::Configuration;
using cppkafka::Consumer;

namespace pirulo {

ConsumerPool::ConsumerPool(size_t consumer_count, Configuration config) {
    for (size_t i = 0; i < consumer_count; ++i) {
        consumers_.emplace_back(config);
        available_consumers_.push(consumers_.rbegin());
    }
}

void ConsumerPool::acquire_consumer(const ConsumerCallback& callback) {
    unique_lock<mutex> lock(consumers_mutex_);
    while (available_consumers_.empty()) {
        consumers_condition_.wait(lock);
    }
    auto iter = available_consumers_.front();
    available_consumers_.pop();
    Consumer& consumer = *iter;

    // Execute outside of the critical section
    lock.unlock();
    callback(consumer);
    lock.lock();

    available_consumers_.push(iter);
    consumers_condition_.notify_one();
}

} // pirulo
