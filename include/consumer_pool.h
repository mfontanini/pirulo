#pragma once

#include <vector>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <queue>
#include <cppkafka/consumer.h>

namespace pirulo {

class ConsumerPool {
public:
    using ConsumerCallback = std::function<void(cppkafka::Consumer&)>;

    ConsumerPool(size_t consumer_count, cppkafka::Configuration config);

    void acquire_consumer(const ConsumerCallback& callback);
private:
    using ConsumerContainer = std::deque<cppkafka::Consumer>;
    ConsumerContainer consumers_;
    std::queue<ConsumerContainer::reverse_iterator> available_consumers_;
    std::mutex consumers_mutex_;
    std::condition_variable consumers_condition_;
};

} // pirulo
