#pragma once

#include <mutex>
#include <cppkafka/consumer.h>
#include "utils/thread_pool.h"
#include "offset_store.h"

namespace pirulo {

class TopicOffsetReader {
public:
    using StorePtr = std::shared_ptr<OffsetStore>;

    TopicOffsetReader(StorePtr store, size_t thread_count, cppkafka::Configuration config);

    void run();
    void stop();
private:
    using TopicPartitionCount = std::unordered_map<std::string, size_t>;

    void process();
    TopicPartitionCount load_metadata();
    void process_topic_partition(const cppkafka::TopicPartition& topic_partition);

    cppkafka::Consumer consumer_;
    StorePtr store_;
    ThreadPool thread_pool_;
    bool running_{true};
};

} // pirulo
