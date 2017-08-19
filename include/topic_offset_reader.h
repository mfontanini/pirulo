#pragma once

#include <mutex>
#include <set>
#include <chrono>
#include <cppkafka/consumer.h>
#include "utils/thread_pool.h"
#include "utils/task_scheduler.h"
#include "offset_store.h"
#include "consumer_offset_reader.h"
#include "consumer_pool.h"

namespace pirulo {

class TopicOffsetReader {
public:
    using StorePtr = std::shared_ptr<OffsetStore>;
    using ConsumerOffsetReaderPtr = std::shared_ptr<ConsumerOffsetReader>;

    TopicOffsetReader(StorePtr store, size_t thread_count,
                      ConsumerOffsetReaderPtr consumer_reader,
                      cppkafka::Configuration config);

    void run();
    void stop();

    StorePtr get_store() const;
private:
    using TopicPartitionCount = std::unordered_map<std::string, size_t>;
    using MetadataCallback = std::function<void(TopicPartitionCount)>;
    using TopicTaskIdMap = std::map<cppkafka::TopicPartition, TaskScheduler::TaskId>;

    void async_process_topics(const TopicPartitionCount& topics);
    void monitor_topics(const TopicPartitionCount& topics);
    void monitor_topics(const cppkafka::TopicPartitionList& topics);
    void monitor_new_topics();
    cppkafka::TopicPartitionList get_new_topic_partitions(const TopicPartitionCount& counts);
    TopicPartitionCount load_metadata();
    void process_metadata(const MetadataCallback& callback);
    void process_topic_partition(const cppkafka::TopicPartition& topic_partition);
    void on_commit(const std::string& topic, int partition);

    ConsumerPool consumer_pool_;
    StorePtr store_;
    ThreadPool thread_pool_;
    TaskScheduler task_scheduler_;
    ConsumerOffsetReaderPtr consumer_offset_reader_;
    TopicTaskIdMap monitored_topic_task_id_;
    std::chrono::seconds maximum_topic_reload_time_{100};
    std::chrono::seconds maximum_metadata_reload_time_{100};
    bool running_{true};
};

} // pirulo
