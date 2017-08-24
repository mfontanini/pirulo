#pragma once

#include <string>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <mutex>
#include <vector>
#include <functional>
#include <boost/optional.hpp>
#include <cppkafka/topic_partition.h>
#include "consumer_offset.h"
#include "utils/async_observer.h"
#include "utils/thread_pool.h"

namespace pirulo {

class OffsetStore {
public:
    using ConsumerCallback = std::function<void(const std::string& group_id)>;
    using TopicCallback = std::function<void(const std::string& topic_name)>;
    using ConsumerCommitCallback = std::function<void(const std::string& group_id,
                                                      const std::string& topic,
                                                      int partition,
                                                      uint64_t offset)>;
    using TopicMessageCallback = std::function<void(const std::string& topic,
                                                    int partition,
                                                    uint64_t offset)>;

    OffsetStore();

    void store_consumer_offset(const std::string& group_id, const std::string& topic,
                               int partition, uint64_t offset);
    void store_topic_offset(const std::string& topic, int partition, uint64_t offset);
    void on_new_consumer(ConsumerCallback callback);
    void on_new_topic(TopicCallback callback);
    void on_consumer_commit(const std::string& group_id, ConsumerCommitCallback callback);
    void on_topic_message(const std::string& topic, TopicMessageCallback callback);

    void enable_notifications();

    std::vector<std::string> get_consumers() const;
    std::vector<ConsumerOffset> get_consumer_offsets(const std::string& group_id) const;
    boost::optional<int64_t> get_topic_offset(const std::string& topic,
                                              int partition) const;
    std::vector<std::string> get_topics() const;
private:
    // Make sure tasks won't start piling up
    static constexpr size_t MAXIMUM_OBSERVER_TASKS = 10000;

    using TopicMap = std::map<cppkafka::TopicPartition, int64_t>;
    using ConsumerMap = std::unordered_map<std::string, TopicMap>;
    using StringSet = std::unordered_set<std::string>;

    ConsumerMap consumer_offsets_;
    TopicMap topic_offsets_;
    StringSet consumers_;
    StringSet topics_;
    ThreadPool thread_pool_{1, MAXIMUM_OBSERVER_TASKS};
    AsyncObserver<int, std::string> new_string_observer_;
    AsyncObserver<std::string, std::string, int, uint64_t> consumer_commit_observer_;
    AsyncObserver<std::string, int, uint64_t> topic_message_observer_; 
    std::string new_consumer_id_;
    mutable std::mutex consumer_offsets_mutex_;
    mutable std::mutex topic_offsets_mutex_;
    bool notifications_enabled_{false};
};

} // pirulo
