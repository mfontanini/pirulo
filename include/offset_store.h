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
#include "utils/observer.h"

namespace pirulo {

class OffsetStore {
public:
    using ConsumerCallback = std::function<void(const std::string& group_id)>;
    using ConsumerCommitCallback = std::function<void(const std::string& group_id,
                                                      const std::string& topic,
                                                      int partition,
                                                      uint64_t offset)>;

    OffsetStore();

    void store_consumer_offset(const std::string& group_id, const std::string& topic,
                               int partition, uint64_t offset);
    void store_topic_offset(const std::string& topic, int partition, uint64_t offset);
    void on_new_consumer(ConsumerCallback callback);
    void on_consumer_commit(const std::string& group_id, ConsumerCommitCallback callback);

    void enable_notifications();

    std::vector<std::string> get_consumers() const;
    std::vector<ConsumerOffset> get_consumer_offsets(const std::string& group_id) const;
    boost::optional<int64_t> get_topic_offset(const std::string& topic,
                                              int partition) const;
private:
    using TopicMap = std::map<cppkafka::TopicPartition, int64_t>;
    using ConsumerMap = std::unordered_map<std::string, TopicMap>;
    using ConsumerSet = std::unordered_set<std::string>;

    ConsumerMap consumer_offsets_;
    TopicMap topic_offsets_;
    ConsumerSet consumers_;
    Observer<int, std::string> new_consumer_observer_;
    Observer<std::string, std::string, int, uint64_t> consumer_commit_observer_;
    std::string new_consumer_id_;
    mutable std::mutex consumer_offsets_mutex_;
    mutable std::mutex topic_offsets_mutex_;
    bool notifications_enabled_{false};
};

} // pirulo
