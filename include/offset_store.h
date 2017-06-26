#pragma once

#include <string>
#include <cstdint>
#include <unordered_map>
#include <map>
#include <mutex>
#include <vector>
#include <boost/optional.hpp>
#include <cppkafka/topic_partition.h>
#include "consumer_offset.h"

namespace pirulo {

class OffsetStore {
public:
    void store_consumer_offset(const std::string& group_id, const std::string& topic,
                               int partition, uint64_t offset);
    void store_topic_offset(const std::string& topic, int partition, uint64_t offset);

    std::vector<std::string> get_consumers() const;
    std::vector<ConsumerOffset> get_consumer_offsets(const std::string& group_id) const;
    boost::optional<int64_t> get_topic_offset(const std::string& topic,
                                              int partition) const;
private:
    using TopicMap = std::map<cppkafka::TopicPartition, int64_t>;
    using ConsumerMap = std::unordered_map<std::string, TopicMap>;

    ConsumerMap consumer_offsets_;
    TopicMap topic_offsets_;
    mutable std::mutex consumer_offsets_mutex_;
    mutable std::mutex topic_offsets_mutex_;
};

} // pirulo
