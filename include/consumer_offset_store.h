#pragma once

#include <string>
#include <cstdint>
#include <unordered_map>
#include <map>
#include <mutex>
#include <vector>
#include <cppkafka/topic_partition.h>

namespace pirulo {

class ConsumerOffsetStore {
public:
    class ConsumerOffset {
    public:
        ConsumerOffset(std::string group_id, std::string topic, unsigned partition,
                       uint64_t offset);
        const std::string& get_group_id() const;
        const cppkafka::TopicPartition& get_topic_partition() const;
    private:
        std::string group_id_;
        cppkafka::TopicPartition topic_partition_;
    };

    void store(const std::string& group_id, const std::string& topic, unsigned partition,
               uint64_t offset);
    std::vector<ConsumerOffset> get_offsets(const std::string& group_id) const;
private:
    using TopicMap = std::map<cppkafka::TopicPartition, int64_t>;
    using ConsumerMap = std::unordered_map<std::string, TopicMap>;

    ConsumerMap offsets_;
    mutable std::mutex offsets_mutex_;
};

} // pirulo
