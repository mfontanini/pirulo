#pragma once

#include <string>
#include <memory>
#include <cppkafka/topic_partition.h>

namespace pirulo {

class ConsumerOffset {
public:
    ConsumerOffset(std::string group_id, std::string topic, int partition,
                   uint64_t offset);
    const std::string& get_group_id() const;
    const cppkafka::TopicPartition& get_topic_partition() const;
private:
    std::string group_id_;
    cppkafka::TopicPartition topic_partition_;
};

bool operator==(const ConsumerOffset& lhs, const ConsumerOffset& rhs);
bool operator!=(const ConsumerOffset& lhs, const ConsumerOffset& rhs);

} // pirulo

// Specialize std::hash for ConsumerOffset
namespace std {

template <>
struct hash<pirulo::ConsumerOffset> {
    size_t operator()(const pirulo::ConsumerOffset& consumer_offset) const;
};

} // std
