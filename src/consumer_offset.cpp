#include <boost/functional/hash.hpp>
#include "consumer_offset.h"

using std::string;

using cppkafka::TopicPartition;

namespace pirulo {

ConsumerOffset::ConsumerOffset(string group_id, string topic,
                               int partition, uint64_t offset)
: group_id_(move(group_id)), topic_partition_(move(topic), partition, offset) {

}

const string& ConsumerOffset::get_group_id() const {
    return group_id_;
}

const TopicPartition& ConsumerOffset::get_topic_partition() const {
    return topic_partition_;
}

bool operator==(const ConsumerOffset& lhs, const ConsumerOffset& rhs) {
    return lhs.get_topic_partition() == rhs.get_topic_partition();
}

bool operator!=(const ConsumerOffset& lhs, const ConsumerOffset& rhs) {
    return !(lhs == rhs);
}

} // pirulo

using CO = pirulo::ConsumerOffset;

namespace std {

size_t hash<CO>::operator()(const CO& consumer_offset) const {
    size_t output = 0;
    boost::hash_combine(output, consumer_offset.get_group_id());
    boost::hash_combine(output, consumer_offset.get_topic_partition().get_topic());
    boost::hash_combine(output, consumer_offset.get_topic_partition().get_partition());
    return output;
}

} // std
