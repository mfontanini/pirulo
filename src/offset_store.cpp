#include "offset_store.h"

using std::string;
using std::mutex;
using std::lock_guard;
using std::vector;

using boost::optional;

using cppkafka::TopicPartition;

namespace pirulo {

// ConsumerOffset

OffsetStore::ConsumerOffset::ConsumerOffset(string group_id, string topic,
                                            int partition, uint64_t offset)
: group_id_(move(group_id)), topic_partition_(move(topic), partition, offset) {

}

const string& OffsetStore::ConsumerOffset::get_group_id() const {
    return group_id_;
}

const TopicPartition& OffsetStore::ConsumerOffset::get_topic_partition() const {
    return topic_partition_;
}

// OffsetStore

void OffsetStore::store_consumer_offset(const string& group_id, const string& topic,
                                        int partition, uint64_t offset) {
    lock_guard<mutex> _(consumer_offsets_mutex_);
    consumer_offsets_[group_id][{ topic, partition }] = offset;
}

void OffsetStore::store_topic_offset(const string& topic, int partition,
                                     uint64_t offset) {
    lock_guard<mutex> _(topic_offsets_mutex_);
    topic_offsets_[{topic, partition}] = offset;
}

vector<OffsetStore::ConsumerOffset>
OffsetStore::get_consumer_offsets(const string& group_id) const {
    lock_guard<mutex> _(consumer_offsets_mutex_);
    auto iter = consumer_offsets_.find(group_id);
    if (iter == consumer_offsets_.end()) {
        return {};
    }
    vector<ConsumerOffset> output;
    for (const auto& topic_pair : iter->second) {
        output.emplace_back(group_id, topic_pair.first.get_topic(),
                            topic_pair.first.get_partition(), topic_pair.second);
    }
    return output;
}

optional<uint64_t> OffsetStore::get_topic_offset(const string& topic, int partition) const {
    lock_guard<mutex> _(topic_offsets_mutex_);
    auto iter = topic_offsets_.find({ topic, partition });
    if (iter == topic_offsets_.end()) {
        return boost::none;
    }
    return iter->second;
}

} // pirulo
