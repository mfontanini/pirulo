#include "consumer_offset_store.h"

using std::string;
using std::mutex;
using std::lock_guard;
using std::vector;

using cppkafka::TopicPartition;

namespace pirulo {

// ConsumerOffset

ConsumerOffsetStore::ConsumerOffset::ConsumerOffset(string group_id, string topic,
                                                    unsigned partition, uint64_t offset)
: group_id_(move(group_id)), topic_partition_(move(topic), partition, offset) {

}

const string& ConsumerOffsetStore::ConsumerOffset::get_group_id() const {
    return group_id_;
}

const TopicPartition& ConsumerOffsetStore::ConsumerOffset::get_topic_partition() const {
    return topic_partition_;
}

// ConsumerOffsetStore

void ConsumerOffsetStore::store(const string& group_id, const string& topic, unsigned partition,
                                uint64_t offset) {
    lock_guard<mutex> _(offsets_mutex_);
    offsets_[group_id][{ topic, static_cast<int>(partition) }] = offset;
}

vector<ConsumerOffsetStore::ConsumerOffset>
ConsumerOffsetStore::get_offsets(const string& group_id) const {
    lock_guard<mutex> _(offsets_mutex_);
    auto iter = offsets_.find(group_id);
    if (iter == offsets_.end()) {
        return {};
    }
    vector<ConsumerOffset> output;
    for (const auto& topic_pair : iter->second) {
        output.emplace_back(group_id, topic_pair.first.get_topic(),
                            topic_pair.first.get_partition(), topic_pair.second);
    }
    return output;
}

} // pirulo
