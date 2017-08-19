#include "offset_store.h"

using std::string;
using std::mutex;
using std::lock_guard;
using std::vector;
using std::move;

using std::chrono::seconds;
using std::chrono::milliseconds;

using boost::optional;

using cppkafka::TopicPartition;

namespace pirulo {

static const int NEW_CONSUMER_ID = 0;

// TODO: don't hardcode these constants
OffsetStore::OffsetStore()
: consumer_commit_observer_(seconds(10)) {

}

void OffsetStore::store_consumer_offset(const string& group_id, const string& topic,
                                        int partition, uint64_t offset) {
    {
        lock_guard<mutex> _(consumer_offsets_mutex_);
        consumer_offsets_[group_id][{ topic, partition }] = offset;
    }

    if (!notifications_enabled_) {
        return;
    }

    // Notify that there was a new commit for this consumer group
    consumer_commit_observer_.notify(group_id, topic, partition, offset);
    // If this is a new consumer group, notify
    if (consumers_.insert(group_id).second) {
        new_consumer_observer_.notify(NEW_CONSUMER_ID, group_id);
    }
}

void OffsetStore::store_topic_offset(const string& topic, int partition,
                                     uint64_t offset) {
    lock_guard<mutex> _(topic_offsets_mutex_);
    topic_offsets_[{topic, partition}] = offset;
}

void OffsetStore::on_new_consumer(ConsumerCallback callback) {
    new_consumer_observer_.observe(NEW_CONSUMER_ID, [=](int, const string& group_id) {
        callback(group_id);
    });
}

void OffsetStore::on_consumer_commit(const string& group_id, ConsumerCommitCallback callback) {
    consumer_commit_observer_.observe(group_id, move(callback));
}

void OffsetStore::enable_notifications() {
    notifications_enabled_ = true;
}

vector<string> OffsetStore::get_consumers() const {
    vector<string> output;
    lock_guard<mutex> _(consumer_offsets_mutex_);
    for (const auto& consumer_pair : consumer_offsets_) {
        output.emplace_back(consumer_pair.first);
    }
    return output;
}

vector<ConsumerOffset> OffsetStore::get_consumer_offsets(const string& group_id) const {
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

optional<int64_t> OffsetStore::get_topic_offset(const string& topic, int partition) const {
    lock_guard<mutex> _(topic_offsets_mutex_);
    auto iter = topic_offsets_.find({ topic, partition });
    if (iter == topic_offsets_.end()) {
        return boost::none;
    }
    return iter->second;
}

} // pirulo
