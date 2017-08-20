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
static const int NEW_TOPIC_ID = 1;

// TODO: don't hardcode these constants
OffsetStore::OffsetStore()
: consumer_commit_observer_(seconds(10)), topic_message_observer_(seconds(10)) {

}

void OffsetStore::store_consumer_offset(const string& group_id, const string& topic,
                                        int partition, uint64_t offset) {
    bool is_new_consumer = false;
    {
        lock_guard<mutex> _(consumer_offsets_mutex_);
        consumer_offsets_[group_id][{ topic, partition }] = offset;
        is_new_consumer = consumers_.insert(group_id).second;
    }
    // If notifications aren't enabled, we're done
    if (!notifications_enabled_) {
        return;
    }

    // If this is a new consumer group, notify
    if (is_new_consumer) {
        new_string_observer_.notify(NEW_CONSUMER_ID, group_id);
    }
    // Notify that there was a new commit for this consumer group
    consumer_commit_observer_.notify(group_id, topic, partition, offset);
}

void OffsetStore::store_topic_offset(const string& topic, int partition,
                                     uint64_t offset) {
    bool is_new_topic = false;
    bool is_new_offset = false;
    {
        lock_guard<mutex> _(topic_offsets_mutex_);
        int64_t& existing_offset = topic_offsets_[{topic, partition}];
        is_new_offset = existing_offset != static_cast<int64_t>(offset);
        is_new_topic = topics_.emplace(topic).second;
        if (is_new_offset) {
            existing_offset = offset;
        }
    }
    // If notifications aren't enabled, we're done
    if (!notifications_enabled_) {
        return;
    }

    if (is_new_topic) {
        new_string_observer_.notify(NEW_TOPIC_ID, topic);
    }
    if (is_new_offset) {
        topic_message_observer_.notify(topic, partition, offset);
    }
}

void OffsetStore::on_new_consumer(ConsumerCallback callback) {
    new_string_observer_.observe(NEW_CONSUMER_ID, [=](int, const string& group_id) {
        callback(group_id);
    });
}

void OffsetStore::on_new_topic(TopicCallback callback) {
    new_string_observer_.observe(NEW_TOPIC_ID, [=](int, const string& topic) {
        callback(topic);
    });
}

void OffsetStore::on_consumer_commit(const string& group_id, ConsumerCommitCallback callback) {
    consumer_commit_observer_.observe(group_id, move(callback));
}

void OffsetStore::on_topic_message(const string& topic, TopicMessageCallback callback) {
    topic_message_observer_.observe(topic, move(callback));
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

vector<string> OffsetStore::get_topics() const {
    return vector<string>(topics_.begin(), topics_.end());
}

} // pirulo
