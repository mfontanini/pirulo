#include <functional>
#include "python/handler.h"
#include "detail/logging.h"

using std::string;
using std::shared_ptr;
using std::bind;
using std::vector;

using namespace std::placeholders;

namespace pirulo {
namespace api {

PIRULO_CREATE_LOGGER("p.handler");

void Handler::initialize(const shared_ptr<OffsetStore>& store) {
    offset_store_ = store;
    handle_initialize();
}

void Handler::subscribe_to_consumers() {
    offset_store_->on_new_consumer(bind(&Handler::on_new_consumer, this, _1));
    for (const string& group_id : offset_store_->get_consumers()) {
        on_new_consumer(group_id);
    }
}

void Handler::subscribe_to_consumer_commits() {
    if (track_consumer_commits_) {
        return;
    }
    track_consumer_commits_ = true;
    for (const string& group_id : offset_store_->get_consumers()) {
        consumer_subscribe(group_id);
    }
}

void Handler::subscribe_to_topics() {
    offset_store_->on_new_topic(bind(&Handler::on_new_topic, this, _1));
    for (const string& topic : offset_store_->get_topics()) {
        on_new_topic(topic);
    }
}

void Handler::subscribe_to_topic_message() {
    if (track_topic_messages_) {
        return;
    }
    track_topic_messages_ = true;
    for (const string& topic : offset_store_->get_topics()) {
        topic_subscribe(topic);
    }
}

const shared_ptr<OffsetStore>& Handler::get_offset_store() const {
    return offset_store_;
}

void Handler::handle_initialize() {

}

void Handler::handle_new_consumer(const string& group_id) {

}

void Handler::handle_new_topic(const string& topic) {

}

void Handler::handle_consumer_commit(const string& group_id, const string& topic,
                                     int partition, int64_t offset) {

}

void Handler::handle_topic_message(const string& topic, int partition, int64_t offset) {

}

void Handler::on_new_consumer(const string& group_id) {
    LOG4CXX_DEBUG(logger, "Found new consumer: " << group_id);
    handle_new_consumer(group_id);
    if (track_consumer_commits_) {
        LOG4CXX_DEBUG(logger, "Subscribing to consumer");
        consumer_subscribe(group_id);
        const vector<ConsumerOffset> offsets = get_offset_store()->get_consumer_offsets(group_id);
        for (const ConsumerOffset& offset : offsets) {
            const auto& topic_partition = offset.get_topic_partition();
            on_consumer_commit(group_id, topic_partition.get_topic(),
                               topic_partition.get_partition(), topic_partition.get_offset());
        }
    }
}

void Handler::on_new_topic(const string& topic) {
    LOG4CXX_DEBUG(logger, "Found new topic: " << topic);
    handle_new_topic(topic);
    if (track_topic_messages_) {
        topic_subscribe(topic);
    }
}

void Handler::on_consumer_commit(const string& group_id, const string& topic,
                                 int partition, int64_t offset) {
    handle_consumer_commit(group_id, topic, partition, offset);
}

void Handler::on_topic_message(const string& topic, int partition, int64_t offset) {
    handle_topic_message(topic, partition, offset);
}

void Handler::consumer_subscribe(const string& group_id) {
    offset_store_->on_consumer_commit(group_id,
                                      bind(&Handler::on_consumer_commit, this, _1, _2,
                                           _3, _4));
}

void Handler::topic_subscribe(const string& topic) {
    offset_store_->on_topic_message(topic,
                                    bind(&Handler::on_topic_message, this, _1, _2, _3));
}

} // api
} // pirulo
