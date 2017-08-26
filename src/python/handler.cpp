#include <functional>
#include "python/handler.h"

using std::string;
using std::shared_ptr;
using std::bind;

using namespace std::placeholders;

namespace pirulo {
namespace api {

void Handler::initialize(const shared_ptr<OffsetStore>& store) {
    offset_store_ = store;
    get_override("handle_initialize")(store);
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
        offset_store_->on_consumer_commit(group_id,
                                          bind(&Handler::on_consumer_commit, this, _1, _2,
                                               _3, _4));
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
        offset_store_->on_topic_message(topic,
                                        bind(&Handler::on_topic_message, this, _1, _2, _3));
    }
}

void Handler::on_new_consumer(const string& group_id) {
    get_override("handle_new_consumer")(group_id);
}

void Handler::on_new_topic(const string& topic) {
    get_override("handle_new_topic")(topic);
}

void Handler::on_consumer_commit(const string& group_id, const string& topic,
                                 int partition, int64_t offset) {
    get_override("handle_consumer_commit")(group_id, topic, partition, offset);
}

void Handler::on_topic_message(const string& topic, int partition, int64_t offset) {
    get_override("handle_topic_message")(topic, partition, offset);
}

} // api
} // pirulo
