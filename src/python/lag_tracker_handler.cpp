#include <algorithm>
#include "detail/logging.h"
#include "python/lag_tracker_handler.h"

using std::string;
using std::vector;
using std::max;
using std::make_tuple;
using std::shared_ptr;

using boost::optional;

namespace pirulo {
namespace api {

PIRULO_CREATE_LOGGER("p.lag_tracker");

void LagTrackerHandler::handle_initialize() {
    LOG4CXX_INFO(logger, "Initializing lag tracker handler");
    const auto& offset_store = get_offset_store();
    for (const string& consumer : offset_store->get_consumers()) {
        const vector<ConsumerOffset> offsets = offset_store->get_consumer_offsets(consumer);
        for (const ConsumerOffset& offset : offsets) {
            const auto& topic_partition = offset.get_topic_partition();
            const string& topic = topic_partition.get_topic();
            const int partition = topic_partition.get_partition();
            TopicPartitionInfo& info = topic_partition_info_[make_tuple(topic, partition)];
            info.consumer_offsets.emplace(consumer, topic_partition.get_offset());
            if (info.offset == -1) {
                const optional<int64_t> maybe_offset = offset_store->get_topic_offset(topic,
                                                                                      partition);
                if (maybe_offset) {
                    info.offset = *maybe_offset;
                }
            }
        }
    }

    subscribe_to_topics();
    subscribe_to_topic_message();
    subscribe_to_consumers();
    subscribe_to_consumer_commits();
}

void LagTrackerHandler::handle_new_consumer(const string& group_id) {

}

void LagTrackerHandler::handle_new_topic(const string& topic) {

}

void LagTrackerHandler::handle_consumer_commit(const string& group_id, const string& topic,
                                               int partition, int64_t offset) {
    auto& info = topic_partition_info_[make_tuple(topic, partition)];
    info.consumer_offsets[group_id] = offset;
    if (info.offset != -1) {
        handle_lag_update(topic, partition, group_id, max<int64_t>(0, info.offset - offset));
    }
}

void LagTrackerHandler::handle_topic_message(const string& topic, int partition, int64_t offset) {
    auto& info = topic_partition_info_[make_tuple(topic, partition)];
    info.offset = offset;
    for (const auto& consumer_offset_pair : info.consumer_offsets) {
        const string& group_id = consumer_offset_pair.first;
        const uint64_t lag = max<int64_t>(0, offset - consumer_offset_pair.second);
        handle_lag_update(topic, partition, group_id, lag);
    }
}

} // api
} // pirulo
