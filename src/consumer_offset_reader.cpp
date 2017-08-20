#include <cstdint>
#include "consumer_offset_reader.h"
#include "exceptions.h"
#include "detail/memory.h"
#include "detail/logging.h"
#include "utils/utils.h"

using std::move;
using std::string;

using std::chrono::milliseconds;

using cppkafka::Configuration;
using cppkafka::ConsumerDispatcher;
using cppkafka::Message;
using cppkafka::TopicPartition;
using cppkafka::TopicPartitionList;

namespace pirulo {

PIRULO_CREATE_LOGGER("p.offsets");

static Configuration prepare_config(Configuration config) {
    config.set_default_topic_configuration({{ "auto.offset.reset", "smallest" }});
    config.set("group.id", utils::generate_group_id());
    return config;
}

ConsumerOffsetReader::ConsumerOffsetReader(StorePtr store, milliseconds consumer_offset_cool_down,
                                           Configuration config) :
    store_(move(store)), consumer_(prepare_config(move(config))),
    observer_(consumer_offset_cool_down) {

}

void ConsumerOffsetReader::run(const EofCallback& callback) {
    consumer_.set_assignment_callback([&](const TopicPartitionList& topic_partitions) {
        for (const TopicPartition& topic_partition : topic_partitions) {
            pending_partitions_.emplace(topic_partition.get_partition());
        }
    });
    LOG4CXX_INFO(logger, "Starting loading consumer offsets");
    
    consumer_.subscribe({ "__consumer_offsets" });
    dispatcher_.run(
        [&](Message msg) {
            try {
                if (msg.get_payload()) {
                    handle_message(msg);
                }
            }
            catch (const ParseException&) {
                LOG4CXX_WARN(logger, "Failed to parse consumer offset record");
            }
        },
        [&](ConsumerDispatcher::EndOfFile, const TopicPartition& topic_partition) {
            // If we reached EOF on all partitions, execute the EOF callback
            if (pending_partitions_.erase(topic_partition.get_partition()) &&
                pending_partitions_.empty()) {
                LOG4CXX_INFO(logger, "Finished loading consumer offsets");
                callback();

                // Enable notifications for new commits
                notifications_enabled_ = true;
            }
        }
    );
}

void ConsumerOffsetReader::stop() {
    dispatcher_.stop();
}

void ConsumerOffsetReader::watch_commits(const string& topic, int partition,
                                         TopicCommitCallback callback) {
    auto wrapped_callback = [callback](const TopicPartition& topic_partition) {
        callback(topic_partition.get_topic(), topic_partition.get_partition());
    };
    observer_.observe({ topic, partition }, move(wrapped_callback));
}

ConsumerOffsetReader::StorePtr ConsumerOffsetReader::get_store() const {
    return store_;
}

void ConsumerOffsetReader::handle_message(const Message& msg) {
    InputMemoryStream key_input(msg.get_key());
    uint16_t version = key_input.read_be<uint16_t>();
    if (version > 1) {
        return;
    }
    string group_id = key_input.read<string>();
    string topic = key_input.read<string>();
    int partition = key_input.read_be<uint32_t>();

    InputMemoryStream value_input(msg.get_payload());
    // Value version
    version = value_input.read_be<uint16_t>();
    if (version > 1) {
        throw ParseException();
    }
    uint64_t offset = value_input.read_be<uint64_t>();
    store_->store_consumer_offset(group_id, topic, partition, offset);

    if (notifications_enabled_) {
        observer_.notify({ topic, partition });
    }
}

} // pirulo
