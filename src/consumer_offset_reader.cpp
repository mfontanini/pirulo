#include <cstdint>
#include "consumer_offset_reader.h"
#include "exceptions.h"
#include "detail/memory.h"
#include "detail/logging.h"

using std::move;
using std::string;

using cppkafka::Configuration;
using cppkafka::ConsumerDispatcher;
using cppkafka::Message;
using cppkafka::TopicPartition;
using cppkafka::TopicPartitionList;

namespace pirulo {

PIRULO_CREATE_LOGGER("p.offsets");

Configuration prepare_config(Configuration config) {
    config.set_default_topic_configuration({{ "auto.offset.reset", "smallest" }});
    return config;
}

ConsumerOffsetReader::ConsumerOffsetReader(StorePtr store, Configuration config) :
    store_(move(store)), consumer_(prepare_config(move(config))) {

}

void ConsumerOffsetReader::run(const EofCallback& callback) {
    consumer_.set_assignment_callback([&](const TopicPartitionList& topic_partitions) {
        for (const TopicPartition& topic_partition : topic_partitions) {
            pending_partitions_.emplace(topic_partition.get_partition());
        }
    });
    consumer_.subscribe({ "__consumer_offsets" });
    dispatcher.run(
        [&](Message msg) {
            try {
                handle_message(move(msg));
            }
            catch (const ParseException&) {
                LOG4CXX_WARN(logger, "Failed to parse consumer offset record");
            }
        },
        [&](ConsumerDispatcher::EndOfFile, const TopicPartition& topic_partition) {
            // If we reached EOF on all partitions, execute the EOF callback
            if (pending_partitions_.erase(topic_partition.get_partition()) &&
                pending_partitions_.empty()) {
                callback();
            }
        }
    );
}

void ConsumerOffsetReader::stop() {
    dispatcher.stop();
}

ConsumerOffsetReader::StorePtr ConsumerOffsetReader::get_store() const {
    return store_;
}

void ConsumerOffsetReader::handle_message(Message msg) {
    InputMemoryStream key_input(msg.get_key());
    uint16_t version = key_input.read_be<uint16_t>();
    if (version > 1) {
        LOG4CXX_TRACE(logger, "Ignoring key message version: " << version);
        return;
    }
    string group_id = key_input.read<string>();
    string topic = key_input.read<string>();
    uint32_t partition = key_input.read_be<uint32_t>();

    InputMemoryStream value_input(msg.get_payload());
    // Value version
    version = value_input.read_be<uint16_t>();
    if (version > 1) {
        throw ParseException();
    }
    uint64_t offset = value_input.read_be<uint64_t>();
    store_->store_consumer_offset(group_id, topic, partition, offset);
}

} // pirulo
