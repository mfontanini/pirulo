#include <tuple>
#include <cppkafka/metadata.h>
#include "topic_offset_reader.h"
#include "detail/logging.h"

using std::string;
using std::move;
using std::unordered_set;
using std::lock_guard;
using std::mutex;
using std::tie;
using std::ignore;

using std::this_thread::sleep_for;

using std::chrono::seconds;

using cppkafka::Message;
using cppkafka::Configuration;
using cppkafka::Metadata;
using cppkafka::TopicMetadata;
using cppkafka::TopicPartition;
using cppkafka::TopicPartitionList;

namespace pirulo {

PIRULO_CREATE_LOGGER("p.topics");

TopicOffsetReader::TopicOffsetReader(StorePtr store, size_t thread_count, Configuration config)
: consumer_(move(config)),store_(move(store)), thread_pool_(thread_count) {

}

void TopicOffsetReader::run() {
    while (running_) {
        process();

        sleep_for(seconds(10));
    }
}

void TopicOffsetReader::stop() {
    running_ = false;
    thread_pool_.stop();
}

TopicOffsetReader::StorePtr TopicOffsetReader::get_store() const {
    return store_;
}

void TopicOffsetReader::process() {
    LOG4CXX_INFO(logger, "Loading topics metadata");
    TopicPartitionCount topics;

    try {
        topics = load_metadata();
    }
    catch (const cppkafka::Exception& ex) {
        LOG4CXX_ERROR(logger, "Failed to fetch topic metadata: " << ex.what());
        return;
    }

    LOG4CXX_INFO(logger, "Fetching offsets for " << topics.size() << " topics");
    for (const auto& topic_pair : topics) {
        const string& topic = topic_pair.first;
        const size_t partition_count = topic_pair.second;
        for (size_t i = 0; i < partition_count; ++i) {
            thread_pool_.add_task([&, topic, i] {
                process_topic_partition({ topic, static_cast<int>(i) });
            });
        }
    }
    thread_pool_.wait_for_tasks();
    LOG4CXX_INFO(logger, "Finished fetching topic offsets");
}

TopicOffsetReader::TopicPartitionCount TopicOffsetReader::load_metadata() {
    // Load all existing topic names and store them atomically
    Metadata md = consumer_.get_metadata();

    TopicPartitionCount topics;
    for (const TopicMetadata& topic_metadata : md.get_topics()) {
        topics.emplace(topic_metadata.get_name(),
                       topic_metadata.get_partitions().size());
    }
    return topics;
}

void TopicOffsetReader::process_topic_partition(const TopicPartition& topic_partition) {
    LOG4CXX_TRACE(logger, "Fetching offset for " << topic_partition);
    uint64_t offset;
    try {
        tie(ignore, offset) = consumer_.query_offsets(topic_partition);
        store_->store_topic_offset(topic_partition.get_topic(), topic_partition.get_partition(),
                                   offset);
    }
    catch (const cppkafka::Exception& ex) {
        LOG4CXX_ERROR(logger, "Failed to fetch offsets for " << topic_partition
                      << ": " << ex.what());
    }
}

} // pirulo
