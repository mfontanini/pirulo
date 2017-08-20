#include <tuple>
#include <cppkafka/metadata.h>
#include "topic_offset_reader.h"
#include "detail/logging.h"
#include "utils/utils.h"

using std::string;
using std::move;
using std::unordered_set;
using std::set;
using std::lock_guard;
using std::mutex;
using std::tie;
using std::ignore;

using std::this_thread::sleep_for;

using std::chrono::seconds;

using cppkafka::Consumer;
using cppkafka::Message;
using cppkafka::Configuration;
using cppkafka::Metadata;
using cppkafka::TopicMetadata;
using cppkafka::TopicPartition;
using cppkafka::TopicPartitionList;

namespace pirulo {

PIRULO_CREATE_LOGGER("p.topics");

static Configuration prepare_config(Configuration config) {
    config.set("group.id", utils::generate_group_id());
    return config;
}

TopicOffsetReader::TopicOffsetReader(StorePtr store, size_t thread_count,
                                     ConsumerOffsetReaderPtr consumer_reader,
                                     Configuration config)
: consumer_pool_(thread_count, prepare_config(move(config))),store_(move(store)),
  thread_pool_(thread_count), consumer_offset_reader_(move(consumer_reader)) {

}

void TopicOffsetReader::run() {
    LOG4CXX_INFO(logger, "Performing topic offsets cold start");
    process_metadata([&](TopicPartitionCount topics) {
        async_process_topics(topics);
        monitor_topics(topics);
    });
    thread_pool_.wait_for_tasks();
    LOG4CXX_INFO(logger, "Finished topic offsets cold start");

    monitor_new_topics();
    while (running_) {
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

void TopicOffsetReader::async_process_topics(const TopicPartitionCount& topics) {
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
}

void TopicOffsetReader::monitor_topics(const TopicPartitionCount& topics) {
    // They're all new, just convert them to a TopicPartitionList
    monitor_topics(get_new_topic_partitions(topics));
}

void TopicOffsetReader::monitor_topics(const TopicPartitionList& topics) {
    for (const TopicPartition& topic_partition : topics) {
        auto task = [&, topic_partition] {
            process_topic_partition(topic_partition);
        };
        // Schedule a task to process it periodically
        auto task_id = task_scheduler_.add_task(move(task), maximum_topic_reload_time_);

        // Mark it as monitored
        monitored_topic_task_id_.emplace(topic_partition, task_id);

        // Watch for commits on this topic
        auto commit_callback = [&](const string& topic, int partition) {
            on_commit(topic, partition);
        };
        consumer_offset_reader_->watch_commits(topic_partition.get_topic(),
                                               topic_partition.get_partition(),
                                               move(commit_callback));
    }
}

void TopicOffsetReader::monitor_new_topics() {
    auto task = [&] {
        process_metadata([&](const TopicPartitionCount& counts) {
            const TopicPartitionList new_topics = get_new_topic_partitions(counts);
            if (!new_topics.empty()) {
                LOG4CXX_INFO(logger, "Found " << new_topics.size() << " new topic/partitions "
                             "to process");
                monitor_topics(new_topics);
            }
        });
    };
    task_scheduler_.add_task(move(task), maximum_metadata_reload_time_);
}

TopicPartitionList TopicOffsetReader::get_new_topic_partitions(const TopicPartitionCount& counts) {
    TopicPartitionList output;
    for (const auto& topic_count_pair : counts) {
        const string& topic = topic_count_pair.first;
        for (size_t i = 0; i < topic_count_pair.second; ++i) {
            TopicPartition topic_partition(topic, i);
            if (!monitored_topic_task_id_.count(topic_partition)) {
                output.emplace_back(move(topic_partition));
            }
        }
    }
    return output;
}

TopicOffsetReader::TopicPartitionCount TopicOffsetReader::load_metadata() {
    // Load all existing topic names
    TopicPartitionCount topics;
    consumer_pool_.acquire_consumer([&](Consumer& consumer) {
        Metadata md = consumer.get_metadata();
        for (const TopicMetadata& topic_metadata : md.get_topics()) {
            topics.emplace(topic_metadata.get_name(),
                           topic_metadata.get_partitions().size());
        }
    });
    return topics;
}

void TopicOffsetReader::process_metadata(const MetadataCallback& callback) {
    LOG4CXX_INFO(logger, "Loading topics metadata");

    try {
        TopicPartitionCount topics = load_metadata();
        LOG4CXX_INFO(logger, "Finished loading metadata, found " << topics.size() << " topics");
        callback(move(topics));
    }
    catch (const cppkafka::Exception& ex) {
        LOG4CXX_ERROR(logger, "Failed to fetch topic metadata: " << ex.what());
    }
}

void TopicOffsetReader::process_topic_partition(const TopicPartition& topic_partition) {
    LOG4CXX_TRACE(logger, "Fetching offset for " << topic_partition);
    uint64_t offset;
    try {
        consumer_pool_.acquire_consumer([&](Consumer& consumer) {
            tie(ignore, offset) = consumer.query_offsets(topic_partition);
            store_->store_topic_offset(topic_partition.get_topic(),
                                       topic_partition.get_partition(), offset);
        });
    }
    catch (const cppkafka::Exception& ex) {
        LOG4CXX_ERROR(logger, "Failed to fetch offsets for " << topic_partition
                      << ": " << ex.what());
    }
}

void TopicOffsetReader::on_commit(const string& topic, int partition) {
    TopicPartition topic_partition(topic, partition);
    LOG4CXX_TRACE(logger, "Bumping up priority of offset loading for " << topic_partition);
    // Increase the priority for this task
    auto task_id = monitored_topic_task_id_.at(topic_partition);
    task_scheduler_.set_priority(task_id, 0.0);
}

} // pirulo
