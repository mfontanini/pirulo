#pragma once

#include <memory>
#include <set>
#include <cppkafka/consumer.h>
#include <cppkafka/utils/consumer_dispatcher.h>
#include "offset_store.h"
#include "utils/observer.h"

namespace pirulo {

class ConsumerOffsetReader {
public:
    using StorePtr = std::shared_ptr<OffsetStore>;
    using EofCallback = std::function<void()>;
    using TopicCommitCallback = std::function<void(const std::string&, int)>;

    ConsumerOffsetReader(StorePtr store, std::chrono::milliseconds consumer_offset_cool_down,
                         cppkafka::Configuration config);

    void run(const EofCallback& callback);
    void stop();

    void watch_commits(const std::string& topic, int partition, TopicCommitCallback callback);

    StorePtr get_store() const;
private:
    void handle_message(cppkafka::Message msg);

    StorePtr store_;
    cppkafka::Consumer consumer_;
    cppkafka::ConsumerDispatcher dispatcher_{consumer_};
    Observer<cppkafka::TopicPartition> observer_;
    std::set<int> pending_partitions_;
    bool notifications_enabled_{false};
};

} // pirulo
