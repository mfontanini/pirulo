#pragma once

#include <memory>
#include <set>
#include <cppkafka/consumer.h>
#include <cppkafka/utils/consumer_dispatcher.h>
#include "consumer_offset_store.h"

namespace pirulo {

class ConsumerOffsetReader {
public:
    using StorePtr = std::shared_ptr<ConsumerOffsetStore>;
    using EofCallback = std::function<void()>;

    ConsumerOffsetReader(StorePtr store, cppkafka::Configuration config);

    void run(const EofCallback& callback);
    void stop();
private:
    void handle_message(cppkafka::Message msg);

    StorePtr store_;
    cppkafka::Consumer consumer_;
    cppkafka::ConsumerDispatcher dispatcher{consumer_};
    std::set<int> pending_partitions_;
};

} // pirulo
