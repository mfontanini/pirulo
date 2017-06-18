#pragma once

#include <memory>
#include <cppkafka/consumer.h>
#include <cppkafka/utils/consumer_dispatcher.h>
#include "consumer_offset_store.h"

namespace pirulo {

class ConsumerOffsetReader {
public:
    using StorePtr = std::shared_ptr<ConsumerOffsetStore>;

    ConsumerOffsetReader(StorePtr store, cppkafka::Configuration config);

    void run();
    void stop();
private:
    void handle_message(cppkafka::Message msg);

    StorePtr store_;
    cppkafka::Consumer consumer_;
    cppkafka::ConsumerDispatcher dispatcher{consumer_};
};

} // pirulo
