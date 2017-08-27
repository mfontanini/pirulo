#pragma once

#include <tuple>
#include <map>
#include <unordered_map>
#include "python/handler.h"

namespace pirulo {
namespace api {

class LagTrackerHandler : public Handler {
public:
    using Handler::Handler;
protected:
    virtual void handle_lag_update(const std::string& topic, int partition,
                                   const std::string& group_id, uint64_t consumer_lag) { }

    void handle_initialize() override;
    void handle_new_consumer(const std::string& group_id) override;
    void handle_new_topic(const std::string& topic) override;
    void handle_consumer_commit(const std::string& group_id, const std::string& topic,
                                int partition, int64_t offset) override;
    void handle_topic_message(const std::string& topic, int partition,
                              int64_t offset) override;
private:
    struct TopicPartitionInfo {
        int64_t offset{-1};
        std::unordered_map<std::string, int64_t> consumer_offsets;
    };
    using TopicPartitionId = std::tuple<std::string, int>;
    using TopicPartitionInfoMap = std::map<TopicPartitionId, TopicPartitionInfo>;

    TopicPartitionInfoMap topic_partition_info_;
};

} // api
} // pirulo
