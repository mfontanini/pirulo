#pragma once

#include <boost/python/wrapper.hpp>
#include "offset_store.h"

namespace pirulo {
namespace api {

class Handler : public boost::python::wrapper<Handler> {
public:
    virtual ~Handler() = default;

    void initialize(const std::shared_ptr<OffsetStore>& store);
    void subscribe_to_consumers();
    void subscribe_to_consumer_commits();
    void subscribe_to_topics();
    void subscribe_to_topic_message();
private:
    void on_new_consumer(const std::string& group_id);
    void on_new_topic(const std::string& topic);
    void on_consumer_commit(const std::string& group_id, const std::string& topic,
                            int partition, int64_t offset);
    void on_topic_message(const std::string& topic, int partition, int64_t offset);

    std::shared_ptr<OffsetStore> offset_store_;
    bool track_consumer_commits_{false};
    bool track_topic_messages_{false};
};

} // api
} // pirulo
