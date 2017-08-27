#pragma once

#include "offset_store.h"

namespace pirulo {
namespace api {

class Handler {
public:
    virtual ~Handler() = default;

    void initialize(const std::shared_ptr<OffsetStore>& store);
    void subscribe_to_consumers();
    void subscribe_to_consumer_commits();
    void subscribe_to_topics();
    void subscribe_to_topic_message();
    const std::shared_ptr<OffsetStore>& get_offset_store() const;
protected:
    virtual void handle_initialize(); 
    virtual void handle_new_consumer(const std::string& group_id);
    virtual void handle_new_topic(const std::string& topic);
    virtual void handle_consumer_commit(const std::string& group_id, const std::string& topic,
                                        int partition, int64_t offset);
    virtual void handle_topic_message(const std::string& topic, int partition, int64_t offset);
private:
    void on_new_consumer(const std::string& group_id);
    void on_new_topic(const std::string& topic);
    void on_consumer_commit(const std::string& group_id, const std::string& topic,
                            int partition, int64_t offset);
    void on_topic_message(const std::string& topic, int partition, int64_t offset);
    void consumer_subscribe(const std::string& group_id);
    void topic_subscribe(const std::string& topic);

    std::shared_ptr<OffsetStore> offset_store_;
    bool track_consumer_commits_{false};
    bool track_topic_messages_{false};
};

} // api
} // pirulo
