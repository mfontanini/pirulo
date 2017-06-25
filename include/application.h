#pragma once

#include <memory>
#include <vector>
#include "topic_offset_reader.h"
#include "consumer_offset_reader.h"
#include "plugin_base.h"

namespace pirulo {

class Application {
public:
    using TopicOffsetReaderPtr = std::unique_ptr<TopicOffsetReader>;
    using ConsumerOffsetReaderPtr = std::unique_ptr<ConsumerOffsetReader>;
    using PluginPtr = std::unique_ptr<PluginBase>;

    Application(TopicOffsetReaderPtr topic_reader, ConsumerOffsetReaderPtr consumer_reader);

    void run();
    void stop();

    void add_plugin(PluginPtr plugin);
private:
    void process();

    TopicOffsetReaderPtr topic_reader_;
    ConsumerOffsetReaderPtr consumer_reader_;
    std::vector<PluginPtr> plugins_;
};

} // pirulo
