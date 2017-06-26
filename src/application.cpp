#include <thread>
#include <vector>
#include <cassert>
#include "application.h"

using std::thread;
using std::vector;

namespace pirulo {

Application::Application(TopicOffsetReaderPtr topic_reader,
                         ConsumerOffsetReaderPtr consumer_reader)
: topic_reader_(move(topic_reader)), consumer_reader_(move(consumer_reader)) {
    // Ensure everything uses the same store
    assert(topic_reader_->get_store() == consumer_reader_->get_store());
}

void Application::run() {
    auto on_consumer_offset_eof = [&] {
        // When we finish loading the __consumer_offsets topic, launch all plugins
        const auto store = consumer_reader_->get_store();
        for (auto& plugin_ptr : plugins_) {
            plugin_ptr->launch(store);
        }
    };

    // Start topic and consumer offset consumption
    vector<thread> threads;
    threads.emplace_back([&] {
        topic_reader_->run();
    });
    threads.emplace_back([&] {
        consumer_reader_->run(on_consumer_offset_eof);
    });

    // wut
    process();

    for (thread& th : threads) {
        th.join();
    }
}

void Application::stop() {
    topic_reader_->stop();
    consumer_reader_->stop();
}

void Application::add_plugin(PluginPtr plugin) {
    plugins_.emplace_back(move(plugin));
}

void Application::process() {

}

} // pirulo
