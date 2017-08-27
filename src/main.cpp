#include <iostream>
#include <thread>
#include <stdexcept>
#include <csignal>
#include <boost/program_options.hpp>
#include "application.h"
#include "python/plugin.h"
#include "detail/logging.h"

using std::cout;
using std::endl;
using std::move;
using std::string;
using std::make_shared;
using std::thread;
using std::cin;
using std::exception;
using std::unique_ptr;
using std::function;

using std::chrono::seconds;

using cppkafka::Configuration;

using pirulo::Application;
using pirulo::ConsumerOffsetReader;
using pirulo::TopicOffsetReader;
using pirulo::OffsetStore;
using pirulo::api::PythonPlugin;
using pirulo::logging::register_console_logger;

namespace po = boost::program_options;

function<void()> signal_handler;

void on_signal(int) {
    signal_handler();
}

int main(int argc, char* argv[]) {
    string brokers;
    string group_id;

    po::options_description options("Options");
    options.add_options()
        ("help,h",       "produce this help message")
        ("brokers,b",    po::value<string>(&brokers)->required(), 
                         "the kafka broker list")
        ;

    po::variables_map vm;

    try {
        po::store(po::command_line_parser(argc, argv).options(options).run(), vm);
        po::notify(vm);
    }
    catch (const exception& ex) {
        cout << "Error parsing options: " << ex.what() << endl;
        cout << endl;
        cout << options << endl;
        return 1;
    }

    register_console_logger("", "TRACE");

    // Construct the configuration
    Configuration config = {
        { "metadata.broker.list", brokers },
        // Disable auto commit
        { "enable.auto.commit", false }
    };

    auto store = make_shared<OffsetStore>();
    auto consumer_reader = make_shared<ConsumerOffsetReader>(store, seconds(10), config);
    auto topic_reader = make_shared<TopicOffsetReader>(store, 2, consumer_reader, config);

    Application app(move(topic_reader), move(consumer_reader));
    // app.add_plugin(unique_ptr<PythonPlugin>(new PythonPlugin("../plugins",
    //                                                         "../plugins/logger.py")));
    app.add_plugin(unique_ptr<PythonPlugin>(new PythonPlugin("../plugins",
                                                             "../plugins/lag_exporter.py")));

    signal_handler = [&] {
        app.stop();
    };

    signal(SIGINT, &on_signal);
    signal(SIGTERM, &on_signal);
    signal(SIGQUIT, &on_signal);

    app.run();
}
