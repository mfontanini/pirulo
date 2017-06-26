#include <iostream>
#include <thread>
#include <stdexcept>
#include <boost/program_options.hpp>
#include "topic_offset_reader.h"
#include "detail/logging.h"

using std::cout;
using std::endl;
using std::move;
using std::string;
using std::make_shared;
using std::thread;
using std::cin;
using std::exception;

using boost::optional;

using cppkafka::Configuration;
using cppkafka::TopicPartition;

using pirulo::TopicOffsetReader;
using pirulo::OffsetStore;
using pirulo::logging::register_console_logger;

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    string brokers;
    string group_id;

    po::options_description options("Options");
    options.add_options()
        ("help,h",       "produce this help message")
        ("brokers,b",    po::value<string>(&brokers)->required(), 
                         "the kafka broker list")
        ("group-id,g",   po::value<string>(&group_id),
                         "the consumer group id to be used")
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

    register_console_logger();

    // Construct the configuration
    Configuration config = {
        { "metadata.broker.list", brokers },
        { "group.id", group_id },
        // Disable auto commit
        { "enable.auto.commit", false },
    };

    auto store = make_shared<OffsetStore>();
    TopicOffsetReader reader(store, 2, move(config));
    thread th([&]() {
        reader.run();
    });

    string topic;
    int partition;
    while (cin >> topic >> partition) {
        optional<int64_t> offset = store->get_topic_offset(topic, partition);
        if (!offset) {
            cout << "Topic/partition not found\n";
        }
        else {
            cout << "Offset for " << TopicPartition(topic, partition) << ": " << *offset << endl;
        }
    }

    reader.stop();
    th.join();
}