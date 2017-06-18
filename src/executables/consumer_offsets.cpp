#include <iostream>
#include <thread>
#include <stdexcept>
#include <boost/program_options.hpp>
#include "consumer_offset_reader.h"
#include "detail/logging.h"

using std::cout;
using std::endl;
using std::move;
using std::string;
using std::make_shared;
using std::thread;
using std::cin;
using std::exception;

using cppkafka::Configuration;

using pirulo::ConsumerOffsetReader;
using pirulo::ConsumerOffsetStore;
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
                         "the consumer group id to look for")
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
        { "enable.auto.commit", false }
    };
    config.set_default_topic_configuration({{ "auto.offset.reset", "smallest" }});

    auto store = make_shared<ConsumerOffsetStore>();
    ConsumerOffsetReader reader(store, move(config));
    thread th([&]() {
        reader.run();
    });

    string consumer_group;
    while (cin >> consumer_group) {
        auto offsets = store->get_offsets(consumer_group);
        if (offsets.empty()) {
            cout << "Consumer not found\n";
        }
        else {
            for (const auto& offset : offsets) {
                cout << offset.get_topic_partition() << ": "
                     << offset.get_topic_partition().get_offset() << endl;
            }
        }
    }

    reader.stop();
    th.join();
}