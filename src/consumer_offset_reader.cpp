#include <cstdint>
#include "consumer_offset_reader.h"
#include "exceptions.h"
#include "detail/memory.h"
#include "detail/logging.h"

using std::move;
using std::string;

using cppkafka::Configuration;
using cppkafka::ConsumerDispatcher;
using cppkafka::Message;

namespace pirulo {

PIRULO_CREATE_LOGGER("p.offsets");

ConsumerOffsetReader::ConsumerOffsetReader(StorePtr store, Configuration config) :
    store_(move(store)), consumer_(move(config)) {

}

void ConsumerOffsetReader::run() {
    consumer_.subscribe({ "__consumer_offsets" });
    dispatcher.run(
        [&](Message msg) {
            try {
                handle_message(move(msg));
            }
            catch (const ParseException&) {
                LOG4CXX_WARN(logger, "Failed to parse consumer offset record");
            }
        }
    );
}

void ConsumerOffsetReader::stop() {
    dispatcher.stop();
}

void ConsumerOffsetReader::handle_message(Message msg) {
    InputMemoryStream key_input(msg.get_key());
    uint16_t version = key_input.read_be<uint16_t>();
    if (version > 1) {
        LOG4CXX_TRACE(logger, "Ignoring key message version: " << version);
        return;
    }
    string group_id = key_input.read<string>();
    string topic = key_input.read<string>();
    uint32_t partition = key_input.read_be<uint32_t>();

    InputMemoryStream value_input(msg.get_payload());
    // Value version
    version = value_input.read_be<uint16_t>();
    if (version > 1) {
        throw ParseException();
    }
    uint64_t offset = value_input.read_be<uint64_t>();
    store_->store(group_id, topic, partition, offset);
}

} // pirulo
