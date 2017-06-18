// Code taken from libtins: https://github.com/mfontanini/libtins/blob/master/include/tins/memory_helpers.h

#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <cppkafka/buffer.h>
#include "../exceptions.h"
#include "endianness.h"

namespace pirulo {

inline void read_data(const uint8_t* buffer, uint8_t* output_buffer, size_t size) {
    std::memcpy(output_buffer, buffer, size);
}

template <typename T>
void read_value(const uint8_t* buffer, T& value) {
    std::memcpy(&value, buffer, sizeof(value));
}

class InputMemoryStream {
public:
    InputMemoryStream(const uint8_t* buffer, size_t total_sz)
    : buffer_(buffer), size_(total_sz) {
    }

    InputMemoryStream(const cppkafka::Buffer& data)
    : buffer_(data.get_data()), size_(data.get_size()) {
    }
 
    template <typename T>
    T read() {
        T output;
        read(output);
        return output;
    }

    template <typename T>
    T read_le() {
        return endian::le_to_host(read<T>());
    }

    template <typename T>
    T read_be() {
        return endian::be_to_host(read<T>());
    }

    template <typename T>
    void read(T& value) {
        if (!can_read(sizeof(value))) {
            throw ParseException();
        }
        read_value(buffer_, value);
        skip(sizeof(value));
    }

    void read(std::string& value) {
        uint16_t length = read_be<uint16_t>();
        if (!can_read(length)) {
            throw ParseException();
        }
        value.assign(pointer(), pointer() + length);
        skip(length);
    }

    void skip(size_t size) {
        if (size > size_) {
            throw ParseException();
        }
        buffer_ += size;
        size_ -= size;
    }

    bool can_read(size_t byte_count) const {
        return size_ >= byte_count;
    }

    void read(void* output_buffer, size_t output_buffer_size) {
        if (!can_read(output_buffer_size)) {
            throw ParseException();
        }
        read_data(buffer_, (uint8_t*)output_buffer, output_buffer_size);
        skip(output_buffer_size);
    }

    const uint8_t* pointer() const {
        return buffer_;
    }

    size_t size() const {
        return size_;
    }

    void size(size_t new_size) {
        size_ = new_size;
    }

    operator bool() const {
        return size_ > 0;
    }
private:
    const uint8_t* buffer_;
    size_t size_;
};

} // pirulo
