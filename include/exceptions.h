#pragma once

#include <stdexcept>

namespace pirulo {

class Exception : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class ParseException : public Exception {
public:
    using Exception::Exception;

    ParseException() : Exception("Parsing record failed") {

    }
};

} // pirulo
