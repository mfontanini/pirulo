#pragma once

#include <string>
#include <log4cxx/logger.h>
#include <boost/python/errors.hpp>
#include <pystate.h>

namespace pirulo {
namespace helpers {

class GILAcquirer {
public:
    GILAcquirer();
    ~GILAcquirer();

    GILAcquirer(const GILAcquirer&) = delete;
    GILAcquirer& operator=(const GILAcquirer&) = delete;
private:
    PyGILState_STATE gstate_;
};

std::string format_python_exception();
void initialize_python();

template <typename Functor>
void safe_exec(const log4cxx::LoggerPtr& logger, const Functor& python_code) {
    GILAcquirer _;
    try {
        python_code();
    }
    catch (const boost::python::error_already_set& ex) {
        LOG4CXX_ERROR(logger, "Error executing python callback: "
                      << format_python_exception());
    }
}

} // helpers
} // pirulo
