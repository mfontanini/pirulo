#pragma once

#include <string>
#include <log4cxx/logger.h>

#define PIRULO_CREATE_LOGGER(name) static const auto logger = log4cxx::Logger::getLogger(name)

namespace pirulo {
namespace logging {

void register_console_logger(const std::string& name = "",
                             const std::string& log_level = "DEBUG");

} // logging
} // pirulo
