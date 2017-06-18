#include <log4cxx/patternlayout.h>
#include <log4cxx/consoleappender.h>
#include "detail/logging.h"

using std::string;

using log4cxx::PatternLayout;
using log4cxx::ConsoleAppender;
using log4cxx::Level;
using log4cxx::Logger;

namespace pirulo {
namespace logging {

void register_console_logger(const string& name, const string& log_level) {
    auto layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss.SSS}{GMT} [%c{2}] - %m%n");
    auto appender = new ConsoleAppender(layout);

    auto logger = Logger::getRootLogger();
    logger->setLevel(Level::toLevel(log_level));
    logger->addAppender(appender);
}

} // logging
} // pirulo
