set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(SOURCES
    offset_store.cpp
    consumer_offset_reader.cpp
    topic_offset_reader.cpp

    utils/thread_pool.cpp

    detail/logging.cpp

    python/plugin.cpp

    plugin_base.cpp
    application.cpp
)

include_directories(${PROJECT_SOURCE_DIR}/include)
# TODO: clean this up
include_directories(SYSTEM /usr/include/python2.7/)

add_library(pirulo-core ${SOURCES})
target_link_libraries(pirulo-core log4cxx cppkafka rdkafka boost_program_options boost_python
                                  python2.7 pthread)

add_executable(pirulo main.cpp)
target_link_libraries(pirulo pirulo-core )
