set(SOURCES
    offset_store.cpp
    consumer_offset_reader.cpp
    topic_offset_reader.cpp

    utils/thread_pool.cpp

    detail/logging.cpp
)

include_directories(${PROJECT_SOURCE_DIR}/include)
add_library(pirulo-core ${SOURCES})
target_link_libraries(pirulo-core log4cxx cppkafka rdkafka boost_program_options pthread)
