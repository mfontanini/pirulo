set(SOURCES
    consumer_offset_store.cpp
    consumer_offset_reader.cpp

    detail/logging.cpp
)

include_directories(${PROJECT_SOURCE_DIR}/include)
add_library(pirulo-core ${SOURCES})
add_subdirectory(executables)
