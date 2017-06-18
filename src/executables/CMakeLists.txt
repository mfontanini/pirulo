set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/utils)

add_executable(consumer_offsets consumer_offsets.cpp)
target_link_libraries(consumer_offsets
                      pirulo-core log4cxx cppkafka rdkafka boost_program_options pthread)