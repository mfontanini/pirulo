set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/utils)

add_custom_target(executables)

macro(create_executable executable_name)
    add_executable(${executable_name} EXCLUDE_FROM_ALL "${executable_name}.cpp")
    add_dependencies(executables ${executable_name})
    target_link_libraries(${executable_name} pirulo-core)
endmacro()

include_directories(${PROJECT_SOURCE_DIR}/include)
create_executable(consumer_offsets)
create_executable(topic_offsets)
