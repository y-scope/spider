cmake_minimum_required(VERSION 3.22.1)
project(spider_joining_tasks)

# Add the Spider library
add_subdirectory(../../ spider EXCLUDE_FROM_ALL)

# Add the tasks library
add_library(
    tasks
    SHARED
    src/tasks.cpp
    src/tasks.hpp
)

# Link the Spider library to the tasks library
target_link_libraries(tasks PRIVATE spider::spider)

# Add the client
add_executable(client src/client.cpp)

# Link the Spider and tasks library to the client
target_link_libraries(
    client
    PRIVATE
        spider::spider
        tasks
)
