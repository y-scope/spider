cmake_minimum_required(VERSION 3.22.1)
project(spider_getting_started)

include("../../build/deps/cmake-settings/all.cmake")
set(CMAKE_MODULE_PATH "../../cmake/Modules")
set(SPIDER_YSTDLIB_SOURCE_DIRECTORY "../../build/deps/ystdlib/ystdlib-src")
find_package(spider REQUIRED)

# Add the tasks library
add_library(
    tasks
    SHARED
    src/tasks.cpp
    src/tasks.hpp
)

# Link the Spider library to the tasks library
target_link_libraries(tasks PRIVATE spider::spider_client)

# Add the client
add_executable(client src/client.cpp)

# Link the Spider and tasks library to the client
target_link_libraries(
    client
    PRIVATE
        spider::spider_client
        tasks
)