# CMake 3.22.1 is the default on Ubuntu 22.04
cmake_minimum_required(VERSION 3.22.1)

project(
    spider
    LANGUAGES
        C
        CXX
    VERSION 0.1.0
)

# Enable exporting compile commands
set(CMAKE_EXPORT_COMPILE_COMMANDS
    ON
    CACHE BOOL
    "Enable/Disable output of compile commands during generation."
    FORCE
)

# Set the default build type to Release if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(SPIDER_DEFAULT_BUILD_TYPE "Release")
    message(STATUS "No build type specified. Setting to '${SPIDER_DEFAULT_BUILD_TYPE}'.")
    set(CMAKE_BUILD_TYPE
        "${SPIDER_DEFAULT_BUILD_TYPE}"
        CACHE STRING
        "Choose the type of build."
        FORCE
    )
endif()

add_executable(spider)
target_compile_features(spider PRIVATE cxx_std_20)

set(SPIDER_SOURCES src/spider/spider.cpp)
target_sources(spider PRIVATE ${SPIDER_SOURCES})

target_include_directories(spider PRIVATE src/)
