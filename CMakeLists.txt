cmake_minimum_required(VERSION 3.23)

project(
    spider
    LANGUAGES
        C
        CXX
    VERSION 0.1.0
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(SPIDER_BUILD_TESTING "Build Spider tests" ON)

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

# Set the project dependency file path
set(SPIDER_CMAKE_DEPENDENCY_FILE "" CACHE STRING "The cmake dependency file to include.")
if(NOT SPIDER_CMAKE_DEPENDENCY_FILE STREQUAL "")
    include("${SPIDER_CMAKE_DEPENDENCY_FILE}")
endif()

# Turn on PIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(SPIDER_BUILD_TESTING)
    include(CTest)
    set(SPIDER_ENABLE_TESTS ON)
endif()

# Find and setup antlr4-runtime
find_package(antlr4-runtime REQUIRED)
message(STATUS "Found antlr4-runtime ${antlr4-runtime_VERSION}")

# Find and setup Boost
find_package(Boost 1.81 REQUIRED COMPONENTS headers)
message(STATUS "Found Boost ${Boost_VERSION}")

# Find and setup fmt
find_package(fmt 8.0.1 REQUIRED)
message(STATUS "Found fmt ${fmt_VERSION}")

if(SPIDER_ENABLE_TESTS)
    find_package(Catch2 3.8.0 REQUIRED)
    message(STATUS "Found Catch2 ${Catch2_VERSION}.")
endif()

# Add abseil
set(ABSL_PROPAGATE_CXX_STD ON)
find_package(absl REQUIRED)
message(STATUS "Found abseil ${absl_VERSION}.")

# Add ystdlib-cpp
find_package(ystdlib REQUIRED)
message(STATUS "Found ystdlib ${ystdlib_VERSION}.")

add_subdirectory(src/spider)

if(SPIDER_ENABLE_TESTS)
    add_subdirectory(tests/tdl)
endif()
