# CMake 3.22.1 is the default on Ubuntu 22.04
cmake_minimum_required(VERSION 3.22.1)

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

# Add local CMake module directory to CMake's modules path
set(CMAKE_MODULE_PATH
    ${CMAKE_MODULE_PATH}
    "${PROJECT_SOURCE_DIR}/cmake/Modules/"
)

# Macro providing the length of the absolute source directory path so we can
# create a relative (rather than absolute) __FILE__ macro
string(LENGTH "${PROJECT_SOURCE_DIR}/" SOURCE_PATH_SIZE)
add_definitions("-DSOURCE_PATH_SIZE=${SOURCE_PATH_SIZE}")

# Profiling options
add_definitions(-DPROF_ENABLED=0)

# Compile-in debug logging statements
#add_definitions(-DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)

# Flush to disk switch
add_definitions(-DFLUSH_TO_DISK_ENABLED=1)

# Make off_t 64-bit
add_definitions(-D_FILE_OFFSET_BITS=64)

# Turn on PIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Detect linking mode (static or shared); Default to static.
set(SPIDER_USE_STATIC_LIBS ON CACHE BOOL "Whether to link against static libraries")
if(SPIDER_USE_STATIC_LIBS)
    if(APPLE)
        set(SPIDER_STATIC_LIBS_UNSUPPORTED_PLATFORM "macOS")
    elseif(EXISTS "/etc/centos-release")
        set(SPIDER_STATIC_LIBS_UNSUPPORTED_PLATFORM "CentOS")
    endif()

    if(DEFINED SPIDER_STATIC_LIBS_UNSUPPORTED_PLATFORM)
        message(
            AUTHOR_WARNING
            "Building with static libraries is unsupported on"
            " ${SPIDER_STATIC_LIBS_UNSUPPORTED_PLATFORM}. Switching to shared libraries."
        )
        set(SPIDER_USE_STATIC_LIBS OFF)
    endif()
endif()
if(SPIDER_USE_STATIC_LIBS)
    set(SPIDER_LIBS_STRING "static")
else()
    set(SPIDER_LIBS_STRING "shared")
endif()
message(STATUS "Building using ${SPIDER_LIBS_STRING} libraries")

if(PROJECT_IS_TOP_LEVEL)
    # Include dependency settings if the project isn't being included as a subproject.
    include("build/deps/cmake-settings/all.cmake")

    # If previously undefined, `BUILD_TESTING` will be set to ON.
    include(CTest)
endif()

if(BUILD_TESTING AND SPIDER_BUILD_TESTING)
    set(SPIDER_ENABLE_TESTS ON)
endif()

# Find and setup Boost Library
if(SPIDER_USE_STATIC_LIBS)
    set(Boost_USE_STATIC_LIBS ON)
endif()
find_package(
    Boost
    1.74
    REQUIRED
    COMPONENTS
        headers
        process
        program_options
        filesystem
        system
)
if(Boost_FOUND)
    message(STATUS "Found Boost ${Boost_VERSION}")
else()
    message(FATAL_ERROR "Could not find ${SPIDER_LIBS_STRING} libraries for Boost")
endif()

# Find and setup fmt
find_package(fmt 8.0.1 REQUIRED)
if(fmt_FOUND)
    message(STATUS "Found fmt ${fmt_VERSION}")
else()
    message(FATAL_ERROR "Could not find static libraries for fmt")
endif()

# Find and setup spdlog
if(SPIDER_USE_STATIC_LIBS)
    # NOTE: On some Linux distributions (e.g. Ubuntu), the spdlog package only contains a dynamic
    # library. If the `find_package(spdlog)` call below fails, re-run
    # `tools/scripts/lib_install/<dist_name>/install-packages-from-source.sh` to build spdlog from
    # source.
    set(spdlog_USE_STATIC_LIBS ON)
endif()
set(SPDLOG_FMT_EXTERNAL ON)
add_compile_definitions(SPDLOG_FMT_EXTERNAL)
find_package(spdlog 1.9.2 REQUIRED)
if(spdlog_FOUND)
    message(STATUS "Found spdlog ${spdlog_VERSION}")
else()
    if(SPIDER_USE_STATIC_LIBS)
        message(FATAL_ERROR "Could not find static libraries for spdlog.")
    else()
        message(FATAL_ERROR "Could not find libraries for spdlog.")
    endif()
endif()

# Find and setup MariaDBClient library
if(SPIDER_USE_STATIC_LIBS)
    # NOTE: We can't statically link to MariaDBClient since it's GPL
    message(AUTHOR_WARNING "MariaDBClient cannot be statically linked due to its license.")
endif()
find_package(MariaDBClientCpp 1.0 REQUIRED)
if(MariaDBClientCpp_FOUND)
    message(STATUS "Found MariaDBClientCpp ${MariaDBClientCpp_VERSION}")
else()
    message(FATAL_ERROR "Could not find ${SPIDER_LIBS_STRING} libraries for MariaDBClientCpp")
endif()

# Find and setup msgpack
if(SPIDER_USE_STATIC_LIBS)
    set(msgpack-cxx_USE_STATIC_LIBS ON)
endif()
find_package(msgpack-cxx 7.0.0 REQUIRED)
if(msgpack-cxx_FOUND)
    message(STATUS "Found msgpack-cxx ${msgpack-cxx_VERSION}")
else()
    message(FATAL_ERROR "Could not find msgpack-cxx")
endif()

if(SPIDER_ENABLE_TESTS)
    find_package(Catch2 3.8.0 REQUIRED)
    message(STATUS "Found Catch2 ${Catch2_VERSION}.")
endif()

# Add abseil
set(ABSL_PROPAGATE_CXX_STD ON)
find_package(absl REQUIRED)
if(absl_FOUND)
    message(STATUS "Found abseil ${absl_VERSION}.")
else()
    message(FATAL_ERROR "Could not find libraries for abseil.")
endif()

# Add ystdlib-cpp
set(YSTDLIB_CPP_BUILD_TESTING OFF)
add_subdirectory(
    "${SPIDER_YSTDLIB_SOURCE_DIRECTORY}"
    "${CMAKE_BINARY_DIR}/ystdlib"
    EXCLUDE_FROM_ALL
)

find_package(Threads REQUIRED)

add_subdirectory(src/spider)

if(SPIDER_ENABLE_TESTS)
    add_subdirectory(tests)
endif()
