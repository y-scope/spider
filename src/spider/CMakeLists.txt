# set variable as CACHE INTERNAL to access it from other scope
set(SPIDER_CORE_SOURCES storage/MysqlStorage.cpp CACHE INTERNAL "spider core source files")

set(SPIDER_CORE_HEADERS
    core/Error.hpp
    core/Data.hpp
    core/Task.hpp
    core/TaskGraph.hpp
    core/Serializer.hpp
    storage/MetadataStorage.hpp
    storage/DataStorage.hpp
    storage/MysqlStorage.hpp
    CACHE INTERNAL
    "spider core header files"
)

add_library(spider_core)
target_sources(spider_core PRIVATE ${SPIDER_CORE_SOURCES})
target_sources(spider_core PUBLIC ${SPIDER_CORE_HEADERS})
target_link_libraries(
    spider_core
    PUBLIC
        Boost::boost
        absl::flat_hash_map
        MariaDBClientCpp::MariaDBClientCpp
        msgpack-cxx
)
target_link_libraries(spider_core PRIVATE fmt::fmt)
# Manually add include directories for msgpack
get_target_property(msgpack_include_dirs msgpack-cxx INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(msgpack_system_include_dirs msgpack-cxx INTERFACE_SYSTEM_INCLUDE_DIRECTORIES)
foreach(msgpack_include_dir ${msgpack_include_dirs})
    set(msgpack_header_dir "${msgpack_include_dir}/msgpack/v1")
    if(EXISTS "${msgpack_header_dir}" AND IS_DIRECTORY "${msgpack_header_dir}")
        list(APPEND msgpack_include_dirs "${msgpack_header_dir}")
    endif()
endforeach()
target_include_directories(spider_core SYSTEM PUBLIC ${msgpack_include_dirs})
target_link_libraries(spider_core PUBLIC msgpack-cxx)

set(SPIDER_WORKER_SOURCES
    worker/worker.cpp
    worker/FunctionManager.hpp
    CACHE INTERNAL
    "spider worker source files"
)

add_executable(spider_worker)
target_sources(spider_worker PRIVATE ${SPIDER_WORKER_SOURCES})
target_link_libraries(spider_worker PRIVATE spider_core)
add_executable(spider::worker ALIAS spider_worker)
