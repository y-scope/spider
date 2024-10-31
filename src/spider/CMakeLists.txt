# set variable as CACHE INTERNAL to access it from other scope
set(SPIDER_CORE_SOURCES
    core/Error.hpp
    core/Data.hpp
    core/Task.hpp
    core/TaskGraph.hpp
    storage/MetadataStorage.hpp
    storage/DataStorage.hpp
    storage/MysqlStorage.cpp
    storage/MysqlStorage.hpp
    CACHE INTERNAL
    "spider core source files"
)

if(SPIDER_USE_STATIC_LIBS)
    add_library(spider_core STATIC)
else()
    add_library(spider_core SHARED)
endif()
target_sources(spider_core PRIVATE ${SPIDER_CORE_SOURCES})
target_link_libraries(spider_core PUBLIC Boost::boost PRIVATE absl::flat_hash_map)

set(SPIDER_WORKER_SOURCES worker/worker.cpp CACHE INTERNAL "spider worker source files")

add_executable(spider_worker)
target_sources(spider_worker PRIVATE ${SPIDER_WORKER_SOURCES})
target_link_libraries(spider_worker PRIVATE spider_core)
add_executable(spider::worker ALIAS spider_worker)

set(SPIDER_CLIENT_SOURCES
    client/Task.cpp
    client/Future.cpp
    client/Data.cpp
    CACHE INTERNAL
    "spider client source files"
)

set(SPIDER_CLIENT_HEADERS
    client/Spider.hpp
    client/Task.hpp
    client/Error.hpp
    client/Future.hpp
    client/Data.hpp
    CACHE INTERNAL
    "spider client header files"
)

add_library(spider_client)
target_sources(
    spider_client
    PRIVATE
        ${SPIDER_CLIENT_SOURCES}
        ${SPIDER_CLIENT_HEADERS}
)
target_link_libraries(spider_client PRIVATE spider_core)
add_library(spider::spider ALIAS spider_client)
