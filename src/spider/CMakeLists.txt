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
    worker/FunctionManager.hpp
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

set(SPIDER_CLIENT_SHARED_SOURCES
    client/Data.cpp
    client/Driver.cpp
    client/TaskContext.cpp
    client/TaskGraph.cpp
    CACHE INTERNAL
    "spider client shared source files"
)

set(SPIDER_CLIENT_SHARED_HEADERS
    client/Data.hpp
    client/Driver.hpp
    client/task.hpp
    client/TaskContext.hpp
    client/TaskGraph.hpp
    client/type_utils.hpp
    client/Exception.hpp
    CACHE INTERNAL
    "spider client shared header files"
)

add_library(spider_client_lib)
target_sources(spider_client_lib PRIVATE ${SPIDER_CLIENT_SHARED_SOURCES})
target_sources(spider_client_lib PUBLIC ${SPIDER_CLIENT_SHARED_HEADERS})
target_link_libraries(
    spider_client_lib
    PUBLIC
        Boost::boost
        absl::flat_hash_map
)

set(SPIDER_CLIENT_SOURCES client/Job.cpp CACHE INTERNAL "spider client source files")

set(SPIDER_CLIENT_HEADERS
    client/spider.hpp
    client/Job.hpp
    CACHE INTERNAL
    "spider client header files"
)

add_library(spider_client)
target_sources(spider_client PRIVATE ${SPIDER_CLIENT_SOURCES})
target_sources(spider_client PUBLIC ${SPIDER_CLIENT_HEADERS})
target_link_libraries(spider_client PRIVATE spider_core)
target_link_libraries(spider_client PUBLIC spider_client_lib)
add_library(spider::spider ALIAS spider_client)

set(SPIDER_WORKER_SOURCES worker/worker.cpp CACHE INTERNAL "spider worker source files")

add_executable(spider_worker)
target_sources(spider_worker PRIVATE ${SPIDER_WORKER_SOURCES})
target_link_libraries(
    spider_worker
    PRIVATE
        spider_core
        spider_client_lib
)
add_executable(spider::worker ALIAS spider_worker)
