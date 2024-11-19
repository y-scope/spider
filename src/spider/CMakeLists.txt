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
        Boost::headers
        absl::flat_hash_map
        MariaDBClientCpp::MariaDBClientCpp
        msgpack-cxx
)
target_link_libraries(spider_core PRIVATE fmt::fmt)

set(SPIDER_WORKER_SOURCES
    worker/worker.cpp
    worker/DllLoader.hpp
    worker/DllLoader.cpp
    CACHE INTERNAL
    "spider worker source files"
)

add_executable(spider_worker)
target_sources(spider_worker PRIVATE ${SPIDER_WORKER_SOURCES})
target_link_libraries(spider_worker PRIVATE spider_core)
target_link_libraries(
    spider_worker
    PRIVATE
        Boost::system
        Boost::program_options
        spdlog::spdlog
)

set(SPIDER_TASK_EXECUTOR_SOURCES
    worker/DllLoader.cpp
    worker/DllLoader.hpp
    worker/TaskExecutor.cpp
    CACHE INTERNAL
    "spider task executor source filees"
)

add_executable(spider_task_executor)
target_sources(spider_task_executor PRIVATE ${SPIDER_TASK_EXECUTOR_SOURCES})
target_link_libraries(spider_task_executor PRIVATE spider_core)
target_link_libraries(
    spider_task_executor
    PRIVATE
        Boost::system
        Boost::program_options
        spdlog::spdlog
)
