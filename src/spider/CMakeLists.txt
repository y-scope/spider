# set variable as CACHE INTERNAL to access it from other scope
set(SPIDER_CORE_SOURCES
    core/Task.cpp
    storage/mysql/MySqlConnection.cpp
    storage/mysql/MySqlStorageFactory.cpp
    storage/mysql/MySqlJobSubmissionBatch.cpp
    storage/mysql/MySqlStorage.cpp
    worker/FunctionManager.cpp
    worker/FunctionNameManager.cpp
    io/msgpack_message.cpp
    CACHE INTERNAL
    "spider core source files"
)

set(SPIDER_CORE_HEADERS
    core/Error.hpp
    core/Data.hpp
    core/Driver.hpp
    core/KeyValueData.hpp
    core/Task.hpp
    core/TaskGraph.hpp
    core/JobMetadata.hpp
    io/BoostAsio.hpp
    io/MsgPack.hpp
    io/msgpack_message.hpp
    io/Serializer.hpp
    utils/LruCache.hpp
    storage/MetadataStorage.hpp
    storage/DataStorage.hpp
    storage/StorageConnection.hpp
    storage/mysql/mysql_stmt.hpp
    storage/mysql/MySqlConnection.hpp
    storage/mysql/MySqlStorageFactory.hpp
    storage/mysql/MySqlStorage.hpp
    storage/mysql/MySqlJobSubmissionBatch.hpp
    storage/JobSubmissionBatch.hpp
    storage/StorageFactory.hpp
    worker/FunctionManager.hpp
    worker/FunctionNameManager.hpp
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
        spdlog::spdlog
        ystdlib::error_handling
)
target_link_libraries(spider_core PRIVATE fmt::fmt)

set(SPIDER_WORKER_SOURCES
    worker/DllLoader.hpp
    worker/DllLoader.cpp
    worker/WorkerErrorCode.cpp
    worker/WorkerErrorCode.hpp
    worker/Process.hpp
    worker/Process.cpp
    worker/TaskExecutor.hpp
    worker/TaskExecutor.cpp
    worker/TaskExecutorMessage.hpp
    worker/message_pipe.cpp
    worker/message_pipe.hpp
    worker/WorkerClient.hpp
    worker/WorkerClient.cpp
    utils/StopToken.hpp
    CACHE INTERNAL
    "spider worker source files"
)

set(SPIDER_TASK_EXECUTOR_SOURCES
    worker/DllLoader.cpp
    worker/DllLoader.hpp
    worker/message_pipe.cpp
    worker/message_pipe.hpp
    worker/task_executor.cpp
    CACHE INTERNAL
    "spider task executor source files"
)

add_executable(spider_task_executor)
target_sources(spider_task_executor PRIVATE ${SPIDER_TASK_EXECUTOR_SOURCES})
target_link_libraries(
    spider_task_executor
    PRIVATE
        spider_core
        spider_client
)
target_link_libraries(
    spider_task_executor
    PRIVATE
        Boost::filesystem
        Boost::program_options
        Boost::system
        ${CMAKE_DL_LIBS}
        fmt::fmt
        spdlog::spdlog
)

add_executable(spider_worker)
target_sources(spider_worker PRIVATE ${SPIDER_WORKER_SOURCES})
target_sources(spider_worker PRIVATE worker/worker.cpp)
target_link_libraries(spider_worker PRIVATE spider_core)
target_link_libraries(
    spider_worker
    PRIVATE
        Boost::headers
        Boost::filesystem
        Boost::process
        Boost::program_options
        Boost::system
        ${CMAKE_DL_LIBS}
        fmt::fmt
        spdlog::spdlog
)
add_dependencies(spider_worker spider_task_executor)

set(SPIDER_SCHEDULER_SOURCES
    scheduler/SchedulerPolicy.hpp
    scheduler/FifoPolicy.cpp
    scheduler/FifoPolicy.hpp
    scheduler/SchedulerMessage.hpp
    scheduler/SchedulerServer.cpp
    scheduler/SchedulerServer.hpp
    utils/StopToken.hpp
    CACHE INTERNAL
    "spider scheduler source files"
)
add_executable(spider_scheduler)
target_sources(spider_scheduler PRIVATE ${SPIDER_SCHEDULER_SOURCES})
target_sources(spider_scheduler PRIVATE scheduler/scheduler.cpp)
target_link_libraries(spider_scheduler PRIVATE spider_core)
target_link_libraries(
    spider_scheduler
    PRIVATE
        Boost::headers
        Boost::program_options
        absl::flat_hash_map
        fmt::fmt
        spdlog::spdlog
)

set(SPIDER_CLIENT_SHARED_SOURCES
    client/Driver.cpp
    client/TaskContext.cpp
    CACHE INTERNAL
    "spider client shared source files"
)

set(SPIDER_CLIENT_SHARED_HEADERS
    client/Data.hpp
    client/Driver.hpp
    client/Job.hpp
    client/task.hpp
    client/spider.hpp
    client/TaskContext.hpp
    client/TaskGraph.hpp
    client/type_utils.hpp
    client/Exception.hpp
    core/DataImpl.hpp
    core/TaskContextImpl.hpp
    core/TaskGraphImpl.hpp
    CACHE INTERNAL
    "spider client shared header files"
)

add_library(spider_client)
target_sources(spider_client PRIVATE ${SPIDER_CLIENT_SHARED_SOURCES})
target_sources(spider_client PUBLIC ${SPIDER_CLIENT_SHARED_HEADERS})
target_include_directories(spider_client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(
    spider_client
    PUBLIC
        spider_core
        Boost::boost
        absl::flat_hash_map
)

add_library(spider::spider ALIAS spider_client)
