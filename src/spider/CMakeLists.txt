
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
    CACHE INTERNAL "spider core source files"
)

if (SPIDER_USE_STATIC_LIBS)
    add_library(spider_core STATIC)
else()
    add_library(spider_core SHARED)
endif()
target_compile_features(spider_core PRIVATE cxx_std_20)
target_sources(spider_core PRIVATE ${SPIDER_CORE_SOURCES})
target_link_libraries(spider_core Boost::boost absl::flat_hash_map absl::flat_hash_map MariaDBClientCpp::MariaDBClientCpp)

set(SPIDER_WORKER_SOURCES
    worker/worker.cpp
    CACHE INTERNAL "spider worker source files"
)

add_executable(spider_worker)
target_compile_features(spider_worker PRIVATE cxx_std_20)
target_sources(spider_worker PRIVATE ${SPIDER_WORKER_SOURCES})
target_link_libraries(spider_worker spider_core)
add_executable(spider::worker ALIAS spider_worker)
