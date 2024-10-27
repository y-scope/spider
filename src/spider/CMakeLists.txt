
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

set(SPIDER_WORKER_SOURCES
    worker/worker.cpp
    CACHE INTERNAL "spider worker source files"
)

add_executable(spider_worker)
target_compile_features(spider_worker PRIVATE cxx_std_20)
target_sources(spider_worker PRIVATE ${SPIDER_CORE_SOURCES} ${SPIDER_WORKER_SOURCES})
target_link_libraries(spider_worker Boost::boost absl::flat_hash_map)
add_executable(spider::worker ALIAS spider_worker)
