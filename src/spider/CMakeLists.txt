# set variable as CACHE INTERNAL to access it from other scope
set(SPIDER_CORE_SOURCES storage/MetadataStorage.hpp CACHE INTERNAL "spider core source files")

set(SPIDER_WORKER_SOURCES worker/worker.cpp CACHE INTERNAL "spider worker source files")

add_executable(spider_worker)
target_compile_features(spider_worker PRIVATE cxx_std_20)
target_sources(
    spider_worker
    PRIVATE
        ${SPIDER_CORE_SOURCES}
        ${SPIDER_WORKER_SOURCES}
)
add_executable(spider::worker ALIAS spider_worker)
