set(SPIDER_TEST_SOURCES
    storage/test-DataStorage.cpp
    storage/test-MetadataStorage.cpp
    storage/StorageTestHelper.hpp
    utils/CoreDataUtils.hpp
    utils/CoreTaskUtils.hpp
    utils/CoreTaskUtils.cpp
    worker/test-FunctionManager.cpp
    worker/test-MessagePipe.cpp
    worker/test-TaskExecutor.cpp
    CACHE INTERNAL
    "spider test source files"
)

add_executable(unitTest)
target_sources(unitTest PRIVATE ${SPIDER_TEST_SOURCES})
set(SPIDER_TEST_WORKER_SOURCES)
foreach(worker_source ${SPIDER_WORKER_SOURCES})
    list(APPEND SPIDER_TEST_WORKER_SOURCES "../src/spider/${worker_source}")
endforeach()
target_sources(unitTest PRIVATE ${SPIDER_TEST_WORKER_SOURCES})
target_link_libraries(unitTest PRIVATE Catch2::Catch2WithMain)
target_link_libraries(unitTest PRIVATE spider_core)
target_link_libraries(
    unitTest
    PRIVATE
        Boost::headers
        Boost::filesystem
        Boost::program_options
        Boost::process
        Boost::system
        spdlog::spdlog
)

add_library(worker_test SHARED)
target_sources(worker_test PRIVATE worker/worker-test.cpp)
target_link_libraries(worker_test PRIVATE spider_core)
