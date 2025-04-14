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
    worker/test-Process.cpp
    io/test-MsgpackMessage.cpp
    scheduler/test-SchedulerPolicy.cpp
    scheduler/test-SchedulerServer.cpp
    client/test-Driver.cpp
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

set(SPIDER_TEST_SCHEDULER_SOURCES)
foreach(scheduler_source ${SPIDER_SCHEDULER_SOURCES})
    list(APPEND SPIDER_TEST_SCHEDULER_SOURCES "../src/spider/${scheduler_source}")
endforeach()
target_sources(unitTest PRIVATE ${SPIDER_TEST_SCHEDULER_SOURCES})

target_link_libraries(unitTest PRIVATE Catch2::Catch2WithMain)
target_link_libraries(
    unitTest
    PRIVATE
        spider_core
        spider_client
)
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
add_dependencies(unitTest worker_test)

add_library(worker_test SHARED)
target_sources(worker_test PUBLIC worker/worker-test.hpp)
target_sources(worker_test PRIVATE worker/worker-test.cpp)
target_link_libraries(
    worker_test
    PRIVATE
        spider_core
        spider_client
)

add_executable(client_test)
target_sources(client_test PRIVATE client/client-test.cpp)
target_link_libraries(
    client_test
    PRIVATE
        spider_core
        spider_client
        worker_test
        Boost::program_options
        spdlog::spdlog
)

add_custom_target(integrationTest ALL)
add_custom_command(
    TARGET integrationTest
    PRE_BUILD
    COMMAND
        ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/integration
        ${CMAKE_CURRENT_BINARY_DIR}/integration
)
add_dependencies(
    integrationTest
    worker_test
    client_test
)
