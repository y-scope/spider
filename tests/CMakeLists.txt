set(SPIDER_TEST_SOURCES
    storage/test-DataStorage.cpp
    storage/test-MetadataStorage.cpp
    storage/StorageTestHelper.hpp
    utils/CoreDataUtils.hpp
    utils/CoreTaskUtils.hpp
    utils/CoreTaskUtils.cpp
    worker/test-FunctionManager.cpp
    worker/test-MessagePipe.cpp
    CACHE INTERNAL
    "spider test source files"
)

add_executable(unitTest)
target_sources(unitTest PRIVATE ${SPIDER_TEST_SOURCES})
target_link_libraries(unitTest PRIVATE Catch2::Catch2WithMain)
target_link_libraries(unitTest PRIVATE spider_core)
