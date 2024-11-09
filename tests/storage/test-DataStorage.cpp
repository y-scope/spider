// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
#include "../../src/spider/core/Data.hpp"
#include "../../src/spider/core/Error.hpp"
#include "../../src/spider/core/Task.hpp"
#include "../../src/spider/core/TaskGraph.hpp"
#include "../../src/spider/storage/DataStorage.hpp"
#include "../utils/CoreDataUtils.hpp"
#include "StorageTestHelper.hpp"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>
#include <memory>
#include <tuple>

namespace {

TEMPLATE_LIST_TEST_CASE(
        "Add, get and remove data",
        "[storage]",
        spider::test::DataStorageTypeList
) {
    std::unique_ptr<spider::core::DataStorage> storage
            = spider::test::create_data_storage<TestType>();

    // Add data
    spider::core::Data const data{"value"};
    REQUIRE(storage->add_data(data).success());

    // Add data with same id again should fail
    spider::core::Data const data_same_id{data.get_id(), "value2"};
    REQUIRE(spider::core::StorageErrType::DuplicateKeyErr == storage->add_data(data_same_id).type);

    // Get data should match
    spider::core::Data result{"temp"};
    REQUIRE(storage->get_data(data.get_id(), &result).success());
    REQUIRE(spider::core::data_equal(data, result));

    // Remove data should succeed
    REQUIRE(storage->remove_data(data.get_id()).success());

    // Get data should fail
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr
            == storage->get_data(data.get_id(), &result).type);
}

TEMPLATE_LIST_TEST_CASE(
        "Add, get and remove data with key",
        "[storage]",
        spider::test::DataStorageTypeList
) {
    std::unique_ptr<spider::core::DataStorage> storage
            = spider::test::create_data_storage<TestType>();

    // Add data
    spider::core::Data const data{"key", "value"};
    REQUIRE(storage->add_data(data).success());

    // Add data with same key again should fail
    spider::core::Data const data_same_key{"key", "value2"};
    REQUIRE(spider::core::StorageErrType::DuplicateKeyErr == storage->add_data(data_same_key).type);

    // Get data should match
    spider::core::Data result{"temp"};
    REQUIRE(storage->get_data_by_key("key", &result).success());
    REQUIRE(spider::core::data_equal(data, result));

    // Remove data should succeed
    REQUIRE(storage->remove_data(data.get_id()).success());

    // Get data should fail
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr
            == storage->get_data_by_key("key", &result).type);
}

TEMPLATE_LIST_TEST_CASE(
        "Add and remove data reference for task",
        "[storage]",
        spider::test::StorageTypeList
) {
    auto [metadata_storage, data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();

    boost::uuids::random_generator gen;
    // Add task reference without data and task should fail.
    REQUIRE(!data_storage->add_task_reference(gen(), gen()).success());

    // Add task
    spider::core::Task const task{"func", spider::core::TaskCreatorType::Client, gen()};
    spider::core::TaskGraph graph;
    graph.add_task(task);
    REQUIRE(metadata_storage->add_job(gen(), gen(), graph).success());

    // Add task reference without data should fail.
    REQUIRE(!data_storage->add_task_reference(gen(), task.get_id()).success());

    // Add data
    spider::core::Data const data{"value"};
    REQUIRE(data_storage->add_data(data).success());

    // Add task reference
    REQUIRE(data_storage->add_task_reference(data.get_id(), task.get_id()).success());

    // Remove task reference
    REQUIRE(data_storage->remove_task_reference(data.get_id(), task.get_id()).success());
}

TEMPLATE_LIST_TEST_CASE(
        "Add and remove data reference for driver",
        "[storage]",
        spider::test::StorageTypeList
) {
    auto [metadata_storage, data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();

    boost::uuids::random_generator gen;

    // Add driver reference without data and driver should fail
    REQUIRE(!data_storage->add_driver_reference(gen(), gen()).success());

    // Add driver
    boost::uuids::uuid const driver_id = gen();
    REQUIRE(metadata_storage->add_driver(driver_id, "127.0.0.1").success());

    // Add driver reference without data should fail
    REQUIRE(!data_storage->add_driver_reference(gen(), driver_id).success());

    // Add data
    spider::core::Data const data{"value"};
    REQUIRE(data_storage->add_data(data).success());

    // Add driver reference
    REQUIRE(data_storage->add_driver_reference(data.get_id(), driver_id).success());

    // Remove driver reference
    REQUIRE(data_storage->remove_driver_reference(data.get_id(), driver_id).success());
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
