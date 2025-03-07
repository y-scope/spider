// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
#include <tuple>
#include <variant>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/core/Data.hpp"
#include "../../src/spider/core/Driver.hpp"
#include "../../src/spider/core/Error.hpp"
#include "../../src/spider/core/KeyValueData.hpp"
#include "../../src/spider/core/Task.hpp"
#include "../../src/spider/core/TaskGraph.hpp"
#include "../../src/spider/storage/mysql/MySqlConnection.hpp"
#include "../utils/CoreDataUtils.hpp"
#include "StorageTestHelper.hpp"

namespace {

TEMPLATE_LIST_TEST_CASE("Add, get and remove data", "[storage]", spider::test::StorageTypeList) {
    auto [metadata_storage, data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();

    std::variant<spider::core::MySqlConnection, spider::core::StorageErr> conn_result
            = spider::core::MySqlConnection::create(metadata_storage->get_url());
    REQUIRE(std::holds_alternative<spider::core::MySqlConnection>(conn_result));
    auto& conn = std::get<spider::core::MySqlConnection>(conn_result);

    // Add driver and data
    spider::core::Data const data{"value"};
    boost::uuids::random_generator gen;
    boost::uuids::uuid const driver_id = gen();
    REQUIRE(metadata_storage->add_driver(conn, spider::core::Driver{driver_id}).success());
    REQUIRE(data_storage->add_driver_data(conn, driver_id, data).success());

    // Add data with same id again should fail
    spider::core::Data const data_same_id{data.get_id(), "value2"};
    REQUIRE(spider::core::StorageErrType::DuplicateKeyErr
            == data_storage->add_driver_data(conn, driver_id, data_same_id).type);

    // Get data should match
    spider::core::Data result{"temp"};
    REQUIRE(data_storage->get_data(conn, data.get_id(), &result).success());
    REQUIRE(spider::test::data_equal(data, result));

    // Remove data should succeed
    REQUIRE(data_storage->remove_data(conn, data.get_id()).success());

    // Get data should fail
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr
            == data_storage->get_data(conn, data.get_id(), &result).type);
}

TEMPLATE_LIST_TEST_CASE(
        "Add and get driver key value data",
        "[storage]",
        spider::test::StorageTypeList
) {
    auto [metadata_storage, data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();

    std::variant<spider::core::MySqlConnection, spider::core::StorageErr> conn_result
            = spider::core::MySqlConnection::create(metadata_storage->get_url());
    REQUIRE(std::holds_alternative<spider::core::MySqlConnection>(conn_result));
    auto& conn = std::get<spider::core::MySqlConnection>(conn_result);

    // Add driver
    boost::uuids::random_generator gen;
    boost::uuids::uuid const driver_id = gen();
    REQUIRE(metadata_storage->add_driver(conn, spider::core::Driver{driver_id}).success());

    // Add data
    spider::core::KeyValueData const data{"key", "value", driver_id};
    REQUIRE(data_storage->add_client_kv_data(conn, data).success());

    // Add data with same key and id again should fail
    spider::core::KeyValueData const data_same_key{"key", "value2", driver_id};
    REQUIRE(spider::core::StorageErrType::DuplicateKeyErr
            == data_storage->add_client_kv_data(conn, data_same_key).type);

    // Get data should match
    std::string value;
    auto err = data_storage->get_client_kv_data(conn, driver_id, "key", &value);
    REQUIRE(data_storage->get_client_kv_data(conn, driver_id, "key", &value).success());
    REQUIRE(data.get_value() == value);
}

TEMPLATE_LIST_TEST_CASE(
        "Add and get task key value data",
        "[storage]",
        spider::test::StorageTypeList
) {
    auto [metadata_storage, data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();

    std::variant<spider::core::MySqlConnection, spider::core::StorageErr> conn_result
            = spider::core::MySqlConnection::create(metadata_storage->get_url());
    REQUIRE(std::holds_alternative<spider::core::MySqlConnection>(conn_result));
    auto& conn = std::get<spider::core::MySqlConnection>(conn_result);

    // Add task
    boost::uuids::random_generator gen;
    spider::core::Task const task{"func"};
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_input_task(task.get_id());
    graph.add_output_task(task.get_id());
    boost::uuids::uuid const job_id = gen();
    REQUIRE(metadata_storage->add_job(conn, job_id, gen(), graph).success());

    // Add data
    spider::core::KeyValueData const data{"key", "value", task.get_id()};
    REQUIRE(data_storage->add_task_kv_data(conn, data).success());

    // Add data with same key and id again should fail
    spider::core::KeyValueData const data_same_key{"key", "value2", task.get_id()};
    REQUIRE(spider::core::StorageErrType::DuplicateKeyErr
            == data_storage->add_task_kv_data(conn, data_same_key).type);

    // Get data should match
    std::string value;
    REQUIRE(data_storage->get_task_kv_data(conn, task.get_id(), "key", &value).success());
    REQUIRE(data.get_value() == value);

    // Clean up
    REQUIRE(metadata_storage->remove_job(conn, job_id).success());
}

TEMPLATE_LIST_TEST_CASE(
        "Add and remove task reference for task",
        "[storage]",
        spider::test::StorageTypeList
) {
    auto [metadata_storage, data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();

    std::variant<spider::core::MySqlConnection, spider::core::StorageErr> conn_result
            = spider::core::MySqlConnection::create(metadata_storage->get_url());
    REQUIRE(std::holds_alternative<spider::core::MySqlConnection>(conn_result));
    auto& conn = std::get<spider::core::MySqlConnection>(conn_result);

    boost::uuids::random_generator gen;
    // Add task reference without data and task should fail.
    REQUIRE(!data_storage->add_task_reference(conn, gen(), gen()).success());

    // Add task
    spider::core::Task const task{"func"};
    spider::core::Task const task_2{"func"};
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_task(task_2);
    graph.add_dependency(task.get_id(), task_2.get_id());
    graph.add_input_task(task.get_id());
    graph.add_output_task(task_2.get_id());
    boost::uuids::uuid const job_id = gen();
    REQUIRE(metadata_storage->add_job(conn, job_id, gen(), graph).success());

    // Add task reference without data should fail.
    REQUIRE(!data_storage->add_task_reference(conn, gen(), task.get_id()).success());

    // Add data
    spider::core::Data const data{"value"};
    REQUIRE(data_storage->add_task_data(conn, task.get_id(), data).success());

    // Add task reference
    REQUIRE(data_storage->add_task_reference(conn, data.get_id(), task_2.get_id()).success());

    // Remove task reference
    REQUIRE(data_storage->remove_task_reference(conn, data.get_id(), task_2.get_id()).success());

    // Remove job
    REQUIRE(metadata_storage->remove_job(conn, job_id).success());

    // Clean up
    REQUIRE(data_storage->remove_dangling_data(conn).success());

    // Get data should fail
    spider::core::Data res{"temp"};
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr
            == data_storage->get_data(conn, data.get_id(), &res).type);
}

TEMPLATE_LIST_TEST_CASE(
        "Add and remove data reference for driver",
        "[storage]",
        spider::test::StorageTypeList
) {
    auto [metadata_storage, data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();

    std::variant<spider::core::MySqlConnection, spider::core::StorageErr> conn_result
            = spider::core::MySqlConnection::create(metadata_storage->get_url());
    REQUIRE(std::holds_alternative<spider::core::MySqlConnection>(conn_result));
    auto& conn = std::get<spider::core::MySqlConnection>(conn_result);

    boost::uuids::random_generator gen;

    // Add driver reference without data and driver should fail
    REQUIRE(!data_storage->add_driver_reference(conn, gen(), gen()).success());

    // Add driver
    boost::uuids::uuid const driver_id = gen();
    boost::uuids::uuid const driver_id_2 = gen();
    REQUIRE(metadata_storage->add_driver(conn, spider::core::Driver{driver_id}).success());
    REQUIRE(metadata_storage->add_driver(conn, spider::core::Driver{driver_id_2}).success());

    // Add driver reference without data should fail
    REQUIRE(!data_storage->add_driver_reference(conn, gen(), driver_id).success());

    // Add data
    spider::core::Data const data{"value"};
    REQUIRE(data_storage->add_driver_data(conn, driver_id, data).success());

    // Add driver reference
    REQUIRE(data_storage->add_driver_reference(conn, data.get_id(), driver_id_2).success());

    // Remove driver reference
    REQUIRE(data_storage->remove_driver_reference(conn, data.get_id(), driver_id_2).success());
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
