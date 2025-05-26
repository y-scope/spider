// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-optin.core.EnumCastOutOfRange)

#include <chrono>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <variant>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include <spider/core/Data.hpp>
#include <spider/core/Driver.hpp>
#include <spider/core/Error.hpp>
#include <spider/core/JobRecovery.hpp>
#include <spider/core/Task.hpp>
#include <spider/core/TaskGraph.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/storage/StorageFactory.hpp>
#include <tests/storage/StorageTestHelper.hpp>

namespace {
TEMPLATE_LIST_TEST_CASE("Recovery single task", "[storage]", spider::test::StorageFactoryTypeList) {
    std::shared_ptr<spider::core::StorageFactory> const storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::shared_ptr<spider::core::MetadataStorage> const metadata_store
            = storage_factory->provide_metadata_storage();
    std::shared_ptr<spider::core::DataStorage> const data_store
            = storage_factory->provide_data_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    std::shared_ptr<spider::core::StorageConnection> const conn
            = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    boost::uuids::random_generator gen;

    boost::uuids::uuid const job_id = gen();
    boost::uuids::uuid const client_id = gen();
    // Submit task without data
    spider::core::Task task{"task"};
    REQUIRE(metadata_store->add_driver(*conn, spider::core::Driver{client_id}).success());
    task.add_input(spider::core::TaskInput{"10", "int"});
    task.add_output(spider::core::TaskOutput{"int"});
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_input_task(task.get_id());
    graph.add_output_task(task.get_id());
    REQUIRE(metadata_store->add_job(*conn, job_id, client_id, graph).success());

    // Set task as failed
    REQUIRE(metadata_store->set_task_state(*conn, task.get_id(), spider::core::TaskState::Failed)
                    .success());

    // Recover the job
    spider::core::JobRecovery recovery{job_id, conn, data_store, metadata_store};
    REQUIRE(recovery.compute_graph().success());
    auto const& ready_tasks = recovery.get_ready_tasks();
    auto const& pending_tasks = recovery.get_pending_tasks();
    REQUIRE(ready_tasks.size() == 1);
    REQUIRE(pending_tasks.empty());
    REQUIRE(ready_tasks[0] == task.get_id());

    REQUIRE(metadata_store->remove_job(*conn, job_id).success());
}

TEMPLATE_LIST_TEST_CASE(
        "Recovery single task with data",
        "[storage]",
        spider::test::StorageFactoryTypeList
) {
    std::shared_ptr<spider::core::StorageFactory> const storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::shared_ptr<spider::core::MetadataStorage> const metadata_store
            = storage_factory->provide_metadata_storage();
    std::shared_ptr<spider::core::DataStorage> const data_store
            = storage_factory->provide_data_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    std::shared_ptr<spider::core::StorageConnection> const conn
            = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    boost::uuids::random_generator gen;

    boost::uuids::uuid const job_id = gen();
    boost::uuids::uuid const client_id = gen();
    // Submit task without data
    spider::core::Task task{"task"};
    spider::core::Data data{"data"};
    REQUIRE(metadata_store->add_driver(*conn, spider::core::Driver{client_id}).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data).success());
    task.add_input(spider::core::TaskInput{data.get_id()});
    task.add_output(spider::core::TaskOutput{"int"});
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_input_task(task.get_id());
    graph.add_output_task(task.get_id());
    REQUIRE(metadata_store->add_job(*conn, job_id, client_id, graph).success());

    // Set task as failed
    REQUIRE(metadata_store->set_task_state(*conn, task.get_id(), spider::core::TaskState::Failed)
                    .success());

    // Recover the job
    spider::core::JobRecovery recovery{job_id, conn, data_store, metadata_store};
    REQUIRE(recovery.compute_graph().success());
    auto const& ready_tasks = recovery.get_ready_tasks();
    auto const& pending_tasks = recovery.get_pending_tasks();
    REQUIRE(ready_tasks.size() == 1);
    REQUIRE(pending_tasks.empty());
    REQUIRE(ready_tasks[0] == task.get_id());

    REQUIRE(metadata_store->remove_job(*conn, job_id).success());
    REQUIRE(data_store->remove_data(*conn, data.get_id()).success());
}

TEMPLATE_LIST_TEST_CASE(
        "Recovery single task with persisted data",
        "[storage]",
        spider::test::StorageFactoryTypeList
) {
    std::shared_ptr<spider::core::StorageFactory> const storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::shared_ptr<spider::core::MetadataStorage> const metadata_store
            = storage_factory->provide_metadata_storage();
    std::shared_ptr<spider::core::DataStorage> const data_store
            = storage_factory->provide_data_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    std::shared_ptr<spider::core::StorageConnection> const conn
            = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    boost::uuids::random_generator gen;

    boost::uuids::uuid const job_id = gen();
    boost::uuids::uuid const client_id = gen();
    // Submit task without data
    spider::core::Task task{"task"};
    spider::core::Data data{"data"};
    data.set_persisted(true);
    REQUIRE(metadata_store->add_driver(*conn, spider::core::Driver{client_id}).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data).success());
    task.add_input(spider::core::TaskInput{data.get_id()});
    task.add_output(spider::core::TaskOutput{"int"});
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_input_task(task.get_id());
    graph.add_output_task(task.get_id());
    REQUIRE(metadata_store->add_job(*conn, job_id, client_id, graph).success());

    // Set task as failed
    REQUIRE(metadata_store->set_task_state(*conn, task.get_id(), spider::core::TaskState::Failed)
                    .success());

    // Recover the job
    spider::core::JobRecovery recovery{job_id, conn, data_store, metadata_store};
    REQUIRE(recovery.compute_graph().success());
    auto const& ready_tasks = recovery.get_ready_tasks();
    auto const& pending_tasks = recovery.get_pending_tasks();
    REQUIRE(ready_tasks.size() == 1);
    REQUIRE(pending_tasks.empty());
    REQUIRE(ready_tasks[0] == task.get_id());

    REQUIRE(metadata_store->remove_job(*conn, job_id).success());
    REQUIRE(data_store->remove_data(*conn, data.get_id()).success());
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-optin.core.EnumCastOutOfRange)
