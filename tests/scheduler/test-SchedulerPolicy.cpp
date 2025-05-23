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
#include <spider/core/Task.hpp>
#include <spider/core/TaskGraph.hpp>
#include <spider/scheduler/FifoPolicy.hpp>
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/storage/StorageFactory.hpp>
#include <tests/storage/StorageTestHelper.hpp>

namespace {
TEMPLATE_LIST_TEST_CASE(
        "FIFO schedule order",
        "[scheduler][storage]",
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

    // Add scheduler
    boost::uuids::uuid const scheduler_id = gen();
    REQUIRE(metadata_store
                    ->add_scheduler(*conn, spider::core::Scheduler{scheduler_id, "127.0.0.1", 8080})
                    .success());

    boost::uuids::uuid const client_id = gen();
    // Submit tasks
    spider::core::Task const task_1{"task_1"};
    spider::core::TaskGraph graph_1;
    graph_1.add_task(task_1);
    graph_1.add_input_task(task_1.get_id());
    graph_1.add_output_task(task_1.get_id());
    boost::uuids::uuid const job_id_1 = gen();
    REQUIRE(metadata_store->add_job(*conn, job_id_1, client_id, graph_1).success());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    spider::core::Task const task_2{"task_2"};
    spider::core::TaskGraph graph_2;
    graph_2.add_task(task_2);
    graph_2.add_input_task(task_2.get_id());
    graph_2.add_output_task(task_2.get_id());
    boost::uuids::uuid const job_id_2 = gen();
    REQUIRE(metadata_store->add_job(*conn, job_id_2, client_id, graph_2).success());

    spider::scheduler::FifoPolicy policy{scheduler_id, metadata_store, data_store, conn};

    // Schedule the earlier task
    std::optional<boost::uuids::uuid> optional_task_id = policy.schedule_next(gen(), "");
    REQUIRE(optional_task_id.has_value());
    if (optional_task_id.has_value()) {
        boost::uuids::uuid const& task_id = optional_task_id.value();
        REQUIRE(task_id == task_1.get_id());
    }

    // Schedule the later task
    optional_task_id = policy.schedule_next(gen(), "");
    REQUIRE(optional_task_id.has_value());
    if (optional_task_id.has_value()) {
        boost::uuids::uuid const& task_id = optional_task_id.value();
        REQUIRE(task_id == task_2.get_id());
    }

    REQUIRE(metadata_store->remove_job(*conn, job_id_1).success());
    REQUIRE(metadata_store->remove_job(*conn, job_id_2).success());

    // Schedule when no task available
    optional_task_id = policy.schedule_next(gen(), "");
    REQUIRE(!optional_task_id.has_value());

    // Clean up
    REQUIRE(metadata_store->remove_driver(*conn, scheduler_id).success());
}

TEMPLATE_LIST_TEST_CASE(
        "Schedule hard locality",
        "[scheduler][storage]",
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

    // Add scheduler
    boost::uuids::uuid const scheduler_id = gen();
    REQUIRE(metadata_store
                    ->add_scheduler(*conn, spider::core::Scheduler{scheduler_id, "127.0.0.1", 8080})
                    .success());

    boost::uuids::uuid const job_id = gen();
    boost::uuids::uuid const client_id = gen();
    // Submit task with hard locality
    spider::core::Task task{"task"};
    spider::core::Data data{"value"};
    data.set_hard_locality(true);
    data.set_locality({"127.0.0.1"});
    REQUIRE(metadata_store->add_driver(*conn, spider::core::Driver{client_id}).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data).success());
    task.add_input(spider::core::TaskInput{data.get_id()});
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_input_task(task.get_id());
    graph.add_output_task(task.get_id());
    REQUIRE(metadata_store->add_job(*conn, job_id, client_id, graph).success());

    spider::scheduler::FifoPolicy policy{scheduler_id, metadata_store, data_store, conn};
    // Schedule with wrong address
    REQUIRE_FALSE(policy.schedule_next(gen(), "").has_value());
    // Schedule with correct address
    std::optional<boost::uuids::uuid> const optional_task_id
            = policy.schedule_next(gen(), "127.0.0.1");
    REQUIRE(optional_task_id.has_value());
    if (optional_task_id.has_value()) {
        boost::uuids::uuid const& task_id = optional_task_id.value();
        REQUIRE(task_id == task.get_id());
    }

    REQUIRE(metadata_store->remove_job(*conn, job_id).success());
    REQUIRE(data_store->remove_data(*conn, data.get_id()).success());
    REQUIRE(metadata_store->remove_driver(*conn, scheduler_id).success());
    REQUIRE(metadata_store->remove_driver(*conn, client_id).success());
}

TEMPLATE_LIST_TEST_CASE(
        "Schedule soft locality",
        "[scheduler][storage]",
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

    // Add scheduler
    boost::uuids::uuid const scheduler_id = gen();
    REQUIRE(metadata_store
                    ->add_scheduler(*conn, spider::core::Scheduler{scheduler_id, "127.0.0.1", 8080})
                    .success());

    // Add task
    boost::uuids::uuid const job_id = gen();
    boost::uuids::uuid const client_id = gen();
    spider::core::Task task{"task"};
    spider::core::Data data;
    data.set_hard_locality(false);
    data.set_locality({"127.0.0.1"});
    REQUIRE(metadata_store->add_driver(*conn, spider::core::Driver{client_id}).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data).success());
    task.add_input(spider::core::TaskInput{data.get_id()});
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_input_task(task.get_id());
    graph.add_output_task(task.get_id());
    REQUIRE(metadata_store->add_job(*conn, job_id, client_id, graph).success());

    spider::scheduler::FifoPolicy policy{scheduler_id, metadata_store, data_store, conn};

    // Schedule with wrong address
    std::optional<boost::uuids::uuid> const optional_task_id = policy.schedule_next(gen(), "");
    REQUIRE(optional_task_id.has_value());
    if (optional_task_id.has_value()) {
        boost::uuids::uuid const& task_id = optional_task_id.value();
        REQUIRE(task_id == task.get_id());
    }

    REQUIRE(metadata_store->remove_job(*conn, job_id).success());
    REQUIRE(data_store->remove_data(*conn, data.get_id()).success());
    REQUIRE(metadata_store->remove_driver(*conn, scheduler_id).success());
    REQUIRE(metadata_store->remove_driver(*conn, client_id).success());
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-optin.core.EnumCastOutOfRange)
