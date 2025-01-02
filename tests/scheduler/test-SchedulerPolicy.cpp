// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-optin.core.EnumCastOutOfRange)

#include <chrono>
#include <memory>
#include <optional>
#include <thread>
#include <tuple>
#include <utility>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/core/Data.hpp"
#include "../../src/spider/core/Driver.hpp"
#include "../../src/spider/core/Task.hpp"
#include "../../src/spider/core/TaskGraph.hpp"
#include "../../src/spider/scheduler/FifoPolicy.hpp"
#include "../../src/spider/storage/DataStorage.hpp"
#include "../../src/spider/storage/MetadataStorage.hpp"
#include "../storage/StorageTestHelper.hpp"

namespace {
TEMPLATE_LIST_TEST_CASE(
        "FIFO schedule order",
        "[scheduler][storage]",
        spider::test::StorageTypeList
) {
    std::tuple<
            std::unique_ptr<spider::core::MetadataStorage>,
            std::unique_ptr<spider::core::DataStorage>>
            storages = spider::test::create_storage<
                    std::tuple_element_t<0, TestType>,
                    std::tuple_element_t<1, TestType>>();
    std::shared_ptr<spider::core::MetadataStorage> const metadata_store
            = std::move(std::get<0>(storages));
    std::shared_ptr<spider::core::DataStorage> const data_store = std::move(std::get<1>(storages));

    boost::uuids::random_generator gen;
    boost::uuids::uuid const client_id = gen();
    // Submit tasks
    spider::core::Task const task_1{"task_1"};
    spider::core::TaskGraph graph_1;
    graph_1.add_task(task_1);
    graph_1.add_input_task(task_1.get_id());
    graph_1.add_output_task(task_1.get_id());
    boost::uuids::uuid const job_id_1 = gen();
    REQUIRE(metadata_store->add_job(job_id_1, client_id, graph_1).success());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    spider::core::Task const task_2{"task_2"};
    spider::core::TaskGraph graph_2;
    graph_2.add_task(task_2);
    graph_2.add_input_task(task_2.get_id());
    graph_2.add_output_task(task_2.get_id());
    boost::uuids::uuid const job_id_2 = gen();
    REQUIRE(metadata_store->add_job(job_id_2, client_id, graph_2).success());

    spider::scheduler::FifoPolicy policy;

    // Scheduler the earlier task
    std::optional<boost::uuids::uuid> const optional_task_id
            = policy.schedule_next(metadata_store, data_store, gen(), "");
    REQUIRE(optional_task_id.has_value());
    if (optional_task_id.has_value()) {
        boost::uuids::uuid const& task_id = optional_task_id.value();
        REQUIRE(task_id == task_1.get_id());
    }

    REQUIRE(metadata_store->remove_job(job_id_1).success());
    REQUIRE(metadata_store->remove_job(job_id_2).success());
}

TEMPLATE_LIST_TEST_CASE(
        "Schedule hard locality",
        "[scheduler][storage]",
        spider::test::StorageTypeList
) {
    std::tuple<
            std::unique_ptr<spider::core::MetadataStorage>,
            std::unique_ptr<spider::core::DataStorage>>
            storages = spider::test::create_storage<
                    std::tuple_element_t<0, TestType>,
                    std::tuple_element_t<1, TestType>>();
    std::shared_ptr<spider::core::MetadataStorage> const metadata_store
            = std::move(std::get<0>(storages));
    std::shared_ptr<spider::core::DataStorage> const data_store = std::move(std::get<1>(storages));

    boost::uuids::random_generator gen;
    boost::uuids::uuid const job_id = gen();
    boost::uuids::uuid const client_id = gen();
    // Submit task with hard locality
    spider::core::Task task{"task"};
    spider::core::Data data{"value"};
    data.set_hard_locality(true);
    data.set_locality({"127.0.0.1"});
    REQUIRE(metadata_store->add_driver(spider::core::Driver{client_id, "127.0.0.1"}).success());
    REQUIRE(data_store->add_driver_data(client_id, data).success());
    task.add_input(spider::core::TaskInput{data.get_id(), "int"});
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_input_task(task.get_id());
    graph.add_output_task(task.get_id());
    REQUIRE(metadata_store->add_job(job_id, client_id, graph).success());

    spider::scheduler::FifoPolicy policy;
    // Schedule with wrong address
    REQUIRE_FALSE(policy.schedule_next(metadata_store, data_store, gen(), "").has_value());
    // Schedule with correct address
    std::optional<boost::uuids::uuid> const optional_task_id
            = policy.schedule_next(metadata_store, data_store, gen(), "127.0.0.1");
    REQUIRE(optional_task_id.has_value());
    if (optional_task_id.has_value()) {
        boost::uuids::uuid const& task_id = optional_task_id.value();
        REQUIRE(task_id == task.get_id());
    }

    REQUIRE(metadata_store->remove_job(job_id).success());
}

TEMPLATE_LIST_TEST_CASE(
        "Schedule soft locality",
        "[scheduler][storage]",
        spider::test::StorageTypeList
) {
    std::tuple<
            std::unique_ptr<spider::core::MetadataStorage>,
            std::unique_ptr<spider::core::DataStorage>>
            storages = spider::test::create_storage<
                    std::tuple_element_t<0, TestType>,
                    std::tuple_element_t<1, TestType>>();
    std::shared_ptr<spider::core::MetadataStorage> const metadata_store
            = std::move(std::get<0>(storages));
    std::shared_ptr<spider::core::DataStorage> const data_store = std::move(std::get<1>(storages));

    // Add task
    boost::uuids::random_generator gen;
    boost::uuids::uuid const job_id = gen();
    boost::uuids::uuid const client_id = gen();
    spider::core::Task task{"task"};
    spider::core::Data data;
    data.set_hard_locality(false);
    data.set_locality({"127.0.0.1"});
    REQUIRE(metadata_store->add_driver(spider::core::Driver{client_id, "127.0.0.1"}).success());
    REQUIRE(data_store->add_driver_data(client_id, data).success());
    task.add_input(spider::core::TaskInput{data.get_id(), "int"});
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_input_task(task.get_id());
    graph.add_output_task(task.get_id());
    REQUIRE(metadata_store->add_job(job_id, client_id, graph).success());

    spider::scheduler::FifoPolicy policy;
    // Schedule with wrong address
    std::optional<boost::uuids::uuid> const optional_task_id
            = policy.schedule_next(metadata_store, data_store, gen(), "");
    REQUIRE(optional_task_id.has_value());
    if (optional_task_id.has_value()) {
        boost::uuids::uuid const& task_id = optional_task_id.value();
        REQUIRE(task_id == task.get_id());
    }

    REQUIRE(metadata_store->remove_job(job_id).success());
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-optin.core.EnumCastOutOfRange)
