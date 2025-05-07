// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <algorithm>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/core/Driver.hpp"
#include "../../src/spider/core/Error.hpp"
#include "../../src/spider/core/JobMetadata.hpp"
#include "../../src/spider/core/Task.hpp"
#include "../../src/spider/core/TaskGraph.hpp"
#include "../../src/spider/storage/JobSubmissionBatch.hpp"
#include "../../src/spider/storage/MetadataStorage.hpp"
#include "../../src/spider/storage/StorageConnection.hpp"
#include "../../src/spider/storage/StorageFactory.hpp"
#include "../utils/CoreTaskUtils.hpp"
#include "StorageTestHelper.hpp"

namespace {
TEMPLATE_LIST_TEST_CASE("Driver heartbeat", "[storage]", spider::test::StorageFactoryTypeList) {
    std::unique_ptr<spider::core::StorageFactory> storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::unique_ptr<spider::core::MetadataStorage> storage
            = storage_factory->provide_metadata_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    auto conn = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    constexpr double cDuration = 100;

    // Add driver should succeed
    boost::uuids::random_generator gen;
    boost::uuids::uuid const driver_id = gen();
    REQUIRE(storage->add_driver(*conn, spider::core::Driver{driver_id}).success());

    std::vector<boost::uuids::uuid> ids{};
    // Driver should not time out
    REQUIRE(storage->heartbeat_timeout(*conn, cDuration, &ids).success());
    // Because other tests may run in parallel, just check `ids` don't have `driver_id`
    REQUIRE(std::ranges::none_of(ids, [&driver_id](boost::uuids::uuid id) {
        return id == driver_id;
    }));
    ids.clear();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    // Driver should time out
    REQUIRE(storage->heartbeat_timeout(*conn, cDuration, &ids).success());
    REQUIRE(!ids.empty());
    REQUIRE(std::ranges::any_of(ids, [&driver_id](boost::uuids::uuid id) {
        return id == driver_id;
    }));
    ids.clear();

    // Update heartbeat
    REQUIRE(storage->update_heartbeat(*conn, driver_id).success());
    // Driver should not time out
    REQUIRE(storage->heartbeat_timeout(*conn, cDuration, &ids).success());
    REQUIRE(std::ranges::none_of(ids, [&driver_id](boost::uuids::uuid id) {
        return id == driver_id;
    }));
}

TEMPLATE_LIST_TEST_CASE("Scheduler addr", "[storage]", spider::test::StorageFactoryTypeList) {
    std::unique_ptr<spider::core::StorageFactory> storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::unique_ptr<spider::core::MetadataStorage> storage
            = storage_factory->provide_metadata_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    auto conn = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    boost::uuids::random_generator gen;
    boost::uuids::uuid const scheduler_id = gen();
    constexpr int cPort = 3306;

    // Add scheduler should succeed
    REQUIRE(storage->add_scheduler(*conn, spider::core::Scheduler{scheduler_id, "127.0.0.1", cPort})
                    .success());

    // Get scheduler addr should succeed
    std::string addr_res;
    int port_res = 0;
    REQUIRE(storage->get_scheduler_addr(*conn, scheduler_id, &addr_res, &port_res).success());
    REQUIRE(addr_res == "127.0.0.1");
    REQUIRE(port_res == cPort);

    // Get non-exist scheduler should fail
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr
            == storage->get_scheduler_addr(*conn, gen(), &addr_res, &port_res).type);
}

TEMPLATE_LIST_TEST_CASE(
        "Job batch add, get and remove",
        "[storage]",
        spider::test::StorageFactoryTypeList
) {
    std::unique_ptr<spider::core::StorageFactory> storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::unique_ptr<spider::core::MetadataStorage> storage
            = storage_factory->provide_metadata_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    auto conn = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    std::unique_ptr<spider::core::JobSubmissionBatch> batch
            = storage_factory->provide_job_submission_batch(*conn);

    boost::uuids::random_generator gen;
    boost::uuids::uuid const job_id = gen();

    // Create a complicated task graph
    boost::uuids::uuid const client_id = gen();
    spider::core::Task child_task{"child"};
    spider::core::Task parent_1{"p1"};
    spider::core::Task parent_2{"p2"};
    parent_1.add_input(spider::core::TaskInput{"1", "float"});
    parent_1.add_input(spider::core::TaskInput{"2", "float"});
    parent_2.add_input(spider::core::TaskInput{"3", "int"});
    parent_2.add_input(spider::core::TaskInput{"4", "int"});
    parent_1.add_output(spider::core::TaskOutput{"float"});
    parent_2.add_output(spider::core::TaskOutput{"int"});
    child_task.add_input(spider::core::TaskInput{parent_1.get_id(), 0, "float"});
    child_task.add_input(spider::core::TaskInput{parent_2.get_id(), 0, "int"});
    child_task.add_output(spider::core::TaskOutput{"float"});
    spider::core::TaskGraph graph;
    // Add task and dependencies to task graph in wrong order
    graph.add_task(child_task);
    graph.add_task(parent_1);
    graph.add_task(parent_2);
    graph.add_dependency(parent_2.get_id(), child_task.get_id());
    graph.add_dependency(parent_1.get_id(), child_task.get_id());
    graph.add_input_task(parent_1.get_id());
    graph.add_input_task(parent_2.get_id());
    graph.add_output_task(child_task.get_id());

    // Get head tasks should succeed
    std::vector<boost::uuids::uuid> heads = graph.get_input_tasks();
    REQUIRE(2 == heads.size());
    REQUIRE(heads[0] == parent_1.get_id());
    REQUIRE(heads[1] == parent_2.get_id());

    std::chrono::system_clock::time_point const job_creation_time
            = std::chrono::system_clock::now();

    // Submit a simple job
    boost::uuids::uuid const simple_job_id = gen();
    spider::core::Task const simple_task{"simple"};
    spider::core::TaskGraph simple_graph;
    simple_graph.add_task(simple_task);
    simple_graph.add_input_task(simple_task.get_id());
    simple_graph.add_output_task(simple_task.get_id());

    heads = simple_graph.get_input_tasks();
    REQUIRE(1 == heads.size());
    REQUIRE(heads[0] == simple_task.get_id());

    // Submit job should success
    REQUIRE(storage->add_job_batch(*conn, *batch, job_id, client_id, graph).success());
    REQUIRE(
            storage->add_job_batch(*conn, *batch, simple_job_id, client_id, simple_graph).success()
    );
    batch->submit_batch(*conn);

    // Get job id for non-existent client id should return empty vector
    std::vector<boost::uuids::uuid> job_ids;
    REQUIRE(storage->get_jobs_by_client_id(*conn, gen(), &job_ids).success());
    REQUIRE(job_ids.empty());

    // Get job id for client id should get correct value
    REQUIRE(storage->get_jobs_by_client_id(*conn, client_id, &job_ids).success());
    REQUIRE(2 == job_ids.size());
    REQUIRE(
            ((job_ids[0] == job_id && job_ids[1] == simple_job_id)
             || (job_ids[0] == simple_job_id && job_ids[1] == job_id))
    );

    // Get job metadata should get correct value
    spider::core::JobMetadata job_metadata{};
    REQUIRE(storage->get_job_metadata(*conn, job_id, &job_metadata).success());
    REQUIRE(job_id == job_metadata.get_id());
    REQUIRE(client_id == job_metadata.get_client_id());
    std::chrono::seconds const time_delta{1};
    // REQUIRE(job_creation_time + time_delta >= job_metadata.get_creation_time());
    // REQUIRE(job_creation_time - time_delta <= job_metadata.get_creation_time());

    // Get task graph should succeed
    spider::core::TaskGraph graph_res{};
    REQUIRE(storage->get_task_graph(*conn, job_id, &graph_res).success());
    REQUIRE(spider::test::task_graph_equal(graph, graph_res));
    spider::core::TaskGraph simple_graph_res{};
    REQUIRE(storage->get_task_graph(*conn, simple_job_id, &simple_graph_res).success());
    REQUIRE(spider::test::task_graph_equal(simple_graph, simple_graph_res));

    // Get task should succeed
    spider::core::Task task_res{""};
    REQUIRE(storage->get_task(*conn, child_task.get_id(), &task_res).success());
    REQUIRE(spider::test::task_equal(child_task, task_res));

    // Get child tasks should succeed
    std::vector<spider::core::Task> tasks;
    REQUIRE(storage->get_child_tasks(*conn, parent_1.get_id(), &tasks).success());
    REQUIRE(1 == tasks.size());
    REQUIRE(spider::test::task_equal(child_task, tasks[0]));
    tasks.clear();

    // Get parent tasks should succeed
    REQUIRE(storage->get_parent_tasks(*conn, child_task.get_id(), &tasks).success());
    REQUIRE(2 == tasks.size());
    REQUIRE(
            ((spider::test::task_equal(tasks[0], parent_1)
              && spider::test::task_equal(tasks[1], parent_2))
             || (spider::test::task_equal(tasks[0], parent_2)
                 && spider::test::task_equal(tasks[1], parent_1)))
    );

    // Remove job should succeed
    REQUIRE(storage->remove_job(*conn, simple_job_id).success());
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr
            == storage->get_task_graph(*conn, simple_job_id, &simple_graph_res).type);
    graph_res = spider::core::TaskGraph{};
    REQUIRE(storage->get_task_graph(*conn, job_id, &graph_res).success());
    REQUIRE(spider::test::task_graph_equal(graph, graph_res));
    REQUIRE(storage->remove_job(*conn, job_id).success());
}

TEMPLATE_LIST_TEST_CASE(
        "Job add, get and remove",
        "[storage]",
        spider::test::StorageFactoryTypeList
) {
    std::unique_ptr<spider::core::StorageFactory> storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::unique_ptr<spider::core::MetadataStorage> storage
            = storage_factory->provide_metadata_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    auto conn = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    boost::uuids::random_generator gen;
    boost::uuids::uuid const job_id = gen();

    // Create a complicated task graph
    boost::uuids::uuid const client_id = gen();
    spider::core::Task child_task{"child"};
    spider::core::Task parent_1{"p1"};
    spider::core::Task parent_2{"p2"};
    parent_1.add_input(spider::core::TaskInput{"1", "float"});
    parent_1.add_input(spider::core::TaskInput{"2", "float"});
    parent_2.add_input(spider::core::TaskInput{"3", "int"});
    parent_2.add_input(spider::core::TaskInput{"4", "int"});
    parent_1.add_output(spider::core::TaskOutput{"float"});
    parent_2.add_output(spider::core::TaskOutput{"int"});
    child_task.add_input(spider::core::TaskInput{parent_1.get_id(), 0, "float"});
    child_task.add_input(spider::core::TaskInput{parent_2.get_id(), 0, "int"});
    child_task.add_output(spider::core::TaskOutput{"float"});
    spider::core::TaskGraph graph;
    // Add task and dependencies to task graph in wrong order
    graph.add_task(child_task);
    graph.add_task(parent_1);
    graph.add_task(parent_2);
    graph.add_dependency(parent_2.get_id(), child_task.get_id());
    graph.add_dependency(parent_1.get_id(), child_task.get_id());
    graph.add_input_task(parent_1.get_id());
    graph.add_input_task(parent_2.get_id());
    graph.add_output_task(child_task.get_id());

    // Get head tasks should succeed
    std::vector<boost::uuids::uuid> heads = graph.get_input_tasks();
    REQUIRE(2 == heads.size());
    REQUIRE(heads[0] == parent_1.get_id());
    REQUIRE(heads[1] == parent_2.get_id());

    std::chrono::system_clock::time_point const job_creation_time
            = std::chrono::system_clock::now();

    // Submit a simple job
    boost::uuids::uuid const simple_job_id = gen();
    spider::core::Task const simple_task{"simple"};
    spider::core::TaskGraph simple_graph;
    simple_graph.add_task(simple_task);
    simple_graph.add_input_task(simple_task.get_id());
    simple_graph.add_output_task(simple_task.get_id());

    heads = simple_graph.get_input_tasks();
    REQUIRE(1 == heads.size());
    REQUIRE(heads[0] == simple_task.get_id());

    // Submit job should success
    REQUIRE(storage->add_job(*conn, job_id, client_id, graph).success());
    REQUIRE(storage->add_job(*conn, simple_job_id, client_id, simple_graph).success());

    // Get job id for non-existent client id should return empty vector
    std::vector<boost::uuids::uuid> job_ids;
    REQUIRE(storage->get_jobs_by_client_id(*conn, gen(), &job_ids).success());
    REQUIRE(job_ids.empty());

    // Get job id for client id should get correct value
    REQUIRE(storage->get_jobs_by_client_id(*conn, client_id, &job_ids).success());
    REQUIRE(2 == job_ids.size());
    REQUIRE(
            ((job_ids[0] == job_id && job_ids[1] == simple_job_id)
             || (job_ids[0] == simple_job_id && job_ids[1] == job_id))
    );

    // Get job metadata should get correct value
    spider::core::JobMetadata job_metadata{};
    REQUIRE(storage->get_job_metadata(*conn, job_id, &job_metadata).success());
    REQUIRE(job_id == job_metadata.get_id());
    REQUIRE(client_id == job_metadata.get_client_id());
    std::chrono::seconds const time_delta{1};
    // REQUIRE(job_creation_time + time_delta >= job_metadata.get_creation_time());
    // REQUIRE(job_creation_time - time_delta <= job_metadata.get_creation_time());

    // Get task graph should succeed
    spider::core::TaskGraph graph_res{};
    REQUIRE(storage->get_task_graph(*conn, job_id, &graph_res).success());
    REQUIRE(spider::test::task_graph_equal(graph, graph_res));
    spider::core::TaskGraph simple_graph_res{};
    REQUIRE(storage->get_task_graph(*conn, simple_job_id, &simple_graph_res).success());
    REQUIRE(spider::test::task_graph_equal(simple_graph, simple_graph_res));

    // Get task should succeed
    spider::core::Task task_res{""};
    REQUIRE(storage->get_task(*conn, child_task.get_id(), &task_res).success());
    REQUIRE(spider::test::task_equal(child_task, task_res));

    // Get child tasks should succeed
    std::vector<spider::core::Task> tasks;
    REQUIRE(storage->get_child_tasks(*conn, parent_1.get_id(), &tasks).success());
    REQUIRE(1 == tasks.size());
    REQUIRE(spider::test::task_equal(child_task, tasks[0]));
    tasks.clear();

    // Get parent tasks should succeed
    REQUIRE(storage->get_parent_tasks(*conn, child_task.get_id(), &tasks).success());
    REQUIRE(2 == tasks.size());
    REQUIRE(
            ((spider::test::task_equal(tasks[0], parent_1)
              && spider::test::task_equal(tasks[1], parent_2))
             || (spider::test::task_equal(tasks[0], parent_2)
                 && spider::test::task_equal(tasks[1], parent_1)))
    );

    // Remove job should succeed
    REQUIRE(storage->remove_job(*conn, simple_job_id).success());
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr
            == storage->get_task_graph(*conn, simple_job_id, &simple_graph_res).type);
    graph_res = spider::core::TaskGraph{};
    REQUIRE(storage->get_task_graph(*conn, job_id, &graph_res).success());
    REQUIRE(spider::test::task_graph_equal(graph, graph_res));
    REQUIRE(storage->remove_job(*conn, job_id).success());
}

TEMPLATE_LIST_TEST_CASE("Task finish", "[storage]", spider::test::StorageFactoryTypeList) {
    std::unique_ptr<spider::core::StorageFactory> storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::unique_ptr<spider::core::MetadataStorage> storage
            = storage_factory->provide_metadata_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    auto conn = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    boost::uuids::random_generator gen;
    boost::uuids::uuid const job_id = gen();

    // Create a complicated task graph
    spider::core::Task child_task{"child"};
    spider::core::Task parent_1{"p1"};
    spider::core::Task parent_2{"p2"};
    parent_1.add_input(spider::core::TaskInput{"1", "float"});
    parent_1.add_input(spider::core::TaskInput{"2", "float"});
    parent_2.add_input(spider::core::TaskInput{"3", "int"});
    parent_2.add_input(spider::core::TaskInput{"4", "int"});
    parent_1.add_output(spider::core::TaskOutput{"float"});
    parent_2.add_output(spider::core::TaskOutput{"int"});
    child_task.add_input(spider::core::TaskInput{parent_1.get_id(), 0, "float"});
    child_task.add_input(spider::core::TaskInput{parent_2.get_id(), 0, "int"});
    child_task.add_output(spider::core::TaskOutput{"float"});
    spider::core::TaskGraph graph;
    // Add task and dependencies to task graph in wrong order
    graph.add_task(child_task);
    graph.add_task(parent_1);
    graph.add_task(parent_2);
    graph.add_dependency(parent_2.get_id(), child_task.get_id());
    graph.add_dependency(parent_1.get_id(), child_task.get_id());
    graph.add_input_task(parent_1.get_id());
    graph.add_input_task(parent_2.get_id());
    graph.add_output_task(child_task.get_id());
    // Submit job should success
    REQUIRE(storage->add_job(*conn, job_id, gen(), graph).success());

    // Task finish for parent 1 should succeed
    spider::core::TaskInstance const parent_1_instance{gen(), parent_1.get_id()};
    REQUIRE(storage->set_task_state(*conn, parent_1.get_id(), spider::core::TaskState::Running)
                    .success());
    REQUIRE(storage->task_finish(
                           *conn,
                           parent_1_instance,
                           {spider::core::TaskOutput{"1.1", "float"}}
    )
                    .success());
    // Parent 1 finish should not update state of any other tasks
    spider::core::Task res_task{""};
    REQUIRE(storage->get_task(*conn, parent_2.get_id(), &res_task).success());
    REQUIRE(spider::test::task_equal(parent_2, res_task));
    REQUIRE(res_task.get_state() == spider::core::TaskState::Ready);
    REQUIRE(storage->get_task(*conn, child_task.get_id(), &res_task).success());
    REQUIRE(res_task.get_state() == spider::core::TaskState::Pending);

    // Task finish for parent 2 should success
    spider::core::TaskInstance const parent_2_instance{gen(), parent_2.get_id()};
    REQUIRE(storage->set_task_state(*conn, parent_2.get_id(), spider::core::TaskState::Running)
                    .success());
    REQUIRE(storage->task_finish(*conn, parent_2_instance, {spider::core::TaskOutput{"2", "int"}})
                    .success());
    // Parent 2 finish should update state of child
    REQUIRE(storage->get_task(*conn, child_task.get_id(), &res_task).success());
    REQUIRE(res_task.get_input(0).get_value() == "1.1");
    REQUIRE(res_task.get_input(1).get_value() == "2");
    REQUIRE(res_task.get_state() == spider::core::TaskState::Ready);

    // Clean up
    REQUIRE(storage->remove_job(*conn, job_id).success());
}

TEMPLATE_LIST_TEST_CASE("Job reset", "[storage]", spider::test::StorageFactoryTypeList) {
    std::unique_ptr<spider::core::StorageFactory> storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::unique_ptr<spider::core::MetadataStorage> storage
            = storage_factory->provide_metadata_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    auto conn = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    boost::uuids::random_generator gen;
    boost::uuids::uuid const job_id = gen();

    // Create a complicated task graph
    spider::core::Task child_task{"child"};
    spider::core::Task parent_1{"p1"};
    spider::core::Task parent_2{"p2"};
    parent_1.add_input(spider::core::TaskInput{"1", "float"});
    parent_1.add_input(spider::core::TaskInput{"2", "float"});
    parent_2.add_input(spider::core::TaskInput{"3", "int"});
    parent_2.add_input(spider::core::TaskInput{"4", "int"});
    parent_1.add_output(spider::core::TaskOutput{"float"});
    parent_2.add_output(spider::core::TaskOutput{"int"});
    child_task.add_input(spider::core::TaskInput{parent_1.get_id(), 0, "float"});
    child_task.add_input(spider::core::TaskInput{parent_2.get_id(), 0, "int"});
    child_task.add_output(spider::core::TaskOutput{"float"});
    parent_1.set_max_retries(1);
    parent_2.set_max_retries(1);
    child_task.set_max_retries(1);
    spider::core::TaskGraph graph;
    // Add task and dependencies to task graph in wrong order
    graph.add_task(child_task);
    graph.add_task(parent_1);
    graph.add_task(parent_2);
    graph.add_dependency(parent_2.get_id(), child_task.get_id());
    graph.add_dependency(parent_1.get_id(), child_task.get_id());
    graph.add_input_task(parent_1.get_id());
    graph.add_input_task(parent_2.get_id());
    graph.add_output_task(child_task.get_id());
    // Submit job should success
    REQUIRE(storage->add_job(*conn, job_id, gen(), graph).success());

    // Task finish for parent 1 should succeed
    spider::core::TaskInstance const parent_1_instance{gen(), parent_1.get_id()};
    REQUIRE(storage->set_task_state(*conn, parent_1.get_id(), spider::core::TaskState::Running)
                    .success());
    REQUIRE(storage->task_finish(
                           *conn,
                           parent_1_instance,
                           {spider::core::TaskOutput{"1.1", "float"}}
    )
                    .success());
    // Task finish for parent 2 should success
    spider::core::TaskInstance const parent_2_instance{gen(), parent_2.get_id()};
    REQUIRE(storage->set_task_state(*conn, parent_2.get_id(), spider::core::TaskState::Running)
                    .success());
    REQUIRE(storage->task_finish(*conn, parent_2_instance, {spider::core::TaskOutput{"2", "int"}})
                    .success());
    // Task finish for child should success
    spider::core::TaskInstance const child_instance{gen(), child_task.get_id()};
    REQUIRE(storage->set_task_state(*conn, child_task.get_id(), spider::core::TaskState::Running)
                    .success());
    REQUIRE(storage->task_finish(*conn, child_instance, {spider::core::TaskOutput{"3.3", "float"}})
                    .success());

    // Job reset
    REQUIRE(storage->reset_job(*conn, job_id).success());
    // Parent tasks states should be ready and child task state should be waiting
    // Parent tasks inputs should be available and child task inputs should be empty
    // All tasks output should be empty
    spider::core::Task res_task{""};
    REQUIRE(storage->get_task(*conn, parent_1.get_id(), &res_task).success());
    REQUIRE(res_task.get_state() == spider::core::TaskState::Ready);
    REQUIRE(res_task.get_num_inputs() == 2);
    REQUIRE(res_task.get_input(0).get_value() == "1");
    REQUIRE(res_task.get_input(1).get_value() == "2");
    REQUIRE(res_task.get_num_outputs() == 1);
    REQUIRE(!res_task.get_output(0).get_value().has_value());
    REQUIRE(storage->get_task(*conn, parent_2.get_id(), &res_task).success());
    REQUIRE(res_task.get_state() == spider::core::TaskState::Ready);
    REQUIRE(res_task.get_num_inputs() == 2);
    REQUIRE(res_task.get_input(0).get_value() == "3");
    REQUIRE(res_task.get_input(1).get_value() == "4");
    REQUIRE(res_task.get_num_outputs() == 1);
    REQUIRE(!res_task.get_output(0).get_value().has_value());
    REQUIRE(storage->get_task(*conn, child_task.get_id(), &res_task).success());
    REQUIRE(res_task.get_state() == spider::core::TaskState::Pending);
    REQUIRE(res_task.get_num_inputs() == 2);
    REQUIRE(!res_task.get_input(0).get_value().has_value());
    REQUIRE(!res_task.get_input(1).get_value().has_value());
    REQUIRE(res_task.get_num_outputs() == 1);
    REQUIRE(!res_task.get_output(0).get_value().has_value());

    // Clean up
    REQUIRE(storage->remove_job(*conn, job_id).success());
}

TEMPLATE_LIST_TEST_CASE(
        "Scheduler lease timeout",
        "[storage]",
        spider::test::StorageFactoryTypeList
) {
    std::unique_ptr<spider::core::StorageFactory> storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::unique_ptr<spider::core::MetadataStorage> storage
            = storage_factory->provide_metadata_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    auto conn = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    boost::uuids::random_generator gen;

    // Register scheduler
    boost::uuids::uuid const scheduler_id = gen();
    constexpr int cPort = 3306;
    REQUIRE(storage->add_scheduler(*conn, spider::core::Scheduler{scheduler_id, "127.0.0.1", cPort})
                    .success());

    // Add simple job
    boost::uuids::uuid const job_id = gen();
    spider::core::Task const task{"simple"};
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_input_task(task.get_id());
    graph.add_output_task(task.get_id());
    REQUIRE(storage->add_job(*conn, job_id, gen(), graph).success());

    // Get ready tasks should schedule the task
    std::vector<spider::core::ScheduleTaskMetadata> tasks;
    REQUIRE(storage->get_ready_tasks(*conn, scheduler_id, &tasks).success());
    REQUIRE(1 == tasks.size());
    REQUIRE(tasks[0].get_id() == task.get_id());

    // Get again should not schedule the task
    tasks.clear();
    REQUIRE(storage->get_ready_tasks(*conn, scheduler_id, &tasks).success());
    REQUIRE(tasks.empty());

    // Wait for lease timeout
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // Get ready tasks should schedule the task again
    tasks.clear();
    REQUIRE(storage->get_ready_tasks(*conn, scheduler_id, &tasks).success());
    REQUIRE(1 == tasks.size());
    REQUIRE(tasks[0].get_id() == task.get_id());

    // Clean up
    REQUIRE(storage->remove_job(*conn, job_id).success());
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
