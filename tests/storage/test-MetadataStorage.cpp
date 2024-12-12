// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <algorithm>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <absl/container/flat_hash_set.h>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/core/Driver.hpp"
#include "../../src/spider/core/Error.hpp"
#include "../../src/spider/core/JobMetadata.hpp"
#include "../../src/spider/core/Task.hpp"
#include "../../src/spider/core/TaskGraph.hpp"
#include "../../src/spider/storage/MetadataStorage.hpp"
#include "../utils/CoreTaskUtils.hpp"
#include "StorageTestHelper.hpp"

namespace {

TEMPLATE_LIST_TEST_CASE("Driver heartbeat", "[storage]", spider::test::MetadataStorageTypeList) {
    std::unique_ptr<spider::core::MetadataStorage> storage
            = spider::test::create_metadata_storage<TestType>();

    constexpr double cDuration = 100;

    // Add driver should succeed
    boost::uuids::random_generator gen;
    boost::uuids::uuid const driver_id = gen();
    REQUIRE(storage->add_driver(spider::core::Driver{driver_id, "127.0.0.1"}).success());

    std::string addr;
    REQUIRE(storage->get_driver(driver_id, &addr).success());
    REQUIRE("127.0.0.1" == addr);

    std::vector<boost::uuids::uuid> ids{};
    // Driver should not time out
    REQUIRE(storage->heartbeat_timeout(cDuration, &ids).success());
    // Because other tests may run in parallel, just check `ids` don't have `driver_id`
    REQUIRE(std::ranges::none_of(ids, [&driver_id](boost::uuids::uuid id) {
        return id == driver_id;
    }));
    ids.clear();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    // Driver should time out
    REQUIRE(storage->heartbeat_timeout(cDuration, &ids).success());
    REQUIRE(!ids.empty());
    REQUIRE(std::ranges::any_of(ids, [&driver_id](boost::uuids::uuid id) {
        return id == driver_id;
    }));
    ids.clear();

    // Update heartbeat
    REQUIRE(storage->update_heartbeat(driver_id).success());
    // Driver should not time out
    REQUIRE(storage->heartbeat_timeout(cDuration, &ids).success());
    REQUIRE(std::ranges::none_of(ids, [&driver_id](boost::uuids::uuid id) {
        return id == driver_id;
    }));
}

TEMPLATE_LIST_TEST_CASE(
        "Scheduler state and addr",
        "[storage]",
        spider::test::MetadataStorageTypeList
) {
    std::unique_ptr<spider::core::MetadataStorage> storage
            = spider::test::create_metadata_storage<TestType>();

    boost::uuids::random_generator gen;
    boost::uuids::uuid const scheduler_id = gen();
    constexpr int cPort = 3306;

    // Add scheduler should succeed
    REQUIRE(storage->add_scheduler(spider::core::Scheduler{scheduler_id, "127.0.0.1", cPort})
                    .success());

    // Get scheduler addr should succeed
    std::string addr_res;
    int port_res = 0;
    REQUIRE(storage->get_scheduler_addr(scheduler_id, &addr_res, &port_res).success());
    REQUIRE(addr_res == "127.0.0.1");
    REQUIRE(port_res == cPort);

    // Get non-exist scheduler should fail
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr
            == storage->get_scheduler_addr(gen(), &addr_res, &port_res).type);

    // Get default state
    std::string state_res;
    REQUIRE(storage->get_scheduler_state(scheduler_id, &state_res).success());
    REQUIRE(state_res == "normal");
    state_res.clear();

    // Update scheduler state should succeed
    std::string state = "recovery";
    REQUIRE(storage->set_scheduler_state(scheduler_id, state).success());

    // Get new state
    REQUIRE(storage->get_scheduler_state(scheduler_id, &state_res).success());
    REQUIRE(state_res == state);
}

TEMPLATE_LIST_TEST_CASE(
        "Job add, get and remove",
        "[storage]",
        spider::test::MetadataStorageTypeList
) {
    std::unique_ptr<spider::core::MetadataStorage> storage
            = spider::test::create_metadata_storage<TestType>();

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

    // Get head tasks should succeed
    absl::flat_hash_set<boost::uuids::uuid> heads = graph.get_head_tasks();
    REQUIRE(2 == heads.size());
    REQUIRE(heads.contains(parent_1.get_id()));
    REQUIRE(heads.contains(parent_2.get_id()));

    std::chrono::system_clock::time_point const job_creation_time
            = std::chrono::system_clock::now();

    // Submit a simple job
    boost::uuids::uuid const simple_job_id = gen();
    spider::core::Task const simple_task{"simple"};
    spider::core::TaskGraph simple_graph;
    simple_graph.add_task(simple_task);

    heads = simple_graph.get_head_tasks();
    REQUIRE(1 == heads.size());
    REQUIRE(heads.contains(simple_task.get_id()));

    // Submit job should success
    REQUIRE(storage->add_job(job_id, client_id, graph).success());
    REQUIRE(storage->add_job(simple_job_id, client_id, simple_graph).success());

    // Get job id for non-existent client id should return empty vector
    std::vector<boost::uuids::uuid> job_ids;
    REQUIRE(storage->get_jobs_by_client_id(gen(), &job_ids).success());
    REQUIRE(job_ids.empty());

    // Get job id for client id should get correct value
    REQUIRE(storage->get_jobs_by_client_id(client_id, &job_ids).success());
    REQUIRE(2 == job_ids.size());
    REQUIRE(
            ((job_ids[0] == job_id && job_ids[1] == simple_job_id)
             || (job_ids[0] == simple_job_id && job_ids[1] == job_id))
    );

    // Get job metadata should get correct value
    spider::core::JobMetadata job_metadata{};
    REQUIRE(storage->get_job_metadata(job_id, &job_metadata).success());
    REQUIRE(job_id == job_metadata.get_id());
    REQUIRE(client_id == job_metadata.get_client_id());
    std::chrono::seconds const time_delta{1};
    // REQUIRE(job_creation_time + time_delta >= job_metadata.get_creation_time());
    // REQUIRE(job_creation_time - time_delta <= job_metadata.get_creation_time());

    // Get task graph should succeed
    spider::core::TaskGraph graph_res{};
    REQUIRE(storage->get_task_graph(job_id, &graph_res).success());
    REQUIRE(spider::test::task_graph_equal(graph, graph_res));
    spider::core::TaskGraph simple_graph_res{};
    REQUIRE(storage->get_task_graph(simple_job_id, &simple_graph_res).success());
    REQUIRE(spider::test::task_graph_equal(simple_graph, simple_graph_res));

    // Get task should succeed
    spider::core::Task task_res{""};
    REQUIRE(storage->get_task(child_task.get_id(), &task_res).success());
    REQUIRE(spider::test::task_equal(child_task, task_res));

    // Get child tasks should succeed
    std::vector<spider::core::Task> tasks;
    REQUIRE(storage->get_child_tasks(parent_1.get_id(), &tasks).success());
    REQUIRE(1 == tasks.size());
    REQUIRE(spider::test::task_equal(child_task, tasks[0]));
    tasks.clear();

    // Get parent tasks should succeed
    REQUIRE(storage->get_parent_tasks(child_task.get_id(), &tasks).success());
    REQUIRE(2 == tasks.size());
    REQUIRE(
            ((spider::test::task_equal(tasks[0], parent_1)
              && spider::test::task_equal(tasks[1], parent_2))
             || (spider::test::task_equal(tasks[0], parent_2)
                 && spider::test::task_equal(tasks[1], parent_1)))
    );

    // Remove job should succeed
    REQUIRE(storage->remove_job(simple_job_id).success());
    REQUIRE(spider::core::StorageErrType::KeyNotFoundErr
            == storage->get_task_graph(simple_job_id, &simple_graph_res).type);
    graph_res = spider::core::TaskGraph{};
    REQUIRE(storage->get_task_graph(job_id, &graph_res).success());
    REQUIRE(spider::test::task_graph_equal(graph, graph_res));
    REQUIRE(storage->remove_job(job_id).success());
}

TEMPLATE_LIST_TEST_CASE("Task finish", "[storage]", spider::test::MetadataStorageTypeList) {
    std::unique_ptr<spider::core::MetadataStorage> storage
            = spider::test::create_metadata_storage<TestType>();

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
    // Submit job should success
    REQUIRE(storage->add_job(job_id, gen(), graph).success());

    // Task finish for parent 1 should succeed
    spider::core::TaskInstance const parent_1_instance{gen(), parent_1.get_id()};
    REQUIRE(storage->set_task_state(parent_1.get_id(), spider::core::TaskState::Running).success());
    REQUIRE(storage->task_finish(parent_1_instance, {spider::core::TaskOutput{"1.1", "float"}})
                    .success());
    // Parent 1 finish should not update state of any other tasks
    spider::core::Task res_task{""};
    REQUIRE(storage->get_task(parent_2.get_id(), &res_task).success());
    REQUIRE(spider::test::task_equal(parent_2, res_task));
    REQUIRE(res_task.get_state() == spider::core::TaskState::Ready);
    REQUIRE(storage->get_task(child_task.get_id(), &res_task).success());
    REQUIRE(res_task.get_state() == spider::core::TaskState::Pending);

    // Task finish for parent 2 should success
    spider::core::TaskInstance const parent_2_instance{gen(), parent_2.get_id()};
    REQUIRE(storage->set_task_state(parent_2.get_id(), spider::core::TaskState::Running).success());
    REQUIRE(storage->task_finish(parent_2_instance, {spider::core::TaskOutput{"2", "int"}})
                    .success());
    // Parent 2 finish should update state of child
    REQUIRE(storage->get_task(child_task.get_id(), &res_task).success());
    REQUIRE(res_task.get_input(0).get_value() == "1.1");
    REQUIRE(res_task.get_input(1).get_value() == "2");
    REQUIRE(res_task.get_state() == spider::core::TaskState::Ready);

    // Clean up
    REQUIRE(storage->remove_job(job_id).success());
}

}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
