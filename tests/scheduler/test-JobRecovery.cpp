// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-optin.core.EnumCastOutOfRange)

#include <algorithm>
#include <memory>
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
    REQUIRE(metadata_store->remove_driver(*conn, client_id).success());
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
    spider::core::Data const data{"data"};
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
    auto ready_tasks = recovery.get_ready_tasks();
    auto pending_tasks = recovery.get_pending_tasks();
    REQUIRE(ready_tasks.size() == 1);
    REQUIRE(pending_tasks.empty());
    REQUIRE(ready_tasks[0] == task.get_id());

    REQUIRE(metadata_store->remove_job(*conn, job_id).success());
    REQUIRE(data_store->remove_data(*conn, data.get_id()).success());
    REQUIRE(metadata_store->remove_driver(*conn, client_id).success());
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
    auto ready_tasks = recovery.get_ready_tasks();
    auto pending_tasks = recovery.get_pending_tasks();
    REQUIRE(ready_tasks.size() == 1);
    REQUIRE(pending_tasks.empty());
    REQUIRE(ready_tasks[0] == task.get_id());

    REQUIRE(metadata_store->remove_job(*conn, job_id).success());
    REQUIRE(data_store->remove_data(*conn, data.get_id()).success());
    REQUIRE(metadata_store->remove_driver(*conn, client_id).success());
}

/**
 * Recovers a job with multiple tasks. The task graph is:
 * \dot
 * digraph task_graph {
 *     node [shape="rect"];
 *     1 [color="green"];
 *     2 [color="green"];
 *     3 [color="green"];
 *     4 [color="green"];
 *     6 [color="blue"];
 *     7 [color="blue"];
 *     1 -> 3;
 *     1 -> 4;
 *     2 -> 4;
 *     2 -> 5;
 *     3 -> 6 [style="dashed"];
 *     4 -> 7 [style="dashed"];
 *     subgraph cluster_recovery {
 *         style=filled;
 *         color=yellow;
 *         5 [color="green"];
 *         8 [color="red"];
 *         5 -> 8 [style="dashed"];
 *     }
 * }
 * \enddot
 */
TEMPLATE_LIST_TEST_CASE(
        "Recovery multiple tasks",
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
    // Build task graph with multiple tasks
    spider::core::Task task1{"task1"};
    task1.add_input(spider::core::TaskInput{"10", "int"});
    spider::core::Data data1{"data1"};
    data1.set_persisted(true);
    task1.add_output(spider::core::TaskOutput{data1.get_id()});
    spider::core::Task task2{"task2"};
    task2.add_input(spider::core::TaskInput{"10", "int"});
    spider::core::Data data2{"data2"};
    data2.set_persisted(true);
    task2.add_output(spider::core::TaskOutput{data2.get_id()});
    spider::core::Task task3{"task3"};
    task3.add_input(spider::core::TaskInput{task1.get_id(), 0, ""});
    spider::core::Data const data3{"data3"};
    task3.add_output(spider::core::TaskOutput{data3.get_id()});
    spider::core::Task task4{"task4"};
    task4.add_input(spider::core::TaskInput{task1.get_id(), 0, ""});
    task4.add_input(spider::core::TaskInput{task2.get_id(), 0, ""});
    spider::core::Data const data4{"data4"};
    task4.add_output(spider::core::TaskOutput{data4.get_id()});
    spider::core::Task task5{"task5"};
    task5.add_input(spider::core::TaskInput{task2.get_id(), 0, ""});
    spider::core::Data const data5{"data5"};
    task5.add_output(spider::core::TaskOutput{data5.get_id()});
    spider::core::Task task6{"task6"};
    task6.add_input(spider::core::TaskInput{task3.get_id(), 0, ""});
    task6.add_output(spider::core::TaskOutput{"int"});
    spider::core::Task task7{"task7"};
    task7.add_input(spider::core::TaskInput{task4.get_id(), 0, ""});
    task7.add_output(spider::core::TaskOutput{"int"});
    spider::core::Task task8{"task8"};
    task8.add_input(spider::core::TaskInput{task5.get_id(), 0, ""});
    task8.add_output(spider::core::TaskOutput{"int"});
    spider::core::TaskGraph graph;
    graph.add_task(task1);
    graph.add_task(task2);
    graph.add_task(task3);
    graph.add_task(task4);
    graph.add_task(task5);
    graph.add_task(task6);
    graph.add_task(task7);
    graph.add_task(task8);
    graph.add_input_task(task1.get_id());
    graph.add_input_task(task2.get_id());
    graph.add_output_task(task6.get_id());
    graph.add_output_task(task7.get_id());
    graph.add_output_task(task8.get_id());
    graph.add_dependency(task1.get_id(), task3.get_id());
    graph.add_dependency(task1.get_id(), task4.get_id());
    graph.add_dependency(task2.get_id(), task4.get_id());
    graph.add_dependency(task2.get_id(), task5.get_id());
    graph.add_dependency(task3.get_id(), task6.get_id());
    graph.add_dependency(task4.get_id(), task7.get_id());
    graph.add_dependency(task5.get_id(), task8.get_id());
    REQUIRE(metadata_store->add_driver(*conn, spider::core::Driver{client_id}).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data1).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data2).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data3).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data4).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data5).success());
    REQUIRE(metadata_store->add_job(*conn, job_id, client_id, graph).success());
    REQUIRE(metadata_store->set_task_running(*conn, task1.get_id()).success());
    REQUIRE(metadata_store
                    ->task_finish(
                            *conn,
                            spider::core::TaskInstance{task1.get_id()},
                            {spider::core::TaskOutput{data1.get_id()}}
                    )
                    .success());
    REQUIRE(metadata_store->set_task_running(*conn, task2.get_id()).success());
    REQUIRE(metadata_store
                    ->task_finish(
                            *conn,
                            spider::core::TaskInstance{task2.get_id()},
                            {spider::core::TaskOutput{data2.get_id()}}
                    )
                    .success());
    REQUIRE(metadata_store->set_task_running(*conn, task3.get_id()).success());
    REQUIRE(metadata_store
                    ->task_finish(
                            *conn,
                            spider::core::TaskInstance{task3.get_id()},
                            {spider::core::TaskOutput{data3.get_id()}}
                    )
                    .success());
    REQUIRE(metadata_store->set_task_running(*conn, task4.get_id()).success());
    REQUIRE(metadata_store
                    ->task_finish(
                            *conn,
                            spider::core::TaskInstance{task4.get_id()},
                            {spider::core::TaskOutput{data4.get_id()}}
                    )
                    .success());
    REQUIRE(metadata_store->set_task_running(*conn, task5.get_id()).success());
    REQUIRE(metadata_store
                    ->task_finish(
                            *conn,
                            spider::core::TaskInstance{task5.get_id()},
                            {spider::core::TaskOutput{data5.get_id()}}
                    )
                    .success());

    REQUIRE(metadata_store->set_task_state(*conn, task8.get_id(), spider::core::TaskState::Failed)
                    .success());

    spider::core::JobRecovery recovery{job_id, conn, data_store, metadata_store};
    REQUIRE(recovery.compute_graph().success());
    auto ready_tasks = recovery.get_ready_tasks();
    auto pending_tasks = recovery.get_pending_tasks();
    REQUIRE(ready_tasks.size() == 1);
    REQUIRE(ready_tasks[0] == task5.get_id());
    REQUIRE(pending_tasks.size() == 1);
    REQUIRE(pending_tasks[0] == task8.get_id());

    REQUIRE(metadata_store->remove_job(*conn, job_id).success());
    REQUIRE(data_store->remove_data(*conn, data1.get_id()).success());
    REQUIRE(data_store->remove_data(*conn, data2.get_id()).success());
    REQUIRE(data_store->remove_data(*conn, data3.get_id()).success());
    REQUIRE(data_store->remove_data(*conn, data4.get_id()).success());
    REQUIRE(data_store->remove_data(*conn, data5.get_id()).success());
    REQUIRE(metadata_store->remove_driver(*conn, client_id).success());
}

/**
 * Recovers a job with multiple tasks. The task graph is:
 * \dot
 * digraph task_graph {
 *     node [shape="rect"];
 *     1 [color="green"];
 *     3 [color="green"];
 *     6 [color="blue"];
 *     7 [color="blue"];
 *     1 -> 3;
 *     1 -> 4;
 *     3 -> 6 [style="dashed"];
 *     4 -> 7 [style="dashed"];
 *     subgraph cluster_recovery {
 *         style=filled;
 *         color=yellow;
 *         2 [color="green"];
 *         4 [color="blue"]
 *         5 [color="green"];
 *         8 [color="red"];
 *         2 -> 4 [style="dashed"];
 *         2 -> 5 [style="dashed"];
 *         5 -> 8 [style="dashed"];
 *     }
 * }
 * \enddot
 */
TEMPLATE_LIST_TEST_CASE(
        "Recovery multiple tasks with children",
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
    // Build task graph with multiple tasks
    spider::core::Task task1{"task1"};
    task1.add_input(spider::core::TaskInput{"10", "int"});
    spider::core::Data data1{"data1"};
    data1.set_persisted(true);
    task1.add_output(spider::core::TaskOutput{data1.get_id()});
    spider::core::Task task2{"task2"};
    task2.add_input(spider::core::TaskInput{"10", "int"});
    spider::core::Data const data2{"data2"};
    task2.add_output(spider::core::TaskOutput{data2.get_id()});
    spider::core::Task task3{"task3"};
    task3.add_input(spider::core::TaskInput{task1.get_id(), 0, ""});
    spider::core::Data const data3{"data3"};
    task3.add_output(spider::core::TaskOutput{data3.get_id()});
    spider::core::Task task4{"task4"};
    task4.add_input(spider::core::TaskInput{task1.get_id(), 0, ""});
    task4.add_input(spider::core::TaskInput{task2.get_id(), 0, ""});
    spider::core::Data const data4{"data4"};
    task4.add_output(spider::core::TaskOutput{data4.get_id()});
    spider::core::Task task5{"task5"};
    task5.add_input(spider::core::TaskInput{task2.get_id(), 0, ""});
    spider::core::Data const data5{"data5"};
    task5.add_output(spider::core::TaskOutput{data5.get_id()});
    spider::core::Task task6{"task6"};
    task6.add_input(spider::core::TaskInput{task3.get_id(), 0, ""});
    task6.add_output(spider::core::TaskOutput{"int"});
    spider::core::Task task7{"task7"};
    task7.add_input(spider::core::TaskInput{task4.get_id(), 0, ""});
    task7.add_output(spider::core::TaskOutput{"int"});
    spider::core::Task task8{"task8"};
    task8.add_input(spider::core::TaskInput{task5.get_id(), 0, ""});
    task8.add_output(spider::core::TaskOutput{"int"});
    spider::core::TaskGraph graph;
    graph.add_task(task1);
    graph.add_task(task2);
    graph.add_task(task3);
    graph.add_task(task4);
    graph.add_task(task5);
    graph.add_task(task6);
    graph.add_task(task7);
    graph.add_task(task8);
    graph.add_input_task(task1.get_id());
    graph.add_input_task(task2.get_id());
    graph.add_output_task(task6.get_id());
    graph.add_output_task(task7.get_id());
    graph.add_output_task(task8.get_id());
    graph.add_dependency(task1.get_id(), task3.get_id());
    graph.add_dependency(task1.get_id(), task4.get_id());
    graph.add_dependency(task2.get_id(), task4.get_id());
    graph.add_dependency(task2.get_id(), task5.get_id());
    graph.add_dependency(task3.get_id(), task6.get_id());
    graph.add_dependency(task4.get_id(), task7.get_id());
    graph.add_dependency(task5.get_id(), task8.get_id());
    REQUIRE(metadata_store->add_driver(*conn, spider::core::Driver{client_id}).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data1).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data2).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data3).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data4).success());
    REQUIRE(data_store->add_driver_data(*conn, client_id, data5).success());
    REQUIRE(metadata_store->add_job(*conn, job_id, client_id, graph).success());
    REQUIRE(metadata_store->set_task_running(*conn, task1.get_id()).success());
    REQUIRE(metadata_store
                    ->task_finish(
                            *conn,
                            spider::core::TaskInstance{task1.get_id()},
                            {spider::core::TaskOutput{data1.get_id()}}
                    )
                    .success());
    REQUIRE(metadata_store->set_task_running(*conn, task2.get_id()).success());
    REQUIRE(metadata_store
                    ->task_finish(
                            *conn,
                            spider::core::TaskInstance{task2.get_id()},
                            {spider::core::TaskOutput{data2.get_id()}}
                    )
                    .success());
    REQUIRE(metadata_store->set_task_running(*conn, task3.get_id()).success());
    REQUIRE(metadata_store
                    ->task_finish(
                            *conn,
                            spider::core::TaskInstance{task3.get_id()},
                            {spider::core::TaskOutput{data3.get_id()}}
                    )
                    .success());
    REQUIRE(metadata_store->set_task_running(*conn, task5.get_id()).success());
    REQUIRE(metadata_store
                    ->task_finish(
                            *conn,
                            spider::core::TaskInstance{task5.get_id()},
                            {spider::core::TaskOutput{data5.get_id()}}
                    )
                    .success());

    REQUIRE(metadata_store->set_task_state(*conn, task8.get_id(), spider::core::TaskState::Failed)
                    .success());

    spider::core::JobRecovery recovery{job_id, conn, data_store, metadata_store};
    REQUIRE(recovery.compute_graph().success());
    auto ready_tasks = recovery.get_ready_tasks();
    auto pending_tasks = recovery.get_pending_tasks();
    REQUIRE(ready_tasks.size() == 1);
    REQUIRE(ready_tasks[0] == task2.get_id());
    REQUIRE(pending_tasks.size() == 3);
    REQUIRE(pending_tasks.end() != std::ranges::find(pending_tasks, task4.get_id()));
    REQUIRE(pending_tasks.end() != std::ranges::find(pending_tasks, task5.get_id()));
    REQUIRE(pending_tasks.end() != std::ranges::find(pending_tasks, task8.get_id()));

    REQUIRE(metadata_store->remove_job(*conn, job_id).success());
    REQUIRE(data_store->remove_data(*conn, data1.get_id()).success());
    REQUIRE(data_store->remove_data(*conn, data2.get_id()).success());
    REQUIRE(data_store->remove_data(*conn, data3.get_id()).success());
    REQUIRE(data_store->remove_data(*conn, data4.get_id()).success());
    REQUIRE(data_store->remove_data(*conn, data5.get_id()).success());
    REQUIRE(metadata_store->remove_driver(*conn, client_id).success());
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-optin.core.EnumCastOutOfRange)
