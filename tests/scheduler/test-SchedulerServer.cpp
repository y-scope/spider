// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-optin.core.EnumCastOutOfRange)
#include <chrono>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/core/Error.hpp"
#include "../../src/spider/core/Task.hpp"
#include "../../src/spider/core/TaskGraph.hpp"
#include "../../src/spider/io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../../src/spider/io/MsgPack.hpp"  // IWYU pragma: keep
#include "../../src/spider/io/msgpack_message.hpp"
#include "../../src/spider/scheduler/FifoPolicy.hpp"
#include "../../src/spider/scheduler/SchedulerMessage.hpp"
#include "../../src/spider/scheduler/SchedulerPolicy.hpp"
#include "../../src/spider/scheduler/SchedulerServer.hpp"
#include "../../src/spider/storage/DataStorage.hpp"
#include "../../src/spider/storage/MetadataStorage.hpp"
#include "../../src/spider/storage/StorageConnection.hpp"
#include "../../src/spider/storage/StorageFactory.hpp"
#include "../storage/StorageTestHelper.hpp"

namespace {
constexpr int cServerWarmupTime = 5;

TEMPLATE_LIST_TEST_CASE(
        "Scheduler server test",
        "[scheduler][server][storage]",
        spider::test::StorageFactoryTypeList
) {
    std::unique_ptr<spider::core::StorageFactory> storage_factory
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

    // Add scheduler
    boost::uuids::random_generator gen;
    boost::uuids::uuid const scheduler_id = gen();
    REQUIRE(metadata_store
                    ->add_scheduler(*conn, spider::core::Scheduler{scheduler_id, "127.0.0.1", 8080})
                    .success());

    std::shared_ptr<spider::scheduler::SchedulerPolicy> const policy
            = std::make_shared<spider::scheduler::FifoPolicy>(
                    scheduler_id,
                    metadata_store,
                    data_store,
                    conn
            );

    constexpr unsigned short cPort = 6021;
    spider::scheduler::SchedulerServer server{cPort, policy, metadata_store, data_store, conn};

    // Pause and resume server
    server.pause();
    server.resume();
    // Sleep for a while to let the server start
    std::this_thread::sleep_for(std::chrono::milliseconds(cServerWarmupTime));

    // Create client socket
    boost::asio::io_context context;
    boost::asio::ip::tcp::endpoint const endpoint{boost::asio::ip::tcp::v4(), cPort};
    boost::asio::ip::tcp::socket socket{context};
    boost::asio::connect(socket, std::vector{endpoint});

    // Add task to storage
    spider::core::Task const parent_task{"parent"};
    spider::core::Task const child_task{"child"};
    spider::core::TaskGraph graph;
    graph.add_task(parent_task);
    graph.add_task(child_task);
    graph.add_dependency(parent_task.get_id(), child_task.get_id());
    graph.add_input_task(parent_task.get_id());
    graph.add_output_task(child_task.get_id());
    boost::uuids::uuid const job_id = gen();
    REQUIRE(metadata_store->add_job(*conn, job_id, gen(), graph).success());

    // Schedule request should succeed
    spider::scheduler::ScheduleTaskRequest const req{gen(), ""};
    msgpack::sbuffer req_buffer;
    msgpack::pack(req_buffer, req);
    REQUIRE(spider::core::send_message(socket, req_buffer));

    // Pause and resume server
    server.pause();
    server.resume();
    std::this_thread::sleep_for(std::chrono::milliseconds(cServerWarmupTime));

    // Get response should succeed and get child task
    std::optional<msgpack::sbuffer> const& res_buffer = spider::core::receive_message(socket);
    REQUIRE(metadata_store->remove_job(*conn, job_id).success());
    REQUIRE(res_buffer.has_value());
    if (res_buffer.has_value()) {
        msgpack::object_handle const handle
                = msgpack::unpack(res_buffer.value().data(), res_buffer.value().size());
        msgpack::object const object = handle.get();
        spider::scheduler::ScheduleTaskResponse const res
                = object.as<spider::scheduler::ScheduleTaskResponse>();
        REQUIRE(res.has_task_id());
        REQUIRE(res.get_task_id() == parent_task.get_id());
    }
    socket.close();
    server.stop();
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-optin.core.EnumCastOutOfRange)
