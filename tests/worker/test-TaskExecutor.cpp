#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/dll/runtime_symbol_info.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include <spider/core/Data.hpp>
#include <spider/core/Driver.hpp>
#include <spider/core/Error.hpp>
#include <spider/core/Task.hpp>
#include <spider/core/TaskGraph.hpp>
#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/storage/DataStorage.hpp>
#include <spider/storage/MetadataStorage.hpp>
#include <spider/storage/StorageConnection.hpp>
#include <spider/storage/StorageFactory.hpp>
#include <spider/worker/FunctionManager.hpp>
#include <spider/worker/TaskExecutor.hpp>

#include "tests/storage/StorageTestHelper.hpp"

// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-unix.BlockInCriticalSection)

namespace {
auto get_environment_variable() -> absl::flat_hash_map<
        boost::process::v2::environment::key,
        boost::process::v2::environment::value> {
    boost::filesystem::path const executable_dir = boost::dll::program_location().parent_path();
    boost::filesystem::path const src_dir = executable_dir.parent_path() / "src" / "spider";

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    char const* path_env_str = std::getenv("PATH");
    std::string path_env = nullptr == path_env_str ? "" : path_env_str;
    path_env.append(":");
    path_env.append(src_dir.string());

    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value>
            environment_variables;

    environment_variables.emplace("PATH", path_env);

    return environment_variables;
}

auto get_libraries() -> std::vector<std::string> {
    boost::filesystem::path const executable_dir = boost::dll::program_location().parent_path();
    boost::filesystem::path const lib_path = executable_dir / "libworker_test.so";
    return {lib_path.string()};
}

TEMPLATE_LIST_TEST_CASE(
        "Task execute success",
        "[worker][storage]",
        spider::test::StorageFactoryTypeList
) {
    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variable
            = get_environment_variable();

    boost::asio::io_context context;

    boost::uuids::random_generator gen;

    spider::worker::TaskExecutor executor{
            context,
            "sum_test",
            gen(),
            spider::test::get_storage_url<TestType>(),
            get_libraries(),
            environment_variable,
            2,
            3
    };
    context.run();
    executor.wait();
    REQUIRE(executor.succeed());
    std::optional<int> const result_option = executor.get_result<int>();
    REQUIRE(result_option.has_value());
    REQUIRE(5 == result_option.value_or(0));
}

TEMPLATE_LIST_TEST_CASE(
        "Task execute wrong number of arguments",
        "[worker][storage]",
        spider::test::StorageFactoryTypeList
) {
    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variable
            = get_environment_variable();

    boost::asio::io_context context;

    boost::uuids::random_generator gen;

    spider::worker::TaskExecutor executor{
            context,
            "sum_test",
            gen(),
            spider::test::get_storage_url<TestType>(),
            get_libraries(),
            environment_variable,
            2
    };
    context.run();
    executor.wait();
    REQUIRE(executor.error());
    std::tuple<spider::core::FunctionInvokeError, std::string> error = executor.get_error();
    REQUIRE(spider::core::FunctionInvokeError::WrongNumberOfArguments == std::get<0>(error));
}

TEMPLATE_LIST_TEST_CASE(
        "Task execute fail",
        "[worker][storage]",
        spider::test::StorageFactoryTypeList
) {
    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variable
            = get_environment_variable();

    boost::asio::io_context context;

    boost::uuids::random_generator gen;

    spider::worker::TaskExecutor executor{
            context,
            "error_test",
            gen(),
            spider::test::get_storage_url<TestType>(),
            get_libraries(),
            environment_variable,
            2
    };
    context.run();
    executor.wait();
    REQUIRE(executor.error());
    std::tuple<spider::core::FunctionInvokeError, std::string> error = executor.get_error();
    REQUIRE(spider::core::FunctionInvokeError::FunctionExecutionError == std::get<0>(error));
}

TEMPLATE_LIST_TEST_CASE(
        "Task execute data argument",
        "[worker][storage]",
        spider::test::StorageFactoryTypeList
) {
    std::shared_ptr<spider::core::StorageFactory> const storage_factory
            = spider::test::create_storage_factory<TestType>();
    std::shared_ptr<spider::core::MetadataStorage> const metadata_storage
            = storage_factory->provide_metadata_storage();
    std::shared_ptr<spider::core::DataStorage> const data_storage
            = storage_factory->provide_data_storage();

    std::variant<std::unique_ptr<spider::core::StorageConnection>, spider::core::StorageErr>
            conn_result = storage_factory->provide_storage_connection();
    REQUIRE(std::holds_alternative<std::unique_ptr<spider::core::StorageConnection>>(conn_result));
    auto conn = std::move(std::get<std::unique_ptr<spider::core::StorageConnection>>(conn_result));

    // Create driver and data
    msgpack::sbuffer buffer;
    msgpack::pack(buffer, 3);
    spider::core::Data const data{std::string{buffer.data(), buffer.size()}};
    boost::uuids::random_generator gen;
    boost::uuids::uuid const driver_id = gen();
    spider::core::Driver const driver{driver_id};
    REQUIRE(metadata_storage->add_driver(*conn, driver).success());
    REQUIRE(data_storage->add_driver_data(*conn, driver_id, data).success());

    // Submit a job for a valid task id
    boost::uuids::uuid const task_id = gen();
    spider::core::Task task{"data_test"};
    task.set_id(task_id);
    task.add_input(spider::core::TaskInput{data.get_id()});
    spider::core::TaskGraph graph;
    graph.add_task(task);
    graph.add_input_task(task_id);
    graph.add_output_task(task_id);
    boost::uuids::uuid const job_id = gen();
    REQUIRE(metadata_storage->add_job(*conn, job_id, driver_id, graph).success());

    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variable
            = get_environment_variable();

    boost::asio::io_context context;

    spider::worker::TaskExecutor executor{
            context,
            "data_test",
            task_id,
            spider::test::get_storage_url<TestType>(),
            get_libraries(),
            environment_variable,
            data.get_id()
    };
    context.run();
    executor.wait();
    REQUIRE(executor.succeed());
    std::optional<int> const optional_result = executor.get_result<int>();
    REQUIRE(optional_result.has_value());
    if (optional_result.has_value()) {
        REQUIRE(3 == optional_result.value());
    }

    // Clean up
    REQUIRE(metadata_storage->remove_job(*conn, job_id).success());
    REQUIRE(data_storage->remove_data(*conn, data.get_id()).success());
}

constexpr int cLargeInputSize = 300;

TEMPLATE_LIST_TEST_CASE(
        "Task execute large input&output",
        "[worker][storage]",
        spider::test::StorageFactoryTypeList
) {
    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variable
            = get_environment_variable();

    boost::asio::io_context context;

    boost::uuids::random_generator gen;

    std::string const input_1(cLargeInputSize, 'a');
    std::string const input_2(cLargeInputSize, 'b');

    spider::worker::TaskExecutor executor{
            context,
            "join_string_test",
            gen(),
            spider::test::get_storage_url<TestType>(),
            get_libraries(),
            environment_variable,
            input_1,
            input_2
    };
    context.run();
    executor.wait();
    REQUIRE(executor.succeed());
    std::optional<std::string> const result_option = executor.get_result<std::string>();
    REQUIRE(result_option.has_value());
    REQUIRE(input_1 + input_2 == result_option.value_or(""));
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-unix.BlockInCriticalSection)
