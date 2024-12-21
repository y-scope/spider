#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/dll/runtime_symbol_info.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/core/Data.hpp"
#include "../../src/spider/core/Driver.hpp"
#include "../../src/spider/io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../../src/spider/io/MsgPack.hpp"  // IWYU pragma: keep
#include "../../src/spider/storage/DataStorage.hpp"
#include "../../src/spider/storage/MetadataStorage.hpp"
#include "../../src/spider/worker/FunctionManager.hpp"
#include "../../src/spider/worker/TaskExecutor.hpp"
#include "../storage/StorageTestHelper.hpp"

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

TEST_CASE("Task execute success", "[worker][storage]") {
    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variable
            = get_environment_variable();

    boost::asio::io_context context;

    spider::worker::TaskExecutor executor{
            context,
            "sum_test",
            spider::test::cStorageUrl,
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

TEST_CASE("Task execute wrong number of arguments", "[worker][storage]") {
    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variable
            = get_environment_variable();

    boost::asio::io_context context;

    spider::worker::TaskExecutor executor{
            context,
            "sum_test",
            spider::test::cStorageUrl,
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

TEST_CASE("Task execute fail", "[worker][storage]") {
    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variable
            = get_environment_variable();

    boost::asio::io_context context;

    spider::worker::TaskExecutor executor{
            context,
            "error_test",
            spider::test::cStorageUrl,
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
        spider::test::StorageTypeList
) {
    auto [unique_metadata_storage, unique_data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();
    std::shared_ptr<spider::core::MetadataStorage> const metadata_storage
            = std::move(unique_metadata_storage);
    std::shared_ptr<spider::core::DataStorage> const data_storage = std::move(unique_data_storage);

    // Create driver and data
    msgpack::sbuffer buffer;
    msgpack::pack(buffer, 3);
    spider::core::Data const data{std::string{buffer.data(), buffer.size()}};
    boost::uuids::random_generator gen;
    boost::uuids::uuid const driver_id = gen();
    spider::core::Driver const driver{driver_id, "127.0.0.1"};
    REQUIRE(metadata_storage->add_driver(driver).success());
    REQUIRE(data_storage->add_driver_data(driver_id, data).success());

    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variable
            = get_environment_variable();

    boost::asio::io_context context;

    spider::worker::TaskExecutor executor{
            context,
            "data_test",
            spider::test::cStorageUrl,
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
    REQUIRE(data_storage->remove_data(data.get_id()).success());
}

}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,clang-analyzer-unix.BlockInCriticalSection)
