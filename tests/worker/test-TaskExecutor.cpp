#include "../../src/spider/core/BoostAsio.hpp"  // IWYU pragma: keep
#include "../../src/spider/worker/TaskExecutor.hpp"

#include <absl/container/flat_hash_map.h>
#include <boost/dll/runtime_symbol_info.hpp>
#include <boost/filesystem/path.hpp>
#include <catch2/catch_test_macros.hpp>

// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

namespace {
auto get_environment_variable() -> absl::flat_hash_map<
                                        boost::process::v2::environment::key,
                                        boost::process::v2::environment::value> {
    boost::filesystem::path const executable_dir = boost::dll::program_location().parent_path();
    boost::filesystem::path const src_dir = executable_dir.parent_path() / "src" / "spider";

    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value>
            environment_variables;

    environment_variables.emplace("PATH", src_dir);

    return environment_variables;
}

auto get_libraries() -> std::vector<std::string> {
    boost::filesystem::path const executable_dir = boost::dll::program_location().parent_path();
    boost::filesystem::path const lib_path = executable_dir / "libworker_test.so";
    return {lib_path.string()};
}

TEST_CASE("Task execute success", "[worker]") {
    absl::flat_hash_map<
            boost::process::v2::environment::key,
            boost::process::v2::environment::value> const environment_variable
            = get_environment_variable();

    boost::asio::io_context context;

    spider::worker::TaskExecutor
            executor{context, "sum_test", get_libraries(), environment_variable, 2, 3};
    executor.wait();
    REQUIRE(executor.succeed());
    std::optional<int> const result_option = executor.get_result<int>();
    REQUIRE(result_option.has_value());
    REQUIRE(5 == result_option.value_or(0));
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
