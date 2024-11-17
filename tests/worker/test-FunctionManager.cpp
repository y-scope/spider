// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
#include "../../src/spider/core/Data.hpp"
#include "../../src/spider/core/MsgPack.hpp"  // IWYU pragma: keep
#include "../../src/spider/worker/FunctionManager.hpp"
#include "../utils/FunctionManagerUtils.hpp"

#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <tuple>

namespace {
auto int_test(int const x, int const y) -> int {
    return x + y;
}

auto tuple_ret_test(std::string const& str, int const x) -> std::tuple<std::string, int> {
    return std::make_tuple(str, x);
}

auto data_test(spider::core::Data const& data) -> spider::core::Data {
    return spider::core::Data{data.get_id(), data.get_value() + data.get_value()};
}

TEST_CASE("Register and run function with POD inputs", "[core]") {
    REGISTER_TASK(int_test);

    spider::core::FunctionManager& manager = spider::core::FunctionManager::get_instance();

    // Get the function that has not been registered should return nullptr
    REQUIRE(nullptr == manager.get_function("foo"));

    // Get the registered function should succeed
    spider::core::Function const* function = manager.get_function("int_test");

    // Run function with two ints should succeed
    spider::core::ArgsBuffers const args_buffers = spider::test::create_args_buffers(2, 3);
    std::optional<msgpack::sbuffer> const result = (*function)(args_buffers);
    REQUIRE(result.has_value());
    REQUIRE(5 == spider::test::get_result<int>(result.value()));

    // Run function with wrong number of inputs should fail
    spider::core::ArgsBuffers wrong_args_buffers = spider::test::create_args_buffers(1);
    std::optional<msgpack::sbuffer> wrong_result = (*function)(wrong_args_buffers);
    REQUIRE(!wrong_result.has_value());

    // Run function with wrong type of inputs should fail
    wrong_args_buffers = spider::test::create_args_buffers(0, "test");
    wrong_result = (*function)(wrong_args_buffers);
    REQUIRE(!wrong_result.has_value());
}

TEST_CASE("Register and run function with tuple return", "[core]") {
    REGISTER_TASK(tuple_ret_test);

    spider::core::FunctionManager& manager = spider::core::FunctionManager::get_instance();

    spider::core::Function const* function = manager.get_function("tuple_ret_test");

    spider::core::ArgsBuffers const args_buffers = spider::test::create_args_buffers("test", 3);
    std::optional<msgpack::sbuffer> const result = (*function)(args_buffers);
    REQUIRE(result.has_value());
    REQUIRE(std::make_tuple<std::string, int>("test", 3)
            == spider::test::get_result<std::tuple<std::string, int>>(result.value()));
}

TEST_CASE("Register and run function with data", "[core]") {
    REGISTER_TASK(data_test);

    spider::core::FunctionManager& manager = spider::core::FunctionManager::get_instance();

    spider::core::Function const* function = manager.get_function("data_test");

    spider::core::Data data{"test"};
    spider::core::ArgsBuffers const args_buffers = spider::test::create_args_buffers(data);
    std::optional<msgpack::sbuffer> const result = (*function)(args_buffers);
    REQUIRE(result.has_value());
    spider::core::Data const result_data
            = spider::test::get_result<spider::core::Data>(result.value());
    REQUIRE(data.get_id() == result_data.get_id());
    REQUIRE("testtest" == result_data.get_value());
}

}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
