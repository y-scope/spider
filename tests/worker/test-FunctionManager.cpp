// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
#include <optional>
#include <string>
#include <tuple>

#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/client/TaskContext.hpp"
#include "../../src/spider/io/MsgPack.hpp"  // IWYU pragma: keep
#include "../../src/spider/worker/FunctionManager.hpp"

namespace {
auto int_test(spider::TaskContext /*context*/, int const x, int const y) -> int {
    return x + y;
}

auto tuple_ret_test(spider::TaskContext /*context*/, std::string const& str, int const x)
        -> std::tuple<std::string, int> {
    return std::make_tuple(str, x);
}

SPIDER_WORKER_REGISTER_TASK(int_test);
SPIDER_WORKER_REGISTER_TASK(tuple_ret_test);

TEST_CASE("Register and run function with POD inputs", "[core]") {
    spider::core::FunctionManager const& manager = spider::core::FunctionManager::get_instance();

    // Get the function that has not been registered should return nullptr
    REQUIRE(nullptr == manager.get_function("foo"));

    // Get the registered function should succeed
    spider::core::Function const* function = manager.get_function("int_test");
    REQUIRE(nullptr != function);

    spider::TaskContext context{};

    // Run function with two ints should succeed
    spider::core::ArgsBuffer const args_buffers = spider::core::create_args_buffers(2, 3);
    constexpr int cExpected = 2 + 3;
    msgpack::sbuffer const result = (*function)(context, args_buffers);
    msgpack::sbuffer buffer{};
    msgpack::pack(buffer, cExpected);
    REQUIRE(cExpected == spider::core::response_get_result<int>(result).value_or(0));

    // Run function with wrong number of inputs should fail
    spider::core::ArgsBuffer wrong_args_buffers = spider::core::create_args_buffers(1);
    msgpack::sbuffer wrong_result = (*function)(context, wrong_args_buffers);
    std::optional<std::tuple<spider::core::FunctionInvokeError, std::string>> wrong_result_option
            = spider::core::response_get_error(wrong_result);
    REQUIRE(wrong_result_option.has_value());
    if (wrong_result_option.has_value()) {
        REQUIRE(spider::core::FunctionInvokeError::WrongNumberOfArguments
                == std::get<0>(wrong_result_option.value()));
    }

    // Run function with wrong type of inputs should fail
    wrong_args_buffers = spider::core::create_args_buffers(0, "test");
    wrong_result = (*function)(context, wrong_args_buffers);
    wrong_result_option = spider::core::response_get_error(wrong_result);
    REQUIRE(wrong_result_option.has_value());
    if (wrong_result_option.has_value()) {
        REQUIRE(spider::core::FunctionInvokeError::ArgumentParsingError
                == std::get<0>(wrong_result_option.value()));
    }
}

TEST_CASE("Register and run function with tuple return", "[core]") {
    spider::TaskContext context{};

    spider::core::FunctionManager const& manager = spider::core::FunctionManager::get_instance();

    spider::core::Function const* function = manager.get_function("tuple_ret_test");

    spider::core::ArgsBuffer const args_buffers = spider::core::create_args_buffers("test", 3);
    msgpack::sbuffer const result = (*function)(context, args_buffers);
    REQUIRE(std::make_tuple("test", 3)
            == spider::core::response_get_result<std::string, int>(result).value_or(
                    std::make_tuple("", 0)
            ));
}

}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
