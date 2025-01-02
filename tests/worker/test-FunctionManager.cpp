// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/client/Data.hpp"
#include "../../src/spider/client/TaskContext.hpp"
#include "../../src/spider/core/Driver.hpp"
#include "../../src/spider/core/TaskContextImpl.hpp"
#include "../../src/spider/core/TaskGraphImpl.hpp"
#include "../../src/spider/io/MsgPack.hpp"  // IWYU pragma: keep
#include "../../src/spider/worker/FunctionManager.hpp"
#include "../storage/StorageTestHelper.hpp"

namespace {
auto int_test(spider::TaskContext& /*context*/, int const x, int const y) -> int {
    return x + y;
}

auto tuple_ret_test(spider::TaskContext& /*context*/, std::string const& str, int const x)
        -> std::tuple<std::string, int> {
    return std::make_tuple(str, x);
}

auto data_test(spider::TaskContext& /*context*/, spider::Data<int>& data) -> int {
    return data.get();
}

auto not_registered(spider::TaskContext& /*context*/) -> int {
    return 0;
}

SPIDER_WORKER_REGISTER_TASK(int_test);
SPIDER_WORKER_REGISTER_TASK(tuple_ret_test);
SPIDER_WORKER_REGISTER_TASK(data_test);

TEST_CASE("Register and get function name", "[core]") {
    spider::core::FunctionManager const& manager = spider::core::FunctionManager::get_instance();

    // Get the function name of non-registered function should return std::nullopt
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    REQUIRE(!manager.get_function_name(reinterpret_cast<void*>(not_registered)).has_value());
    // Get the function name of registered function should return the name
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    REQUIRE("int_test" == manager.get_function_name(reinterpret_cast<void*>(int_test)).value_or("")
    );
}

TEMPLATE_LIST_TEST_CASE(
        "Register and run function with POD inputs",
        "[core][storage]",
        spider::test::StorageTypeList
) {
    auto [metadata_storage, data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();
    spider::TaskContext const context = spider::core::TaskContextImpl::create_task_context(
            std::move(data_storage),
            std::move(metadata_storage)
    );

    spider::core::FunctionManager const& manager = spider::core::FunctionManager::get_instance();

    // Get the function that has not been registered should return nullptr
    REQUIRE(nullptr == manager.get_function("foo"));

    // Get the registered function should succeed
    spider::core::Function const* function = manager.get_function("int_test");
    REQUIRE(nullptr != function);

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

TEMPLATE_LIST_TEST_CASE(
        "Register and run function with tuple return",
        "[core][storage]",
        spider::test::StorageTypeList
) {
    auto [metadata_storage, data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();
    spider::TaskContext const context = spider::core::TaskContextImpl::create_task_context(
            std::move(data_storage),
            std::move(metadata_storage)
    );

    spider::core::FunctionManager const& manager = spider::core::FunctionManager::get_instance();

    spider::core::Function const* function = manager.get_function("tuple_ret_test");

    spider::core::ArgsBuffer const args_buffers = spider::core::create_args_buffers("test", 3);
    msgpack::sbuffer const result = (*function)(context, args_buffers);
    REQUIRE(std::make_tuple("test", 3)
            == spider::core::response_get_result<std::string, int>(result).value_or(
                    std::make_tuple("", 0)
            ));
}

TEMPLATE_LIST_TEST_CASE(
        "Register and run function with data inputs",
        "[core][storage]",
        spider::test::StorageTypeList
) {
    auto [unique_metadata_storage, unique_data_storage] = spider::test::
            create_storage<std::tuple_element_t<0, TestType>, std::tuple_element_t<1, TestType>>();

    std::shared_ptr<spider::core::MetadataStorage> const metadata_storage
            = std::move(unique_metadata_storage);
    std::shared_ptr<spider::core::DataStorage> const data_storage = std::move(unique_data_storage);

    msgpack::sbuffer buffer;
    msgpack::pack(buffer, 3);
    spider::core::Data const data{std::string{buffer.data(), buffer.size()}};
    boost::uuids::random_generator gen;
    boost::uuids::uuid const driver_id = gen();
    spider::core::Driver const driver{driver_id, "127.0.0.1"};
    REQUIRE(metadata_storage->add_driver(driver).success());
    REQUIRE(data_storage->add_driver_data(driver_id, data).success());

    spider::TaskContext const context
            = spider::core::TaskContextImpl::create_task_context(data_storage, metadata_storage);

    spider::core::FunctionManager const& manager = spider::core::FunctionManager::get_instance();

    spider::core::Function const* function = manager.get_function("data_test");

    spider::core::ArgsBuffer const args_buffers = spider::core::create_args_buffers(data.get_id());
    msgpack::sbuffer const result = (*function)(context, args_buffers);
    REQUIRE(3 == spider::core::response_get_result<int>(result).value_or(0));

    REQUIRE(data_storage->remove_data(data.get_id()).success());
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
