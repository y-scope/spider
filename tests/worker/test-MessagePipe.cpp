#include <chrono>
#include <future>
#include <optional>
#include <string>
#include <tuple>

#include <catch2/catch_test_macros.hpp>

#include <spider/io/BoostAsio.hpp>
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/worker/FunctionManager.hpp>
#include <spider/worker/message_pipe.hpp>
#include <spider/worker/TaskExecutorMessage.hpp>

// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
namespace {
TEST_CASE("pipe message response", "[worker]") {
    boost::asio::io_context context;
    boost::asio::readable_pipe read_pipe(context);
    boost::asio::writable_pipe write_pipe(context);
    boost::asio::connect_pipe(read_pipe, write_pipe);

    constexpr std::tuple cSampleResult = std::make_tuple("test", 3);
    msgpack::sbuffer const buffer = spider::core::create_result_response(cSampleResult);

    std::future<std::optional<msgpack::sbuffer>> future = boost::asio::co_spawn(
            context,
            spider::worker::receive_message_async(read_pipe),
            boost::asio::use_future
    );

    // Send message should succeed
    REQUIRE(spider::worker::send_message(write_pipe, buffer));

    context.run();

    REQUIRE(future.wait_for(std::chrono::seconds(5)) == std::future_status::ready);

    // Get value should succeed
    std::optional<msgpack::sbuffer> const& response_option = future.get();
    REQUIRE(response_option.has_value());
    if (response_option.has_value()) {
        msgpack::sbuffer const& response_buffer = response_option.value();
        REQUIRE(spider::worker::TaskExecutorResponseType::Result
                == spider::worker::get_response_type(response_buffer));
        std::optional<std::tuple<std::string, int>> const parse_response
                = spider::core::response_get_result<std::string, int>(response_buffer);
        REQUIRE(parse_response.has_value());
        if (parse_response.has_value()) {
            std::tuple<std::string, int> result = parse_response.value();
            REQUIRE(cSampleResult == result);
        }
    }
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
