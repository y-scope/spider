#include "../../src/spider/worker/FunctionManager.hpp"
#include "../../src/spider/worker/message_pipe.hpp"
#include "../../src/spider/core/MsgPack.hpp" // IWYU pragma: keep

#include <boost/asio/connect_pipe.hpp>
#include <boost/asio/readable_pipe.hpp>
#include <boost/asio/writable_pipe.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/strand.hpp>

#include <catch2/catch_test_macros.hpp>

// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
namespace {

TEST_CASE("pipe message", "[worker]") {
    boost::asio::io_context context;
    boost::asio::strand executor = boost::asio::make_strand(context);
    boost::asio::readable_pipe read_pipe{context};
    boost::asio::writable_pipe write_pipe{context};
    boost::asio::connect_pipe(read_pipe, write_pipe);

    std::tuple constexpr sample_result = std::make_tuple("test", 3);
    msgpack::sbuffer buffer = spider::core::create_result_response(sample_result);

    std::optional<msgpack::sbuffer> response_option = co_await spider::worker::receive_response_async(std::move(read_pipe));
}

}

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
