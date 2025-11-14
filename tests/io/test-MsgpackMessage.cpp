#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <optional>
#include <thread>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include <spider/io/BoostAsio.hpp>  // IWYU pragma: keep
#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/io/msgpack_message.hpp>

namespace {
using namespace boost::asio::ip;

constexpr std::array<size_t, 12> cBufferSizes{1, 2, 3, 4, 5, 6, 7, 8, 9, 17, 257, 65'537};

// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
TEST_CASE("Sync socket msgpack", "[io]") {
    boost::asio::io_context context;
    // Create server acceptor
    tcp::endpoint const local_endpoint{make_address("127.0.0.1"), 0};
    tcp::acceptor acceptor{context, local_endpoint};

    std::thread server_thread([&acceptor, &context]() {
        // Create server socket
        tcp::socket socket{context};
        acceptor.accept(socket);

        // NOLINTBEGIN(clang-analyzer-unix.Malloc)
        for (size_t const buffer_size : cBufferSizes) {
            std::optional<msgpack::sbuffer> const optional_buffer
                    = spider::core::receive_message(socket);
            REQUIRE(optional_buffer.has_value());
            if (optional_buffer.has_value()) {
                msgpack::sbuffer const& buffer = optional_buffer.value();
                REQUIRE(buffer_size == buffer.size());
                for (size_t i = 0; i < buffer.size(); ++i) {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    REQUIRE(i % 256 == std::bit_cast<uint8_t>(buffer.data()[i]));
                }
            }
        }
        // NOLINTEND(clang-analyzer-unix.Malloc)
    });

    // Create client socket
    tcp::socket socket(context);
    boost::asio::connect(
            socket,
            std::vector{tcp::endpoint{make_address("127.0.0.1"), acceptor.local_endpoint().port()}}
    );

    for (size_t const buffer_size : cBufferSizes) {
        msgpack::sbuffer buffer;
        for (size_t i = 0; i < buffer_size; ++i) {
            // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
            char const value = i % 256;
            buffer.write(&value, sizeof(value));
        }
        REQUIRE(spider::core::send_message(socket, buffer));
    }
    server_thread.join();
}

TEST_CASE("Async socket msgpack", "[io]") {
    boost::asio::io_context context;
    // Create server acceptor
    tcp::endpoint const local_endpoint{make_address("127.0.0.1"), 0};
    tcp::acceptor acceptor{context, local_endpoint};

    // Create client socket
    tcp::socket client_socket(context);
    boost::asio::connect(
            client_socket,
            std::vector{tcp::endpoint{make_address("127.0.0.1"), acceptor.local_endpoint().port()}}
    );

    // Create server socket
    tcp::socket server_socket{context};
    acceptor.accept(server_socket);

    for (size_t const buffer_size : cBufferSizes) {
        msgpack::sbuffer client_buffer;
        for (size_t i = 0; i < buffer_size; ++i) {
            // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
            char const value = i % 256;
            client_buffer.write(&value, sizeof(value));
        }
        std::future<bool> client_future = boost::asio::co_spawn(
                context,
                spider::core::send_message_async(client_socket, client_buffer),
                boost::asio::use_future
        );
        std::future<std::optional<msgpack::sbuffer>> server_future = boost::asio::co_spawn(
                context,
                spider::core::receive_message_async(server_socket),
                boost::asio::use_future
        );

        context.run();
        REQUIRE(client_future.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        REQUIRE(server_future.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        context.restart();

        REQUIRE(client_future.get());
        std::optional<msgpack::sbuffer> const& optional_result_buffer = server_future.get();
        REQUIRE(optional_result_buffer.has_value());
        if (optional_result_buffer.has_value()) {
            msgpack::sbuffer const& result_buffer = optional_result_buffer.value();
            REQUIRE(buffer_size == result_buffer.size());
            for (size_t i = 0; i < result_buffer.size(); ++i) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                REQUIRE(i % 256 == std::bit_cast<uint8_t>(result_buffer.data()[i]));
            }
        }
    }
}

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
}  // namespace
