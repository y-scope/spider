#include <bit>
#include <thread>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../../src/spider/io/MsgPack.hpp"  // IWYU pragma: keep
#include "../../src/spider/io/msgpack_message.hpp"

namespace {

using namespace boost::asio::ip;

constexpr std::array<size_t, 12> cBufferSizes{1, 2, 3, 4, 5, 6, 7, 8, 9, 17, 257, 65'537};
constexpr unsigned cPort = 6021;

TEST_CASE("Synchronized socket io", "[io]") {
    boost::asio::io_context context;
    // Create server acceptor
    tcp::endpoint const local_endpoint{boost::asio::ip::address::from_string("127.0.0.1"), cPort};
    tcp::acceptor acceptor{context, local_endpoint};

    std::thread server_thread([&acceptor, &context]() {
        // Create server socket
        tcp::socket socket{context};
        acceptor.accept(socket);

        for (size_t const buffer_size : cBufferSizes) {
            std::optional<msgpack::sbuffer> const optional_buffer
                    = spider::core::receive_message(socket);
            REQUIRE(optional_buffer.has_value());
            if (optional_buffer.has_value()) {
                msgpack::sbuffer const& buffer = optional_buffer.value();
                REQUIRE(buffer_size == buffer.size());
                for (size_t i = 0; i < buffer.size(); ++i) {
                    REQUIRE(i % 256 == std::bit_cast<uint8_t>(buffer.data()[i]));
                }
            }
        }
    });

    // Create client socket
    tcp::socket socket(context);
    boost::asio::connect(socket, std::vector{local_endpoint});

    for (size_t const buffer_size : cBufferSizes) {
        msgpack::sbuffer buffer;
        for (size_t i = 0; i < buffer_size; ++i) {
            char const value = i % 256;
            buffer.write(&value, sizeof(value));
        }
        REQUIRE(spider::core::send_message(socket, buffer));
    }
    server_thread.join();
}

}  // namespace
