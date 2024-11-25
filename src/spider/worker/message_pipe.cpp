#include "message_pipe.hpp"

#include <array>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "../core/MsgPack.hpp"  // IWYU pragma: keep

namespace spider::worker {

constexpr size_t cHeaderSize = 16;

auto send_request(boost::asio::writable_pipe& pipe, msgpack::sbuffer const& request) -> bool {
    try {
        size_t const size = request.size();
        std::string const size_str = fmt::format("{:016d}", size);
        boost::asio::write(pipe, boost::asio::buffer{size_str});
        return true;
    } catch (boost::system::system_error const& e) {
        return false;
    }
}

auto receive_response_async(boost::asio::readable_pipe& pipe
) -> boost::asio::awaitable<std::optional<msgpack::sbuffer>> {
    std::array<char, cHeaderSize> header_buffer{0};
    auto [header_ec, header_n] = co_await boost::asio::async_read(
            pipe,
            boost::asio::buffer{header_buffer},
            boost::asio::as_tuple(boost::asio::use_awaitable)
    );
    if (header_ec) {
        if (boost::asio::error::eof != header_ec) {
            spdlog::error(
                    "Cannot read header from pipe {}: {}",
                    header_ec.value(),
                    header_ec.message()
            );
        }
        co_return std::nullopt;
    }
    size_t response_size = 0;
    try {
        response_size = std::stol(std::string{header_buffer.data(), cHeaderSize});
    } catch (std::exception& e) {
        spdlog::error("Cannot parse header: {}", e.what());
        co_return std::nullopt;
    }
    if (response_size == 0) {
        co_return std::nullopt;
    }
    std::vector<char> response_buffer(response_size);
    auto [response_ec, response_n] = co_await boost::asio::async_read(
            pipe,
            boost::asio::buffer(response_buffer),
            boost::asio::as_tuple(boost::asio::use_awaitable)
    );
    if (response_ec) {
        if (boost::asio::error::eof != response_ec) {
            spdlog::error(
                    "Cannot read header from pipe {}: {}",
                    response_ec.value(),
                    response_ec.message()
            );
        }
        co_return std::nullopt;
    }
    if (response_size != response_n) {
        spdlog::error(
                "Response read size not match. Expect {}. Got {}.",
                response_size,
                response_n
        );
        co_return std::nullopt;
    }
    msgpack::sbuffer buffer;
    buffer.write(response_buffer.data(), response_buffer.size());
    co_return buffer;
}

}  // namespace spider::worker
