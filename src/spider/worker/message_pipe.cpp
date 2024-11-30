#include "message_pipe.hpp"

#include <array>
#include <cstddef>
#include <exception>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "../core/BoostAsio.hpp"  // IWYU pragma: keep
#include "../core/MsgPack.hpp"  // IWYU pragma: keep

namespace spider::worker {

constexpr size_t cHeaderSize = 16;

auto send_message(boost::asio::writable_pipe& pipe, msgpack::sbuffer const& request) -> bool {
    try {
        size_t const size = request.size();
        std::string const size_str = fmt::format("{:016d}", size);
        boost::asio::write(pipe, boost::asio::buffer(size_str));
        boost::asio::write(pipe, boost::asio::buffer(request.data(), size));
        return true;
    } catch (boost::system::system_error const& e) {
        spdlog::error("Failed to send message: {}", e.what());
        return false;
    }
}

auto send_message(boost::asio::posix::stream_descriptor& fd, msgpack::sbuffer const& request)
        -> bool {
    try {
        size_t const size = request.size();
        std::string const size_str = fmt::format("{:016d}", size);
        boost::asio::write(fd, boost::asio::buffer(size_str));
        boost::asio::write(fd, boost::asio::buffer(request.data(), size));
        return true;
    } catch (boost::system::system_error const& e) {
        spdlog::error("Failed to send message: {}", e.what());
        return false;
    }
}

auto receive_message(boost::asio::posix::stream_descriptor& fd) -> std::optional<msgpack::sbuffer> {
    std::array<char, cHeaderSize> header_buffer{0};
    try {
        boost::asio::read(fd, boost::asio::buffer(header_buffer));
    } catch (boost::system::system_error& e) {
        if (boost::asio::error::eof != e.code()) {
            spdlog::error("Fail to read header: {}", e.what());
        }
        return std::nullopt;
    }
    size_t body_size = 0;
    try {
        body_size = std::stoul(std::string{header_buffer.data(), cHeaderSize});
    } catch (std::exception& e) {
        spdlog::error(
                "Cannot parse header: {} {}",
                e.what(),
                std::string{header_buffer.data(), cHeaderSize}
        );
        return std::nullopt;
    }

    std::vector<char> body_buffer(body_size);
    try {
        boost::asio::read(fd, boost::asio::buffer(body_buffer));
    } catch (boost::system::system_error& e) {
        spdlog::error("Fail to read response body: {}", e.what());
        return std::nullopt;
    }
    msgpack::sbuffer buffer;
    buffer.write(body_buffer.data(), body_buffer.size());
    return buffer;
}

auto receive_message_async(std::reference_wrapper<boost::asio::readable_pipe> pipe
) -> boost::asio::awaitable<std::optional<msgpack::sbuffer>> {
    std::array<char, cHeaderSize> header_buffer{0};
    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
    auto [header_ec, header_n] = co_await boost::asio::async_read(
            pipe.get(),
            boost::asio::buffer(header_buffer),
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
        response_size = std::stoul(std::string{header_buffer.data(), cHeaderSize});
    } catch (std::exception& e) {
        spdlog::error(
                "Cannot parse header: {} {}",
                e.what(),
                std::string{header_buffer.data(), cHeaderSize}
        );
        co_return std::nullopt;
    }
    if (response_size == 0) {
        co_return std::nullopt;
    }
    std::vector<char> response_buffer(response_size);
    auto [response_ec, response_n] = co_await boost::asio::async_read(
            pipe.get(),
            boost::asio::buffer(response_buffer),
            boost::asio::as_tuple(boost::asio::use_awaitable)
    );
    if (response_ec) {
        if (boost::asio::error::eof != response_ec) {
            spdlog::error(
                    "Cannot read response body from pipe {}: {}",
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
