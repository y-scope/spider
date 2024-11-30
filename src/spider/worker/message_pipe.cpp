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

template <typename T>
auto send_message_impl(T& output, msgpack::sbuffer const& request) -> bool {
    try {
        size_t const size = request.size();
        std::string const size_str = fmt::format("{:016d}", size);
        boost::asio::write(output, boost::asio::buffer(size_str));
        boost::asio::write(output, boost::asio::buffer(request.data(), size));
        return true;
    } catch (boost::system::system_error const& e) {
        spdlog::error("Failed to send message: {}", e.what());
        return false;
    }
}

auto send_message(boost::asio::writable_pipe& pipe, msgpack::sbuffer const& request) -> bool {
    return send_message_impl(pipe, request);
}

auto send_message(boost::asio::posix::stream_descriptor& fd, msgpack::sbuffer const& request)
        -> bool {
    return send_message_impl(fd, request);
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
        size_t body_read_size = boost::asio::read(fd, boost::asio::buffer(body_buffer));
        if (body_read_size != body_size) {
            spdlog::error(
                    "Message body read size mismatch. Expect {}. Got {}",
                    body_size,
                    body_read_size
            );
            return std::nullopt;
        }
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
    size_t body_size = 0;
    try {
        body_size = std::stoul(std::string{header_buffer.data(), cHeaderSize});
    } catch (std::exception& e) {
        spdlog::error(
                "Cannot parse header: {} {}",
                e.what(),
                std::string{header_buffer.data(), cHeaderSize}
        );
        co_return std::nullopt;
    }
    if (body_size == 0) {
        co_return std::nullopt;
    }
    std::vector<char> body_buffer(body_size);
    auto [body_ec, body_n] = co_await boost::asio::async_read(
            pipe.get(),
            boost::asio::buffer(body_buffer),
            boost::asio::as_tuple(boost::asio::use_awaitable)
    );
    if (body_ec) {
        if (boost::asio::error::eof != body_ec) {
            spdlog::error(
                    "Cannot read response body from pipe {}: {}",
                    body_ec.value(),
                    body_ec.message()
            );
        }
        co_return std::nullopt;
    }
    if (body_size != body_n) {
        spdlog::error("Message body read size not match. Expect {}. Got {}.", body_size, body_n);
        co_return std::nullopt;
    }
    msgpack::sbuffer buffer;
    buffer.write(body_buffer.data(), body_buffer.size());
    co_return buffer;
}

}  // namespace spider::worker
