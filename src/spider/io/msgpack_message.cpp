#include "msgpack_message.hpp"

#include <netinet/in.h>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include "BoostAsio.hpp"  // IWYU pragma: keep
#include "MsgPack.hpp"  // IWYU pragma: keep

namespace {

/**
 * Read the type of ext msgpack message.
 *
 * @param type
 * @return std::nullopt if type is not an ext type, a pair otherwise,
 * - Size of the next read
 * - True if next read reads only the body size, false otherwise
 */
auto read_ext_type(char8_t const type) -> std::optional<std::pair<size_t, bool>> {
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    switch (type) {
        case 0xd4:
            return std::make_pair(2, false);
        case 0xd5:
            return std::make_pair(3, false);
        case 0xd6:
            return std::make_pair(7, false);
        case 0xd7:
            return std::make_pair(9, false);
        case 0xd8:
            return std::make_pair(17, false);
        case 0xc7:
            return std::make_pair(1, true);
        case 0xc8:
            return std::make_pair(2, true);
        case 0xc9:
            return std::make_pair(4, true);
        default:
            return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
}

auto read_ext_body_size(std::u8string_view const body_size) -> std::optional<size_t> {
    switch (body_size.size()) {
        case 1:
            return std::bit_cast<std::uint8_t>(body_size[0]);
        case 2: {
            std::uint16_t body_size_16 = 0;
            memcpy(&body_size_16, body_size.data(), sizeof(std::uint16_t));
            return ntohs(body_size_16);
        }
        case 4: {
            std::uint32_t body_size_32 = 0;
            memcpy(&body_size_32, body_size.data(), sizeof(std::uint32_t));
            return ntohl(body_size_32);
        }
        default:
            return std::nullopt;
    }
}

}  // namespace

namespace spider::core {

auto send_message(boost::asio::ip::tcp::socket& socket, msgpack::sbuffer const& buffer) -> bool {
    msgpack::sbuffer message_buffer;
    msgpack::packer packer{message_buffer};
    packer.pack_ext(buffer.size(), msgpack::type::BIN);
    packer.pack_ext_body(buffer.data(), buffer.size());
    size_t const size = boost::asio::write(
            socket,
            boost::asio::buffer(message_buffer.data(), message_buffer.size())
    );
    return size == message_buffer.size();
}

auto send_message_async(
        std::reference_wrapper<boost::asio::ip::tcp::socket> socket,
        std::reference_wrapper<msgpack::sbuffer> buffer
) -> boost::asio::awaitable<bool> {
    msgpack::sbuffer message_buffer;
    msgpack::packer packer{message_buffer};
    packer.pack_ext(buffer.get().size(), msgpack::type::BIN);
    packer.pack_ext_body(buffer.get().data(), buffer.get().size());
    auto const& [ec, size] = co_await boost::asio::async_write(
            socket.get(),
            boost::asio::buffer(message_buffer.data(), message_buffer.size()),
            boost::asio::as_tuple(boost::asio::use_awaitable)
    );
    if (ec) {
        if (boost::asio::error::eof != ec) {
            spdlog::error("Cannot send message to socket {}: {}", ec.value(), ec.message());
        }
        co_return false;
    }
    co_return size == message_buffer.size();
}

auto receive_message(boost::asio::ip::tcp::socket& socket) -> std::optional<msgpack::sbuffer> {
    // Read header
    char8_t header = 0;
    boost::asio::read(socket, boost::asio::buffer(&header, sizeof(header)));
    std::optional<std::pair<size_t, bool>> const optional_body_pair = read_ext_type(header);
    if (false == optional_body_pair.has_value()) {
        return std::nullopt;
    }

    // Read next
    std::pair<size_t, bool> const body_pair = optional_body_pair.value();
    std::vector<char8_t> body_size_vec(body_pair.first);
    boost::asio::read(socket, boost::asio::buffer(body_size_vec));
    if (body_pair.second) {
        // Entire body read with type. Validate type to be bin.
        if (body_size_vec[0] != msgpack::type::BIN) {
            return std::nullopt;
        }
        msgpack::sbuffer buffer;
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        buffer.write(reinterpret_cast<char*>(&body_size_vec[1]), body_size_vec.size() - 1);
        return buffer;
    }
    std::optional<size_t> const optional_body_size
            = read_ext_body_size(std::u8string_view{body_size_vec.data(), body_size_vec.size()});
    if (false == optional_body_size.has_value()) {
        return std::nullopt;
    }
    size_t const body_size = optional_body_size.value();

    // Read body
    std::vector<char8_t> body_vec(body_size + 1);
    boost::asio::read(socket, boost::asio::buffer(body_vec));
    // Validate type to be bin
    if (body_vec[0] != msgpack::type::BIN) {
        return std::nullopt;
    }
    msgpack::sbuffer buffer;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    buffer.write(reinterpret_cast<char*>(&body_vec[1]), body_vec.size() - 1);
    return buffer;
}

auto receive_message_async(std::reference_wrapper<boost::asio::ip::tcp::socket> socket
) -> boost::asio::awaitable<std::optional<msgpack::sbuffer>> {
    // Read header
    char8_t header = 0;
    // Suppress clang-tidy warning inside boost asio
    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
    auto const& [header_ec, header_size] = co_await boost::asio::async_read(
            socket.get(),
            boost::asio::buffer(&header, sizeof(header)),
            boost::asio::as_tuple(boost::asio::use_awaitable)
    );
    if (header_ec) {
        if (boost::asio::error::eof != header_ec) {
            spdlog::error(
                    "Cannot read message header from socket {}: {}",
                    header_ec.value(),
                    header_ec.message()
            );
        }
        co_return std::nullopt;
    }
    if (header_size != sizeof(header)) {
        co_return std::nullopt;
    }
    std::optional<std::pair<size_t, bool>> const optional_body_pair = read_ext_type(header);
    if (false == optional_body_pair.has_value()) {
        co_return std::nullopt;
    }

    // Read next
    std::pair<size_t, bool> const body_pair = optional_body_pair.value();
    std::vector<char8_t> body_size_vec(body_pair.first);
    auto const& [body_size_ec, body_size_size] = co_await boost::asio::async_read(
            socket.get(),
            boost::asio::buffer(body_size_vec),
            boost::asio::as_tuple(boost::asio::use_awaitable)
    );
    if (body_size_ec) {
        if (boost::asio::error::eof != header_ec) {
            spdlog::error(
                    "Cannot read message body size or body from socket {}: {}",
                    body_size_ec.value(),
                    body_size_ec.message()
            );
        }
        co_return std::nullopt;
    }
    if (body_size_size != body_pair.first) {
        co_return std::nullopt;
    }
    if (body_pair.second) {
        // Entire body read with type. Validate type to be bin.
        if (body_size_vec[0] != msgpack::type::BIN) {
            co_return std::nullopt;
        }
        msgpack::sbuffer buffer;
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        buffer.write(reinterpret_cast<char*>(&body_size_vec[1]), body_size_vec.size() - 1);
        co_return buffer;
    }
    std::optional<size_t> const optional_body_size
            = read_ext_body_size(std::u8string_view{body_size_vec.data(), body_size_vec.size()});
    if (false == optional_body_size.has_value()) {
        co_return std::nullopt;
    }
    size_t const body_size = optional_body_size.value();

    // Read body
    std::vector<char8_t> body_vec(body_size + 1);
    auto const& [body_ec, body_read_size] = co_await boost::asio::async_read(
            socket.get(),
            boost::asio::buffer(body_vec),
            boost::asio::as_tuple(boost::asio::use_awaitable)
    );
    if (body_ec) {
        if (boost::asio::error::eof != header_ec) {
            spdlog::error(
                    "Cannot read message body size or body from socket {}: {}",
                    body_ec.value(),
                    body_ec.message()
            );
        }
        co_return std::nullopt;
    }
    if (body_read_size != body_size + 1) {
        co_return std::nullopt;
    }

    // Validate type to be bin
    if (body_vec[0] != msgpack::type::BIN) {
        co_return std::nullopt;
    }
    msgpack::sbuffer buffer;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    buffer.write(reinterpret_cast<char*>(&body_vec[1]), body_vec.size() - 1);
    co_return buffer;
}

}  // namespace spider::core
