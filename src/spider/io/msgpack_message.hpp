#ifndef SPIDER_CORE_MSGPACKMESSAGE_HPP
#define SPIDER_CORE_MSGPACKMESSAGE_HPP

#include <functional>
#include <optional>

#include "BoostAsio.hpp"  // IWYU pragma: keep
#include "MsgPack.hpp"  // IWYU pragma :keep

namespace spider::core {

auto send_message(boost::asio::ip::tcp::socket& socket, msgpack::sbuffer const& buffer) -> bool;

auto send_message_async(
        std::reference_wrapper<boost::asio::ip::tcp::socket> socket,
        std::reference_wrapper<msgpack::sbuffer> buffer
) -> boost::asio::awaitable<bool>;

auto receive_message(boost::asio::ip::tcp::socket& socket) -> std::optional<msgpack::sbuffer>;

auto receive_message_async(std::reference_wrapper<boost::asio::ip::tcp::socket> socket
) -> boost::asio::awaitable<std::optional<msgpack::sbuffer>>;

}  // namespace spider::core

#endif  // SPIDER_CORE_MSGPACKMESSAGE_HPP
