#ifndef SPIDER_WORKER_MESSAGE_PIPE_HPP
#define SPIDER_WORKER_MESSAGE_PIPE_HPP

#include <optional>

#include "../core/BoostAsio.hpp"  // IWYU pragma: keep
#include "../core/MsgPack.hpp"  // IWYU pragma: keep

namespace spider::worker {

auto send_message(boost::asio::writable_pipe& pipe, msgpack::sbuffer const& request) -> bool;

auto send_message(boost::asio::posix::stream_descriptor& fd, msgpack::sbuffer const& request)
        -> bool;

auto receive_message_async(boost::asio::readable_pipe pipe
) -> boost::asio::awaitable<std::optional<msgpack::sbuffer>>;

auto receive_message(boost::asio::posix::stream_descriptor& fd) -> std::optional<msgpack::sbuffer>;

}  // namespace spider::worker

#endif  // SPIDER_WORKER_MESSAGE_PIPE_HPP
