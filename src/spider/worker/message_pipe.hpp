#ifndef SPIDER_WORKER_MESSAGE_PIPE_HPP
#define SPIDER_WORKER_MESSAGE_PIPE_HPP

#include <boost/asio/awaitable.hpp>
#include <boost/asio/readable_pipe.hpp>
#include <boost/asio/writable_pipe.hpp>

#include "../core/MsgPack.hpp"  // IWYU pragma: keep

namespace spider::worker {

auto send_message(boost::asio::writable_pipe& pipe, msgpack::sbuffer const& request) -> bool;

auto receive_message_async(boost::asio::readable_pipe pipe
) -> boost::asio::awaitable<std::optional<msgpack::sbuffer>>;

}  // namespace spider::worker

#endif  // SPIDER_WORKER_MESSAGE_PIPE_HPP
