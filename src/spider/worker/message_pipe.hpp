#ifndef SPIDER_WORKER_MESSAGE_PIPE_HPP
#define SPIDER_WORKER_MESSAGE_PIPE_HPP

#include <boost/asio/readable_pipe.hpp>
#include <boost/asio/writable_pipe.hpp>
#include <future>

#include "../core/MsgPack.hpp"  // IWYU pragma: keep

namespace spider::worker {

auto send_request(boost::asio::writable_pipe& pipe, msgpack::sbuffer const& request) -> bool;

auto receive_response_async(boost::asio::readable_pipe& pipe
) -> std::future<std::optional<msgpack::sbuffer>>;

}  // namespace spider::worker

#endif  // SPIDER_WORKER_MESSAGE_PIPE_HPP
