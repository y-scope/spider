#ifndef SPIDER_CORE_BOOSTASIO_HPP
#define SPIDER_CORE_BOOSTASIO_HPP

// clang-format off
// IWYU pragma: begin_exports

#include <boost/asio.hpp>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/readable_pipe.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/writable_pipe.hpp>
#include <boost/system/system_error.hpp>
#include <boost/asio/impl/co_spawn.hpp>
#include <boost/asio/impl/connect_pipe.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>

#include <boost/asio/impl/write.hpp>

// IWYU pragma: end_exports
// clang-format on
#endif  // SPIDER_CORE_BOOSTASIO_HPP
