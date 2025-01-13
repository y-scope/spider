#ifndef SPIDER_CORE_BOOSTASIO_HPP
#define SPIDER_CORE_BOOSTASIO_HPP

#include <optional>

// clang-format off
// IWYU pragma: begin_exports

#include <boost/asio.hpp>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <boost/asio/readable_pipe.hpp>
#include <boost/asio/writable_pipe.hpp>
#include <boost/asio/impl/connect_pipe.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/asio/impl/connect.hpp>

#include <boost/asio/detached.hpp>
#include <boost/asio/impl/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include <boost/asio/impl/write.hpp>
#include <boost/asio/impl/read.hpp>

#include <boost/system/system_error.hpp>

// IWYU pragma: end_exports
// clang-format on

#endif  // SPIDER_CORE_BOOSTASIO_HPP
