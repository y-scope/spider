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

#include <string>

#include <spdlog/spdlog.h>

namespace spider::core {
inline auto get_address() -> std::optional<std::string> {
    try {
        boost::asio::io_context io_context;
        boost::asio::ip::tcp::resolver resolver(io_context);
        auto const endpoints = resolver.resolve(boost::asio::ip::host_name(), "");
        for (auto const& endpoint : endpoints) {
            if (endpoint.endpoint().address().is_v4()
                && !endpoint.endpoint().address().is_loopback())
            {
                return endpoint.endpoint().address().to_string();
            }
        }
        // If no non-loopback address found, return loopback address
        spdlog::warn("No non-loopback address found, using loopback address");
        for (auto const& endpoint : endpoints) {
            if (endpoint.endpoint().address().is_v4()) {
                return endpoint.endpoint().address().to_string();
            }
        }
        return std::nullopt;
    } catch (boost::system::system_error const& e) {
        return std::nullopt;
    }
}
}  // namespace spider::core

#endif  // SPIDER_CORE_BOOSTASIO_HPP
