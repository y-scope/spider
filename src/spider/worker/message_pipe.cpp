#include "message_pipe.hpp"

#include <fmt/format.h>

#include <future>
#include <memory>
#include <string>

#include "../core/MsgPack.hpp"  // IWYU pragma: keep

namespace spider::worker {

auto send_request(boost::asio::writable_pipe& pipe, msgpack::sbuffer const& request) -> bool {
    try {
        size_t size = request.size();
        pipe.write_some(fmt::format("{:016d}", size));
        return true;
    } catch (boost::system::system_error cosnt& e) {
        return false;
    }
}

auto receive_response_async(boost::asio::readable_pipe& pipe
) -> std::future<std::optional<msgpack::sbuffer>> {
    std::shared_ptr<std::promise<std::optional<msgpack::sbuffer>>> promise
            = std::make_shared<std::promise<std::optional<msgpack::sbuffer>>>();
    std::shared_ptr<std::array<char, 16>> size_buffer = std::make_shared<std::array<char, 16>>();
    pipe.async_read_some(
            size_buffer,
            [&promise, size_buffer, &pipe](boost::system::error_code ec, std::size_t size) {
                if (ec) {
                    promise->set_value(std::nullopt);
                    return;
                }
                if (size != size_buffer->size()) {
                    promise->set_value(std::nullopt);
                    return;
                }
                size_t const response_size
                        = std::stol(std::string(size_buffer->data(), size_buffer->size()));
                std::shared_ptr<std::vector<char>> response_buffer
                        = std::make_shared<std::vector<char>>(response_size);
                pipe.async_read_some(
                        response_buffer,
                        [&promise,
                         response_size,
                         response_buffer](boost::system::error_code ec, std::size_t size) {
                            if (ec) {
                                promise->set_value(std::nullopt);
                                return;
                            }
                            if (size != response_size) {
                                promise->set_value(std::nullopt);
                                return;
                            }
                            msgpack::sbuffer message_buffer;
                            message_buffer.write(response_buffer->data(), response_size);
                            promise->set_value(std::move(message_buffer));
                        }
                );
            }
    );
    return promise->get_future();
}

}  // namespace spider::worker
