#ifndef SPIDER_WORKER_TASKEXECUTORMESSAGE_HPP
#define SPIDER_WORKER_TASKEXECUTORMESSAGE_HPP

#include <cstdint>

#include "MsgPack.hpp"  // IWYU pragma: keep

namespace spider::worker {
enum class TaskExecutorMessageType : std::uint8_t {
    Unknown = 0,
    Result,
    Error,
    Block,
    Ready,
};

inline auto get_message_type(msgpack::sbuffer const& buffer) -> TaskExecutorMessageType {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    msgpack::object_handle handle = msgpack::unpack(buffer.data(), buffer.size());
    msgpack::object object = handle.get();
    if (object.type != msgpack::type::ARRAY || object.via.array.size < 2) {
        return TaskExecutorMessageType::Unknown;
    }
    msgpack::object header = object.via.array.ptr[0];
    try {
        return header.as<TaskExecutorMessageType>();
    } catch (msgpack::type_error const&) {
        return TaskExecutorMessageType::Unknown;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

}  // namespace spider::worker

#endif  // SPIDER_WORKER_TASKEXECUTORMESSAGE_HPP
