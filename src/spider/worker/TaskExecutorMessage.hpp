#ifndef SPIDER_WORKER_TASKEXECUTORMESSAGE_HPP
#define SPIDER_WORKER_TASKEXECUTORMESSAGE_HPP

#include <cstdint>

#include "../core/MsgPack.hpp"  // IWYU pragma: keep

namespace spider::worker {
enum class TaskExecutorResponseType : std::uint8_t {
    Unknown = 0,
    Result = 1,
    Error = 2,
    Block = 3,
    Ready = 4,
};

inline auto get_response_type(msgpack::sbuffer const& buffer) -> TaskExecutorResponseType {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    msgpack::object_handle handle = msgpack::unpack(buffer.data(), buffer.size());
    msgpack::object object = handle.get();
    if (object.type != msgpack::type::ARRAY || object.via.array.size < 2) {
        return TaskExecutorResponseType::Unknown;
    }
    msgpack::object header = object.via.array.ptr[0];
    try {
        return header.as<TaskExecutorResponseType>();
    } catch (msgpack::type_error const&) {
        return TaskExecutorResponseType::Unknown;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

enum class TaskExecutorRequestType : std::uint8_t {
    Unknown = 0,
    Arguments,
    Resume,
};

inline auto get_request_type(msgpack::sbuffer const& buffer) -> TaskExecutorRequestType {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    msgpack::object_handle handle = msgpack::unpack(buffer.data(), buffer.size());
    msgpack::object object = handle.get();
    if (object.type != msgpack::type::ARRAY || object.via.array.size < 2) {
        return TaskExecutorRequestType::Unknown;
    }
    msgpack::object header = object.via.array.ptr[0];
    try {
        return header.as<TaskExecutorRequestType>();
    } catch (msgpack::type_error const&) {
        return TaskExecutorRequestType::Unknown;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

}  // namespace spider::worker

#endif  // SPIDER_WORKER_TASKEXECUTORMESSAGE_HPP
