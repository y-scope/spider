#ifndef SPIDER_WORKER_TASKEXECUTORMESSAGE_HPP
#define SPIDER_WORKER_TASKEXECUTORMESSAGE_HPP

#include <cstdint>

#include "../io/MsgPack.hpp"  // IWYU pragma: keep

namespace spider::worker {
enum class TaskExecutorResponseType : std::uint8_t {
    Unknown = 0,
    Result,
    Error,
    Block,
    Ready,
    Cancel,
};

inline auto get_response_type(msgpack::sbuffer const& buffer) -> TaskExecutorResponseType {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
    msgpack::object const object = handle.get();
    if (object.type != msgpack::type::ARRAY || object.via.array.size < 2) {
        return TaskExecutorResponseType::Unknown;
    }
    msgpack::object const header = object.via.array.ptr[0];
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

class TaskExecutorRequestParser {
public:
    /**
     * @param buffer
     * @throw std::bad_cast if the buffer does not store a valid msgpack object
     */
    explicit TaskExecutorRequestParser(msgpack::sbuffer const& buffer)
            : m_obj(msgpack::unpack(buffer.data(), buffer.size())) {}

    /**
     * @return The type of the message.
     */
    [[nodiscard]] auto get_type() const -> TaskExecutorRequestType {
        // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
        msgpack::object const object = m_obj.get();
        if (object.type != msgpack::type::ARRAY || object.via.array.size < 2) {
            return TaskExecutorRequestType::Unknown;
        }
        msgpack::object const header = object.via.array.ptr[0];
        try {
            return header.as<TaskExecutorRequestType>();
        } catch (msgpack::type_error const&) {
            return TaskExecutorRequestType::Unknown;
        }
        // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }

    /**
     * @return The body of the message. Cannot outlive the `TaskExecutorRequestParser` object.
     */
    [[nodiscard]] auto get_body() const -> msgpack::object {
        // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
        msgpack::object const object = m_obj.get();
        return object.via.array.ptr[1];
        // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }

private:
    msgpack::object_handle m_obj;
};
}  // namespace spider::worker

// MSGPACK_ADD_ENUM must be called in global namespace
MSGPACK_ADD_ENUM(spider::worker::TaskExecutorResponseType);
MSGPACK_ADD_ENUM(spider::worker::TaskExecutorRequestType);

#endif  // SPIDER_WORKER_TASKEXECUTORMESSAGE_HPP
