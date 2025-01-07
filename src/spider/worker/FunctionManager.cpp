#include "FunctionManager.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include <boost/dll/alias.hpp>
#include <spdlog/spdlog.h>

#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "TaskExecutorMessage.hpp"

namespace spider::core {

auto response_get_error(msgpack::sbuffer const& buffer
) -> std::optional<std::tuple<FunctionInvokeError, std::string>> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::ARRAY != object.type || 3 != object.via.array.size) {
            return std::nullopt;
        }

        if (worker::TaskExecutorResponseType::Error
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            return std::nullopt;
        }

        return std::make_tuple(
                object.via.array.ptr[1].as<FunctionInvokeError>(),
                object.via.array.ptr[2].as<std::string>()
        );
    } catch (msgpack::type_error& e) {
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

auto create_error_response(FunctionInvokeError error, std::string const& message)
        -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(3);
    packer.pack(worker::TaskExecutorResponseType::Error);
    packer.pack(error);
    packer.pack(message);
    return buffer;
}

void create_error_buffer(
        FunctionInvokeError error,
        std::string const& message,
        msgpack::sbuffer& buffer
) {
    msgpack::packer packer{buffer};
    packer.pack_array(2);
    packer.pack(error);
    packer.pack(message);
}

auto response_get_result_buffers(msgpack::sbuffer const& buffer
) -> std::optional<std::vector<msgpack::sbuffer>> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        std::vector<msgpack::sbuffer> result_buffers;
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::ARRAY != object.type || object.via.array.size < 2) {
            spdlog::error("Cannot split result into buffers: Wrong type");
            return std::nullopt;
        }

        if (worker::TaskExecutorResponseType::Result
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            spdlog::error(
                    "Cannot split result into buffers: Wrong response type {}",
                    static_cast<std::underlying_type_t<worker::TaskExecutorResponseType>>(
                            object.via.array.ptr[0].as<worker::TaskExecutorResponseType>()
                    )
            );
            return std::nullopt;
        }

        for (size_t i = 1; i < object.via.array.size; ++i) {
            msgpack::object const& obj = object.via.array.ptr[i];
            result_buffers.emplace_back();
            msgpack::pack(result_buffers.back(), obj);
        }
        return result_buffers;
    } catch (msgpack::type_error& e) {
        spdlog::error("Cannot split result into buffers: {}", e.what());
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

auto FunctionManager::get_function(std::string const& name) const -> Function const* {
    if (auto const func_iter = m_function_map.find(name); func_iter != m_function_map.end()) {
        return &(func_iter->second);
    }
    return nullptr;
}

}  // namespace spider::core

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
BOOST_DLL_ALIAS(spider::core::FunctionManager::get_instance, function_manager_get_instance)
