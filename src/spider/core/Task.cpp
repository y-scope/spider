#include "Task.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <spdlog/spdlog.h>

#include <spider/io/MsgPack.hpp>  // IWYU pragma: keep
#include <spider/io/Serializer.hpp>  // IWYU pragma: keep

namespace spider::core {
auto Task::get_arg_buffers() const -> std::optional<std::vector<msgpack::sbuffer>> {
    std::vector<msgpack::sbuffer> arg_buffers;
    for (size_t i = 0; i < m_inputs.size(); ++i) {
        TaskInput const& input = m_inputs[i];
        std::optional<std::string> const optional_value = input.get_value();
        if (optional_value.has_value()) {
            std::string const& value = optional_value.value();
            arg_buffers.emplace_back();
            arg_buffers.back().write(value.data(), value.size());
            continue;
        }
        std::optional<boost::uuids::uuid> const optional_data_id = input.get_data_id();
        if (optional_data_id.has_value()) {
            boost::uuids::uuid const data_id = optional_data_id.value();
            arg_buffers.emplace_back();
            msgpack::pack(arg_buffers.back(), data_id);
            continue;
        }
        spdlog::error(
                "Task {} {} input {} has no value or data id",
                m_function_name,
                boost::uuids::to_string(m_id),
                i
        );
        return std::nullopt;
    }
    return arg_buffers;
}
}  // namespace spider::core
