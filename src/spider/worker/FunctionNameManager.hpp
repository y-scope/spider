#ifndef SPIDER_CORE_FUNCTIONNAMEMANAGER_HPP
#define SPIDER_CORE_FUNCTIONNAMEMANAGER_HPP

#include <optional>
#include <string>
#include <utility>
#include <vector>

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define NAME_CONCAT_DIRECT(s1, s2) s1##s2
#define NAME_CONCAT(s1, s2) NAME_CONCAT_DIRECT(s1, s2)
#define NAME_ANONYMOUS_VARIABLE(str) NAME_CONCAT(str, __COUNTER__)
// NOLINTEND(cppcoreguidelines-macro-usage)

#define SPIDER_WORKER_REGISTER_TASK_NAME(func) \
    inline const auto NAME_ANONYMOUS_VARIABLE(var) \
            = spider::core::FunctionNameManager::get_instance().register_function(#func, func);

namespace spider::core {
using TaskFunctionPointer = void (*)();

using FunctionNameMap = std::vector<std::pair<TaskFunctionPointer, std::string>>;

class FunctionNameManager {
public:
    FunctionNameManager(FunctionNameManager const&) = delete;

    auto operator=(FunctionNameManager const&) -> FunctionNameManager& = delete;

    FunctionNameManager(FunctionNameManager&&) = delete;

    auto operator=(FunctionNameManager&&) -> FunctionNameManager& = delete;

    static auto get_instance() -> FunctionNameManager&;

    template <typename F>
    auto register_function(std::string const& name, F function_pointer) -> bool {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        if (m_name_map.cend() != get(reinterpret_cast<TaskFunctionPointer>(function_pointer))) {
            return false;
        }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        m_name_map.emplace_back(reinterpret_cast<TaskFunctionPointer>(function_pointer), name);
        return true;
    }

    [[nodiscard]] auto get_function_name(TaskFunctionPointer ptr) const
            -> std::optional<std::string>;

    [[nodiscard]] auto get_function_name_map() const -> FunctionNameMap const& {
        return m_name_map;
    }

private:
    [[nodiscard]] auto get(TaskFunctionPointer) const -> FunctionNameMap::const_iterator;

    FunctionNameManager() = default;

    ~FunctionNameManager() = default;

    FunctionNameMap m_name_map;
};
}  // namespace spider::core

#endif  // SPIDER_CORE_FUNCTIONNAMEMANAGER_HPP
