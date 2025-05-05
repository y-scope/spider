#ifndef SPIDER_CORE_FUNCTIONNAMEMANAGER_HPP
#define SPIDER_CORE_FUNCTIONNAMEMANAGER_HPP

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
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
using FunctionNameMap = std::vector<std::pair<std::uintptr_t, std::string_view>>;

class FunctionNameManager {
public:
    FunctionNameManager(FunctionNameManager const&) = delete;

    auto operator=(FunctionNameManager const&) -> FunctionNameManager& = delete;

    FunctionNameManager(FunctionNameManager&&) = delete;

    auto operator=(FunctionNameManager&&) -> FunctionNameManager& = delete;

    static auto get_instance() noexcept -> FunctionNameManager&;

    template <typename F>
    auto register_function(std::string_view name, F function_pointer) noexcept -> bool {
        if (std::ranges::find_if(
                    m_name_map,
                    [function_pointer](auto const& pair) {
                        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                        return pair.first == reinterpret_cast<uintptr_t>(function_pointer);
                    }
            )
            != m_name_map.end())
        {
            return false;
        }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        m_name_map.emplace_back(reinterpret_cast<uintptr_t>(function_pointer), name);
        return true;
    }

    [[nodiscard]] auto get_function_name(uintptr_t ptr) const -> std::optional<std::string>;

    [[nodiscard]] auto get_function_name_map() const -> FunctionNameMap const& {
        return m_name_map;
    }

private:
    FunctionNameManager() = default;

    ~FunctionNameManager() = default;

    FunctionNameMap m_name_map;
};
}  // namespace spider::core

#endif  // SPIDER_CORE_FUNCTIONNAMEMANAGER_HPP
