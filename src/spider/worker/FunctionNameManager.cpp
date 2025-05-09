#include "FunctionNameManager.hpp"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <boost/dll/alias.hpp>

namespace spider::core {
auto FunctionNameManager::get_instance() -> FunctionNameManager& {
    static FunctionNameManager instance;
    return instance;
}

auto FunctionNameManager::get(TaskFunctionPointer const ptr) const
        -> FunctionNameMap::const_iterator {
    for (auto it = m_name_map.cbegin(); it != m_name_map.cend(); ++it) {
        if (it->first == ptr) {
            return it;
        }
    }
    return m_name_map.cend();
}

auto FunctionNameManager::get_function_name(TaskFunctionPointer const ptr) const
        -> std::optional<std::string> {
    auto const it = get(ptr);
    if (it != m_name_map.cend()) {
        return it->second;
    }
    return std::nullopt;
}
}  // namespace spider::core

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
BOOST_DLL_ALIAS(
        spider::core::FunctionNameManager::get_instance,
        g_function_name_manager_get_instance
);
