#include "FunctionNameManager.hpp"

#include <cstdint>
#include <optional>
#include <string>

#include <boost/dll/alias.hpp>

namespace spider::core {
auto FunctionNameManager::get_instance() noexcept -> FunctionNameManager& {
    static FunctionNameManager instance;
    return instance;
}

auto FunctionNameManager::get_function_name(uintptr_t const ptr) const
        -> std::optional<std::string> {
    for (auto const& it : m_name_map) {
        if (it.first == ptr) {
            return std::string{it.second};
        }
    }
    return std::nullopt;
}
}  // namespace spider::core

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
BOOST_DLL_ALIAS(
        spider::core::FunctionNameManager::get_instance,
        g_function_name_manager_get_instance
);
