#include "FunctionNameManager.hpp"

#include <optional>
#include <string>

#include <boost/dll/alias.hpp>

namespace spider::core {

auto FunctionNameManager::get_function_name(void const* ptr) const -> std::optional<std::string> {
    if (auto const& it = m_name_map.find(ptr); it != m_name_map.end()) {
        return it->second;
    }
    return std::nullopt;
}

}  // namespace spider::core

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
BOOST_DLL_ALIAS(
        spider::core::FunctionNameManager::get_instance,
        function_name_manager_get_instance
);
