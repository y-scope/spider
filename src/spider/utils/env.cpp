#include "env.hpp"

#include <optional>
#include <string>
#include <string_view>

#include <boost/process/v2/environment.hpp>

namespace spider::utils {
auto get_env(std::string_view key) -> std::optional<std::string> {
    auto const env = boost::process::v2::environment::current();
    for (auto const& entry : env) {
        if (entry.key().string() == key) {
            return entry.value().string();
        }
    }
    return std::nullopt;
}
}  // namespace spider::utils
