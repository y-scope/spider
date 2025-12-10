#include "env.hpp"

#include <optional>
#include <string>
#include <string_view>

#include <boost/process/environment.hpp>
#include <fmt/format.h>

namespace spider::utils {
auto get_env(std::string const& key) -> std::optional<std::string> {
    boost::process::environment env = boost::this_process::environment();
    auto const it = env.find(key);
    if (it == env.end()) {
        return std::nullopt;
    }
    return it->to_string();
}
}  // namespace spider::utils
