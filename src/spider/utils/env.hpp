#ifndef SPIDER_UTILS_ENV_HPP
#define SPIDER_UTILS_ENV_HPP

#include <optional>
#include <string>
#include <string_view>

namespace spider::utils {
/**
 * Gets the value of an environment variable.
 *
 * @param key The key of the environment variable.
 * @return The value of the environment variable, or `std::nullopt` if not found.
 */
[[nodiscard]] auto get_env(std::string_view key) -> std::optional<std::string>;
}  // namespace spider::utils

#endif
