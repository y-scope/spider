#include "KeyValueData.hpp"

#include <optional>
#include <string>

namespace spider {

auto insert_kv(std::string const& /*key*/, std::string const& /*value*/) {}

auto get_kv(std::string const& /*key*/) -> std::optional<std::string> {
    return std::nullopt;
}

}  // namespace spider
