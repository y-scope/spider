#include "Task.hpp"

#include <optional>
#include <string>

#include "Data.hpp"

namespace spider {

template <typename T>
auto get_data(std::string const& /*key*/) -> std::optional<Data<T>> {
    return std::nullopt;
}

}  // namespace spider
