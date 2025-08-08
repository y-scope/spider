#include "utils.hpp"

#include <cstddef>
#include <string>

namespace spider::tdl::parser::ast {
auto create_indentation(size_t indentation_level) -> std::string {
    // We can't use braced init list for the following string initialization, as the compiler will
    // treat the init list as chars.
    // NOLINTNEXTLINE(modernize-return-braced-init-list)
    return std::string(indentation_level * 2, ' ');
}
}  // namespace spider::tdl::parser::ast
