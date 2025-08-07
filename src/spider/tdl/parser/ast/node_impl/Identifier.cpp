#include "Identifier.hpp"

#include <cstddef>
#include <string>

#include <fmt/format.h>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/utils.hpp>

namespace spider::tdl::parser::ast::node_impl {
auto Identifier::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    return fmt::format("{}{}: {}", create_indentation(indentation_level), "Identifier", m_name);
}
}  // namespace spider::tdl::parser::ast::node_impl
