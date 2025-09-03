#include "Bool.hpp"

#include <cstddef>
#include <string>

#include <fmt/format.h>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/utils.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl {
auto Bool::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    return fmt::format(
            "{}[Type[Primitive[Bool]]]{}",
            create_indentation(indentation_level),
            get_source_location().serialize_to_str()
    );
}
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl
