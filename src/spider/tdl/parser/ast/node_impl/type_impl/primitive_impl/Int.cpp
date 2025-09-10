#include "Int.hpp"

#include <cstddef>
#include <string>

#include <fmt/format.h>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/utils.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl {
auto Int::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    return fmt::format(
            "{}[Type[Primitive[Int]]]{}:{}",
            create_indentation(indentation_level),
            get_source_location().serialize_to_str(),
            YSTDLIB_ERROR_HANDLING_TRYX(serialize_int_spec(m_spec))
    );
}
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl
