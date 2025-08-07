#include "Float.hpp"

#include <cstddef>
#include <string>

#include <fmt/format.h>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/utils.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl {
auto Float::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    return fmt::format(
            "{}[Type[Primitive[Float]]]:{}",
            create_indentation(indentation_level),
            YSTDLIB_ERROR_HANDLING_TRYX(serialize_float_spec(m_spec))
    );
}
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl
