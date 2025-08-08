#include "List.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Type.hpp>
#include <spider/tdl/parser/ast/utils.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl {
auto List::create(std::unique_ptr<Node> element_type)
        -> ystdlib::error_handling::Result<std::unique_ptr<Node>> {
    YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Type>(element_type.get()));

    auto list{std::make_unique<List>(List{})};
    YSTDLIB_ERROR_HANDLING_TRYV(list->add_child(std::move(element_type)));
    return list;
}

auto List::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    return fmt::format(
            "{}[Type[Container[List]]]:\n{}ElementType:\n{}",
            create_indentation(indentation_level),
            create_indentation(indentation_level + 1),
            YSTDLIB_ERROR_HANDLING_TRYX(get_element_type()->serialize_to_str(indentation_level + 2))
    );
}
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl
