#include "NamedVar.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/Type.hpp>
#include <spider/tdl/parser/ast/utils.hpp>
#include <spider/tdl/parser/ast/Node.hpp>

namespace spider::tdl::parser::ast::node_impl {
auto NamedVar::create(std::unique_ptr<Node> id, std::unique_ptr<Node> type)
        -> ystdlib::error_handling::Result<std::unique_ptr<Node>> {
    YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Identifier>(id.get()));
    YSTDLIB_ERROR_HANDLING_TRYV(validate_child_node_type<Type>(type.get()));

    auto named_var{std::make_unique<NamedVar>(NamedVar{})};
    YSTDLIB_ERROR_HANDLING_TRYV(named_var->add_child(std::move(id)));
    YSTDLIB_ERROR_HANDLING_TRYV(named_var->add_child(std::move(type)));
    return named_var;
}

auto NamedVar::serialize_to_str(size_t indentation_level) const
        -> ystdlib::error_handling::Result<std::string> {
    return fmt::format(
            "{}[NamedVar]:\n{}\n{}",
            create_indentation(indentation_level),
            YSTDLIB_ERROR_HANDLING_TRYX(get_id()->serialize_to_str(indentation_level + 1)),
            YSTDLIB_ERROR_HANDLING_TRYX(get_type()->serialize_to_str(indentation_level + 1))
    );
}
}  // namespace spider::tdl::parser::ast::node_impl
