#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_HPP

#include <spider/tdl/parser/ast/node_impl/Type.hpp>
#include <spider/tdl/parser/ast/SourceLocation.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl {
// Abstract base class for all container type nodes in the AST.
class Container : public Type {
protected:
    // Constructor
    explicit Container(SourceLocation source_location) : Type{source_location} {}
};
}  // namespace spider::tdl::parser::ast::node_impl::type_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_HPP
