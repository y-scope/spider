#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_HPP

#include <spider/tdl/parser/ast/node_impl/Type.hpp>
#include <spider/tdl/parser/ast/SourceLocation.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl {
// Abstract base class for all primitive type nodes in the AST.
class Primitive : public Type {
protected:
    // Constructor
    explicit Primitive(SourceLocation source_location) : Type{source_location} {}
};
}  // namespace spider::tdl::parser::ast::node_impl::type_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_HPP
