#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_HPP

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>

namespace spider::tdl::parser::ast::node_impl {
// Abstract base class for all type nodes in the AST.
class Type : public Node {
protected:
    // Constructor
    explicit Type(SourceLocation source_location) : Node{source_location} {}
};
}  // namespace spider::tdl::parser::ast::node_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_HPP
