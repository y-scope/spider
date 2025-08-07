#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_NAMEDVAR_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_NAMEDVAR_HPP

#include <memory>

#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/Type.hpp>

namespace spider::tdl::parser::ast::node_impl {
/**
 * Represents a named variable in the AST. A named variable contains an identifier and a type.
 */
class NamedVar : public Node {
public:
    // Factory function
    /**
     * @param id
     * @param type
     * @return A result containing a unique pointer to a new `NamedVar` instance with the given name
     * on success, or an error code indicating the failure:
     * - Map::ErrorCodeEnum::UnsupportedKeyType if the `key_type` is not supported.
     * - Forwards `validate_child_node_type`'s return values.
     */
    [[nodiscard]] static auto create(std::unique_ptr<Node> id, std::unique_ptr<Node> type)
            -> ystdlib::error_handling::Result<std::unique_ptr<Node>>;

private:
    // Constructor
    NamedVar() = default;
};
}  // namespace spider::tdl::parser::ast::node_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_NAMEDVAR_HPP
