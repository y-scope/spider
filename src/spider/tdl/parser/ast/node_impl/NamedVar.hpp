#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_NAMEDVAR_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_NAMEDVAR_HPP

#include <cstddef>
#include <memory>
#include <string>

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
     * - Forwards `validate_child_node_type`'s return values.
     */
    [[nodiscard]] static auto create(std::unique_ptr<Node> id, std::unique_ptr<Node> type)
            -> ystdlib::error_handling::Result<std::unique_ptr<Node>>;

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    [[nodiscard]] auto get_id() const noexcept -> Identifier const* {
        // The factory function ensures that the first child is of type `Identifier`.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return static_cast<Identifier const*>(get_child_unsafe(0));
    }

    [[nodiscard]] auto get_type() const noexcept -> Type const* {
        // The factory function ensures that the second child is of type `Type`.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return static_cast<Type const*>(get_child_unsafe(1));
    }

private:
    // Constructor
    NamedVar() = default;
};
}  // namespace spider::tdl::parser::ast::node_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_NAMEDVAR_HPP
