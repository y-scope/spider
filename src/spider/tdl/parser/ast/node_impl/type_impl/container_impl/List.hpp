#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_IMPL_LIST_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_IMPL_LIST_HPP

#include <cstddef>
#include <memory>
#include <string>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Type.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Container.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl {
class List : public Container {
public:
    // Factory function
    /**
     * @param element_type The type of elements in the list.
     * @return A result containing a unique pointer to a new `List` instance with the given name on
     * success, or an error code indicating the failure:
     * - Forwards `validate_child_node_type`'s return values.
     */
    [[nodiscard]] static auto create(std::unique_ptr<Node> element_type)
            -> ystdlib::error_handling::Result<std::unique_ptr<Node>>;

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    [[nodiscard]] auto get_element_type() const noexcept -> Type const* {
        // The factory function ensures that the first child is of type `Type`.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return static_cast<Type const*>(get_child_unsafe(0));
    }

private:
    // Constructor
    List() = default;
};
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_IMPL_LIST_HPP
