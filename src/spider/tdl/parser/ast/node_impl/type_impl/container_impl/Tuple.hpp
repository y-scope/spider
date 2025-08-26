#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_IMPL_TUPLE_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_IMPL_TUPLE_HPP

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Container.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl {
class Tuple : public Container {
public:
    // Factory function
    /**
     * @param elements
     * @param source_location
     * @return A result containing a unique pointer to a new `Tuple` instance as a collection of the
     * given element types, or an error code indicating the failure:
     * - Forwards `validate_child_node_type`'s return values.
     */
    [[nodiscard]] static auto
    create(std::vector<std::unique_ptr<Node>> elements, SourceLocation source_location)
            -> ystdlib::error_handling::Result<std::unique_ptr<Node>>;

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    [[nodiscard]] auto is_empty() const -> bool { return 0 == get_num_children(); }

private:
    // Constructor
    explicit Tuple(SourceLocation source_location) : Container{source_location} {}
};
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::container_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_CONTAINER_IMPL_TUPLE_HPP
