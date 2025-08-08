#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_BOOL_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_BOOL_HPP

#include <cstddef>
#include <memory>
#include <string>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Primitive.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl {
class Bool : public Primitive {
public:
    // Factory function
    /**
     * @return A unique pointer to a new `Bool` instance.
     */
    [[nodiscard]] static auto create() -> std::unique_ptr<Node> {
        return std::make_unique<Bool>(Bool{});
    }

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

private:
    // Constructor
    explicit Bool() = default;
};
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_BOOL_HPP
