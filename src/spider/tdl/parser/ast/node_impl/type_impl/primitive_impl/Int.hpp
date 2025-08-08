#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_INT_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_INT_HPP

#include <cstddef>
#include <memory>
#include <string>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Primitive.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl {
class Int : public Primitive {
public:
    // Factory function
    /**
     * @param spec
     * @return A unique pointer to a new `Int` instance with the given type spec.
     */
    [[nodiscard]] static auto create(IntSpec spec) -> std::unique_ptr<Node> {
        return std::make_unique<Int>(Int{spec});
    }

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    [[nodiscard]] auto get_spec() const -> IntSpec { return m_spec; }

private:
    // Constructor
    explicit Int(IntSpec spec) : m_spec{spec} {}

    // Variables
    IntSpec m_spec;
};
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_INT_HPP
