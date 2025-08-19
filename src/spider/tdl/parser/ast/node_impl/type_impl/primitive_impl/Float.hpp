#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_FLOAT_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_FLOAT_HPP

#include <cstddef>
#include <memory>
#include <string>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Primitive.hpp>
#include <spider/tdl/parser/ast/SourceLocation.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl {
class Float : public Primitive {
public:
    // Factory function
    /**
     * @param spec
     * @param source_location
     * @return A unique pointer to a new `Float` instance with the given type spec.
     */
    [[nodiscard]] static auto create(FloatSpec spec, SourceLocation source_location)
            -> std::unique_ptr<Node> {
        return std::make_unique<Float>(Float{spec, source_location});
    }

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    [[nodiscard]] auto get_spec() const -> FloatSpec { return m_spec; }

private:
    // Constructor
    Float(FloatSpec spec, SourceLocation source_location)
            : Primitive{source_location},
              m_spec{spec} {}

    // Variables
    FloatSpec m_spec;
};
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_FLOAT_HPP
