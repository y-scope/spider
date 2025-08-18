#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_BOOL_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_BOOL_HPP

#include <cstddef>
#include <memory>
#include <string>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/type_impl/Primitive.hpp>
#include <spider/tdl/parser/ast/SourceLocation.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl {
class Bool : public Primitive {
public:
    // Factory function
    /**
     * @param source_location
     * @return A unique pointer to a new `Bool` instance.
     */
    [[nodiscard]] static auto create(SourceLocation source_location) -> std::unique_ptr<Node> {
        return std::make_unique<Bool>(Bool{source_location});
    }

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

private:
    // Constructor
    explicit Bool(SourceLocation source_location) : Primitive{source_location} {}
};
}  // namespace spider::tdl::parser::ast::node_impl::type_impl::primitive_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_PRIMITIVE_IMPL_BOOL_HPP
