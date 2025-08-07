#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_IDENTIFIER_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_IDENTIFIER_HPP

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>

namespace spider::tdl::parser::ast::node_impl {
class Identifier : public Node {
public:
    // Factory function
    /**
     * @param name
     * @return A unique pointer to a new `Identifier` instance with the given name.
     */
    static auto create(std::string name) -> std::unique_ptr<Node> {
        return std::make_unique<Identifier>(Identifier{std::move(name)});
    }

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    [[nodiscard]] auto get_name() const noexcept -> std::string_view { return m_name; }

private:
    // Constructor
    explicit Identifier(std::string name) noexcept : m_name{std::move(name)} {}

    // Variables
    std::string m_name;
};
}  // namespace spider::tdl::parser::ast::node_impl

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_IDENTIFIER_HPP
