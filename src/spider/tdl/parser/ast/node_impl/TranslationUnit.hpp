#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TRANSLATIONUNIT_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TRANSLATIONUNIT_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include <absl/container/flat_hash_map.h>
#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/StructSpec.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>

namespace spider::tdl::parser::ast::node_impl {
/**
 * Represents the root of a TDL AST, encapsulating the entire translation unit.
 * A translation unit maintains a list of namespaces as its children, and also a symbol table for
 * all defined structs as struct specs.
 */
class TranslationUnit : public Node {
public:
    // Factory function
    // Types
    enum class ErrorCodeEnum : uint8_t {
        DuplicatedNamespaceName = 1,
        DuplicatedStructSpecName,
    };

    using ErrorCode = ystdlib::error_handling::ErrorCode<ErrorCodeEnum>;

    /**
     * @param source_location
     * @return A unique pointer to a new `TranslationUnit` instance.
     */
    [[nodiscard]] static auto create(SourceLocation source_location) -> std::unique_ptr<Node> {
        return std::make_unique<TranslationUnit>(TranslationUnit{source_location});
    }

    // Methods implementing `Node`.
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    /**
     * @param name
     * @return A shared pointer to the `StructSpec` with the given name if it exists in the struct
     * spec table, nullptr otherwise.
     */
    [[nodiscard]] auto get_struct_spec(std::string_view name) const
            -> std::shared_ptr<StructSpec const> {
        if (m_struct_spec_table.contains(name)) {
            return m_struct_spec_table.at(name);
        }
        return nullptr;
    }

    /**
     * Adds a `StructSpec` to the struct spec table.
     * @param struct_spec
     * @return A void result on success, or an error code indicating the failure:
     * - TranslationUnit::ErrorCodeEnum::DuplicatedStructSpecName if another struct spec with the
     *   same name already exists.
     */
    [[nodiscard]] auto add_struct_spec(std::shared_ptr<StructSpec const> const& struct_spec)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Adds a `Namespace` node as a child.
     * @param namespace_node
     * @return A void result on success, or an error code indicating the failure:
     * - TranslationUnit::ErrorCodeEnum::DuplicatedNamespaceName if another namespace with the same
     *   name already exists.
     * - Forwards `Node::add_child`'s return values.
     */
    [[nodiscard]] auto add_namespace(std::unique_ptr<Node> namespace_node)
            -> ystdlib::error_handling::Result<void>;

private:
    // Constructor
    explicit TranslationUnit(SourceLocation source_location) : Node{source_location} {}

    // Variables
    absl::flat_hash_map<std::string, std::shared_ptr<StructSpec const>> m_struct_spec_table;
};
}  // namespace spider::tdl::parser::ast::node_impl

YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(
        spider::tdl::parser::ast::node_impl::TranslationUnit::ErrorCodeEnum
);

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TRANSLATIONUNIT_HPP
