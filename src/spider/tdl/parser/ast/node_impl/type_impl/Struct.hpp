#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_STRUCT_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_STRUCT_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/StructSpec.hpp>
#include <spider/tdl/parser/ast/node_impl/Type.hpp>

namespace spider::tdl::parser::ast::node_impl::type_impl {
class Struct : public Type {
public:
    // Types
    enum class ErrorCodeEnum : uint8_t {
        StructSpecAlreadySet = 1,
        StructSpecNameMismatch,
    };

    using ErrorCode = ystdlib::error_handling::ErrorCode<ErrorCodeEnum>;

    // Factory function
    /**
     * @param name
     * @return A result containing a unique pointer to a new `Struct` instance with the given name
     * on success, or an error code indicating the failure:
     * - Forwards `validate_child_node_type`'s return values.
     */
    [[nodiscard]] static auto create(std::unique_ptr<Node> name)
            -> ystdlib::error_handling::Result<std::unique_ptr<Node>>;

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    [[nodiscard]] auto get_name() const -> std::string_view {
        // The factory function ensures that the first child is of type `Identifier`.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return static_cast<Identifier const*>(get_child_unsafe(0))->get_name();
    }

    /**
     * Sets the specification for this struct.
     * @param spec
     * @return A void result on success, or an error code indicating the failure:
     * - ErrorCodeEnum::StructSpecNameMismatch if `spec`'s name does not match the underlying name.
     * - ErrorCodeEnum::StructSpecAlreadySet if the specification has already been set.
     */
    [[nodiscard]] auto set_spec(std::shared_ptr<StructSpec> spec)
            -> ystdlib::error_handling::Result<void>;

    [[nodiscard]] auto get_spec() const -> StructSpec const* { return m_spec.get(); }

private:
    // Constructor
    Struct() = default;

    // Variables
    std::shared_ptr<StructSpec> m_spec;
};
}  // namespace spider::tdl::parser::ast::node_impl::type_impl

YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(
        spider::tdl::parser::ast::node_impl::type_impl::Struct::ErrorCodeEnum
);

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_TYPE_IMPL_STRUCT_HPP
