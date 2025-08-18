#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_STRUCTSPEC_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_STRUCTSPEC_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/NamedVar.hpp>
#include <spider/tdl/parser/ast/SourceLocation.hpp>

namespace spider::tdl::parser::ast::node_impl {
/**
 * Represents the specification of a struct in the TDL.
 */
class StructSpec : public Node {
public:
    // Types
    enum class ErrorCodeEnum : uint8_t {
        DuplicatedFieldName = 1,
        EmptyStruct,
    };

    using ErrorCode = ystdlib::error_handling::ErrorCode<ErrorCodeEnum>;

    // Factory function
    /**
     * @param name
     * @param fields
     * @param source_location
     * @return A result containing a shared pointer to a new `StructSpec` instance with the name and
     * fields on success, or an error code indicating the failure:
     * - StructSpec::ErrorCodeEnum::DuplicatedFieldName if the `fields` contains duplicated field
     *   names.
     * - StructSpec::ErrorCodeEnum::EmptyStruct if the `fields` is empty.
     * - Forwards `validate_child_node_type`'s return values.
     */
    [[nodiscard]] static auto create(
            std::unique_ptr<Node> name,
            std::vector<std::unique_ptr<Node>> fields,
            SourceLocation source_location
    ) -> ystdlib::error_handling::Result<std::shared_ptr<StructSpec>>;

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    [[nodiscard]] auto get_name() const -> std::string_view {
        // The factory function ensures that the first child is of type `Identifier`.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return static_cast<Identifier const*>(get_child_unsafe(0))->get_name();
    }

    [[nodiscard]] auto get_num_fields() const -> size_t { return get_num_children() - 1; }

    /**
     * Visits the fields.
     * @tparam FieldVisitor
     * @param visitor
     * @return A void result on success, or an error code indicating the failure:
     * - Forwards `visitor`'s return values.
     */
    template <typename FieldVisitor>
    requires(std::is_invocable_r_v<
             ystdlib::error_handling::Result<void>,
             FieldVisitor,
             NamedVar const&>)
    [[nodiscard]] auto visit_fields(FieldVisitor visitor) const
            -> ystdlib::error_handling::Result<void> {
        for (size_t child_idx{1}; child_idx < get_num_children(); ++child_idx) {
            // The factory function ensures that all the child nodes are `NamedVar` except the first
            // one.
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
            YSTDLIB_ERROR_HANDLING_TRYV(
                    visitor(static_cast<NamedVar const&>(*get_child_unsafe(child_idx)))
            );
        }
        return ystdlib::error_handling::success();
    }

private:
    // Constructor
    explicit StructSpec(SourceLocation source_location) : Node{source_location} {}
};
}  // namespace spider::tdl::parser::ast::node_impl

YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(
        spider::tdl::parser::ast::node_impl::StructSpec::ErrorCodeEnum
);

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_STRUCTSPEC_HPP
