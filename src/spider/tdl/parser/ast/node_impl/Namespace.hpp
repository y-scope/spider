#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_NAMESPACE_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_NAMESPACE_HPP

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
#include <spider/tdl/parser/ast/node_impl/Function.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>

namespace spider::tdl::parser::ast::node_impl {
/**
 * Represents a named collection of `Function`s.
 */
class Namespace : public Node {
public:
    // Types
    enum class ErrorCodeEnum : uint8_t {
        DuplicatedFunctionName = 1,
        EmptyNamespace,
    };

    using ErrorCode = ystdlib::error_handling::ErrorCode<ErrorCodeEnum>;

    // Factory function
    /**
     * @param name
     * @param functions
     * @return A result containing a unique pointer to a new `Namespace` instance with the given
     * name and functions on success, or an error code indicating the failure:
     * - ErrorCodeEnum::DuplicatedFunctionName if the `functions` contains duplicated function
     *   names.
     * - ErrorCodeEnum::EmptyNamespace if the `functions` is empty.
     * - Forwards `validate_child_node_type`'s return values.
     */
    [[nodiscard]] static auto
    create(std::unique_ptr<Node> name, std::vector<std::unique_ptr<Node>> functions)
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

    [[nodiscard]] auto get_num_functions() const -> size_t { return get_num_children() - 1; }

    /**
     * Visits all functions.
     * @tparam FuncVisitor
     * @param visitor
     * @return A void result on success, or an error code indicating the failure:
     * - Forwards `visitor`'s return values.
     */
    template <typename FuncVisitor>
    requires(std::is_invocable_r_v<
             ystdlib::error_handling::Result<void>,
             FuncVisitor,
             Function const&>)
    [[nodiscard]] auto visit_functions(FuncVisitor visitor) const
            -> ystdlib::error_handling::Result<void> {
        for (size_t child_idx{1}; child_idx < get_num_children(); ++child_idx) {
            // The factory function ensures that all the child nodes are `NamedVar` except the first
            // one.
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
            YSTDLIB_ERROR_HANDLING_TRYV(
                    visitor(static_cast<Function const&>(*get_child_unsafe(child_idx)))
            );
        }
        return ystdlib::error_handling::success();
    }

private:
    // Constructor
    Namespace() = default;
};
}  // namespace spider::tdl::parser::ast::node_impl

YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(
        spider::tdl::parser::ast::node_impl::Namespace::ErrorCodeEnum
);

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_NAMESPACE_HPP
