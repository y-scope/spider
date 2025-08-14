#ifndef SPIDER_TDL_PARSER_AST_NODE_IMPL_FUNCTION_HPP
#define SPIDER_TDL_PARSER_AST_NODE_IMPL_FUNCTION_HPP

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <cstdint>
#include <vector>

#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>
#include <spider/tdl/parser/ast/node_impl/Identifier.hpp>
#include <spider/tdl/parser/ast/node_impl/NamedVar.hpp>
#include <spider/tdl/parser/ast/node_impl/Type.hpp>

namespace spider::tdl::parser::ast::node_impl {
class Function : public Node {
public:
    // Types
    enum class ErrorCodeEnum : uint8_t {
        DuplicatedParamName = 1,
    };

    using ErrorCode = ystdlib::error_handling::ErrorCode<ErrorCodeEnum>;

    // Factory function
    /**
     * @param name
     * @param return_type
     * @param params
     * @return A result containing a unique pointer to a new `Function` instance with the given
     * name, return type, and parameters on success, or an error code indicating the failure:
     * - ErrorCodeEnum::DuplicatedParamName if `params` contains duplicated parameter names.
     * - Forwards `validate_child_node_type`'s return values.
     */
    [[nodiscard]] static auto create(
            std::unique_ptr<Node> name,
            std::unique_ptr<Node> return_type,
            std::vector<std::unique_ptr<Node>> params
    ) -> ystdlib::error_handling::Result<std::unique_ptr<Node>>;

    // Methods implementing `Node`
    [[nodiscard]] auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string> override;

    // Methods
    [[nodiscard]] auto has_return() const noexcept -> bool { return m_has_return; }

    [[nodiscard]] auto get_name() const noexcept -> std::string_view {
        // The factory function ensures that the first child is of type `Identifier`.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return static_cast<Identifier const*>(get_child_unsafe(0))->get_name();
    }

    /**
     * @return The return type of the function, or nullptr if the function doesn't have a return.
     */
    [[nodiscard]] auto get_return_type() const noexcept -> Type const* {
        if (false == m_has_return) {
            return nullptr;
        }
        // The factory function ensures that the second child is of type `Type`, if not nullptr.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return static_cast<Type const*>(get_child_unsafe(1));
    }

    [[nodiscard]] auto get_num_params() const noexcept -> size_t {
        return get_num_children() - get_num_non_param_children();
    }

    /**
     * Visits parameters.
     * @tparam ParamVisitor
     * @param visitor
     * @return A void result on success, or an error code indicating the failure:
     * - Forwards `visitor`'s return values.
     */
    template <typename ParamVisitor>
    requires(std::is_invocable_r_v<
             ystdlib::error_handling::Result<void>,
             ParamVisitor,
             NamedVar const&>)
    [[nodiscard]] auto visit_params(ParamVisitor visitor) const
            -> ystdlib::error_handling::Result<void> {
        for (size_t child_idx{get_num_non_param_children()}; child_idx < get_num_children();
             ++child_idx)
        {
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
    explicit Function(bool has_return) : m_has_return{has_return} {}

    // Methods
    [[nodiscard]] auto get_num_non_param_children() const noexcept -> size_t {
        return m_has_return ? 2 : 1;
    }

    // Variables
    bool m_has_return;
};
}  // namespace spider::tdl::parser::ast::node_impl

YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(
        spider::tdl::parser::ast::node_impl::Function::ErrorCodeEnum
);

#endif  // SPIDER_TDL_PARSER_AST_NODE_IMPL_FUNCTION_HPP
