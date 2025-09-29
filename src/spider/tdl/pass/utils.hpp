#ifndef SPIDER_TDL_PASS_UTILS_HPP
#define SPIDER_TDL_PASS_UTILS_HPP

#include <tuple>
#include <type_traits>
#include <vector>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/nodes.hpp>

namespace spider::tdl::pass {
/**
 * Visits all `Struct` nodes in the AST rooted at `root` in a depth-first manner, invoking the given
 * `visitor` for each `Struct` node encountered.
 * @tparam StructVisitor
 * @param root The root to start traversal from.
 * @param visitor
 * @return A void result on success, or an error code indicating the failure:
 * - Forwards `visitor`'s return values.
 */
template <typename StructVisitor>
requires std::is_invocable_r_v<
        ystdlib::error_handling::Result<void>,
        StructVisitor,
        parser::ast::Struct const*
>
[[nodiscard]] auto visit_struct_node_using_dfs(parser::ast::Node const* root, StructVisitor visitor)
        -> ystdlib::error_handling::Result<void>;

template <typename StructVisitor>
requires std::is_invocable_r_v<
        ystdlib::error_handling::Result<void>,
        StructVisitor,
        parser::ast::Struct const*
>
auto visit_struct_node_using_dfs(parser::ast::Node const* root, StructVisitor visitor)
        -> ystdlib::error_handling::Result<void> {
    std::vector<parser::ast::Node const*> ast_dfs_stack{root};
    while (false == ast_dfs_stack.empty()) {
        auto const* node{ast_dfs_stack.back()};
        ast_dfs_stack.pop_back();

        if (node == nullptr) {
            // NOTE: This check is required by clang-tidy. In practice, this should never happen.
            continue;
        }

        auto const* node_as_struct{dynamic_cast<parser::ast::Struct const*>(node)};
        if (nullptr != node_as_struct) {
            YSTDLIB_ERROR_HANDLING_TRYV(visitor(node_as_struct));
            continue;
        }

        std::ignore = node->visit_children(
                [&](parser::ast::Node const& child) -> ystdlib::error_handling::Result<void> {
                    ast_dfs_stack.emplace_back(&child);
                    return ystdlib::error_handling::success();
                }
        );
    }
    return ystdlib::error_handling::success();
}
}  // namespace spider::tdl::pass

#endif  // SPIDER_TDL_PASS_UTILS_HPP
