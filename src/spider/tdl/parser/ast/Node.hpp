#ifndef SPIDER_TDL_PARSER_AST_NODE_HPP
#define SPIDER_TDL_PARSER_AST_NODE_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

namespace spider::tdl::parser::ast {
/**
 * Abstracted base class for all AST nodes in the TDL.
 */
class Node {
public:
    // Types
    enum class ErrorCodeEnum : uint8_t {
        ChildIndexOutOfBounds = 1,
        ChildIsNull,
        ParentAlreadySet,
        UnexpectedChildNodeType,
        UnknownTypeSpec,
    };

    using ErrorCode = ystdlib::error_handling::ErrorCode<ErrorCodeEnum>;

    // Delete copy constructor and assignment operator
    Node(Node const&) = delete;
    auto operator=(Node const&) -> Node& = delete;

    // Default move constructor and assignment operator
    Node(Node&&) = default;
    auto operator=(Node&&) -> Node& = default;

    // Destructor
    virtual ~Node() = default;

    // Methods
    /**
     * @return The parent node of this AST node, or nullptr if it has no parent.
     */
    [[nodiscard]] auto get_parent() const noexcept -> Node const* { return m_parent; }

    /**
     * @return The number of children this AST node has.
     */
    [[nodiscard]] auto get_num_children() const noexcept -> size_t { return m_children.size(); }

    /**
     * Gets a child node by its index.
     * @param child_idx
     * @return A result containing a pointer to the child on success, or an error code indicating
     * the failure:
     * - ErrorCodeEnum::ChildIndexOutOfBounds if `child_idx` is out of bounds.
     */
    [[nodiscard]] auto get_child(size_t child_idx) const
            -> ystdlib::error_handling::Result<Node const*>;

    /**
     * Visits the children of this AST node using the provided visitor function.
     * @tparam ChildVisitor
     * @param visitor
     * @return A void result on success, or an error code indicating the failure:
     * - Forwards `visitor`'s return values.
     */
    template <typename ChildVisitor>
    requires(
            std::is_invocable_r_v<ystdlib::error_handling::Result<void>, ChildVisitor, Node const&>
    )
    [[nodiscard]] auto visit_children(ChildVisitor visitor) const
            -> ystdlib::error_handling::Result<void> {
        for (auto const& child : m_children) {
            YSTDLIB_ERROR_HANDLING_TRYV(visitor(*child));
        }
        return ystdlib::error_handling::success();
    }

    /**
     * Serializes this AST node and its children to a string representation.
     * @param indentation_level The indentation level for pretty-printing. Each level of indentation
     * is represented by 2 spaces.
     * @return A result containing the string representation of this AST node, or an error code
     * indicating the failure (implementation-defined).
     */
    [[nodiscard]] virtual auto serialize_to_str(size_t indentation_level) const
            -> ystdlib::error_handling::Result<std::string>
            = 0;

protected:
    // Constructor
    Node() = default;

    /**
     * Adds a child node to this AST node.
     * @param child
     * @return A void result on success, or an error code indicating the failure:
     * - ErrorCodeEnum::ChildIsNull if `child` is NULL.
     * - ErrorCodeEnum::ParentAlreadySet if the child node already has a parent set.
     */
    [[nodiscard]] auto add_child(std::unique_ptr<Node> child)
            -> ystdlib::error_handling::Result<void>;

    /**
     * Gets a child node by its index.
     * NOTE: This method is unsafe. The caller must ensure the given index is valid.
     * @param child_idx
     * @return The child node at the specified index.
     */
    [[nodiscard]] auto get_child_unsafe(size_t child_idx) const -> Node const* {
        return m_children[child_idx].get();
    }

private:
    // Variables
    std::vector<std::unique_ptr<Node>> m_children;
    Node const* m_parent = nullptr;
};
}  // namespace spider::tdl::parser::ast

YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(spider::tdl::parser::ast::Node::ErrorCodeEnum);

#endif  // SPIDER_TDL_PARSER_AST_NODE_HPP
