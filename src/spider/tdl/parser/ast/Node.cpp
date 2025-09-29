#include "Node.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include <ystdlib/error_handling/ErrorCode.hpp>
#include <ystdlib/error_handling/Result.hpp>

using spider::tdl::parser::ast::Node;
using PyGeneratorErrorCodeCategory = ystdlib::error_handling::ErrorCategory<Node::ErrorCodeEnum>;

template <>
auto PyGeneratorErrorCodeCategory::name() const noexcept -> char const* {
    return "spider::tdl::parser::ast::Node";
}

template <>
auto PyGeneratorErrorCodeCategory ::message(Node::ErrorCodeEnum error_enum) const -> std::string {
    switch (error_enum) {
        case Node::ErrorCodeEnum::ChildIndexOutOfBounds:
            return "The child index is out of bounds.";
        case Node::ErrorCodeEnum::ChildIsNull:
            return "The child node is NULL.";
        case Node::ErrorCodeEnum::ParentAlreadySet:
            return "The AST node's parent has already been set.";
        case Node::ErrorCodeEnum::UnexpectedChildNodeType:
            return "The child node type is unexpected.";
        case Node::ErrorCodeEnum::UnknownTypeSpec:
            return "The type spec is unknown.";
        default:
            return "Unknown error code enum";
    }
}

namespace spider::tdl::parser::ast {
namespace {
using ystdlib::error_handling::Result;
}  // namespace

auto Node::get_child(size_t child_idx) const -> Result<Node const*> {
    if (m_children.size() <= child_idx) {
        return Node::ErrorCode{Node::ErrorCodeEnum::ChildIndexOutOfBounds};
    }
    return get_child_unsafe(child_idx);
}

auto Node::add_child(std::unique_ptr<Node> child) -> Result<void> {
    if (nullptr == child) {
        return Node::ErrorCode{Node::ErrorCodeEnum::ChildIsNull};
    }

    if (nullptr != child->get_parent()) {
        return Node::ErrorCode{Node::ErrorCodeEnum::ParentAlreadySet};
    }

    child->m_parent = this;
    m_children.emplace_back(std::move(child));
    return ystdlib::error_handling::success();
}
}  // namespace spider::tdl::parser::ast
