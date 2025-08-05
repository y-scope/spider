#include "Node.hpp"

#include <string>

#include <ystdlib/error_handling/ErrorCode.hpp>

using spider::tdl::parser::ast::Node;
using NodeErrorCodeCategory = ystdlib::error_handling::ErrorCategory<Node::ErrorCodeEnum>;

template <>
auto NodeErrorCodeCategory::name() const noexcept -> char const* {
    return "spider::ast::tdl::Node";
}

template <>
auto NodeErrorCodeCategory ::message(Node::ErrorCodeEnum error_enum) const -> std::string {
    switch (error_enum) {
        case Node::ErrorCodeEnum::PlaceholderError:
            return "This is a placeholder error code enum";
        default:
            return "Unknown error code enum";
    }
}
