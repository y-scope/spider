#ifndef SPIDER_TDL_PARSER_AST_UTILS_HPP
#define SPIDER_TDL_PARSER_AST_UTILS_HPP

#include <cstddef>
#include <string>
#include <type_traits>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/Node.hpp>

namespace spider::tdl::parser::ast {
/**
 * Creates a string with the specified number of indentation levels.
 * Each level of indentation is represented by 2 spaces.
 * @param indentation_level The number of indentation levels to create.
 * @return A string containing the specified number of indentation levels.
 */
[[nodiscard]] auto create_indentation(size_t indentation_level) -> std::string;

/**
 * Serializes an `IntSpec` to a string view.
 * @param spec
 * @return A result containing a string view representation of `spec` on success, or an error code
 * indicating the failure:
 * - Node::ErrorCodeEnum::UnknownTypeSpec if the type spec is unrecognized.
 */
[[nodiscard]] auto serialize_int_spec(IntSpec spec)
        -> ystdlib::error_handling::Result<std::string_view>;

/**
 * Serializes an `FloatSpec` to a string view.
 * @param spec
 * @return A result containing a string view representation of `spec` on success, or an error code
 * indicating the failure:
 * - Node::ErrorCodeEnum::UnknownTypeSpec if the type spec is unrecognized.
 */
[[nodiscard]] auto serialize_float_spec(FloatSpec spec)
        -> ystdlib::error_handling::Result<std::string_view>;

/**
 * Validates that the given node is of the expected type.
 * @tparam ExpectedNodeType
 * @param node The node to validate.
 * @return A result containing void on success, or an error code indicating the failure:
 * - Node::ErrorCodeEnum::UnexpectedChildNodeType if the node is not of the expected type.
 */
template <typename ExpectedNodeType>
requires std::is_base_of_v<Node, ExpectedNodeType>
[[nodiscard]] auto validate_child_node_type(Node const* node)
        -> ystdlib::error_handling::Result<void>;

template <typename ExpectedNodeType>
requires std::is_base_of_v<Node, ExpectedNodeType>
auto validate_child_node_type(Node const* node) -> ystdlib::error_handling::Result<void> {
    if (nullptr == dynamic_cast<ExpectedNodeType const*>(node)) {
        return Node::ErrorCode{Node::ErrorCodeEnum::UnexpectedChildNodeType};
    }
    return ystdlib::error_handling::success();
}
}  // namespace spider::tdl::parser::ast

#endif  // SPIDER_TDL_PARSER_AST_UTILS_HPP
