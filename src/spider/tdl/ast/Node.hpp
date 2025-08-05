#ifndef SPIDER_TDL_AST_NODE_HPP
#define SPIDER_TDL_AST_NODE_HPP

#include <cstdint>

#include <ystdlib/error_handling/ErrorCode.hpp>

namespace spider::tdl::ast {
/**
 * Abstracted base class for all AST nodes in the TDL.
 */
class Node {
public:
    // Types
    enum class ErrorCodeEnum : uint8_t {
        PlaceholderError = 1,
    };

    using ErrorCode = ystdlib::error_handling::ErrorCode<ErrorCodeEnum>;

private:
};
}  // namespace spider::tdl::ast

YSTDLIB_ERROR_HANDLING_MARK_AS_ERROR_CODE_ENUM(spider::tdl::ast::Node::ErrorCodeEnum);

#endif  // SPIDER_TDL_AST_NODE_HPP
