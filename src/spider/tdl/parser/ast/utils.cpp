#include "utils.hpp"

#include <cstddef>
#include <string>
#include <string_view>

#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/FloatSpec.hpp>
#include <spider/tdl/parser/ast/IntSpec.hpp>
#include <spider/tdl/parser/ast/Node.hpp>

namespace spider::tdl::parser::ast {
auto create_indentation(size_t indentation_level) -> std::string {
    // We can't use braced init list for the following string initialization, as the compiler will
    // treat the init list as chars.
    // NOLINTNEXTLINE(modernize-return-braced-init-list)
    return std::string(indentation_level * 2, ' ');
}

auto serialize_int_spec(IntSpec spec) -> ystdlib::error_handling::Result<std::string_view> {
    switch (spec) {
        case IntSpec::Int8:
            return "int8";
        case IntSpec::Int16:
            return "int16";
        case IntSpec::Int32:
            return "int32";
        case IntSpec::Int64:
            return "int64";
        default:
            return Node::ErrorCode{Node::ErrorCodeEnum::UnknownTypeSpec};
    }
}

auto serialize_float_spec(FloatSpec spec) -> ystdlib::error_handling::Result<std::string_view> {
    switch (spec) {
        case FloatSpec::Float:
            return "float";
        case FloatSpec::Double:
            return "double";
        default:
            return Node::ErrorCode{Node::ErrorCodeEnum::UnknownTypeSpec};
    }
}
}  // namespace spider::tdl::parser::ast
