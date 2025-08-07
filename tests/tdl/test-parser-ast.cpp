// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <catch2/catch_test_macros.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/Node.hpp>

namespace {
TEST_CASE("test-ast-node", "[tdl][ast][Node]") {
    using spider::tdl::parser::ast::Node;
    using ystdlib::error_handling::Result;

    Result<void> const result{Node::ErrorCode{Node::ErrorCodeEnum::ChildIndexOutOfBounds}};
    REQUIRE(result.has_error());
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
