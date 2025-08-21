// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <sstream>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

#include <spider/tdl/parser/parse.hpp>
#include <spider/tdl/parser/SourceLocation.hpp>

namespace {
using spider::tdl::parser::parse_translation_unit_from_istream;
using spider::tdl::parser::SourceLocation;

constexpr std::string_view cTestInput1{R"(
namespace test1 {
    // Function with no parameters and no return type
    fn empty_func();

    // Function with parameters and return type
    fn add(a: int32, b: int32) -> int64;

    // Function that returns an empty tuple
    fn return_empty_tuple() -> Tuple<>;
}

struct Input {
    field_0: int8,
    field_1: int16,
    field_2: int32,
    field_3: int64,
    field_4: float,
    field_5: double,
    field_6: bool,
    field_7: List<int8>,
    field_8: Map<List<int8>, double>,
};

struct Output {
    // Notice that the field doesn't end with a comma
    processed_input: Map<int64, Input>
};

namespace test2 {
    fn process_input(input: Input, task_id: int64) -> Output;
    fn process_inputs(inputs: List<Input>, task_id: int64) -> Output;
}
)"};

TEST_CASE("Parser test basic", "[tdl][parser]") {
    std::istringstream input_stream{std::string{cTestInput1}};
    REQUIRE_FALSE(parse_translation_unit_from_istream(input_stream).has_error());
}

TEST_CASE("Parser errors", "[tdl][parser]") {
    SECTION("Namespace without an identifier") {
        constexpr std::string_view cEmptyNamespaceInput{"namespace { fn empty_func(); }"};
        std::istringstream input_stream{std::string{cEmptyNamespaceInput}};
        auto const parse_result{parse_translation_unit_from_istream(input_stream)};
        REQUIRE(parse_result.has_error());
        auto const& error{parse_result.error()};

        constexpr std::string_view cExpectedErrorMessage{"Parser: missing ID at '{'"};
        constexpr SourceLocation cExpectedErrorLocation{1, 10};
        REQUIRE(error.get_message() == cExpectedErrorMessage);
        REQUIRE(error.get_source_location() == cExpectedErrorLocation);
    }

    SECTION("Tuple as a variable type") {
        constexpr std::string_view cTupleAsFuncParam{
                "namespace test { fn empty_func(invalid: Tuple<int8>); }"
        };
        std::istringstream input_stream{std::string{cTupleAsFuncParam}};
        auto const parse_result{parse_translation_unit_from_istream(input_stream)};
        REQUIRE(parse_result.has_error());
        auto const& error{parse_result.error()};

        constexpr std::string_view cExpectedErrorMessage{
                "Parser: mismatched input 'Tuple' expecting {'List', 'Map', 'int8', 'int16', "
                "'int32', 'int64', 'float', 'double', 'bool', ID}"
        };
        constexpr SourceLocation cExpectedErrorLocation{1, 40};
        REQUIRE(error.get_message() == cExpectedErrorMessage);
        REQUIRE(error.get_source_location() == cExpectedErrorLocation);
    }
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
