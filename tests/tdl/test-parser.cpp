// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <sstream>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

#include <spider/tdl/parser/ast/nodes.hpp>
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

    // Function that returns a tuple of containers
    fn return_tuple_of_containers() -> Tuple<List<int8>, Map<List<int8>, Map<int64, List<int8>>>>;
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

TEST_CASE("Parsing `cTestInput1`", "[tdl][parser]") {
    std::istringstream input_stream{std::string{cTestInput1}};
    auto const parse_result{parse_translation_unit_from_istream(input_stream)};
    REQUIRE_FALSE(parse_result.has_error());
    auto const& translation_unit{parse_result.value()};

    constexpr std::string_view cExpectedSerializedAst{
            "[TranslationUnit]:\n"
            "  StructSpecs:\n"
            "    [StructSpec]:\n"
            "      Name:Input\n"
            "      Fields[0]:\n"
            "        [NamedVar]:\n"
            "          Id:\n"
            "            [Identifier]:field_0\n"
            "          Type:\n"
            "            [Type[Primitive[Int]]]:int8\n"
            "      Fields[1]:\n"
            "        [NamedVar]:\n"
            "          Id:\n"
            "            [Identifier]:field_1\n"
            "          Type:\n"
            "            [Type[Primitive[Int]]]:int16\n"
            "      Fields[2]:\n"
            "        [NamedVar]:\n"
            "          Id:\n"
            "            [Identifier]:field_2\n"
            "          Type:\n"
            "            [Type[Primitive[Int]]]:int32\n"
            "      Fields[3]:\n"
            "        [NamedVar]:\n"
            "          Id:\n"
            "            [Identifier]:field_3\n"
            "          Type:\n"
            "            [Type[Primitive[Int]]]:int64\n"
            "      Fields[4]:\n"
            "        [NamedVar]:\n"
            "          Id:\n"
            "            [Identifier]:field_4\n"
            "          Type:\n"
            "            [Type[Primitive[Float]]]:float\n"
            "      Fields[5]:\n"
            "        [NamedVar]:\n"
            "          Id:\n"
            "            [Identifier]:field_5\n"
            "          Type:\n"
            "            [Type[Primitive[Float]]]:double\n"
            "      Fields[6]:\n"
            "        [NamedVar]:\n"
            "          Id:\n"
            "            [Identifier]:field_6\n"
            "          Type:\n"
            "            [Type[Primitive[Bool]]]\n"
            "      Fields[7]:\n"
            "        [NamedVar]:\n"
            "          Id:\n"
            "            [Identifier]:field_7\n"
            "          Type:\n"
            "            [Type[Container[List]]]:\n"
            "              ElementType:\n"
            "                [Type[Primitive[Int]]]:int8\n"
            "      Fields[8]:\n"
            "        [NamedVar]:\n"
            "          Id:\n"
            "            [Identifier]:field_8\n"
            "          Type:\n"
            "            [Type[Container[Map]]]:\n"
            "              KeyType:\n"
            "                [Type[Container[List]]]:\n"
            "                  ElementType:\n"
            "                    [Type[Primitive[Int]]]:int8\n"
            "              ValueType:\n"
            "                [Type[Primitive[Float]]]:double\n"
            "    [StructSpec]:\n"
            "      Name:Output\n"
            "      Fields[0]:\n"
            "        [NamedVar]:\n"
            "          Id:\n"
            "            [Identifier]:processed_input\n"
            "          Type:\n"
            "            [Type[Container[Map]]]:\n"
            "              KeyType:\n"
            "                [Type[Primitive[Int]]]:int64\n"
            "              ValueType:\n"
            "                [Type[Struct]]:\n"
            "                  Name:\n"
            "                    [Identifier]:Input\n"
            "  Namespaces:\n"
            "    [Namespace]:\n"
            "      Name:test1\n"
            "      Func[0]:\n"
            "        [Function]:\n"
            "          Name:empty_func\n"
            "          Return:\n"
            "            void\n"
            "          No Params\n"
            "      Func[1]:\n"
            "        [Function]:\n"
            "          Name:add\n"
            "          Return:\n"
            "            [Type[Primitive[Int]]]:int64\n"
            "          Params[0]:\n"
            "            [NamedVar]:\n"
            "              Id:\n"
            "                [Identifier]:a\n"
            "              Type:\n"
            "                [Type[Primitive[Int]]]:int32\n"
            "          Params[1]:\n"
            "            [NamedVar]:\n"
            "              Id:\n"
            "                [Identifier]:b\n"
            "              Type:\n"
            "                [Type[Primitive[Int]]]:int32\n"
            "      Func[2]:\n"
            "        [Function]:\n"
            "          Name:return_empty_tuple\n"
            "          Return:\n"
            "            [Type[Container[Tuple]]]:Empty\n"
            "          No Params\n"
            "      Func[3]:\n"
            "        [Function]:\n"
            "          Name:return_tuple_of_containers\n"
            "          Return:\n"
            "            [Type[Container[Tuple]]]:\n"
            "              Element[0]:\n"
            "                [Type[Container[List]]]:\n"
            "                  ElementType:\n"
            "                    [Type[Primitive[Int]]]:int8\n"
            "              Element[1]:\n"
            "                [Type[Container[Map]]]:\n"
            "                  KeyType:\n"
            "                    [Type[Container[List]]]:\n"
            "                      ElementType:\n"
            "                        [Type[Primitive[Int]]]:int8\n"
            "                  ValueType:\n"
            "                    [Type[Container[Map]]]:\n"
            "                      KeyType:\n"
            "                        [Type[Primitive[Int]]]:int64\n"
            "                      ValueType:\n"
            "                        [Type[Container[List]]]:\n"
            "                          ElementType:\n"
            "                            [Type[Primitive[Int]]]:int8\n"
            "          No Params\n"
            "    [Namespace]:\n"
            "      Name:test2\n"
            "      Func[0]:\n"
            "        [Function]:\n"
            "          Name:process_input\n"
            "          Return:\n"
            "            [Type[Struct]]:\n"
            "              Name:\n"
            "                [Identifier]:Output\n"
            "          Params[0]:\n"
            "            [NamedVar]:\n"
            "              Id:\n"
            "                [Identifier]:input\n"
            "              Type:\n"
            "                [Type[Struct]]:\n"
            "                  Name:\n"
            "                    [Identifier]:Input\n"
            "          Params[1]:\n"
            "            [NamedVar]:\n"
            "              Id:\n"
            "                [Identifier]:task_id\n"
            "              Type:\n"
            "                [Type[Primitive[Int]]]:int64\n"
            "      Func[1]:\n"
            "        [Function]:\n"
            "          Name:process_inputs\n"
            "          Return:\n"
            "            [Type[Struct]]:\n"
            "              Name:\n"
            "                [Identifier]:Output\n"
            "          Params[0]:\n"
            "            [NamedVar]:\n"
            "              Id:\n"
            "                [Identifier]:inputs\n"
            "              Type:\n"
            "                [Type[Container[List]]]:\n"
            "                  ElementType:\n"
            "                    [Type[Struct]]:\n"
            "                      Name:\n"
            "                        [Identifier]:Input\n"
            "          Params[1]:\n"
            "            [NamedVar]:\n"
            "              Id:\n"
            "                [Identifier]:task_id\n"
            "              Type:\n"
            "                [Type[Primitive[Int]]]:int64"
    };
    auto const serialize_result{translation_unit->serialize_to_str(0)};
    REQUIRE_FALSE(serialize_result.has_error());
    REQUIRE(serialize_result.value() == cExpectedSerializedAst);
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

    SECTION("Errors from parser exception") {
        using spider::tdl::parser::ast::Namespace;

        constexpr std::string_view cNamespaceContainingDuplicatedFunctionNames{
                " namespace test { fn empty(); fn empty(); }"
        };
        std::istringstream input_stream{std::string{cNamespaceContainingDuplicatedFunctionNames}};
        auto const parse_result{parse_translation_unit_from_istream(input_stream)};
        REQUIRE(parse_result.has_error());
        auto const& error{parse_result.error()};

        constexpr std::string_view cExpectedErrorMessage{"spider::tdl::parser::Exception"};
        constexpr SourceLocation cExpectedSourceLocation{1, 1};
        REQUIRE(error.get_message() == cExpectedErrorMessage);
        REQUIRE(error.get_source_location() == cExpectedSourceLocation);
        auto const optional_error_code{error.get_error_code()};
        REQUIRE(optional_error_code.has_value());
        // Checked by the previous `REQUIRE`.
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        REQUIRE(optional_error_code.value()
                == Namespace::ErrorCode{Namespace::ErrorCodeEnum::DuplicatedFunctionName});
    }
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
