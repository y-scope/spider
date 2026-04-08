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

constexpr std::string_view cTestCase1{R"(// Start of a TDL file. This is line#1.
namespace test1 {
    // Function with no parameters and no return type
    fn empty_func();

    // Function with parameters and return type
    fn add(a: int32, b: int32) -> int64;

    // Function that returns an empty tuple
    fn return_empty_tuple() -> Tuple<>;

    // Function that returns a Tuple of one element, and takes only one parameter
    fn return_singleton_tuple(a: int64) -> Tuple<int32>;

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
    std::istringstream input_stream{std::string{cTestCase1}};
    auto const parse_result{parse_translation_unit_from_istream(input_stream)};
    REQUIRE_FALSE(parse_result.has_error());
    auto const& translation_unit{parse_result.value()};

    constexpr std::string_view cExpectedSerializedAst{
            "[TranslationUnit](2:0):\n"
            "  StructSpecs:\n"
            "    [StructSpec](19:0):\n"
            "      Name:\n"
            "        [Identifier](19:7):Input\n"
            "      Fields[0]:\n"
            "        [NamedVar](20:4):\n"
            "          Id:\n"
            "            [Identifier](20:4):field_0\n"
            "          Type:\n"
            "            [Type[Primitive[Int]]](20:13):int8\n"
            "      Fields[1]:\n"
            "        [NamedVar](21:4):\n"
            "          Id:\n"
            "            [Identifier](21:4):field_1\n"
            "          Type:\n"
            "            [Type[Primitive[Int]]](21:13):int16\n"
            "      Fields[2]:\n"
            "        [NamedVar](22:4):\n"
            "          Id:\n"
            "            [Identifier](22:4):field_2\n"
            "          Type:\n"
            "            [Type[Primitive[Int]]](22:13):int32\n"
            "      Fields[3]:\n"
            "        [NamedVar](23:4):\n"
            "          Id:\n"
            "            [Identifier](23:4):field_3\n"
            "          Type:\n"
            "            [Type[Primitive[Int]]](23:13):int64\n"
            "      Fields[4]:\n"
            "        [NamedVar](24:4):\n"
            "          Id:\n"
            "            [Identifier](24:4):field_4\n"
            "          Type:\n"
            "            [Type[Primitive[Float]]](24:13):float\n"
            "      Fields[5]:\n"
            "        [NamedVar](25:4):\n"
            "          Id:\n"
            "            [Identifier](25:4):field_5\n"
            "          Type:\n"
            "            [Type[Primitive[Float]]](25:13):double\n"
            "      Fields[6]:\n"
            "        [NamedVar](26:4):\n"
            "          Id:\n"
            "            [Identifier](26:4):field_6\n"
            "          Type:\n"
            "            [Type[Primitive[Bool]]](26:13)\n"
            "      Fields[7]:\n"
            "        [NamedVar](27:4):\n"
            "          Id:\n"
            "            [Identifier](27:4):field_7\n"
            "          Type:\n"
            "            [Type[Container[List]]](27:13):\n"
            "              ElementType:\n"
            "                [Type[Primitive[Int]]](27:18):int8\n"
            "      Fields[8]:\n"
            "        [NamedVar](28:4):\n"
            "          Id:\n"
            "            [Identifier](28:4):field_8\n"
            "          Type:\n"
            "            [Type[Container[Map]]](28:13):\n"
            "              KeyType:\n"
            "                [Type[Container[List]]](28:17):\n"
            "                  ElementType:\n"
            "                    [Type[Primitive[Int]]](28:22):int8\n"
            "              ValueType:\n"
            "                [Type[Primitive[Float]]](28:29):double\n"
            "    [StructSpec](31:0):\n"
            "      Name:\n"
            "        [Identifier](31:7):Output\n"
            "      Fields[0]:\n"
            "        [NamedVar](33:4):\n"
            "          Id:\n"
            "            [Identifier](33:4):processed_input\n"
            "          Type:\n"
            "            [Type[Container[Map]]](33:21):\n"
            "              KeyType:\n"
            "                [Type[Primitive[Int]]](33:25):int64\n"
            "              ValueType:\n"
            "                [Type[Struct]](33:32):\n"
            "                  Name:\n"
            "                    [Identifier](33:32):Input\n"
            "  Namespaces:\n"
            "    [Namespace](2:0):\n"
            "      Name:\n"
            "        [Identifier](2:10):test1\n"
            "      Func[0]:\n"
            "        [Function](4:4):\n"
            "          Name:\n"
            "            [Identifier](4:7):empty_func\n"
            "          Return:\n"
            "            void\n"
            "          No Params\n"
            "      Func[1]:\n"
            "        [Function](7:4):\n"
            "          Name:\n"
            "            [Identifier](7:7):add\n"
            "          Return:\n"
            "            [Type[Primitive[Int]]](7:34):int64\n"
            "          Params[0]:\n"
            "            [NamedVar](7:11):\n"
            "              Id:\n"
            "                [Identifier](7:11):a\n"
            "              Type:\n"
            "                [Type[Primitive[Int]]](7:14):int32\n"
            "          Params[1]:\n"
            "            [NamedVar](7:21):\n"
            "              Id:\n"
            "                [Identifier](7:21):b\n"
            "              Type:\n"
            "                [Type[Primitive[Int]]](7:24):int32\n"
            "      Func[2]:\n"
            "        [Function](10:4):\n"
            "          Name:\n"
            "            [Identifier](10:7):return_empty_tuple\n"
            "          Return:\n"
            "            [Type[Container[Tuple]]](10:31):Empty\n"
            "          No Params\n"
            "      Func[3]:\n"
            "        [Function](13:4):\n"
            "          Name:\n"
            "            [Identifier](13:7):return_singleton_tuple\n"
            "          Return:\n"
            "            [Type[Container[Tuple]]](13:43):\n"
            "              Element[0]:\n"
            "                [Type[Primitive[Int]]](13:49):int32\n"
            "          Params[0]:\n"
            "            [NamedVar](13:30):\n"
            "              Id:\n"
            "                [Identifier](13:30):a\n"
            "              Type:\n"
            "                [Type[Primitive[Int]]](13:33):int64\n"
            "      Func[4]:\n"
            "        [Function](16:4):\n"
            "          Name:\n"
            "            [Identifier](16:7):return_tuple_of_containers\n"
            "          Return:\n"
            "            [Type[Container[Tuple]]](16:39):\n"
            "              Element[0]:\n"
            "                [Type[Container[List]]](16:45):\n"
            "                  ElementType:\n"
            "                    [Type[Primitive[Int]]](16:50):int8\n"
            "              Element[1]:\n"
            "                [Type[Container[Map]]](16:57):\n"
            "                  KeyType:\n"
            "                    [Type[Container[List]]](16:61):\n"
            "                      ElementType:\n"
            "                        [Type[Primitive[Int]]](16:66):int8\n"
            "                  ValueType:\n"
            "                    [Type[Container[Map]]](16:73):\n"
            "                      KeyType:\n"
            "                        [Type[Primitive[Int]]](16:77):int64\n"
            "                      ValueType:\n"
            "                        [Type[Container[List]]](16:84):\n"
            "                          ElementType:\n"
            "                            [Type[Primitive[Int]]](16:89):int8\n"
            "          No Params\n"
            "    [Namespace](36:0):\n"
            "      Name:\n"
            "        [Identifier](36:10):test2\n"
            "      Func[0]:\n"
            "        [Function](37:4):\n"
            "          Name:\n"
            "            [Identifier](37:7):process_input\n"
            "          Return:\n"
            "            [Type[Struct]](37:54):\n"
            "              Name:\n"
            "                [Identifier](37:54):Output\n"
            "          Params[0]:\n"
            "            [NamedVar](37:21):\n"
            "              Id:\n"
            "                [Identifier](37:21):input\n"
            "              Type:\n"
            "                [Type[Struct]](37:28):\n"
            "                  Name:\n"
            "                    [Identifier](37:28):Input\n"
            "          Params[1]:\n"
            "            [NamedVar](37:35):\n"
            "              Id:\n"
            "                [Identifier](37:35):task_id\n"
            "              Type:\n"
            "                [Type[Primitive[Int]]](37:44):int64\n"
            "      Func[1]:\n"
            "        [Function](38:4):\n"
            "          Name:\n"
            "            [Identifier](38:7):process_inputs\n"
            "          Return:\n"
            "            [Type[Struct]](38:62):\n"
            "              Name:\n"
            "                [Identifier](38:62):Output\n"
            "          Params[0]:\n"
            "            [NamedVar](38:22):\n"
            "              Id:\n"
            "                [Identifier](38:22):inputs\n"
            "              Type:\n"
            "                [Type[Container[List]]](38:30):\n"
            "                  ElementType:\n"
            "                    [Type[Struct]](38:35):\n"
            "                      Name:\n"
            "                        [Identifier](38:35):Input\n"
            "          Params[1]:\n"
            "            [NamedVar](38:43):\n"
            "              Id:\n"
            "                [Identifier](38:43):task_id\n"
            "              Type:\n"
            "                [Type[Primitive[Int]]](38:52):int64"
    };
    auto const serialize_result{translation_unit->serialize_to_str(0)};
    REQUIRE_FALSE(serialize_result.has_error());
    REQUIRE(serialize_result.value() == cExpectedSerializedAst);

    auto struct_spec_dependency_graph{translation_unit->create_struct_spec_dependency_graph()};
    REQUIRE(struct_spec_dependency_graph->get_num_struct_specs() == 2);
    REQUIRE(struct_spec_dependency_graph->get_strongly_connected_components().empty());
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
