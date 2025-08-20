// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <sstream>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

#include <spider/tdl/parser/parse.hpp>

namespace {
constexpr std::string_view cTestInput1{
        "namespace test1 {\n"
        "    // Function with no parameters and no return type\n"
        "    fn empty_func();\n"
        "\n"
        "    // Function with parameters and return type\n"
        "    fn add(a: int32, b: int32) -> int64;\n"
        "\n"
        "    // Function that returns an empty tuple\n"
        "    fn return_empty_tuple() -> Tuple<>;\n"
        "}\n"
        "\n"
        "struct Input {\n"
        "    field_0: int8,\n"
        "    field_1: int16,\n"
        "    field_2: int32,\n"
        "    field_3: int64,\n"
        "    field_4: float,\n"
        "    field_5: double,\n"
        "    field_6: bool,\n"
        "    field_7: List<int8>,\n"
        "    field_8: Map<List<int8>, double>,\n"
        "    field_9: Tuple<int8, int16, int32>,\n"
        "};\n"
        "\n"
        "struct Output {\n"
        "    // Notice that the field doesn't end with a comma\n"
        "    processed_input: Map<int64, Input>\n"
        "};\n"
        "\n"
        "namespace test2 {\n"
        "    fn process_input(input: Input, task_id: int64) -> Output;\n"
        "    fn process_inputs(inputs: List<Input>, task_id: int64) -> Output;\n"
        "}"
};

TEST_CASE("Parser test basic", "[tdl][parser]") {
    using namespace spider::tdl::parser;
    std::istringstream input_stream{std::string{cTestInput1}};
    REQUIRE(parse_translation_unit_from_istream(input_stream));
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
