// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <sstream>
#include <string_view>
#include <utility>

#include <catch2/catch_test_macros.hpp>

#include <spider/tdl/code_gen/python/PyGenerator.hpp>
#include <spider/tdl/parser/parse.hpp>

namespace {
using spider::tdl::code_gen::python::PyGenerator;
using spider::tdl::parser::parse_translation_unit_from_istream;

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

TEST_CASE("Python Codegen `cTestInput1`", "[tdl][codegen][python]") {
    std::istringstream input_stream{std::string{cTestCase1}};
    auto parse_result{parse_translation_unit_from_istream(input_stream)};
    REQUIRE_FALSE(parse_result.has_error());

    std::ostringstream output_stream;
    auto struct_spec_dependency_graph{parse_result.value()->create_struct_spec_dependency_graph()};
    PyGenerator code_generator{
            std::move(parse_result.value()),
            std::move(struct_spec_dependency_graph)
    };
    auto const codegen_result{code_generator.generate(output_stream)};
    REQUIRE_FALSE(codegen_result.has_error());
    constexpr std::string_view cExpectedGeneratedCode{
            "# Auto-generated Python code from TDL\n"
            "\n"
            "from dataclasses import dataclass\n"
            "import spider_py\n"
            "\n"
            "\n"
            "@dataclass\n"
            "class Output:\n"
            "    processed_input: dict[spider_py.Int64, Input]\n"
            "\n"
            "\n"
            "@dataclass\n"
            "class Input:\n"
            "    field_0: spider_py.Int8\n"
            "    field_1: spider_py.Int16\n"
            "    field_2: spider_py.Int32\n"
            "    field_3: spider_py.Int64\n"
            "    field_4: spider_py.Float\n"
            "    field_5: spider_py.Double\n"
            "    field_6: bool\n"
            "    field_7: list[spider_py.Int8]\n"
            "    field_8: dict[list[spider_py.Int8], spider_py.Double]\n"
            "\n"
            "\n"
            "class test1:\n"
            "    @staticmethod\n"
            "    def empty_func():\n"
            "        pass\n"
            "\n"
            "    @staticmethod\n"
            "    def add(\n"
            "        a: spider_py.Int32,\n"
            "        b: spider_py.Int32,\n"
            "    ) -> spider_py.Int64:\n"
            "        pass\n"
            "\n"
            "    @staticmethod\n"
            "    def return_empty_tuple() -> ():\n"
            "        pass\n"
            "\n"
            "    @staticmethod\n"
            "    def return_singleton_tuple(\n"
            "        a: spider_py.Int64,\n"
            "    ) -> (spider_py.Int32):\n"
            "        pass\n"
            "\n"
            "    @staticmethod\n"
            "    def return_tuple_of_containers() -> (list[spider_py.Int8], "
            "dict[list[spider_py.Int8],"
            " dict[spider_py.Int64, list[spider_py.Int8]]]):\n"
            "        pass\n"
            "\n"
            "\n"
            "class test2:\n"
            "    @staticmethod\n"
            "    def process_input(\n"
            "        input: Input,\n"
            "        task_id: spider_py.Int64,\n"
            "    ) -> Output:\n"
            "        pass\n"
            "\n"
            "    @staticmethod\n"
            "    def process_inputs(\n"
            "        inputs: list[Input],\n"
            "        task_id: spider_py.Int64,\n"
            "    ) -> Output:\n"
            "        pass\n"
            "\n"
            "\n"
    };
    REQUIRE(output_stream.str() == cExpectedGeneratedCode);
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
