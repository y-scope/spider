// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <sstream>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

#include <spider/tdl/parser/parse.hpp>
#include <spider/tdl/pass/analysis/DetectUndefinedStruct.hpp>

namespace {
using spider::tdl::parser::parse_translation_unit_from_istream;
using spider::tdl::pass::analysis::DetectUndefinedStruct;

constexpr std::string_view cTestCase1{R"(// Start of a TDL file. This is line#1.
struct class0 {
    use_0: classN, // Undefined struct reference `classN`
    use_1: Map<int8, List<Map<int16, class2>>>,
};

namespace ns1 {
    fn func0(input: class0) -> class1;

    fn func1(
        input: class2 // Undefined struct reference `class2`
    ) -> class3; // Undefined struct reference `class3`

    fn func2(a: ClassA, b: ClassB) -> int32; // Undefined struct references `ClassA` and `ClassB`
}

struct class1 {
    use_0: class0,
    use_1: Map<int8, List<Map<int16, class2>>>, // Undefined struct reference `class2`
};
)"};

constexpr std::string_view cTestCase2{R"(// Start of a TDL file. This is line#1.
struct class0 {
    use_1: Map<int8, List<Map<int16, int32>>>,
};

namespace ns1 {
    fn func0(input: class0) -> class1;
}

struct class1 {
    use_0: class0,
};
)"};

TEST_CASE("DetectUndefinedStruct Case 1", "[tdl][pass][analytics][DetectUndefinedStruct]") {
    std::istringstream input_stream{std::string{cTestCase1}};
    auto const parse_result{parse_translation_unit_from_istream(input_stream)};
    REQUIRE_FALSE(parse_result.has_error());
    auto const& translation_unit{parse_result.value()};

    auto detect_undefined_struct_pass{DetectUndefinedStruct{translation_unit.get()}};
    auto const run_result{detect_undefined_struct_pass.run()};
    REQUIRE(run_result.has_error());
    auto const* error{dynamic_cast<DetectUndefinedStruct::Error const*>(run_result.error().get())};
    REQUIRE(nullptr != error);
    constexpr std::string_view cExpectedErrorMessage{
            "Found 7 undefined struct reference(s):\n"
            "Referencing to an undefined struct `classN` at (3:11)\n"
            "Referencing to an undefined struct `class2` at (4:37)\n"
            "Referencing to an undefined struct `class2` at (11:15)\n"
            "Referencing to an undefined struct `class3` at (12:9)\n"
            "Referencing to an undefined struct `ClassA` at (14:16)\n"
            "Referencing to an undefined struct `ClassB` at (14:27)\n"
            "Referencing to an undefined struct `class2` at (19:37)"
    };
    REQUIRE(cExpectedErrorMessage == error->to_string());
}

TEST_CASE("DetectUndefinedStruct Case 2", "[tdl][pass][analytics][DetectUndefinedStruct]") {
    std::istringstream input_stream{std::string{cTestCase2}};
    auto const parse_result{parse_translation_unit_from_istream(input_stream)};
    REQUIRE_FALSE(parse_result.has_error());
    auto const& translation_unit{parse_result.value()};

    auto detect_undefined_struct_pass{DetectUndefinedStruct{translation_unit.get()}};
    auto const run_result{detect_undefined_struct_pass.run()};
    REQUIRE_FALSE(run_result.has_error());
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
