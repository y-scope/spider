// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <algorithm>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include <catch2/catch_test_macros.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <spider/tdl/parser/parse.hpp>
#include <spider/tdl/pass/analysis/StructSpecDependencyGraph.hpp>

namespace {
using spider::tdl::parser::parse_translation_unit_from_istream;
using spider::tdl::pass::analysis::StructSpecDependencyGraph;

constexpr std::string_view cTestCase1{R"(// Start of a TDL file. This is line#1.
// Each struct is identified by a number, and form the following dependency graph (the edges
// indicate def-use chains):
//                  ┌─────────────► 5 ──┐
//                  │               ▲   │
//                  │               └───┘
//      ┌─────────► 2 ───► 4
//      │           ▲     │
//      │           └─────┘
//      │
//      0 ───► 1
//      ▲     │
//      │     │
//      3 ◄───┘
//
//      6 ──┐
//      ▲   │
//      └───┘

struct class0 {
    use_0: class1,
    use_1: Map<int8, List<Map<int16, class2>>>,
};

struct class1 {
    use_0: class3,
};

struct class2 {
    use_0: class4,
    use_1: class5,
};

struct class3 {
    use_0: class0,
};

struct class4 {
    use_0: class2,
};

struct class5 {
    use_0: class5,
};

struct class6 {
    use_0: List<class6>,
};
)"};

constexpr std::string_view cTestCase2{R"(// Start of a TDL file. This is line#1.
// Each struct is identified by a number, and form the following dependency graph (the edges
// indicate def-use chains):
//               ┌───── 1
//               │      ▲
//               ▼      │
//        0 ───► 2 ───► 3
//        ▲      │
//        │      ▼
//        └───── 4 ───► 5

struct class0 {
    use_0: class2,
};

struct class1 {
    use_0: class2,
};

struct class2 {
    use_0: List<class3>,
    use_1: List<class4>,
};

struct class3 {
    use_0: Map<int32, class1>,
};

struct class4 {
    use_0: Map<int32, class0>,
    use_1: class5,
};

struct class5 {
    no_use: int64,
};
)"};

/**
 * Serializes the strongly connected components into human-readable strings
 * @param graph The struct spec dependency graph containing the struct specs.
 * @return A set of strings, each representing a strongly connected component, with struct specs
 * represented by their names and sorted lexicographically within each component.
 */
[[nodiscard]] auto get_serialized_strongly_connected_components(StructSpecDependencyGraph& graph)
        -> std::set<std::string>;

auto serialize_strongly_connected_components(StructSpecDependencyGraph& graph)
        -> std::set<std::string> {
    auto const& strongly_connected_components{graph.get_strongly_connected_components()};
    std::set<std::string> serialized_strongly_connected_components;
    for (auto const& scc : strongly_connected_components) {
        std::vector<std::string> struct_spec_names;
        struct_spec_names.reserve(scc.size());
        for (auto const id : scc) {
            auto const struct_spec{graph.get_struct_spec_from_id(id)};
            REQUIRE(nullptr != struct_spec);
            struct_spec_names.emplace_back(struct_spec->get_name());
        }
        std::ranges::sort(struct_spec_names);
        serialized_strongly_connected_components.emplace(
                fmt::format("{{{}}}", fmt::join(struct_spec_names, ", "))
        );
    }
    return serialized_strongly_connected_components;
}

TEST_CASE("SCC Detection Case 1", "[tdl][pass][analytics][StructSpecDependencyGraph]") {
    std::istringstream input_stream{std::string{cTestCase1}};
    auto const parse_result{parse_translation_unit_from_istream(input_stream)};
    REQUIRE_FALSE(parse_result.has_error());
    auto const& translation_unit{parse_result.value()};

    auto struct_spec_dependency_graph{translation_unit->create_struct_spec_dependency_graph()};
    REQUIRE(struct_spec_dependency_graph.get_num_struct_specs() == 7);

    auto const serialized_strongly_connected_components{
            serialize_strongly_connected_components(struct_spec_dependency_graph)
    };
    REQUIRE(serialized_strongly_connected_components.size() == 4);
    std::set<std::string> const expected_serialized_strongly_connected_components{
            "{class0, class1, class3}",
            "{class2, class4}",
            "{class5}",
            "{class6}"
    };
    REQUIRE(serialized_strongly_connected_components
            == expected_serialized_strongly_connected_components);
}

TEST_CASE("SCC Detection Case 2", "[tdl][pass][analytics][StructSpecDependencyGraph]") {
    std::istringstream input_stream{std::string{cTestCase2}};
    auto const parse_result{parse_translation_unit_from_istream(input_stream)};
    REQUIRE_FALSE(parse_result.has_error());
    auto const& translation_unit{parse_result.value()};

    auto struct_spec_dependency_graph{translation_unit->create_struct_spec_dependency_graph()};
    REQUIRE(struct_spec_dependency_graph.get_num_struct_specs() == 6);

    auto const serialized_strongly_connected_components{
            serialize_strongly_connected_components(struct_spec_dependency_graph)
    };
    REQUIRE(serialized_strongly_connected_components.size() == 1);
    std::set<std::string> const expected_serialized_strongly_connected_components{
            "{class0, class1, class2, class3, class4}"
    };
    REQUIRE(serialized_strongly_connected_components
            == expected_serialized_strongly_connected_components);
}
}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
