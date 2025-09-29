#include "DetectStructCircularDependency.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/pass/Pass.hpp>

namespace spider::tdl::pass::analysis {
auto DetectStructCircularDependency::Error::to_string() const -> std::string {
    std::vector<std::string> circular_dependency_group_error_messages;
    circular_dependency_group_error_messages.reserve(m_strongly_connected_components.size());
    for (auto const& group : m_strongly_connected_components) {
        std::vector<std::string> struct_descriptions;
        struct_descriptions.reserve(group.size());
        for (auto const& struct_spec : group) {
            struct_descriptions.emplace_back(
                    fmt::format(
                            "  `{}` at {}",
                            struct_spec->get_name(),
                            struct_spec->get_source_location().serialize_to_str()
                    )
            );
        }
        circular_dependency_group_error_messages.emplace_back(
                fmt::format(
                        "Found a circular dependency group of {} struct spec(s):\n{}",
                        group.size(),
                        fmt::join(struct_descriptions, "\n")
                )
        );
    }
    return fmt::format(
            "Found {} circular dependency group(s):\n{}",
            m_strongly_connected_components.size(),
            fmt::join(circular_dependency_group_error_messages, "\n")
    );
}

auto DetectStructCircularDependency::run()
        -> boost::outcome_v2::std_checked<void, std::unique_ptr<Pass::Error>> {
    auto const& strongly_connected_components{
            m_struct_spec_dependency_graph->get_strongly_connected_components()
    };
    if (strongly_connected_components.empty()) {
        return boost::outcome_v2::success();
    }

    std::vector<std::vector<std::shared_ptr<parser::ast::StructSpec const>>>
            circular_dependency_groups;
    circular_dependency_groups.reserve(strongly_connected_components.size());
    for (auto const& scc : strongly_connected_components) {
        std::vector<std::shared_ptr<parser::ast::StructSpec const>> group;
        group.reserve(scc.size());
        for (auto const id : scc) {
            group.emplace_back(m_struct_spec_dependency_graph->get_struct_spec_from_id(id));
        }
        std::ranges::sort(group, [](auto const& lhs, auto const& rhs) -> bool {
            return lhs->get_source_location() < rhs->get_source_location();
        });
        circular_dependency_groups.emplace_back(std::move(group));
    }

    std::ranges::sort(circular_dependency_groups, [](auto const& lhs, auto const& rhs) -> bool {
        // Compare by the source location of the first struct spec in each group. This is safe
        // because:
        // - Each group is guaranteed to be non-empty.
        // - Each struct spec should only appear in one SCC, which guarantees the source locations
        //   are unique.
        return lhs.front()->get_source_location() < rhs.front()->get_source_location();
    });

    return boost::outcome_v2::failure(
            std::make_unique<Error>(std::move(circular_dependency_groups))
    );
}
}  // namespace spider::tdl::pass::analysis
