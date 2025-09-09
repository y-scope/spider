#include "StructSpecDependencyGraph.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/nodes.hpp>

namespace spider::tdl::pass::analysis {
namespace {
/**
 * Collects the IDs of struct specs used by the given struct spec definition.
 * @param def The struct spec definition to analyze.
 * @param struct_specs A map from struct names to their corresponding `StructSpec` objects.
 * @param struct_spec_ids A map from `StructSpec` pointers to their corresponding IDs.
 * @return A vector of struct spec IDs (with no duplication) that are used by the given definition.
 */
[[nodiscard]] auto collect_use_ids(
        parser::ast::StructSpec const* def,
        absl::flat_hash_map<std::string, std::shared_ptr<parser::ast::StructSpec const>> const&
                struct_specs,
        absl::flat_hash_map<parser::ast::StructSpec const*, size_t> const& struct_spec_ids
) -> std::vector<size_t>;

auto collect_use_ids(
        parser::ast::StructSpec const* def,
        absl::flat_hash_map<std::string, std::shared_ptr<parser::ast::StructSpec const>> const&
                struct_specs,
        absl::flat_hash_map<parser::ast::StructSpec const*, size_t> const& struct_spec_ids
) -> std::vector<size_t> {
    absl::flat_hash_set<size_t> use_ids;
    std::vector<parser::ast::Node const*> ast_dfs_stack;
    ast_dfs_stack.emplace_back(def);
    while (false == ast_dfs_stack.empty()) {
        auto const* node{ast_dfs_stack.back()};
        ast_dfs_stack.pop_back();

        if (node == nullptr) {
            // NOTE: This check is required by clang-tidy. In practice, this should never happen.
            continue;
        }

        auto const* node_as_struct{dynamic_cast<parser::ast::Struct const*>(node)};
        if (nullptr == node_as_struct) {
            // Not a struct node, continue DFS by pushing all the child nodes to the stack.
            std::ignore = node->visit_children(
                    [&](parser::ast::Node const& child) -> ystdlib::error_handling::Result<void> {
                        ast_dfs_stack.emplace_back(&child);
                        return ystdlib::error_handling::success();
                    }
            );
            continue;
        }

        auto const struct_name{node_as_struct->get_name()};
        auto const it{struct_specs.find(struct_name)};
        if (struct_specs.cend() == it) {
            // This is a dangling reference, which will be caught in other analysis pass. In this
            // dependency graph, we just ignore it.
            continue;
        }

        use_ids.emplace(struct_spec_ids.at(it->second.get()));
    }

    return std::vector<size_t>{use_ids.cbegin(), use_ids.cend()};
}
}  // namespace

StructSpecDependencyGraph::StructSpecDependencyGraph(
        absl::flat_hash_map<std::string, std::shared_ptr<StructSpec const>> const& struct_specs
) {
    auto const num_struct_specs{struct_specs.size()};

    // Initialize the graph nodes and their ids
    m_struct_spec_refs.reserve(num_struct_specs);
    m_struct_spec_ids.reserve(num_struct_specs);
    for (auto const& [_, struct_spec] : struct_specs) {
        m_struct_spec_refs.emplace_back(struct_spec);
        m_struct_spec_ids.emplace(struct_spec.get(), m_struct_spec_ids.size());
    }

    // Build def-use chains
    m_def_use_chains.reserve(num_struct_specs);
    for (auto const& def : m_struct_spec_refs) {
        m_def_use_chains.emplace_back(collect_use_ids(def.get(), struct_specs, m_struct_spec_ids));
    }
}
}  // namespace spider::tdl::pass::analysis
