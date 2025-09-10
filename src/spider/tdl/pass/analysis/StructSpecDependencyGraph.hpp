#ifndef SPIDER_TDL_PASS_ANALYSIS_STRUCTSPECDEPENDENCYGRAPH_HPP
#define SPIDER_TDL_PASS_ANALYSIS_STRUCTSPECDEPENDENCYGRAPH_HPP

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <absl/container/flat_hash_map.h>

#include <spider/tdl/parser/ast/node_impl/StructSpec.hpp>

namespace spider::tdl::pass::analysis {
/**
 * Represents a dependency graph of struct specifications defined in a translation unit.
 */
class StructSpecDependencyGraph {
public:
    // Types
    using StructSpec = parser::ast::node_impl::StructSpec;

    // Constructors
    /**
     * @param struct_specs A map from struct names to their corresponding `StructSpec` objects.
     */
    explicit StructSpecDependencyGraph(
            absl::flat_hash_map<std::string, std::shared_ptr<StructSpec const>> const& struct_specs
    );

    // Delete copy constructor and assignment operator
    StructSpecDependencyGraph(StructSpecDependencyGraph const&) = delete;
    auto operator=(StructSpecDependencyGraph const&) -> StructSpecDependencyGraph& = delete;

    // Default move constructor and assignment operator
    StructSpecDependencyGraph(StructSpecDependencyGraph&&) = default;
    auto operator=(StructSpecDependencyGraph&&) -> StructSpecDependencyGraph& = default;

    // Destructor
    ~StructSpecDependencyGraph() = default;

    // Methods
    [[nodiscard]] auto get_num_struct_specs() const -> size_t { return m_struct_spec_refs.size(); }

    [[nodiscard]] auto get_strongly_connected_components()
            -> std::vector<std::vector<size_t>> const& {
        if (false == m_strongly_connected_components.has_value()) {
            compute_strongly_connected_components();
        }
        // The value is guaranteed to be present after the above check and computation.
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        return m_strongly_connected_components.value();
    }

    [[nodiscard]] auto get_def_use_chains() const -> std::vector<std::vector<size_t>> const& {
        return m_def_use_chains;
    }

    /**
     * @param id
     * @return A shared pointer to the `StructSpec` with the given ID if it exists, otherwise
     * nullptr.
     */
    [[nodiscard]] auto get_struct_spec_from_id(size_t id) const
            -> std::shared_ptr<StructSpec const> {
        if (id >= m_struct_spec_refs.size()) {
            return nullptr;
        }
        return m_struct_spec_refs.at(id);
    }

private:
    // Methods
    /**
     * Computes the strongly connected components of the dependency graph using Tarjan's algorithm.
     * The computed results are stored in `m_strongly_connected_components`.
     */
    auto compute_strongly_connected_components() -> void;

    // Variables
    std::vector<std::shared_ptr<StructSpec const>> m_struct_spec_refs;
    absl::flat_hash_map<StructSpec const*, size_t> m_struct_spec_ids;
    std::vector<std::vector<size_t>> m_def_use_chains;

    std::optional<std::vector<std::vector<size_t>>> m_strongly_connected_components;
};
}  // namespace spider::tdl::pass::analysis

#endif  // SPIDER_TDL_PASS_ANALYSIS_STRUCTSPECDEPENDENCYGRAPH_HPP
