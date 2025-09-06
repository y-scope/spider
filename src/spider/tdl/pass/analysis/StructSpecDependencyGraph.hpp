#ifndef SPIDER_TDL_PASS_ANALYSIS_STRUCTSPECDEPENDENCYGRAPH_HPP
#define SPIDER_TDL_PASS_ANALYSIS_STRUCTSPECDEPENDENCYGRAPH_HPP

#include <cstddef>
#include <memory>
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

private:
    // Variables
    std::vector<std::shared_ptr<StructSpec const>> m_struct_spec_refs;
    absl::flat_hash_map<StructSpec const*, size_t> m_struct_spec_ids;
    absl::flat_hash_map<size_t, std::vector<size_t>> m_def_use_chains;
};
}  // namespace spider::tdl::pass::analysis

#endif  // SPIDER_TDL_PASS_ANALYSIS_STRUCTSPECDEPENDENCYGRAPH_HPP
