#include "StructSpecDependencyGraph.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/nodes.hpp>

namespace spider::tdl::pass::analysis {
namespace {
class TarjanSccComputer {
public:
    // Types
    using DfsIndex = size_t;

    /**
     * Represents a vertex in the Tarjan's algorithm for finding strongly connected components.
     */
    class Vertex {
    public:
        // Constructors
        /**
         * @param tarjan_index The index assigned to the vertex in Tarjan's algorithm.
         */
        explicit Vertex(DfsIndex tarjan_index)
                : m_tarjan_index{tarjan_index},
                  m_low_link{tarjan_index} {}

        // Methods
        [[nodiscard]] auto get_tarjan_index() const noexcept -> DfsIndex { return m_tarjan_index; }

        /**
         * Updates the low link value with the given candidate value.
         */
        auto update_low_link(DfsIndex candidate) noexcept -> void {
            m_low_link = std::min(m_low_link, candidate);
        }

        [[nodiscard]] auto get_low_link() const noexcept -> DfsIndex { return m_low_link; }

        [[nodiscard]] auto is_on_stack() const noexcept -> bool { return m_on_stack; }

        auto remove_from_stack() noexcept -> void { m_on_stack = false; }

    private:
        DfsIndex m_tarjan_index;
        DfsIndex m_low_link;
        bool m_on_stack{true};
    };

    class DfsIterator {
    public:
        // Types
        using ChildIdIt = std::vector<size_t>::const_iterator;

        // Constructors
        /**
         * @param id The ID of the vertex in the dependency graph.
         * @param child_ids The IDs of the child vertices in the dependency graph.
         */
        DfsIterator(size_t id, std::vector<size_t> const& child_ids)
                : m_id{id},
                  m_curr_child_it{child_ids.cbegin()},
                  m_end_child_it{child_ids.cend()} {}

        // Methods
        [[nodiscard]] auto get_id() const noexcept -> size_t { return m_id; }

        /**
         * Retrieves the current child vertex to visit during the DFS traversal.
         * @return An optional pair containing:
         *   - The ID of the current child vertex.
         *   - A boolean indicating if this is a backtrace visit.
         * @return std::nullopt if all children have been visited.
         */
        [[nodiscard]] auto get_curr() -> std::optional<std::pair<size_t, bool>> {
            if (m_curr_child_it == m_end_child_it) {
                return std::nullopt;
            }
            auto const result{std::make_pair(*m_curr_child_it, m_is_backtrace)};
            m_is_backtrace = true;
            return result;
        }

        auto advance_to_next_child() noexcept -> void {
            ++m_curr_child_it;
            m_is_backtrace = false;
        }

    private:
        size_t m_id;
        bool m_is_backtrace{false};
        ChildIdIt m_curr_child_it;
        ChildIdIt m_end_child_it;
    };

    // Constructors
    /**
     * @param struct_spec_dep_graph The struct spec dependency graph to compute.
     * NOTE: This object does not take ownership of the input graph, and the input graph must remain
     * valid and immutable for the lifetime of this object.
     */
    explicit TarjanSccComputer(StructSpecDependencyGraph const& struct_spec_dep_graph)
            : m_def_use_chains_view{struct_spec_dep_graph.get_def_use_chains()},
              m_tarjan_vertices(struct_spec_dep_graph.get_def_use_chains().size(), std::nullopt) {
        compute();
    }

    // Delete copy & move constructors and assignment operators
    TarjanSccComputer(TarjanSccComputer const&) = delete;
    TarjanSccComputer(TarjanSccComputer&&) = delete;
    auto operator=(TarjanSccComputer const&) -> TarjanSccComputer& = delete;
    auto operator=(TarjanSccComputer&&) -> TarjanSccComputer& = delete;

    // Destructor
    ~TarjanSccComputer() = default;

    // Methods
    /**
     * Transfers the ownership of the computed strongly connected components to the caller.
     * @return A vector of strongly connected components.
     */
    [[nodiscard]] auto release() -> std::vector<std::vector<size_t>> {
        return std::move(m_computed_strongly_connected_components);
    }

private:
    // Methods
    /**
     * Computes the strongly connected components of the dependency graph using Tarjan's algorithm.
     * The computed SCCs will be stored in `m_computed_strongly_connected_components`. Notice that
     * single-node components are ignored unless the node has a self-loop.
     */
    auto compute() -> void;

    /**
     * Visits a vertex and pushes it onto both the DFS stack and the Tarjan stack.
     * @param id The in-graph ID of the vertex to visit.
     */
    auto visit_and_push_to_stack(size_t id) -> void {
        m_dfs_stack.emplace_back(id, m_def_use_chains_view[id]);
        m_tarjan_stack.emplace_back(id);
        m_tarjan_vertices.at(id).emplace(m_tarjan_index++);
    }

    auto pop_stack_and_form_scc() -> void;

    // Variables
    std::span<std::vector<size_t> const> m_def_use_chains_view;
    std::vector<std::optional<Vertex>> m_tarjan_vertices;
    std::vector<DfsIterator> m_dfs_stack;
    std::vector<size_t> m_tarjan_stack;
    DfsIndex m_tarjan_index{0};
    std::vector<std::vector<size_t>> m_computed_strongly_connected_components;
};

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

auto TarjanSccComputer::compute() -> void {
    // The Tarjan's algorithm ensures the following optional value access will always be valid.
    // NOLINTBEGIN(bugprone-unchecked-optional-access)
    for (size_t id{0}; id < m_def_use_chains_view.size(); ++id) {
        if (m_tarjan_vertices.at(id).has_value()) {
            // Already visited
            continue;
        }
        visit_and_push_to_stack(id);

        while (false == m_dfs_stack.empty()) {
            auto& curr_dfs_it{m_dfs_stack.back()};
            auto const optional_curr_child{curr_dfs_it.get_curr()};
            if (optional_curr_child.has_value()) {
                auto const [child_id, is_backtrace]{*optional_curr_child};
                auto const& child_tarjan_vertex{m_tarjan_vertices.at(child_id)};

                if (is_backtrace) {
                    // This is a backtrace path of the DFS.
                    m_tarjan_vertices.at(curr_dfs_it.get_id())
                            ->update_low_link(child_tarjan_vertex->get_low_link());
                    curr_dfs_it.advance_to_next_child();
                    continue;
                }

                if (false == child_tarjan_vertex.has_value()) {
                    // This child is not visited yet by Tarjan's DFS, push it to the DFS stack.
                    visit_and_push_to_stack(child_id);
                    continue;
                }

                // It's the first time to iterate on this child, but it has already been visited by
                // Tarjan's DFS from another path. Update the low link only if it's on the Tarjan
                // stack.
                if (child_tarjan_vertex->is_on_stack()) {
                    m_tarjan_vertices.at(curr_dfs_it.get_id())
                            ->update_low_link(child_tarjan_vertex->get_tarjan_index());
                }

                curr_dfs_it.advance_to_next_child();
                continue;
            }

            // All children have been iterated, pop from the DFS stack and form an SCC if possible.
            pop_stack_and_form_scc();
        }
    }
    // NOLINTEND(bugprone-unchecked-optional-access)
}

auto TarjanSccComputer::pop_stack_and_form_scc() -> void {
    // The Tarjan's algorithm ensures the following optional value access will always be valid.
    // NOLINTBEGIN(bugprone-unchecked-optional-access)
    auto const node_id{m_dfs_stack.back().get_id()};
    m_dfs_stack.pop_back();
    auto const& tarjan_vertex{m_tarjan_vertices.at(node_id).value()};
    if (tarjan_vertex.get_low_link() != tarjan_vertex.get_tarjan_index()) {
        // Not a root of an SCC
        return;
    }

    std::vector<size_t> scc;
    while (true) {
        auto const popped_id{m_tarjan_stack.back()};
        m_tarjan_stack.pop_back();
        m_tarjan_vertices.at(popped_id)->remove_from_stack();
        scc.emplace_back(popped_id);
        if (popped_id == node_id) {
            break;
        }
    }

    if (scc.size() > 1) {
        m_computed_strongly_connected_components.emplace_back(std::move(scc));
        return;
    }

    // Detect self-loop, otherwise ignore this single-node SCC
    auto const& def_use_chain{m_def_use_chains_view[scc.front()]};
    if (std::ranges::find(def_use_chain, scc.front()) != def_use_chain.cend()) {
        m_computed_strongly_connected_components.emplace_back(std::move(scc));
    }
    // NOLINTEND(bugprone-unchecked-optional-access)
}

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

auto StructSpecDependencyGraph::compute_strongly_connected_components() -> void {
    m_strongly_connected_components.reset();
    m_strongly_connected_components.emplace(TarjanSccComputer{*this}.release());
}
}  // namespace spider::tdl::pass::analysis
