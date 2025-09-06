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
        absl::flat_hash_set<size_t> use_ids;
        auto field_visitor
                = [&](parser::ast::NamedVar const& field) -> ystdlib::error_handling::Result<void> {
            auto const* type_as_struct{dynamic_cast<parser::ast::Struct const*>(field.get_type())};
            if (nullptr == type_as_struct) {
                return ystdlib::error_handling::success();
            }

            auto const struct_name{type_as_struct->get_name()};
            auto const it{struct_specs.find(struct_name)};
            if (struct_specs.cend() == it) {
                // This is a dangling reference, which will be caught in other analysis pass. In
                // this dependency graph, we just ignore it.
                return ystdlib::error_handling::success();
            }

            auto const use_id{m_struct_spec_ids.at(it->second.get())};
            use_ids.emplace(use_id);
            return ystdlib::error_handling::success();
        };

        std::ignore = def->visit_fields(field_visitor);
        m_def_use_chains.emplace_back(use_ids.cbegin(), use_ids.cend());
    }
}
}  // namespace spider::tdl::pass::analysis
