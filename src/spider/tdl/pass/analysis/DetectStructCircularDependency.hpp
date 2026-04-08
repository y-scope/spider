#ifndef SPIDER_TDL_PASS_ANALYSIS_DETECTSTRUCTCIRCULARDEPENDENCY_HPP
#define SPIDER_TDL_PASS_ANALYSIS_DETECTSTRUCTCIRCULARDEPENDENCY_HPP

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/outcome/std_result.hpp>

#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/pass/analysis/StructSpecDependencyGraph.hpp>
#include <spider/tdl/pass/Pass.hpp>

namespace spider::tdl::pass::analysis {
/**
 * Wrapper of `StructSpecDependencyGraph` to detect circular dependencies among struct specs.
 */
class DetectStructCircularDependency : public Pass {
public:
    // Types
    /**
     * Represents an error including all circular dependency groups (reported as strongly connected
     * components).
     */
    class Error : public Pass::Error {
    public:
        // Constructor
        explicit Error(
                std::vector<std::vector<std::shared_ptr<parser::ast::StructSpec const>>>
                        strongly_connected_components
        )
                : m_strongly_connected_components{std::move(strongly_connected_components)} {}

        // Methods implementing `Pass::Error`
        [[nodiscard]] auto to_string() const -> std::string override;

    private:
        // Variables
        std::vector<std::vector<std::shared_ptr<parser::ast::StructSpec const>>>
                m_strongly_connected_components;
    };

    // Constructor
    explicit DetectStructCircularDependency(
            std::shared_ptr<StructSpecDependencyGraph> struct_spec_dependency_graph
    )
            : m_struct_spec_dependency_graph{std::move(struct_spec_dependency_graph)} {}

    // Methods implementing `Pass`
    /**
     * @return A void result on success, or a pointer to `DetectStructCircularDependency::Error`
     * on failure.
     */
    [[nodiscard]] auto run()
            -> boost::outcome_v2::std_checked<void, std::unique_ptr<Pass::Error>> override;

private:
    std::shared_ptr<StructSpecDependencyGraph> m_struct_spec_dependency_graph;
};
}  // namespace spider::tdl::pass::analysis

#endif  // SPIDER_TDL_PASS_ANALYSIS_DETECTSTRUCTCIRCULARDEPENDENCY_HPP
