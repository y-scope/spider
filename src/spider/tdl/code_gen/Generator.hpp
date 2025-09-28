#ifndef SPIDER_TDL_CODE_GEN_GENERATOR_HPP
#define SPIDER_TDL_CODE_GEN_GENERATOR_HPP

#include <memory>
#include <ostream>
#include <utility>

#include <boost/outcome/std_result.hpp>

#include <spider/tdl/Error.hpp>
#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/pass/analysis/StructSpecDependencyGraph.hpp>

namespace spider::tdl::code_gen {
/**
 * Abstract base class for generating code from a translation unit to a target language.
 */
class Generator {
public:
    // Constructor
    Generator(
            std::unique_ptr<parser::ast::TranslationUnit> translation_unit,
            std::shared_ptr<pass::analysis::StructSpecDependencyGraph> struct_spec_dependency_graph
    )
            : m_translation_unit{std::move(translation_unit)},
              m_struct_spec_dependency_graph{std::move(struct_spec_dependency_graph)} {}

    // Delete copy constructor and copy assignment operator
    Generator(Generator const&) = delete;
    auto operator=(Generator const&) -> Generator& = delete;

    // Default move constructor and move assignment operator
    Generator(Generator&&) noexcept = default;
    auto operator=(Generator&&) -> Generator& = delete;

    // Destructor
    virtual ~Generator() = default;

    // Methods
    /**
     * Generates code from the translation unit to the target language.
     * @param out_stream Output stream to write the generated code.
     * @return A void result on success, or an error specified by an `Error` instance on failure.
     */
    [[nodiscard]] virtual auto generate(std::ostream& out_stream)
            -> boost::outcome_v2::std_checked<void, Error>
            = 0;

protected:
    [[nodiscard]] auto get_translation_unit() const -> parser::ast::TranslationUnit const* {
        return m_translation_unit.get();
    }

    [[nodiscard]] auto get_struct_spec_dependency_graph() const
            -> std::shared_ptr<pass::analysis::StructSpecDependencyGraph> {
        return m_struct_spec_dependency_graph;
    }

private:
    std::unique_ptr<parser::ast::TranslationUnit const> m_translation_unit;
    std::shared_ptr<pass::analysis::StructSpecDependencyGraph> m_struct_spec_dependency_graph;
};
}  // namespace spider::tdl::code_gen

#endif  // SPIDER_TDL_CODE_GEN_GENERATOR_HPP
