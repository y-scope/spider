#ifndef SPIDER_TDL_CODE_GEN_PY_GENERATOR_HPP
#define SPIDER_TDL_CODE_GEN_PY_GENERATOR_HPP

#include <memory>
#include <ostream>
#include <utility>

#include <boost/outcome/std_result.hpp>

#include <spider/tdl/code_gen/Generator.hpp>
#include <spider/tdl/Error.hpp>
#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/pass/analysis/StructSpecDependencyGraph.hpp>

namespace spider::tdl::code_gen::python {
class PyGenerator : public Generator {
public:
    // Constructor
    PyGenerator(
            std::unique_ptr<parser::ast::TranslationUnit> translation_unit,
            std::shared_ptr<pass::analysis::StructSpecDependencyGraph> dependency_graph
    )
            : Generator{std::move(translation_unit), std::move(dependency_graph)} {}

    // Methods implementing `Generator`.
    [[nodiscard]] auto generate(std::ostream& out_stream)
            -> boost::outcome_v2::std_checked<void, Error> override;
};
}  // namespace spider::tdl::code_gen::python

#endif  // SPIDER_TDL_CODE_GEN_PY_GENERATOR_HPP
