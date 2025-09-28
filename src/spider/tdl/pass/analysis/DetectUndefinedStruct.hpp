#ifndef SPIDER_TDL_PASS_ANALYSIS_DETECTUNDEFINEDSTRUCTREF_HPP
#define SPIDER_TDL_PASS_ANALYSIS_DETECTUNDEFINEDSTRUCTREF_HPP

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/outcome/std_result.hpp>

#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/pass/Pass.hpp>

namespace spider::tdl::pass::analysis {
/**
 * A pass that detects undefined struct references in a TDL translation unit.
 * NOTE: The translation unit must outlive the pass instance.
 */
class DetectUndefinedStruct : public Pass {
public:
    // Types
    /**
     * Represents an error including all undefined structs.
     * NOTE: The translation unit must outlive the error object.
     */
    class Error : public Pass::Error {
    public:
        // Constructor
        explicit Error(std::vector<parser::ast::Struct const*> undefined_struct)
                : m_undefined_struct{std::move(undefined_struct)} {}

        // Methods implementing `Pass::Error`
        [[nodiscard]] auto to_string() const -> std::string override;

    private:
        // Variables
        std::vector<parser::ast::Struct const*> m_undefined_struct;
    };

    // Constructor
    explicit DetectUndefinedStruct(parser::ast::TranslationUnit const* translation_unit)
            : m_translation_unit{translation_unit} {}

    // Methods implementing `Pass`
    /**
     * @return A void result on success, or a pointer to `DetectUndefinedStruct::Error` on failure.
     */
    [[nodiscard]] auto run()
            -> boost::outcome_v2::std_checked<void, std::unique_ptr<Pass::Error>> override;

private:
    // Variables
    parser::ast::TranslationUnit const* m_translation_unit;
};
}  // namespace spider::tdl::pass::analysis

#endif  // SPIDER_TDL_PASS_ANALYSIS_DETECTUNDEFINEDSTRUCTREF_HPP
