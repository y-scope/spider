#include "DetectUndefinedStruct.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ystdlib/error_handling/Result.hpp>

#include <spider/tdl/parser/ast/nodes.hpp>
#include <spider/tdl/pass/Pass.hpp>
#include <spider/tdl/pass/utils.hpp>

namespace spider::tdl::pass::analysis {
auto DetectUndefinedStruct::Error::to_string() const -> std::string {
    std::vector<std::string> undefined_struct_error_messages;
    undefined_struct_error_messages.reserve(m_undefined_struct.size());
    for (auto const* undefined_struct : m_undefined_struct) {
        undefined_struct_error_messages.emplace_back(
                fmt::format(
                        "Referencing to an undefined struct `{}` at {}",
                        undefined_struct->get_name(),
                        undefined_struct->get_source_location().serialize_to_str()
                )
        );
    }
    return fmt::format(
            "Found {} undefined struct reference(s):\n{}",
            m_undefined_struct.size(),
            fmt::join(undefined_struct_error_messages, "\n")
    );
}

auto DetectUndefinedStruct::run()
        -> boost::outcome_v2::std_checked<void, std::unique_ptr<Pass::Error>> {
    std::vector<parser::ast::Struct const*> undefined_structs;

    auto struct_visitor
            = [&](parser::ast::Struct const* struct_node) -> ystdlib::error_handling::Result<void> {
        if (nullptr == m_translation_unit->get_struct_spec(struct_node->get_name())) {
            undefined_structs.emplace_back(struct_node);
        }
        return ystdlib::error_handling::success();
    };

    std::ignore = visit_struct_node_using_dfs(m_translation_unit, struct_visitor);
    std::ignore = m_translation_unit->visit_struct_specs(
            [&](
                    parser::ast::StructSpec const* struct_spec
            ) -> ystdlib::error_handling::Result<void> {
                return visit_struct_node_using_dfs(struct_spec, struct_visitor);
            }
    );

    if (undefined_structs.empty()) {
        return boost::outcome_v2::success();
    }
    std::ranges::sort(
            undefined_structs,
            [](parser::ast::Struct const* lhs, parser::ast::Struct const* rhs) -> bool {
                return lhs->get_source_location() < rhs->get_source_location();
            }
    );
    return boost::outcome_v2::failure(
            std::make_unique<DetectUndefinedStruct::Error>(std::move(undefined_structs))
    );
}
}  // namespace spider::tdl::pass::analysis
