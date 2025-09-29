#ifndef SPIDER_TDL_PASS_PASS_HPP
#define SPIDER_TDL_PASS_PASS_HPP

#include <memory>
#include <string>

#include <boost/outcome/std_result.hpp>

namespace spider::tdl::pass {
/**
 * Represents an abstract pass over a TDL AST.
 */
class Pass {
public:
    // Types
    /**
     * Represents an abstract error that can occur during the execution of a pass.
     */
    class Error {
    public:
        // Constructor
        Error() = default;

        // Delete copy constructor and assignment operator
        Error(Error const&) = delete;
        auto operator=(Error const&) -> Error& = delete;

        // Default move constructor and assignment operator
        Error(Error&&) = default;
        auto operator=(Error&&) -> Error& = default;

        // Destructor
        virtual ~Error() = default;

        // Methods
        [[nodiscard]] virtual auto to_string() const -> std::string = 0;
    };

    // Constructors
    Pass() = default;

    // Delete copy constructor and assignment operator
    Pass(Pass const&) = delete;
    auto operator=(Pass const&) -> Pass& = delete;

    // Default move constructor and assignment operator
    Pass(Pass&&) = default;
    auto operator=(Pass&&) -> Pass& = default;

    // Destructor
    virtual ~Pass() = default;

    // Methods
    /**
     * Executes the pass.
     * @return A void result on success, or a pointer to the error on failure.
     */
    [[nodiscard]] virtual auto run() -> boost::outcome_v2::std_checked<void, std::unique_ptr<Error>>
            = 0;
};
}  // namespace spider::tdl::pass

#endif  // SPIDER_TDL_PASS_PASS_HPP
