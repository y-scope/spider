#ifndef SPIDER_TEST_SIGNAL_TEST_LIB_HPP
#define SPIDER_TEST_SIGNAL_TEST_LIB_HPP

#include <csignal>

#include <spider/client/TaskContext.hpp>

/*
 * Singleton class to store the signal number.
 */
class SignalNumber {
public:
    /*
     * @return The singleton instance of SignalNumber.
     */
    static auto get_instance() -> SignalNumber&;

    /*
     * @return The signal number.
     */
    [[nodiscard]] auto get_signal_number() const -> int;

    /*
     * @param signal_number The signal number to set.
     */
    auto set_signal_number(int signal_number) -> void;

private:
    std::sig_atomic_t volatile m_signal_number{0};
};

/*
 * Signal handler function for SIGTERM. Sets the signal number in the singleton instance.
 * @param signal_number The signal number to handle.
 */
auto signal_catcher(int signal_number) -> void;

/*
 * Installs the signal handler on SIGTERM to watch for whether the signal handler is triggered.
 * @return Signal number if the installed signal handler is triggered, 0 otherwise.
 */
auto signal_handler_test(spider::TaskContext& /*context*/, int /*x*/) -> int;

#endif
