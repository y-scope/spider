#include "signal-test.hpp"

#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include <spider/client/Driver.hpp>
#include <spider/client/TaskContext.hpp>

auto SignalNumber::get_instance() -> SignalNumber& {
    static SignalNumber instance;
    return instance;
}

auto SignalNumber::set_signal_number(int const signal_number) -> void {
    m_signal_number = signal_number;
}

auto SignalNumber::get_signal_number() const -> int {
    return m_signal_number;
}

namespace {
/*
 * Signal handler function for SIGTERM. Sets the signal number in the singleton instance.
 * @param signal_number The signal number to handle.
 */
auto signal_handler(int const signal_number) -> void {
    SignalNumber::get_instance().set_signal_number(signal_number);
}

constexpr int cSleepTime = 10;
}  // namespace

auto signal_handler_test(spider::TaskContext&, int const) -> int {
    if (SIG_ERR == std::signal(SIGTERM, signal_handler)) {
        std::cerr << "Failed to set signal handler for SIGTERM\n";
        return 1;
    }
    std::this_thread::sleep_for(std::chrono::seconds(cSleepTime));
    int const signal_number = SignalNumber::get_instance().get_signal_number();
    return signal_number;
}

auto sleep_test(spider::TaskContext&, int const seconds) -> int {
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    return 0;
}

// NOLINTNEXTLINE(cert-err58-cpp)
SPIDER_REGISTER_TASK(signal_handler_test);
// NOLINTNEXTLINE(cert-err58-cpp)
SPIDER_REGISTER_TASK(sleep_test);
