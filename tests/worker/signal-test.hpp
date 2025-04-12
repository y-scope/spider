#ifndef SPIDER_TEST_SIGNAL_TEST_LIB_HPP
#define SPIDER_TEST_SIGNAL_TEST_LIB_HPP

#include <csignal>

#include <spider/client/TaskContext.hpp>

class SignalNumber {
public:
    static auto get_instance() -> SignalNumber&;

    [[nodiscard]] auto get_signal_number() const -> int;

    auto set_signal_number(int signal_number) -> void;

private:
    std::sig_atomic_t volatile m_signal_number{0};
};

auto signal_catcher(int signal_number) -> void;

auto signal_handler_test(spider::TaskContext& /*context*/, int /*x*/) -> int;

#endif
