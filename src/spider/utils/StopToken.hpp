#ifndef SPIDER_UTILS_STOPTOKEN_HPP
#define SPIDER_UTILS_STOPTOKEN_HPP

#include <csignal>

namespace spider::core {
class StopToken {
public:
    auto request_stop() -> void { m_stop = 1; }

    [[nodiscard]] auto stop_requested() const -> bool { return 0 != m_stop; }

    auto reset() -> void { m_stop = 0; }

private:
    std::sig_atomic_t volatile m_stop{0};
};
}  // namespace spider::core

#endif
