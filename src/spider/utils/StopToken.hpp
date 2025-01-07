#ifndef SPIDER_UTILS_STOPTOKEN_HPP
#define SPIDER_UTILS_STOPTOKEN_HPP

#include <atomic>

namespace spider::core {
class StopToken {
public:
    StopToken() : m_stop{false} {}

    auto request_stop() -> void { m_stop = true; }

    [[nodiscard]] auto stop_requested() const -> bool { return m_stop; }

    auto reset() -> void { m_stop = false; }

private:
    std::atomic<bool> m_stop;
};
}  // namespace spider::core

#endif
