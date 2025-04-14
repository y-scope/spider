#ifndef SPIDER_UTILS_STOPTOKEN_HPP
#define SPIDER_UTILS_STOPTOKEN_HPP

#include <atomic>

namespace spider::core {
class StopToken {
public:
    /*
     * Request to token owners to stop.
     */
    static auto request_stop() -> void;

    /*
     * @return A boolean indicating whether the stop was requested.
     */
    [[nodiscard]] static auto is_stop_requested() -> bool;

    /*
     * Reset the stop token.
     */
    static auto reset() -> void;

    // Delete copy constructor and assignment operator
    StopToken(StopToken const&) = delete;
    auto operator=(StopToken const&) -> StopToken& = delete;
    // Delete move constructor and assignment operator
    StopToken(StopToken&&) = delete;
    auto operator=(StopToken&&) -> StopToken& = delete;
    // Default destructor
    ~StopToken() = default;

private:
    // Private constructor for singleton class
    StopToken() = default;

    static std::atomic_flag m_stop;
};
}  // namespace spider::core

#endif
