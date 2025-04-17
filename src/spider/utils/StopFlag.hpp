#ifndef SPIDER_UTILS_STOPTOKEN_HPP
#define SPIDER_UTILS_STOPTOKEN_HPP

#include <atomic>

namespace spider::core {
/**
 * @brief A singleton class that provides a stop flag for threads and signal handlers.
 *
 * User can call request_stop() to set the stop flag, and check if the stop flag is set.
 * This class is thread-safe and signal-safe.
 */
class StopFlag {
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
    StopFlag(StopFlag const&) = delete;
    auto operator=(StopFlag const&) -> StopFlag& = delete;
    // Delete move constructor and assignment operator
    StopFlag(StopFlag&&) = delete;
    auto operator=(StopFlag&&) -> StopFlag& = delete;
    // Default destructor
    ~StopFlag() = default;

private:
    // Private constructor for singleton class
    StopFlag() = default;

    static std::atomic_flag m_stop;
};
}  // namespace spider::core

#endif
