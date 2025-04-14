#ifndef SPIDER_UTILS_STOPTOKEN_HPP
#define SPIDER_UTILS_STOPTOKEN_HPP

#include <atomic>

namespace spider::core {
class StopToken {
public:
    /*
     * @return A reference to the singleton instance of StopToken.
     */
    static auto get_instance() -> StopToken&;

    /*
     * Request to token owners to stop.
     */
    auto request_stop() -> void;

    /*
     * @return A boolean indicating whether the stop was requested.
     */
    [[nodiscard]] auto stop_requested() const -> bool;

    /*
     * Reset the stop token.
     */
    auto reset() -> void;

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

    std::atomic_flag m_stop;
};
}  // namespace spider::core

#endif
