#ifndef SPIDER_CLIENT_FUTURE_HPP
#define SPIDER_CLIENT_FUTURE_HPP

#include <memory>

namespace spider {
class FutureImpl;

/**
 * Future represents a value that will be ready.
 *
 * @tparam T type of the value represented by Future.
 */
template <class T>
class Future {
public:
    /**
     * Gets the value of the future. Blocks until the value is available.
     * @return value of the future
     */
    auto get() -> T;

    /**
     * Checks if value of the future is ready.
     * @return true if future is ready, false otherwise
     */
    auto ready() -> bool;

private:
    std::unique_ptr<FutureImpl> m_impl;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_FUTURE_HPP
