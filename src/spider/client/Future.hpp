#ifndef SPIDER_CLIENT_FUTURE_HPP
#define SPIDER_CLIENT_FUTURE_HPP

#include <memory>

namespace spider {

class FutureImpl;

template <class T>
class Future {
private:
    std::unique_ptr<FutureImpl> m_impl;

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
};

}  // namespace spider

#endif  // SPIDER_CLIENT_FUTURE_HPP
