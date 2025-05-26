#ifndef SPIDER_TESTS_COREDATAUTILS_HPP
#define SPIDER_TESTS_COREDATAUTILS_HPP
#include <spider/core/Data.hpp>

namespace spider::test {
inline auto data_equal(core::Data const& d1, core::Data const& d2) -> bool {
    if (d1.get_id() != d2.get_id()) {
        return false;
    }

    if (d1.get_locality() != d2.get_locality()) {
        return false;
    }

    if (d1.is_hard_locality() != d2.is_hard_locality()) {
        return false;
    }

    if (d1.get_value() != d2.get_value()) {
        return false;
    }

    if (d1.is_persisted() != d2.is_persisted()) {
        return false;
    }

    return true;
}
}  // namespace spider::test

#endif  // SPIDER_TESTS_COREDATAUTILS_HPP
