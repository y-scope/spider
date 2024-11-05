#include "../../src/spider/core/Data.hpp"

namespace spider::core {

inline auto data_equal(Data const& d1, Data const& d2) -> bool {
    if (d1.get_id() != d2.get_id()) {
        return false;
    }

    if (d1.get_key() != d2.get_key()) {
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

    return true;
}

}
