#include "Future.hpp"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string>

namespace spider {

class FutureImpl {
    // Implementation details subject to change
private:
    boost::uuids::uuid m_id;

public:
    auto value() -> std::string { return boost::uuids::to_string(m_id); }

    auto ready() -> bool { return m_id.is_nil(); }
};

template <class T>
auto Future<T>::get() -> T {
    return T();
}

template <class T>
auto Future<T>::ready() -> bool {
    return true;
}

}  // namespace spider
