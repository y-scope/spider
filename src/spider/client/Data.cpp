#include "Data.hpp"

#include <functional>
#include <string>
#include <vector>

#include "../core/Data.hpp"
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/Serializer.hpp"

namespace spider {

template <Serializable T>
auto Data<T>::get() -> T {
    std::string const& value = m_impl->get_value();
    return msgpack::unpack(value.data(), value.size()).get().as<T>();
}

template <Serializable T>
auto Data<T>::set_locality(std::vector<std::string> const& nodes, bool hard) -> void {
    m_impl->set_locality(nodes);
    m_impl->set_hard_locality(hard);
    // TODO: update data storage
}

template <Serializable T>
auto Data<T>::Builder::set_locality(std::vector<std::string> const& nodes, bool hard) -> Builder& {
    m_nodes = nodes;
    m_hard_locality = hard;
    return *this;
}

template <Serializable T>
auto Data<T>::Builder::set_cleanup_func(std::function<void(T const&)> const& f) -> Builder& {
    m_cleanup_func = f;
    return *this;
}

template <Serializable T>
auto Data<T>::Builder::build(T const& t) -> Data {
    auto impl = std::make_shared<core::Data>(t);
    impl->set_locality(m_nodes);
    impl->set_hard_locality(m_hard_locality);
    impl->set_cleanup_func(m_cleanup_func);
    // TODO: update data storage
    return Data(impl);
}

}  // namespace spider
