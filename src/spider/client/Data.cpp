#include "Data.hpp"

#include <functional>
#include <string>
#include <vector>

#include "../core/Data.hpp"
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/Serializer.hpp"
#include "../storage/DataStorage.hpp"
#include "Exception.hpp"

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
    m_data_store->set_data_locality(*m_impl);
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
    auto data = std::make_unique<core::Data>(t);
    data->set_locality(m_nodes);
    data->set_hard_locality(m_hard_locality);
    data->set_cleanup_func(m_cleanup_func);
    core::StorageErr err;
    switch (m_data_source) {
        case DataSource::Driver:
            err = m_data_store->add_driver_data(m_source_id, *data);
            if (!err.success()) {
                throw ConnectionException(err.description);
            }
            break;
        case DataSource::TaskContext:
            err = m_data_store->add_task_data(m_source_id, *data);
            if (!err.success()) {
                throw ConnectionException(err.description);
            }
            break;
    }
    return Data{data, m_data_store};
}

}  // namespace spider
