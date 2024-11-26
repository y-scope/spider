#include "Data.hpp"

#include <functional>
#include <string>
#include <vector>

#include "../core/Serializer.hpp"

namespace spider {

class DataImpl {};

template <Serializable T>
auto Data<T>::get() -> T {
    return T();
}

template <Serializable T>
void Data<T>::set_locality(std::vector<std::string> const& /*nodes*/, bool /*hard*/) {}

template <Serializable T>
auto Data<T>::Builder::set_locality(std::vector<std::string> const& /*nodes*/, bool /*hard*/)
        -> Data<T>::Builder& {
    return this;
}

template <Serializable T>
auto Data<T>::Builder::set_cleanup_func(std::function<T const&()> const& /*f*/)
        -> Data<T>::Builder& {
    return this;
}

template <Serializable T>
auto Data<T>::Builder::build(T const& /*t*/) -> Data<T> {
    return Data<T>();
}

}  // namespace spider
