#include "Data.hpp"

#include <functional>
#include <string>
#include <vector>

namespace spider {

class DataImpl {};

template <class T>
auto Data<T>::get() -> T {
    return T();
}

template <class T>
void Data<T>::set_locality(std::vector<std::string> const& /*nodes*/, bool /*hard*/) {}

template <class T>
auto Data<T>::Builder::set_key(std::string const& /*key*/) -> Data<T>::Builder& {
    return this;
}

template <class T>
auto Data<T>::Builder::set_locality(std::vector<std::string> const& /*nodes*/, bool /*hard*/)
        -> Data<T>::Builder& {
    return this;
}

template <class T>
auto Data<T>::Builder::set_cleanup(std::function<T const&()> const& /*f*/) -> Data<T>::Builder& {
    return this;
}

template <class T>
auto Data<T>::Builder::build(T const& /*t*/) -> Data<T> {
    return Data<T>();
}

}  // namespace spider
