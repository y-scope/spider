#include "Data.hpp"

namespace spider {

class DataImpl {};

template <class T>
auto Data<T>::get() -> T {
    return T();
}

template <class T>
void Data<T>::set_locality(std::vector<std::string> const& /*nodes*/, bool /*hard*/) {}

template <class T>
auto Data<T>::Builder::key(std::string const& /*key*/) -> Data<T>::Builder& {
    return this;
}

template <class T>
auto Data<T>::Builder::locality(std::vector<std::string> const& /*nodes*/, bool /*hard*/)
        -> Data<T>::Builder& {
    return this;
}

template <class T>
auto Data<T>::Builder::cleanup(std::function<T const&()> const& /*f*/) -> Data<T>::Builder& {
    return this;
}

template <class T>
auto Data<T>::Builder::build(T&& /*t*/) -> Data<T> {
    return Data<T>();
}

}  // namespace spider
