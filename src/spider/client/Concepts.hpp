#ifndef SPIDER_CLIENT_CONCEPTS_HPP
#define SPIDER_CLIENT_CONCEPTS_HPP

#include "../core/Serializer.hpp"
#include "Data.hpp"

namespace spider {

namespace {
template <class , template <class...> class>
struct IsSpecialization: public std::false_type {};

template <template <class...> class u, class... Ts>
struct IsSpecialization<u<Ts...>, u>: public std::true_type {};

template <class t, template <class, class...> class u>
inline constexpr bool cIsSpecializationV = IsSpecialization<t, u>::value;

}

template <class T>
concept TaskArgument = Serializable<T> || cIsSpecializationV<T, Data>;

}

#endif // SPIDER_CLIENT_CONCEPTS_HPP
