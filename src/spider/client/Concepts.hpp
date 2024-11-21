#ifndef SPIDER_CLIENT_CONCEPTS_HPP
#define SPIDER_CLIENT_CONCEPTS_HPP

#include <type_traits>

#include "../core/Serializer.hpp"
#include "Data.hpp"

namespace spider {

template <class, template <class...> class>
struct IsSpecialization : public std::false_type {};

template <template <class...> class u, class... Ts>
struct IsSpecialization<u<Ts...>, u> : public std::true_type {};

template <class T, template <class, class...> class u>
inline constexpr bool cIsSpecializationV = IsSpecialization<T, u>::value;

template <class T>
concept TaskArgument = Serializable<T> || cIsSpecializationV<T, Data>;

}  // namespace spider

#endif  // SPIDER_CLIENT_CONCEPTS_HPP
