#ifndef SPIDER_CLIENT_TYPE_UTILS_HPP
#define SPIDER_CLIENT_TYPE_UTILS_HPP

#include <cstddef>
#include <initializer_list>
#include <type_traits>
#include <utility>

namespace spider {
// The template and partial specialization below check whether a given type is a specialization of
// a given type.
/**
 * Template to check if a given type is specialization of a given template type.
 *
 * NOTE: This inherits from `std::false_type` so that by default, `Type` is not considered
 * a specialization of `TemplateType`. The partial specialization of `IsSpecialization` below
 * defines the case where `Type` is considered a specialization of `TemplateType`.
 *
 * @tparam Type
 * @tparam template_type
 */
template <typename Type, template <typename...> class template_type>
struct IsSpecialization : public std::false_type {};

// Specialization of `IsSpecialization` that inherits from `std::true_type` only when the first
// template argument is a specialization (i.e., the same type with template parameters) of the
// second template argument.
template <template <typename...> class type, class... TypeParams>
struct IsSpecialization<type<TypeParams...>, type> : public std::true_type {};

template <class Type, template <typename...> class template_type>
inline constexpr bool cIsSpecializationV = IsSpecialization<Type, template_type>::value;

template <std::size_t n>
struct Num {
    static constexpr auto cValue = n;
};

template <class F, std::size_t... is>
void for_n(F func, std::index_sequence<is...>) {
    (void)std::initializer_list{0, ((void)func(Num<is>{}), 0)...};
}

template <std::size_t n, typename F>
void for_n(F func) {
    for_n(func, std::make_index_sequence<n>());
}

template <class T>
struct ExtractTemplateParam {
    using Type = T;
};

template <template <class> class t, class P>
struct ExtractTemplateParam<t<P>> {
    using Type = P;
};

template <class T>
using ExtractTemplateParamT = typename ExtractTemplateParam<T>::Type;
}  // namespace spider
#endif  // SPIDER_CLIENT_TYPE_UTILS_HPP
