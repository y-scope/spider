#ifndef SPIDER_CLIENT_CONCEPTS_HPP
#define SPIDER_CLIENT_CONCEPTS_HPP

#include <type_traits>

#include "../core/Serializer.hpp"
#include "Data.hpp"

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
 * @tparam TemplateType
 */
template <typename Type, template <typename...> class TemplateType>
struct IsSpecialization : public std::false_type {};

// Specialization of `IsSpecialization` that inherits from `std::true_type` only when the first
// template argument is a specialization (i.e., the same type with template parameters) of the
// second template argument.
template <template <typename...> class Type, class... TypeParams>
struct IsSpecialization<Type<TypeParams...>, Type> : public std::true_type {};

template <class Type, template <typename...> class TemplateType>
inline constexpr bool cIsSpecializationV = IsSpecialization<Type, TemplateType>::value;

/**
 * Concept that represents the input to or output from a Task.
 *
 * @tparam T
 */
template <class T>
concept TaskIo = Serializable<T> || cIsSpecializationV<T, Data>;
}  // namespace spider

#endif  // SPIDER_CLIENT_CONCEPTS_HPP
