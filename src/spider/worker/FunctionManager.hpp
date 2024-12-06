#ifndef SPIDER_WORKER_FUNCTIONMANAGER_HPP
#define SPIDER_WORKER_FUNCTIONMANAGER_HPP

#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <initializer_list>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>

#include "../utils/MsgPack.hpp"  // IWYU pragma: keep
#include "TaskExecutorMessage.hpp"

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define CONCAT_DIRECT(s1, s2) s1##s2
#define CONCAT(s1, s2) CONCAT_DIRECT(s1, s2)
#define ANONYMOUS_VARIABLE(str) CONCAT(str, __COUNTER__)
// NOLINTEND(cppcoreguidelines-macro-usage)

#define SPIDER_WORKER_REGISTER_TASK(func) \
    inline const auto ANONYMOUS_VARIABLE(var) \
            = spider::core::FunctionManager::get_instance().register_function(#func, func);

namespace spider::core {
using ArgsBuffer = msgpack::sbuffer;

using ResultBuffer = msgpack::sbuffer;

using Function = std::function<ResultBuffer(ArgsBuffer const&)>;

using FunctionMap = absl::flat_hash_map<std::string, Function>;

template <class Sig>
struct signature;

template <class R, class... Args>
struct signature<R(Args...)> {
    using args_t = std::tuple<std::decay_t<Args>...>;
    using ret_t = R;
};

template <class R, class... Args>
struct signature<R (*)(Args...)> {
    using args_t = std::tuple<std::decay_t<Args>...>;
    using ret_t = R;
};

template <class, class = void>
struct IsDataT : std::false_type {};

template <class T>
struct IsDataT<T, std::void_t<decltype(std::declval<T>().is_data())>> : std::true_type {};

template <class T>
constexpr auto cIsDataV = IsDataT<T>::value;

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

enum class FunctionInvokeError : std::uint8_t {
    Success = 0,
    WrongNumberOfArguments = 1,
    ArgumentParsingError = 2,
    ResultParsingError = 3,
    FunctionExecutionError = 4,
};
}  // namespace spider::core

// MSGPACK_ADD_ENUM must be called from global namespace
MSGPACK_ADD_ENUM(spider::core::FunctionInvokeError);

namespace spider::core {

auto response_get_error(msgpack::sbuffer const& buffer
) -> std::optional<std::tuple<FunctionInvokeError, std::string>>;

auto create_error_response(FunctionInvokeError error, std::string const& message)
        -> msgpack::sbuffer;

void create_error_buffer(
        FunctionInvokeError error,
        std::string const& message,
        msgpack::sbuffer& buffer
);

template <class T>
auto response_get_result(msgpack::sbuffer const& buffer) -> std::optional<T> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::ARRAY != object.type || 2 != object.via.array.size) {
            return std::nullopt;
        }

        if (worker::TaskExecutorResponseType::Result
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            return std::nullopt;
        }

        return object.via.array.ptr[1].as<T>();
    } catch (msgpack::type_error& e) {
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

template <class T>
auto create_result_response(T const& t) -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(2);
    packer.pack(worker::TaskExecutorResponseType::Result);
    packer.pack(t);
    return buffer;
}

// NOLINTBEGIN(cppcoreguidelines-missing-std-forward)
template <class... Args>
auto create_args_buffers(Args&&... args) -> ArgsBuffer {
    ArgsBuffer args_buffer;
    msgpack::packer packer(args_buffer);
    packer.pack_array(sizeof...(args));
    ([&] { packer.pack(args); }(), ...);
    return args_buffer;
}

template <class... Args>
auto create_args_request(Args&&... args) -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(2);
    packer.pack(worker::TaskExecutorRequestType::Arguments);
    packer.pack_array(sizeof...(args));
    ([&] { packer.pack(args); }(), ...);
    return buffer;
}

// NOLINTEND(cppcoreguidelines-missing-std-forward)

template <class F>
class FunctionInvoker {
public:
    static auto apply(F const& function, ArgsBuffer const& args_buffer) -> ResultBuffer {
        // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
        using ArgsTuple = signature<F>::args_t;
        using ReturnType = signature<F>::ret_t;

        ArgsTuple args_tuple{};
        try {
            msgpack::object_handle const handle
                    = msgpack::unpack(args_buffer.data(), args_buffer.size());
            msgpack::object const object = handle.get();

            if (msgpack::type::ARRAY != object.type) {
                return create_error_response(
                        FunctionInvokeError::ArgumentParsingError,
                        fmt::format("Cannot parse arguments.")
                );
            }

            if (std::tuple_size_v<ArgsTuple> != object.via.array.size) {
                return create_error_response(
                        FunctionInvokeError::WrongNumberOfArguments,
                        fmt::format(
                                "Wrong number of arguments. Expect {}. Get {}.",
                                std::tuple_size_v<ArgsTuple>,
                                object.via.array.size
                        )
                );
            }

            for_n<std::tuple_size_v<ArgsTuple>>([&](auto i) {
                msgpack::object arg = object.via.array.ptr[i.cValue];
                std::get<i.cValue>(args_tuple)
                        = arg.as<std::tuple_element_t<i.cValue, ArgsTuple>>();
            });
        } catch (msgpack::type_error& e) {
            return create_error_response(
                    FunctionInvokeError::ArgumentParsingError,
                    fmt::format("Cannot parse arguments.")
            );
        }

        try {
            ReturnType result = std::apply(function, args_tuple);
            return create_result_response(result);
        } catch (msgpack::type_error& e) {
            return create_error_response(
                    FunctionInvokeError::ResultParsingError,
                    fmt::format("Cannot parse result.")
            );
        } catch (std::exception& e) {
            return create_error_response(
                    FunctionInvokeError::FunctionExecutionError,
                    "Function execution error"
            );
        }
        // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }
};

class FunctionManager {
public:
    FunctionManager(FunctionManager const&) = delete;

    auto operator=(FunctionManager const&) -> FunctionManager& = delete;

    FunctionManager(FunctionManager&&) = delete;

    auto operator=(FunctionManager&&) -> FunctionManager& = delete;

    static auto get_instance() -> FunctionManager& {
        static FunctionManager instance;
        return instance;
    }

    template <class F>
    auto register_function(std::string const& name, F f) -> bool {
        return m_map
                .emplace(
                        name,
                        std::bind(&FunctionInvoker<F>::apply, std::move(f), std::placeholders::_1)
                )
                .second;
    }

    auto register_function_invoker(std::string const& name, Function f) -> bool {
        return m_map.emplace(name, f).second;
    }

    [[nodiscard]] auto get_function(std::string const& name) const -> Function const*;

    [[nodiscard]] auto get_function_map() const -> FunctionMap const& { return m_map; }

private:
    FunctionManager() = default;

    ~FunctionManager() = default;

    FunctionMap m_map;
};
}  // namespace spider::core

#endif  // SPIDER_WORKER_FUNCTIONMANAGER_HPP
