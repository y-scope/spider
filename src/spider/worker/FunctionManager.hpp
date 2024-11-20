#ifndef SPIDER_WORKER_FUNCTIONMANAGER_HPP
#define SPIDER_WORKER_FUNCTIONMANAGER_HPP

#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>

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

#include "../core/MsgPack.hpp"  // IWYU pragma: keep

#define REGISTER_TASK(func) \
    spider::core::FunctionManager::get_instance().register_function(#func, func);

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

inline auto buffer_get_error(msgpack::sbuffer const& buffer
) -> std::optional<std::tuple<FunctionInvokeError, std::string>> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::MAP != object.type || 2 != object.via.map.size) {
            return std::nullopt;
        }

        std::optional<FunctionInvokeError> err;
        std::optional<std::string> message;
        for (size_t i = 0; i < object.via.map.size; ++i) {
            msgpack::object_kv const& kv = object.via.map.ptr[i];
            std::string const key = kv.key.as<std::string>();
            if ("err" == key) {
                err = kv.val.as<FunctionInvokeError>();
            } else if ("msg" == key) {
                message = kv.val.as<std::string>();
            }
        }
        if (!err || !message) {
            return std::nullopt;
        }

        return std::make_tuple(*err, *message);
    } catch (msgpack::type_error& e) {
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

template <class T>
auto buffer_get(msgpack::sbuffer const& buffer) -> std::optional<T> {
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object object = handle.get();
        T t;
        object.convert(t);
        return t;
    } catch (msgpack::type_error& e) {
        return std::nullopt;
    }
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
                return generate_error(
                        FunctionInvokeError::ArgumentParsingError,
                        fmt::format("Cannot parse arguments.")
                );
            }

            if (std::tuple_size_v<ArgsTuple> != object.via.array.size) {
                return generate_error(
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
            return generate_error(
                    FunctionInvokeError::ArgumentParsingError,
                    fmt::format("Cannot parse arguments.")
            );
        }

        try {
            ReturnType result = std::apply(function, args_tuple);
            msgpack::sbuffer result_buffer;
            msgpack::pack(result_buffer, result);
            return result_buffer;
        } catch (msgpack::type_error& e) {
            return generate_error(
                    FunctionInvokeError::ResultParsingError,
                    fmt::format("Cannot parse result.")
            );
        } catch (std::exception& e) {
            return generate_error(
                    FunctionInvokeError::FunctionExecutionError,
                    "Function execution error"
            );
        }
        // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }

private:
    static auto
    generate_error(FunctionInvokeError const err, std::string const& message) -> msgpack::sbuffer {
        msgpack::sbuffer buffer;
        msgpack::packer packer{buffer};
        packer.pack_map(2);
        packer.pack("err");
        packer.pack(err);
        packer.pack("msg");
        packer.pack(message);
        return buffer;
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

    auto get_function(std::string const& name) -> Function* {
        if (auto const func_iter = m_map.find(name); func_iter != m_map.end()) {
            return &func_iter->second;
        }
        return nullptr;
    }

private:
    FunctionManager() = default;

    ~FunctionManager() = default;

    FunctionMap m_map;
};
}  // namespace spider::core

#endif  // SPIDER_WORKER_FUNCTIONMANAGER_HPP
