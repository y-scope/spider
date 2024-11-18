#ifndef SPIDER_WORKER_FUNCTIONMANAGER_HPP
#define SPIDER_WORKER_FUNCTIONMANAGER_HPP

#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>

#include <cstddef>
#include <functional>
#include <initializer_list>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "../core/MsgPack.hpp"  // IWYU pragma: keep

#define REGISTER_TASK(func) \
    spider::core::FunctionManager::get_instance().register_function(#func, func);

namespace spider::core {
using ArgsBuffers = std::vector<msgpack::sbuffer>;

using Function = std::function<msgpack::sbuffer(ArgsBuffers const&)>;

using FunctionMap = absl::flat_hash_map<std::string, Function>;

template <class Sig>
struct signature;

template <class R, class... Args>
struct signature<R(Args...)> {
    using args_t = std::tuple<std::remove_const_t<std::remove_reference_t<Args>>...>;
    using ret_t = R;
};

template <class R, class... Args>
struct signature<R (*)(Args...)> {
    using args_t = std::tuple<std::remove_const_t<std::remove_reference_t<Args>>...>;
    using ret_t = R;
};

template <class, class = void>
struct IsDataT : std::false_type {};

template <class T>
struct IsDataT<T, std::void_t<decltype(std::declval<T>().is_data())>> : std::true_type {};

template <class T>
constexpr auto cIsDataV = IsDataT<T>::value;

enum class FunctionInvokeError : std::uint8_t {
    Success = 0,
    WrongNumberOfArguments = 1,
    ArgumentParsingError = 2,
    ResultParsingError = 3,
};

auto buffer_get_error(msgpack::sbuffer const& buffer
) -> std::optional<std::tuple<FunctionInvokeError, std::string>> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access)
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::MAP != object.type || 2 != object.via.map.size) {
            return std::nullopt;
        }

        if ("err" != object.via.map.ptr[0].key) {
            return std::nullopt;
        }
        FunctionInvokeError const err{object.via.map.ptr[0].val.as<std::uint8_t>()};

        if ("msg" != object.via.map.ptr[1].key) {
            return std::nullopt;
        }
        std::string const message{object.via.map.ptr[1].val.as<std::string>()};

        return std::make_tuple(err, message);
    } catch (msgpack::type_error& e) {
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access)
}

template <class T>
auto buffer_get(msgpack::sbuffer const& buffer) -> std::optional<T> {
    msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
    msgpack::object object = handle.get();
    T t;
    object.convert(t);
    return t;
}

// NOLINTBEGIN(cppcoreguidelines-missing-std-forward)
template <class... Args>
auto create_args_buffers(Args&&... args) -> ArgsBuffers {
    ArgsBuffers args_buffers{};
    (
            [&] {
                args_buffers.emplace_back();
                msgpack::sbuffer& arg = args_buffers[args_buffers.size() - 1];
                msgpack::pack(arg, args);
            }(),
            ...
    );
    return args_buffers;
}

// NOLINTEND(cppcoreguidelines-missing-std-forward)

template <class F>
class FunctionInvoker {
public:
    static auto apply(F const& function, ArgsBuffers const& args_buffers) -> msgpack::sbuffer {
        using ArgsTuple = signature<F>::args_t;
        using ReturnType = signature<F>::ret_t;
        if (std::tuple_size_v<ArgsTuple> != args_buffers.size()) {
            return generate_error(
                    FunctionInvokeError::WrongNumberOfArguments,
                    fmt::format(
                            "Wrong number of arguments. Expect {}. Get {}.",
                            std::tuple_size_v<ArgsTuple>,
                            args_buffers.size()
                    )
            );
        }

        ArgsTuple args_tuple{};
        bool success = get_args_tuple(
                args_tuple,
                args_buffers,
                std::make_index_sequence<std::tuple_size_v<ArgsTuple>>{}
        );
        if (!success) {
            return generate_error(
                    FunctionInvokeError::ArgumentParsingError,
                    fmt::format("Cannot parse arguments.")
            );
        }

        ReturnType result = std::apply(function, args_tuple);
        try {
            msgpack::sbuffer result_buffer;
            msgpack::pack(result_buffer, result);
            return result_buffer;
        } catch (msgpack::type_error& e) {
            return generate_error(
                    FunctionInvokeError::ResultParsingError,
                    fmt::format("Cannot parse result.")
            );
        }
    }

private:
    static auto
    generate_error(FunctionInvokeError err, std::string const& message) -> msgpack::sbuffer {
        msgpack::sbuffer buffer;
        msgpack::packer packer{buffer};
        packer.pack_map(2);
        packer.pack("err");
        packer.pack(err);
        packer.pack("msg");
        packer.pack(message);
        return buffer;
    }

    template <class T>
    static auto parse_arg(msgpack::sbuffer const& arg_buffer, bool& success) -> T {
        try {
            if constexpr (cIsDataV<T>) {
                msgpack::object_handle const handle
                        = msgpack::unpack(arg_buffer.data(), arg_buffer.size());
                msgpack::object object = handle.get();
                T t;
                object.convert(t);
                return t;
            }
            msgpack::object_handle const handle
                    = msgpack::unpack(arg_buffer.data(), arg_buffer.size());
            msgpack::object object = handle.get();
            T t;
            object.convert(t);
            return t;
        } catch (msgpack::type_error& e) {
            success = false;
            return T{};
        }
    }

    static auto
    get_args_tuple(std::tuple<>& /*tuple*/, ArgsBuffers const& /*args_buffer*/, std::index_sequence<>)
            -> bool {
        return true;
    }

    template <size_t... i, typename... Args>
    static auto
    get_args_tuple(std::tuple<Args...>& tuple, ArgsBuffers const& args_buffer, std::index_sequence<i...>)
            -> bool {
        bool success = true;
        (void)std::initializer_list<int>{
                (std::get<i>(tuple) = parse_arg<Args>(args_buffer.at(i), success), 0)...
        };
        return success;
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
