#ifndef SPIDER_WORKER_FUNCTIONMANAGER_HPP
#define SPIDER_WORKER_FUNCTIONMANAGER_HPP

#include <absl/container/flat_hash_map.h>

#include <functional>
#include <msgpack.hpp>
#include <optional>
#include <type_traits>
#include <vector>

#include "../core/Data.hpp"

namespace spider::worker {

using ArgsBuffers = std::vector<msgpack::sbuffer>;

using Function = std::function<msgpack::sbuffer(ArgsBuffers)>;

using FunctionMap = absl::flat_hash_map<std::string, Function>;

template <class Sig>
struct signature;

template <class R, class... Args>
struct signature<R(Args...)> {
    using args_t = std::tuple<std::remove_const_t<std::remove_reference_t<Args>>...>;
    using ret_t = R;
};

template <class, class = void>
struct is_data_t : std::false_type {};

template <class T>
struct is_data_t<T, std::void_t<decltype(std::declval<T>().is_data())>> : std::true_type {};

template <class T>
constexpr auto is_data_v = is_data_t<T>::value;

template <class F>
requires std::is_function_v<F>
class FunctionInvoker {
public:
    static inline auto
    apply(F const& function, ArgsBuffers const& args_buffers) -> std::optional<msgpack::sbuffer> {
        using ArgsTuple = signature<F>::args_t;
        using ReturnType = signature<F>::ret_t;
        if (std::tuple_size_v<ArgsTuple> != args_buffers.size()) {
            return std::nullopt;
        }

        ArgsTuple args_tuple{};
        bool success = get_args_tuple(
                args_tuple,
                args_buffers,
                std::make_index_sequence<std::tuple_size_v<ArgsTuple>>{}
        );
        if (!success) {
            return std::nullopt;
        }

        ReturnType result = std::apply(function, args_tuple);
        msgpack::sbuffer result_buffer;
        try {
            msgpack::pack(result_buffer, result);
            return result_buffer;
        } catch (msgpack::type_error& e) {
            return std::nullopt;
        }
    }

private:
    template <class T>
    static inline auto parse_arg(msgpack::sbuffer const& arg_buffer, bool& success) -> T {
        try {
            if constexpr (is_data_v<T>) {
                msgpack::object_handle const handle
                        = msgpack::unpack(arg_buffer.data(), arg_buffer.size());
                msgpack::object object = handle.get();
                T t("");
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

    static inline bool
    get_args_tuple(std::tuple<>& tuple, ArgsBuffers const& args_buffer, std::index_sequence<>) {
        return true;
    }

    template <size_t... I, typename... Args>
    static inline bool
    get_args_tuple(std::tuple<Args...>& tuple, ArgsBuffers const& args_buffer, std::index_sequence<I...>) {
        bool success = true;
        (void)std::initializer_list<int>{
                (std::get<I>(tuple) = parse_arg<Args>(args_buffer.at(I), success), 0)...
        };
        return success;
    }
};

class FunctionManager {
public:
    static auto get_instance() -> FunctionManager& {
        static FunctionManager instance;
        return instance;
    }

    auto add_function(std::string const& name, Function const& func) { m_map.emplace(name, func); }

    auto get_function(std::string const& name) -> Function* {
        if (auto func_iter = m_map.find(name); func_iter != m_map.end()) {
            return &func_iter->second;
        }
        return nullptr;
    }

private:
    FunctionManager() = default;
    ~FunctionManager() = default;
    FunctionManager(FunctionManager const&) = delete;
    FunctionManager(FunctionManager&&) = delete;

    FunctionMap m_map;
};

}  // namespace spider::worker

#endif  // SPIDER_WORKER_FUNCTIONMANAGER_HPP
