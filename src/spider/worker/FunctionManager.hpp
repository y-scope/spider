#ifndef SPIDER_WORKER_FUNCTIONMANAGER_HPP
#define SPIDER_WORKER_FUNCTIONMANAGER_HPP

#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/uuid/uuid.hpp>
#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "../client/Data.hpp"
#include "../client/task.hpp"
#include "../client/TaskContext.hpp"
#include "../core/DataImpl.hpp"
#include "../core/Error.hpp"
#include "../core/TaskContextImpl.hpp"
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/Serializer.hpp"
#include "../storage/DataStorage.hpp"
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

using Function = std::function<ResultBuffer(TaskContext context, ArgsBuffer const&)>;

using FunctionMap = absl::flat_hash_map<std::string, Function>;

template <class T>
struct TemplateParameter;

template <template <class...> class t, class Param>
struct TemplateParameter<t<Param>> {
    using Type = Param;
};

template <class T>
using TemplateParameterT = typename TemplateParameter<T>::Type;

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

template <Serializable T>
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

        return std::make_optional(object.via.array.ptr[1].as<T>());
    } catch (msgpack::type_error& e) {
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

template <Serializable... Ts>
requires(sizeof...(Ts) > 1)
auto response_get_result(msgpack::sbuffer const& buffer) -> std::optional<std::tuple<Ts...>> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::ARRAY != object.type || sizeof...(Ts) + 1 != object.via.array.size) {
            return std::nullopt;
        }

        if (worker::TaskExecutorResponseType::Result
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            return std::nullopt;
        }

        std::tuple<Ts...> result;
        for_n<sizeof...(Ts)>([&](auto i) {
            object.via.array.ptr[i.cValue + 1].convert(std::get<i.cValue>(result));
        });
        return std::make_optional(result);
    } catch (msgpack::type_error& e) {
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

inline auto response_get_result_buffers(msgpack::sbuffer const& buffer
) -> std::optional<std::vector<msgpack::sbuffer>> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
    try {
        std::vector<msgpack::sbuffer> result_buffers;
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object const object = handle.get();

        if (msgpack::type::ARRAY != object.type || object.via.array.size < 2) {
            spdlog::error("Cannot split result into buffers: Wrong type");
            return std::nullopt;
        }

        if (worker::TaskExecutorResponseType::Result
            != object.via.array.ptr[0].as<worker::TaskExecutorResponseType>())
        {
            spdlog::error(
                    "Cannot split result into buffers: Wrong response type {}",
                    static_cast<std::underlying_type_t<worker::TaskExecutorResponseType>>(
                            object.via.array.ptr[0].as<worker::TaskExecutorResponseType>()
                    )
            );
            return std::nullopt;
        }

        for (size_t i = 1; i < object.via.array.size; ++i) {
            msgpack::object const& obj = object.via.array.ptr[i];
            result_buffers.emplace_back();
            msgpack::pack(result_buffers.back(), obj);
        }
        return result_buffers;
    } catch (msgpack::type_error& e) {
        spdlog::error("Cannot split result into buffers: {}", e.what());
        return std::nullopt;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

template <Serializable T>
auto create_result_response(T const& t) -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(2);
    packer.pack(worker::TaskExecutorResponseType::Result);
    packer.pack(t);
    return buffer;
}

template <Serializable... Values>
auto create_result_response(std::tuple<Values...> const& t) -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(sizeof...(Values) + 1);
    packer.pack(worker::TaskExecutorResponseType::Result);
    (..., packer.pack(std::get<Values>(t)));
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

inline auto create_args_request(std::vector<msgpack::sbuffer> const& args_buffers
) -> msgpack::sbuffer {
    msgpack::sbuffer buffer;
    msgpack::packer packer{buffer};
    packer.pack_array(2);
    packer.pack(worker::TaskExecutorRequestType::Arguments);
    packer.pack_array(args_buffers.size());
    for (msgpack::sbuffer const& args_buffer : args_buffers) {
        buffer.write(args_buffer.data(), args_buffer.size());
    }
    return buffer;
}

// NOLINTEND(cppcoreguidelines-missing-std-forward)

template <class F>
class FunctionInvoker {
public:
    static auto
    apply(F const& function, TaskContext context, ArgsBuffer const& args_buffer) -> ResultBuffer {
        // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access,cppcoreguidelines-pro-bounds-pointer-arithmetic)
        using ArgsTuple = signature<F>::args_t;
        using ReturnType = signature<F>::ret_t;

        static_assert(TaskIo<ReturnType>, "Return type must be TaskIo");
        static_assert(
                std::is_same_v<TaskContext, std::tuple_element_t<0, ArgsTuple>>,
                "First argument must be TaskContext"
        );
        for_n<std::tuple_size_v<ArgsTuple> - 1>([&](auto i) {
            static_assert(
                    TaskIo<std::tuple_element_t<i.cValue + 1, ArgsTuple>>,
                    "Other arguments must be TaskIo"
            );
        });

        std::shared_ptr<DataStorage> data_store = TaskContextImpl::get_data_store(context);

        ArgsTuple args_tuple;
        try {
            msgpack::object_handle const handle
                    = msgpack::unpack(args_buffer.data(), args_buffer.size());
            msgpack::object const object = handle.get();

            if (msgpack::type::ARRAY != object.type && object.via.array.size < 1) {
                return create_error_response(
                        FunctionInvokeError::ArgumentParsingError,
                        fmt::format("Cannot parse arguments.")
                );
            }

            if (std::tuple_size_v<ArgsTuple> - 1 != object.via.array.size) {
                return create_error_response(
                        FunctionInvokeError::WrongNumberOfArguments,
                        fmt::format(
                                "Wrong number of arguments. Expect {}. Get {}.",
                                std::tuple_size_v<ArgsTuple>,
                                object.via.array.size
                        )
                );
            }

            // Fill args_tuple
            StorageErr err;
            std::get<0>(args_tuple) = context;
            for_n<std::tuple_size_v<ArgsTuple> - 1>([&](auto i) {
                if (!err.success()) {
                    return;
                }
                using T = std::tuple_element_t<i.cValue + 1, ArgsTuple>;
                msgpack::object arg = object.via.array.ptr[i.cValue];
                if constexpr (cIsSpecializationV<T, spider::Data>) {
                    boost::uuids::uuid const data_id = arg.as<boost::uuids::uuid>();
                    std::unique_ptr<Data> data = std::make_unique<Data>();
                    err = data_store->get_data(data_id, data.get());
                    if (!err.success()) {
                        return;
                    }

                    std::get<i.cValue + 1>(args_tuple
                    ) = DataImpl::create_data<TemplateParameterT<T>>(std::move(data), data_store);
                } else {
                    std::get<i.cValue + 1>(args_tuple)
                            = arg.as<std::tuple_element_t<i.cValue + 1, ArgsTuple>>();
                }
            });
            if (!err.success()) {
                return create_error_response(
                        FunctionInvokeError::ArgumentParsingError,
                        fmt::format("Cannot parse arguments: {}.", err.description)
                );
            }
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
                        std::bind(
                                &FunctionInvoker<F>::apply,
                                std::move(f),
                                std::placeholders::_1,
                                std::placeholders::_2
                        )
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
