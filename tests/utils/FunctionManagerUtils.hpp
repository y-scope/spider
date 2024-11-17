#ifndef SPIDER_TEST_FUNCTIONMANAGER_HPP
#define SPIDER_TEST_FUNCTIONMANAGER_HPP

#include "../../src/spider/core/MsgPack.hpp"  // IWYU pragma: keep
#include "../../src/spider/worker/FunctionManager.hpp"

namespace spider::test {
// NOLINTBEGIN(cppcoreguidelines-missing-std-forward)
template <class... Args>
auto create_args_buffers(Args&&... args) -> core::ArgsBuffers {
    core::ArgsBuffers args_buffers{};
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

    template<class T>
    auto get_result(msgpack::sbuffer const &buffer) -> T {
        msgpack::object_handle const handle = msgpack::unpack(buffer.data(), buffer.size());
        msgpack::object object = handle.get();
        T t;
        object.convert(t);
        return t;
    }
} // namespace spider::test

#endif  // SPIDER_TEST_FUNCTIONMANAGER_HPP
