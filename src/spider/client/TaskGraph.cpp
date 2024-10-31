#include "TaskGraph.hpp"

#include "Future.hpp"

namespace spider {

class TaskGraphImpl {};

template <class R, class... Args>
auto TaskGraph<R, Args...>::run(Args&&... /*args*/) -> Future<R> {
    return Future<R>();
}

}  // namespace spider
