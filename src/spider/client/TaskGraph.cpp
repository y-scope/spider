#include "TaskGraph.hpp"

#include "Job.hpp"

namespace spider {

class TaskGraphImpl {};

template <class R, class... Args>
auto TaskGraph<R, Args...>::run(Args&&... /*args*/) -> Job<R> {
    return Job<R>();
}

}  // namespace spider
