#ifndef SPIDER_CLIENT_TASK_HPP
#define SPIDER_CLIENT_TASK_HPP

#include <memory>

#include "Future.hpp"

namespace spider {

class TaskGraphImpl;

template <class R, class... Args>
class TaskGraph {
private:
    std::unique_ptr<TaskGraphImpl> m_impl;

public:
    auto run(Args&&... args) -> Future<R>;
};

}  // namespace spider

#endif  // SPIDER_CLIENT_TASK_HPP
