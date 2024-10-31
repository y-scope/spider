#ifndef SPIDER_CLIENT_TASK_HPP
#define SPIDER_CLIENT_TASK_HPP

#include <memory>

namespace spider {

class TaskGraphImpl;

template <class R, class... Args>
class TaskGraph {
private:
    std::unique_ptr<TaskGraphImpl> m_impl;

public:
};

}  // namespace spider

#endif  // SPIDER_CLIENT_TASK_HPP
