#ifndef SPIDER_SCHEDULER_SCHEDULERMESSAGE_HPP
#define SPIDER_SCHEDULER_SCHEDULERMESSAGE_HPP

#include <string>
#include <utility>

#include <boost/uuid/uuid.hpp>

#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../io/Serializer.hpp" // IWYU pragma: keep

namespace spider::scheduler {

class TaskCompleteRequest {
public:
    TaskCompleteRequest(
            boost::uuids::uuid const task_id,
            boost::uuids::uuid const task_instance_id,
            boost::uuids::uuid const worker_id,
            std::string addr
    )
            : m_task_id{task_id},
              m_task_instance_id{task_instance_id},
              m_worker_id{worker_id},
              m_worker_addr{std::move(addr)} {}

    [[nodiscard]] auto get_task_id() const -> boost::uuids::uuid { return m_task_id; }

    [[nodiscard]] auto get_task_instance_id() const -> boost::uuids::uuid {
        return m_task_instance_id;
    }

    [[nodiscard]] auto get_worker_id() const -> boost::uuids::uuid { return m_worker_id; }

    [[nodiscard]] auto get_worker_addr() const -> std::string const& { return m_worker_addr; }

    MSGPACK_DEFINE_ARRAY(m_task_id, m_task_instance_id, m_worker_id, m_worker_addr);

private:
    boost::uuids::uuid m_task_id;
    boost::uuids::uuid m_task_instance_id;
    boost::uuids::uuid m_worker_id;
    std::string m_worker_addr;
};

class ScheduleTaskRequest {
public:
    ScheduleTaskRequest(boost::uuids::uuid const id, std::string addr)
            : m_worker_id{id},
              m_worker_addr{std::move(addr)} {}

    [[nodiscard]] auto get_worker_id() const -> boost::uuids::uuid { return m_worker_id; }

    [[nodiscard]] auto get_worker_addr() const -> std::string const& { return m_worker_addr; }

    MSGPACK_DEFINE_ARRAY(m_worker_id, m_worker_addr);

private:
    boost::uuids::uuid m_worker_id;
    std::string m_worker_addr;
};

class ScheduleTaskResponse {
public:
    explicit ScheduleTaskResponse(boost::uuids::uuid const task_id) : m_task_id{task_id} {}

    [[nodiscard]] auto get_task_id() const -> boost::uuids::uuid { return m_task_id; }

    MSGPACK_DEFINE_ARRAY(m_task_id);

private:
    boost::uuids::uuid m_task_id;
};

}  // namespace spider::scheduler

#endif  // SPIDER_SCHEDULER_SCHEDULERMESSAGE_HPP
