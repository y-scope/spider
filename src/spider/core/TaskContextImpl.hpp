#ifndef SPIDER_CORE_TASKCONTEXTIMPL_HPP
#define SPIDER_CORE_TASKCONTEXTIMPL_HPP

#include <memory>

#include <boost/uuid/uuid.hpp>

#include "../client/TaskContext.hpp"
#include "../storage/DataStorage.hpp"
#include "../storage/MetadataStorage.hpp"

namespace spider::core {
class TaskContextImpl {
public:
    static auto create_task_context(
            boost::uuids::uuid const& task_id,
            std::shared_ptr<DataStorage> const& data_storage,
            std::shared_ptr<MetadataStorage> const& metadata_storage
    ) -> TaskContext {
        return TaskContext{task_id, data_storage, metadata_storage};
    }

    static auto get_data_store(TaskContext const& task_context) -> std::shared_ptr<DataStorage> {
        return task_context.m_data_store;
    }

    static auto get_metadata_store(TaskContext const& task_context
    ) -> std::shared_ptr<MetadataStorage> {
        return task_context.m_metadata_store;
    }
};

}  // namespace spider::core

#endif
