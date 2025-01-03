#ifndef SPIDER_CLIENT_JOB_HPP
#define SPIDER_CLIENT_JOB_HPP

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <boost/uuid/uuid.hpp>

#include "../core/DataImpl.hpp"
#include "../core/JobMetadata.hpp"
#include "../io/MsgPack.hpp"  // IWYU pragma: keep
#include "../storage/MetadataStorage.hpp"
#include "task.hpp"
#include "type_utils.hpp"

namespace spider {
namespace core {
class Data;
class DataStorage;
class MetadataStorage;
}  // namespace core
class Driver;

// TODO: Use std::expected or Boost's outcome so that the user can get the result of the job in one
// call rather than the current error-prone approach which requires that the user check the job's
// status and then call the relevant method.

enum class JobStatus : uint8_t {
    Running,
    Succeeded,
    Failed,
    Cancelled,
};

/**
 * A running task graph.
 *
 * @tparam ReturnType
 */
template <TaskIo ReturnType>
class Job {
public:
    /**
     * Waits for the job to complete.
     *
     * @throw spider::ConnectionException
     */
    auto wait_complete() -> void {
        bool complete = false;
        core::StorageErr err = m_metadata_storage->get_job_complete(m_id, &complete);
        if (!err.success()) {
            throw ConnectionException{
                    fmt::format("Failed to get job completion status: {}", err.description)
            };
        }
        while (!complete) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            err = m_metadata_storage->get_job_complete(m_id, &complete);
            if (!err.success()) {
                throw ConnectionException{
                        fmt::format("Failed to get job completion status: {}", err.description)
                };
            }
        }
    }

    /**
     * Cancels the job and waits for the job's tasks to be cancelled.
     *
     * @throw spider::ConnectionException
     */
    auto cancel();

    /**
     * @return Status of the job.
     * @throw spider::ConnectionException
     */
    auto get_status() -> JobStatus {
        core::JobStatus status;
        core::StorageErr const err = m_metadata_storage->get_job_status(m_id, &status);
        if (!err.success()) {
            throw ConnectionException{fmt::format("Failed to get job status: {}", err.description)};
        }
        switch (status) {
            case core::JobStatus::Running:
                return JobStatus::Running;
            case core::JobStatus::Succeeded:
                return JobStatus::Succeeded;
            case core::JobStatus::Failed:
                return JobStatus::Failed;
            case core::JobStatus::Cancelled:
                return JobStatus::Cancelled;
        }
        throw ConnectionException{
                fmt::format("Unknown job status: {}", static_cast<uint8_t>(status))
        };
    }

    /**
     * NOTE: It is undefined behavior to call this method for a job that is not in the `Succeeded`
     * state.
     *
     * @return Result of the job.
     * @throw spider::ConnectionException
     */
    auto get_result() -> ReturnType {
        std::vector<boost::uuids::uuid> output_task_ids;
        core::StorageErr err = m_metadata_storage->get_job_output_tasks(m_id, &output_task_ids);
        if (!err.success()) {
            throw ConnectionException{
                    fmt::format("Failed to get job output tasks: {}", err.description)
            };
        }
        std::vector<core::Task> tasks;
        for (auto const& id : output_task_ids) {
            core::Task task{""};
            err = m_metadata_storage->get_task(id, &task);
            if (!err.success()) {
                throw ConnectionException{fmt::format("Failed to get task: {}", err.description)};
            }
            tasks.push_back(task);
        }
        ReturnType result;
        if constexpr (cIsSpecializationV<ReturnType, std::tuple>) {
            size_t task_index = 0;
            size_t output_index = 0;
            for_n<std::tuple_size_v<ReturnType>>([&](auto i) {
                using T = std::tuple_element_t<i.cValue, ReturnType>;
                if (task_index >= output_task_ids.size()) {
                    throw ConnectionException{fmt::format("Not enough output tasks for job result")
                    };
                }
                core::Task const& task = tasks[task_index];
                if (output_index >= task.get_num_outputs()) {
                    throw ConnectionException{fmt::format("Not enough outputs for task")};
                }
                core::TaskOutput const& output = task.get_output(output_index);
                if (output.get_type() != typeid(T).name()) {
                    throw ConnectionException{fmt::format("Output type mismatch")};
                }
                if constexpr (cIsSpecializationV<T, Data>) {
                    using DataType = ExtractTemplateParamT<T>;
                    core::Data data;
                    std::optional<boost::uuids::uuid> const optional_data_id = output.get_data_id();
                    if (!optional_data_id.has_value()) {
                        throw ConnectionException{fmt::format("Output data ID is missing")};
                    }
                    err = m_data_storage->get_data(optional_data_id.value(), &data);
                    if (!err.success()) {
                        throw ConnectionException{
                                fmt::format("Failed to get data: {}", err.description)
                        };
                    }
                    std::get<i.cValue>(result) = core::DataImpl::create_data<DataType>(
                            std::make_unique<core::Data>(std::move(data)),
                            m_data_storage
                    );
                } else {
                    std::optional<std::string> const optional_value = output.get_value();
                    if (!optional_value.has_value()) {
                        throw ConnectionException{fmt::format("Output value is missing")};
                    }
                    std::string const& value = optional_value.value();
                    try {
                        msgpack::object_handle const handle
                                = msgpack::unpack(value.data(), value.size());
                        msgpack::object const& obj = handle.get();
                        std::get<i.cValue>(result) = obj.as<T>();
                    } catch (msgpack::type_error const& e) {
                        throw ConnectionException{fmt::format("Failed to unpack data: {}", e.what())
                        };
                    }
                }
                output_index++;
                if (output_index >= task.get_num_outputs()) {
                    task_index++;
                    output_index = 0;
                }
            });
        } else {
            if (output_task_ids.size() != 1) {
                throw ConnectionException{fmt::format("Expected one output task for job result")};
            }
            core::Task task{""};
            err = m_metadata_storage->get_task(output_task_ids[0], &task);
            if (!err.success()) {
                throw ConnectionException{fmt::format("Failed to get task: {}", err.description)};
            }
            if (task.get_num_outputs() != 1) {
                throw ConnectionException{fmt::format("Expected one output for task")};
            }
            core::TaskOutput const& output = task.get_output(0);
            if (output.get_type() != typeid(ReturnType).name()) {
                throw ConnectionException{fmt::format("Output type mismatch")};
            }
            if constexpr (cIsSpecializationV<ReturnType, Data>) {
                using DataType = ExtractTemplateParamT<ReturnType>;
                core::Data data;
                std::optional<boost::uuids::uuid> const optional_data_id = output.get_data_id();
                if (!optional_data_id.has_value()) {
                    throw ConnectionException{fmt::format("Output data ID is missing")};
                }
                err = m_data_storage->get_data(optional_data_id.value(), &data);
                if (!err.success()) {
                    throw ConnectionException{fmt::format("Failed to get data: {}", err.description)
                    };
                }
                return core::DataImpl::create_data<DataType>(
                        std::make_unique<core::Data>(std::move(data)),
                        m_data_storage
                );
            } else {
                std::optional<std::string> const optional_value = output.get_value();
                if (!optional_value.has_value()) {
                    throw ConnectionException{fmt::format("Output value is missing")};
                }
                std::string const& value = optional_value.value();
                try {
                    msgpack::object_handle const handle
                            = msgpack::unpack(value.data(), value.size());
                    msgpack::object const& obj = handle.get();
                    return obj.as<ReturnType>();
                } catch (msgpack::type_error const& e) {
                    throw ConnectionException{fmt::format("Failed to unpack data: {}", e.what())};
                }
            }
        }
    }

    /**
     * NOTE: It is undefined behavior to call this method for a job that is not in the `Failed`
     * state.
     *
     * @return A pair:
     * - the name of the task function that failed.
     * - the error message sent from the task through `TaskContext::abort` or from Spider.
     * @throw spider::ConnectionException
     */
    auto get_error() -> std::pair<std::string, std::string>;

private:
    Job(boost::uuids::uuid id,
        std::shared_ptr<core::MetadataStorage> metadata_storage,
        std::shared_ptr<core::DataStorage> data_storage)
            : m_id{id},
              m_metadata_storage{std::move(metadata_storage)},
              m_data_storage{std::move(data_storage)} {}

    boost::uuids::uuid m_id;
    std::shared_ptr<core::MetadataStorage> m_metadata_storage;
    std::shared_ptr<core::DataStorage> m_data_storage;

    friend class Driver;
};
}  // namespace spider

#endif  // SPIDER_CLIENT_JOB_HPP
