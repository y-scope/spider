#ifndef SPIDER_STORAGE_JOBSUBMISSIONBATCH_HPP
#define SPIDER_STORAGE_JOBSUBMISSIONBATCH_HPP

#include <spider/core/Error.hpp>
#include <spider/storage/StorageConnection.hpp>

namespace spider::core {
class JobSubmissionBatch {
public:
    virtual auto submit_batch(StorageConnection& conn) -> StorageErr = 0;

    JobSubmissionBatch() = default;
    JobSubmissionBatch(JobSubmissionBatch const&) = delete;
    auto operator=(JobSubmissionBatch const&) -> JobSubmissionBatch& = delete;
    JobSubmissionBatch(JobSubmissionBatch&&) = default;
    auto operator=(JobSubmissionBatch&&) -> JobSubmissionBatch& = default;
    virtual ~JobSubmissionBatch() = default;
};
}  // namespace spider::core

#endif
