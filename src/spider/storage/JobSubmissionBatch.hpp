#ifndef SPIDER_STORAGE_JOBSUBMISSIONBATCH_HPP
#define SPIDER_STORAGE_JOBSUBMISSIONBATCH_HPP

#include "../../core/Error.hpp"
#include "../StorageConnection.hpp"

namespace spider::core {
class JobSubmissionBatch {
public:
    virtual ~JobSubmissionBatch() = 0;
    virtual auto submit_batch(StorageConnection& conn) -> StorageErr = 0;
};
}  // namespace spider::core

#endif
