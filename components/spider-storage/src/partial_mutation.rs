//! The guard that makes a handler body's partial mutation of the cache or the database fatal.

use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_utils::shutdown::ShutdownHandle;

/// A drop guard over a handler body's mutation of the cache or the database, which shuts the
/// storage service down fatally if the body stops before the mutation is complete.
///
/// The body creates the guard at its first mutation, and disarms it once the mutation is complete
/// or handed to its tree's committer. Dropping the guard without disarming it, on an error or a
/// panic, shuts the service down fatally, so that the next start recovers from the database, which
/// never saw the partial mutation. Dropping it other than by a panic while the service is already
/// shutting down leaves the shutdown as it is.
pub struct PartialMutationGuard<'shutdown_handle> {
    shutdown_handle: &'shutdown_handle ShutdownHandle,
    job_id: JobId,
    task_instance: Option<TaskInstanceMetadata>,
}

impl<'shutdown_handle> PartialMutationGuard<'shutdown_handle> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A guard over a mutation of `job_id`, which shuts the service down through `shutdown_handle`.
    #[must_use]
    pub const fn new(shutdown_handle: &'shutdown_handle ShutdownHandle, job_id: JobId) -> Self {
        Self {
            shutdown_handle,
            job_id,
            task_instance: None,
        }
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// A guard over a mutation of `job_id` for the task instance `task_instance_id` of `task_id`,
    /// which shuts the service down through `shutdown_handle`.
    #[must_use]
    pub const fn for_task_instance(
        shutdown_handle: &'shutdown_handle ShutdownHandle,
        job_id: JobId,
        task_id: TaskId,
        task_instance_id: TaskInstanceId,
    ) -> Self {
        Self {
            shutdown_handle,
            job_id,
            task_instance: Some(TaskInstanceMetadata {
                task_id,
                task_instance_id,
            }),
        }
    }

    /// Disarms the guard once the mutation is complete or handed to the tree's committer.
    pub const fn disarm(self) {
        std::mem::forget(self);
    }
}

impl Drop for PartialMutationGuard<'_> {
    fn drop(&mut self) {
        let (task_id, task_instance_id) = self
            .task_instance
            .as_ref()
            .map(|task_instance_metadata| {
                (
                    task_instance_metadata.task_id,
                    task_instance_metadata.task_instance_id,
                )
            })
            .unzip();
        if !std::thread::panicking() && self.shutdown_handle.is_cancelled() {
            tracing::warn!(
                job_id = ? self.job_id,
                task_id = ? task_id,
                task_instance_id = ? task_instance_id,
                "Handler body stopped after a partial mutation while the service is shutting down."
            );
            return;
        }
        tracing::error!(
            job_id = ? self.job_id,
            task_id = ? task_id,
            task_instance_id = ? task_instance_id,
            "Handler body stopped after a partial mutation. Cancelling service."
        );
        self.shutdown_handle.shut_down_fatally();
    }
}

/// The IDs of the task instance whose mutation a [`PartialMutationGuard`] guards.
struct TaskInstanceMetadata {
    task_id: TaskId,
    task_instance_id: TaskInstanceId,
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use spider_core::types::id::JobId;
    use spider_core::types::id::TaskId;
    use spider_utils::shutdown::ShutdownHandle;

    use super::PartialMutationGuard;

    #[test]
    fn dropping_a_guard_shuts_down_fatally() {
        let shutdown_handle = ShutdownHandle::new();
        let guard = PartialMutationGuard::for_task_instance(
            &shutdown_handle,
            JobId::random(),
            TaskId::Index(0),
            1,
        );

        drop(guard);

        assert!(shutdown_handle.is_fatal());
        assert!(shutdown_handle.is_cancelled());
    }

    #[test]
    fn a_disarmed_guard_is_not_fatal() {
        let shutdown_handle = ShutdownHandle::new();

        PartialMutationGuard::new(&shutdown_handle, JobId::random()).disarm();

        assert!(!shutdown_handle.is_cancelled());
    }

    #[test]
    fn dropping_a_guard_during_a_shutdown_is_not_fatal() {
        let shutdown_handle = ShutdownHandle::new();
        let guard = PartialMutationGuard::new(&shutdown_handle, JobId::random());

        shutdown_handle.shut_down_gracefully();
        drop(guard);

        assert!(!shutdown_handle.is_fatal());
    }

    #[test]
    fn a_panic_while_guarded_shuts_down_fatally_even_during_a_shutdown() {
        for is_shutting_down in [false, true] {
            let shutdown_handle = ShutdownHandle::new();
            if is_shutting_down {
                shutdown_handle.shut_down_gracefully();
            }

            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let _guard = PartialMutationGuard::new(&shutdown_handle, JobId::random());
                panic!("injected panic while guarded");
            }));

            assert!(result.is_err());
            assert!(
                shutdown_handle.is_fatal(),
                "a panic while guarded should be fatal, shutting down: {is_shutting_down}"
            );
            assert!(shutdown_handle.is_cancelled());
        }
    }
}
