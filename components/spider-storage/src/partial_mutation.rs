//! The guard that makes a handler body's partial mutation of the cache or the database fatal.

use spider_core::types::id::JobId;
use spider_core::types::id::TaskId;
use spider_core::types::id::TaskInstanceId;
use spider_utils::shutdown::ShutdownHandle;

/// A drop guard over a handler body's mutation of the cache or the database, which shuts the
/// storage service down fatally if the body stops before the mutation is complete.
///
/// The body arms the guard at its first mutation, and disarms it once the mutation is complete or
/// handed to its tree's committer. Dropping the guard armed, on an error or a panic, shuts the
/// service down fatally, so that the next start recovers from the database, which never saw the
/// partial mutation. Dropping it armed other than by a panic while the service is already shutting
/// down leaves the shutdown as it is.
pub struct PartialMutationGuard<'shutdown_handle> {
    shutdown_handle: &'shutdown_handle ShutdownHandle,
    job_id: JobId,
    task_id: Option<TaskId>,
    task_instance_id: Option<TaskInstanceId>,
    is_armed: bool,
}

impl<'shutdown_handle> PartialMutationGuard<'shutdown_handle> {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A disarmed guard over a mutation of `job_id`, which shuts the service down through
    /// `shutdown_handle`.
    #[must_use]
    pub const fn new(shutdown_handle: &'shutdown_handle ShutdownHandle, job_id: JobId) -> Self {
        Self {
            shutdown_handle,
            job_id,
            task_id: None,
            task_instance_id: None,
            is_armed: false,
        }
    }

    /// Factory function.
    ///
    /// # Returns
    ///
    /// A disarmed guard over a mutation of `job_id` for the task instance `task_instance_id` of
    /// `task_id`, which shuts the service down through `shutdown_handle`.
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
            task_id: Some(task_id),
            task_instance_id: Some(task_instance_id),
            is_armed: false,
        }
    }

    /// Arms the guard at the first mutation.
    pub const fn arm(&mut self) {
        self.is_armed = true;
    }

    /// Disarms the guard once the mutation is complete or handed to the tree's committer.
    pub const fn disarm(&mut self) {
        self.is_armed = false;
    }
}

impl Drop for PartialMutationGuard<'_> {
    fn drop(&mut self) {
        if !self.is_armed {
            return;
        }
        if !std::thread::panicking() && self.shutdown_handle.is_cancelled() {
            tracing::warn!(
                job_id = ? self.job_id,
                task_id = ? self.task_id,
                task_instance_id = ? self.task_instance_id,
                "Handler body stopped after a partial mutation while the service is shutting down."
            );
            return;
        }
        tracing::error!(
            job_id = ? self.job_id,
            task_id = ? self.task_id,
            task_instance_id = ? self.task_instance_id,
            "Handler body stopped after a partial mutation. Cancelling service."
        );
        self.shutdown_handle.shut_down_fatally();
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use spider_core::types::id::JobId;
    use spider_core::types::id::TaskId;
    use spider_utils::shutdown::ShutdownHandle;

    use super::PartialMutationGuard;

    #[test]
    fn dropping_an_armed_guard_shuts_down_fatally() {
        let shutdown_handle = ShutdownHandle::new();
        let mut guard = PartialMutationGuard::for_task_instance(
            &shutdown_handle,
            JobId::random(),
            TaskId::Index(0),
            1,
        );

        guard.arm();
        drop(guard);

        assert!(shutdown_handle.is_fatal());
        assert!(shutdown_handle.is_cancelled());
    }

    #[test]
    fn dropping_a_disarmed_guard_is_not_fatal() {
        let shutdown_handle = ShutdownHandle::new();
        let mut guard = PartialMutationGuard::new(&shutdown_handle, JobId::random());

        guard.arm();
        guard.disarm();
        drop(guard);
        drop(PartialMutationGuard::new(&shutdown_handle, JobId::random()));

        assert!(!shutdown_handle.is_cancelled());
    }

    #[test]
    fn dropping_an_armed_guard_during_a_shutdown_is_not_fatal() {
        let shutdown_handle = ShutdownHandle::new();
        let mut guard = PartialMutationGuard::new(&shutdown_handle, JobId::random());

        guard.arm();
        shutdown_handle.shut_down_gracefully();
        drop(guard);

        assert!(!shutdown_handle.is_fatal());
    }

    #[test]
    fn a_panic_while_armed_shuts_down_fatally_even_during_a_shutdown() {
        for is_shutting_down in [false, true] {
            let shutdown_handle = ShutdownHandle::new();
            if is_shutting_down {
                shutdown_handle.shut_down_gracefully();
            }

            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let mut guard = PartialMutationGuard::new(&shutdown_handle, JobId::random());
                guard.arm();
                panic!("injected panic while armed");
            }));

            assert!(result.is_err());
            assert!(
                shutdown_handle.is_fatal(),
                "a panic while armed should be fatal, shutting down: {is_shutting_down}"
            );
            assert!(shutdown_handle.is_cancelled());
        }
    }
}
