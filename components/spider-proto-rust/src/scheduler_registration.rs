//! Conversions between protobuf scheduler messages and their Spider core representations.

use spider_core::types::scheduler::RegisteredScheduler;

use crate::storage;

impl From<RegisteredScheduler> for storage::Scheduler {
    fn from(scheduler: RegisteredScheduler) -> Self {
        Self {
            scheduler_id: scheduler.id.get(),
            host: scheduler.host.into_inner(),
            port: u32::from(scheduler.port),
        }
    }
}

#[cfg(test)]
mod tests {
    use spider_core::types::id::SchedulerId;
    use spider_core::types::scheduler::RegisteredScheduler;
    use spider_utils::config::Host;

    use crate::storage;

    #[test]
    fn test_registered_scheduler_to_protocol_carries_id_host_and_port() {
        const SCHEDULER_ID: SchedulerId = SchedulerId::from(42);
        const PORT: u16 = 5678;

        let scheduler = storage::Scheduler::from(RegisteredScheduler {
            id: SCHEDULER_ID,
            host: Host::new("spider-scheduler".to_owned())
                .expect("scheduler host should not be empty"),
            port: PORT,
        });

        assert_eq!(scheduler.scheduler_id, SCHEDULER_ID.get());
        assert_eq!(scheduler.host, "spider-scheduler");
        assert_eq!(scheduler.port, u32::from(PORT));
    }
}
