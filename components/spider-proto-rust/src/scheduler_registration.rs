//! Conversions between protobuf scheduler messages and their Spider core representations.

use spider_core::types::scheduler::RegisteredScheduler;

use crate::storage;

impl From<RegisteredScheduler> for storage::Scheduler {
    fn from(scheduler: RegisteredScheduler) -> Self {
        Self {
            scheduler_id: scheduler.id.get(),
            ip_address: scheduler.ip_address.to_string(),
            port: u32::from(scheduler.port),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use spider_core::types::id::SchedulerId;
    use spider_core::types::scheduler::RegisteredScheduler;

    use crate::storage;

    #[test]
    fn registered_scheduler_to_protocol_carries_id_ip_and_port() {
        const SCHEDULER_ID: SchedulerId = SchedulerId::from(42);
        const PORT: u16 = 5678;

        let scheduler = storage::Scheduler::from(RegisteredScheduler {
            id: SCHEDULER_ID,
            ip_address: IpAddr::V4("127.0.0.1".parse().expect("valid IP")),
            port: PORT,
        });

        assert_eq!(scheduler.scheduler_id, SCHEDULER_ID.get());
        assert_eq!(scheduler.ip_address, "127.0.0.1");
        assert_eq!(scheduler.port, u32::from(PORT));
    }
}
