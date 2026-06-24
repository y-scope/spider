//! Conversions between protobuf scheduler messages and their Spider core representations.

use std::net::IpAddr;

use spider_core::types::{id::SchedulerId, scheduler::RegisteredScheduler};

use crate::{error::Error, storage};

impl From<RegisteredScheduler> for storage::Scheduler {
    fn from(scheduler: RegisteredScheduler) -> Self {
        Self {
            scheduler_id: scheduler.id.get(),
            ip_address: scheduler.ip_address.to_string(),
            port: u32::from(scheduler.port),
        }
    }
}

impl TryFrom<storage::Scheduler> for RegisteredScheduler {
    type Error = Error;

    fn try_from(scheduler: storage::Scheduler) -> Result<Self, Self::Error> {
        let ip_address = scheduler
            .ip_address
            .parse::<IpAddr>()
            .map_err(|error| Error::IpAddressInvalid(error.to_string()))?;
        let port =
            u16::try_from(scheduler.port).map_err(|_| Error::PortOutOfRange(scheduler.port))?;
        Ok(Self {
            id: SchedulerId::from(scheduler.scheduler_id),
            ip_address,
            port,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use spider_core::types::{id::SchedulerId, scheduler::RegisteredScheduler};

    use crate::{error::Error, storage};

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

    #[test]
    fn protocol_scheduler_to_core_round_trips() {
        const SCHEDULER_ID: SchedulerId = SchedulerId::from(42);

        let proto = storage::Scheduler::from(RegisteredScheduler {
            id: SCHEDULER_ID,
            ip_address: IpAddr::V4("127.0.0.1".parse().expect("valid IP")),
            port: 5678,
        });

        let scheduler = RegisteredScheduler::try_from(proto).expect("conversion should succeed");

        assert_eq!(scheduler.id, SCHEDULER_ID);
        assert_eq!(scheduler.ip_address.to_string(), "127.0.0.1");
        assert_eq!(scheduler.port, 5678);
    }

    #[test]
    fn protocol_scheduler_to_core_rejects_invalid_ip() {
        let proto = storage::Scheduler {
            scheduler_id: 1,
            ip_address: "not an ip".to_owned(),
            port: 8080,
        };

        assert!(matches!(
            RegisteredScheduler::try_from(proto),
            Err(Error::IpAddressInvalid(_))
        ));
    }
}
