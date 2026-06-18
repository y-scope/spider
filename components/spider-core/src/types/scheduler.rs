use std::net::IpAddr;

use crate::types::id::SchedulerId;

/// The currently registered scheduler endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisteredScheduler {
    pub id: SchedulerId,
    pub ip_address: IpAddr,
    pub port: u16,
}
