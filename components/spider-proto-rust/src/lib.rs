//! Rust gRPC protocol definitions generated from Spider protobuf files.

pub mod assignment;
pub mod error;
pub mod id;
pub mod io;
pub mod job;
pub mod scheduler_registration;
pub mod unpack;

#[allow(clippy::all, clippy::nursery, clippy::pedantic)]
pub mod common {
    include!("generated/common.rs");
}

#[allow(clippy::all, clippy::nursery, clippy::pedantic)]
pub mod scheduler {
    include!("generated/scheduler.rs");

    pub use scheduler_service_client::SchedulerServiceClient;
    pub use scheduler_service_server::SchedulerService;
    pub use scheduler_service_server::SchedulerServiceServer;
}

#[allow(clippy::all, clippy::nursery, clippy::pedantic)]
pub mod storage {
    include!("generated/storage.rs");

    pub use execution_manager_liveness_service_client::ExecutionManagerLivenessServiceClient;
    pub use execution_manager_liveness_service_server::ExecutionManagerLivenessService;
    pub use execution_manager_liveness_service_server::ExecutionManagerLivenessServiceServer;
    pub use inbound_queue_service_client::InboundQueueServiceClient;
    pub use inbound_queue_service_server::InboundQueueService;
    pub use inbound_queue_service_server::InboundQueueServiceServer;
    pub use job_orchestration_service_client::JobOrchestrationServiceClient;
    pub use job_orchestration_service_server::JobOrchestrationService;
    pub use job_orchestration_service_server::JobOrchestrationServiceServer;
    pub use resource_group_management_service_client::ResourceGroupManagementServiceClient;
    pub use resource_group_management_service_server::ResourceGroupManagementService;
    pub use resource_group_management_service_server::ResourceGroupManagementServiceServer;
    pub use scheduler_registration_service_client::SchedulerRegistrationServiceClient;
    pub use scheduler_registration_service_server::SchedulerRegistrationService;
    pub use scheduler_registration_service_server::SchedulerRegistrationServiceServer;
    pub use session_management_service_client::SessionManagementServiceClient;
    pub use session_management_service_server::SessionManagementService;
    pub use session_management_service_server::SessionManagementServiceServer;
    pub use task_instance_management_service_client::TaskInstanceManagementServiceClient;
    pub use task_instance_management_service_server::TaskInstanceManagementService;
    pub use task_instance_management_service_server::TaskInstanceManagementServiceServer;
}
