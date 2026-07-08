//! Conversions between protobuf I/O messages and their Spider core representations.

use spider_core::task::TdlContext;
use spider_core::task::TimeoutPolicy;
use spider_core::types::io::ExecutionContext;

use crate::error::Error;
use crate::storage;

impl TryFrom<storage::ExecutionContext> for ExecutionContext {
    type Error = Error;

    fn try_from(execution_context: storage::ExecutionContext) -> Result<Self, Self::Error> {
        let tdl_context = execution_context
            .tdl_context
            .ok_or(Error::TdlContextMissing)?;
        let timeout_policy = execution_context
            .timeout_policy
            .ok_or(Error::TimeoutPolicyMissing)?;
        Ok(Self {
            task_instance_id: execution_context.task_instance_id,
            tdl_context: TdlContext {
                package: tdl_context.package,
                task_func: tdl_context.task_func,
            },
            timeout_policy: TimeoutPolicy {
                soft_timeout_ms: timeout_policy.soft_timeout_ms,
                hard_timeout_ms: timeout_policy.hard_timeout_ms,
            },
            serialized_inputs: execution_context.serialized_inputs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_context_converts_from_proto() {
        let proto = storage::ExecutionContext {
            task_instance_id: 7,
            tdl_context: Some(storage::TdlContext {
                package: "pkg".to_owned(),
                task_func: "func".to_owned(),
            }),
            timeout_policy: Some(storage::TimeoutPolicy {
                soft_timeout_ms: 100,
                hard_timeout_ms: 200,
            }),
            serialized_inputs: vec![1, 2, 3],
        };

        let execution_context =
            ExecutionContext::try_from(proto).expect("conversion should succeed");

        assert_eq!(execution_context.task_instance_id, 7);
        assert_eq!(execution_context.tdl_context.package, "pkg");
        assert_eq!(execution_context.tdl_context.task_func, "func");
        assert_eq!(execution_context.timeout_policy.soft_timeout_ms, 100);
        assert_eq!(execution_context.timeout_policy.hard_timeout_ms, 200);
        assert_eq!(execution_context.serialized_inputs, vec![1, 2, 3]);
    }

    #[test]
    fn execution_context_rejects_missing_tdl_context() {
        let proto = storage::ExecutionContext {
            task_instance_id: 7,
            tdl_context: None,
            timeout_policy: Some(storage::TimeoutPolicy {
                soft_timeout_ms: 100,
                hard_timeout_ms: 200,
            }),
            serialized_inputs: Vec::new(),
        };

        assert!(matches!(
            ExecutionContext::try_from(proto),
            Err(Error::TdlContextMissing)
        ));
    }
}
