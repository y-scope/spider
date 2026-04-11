use async_trait::async_trait;
use spider_core::{task::TaskIndex, types::id::JobId};

use crate::cache::error::InternalError;

#[derive(Debug)]
/// A message sent through the ready queue.
///
/// Each message represents a schedulable unit of work, tagged with the [`JobId`] it belongs to so
/// that the scheduler can dispatch work to the correct job context in a multi-job environment.
pub enum ReadyMessage {
    /// A batch of tasks are ready to be scheduled.
    Task {
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    },

    /// The commit task is ready to be scheduled.
    Commit { job_id: JobId },

    /// The cleanup task is ready to be scheduled.
    Cleanup { job_id: JobId },
}

/// Connector for publishing task execution events to the ready queue.
///
/// This trait is invoked by the cache layer to enqueue tasks that are ready for scheduling.
#[async_trait]
pub trait ReadyQueueSender: Clone + Send + Sync {
    /// Enqueues a batch of tasks for the specified job which are ready to be scheduled.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    /// * `task_indices` - The indices of the tasks that are ready.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the tasks fail to be sent to the ready queue.
    async fn send_task_ready(
        &self,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError>;

    /// Enqueues a signal indicating that the commit task of the given job is ready to be scheduled.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the message fails to be sent to the ready queue.
    async fn send_commit_ready(&self, job_id: JobId) -> Result<(), InternalError>;

    /// Enqueues a signal indicating that the cleanup task of the given job is ready to be
    /// scheduled.
    ///
    /// # Parameters
    ///
    /// * `job_id` - The job ID.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the message fails to be sent to the ready queue.
    async fn send_cleanup_ready(&self, job_id: JobId) -> Result<(), InternalError>;
}

/// Connector for consuming task execution events from the ready queue.
///
/// This trait is invoked by the scheduler to dequeue tasks that are ready for dispatch.
#[async_trait]
pub trait ReadyQueueReceiver: Clone + Send + Sync {
    /// Receives the next message from the ready queue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`InternalError`] if the message fails to be received from the ready queue.
    async fn recv(&self) -> Result<ReadyMessage, InternalError>;
}

/// Creates a new unbounded ready queue.
///
/// # Returns
///
/// A tuple of (sender, receiver) backed by an [`async_channel`].
#[must_use]
pub fn channel() -> (ReadyQueueSenderImpl, ReadyQueueReceiverImpl) {
    let (sender, receiver) = async_channel::unbounded();
    (
        ReadyQueueSenderImpl { sender },
        ReadyQueueReceiverImpl { receiver },
    )
}

#[derive(Clone)]
pub struct ReadyQueueSenderImpl {
    sender: async_channel::Sender<ReadyMessage>,
}

#[async_trait]
impl ReadyQueueSender for ReadyQueueSenderImpl {
    async fn send_task_ready(
        &self,
        job_id: JobId,
        task_indices: Vec<TaskIndex>,
    ) -> Result<(), InternalError> {
        self.sender
            .send(ReadyMessage::Task {
                job_id,
                task_indices,
            })
            .await
            .map_err(|e| InternalError::ReadyQueueSendFailure(e.to_string()))
    }

    async fn send_commit_ready(&self, job_id: JobId) -> Result<(), InternalError> {
        self.sender
            .send(ReadyMessage::Commit { job_id })
            .await
            .map_err(|e| InternalError::ReadyQueueSendFailure(e.to_string()))
    }

    async fn send_cleanup_ready(&self, job_id: JobId) -> Result<(), InternalError> {
        self.sender
            .send(ReadyMessage::Cleanup { job_id })
            .await
            .map_err(|e| InternalError::ReadyQueueSendFailure(e.to_string()))
    }
}

#[derive(Clone)]
pub struct ReadyQueueReceiverImpl {
    receiver: async_channel::Receiver<ReadyMessage>,
}

#[async_trait]
impl ReadyQueueReceiver for ReadyQueueReceiverImpl {
    async fn recv(&self) -> Result<ReadyMessage, InternalError> {
        self.receiver
            .recv()
            .await
            .map_err(|e| InternalError::ReadyQueueReceiveFailure(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_task_message() {
        let (sender, receiver) = channel();
        let job_id = JobId::default();
        let task_indices = vec![0, 1, 2];

        sender
            .send_task_ready(job_id, task_indices.clone())
            .await
            .expect("send should succeed");

        let msg = receiver.recv().await.expect("recv should succeed");
        assert!(
            matches!(
                &msg,
                ReadyMessage::Task {
                    job_id: received_job_id,
                    task_indices: received_indices,
                } if *received_job_id == job_id && *received_indices == task_indices
            ),
            "received message should match sent task message"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_commit_message() {
        let (sender, receiver) = channel();
        let job_id = JobId::default();

        sender
            .send_commit_ready(job_id)
            .await
            .expect("send should succeed");

        let msg = receiver.recv().await.expect("recv should succeed");
        assert!(
            matches!(
                &msg,
                ReadyMessage::Commit {
                    job_id: received_job_id,
                } if *received_job_id == job_id
            ),
            "received message should match sent commit message"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_cleanup_message() {
        let (sender, receiver) = channel();
        let job_id = JobId::default();

        sender
            .send_cleanup_ready(job_id)
            .await
            .expect("send should succeed");

        let msg = receiver.recv().await.expect("recv should succeed");
        assert!(
            matches!(
                &msg,
                ReadyMessage::Cleanup {
                    job_id: received_job_id,
                } if *received_job_id == job_id
            ),
            "received message should match sent cleanup message"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recv_fails_when_sender_dropped() {
        let (sender, receiver) = channel();
        drop(sender);

        let result = receiver.recv().await;
        assert!(
            matches!(result, Err(InternalError::ReadyQueueReceiveFailure(_))),
            "recv should fail when the sender is dropped, got: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_fails_when_receiver_dropped() {
        let (sender, receiver) = channel();
        drop(receiver);

        let result = sender.send_task_ready(JobId::default(), vec![0]).await;
        assert!(
            matches!(result, Err(InternalError::ReadyQueueSendFailure(_))),
            "send should fail when the receiver is dropped, got: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_and_recv_preserves_order() {
        let (sender, receiver) = channel();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![0])
            .await
            .expect("send task should succeed");
        sender
            .send_commit_ready(job_id)
            .await
            .expect("send commit should succeed");
        sender
            .send_cleanup_ready(job_id)
            .await
            .expect("send cleanup should succeed");

        assert!(matches!(
            receiver.recv().await,
            Ok(ReadyMessage::Task { .. })
        ));
        assert!(matches!(
            receiver.recv().await,
            Ok(ReadyMessage::Commit { .. })
        ));
        assert!(matches!(
            receiver.recv().await,
            Ok(ReadyMessage::Cleanup { .. })
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cloned_receiver_receives_from_same_channel() {
        let (sender, receiver) = channel();
        let receiver2 = receiver.clone();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![42])
            .await
            .expect("send should succeed");

        let msg = tokio::select! {
            m = receiver.recv() => m,
            m = receiver2.recv() => m,
        }
        .expect("one receiver should get the message");

        assert!(matches!(msg, ReadyMessage::Task { task_indices, .. } if task_indices == vec![42]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cloned_sender_sends_to_same_channel() {
        let (sender, receiver) = channel();
        let sender2 = sender.clone();
        let job_id = JobId::default();

        sender
            .send_task_ready(job_id, vec![1])
            .await
            .expect("send from original should succeed");
        sender2
            .send_task_ready(job_id, vec![2])
            .await
            .expect("send from clone should succeed");

        let msg1 = receiver.recv().await.expect("recv 1 should succeed");
        let msg2 = receiver.recv().await.expect("recv 2 should succeed");

        let all_indices: Vec<TaskIndex> = [&msg1, &msg2]
            .iter()
            .filter_map(|m| match m {
                ReadyMessage::Task { task_indices, .. } => Some(task_indices.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(all_indices.len(), 2);
        assert!(all_indices.contains(&1));
        assert!(all_indices.contains(&2));
    }
}
