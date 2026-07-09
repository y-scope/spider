//! Liveness actor — owns the periodic heartbeat to storage and the runtime's view of the current
//! storage session id.
//!
//! The actor runs as a dedicated tokio task driven by [`tokio::select!`] over three sources:
//!
//! 1. A [`tokio::time::interval`] driving periodic heartbeat ticks.
//! 2. An [`mpsc`] command channel from the rest of the runtime.
//! 3. A [`CancellationToken`] that the runtime flips on shutdown.

use std::time::Duration;

use spider_core::session::SessionTracker;
use spider_core::types::id::ExecutionManagerId;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Interval;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::client::LivenessClient;
use crate::client::LivenessResponseError;

/// Commands the runtime sends to the actor.
#[derive(Debug)]
pub enum LivenessCommand {
    /// Asks the actor to send an immediate heartbeat to storage instead of waiting for the next
    /// interval tick.
    ///
    /// Sent by the main loop when it suspects its session view is stale (e.g. after storage replies
    /// with a stale-session error). Storage's heartbeat response is the authoritative source of
    /// truth for the current session id, so the actor always re-checks rather than blindly trusting
    /// the caller's observation.
    Refresh,
}

/// Cloneable handle for sending commands into the running actor.
#[derive(Clone)]
pub struct LivenessHandle {
    cmd_sender: mpsc::Sender<LivenessCommand>,
}

impl LivenessHandle {
    /// Asks the actor to send an immediate heartbeat to storage in a fire-and-forget manner.
    pub async fn refresh(&self) {
        let _ = self.cmd_sender.send(LivenessCommand::Refresh).await;
    }
}

/// Spawns the liveness actor on the current tokio runtime.
///
/// The first heartbeat fires immediately when the spawned task is polled for the first time; from
/// there it ticks every `heartbeat_interval`. Missed ticks are skipped rather than burst-replayed.
///
/// # Returns
///
/// A pair containing:
///
/// * A handle for sending commands to the actor.
/// * The spawned task's [`JoinHandle`].
pub fn spawn<LivenessClientType: LivenessClient + Clone + 'static>(
    em_id: ExecutionManagerId,
    client: LivenessClientType,
    session_tracker: SessionTracker,
    cancellation_token: CancellationToken,
    heartbeat_interval: Duration,
) -> (LivenessHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(COMMAND_CHANNEL_CAP);
    let mut interval = tokio::time::interval(heartbeat_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let actor = LivenessActor {
        em_id,
        client,
        session_tracker,
        cmd_receiver: rx,
        cancellation_token,
        interval,
    };
    let join = tokio::spawn(actor.run());
    (LivenessHandle { cmd_sender: tx }, join)
}

/// Capacity of the command channel between the runtime and the actor.
const COMMAND_CHANNEL_CAP: usize = 16;

/// The actor's owned state. Lives entirely inside the spawned task.
struct LivenessActor<LivenessClientType: LivenessClient + Clone> {
    em_id: ExecutionManagerId,
    client: LivenessClientType,
    session_tracker: SessionTracker,
    cmd_receiver: mpsc::Receiver<LivenessCommand>,
    cancellation_token: CancellationToken,
    interval: Interval,
}

impl<LivenessClientType: LivenessClient + Clone> LivenessActor<LivenessClientType> {
    /// Drives the actor until cancellation or the command channel closes.
    async fn run(mut self) {
        loop {
            tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    tracing::info!("Cancellation token received. Liveness actor shutting down.");
                    break;
                },
                cmd = self.cmd_receiver.recv() => if let Some(cmd) = cmd {
                    self.on_command(&cmd).await;
                } else {
                    tracing::info!("Command channel closed. Liveness actor shutting down.");
                    break;
                },
                _ = self.interval.tick() => self.send_heartbeat().await,
            }
        }
    }

    /// Handles one command popped from the channel.
    async fn on_command(&mut self, cmd: &LivenessCommand) {
        match cmd {
            LivenessCommand::Refresh => {
                self.send_heartbeat().await;
            }
        }
    }

    /// Sends one heartbeat to storage, processes the response, and resets the interval so the next
    /// scheduled tick fires one period from now.
    ///
    /// Resetting the interval rate-limits refresh-triggered heartbeats: an off-schedule call
    /// (driven by [`LivenessCommand::Refresh`]) postpones the next scheduled tick, so the actor
    /// never sends two heartbeats closer together than `heartbeat_interval`.
    async fn send_heartbeat(&mut self) {
        match self.client.heartbeat(self.em_id).await {
            Ok(session_id) => {
                let previous = self.session_tracker.current();
                if previous != session_id {
                    if self.session_tracker.try_advance(session_id) {
                        tracing::info!(
                            from = previous,
                            to = session_id,
                            "Session advanced by heartbeat."
                        );
                    } else {
                        tracing::error!(
                            from = previous,
                            to = session_id,
                            "Session update rejected. This is unexpected since there should be no \
                             concurrent session updates in the current implementation. Cancelling \
                             the runtime."
                        );
                        self.cancellation_token.cancel();
                    }
                }
            }
            Err(LivenessResponseError::MarkedDead) => {
                tracing::error!(
                    "Liveness reports execution manager marked dead. Cancelling the runtime."
                );
                self.cancellation_token.cancel();
            }
            Err(LivenessResponseError::IllegalId(msg)) => {
                tracing::error!(
                    err = %msg,
                    "Liveness rejected the execution manager ID. Cancelling the runtime."
                );
                self.cancellation_token.cancel();
            }
            Err(LivenessResponseError::Transport(msg)) => {
                tracing::warn!(err = %msg, "Heartbeat transport error; retrying next tick.");
            }
        }
        self.interval.reset();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::net::IpAddr;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use spider_core::session::SessionTracker;
    use spider_core::types::id::ExecutionManagerId;
    use spider_core::types::id::SessionId;
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use super::LivenessHandle;
    use super::spawn;
    use crate::client::LivenessClient;
    use crate::client::LivenessResponseError;
    use crate::client::RegistrationResponse;

    struct MockState {
        responses: VecDeque<Result<SessionId, LivenessResponseError>>,
        call_count: u64,
    }

    /// Mock [`LivenessClient`] that returns scripted heartbeat responses and notifies the test
    /// once per call.
    struct MockLivenessClient {
        state: Mutex<MockState>,
        notify: Notify,
    }

    impl MockLivenessClient {
        /// Builds an empty mock. Tests prime the response queue via [`Self::push_response`] before
        /// spawning the actor.
        ///
        /// # Returns
        ///
        /// A newly created [`MockLivenessClient`] with an empty response queue.
        fn new() -> Self {
            Self {
                state: Mutex::new(MockState {
                    responses: VecDeque::new(),
                    call_count: 0,
                }),
                notify: Notify::new(),
            }
        }

        /// Pushes one scripted heartbeat response onto the queue.
        ///
        /// Responses are returned in FIFO order, one per [`LivenessClient::heartbeat`] call. If the
        /// queue is exhausted, the mock returns a synthetic [`LivenessResponseError::Transport`] so
        /// a misconfigured test fails loudly rather than hanging.
        fn push_response(&self, response: Result<SessionId, LivenessResponseError>) {
            self.state
                .lock()
                .expect("mock state lock poisoned")
                .responses
                .push_back(response);
        }

        /// # Returns
        ///
        /// The total number of [`LivenessClient::heartbeat`] invocations observed so far.
        fn call_count(&self) -> u64 {
            self.state
                .lock()
                .expect("mock state lock poisoned")
                .call_count
        }

        /// Awaits the next [`LivenessClient::heartbeat`] invocation.
        ///
        /// Backed by a [`Notify`] permit, so an invocation that fires before this future is polled
        /// can be still observed.
        async fn wait_for_call(&self) {
            self.notify.notified().await;
        }
    }

    #[async_trait]
    impl LivenessClient for MockLivenessClient {
        async fn register(
            &self,
            _ip: IpAddr,
        ) -> Result<RegistrationResponse, LivenessResponseError> {
            unimplemented!("`LivenessClient::register` is not exercised by actor tests")
        }

        async fn heartbeat(
            &self,
            _em_id: ExecutionManagerId,
        ) -> Result<SessionId, LivenessResponseError> {
            let response = {
                let mut state = self.state.lock().expect("mock state lock poisoned");
                state.call_count += 1;
                state.responses.pop_front().unwrap_or_else(|| {
                    Err(LivenessResponseError::Transport(
                        "MockLivenessClient: response queue exhausted".to_owned(),
                    ))
                })
            };
            self.notify.notify_one();
            response
        }
    }

    /// Spawns the actor with a long heartbeat interval so only the initial tick and explicit
    /// `Refresh`-driven heartbeats fire during the test.
    ///
    /// # Returns
    ///
    /// Forwards [`spawn`]'s return values.
    fn spawn_actor(
        client: Arc<MockLivenessClient>,
        tracker: SessionTracker,
        cancellation_token: CancellationToken,
    ) -> (LivenessHandle, JoinHandle<()>) {
        spawn(
            ExecutionManagerId::random(),
            client,
            tracker,
            cancellation_token,
            Duration::from_mins(1),
        )
    }

    /// Joins the actor with a short upper bound so a stuck task surfaces as a test failure
    /// instead of an infinite hang.
    async fn join_actor(join: JoinHandle<()>) {
        tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .expect("actor did not exit within 1s")
            .expect("actor task panicked");
    }

    #[tokio::test]
    async fn heartbeat_advances_tracker_on_success() {
        let client = Arc::new(MockLivenessClient::new());
        client.push_response(Ok(7));
        let tracker = SessionTracker::new(5);
        let cancellation_token = CancellationToken::new();

        let (_handle, join) = spawn_actor(
            Arc::clone(&client),
            tracker.clone(),
            cancellation_token.clone(),
        );

        client.wait_for_call().await;
        assert_eq!(tracker.current(), 7);
        assert!(!cancellation_token.is_cancelled());

        cancellation_token.cancel();
        join_actor(join).await;
    }

    #[tokio::test]
    async fn marked_dead_cancels_runtime() {
        let client = Arc::new(MockLivenessClient::new());
        client.push_response(Err(LivenessResponseError::MarkedDead));
        let cancellation_token = CancellationToken::new();

        let (_handle, join) = spawn_actor(
            Arc::clone(&client),
            SessionTracker::new(0),
            cancellation_token.clone(),
        );

        tokio::time::timeout(Duration::from_secs(1), cancellation_token.cancelled())
            .await
            .expect("token was not cancelled within 1s");
        join_actor(join).await;
    }

    #[tokio::test]
    async fn transport_error_does_not_cancel_runtime() {
        let client = Arc::new(MockLivenessClient::new());
        client.push_response(Err(LivenessResponseError::Transport(
            "simulated".to_owned(),
        )));
        let tracker = SessionTracker::new(5);
        let cancellation_token = CancellationToken::new();

        let (_handle, join) = spawn_actor(
            Arc::clone(&client),
            tracker.clone(),
            cancellation_token.clone(),
        );

        client.wait_for_call().await;
        assert!(!cancellation_token.is_cancelled());
        assert_eq!(tracker.current(), 5);

        cancellation_token.cancel();
        join_actor(join).await;
    }

    #[tokio::test]
    async fn refresh_triggers_immediate_heartbeat() {
        let client = Arc::new(MockLivenessClient::new());
        client.push_response(Ok(5));
        client.push_response(Ok(7));
        let tracker = SessionTracker::new(0);
        let cancellation_token = CancellationToken::new();

        let (handle, join) = spawn_actor(
            Arc::clone(&client),
            tracker.clone(),
            cancellation_token.clone(),
        );

        client.wait_for_call().await;
        assert_eq!(tracker.current(), 5);
        assert_eq!(client.call_count(), 1);

        handle.refresh().await;
        client.wait_for_call().await;
        assert_eq!(tracker.current(), 7);
        assert_eq!(client.call_count(), 2);

        cancellation_token.cancel();
        join_actor(join).await;
    }
}
