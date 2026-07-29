//! Executor log actor.
//!
//! Every spawned `spider-task-executor` subprocess has its own coroutine forwarding the
//! subprocess's log output, so writes to the stream are concurrent and may overlap across executor
//! respawns. A single actor task owns the stream and appends one line at a time, which is what
//! makes each forwarded line land whole.

use std::io;

#[cfg(test)]
use tokio::io::AsyncBufReadExt;
#[cfg(test)]
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
#[cfg(test)]
use tokio::io::BufReader;
use tokio::io::BufWriter;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Cloneable handle for sending log lines into the running actor.
#[derive(Clone, Debug)]
pub struct ExecutorLogHandle {
    sender: mpsc::Sender<String>,
}

impl ExecutorLogHandle {
    /// Sends one log line to the actor in a fire-and-forget manner.
    ///
    /// `line` should not carry a trailing newline: the actor appends one.
    pub async fn write_line(&self, line: String) {
        let _ = self.sender.send(line).await;
    }
}

/// Spawns the executor log actor on the current tokio runtime.
///
/// The actor exits once every [`ExecutorLogHandle`] has been dropped and the lines they queued have
/// been written out, or earlier if the output stream fails. Both paths cancel `cancellation_token`,
/// so a completed [`JoinHandle`] does not by itself mean the shutdown was a clean one.
///
/// `cancellation_token` is fire-only: the actor never awaits it, since stopping on cancellation
/// would discard the lines still queued behind it.
///
/// # Type Parameters
///
/// * `WriterType` - The output stream that the actor takes ownership of and appends log lines to.
///
/// # Returns
///
/// A pair containing:
///
/// * A handle for sending log lines to the actor.
/// * The spawned actor's [`JoinHandle`].
pub fn spawn<WriterType: AsyncWrite + Unpin + Send + 'static>(
    writer: WriterType,
    cancellation_token: CancellationToken,
) -> (ExecutorLogHandle, JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(LINE_CHANNEL_CAP);
    let actor = ExecutorLogActor {
        receiver,
        writer: BufWriter::new(writer),
        cancellation_token,
    };
    let join = tokio::spawn(actor.run());
    (ExecutorLogHandle { sender }, join)
}

/// Spawns a task draining `reader` line by line until the writing end of the stream is dropped.
///
/// The drain has to run concurrently with the actor, otherwise a full stream buffer would deadlock
/// the caller.
///
/// # Type Parameters
///
/// * `ReaderType` - The stream the collector takes ownership of and reads lines from.
///
/// # Returns
///
/// The spawned task's [`JoinHandle`], resolving to every line read.
///
/// # Panics
///
/// The spawned task panics if reading from `reader` fails.
#[cfg(test)]
pub fn spawn_line_collector<ReaderType: AsyncRead + Unpin + Send + 'static>(
    reader: ReaderType,
) -> JoinHandle<Vec<String>> {
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        let mut collected = Vec::new();
        while let Some(line) = lines.next_line().await.expect("failed to read a line") {
            collected.push(line);
        }
        collected
    })
}

/// Capacity of the log line channel between the producers and the actor.
///
/// The channel is deliberately bounded: once it fills, producers block instead of dropping lines,
/// and the resulting backpressure propagates into the executors' log pipes.
const LINE_CHANNEL_CAP: usize = 1024;

/// The actor's owned state. Lives entirely inside the spawned task.
struct ExecutorLogActor<WriterType: AsyncWrite + Unpin> {
    receiver: mpsc::Receiver<String>,
    writer: BufWriter<WriterType>,
    cancellation_token: CancellationToken,
}

impl<WriterType: AsyncWrite + Unpin> ExecutorLogActor<WriterType> {
    /// Drives the actor until the log line channel closes or the output stream fails, then cancels
    /// the runtime.
    ///
    /// Cancelling unconditionally is safe because the process pool holds an [`ExecutorLogHandle`]
    /// for its whole lifetime: the channel cannot close while the runtime is live, and a normal
    /// shutdown drops the pool first, so the token is already cancelled by the time the actor
    /// exits.
    async fn run(mut self) {
        // Channel closure is the actor's only shutdown signal: `recv` yields `None` only once every
        // sender is gone and the queue is drained, so no queued line can be lost.
        while let Some(line) = self.receiver.recv().await {
            if let Err(e) = self.append_line(&line).await {
                tracing::error!(
                    err = % e,
                    "Failed to write an executor log line. Executor log actor shutting down."
                );
                break;
            }
        }

        if let Err(e) = self.writer.flush().await {
            tracing::error!(err = % e, "Failed to flush the final executor log lines.");
        }

        tracing::info!("Executor log actor exited. Cancelling the runtime.");
        self.cancellation_token.cancel();
    }

    /// Appends one newline-terminated line to the output stream.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * Forwards [`AsyncWriteExt::write_all`]'s return values on failure.
    /// * Forwards [`AsyncWriteExt::flush`]'s return values on failure.
    async fn append_line(&mut self, line: &str) -> io::Result<()> {
        self.writer.write_all(line.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;

        // NOTE: Flushing only once the queue runs dry coalesces a burst into a single syscall while
        // still writing a lone line out immediately. The size-based flushing is implicitly enforced
        // by [`BufWriter`].
        if self.receiver.is_empty() {
            self.writer.flush().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use tokio::io::AsyncWrite;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use super::spawn;
    use super::spawn_line_collector;

    /// Buffer capacity of the in-memory stream standing in for the actor's output.
    ///
    /// Deliberately smaller than the payload of the tests that write many lines, so that the actor
    /// has to interleave with the reader instead of writing everything into the buffer at once.
    const DUPLEX_BUF_CAP: usize = 32;

    /// Output stream standing in for broken stdout, which fails every operation intentionally.
    struct BrokenWriter;

    impl AsyncWrite for BrokenWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
        }
    }

    /// Joins the actor with a short upper bound so a stuck task surfaces as a test failure instead
    /// of an infinite hang.
    async fn join_actor(join: JoinHandle<()>) {
        tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .expect("actor did not exit within 1s")
            .expect("actor task panicked");
    }

    /// Joins the collector spawned by [`spawn_line_collector`] with a short upper bound.
    ///
    /// # Returns
    ///
    /// Every line the collector read.
    async fn join_collector(collector: JoinHandle<Vec<String>>) -> Vec<String> {
        tokio::time::timeout(Duration::from_secs(1), collector)
            .await
            .expect("collector did not exit within 1s")
            .expect("collector task panicked")
    }

    #[tokio::test]
    async fn lines_are_written_in_order() {
        const LINE_COUNT: usize = 64;

        let (writer, reader) = tokio::io::duplex(DUPLEX_BUF_CAP);
        let collector = spawn_line_collector(reader);
        let (handle, join) = spawn(writer, CancellationToken::new());

        let expected: Vec<String> = (0..LINE_COUNT)
            .map(|i| format!("executor line {i}"))
            .collect();
        for line in &expected {
            handle.write_line(line.clone()).await;
        }
        drop(handle);

        join_actor(join).await;
        assert_eq!(join_collector(collector).await, expected);
    }

    #[tokio::test]
    async fn queued_lines_are_written_before_exit() {
        let (writer, reader) = tokio::io::duplex(DUPLEX_BUF_CAP);
        let collector = spawn_line_collector(reader);
        let cancellation_token = CancellationToken::new();
        let (handle, join) = spawn(writer, cancellation_token.clone());

        let expected = vec!["first".to_owned(), "second".to_owned(), "third".to_owned()];
        for line in &expected {
            handle.write_line(line.clone()).await;
        }
        drop(handle);

        join_actor(join).await;
        assert_eq!(join_collector(collector).await, expected);
        assert!(cancellation_token.is_cancelled());
    }

    #[tokio::test]
    async fn cloned_handles_share_the_writer() {
        let (writer, reader) = tokio::io::duplex(DUPLEX_BUF_CAP);
        let collector = spawn_line_collector(reader);
        let (handle, join) = spawn(writer, CancellationToken::new());
        let cloned_handle = handle.clone();

        handle.write_line("from the original".to_owned()).await;
        cloned_handle.write_line("from the clone".to_owned()).await;
        drop(handle);
        drop(cloned_handle);

        join_actor(join).await;
        assert_eq!(
            join_collector(collector).await,
            vec!["from the original".to_owned(), "from the clone".to_owned()]
        );
    }

    #[tokio::test]
    async fn write_error_exits_and_cancels_runtime() {
        let cancellation_token = CancellationToken::new();
        let (handle, join) = spawn(BrokenWriter, cancellation_token.clone());

        handle.write_line("into the void".to_owned()).await;

        // `handle` is deliberately still alive: the actor must exit on the broken stream alone,
        // without the channel closing.
        join_actor(join).await;
        assert!(cancellation_token.is_cancelled());
        drop(handle);
    }
}
