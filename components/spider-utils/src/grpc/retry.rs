//! An async retry helper for transient gRPC call failures.

use std::error::Error;
use std::future::Future;
use std::time::Duration;

use rand::Rng;
use tonic::Code;
use tonic::Status;

/// Tunable retry policy for transient gRPC call failures.
#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    /// The number of retries allowed after the initial attempt.
    pub max_retries: usize,
    /// The upper bound on the exponential backoff between attempts.
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    /// # Returns
    ///
    /// A [`RetryConfig`] with [`DEFAULT_MAX_RETRIES`] retries and a [`DEFAULT_MAX_BACKOFF`] backoff
    /// cap.
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            max_backoff: DEFAULT_MAX_BACKOFF,
        }
    }
}

/// Repeatedly invokes an async gRPC call until it succeeds, a non-retriable error occurs, or the
/// retry budget is exhausted.
///
/// Between attempts, the helper sleeps for an exponentially increasing backoff that doubles on each
/// retry, capped at `max_backoff`, plus a small random jitter, so the actual wait may slightly
/// exceed `max_backoff`.
///
/// `max_retries` counts the retries allowed *after* the initial attempt, so `grpc_call` is invoked
/// at most `max_retries + 1` times.
///
/// # Type Parameters
///
/// * `ResponseType` - The success value produced by `grpc_call`.
/// * `ErrorType` - The error produced by `grpc_call`.
/// * `GrpcCall` - The closure performing the gRPC call.
/// * `FutureType` - The `Send` future returned by `grpc_call`.
/// * `RetriableCheck` - Classifies an error as retriable or not.
///
/// # Returns
///
/// The first `Ok` value returned by `grpc_call`.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards `grpc_call`'s return values on failure if:
///   * The error is rejected by `is_retriable`.
///   * The retry budget is exhausted.
pub async fn execute_with_retry<
    ResponseType,
    ErrorType,
    GrpcCall: FnMut() -> FutureType,
    FutureType: Future<Output = Result<ResponseType, ErrorType>> + Send,
    RetriableCheck: Fn(&ErrorType) -> bool,
>(
    max_retries: usize,
    max_backoff: Duration,
    mut grpc_call: GrpcCall,
    is_retriable: RetriableCheck,
) -> Result<ResponseType, ErrorType> {
    let mut retry = 0usize;
    loop {
        let error = match grpc_call().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if !is_retriable(&error) || retry >= max_retries {
            return Err(error);
        }
        tokio::time::sleep(backoff(retry, max_backoff)).await;
        retry += 1;
    }
}

/// Executes a gRPC call under the retry policy configured by `retry_config`, retrying only on
/// [`Code::Unavailable`].
///
/// # Type Parameters
///
/// * `ResponseType` - The success value produced by `grpc_call`.
/// * `GrpcCall` - The closure performing the gRPC round-trip.
/// * `FutureType` - The `Send` future returned by `grpc_call`.
///
/// # Returns
///
/// The first successful response returned by `grpc_call`.
///
/// # Errors
///
/// Returns an error if:
///
/// * Forwards `grpc_call`'s [`Status`] on failure if the status is non-retriable or the retry
///   budget is exhausted.
pub async fn call_with_retry<
    ResponseType,
    GrpcCall: FnMut() -> FutureType,
    FutureType: Future<Output = Result<ResponseType, Status>> + Send,
>(
    retry_config: RetryConfig,
    grpc_call: GrpcCall,
) -> Result<ResponseType, Status> {
    execute_with_retry(
        retry_config.max_retries,
        retry_config.max_backoff,
        grpc_call,
        is_retriable_status,
    )
    .await
}

/// Classifies a gRPC [`Status`] as retriable.
///
/// # Returns
///
/// Whether `status` is retriable, which holds when its code is:
///
/// * [`Code::Unavailable`], the server's signal that the request may be retried; or
/// * [`Code::Unknown`] carrying a [`tonic::transport::Error`] source, which is how tonic surfaces a
///   client-side transport failure (such as a dropped connection) that is worth retrying.
fn is_retriable_status(status: &Status) -> bool {
    match status.code() {
        Code::Unavailable => true,
        Code::Unknown => status
            .source()
            .is_some_and(|source| source.downcast_ref::<tonic::transport::Error>().is_some()),
        _ => false,
    }
}

/// The default number of retries allowed after the initial attempt.
const DEFAULT_MAX_RETRIES: usize = 10;
/// The default upper bound on the exponential backoff between attempts.
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(3);

/// Computes the delay before the `retry`-th retry.
///
/// # Returns
///
/// An exponentially increasing delay capped at `max_backoff`, plus a small random jitter; the
/// returned value may slightly exceed `max_backoff`.
fn backoff(retry: usize, max_backoff: Duration) -> Duration {
    /// The backoff applied before the first retry, doubled on each subsequent retry.
    const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
    /// The maximum random jitter, in milliseconds, added on top of the capped backoff.
    const MAX_JITTER_MILLIS: u64 = 20;

    let capped = u32::try_from(retry)
        .ok()
        .and_then(|shift| 1u32.checked_shl(shift))
        .and_then(|multiplier| INITIAL_BACKOFF.checked_mul(multiplier))
        .unwrap_or(max_backoff)
        .min(max_backoff);
    let jitter = Duration::from_millis(rand::rng().random_range(0..=MAX_JITTER_MILLIS));
    capped.saturating_add(jitter)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tonic::Code;
    use tonic::Status;

    use super::RetryConfig;
    use super::backoff;
    use super::call_with_retry;
    use super::execute_with_retry;

    /// A negligible cap so the tests never sleep for a meaningful amount of time.
    const TEST_MAX_BACKOFF: Duration = Duration::from_millis(1);

    #[tokio::test]
    async fn succeeds_on_first_attempt() {
        let calls = AtomicUsize::new(0);
        let result: Result<i32, i32> = execute_with_retry(
            3,
            TEST_MAX_BACKOFF,
            async || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(42)
            },
            |_error| true,
        )
        .await;

        assert_eq!(result, Ok(42));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn succeeds_after_retriable_failures() {
        let calls = AtomicUsize::new(0);
        let result: Result<i32, i32> = execute_with_retry(
            5,
            TEST_MAX_BACKOFF,
            async || {
                let attempt = calls.fetch_add(1, Ordering::Relaxed);
                if attempt < 2 { Err(-1) } else { Ok(7) }
            },
            |_error| true,
        )
        .await;

        assert_eq!(result, Ok(7));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn non_retriable_error_returns_immediately() {
        let calls = AtomicUsize::new(0);
        let result: Result<i32, i32> = execute_with_retry(
            3,
            TEST_MAX_BACKOFF,
            async || {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(99)
            },
            |error| *error != 99,
        )
        .await;

        assert_eq!(result, Err(99));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn retries_are_exhausted() {
        let max_retries = 4usize;
        let calls = AtomicUsize::new(0);
        let result: Result<i32, i32> = execute_with_retry(
            max_retries,
            TEST_MAX_BACKOFF,
            async || {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(-7)
            },
            |_error| true,
        )
        .await;

        assert_eq!(result, Err(-7));
        assert_eq!(calls.load(Ordering::Relaxed), max_retries + 1);
    }

    #[tokio::test]
    async fn call_with_retry_retries_unavailable_then_succeeds() {
        let config = RetryConfig {
            max_retries: 5,
            max_backoff: TEST_MAX_BACKOFF,
        };
        let calls = AtomicUsize::new(0);
        let result: Result<i32, Status> = call_with_retry(config, async || {
            let attempt = calls.fetch_add(1, Ordering::Relaxed);
            if attempt < 2 {
                Err(Status::unavailable("connection lost"))
            } else {
                Ok(11)
            }
        })
        .await;

        assert_eq!(
            result.expect("call_with_retry should succeed after retriable failures"),
            11
        );
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn call_with_retry_returns_immediately_on_non_retriable_status() {
        let config = RetryConfig {
            max_retries: 5,
            max_backoff: TEST_MAX_BACKOFF,
        };
        let calls = AtomicUsize::new(0);
        let result: Result<i32, Status> = call_with_retry(config, async || {
            calls.fetch_add(1, Ordering::Relaxed);
            Err(Status::not_found("missing"))
        })
        .await;

        assert_eq!(
            result
                .expect_err("call_with_retry should propagate a non-retriable status")
                .code(),
            Code::NotFound
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn backoff_stays_within_jitter_bounds() {
        /// The expected initial backoff, mirroring the implementation's private constant.
        const EXPECTED_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
        /// The expected maximum jitter, mirroring the implementation's private constant.
        const EXPECTED_MAX_JITTER: Duration = Duration::from_millis(20);

        for (retry, max_backoff) in [
            (0usize, Duration::from_millis(1000)),
            (3usize, Duration::from_millis(1000)),
            (5usize, Duration::from_millis(1000)),
        ] {
            let capped_expected = (EXPECTED_INITIAL_BACKOFF * (1u32 << retry)).min(max_backoff);
            for _ in 0..100 {
                let actual = backoff(retry, max_backoff);
                assert!(
                    actual >= capped_expected,
                    "backoff {actual:?} is below the expected floor {capped_expected:?}"
                );
                assert!(
                    actual <= capped_expected + EXPECTED_MAX_JITTER,
                    "backoff {actual:?} exceeds the expected ceiling {:?}",
                    capped_expected + EXPECTED_MAX_JITTER
                );
            }
        }
    }
}
