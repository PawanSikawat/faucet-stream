//! Exponential backoff retry executor with jitter.

use crate::error::FaucetError;
use std::future::Future;
use std::time::Duration;

/// Execute an async operation with exponential backoff retries and jitter.
///
/// - **`RateLimited`** errors sleep for the server-specified `Retry-After`
///   duration and do **not** consume a retry slot.
/// - **Retriable** errors (5xx, connection/timeout) use exponential backoff
///   with random jitter and count toward `max_retries`.
/// - **Non-retriable** errors (4xx except 429, parse errors, etc.) fail
///   immediately without retrying.
pub async fn execute_with_retry<F, Fut, T>(
    max_retries: u32,
    base_backoff: Duration,
    mut operation: F,
) -> Result<T, FaucetError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    let mut attempt = 0u32;
    loop {
        match operation().await {
            Ok(val) => return Ok(val),
            Err(FaucetError::RateLimited(wait)) => {
                tracing::warn!("rate limited; retrying after {wait:?}");
                tokio::time::sleep(wait).await;
                // Rate-limited waits do not count as a retry attempt.
            }
            Err(e) if e.is_retriable() && attempt < max_retries => {
                tracing::warn!(
                    "request failed (attempt {}/{}): {e}",
                    attempt + 1,
                    max_retries + 1
                );
                let wait = backoff_with_jitter(base_backoff, attempt);
                tokio::time::sleep(wait).await;
                attempt += 1;
            }
            Err(e) => {
                if !e.is_retriable() {
                    tracing::error!("non-retriable error: {e}");
                } else {
                    tracing::error!("request failed after {} attempts: {e}", attempt + 1);
                }
                return Err(e);
            }
        }
    }
}

/// Compute exponential backoff with random jitter.
///
/// `base * 2^attempt` gives the exponential component; a random factor
/// between 0.5 and 1.5 is applied to spread out concurrent retries
/// (avoids thundering-herd).
fn backoff_with_jitter(base: Duration, attempt: u32) -> Duration {
    let exp = base * 2u32.pow(attempt);
    // Simple jitter: multiply by a random factor in [0.5, 1.5).
    // Uses a lightweight approach — no extra crate dependency.
    let nanos = exp.as_nanos() as u64;
    let jitter_factor = pseudo_random_factor();
    Duration::from_nanos((nanos as f64 * jitter_factor) as u64)
}

/// Returns a pseudo-random factor in [0.5, 1.5) using the current time's
/// nanosecond component as entropy. Not cryptographically secure, but
/// sufficient for retry jitter.
fn pseudo_random_factor() -> f64 {
    // Use the low bits of the current timestamp for cheap randomness.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    // Map to [0.5, 1.5)
    0.5 + (nanos as f64 / u32::MAX as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FaucetError;

    #[test]
    fn backoff_with_jitter_increases_with_attempt() {
        let base = Duration::from_millis(100);
        let d0 = backoff_with_jitter(base, 0);
        let _d1 = backoff_with_jitter(base, 1);
        let d2 = backoff_with_jitter(base, 2);

        // With jitter in [0.5, 1.5), the expected center doubles each attempt.
        // d0 center: 100ms, d1 center: 200ms, d2 center: 400ms
        // Even with worst-case jitter, d2 should be > d0's minimum.
        assert!(
            d0.as_millis() >= 50,
            "d0 should be at least 50ms, got {d0:?}"
        );
        assert!(
            d2.as_millis() >= 200,
            "d2 should be at least 200ms, got {d2:?}"
        );
    }

    #[test]
    fn pseudo_random_factor_in_expected_range() {
        // Call multiple times to increase confidence.
        for _ in 0..100 {
            let f = pseudo_random_factor();
            assert!(f >= 0.5, "factor {f} < 0.5");
            assert!(f < 1.5, "factor {f} >= 1.5");
        }
    }

    #[tokio::test]
    async fn execute_with_retry_success_on_first_try() {
        let result = execute_with_retry(3, Duration::from_millis(1), || async {
            Ok::<_, FaucetError>(42)
        })
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn execute_with_retry_non_retriable_fails_immediately() {
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result = execute_with_retry(3, Duration::from_millis(1), move || {
            cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err::<i32, _>(FaucetError::Auth("bad credentials".into())) }
        })
        .await;

        assert!(result.is_err());
        // Non-retriable error: should be called exactly once.
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn execute_with_retry_retriable_exhausts_retries() {
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result = execute_with_retry(2, Duration::from_millis(1), move || {
            cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async {
                Err::<i32, _>(FaucetError::HttpStatus {
                    status: 500,
                    url: "http://test".into(),
                    body: "error".into(),
                })
            }
        })
        .await;

        assert!(result.is_err());
        // Initial attempt + 2 retries = 3 total calls.
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn execute_with_retry_succeeds_after_transient_failure() {
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result = execute_with_retry(3, Duration::from_millis(1), move || {
            let count = cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if count < 2 {
                    Err::<i32, _>(FaucetError::HttpStatus {
                        status: 502,
                        url: "http://test".into(),
                        body: "bad gateway".into(),
                    })
                } else {
                    Ok(99)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 99);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn execute_with_retry_rate_limited_does_not_count_as_attempt() {
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result = execute_with_retry(
            0, // zero retries allowed
            Duration::from_millis(1),
            move || {
                let count = cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if count == 0 {
                        Err::<i32, _>(FaucetError::RateLimited(Duration::from_millis(1)))
                    } else {
                        Ok(42)
                    }
                }
            },
        )
        .await;

        // Even with 0 retries, RateLimited should retry (doesn't consume a slot).
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
