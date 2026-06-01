//! Exponential backoff retry executor with jitter.

use faucet_core::FaucetError;
use std::future::Future;
use std::time::Duration;

/// Hard cap on *consecutive* `RateLimited` (HTTP 429) responses before the
/// executor gives up. A single transient 429 with a `Retry-After` is honoured
/// and retried (not counting against `max_retries`), but a perpetually
/// rate-limited endpoint must not loop forever (#78/#14). The counter resets
/// whenever any non-429 outcome occurs.
const MAX_CONSECUTIVE_RATE_LIMITS: u32 = 10;

/// Execute an async operation with exponential backoff retries and jitter.
///
/// - **`RateLimited`** errors sleep for the server-specified `Retry-After`
///   duration and do **not** consume a `max_retries` slot, but are bounded by
///   `MAX_CONSECUTIVE_RATE_LIMITS` consecutive occurrences so a permanently
///   throttled endpoint surfaces the `RateLimited` error instead of hanging.
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
    let mut rate_limited = 0u32;
    loop {
        match operation().await {
            Ok(val) => return Ok(val),
            Err(FaucetError::RateLimited(wait)) => {
                if rate_limited >= MAX_CONSECUTIVE_RATE_LIMITS {
                    tracing::error!("rate limited {rate_limited} consecutive times; giving up");
                    return Err(FaucetError::RateLimited(wait));
                }
                rate_limited += 1;
                tracing::warn!(
                    "rate limited; retrying after {wait:?} ({rate_limited}/{MAX_CONSECUTIVE_RATE_LIMITS})"
                );
                tokio::time::sleep(wait).await;
                // Rate-limited waits do not count as a `max_retries` attempt.
            }
            Err(e) if e.is_retriable() && attempt < max_retries => {
                // A non-429 outcome breaks the consecutive-429 streak.
                rate_limited = 0;
                tracing::warn!(
                    "request failed (attempt {}/{}): {e}",
                    attempt + 1,
                    max_retries + 1
                );
                let wait = faucet_core::retry::backoff_with_jitter(base_backoff, attempt);
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

// Backoff + jitter now lives in `faucet_core::retry::backoff_with_jitter` (capped
// at 60s, range-correct [0.5,1.5), decorrelated across concurrent retries). This
// module keeps only the 429/`Retry-After`-aware loop and delegates the sleep
// computation, so the jitter bug fixed in core can never diverge in a copy here.

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::FaucetError;

    // Backoff/jitter behaviour (range, cap, decorrelation) is tested in
    // `faucet_core::retry`; this module now delegates to it. The tests below
    // exercise the 429/`Retry-After`-aware retry loop that is unique to REST.

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

    #[tokio::test]
    async fn execute_with_retry_perpetual_rate_limit_is_bounded() {
        // Regression for #78/#14: a permanently rate-limited endpoint must not
        // loop forever. After MAX_CONSECUTIVE_RATE_LIMITS retries the executor
        // surfaces the RateLimited error.
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cc = call_count.clone();

        let result = execute_with_retry(3, Duration::from_millis(1), move || {
            cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err::<i32, _>(FaucetError::RateLimited(Duration::from_millis(1))) }
        })
        .await;

        assert!(matches!(result, Err(FaucetError::RateLimited(_))));
        // 1 initial call + MAX_CONSECUTIVE_RATE_LIMITS retries, then it gives up.
        assert_eq!(
            call_count.load(std::sync::atomic::Ordering::SeqCst),
            MAX_CONSECUTIVE_RATE_LIMITS + 1
        );
    }
}
