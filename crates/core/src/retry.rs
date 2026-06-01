//! Shared exponential-backoff retry executor for HTTP-style sources.
//!
//! Connector authors can wrap any fallible async operation so transient
//! failures (5xx, connection resets, timeouts — see
//! [`FaucetError::is_retriable`]) are retried with jittered backoff, while
//! non-retriable errors fail fast. Used by the XML, GraphQL, and other
//! sources that talk to flaky HTTP endpoints.

use crate::error::FaucetError;
use std::future::Future;
use std::time::Duration;

/// Execute `operation` with up to `max_retries` retries on
/// [retriable](FaucetError::is_retriable) errors, using exponential backoff
/// (`base_backoff * 2^attempt`) with random jitter. Non-retriable errors
/// return immediately; `Ok` returns immediately.
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
            Err(e) if e.is_retriable() && attempt < max_retries => {
                let wait = backoff_with_jitter(base_backoff, attempt);
                tracing::warn!(
                    "request failed (attempt {}/{}), retrying in {wait:?}: {e}",
                    attempt + 1,
                    max_retries + 1
                );
                tokio::time::sleep(wait).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// `base * 2^attempt` scaled by a random factor in `[0.5, 1.5)` to avoid a
/// thundering herd.
fn backoff_with_jitter(base: Duration, attempt: u32) -> Duration {
    let exp = base.saturating_mul(2u32.saturating_pow(attempt));
    let nanos = exp.as_nanos() as u64;
    Duration::from_nanos((nanos as f64 * pseudo_random_factor()) as u64)
}

/// Cheap, non-cryptographic random factor in `[0.5, 1.5)` from the clock's
/// sub-second component.
fn pseudo_random_factor() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    jitter_factor(nanos)
}

/// Map a sub-second nanosecond count (`[0, 1_000_000_000)`) to a jitter factor
/// in `[0.5, 1.5)`. `subsec_nanos()` is bounded by 1e9, so the divisor must be
/// 1e9 — not `u32::MAX` (~4.29e9), which would cap the factor at ~0.733 and
/// make every backoff shorter than documented.
fn jitter_factor(nanos: u32) -> f64 {
    0.5 + (nanos as f64 / 1_000_000_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn returns_immediately_on_success() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_retry(3, Duration::from_millis(1), move || {
            c.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, FaucetError>(7) }
        })
        .await;
        assert_eq!(r.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_then_succeeds_on_transient_5xx() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_retry(3, Duration::from_millis(1), move || {
            let n = c.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err::<i32, _>(FaucetError::HttpStatus {
                        status: 503,
                        url: "http://t".into(),
                        body: "x".into(),
                    })
                } else {
                    Ok(42)
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn jitter_factor_spans_documented_half_to_one_and_a_half_range() {
        // The factor must span [0.5, 1.5): 0 nanos → 0.5, the midpoint → 1.0,
        // and the maximum sub-second value → just under 1.5. A `u32::MAX`
        // divisor caps the top at ~0.733, so backoff is always too short.
        assert_eq!(jitter_factor(0), 0.5);
        let mid = jitter_factor(500_000_000);
        assert!((mid - 1.0).abs() < 1e-6, "midpoint factor was {mid}");
        let hi = jitter_factor(999_999_999);
        assert!(
            (1.4..1.5).contains(&hi),
            "factor at max sub-second nanos was {hi}, expected ~1.5"
        );
    }

    #[tokio::test]
    async fn non_retriable_fails_immediately() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_retry(3, Duration::from_millis(1), move || {
            c.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>(FaucetError::Auth("nope".into())) }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
