//! The retry runner: executes a fallible async op under a [`RetryPolicy`],
//! honoring backoff, jitter, retry classification, and a cancellation token.

use crate::error::FaucetError;
use crate::resilience::policy::RetryPolicy;
use std::future::Future;
use tokio_util::sync::CancellationToken;

/// Execute `op` under `policy`. Retries retriable errors (per
/// [`RetryPolicy::is_retriable`]) up to `max_attempts` total attempts, sleeping
/// the backoff delay between attempts. If `cancel` is provided, a cancellation
/// during a backoff sleep stops waiting and returns the last error immediately
/// (the caller observes the token and flushes). `Ok` returns at once.
pub async fn execute_with_policy<F, Fut, T>(
    policy: &RetryPolicy,
    cancel: Option<&CancellationToken>,
    mut op: F,
) -> Result<T, FaucetError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    let mut attempt = 0u32;
    loop {
        match op().await {
            Ok(val) => return Ok(val),
            Err(e) if policy.is_retriable(&e) && attempt + 1 < policy.max_attempts => {
                let base = policy.backoff.delay(policy.base, policy.max, attempt);
                let wait = if policy.jitter {
                    crate::retry::apply_jitter(base)
                } else {
                    base
                };
                tracing::warn!(
                    "operation failed (attempt {}/{}), retrying in {wait:?}: {e}",
                    attempt + 1,
                    policy.max_attempts
                );
                if !wait.is_zero() {
                    match cancel {
                        Some(token) => {
                            tokio::select! {
                                biased;
                                _ = token.cancelled() => return Err(e),
                                _ = tokio::time::sleep(wait) => {}
                            }
                        }
                        None => tokio::time::sleep(wait).await,
                    }
                }
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resilience::policy::BackoffKind;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    fn fast_policy(max_attempts: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            backoff: BackoffKind::None,
            base: Duration::from_millis(0),
            max: Duration::from_millis(0),
            jitter: false,
            ..RetryPolicy::default()
        }
    }

    #[tokio::test]
    async fn retries_then_succeeds() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_policy(&fast_policy(5), None, move || {
            let n = c.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err::<i32, _>(FaucetError::HttpStatus {
                        status: 503,
                        url: "u".into(),
                        body: "".into(),
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

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_policy(&fast_policy(3), None, move || {
            c.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<i32, _>(FaucetError::HttpStatus {
                    status: 503,
                    url: "u".into(),
                    body: "".into(),
                })
            }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3, "1 initial + 2 retries");
    }

    #[tokio::test]
    async fn non_retriable_fails_immediately() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_policy(&fast_policy(5), None, move || {
            c.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>(FaucetError::Auth("nope".into())) }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn jittered_nonzero_backoff_with_no_cancel_retries() {
        // Exercises the `jitter: true` + non-zero `Fixed` backoff + `None` cancel
        // arms directly (the zero-backoff `fast_policy` skips both). Backoff is a
        // few ms so the test stays fast while still sleeping a real interval.
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff: BackoffKind::Fixed,
            base: Duration::from_millis(2),
            max: Duration::from_millis(2),
            jitter: true,
            ..RetryPolicy::default()
        };
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let r = execute_with_policy(&policy, None, move || {
            let n = c.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 1 {
                    Err::<i32, _>(FaucetError::HttpStatus {
                        status: 503,
                        url: "u".into(),
                        body: "".into(),
                    })
                } else {
                    Ok(9)
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 9);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancel_during_backoff_returns_last_error_promptly() {
        let policy = RetryPolicy {
            max_attempts: 10,
            backoff: BackoffKind::Fixed,
            base: Duration::from_secs(30),
            max: Duration::from_secs(30),
            jitter: false,
            ..RetryPolicy::default()
        };
        let token = CancellationToken::new();
        token.cancel(); // already cancelled → backoff sleep returns at once
        let r = execute_with_policy(&policy, Some(&token), || async {
            Err::<i32, _>(FaucetError::HttpStatus {
                status: 503,
                url: "u".into(),
                body: "".into(),
            })
        })
        .await;
        assert!(r.is_err());
    }
}
