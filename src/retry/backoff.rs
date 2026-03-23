//! Exponential backoff retry executor.

use crate::error::FaucetError;
use std::future::Future;
use std::time::Duration;

/// Execute an async operation with exponential backoff retries.
pub async fn execute_with_retry<F, Fut, T>(
    max_retries: u32,
    base_backoff: Duration,
    mut operation: F,
) -> Result<T, FaucetError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    let mut last_err = None;
    for attempt in 0..=max_retries {
        match operation().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                tracing::warn!("request failed (attempt {}/{}): {e}", attempt + 1, max_retries + 1);
                last_err = Some(e);
                if attempt < max_retries {
                    let wait = base_backoff * 2u32.pow(attempt);
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }
    Err(last_err.unwrap())
}
