//! Configuration value types for the resilience policy.

use crate::error::FaucetError;
use crate::resilience::classify::{RetryClass, RetryClassSet, classify};
use std::time::Duration;

/// How the inter-attempt delay grows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackoffKind {
    /// No delay between attempts.
    None,
    /// Constant `base` delay.
    Fixed,
    /// `base * 2^attempt`, capped at `max`.
    #[default]
    Exponential,
}

impl BackoffKind {
    /// Pre-jitter delay for a zero-based `attempt` index.
    pub fn delay(self, base: Duration, max: Duration, attempt: u32) -> Duration {
        match self {
            BackoffKind::None => Duration::ZERO,
            BackoffKind::Fixed => base.min(max),
            BackoffKind::Exponential => base
                .saturating_mul(2u32.saturating_pow(attempt))
                .min(max),
        }
    }
}

/// Retry configuration shared by the pipeline loop and source connectors.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total attempts including the first (1 = no retry).
    pub max_attempts: u32,
    /// Backoff growth shape.
    pub backoff: BackoffKind,
    /// Base delay.
    pub base: Duration,
    /// Per-sleep cap (pre-jitter).
    pub max: Duration,
    /// Whether to apply `[0.5, 1.5)` jitter.
    pub jitter: bool,
    /// Which error classes are retried.
    pub retry_on: RetryClassSet,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            backoff: BackoffKind::Exponential,
            base: Duration::from_millis(200),
            max: Duration::from_secs(30),
            jitter: true,
            retry_on: RetryClassSet::default(),
        }
    }
}

impl RetryPolicy {
    /// Whether `err` should be retried under this policy: it must classify into
    /// a class that is in `retry_on`.
    pub fn is_retriable(&self, err: &FaucetError) -> bool {
        classify(err).is_some_and(|c| self.retry_on.contains(c))
    }
}

/// Circuit-breaker tuning. Counts consecutive page-level write failures.
#[derive(Debug, Clone, Copy)]
pub struct CircuitBreakerConfig {
    /// Consecutive exhausted-retry failures before the circuit opens.
    pub consecutive_failures: u32,
    /// Re-entry cooldown (honored by the orchestration layer, e.g. `schedule`).
    pub cooldown: Duration,
}

/// What to do with a row that keeps failing after `max_row_attempts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PoisonAction {
    /// Route to the DLQ (requires a DLQ to be configured).
    #[default]
    Dlq,
    /// Discard the row (counts + warns).
    Drop,
    /// Propagate the row error and abort the run.
    Fail,
}

/// Poison-pill (per-row) policy.
#[derive(Debug, Clone, Copy)]
pub struct PoisonPolicy {
    /// Per-row write attempts before applying `action`.
    pub max_row_attempts: u32,
    /// Terminal action for a persistently failing row.
    pub action: PoisonAction,
}

/// The umbrella policy attached to a run.
#[derive(Debug, Clone, Default)]
pub struct ResiliencePolicy {
    /// Retry/backoff applied to sink-write, flush, and state-store I/O.
    pub retry: RetryPolicy,
    /// Optional circuit breaker.
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    /// Optional poison-pill row handling (DLQ path only).
    pub poison: Option<PoisonPolicy>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_retries_all_classes() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 5);
        assert!(p.is_retriable(&FaucetError::HttpStatus {
            status: 500, url: "u".into(), body: "".into()
        }));
        assert!(!p.is_retriable(&FaucetError::Auth("x".into())));
    }

    #[test]
    fn restricted_retry_on_excludes_unlisted_classes() {
        let p = RetryPolicy {
            retry_on: RetryClassSet::from_iter([RetryClass::Http5xx]),
            ..RetryPolicy::default()
        };
        assert!(p.is_retriable(&FaucetError::HttpStatus {
            status: 500, url: "u".into(), body: "".into()
        }));
        // 429 classifies RateLimited, which is not opted in.
        assert!(!p.is_retriable(&FaucetError::HttpStatus {
            status: 429, url: "u".into(), body: "".into()
        }));
    }

    #[test]
    fn backoff_none_is_zero_fixed_is_constant_exp_grows() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(10);
        assert_eq!(BackoffKind::None.delay(base, max, 0), Duration::ZERO);
        assert_eq!(BackoffKind::Fixed.delay(base, max, 3), base);
        assert_eq!(BackoffKind::Exponential.delay(base, max, 0), base);
        assert_eq!(BackoffKind::Exponential.delay(base, max, 2), base * 4);
        // capped at max
        assert_eq!(BackoffKind::Exponential.delay(base, max, 30), max);
    }
}
