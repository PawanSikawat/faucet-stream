//! Unified resilience policy: retry, backoff classification, circuit breaker,
//! and poison-pill row handling. See
//! `docs/superpowers/specs/2026-06-17-resilience-policy-design.md`.

mod breaker;
mod classify;
mod execute;
mod policy;

pub use breaker::CircuitBreaker;
pub use classify::{RetryClass, RetryClassSet, classify};
pub use execute::execute_with_policy;
pub use policy::{
    BackoffKind, CircuitBreakerConfig, PoisonAction, PoisonPolicy, ResiliencePolicy, RetryPolicy,
};
