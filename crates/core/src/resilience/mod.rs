//! Unified resilience policy: retry, backoff classification, circuit breaker,
//! and poison-pill row handling. See
//! `docs/superpowers/specs/2026-06-17-resilience-policy-design.md`.

mod classify;

pub use classify::{RetryClass, RetryClassSet, classify};
