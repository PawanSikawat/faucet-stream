//! Event-driven pipeline triggers for `faucet serve` (#196).
//!
//! A static `--triggers <file>` defines watchers (object-arrival / webhook /
//! queue-depth) that, on fire, enqueue a run via [`crate::serve::runner::submit`]
//! — reusing the whole queue/executor/idempotency pipeline. Pure decision logic
//! (spec validation, `${trigger.*}` substitution, cursors, edge detection) is
//! separated from the IO shell (watchers, fire path, webhook route).

pub mod compiled;
pub mod context;
pub mod enqueue;
pub mod health;
pub mod metrics;
pub mod spec;
pub mod watcher;
pub mod webhook;

#[cfg(feature = "triggers-object-store")]
pub mod object_arrival;
#[cfg(any(feature = "triggers-redis", feature = "triggers-kafka"))]
pub mod queue_depth;
