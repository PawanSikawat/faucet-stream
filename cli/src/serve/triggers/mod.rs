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

use crate::error::{CliError, CliResult};
use crate::serve::state::ServerState;
#[allow(unused_imports)]
use compiled::{CompiledTrigger, CompiledTriggers};
#[cfg(any(feature = "triggers-object-store", feature = "triggers-redis", feature = "triggers-kafka"))]
use std::sync::Arc;
#[cfg(any(feature = "triggers-object-store", feature = "triggers-redis", feature = "triggers-kafka"))]
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Load + validate a triggers file. Surfaces a clear `CliError::Serve` on any
/// parse/validation failure (fail-fast at startup).
pub async fn load_triggers(path: &std::path::Path) -> CliResult<CompiledTriggers> {
    let text = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| CliError::Serve(format!("reading triggers file {}: {e}", path.display())))?;
    let file: spec::TriggersFile =
        if path.extension().map(|e| e.eq_ignore_ascii_case("json")).unwrap_or(false) {
            serde_json::from_str(&text)
                .map_err(|e| CliError::Serve(format!("parsing triggers JSON: {e}")))?
        } else {
            serde_yaml::from_str(&text)
                .map_err(|e| CliError::Serve(format!("parsing triggers YAML: {e}")))?
        };
    CompiledTriggers::compile(file).map_err(CliError::Serve)
}

/// Spawn supervised watcher tasks for every enabled polling trigger. Webhook
/// triggers need no task (they are served by the route). Returns the join handles
/// (the caller aborts them on shutdown, like the maintenance/lease loops).
pub fn spawn_watchers(
    state: ServerState,
    compiled: &CompiledTriggers,
    #[cfg_attr(
        not(any(feature = "triggers-object-store", feature = "triggers-redis", feature = "triggers-kafka")),
        allow(unused_variables)
    )]
    shutdown: CancellationToken,
) -> Vec<JoinHandle<()>> {
    #[cfg_attr(
        not(any(feature = "triggers-object-store", feature = "triggers-redis", feature = "triggers-kafka")),
        allow(unused_mut)
    )]
    let mut handles = Vec::new();
    #[cfg_attr(
        not(any(feature = "triggers-object-store", feature = "triggers-redis", feature = "triggers-kafka")),
        allow(unused_variables)
    )]
    let health = state.triggers().clone();
    let mut active = 0usize;
    for t in &compiled.triggers {
        if !t.spec.enabled {
            tracing::info!(trigger = t.name(), "trigger disabled; not spawning");
            continue;
        }
        match &t.spec.kind {
            spec::TriggerKind::Webhook { .. } => {
                active += 1; // served by the route, no task
                tracing::info!(trigger = t.name(), path = ?t.webhook_path, "webhook trigger registered");
            }
            #[cfg(feature = "triggers-object-store")]
            spec::TriggerKind::ObjectArrival { store, poll_interval_secs, mode, start_at } => {
                match object_arrival::ObjectArrivalWatcher::build_store(store) {
                    Ok((s, bucket, prefix)) => {
                        let w = object_arrival::ObjectArrivalWatcher::new(
                            Arc::new(t.clone()),
                            s,
                            bucket,
                            prefix,
                            *mode,
                            Duration::from_secs(*poll_interval_secs),
                            *start_at,
                            chrono::Utc::now(),
                        );
                        handles.push(tokio::spawn(watcher::run_supervised(
                            w,
                            state.clone(),
                            health.clone(),
                            shutdown.clone(),
                        )));
                        active += 1;
                    }
                    Err(e) => tracing::error!(trigger = t.name(), error = %e, "failed to build object store; skipping watcher"),
                }
            }
            #[cfg(any(feature = "triggers-redis", feature = "triggers-kafka"))]
            spec::TriggerKind::QueueDepth { queue, threshold, poll_interval_secs } => {
                match queue_depth::build_probe(queue) {
                    Ok(probe) => {
                        let w = queue_depth::QueueDepthWatcher::new(
                            Arc::new(t.clone()),
                            probe,
                            *threshold,
                            Duration::from_secs(*poll_interval_secs),
                        );
                        handles.push(tokio::spawn(watcher::run_supervised(
                            w,
                            state.clone(),
                            health.clone(),
                            shutdown.clone(),
                        )));
                        active += 1;
                    }
                    Err(e) => tracing::error!(trigger = t.name(), error = %e, "failed to build queue probe; skipping watcher"),
                }
            }
            // Backends not compiled in were already rejected by `compile`, but the
            // match must be exhaustive when their features are off.
            #[cfg(not(feature = "triggers-object-store"))]
            spec::TriggerKind::ObjectArrival { .. } => {}
            #[cfg(not(any(feature = "triggers-redis", feature = "triggers-kafka")))]
            spec::TriggerKind::QueueDepth { .. } => {}
        }
    }
    metrics::active(active);
    handles
}

// Bring the compiled types into the public surface for `server.rs`.
pub use compiled::CompiledTriggers as Compiled;
