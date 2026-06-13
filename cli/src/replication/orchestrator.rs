//! Two-phase snapshot→CDC orchestration. Reuses `executor::run_expanded` for
//! each phase; seeds the CDC bookmark from a position captured before the
//! snapshot so the handoff has no gap (idempotent at the boundary under
//! `write_mode: upsert`).

use crate::auth_catalog::AuthCatalog;
use crate::config::{ExecutionSpec, PipelineConfig};
use crate::error::{CliError, CliResult};
use crate::executor::{ExecuteOptions, run_expanded};
use crate::expand::{ExpandedNode, expand};
use crate::registry::build_source;
use crate::replication::compiled::CompiledReplication;
use crate::replication::state::{
    Phase, Plan, ReplicationState, cdc_state_key, marker_key, plan_from_marker,
};
use crate::state::build_state_store;
use chrono::{DateTime, FixedOffset};
use tokio_util::sync::CancellationToken;

/// Inputs for one `faucet replicate` invocation.
pub struct ReplicationOptions {
    pub pipeline_name: String,
    pub execution: Option<ExecutionSpec>,
    pub auth: AuthCatalog,
    pub clock: DateTime<FixedOffset>,
}

/// Build the snapshot-phase node by cloning the CDC node and swapping in the
/// bulk-read source. The snapshot always runs at-least-once (the query source
/// is not exactly-once-capable; upsert makes re-runs idempotent).
pub(crate) fn build_snapshot_node(
    cdc_node: &ExpandedNode,
    snapshot_source: crate::config::ConnectorSpec,
) -> ExpandedNode {
    let mut n = cdc_node.clone();
    n.id = "snapshot".to_string();
    n.source = snapshot_source;
    n.delivery = faucet_core::DeliveryMode::AtLeastOnce;
    n
}

/// Build a fresh `ExecuteOptions` for one phase run.
fn make_opts(opts: &ReplicationOptions, cancel: Option<CancellationToken>) -> ExecuteOptions {
    ExecuteOptions {
        pipeline_name: opts.pipeline_name.clone(),
        execution: opts.execution.clone(),
        dry_run: false,
        limit: None,
        state_path_override: None,
        auth: opts.auth.clone(),
        clock: opts.clock,
        cancel,
        #[cfg(feature = "lineage")]
        lineage: None,
        #[cfg(feature = "lineage")]
        lineage_cfg: None,
    }
}

/// Spawn a task that cancels `token` on SIGTERM (Unix) or Ctrl-C.
fn spawn_cancel_on_signal(token: CancellationToken) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            match signal(SignalKind::terminate()) {
                Ok(mut sigterm) => {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {}
                        _ = sigterm.recv() => {}
                    }
                }
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        token.cancel();
    });
}

/// Run the two-phase replication. Validates, bootstraps (captures + seeds the
/// CDC position), snapshots (if not already done), then streams CDC (looping
/// until SIGTERM when `continuous`).
pub async fn run_replication(
    cfg: &PipelineConfig,
    compiled: &CompiledReplication,
    opts: ReplicationOptions,
) -> CliResult<()> {
    // expand() runs the generic gates (exactly-once, write_mode×sink). With no
    // matrix (enforced by CompiledReplication) there is exactly one node.
    let mut nodes = expand(cfg)?;
    let mut cdc_node = nodes
        .drain(..)
        .next()
        .ok_or_else(|| CliError::Internal("replication: expand produced no node".into()))?;
    cdc_node.id = "cdc".to_string();
    let snapshot_node = build_snapshot_node(&cdc_node, compiled.snapshot_source.clone());

    let state_spec = cfg
        .pipeline
        .state
        .as_ref()
        .ok_or_else(|| CliError::Config("replication requires a state store".into()))?;
    let store = build_state_store(state_spec).await?;

    let marker_k = marker_key(&opts.pipeline_name);
    let cdc_k = cdc_state_key(&opts.pipeline_name);

    let marker = match store.get(&marker_k).await? {
        Some(v) => Some(ReplicationState::from_value(v)?),
        None => None,
    };

    // ── Bootstrap: capture the CDC position before the snapshot ──────────────
    if plan_from_marker(marker.as_ref()) == Plan::Bootstrap {
        let cdc_source = build_source(
            &cdc_node.source.kind,
            cdc_node.source.config.clone(),
            &opts.auth,
        )
        .await?;
        let position = cdc_source.capture_resume_position().await?.ok_or_else(|| {
            CliError::Config(format!(
                "replication: source '{}' does not support position capture",
                cdc_node.source.kind
            ))
        })?;
        // Seed the CDC bookmark (bare value — exactly-once's unwrap_state reads a
        // bare value as seq=0, so this works for both delivery modes) and record
        // the phase marker.
        store.put(&cdc_k, &position).await?;
        store
            .put(
                &marker_k,
                &ReplicationState {
                    phase: Phase::Snapshot,
                    snapshot_done: false,
                    position: position.clone(),
                }
                .to_value()?,
            )
            .await?;
        tracing::info!(pipeline = %opts.pipeline_name, "replication bootstrap: captured CDC position, seeded bookmark");
    }

    // Re-read the marker (it now exists). Decide the remaining work.
    let marker = match store.get(&marker_k).await? {
        Some(v) => ReplicationState::from_value(v)?,
        None => {
            return Err(CliError::Internal(
                "replication: marker missing after bootstrap".into(),
            ));
        }
    };

    // ── Snapshot phase (idempotent redo on resume) ───────────────────────────
    if !marker.snapshot_done {
        tracing::info!(pipeline = %opts.pipeline_name, "replication: running snapshot phase");
        let summary = run_expanded(vec![snapshot_node.clone()], make_opts(&opts, None)).await?;
        if summary.had_failures() {
            return Err(CliError::PipelineHadFailures {
                count: summary.failure_count(),
            });
        }
        store
            .put(
                &marker_k,
                &ReplicationState {
                    phase: Phase::Cdc,
                    snapshot_done: true,
                    position: marker.position.clone(),
                }
                .to_value()?,
            )
            .await?;
        tracing::info!(pipeline = %opts.pipeline_name, "replication: snapshot complete; handing off to CDC");
    }

    // ── CDC phase (loop until SIGTERM when continuous) ───────────────────────
    let cancel = CancellationToken::new();
    if compiled.continuous {
        spawn_cancel_on_signal(cancel.clone());
        tracing::info!(pipeline = %opts.pipeline_name, "replication: streaming CDC (Ctrl-C / SIGTERM to stop)");
    }
    loop {
        let summary = run_expanded(
            vec![cdc_node.clone()],
            make_opts(&opts, Some(cancel.clone())),
        )
        .await?;
        if summary.had_failures() {
            return Err(CliError::PipelineHadFailures {
                count: summary.failure_count(),
            });
        }
        if !compiled.continuous || cancel.is_cancelled() {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectorSpec;
    use crate::expand::expand;

    fn cdc_node() -> ExpandedNode {
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: postgres-cdc, config: { connection_url: "postgres://x", slot_name: s, publication_name: p } }
  sink:   { type: postgres, config: { connection_url: "postgres://y", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id] } }
  state:  { type: file, config: { path: ./st } }
"#,
            "yaml",
        )
        .unwrap();
        expand(&cfg).unwrap().into_iter().next().unwrap()
    }

    #[test]
    fn snapshot_node_swaps_source_and_forces_at_least_once() {
        let mut cdc = cdc_node();
        cdc.id = "cdc".into();
        cdc.delivery = faucet_core::DeliveryMode::ExactlyOnce;
        let snap_src = ConnectorSpec {
            kind: "postgres".into(),
            config: serde_json::json!({ "connection_url": "postgres://x", "query": "SELECT * FROM t" }),
            transforms: None,
            inherit_transforms: true,
        };
        let node = build_snapshot_node(&cdc, snap_src);
        assert_eq!(node.id, "snapshot");
        assert_eq!(node.source.kind, "postgres");
        assert_eq!(node.sink.kind, "postgres"); // sink preserved
        assert_eq!(node.delivery, faucet_core::DeliveryMode::AtLeastOnce);
    }
}
