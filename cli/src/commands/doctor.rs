//! `faucet doctor` — preflight probes for every connector in a config (#126).
//!
//! Expands the config, builds each **root** invocation's source / sink / state
//! store, and runs their non-mutating `check()` probes concurrently (bounded by
//! a semaphore, each wrapped in a per-probe timeout). Prints a green/red
//! checklist (or `--json`) and exits with the number of failed probes
//! (clamped to 255).
//!
//! Child invocations are listed but not probed: their configs depend on parent
//! records that only exist at run time (same limitation as `faucet preview`).

use crate::auth_catalog::{AuthCatalog, build_auth_catalog};
use crate::cli::DoctorArgs;
use crate::config::{ConnectorSpec, PipelineConfig, StateStoreSpec};
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole, expand};
use crate::registry::{build_sink, build_source};
use crate::secrets::registry::redact;
use crate::state::build_state_store;
use faucet_core::check::{CheckContext, CheckReport, Probe, ProbeStatus};
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// A single probe enriched with the role + connector it came from. This is the
/// `--json` shape for each probe.
#[derive(Debug, Serialize)]
struct ProbeOut {
    role: &'static str,
    connector: String,
    name: &'static str,
    #[serde(flatten)]
    status: ProbeStatus,
    elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
}

impl ProbeOut {
    fn from_probe(role: &'static str, connector: String, p: Probe) -> Self {
        Self {
            role,
            connector,
            name: p.name,
            status: p.status,
            elapsed_ms: p.elapsed_ms,
            hint: p.hint,
        }
    }
}

/// One expanded invocation and its probes (the `--json` per-invocation shape).
#[derive(Debug, Serialize)]
struct InvocationOut {
    id: String,
    probes: Vec<ProbeOut>,
    // Connector kinds for the human header; not part of the JSON contract.
    #[serde(skip)]
    source_kind: String,
    #[serde(skip)]
    sink_kind: String,
}

/// Execute the `doctor` subcommand.
pub async fn run(args: DoctorArgs) -> CliResult<()> {
    let overall = Instant::now();
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };

    let t_cfg = Instant::now();
    let cfg = PipelineConfig::from_path_async(&path).await?;
    let cfg_ms = t_cfg.elapsed().as_millis();

    let nodes = expand(&cfg)?;
    let auth = build_auth_catalog(cfg.auth.as_ref())?;
    let ctx = CheckContext {
        timeout: Duration::from_secs(args.timeout_secs),
    };

    let roots: Vec<&ExpandedNode> = nodes
        .iter()
        .filter(|n| matches!(n.role, NodeRole::Root))
        .collect();
    let n_children = nodes.len() - roots.len();

    let permits = cfg
        .execution
        .as_ref()
        .and_then(|e| e.max_concurrent)
        .filter(|n| *n > 0)
        .unwrap_or(8);
    let sem = Arc::new(Semaphore::new(permits));

    let mut handles = Vec::with_capacity(roots.len());
    for node in &roots {
        let id = node.id.clone();
        let source = node.source.clone();
        let sink = node.sink.clone();
        let state = node.state.clone();
        let auth = auth.clone();
        let ctx = ctx.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore not closed");
            probe_invocation(id, source, sink, state, &auth, &ctx).await
        }));
    }

    let mut invocations = Vec::with_capacity(handles.len());
    for h in handles {
        invocations.push(h.await.expect("doctor probe task panicked"));
    }

    redact_invocations(&mut invocations);
    let (_passed, failed, _skipped) = tally(&invocations);

    if args.json {
        let v = build_json(&path, overall.elapsed().as_millis(), &invocations);
        println!(
            "{}",
            serde_json::to_string_pretty(&v).expect("doctor json serializes")
        );
    } else {
        render_human(cfg_ms, roots.len(), n_children, overall.elapsed(), &invocations);
    }

    if failed > 0 {
        return Err(CliError::DoctorFailed { failed });
    }
    Ok(())
}

/// Build the three connectors for one invocation and run their probes.
async fn probe_invocation(
    id: String,
    source: ConnectorSpec,
    sink: ConnectorSpec,
    state: Option<StateStoreSpec>,
    auth: &AuthCatalog,
    ctx: &CheckContext,
) -> InvocationOut {
    let mut probes = Vec::new();

    match build_source(&source.kind, source.config.clone(), auth).await {
        Ok(src) => probes.extend(collect_probes("source", src.connector_name(), ctx, src.check(ctx)).await),
        Err(e) => probes.push(construct_fail("source", &source.kind, &e)),
    }

    match build_sink(&sink.kind, sink.config.clone(), auth).await {
        Ok(snk) => probes.extend(collect_probes("sink", snk.connector_name(), ctx, snk.check(ctx)).await),
        Err(e) => probes.push(construct_fail("sink", &sink.kind, &e)),
    }

    if let Some(spec) = state {
        match build_state_store(&spec).await {
            Ok(st) => {
                probes.extend(collect_probes("state", &spec.kind, ctx, st.check(ctx)).await)
            }
            Err(e) => probes.push(construct_fail("state", &spec.kind, &e)),
        }
    }

    InvocationOut {
        id,
        probes,
        source_kind: source.kind,
        sink_kind: sink.kind,
    }
}

/// Run one connector's `check()` future under the timeout, mapping the report
/// (or an outer error / timeout) into role-tagged [`ProbeOut`]s.
async fn collect_probes(
    role: &'static str,
    connector: &str,
    ctx: &CheckContext,
    fut: impl std::future::Future<Output = Result<CheckReport, faucet_core::FaucetError>>,
) -> Vec<ProbeOut> {
    let start = Instant::now();
    let report = match tokio::time::timeout(ctx.timeout, fut).await {
        Err(_) => CheckReport::single(Probe::fail("timeout", start.elapsed(), "check timed out")),
        Ok(Ok(r)) => r,
        Ok(Err(e)) => CheckReport::single(Probe::fail("check", start.elapsed(), e.to_string())),
    };
    report
        .probes
        .into_iter()
        .map(|p| ProbeOut::from_probe(role, connector.to_string(), p))
        .collect()
}

/// A `construct` failure: the connector could not even be built from its config.
/// Accepts any `Display` error (build errors are `CliError`; the unit test uses
/// `FaucetError`).
fn construct_fail(role: &'static str, kind: &str, e: impl std::fmt::Display) -> ProbeOut {
    ProbeOut::from_probe(
        role,
        kind.to_string(),
        Probe::fail("construct", Duration::ZERO, e.to_string()),
    )
}

/// Scrub resolved secrets out of every probe `reason` / `hint`.
fn redact_invocations(invs: &mut [InvocationOut]) {
    for inv in invs.iter_mut() {
        for p in inv.probes.iter_mut() {
            match &mut p.status {
                ProbeStatus::Fail { reason } | ProbeStatus::Skip { reason } => {
                    *reason = redact(reason).into_owned();
                }
                ProbeStatus::Pass => {}
            }
            if let Some(h) = &mut p.hint {
                *h = redact(h).into_owned();
            }
        }
    }
}

/// Count (passed, failed, skipped) probes across all invocations.
fn tally(invs: &[InvocationOut]) -> (usize, usize, usize) {
    let (mut p, mut f, mut s) = (0usize, 0usize, 0usize);
    for inv in invs {
        for pr in &inv.probes {
            match pr.status {
                ProbeStatus::Pass => p += 1,
                ProbeStatus::Fail { .. } => f += 1,
                ProbeStatus::Skip { .. } => s += 1,
            }
        }
    }
    (p, f, s)
}

/// Build the `--json` envelope.
fn build_json(config: &Path, total_ms: u128, invs: &[InvocationOut]) -> serde_json::Value {
    let (passed, failed, skipped) = tally(invs);
    serde_json::json!({
        "config": config.display().to_string(),
        "invocations": invs,
        "summary": {
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "elapsed_ms": total_ms,
        }
    })
}

/// Render the human checklist to stdout.
fn render_human(
    cfg_ms: u128,
    n_roots: usize,
    n_children: usize,
    total: Duration,
    invs: &[InvocationOut],
) {
    println!("✓ Config parses and interpolates{:>34} ms", cfg_ms);
    println!(
        "✓ Matrix expands to {} invocation{}{:>22} skipped (children)",
        n_roots,
        if n_roots == 1 { "" } else { "s" },
        n_children
    );

    for inv in invs {
        println!();
        println!(
            "▸ Invocation {}  (source={}, sink={})",
            inv.id, inv.source_kind, inv.sink_kind
        );
        for p in &inv.probes {
            let (sym, extra) = match &p.status {
                ProbeStatus::Pass => ("✓", String::new()),
                ProbeStatus::Fail { reason } => ("✗", format!(" ({reason})")),
                ProbeStatus::Skip { reason } => ("•", format!(" (skip: {reason})")),
            };
            println!(
                "  {} {:6} [{}] {}{}{:>8} ms",
                sym, p.role, p.connector, p.name, extra, p.elapsed_ms
            );
            if let Some(hint) = &p.hint {
                println!("        hint: {hint}");
            }
        }
    }

    let (passed, failed, skipped) = tally(invs);
    println!();
    println!(
        "Summary: {passed} passed, {failed} failed, {skipped} skipped       total elapsed {:.1}s",
        total.as_secs_f64()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe_out(role: &'static str, name: &'static str, status: ProbeStatus) -> ProbeOut {
        ProbeOut {
            role,
            connector: "x".into(),
            name,
            status,
            elapsed_ms: 1,
            hint: None,
        }
    }

    fn inv(probes: Vec<ProbeOut>) -> InvocationOut {
        InvocationOut {
            id: "default".into(),
            probes,
            source_kind: "rest".into(),
            sink_kind: "stdout".into(),
        }
    }

    #[test]
    fn tally_counts_each_status() {
        let invs = vec![inv(vec![
            probe_out("source", "read", ProbeStatus::Pass),
            probe_out("sink", "auth", ProbeStatus::Fail { reason: "x".into() }),
            probe_out("state", "sentinel", ProbeStatus::Skip { reason: "n/a".into() }),
            probe_out("sink", "schema", ProbeStatus::Fail { reason: "y".into() }),
        ])];
        assert_eq!(tally(&invs), (1, 2, 1));
    }

    #[test]
    fn json_has_summary_and_invocations() {
        let invs = vec![inv(vec![probe_out("source", "read", ProbeStatus::Pass)])];
        let v = build_json(Path::new("pipeline.yaml"), 100, &invs);
        assert_eq!(v["config"], "pipeline.yaml");
        assert_eq!(v["invocations"][0]["id"], "default");
        assert_eq!(v["invocations"][0]["probes"][0]["role"], "source");
        assert_eq!(v["invocations"][0]["probes"][0]["status"], "pass");
        assert_eq!(v["summary"]["passed"], 1);
        assert_eq!(v["summary"]["failed"], 0);
        assert_eq!(v["summary"]["elapsed_ms"], 100);
    }

    #[test]
    fn redaction_scrubs_secret_in_reason_and_hint() {
        crate::secrets::registry::register("supersecretvalue");
        let mut invs = vec![inv(vec![ProbeOut {
            role: "sink",
            connector: "postgres".into(),
            name: "auth",
            status: ProbeStatus::Fail {
                reason: "login failed for supersecretvalue".into(),
            },
            elapsed_ms: 5,
            hint: Some("token supersecretvalue rejected".into()),
        }])];
        redact_invocations(&mut invs);
        if let ProbeStatus::Fail { reason } = &invs[0].probes[0].status {
            assert!(!reason.contains("supersecretvalue"), "reason not redacted: {reason}");
        } else {
            panic!("expected fail");
        }
        assert!(!invs[0].probes[0].hint.as_ref().unwrap().contains("supersecretvalue"));
    }

    #[test]
    fn construct_fail_is_a_fail_probe() {
        let e = faucet_core::FaucetError::Config("bad".into());
        let p = construct_fail("sink", "postgres", &e);
        assert_eq!(p.name, "construct");
        assert!(matches!(p.status, ProbeStatus::Fail { .. }));
        assert_eq!(p.connector, "postgres");
    }
}
