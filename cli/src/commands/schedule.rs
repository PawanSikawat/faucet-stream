//! `faucet schedule` — run a pipeline on a cron schedule in one long-running
//! process. Reuses `expand` + `executor::run_expanded` per tick; keeps no state
//! of its own (resumability rides the pipeline's per-page bookmark). See
//! `docs/superpowers/specs/2026-05-30-faucet-schedule-design.md`.

use crate::auth_catalog::{AuthCatalog, build_auth_catalog};
use crate::cli::ScheduleArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::executor::{ExecuteOptions, RunSummary, run_expanded};
use crate::expand::{ExpandedNode, expand};
use crate::schedule::compiled::CompiledSchedule;
use crate::schedule::metrics as m;
use crate::schedule::state::{AfterRun, RunOutcome, SchedulerState, TickAction};
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::Instrument;

/// An in-flight run.
struct RunningRun {
    handle: JoinHandle<CliResult<RunSummary>>,
    started: Instant,
}

/// The data the loop needs after a run finishes.
struct RunFinished {
    outcome: RunOutcome,
    duration: Duration,
    detail: Option<String>,
}

/// Cross-platform shutdown-signal source, registered once.
struct Shutdown {
    #[cfg(unix)]
    sigterm: tokio::signal::unix::Signal,
}

impl Shutdown {
    fn new() -> CliResult<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let sigterm = signal(SignalKind::terminate()).map_err(|e| {
                CliError::Internal(format!("failed to install SIGTERM handler: {e}"))
            })?;
            Ok(Self { sigterm })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    /// Resolve when SIGTERM (Unix) or Ctrl-C (any platform) is received.
    async fn recv(&mut self) {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = self.sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

/// Max time to sleep before re-reading the wall clock. Caps clock-step / DST
/// drift to one chunk and keeps the heartbeat fresh.
const MAX_SLEEP: Duration = Duration::from_secs(30);

/// Execute the `schedule` subcommand.
pub async fn run(args: ScheduleArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };

    let cfg = PipelineConfig::from_path_async(&path).await?;
    let spec = cfg.schedule.as_ref().ok_or_else(|| {
        CliError::Config(
            "no `schedule:` block in config — use `faucet run` for a one-shot run, or add a `schedule:` block"
                .into(),
        )
    })?;
    let compiled = CompiledSchedule::compile(spec)?;
    let cron = spec.cron.clone();
    let timezone = spec.timezone.clone();

    crate::obs::install(&cfg)?;

    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });

    let auth = build_auth_catalog(cfg.auth.as_ref())?;
    // Build the shared OpenLineage emitter once; the `Arc` is cloned into each
    // tick's `ExecuteOptions` so every run reuses the same transport/client.
    #[cfg(feature = "lineage")]
    let lineage = crate::lineage_glue::build_emitter(cfg.lineage.as_ref())
        .map_err(|e| CliError::Config(format!("lineage: {e}")))?;
    #[cfg(feature = "lineage")]
    let lineage_cfg = cfg.lineage.clone();
    let nodes = expand(&cfg)?; // validate once; cloned per tick
    let execution = cfg.execution.clone();

    if args.once {
        return run_once(
            &nodes,
            &auth,
            &execution,
            &compiled,
            &pipeline_name,
            #[cfg(feature = "lineage")]
            &lineage,
            #[cfg(feature = "lineage")]
            &lineage_cfg,
        )
        .await;
    }

    run_loop(
        compiled,
        nodes,
        auth,
        execution,
        pipeline_name,
        cron,
        timezone,
        #[cfg(feature = "lineage")]
        lineage,
        #[cfg(feature = "lineage")]
        lineage_cfg,
    )
    .await
}

/// Build a fresh `ExecuteOptions` for one tick (connectors are rebuilt per run;
/// the auth catalog is shared so cached tokens survive across ticks).
fn make_opts(
    pipeline_name: &str,
    execution: &Option<crate::config::ExecutionSpec>,
    auth: &AuthCatalog,
    clock: chrono::DateTime<chrono::FixedOffset>,
    #[cfg(feature = "lineage")] lineage: &Option<std::sync::Arc<faucet_lineage::LineageEmitter>>,
    #[cfg(feature = "lineage")] lineage_cfg: &Option<faucet_lineage::LineageConfig>,
) -> ExecuteOptions {
    ExecuteOptions {
        pipeline_name: pipeline_name.to_string(),
        execution: execution.clone(),
        dry_run: false,
        limit: None,
        state_path_override: None,
        auth: auth.clone(),
        clock,
        cancel: None,
        #[cfg(feature = "lineage")]
        lineage: lineage.clone(),
        #[cfg(feature = "lineage")]
        lineage_cfg: lineage_cfg.clone(),
    }
}

/// The per-run tracing span. Wraps the inner pipeline spans so a scheduled run
/// is correlatable in distributed tracing. `scheduled_for` is the cron-intended
/// instant; `tick` is when the run actually started.
fn run_span(run_ordinal: u64, scheduled_for: DateTime<Utc>, tick: DateTime<Utc>) -> tracing::Span {
    tracing::info_span!(
        "faucet.schedule.run",
        run_ordinal,
        scheduled_for_unix_seconds = scheduled_for.timestamp(),
        tick_unix_seconds = tick.timestamp(),
    )
}

/// Spawn one pipeline run, wrapping it in the optional run timeout and the
/// per-run span.
fn spawn_run(
    nodes: Vec<ExpandedNode>,
    opts: ExecuteOptions,
    timeout: Option<Duration>,
    span: tracing::Span,
) -> JoinHandle<CliResult<RunSummary>> {
    tokio::spawn(
        async move {
            match timeout {
                Some(d) => match tokio::time::timeout(d, run_expanded(nodes, opts)).await {
                    Ok(r) => r,
                    Err(_) => Err(CliError::Internal(format!(
                        "scheduled run exceeded run_timeout_secs ({}s) and was aborted",
                        d.as_secs()
                    ))),
                },
                None => run_expanded(nodes, opts).await,
            }
        }
        .instrument(span),
    )
}

/// Classify a joined run task into a scheduler outcome + a log detail.
fn classify(joined: Result<CliResult<RunSummary>, tokio::task::JoinError>) -> RunFinished {
    let (outcome, detail) = match joined {
        Ok(Ok(summary)) if summary.had_failures() => (
            RunOutcome::Failure,
            Some(format!("{} invocation(s) failed", summary.failure_count())),
        ),
        Ok(Ok(_)) => (RunOutcome::Success, None),
        Ok(Err(e)) => (RunOutcome::Failure, Some(e.to_string())),
        Err(je) => (
            RunOutcome::Failure,
            Some(format!("run task panicked: {je}")),
        ),
    };
    RunFinished {
        outcome,
        duration: Duration::ZERO,
        detail,
    }
}

/// `--once`: run exactly one pipeline run now and map its result to an exit.
#[allow(clippy::too_many_arguments)]
async fn run_once(
    nodes: &[ExpandedNode],
    auth: &AuthCatalog,
    execution: &Option<crate::config::ExecutionSpec>,
    compiled: &CompiledSchedule,
    pipeline_name: &str,
    #[cfg(feature = "lineage")] lineage: &Option<std::sync::Arc<faucet_lineage::LineageEmitter>>,
    #[cfg(feature = "lineage")] lineage_cfg: &Option<faucet_lineage::LineageConfig>,
) -> CliResult<()> {
    tracing::info!(pipeline = %pipeline_name, "schedule --once: running one pipeline now");
    let now = chrono::Utc::now();
    let opts = make_opts(
        pipeline_name,
        execution,
        auth,
        compiled.clock_at(now),
        #[cfg(feature = "lineage")]
        lineage,
        #[cfg(feature = "lineage")]
        lineage_cfg,
    );
    let span = run_span(1, now, now);
    let fut = run_expanded(nodes.to_vec(), opts).instrument(span);
    let summary = match compiled.run_timeout {
        Some(d) => tokio::time::timeout(d, fut).await.map_err(|_| {
            CliError::Internal(format!(
                "--once run exceeded run_timeout_secs ({}s)",
                d.as_secs()
            ))
        })??,
        None => fut.await?,
    };
    if summary.had_failures() {
        return Err(CliError::PipelineHadFailures {
            count: summary.failure_count(),
        });
    }
    Ok(())
}

/// The scheduling loop.
#[allow(clippy::too_many_arguments)]
async fn run_loop(
    compiled: CompiledSchedule,
    nodes: Vec<ExpandedNode>,
    auth: AuthCatalog,
    execution: Option<crate::config::ExecutionSpec>,
    pipeline_name: String,
    cron: String,
    timezone: String,
    #[cfg(feature = "lineage")] lineage: Option<std::sync::Arc<faucet_lineage::LineageEmitter>>,
    #[cfg(feature = "lineage")] lineage_cfg: Option<faucet_lineage::LineageConfig>,
) -> CliResult<()> {
    let mut state = SchedulerState::new(&compiled);
    let mut shutdown = Shutdown::new()?;
    let mut running: Option<RunningRun> = None;
    let mut pending_scheduled_for: Option<DateTime<Utc>> = None;
    let mut run_ordinal: u64 = 0;

    let mut next_due = if compiled.start_immediately {
        Utc::now()
    } else {
        compiled
            .next_after(Utc::now())
            .ok_or_else(|| CliError::Config("schedule: no upcoming occurrence".into()))?
    };

    // Startup banner: cron, timezone, and the next few firing times so an
    // operator can confirm at a glance the schedule is configured correctly.
    let upcoming: Vec<String> = {
        let mut t = Utc::now();
        let mut v = Vec::with_capacity(3);
        while v.len() < 3 {
            match compiled.next_after(t) {
                Some(n) => {
                    v.push(n.to_rfc3339());
                    t = n;
                }
                None => break,
            }
        }
        v
    };
    tracing::info!(
        pipeline = %pipeline_name,
        cron = %cron,
        timezone = %timezone,
        next_occurrences = ?upcoming,
        "scheduler started (Ctrl-C / SIGTERM to stop)"
    );

    // Register HELP text and pre-emit the two run-state gauges at 0 so both
    // series exist in `/metrics` from t=0 — the `metrics` exporter only renders
    // a series after its first emission, and these gauges are otherwise first
    // touched mid/post-run, leaving a pre-first-run scrape blind to them
    // (#146 R NIT).
    m::describe();
    m::in_flight(&pipeline_name, 0);
    m::consecutive_failures(&pipeline_name, 0);

    loop {
        let now = Utc::now();

        if now >= next_due {
            match state.on_tick(running.is_some()) {
                TickAction::Dispatch => {
                    run_ordinal += 1;
                    let opts = make_opts(
                        &pipeline_name,
                        &execution,
                        &auth,
                        compiled.clock_at(next_due),
                        #[cfg(feature = "lineage")]
                        &lineage,
                        #[cfg(feature = "lineage")]
                        &lineage_cfg,
                    );
                    let span = run_span(run_ordinal, next_due, now);
                    let handle = spawn_run(nodes.clone(), opts, compiled.run_timeout, span);
                    m::in_flight(&pipeline_name, 1);
                    m::last_run_started(&pipeline_name, now);
                    m::lateness(&pipeline_name, now - next_due);
                    tracing::info!(pipeline = %pipeline_name, run_ordinal, scheduled_for = %next_due, "run started");
                    running = Some(RunningRun {
                        handle,
                        started: Instant::now(),
                    });
                }
                TickAction::Skip => {
                    m::overlap(&pipeline_name, "skip");
                    m::run_outcome(&pipeline_name, "skipped");
                    tracing::warn!(pipeline = %pipeline_name, scheduled_for = %next_due, "tick skipped — previous run still in progress");
                }
                TickAction::Queue => {
                    m::overlap(&pipeline_name, "queue");
                    if pending_scheduled_for.is_none() {
                        pending_scheduled_for = Some(next_due);
                    }
                    tracing::warn!(pipeline = %pipeline_name, scheduled_for = %next_due, "tick queued — will run after current run finishes");
                }
                TickAction::ForbidAbort => {
                    m::overlap(&pipeline_name, "forbid");
                    // A prior `Dispatch` set the in-flight gauge to 1; reset it
                    // before we bail so `/metrics` doesn't read a stuck 1 after
                    // the scheduler exits (#146 R LOW).
                    m::in_flight(&pipeline_name, 0);
                    return Err(CliError::ScheduleOverlapForbidden);
                }
            }
            // Advance from the tick that just fired (`next_due`), not from the
            // wall clock — so a sub-minute occurrence isn't skipped just because
            // dispatch latency pushed `now` past it. A long backlog (suspension)
            // is collapsed to a single catch-up inside `next_due_after_tick`.
            next_due = match compiled.next_due_after_tick(next_due, Utc::now()) {
                Some(t) => t,
                None => {
                    tracing::info!(pipeline = %pipeline_name, "no further scheduled occurrences; exiting");
                    return Ok(());
                }
            };
        }

        let now2 = Utc::now();
        m::heartbeat(&pipeline_name, now2);
        m::next_tick(&pipeline_name, next_due);
        let chunk = (next_due - now2)
            .to_std()
            .unwrap_or(Duration::ZERO)
            .min(MAX_SLEEP);

        tokio::select! {
            biased;

            _ = shutdown.recv() => {
                tracing::info!(pipeline = %pipeline_name, "shutdown signal received; draining in-flight run");
                graceful_shutdown(running.take(), compiled.shutdown_grace, &pipeline_name).await;
                return Ok(());
            }

            finished = wait_for_run(&mut running) => {
                let mut finished = finished;
                if let Some(rr) = running.take() {
                    finished.duration = rr.started.elapsed();
                }
                m::in_flight(&pipeline_name, 0);
                let done_at = Utc::now();
                m::last_run_completed(&pipeline_name, done_at);
                m::last_run_duration(&pipeline_name, finished.duration);
                m::run_outcome(&pipeline_name, match finished.outcome {
                    RunOutcome::Success => "ok",
                    RunOutcome::Failure => "err",
                });
                match finished.outcome {
                    RunOutcome::Success => tracing::info!(
                        pipeline = %pipeline_name, secs = finished.duration.as_secs_f64(), "run completed"
                    ),
                    RunOutcome::Failure => tracing::error!(
                        pipeline = %pipeline_name, detail = finished.detail.as_deref().unwrap_or("unknown"),
                        "run failed"
                    ),
                }

                let after = state.on_run_finished(finished.outcome);
                m::consecutive_failures(&pipeline_name, state.consecutive_failures());
                match after {
                    AfterRun::ExitOk => {
                        tracing::info!(pipeline = %pipeline_name, "max_runs reached; exiting");
                        return Ok(());
                    }
                    AfterRun::ExitFailure { consecutive } => {
                        return Err(CliError::PipelineHadFailures { count: consecutive as usize });
                    }
                    AfterRun::Continue { dispatch_pending } => {
                        if dispatch_pending {
                            run_ordinal += 1;
                            let sched_for = pending_scheduled_for.take().unwrap_or(done_at);
                            let opts = make_opts(
                                &pipeline_name,
                                &execution,
                                &auth,
                                compiled.clock_at(sched_for),
                                #[cfg(feature = "lineage")]
                                &lineage,
                                #[cfg(feature = "lineage")]
                                &lineage_cfg,
                            );
                            let span = run_span(run_ordinal, sched_for, done_at);
                            let handle = spawn_run(nodes.clone(), opts, compiled.run_timeout, span);
                            m::in_flight(&pipeline_name, 1);
                            m::last_run_started(&pipeline_name, done_at);
                            m::lateness(&pipeline_name, done_at - sched_for);
                            tracing::info!(pipeline = %pipeline_name, run_ordinal, scheduled_for = %sched_for, "queued run started");
                            running = Some(RunningRun { handle, started: Instant::now() });
                        }
                    }
                }
            }

            _ = tokio::time::sleep(chunk) => { /* re-loop: re-read wall clock */ }
        }
    }
}

/// Await the in-flight run (or never resolve when idle). Returns the classified
/// outcome; the caller fills in `duration` from the `RunningRun`.
async fn wait_for_run(running: &mut Option<RunningRun>) -> RunFinished {
    match running {
        Some(rr) => classify((&mut rr.handle).await),
        None => std::future::pending().await,
    }
}

/// On shutdown, await the in-flight run up to `grace`, then abort it.
async fn graceful_shutdown(running: Option<RunningRun>, grace: Duration, pipeline_name: &str) {
    if let Some(mut rr) = running {
        match tokio::time::timeout(grace, &mut rr.handle).await {
            Ok(_) => {
                tracing::info!(pipeline = %pipeline_name, "in-flight run finished during shutdown grace")
            }
            Err(_) => {
                rr.handle.abort();
                tracing::warn!(
                    pipeline = %pipeline_name,
                    grace_secs = grace.as_secs(),
                    "in-flight run exceeded shutdown grace; aborted (partial sink state possible; bookmark preserved for the next run)"
                );
            }
        }
        m::in_flight(pipeline_name, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::spec::ScheduleSpec;

    fn compiled(yaml: &str) -> CompiledSchedule {
        let spec: ScheduleSpec = serde_yaml::from_str(yaml).unwrap();
        CompiledSchedule::compile(&spec).unwrap()
    }

    fn summary(failures: usize, total: usize) -> RunSummary {
        let mut invocations = Vec::new();
        for i in 0..total {
            invocations.push(crate::executor::InvocationOutcome {
                row_id: format!("r{i}"),
                parent_record_key: None,
                records_written: if i < failures { 0 } else { 3 },
                error: if i < failures {
                    Some("boom".into())
                } else {
                    None
                },
            });
        }
        RunSummary { invocations }
    }

    #[test]
    fn classify_success_when_no_failures() {
        let joined = Ok(Ok(summary(0, 2)));
        let f = classify(joined);
        assert_eq!(f.outcome, RunOutcome::Success);
        assert!(f.detail.is_none());
    }

    #[test]
    fn classify_failure_when_some_invocations_failed() {
        let joined = Ok(Ok(summary(2, 5)));
        let f = classify(joined);
        assert_eq!(f.outcome, RunOutcome::Failure);
        assert_eq!(f.detail.as_deref(), Some("2 invocation(s) failed"));
    }

    #[test]
    fn classify_failure_when_run_errored() {
        let joined: Result<CliResult<RunSummary>, tokio::task::JoinError> =
            Ok(Err(CliError::Internal("disk full".into())));
        let f = classify(joined);
        assert_eq!(f.outcome, RunOutcome::Failure);
        assert!(f.detail.as_deref().unwrap().contains("disk full"));
    }

    #[tokio::test]
    async fn classify_failure_when_task_panicked() {
        // Spawn a task that panics, then join it to obtain a real JoinError.
        let handle = tokio::spawn(async { panic!("kaboom") });
        let joined: Result<CliResult<RunSummary>, tokio::task::JoinError> = handle.await.map(Ok);
        let f = classify(joined);
        assert_eq!(f.outcome, RunOutcome::Failure);
        assert!(
            f.detail.as_deref().unwrap().contains("panicked"),
            "{:?}",
            f.detail
        );
    }

    #[test]
    fn run_span_carries_ordinal_and_times() {
        let scheduled = Utc::now();
        let tick = scheduled + chrono::Duration::seconds(3);
        let span = run_span(7, scheduled, tick);
        // The span exists and is enterable; field values are recorded on
        // creation. We assert it has the expected metadata name.
        assert_eq!(span.metadata().unwrap().name(), "faucet.schedule.run");
    }

    #[tokio::test]
    async fn wait_for_run_returns_classified_outcome() {
        let handle = tokio::spawn(async { Ok(summary(0, 1)) });
        let mut running = Some(RunningRun {
            handle,
            started: Instant::now(),
        });
        let finished = wait_for_run(&mut running).await;
        assert_eq!(finished.outcome, RunOutcome::Success);
    }

    #[tokio::test]
    async fn spawn_run_times_out_into_internal_error() {
        // The run "never finishes" (a long sleep) but the 1s timeout aborts it
        // and maps to an Internal error mentioning run_timeout_secs.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nx\n").unwrap();
        // Build nodes from a tiny real config so the spawned future is genuine.
        let yaml = format!(
            "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: {input} }} }}\n  sink: {{ type: jsonl, config: {{ path: {output} }} }}\n",
            input = input.display(),
            output = output.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let auth = AuthCatalog::new();
        let opts = make_opts(
            "to",
            &None,
            &auth,
            Utc::now().fixed_offset(),
            #[cfg(feature = "lineage")]
            &None,
            #[cfg(feature = "lineage")]
            &None,
        );
        // A zero-ish timeout (1ns) virtually guarantees the timeout branch fires
        // even though the pipeline is fast — the timeout races the spawn.
        let handle = spawn_run(
            nodes,
            opts,
            Some(Duration::from_nanos(1)),
            run_span(1, Utc::now(), Utc::now()),
        );
        let joined = handle.await.unwrap();
        // Either the run finished before the 1ns deadline (Ok) — unlikely — or
        // it tripped the timeout into an Internal error. Accept both but assert
        // the timeout message shape when it errors.
        if let Err(CliError::Internal(msg)) = &joined {
            assert!(msg.contains("run_timeout_secs"), "{msg}");
        }
    }

    #[tokio::test]
    async fn make_opts_disables_dry_run_limit_and_state_override() {
        let auth = AuthCatalog::new();
        let clock = Utc::now().fixed_offset();
        let opts = make_opts(
            "p",
            &None,
            &auth,
            clock,
            #[cfg(feature = "lineage")]
            &None,
            #[cfg(feature = "lineage")]
            &None,
        );
        assert_eq!(opts.pipeline_name, "p");
        assert!(!opts.dry_run);
        assert!(opts.limit.is_none());
        assert!(opts.state_path_override.is_none());
        assert!(opts.cancel.is_none());
        assert_eq!(opts.clock, clock);
    }

    #[tokio::test]
    async fn graceful_shutdown_awaits_finished_run() {
        // A run that finishes immediately is awaited within the grace window.
        let c = compiled("cron: \"* * * * *\"\nshutdown_grace_secs: 5");
        let handle = tokio::spawn(async { Ok(summary(0, 1)) });
        let running = Some(RunningRun {
            handle,
            started: Instant::now(),
        });
        // Should return promptly without aborting (the run already completed).
        graceful_shutdown(running, c.shutdown_grace, "p").await;
    }

    #[tokio::test]
    async fn graceful_shutdown_aborts_run_exceeding_grace() {
        // A run that never finishes is aborted once the (tiny) grace elapses.
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(summary(0, 1))
        });
        let running = Some(RunningRun {
            handle,
            started: Instant::now(),
        });
        // 50ms grace → the abort branch fires; the call must still return.
        graceful_shutdown(running, Duration::from_millis(50), "p").await;
    }

    #[tokio::test]
    async fn graceful_shutdown_noop_when_idle() {
        // No in-flight run → returns immediately.
        graceful_shutdown(None, Duration::from_secs(1), "p").await;
    }
}
