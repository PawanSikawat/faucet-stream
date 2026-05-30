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
            let sigterm = signal(SignalKind::terminate())
                .map_err(|e| CliError::Internal(format!("failed to install SIGTERM handler: {e}")))?;
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
    let nodes = expand(&cfg)?; // validate once; cloned per tick
    let execution = cfg.execution.clone();

    if args.once {
        return run_once(&nodes, &auth, &execution, &compiled, &pipeline_name).await;
    }

    run_loop(compiled, nodes, auth, execution, pipeline_name, cron, timezone).await
}

/// Build a fresh `ExecuteOptions` for one tick (connectors are rebuilt per run;
/// the auth catalog is shared so cached tokens survive across ticks).
fn make_opts(
    pipeline_name: &str,
    execution: &Option<crate::config::ExecutionSpec>,
    auth: &AuthCatalog,
) -> ExecuteOptions {
    ExecuteOptions {
        pipeline_name: pipeline_name.to_string(),
        execution: execution.clone(),
        dry_run: false,
        limit: None,
        state_path_override: None,
        auth: auth.clone(),
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
        Err(je) => (RunOutcome::Failure, Some(format!("run task panicked: {je}"))),
    };
    RunFinished { outcome, duration: Duration::ZERO, detail }
}

/// `--once`: run exactly one pipeline run now and map its result to an exit.
async fn run_once(
    nodes: &[ExpandedNode],
    auth: &AuthCatalog,
    execution: &Option<crate::config::ExecutionSpec>,
    compiled: &CompiledSchedule,
    pipeline_name: &str,
) -> CliResult<()> {
    tracing::info!(pipeline = %pipeline_name, "schedule --once: running one pipeline now");
    let opts = make_opts(pipeline_name, execution, auth);
    let fut = run_expanded(nodes.to_vec(), opts);
    let summary = match compiled.run_timeout {
        Some(d) => tokio::time::timeout(d, fut)
            .await
            .map_err(|_| {
                CliError::Internal(format!("--once run exceeded run_timeout_secs ({}s)", d.as_secs()))
            })??,
        None => fut.await?,
    };
    if summary.had_failures() {
        return Err(CliError::PipelineHadFailures { count: summary.failure_count() });
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

    loop {
        let now = Utc::now();

        if now >= next_due {
            match state.on_tick(running.is_some()) {
                TickAction::Dispatch => {
                    run_ordinal += 1;
                    let opts = make_opts(&pipeline_name, &execution, &auth);
                    let span = run_span(run_ordinal, next_due, now);
                    let handle = spawn_run(nodes.clone(), opts, compiled.run_timeout, span);
                    m::in_flight(&pipeline_name, 1);
                    m::last_run_started(&pipeline_name, now);
                    m::lateness(&pipeline_name, now - next_due);
                    tracing::info!(pipeline = %pipeline_name, run_ordinal, scheduled_for = %next_due, "run started");
                    running = Some(RunningRun { handle, started: Instant::now() });
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
                    return Err(CliError::ScheduleOverlapForbidden);
                }
            }
            next_due = match compiled.next_after(Utc::now()) {
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
        let chunk = (next_due - now2).to_std().unwrap_or(Duration::ZERO).min(MAX_SLEEP);

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
                            let opts = make_opts(&pipeline_name, &execution, &auth);
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
            Ok(_) => tracing::info!(pipeline = %pipeline_name, "in-flight run finished during shutdown grace"),
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
