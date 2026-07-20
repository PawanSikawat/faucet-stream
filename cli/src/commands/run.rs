//! `faucet run` — load a pipeline config, expand the matrix, execute every
//! invocation under bounded concurrency.

use crate::cli::RunArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::executor::{ExecuteOptions, run_expanded};
use crate::expand::expand;

/// Parse the optional `--clock` override (RFC3339 or `YYYY-MM-DD`), or default
/// to process start. Returned as a UTC fixed-offset clock for `${now.*}`.
/// Shared with `faucet test` (the `--clock` flag and per-case `clock:` field).
pub(crate) fn resolve_run_clock(
    flag: Option<&str>,
) -> CliResult<chrono::DateTime<chrono::FixedOffset>> {
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    match flag {
        None => Ok(Utc::now().fixed_offset()),
        Some(s) => {
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                return Ok(dt);
            }
            if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                let ndt = d.and_hms_opt(0, 0, 0).expect("00:00:00 is valid");
                return Ok(Utc.from_utc_datetime(&ndt).fixed_offset());
            }
            Err(CliError::Config(format!(
                "--clock '{s}' is not RFC3339 (2026-01-31T00:00:00Z) or a date (2026-01-31)"
            )))
        }
    }
}

/// Execute the `run` subcommand.
pub async fn run(args: RunArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let resolved_config_path: Option<std::path::PathBuf> = if args.from_env {
        None
    } else {
        Some(match args.config.as_ref() {
            Some(p) => p.clone(),
            None => {
                crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?
            }
        })
    };

    let cfg = if args.from_env {
        if args.profile.is_some() {
            tracing::warn!(
                "--profile / FAUCET_PROFILE has no effect in --from-env mode (no config file to compose); ignoring"
            );
        }
        crate::env_config::from_process_env()?
    } else {
        PipelineConfig::from_path_async(
            resolved_config_path
                .as_ref()
                .expect("YAML mode always resolves a path above"),
            args.profile.as_deref(),
        )
        .await?
    };

    #[cfg(not(feature = "cli-tui"))]
    if args.tui {
        return Err(CliError::Config(
            "--tui requires a binary built with the `cli-tui` feature \
             (e.g. `cargo install faucet-cli --features cli-tui`)"
                .into(),
        ));
    }
    #[cfg(feature = "cli-tui")]
    let tui_active = crate::tui::is_tui_session(args.tui);
    #[cfg(feature = "cli-tui")]
    let tui_handle = if tui_active {
        Some(crate::tui::setup_observability(&cfg)?)
    } else {
        if args.tui {
            tracing::info!("--tui: stdout is not a terminal; running without the TUI");
        }
        crate::obs::install(&cfg)?;
        None
    };
    #[cfg(not(feature = "cli-tui"))]
    crate::obs::install(&cfg)?;

    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        resolved_config_path
            .as_ref()
            .and_then(|p| p.file_stem())
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });

    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    #[cfg(feature = "lineage")]
    let lineage = crate::lineage_glue::build_emitter(cfg.lineage.as_ref())
        .map_err(|e| CliError::Config(format!("lineage: {e}")))?;
    let resilience = match &cfg.resilience {
        Some(spec) => Some(spec.to_policy()?),
        None => None,
    };
    #[cfg(feature = "notify")]
    let notifier = crate::notify::Notifier::from_specs(&cfg.notifications)?;
    #[cfg(feature = "catalog")]
    let catalog = match cfg.catalog.as_ref() {
        Some(spec) => Some(crate::catalog::connect_from_spec(spec).await?),
        None => None,
    };
    let nodes = expand(&cfg)?;
    // Build the config snapshot (#374) before `nodes`/`catalog` are moved into
    // the executor. Pure + cheap; recorded after a fully-successful run below.
    #[cfg(feature = "catalog")]
    let config_snapshot = catalog.as_ref().map(|_| {
        crate::catalog::snapshot::build_snapshot(
            pipeline_name.clone(),
            crate::catalog::snapshot::on_error_str(&cfg.execution),
            &nodes,
            chrono::Utc::now(),
        )
    });
    #[cfg(feature = "catalog")]
    let catalog_for_snapshot = catalog.clone();
    // The TUI wires `q` / Ctrl-C to this token: in-flight invocations stop at
    // their next page boundary and flush (#146 H16). Plain runs keep `None`.
    #[cfg(feature = "cli-tui")]
    let tui_cancel = tui_active.then(faucet_core::CancellationToken::new);
    #[cfg(not(feature = "cli-tui"))]
    let tui_cancel: Option<faucet_core::CancellationToken> = None;
    let run_fut = run_expanded(
        nodes,
        ExecuteOptions {
            pipeline_name: pipeline_name.clone(),
            execution: cfg.execution.clone(),
            dry_run: args.dry_run,
            limit: args.limit,
            state_path_override: args.state_path.clone(),
            shard: None,
            auth,
            clock: resolve_run_clock(args.clock.as_deref())?,
            // Plain runs have no external cancel signal (the executor still
            // cooperatively cancels in-flight rows on `on_error: stop`); a
            // TUI session cancels via `q` / Ctrl-C.
            cancel: tui_cancel.clone(),
            resilience,
            sla: cfg.sla.clone(),
            #[cfg(feature = "lineage")]
            lineage,
            #[cfg(feature = "lineage")]
            lineage_cfg: cfg.lineage.clone(),
            #[cfg(feature = "notify")]
            notifier,
            #[cfg(feature = "catalog")]
            catalog,
        },
    );
    #[cfg(feature = "cli-tui")]
    let summary = match (tui_handle, tui_cancel) {
        (Some(handle), Some(cancel)) => {
            let result = crate::tui::drive(run_fut, &pipeline_name, handle, cancel).await;
            if result
                .as_ref()
                .map(|s| s.failure_count() > 0)
                .unwrap_or(true)
            {
                // The failure context lived on the alternate screen — replay
                // the tail of the log ring to stderr now that it's gone.
                crate::tui::flush_logs_to_stderr(25);
            }
            result?
        }
        _ => run_fut.await?,
    };
    #[cfg(not(feature = "cli-tui"))]
    let summary = run_fut.await?;

    let total_written: usize = summary.invocations.iter().map(|i| i.records_written).sum();
    let success = summary
        .invocations
        .iter()
        .filter(|i| i.error.is_none())
        .count();
    let failed = summary.failure_count();

    // Record the resolved config snapshot for `faucet plan --diff` on a fully
    // successful run only (best-effort — never fails the run; #374 / #279).
    #[cfg(feature = "catalog")]
    if failed == 0
        && let (Some(handle), Some(snap)) = (catalog_for_snapshot, config_snapshot)
    {
        crate::catalog::record_config_snapshot(&handle, &snap).await;
    }

    tracing::info!(
        pipeline = %pipeline_name,
        invocations = summary.invocations.len(),
        succeeded = success,
        failed,
        records_written = total_written,
        "pipeline completed"
    );
    println!(
        "{}: {} invocation{}, {} ok, {} failed, wrote {} record{}",
        pipeline_name,
        summary.invocations.len(),
        if summary.invocations.len() == 1 {
            ""
        } else {
            "s"
        },
        success,
        failed,
        total_written,
        if total_written == 1 { "" } else { "s" }
    );

    // Flush any buffered OTLP telemetry before the process exits (no-op without
    // the `otel` feature). Done on both the success and failure exit paths.
    faucet_core::shutdown_otel();

    if summary.had_failures() {
        return Err(CliError::PipelineHadFailures { count: failed });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_clock_parses_rfc3339_date_and_defaults() {
        // RFC3339
        let c = resolve_run_clock(Some("2026-01-31T12:00:00Z")).unwrap();
        assert_eq!(c.format("%Y-%m-%d %H:%M").to_string(), "2026-01-31 12:00");
        // date-only → midnight UTC
        let c = resolve_run_clock(Some("2026-01-31")).unwrap();
        assert_eq!(c.format("%Y-%m-%d %H:%M").to_string(), "2026-01-31 00:00");
        // default = now (just assert it's Ok / recent year)
        let c = resolve_run_clock(None).unwrap();
        assert!(c.format("%Y").to_string().parse::<i32>().unwrap() >= 2025);
        // bad input errors
        assert!(resolve_run_clock(Some("not-a-date")).is_err());
    }
}
