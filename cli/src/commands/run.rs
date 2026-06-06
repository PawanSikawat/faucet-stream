//! `faucet run` — load a pipeline config, expand the matrix, execute every
//! invocation under bounded concurrency.

use crate::cli::RunArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::executor::{ExecuteOptions, run_expanded};
use crate::expand::expand;

/// Parse the optional `--clock` override (RFC3339 or `YYYY-MM-DD`), or default
/// to process start. Returned as a UTC fixed-offset clock for `${now.*}`.
fn resolve_run_clock(flag: Option<&str>) -> CliResult<chrono::DateTime<chrono::FixedOffset>> {
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
        crate::env_config::from_process_env()?
    } else {
        PipelineConfig::from_path_async(
            resolved_config_path
                .as_ref()
                .expect("YAML mode always resolves a path above"),
        )
        .await?
    };

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
    let nodes = expand(&cfg)?;
    let summary = run_expanded(
        nodes,
        ExecuteOptions {
            pipeline_name: pipeline_name.clone(),
            execution: cfg.execution.clone(),
            dry_run: args.dry_run,
            limit: args.limit,
            state_path_override: args.state_path.clone(),
            auth,
            clock: resolve_run_clock(args.clock.as_deref())?,
            // `faucet run` has no external cancel signal; the executor still
            // cooperatively cancels in-flight rows on `on_error: stop`.
            cancel: None,
            // Lineage emitter is wired in Task 27 (`faucet run`); `None` here so
            // the literal compiles under the `lineage` feature meanwhile.
            #[cfg(feature = "lineage")]
            lineage: None,
            #[cfg(feature = "lineage")]
            lineage_cfg: None,
        },
    )
    .await?;

    let total_written: usize = summary.invocations.iter().map(|i| i.records_written).sum();
    let success = summary
        .invocations
        .iter()
        .filter(|i| i.error.is_none())
        .count();
    let failed = summary.failure_count();

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
