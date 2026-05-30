//! `faucet run` — load a pipeline config, expand the matrix, execute every
//! invocation under bounded concurrency.

use crate::cli::RunArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::executor::{ExecuteOptions, run_expanded};
use crate::expand::expand;
use faucet_core::{ObservabilityConfig, PrometheusConfig, TracingConfig, install_observability};

/// Resolve the effective `tracing-subscriber` env-filter directive using
/// the documented precedence:
///   `--log-level` flag > `FAUCET_LOG` > `RUST_LOG` > YAML `observability.tracing.level` > `None`
///
/// Returns `None` when nothing is set (caller falls back to the default).
///
/// Note: `cli_flag` is `None` when called from `run` because the top-level
/// `Cli.log_level` (already applied by `main.rs`) is not forwarded through
/// `RunArgs`. Pass `Some(level)` in tests or future call-sites that do have
/// the CLI value available.
fn resolve_tracing_level(cli_flag: Option<&str>, yaml_level: Option<&str>) -> Option<String> {
    if let Some(l) = cli_flag {
        return Some(l.to_string());
    }
    if let Ok(l) = std::env::var("FAUCET_LOG")
        && !l.is_empty()
    {
        return Some(l);
    }
    if let Ok(l) = std::env::var("RUST_LOG")
        && !l.is_empty()
    {
        return Some(l);
    }
    yaml_level.map(|s| s.to_string())
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

    // Install observability (Prometheus + tracing) before any pipeline work.
    // `main.rs` already called `install_tracing` from `Cli.log_level`, so
    // the tracing subscriber is likely already set; `install_observability`
    // is idempotent — it logs a warning and continues rather than panicking.
    //
    // Tracing-level precedence for the YAML-block tracing config:
    //   --log-level flag (in Cli, not in RunArgs) > FAUCET_LOG > RUST_LOG
    //   > YAML observability.tracing.level > None (main.rs default applies)
    let level = resolve_tracing_level(
        // `RunArgs` does not carry `log_level`; the top-level `Cli.log_level`
        // (already consumed in main.rs) is not forwarded here, so pass `None`.
        None,
        cfg.observability
            .as_ref()
            .and_then(|o| o.tracing.as_ref())
            .and_then(|t| t.level.as_deref()),
    );
    let obs_cfg = ObservabilityConfig {
        prometheus: cfg
            .observability
            .as_ref()
            .and_then(|o| o.prometheus.as_ref())
            .map(|p| PrometheusConfig {
                listen: p.listen.clone(),
                buckets: p.buckets.clone(),
            }),
        tracing: level.map(|l| TracingConfig { level: l }),
    };
    let report = install_observability(&obs_cfg)?;
    if let Some(addr) = report.prometheus_listen.as_deref() {
        tracing::info!("Prometheus /metrics listening on {addr}");
    }
    if report.prometheus_already_installed {
        tracing::warn!(
            "Prometheus recorder already installed; metrics route through the existing recorder"
        );
    }
    if report.tracing_already_installed {
        tracing::warn!(
            "tracing subscriber already installed; logs route through the existing subscriber"
        );
    }

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

    // The env tests share process-global state — serialize them via a mutex.
    use std::sync::Mutex;
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_clean_env<F: FnOnce()>(f: F) {
        let _g = ENV_LOCK.lock().unwrap();
        // Rust 2024: set_var/remove_var are unsafe (touch process-wide state).
        unsafe {
            std::env::remove_var("FAUCET_LOG");
            std::env::remove_var("RUST_LOG");
        }
        f();
    }

    #[test]
    fn cli_flag_beats_env_and_yaml() {
        with_clean_env(|| {
            unsafe {
                std::env::set_var("FAUCET_LOG", "debug");
                std::env::set_var("RUST_LOG", "trace");
            }
            assert_eq!(
                resolve_tracing_level(Some("error"), Some("info")).as_deref(),
                Some("error")
            );
        });
    }

    #[test]
    fn faucet_log_beats_rust_log_and_yaml() {
        with_clean_env(|| {
            unsafe {
                std::env::set_var("FAUCET_LOG", "debug");
                std::env::set_var("RUST_LOG", "trace");
            }
            assert_eq!(
                resolve_tracing_level(None, Some("info")).as_deref(),
                Some("debug")
            );
        });
    }

    #[test]
    fn rust_log_beats_yaml() {
        with_clean_env(|| {
            unsafe {
                std::env::set_var("RUST_LOG", "trace");
            }
            assert_eq!(
                resolve_tracing_level(None, Some("info")).as_deref(),
                Some("trace")
            );
        });
    }

    #[test]
    fn yaml_used_when_no_flag_or_env() {
        with_clean_env(|| {
            assert_eq!(
                resolve_tracing_level(None, Some("info")).as_deref(),
                Some("info")
            );
        });
    }

    #[test]
    fn none_returned_when_nothing_set() {
        with_clean_env(|| {
            assert_eq!(resolve_tracing_level(None, None), None);
        });
    }
}
