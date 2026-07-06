//! `faucet notify test` — fire one synthetic event through a config's
//! `notifications:` rules to validate channel setup end-to-end (no pipeline
//! runs). Uses the real delivery path, so a Slack/PagerDuty/webhook that is
//! reachable will actually receive the test message.

use crate::cli::{NotifyArgs, NotifyCommand, NotifyTestArgs};
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::notify::{NotifyEvent, Notifier};

/// Execute the `notify` subcommand.
pub async fn run(args: NotifyArgs) -> CliResult<()> {
    match args.command {
        NotifyCommand::Test(a) => test(a).await,
    }
}

async fn test(args: NotifyTestArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    // Real load (resolves secrets) so channel credentials are live for delivery.
    let cfg = PipelineConfig::from_path_async(&path, None).await?;
    if cfg.notifications.is_empty() {
        return Err(CliError::Config(
            "no `notifications:` block in this config — add one, or run \
             `faucet schema notifications` to see the block's JSON Schema"
                .to_string(),
        ));
    }

    let notifier = Notifier::from_specs(&cfg.notifications)?
        .expect("a non-empty notifications list yields Some(notifier)");
    let pipeline = cfg
        .name
        .clone()
        .unwrap_or_else(|| "faucet-notify-test".to_string());
    let event = synth_event(&args.event, &pipeline)?;

    println!(
        "Firing synthetic `{}` event through {} notification rule(s)…",
        args.event,
        cfg.notifications.len()
    );
    notifier.emit(event).await;
    println!("Done — check your channels. Any delivery failure was logged above.");
    Ok(())
}

/// Build a synthetic event for the requested kind. DLQ uses a large count so it
/// clears any configured `dlq_threshold`.
fn synth_event(kind: &str, pipeline: &str) -> CliResult<NotifyEvent> {
    Ok(match kind {
        "run_failure" => NotifyEvent::run_failure(
            pipeline,
            "",
            "test",
            "synthetic test failure from `faucet notify test`",
        ),
        "run_success" => NotifyEvent::run_success(pipeline, "", 0),
        "sla_breach" => NotifyEvent::sla_breach(pipeline, "", "staleness", "synthetic SLA breach"),
        "circuit_open" => NotifyEvent::circuit_open(pipeline, "", 5, 30),
        "contract_abort" => NotifyEvent::contract_abort(pipeline, "", "synthetic contract breach"),
        "dlq_threshold" => NotifyEvent::dlq_threshold(pipeline, "", 1_000_000),
        "scheduler_stuck" => NotifyEvent::scheduler_stuck(pipeline, "synthetic scheduler-stuck"),
        other => {
            return Err(CliError::Config(format!(
                "unknown --event `{other}` (expected one of: run_failure, run_success, \
                 sla_breach, circuit_open, contract_abort, dlq_threshold, scheduler_stuck)"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synth_event_maps_known_kinds() {
        use crate::notify::EventKind;
        assert_eq!(
            synth_event("run_failure", "p").unwrap().kind,
            EventKind::RunFailure
        );
        assert_eq!(
            synth_event("scheduler_stuck", "p").unwrap().kind,
            EventKind::SchedulerStuck
        );
        // DLQ synthetic count clears any reasonable threshold.
        assert_eq!(
            synth_event("dlq_threshold", "p")
                .unwrap()
                .details
                .get("records_dlq")
                .and_then(|v| v.as_u64()),
            Some(1_000_000)
        );
    }

    #[test]
    fn synth_event_rejects_unknown_kind() {
        assert!(synth_event("nope", "p").is_err());
    }
}
