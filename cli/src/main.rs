//! `faucet` — binary entry point.

use clap::Parser;
use faucet_cli::cli::{Cli, Command};
use faucet_cli::commands;
use faucet_cli::error::CliError;
#[cfg(feature = "observability")]
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    #[cfg(feature = "serve")]
    let is_serve = matches!(cli.command, Command::Serve(_));
    #[cfg(not(feature = "serve"))]
    let is_serve = false;
    if !is_serve {
        install_tracing(&cli.log_level);
    }
    #[cfg(feature = "serve")]
    let serve_log_level = cli.log_level.clone();

    let result = match cli.command {
        Command::Run(args) => commands::run::run(args).await,
        Command::Replicate(args) => commands::replicate::run(args).await,
        Command::Validate(args) => commands::validate::run(args).await,
        Command::Schema(args) => commands::schema::run(args).await,
        Command::List => commands::list::run().await,
        Command::Preview(args) => commands::preview::run(args).await,
        Command::Init(args) => commands::init::run(args).await,
        Command::Doctor(args) => commands::doctor::run(args).await,
        Command::Test(args) => commands::test::run(args).await,
        #[cfg(feature = "contract")]
        Command::Contract(args) => commands::contract::run(args).await,
        #[cfg(feature = "schedule")]
        Command::Schedule(args) => commands::schedule::run(args).await,
        #[cfg(feature = "serve")]
        Command::Serve(args) => commands::serve::run(args, serve_log_level).await,
    };

    if let Err(err) = result {
        // `doctor` already printed its checklist; surface the failure count as
        // the exit code (clamped to 255) rather than the generic exit-1 path.
        if let CliError::DoctorFailed { failed } = &err {
            std::process::exit((*failed).min(255) as i32);
        }
        // Same shape for `test`: the report is already printed; the exit code
        // is the failed-case count (clamped to 255).
        if let CliError::TestsFailed { failed } = &err {
            std::process::exit((*failed).min(255) as i32);
        }
        commands::report(&err);
        std::process::exit(1);
    }
}

#[cfg(feature = "observability")]
fn install_tracing(level: &str) {
    use faucet_cli::secrets::registry::RedactingMakeWriter;
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(RedactingMakeWriter)
        .try_init();
}

/// Stub used when the `observability` feature is disabled. Logging falls
/// back to whatever the host environment has wired (or nothing).
#[cfg(not(feature = "observability"))]
fn install_tracing(_level: &str) {}
