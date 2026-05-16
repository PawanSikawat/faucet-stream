//! `faucet` — binary entry point.

use clap::Parser;
use faucet_cli::cli::{Cli, Command};
use faucet_cli::commands;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    install_tracing(&cli.log_level);

    let result = match cli.command {
        Command::Run(args) => commands::run::run(args).await,
        Command::Validate(args) => commands::validate::run(args).await,
        Command::Schema(args) => commands::schema::run(args).await,
        Command::List => commands::list::run().await,
        Command::Preview(args) => commands::preview::run(args).await,
        Command::Init(args) => commands::init::run(args).await,
    };

    if let Err(err) = result {
        commands::report(&err);
        std::process::exit(1);
    }
}

fn install_tracing(level: &str) {
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}
