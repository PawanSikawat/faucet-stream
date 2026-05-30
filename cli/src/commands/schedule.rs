//! `faucet schedule` — cron-driven long-running pipeline runner.

use crate::cli::ScheduleArgs;
use crate::error::CliResult;

/// Execute the `schedule` subcommand. (Implemented in Task 9.)
pub async fn run(_args: ScheduleArgs) -> CliResult<()> {
    unimplemented!("faucet schedule — implemented in Task 9")
}
