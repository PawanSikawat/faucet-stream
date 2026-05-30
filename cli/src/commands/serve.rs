//! `faucet serve` — thin command layer: parse args into a `ServeConfig`, load
//! the startup `.env`, and hand off to `serve::run_server`.

use crate::cli::ServeArgs;
use crate::error::CliResult;
use crate::serve::ServeConfig;

pub async fn run(args: ServeArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let config = ServeConfig::from_args(args)?;
    crate::serve::run_server(config).await
}
