//! Subcommand implementations. Each module owns its `run(...)` entry point
//! and stays free of clap so the integration tests can drive it directly.

pub mod init;
pub mod list;
pub mod preview;
pub mod run;
pub mod schema;
pub mod validate;

use crate::error::CliError;

/// Render any [`CliError`] to a single line on stderr.
pub fn report(err: &CliError) {
    eprintln!("error: {err}");
    let mut src = std::error::Error::source(err);
    while let Some(s) = src {
        eprintln!("  caused by: {s}");
        src = std::error::Error::source(s);
    }
}
