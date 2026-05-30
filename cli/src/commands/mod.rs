//! Subcommand implementations. Each module owns its `run(...)` entry point
//! and stays free of clap so the integration tests can drive it directly.

pub mod doctor;
pub mod init;
pub mod list;
pub mod preview;
pub mod run;
#[cfg(feature = "schedule")]
pub mod schedule;
pub mod schema;
#[cfg(feature = "serve")]
pub mod serve;
pub mod validate;

use crate::error::CliError;

/// Render any [`CliError`] to a single line on stderr, scrubbing any resolved
/// secret values that may have reached the error chain.
pub fn report(err: &CliError) {
    use crate::secrets::registry::redact;
    eprintln!("error: {}", redact(&err.to_string()));
    let mut src = std::error::Error::source(err);
    while let Some(s) = src {
        eprintln!("  caused by: {}", redact(&s.to_string()));
        src = std::error::Error::source(s);
    }
}
