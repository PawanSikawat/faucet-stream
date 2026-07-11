//! Subcommand implementations. Each module owns its `run(...)` entry point
//! and stays free of clap so the integration tests can drive it directly.

pub mod backfill;
#[cfg(feature = "catalog")]
pub mod catalog;
#[cfg(feature = "contract")]
pub mod contract;
pub mod discover;
pub mod dlq;
pub mod doctor;
pub mod init;
pub mod list;
#[cfg(feature = "masking")]
pub mod masking;
pub mod new;
#[cfg(feature = "notify")]
pub mod notify;
pub mod preview;
pub mod replicate;
pub mod run;
#[cfg(feature = "schedule")]
pub mod schedule;
pub mod schema;
#[cfg(feature = "serve")]
pub mod serve;
pub mod test;
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
