//! `faucet list` — show every compiled-in source, sink, transform, and
//! state-store backend so users can discover what their binary supports.

use crate::error::CliResult;
use crate::registry::{sink_descriptions, source_descriptions};
use crate::state::available_state_kinds;
#[cfg(feature = "quality")]
use crate::transforms::quality_descriptions;
use crate::transforms::transform_descriptions;

/// Execute the `list` subcommand.
pub async fn run() -> CliResult<()> {
    println!("Sources:");
    print_two_column(&source_descriptions());
    println!();
    println!("Sinks:");
    print_two_column(&sink_descriptions());
    println!();
    println!("Transforms:");
    print_two_column(&transform_descriptions());
    println!();
    #[cfg(feature = "quality")]
    {
        println!("Quality checks:");
        print_two_column(&quality_descriptions());
        println!();
    }
    println!("State stores: {}", available_state_kinds().join(", "));
    Ok(())
}

fn print_two_column(entries: &[(&'static str, &'static str)]) {
    if entries.is_empty() {
        println!("  (none — rebuild faucet-cli with the relevant features enabled)");
        return;
    }
    let width = entries.iter().map(|(n, _)| n.len()).max().unwrap_or(0);
    for (name, desc) in entries {
        println!("  {name:<width$}  {desc}", width = width);
    }
}
