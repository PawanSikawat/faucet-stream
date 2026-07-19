//! `faucet list` — show every compiled-in source, sink, transform, and
//! state-store backend so users can discover what their binary supports.
//! `--available` instead lists every connector in the registry index (#208),
//! marking which are compiled into this binary.

use crate::cli::ListArgs;
use crate::conformance::tier_for;
use crate::error::CliResult;
use crate::registry::{sink_descriptions, sink_exists, source_descriptions, source_exists};
use crate::registry_index::RegistryIndex;
use crate::state::available_state_kinds;
#[cfg(feature = "quality")]
use crate::transforms::quality_descriptions;
use crate::transforms::transform_descriptions;

/// Execute the `list` subcommand.
pub async fn run(args: ListArgs) -> CliResult<()> {
    if args.available {
        return list_available(args);
    }
    println!("Sources:");
    print_connectors(&source_descriptions(), true);
    println!();
    println!("Sinks:");
    print_connectors(&sink_descriptions(), false);
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
    #[cfg(feature = "schedule")]
    println!("Scheduler:    compiled in (run `faucet schedule --help`, `faucet schema schedule`)");
    Ok(())
}

/// `faucet list --available` — every connector in the registry index, with a
/// marker for those already compiled into this binary.
fn list_available(args: ListArgs) -> CliResult<()> {
    let idx = RegistryIndex::load(args.index.as_deref())?;
    let mut connectors: Vec<_> = idx.connectors.iter().collect();
    connectors.sort_by(|a, b| {
        (a.kind.as_str(), a.name.as_str()).cmp(&(b.kind.as_str(), b.name.as_str()))
    });
    println!(
        "Registry connectors ({} total). ● = compiled into this binary, ○ = available via `faucet install`:\n",
        connectors.len()
    );
    for c in connectors {
        let compiled = match c.kind.as_str() {
            "source" => source_exists(&c.name),
            "sink" => sink_exists(&c.name),
            _ => false,
        };
        let mark = if compiled { '●' } else { '○' };
        let badge = if c.verified { "verified" } else { "community" };
        let tier = c.tier.as_deref().unwrap_or("-");
        println!(
            "  {mark} {kind:<6} {name:<14} {tier:<12} {desc}  [{badge}]",
            kind = c.kind,
            name = c.name,
            desc = c.description
        );
    }
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

/// Like [`print_two_column`] but prefixes each connector with its conformance
/// maturity tier badge (`faucet conformance` for the full scorecards).
fn print_connectors(entries: &[(&'static str, &'static str)], is_source: bool) {
    if entries.is_empty() {
        println!("  (none — rebuild faucet-cli with the relevant features enabled)");
        return;
    }
    let width = entries.iter().map(|(n, _)| n.len()).max().unwrap_or(0);
    for (name, desc) in entries {
        let tier = tier_for(name, is_source);
        println!(
            "  {badge} {name:<width$}  {tier:<12} {desc}",
            badge = tier.badge(),
            tier = tier.label(),
            width = width,
        );
    }
}
