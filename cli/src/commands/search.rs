//! `faucet search` — find connectors in the registry index (#208).

use crate::cli::SearchArgs;
use crate::error::{CliError, CliResult};
use crate::registry_index::RegistryIndex;

/// Execute the `search` subcommand.
pub async fn run(args: SearchArgs) -> CliResult<()> {
    let idx = RegistryIndex::load(args.index.as_deref())?;
    let hits = idx.search(&args.term);

    if args.json {
        let out =
            serde_json::to_string_pretty(&hits).map_err(|e| CliError::Config(e.to_string()))?;
        println!("{out}");
        return Ok(());
    }

    if hits.is_empty() {
        println!("No connectors match '{}'.", args.term);
        println!("Browse everything with `faucet list --available`.");
        return Ok(());
    }

    println!("{} connector(s) matching '{}':\n", hits.len(), args.term);
    for c in hits {
        let badge = if c.verified { "verified" } else { "community" };
        println!(
            "  {:<6} {:<14} {}  [{badge}]",
            c.kind, c.name, c.description
        );
        println!(
            "         crate {} · feature {}",
            c.crate_name(),
            c.feature_flag()
        );
    }
    println!("\nInstall one with `faucet install <name> [--kind source|sink]`.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::SearchArgs;

    #[tokio::test]
    async fn search_runs_json_and_human() {
        // Human form.
        run(SearchArgs {
            term: "kafka".into(),
            index: None,
            json: false,
        })
        .await
        .unwrap();
        // JSON form.
        run(SearchArgs {
            term: "kafka".into(),
            index: None,
            json: true,
        })
        .await
        .unwrap();
        // No-match form.
        run(SearchArgs {
            term: "zzz-nope".into(),
            index: None,
            json: false,
        })
        .await
        .unwrap();
    }
}
