//! `faucet install` — tell the user how to obtain/enable a connector (#208).
//!
//! Never executes anything: it resolves the connector in the registry index and
//! prints the exact recipe (a `cargo install … --features …` line for a
//! built-in, or custom-binary guidance for a community connector).

use crate::cli::InstallArgs;
use crate::error::{CliError, CliResult};
use crate::registry::{sink_exists, source_exists};
use crate::registry_index::{InstallRecipe, RegistryIndex, install_recipe};

/// Execute the `install` subcommand.
pub async fn run(args: InstallArgs) -> CliResult<()> {
    let idx = RegistryIndex::load(args.index.as_deref())?;
    let matches = idx.find(&args.name, args.kind.as_deref());
    let entry = match matches.as_slice() {
        [] => {
            return Err(CliError::Config(format!(
                "connector '{}' is not in the registry index — try `faucet search {}`",
                args.name, args.name
            )));
        }
        [one] => *one,
        many => {
            return Err(CliError::Config(format!(
                "connector '{}' is ambiguous ({} entries: {}); disambiguate with --kind source|sink",
                args.name,
                many.len(),
                many.iter()
                    .map(|c| c.kind.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    };

    let compiled = match entry.kind.as_str() {
        "source" => source_exists(&entry.name),
        "sink" => sink_exists(&entry.name),
        _ => false,
    };

    match install_recipe(entry, compiled) {
        InstallRecipe::AlreadyAvailable { feature } => {
            println!(
                "✔ {} '{}' is already available in this binary (feature `{}`).",
                entry.kind, entry.name, feature
            );
            println!("   Use it directly: `type: {}` in your config.", entry.name);
        }
        InstallRecipe::CargoInstall { feature } => {
            println!(
                "'{}' is a built-in {} connector, not compiled into this binary.",
                entry.name, entry.kind
            );
            println!("Reinstall the CLI with it enabled:\n");
            println!("  cargo install faucet-cli --features {feature}\n");
            println!("(add `{feature}` to your existing `--features` list to keep the others).");
        }
        InstallRecipe::CustomBinary { krate, feature } => {
            println!(
                "'{}' is a community {} connector (crate `{}`).",
                entry.name, entry.kind, krate
            );
            println!("Use it by building a custom `faucet` binary that registers it:\n");
            println!("  cargo new my-faucet && cd my-faucet");
            println!("  cargo add faucet-cli faucet-core {krate}\n");
            let (reg_fn, trait_ctor) = match entry.kind.as_str() {
                "source" => ("register_source", "MySource::from_value(cfg)?"),
                _ => ("register_sink", "MySink::from_value(cfg)?"),
            };
            println!("  // src/main.rs");
            println!("  use faucet_cli::registry::PluginRegistry;");
            println!("  fn main() -> std::process::ExitCode {{");
            println!(
                "      let reg = PluginRegistry::with_builtins().{reg_fn}(\"{}\", |cfg| Ok(Box::new({trait_ctor})));",
                entry.name
            );
            println!("      faucet_cli::run_main(reg)");
            println!("  }}\n");
            println!(
                "See cli/README.md → \"Custom binaries with third-party connectors\". (feature hint: `{feature}`)"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::InstallArgs;

    #[tokio::test]
    async fn install_builtin_ok() {
        // `jsonl` sink is built-in; under default features it is compiled in.
        run(InstallArgs {
            name: "jsonl".into(),
            kind: Some("sink".into()),
            index: None,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn install_unknown_errors() {
        let err = run(InstallArgs {
            name: "nope-connector".into(),
            kind: None,
            index: None,
        })
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::Config(_)));
    }

    #[tokio::test]
    async fn install_ambiguous_requires_kind() {
        // `postgres` exists as both a source and a sink → ambiguous without --kind.
        let err = run(InstallArgs {
            name: "postgres".into(),
            kind: None,
            index: None,
        })
        .await
        .unwrap_err();
        match err {
            CliError::Config(msg) => assert!(msg.contains("ambiguous"), "{msg}"),
            other => panic!("expected ambiguity error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn install_community_custom_binary() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("idx.json");
        std::fs::write(
            &p,
            r#"{"version":1,"connectors":[{"name":"acme","kind":"source","verified":false,"description":"Acme"}]}"#,
        )
        .unwrap();
        run(InstallArgs {
            name: "acme".into(),
            kind: None,
            index: Some(p),
        })
        .await
        .unwrap();
    }
}
