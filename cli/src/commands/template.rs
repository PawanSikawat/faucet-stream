//! `faucet template` — the CLI half of the pipeline template registry (#444).
//!
//! Register a parameterized config once into a store (`sqlite:` / `postgres://` /
//! `memory`), then list / inspect / delete versions, or materialize one with
//! `--param` values and run it locally. Pointing `faucet serve --history` at the
//! same URL makes the very same templates triggerable over HTTP — the CLI and
//! the control plane share one registry, not two.

use crate::cli::{
    TemplateArgs, TemplateCommand, TemplateDeleteArgs, TemplateListArgs, TemplatePromoteArgs,
    TemplateRegisterArgs, TemplateRunArgs, TemplateShowArgs, TemplateStoreArgs,
};
use crate::error::{CliError, CliResult};
use crate::serve::history::templates::{
    TemplateRecord, TemplateSummary, VersionChannel, VersionSelector,
};
use crate::serve::load::ConfigFormat;
use crate::templates::{RegisterRequest, TemplateStore};

/// Execute the `template` subcommand.
pub async fn run(args: TemplateArgs) -> CliResult<()> {
    match args.command {
        TemplateCommand::Register(a) => register(a).await,
        TemplateCommand::List(a) => list(a).await,
        TemplateCommand::Show(a) => show(a).await,
        TemplateCommand::Promote(a) => promote(a).await,
        TemplateCommand::Delete(a) => delete(a).await,
        TemplateCommand::Run(a) => run_template(a).await,
    }
}

/// Load `.env` (so a `${env:…}` in a materialized template resolves) and connect
/// the registry store.
async fn connect(common: &TemplateStoreArgs) -> CliResult<TemplateStore> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(common.env_file.as_deref(), common.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    crate::templates::resolve_store_url(&common.store).await
}

fn to_pretty<T: serde::Serialize>(value: &T) -> CliResult<String> {
    serde_json::to_string_pretty(value)
        .map_err(|e| CliError::Internal(format!("rendering template JSON: {e}")))
}

/// Pick the wire format from a config path's extension.
fn format_of(path: &std::path::Path) -> CliResult<ConfigFormat> {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("yaml" | "yml") => Ok(ConfigFormat::Yaml),
        Some("json") => Ok(ConfigFormat::Json),
        _ => Err(CliError::UnknownExtension {
            path: path.to_path_buf(),
        }),
    }
}

async fn register(args: TemplateRegisterArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let format = format_of(&args.config)?;
    let body = std::fs::read_to_string(&args.config).map_err(|e| {
        CliError::Config(format!(
            "reading template config '{}': {e}",
            args.config.display()
        ))
    })?;
    let tags = args
        .tag
        .iter()
        .map(|t| VersionChannel::parse(t))
        .collect::<CliResult<Vec<_>>>()?;
    let record = crate::templates::register(
        &store,
        RegisterRequest {
            id: args.id.clone(),
            body,
            format,
            description: args.description.clone(),
            tags: tags.clone(),
            created_by: None,
        },
    )
    .await?;

    if args.common.json {
        println!("{}", to_pretty(&record.summary())?);
        return Ok(());
    }
    println!(
        "registered template '{}' version {}{}",
        record.id,
        record.version,
        if tags.is_empty() {
            String::new()
        } else {
            format!(
                "  (channels: {})",
                tags.iter()
                    .map(|c| c.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    );
    print_params(&record.summary());
    println!(
        "\ntrigger it with:\n  faucet template run {} --store {}{}",
        record.id,
        args.common.store,
        required_param_hint(&record.summary())
    );
    Ok(())
}

/// `--param name=<…>` hints for every required param, for the register / show
/// "how do I run this" line.
fn required_param_hint(summary: &TemplateSummary) -> String {
    summary
        .params
        .iter()
        .filter(|(_, p)| p.required)
        .map(|(name, p)| format!(" --param {name}=<{}>", p.kind.as_str()))
        .collect()
}

fn print_params(summary: &TemplateSummary) {
    if summary.params.is_empty() {
        println!("params: (none — this template takes no overrides)");
        return;
    }
    println!("\nparams:");
    for (name, p) in &summary.params {
        let requirement = if p.required {
            "required".to_string()
        } else {
            match &p.default {
                Some(d) => format!("default {d}"),
                None => "optional".to_string(),
            }
        };
        println!(
            "  {:<20} {:<7} {}{}{}",
            name,
            p.kind.as_str(),
            requirement,
            if p.secret { "  [secret]" } else { "" },
            match &p.description {
                Some(d) => format!("  — {d}"),
                None => String::new(),
            }
        );
    }
}

async fn list(args: TemplateListArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let templates = store
        .template_list()
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?;
    if args.common.json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({ "templates": templates }))?
        );
        return Ok(());
    }
    if templates.is_empty() {
        println!("no templates registered in this store — add one with `faucet template register`");
        return Ok(());
    }
    // Every row IS the latest version of its id, so the column is labelled as
    // such — `list` never shows a superseded version.
    println!(
        "{:<28}  {:<8}  {:>6}  {:<20}  DESCRIPTION",
        "ID", "LATEST", "PARAMS", "REGISTERED"
    );
    for t in &templates {
        println!(
            "{:<28}  {:<8}  {:>6}  {:<20}  {}",
            t.id,
            format!("v{}", t.version),
            t.params.len(),
            t.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            t.description.as_deref().unwrap_or("")
        );
    }
    Ok(())
}

/// Fetch one template, mapping "not found" to a typed error naming the id.
async fn fetch(store: &TemplateStore, id: &str, version: Option<u32>) -> CliResult<TemplateRecord> {
    store
        .template_get(id, version)
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
        .ok_or_else(|| CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version,
        })
}

async fn show(args: TemplateShowArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let selector = VersionSelector::parse(&args.version)?;
    let want = crate::templates::resolve_version(&store, &args.id, selector).await?;
    let record = fetch(&store, &args.id, want).await?;
    let versions = store
        .template_versions(&args.id)
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?;
    let tags = store
        .template_tags(&args.id)
        .await
        .map_err(|e| CliError::Internal(format!("template channel read: {e}")))?;
    let latest = versions.first().copied().unwrap_or(record.version);

    if args.common.json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({
                "template": record,
                "versions": versions,
                "latest_version": latest,
                "is_latest": record.version == latest,
                "tags": tags,
            }))?
        );
        return Ok(());
    }
    println!("template  {}", record.id);
    println!(
        "version   {}{}   (stored: {})",
        record.version,
        if record.version == latest {
            "  [latest]"
        } else {
            ""
        },
        versions
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "channels {}",
        if tags.is_empty() {
            format!("latest=v{latest}  (no other channel promoted yet)")
        } else {
            let mut rendered = vec![format!("latest=v{latest}")];
            rendered.extend(tags.iter().map(|(t, v)| format!("{t}=v{v}")));
            rendered.join("  ")
        }
    );
    if let Some(name) = &record.name {
        println!("name      {name}");
    }
    if let Some(d) = &record.description {
        println!("about     {d}");
    }
    println!(
        "created   {}{}",
        record.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        match &record.created_by {
            Some(p) => format!(" by {p}"),
            None => String::new(),
        }
    );
    print_params(&record.summary());
    println!("\nconfig ({:?}, stored verbatim):", record.format);
    for line in record.body.lines() {
        println!("  {line}");
    }
    Ok(())
}

async fn delete(args: TemplateDeleteArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    // No `--version` deletes the whole template; a selector deletes one version.
    let pinned = match args.version.as_deref() {
        None => None,
        Some(raw) => {
            let selector = VersionSelector::parse(raw)?;
            Some(
                match crate::templates::resolve_version(&store, &args.id, selector).await? {
                    Some(v) => v,
                    // `latest` means "newest" — resolve it to a concrete number so
                    // the delete removes one version rather than all of them.
                    None => store
                        .template_versions(&args.id)
                        .await
                        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
                        .first()
                        .copied()
                        // Nothing stored → the delete below reports 0 → 404.
                        .unwrap_or(u32::MAX),
                },
            )
        }
    };
    let removed = store
        .template_delete(&args.id, pinned)
        .await
        .map_err(|e| CliError::Internal(format!("template registry write: {e}")))?;
    if removed == 0 {
        return Err(CliError::UnknownPipelineTemplate {
            id: args.id.clone(),
            version: pinned,
        });
    }
    if args.common.json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({ "id": args.id, "deleted_versions": removed }))?
        );
        return Ok(());
    }
    println!("deleted {removed} version(s) of template '{}'", args.id);
    Ok(())
}

async fn promote(args: TemplatePromoteArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let tag = VersionChannel::parse(&args.tag)?;
    let target = VersionSelector::parse(&args.version)?;
    let version = crate::templates::promote(&store, &args.id, tag, target).await?;
    if args.common.json {
        println!(
            "{}",
            to_pretty(&serde_json::json!({
                "id": args.id, "tag": tag.as_str(), "version": version,
            }))?
        );
        return Ok(());
    }
    println!("template '{}': {tag} → v{version}", args.id);
    Ok(())
}

async fn run_template(args: TemplateRunArgs) -> CliResult<()> {
    let store = connect(&args.common).await?;
    let supplied = crate::params::collect_cli_params(&args.param)?;
    let env = crate::params::collect_env_overrides(&args.param_env)?;
    let selector = VersionSelector::parse(&args.version)?;
    let want = crate::templates::resolve_version(&store, &args.id, selector).await?;
    let materialized =
        crate::templates::materialize(&store, &args.id, want, &supplied, &env).await?;

    tracing::info!(
        template = %materialized.template_id,
        version = materialized.version,
        "materialized pipeline template"
    );

    // The materialized body is JSON with every `${param.*}` bound; `${env:…}`
    // for overridden variables is bound too. Remaining directives (secrets,
    // un-overridden env) resolve on the normal load path below.
    let doc: serde_json::Value = serde_json::from_str(&materialized.body)
        .map_err(|e| CliError::Internal(format!("re-parsing materialized template: {e}")))?;
    let mut cfg = crate::config::PipelineConfig::from_value(doc)?;
    crate::secrets::resolve_secrets(&mut cfg).await?;

    if args.dry_run && args.common.json {
        println!("{}", to_pretty(&cfg)?);
        return Ok(());
    }

    // Run through the identical path as `faucet run`, so observability,
    // lineage, notifications, the catalog, SLA evaluation, and row selection all
    // behave the same as they would for the same config on disk.
    let run_args = crate::cli::RunArgs {
        dry_run: args.dry_run,
        limit: args.limit,
        no_env_file: true,
        ..Default::default()
    };
    crate::commands::run::execute(cfg, run_args, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::TemplateStoreArgs;

    fn common(store: &str, json: bool) -> TemplateStoreArgs {
        TemplateStoreArgs {
            store: store.to_string(),
            env_file: None,
            no_env_file: true,
            json,
        }
    }

    const BODY: &str = "\
version: 1
name: cli-tpl
params:
  tag: { required: true, description: Output tag }
  page: { type: int, default: 5 }
pipeline:
  source:
    type: csv
    config:
      path: IN_PATH
  sink:
    type: jsonl
    config:
      path: OUT_PATH
";

    /// A registered template needs a *persistent* store to be visible to a
    /// second command, so the CLI round-trip test uses a temp SQLite file.
    /// Without the SQL backend feature the whole test is skipped.
    #[cfg(feature = "serve-history-sqlite")]
    #[tokio::test]
    async fn register_list_show_run_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();
        let output = dir.path().join("out.jsonl");
        let cfg_path = dir.path().join("tpl.yaml");
        std::fs::write(
            &cfg_path,
            BODY.replace("IN_PATH", &input.display().to_string())
                .replace("OUT_PATH", &output.display().to_string()),
        )
        .unwrap();
        let store = format!("sqlite:{}", dir.path().join("registry.db").display());

        register(TemplateRegisterArgs {
            config: cfg_path.clone(),
            id: None,
            description: Some("round trip".into()),
            tag: vec!["dev".into()],
            common: common(&store, false),
        })
        .await
        .expect("register");

        list(TemplateListArgs {
            common: common(&store, true),
        })
        .await
        .expect("list");

        show(TemplateShowArgs {
            id: "cli-tpl".into(),
            version: "latest".into(),
            common: common(&store, false),
        })
        .await
        .expect("show");

        // A required param is enforced.
        let err = run_template(TemplateRunArgs {
            id: "cli-tpl".into(),
            version: "latest".into(),
            param: vec![],
            param_env: vec![],
            dry_run: true,
            limit: None,
            common: common(&store, true),
        })
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::MissingParam { .. }), "{err:?}");

        // Supplying it runs the pipeline for real.
        run_template(TemplateRunArgs {
            id: "cli-tpl".into(),
            version: "latest".into(),
            param: vec!["tag=alpha".into()],
            param_env: vec![],
            dry_run: false,
            limit: None,
            common: common(&store, false),
        })
        .await
        .expect("run");

        // `register --tag dev` pointed `dev` at v1; promote `prod` from it, then
        // run *by channel* rather than by number.
        promote(TemplatePromoteArgs {
            id: "cli-tpl".into(),
            tag: "prod".into(),
            version: "dev".into(),
            common: common(&store, false),
        })
        .await
        .expect("promote");
        run_template(TemplateRunArgs {
            id: "cli-tpl".into(),
            version: "prod".into(),
            param: vec!["tag=viachannel".into()],
            param_env: vec![],
            dry_run: true,
            limit: None,
            common: common(&store, true),
        })
        .await
        .expect("run via channel");

        // `show` renders the channel map (and marks `[latest]`).
        show(TemplateShowArgs {
            id: "cli-tpl".into(),
            version: "prod".into(),
            common: common(&store, true),
        })
        .await
        .expect("show pinned via channel");

        // `latest` and an invented channel are rejected on promote.
        assert!(
            promote(TemplatePromoteArgs {
                id: "cli-tpl".into(),
                tag: "latest".into(),
                version: "1".into(),
                common: common(&store, false),
            })
            .await
            .is_err(),
            "`latest` is derived and must not be assignable"
        );
        assert!(
            promote(TemplatePromoteArgs {
                id: "cli-tpl".into(),
                tag: "prd".into(),
                version: "1".into(),
                common: common(&store, false),
            })
            .await
            .is_err(),
            "channels are a closed set"
        );

        // `--version latest` deletes only the newest version, unlike an omitted
        // `--version` (which removes the whole template, below).
        register(TemplateRegisterArgs {
            config: cfg_path.clone(),
            id: None,
            description: None,
            tag: vec![],
            common: common(&store, false),
        })
        .await
        .expect("register v2");
        delete(TemplateDeleteArgs {
            id: "cli-tpl".into(),
            version: Some("latest".into()),
            common: common(&store, false),
        })
        .await
        .expect("delete latest");
        assert_eq!(
            std::fs::read_to_string(&output).unwrap().lines().count(),
            2,
            "the template's pipeline wrote both records"
        );

        // Delete, then confirm the id is gone.
        delete(TemplateDeleteArgs {
            id: "cli-tpl".into(),
            version: None,
            common: common(&store, true),
        })
        .await
        .expect("delete");
        let err = delete(TemplateDeleteArgs {
            id: "cli-tpl".into(),
            version: None,
            common: common(&store, false),
        })
        .await
        .unwrap_err();
        assert!(
            matches!(err, CliError::UnknownPipelineTemplate { .. }),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn show_and_run_report_an_unknown_template() {
        let c = common("memory", false);
        let store = connect(&c).await.unwrap();
        let err = fetch(&store, "nope", None).await.unwrap_err();
        assert!(
            matches!(err, CliError::UnknownPipelineTemplate { ref id, .. } if id == "nope"),
            "{err:?}"
        );
        // Promoting a channel on a template that does not exist is the same
        // typed error, not a silently-created pointer.
        let err = promote(TemplatePromoteArgs {
            id: "nope".into(),
            tag: "prod".into(),
            version: "latest".into(),
            common: common("memory", false),
        })
        .await
        .unwrap_err();
        assert!(
            matches!(err, CliError::UnknownPipelineTemplate { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn format_is_taken_from_the_extension() {
        assert_eq!(
            format_of(std::path::Path::new("a.yaml")).unwrap(),
            ConfigFormat::Yaml
        );
        assert_eq!(
            format_of(std::path::Path::new("a.YML")).unwrap(),
            ConfigFormat::Yaml
        );
        assert_eq!(
            format_of(std::path::Path::new("a.json")).unwrap(),
            ConfigFormat::Json
        );
        assert!(format_of(std::path::Path::new("a.toml")).is_err());
        assert!(format_of(std::path::Path::new("a")).is_err());
    }

    #[test]
    fn required_param_hint_lists_only_required_params() {
        let mut params = crate::params::ParamsSpec::new();
        params.insert(
            "tag".into(),
            crate::params::ParamSpec {
                kind: crate::params::ParamType::String,
                required: true,
                default: None,
                secret: false,
                description: None,
            },
        );
        params.insert(
            "page".into(),
            crate::params::ParamSpec {
                kind: crate::params::ParamType::Int,
                required: false,
                default: Some(serde_json::json!(5)),
                secret: false,
                description: None,
            },
        );
        let summary = TemplateSummary {
            id: "t".into(),
            version: 1,
            name: None,
            description: None,
            params,
            created_at: chrono::Utc::now(),
            created_by: None,
        };
        let hint = required_param_hint(&summary);
        assert_eq!(hint, " --param tag=<string>");
        // `print_params` renders both without panicking.
        print_params(&summary);
    }

    #[tokio::test]
    async fn register_rejects_a_bad_extension_and_a_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("cfg.toml");
        std::fs::write(&bad, "x = 1").unwrap();
        let err = register(TemplateRegisterArgs {
            config: bad,
            id: None,
            description: None,
            tag: vec![],
            common: common("memory", false),
        })
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::UnknownExtension { .. }), "{err:?}");

        let err = register(TemplateRegisterArgs {
            config: dir.path().join("nope.yaml"),
            id: None,
            description: None,
            tag: vec![],
            common: common("memory", false),
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("reading template config"), "{err}");
    }

    #[tokio::test]
    async fn list_reports_an_empty_store() {
        list(TemplateListArgs {
            common: common("memory", false),
        })
        .await
        .expect("empty list is not an error");
    }
}
