//! Registration + materialization of pipeline templates (#444).
//!
//! Pure orchestration over [`crate::serve::history::RunHistory`]'s template
//! methods and [`crate::params`]; no HTTP, no clap, no MCP shapes — the three
//! front-ends are thin adapters over the two entry points here.

use crate::error::{CliError, CliResult};
use crate::params::{self, BindMode, SuppliedParams};
use crate::serve::config::HistoryBackendSpec;
use crate::serve::history::templates::{
    TemplateDraft, TemplateId, TemplateRecord, VersionChannel, VersionSelector,
};
use crate::serve::history::{self, RunHistory};
use crate::serve::load::ConfigFormat;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// The registry handle. Any `RunHistory` backend will do — `faucet serve` passes
/// its own `--history` store so templates live beside run records; the CLI
/// connects one from `--store` / the config's `catalog:` block.
pub type TemplateStore = Arc<dyn RunHistory>;

/// A registration, before validation.
#[derive(Debug, Clone)]
pub struct RegisterRequest {
    /// Explicit id. When `None` the id is derived from the config's `name:`.
    pub id: Option<String>,
    /// The config document, stored verbatim.
    pub body: String,
    pub format: ConfigFormat,
    /// Free-text description (falls back to nothing).
    pub description: Option<String>,
    /// Named channels to point at the newly registered version. The version
    /// number itself always auto-increments; these are the human-facing pointers
    /// (`dev`, `pre-prod`, …) the caller wants moved onto it in the same step.
    /// `latest` is derived and rejected.
    pub tags: Vec<VersionChannel>,
    /// Principal performing the registration, for provenance.
    pub created_by: Option<String>,
}

/// A template rendered for one trigger: a config document with every
/// `${param.*}` bound, ready to hand to the ordinary run path.
#[derive(Debug, Clone)]
pub struct MaterializedConfig {
    pub template_id: String,
    pub version: u32,
    /// The config's own `name:`, for the run record.
    pub name: Option<String>,
    /// JSON config document (params bound). JSON regardless of how the template
    /// was registered — one canonical hand-off shape for the run path.
    pub body: String,
    /// Bound param values with `secret: true` entries replaced by `"***"` — the
    /// only form safe to echo, audit, or persist.
    pub params_redacted: BTreeMap<String, Value>,
    /// True when at least one bound param was declared `secret: true`.
    pub used_secret_params: bool,
}

impl MaterializedConfig {
    /// Wire format of [`Self::body`]. Always JSON.
    pub fn format(&self) -> ConfigFormat {
        ConfigFormat::Json
    }
}

/// Parse a config document by declared format into an untyped value.
fn parse_body(body: &str, format: ConfigFormat) -> CliResult<Value> {
    match format {
        ConfigFormat::Yaml => {
            serde_yaml::from_str(body).map_err(|e| CliError::Config(format!("invalid YAML: {e}")))
        }
        ConfigFormat::Json => {
            serde_json::from_str(body).map_err(|e| CliError::Config(format!("invalid JSON: {e}")))
        }
    }
}

/// Validate a submitted config and append it as a new template version.
///
/// Validation deliberately runs against a **placeholder binding**: required
/// params have no value at registration time, so each is filled with a
/// type-shaped stand-in and the config is then taken through the real
/// `PipelineConfig` parse plus `expand` (matrix mode) or
/// [`crate::topology::validate_topology_spec`] (topology mode). That checks
/// everything structural — grammar, named templates, the matrix graph
/// (parent/`depends_on` cycles, duplicate state keys), the exactly-once and
/// write-mode gates, edge endpoints — without resolving a single secret or
/// constructing a single connector. Node arity in topology mode is validated
/// when the graph is built, i.e. at trigger time, because building it requires
/// live connectors that a placeholder-bound config must not create.
pub async fn register(store: &TemplateStore, req: RegisterRequest) -> CliResult<TemplateRecord> {
    let mut doc = parse_body(&req.body, req.format)?;
    if !doc.is_object() {
        return Err(CliError::Config(
            "a pipeline template must be a config document (a YAML/JSON mapping)".into(),
        ));
    }

    // The declared trigger surface, validated and stored alongside the body so
    // callers can discover it without re-parsing.
    let declared = params::declared(&doc)?;

    // Structural validation on a placeholder-bound copy. `${env:…}` and secret
    // directives are left untouched — registration must never read the server's
    // secrets, and the body we persist is the one that was submitted.
    let mut probe = doc.clone();
    params::bind_document(&mut probe, &SuppliedParams::new(), BindMode::Placeholder)?;
    let cfg = crate::config::PipelineConfig::from_value(probe)?;
    if crate::topology::is_topology(&cfg) {
        crate::topology::validate_topology_spec(&cfg)?;
    } else {
        crate::expand::expand(&cfg)?;
    }

    let id = match &req.id {
        Some(raw) => TemplateId::parse(raw)?,
        None => {
            let name = cfg.name.as_deref().ok_or_else(|| {
                CliError::Config(
                    "no template id given and the config has no `name:` to derive one from — \
                     pass an explicit id"
                        .into(),
                )
            })?;
            TemplateId::from_config_name(name)?
        }
    };

    // Keep the persisted body byte-identical to what was submitted.
    let _ = &mut doc;

    // Reject a derived channel before writing anything, so a bad request never
    // leaves a half-registered version behind.
    for tag in &req.tags {
        reject_derived(*tag)?;
    }

    let draft = TemplateDraft {
        id,
        name: cfg.name.clone(),
        description: req.description.clone(),
        body: req.body.clone(),
        format: req.format,
        params: declared,
        created_by: req.created_by.clone(),
    };
    let record = store
        .template_register(&draft)
        .await
        .map_err(|e| CliError::Internal(format!("template registry write: {e}")))?;

    // Point the requested channels at the version just created.
    for tag in &req.tags {
        store
            .template_set_tag(&record.id, tag.as_str(), record.version)
            .await
            .map_err(|e| CliError::Internal(format!("template channel write: {e}")))?;
    }
    Ok(record)
}

/// `latest` is computed from the version list, so promoting or deleting it makes
/// no sense — say so instead of silently no-oping.
fn reject_derived(tag: VersionChannel) -> CliResult<()> {
    if tag.is_derived() {
        return Err(CliError::Config(format!(
            "`{tag}` is derived — it always names the newest version and cannot be assigned or \
             moved. Register a new version instead, or promote one of: {}",
            VersionChannel::ASSIGNABLE
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    Ok(())
}

/// Resolve a [`VersionSelector`] to the exact version to act on.
///
/// `Ok(None)` means "the newest" (`latest`, or an omitted selector) — the shape
/// [`RunHistory::template_get`] already takes. A named channel is looked up in
/// the registry; an unset one is an error naming the channels that *are* set,
/// because silently falling back to `latest` would run the wrong code.
pub async fn resolve_version(
    store: &TemplateStore,
    id: &str,
    selector: VersionSelector,
) -> CliResult<Option<u32>> {
    match selector {
        VersionSelector::Pinned(n) => Ok(Some(n)),
        VersionSelector::Channel(c) if c.is_derived() => Ok(None),
        VersionSelector::Channel(c) => {
            let tags = store
                .template_tags(id)
                .await
                .map_err(|e| CliError::Internal(format!("template channel read: {e}")))?;
            tags.get(c.as_str()).copied().map(Some).ok_or_else(|| {
                CliError::Config(format!(
                    "template '{id}' has no `{c}` version. Channels currently set: {}. Promote one \
                     with `faucet template promote {id} --tag {c} --version <n>`",
                    if tags.is_empty() {
                        String::from("(none)")
                    } else {
                        tags.iter()
                            .map(|(t, v)| format!("{t}=v{v}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                ))
            })
        }
    }
}

/// Point a named channel at a version, moving it if already set.
///
/// The target is itself a selector, so `--tag prod --version stable` promotes
/// whatever `stable` currently names — the "select an existing named version"
/// case — and `--version 3` pins an exact one. The version must exist.
pub async fn promote(
    store: &TemplateStore,
    id: &str,
    tag: VersionChannel,
    target: VersionSelector,
) -> CliResult<u32> {
    reject_derived(tag)?;
    let version = match resolve_version(store, id, target).await? {
        Some(v) => v,
        // `latest` → resolve to the concrete newest, so the pointer is stable
        // rather than tracking future registrations.
        None => {
            store
                .template_get(id, None)
                .await
                .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
                .ok_or_else(|| CliError::UnknownPipelineTemplate {
                    id: id.to_string(),
                    version: None,
                })?
                .version
        }
    };
    // Refuse to aim a channel at a version that does not exist.
    if store
        .template_get(id, Some(version))
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
        .is_none()
    {
        return Err(CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: Some(version),
        });
    }
    store
        .template_set_tag(id, tag.as_str(), version)
        .await
        .map_err(|e| CliError::Internal(format!("template channel write: {e}")))?;
    Ok(version)
}

/// Fetch a template (latest version when `version` is `None`), binding the
/// supplied params and env overrides into a runnable config document.
///
/// Ordering matters and mirrors the file-load path exactly: `${env:}` /
/// `${file:}` / `${secret:}` resolve **first** (with `env_overrides` taking
/// precedence over the process environment), then `${param.*}` binds. A supplied
/// param value is therefore never itself scanned for directives, so a caller
/// cannot use a param to read the server's environment or secret store.
pub async fn materialize(
    store: &TemplateStore,
    id: &str,
    version: Option<u32>,
    supplied: &SuppliedParams,
    env_overrides: &BTreeMap<String, String>,
) -> CliResult<MaterializedConfig> {
    let record = store
        .template_get(id, version)
        .await
        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
        .ok_or_else(|| CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version,
        })?;

    let mut doc = parse_body(&record.body, record.format)?;
    let overlay: crate::interpolate::EnvOverlay = env_overrides
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    crate::interpolate::interpolate_value_with_env(&mut doc, &overlay)?;
    let bound = params::bind_document(&mut doc, supplied, BindMode::Strict)?;
    // Drop the declaration block: materialization is the moment params cease to
    // exist. Leaving it would make any later load re-run the bind pass with no
    // supplied values and reject the config for a "missing" required param —
    // and the param surface is already recorded on the template and echoed to
    // the caller, so nothing is lost.
    if let Some(map) = doc.as_object_mut() {
        map.remove(params::PARAMS_KEY);
    }

    let body = serde_json::to_string(&doc)
        .map_err(|e| CliError::Internal(format!("re-serializing template body: {e}")))?;
    Ok(MaterializedConfig {
        template_id: record.id.clone(),
        version: record.version,
        name: record.name.clone(),
        body,
        params_redacted: bound.redacted(),
        used_secret_params: bound.has_secrets(),
    })
}

/// Connect a template store from a URL: `memory`, `sqlite:<path>`, or a
/// `postgres://…` URL. Same grammar (and same build-feature requirements) as
/// `catalog.url` and `faucet serve --history`, so one store can hold run
/// history, the dataset catalog, and the template registry together.
pub async fn resolve_store_url(url: &str) -> CliResult<TemplateStore> {
    let backend = match url {
        "memory" => HistoryBackendSpec::Memory,
        u if u.starts_with("postgres://") || u.starts_with("postgresql://") => {
            HistoryBackendSpec::Postgres(u.to_string())
        }
        u if u.starts_with("sqlite:") => HistoryBackendSpec::Sqlite(u.to_string()),
        other => {
            return Err(CliError::Config(format!(
                "template store '{other}' is not recognised — expected 'memory', \
                 'sqlite:<path>', or a 'postgres://…' URL"
            )));
        }
    };
    history::connect(
        &backend,
        // Idempotency claims and run leases are run-history concerns; a
        // template-only connection never uses them.
        Duration::from_secs(3600),
        Duration::from_secs(30),
        &uuid::Uuid::now_v7().to_string(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;
    use serde_json::json;

    fn store() -> TemplateStore {
        Arc::new(MemoryHistory::new(Duration::from_secs(60))) as TemplateStore
    }

    const PARAMETERIZED: &str = "\
version: 1
name: tenant-sync
params:
  tenant_id: { required: true, description: Tenant to sync }
  since: { default: \"1970-01-01\" }
  page: { type: int, default: 100 }
pipeline:
  source:
    type: rest
    config:
      url: \"https://api.example.com/${param.tenant_id}/events?since=${param.since}\"
  sink:
    type: jsonl
    config:
      path: ./out.jsonl
";

    fn req(body: &str) -> RegisterRequest {
        RegisterRequest {
            id: None,
            body: body.to_string(),
            format: ConfigFormat::Yaml,
            description: Some("test".into()),
            tags: Vec::new(),
            created_by: Some("tester".into()),
        }
    }

    #[tokio::test]
    async fn registers_and_versions() {
        let s = store();
        let first = register(&s, req(PARAMETERIZED)).await.unwrap();
        assert_eq!(first.id, "tenant-sync");
        assert_eq!(first.version, 1);
        assert_eq!(first.created_by.as_deref(), Some("tester"));
        // The declared param surface is extracted and stored.
        assert!(first.params["tenant_id"].required);
        assert_eq!(first.params["page"].default, Some(json!(100)));
        // Body stored verbatim.
        assert_eq!(first.body, PARAMETERIZED);

        let second = register(&s, req(PARAMETERIZED)).await.unwrap();
        assert_eq!(second.version, 2);
        // `template_get` with no version returns the newest.
        assert_eq!(
            s.template_get("tenant-sync", None)
                .await
                .unwrap()
                .unwrap()
                .version,
            2
        );
        assert_eq!(
            s.template_get("tenant-sync", Some(1))
                .await
                .unwrap()
                .unwrap()
                .version,
            1
        );
        assert_eq!(
            s.template_versions("tenant-sync").await.unwrap(),
            vec![2, 1]
        );
        let listed = s.template_list().await.unwrap();
        assert_eq!(listed.len(), 1, "list folds to the latest version per id");
        assert_eq!(listed[0].version, 2);
    }

    #[tokio::test]
    async fn explicit_id_wins_and_is_validated() {
        let s = store();
        let mut r = req(PARAMETERIZED);
        r.id = Some("my-template".into());
        assert_eq!(register(&s, r).await.unwrap().id, "my-template");

        let mut bad = req(PARAMETERIZED);
        bad.id = Some("Bad Id".into());
        assert!(register(&s, bad).await.is_err());
    }

    #[tokio::test]
    async fn register_requires_an_id_source() {
        let s = store();
        // No `name:` and no explicit id.
        let body = "version: 1\npipeline:\n  source: { type: csv, config: { path: a.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n";
        let err = register(&s, req(body)).await.unwrap_err().to_string();
        assert!(err.contains("no template id"), "{err}");
    }

    #[tokio::test]
    async fn register_rejects_a_structurally_invalid_config() {
        let s = store();
        // Unknown connector kinds are caught by expand's registry lookup... but
        // an unknown *field* is caught by the typed parse, which is the cheaper
        // and more common failure. Either way registration must fail.
        let err = register(&s, req("version: 1\nname: x\nnope: 1\npipeline: {}\n"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope") || err.contains("pipeline"), "{err}");
    }

    #[tokio::test]
    async fn register_rejects_an_invalid_params_block() {
        let s = store();
        let body = "version: 1\nname: x\nparams:\n  a: { required: true, default: 1 }\npipeline:\n  source: { type: csv, config: { path: a.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n";
        let err = register(&s, req(body)).await.unwrap_err().to_string();
        assert!(err.contains("required"), "{err}");
    }

    #[tokio::test]
    async fn register_rejects_a_non_mapping_body() {
        let s = store();
        let err = register(&s, req("- a\n- b\n"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("mapping"), "{err}");
        let err = register(&s, req(": :\n")).await.unwrap_err().to_string();
        assert!(err.contains("YAML"), "{err}");
    }

    #[tokio::test]
    async fn materialize_binds_params_and_defaults() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        let supplied: SuppliedParams = [("tenant_id".to_string(), json!("acme"))].into();
        let out = materialize(&s, "tenant-sync", None, &supplied, &BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(out.version, 1);
        assert_eq!(out.name.as_deref(), Some("tenant-sync"));
        assert_eq!(out.format(), ConfigFormat::Json);
        let doc: Value = serde_json::from_str(&out.body).unwrap();
        assert_eq!(
            doc["pipeline"]["source"]["config"]["url"],
            "https://api.example.com/acme/events?since=1970-01-01"
        );
        assert_eq!(out.params_redacted["tenant_id"], json!("acme"));
        assert_eq!(out.params_redacted["page"], json!(100));
        assert!(!out.used_secret_params);
    }

    #[tokio::test]
    async fn materialize_reports_missing_and_unknown_params() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        let err = materialize(
            &s,
            "tenant-sync",
            None,
            &SuppliedParams::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::MissingParam { .. }), "{err:?}");

        let supplied: SuppliedParams = [
            ("tenant_id".to_string(), json!("a")),
            ("bogus".to_string(), json!("b")),
        ]
        .into();
        let err = materialize(&s, "tenant-sync", None, &supplied, &BTreeMap::new())
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::UnknownParam { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn materialize_reports_unknown_template_and_version() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        let err = materialize(&s, "nope", None, &SuppliedParams::new(), &BTreeMap::new())
            .await
            .unwrap_err();
        assert!(
            matches!(err, CliError::UnknownPipelineTemplate { ref id, .. } if id == "nope"),
            "{err:?}"
        );
        let supplied: SuppliedParams = [("tenant_id".to_string(), json!("a"))].into();
        let err = materialize(&s, "tenant-sync", Some(9), &supplied, &BTreeMap::new())
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                CliError::UnknownPipelineTemplate {
                    version: Some(9),
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn env_overrides_win_over_the_process_environment() {
        let s = store();
        let body = "\
version: 1
name: env-template
pipeline:
  source: { type: rest, config: { url: \"https://x/${env:FAUCET_TPL_REGION}\" } }
  sink: { type: jsonl, config: { path: ./o.jsonl } }
";
        unsafe { std::env::set_var("FAUCET_TPL_REGION", "from-process") };
        register(&s, req(body)).await.unwrap();

        // No override → the process environment.
        let out = materialize(
            &s,
            "env-template",
            None,
            &SuppliedParams::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
        let doc: Value = serde_json::from_str(&out.body).unwrap();
        assert_eq!(
            doc["pipeline"]["source"]["config"]["url"],
            "https://x/from-process"
        );

        // Override wins, without mutating the process environment.
        let overrides: BTreeMap<String, String> =
            [("FAUCET_TPL_REGION".to_string(), "from-request".to_string())].into();
        let out = materialize(&s, "env-template", None, &SuppliedParams::new(), &overrides)
            .await
            .unwrap();
        let doc: Value = serde_json::from_str(&out.body).unwrap();
        assert_eq!(
            doc["pipeline"]["source"]["config"]["url"],
            "https://x/from-request"
        );
        assert_eq!(std::env::var("FAUCET_TPL_REGION").unwrap(), "from-process");
        unsafe { std::env::remove_var("FAUCET_TPL_REGION") };
    }

    #[tokio::test]
    async fn secret_params_are_flagged_and_redacted() {
        let s = store();
        let body = "\
version: 1
name: secret-template
params:
  api_token: { required: true, secret: true }
pipeline:
  source:
    type: rest
    config:
      url: https://api.example.com/events
      auth: { type: bearer, config: { token: \"${param.api_token}\" } }
  sink: { type: jsonl, config: { path: ./o.jsonl } }
";
        register(&s, req(body)).await.unwrap();
        let supplied: SuppliedParams =
            [("api_token".to_string(), json!("tok-abcdefghijklmnop"))].into();
        let out = materialize(&s, "secret-template", None, &supplied, &BTreeMap::new())
            .await
            .unwrap();
        assert!(out.used_secret_params);
        assert_eq!(out.params_redacted["api_token"], json!("***"));
        // The value IS in the materialized body (the pipeline needs it) but is
        // registered for redaction, so it can never reach a log or an API error.
        assert!(out.body.contains("tok-abcdefghijklmnop"));
        assert_eq!(
            crate::secrets::registry::redact("token=tok-abcdefghijklmnop"),
            "token=***"
        );
    }

    #[tokio::test]
    async fn delete_removes_one_version_or_all() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        assert_eq!(s.template_delete("tenant-sync", Some(1)).await.unwrap(), 1);
        assert_eq!(s.template_versions("tenant-sync").await.unwrap(), vec![2]);
        assert_eq!(s.template_delete("tenant-sync", None).await.unwrap(), 1);
        assert!(s.template_list().await.unwrap().is_empty());
        // Deleting what isn't there is 0, not an error.
        assert_eq!(s.template_delete("tenant-sync", None).await.unwrap(), 0);
        assert_eq!(s.template_delete("tenant-sync", Some(3)).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn channels_point_at_versions_and_promote() {
        let s = store();
        // Register three versions; `dev` rides the newest via `--tag` on register.
        register(&s, req(PARAMETERIZED)).await.unwrap();
        let mut with_tag = req(PARAMETERIZED);
        with_tag.tags = vec![VersionChannel::Dev];
        let v2 = register(&s, with_tag).await.unwrap();
        assert_eq!(v2.version, 2);
        register(&s, req(PARAMETERIZED)).await.unwrap(); // v3, untagged

        // `dev` still names v2 even though v3 exists — that is the whole point.
        assert_eq!(
            resolve_version(
                &s,
                "tenant-sync",
                VersionSelector::Channel(VersionChannel::Dev)
            )
            .await
            .unwrap(),
            Some(2)
        );
        // `latest` stays derived → `None`, i.e. "whatever is newest".
        assert_eq!(
            resolve_version(&s, "tenant-sync", VersionSelector::latest())
                .await
                .unwrap(),
            None
        );

        // Promote `prod` to an exact version, then move it by copying another
        // channel's current target.
        assert_eq!(
            promote(
                &s,
                "tenant-sync",
                VersionChannel::Prod,
                VersionSelector::Pinned(1)
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            promote(
                &s,
                "tenant-sync",
                VersionChannel::Prod,
                VersionSelector::Channel(VersionChannel::Dev)
            )
            .await
            .unwrap(),
            2,
            "promoting from another channel copies its current target"
        );
        // Promoting from `latest` resolves to the concrete newest, so the pointer
        // does not silently follow future registrations.
        assert_eq!(
            promote(
                &s,
                "tenant-sync",
                VersionChannel::Stable,
                VersionSelector::latest()
            )
            .await
            .unwrap(),
            3
        );
        let tags = s.template_tags("tenant-sync").await.unwrap();
        assert_eq!(tags["dev"], 2);
        assert_eq!(tags["prod"], 2);
        assert_eq!(tags["stable"], 3);
        assert!(
            !tags.contains_key("latest"),
            "latest is derived, never stored"
        );
    }

    #[tokio::test]
    async fn latest_cannot_be_assigned_and_unknown_channels_are_rejected() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();

        let err = promote(
            &s,
            "tenant-sync",
            VersionChannel::Latest,
            VersionSelector::Pinned(1),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("derived"), "{err}");

        // Same guard on the register path, before anything is written.
        let mut bad = req(PARAMETERIZED);
        bad.tags = vec![VersionChannel::Latest];
        assert!(register(&s, bad).await.is_err());
        assert_eq!(
            s.template_versions("tenant-sync").await.unwrap(),
            vec![1],
            "the rejected register must not have appended a version"
        );

        // An unset channel is an error naming what *is* set, not a silent
        // fallback to latest — running the wrong code is the failure to avoid.
        let err = resolve_version(
            &s,
            "tenant-sync",
            VersionSelector::Channel(VersionChannel::Prod),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("no `prod` version"), "{err}");
        assert!(err.contains("(none)"), "{err}");
    }

    #[tokio::test]
    async fn promoting_to_a_missing_version_is_rejected() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        let err = promote(
            &s,
            "tenant-sync",
            VersionChannel::Prod,
            VersionSelector::Pinned(9),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                CliError::UnknownPipelineTemplate {
                    version: Some(9),
                    ..
                }
            ),
            "{err:?}"
        );
        // An unknown template id is likewise a typed error.
        assert!(matches!(
            promote(&s, "nope", VersionChannel::Prod, VersionSelector::latest())
                .await
                .unwrap_err(),
            CliError::UnknownPipelineTemplate { .. }
        ));
    }

    #[tokio::test]
    async fn deleting_a_version_drops_channels_aimed_at_it() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        promote(
            &s,
            "tenant-sync",
            VersionChannel::Prod,
            VersionSelector::Pinned(1),
        )
        .await
        .unwrap();
        promote(
            &s,
            "tenant-sync",
            VersionChannel::Dev,
            VersionSelector::Pinned(2),
        )
        .await
        .unwrap();

        assert_eq!(s.template_delete("tenant-sync", Some(1)).await.unwrap(), 1);
        let tags = s.template_tags("tenant-sync").await.unwrap();
        assert!(
            !tags.contains_key("prod"),
            "a channel must never dangle at a deleted version: {tags:?}"
        );
        assert_eq!(tags["dev"], 2);

        // Deleting the whole template clears the rest.
        s.template_delete("tenant-sync", None).await.unwrap();
        assert!(s.template_tags("tenant-sync").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn materialize_follows_a_promoted_channel() {
        let s = store();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        register(&s, req(PARAMETERIZED)).await.unwrap();
        promote(
            &s,
            "tenant-sync",
            VersionChannel::Prod,
            VersionSelector::Pinned(1),
        )
        .await
        .unwrap();

        let supplied: SuppliedParams = [("tenant_id".to_string(), json!("acme"))].into();
        let pinned = resolve_version(
            &s,
            "tenant-sync",
            VersionSelector::Channel(VersionChannel::Prod),
        )
        .await
        .unwrap();
        let out = materialize(&s, "tenant-sync", pinned, &supplied, &BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(out.version, 1, "prod still points at v1");
    }

    #[tokio::test]
    async fn store_url_grammar() {
        assert!(resolve_store_url("memory").await.is_ok());
        // `RunHistory` is not Debug, so match rather than `unwrap_err`.
        match resolve_store_url("mysql://nope").await {
            Ok(_) => panic!("an unrecognised scheme must be rejected"),
            Err(e) => assert!(e.to_string().contains("template store"), "{e}"),
        }
        // SQL schemes are recognised even without the build feature — the error
        // then names the missing feature rather than the URL grammar.
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}", dir.path().join("t.db").display());
        if let Err(e) = resolve_store_url(&url).await {
            assert!(e.to_string().contains("serve-history-sqlite"), "{e}");
        }
    }

    #[tokio::test]
    async fn registers_a_topology_config() {
        let s = store();
        let body = "\
version: 1
name: topo-template
params:
  path: { default: ./in.csv }
pipeline:
  sources:
    s: { type: csv, config: { path: \"${param.path}\" } }
  sinks:
    o: { type: jsonl, config: { path: ./out.jsonl } }
  nodes:
    src: { kind: source, ref: s }
    w: { kind: sink, ref: o }
  edges:
    - { from: src, to: w }
";
        let rec = register(&s, req(body)).await.unwrap();
        assert_eq!(rec.id, "topo-template");
        let out = materialize(
            &s,
            "topo-template",
            None,
            &SuppliedParams::new(),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
        let doc: Value = serde_json::from_str(&out.body).unwrap();
        assert_eq!(
            doc["pipeline"]["sources"]["s"]["config"]["path"],
            "./in.csv"
        );
    }

    #[tokio::test]
    async fn version_history_is_bounded() {
        use crate::serve::history::templates::VERSION_RETAIN;
        let s = store();
        for _ in 0..(VERSION_RETAIN + 3) {
            register(&s, req(PARAMETERIZED)).await.unwrap();
        }
        let versions = s.template_versions("tenant-sync").await.unwrap();
        assert_eq!(versions.len(), VERSION_RETAIN);
        assert_eq!(versions[0], (VERSION_RETAIN + 3) as u32, "newest kept");
        assert!(!versions.contains(&1), "oldest pruned");
    }
}
