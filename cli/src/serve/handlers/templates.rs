//! `/v1/templates*` — the pipeline template registry + parameterized trigger
//! API (#444).
//!
//! Thin adapters over [`crate::templates`]: deserialize, call, map to a status
//! code. Registration/read/delete need the `TemplateWrite` / `TemplateRead`
//! permissions; triggering a run needs **both** `TemplateRead` (to resolve the
//! template) and `RunWrite` (to start a run), and then flows through the very
//! same [`crate::serve::runner::submit`] as `POST /v1/runs` — so idempotency
//! keys, `doctor_first`, queue limits, cluster dispatch, metrics, and the audit
//! log all behave identically.

use crate::params::SuppliedParams;
use crate::serve::error::ServeError;
use crate::serve::history::templates::{
    TemplateRecord, TemplateSummary, VersionChannel, VersionSelector,
};
use crate::serve::rbac::AuthContext;
use crate::serve::runner::{self, ConfigFormatWire, SubmitRequest, SubmitResponse};
use crate::serve::state::ServerState;
use crate::templates::{RegisterRequest, TemplateStore};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Map a `CliError` from the templates layer onto an HTTP status. A missing
/// template is a 404; anything the caller could have sent differently is a 422;
/// a registry failure is a 500.
fn map_err(e: crate::error::CliError) -> ServeError {
    use crate::error::CliError;
    match e {
        CliError::UnknownPipelineTemplate { .. } => ServeError::NotFound,
        CliError::Internal(m) => ServeError::Internal(m),
        other => ServeError::Unprocessable {
            message: other.to_string(),
            details: None,
        },
    }
}

/// The server's own run-history backend doubles as the template registry, so a
/// `--history sqlite:…`/`postgres://…` server persists templates across
/// restarts and shares them across a cluster.
fn store(state: &ServerState) -> TemplateStore {
    state.history()
}

// ── POST /v1/templates ──────────────────────────────────────────────────────

/// `POST /v1/templates` request body.
#[derive(Debug, Deserialize)]
pub struct RegisterBody {
    /// Registry id. Derived from the config's `name:` when omitted.
    #[serde(default)]
    pub id: Option<String>,
    /// The config document, stored verbatim.
    pub config: String,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    #[serde(default)]
    pub description: Option<String>,
    /// Named channels to point at the newly registered version (`dev`,
    /// `pre-prod`, …). The version number always auto-increments; these are the
    /// pointers moved onto it. `latest` is derived and rejected.
    #[serde(default)]
    pub tags: Vec<VersionChannel>,
}

/// `POST /v1/templates` → 201 with the newly registered version's summary.
pub async fn register_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(body): Json<RegisterBody>,
) -> Result<(StatusCode, Json<TemplateSummary>), ServeError> {
    let record = crate::templates::register(
        &store(&state),
        RegisterRequest {
            id: body.id,
            body: body.config,
            format: body.config_format.into(),
            description: body.description,
            tags: body.tags,
            created_by: Some(actor.principal.clone()),
        },
    )
    .await
    .map_err(map_err)?;

    // `config_fingerprint` carries the sha256 of the registered document — a
    // genuine config fingerprint, and a stable identifier for exactly what was
    // stored. Which template/version it became is in the structured log line
    // below and, durably, in the registry record's own `created_by`/`created_at`.
    let fingerprint = crate::serve::idempotency::fingerprint(
        &serde_json::Value::String(record.body.clone()),
        record.name.as_deref(),
    );
    tracing::info!(
        principal = %actor.principal,
        template = %record.id,
        version = record.version,
        "registered pipeline template"
    );
    crate::serve::audit::write(
        &state,
        &actor,
        "template.register",
        None,
        Some(fingerprint),
        "ok",
    )
    .await;
    Ok((StatusCode::CREATED, Json(record.summary())))
}

// ── GET /v1/templates ───────────────────────────────────────────────────────

/// `GET /v1/templates` response body.
#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub templates: Vec<TemplateSummary>,
}

/// `GET /v1/templates` → 200. Latest version of each registered template.
pub async fn list_templates(
    State(state): State<ServerState>,
) -> Result<Json<ListResponse>, ServeError> {
    let templates = store(&state)
        .template_list()
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    Ok(Json(ListResponse { templates }))
}

// ── GET /v1/templates/{id} ──────────────────────────────────────────────────

/// Optional `?version=` selector shared by get + delete. Accepts `latest` (the
/// default when omitted) or an exact version number.
#[derive(Debug, Default, Deserialize)]
pub struct VersionQuery {
    #[serde(default)]
    pub version: Option<VersionSelector>,
}

impl VersionQuery {
    /// The selector to act on, defaulting to `latest`.
    fn selector(&self) -> VersionSelector {
        self.version.unwrap_or_default()
    }
}

/// `GET /v1/templates/{id}` response body: one version plus the version list and
/// which of them is `latest`, so a client can pin or roll back without a second
/// request.
#[derive(Debug, Serialize)]
pub struct GetResponse {
    #[serde(flatten)]
    pub template: TemplateRecord,
    /// Every stored version, newest first.
    pub versions: Vec<u32>,
    /// The version the `latest` tag resolves to right now.
    pub latest_version: u32,
    /// Whether the returned version *is* the latest (false ⇒ a pinned older one).
    pub is_latest: bool,
    /// Named channel pointers for this template (`{tag: version}`). Excludes the
    /// derived `latest`, which is always `latest_version`.
    pub tags: BTreeMap<String, u32>,
}

/// `GET /v1/templates/{id}[?version=N]` → 200 / 404.
pub async fn get_template(
    State(state): State<ServerState>,
    Path(id): Path<String>,
    Query(q): Query<VersionQuery>,
) -> Result<Json<GetResponse>, ServeError> {
    let s = store(&state);
    let want = crate::templates::resolve_version(&s, &id, q.selector())
        .await
        .map_err(map_err)?;
    let template = s
        .template_get(&id, want)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    let versions = s
        .template_versions(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    let tags = s
        .template_tags(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    // `template_versions` is newest-first, and the record above exists, so the
    // list is non-empty; fall back to the record's own version defensively.
    let latest_version = versions.first().copied().unwrap_or(template.version);
    let is_latest = template.version == latest_version;
    Ok(Json(GetResponse {
        template,
        versions,
        latest_version,
        is_latest,
        tags,
    }))
}

// ── DELETE /v1/templates/{id} ───────────────────────────────────────────────

/// `DELETE /v1/templates/{id}[?version=N]` → 204 / 404. Without `version` every
/// version of the template is removed. Runs already produced by the template are
/// untouched — their records stand on their own.
pub async fn delete_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Query(q): Query<VersionQuery>,
) -> Result<StatusCode, ServeError> {
    let s = store(&state);
    // Unlike `GET`, an omitted selector and `latest` mean *different* things
    // here: no selector deletes the whole template, `latest` deletes only its
    // newest version. So `latest` must be resolved to a number first rather than
    // collapsing to the "all versions" `None`.
    let target = match q.version {
        None => None,
        Some(selector) => Some(
            match crate::templates::resolve_version(&s, &id, selector)
                .await
                .map_err(map_err)?
            {
                Some(v) => v,
                // `latest` resolves to "newest" — turn that into a concrete
                // number so the delete removes one version, not all of them.
                None => s
                    .template_versions(&id)
                    .await
                    .map_err(|e| ServeError::Internal(e.to_string()))?
                    .first()
                    .copied()
                    // Nothing stored → fall through to a 0-row delete, i.e. 404.
                    .unwrap_or(u32::MAX),
            },
        ),
    };
    let removed = s
        .template_delete(&id, target)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    if removed == 0 {
        return Err(ServeError::NotFound);
    }
    tracing::info!(
        principal = %actor.principal,
        template = %id,
        version = ?target,
        removed,
        "deleted pipeline template version(s)"
    );
    crate::serve::audit::write(&state, &actor, "template.delete", None, None, "ok").await;
    Ok(StatusCode::NO_CONTENT)
}

// ── POST /v1/templates/{id}/tags ────────────────────────────────────────────

/// `POST /v1/templates/{id}/tags` request body: point a named channel at a
/// version.
#[derive(Debug, Deserialize)]
pub struct PromoteBody {
    /// Channel to move — one of the closed set, and never the derived `latest`.
    pub tag: VersionChannel,
    /// Where to point it: a version number, or another channel whose current
    /// target should be copied (`{"tag":"prod","version":"stable"}` promotes
    /// whatever `stable` names today). Defaults to `latest`.
    #[serde(default)]
    pub version: Option<VersionSelector>,
}

/// `POST /v1/templates/{id}/tags` response body.
#[derive(Debug, Serialize)]
pub struct PromoteResponse {
    pub id: String,
    pub tag: String,
    /// The concrete version the channel now points at.
    pub version: u32,
}

/// `POST /v1/templates/{id}/tags` → 200 / 404 / 422.
///
/// Promotion is the whole point of the named channels: versions themselves are
/// immutable and auto-incrementing, and this is how `prod` moves from v3 to v4.
pub async fn promote_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(body): Json<PromoteBody>,
) -> Result<Json<PromoteResponse>, ServeError> {
    let version = crate::templates::promote(
        &store(&state),
        &id,
        body.tag,
        body.version.unwrap_or_default(),
    )
    .await
    .map_err(map_err)?;
    tracing::info!(
        principal = %actor.principal,
        template = %id,
        tag = body.tag.as_str(),
        version,
        "promoted pipeline template channel"
    );
    crate::serve::audit::write(&state, &actor, "template.promote", None, None, "ok").await;
    Ok(Json(PromoteResponse {
        id,
        tag: body.tag.as_str().to_string(),
        version,
    }))
}

// ── POST /v1/templates/{id}/runs ────────────────────────────────────────────

/// `POST /v1/templates/{id}/runs` request body. Everything after `params`/`env`
/// mirrors `POST /v1/runs`, because the run is submitted through the same path.
#[derive(Debug, Default, Deserialize)]
pub struct TriggerBody {
    /// Values for the template's declared `params:`.
    #[serde(default)]
    pub params: BTreeMap<String, Value>,
    /// Values that win over the server's environment for `${env:VAR}` during
    /// this materialization only.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Version to run: a number, or a named channel (`"latest"` — the default
    /// when omitted — `"prod"`, `"pre-prod"`, `"dev"`, …).
    #[serde(default)]
    pub version: Option<VersionSelector>,
    /// Run name override (default: the template's config `name:`).
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub doctor_first: bool,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub clock: Option<String>,
}

/// `POST /v1/templates/{id}/runs` success body (202): the ordinary submit
/// response plus which template version produced it and the (redacted) params
/// it was bound with.
#[derive(Debug, Serialize)]
pub struct TriggerResponse {
    #[serde(flatten)]
    pub run: SubmitResponse,
    pub template_id: String,
    pub template_version: u32,
    /// Bound params with every `secret: true` value replaced by `"***"`.
    pub params: BTreeMap<String, Value>,
}

/// Label keys stamped on a template-triggered run, so `GET /v1/runs` can filter
/// by provenance.
const LABEL_TEMPLATE: &str = "template";
const LABEL_TEMPLATE_VERSION: &str = "template_version";

/// `POST /v1/templates/{id}/runs` → 202 / 404 / 422 / 429.
pub async fn trigger_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(body): Json<TriggerBody>,
) -> Result<(StatusCode, Json<TriggerResponse>), ServeError> {
    let supplied: SuppliedParams = body.params.into_iter().collect();
    let materialized = crate::templates::materialize(
        &store(&state),
        &id,
        body.version.unwrap_or_default().pinned(),
        &supplied,
        &body.env,
    )
    .await
    .map_err(map_err)?;

    // A clustered submit persists the raw config so any instance can re-run it.
    // A `secret: true` param value would therefore be written to the shared
    // history database — which is deliberately never a secret store. Refuse
    // instead, and point at the two safe ways to get a secret into a clustered
    // template run.
    if state.cluster().enabled() && materialized.used_secret_params {
        return Err(ServeError::Unprocessable {
            message: "this template declares `secret: true` param(s), and a clustered server \
                      persists the materialized config so a peer can execute it — which would \
                      store the secret in the shared run-history database. Reference the secret \
                      from the template body instead (`${env:VAR}` or `${vault:…}`, resolved on \
                      the executing instance), or trigger it on a non-clustered server"
                .into(),
            details: None,
        });
    }

    let mut labels = body.labels;
    labels.insert(LABEL_TEMPLATE.into(), materialized.template_id.clone());
    labels.insert(
        LABEL_TEMPLATE_VERSION.into(),
        materialized.version.to_string(),
    );

    let req = SubmitRequest {
        config: materialized.body.clone(),
        config_format: ConfigFormatWire::Json,
        name: body.name.or_else(|| materialized.name.clone()),
        labels,
        timeout_secs: body.timeout_secs,
        doctor_first: body.doctor_first,
        idempotency_key: body.idempotency_key,
        clock: body.clock,
    };
    let run = runner::submit(state.clone(), req, actor.clone()).await?;
    // `submit` already recorded `run.submit`; this second entry attributes the
    // *trigger* specifically, and its `run_id` links to the run record whose
    // `template` / `template_version` labels name the version used.
    crate::serve::audit::write(
        &state,
        &actor,
        "template.run",
        Some(run.run_id.clone()),
        None,
        "ok",
    )
    .await;
    Ok((
        StatusCode::ACCEPTED,
        Json(TriggerResponse {
            run,
            template_id: materialized.template_id,
            template_version: materialized.version,
            params: materialized.params_redacted,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::AuditFilter;
    use crate::serve::rbac::Role;
    use crate::serve::test_support::test_state;
    use serde_json::json;

    fn actor() -> AuthContext {
        AuthContext {
            principal: "tester".into(),
            role: Role::Admin,
            source_ip: None,
        }
    }

    fn template_yaml(out: &std::path::Path) -> String {
        format!(
            "version: 1\nname: tpl-demo\nparams:\n  tag: {{ required: true }}\n  page: {{ type: int, default: 7 }}\npipeline:\n  source:\n    type: csv\n    config:\n      path: ./missing-${{param.tag}}.csv\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            out.display()
        )
    }

    async fn register_demo(state: &ServerState, out: &std::path::Path) -> TemplateSummary {
        register_template(
            State(state.clone()),
            Extension(actor()),
            Json(RegisterBody {
                id: None,
                config: template_yaml(out),
                config_format: ConfigFormatWire::Yaml,
                description: Some("demo".into()),
                tags: vec![],
            }),
        )
        .await
        .expect("register")
        .1
        .0
    }

    #[tokio::test]
    async fn register_list_get_delete_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        let summary = register_demo(&state, &dir.path().join("o.jsonl")).await;
        assert_eq!(summary.id, "tpl-demo");
        assert_eq!(summary.version, 1);
        assert_eq!(summary.created_by.as_deref(), Some("tester"));
        assert!(summary.params["tag"].required);

        let listed = list_templates(State(state.clone())).await.unwrap().0;
        assert_eq!(listed.templates.len(), 1);

        let got = get_template(
            State(state.clone()),
            Path("tpl-demo".into()),
            Query(VersionQuery::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(got.template.version, 1);
        assert_eq!(got.versions, vec![1]);
        // The stored body is verbatim, so the interpolation token survives.
        assert!(got.template.body.contains("${param.tag}"));

        // 404s.
        assert!(matches!(
            get_template(
                State(state.clone()),
                Path("nope".into()),
                Query(VersionQuery::default())
            )
            .await,
            Err(ServeError::NotFound)
        ));
        assert!(matches!(
            get_template(
                State(state.clone()),
                Path("tpl-demo".into()),
                Query(VersionQuery {
                    version: Some(VersionSelector::Pinned(9))
                })
            )
            .await,
            Err(ServeError::NotFound)
        ));

        let code = delete_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Query(VersionQuery::default()),
        )
        .await
        .unwrap();
        assert_eq!(code, StatusCode::NO_CONTENT);
        assert!(matches!(
            delete_template(
                State(state.clone()),
                Extension(actor()),
                Path("tpl-demo".into()),
                Query(VersionQuery::default())
            )
            .await,
            Err(ServeError::NotFound)
        ));

        // Both mutations were audited.
        let entries = state
            .history()
            .list_audit(&AuditFilter {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        let actions: Vec<&str> = entries.iter().map(|e| e.action.as_str()).collect();
        assert!(actions.contains(&"template.register"), "{actions:?}");
        assert!(actions.contains(&"template.delete"), "{actions:?}");
    }

    #[tokio::test]
    async fn register_rejects_an_invalid_config() {
        let state = test_state();
        let err = register_template(
            State(state),
            Extension(actor()),
            Json(RegisterBody {
                id: None,
                config: "version: 1\nname: x\nbogus_key: 1\npipeline: {}\n".into(),
                config_format: ConfigFormatWire::Yaml,
                description: None,
                tags: vec![],
            }),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::Unprocessable { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn trigger_binds_params_and_stamps_provenance() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("o.jsonl")).await;

        let (code, resp) = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(TriggerBody {
                params: [("tag".to_string(), json!("alpha"))].into(),
                ..Default::default()
            }),
        )
        .await
        .expect("trigger");
        assert_eq!(code, StatusCode::ACCEPTED);
        assert_eq!(resp.0.template_id, "tpl-demo");
        assert_eq!(resp.0.template_version, 1);
        assert_eq!(resp.0.params["tag"], json!("alpha"));
        assert_eq!(resp.0.params["page"], json!(7));

        // The run carries the template provenance labels and the config name.
        let rec = state
            .history()
            .get(&resp.0.run.run_id)
            .await
            .unwrap()
            .expect("run record");
        assert_eq!(rec.labels[LABEL_TEMPLATE], "tpl-demo");
        assert_eq!(rec.labels[LABEL_TEMPLATE_VERSION], "1");
        assert_eq!(rec.name.as_deref(), Some("tpl-demo"));

        // The trigger is audited in its own right, linked to the run it started.
        let entries = state
            .history()
            .list_audit(&AuditFilter {
                action: Some("template.run".into()),
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].run_id.as_deref(),
            Some(resp.0.run.run_id.as_str())
        );
    }

    #[tokio::test]
    async fn trigger_reports_missing_params_and_unknown_template() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("o.jsonl")).await;

        // `tag` is required.
        let err = trigger_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Json(TriggerBody::default()),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("tag"), "{message}")
            }
            other => panic!("expected 422, got {other:?}"),
        }

        // Unknown id → 404.
        assert!(matches!(
            trigger_template(
                State(state.clone()),
                Extension(actor()),
                Path("nope".into()),
                Json(TriggerBody::default())
            )
            .await,
            Err(ServeError::NotFound)
        ));
    }

    #[tokio::test]
    async fn secret_params_are_refused_on_a_clustered_server() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state_clustered();
        let body = format!(
            "version: 1\nname: tpl-secret\nparams:\n  token: {{ required: true, secret: true }}\npipeline:\n  source:\n    type: csv\n    config:\n      path: ./x-${{param.token}}.csv\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            dir.path().join("o.jsonl").display()
        );
        let _registered = register_template(
            State(state.clone()),
            Extension(actor()),
            Json(RegisterBody {
                id: None,
                config: body,
                config_format: ConfigFormatWire::Yaml,
                description: None,
                tags: vec![],
            }),
        )
        .await
        .expect("register");

        let err = trigger_template(
            State(state),
            Extension(actor()),
            Path("tpl-secret".into()),
            Json(TriggerBody {
                params: [("token".to_string(), json!("super-secret-value"))].into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        match err {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.contains("clustered"), "{message}");
                assert!(!message.contains("super-secret-value"), "leaked: {message}");
            }
            other => panic!("expected 422, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn latest_tag_and_omitted_version_agree() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("v1.jsonl")).await;
        register_demo(&state, &dir.path().join("v2.jsonl")).await;
        register_demo(&state, &dir.path().join("v3.jsonl")).await;

        // Omitted, `latest`, and the explicit newest number are three spellings
        // of one thing.
        for q in [
            VersionQuery::default(),
            VersionQuery {
                version: Some(VersionSelector::latest()),
            },
            VersionQuery {
                version: Some(VersionSelector::Pinned(3)),
            },
        ] {
            let got = get_template(State(state.clone()), Path("tpl-demo".into()), Query(q))
                .await
                .unwrap()
                .0;
            assert_eq!(got.template.version, 3);
            assert_eq!(got.latest_version, 3);
            assert!(got.is_latest);
            assert!(got.template.body.contains("v3.jsonl"));
            assert_eq!(got.versions, vec![3, 2, 1]);
        }

        // A pinned older version reports itself as *not* latest, while still
        // naming what latest resolves to.
        let got = get_template(
            State(state.clone()),
            Path("tpl-demo".into()),
            Query(VersionQuery {
                version: Some(VersionSelector::Pinned(1)),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(got.template.version, 1);
        assert_eq!(got.latest_version, 3);
        assert!(!got.is_latest);

        // Triggering with `latest` runs the newest body; pinning runs the older.
        let trigger = |version: Option<VersionSelector>| {
            let state = state.clone();
            async move {
                trigger_template(
                    State(state),
                    Extension(actor()),
                    Path("tpl-demo".into()),
                    Json(TriggerBody {
                        params: [("tag".to_string(), json!("v"))].into(),
                        version,
                        ..Default::default()
                    }),
                )
                .await
                .expect("trigger")
                .1
                .0
            }
        };
        assert_eq!(trigger(None).await.template_version, 3);
        assert_eq!(
            trigger(Some(VersionSelector::latest()))
                .await
                .template_version,
            3
        );
        assert_eq!(
            trigger(Some(VersionSelector::Pinned(2)))
                .await
                .template_version,
            2
        );
    }

    #[tokio::test]
    async fn delete_latest_removes_one_version_but_omitted_removes_all() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("v1.jsonl")).await;
        register_demo(&state, &dir.path().join("v2.jsonl")).await;
        register_demo(&state, &dir.path().join("v3.jsonl")).await;

        // `?version=latest` peels off only the newest — the distinction that
        // makes it different from an omitted selector here.
        delete_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Query(VersionQuery {
                version: Some(VersionSelector::latest()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            state.history().template_versions("tpl-demo").await.unwrap(),
            vec![2, 1]
        );

        // No selector removes the whole template.
        delete_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Query(VersionQuery::default()),
        )
        .await
        .unwrap();
        assert!(state.history().template_list().await.unwrap().is_empty());

        // `latest` on a template that no longer exists is a 404, not a panic.
        assert!(matches!(
            delete_template(
                State(state.clone()),
                Extension(actor()),
                Path("tpl-demo".into()),
                Query(VersionQuery {
                    version: Some(VersionSelector::latest()),
                }),
            )
            .await,
            Err(ServeError::NotFound)
        ));
    }

    #[test]
    fn version_query_deserializes_latest_and_numbers() {
        // A query string decodes every value as a string, so that is the shape
        // that matters here; a JSON body may send a bare number.
        let cases: &[(Value, Option<u32>)] = &[
            (json!({}), None),
            (json!({ "version": "latest" }), None),
            (json!({ "version": "2" }), Some(2)),
            (json!({ "version": 2 }), Some(2)),
        ];
        for (wire, want) in cases {
            let q: VersionQuery = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(q.selector().pinned(), *want, "{wire}");
        }
        for bad in [json!({ "version": "nope" }), json!({ "version": 0 })] {
            assert!(
                serde_json::from_value::<VersionQuery>(bad.clone()).is_err(),
                "{bad} should be rejected"
            );
        }
    }

    #[tokio::test]
    async fn version_pinning_selects_an_older_body() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state();
        register_demo(&state, &dir.path().join("v1.jsonl")).await;
        let v2 = register_demo(&state, &dir.path().join("v2.jsonl")).await;
        assert_eq!(v2.version, 2);

        let got = get_template(
            State(state.clone()),
            Path("tpl-demo".into()),
            Query(VersionQuery {
                version: Some(VersionSelector::Pinned(1)),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(got.template.body.contains("v1.jsonl"));
        assert_eq!(got.versions, vec![2, 1]);

        // Deleting one version leaves the other.
        delete_template(
            State(state.clone()),
            Extension(actor()),
            Path("tpl-demo".into()),
            Query(VersionQuery {
                version: Some(VersionSelector::Pinned(1)),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            state.history().template_versions("tpl-demo").await.unwrap(),
            vec![2]
        );
    }
}
