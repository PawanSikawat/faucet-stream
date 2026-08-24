//! `/v1/local-outputs*` — list and reclaim the local files this server's sinks
//! wrote (#587).
//!
//! The control surface behind the Datasets page's cleanup controls (#588):
//!
//! | route | scope | permission |
//! |---|---|---|
//! | `GET /v1/local-outputs` | list, with age + state | `LocalOutputRead` (viewer+) |
//! | `DELETE /v1/local-outputs/{id}` | one output, now | `LocalOutputManage` (operator+) |
//! | `POST /v1/local-outputs/cleanup` | older-than-N / expired / all | `LocalOutputManage` |
//!
//! Every deletion goes through [`crate::local_outputs::sweep`], so the guardrail
//! is the same one the background sweeper obeys: **only paths the ledger records
//! as faucet's own sink outputs**, never a glob or a directory, and never a file
//! faucet merely appended to. There is no request body that can widen that —
//! `all` is the widest scope and still means "every *tracked* output".
//!
//! `clean_all` must be asked for explicitly (`{"all": true}`); the console pairs
//! that with a confirm step. Run history, catalog entries, and lineage are never
//! touched here.

use crate::local_outputs::{
    LocalOutputFilter, LocalOutputRecord, SweepReport, SweepScope,
    sweep::{self, SweepOptions},
};
use crate::serve::error::ServeError;
use crate::serve::rbac::{AuthContext, Permission};
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use chrono::Utc;
use serde::{Deserialize, Serialize};

const DEFAULT_LIMIT: usize = 200;
const MAX_LIMIT: usize = 2000;

/// `GET /v1/local-outputs` query string.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Only this dataset's outputs (catalog dataset id) — how the Datasets
    /// detail view scopes the list.
    pub dataset_id: Option<String>,
    /// Only this pipeline's outputs.
    pub pipeline: Option<String>,
    /// Include already-collected outputs, rendered as `expired`. Default `false`.
    #[serde(default)]
    pub include_expired: bool,
    pub limit: Option<usize>,
}

/// One row as the console sees it: the stored record plus the two derived fields
/// the UI would otherwise have to recompute (and get subtly wrong).
#[derive(Debug, Clone, Serialize)]
pub struct LocalOutputView {
    #[serde(flatten)]
    pub record: LocalOutputRecord,
    /// `present` / `expired` / `external`.
    pub state: &'static str,
    /// Age since the last write, in seconds — what the list sorts and labels by.
    pub age_secs: u64,
    /// The retention window actually in force for this row, in days. `null` =
    /// never expires (`retention_days: 0` at either level).
    pub retention_days_effective: Option<u32>,
}

/// `GET /v1/local-outputs` response body.
#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub outputs: Vec<LocalOutputView>,
    /// The server's default retention window, so the console can label its
    /// "purge older than N days" control with the configured default instead of
    /// hardcoding 7.
    pub retention_days: u32,
    /// Whether the background sweeper is running (`retention_days > 0`).
    pub gc_enabled: bool,
    /// Whether the **calling principal** may delete outputs
    /// (`LocalOutputManage`). The console reads this to decide whether to render
    /// the destructive controls at all, rather than offering a viewer buttons
    /// that can only ever 403.
    pub can_manage: bool,
}

/// `GET /v1/local-outputs` → 200.
pub async fn list_outputs(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ListResponse>, ServeError> {
    let retention_days = retention_days(&state);
    let filter = LocalOutputFilter {
        dataset_id: query.dataset_id,
        pipeline: query.pipeline,
        include_deleted: query.include_expired,
        limit: query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
    };
    let now = Utc::now();
    let outputs = state
        .history()
        .local_output_list(&filter)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .into_iter()
        .map(|record| LocalOutputView {
            state: record.state().as_str(),
            age_secs: record.age_secs(now),
            retention_days_effective: record.effective_retention_days(retention_days),
            record,
        })
        .collect();
    Ok(Json(ListResponse {
        outputs,
        retention_days,
        gc_enabled: retention_days > 0,
        can_manage: actor.role.grants(Permission::LocalOutputManage),
    }))
}

/// `DELETE /v1/local-outputs/{id}` → 200 with a one-output report, 404 if the
/// ledger has no such output.
///
/// The per-output "delete now". A refusal is a **200 with `deleted: 0`** and a
/// `skipped` reason, not an error: "faucet did not create this file so it will
/// not delete it" is an answer about the file, not a failure of the request, and
/// the console renders the reason next to the output.
pub async fn delete_output(
    State(state): State<ServerState>,
    Path(id): Path<String>,
) -> Result<Json<SweepReport>, ServeError> {
    // 404 before sweeping so an unknown id is distinguishable from a refusal.
    if state
        .history()
        .local_output_get(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .is_none()
    {
        return Err(ServeError::NotFound);
    }
    let report = sweep::run(
        state.history().as_ref(),
        &SweepScope::Output(id),
        &options(&state, false),
    )
    .await
    .map_err(|e| ServeError::Internal(e.to_string()))?;
    Ok(Json(report))
}

/// `POST /v1/local-outputs/cleanup` request body.
///
/// Exactly one scope must be chosen. They are separate optional fields rather
/// than an untagged enum so a client that sends nothing gets a clear 400 instead
/// of silently falling through to the widest interpretation.
#[derive(Debug, Default, Deserialize)]
pub struct CleanupRequest {
    /// Delete outputs older than this many days, regardless of per-pipeline
    /// retention overrides.
    #[serde(default)]
    pub older_than_days: Option<u32>,
    /// Delete outputs whose own retention window has elapsed — a manual run of
    /// what the background sweeper does.
    #[serde(default)]
    pub expired: bool,
    /// Delete **every** tracked output, including ones still inside their
    /// retention window. Must be set explicitly; the console confirms first.
    #[serde(default)]
    pub all: bool,
    /// Only this dataset's outputs. Combines with nothing else — it is its own
    /// scope (the Datasets detail view's "clean this dataset").
    #[serde(default)]
    pub dataset_id: Option<String>,
    /// Only the outputs one run most recently wrote — "clean up after that run".
    /// The run's history record is untouched.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Report what would be deleted without touching anything.
    #[serde(default)]
    pub dry_run: bool,
}

impl CleanupRequest {
    /// Resolve the request to exactly one scope, or explain what was wrong.
    fn scope(&self) -> Result<SweepScope, ServeError> {
        let chosen: Vec<SweepScope> = [
            self.older_than_days.map(SweepScope::OlderThanDays),
            self.expired.then_some(SweepScope::Expired),
            self.all.then_some(SweepScope::All),
            self.dataset_id.clone().map(SweepScope::Dataset),
            self.run_id.clone().map(SweepScope::Run),
        ]
        .into_iter()
        .flatten()
        .collect();
        match chosen.len() {
            1 => Ok(chosen.into_iter().next().expect("length checked")),
            0 => Err(ServeError::BadConfig(
                "cleanup: choose a scope — one of `older_than_days`, `expired`, \
                 `dataset_id`, `run_id`, or `all`"
                    .into(),
            )),
            // Refusing beats guessing: `{"all": true, "older_than_days": 30}`
            // could plausibly mean either, and one of the two readings deletes
            // far more than the caller expected.
            _ => Err(ServeError::BadConfig(
                "cleanup: `older_than_days`, `expired`, `dataset_id`, `run_id`, and `all` \
                 are mutually exclusive — send exactly one"
                    .into(),
            )),
        }
    }
}

/// `POST /v1/local-outputs/cleanup` → 200 with the sweep report.
pub async fn cleanup(
    State(state): State<ServerState>,
    Json(req): Json<CleanupRequest>,
) -> Result<Json<SweepReport>, ServeError> {
    let scope = req.scope()?;
    let report = sweep::run(
        state.history().as_ref(),
        &scope,
        &options(&state, req.dry_run),
    )
    .await
    .map_err(|e| ServeError::Internal(e.to_string()))?;
    Ok(Json(report))
}

/// The server's configured default retention window, in days (`0` = the
/// background sweep is off).
fn retention_days(state: &ServerState) -> u32 {
    state.local_output_retention_days()
}

/// Sweep options for a request-driven cleanup. Always carries this instance's
/// in-flight run ids: an operator clicking "clean all" must still not have a file
/// unlinked out from under a run that is writing it.
fn options(state: &ServerState, dry_run: bool) -> SweepOptions {
    SweepOptions::new(retention_days(state))
        .dry_run(dry_run)
        .in_flight(state.registry().live_run_ids())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> CleanupRequest {
        CleanupRequest::default()
    }

    #[test]
    fn each_field_resolves_to_its_scope() {
        let mut r = req();
        r.older_than_days = Some(3);
        assert_eq!(r.scope().unwrap(), SweepScope::OlderThanDays(3));

        let mut r = req();
        r.expired = true;
        assert_eq!(r.scope().unwrap(), SweepScope::Expired);

        let mut r = req();
        r.all = true;
        assert_eq!(r.scope().unwrap(), SweepScope::All);

        let mut r = req();
        r.dataset_id = Some("ds1".into());
        assert_eq!(r.scope().unwrap(), SweepScope::Dataset("ds1".into()));

        let mut r = req();
        r.run_id = Some("run-1".into());
        assert_eq!(r.scope().unwrap(), SweepScope::Run("run-1".into()));
    }

    #[test]
    fn an_empty_request_is_rejected_rather_than_defaulting() {
        // Defaulting a scopeless cleanup to anything is how you delete more than
        // was asked for.
        let err = req().scope().unwrap_err();
        assert!(matches!(err, ServeError::BadConfig(_)), "{err:?}");
    }

    #[test]
    fn combining_scopes_is_rejected() {
        let mut r = req();
        r.all = true;
        r.older_than_days = Some(30);
        let err = r.scope().unwrap_err();
        match err {
            ServeError::BadConfig(m) => assert!(m.contains("mutually exclusive"), "{m}"),
            other => panic!("expected BadConfig, got {other:?}"),
        }
    }

    #[test]
    fn dry_run_is_independent_of_the_scope() {
        let mut r = req();
        r.all = true;
        r.dry_run = true;
        assert_eq!(r.scope().unwrap(), SweepScope::All);
        assert!(r.dry_run);
    }

    #[test]
    fn request_parses_from_the_console_payloads() {
        let r: CleanupRequest = serde_json::from_str(r#"{"older_than_days": 7}"#).unwrap();
        assert_eq!(r.scope().unwrap(), SweepScope::OlderThanDays(7));
        let r: CleanupRequest = serde_json::from_str(r#"{"all": true}"#).unwrap();
        assert_eq!(r.scope().unwrap(), SweepScope::All);
        let r: CleanupRequest =
            serde_json::from_str(r#"{"dataset_id": "abc", "dry_run": true}"#).unwrap();
        assert_eq!(r.scope().unwrap(), SweepScope::Dataset("abc".into()));
        assert!(r.dry_run);
    }
}
