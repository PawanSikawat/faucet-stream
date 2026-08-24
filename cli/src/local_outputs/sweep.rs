//! The GC engine: which recorded outputs a request covers ([`select`], pure),
//! and the filesystem work that acts on that decision ([`run`]) (#587).
//!
//! Split deliberately. Every rule that decides whether a file may be deleted —
//! the retention arithmetic, the `pre_existing` refusal, the in-flight guard —
//! lives in the pure half, where it is exhaustively testable without a
//! filesystem. The I/O half does only what it is told: `metadata`, `remove_file`,
//! mark the row. There is no `read_dir`, no glob, and no `remove_dir_all`
//! anywhere in this module, which is how the "never a directory wipe" guardrail
//! is held structurally rather than by review.

use super::ledger::{
    LocalOutputFilter, LocalOutputRecord, SkipReason, SweepOutcome, SweepReport, SweepScope,
};
use super::metrics;
use crate::serve::history::{HistoryError, RunHistory};
use chrono::{DateTime, Utc};
use std::collections::BTreeSet;

/// Inputs a sweep needs beyond the scope itself.
#[derive(Debug, Clone)]
pub struct SweepOptions {
    /// Runtime default retention window in days, for rows without an override.
    pub default_retention_days: u32,
    /// Report what would be deleted without touching anything.
    pub dry_run: bool,
    /// Evaluation instant (injected so retention arithmetic is testable).
    pub now: DateTime<Utc>,
    /// Run ids currently executing. An output last written by one of them is
    /// skipped: a sweep must never delete a file out from under a live writer.
    pub in_flight: BTreeSet<String>,
}

impl SweepOptions {
    pub fn new(default_retention_days: u32) -> Self {
        Self {
            default_retention_days,
            dry_run: false,
            now: Utc::now(),
            in_flight: BTreeSet::new(),
        }
    }

    pub fn dry_run(mut self, yes: bool) -> Self {
        self.dry_run = yes;
        self
    }

    pub fn at(mut self, now: DateTime<Utc>) -> Self {
        self.now = now;
        self
    }

    pub fn in_flight(mut self, runs: BTreeSet<String>) -> Self {
        self.in_flight = runs;
        self
    }
}

/// One selected row and what should happen to it: `None` = delete the file,
/// `Some(reason)` = leave it alone and report why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    pub record: LocalOutputRecord,
    pub skip: Option<SkipReason>,
}

/// Decide, from the ledger alone, what a request covers — pure.
///
/// Two-stage: a row must first be **in scope**, then it must be **collectable**.
///
/// Already-deleted rows are in scope only for [`SweepScope::Output`], the
/// single-target case where the caller needs to be told why nothing happened.
/// For the bulk scopes they are silently out of scope, so a nightly sweep does
/// not report every file it collected last week as "skipped" forever.
pub fn select(
    rows: &[LocalOutputRecord],
    scope: &SweepScope,
    opts: &SweepOptions,
) -> Vec<Selection> {
    rows.iter()
        .filter(|rec| in_scope(rec, scope, opts))
        .map(|rec| Selection {
            record: rec.clone(),
            skip: skip_reason(rec, scope, opts),
        })
        .collect()
}

/// Whether `scope` names this row at all.
fn in_scope(rec: &LocalOutputRecord, scope: &SweepScope, opts: &SweepOptions) -> bool {
    match scope {
        SweepScope::Output(id) => rec.id == *id,
        // The bulk scopes ignore rows whose file is already gone.
        _ if rec.deleted_at.is_some() => false,
        SweepScope::Dataset(dataset_id) => rec.dataset_id == *dataset_id,
        SweepScope::Run(run_id) => rec.run_id == *run_id,
        SweepScope::OlderThanDays(days) => {
            // An explicit "purge older than N days" means exactly N days —
            // per-row retention overrides do not extend it. It is the operator
            // asking directly, not the retention policy running.
            rec.age_secs(opts.now) >= u64::from(*days) * 86_400
        }
        SweepScope::Expired => rec.is_expired_by_age(opts.default_retention_days, opts.now),
        SweepScope::All => true,
    }
}

/// Why an in-scope row must not be deleted, if it must not.
fn skip_reason(
    rec: &LocalOutputRecord,
    _scope: &SweepScope,
    opts: &SweepOptions,
) -> Option<SkipReason> {
    if rec.deleted_at.is_some() {
        return Some(SkipReason::AlreadyDeleted);
    }
    // The guardrail, applied uniformly: an explicit single-output "delete now"
    // gets the same refusal as the background sweeper. faucet appended to this
    // file; it did not create it, and deleting somebody else's data is not a
    // thing a retention policy is allowed to do.
    if rec.pre_existing {
        return Some(SkipReason::PreExisting);
    }
    if opts.in_flight.contains(&rec.run_id) {
        return Some(SkipReason::InFlight);
    }
    None
}

/// Execute a cleanup request: select from the ledger, delete the files that are
/// collectable, and mark their rows.
///
/// The run record, catalog entry, and lineage for these outputs are untouched —
/// only the data files go. A ledger row is kept and marked `deleted_at`, which
/// is what makes the output render as *expired* rather than disappear.
///
/// Never returns `Err` for a per-file problem: a missing file is a no-op, an
/// undeletable one is reported in the outcome. Only a *store* failure (the
/// ledger itself is unreachable) propagates, because then the sweep has no idea
/// what it is allowed to touch.
pub async fn run(
    store: &dyn RunHistory,
    scope: &SweepScope,
    opts: &SweepOptions,
) -> Result<SweepReport, HistoryError> {
    let rows = match scope {
        // One output: fetch just that row rather than paging the whole ledger.
        SweepScope::Output(id) => store.local_output_get(id).await?.into_iter().collect(),
        _ => {
            store
                .local_output_list(&LocalOutputFilter {
                    // The engine needs deleted rows too: `in_scope` decides what
                    // to do with them, and for a single-output request the
                    // "already gone" answer is the useful one.
                    include_deleted: true,
                    ..Default::default()
                })
                .await?
        }
    };

    let mut report = SweepReport {
        dry_run: opts.dry_run,
        scope: scope.label().to_string(),
        ..Default::default()
    };

    for sel in select(&rows, scope, opts) {
        report.push(apply(store, sel, opts).await);
    }

    metrics::sweep(scope.label(), &report);
    if report.deleted > 0 {
        // A GC that deletes data must never be silent.
        tracing::info!(
            scope = scope.label(),
            deleted = report.deleted,
            bytes = report.bytes,
            skipped = report.skipped,
            dry_run = opts.dry_run,
            "local sink outputs cleaned"
        );
    }
    Ok(report)
}

/// Act on one selection. Infallible by construction — every failure becomes a
/// [`SkipReason`] on the outcome so one unreadable file cannot abort the sweep.
async fn apply(store: &dyn RunHistory, sel: Selection, opts: &SweepOptions) -> SweepOutcome {
    let rec = sel.record;
    let mut outcome = SweepOutcome {
        id: rec.id.clone(),
        path: rec.path.clone(),
        dataset_uri: rec.dataset_uri.clone(),
        deleted: false,
        bytes: 0,
        skipped: sel.skip,
        error: None,
    };
    if outcome.skipped.is_some() {
        return outcome;
    }

    // Size first, so the report can say how much was reclaimed. A stat failure
    // that is not "missing" (a permission problem) is not fatal: the delete is
    // still attempted and its own error is what gets reported.
    let bytes = match tokio::fs::metadata(rec.fs_path()).await {
        Ok(meta) => meta.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Already gone — somebody's `rm`, or a moved workspace. Not an
            // error; the row is marked so the console stops offering a delete
            // that cannot do anything.
            outcome.skipped = Some(SkipReason::NotOnDisk);
            if !opts.dry_run {
                mark_deleted(store, &rec, opts.now, 0).await;
            }
            return outcome;
        }
        Err(_) => 0,
    };

    if opts.dry_run {
        outcome.deleted = true;
        outcome.bytes = bytes;
        return outcome;
    }

    // The only deletion in this subsystem: one recorded file, by path.
    match tokio::fs::remove_file(rec.fs_path()).await {
        Ok(()) => {
            outcome.deleted = true;
            outcome.bytes = bytes;
            mark_deleted(store, &rec, opts.now, bytes).await;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Raced with another sweeper (or a manual `rm`) between the stat and
            // the unlink. The end state is the one we wanted.
            outcome.skipped = Some(SkipReason::NotOnDisk);
            mark_deleted(store, &rec, opts.now, 0).await;
        }
        Err(e) => {
            outcome.skipped = Some(SkipReason::DeleteFailed);
            outcome.error = Some(e.to_string());
            tracing::warn!(
                path = %rec.path,
                error = %e,
                "could not delete local sink output — leaving it in place"
            );
        }
    }
    outcome
}

/// Mark a row's file gone. A ledger write failure is logged, not propagated: the
/// file is already deleted, and failing the sweep would not bring it back. The
/// worst case is a row that still reads `present` and gets a `not_on_disk`
/// no-op on the next pass.
async fn mark_deleted(
    store: &dyn RunHistory,
    rec: &LocalOutputRecord,
    at: DateTime<Utc>,
    bytes: u64,
) {
    if let Err(e) = store.local_output_mark_deleted(&rec.id, at, bytes).await {
        tracing::warn!(
            path = %rec.path,
            error = %e,
            "local output deleted but the ledger row could not be updated"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_outputs::ledger::LocalOutputObservation;
    use crate::serve::history::memory::MemoryHistory;
    use std::path::PathBuf;
    use std::time::Duration;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    const NOW: &str = "2026-08-20T00:00:00Z";

    fn obs(path: &str, at: &str) -> LocalOutputObservation {
        LocalOutputObservation {
            path: PathBuf::from(path),
            dataset_uri: format!("file://{path}"),
            dataset_id: "ds1".into(),
            kind: "jsonl".into(),
            pipeline: "p".into(),
            row: "default".into(),
            run_id: "run-1".into(),
            pre_existing: false,
            retention_days: None,
            observed_at: ts(at),
        }
    }

    fn rec(path: &str, at: &str) -> LocalOutputRecord {
        LocalOutputRecord::new(&obs(path, at))
    }

    fn opts() -> SweepOptions {
        SweepOptions::new(7).at(ts(NOW))
    }

    // ── select(): scope ──────────────────────────────────────────────────────

    #[test]
    fn expired_scope_selects_only_rows_past_their_window() {
        let rows = vec![
            rec("/tmp/old.jsonl", "2026-08-01T00:00:00Z"), // 19 days
            rec("/tmp/new.jsonl", "2026-08-19T00:00:00Z"), // 1 day
        ];
        let sel = select(&rows, &SweepScope::Expired, &opts());
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].record.path, "/tmp/old.jsonl");
        assert_eq!(sel[0].skip, None);
    }

    #[test]
    fn all_scope_selects_rows_still_inside_their_window() {
        let rows = vec![rec("/tmp/new.jsonl", "2026-08-19T00:00:00Z")];
        assert!(select(&rows, &SweepScope::Expired, &opts()).is_empty());
        assert_eq!(select(&rows, &SweepScope::All, &opts()).len(), 1);
    }

    #[test]
    fn older_than_ignores_a_per_row_retention_override() {
        // The row says "keep me for 30 days"; the operator says "purge anything
        // older than 5". An explicit request wins over the policy.
        let mut o = obs("/tmp/a.jsonl", "2026-08-10T00:00:00Z");
        o.retention_days = Some(30);
        let rows = vec![LocalOutputRecord::new(&o)];
        assert!(select(&rows, &SweepScope::Expired, &opts()).is_empty());
        assert_eq!(
            select(&rows, &SweepScope::OlderThanDays(5), &opts()).len(),
            1
        );
        assert!(select(&rows, &SweepScope::OlderThanDays(30), &opts()).is_empty());
    }

    #[test]
    fn dataset_scope_selects_only_that_datasets_outputs() {
        let mut other = obs("/tmp/b.jsonl", NOW);
        other.dataset_id = "ds2".into();
        let rows = vec![rec("/tmp/a.jsonl", NOW), LocalOutputRecord::new(&other)];
        let sel = select(&rows, &SweepScope::Dataset("ds2".into()), &opts());
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].record.path, "/tmp/b.jsonl");
    }

    #[test]
    fn run_scope_selects_only_that_runs_outputs() {
        // "Clean up after that run" — the other reading of an immediate cleanup.
        let mut other = obs("/tmp/b.jsonl", NOW);
        other.run_id = "run-2".into();
        let rows = vec![rec("/tmp/a.jsonl", NOW), LocalOutputRecord::new(&other)];
        let sel = select(&rows, &SweepScope::Run("run-2".into()), &opts());
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].record.path, "/tmp/b.jsonl");
        assert_eq!(sel[0].skip, None);
        // A run that wrote nothing selects nothing, rather than everything.
        assert!(select(&rows, &SweepScope::Run("run-9".into()), &opts()).is_empty());
    }

    #[test]
    fn output_scope_selects_exactly_one_row() {
        let rows = vec![rec("/tmp/a.jsonl", NOW), rec("/tmp/b.jsonl", NOW)];
        let target = rows[1].id.clone();
        let sel = select(&rows, &SweepScope::Output(target.clone()), &opts());
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].record.id, target);
    }

    #[test]
    fn an_unknown_output_id_selects_nothing() {
        let rows = vec![rec("/tmp/a.jsonl", NOW)];
        assert!(select(&rows, &SweepScope::Output("nope".into()), &opts()).is_empty());
    }

    // ── select(): collectability ─────────────────────────────────────────────

    #[test]
    fn a_pre_existing_file_is_never_collectable_by_any_scope() {
        // The guardrail. Every scope, including an explicit single-output
        // delete and "clean all", must refuse.
        let mut o = obs("/tmp/theirs.jsonl", "2026-01-01T00:00:00Z");
        o.pre_existing = true;
        let rows = vec![LocalOutputRecord::new(&o)];
        let id = rows[0].id.clone();
        for scope in [
            SweepScope::Expired,
            SweepScope::All,
            SweepScope::OlderThanDays(1),
            SweepScope::Dataset("ds1".into()),
            SweepScope::Run("run-1".into()),
            SweepScope::Output(id),
        ] {
            let sel = select(&rows, &scope, &opts());
            assert_eq!(sel.len(), 1, "{}", scope.label());
            assert_eq!(
                sel[0].skip,
                Some(SkipReason::PreExisting),
                "scope {} must refuse a file faucet did not create",
                scope.label()
            );
        }
    }

    #[test]
    fn an_output_of_a_running_run_is_skipped_not_deleted() {
        // Concurrent write vs sweep: deleting mid-write would corrupt the run's
        // output. Retried on the next pass.
        let rows = vec![rec("/tmp/a.jsonl", "2026-01-01T00:00:00Z")];
        let o = opts().in_flight(BTreeSet::from(["run-1".to_string()]));
        let sel = select(&rows, &SweepScope::Expired, &o);
        assert_eq!(sel[0].skip, Some(SkipReason::InFlight));

        // A different run being in flight is irrelevant.
        let o = opts().in_flight(BTreeSet::from(["run-other".to_string()]));
        assert_eq!(select(&rows, &SweepScope::Expired, &o)[0].skip, None);
    }

    #[test]
    fn already_deleted_rows_are_out_of_scope_for_bulk_but_explained_for_one() {
        let mut row = rec("/tmp/a.jsonl", "2026-01-01T00:00:00Z");
        row.deleted_at = Some(ts("2026-08-01T00:00:00Z"));
        let id = row.id.clone();
        let rows = vec![row];

        // Bulk scopes stay quiet — otherwise every nightly sweep would report
        // last week's collected files forever.
        for scope in [
            SweepScope::Expired,
            SweepScope::All,
            SweepScope::OlderThanDays(1),
            SweepScope::Dataset("ds1".into()),
        ] {
            assert!(
                select(&rows, &scope, &opts()).is_empty(),
                "{}",
                scope.label()
            );
        }
        // A single-output request gets told why nothing happened.
        let sel = select(&rows, &SweepScope::Output(id), &opts());
        assert_eq!(sel[0].skip, Some(SkipReason::AlreadyDeleted));
    }

    #[test]
    fn a_keep_forever_row_is_never_expired_but_is_still_explicitly_cleanable() {
        let mut o = obs("/tmp/a.jsonl", "2020-01-01T00:00:00Z");
        o.retention_days = Some(0);
        let rows = vec![LocalOutputRecord::new(&o)];
        assert!(select(&rows, &SweepScope::Expired, &opts()).is_empty());
        // "clean all" is an explicit operator action and still covers it.
        assert_eq!(select(&rows, &SweepScope::All, &opts())[0].skip, None);
    }

    // ── run(): filesystem effects ────────────────────────────────────────────

    async fn store_with(rows: &[LocalOutputObservation]) -> MemoryHistory {
        let store = MemoryHistory::new(Duration::from_secs(60));
        for o in rows {
            store.local_output_record(o).await.unwrap();
        }
        store
    }

    #[tokio::test]
    async fn run_deletes_an_expired_file_and_marks_the_row_expired() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        std::fs::write(&path, b"{\"a\":1}\n").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-08-01T00:00:00Z")]).await;

        let report = run(&store, &SweepScope::Expired, &opts()).await.unwrap();
        assert_eq!(report.deleted, 1);
        assert_eq!(report.bytes, 8);
        assert!(!path.exists(), "the file must actually be gone");

        // The record survives, as `expired` — the durable side is untouched.
        let rows = store
            .local_output_list(&LocalOutputFilter {
                include_deleted: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state(), super::super::LocalOutputState::Expired);
        assert_eq!(rows[0].deleted_bytes, Some(8));
    }

    #[tokio::test]
    async fn a_dry_run_reports_but_deletes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        std::fs::write(&path, b"xy").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-08-01T00:00:00Z")]).await;

        let report = run(&store, &SweepScope::Expired, &opts().dry_run(true))
            .await
            .unwrap();
        assert!(report.dry_run);
        assert_eq!((report.deleted, report.bytes), (1, 2));
        assert!(path.exists(), "a dry run must not touch the filesystem");
        assert_eq!(
            store
                .local_output_list(&LocalOutputFilter::default())
                .await
                .unwrap()[0]
                .state(),
            super::super::LocalOutputState::Present,
            "a dry run must not mark the row either"
        );
    }

    #[tokio::test]
    async fn a_missing_file_is_a_no_op_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gone.jsonl"); // never created
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-08-01T00:00:00Z")]).await;

        let report = run(&store, &SweepScope::Expired, &opts()).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped_for(SkipReason::NotOnDisk), 1);
        // Marked expired anyway: the file is gone, which is the desired state.
        let rows = store
            .local_output_list(&LocalOutputFilter {
                include_deleted: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(rows[0].state(), super::super::LocalOutputState::Expired);
    }

    #[tokio::test]
    async fn run_never_deletes_a_pre_existing_file_from_disk() {
        // End-to-end proof of the guardrail, not just of `select`.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("theirs.jsonl");
        std::fs::write(&path, b"someone elses data").unwrap();
        let mut o = obs(path.to_str().unwrap(), "2020-01-01T00:00:00Z");
        o.pre_existing = true;
        let store = store_with(&[o]).await;

        let report = run(&store, &SweepScope::All, &opts()).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped_for(SkipReason::PreExisting), 1);
        assert!(path.exists());
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"someone elses data",
            "and it was not truncated either"
        );
    }

    #[tokio::test]
    async fn run_deletes_only_the_named_output_leaving_its_siblings() {
        // "delete now" on one file in a directory of rolled parts must not take
        // the directory or its neighbours with it.
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("part-a.parquet");
        let b = dir.path().join("part-b.parquet");
        std::fs::write(&a, b"aaa").unwrap();
        std::fs::write(&b, b"bbb").unwrap();
        let store =
            store_with(&[obs(a.to_str().unwrap(), NOW), obs(b.to_str().unwrap(), NOW)]).await;
        let target = crate::local_outputs::ledger::output_id(&a);

        let report = run(&store, &SweepScope::Output(target), &opts())
            .await
            .unwrap();
        assert_eq!(report.deleted, 1);
        assert!(!a.exists());
        assert!(b.exists(), "a sibling file must survive");
        assert!(dir.path().exists(), "the directory must survive");
    }

    #[tokio::test]
    async fn clean_all_covers_every_tracked_output_regardless_of_age() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old.jsonl");
        let fresh = dir.path().join("fresh.jsonl");
        std::fs::write(&old, b"o").unwrap();
        std::fs::write(&fresh, b"f").unwrap();
        let store = store_with(&[
            obs(old.to_str().unwrap(), "2026-01-01T00:00:00Z"),
            obs(fresh.to_str().unwrap(), NOW),
        ])
        .await;

        let report = run(&store, &SweepScope::All, &opts()).await.unwrap();
        assert_eq!(report.deleted, 2);
        assert!(!old.exists() && !fresh.exists());
    }

    #[tokio::test]
    async fn a_second_sweep_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        std::fs::write(&path, b"x").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-08-01T00:00:00Z")]).await;

        assert_eq!(
            run(&store, &SweepScope::Expired, &opts())
                .await
                .unwrap()
                .deleted,
            1
        );
        let second = run(&store, &SweepScope::Expired, &opts()).await.unwrap();
        assert_eq!((second.deleted, second.skipped), (0, 0));
    }

    #[tokio::test]
    async fn an_empty_ledger_sweeps_cleanly() {
        let store = MemoryHistory::new(Duration::from_secs(60));
        let report = run(&store, &SweepScope::All, &opts()).await.unwrap();
        assert_eq!((report.deleted, report.skipped), (0, 0));
        assert_eq!(report.scope, "all");
    }

    #[tokio::test]
    async fn an_in_flight_output_survives_the_sweep_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.jsonl");
        std::fs::write(&path, b"mid-write").unwrap();
        let store = store_with(&[obs(path.to_str().unwrap(), "2026-01-01T00:00:00Z")]).await;

        let o = opts().in_flight(BTreeSet::from(["run-1".to_string()]));
        let report = run(&store, &SweepScope::Expired, &o).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped_for(SkipReason::InFlight), 1);
        assert!(path.exists());
    }
}
