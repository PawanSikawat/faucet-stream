//! Property tests for the Singer resume / checkpoint state machine — the proof
//! of the **effectively-once** (idempotent at-least-once) delivery claim.
//!
//! [`tests/integration.rs`] pins one hand-written crash-resume case end-to-end
//! through a real subprocess. This generalizes that proof: it drives the pure
//! [`PageAssembler`] state machine (`src/assemble.rs`) plus a faithful model of
//! the pipeline's resume loop over **arbitrary** message interleavings and
//! **arbitrary** crash points, with in-memory `Sink` / `StateStore` doubles. No
//! subprocess, so it is deterministic and fast.
//!
//! The model mirrors [`faucet_core::Pipeline`]'s documented ordering exactly:
//! for each page the sink write happens **first**, and only after it confirms is
//! the page's bookmark persisted (see `Pipeline::with_state_store`). On a crash
//! the tap's trailing un-checkpointed buffer is dropped (the stream errors
//! before `on_eof` commits it), and on resume a real tap replays *coarsely* from
//! the last persisted STATE — re-emitting the boundary record — so a keyed
//! (upsert) sink is what turns at-least-once redelivery into no-duplicates.
//!
//! Invariants asserted (the "effectively-once" proof):
//!   1. No loss — after a successful resume the sink holds every id.
//!   2. No duplicates with a keyed/idempotent sink — each id appears once.
//!   3. Checkpoint never ahead of durable data — a persisted bookmark never
//!      references a record not already written to the sink (asserted at *every*
//!      checkpoint, across the crash, not just at the end).
//!   4. Monotonic checkpoints — the persisted bookmark never moves backward.
//!   5. Empty-with-bookmark — a tail that is only a STATE still advances the
//!      checkpoint and loses nothing.
//!
//! Case count defaults to 256; override with `PROPTEST_CASES=<n>` (proptest
//! reads it automatically), e.g. `PROPTEST_CASES=4096 cargo test -p
//! faucet-source-singer --test resume_proptest`.

use std::collections::{BTreeMap, BTreeSet};

use faucet_core::{StreamPage, Value};
use faucet_source_singer::assemble::PageAssembler;
use proptest::prelude::*;
use serde_json::json;

const STREAM: &str = "s";

/// A record shaped like the fake tap's output (`{"id":N,"name":"row-N"}`).
fn rec(id: i64) -> Value {
    json!({ "id": id, "name": format!("row-{id}") })
}

/// One line of a modeled tap script.
#[derive(Debug, Clone)]
enum Msg {
    /// A RECORD for the target stream with the given id.
    Record(i64),
    /// A STATE checkpoint referencing the last emitted record id.
    State(i64),
}

/// In-memory stand-in for a keyed/upsert sink + a durable state store, wired in
/// the pipeline's write-then-checkpoint order. Enforces invariants 3 & 4 the
/// moment a checkpoint is persisted, so a regression is caught mid-run.
struct Model {
    /// Keyed (upsert) sink: a re-delivered id overwrites rather than duplicates.
    sink: BTreeMap<i64, Value>,
    /// Every row ever handed to the sink — proves overlap really happened.
    total_writes: usize,
    /// The single persisted state bookmark (`{"last_id": N}`), or `None`.
    store: Option<Value>,
    /// Last persisted `last_id`, for the monotonicity check.
    last_ck: Option<i64>,
    /// Every record id in the *full* script — the ground truth for "durable".
    all_ids: BTreeSet<i64>,
}

impl Model {
    fn new(all_ids: BTreeSet<i64>) -> Self {
        Self {
            sink: BTreeMap::new(),
            total_writes: 0,
            store: None,
            last_ck: None,
            all_ids,
        }
    }

    /// Apply one page exactly as `Pipeline::run` does: durably write the records
    /// first, then (only if the page carries one) persist the bookmark.
    fn process(&mut self, page: StreamPage) {
        // 1. Durable write (records become visible in the sink).
        for r in &page.records {
            let id = r
                .get("id")
                .and_then(Value::as_i64)
                .expect("modeled record always has an integer id");
            self.sink.insert(id, r.clone());
            self.total_writes += 1;
        }

        // 2. Checkpoint — strictly after the write confirms.
        if let Some(bm) = page.bookmark {
            let x = bm
                .get("last_id")
                .and_then(Value::as_i64)
                .expect("modeled STATE always carries last_id");

            // INVARIANT 4: checkpoints never regress.
            if let Some(prev) = self.last_ck {
                assert!(
                    x >= prev,
                    "checkpoint moved backward: {prev} -> {x} (page bookmark {bm})"
                );
            }

            // INVARIANT 3: the checkpoint is never ahead of durable data — every
            // record id at or below the bookmark must already be in the sink.
            for id in self.all_ids.iter().copied().filter(|&id| id <= x) {
                assert!(
                    self.sink.contains_key(&id),
                    "bookmark last_id={x} persisted while record id={id} is not \
                     yet durable in the sink"
                );
            }

            self.last_ck = Some(x);
            self.store = Some(bm);
        }
    }

    /// The resume cursor a real tap would receive via `--state` (0 = fresh).
    fn cursor(&self) -> i64 {
        self.store
            .as_ref()
            .and_then(|v| v.get("last_id"))
            .and_then(Value::as_i64)
            .unwrap_or(0)
    }

    fn sink_ids(&self) -> Vec<i64> {
        self.sink.keys().copied().collect()
    }
}

/// Build the full tap script from record ids + per-record "checkpoint after"
/// flags + an optional forced trailing STATE (a clean tap's final checkpoint).
fn build_msgs(ids: &[i64], ck_after: &[bool], final_state: bool) -> Vec<Msg> {
    let mut v = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        v.push(Msg::Record(id));
        if ck_after.get(i).copied().unwrap_or(false) {
            v.push(Msg::State(id));
        }
    }
    if final_state && let Some(&last) = ids.last() {
        // Only if a STATE for the last id isn't already the tail.
        if !matches!(v.last(), Some(Msg::State(x)) if *x == last) {
            v.push(Msg::State(last));
        }
    }
    v
}

/// A real Singer tap resumes *coarsely*: given a persisted `cursor` it re-emits
/// from the first record whose id is >= cursor (re-emitting the boundary), with
/// the same interleaved STATEs. `cursor <= 0` replays from the start.
fn coarse_resume(full: &[Msg], cursor: i64) -> Vec<Msg> {
    if cursor <= 0 {
        return full.to_vec();
    }
    match full
        .iter()
        .position(|m| matches!(m, Msg::Record(id) if *id >= cursor))
    {
        Some(i) => full[i..].to_vec(),
        None => Vec::new(),
    }
}

/// Feed a message sequence through a fresh assembler into `model`.
///
/// `crash_after == Some(k)`: the tap emits `k` messages then exits non-zero —
/// the trailing buffer is dropped and `on_eof` is **not** called (no
/// un-checkpointed page is committed). `None`: a clean run ending in `on_eof`.
fn run_pass(
    model: &mut Model,
    batch_size: usize,
    flush_on_state: bool,
    msgs: &[Msg],
    crash_after: Option<usize>,
) {
    let mut asm = PageAssembler::new(STREAM, batch_size, flush_on_state);
    let limit = crash_after.map(|k| k.min(msgs.len())).unwrap_or(msgs.len());
    for m in &msgs[..limit] {
        let page = match m {
            Msg::Record(id) => asm.on_record(STREAM, rec(*id)),
            Msg::State(v) => asm.on_state(json!({ "last_id": v })),
        };
        if let Some(page) = page {
            model.process(page);
        }
    }
    if crash_after.is_none()
        && let Some(page) = asm.on_eof()
    {
        model.process(page);
    }
}

/// Run one full crash-then-resume scenario and assert every invariant.
fn check_scenario(
    ids: Vec<i64>,
    ck_after: Vec<bool>,
    final_state: bool,
    batch_size: usize,
    flush_on_state: bool,
    crash: Option<usize>,
) {
    let full = build_msgs(&ids, &ck_after, final_state);
    let all_ids: BTreeSet<i64> = ids.iter().copied().collect();
    let mut model = Model::new(all_ids);

    // ── Run 1: may crash. Invariants 3 & 4 are checked inside `process`. ──
    run_pass(&mut model, batch_size, flush_on_state, &full, crash);

    // The checkpoint must point at durably-written data (invariant 3 at the run
    // boundary): a non-zero cursor is always an id already in the sink.
    let cursor = model.cursor();
    if cursor > 0 {
        assert!(
            model.sink.contains_key(&cursor),
            "resume cursor last_id={cursor} is not durable after run 1"
        );
    }

    // ── Run 2: resume from the persisted cursor, replay coarsely, finish. ──
    let replay = coarse_resume(&full, cursor);
    run_pass(&mut model, batch_size, flush_on_state, &replay, None);

    // INVARIANT 1 (no loss) + 2 (no duplicates, via the keyed sink): the final
    // sink holds exactly every id, each once.
    assert_eq!(
        model.sink_ids(),
        ids,
        "after resume the sink must reconstruct every id exactly once \
         (batch_size={batch_size}, flush_on_state={flush_on_state}, crash={crash:?})"
    );

    // The keyed sink must have seen at least every unique id (overlap on resume
    // only ever adds writes; it never drops one).
    assert!(
        model.total_writes >= ids.len(),
        "total writes {} < unique ids {}",
        model.total_writes,
        ids.len()
    );
}

/// Strictly-increasing ids (1..) paired with a "checkpoint after" flag, built
/// from small positive gaps so the sequence is monotonic but not necessarily
/// contiguous.
fn script_strategy() -> impl Strategy<Value = (Vec<i64>, Vec<bool>)> {
    prop::collection::vec((1i64..=5, any::<bool>()), 0..=25).prop_map(|pairs| {
        let mut acc = 0i64;
        let mut ids = Vec::with_capacity(pairs.len());
        let mut ck = Vec::with_capacity(pairs.len());
        for (gap, checkpoint) in pairs {
            acc += gap;
            ids.push(acc);
            ck.push(checkpoint);
        }
        (ids, ck)
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// Arbitrary interleavings × arbitrary crash points × page/flush config all
    /// satisfy the five resume invariants.
    #[test]
    fn resume_invariants_hold(
        (ids, ck_after) in script_strategy(),
        final_state in any::<bool>(),
        batch_size in 0usize..=8,
        flush_on_state in any::<bool>(),
        // `None` = clean run; `Some(k)` = crash after k emitted messages
        // (clamped to the script length in the harness). Covers crashes
        // before/after a STATE, mid-page, and at page boundaries.
        crash in prop_oneof![Just(None), (0usize..=60).prop_map(Some)],
    ) {
        check_scenario(ids, ck_after, final_state, batch_size, flush_on_state, crash);
    }
}

// ─────────────────────────── targeted regression cases ───────────────────────
// Deterministic corners the generator only rarely hits.

/// Crash in the gap *between* "page written" and "checkpoint persisted": the
/// records are durable but the bookmark never advances. Resume from the *old*
/// cursor must redeliver and dedup — no loss, no duplicates.
#[test]
fn crash_between_write_and_checkpoint_loses_nothing() {
    let ids = vec![1, 2, 3, 4];
    let all_ids: BTreeSet<i64> = ids.iter().copied().collect();
    let mut model = Model::new(all_ids);

    // A state-bearing page is produced and its records are written…
    let mut asm = PageAssembler::new(STREAM, 1000, true);
    assert!(asm.on_record(STREAM, rec(1)).is_none());
    assert!(asm.on_record(STREAM, rec(2)).is_none());
    let page = asm
        .on_state(json!({ "last_id": 2 }))
        .expect("flush on state");
    // …but the process dies before the bookmark is persisted: write only.
    for r in &page.records {
        let id = r["id"].as_i64().unwrap();
        model.sink.insert(id, r.clone());
        model.total_writes += 1;
    }
    // Cursor never advanced.
    assert_eq!(model.cursor(), 0);
    assert_eq!(model.sink_ids(), vec![1, 2]);

    // Resume from the old cursor (0): the tap replays the whole script.
    let full = build_msgs(&ids, &[false, true, false, true], true);
    let replay = coarse_resume(&full, model.cursor());
    let writes_before = model.total_writes;
    run_pass(&mut model, 1000, true, &replay, None);

    // Overlap really happened (1 and 2 were re-delivered)…
    assert!(model.total_writes > writes_before + 2);
    // …and the keyed sink deduped it: every id, once.
    assert_eq!(model.sink_ids(), vec![1, 2, 3, 4]);
    assert_eq!(model.cursor(), 4);
}

/// A STATE arriving mid-page (with `flush_on_state = false`) is deferred and
/// attached to the next size-based flush; the checkpoint it carries is still
/// backed by already-written records (invariant 3).
#[test]
fn state_mid_page_defers_and_stays_backed() {
    let ids = [1, 2, 3];
    let all_ids: BTreeSet<i64> = ids.iter().copied().collect();
    let mut model = Model::new(all_ids);

    let mut asm = PageAssembler::new(STREAM, 3, false);
    assert!(asm.on_record(STREAM, rec(1)).is_none());
    // STATE mid-page: recorded as pending, no flush.
    assert!(asm.on_state(json!({ "last_id": 1 })).is_none());
    assert!(asm.on_record(STREAM, rec(2)).is_none());
    // Size threshold reached: page carries [1,2,3] with the deferred bookmark.
    let page = asm.on_record(STREAM, rec(3)).expect("size flush at 3");
    assert_eq!(page.bookmark, Some(json!({ "last_id": 1 })));
    // `process` asserts invariant 3 (record 1 is written before last_id=1 is
    // persisted) and invariant 4 internally.
    model.process(page);
    assert_eq!(model.sink_ids(), vec![1, 2, 3]);
    assert_eq!(model.cursor(), 1);
}

/// Invariant 5: a script whose only tail is a STATE (an empty page carrying a
/// bookmark) still advances the checkpoint and loses nothing.
#[test]
fn empty_page_with_trailing_state_advances_checkpoint() {
    // batch_size=1 forces record 1 out as a bookmark-less page; the following
    // STATE then produces an *empty* page carrying the bookmark.
    let mut asm = PageAssembler::new(STREAM, 1, true);
    let first = asm.on_record(STREAM, rec(1)).expect("size flush at 1");
    assert!(first.bookmark.is_none());
    let empty = asm.on_state(json!({ "last_id": 1 })).expect("state flush");
    assert!(empty.records.is_empty(), "trailing-state page is empty");
    assert_eq!(empty.bookmark, Some(json!({ "last_id": 1 })));

    let mut model = Model::new([1].into_iter().collect());
    model.process(first);
    model.process(empty); // invariant 3 holds: record 1 durable before last_id=1
    assert_eq!(model.cursor(), 1);
    assert_eq!(model.sink_ids(), vec![1]);
}
