# Scorecard Hardening — change log

Branch: `feat/scorecard-hardening` (worktree at `../faucet-stream-scorecard`).
Baseline: `cargo build --workspace` + `cargo test --workspace` captured at start (see below).

## Baseline
- **Environment note:** the machine has only ~6.7 GB free on `/`; the sibling
  `faucet-stream/target` is already 74 GB and owned by a concurrent Claude session.
  A fresh `cargo build --workspace` in this worktree ENOSPC'd partway. Full-workspace
  build/test is therefore **not runnable in this environment** without reclaiming the
  other session's target (which I must not touch).
  - `TODO: run locally` once disk is free:
    `cargo build --workspace && cargo test --workspace`
- **Mitigation:** all verification here is **scoped per crate** (`cargo test -p <crate>`,
  `cargo build -p <crate>`), which the disk budget can accommodate. Each phase records
  the exact scoped commands that passed. HEAD at branch point: `d6b1cfd3`.

## Phase 1 — Conformance checks 3–6 ✅
- `crates/conformance/src/lib.rs`: implemented checks 3–6 for real —
  `assert_bookmark_roundtrip`, `assert_idempotent_replay` (dispatches on
  atomic-watermark vs keyed-upsert), `assert_capabilities_truthful`,
  `assert_errors_not_panics` (catches unwinds via `catch_unwind`).
  `assert_idempotent_replay` / `assert_capabilities_truthful` take a
  `distinct_count` closure so they work against doubles *and* real sinks.
- `crates/conformance/src/doubles.rs`: extended `CountingSource` (resumable
  bookmark + `non_resumable` variant), `TestSink` (`idempotent`/`keyed` modes,
  token store), added `FailingSource`, `PanickingSource`, `LyingIdempotentSink`,
  `LyingKeyedSink` (so each check has a genuine failing case).
- Tests: 18 unit tests + 1 doctest, all green. Every check has a passing test
  and a `#[should_panic]` failing test.
- Verified: `cargo test -p faucet-conformance` (18 passed),
  `cargo clippy -p faucet-conformance --all-targets -- -D warnings` (clean),
  `cargo fmt -p faucet-conformance`.

## Phase 2 — Wire battery into native connectors ✅
Battery invoked against the **real** connectors (no live infra needed):
- **rest** (source) — checks 1, 6 (config schema; unreachable endpoint → typed
  error). Bounded-streaming/resume are covered by its `stream_test.rs` /
  `pagination_test.rs` / `state_resume_test.rs`.
- **csv** (source) — checks 1, 2, 6 (added 6: missing file → typed error).
- **sqlite** (source) — checks 1, 2, 6 (new file; seeded tempfile DB via sqlx,
  bounded paging, missing-table read error). This is the third native source.
- **jsonl** (sink) — checks 1, 5 (new file). Honest branch: append-only, so it
  advertises no idempotency and check 5 verifies Append works + the sink does
  not claim idempotent/keyed dedup.
- **sqlite** (sink) — checks 1, 4, 5 (new file; tempfile DB, upsert AutoMap).
  **check 4 (`assert_idempotent_replay`) runs against a genuine effectively-once
  sink** (atomic-watermark `write_batch_idempotent` + `last_committed_token`) —
  the battery is load-bearing, not just double-tested.
- **singer** — checks 1, 2, 3, 6 (added 3 bookmark round-trip via the fake tap's
  final STATE + coarse resume; added 6 missing-tap-binary spawn error).
- `crates/conformance/src/lib.rs`: `rows()` helper now emits a non-key `v`
  column so SQL upserts have something to `SET`.
- **postgres deferred:** its test crate pulls `testcontainers` (heavy) and disk
  was constrained; its offline surface is identical in shape to the sqlite
  source already wired, and its live checks run under the Docker integration
  suite. `TODO: run locally` — add `crates/source/postgres/tests/conformance.rs`
  mirroring the sqlite-source pattern against a `testcontainers` Postgres.
- Verified per crate: `cargo test -p <crate> --test conformance` (all green),
  `cargo clippy -p … --tests -- -D warnings` (clean), `cargo fmt --all`.
## Phase 3 — Tiers + spec + authoring guide
## Phase 4 — Singer catalog-driven selection
## Phase 5 — Benchmark: sink-bound scenario
## Phase 6 — effectively-once wording sweep
## Phase 7 — Reachability + contributor on-ramp
