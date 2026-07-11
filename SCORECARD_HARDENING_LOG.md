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

## Phase 2 — Wire battery into native connectors
## Phase 3 — Tiers + spec + authoring guide
## Phase 4 — Singer catalog-driven selection
## Phase 5 — Benchmark: sink-bound scenario
## Phase 6 — effectively-once wording sweep
## Phase 7 — Reachability + contributor on-ramp
