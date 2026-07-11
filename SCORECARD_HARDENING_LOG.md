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
## Phase 3 — Tiers + spec + authoring guide ✅
- **FCP v0 spec** committed at `docs/spec/faucet-connector-spec-v0.md` (grounded
  in the real trait contract; says effectively-once, not exactly-once). Surfaced
  in the book via `docs/book/src/spec/faucet-connector-spec-v0.md` (`{{#include}}`)
  + a `SUMMARY.md` "Connector protocol (FCP v0)" entry; linked from README.
- **Support-tier column** added to the README Sources/Sinks tables *and* the
  docs-site capability matrix (`reference/connectors.md`): **T1 ✅** = passes the
  `faucet-conformance` battery in CI (rest, csv, sqlite source; jsonl, sqlite
  sink), **T2** = not yet wired (honest note: not "low quality"). Singer = T2 ⚠️
  (passes battery, experimental v0). The battery IS the tiering mechanism.
- **Authoring guide** (`extending/authoring-connectors.md`) gained a
  "Self-certify with the conformance battery" section; **CONTRIBUTING.md**
  "Adding a connector" now points at the battery + FCP spec.
- Verified: `mdbook build docs/book` clean; spec renders in the book; both matrix
  tables cell-count consistent.

## Phase 4 — Singer catalog-driven selection ✅
- `crates/source/singer/src/discover.rs`: new pure `select_streams(catalog,
  target) -> StreamSelection { catalog, selected, warnings }` — marks the target
  stream `selected` (stream-level flag + `breadcrumb: []` metadata) and includes
  inferable **parent** streams (`parent_stream`/`parent` on the stream or its
  metadata); warns on missing target, absent referenced parent, or an
  un-inferrable parent among multiple streams. 6 unit tests.
- `tests/fake_taps/fake_tap.sh`: `--parent-child` discovery mode emits an
  `issues`→`repositories` parent-keyed catalog; new `tests/selection.rs`
  integration test drives real discovery + selection.
- CLI: `faucet init --source singer --discover … --stream <name>` now applies
  selection, writes the selected catalog, sets `stream:`, and prints selected
  streams + warnings (`cli/src/cli.rs` `--stream`, `cli/src/commands/init.rs`).
- Singer README documents catalog-driven selection + the ACTIVATE_VERSION/
  FULL_TABLE-into-append-sink duplicate caveat (use a keyed sink).
- Single-stream **output** preserved (extraction may pull parents; only the
  configured stream is emitted).
- Verified: `cargo test -p faucet-source-singer` (27 lib + selection + others),
  `cargo clippy -p faucet-source-singer --all-targets -- -D warnings` clean,
  `cargo check -p faucet-cli --no-default-features --features source-singer,…` clean.
## Phase 5 — Benchmark: sink-bound scenario ✅
- **Scenario C — Postgres → Postgres (sink-bound)** added: faucet config
  `benchmarks/faucet/postgres_to_postgres.yaml` (source-postgres → sink-postgres
  AutoMap, `batch_size: 5000`), Meltano `target-postgres` loader in `meltano.yml`.
- **`scripts/run-bench.sh`**: replaced the Scenario B stub with a real
  `run_pg_scenarios` (Docker Postgres 16, `COPY`-load `bench`, typed `bench_dest`,
  runs B pg→jsonl and C pg→pg for both tools, TRUNCATE/ DROP SCHEMA prepares,
  appends results). `bash -n` clean.
- **Makefile**: `bench-build` now includes `sink-postgres`; new `bench-postgres`
  target runs `--postgres`.
- **Headline reframed honestly** in BENCHMARKS.md + README: "**~1–2 orders of
  magnitude on single-machine batch throughput**", CSV→JSONL called out as a
  *best case* (upper bound), sink-bound moves narrow the gap. Kept faucet's own
  A/B throughput+RSS rows.
- **"Reproduce and report" callout** added (one independent confirmation > a new
  connector). New caveat #0 (best-case vs sink-bound).
- **Scenario C numbers are `TODO: run locally`** — Docker/Postgres + a Meltano
  venv weren't available in this authoring environment; scenario/configs/harness
  are fully wired and the placeholder is never fabricated. Exact repro command:
  `make bench-postgres` (or `scripts/run-bench.sh --postgres --rows 1000000`).
- Verified: `bash -n scripts/run-bench.sh` clean; sink config fields checked
  against `crates/sink/postgres/src/config.rs`.
## Phase 6 — effectively-once wording sweep
## Phase 7 — Reachability + contributor on-ramp
