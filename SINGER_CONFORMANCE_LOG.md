# Singer bridge + conformance — build log

Branch `feat/singer-bridge-and-conformance`. Remove before the final commit if asked.

## Baseline
- Off `main` @ 582f6c3 (a merged, CI-green release commit).
- `cargo build -p faucet-core` green with the pinned rustup 1.96.0 toolchain
  (Homebrew rustc shadows PATH — must pin both `RUSTC` and PATH).
- A full clean `cargo build/test --workspace` was **not** re-run from scratch
  (large; disk was recently at 100% and reclaimed). Each new crate is built +
  tested green, and every commit keeps the workspace compiling.

## Real faucet-core signatures confirmed (copied, not trusted from prompt)
- `StreamPage { records: Vec<Value>, bookmark: Option<Value> }` (`pipeline.rs`).
- `Source`: required `fetch_with_context(&self, &HashMap<String,Value>) -> Result<Vec<Value>>`;
  override `stream_pages<'a>(&'a self, &'a HashMap, usize) -> Pin<Box<dyn Stream<Item=Result<StreamPage>> + Send + 'a>>`;
  defaulted `config_schema`, `connector_name`, `state_key`, `apply_start_bookmark(&self, Value)`.
  **No `record_schema`** — confirmed; not added.
- Pipeline owns the `StateStore`: it reads `state_key()` and calls
  `apply_start_bookmark(value)`. The source does not touch the store directly.
- `FaucetError::Source(String)` / `Config(String)`.
- Core re-exports: `async_trait`, `Value`, `json`, `JsonSchema`, `schema_for`.

## Phase 1 — faucet-source-singer skeleton
- New crate `crates/source/singer` (mirrors `source-rest` layout):
  - `config.rs` — `SingerSourceConfig` + `MalformedPolicy` (serde + JsonSchema).
  - `message.rs` — `SingerMessage` + `parse_line` (RECORD/SCHEMA/STATE/Other, malformed → Err).
  - `process.rs` — `TapProcess`: spawn `<exe> --config <tmp> [--catalog] [--state] args…`;
    stdout → parsed lines over a **bounded** mpsc (backpressure); stderr drained to `tracing`
    with a conservative secret `Redactor`; SIGTERM→grace→SIGKILL + reap (`kill_on_drop` backstop);
    temp files 0600, deleted on drop.
  - `stream.rs` — `SingerSource: Source`. `stream_pages` is the real path (page assembly at
    `batch_size` / STATE / EOF; bookmark = latest STATE `value`; empty-with-bookmark supported;
    failed run does not commit the trailing un-checkpointed page). `fetch_with_context` collects
    pages with a bounded-memory `warn`. `state_key` + `apply_start_bookmark` round-trip the STATE.
    SCHEMA is pass-through with a `// FUTURE (core RFC):` note (no core change).
- Wired umbrella `source-singer` feature + `source::singer` re-export (both source-module blocks);
  added to the `source` aggregate.
- **Constraints honored:** single-stream only; no `faucet-core` changes; depends on `faucet-core` only;
  the word "exactly-once" appears nowhere (guarantee described as effectively-once / idempotent
  at-least-once).
- Acceptance: `cargo build -p faucet-source-singer` ✓; `cargo build -p faucet-stream --features source-singer` ✓;
  clippy clean; 11 lib unit tests pass.

## Phase 2 — tests (no real tap needed in CI)
- `assemble.rs` extracted as a pure `PageAssembler`; stream.rs now drives it (cleaner + unit-testable).
- Unit tests (18 total in the lib): message parsing (every type + malformed), page assembly at
  batch_size / STATE / EOF, bookmark attachment, empty-with-bookmark, other-stream ignore,
  flush_on_state=false deferral, redactor, 0600 temp file, state-key sanitization.
- Fake tap `tests/fake_taps/fake_tap.sh` — dependency-free POSIX sh; scripted RECORD/STATE NDJSON,
  `--crash-after-new N` (exit 1) mode, resume-aware via `--state` with a deliberate 1-record overlap
  (coarse resume) so the idempotent sink dedup is genuinely exercised. Tracked mode 100755.
- Integration (`tests/integration.rs`) through a real `faucet_core::Pipeline` + `MemoryStateStore`
  into an in-crate `UpsertSink` (keyed dedup = SQLite/Postgres `write_mode: upsert` stand-in):
  - `clean_run_writes_all_rows_once` — 6 rows, one each, bookmark persisted.
  - `crash_then_resume_produces_no_duplicates` — **THE PROOF**: run 1 crashes after the STATE(3)
    checkpoint (only ids 1-3 visible, bookmark last_id=3); run 2 resumes, tap re-emits id 3 (overlap)
    + 4,5,6; total write calls > 6 (overlap real) yet final ids = {1..6} with no dup. ✓
  - `real_tap_csv_end_to_end` — `#[ignore]`d (needs a real tap on PATH); documents how to run.
- No `DeliveryMode::ExactlyOnce` used (singer source is not deterministic-replay; the pipeline gate
  would reject it) — no-dup rides the keyed sink. The phrase "exactly-once" appears nowhere.
- clippy --all-targets clean; `cargo test -p faucet-source-singer` green (18 unit + 2 integ, 1 ignored).
