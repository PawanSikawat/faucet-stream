# Engineering Principles

*The design principles the faucet-stream codebase actually embodies — each stated, justified, and backed by concrete evidence in the source.*

This document is descriptive, not aspirational. Every principle below is one the
codebase already follows; the "Evidence" lines point at the code that enforces
it. New contributions are expected to uphold these principles, and reviewers may
cite this page directly. Where a principle is codified as an enforceable rule,
the relevant [standard](./standards/) is linked.

The ordering is deliberate: the first principle — correctness over everything —
is the reason the project exists, and it breaks every tie below it.

---

## 1. Correctness first — never silently corrupt downstream data

**Statement.** A subtle bug that silently produces wrong output for a downstream
consumer is the worst class of defect in this codebase, worse than a crash or a
missing feature. Data-integrity paths (checkpoint ordering, retry safety,
pagination state, exactly-once) are held to a higher bar than anything else.

**Why.** faucet-stream moves data between systems of record. A crash is visible
and recoverable; a duplicated or dropped row that lands in a warehouse and feeds
a report is invisible and may never be caught. The cost asymmetry justifies
paying for correctness up front.

**Evidence.**
- The retry layer refuses to retry a non-idempotent `write_batch` unless the sink
  advertises `supports_idempotent_writes()` — a lost response on a committed
  write would otherwise duplicate every row (`crates/core/src/pipeline.rs`, the
  `with_retry_write!` macro). This is the single most load-bearing correctness
  decision in the runtime; see [ADR 0007](./adr/0007-retries.md).
- The **central invariant** — a page's bookmark is persisted only *after* the
  sink has durably written and flushed that page — holds identically across all
  three write paths in `run_stream`. See [ADR 0002](./adr/0002-checkpoint-ordering.md)
  and [Design invariants](./architecture/invariants.md).
- Silent-corruption-prone conditions get their own typed `FaucetError`
  variants — `QualityFailure`, `SchemaDrift`, `ContractViolation`, `CircuitOpen`
  (`crates/core/src/error.rs`) — so they surface as explicit, catchable failures
  rather than being swallowed.

**Standard:** [state](./standards/state.md), [error-handling](./standards/error-handling.md).

---

## 2. Composition over inheritance

**Statement.** Behaviour is layered by *wrapping* a value in another value that
implements the same trait, never by an inheritance hierarchy. Cross-cutting
concerns compose as onion layers around a plain connector.

**Why.** Rust has no inheritance, but more importantly composition keeps each
concern independently testable and optional. A connector author writes a bare
`Source`; the runtime decides whether to wrap it in instrumentation, transforms,
sharding, or state overrides. None of those concerns leak into the connector.

**Evidence.**
- `TransformingSource` (`crates/core/src/transforming_source.rs`) wraps any
  `Source` to apply transform stages per page — the canonical way transforms
  attach, with no per-connector escape hatch.
- `InstrumentedSource` / `InstrumentedSink` / `InstrumentedStateStore`
  (`crates/core/src/observability/`) wrap connectors to emit metrics and spans;
  the connector emits none itself.
- The CLI executor wraps a source in `StateKeyOverride` so per-row state keys
  override the source's natural one, without the source knowing.

**Standard:** [api-design](./standards/api-design.md). See also
[ADR 0003](./adr/0003-builder-pattern.md).

---

## 3. Small, focused, object-safe traits with defaulted methods

**Statement.** `Source` and `Sink` stay small and object-safe (`Box<dyn Source>`
works). Capabilities beyond the minimum are added as trait methods *with default
implementations*, so existing connectors never break when the trait grows.

**Why.** This is what makes the third-party marketplace viable: a connector from
2024 keeps compiling when the trait gains `discover()`, `enumerate_shards()`, or
`sink_guarantee()` in 2026, because each new method has a safe default. It also
keeps the mental model small — a new author implements two required methods and
opts into the rest.

**Evidence.**
- Every capability beyond `fetch_with_context` / `write_batch` is defaulted:
  `stream_pages`, `state_key`, `apply_start_bookmark`, `supports_exactly_once`,
  `capture_resume_position`, `is_shardable`/`enumerate_shards`/`apply_shard`,
  `supports_discover`/`discover`, `check`, `current_schema`/`evolve_schema`,
  `write_batch_idempotent`/`last_committed_token` (`crates/core/src/traits.rs`).
- No trait method uses connector-specific types, generics, or associated types
  that would break object safety — a hard rule for the ecosystem.

**Standard:** [api-design](./standards/api-design.md).

---

## 4. Progressive builder APIs

**Statement.** Optional behaviour is configured through chained `with_*` builder
methods that each default to the safe, minimal choice. A caller pays only for
the complexity they opt into.

**Why.** A single wide config struct forces every caller to reason about every
knob. A progressive builder lets `Pipeline::new(&source, &sink).run()` be the
complete happy path, while `with_state_store`, `with_dlq`, `with_quality`,
`with_delivery`, `with_cancel`, etc. remain discoverable and independently
composable.

**Evidence.**
- `Pipeline` exposes `with_state_store`, `with_name`, `with_row`, `with_dlq`,
  `with_quality`, `with_contract`, `with_masking`, `with_adaptive`, `with_cancel`,
  `with_delivery`, `with_resilience`, `with_schema_drift`
  (`crates/core/src/pipeline.rs`), each defaulting to "off".
- `RunStreamOptions` mirrors the same shape for callers who drive `run_stream`
  directly.

**Standard:** [api-design](./standards/api-design.md). See
[ADR 0003](./adr/0003-builder-pattern.md).

---

## 5. Observable by default

**Statement.** Every source, sink, transform, and state-store operation emits
metrics and spans automatically, with zero per-connector instrumentation code.

**Why.** Observability that must be added by hand is observability that is
missing exactly where it matters. By instrumenting at the pipeline boundary,
every connector — including third-party ones — is observable the moment it runs.

**Evidence.**
- The pipeline wraps connectors in the `observability/` decorators before the run
  starts (`Pipeline::run` in `crates/core/src/pipeline.rs`); connectors implement
  only `connector_name()` to supply a friendly label.
- The universal metric set (`faucet_source_*`, `faucet_sink_*`,
  `faucet_transform_*`, `faucet_state_*`, `faucet_pipeline_*`) is emitted by the
  decorators, not the connectors.

**Standard:** [logging](./standards/logging.md). See
[ADR 0008](./adr/0008-observability.md) and
[Observability architecture](./architecture/observability.md).

---

## 6. Explicit, externalized state

**Statement.** Resumability state lives in an explicit `StateStore`, keyed by a
validated key, and is written only through the pipeline's durability protocol.
Bookmarks are opaque values owned by the source that produced them.

**Why.** Hidden or implicit state is impossible to reason about across restarts
and across a cluster. Making state an explicit, pluggable interface lets the same
pipeline resume from a file, Redis, or Postgres, and lets the runtime guarantee
*when* state advances.

**Evidence.**
- `StateStore` is a three-method async trait (`get`/`put`/`delete` over
  `serde_json::Value`) with `validate_state_key` guarding every access
  (`crates/core/src/state.rs`).
- Bookmarks are opaque `Value`s: the pipeline never interprets them, only the
  producing source does (via `apply_start_bookmark`). Commit tokens are likewise
  stored opaquely by sinks and never parsed by them.

**Standard:** [state](./standards/state.md). See
[ADR 0006](./adr/0006-state-management.md) and
[State management](./architecture/state-management.md).

---

## 7. Deterministic, safe retries

**Statement.** Retries use bounded exponential backoff with jitter, are gated on
a typed classification of which errors are retriable, and never retry an
operation whose replay could duplicate committed data.

**Why.** Undisciplined retries either amplify a thundering herd or silently
duplicate writes. Both are worse than the original transient failure. Retry
safety is a special case of Principle 1.

**Evidence.**
- `crates/core/src/retry.rs` implements capped exponential backoff with
  decorrelated per-attempt jitter (`backoff_with_jitter`), bounded by
  `MAX_BACKOFF`.
- The resilience layer classifies errors into a closed `RetryClass` set and only
  retries those (`crates/core/src/resilience/classify.rs`).
- The write path retries `write_batch` only when the sink is idempotent (see
  Principle 1); `write_batch_idempotent` is always safe to retry because a
  replayed token-stamped write is a no-op.

**Standard:** [performance](./standards/performance.md) (backoff),
[state](./standards/state.md) (retry safety). See
[ADR 0007](./adr/0007-retries.md) and [Resilience](./architecture/resilience.md).

---

## 8. Backward compatibility is a feature

**Statement.** The public API evolves additively. Trait growth uses defaulted
methods; crates follow semver from a 1.0.0 floor; breaking changes are gated by a
tool, not vigilance.

**Why.** A connector ecosystem only forms if authors trust that their crate keeps
working. Compatibility is the contract that makes the marketplace possible.

**Evidence.**
- Defaulted trait methods (Principle 3) mean trait growth is non-breaking.
- Every crate is versioned `1.0.0` or higher — never `0.x` — a hard workspace
  rule; new crates scaffold at `1.0.0`.
- `cargo-semver-checks` gates the public API in CI.

**Standard:** [api-design](./standards/api-design.md). See
[Stability policy](./stability.md).

---

## 9. Safe defaults

**Statement.** Every default is the choice that is hardest to get wrong.
Stronger-but-riskier behaviour is always an explicit opt-in.

**Why.** Most users take the defaults. The default must therefore be the safe
one, even when a more aggressive setting would be faster or stronger.

**Evidence.**
- Delivery defaults to `AtLeastOnce`; exactly-once is an explicit
  `with_delivery(ExactlyOnce)` gated on strict preconditions
  (`crates/core/src/pipeline.rs`).
- `batch_size` is bounded by `MAX_BATCH_SIZE` and validated, so a typo cannot
  cause unbounded O(total) buffering; `0` (opt out of batching) is an explicit,
  documented sentinel (`validate_batch_size`).
- Riskier connector settings — e.g. the Postgres CDC replication connection
  defaulting `tls: disable` — are explicit and warn loudly rather than being
  silently enabled.

**Standard:** [api-design](./standards/api-design.md).

---

## 10. Bounded memory and minimal allocation

**Statement.** Memory use is O(batch_size), independent of total data volume.
The runtime holds at most one page of records at a time.

**Why.** A data-movement tool must move datasets larger than RAM. Streaming
page-by-page is what makes an unbounded source safe to run on a small machine.

**Evidence.**
- `Pipeline::run` drives `Source::stream_pages` and writes each `StreamPage` as it
  arrives, never materializing the whole result set (`crates/core/src/pipeline.rs`).
- `DEFAULT_BATCH_SIZE = 1000`, `MAX_BATCH_SIZE = 1_000_000`; the default
  `stream_pages` implementation chunks `fetch_all`, and native sources override it
  to stream from their underlying primitive.
- The known tension — `serde_json::Value` allocates per record — is acknowledged
  as the main allocation cost and is the motivation for the Arrow record-model
  exploration ([RFC 0002](../rfcs/0002-arrow-support.md)).

**Standard:** [performance](./standards/performance.md). See
[ADR 0001](./adr/0001-stream-pages.md) and [Batching](./architecture/batching.md).

---

## 11. Developer ergonomics for connector authors

**Statement.** Building a connector requires depending on exactly one crate,
learning one config shape, and implementing two required methods. Tooling makes
the rest discoverable.

**Why.** The marketplace thesis (a third party can publish
`faucet-source-*` / `faucet-sink-*` crates) only works if the barrier to entry is
low. Ergonomics is a growth strategy, not a nicety.

**Evidence.**
- `faucet-core` is the only required dependency and re-exports everything an
  author needs (`async_trait`, `serde_json` with `Value`/`json!`, `schemars` with
  `JsonSchema`/`schema_for!`).
- Auth/credentials everywhere serialize as one consistent adjacently-tagged
  `{ type, config }` shape, so learning it once transfers across connectors.
- `faucet init` scaffolds config from a connector's JSON Schema; `faucet doctor`
  probes connectivity; `faucet test` runs offline fixture-based pipeline tests.

**Standard:** [api-design](./standards/api-design.md). See
[Extensibility](./architecture/extensibility.md) and
[Connector authoring](./contributing/connector-authoring.md).

---

## Related

- [Architecture overview](./architecture/overview.md)
- [Design invariants](./architecture/invariants.md)
- [Repository standards](./standards/api-design.md)
- [Architecture Decision Records](./adr/0001-stream-pages.md)
- [Stability policy](./stability.md)
- [Architecture review](./architecture-review.md)
