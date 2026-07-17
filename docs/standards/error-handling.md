# Error Handling Standard

*Every failure path is explicit, typed, and mapped to a `FaucetError` variant — silent incorrectness is the worst class of bug this project can ship.*

Data movement fails in the field: APIs rate-limit, WAL slots vanish, a page arrives with the wrong shape. The standard exists because a mishandled failure here does not crash loudly — it corrupts a downstream table quietly. Correctness of the failure path is therefore treated as equal to correctness of the happy path.

## The `FaucetError` taxonomy

All fallible operations return `Result<_, FaucetError>` (`crates/core/src/error.rs`). Pick the variant that names the failure; do not flatten everything into a string.

| Variant | Use for |
|---|---|
| `Http(reqwest::Error)` | Transport-level HTTP failures (`#[from]`). |
| `HttpStatus { status, url, body }` | A non-2xx response the connector chooses to surface with context. |
| `Json(serde_json::Error)` | (De)serialization failures (`#[from]`). |
| `JsonPath(String)` | A JSONPath/extraction expression that did not resolve as required. |
| `Auth(String)` | Credential acquisition/refresh failure. |
| `RateLimited(Duration)` | A throttle signal carrying the retry-after hint. |
| `Url(String)` | Malformed/unbuildable URL. |
| `Transform(String)` | A transform stage failed on a record/page. |
| `Config(String)` | Load-time / validation failure. **Prefer this over a mid-run panic** — reject bad config before any data moves. |
| `Source(String)` / `Sink(String)` | Connector-specific runtime failures with no better variant. |
| `QualityFailure { check, message }` | An `abort`-policy quality check tripped. |
| `SchemaDrift { columns, message }` | A drift policy set to `fail` (or `on_incompatible: fail`). |
| `ContractViolation { version, message }` | A data-contract breach under `on_breach: fail`. |
| `State(String)` | State-store get/put/delete failure. |
| `CircuitOpen { failures, cooldown }` | The resilience circuit breaker tripped after N fully-failed pages. |
| `Custom(Box<dyn Error + Send + Sync>)` | **Third-party connectors wrap their own error types here** (`#[from]`). Never remove this variant. |

- **MUST use `thiserror`** for all error enums, with a human-readable `#[error("…")]` per variant.
- **MUST add a typed variant for any silent-corruption-prone path.** The reason `QualityFailure`, `SchemaDrift`, and `ContractViolation` are first-class variants rather than `Source(String)` is that the runtime, the DLQ router, and dashboards must distinguish "a record broke a promise" from "the network hiccupped". If you introduce a new correctness gate, give it a typed variant.
- **Third-party connectors MUST route their driver errors through `Custom`**, not by stringifying into `Source`/`Sink` (stringifying loses the source chain).

## Panics

- **MUST NOT `.unwrap()` / `.expect()` on a value that can fail at runtime.** Network, parsing, config, and I/O are all runtime-fallible — they return `Result`.
- **MAY use `.expect()` only for a construction-time invariant** that was validated earlier and cannot be false at that point — and the message must state the invariant.

  ```rust
  // GOOD — runtime-fallible, returns a typed error.
  let resp = client.get(&url).send().await.map_err(FaucetError::Http)?;

  // GOOD — invariant validated in new(); message names it.
  let re = self.compiled.as_ref().expect("regex compiled in QualitySpec::compile");

  // BAD — a lost response or 500 becomes a panic that takes down the run.
  let resp = client.get(&url).send().await.unwrap();
  ```

## Explicitness

- **Every failure branch MUST be handled deliberately.** Think through the field failure modes for the code you touch: an empty page mid-stream, a malformed `Link` header, a JSONPath that matches nothing, an API that commits server-side but drops the response. Each must resolve to a defined outcome (typed error, retry, DLQ route, or documented skip) — never an implicit `unwrap`, silent drop, or duplicated write.
- **Config validation MUST be fail-fast.** Compile regexes, JSON Schemas, bounds, and enum values at load time and return `FaucetError::Config`, so an operator sees the problem at `faucet validate` rather than three hours into a run. This is how `CompiledQuality::compile`, `CompiledContract::compile`, and `SchemaDriftPolicy::compile` behave.

## Related

- [Error taxonomy in core](../architecture/overview.md)
- [Resilience](../architecture/resilience.md) · [Retries](../architecture/retries.md)
- [State & Durability Standard](./state.md)
- [Testing Standard](./testing.md)
- [Common Mistakes](../contributing/common-mistakes.md)
