# faucet-conformance

A reusable connector **conformance test battery** for the
[faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Any
`faucet-source-*` / `faucet-sink-*` crate calls it from its own `tests/` to
prove it upholds the connector contract.

**Passing this battery is the Tier-1 (supported) criterion for a connector.**
There is no separate tiering scheme — a connector is "supported" exactly when it
invokes and passes these checks in CI; connectors that don't are Tier-2
(experimental).

## Checks

| # | Function | Status |
|---|---|---|
| 1 | `assert_config_schema_valid` | ✅ implemented — `config_schema()` is a valid, round-tripping JSON Schema |
| 2 | `assert_bounded_memory` | ✅ implemented — `stream_pages` pages instead of buffering the whole set |
| 3 | `assert_bookmark_roundtrip` | ✅ implemented — a resumed run picks up *after* the emitted bookmark; strictly fewer records reappear |
| 4 | `assert_idempotent_replay` | ✅ implemented — atomic-watermark replay (or keyed-upsert) converges to one row per key, no double-writes |
| 5 | `assert_capabilities_truthful` | ✅ implemented — advertised `supports_*` capabilities match actual behavior (idempotency, schema evolution) |
| 6 | `assert_errors_not_panics` | ✅ implemented — a failing source surfaces a typed `FaucetError` without unwinding |

## Usage

```rust
use faucet_conformance as conf;

#[test]
fn config_schema_is_valid() {
    let source = MySource::new(/* … */);
    conf::assert_config_schema_valid(&source);
}

#[tokio::test]
async fn streams_with_bounded_memory() {
    let source = MySource::over_n_rows(10_000);
    conf::assert_bounded_memory(&source, 500, 10_000).await;
}
```

Sinks use the value form of check 1:
`conf::assert_config_schema_valid_value(&sink.config_schema(), sink.connector_name())`.

## Doubles

`faucet_conformance::doubles` provides a `CountingSource` (lazily emits N
synthetic records in pages — genuinely bounded) and a `TestSink` (append or
keyed/upsert recording sink) for use in your own tests.

## License

Licensed under either of Apache-2.0 or MIT at your option.
