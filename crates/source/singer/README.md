# faucet-source-singer

> **Support tier: Tier-2 / experimental.** Best-effort — correctness bugs are
> fixed, but breadth of testing and upstream-drift tracking are not guaranteed.

A [Singer](https://www.singer.io/) tap bridge source for
[faucet-stream](https://github.com/PawanSikawat/faucet-stream). It runs an
existing Singer **tap** executable and adapts its stdout message stream into
faucet records — so any of the hundreds of community taps can feed a faucet
pipeline.

## Honest trade-offs

- **It reintroduces a runtime dependency.** Most Singer taps are Python; a
  pipeline using this source needs that interpreter (and the tap) installed on
  the box. This is the one place faucet steps outside its "single static binary"
  story — use a native faucet source when one exists.
- **Throughput is Singer-class, not faucet-class.** Records cross a process
  boundary as newline-delimited JSON; expect tap-bound throughput, not faucet's
  native streaming numbers.
- **Resume granularity depends on the tap.** faucet checkpoints at the tap's own
  `STATE` messages; how coarse or fine that is (and whether re-emitted rows
  overlap) is a property of the individual tap. Pair with an idempotent sink for
  clean **effectively-once** (idempotent at-least-once) behavior.

## v0 scope

- **Single-stream.** Exactly the configured `stream` is emitted; RECORD messages
  for other streams are ignored. Multi-stream fan-out is future work.
- Handles `RECORD`, `SCHEMA` (pass-through — faucet sinks infer schema from
  records), and `STATE` (resume bookmark). `ACTIVATE_VERSION` / `BATCH` are
  logged and skipped.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `executable` | string | — (required) | Tap binary on `PATH` or an absolute path |
| `stream` | string | — (required) | The single stream to emit |
| `args` | string[] | `[]` | Extra args appended after faucet's `--config`/`--catalog`/`--state` |
| `tap_config` | object | `{}` | The tap's config (secret-resolved by faucet; written to a private temp file) |
| `catalog` | object | — | Singer catalog, passed as `--catalog` |
| `state_key` | string | `singer:{executable}:{stream}` | State-store key for the resume bookmark |
| `flush_on_state` | bool | `true` | Flush a page (and checkpoint) on every STATE message |
| `idle_timeout_secs` | int | — | Abort if no output arrives within this many seconds |
| `on_malformed` | `skip` \| `fail` | `skip` | What to do with a non-Singer output line |

## Example

```yaml
version: 1
pipeline:
  source:
    type: singer
    config:
      executable: tap-github
      stream: issues
      tap_config:
        access_token: ${env:GITHUB_TOKEN}
        repository: PawanSikawat/faucet-stream
  sink:
    type: jsonl
    config:
      path: ./out/issues.jsonl
  state:
    type: file
    config:
      path: ./state
```

## License

Licensed under either of Apache-2.0 or MIT at your option.
