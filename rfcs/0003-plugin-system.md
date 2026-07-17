# RFC 0003 — Plugin system evolution

*Grow the compile-time `PluginRegistry` toward richer capability/version negotiation and marketplace discovery, while keeping static linking the pragmatic default over dynamic loading.*

| | |
|---|---|
| **RFC** | 0003 |
| **Title** | Plugin system evolution |
| **Status** | Draft (proposal) |
| **Authors** | faucet-stream maintainers |
| **Related issues** | #60, epic #38 |
| **Related ADRs** | [0010 pipeline runtime](../docs/adr/0010-pipeline-runtime.md), [0003 builder pattern](../docs/adr/0003-builder-pattern.md) |

## Summary

faucet-stream already supports third-party connectors through a **compile-time**
plugin registry: a custom CLI links its connector crates and registers them via
`PluginRegistry` before calling `run_main` (`cli/src/registry.rs`). This RFC
proposes evolving that model with capability/version negotiation and marketplace
discovery — and argues explicitly for keeping static linking the default rather
than adopting dynamic `.so`/`.dll` loading.

## Motivation

The current model (issue #60) is solid and safe:

- `PluginRegistry` holds `SourceFactory` / `SinkFactory` closures
  (`Fn(Value) -> CliResult<Box<dyn Source|Sink>>`), registered via
  `register_source[_with]` / `register_sink[_with]`, collision-checked against
  built-ins.
- A custom-CLI author writes a one-line `main.rs`:
  `faucet_cli::run_main(PluginRegistry::with_builtins().register_source(...))`.
- Because it is compile-time, there is no ABI surface, no unsafe dynamic loading,
  and the borrow checker + type system cover the whole plugin.

Two gaps limit the ecosystem story:

1. **No capability/version negotiation.** A plugin connector implements the same
   defaulted trait methods as a built-in, but there is no declared contract
   version or capability handshake — so the host cannot reason about a plugin's
   support surface uniformly (this is the problem
   [RFC 0001](./0001-capability-traits.md) addresses on the trait side).
2. **No discovery.** Finding, trusting, and installing third-party
   `faucet-source-*` / `faucet-sink-*` crates is manual. The naming convention
   and the [connector spec](../docs/spec/faucet-connector-spec-v0.md) exist; a
   discovery/marketplace layer does not.

## Guide-level explanation

Three additive layers on top of the existing registry:

1. **Contract versioning.** Each connector declares which version of the Faucet
   Connector Protocol (FCP) it targets. `run_main` records this and can warn or
   refuse on a mismatch, giving a clear error instead of a subtle trait-behaviour
   drift.
2. **Capability negotiation.** The host reads a plugin's
   [capability descriptor](./0001-capability-traits.md) at registration time, so
   `faucet validate` / `doctor` treat plugin and built-in connectors identically.
3. **Marketplace discovery.** A `faucet` subcommand and/or a manifest format lets
   users find connectors following the `faucet-source-*` / `faucet-sink-*`
   naming convention on crates.io, scaffolded from the FCP spec — still linked at
   build time, but discoverable and templated.

Nothing here changes how an existing custom CLI is written; a plugin that omits
the new metadata gets sensible defaults.

## Reference-level explanation

- **FCP version field.** Add a `const FCP_VERSION` / method to the connector
  contract and record it in the registry entry. `run_main` validates it against
  the host's supported range and emits a typed `CliError` on incompatibility.
- **Capabilities at registration.** Once [RFC 0001](./0001-capability-traits.md)
  lands, the registry stores each connector's capability descriptor so the CLI's
  static allowlists (`source_supports_exactly_once`,
  `sink_supported_write_modes`, …) are populated from plugins too, not only
  built-ins.
- **Scaffolding + spec.** Formalize [`docs/spec/faucet-connector-spec-v0.md`](../docs/spec/faucet-connector-spec-v0.md)
  as the normative contract and provide a `cargo generate`-style template so new
  connectors start compliant.
- **Static linking stays default.** No `dlopen`. See the drawbacks for why.

### Why static-link-first (and probably static-link-only for v1)

Dynamic loading in Rust is genuinely hazardous:

- **No stable ABI.** Rust has no stable ABI; a plugin `.so` and host built with
  different compiler versions can violate layout assumptions. A safe dynamic
  boundary means constraining the interface to `extern "C"` + `#[repr(C)]` +
  raw pointers, which is exactly the ergonomic surface `faucet-core` avoids
  (`Box<dyn Source>` and `serde_json::Value` are not FFI-safe).
- **Safety.** A loaded `.so` runs with full process privileges; a panic across
  the FFI boundary is undefined behaviour unless carefully caught.
- **Diminishing returns.** The compile-time model already delivers the headline
  benefit — third parties ship connectors as ordinary crates and link them into
  a custom `faucet` binary — with none of the ABI/safety cost.

Static linking keeps the whole plugin inside Rust's type and safety guarantees.
The pragmatic default is: make the *ecosystem* (discovery, versioning,
scaffolding, trust) richer, not the *loading mechanism* more dangerous.

## Drawbacks

- **Rebuild to add a connector.** Static linking means users assemble their own
  binary (or use the full distribution). For most operators the prebuilt CLI
  with all built-ins suffices; power users already build custom CLIs.
- **Version field maintenance.** An FCP version introduces a compatibility
  matrix to keep honest.
- **Marketplace trust.** Discovery raises supply-chain questions (a connector is
  arbitrary Rust code linked into the binary) that documentation must address
  head-on.

## Rationale and alternatives

- **Dynamic `dlopen` plugins.** Rejected as the default for the ABI/safety
  reasons above; may be revisited only behind a narrow, explicitly-`unsafe`,
  `extern "C"` shim if a compelling use case (e.g. closed-source connectors)
  emerges.
- **WASM component plugins.** Interesting for sandboxing/portability; deferred —
  the async + native-client (rdkafka, sqlx, cloud SDKs) requirements of real
  connectors make a WASM boundary costly today. Worth a future RFC.
- **Status quo (registry only).** Viable, but leaves versioning and discovery
  unsolved.

## Prior art

Kafka Connect (JAR plugins on a classpath — JVM has a stable-ish ABI we lack);
Singer/Meltano (process-per-tap over stdio — a robust dynamic boundary we could
emulate for out-of-process connectors); Terraform providers (separate processes
over gRPC — the strongest model for a language-agnostic, safe dynamic boundary,
and the most likely inspiration if we ever go out-of-process).

## Unresolved questions

- Must resolve before Accepted: the FCP version compatibility policy (strict vs
  range) and whether v1 includes any out-of-process option at all.
- During implementation: marketplace manifest format; trust/signing story.

## Future possibilities

- Out-of-process connectors over a stable protocol (Terraform-provider style),
  which *would* be a true dynamic boundary without Rust-ABI risk.
- WASM-sandboxed transforms/connectors.

## Related

- [RFC process](./README.md) · [RFC 0001 capabilities](./0001-capability-traits.md)
- [Extensibility](../docs/architecture/extensibility.md) · [Connector spec (FCP v0)](../docs/spec/faucet-connector-spec-v0.md) · [ADR 0010](../docs/adr/0010-pipeline-runtime.md)
