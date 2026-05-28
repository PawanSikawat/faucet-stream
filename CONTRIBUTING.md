# Contributing to faucet-stream

Thanks for your interest in contributing! faucet-stream is a Rust workspace of
connector crates plus the `faucet` CLI, and it's designed as an ecosystem — both
core changes and third-party connectors are welcome.

By participating you agree to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).

## Getting set up

```bash
git clone https://github.com/PawanSikawat/faucet-stream
cd faucet-stream
cargo build --workspace
```

The toolchain is pinned in `rust-toolchain.toml`. Some connectors link native
libraries — the **Kafka** connectors build `librdkafka`, which needs `cmake` and
a C toolchain (`libsasl2-dev libssl-dev libcurl4-openssl-dev` on Debian/Ubuntu).

## Before you open a PR

Run the same checks CI runs:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features        # Kafka integration tests need Docker
cargo doc --workspace --all-features --no-deps   # must be warning-free
```

For pipelines that touch real services, the [`examples/`](./examples/) directory
has a `docker compose` stack and `make demo` for a no-infrastructure smoke test.

## Tests

- New functions and behaviors **must** have tests — untested public API is a
  liability. Unit tests live in `#[cfg(test)]` modules; HTTP-based connectors use
  `wiremock` integration tests under the crate's `tests/`.
- Don't blindly update an existing test to make it pass — if a change breaks a
  test, investigate first; silently rewriting tests hides regressions.
- Assert the specific outcome, not just "no panic".

## Code quality

- Every failure path maps to a typed `FaucetError` variant. No `.unwrap()` /
  `.expect()` on anything that can fail at runtime.
- No hardcoded credentials, tokens, or service URLs — ever.
- Reuse clients/connections; pool database connections; prefer bulk/multi-row
  APIs and streaming. Performance and reliability are the project's first
  priority.

## Adding a connector

faucet-stream connectors follow a fixed shape. To add `faucet-source-foo` /
`faucet-sink-foo`:

1. **Crate layout** — `lib.rs` (re-exports), `config.rs` (config struct + enums,
   no I/O), `stream.rs` / `sink.rs` (the only place that does I/O; create
   clients/pools in `new()`).
2. **Config** — derive `Serialize + Deserialize + JsonSchema`; implement
   `config_schema()` via `schema_for!`.
3. **docs.rs** — add `[package.metadata.docs.rs]` (`all-features = true`,
   `rustdoc-args = ["--cfg", "docsrs"]`) and make the first line of `lib.rs`
   `#![cfg_attr(docsrs, feature(doc_cfg))]`.
4. **Tests** — unit + integration.
5. **Wire it up** — add the feature to the umbrella crate, the CLI registry, and
   the `feature-check` matrix in `.github/workflows/ci.yml`.
6. **Docs** — a crate `README.md`, an entry in the root README + the docs-site
   [connector catalog](./docs/book/src/reference/connectors.md), and a runnable
   example under `cli/examples/`.

Shared types for a source/sink pair (auth, formats) go in a
`faucet-<name>-common` crate that both depend on. See `faucet-source-rest` for a
reference implementation, and the docs-site
[authoring guide](./docs/book/src/extending/authoring-connectors.md).

## Filing issues

Use the issue templates. We label every issue with:

- **Type** — `feature` (new capability), `enhancement` (improve existing), or
  `bug` (incorrect behavior).
- **Tier** — `tier-1` (critical / blocks a core use case), `tier-2` (important),
  or `tier-3` (nice-to-have).

Search open issues before filing to avoid duplicates. Feature/enhancement issues
are tracked in the roadmap epic (search the `epic` label).

## Pull requests

- Keep PRs focused; one logical change per PR.
- Put `Closes #N` in the **PR body** (not just commit messages) to link the issue.
- Update the relevant crate `README.md`, the root README, and the docs site when
  you change config fields, defaults, or behavior.
- Don't skip hooks (`--no-verify`) or CI; if a check fails, fix the root cause.

## Versioning & MSRV

- **Semantic Versioning.** The project follows [SemVer](https://semver.org/).
  While pre-1.0, breaking changes may land in minor (`0.x`) releases, but we call
  them out in the changelog. For a connector, a **breaking change** includes
  renaming/removing a config field, changing a field's type, or changing a
  default in a way that alters behavior — not just Rust API changes.
- **Independent crate versions.** Connector crates version independently on
  crates.io, so `faucet-source-rest` and `faucet-sink-bigquery` may sit at
  different versions. The repo-level `vX.Y.Z` tags track the overall release line.
- **Changelog.** Notable changes are recorded in [CHANGELOG.md](./CHANGELOG.md),
  generated from Conventional Commit messages via
  [git-cliff](https://git-cliff.org) (`cliff.toml`). Write commit subjects as
  `type(scope): summary` (`feat`, `fix`, `perf`, `refactor`, `docs`, `test`,
  `ci`, `chore`) so they're grouped correctly.
- **MSRV.** The minimum supported Rust version is pinned in
  `rust-toolchain.toml` and enforced by CI (fmt/clippy/test/docs all run on it).
  Bumping the MSRV is itself a notable change — raise it only when needed and
  note it in the changelog.

## License

By contributing, you agree that your contributions are licensed under both the
MIT and Apache-2.0 licenses, matching the project's dual license.
