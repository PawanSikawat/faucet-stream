//! Pipeline template registry (#444) — register a parameterized config **once**,
//! then trigger runs by `{id, params}`.
//!
//! This module is the single implementation the three surfaces adapt to:
//!
//! | Surface | Register | List / get | Trigger |
//! |---|---|---|---|
//! | HTTP (`faucet serve`) | `POST /v1/templates` | `GET /v1/templates[/{id}]` | `POST /v1/templates/{id}/runs` |
//! | MCP (`--mcp` / `faucet mcp`) | `register_template` | `list_templates` / `get_template` | `run_template` |
//! | CLI | `faucet template register` | `faucet template list` / `show` | `faucet template run` |
//!
//! [`register`] validates a submitted config and appends a new version;
//! [`promote`] moves a named channel; [`resolve_version`] turns a selector into a
//! concrete version; [`materialize`] turns `{id, version, params, env}` back into
//! a ready-to-run config document.
//!
//! ## Versions and channels
//!
//! Versions are **numeric and auto-incrementing** — every register appends
//! `max + 1`, so iterating a template never disturbs what is already running.
//! On top of that sits a **closed set of named channels**
//! ([`VersionChannel`](crate::serve::history::templates::VersionChannel)):
//! `latest` (derived — always the newest, never assignable) plus the movable
//! pointers `dev`, `test`, `staging`, `pre-prod`, `canary`, `stable`, `prod`, and
//! `previous`. The set is closed on purpose: an open tag namespace becomes a
//! second, unreviewable naming system in which `prd` silently creates a channel
//! nobody watches. Everything downstream of `materialize` is the ordinary run
//! path (`load_submission` → `expand` → `run_expanded`), so templates inherit
//! every existing guarantee — matrix/topology expansion, the exactly-once gate,
//! idempotency keys, RBAC, and the audit log — for free.
//!
//! ## What is and isn't persisted
//!
//! The body is stored **verbatim**: `${env:…}` / `${vault:…}` / `${secret:…}`
//! directives stay unresolved and are resolved at trigger time against the
//! *executing server's* environment and credentials — exactly the privilege
//! surface of a normally-submitted config. Caller-supplied `secret: true` param
//! values are never persisted at all; see [`materialize`] for the one place that
//! distinction is enforced.

pub mod store;

pub use store::{
    MaterializedConfig, RegisterRequest, TemplateStore, materialize, promote, register,
    resolve_store_url, resolve_version,
};
