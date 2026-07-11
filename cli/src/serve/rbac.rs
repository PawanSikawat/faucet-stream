//! Role-based access control for the `faucet serve` control plane (#205).
//!
//! The default single-`--auth-token` mode is one implicit `admin` principal. A
//! `--auth-config <file>` promotes serve to a multi-principal deployment: a list
//! of `{ name, token, role }` principals, each token mapped to a [`Role`] that
//! grants a fixed set of [`Permission`]s. Every `/v1` route declares the
//! permission it needs ([`required_permission`]); the auth middleware
//! (`serve::auth::require_auth`) resolves the bearer token to an
//! [`AuthContext`] and denies (`403`) any request whose role lacks the permission.
//!
//! Tokens are compared in constant time (via `serve::auth::constant_time_eq`)
//! and never appear in `{:?}` output — [`PrincipalSpec`]'s `Debug` masks them,
//! and the server registers every token with the redaction writer at startup.

use crate::error::{CliError, CliResult};
use axum::http::Method;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// A discrete capability a route requires. Roles grant a fixed set of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    /// Read run records / logs (`GET /v1/runs*`).
    RunRead,
    /// Submit / cancel / delete runs (`POST`/`DELETE /v1/runs*`).
    RunWrite,
    /// Read the connector/transform schema catalog (`GET /v1/schemas*`).
    SchemaRead,
    /// Run the preflight probe (`POST /v1/doctor`).
    Doctor,
    /// Fire an event-driven trigger (`POST`/`PUT /v1/triggers/{name}`).
    TriggerFire,
    /// Inspect a dead-letter-queue location (`POST /v1/dlq/inspect`) — read-only.
    DlqRead,
    /// Replay / discard dead-letter-queue envelopes
    /// (`POST /v1/dlq/replay`, `POST /v1/dlq/discard`).
    DlqManage,
    /// Read the Data Movement Catalog (`GET /v1/catalog/*`, #279) — read-only.
    CatalogRead,
    /// Read the audit log (`GET /v1/audit`) — admin-only.
    AuditRead,
    /// Hot-reload the server's `--default-config` (`POST /v1/reload`) — admin-only.
    Reload,
}

/// A named role. Roles are a fixed, built-in ladder — `viewer` ⊂ `operator` ⊂
/// `admin` — chosen so the common cases (read-only dashboard user, run
/// operator, full admin) need no custom permission wiring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Read-only: runs + logs + schemas.
    Viewer,
    /// Everything a viewer can do, plus submit/cancel/delete runs, doctor, and
    /// firing triggers.
    Operator,
    /// Full access, including reading the audit log.
    Admin,
}

impl Role {
    /// Whether this role grants `perm`.
    pub fn grants(self, perm: Permission) -> bool {
        use Permission::*;
        match self {
            Role::Viewer => matches!(perm, RunRead | SchemaRead | DlqRead | CatalogRead),
            Role::Operator => {
                matches!(
                    perm,
                    RunRead
                        | SchemaRead
                        | DlqRead
                        | CatalogRead
                        | RunWrite
                        | Doctor
                        | TriggerFire
                        | DlqManage
                )
            }
            Role::Admin => true,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Admin => "admin",
        }
    }
}

/// One principal entry in an `--auth-config` file: a human-readable `name`, its
/// bearer `token`, and the `role` it is granted.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalSpec {
    pub name: String,
    pub token: String,
    pub role: Role,
}

// Hand-written Debug so a `{:?}` of a spec (or the RbacConfig embedding it) never
// prints the bearer token in clear — mirrors `AuthMode`'s masking.
impl std::fmt::Debug for PrincipalSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrincipalSpec")
            .field("name", &self.name)
            .field("token", &"***")
            .field("role", &self.role)
            .finish()
    }
}

/// File shape for `--auth-config` (`{ principals: [ … ] }`), parsed from YAML or
/// JSON (YAML is a JSON superset, so one parser handles both).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthConfigFile {
    principals: Vec<PrincipalSpec>,
}

/// A validated RBAC configuration: a non-empty set of principals with unique
/// names and unique, non-empty tokens.
#[derive(Debug, Clone)]
pub struct RbacConfig {
    principals: Vec<PrincipalSpec>,
}

/// The resolved identity for one request, carried in the request extensions for
/// handlers (and the audit writer) to read. Holds no token.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub principal: String,
    pub role: Role,
    pub source_ip: Option<String>,
}

impl AuthContext {
    /// Actor for a trigger-originated (non-HTTP) submission — `trigger:<name>`,
    /// treated as an operator for audit attribution.
    pub fn trigger(name: &str) -> Self {
        Self {
            principal: format!("trigger:{name}"),
            role: Role::Operator,
            source_ip: None,
        }
    }
}

impl RbacConfig {
    /// Load + validate an `--auth-config` file (YAML or JSON).
    pub fn from_file(path: &Path) -> CliResult<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            CliError::Serve(format!("reading --auth-config {}: {e}", path.display()))
        })?;
        let file: AuthConfigFile = serde_yaml::from_str(&text).map_err(|e| {
            CliError::Serve(format!("parsing --auth-config {}: {e}", path.display()))
        })?;
        Self::new(file.principals)
    }

    /// Build from an already-parsed principal list, validating invariants.
    pub fn new(principals: Vec<PrincipalSpec>) -> CliResult<Self> {
        if principals.is_empty() {
            return Err(CliError::Serve(
                "--auth-config must define at least one principal".into(),
            ));
        }
        let mut seen_names = std::collections::HashSet::new();
        let mut seen_tokens = std::collections::HashSet::new();
        for p in &principals {
            if p.name.trim().is_empty() {
                return Err(CliError::Serve(
                    "--auth-config: every principal must have a non-empty name".into(),
                ));
            }
            if p.token.is_empty() {
                return Err(CliError::Serve(format!(
                    "--auth-config: principal '{}' has an empty token",
                    p.name
                )));
            }
            if !seen_names.insert(p.name.clone()) {
                return Err(CliError::Serve(format!(
                    "--auth-config: duplicate principal name '{}'",
                    p.name
                )));
            }
            if !seen_tokens.insert(p.token.clone()) {
                return Err(CliError::Serve(format!(
                    "--auth-config: principal '{}' reuses a token already assigned to another \
                     principal",
                    p.name
                )));
            }
        }
        Ok(Self { principals })
    }

    /// Resolve a bearer token to its principal in constant time. Every principal
    /// is compared (no early return) so the match position doesn't leak via
    /// timing; the matched role/name is returned after the full scan.
    pub fn authenticate(&self, token: &str) -> Option<AuthContext> {
        let mut matched: Option<(&str, Role)> = None;
        for p in &self.principals {
            if crate::serve::auth::constant_time_eq(token.as_bytes(), p.token.as_bytes()) {
                matched = Some((p.name.as_str(), p.role));
            }
        }
        matched.map(|(name, role)| AuthContext {
            principal: name.to_string(),
            role,
            source_ip: None,
        })
    }

    /// Every configured token, for redaction registration at startup.
    pub fn tokens(&self) -> impl Iterator<Item = &str> {
        self.principals.iter().map(|p| p.token.as_str())
    }
}

/// The permission a `(method, matched-route-template)` pair requires. `None`
/// means the route has no specific mapping and is therefore admin-only (fail
/// closed for any route added without an explicit entry here).
pub fn required_permission(method: &Method, matched_path: &str) -> Option<Permission> {
    use Permission::*;
    match (method, matched_path) {
        (&Method::POST, "/v1/runs") => Some(RunWrite),
        (&Method::GET, "/v1/runs") => Some(RunRead),
        (&Method::GET, "/v1/runs/{id}") => Some(RunRead),
        (&Method::DELETE, "/v1/runs/{id}") => Some(RunWrite),
        (&Method::POST, "/v1/runs/{id}/cancel") => Some(RunWrite),
        (&Method::GET, "/v1/runs/{id}/logs") => Some(RunRead),
        (&Method::GET, "/v1/schemas") => Some(SchemaRead),
        (&Method::GET, "/v1/schemas/{kind}/{name}") => Some(SchemaRead),
        (&Method::POST, "/v1/doctor") => Some(Doctor),
        (&Method::POST, "/v1/backfill") => Some(RunWrite),
        (&Method::POST, "/v1/dlq/inspect") => Some(DlqRead),
        (&Method::POST, "/v1/dlq/replay") => Some(DlqManage),
        (&Method::POST, "/v1/dlq/discard") => Some(DlqManage),
        (&Method::GET, "/v1/audit") => Some(AuditRead),
        (&Method::POST, "/v1/triggers/{name}") => Some(TriggerFire),
        (&Method::PUT, "/v1/triggers/{name}") => Some(TriggerFire),
        (&Method::GET, "/v1/catalog/datasets") => Some(CatalogRead),
        (&Method::GET, "/v1/catalog/datasets/{id}") => Some(CatalogRead),
        (&Method::GET, "/v1/catalog/lineage") => Some(CatalogRead),
        (&Method::POST, "/v1/reload") => Some(Reload),
        _ => None,
    }
}

/// A short, stable audit action label for a `(method, matched-route)` pair.
pub fn audit_action(method: &Method, matched_path: &str) -> &'static str {
    match (method, matched_path) {
        (&Method::POST, "/v1/runs") => "run.submit",
        (&Method::GET, "/v1/runs") => "run.list",
        (&Method::GET, "/v1/runs/{id}") => "run.get",
        (&Method::DELETE, "/v1/runs/{id}") => "run.delete",
        (&Method::POST, "/v1/runs/{id}/cancel") => "run.cancel",
        (&Method::GET, "/v1/runs/{id}/logs") => "run.logs",
        (&Method::GET, "/v1/schemas") => "schema.list",
        (&Method::GET, "/v1/schemas/{kind}/{name}") => "schema.get",
        (&Method::POST, "/v1/doctor") => "doctor",
        (&Method::POST, "/v1/backfill") => "backfill.submit",
        (&Method::POST, "/v1/dlq/inspect") => "dlq.inspect",
        (&Method::POST, "/v1/dlq/replay") => "dlq.replay",
        (&Method::POST, "/v1/dlq/discard") => "dlq.discard",
        (&Method::GET, "/v1/audit") => "audit.list",
        (&Method::POST | &Method::PUT, "/v1/triggers/{name}") => "trigger.fire",
        (&Method::GET, "/v1/catalog/datasets") => "catalog.list",
        (&Method::GET, "/v1/catalog/datasets/{id}") => "catalog.get",
        (&Method::GET, "/v1/catalog/lineage") => "catalog.lineage",
        (&Method::POST, "/v1/reload") => "config.reload",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, token: &str, role: Role) -> PrincipalSpec {
        PrincipalSpec {
            name: name.into(),
            token: token.into(),
            role,
        }
    }

    #[test]
    fn role_permission_ladder() {
        use Permission::*;
        // Viewer: reads only.
        assert!(Role::Viewer.grants(RunRead));
        assert!(Role::Viewer.grants(SchemaRead));
        assert!(Role::Viewer.grants(DlqRead));
        assert!(!Role::Viewer.grants(RunWrite));
        assert!(!Role::Viewer.grants(Doctor));
        assert!(!Role::Viewer.grants(DlqManage));
        assert!(!Role::Viewer.grants(AuditRead));
        // Operator: reads + writes + doctor + triggers + dlq management, but not audit.
        assert!(Role::Operator.grants(RunWrite));
        assert!(Role::Operator.grants(Doctor));
        assert!(Role::Operator.grants(TriggerFire));
        assert!(Role::Operator.grants(DlqRead));
        assert!(Role::Operator.grants(DlqManage));
        assert!(!Role::Operator.grants(AuditRead));
        // Admin: everything.
        for p in [
            RunRead,
            RunWrite,
            SchemaRead,
            Doctor,
            TriggerFire,
            DlqRead,
            DlqManage,
            AuditRead,
        ] {
            assert!(Role::Admin.grants(p));
        }
    }

    #[test]
    fn authenticate_resolves_token_to_principal() {
        let cfg = RbacConfig::new(vec![
            spec("alice", "tok-a", Role::Admin),
            spec("bob", "tok-b", Role::Viewer),
        ])
        .unwrap();
        let a = cfg.authenticate("tok-a").unwrap();
        assert_eq!(a.principal, "alice");
        assert_eq!(a.role, Role::Admin);
        let b = cfg.authenticate("tok-b").unwrap();
        assert_eq!(b.role, Role::Viewer);
        assert!(cfg.authenticate("nope").is_none());
    }

    #[test]
    fn rejects_empty_duplicate_and_blank() {
        assert!(RbacConfig::new(vec![]).is_err());
        assert!(RbacConfig::new(vec![spec("", "t", Role::Admin)]).is_err());
        assert!(RbacConfig::new(vec![spec("a", "", Role::Admin)]).is_err());
        // Duplicate name.
        assert!(
            RbacConfig::new(vec![
                spec("a", "t1", Role::Admin),
                spec("a", "t2", Role::Viewer),
            ])
            .is_err()
        );
        // Duplicate token.
        assert!(
            RbacConfig::new(vec![
                spec("a", "dup", Role::Admin),
                spec("b", "dup", Role::Viewer),
            ])
            .is_err()
        );
    }

    #[test]
    fn debug_masks_token() {
        let s = format!("{:?}", spec("alice", "supersecret", Role::Admin));
        assert!(!s.contains("supersecret"), "token leaked: {s}");
        assert!(s.contains("***"));
    }

    #[test]
    fn trigger_actor_is_operator() {
        let ctx = AuthContext::trigger("nightly");
        assert_eq!(ctx.principal, "trigger:nightly");
        assert_eq!(ctx.role, Role::Operator);
        assert!(ctx.source_ip.is_none());
    }

    #[test]
    fn tokens_iterates_all_principals() {
        let cfg = RbacConfig::new(vec![
            spec("a", "t1", Role::Admin),
            spec("b", "t2", Role::Viewer),
        ])
        .unwrap();
        let toks: Vec<&str> = cfg.tokens().collect();
        assert_eq!(toks, vec!["t1", "t2"]);
    }

    #[test]
    fn required_permission_covers_all_routes() {
        use Permission::*;
        for (m, path, want) in [
            (Method::GET, "/v1/runs/{id}", RunRead),
            (Method::DELETE, "/v1/runs/{id}", RunWrite),
            (Method::POST, "/v1/runs/{id}/cancel", RunWrite),
            (Method::GET, "/v1/runs/{id}/logs", RunRead),
            (Method::GET, "/v1/schemas", SchemaRead),
            (Method::GET, "/v1/schemas/{kind}/{name}", SchemaRead),
            (Method::POST, "/v1/doctor", Doctor),
            (Method::POST, "/v1/triggers/{name}", TriggerFire),
            (Method::PUT, "/v1/triggers/{name}", TriggerFire),
            (Method::POST, "/v1/backfill", RunWrite),
            (Method::POST, "/v1/dlq/inspect", DlqRead),
            (Method::POST, "/v1/dlq/replay", DlqManage),
            (Method::POST, "/v1/dlq/discard", DlqManage),
            (Method::GET, "/v1/catalog/datasets", CatalogRead),
            (Method::GET, "/v1/catalog/datasets/{id}", CatalogRead),
            (Method::GET, "/v1/catalog/lineage", CatalogRead),
            (Method::POST, "/v1/reload", Reload),
        ] {
            assert_eq!(required_permission(&m, path), Some(want), "{m} {path}");
        }
        // Reload is admin-only.
        assert!(!Role::Viewer.grants(Permission::Reload));
        assert!(!Role::Operator.grants(Permission::Reload));
        assert!(Role::Admin.grants(Permission::Reload));
        // Every role can read the catalog; a viewer still can't write runs.
        assert!(Role::Viewer.grants(Permission::CatalogRead));
        assert!(Role::Operator.grants(Permission::CatalogRead));
        assert!(Role::Admin.grants(Permission::CatalogRead));
    }

    #[test]
    fn role_and_permission_serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&Role::Operator).unwrap(),
            "\"operator\""
        );
        assert_eq!(
            serde_json::to_string(&Permission::AuditRead).unwrap(),
            "\"audit_read\""
        );
    }

    #[test]
    fn required_permission_maps_routes() {
        assert_eq!(
            required_permission(&Method::POST, "/v1/runs"),
            Some(Permission::RunWrite)
        );
        assert_eq!(
            required_permission(&Method::GET, "/v1/runs"),
            Some(Permission::RunRead)
        );
        assert_eq!(
            required_permission(&Method::GET, "/v1/audit"),
            Some(Permission::AuditRead)
        );
        // Unmapped → admin-only (None).
        assert_eq!(required_permission(&Method::GET, "/v1/unknown"), None);
    }

    #[test]
    fn parses_yaml_and_json() {
        let yaml = "principals:\n  - name: alice\n    token: tok-a\n    role: admin\n";
        let cfg: AuthConfigFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.principals.len(), 1);
        let json = r#"{"principals":[{"name":"bob","token":"tok-b","role":"viewer"}]}"#;
        let cfg: AuthConfigFile = serde_yaml::from_str(json).unwrap();
        assert_eq!(cfg.principals[0].role, Role::Viewer);
    }

    #[test]
    fn audit_action_labels() {
        assert_eq!(audit_action(&Method::POST, "/v1/runs"), "run.submit");
        assert_eq!(
            audit_action(&Method::POST, "/v1/runs/{id}/cancel"),
            "run.cancel"
        );
        assert_eq!(audit_action(&Method::GET, "/v1/whatever"), "unknown");
    }
}
