//! `ServeConfig` — the validated, runtime-ready server configuration built from
//! `ServeArgs`. The no-auth gate lives here so an unauthenticated server can
//! never start silently.

use crate::cli::ServeArgs;
use crate::error::{CliError, CliResult};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// How `/v1/*` requests are authenticated.
#[derive(Debug, Clone)]
pub enum AuthMode {
    /// Require `Authorization: Bearer <token>`.
    Token(String),
    /// Authentication explicitly disabled via `--no-auth`.
    None,
}

/// Run-history storage backend selection (parsed from `--history`).
#[derive(Debug, Clone)]
pub enum HistoryBackendSpec {
    /// In-process `DashMap`; lost on restart (default).
    Memory,
    /// Postgres connection URL (stored verbatim, e.g. `postgres://host/db`).
    Postgres(String),
    /// SQLite connection URL, stored verbatim including the `sqlite:` scheme
    /// (e.g. `sqlite:runs.db`, `sqlite::memory:`) so the backend can hand it
    /// straight to `sqlx` without re-deriving the form.
    Sqlite(String),
}

/// Validated server configuration.
#[derive(Debug, Clone)]
pub struct ServeConfig {
    pub listen: SocketAddr,
    pub auth: AuthMode,
    pub max_concurrent_runs: usize,
    pub max_queued_runs: usize,
    pub default_config_path: Option<PathBuf>,
    pub history: HistoryBackendSpec,
    pub cors_origins: Vec<String>,
    pub body_limit_bytes: usize,
    pub shutdown_grace: Duration,
    pub retain_terminal_runs: Duration,
    pub idempotency_retention: Duration,
    /// Run-ownership lease TTL for multi-instance orphan fencing (#146 H7).
    pub lease_ttl: Duration,
    pub probe_timeout: Duration,
    pub env_file: Option<PathBuf>,
    pub no_env_file: bool,
    /// Tracing filter directive for serve's own subscriber. Set from the
    /// clap-resolved `--log-level` / `FAUCET_LOG`; defaults to `"info"`.
    pub log_level: String,
}

fn default_max_concurrent() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, 16)
}

impl ServeConfig {
    /// Build + validate a `ServeConfig` from parsed CLI args. Enforces the
    /// no-auth gate: a server with neither a token nor `--no-auth` refuses to start.
    pub fn from_args(args: ServeArgs) -> CliResult<Self> {
        let auth = match (args.auth_token, args.no_auth) {
            (Some(t), _) if t.is_empty() => {
                return Err(CliError::Serve(
                    "--auth-token / FAUCET_SERVE_AUTH_TOKEN must not be empty \
                     (use --no-auth to explicitly disable authentication)"
                        .into(),
                ));
            }
            (Some(t), _) => AuthMode::Token(t),
            (None, true) => AuthMode::None,
            (None, false) => {
                return Err(CliError::Serve(
                    "refusing to start without authentication: pass --auth-token \
                     (or FAUCET_SERVE_AUTH_TOKEN), or --no-auth to explicitly disable it"
                        .into(),
                ));
            }
        };

        let listen: SocketAddr = args
            .listen
            .parse()
            .map_err(|e| CliError::Serve(format!("invalid --listen '{}': {e}", args.listen)))?;

        let history = match args.history {
            None => HistoryBackendSpec::Memory,
            Some(u) if u.starts_with("postgres://") || u.starts_with("postgresql://") => {
                HistoryBackendSpec::Postgres(u)
            }
            Some(u) if u.starts_with("sqlite:") => HistoryBackendSpec::Sqlite(u),
            Some(u) => {
                return Err(CliError::Serve(format!(
                    "unrecognised --history '{u}': use a postgres:// URL or sqlite:<path>"
                )));
            }
        };

        if args.lease_ttl_secs == 0 {
            return Err(CliError::Serve(
                "--lease-ttl-secs must be > 0 (it gates multi-instance orphan recovery; \
                 0 would immediately expire every run's lease and let a maintenance tick \
                 fail in-flight runs)"
                    .into(),
            ));
        }

        let max_concurrent_runs = args
            .max_concurrent_runs
            .unwrap_or_else(default_max_concurrent)
            .max(1);
        let max_queued_runs = args
            .max_queued_runs
            .unwrap_or_else(|| max_concurrent_runs.saturating_mul(8))
            .max(1);

        Ok(Self {
            listen,
            auth,
            max_concurrent_runs,
            max_queued_runs,
            default_config_path: args.default_config,
            history,
            cors_origins: args.cors_origin,
            body_limit_bytes: args.body_limit_bytes,
            shutdown_grace: Duration::from_secs(args.shutdown_grace_secs),
            retain_terminal_runs: Duration::from_secs(args.retain_terminal_runs_secs),
            idempotency_retention: Duration::from_secs(args.idempotency_retention_secs),
            lease_ttl: Duration::from_secs(args.lease_ttl_secs),
            probe_timeout: Duration::from_secs(args.probe_timeout_secs),
            env_file: args.env_file,
            no_env_file: args.no_env_file,
            log_level: "info".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn base_args() -> crate::cli::ServeArgs {
        crate::cli::ServeArgs {
            listen: "127.0.0.1:8080".into(),
            auth_token: None,
            no_auth: false,
            max_concurrent_runs: None,
            max_queued_runs: None,
            default_config: None,
            history: None,
            cors_origin: vec![],
            body_limit_bytes: 1_048_576,
            shutdown_grace_secs: 60,
            retain_terminal_runs_secs: 604_800,
            idempotency_retention_secs: 86_400,
            lease_ttl_secs: 30,
            probe_timeout_secs: 10,
            env_file: None,
            no_env_file: false,
        }
    }

    #[test]
    fn no_auth_gate_rejects_silent_unauthenticated_start() {
        let err = ServeConfig::from_args(base_args()).unwrap_err();
        assert!(err.to_string().contains("--no-auth"), "{err}");
    }

    #[test]
    fn explicit_no_auth_is_allowed() {
        let mut a = base_args();
        a.no_auth = true;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert!(matches!(cfg.auth, AuthMode::None));
    }

    #[test]
    fn token_sets_token_auth() {
        let mut a = base_args();
        a.auth_token = Some("hunter2".into());
        let cfg = ServeConfig::from_args(a).unwrap();
        assert!(matches!(cfg.auth, AuthMode::Token(t) if t == "hunter2"));
    }

    #[test]
    fn listen_parses_to_socket_addr() {
        let mut a = base_args();
        a.no_auth = true;
        a.listen = "0.0.0.0:9999".into();
        let cfg = ServeConfig::from_args(a).unwrap();
        assert_eq!(cfg.listen, "0.0.0.0:9999".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn history_url_selects_backend() {
        let mut a = base_args();
        a.no_auth = true;
        a.history = Some("postgres://localhost/db".into());
        assert!(matches!(
            ServeConfig::from_args(a.clone()).unwrap().history,
            HistoryBackendSpec::Postgres(_)
        ));
        // The `postgresql://` alias maps to the same backend.
        a.history = Some("postgresql://localhost/db".into());
        assert!(matches!(
            ServeConfig::from_args(a.clone()).unwrap().history,
            HistoryBackendSpec::Postgres(_)
        ));
        // SQLite is stored verbatim, scheme included (for direct sqlx use).
        a.history = Some("sqlite:runs.db".into());
        match ServeConfig::from_args(a).unwrap().history {
            HistoryBackendSpec::Sqlite(url) => assert_eq!(url, "sqlite:runs.db"),
            other => panic!("expected Sqlite, got {other:?}"),
        }
    }

    #[test]
    fn empty_token_is_rejected() {
        let mut a = base_args();
        a.auth_token = Some(String::new());
        let err = ServeConfig::from_args(a).unwrap_err();
        assert!(err.to_string().contains("must not be empty"), "{err}");
    }

    #[test]
    fn invalid_listen_returns_error() {
        let mut a = base_args();
        a.no_auth = true;
        a.listen = "not-a-socket".into();
        let err = ServeConfig::from_args(a).unwrap_err();
        assert!(err.to_string().contains("invalid --listen"), "{err}");
    }

    #[test]
    fn unrecognised_history_scheme_returns_error() {
        let mut a = base_args();
        a.no_auth = true;
        a.history = Some("mysql://localhost/db".into());
        let err = ServeConfig::from_args(a).unwrap_err();
        assert!(err.to_string().contains("unrecognised --history"), "{err}");
    }

    #[test]
    fn zero_lease_ttl_is_rejected() {
        let mut a = base_args();
        a.no_auth = true;
        a.lease_ttl_secs = 0;
        let err = ServeConfig::from_args(a).unwrap_err();
        assert!(err.to_string().contains("--lease-ttl-secs"), "{err}");
    }

    #[test]
    fn lease_ttl_maps_to_duration() {
        let mut a = base_args();
        a.no_auth = true;
        a.lease_ttl_secs = 45;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert_eq!(cfg.lease_ttl, Duration::from_secs(45));
    }
}
