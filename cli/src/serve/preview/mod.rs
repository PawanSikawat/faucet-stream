//! Dataset previews as **source-backed capped reads** (#586).
//!
//! The console's "show me the rows a run just wrote" feature is deliberately
//! *not* a file reader. It is one generic operation:
//!
//! > source spec (`type` + `config`) + a row cap
//! >   → one page via [`Source::stream_pages`](faucet_core::Source::stream_pages)
//! >   → rows
//!
//! Two layers, kept apart on purpose:
//!
//! 1. **The engine** ([`engine`]) — generic. It takes a connector kind, that
//!    connector's config, and a cap, and returns a page. It knows nothing about
//!    "local", nothing about the ledger, and nothing about who asked. Every
//!    source in the registry is fair game.
//! 2. **The trust / input policy** — per surface. For the *local* preview
//!    ([`crate::serve::handlers::preview`]) that policy is: no auth, no path
//!    from the caller (only ledger-recorded sink outputs), and off unless the
//!    operator opted in with `--preview-local-outputs`.
//!
//! Splitting it this way is what makes the non-local preview (#591) a strict
//! superset rather than a rewrite: it swaps in a different policy layer (remote
//! `type`s, a UI auth-collection step, production guardrails) over the *same*
//! engine.
//!
//! ## Limit, not limit/offset
//!
//! There is no `offset` and no cursor, and that is a deliberate reading of what
//! these sources are. A `.jsonl` or `.csv` file is a sequential byte stream with
//! no row index: `OFFSET 500` can only be implemented as *read the first 500
//! records and throw them away*, which costs exactly what asking for 1000
//! records and keeping the tail costs. Paging would therefore add a cursor, a
//! state contract, and a "did the file change between pages?" problem while
//! buying nothing. **"Show me more" is spelled "raise the limit"**, and the
//! engine makes that cheap by *stopping* rather than truncating (see
//! [`engine`]).
//!
//! ## Caps
//!
//! [`PreviewConfig`] holds the two ops-facing caps, and the request param that
//! rides between them:
//!
//! | knob | env | default | meaning |
//! |---|---|---|---|
//! | soft cap | `FAUCET_SERVE_PREVIEW_DEFAULT_ROWS` | 500 | rows loaded when `row_count_to_load` is omitted |
//! | hard cap | `FAUCET_SERVE_PREVIEW_MAX_ROWS` | 5000 | ceiling — a larger `row_count_to_load` is **clamped**, never honoured |
//!
//! **`0` means "no limit"** for either knob and for `row_count_to_load`
//! (`row_count_to_load=all` is the readable spelling), matching this codebase's
//! existing convention — `batch_size: 0` is "do not batch",
//! `retention_days: 0` is "keep forever". So a whole-dataset preview is
//! available, and *whether it is available at all* stays an operator decision:
//! with a non-zero hard cap the request is clamped to it, so the ceiling can
//! never be argued away by a client. Set `--preview-max-rows 0` to lift it.
//!
//! Unlimited is not unbounded: the engine still stops on a byte budget and a
//! deadline, and says which ([`engine::Capped`]) — a whole-dataset read of a file
//! that is larger than the server can hold comes back as a *truncated answer*,
//! never as an OOM.
//!
//! These caps govern the **serve** preview only. `faucet preview` is a local,
//! deliberate, single-user command with its own `--limit`: different trust
//! model, deliberately not sharing a knob.

pub mod engine;
pub mod jsonl;

pub use engine::{Capped, PreviewPage, PreviewRequest, read_capped};

/// Rows loaded when a request omits `row_count_to_load`. `0` = the whole dataset.
pub const DEFAULT_PREVIEW_ROWS: usize = 500;

/// Ceiling on the rows one preview request may load. `0` = no ceiling.
///
/// Ten times the soft cap, so raising the row count in the console is an
/// ordinary thing to do rather than something that immediately hits a wall,
/// while a *default* deployment still cannot be asked for a whole dataset.
pub const MAX_PREVIEW_ROWS: usize = 5_000;

/// What one request asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowRequest {
    /// This many rows.
    Count(usize),
    /// Every row in the dataset (still bounded by the hard cap, if there is one).
    All,
}

impl RowRequest {
    /// Parse a `row_count_to_load` query value.
    ///
    /// `all` (any case) and `0` both mean the whole dataset — the word for
    /// people, the number for the `0 = no limit` convention the rest of the
    /// config uses. Anything else must be a row count, and a non-numeric value
    /// is an error rather than a silent fallback to the default: a client that
    /// sent `row_count_to_load=lots` asked a question, and answering a different
    /// one quietly is how a capped read gets mistaken for a whole file.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        if trimmed.eq_ignore_ascii_case("all") {
            return Ok(Self::All);
        }
        match trimmed.parse::<usize>() {
            Ok(0) => Ok(Self::All),
            Ok(n) => Ok(Self::Count(n)),
            Err(_) => Err(format!(
                "row_count_to_load must be a row count or `all` (`0` also means all) — got `{raw}`"
            )),
        }
    }
}

/// The resolved bound on one read, after the request has met the server's caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowCap {
    /// At most this many rows. Always ≥ 1.
    Rows(usize),
    /// Every row — reachable only when the operator set no hard cap.
    Unlimited,
}

impl RowCap {
    /// The row bound, or `None` when unlimited. This is the shape the HTTP
    /// response and the console use (`null` = "everything").
    pub fn rows(self) -> Option<usize> {
        match self {
            Self::Rows(n) => Some(n),
            Self::Unlimited => None,
        }
    }
}

/// The server's preview policy: whether previews are served at all, and the two
/// row caps every request is resolved against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreviewConfig {
    /// Whether the local-output preview endpoint does anything. **Off by
    /// default** — it reads the *contents* of files on the server's disk back
    /// over HTTP, which is a local-testing convenience, not something a normal
    /// exposed `serve` should offer. `--preview-local-outputs` opts in.
    pub enabled: bool,
    /// Soft cap: rows loaded when `row_count_to_load` is omitted. `0` = the whole
    /// dataset by default.
    pub default_rows: usize,
    /// Hard cap: the ceiling `row_count_to_load` is clamped to. `0` = no ceiling,
    /// which is what makes a whole-dataset preview possible.
    pub max_rows: usize,
}

impl Default for PreviewConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_rows: DEFAULT_PREVIEW_ROWS,
            max_rows: MAX_PREVIEW_ROWS,
        }
    }
}

impl PreviewConfig {
    /// Build from the resolved flag/env values.
    ///
    /// An inconsistent pair (soft cap above a non-zero hard cap) is **clamped
    /// with a warning**, not rejected: an operator who lowers
    /// `--preview-max-rows` to 50 and leaves the soft cap at its default should
    /// get a server that boots and honours the ceiling, not a crash loop. The
    /// hard cap always wins — that is what makes it hard.
    pub fn new(enabled: bool, default_rows: usize, max_rows: usize) -> Self {
        // `default_rows == 0` is "everything by default" and is left alone —
        // `resolve_rows` folds it into the ceiling. Only a finite soft cap can
        // overshoot a finite ceiling.
        let clamped = if max_rows > 0 && default_rows > max_rows {
            tracing::warn!(
                requested = default_rows,
                effective = max_rows,
                max_rows,
                "preview default row count clamped to the hard cap \
                 (--preview-default-rows above --preview-max-rows)"
            );
            max_rows
        } else {
            default_rows
        };
        Self {
            enabled,
            default_rows: clamped,
            max_rows,
        }
    }

    /// The soft cap as the wire reports it: `None` = "everything by default".
    pub fn default_rows(&self) -> Option<usize> {
        (self.default_rows > 0).then_some(self.default_rows)
    }

    /// The hard cap as the wire reports it: `None` = no ceiling.
    pub fn max_rows(&self) -> Option<usize> {
        (self.max_rows > 0).then_some(self.max_rows)
    }

    /// Resolve one request's `row_count_to_load` into the bound to read under.
    ///
    /// Omitted → the soft cap. Present → itself, clamped by the hard cap in both
    /// directions. `Unlimited` is returned only when nothing bounds the read:
    /// the caller asked for everything **and** the operator set no ceiling.
    pub fn resolve_rows(&self, requested: Option<RowRequest>) -> RowCap {
        let asked = match requested {
            None => self.default_rows,
            Some(RowRequest::All) => 0,
            Some(RowRequest::Count(n)) => n,
        };
        match (asked, self.max_rows) {
            // Everything, and nothing to clamp it to.
            (0, 0) => RowCap::Unlimited,
            // Everything, but the operator set a ceiling: the ceiling is the
            // answer. This is the clamp that a client cannot argue away.
            (0, ceiling) => RowCap::Rows(ceiling),
            // A finite ask with no ceiling.
            (n, 0) => RowCap::Rows(n),
            (n, ceiling) => RowCap::Rows(n.min(ceiling)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_off_with_the_documented_caps() {
        let c = PreviewConfig::default();
        assert!(!c.enabled, "preview must be opt-in");
        assert_eq!(c.default_rows, 500);
        assert_eq!(c.max_rows, 5_000);
    }

    #[test]
    fn an_omitted_row_count_uses_the_soft_cap() {
        let c = PreviewConfig::new(true, 25, 1000);
        assert_eq!(c.resolve_rows(None), RowCap::Rows(25));
    }

    #[test]
    fn a_request_over_the_hard_cap_is_clamped_not_honoured() {
        let c = PreviewConfig::new(true, 100, 500);
        assert_eq!(
            c.resolve_rows(Some(RowRequest::Count(10_000))),
            RowCap::Rows(500)
        );
        assert_eq!(
            c.resolve_rows(Some(RowRequest::Count(499))),
            RowCap::Rows(499)
        );
        assert_eq!(
            c.resolve_rows(Some(RowRequest::Count(usize::MAX))),
            RowCap::Rows(500)
        );
    }

    #[test]
    fn asking_for_everything_is_clamped_by_a_configured_ceiling() {
        // The whole point of the hard cap: a client cannot talk its way past it.
        let c = PreviewConfig::new(true, 100, 500);
        assert_eq!(c.resolve_rows(Some(RowRequest::All)), RowCap::Rows(500));
    }

    #[test]
    fn asking_for_everything_is_unlimited_only_when_the_operator_lifted_the_ceiling() {
        let c = PreviewConfig::new(true, 500, 0);
        assert_eq!(c.resolve_rows(Some(RowRequest::All)), RowCap::Unlimited);
        // …and a finite ask is still honoured verbatim on such a server.
        assert_eq!(c.resolve_rows(Some(RowRequest::Count(7))), RowCap::Rows(7));
    }

    #[test]
    fn a_zero_soft_cap_means_everything_by_default() {
        let unbounded = PreviewConfig::new(true, 0, 0);
        assert_eq!(unbounded.resolve_rows(None), RowCap::Unlimited);
        // Still bounded when a ceiling exists.
        let bounded = PreviewConfig::new(true, 0, 250);
        assert_eq!(bounded.resolve_rows(None), RowCap::Rows(250));
    }

    #[test]
    fn a_soft_cap_above_the_hard_cap_is_clamped_to_it() {
        let c = PreviewConfig::new(true, 5_000, 200);
        assert_eq!(c.default_rows, 200, "the hard cap wins");
        assert_eq!(c.max_rows, 200);
        assert_eq!(c.resolve_rows(None), RowCap::Rows(200));
    }

    #[test]
    fn the_wire_shape_reports_no_limit_as_null() {
        let c = PreviewConfig::new(true, 0, 0);
        assert_eq!(c.default_rows(), None);
        assert_eq!(c.max_rows(), None);
        let c = PreviewConfig::new(true, 10, 20);
        assert_eq!(c.default_rows(), Some(10));
        assert_eq!(c.max_rows(), Some(20));
    }

    #[test]
    fn row_request_parses_counts_and_every_spelling_of_all() {
        assert_eq!(RowRequest::parse("25").unwrap(), RowRequest::Count(25));
        assert_eq!(RowRequest::parse(" 25 ").unwrap(), RowRequest::Count(25));
        assert_eq!(RowRequest::parse("all").unwrap(), RowRequest::All);
        assert_eq!(RowRequest::parse("ALL").unwrap(), RowRequest::All);
        // `0` = no limit, the same convention as `batch_size` / `retention_days`.
        assert_eq!(RowRequest::parse("0").unwrap(), RowRequest::All);
    }

    #[test]
    fn a_nonsense_row_count_is_an_error_not_a_silent_default() {
        // Answering a different question quietly is how a capped read gets
        // mistaken for a whole file.
        for bad in ["lots", "-1", "1.5", ""] {
            let err = RowRequest::parse(bad).unwrap_err();
            assert!(err.contains("row_count_to_load"), "{bad}: {err}");
        }
    }
}
