//! Retention GC for local sink outputs (#587).
//!
//! Local runs write real files — `out.jsonl`, `rows.csv`, a directory of rolled
//! parquet parts. Repeated local iteration, or a long-running `faucet serve`
//! used for local testing, accumulates them until someone remembers to `rm`.
//! This module bounds that footprint: faucet records every local file its sinks
//! open, then deletes the ones past a retention window (**7 days** by default),
//! plus on-demand *immediate* (one output), *bulk* (older than N days), and
//! *clean-all* sweeps.
//!
//! ## The guardrail is the whole design
//!
//! **The sweeper may only delete files faucet recorded as its own local sink
//! outputs — never a glob, never a directory wipe, not even for "clean all".**
//! Everything here follows from that:
//!
//! - Paths come from the sink that opened them
//!   ([`Sink::local_outputs`](faucet_core::Sink::local_outputs)) and are stored in
//!   the ledger. A sweep iterates ledger rows and calls `remove_file` on each —
//!   there is no code path in this module that expands a pattern, walks a
//!   directory, or removes one.
//! - A file faucet *wrote* but did not *create* (`append:` onto an existing
//!   export, a mistyped path landing on real data) carries
//!   [`pre_existing`](faucet_core::LocalOutput::pre_existing) and is **never**
//!   deleted — not by the sweeper, not by an explicit single-path request. See
//!   [`SkipReason::PreExisting`].
//! - A parquet run in rollover mode records each UUID-named part as its own row,
//!   so "delete this dataset's outputs" is still a list of individual files.
//!
//! ## What it does *not* touch
//!
//! Run-history rows, catalog entries, and lineage are the durable record; this
//! GC removes **data files only**. A swept run keeps its history row, and its
//! ledger row survives too — marked `deleted_at`, which is what renders the
//! output as **expired** in the console rather than as a broken link. That
//! asymmetry is deliberate: *data artifacts are disposable, the record of what
//! ran is not*.
//!
//! ## Layout
//!
//! - [`ledger`] — the stored record ([`LocalOutputRecord`]) + the filter/report
//!   types the storage backends and HTTP handlers share.
//! - [`spec`] — the top-level `local_outputs:` config block.
//! - [`sweep`] — the engine: pure selection ([`sweep::select`]) separated from
//!   the I/O that acts on it ([`sweep::run`]).
//! - [`record`] — the write path the executor calls after an invocation.
//! - [`metrics`] — the `faucet_local_outputs_*` counters.

pub mod ledger;
pub mod metrics;
pub mod record;
pub mod spec;
pub mod sweep;

pub use ledger::{
    LocalOutputFilter, LocalOutputObservation, LocalOutputRecord, LocalOutputState, SkipReason,
    SweepOutcome, SweepReport, SweepScope,
};
pub use record::{RecordContext, record};
pub use spec::LocalOutputsSpec;
pub use sweep::{SweepOptions, select};

/// Default retention window for local sink outputs, in days.
///
/// Seven days matches `--retain-terminal-runs-secs`, so a run record and the
/// files it produced age out on the same clock by default.
pub const DEFAULT_RETENTION_DAYS: u32 = 7;
