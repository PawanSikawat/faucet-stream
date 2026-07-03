//! `faucet test` — fixture-based, fully-offline pipeline testing (#210).
//!
//! A spec file declares test cases: fixture input records, the pipeline logic
//! under test (a config file's transforms/quality/contract, or the same
//! declared inline), and the expected outcome (output records, DLQ routing,
//! counts, or an expected failure). The runner streams the fixtures through
//! the real `faucet_core::Pipeline` loop with in-memory source/sink/DLQ, so
//! CI can assert pipeline logic without any external infrastructure.
//!
//! Module layout mirrors `schedule/` and `replication/`:
//! - [`spec`] — serde types + validation for the spec file (`faucet schema test`).
//! - [`fixtures`] — fixture-input loading (inline / `.jsonl` / `.json` / `.yaml`).
//! - [`runner`] — the offline execution harness (fixture source, capturing
//!   sink + DLQ, the real pipeline pass chain).
//! - [`diff`] — tolerant matchers (`exact`/`subset`, ordered/unordered) and
//!   the structured path diff behind failure messages.
//! - [`report`] — human checklist + `--json` rendering.

pub mod diff;
pub mod fixtures;
pub mod report;
pub mod runner;
pub mod spec;
