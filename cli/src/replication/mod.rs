//! `faucet replicate` — consistent snapshot → CDC streaming handoff.
//!
//! A CLI-level orchestration layer (like `schedule`/`serve`) over
//! `expand` + `executor::run_expanded`. See
//! `docs/superpowers/specs/2026-06-13-replication-snapshot-cdc-handoff-design.md`.

pub mod compiled;
pub mod spec;
