//! Pipeline-internal observability: tracing spans and `metrics` counters/
//! histograms wired automatically around every source, sink, transform, and
//! state-store operation. See
//! `docs/superpowers/specs/2026-05-23-observability-otel-prometheus-design.md`.

mod bookmark;
pub(crate) mod decorator;
mod install;
mod labels;
mod options;
mod state;
mod strip;
mod timer;
mod transform;

pub use bookmark::update_bookmark_lag;
pub use decorator::{InstrumentedSink, InstrumentedSource};
pub use install::{InstallError, InstallReport, ObservabilityConfig, install_observability};
pub use labels::Labels;
pub use options::RunStreamOptions;
pub use state::InstrumentedStateStore;
pub use strip::strip_type_name;
pub use timer::DurationGuard;
pub use transform::instrumented_apply_all;
