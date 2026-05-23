//! See `crates/core/src/observability/mod.rs` and the design spec.
pub struct ObservabilityConfig;
pub struct InstallReport;
#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    #[error("placeholder")]
    Placeholder,
}
pub fn install_observability(_: &ObservabilityConfig) -> Result<InstallReport, InstallError> {
    unimplemented!()
}
