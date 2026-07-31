//! WASM-transform metrics. Emitted directly via the `metrics` facade; the
//! recorder is installed by the pipeline's observability layer. The `module`
//! label is the module file basename — low cardinality, user-controlled.

use metrics::{counter, gauge, histogram};

/// One transform invocation with its terminal outcome (`ok` / `filter` /
/// `error`). `filter` is the module returning the drop sentinel.
pub(crate) fn invocation(module: &str, outcome: &'static str) {
    counter!(
        "faucet_wasm_invocations_total",
        "module" => module.to_string(),
        "outcome" => outcome,
    )
    .increment(1);
}

/// Wall-clock duration of one `transform` call (seconds).
pub(crate) fn invocation_duration(module: &str, secs: f64) {
    histogram!(
        "faucet_wasm_invocation_duration_seconds",
        "module" => module.to_string(),
    )
    .record(secs);
}

/// Fuel consumed by one `transform` call.
pub(crate) fn fuel_consumed(module: &str, fuel: u64) {
    counter!(
        "faucet_wasm_fuel_consumed_total",
        "module" => module.to_string(),
    )
    .increment(fuel);
}

/// Peak linear-memory size observed for the page's instance (bytes).
pub(crate) fn memory_bytes(module: &str, bytes: u64) {
    gauge!(
        "faucet_wasm_memory_bytes",
        "module" => module.to_string(),
    )
    .set(bytes as f64);
}

/// Cost of compiling the module (seconds) — hot-reload visibility.
pub(crate) fn compile_duration(module: &str, secs: f64) {
    histogram!(
        "faucet_wasm_compile_duration_seconds",
        "module" => module.to_string(),
    )
    .record(secs);
}
