//! Process resident-memory reporting (#631).
//!
//! [`process_peak_rss_bytes`] is the **reliable, absolute** memory figure: the
//! high-water mark of this process's resident set, read from `getrusage`. It is
//! process-wide, so it is a clean *per-run* number only for a one-shot `faucet
//! run` process (which runs exactly one sync); under a long-lived `faucet serve`
//! it is process-lifetime and must be treated as process-scoped, not attributed
//! to a single run. It is NOT an in-flight/serialized proxy — it is the real RSS.

/// Peak resident set size of the current process since start, in **bytes**, or
/// `None` if the platform call fails.
///
/// `getrusage`'s `ru_maxrss` unit differs by OS — **bytes on macOS/iOS**,
/// **kibibytes on Linux and the BSDs** — so it is normalized to bytes here.
pub fn process_peak_rss_bytes() -> Option<u64> {
    // SAFETY: `getrusage` fills a caller-owned `rusage`; we zero-init it and only
    // read the scalar `ru_maxrss` field afterwards. No aliasing or lifetime risk.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc != 0 {
        return None;
    }
    let maxrss = usage.ru_maxrss;
    if maxrss <= 0 {
        return None;
    }
    let maxrss = maxrss as u64;
    let bytes = if cfg!(any(target_os = "macos", target_os = "ios")) {
        maxrss // already bytes
    } else {
        maxrss.saturating_mul(1024) // KiB → bytes (Linux, *BSD)
    };
    Some(bytes)
}

/// Format a byte count as a human `"~N MB"` string for run summaries.
pub fn fmt_mb(bytes: u64) -> String {
    format!("~{:.0} MB", bytes as f64 / 1_048_576.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peak_rss_is_reported_and_nonzero() {
        // Any live process has a non-trivial resident set; the call must succeed
        // and return a plausible value (> 1 MB, < 1 TB) on supported platforms.
        let b = process_peak_rss_bytes().expect("getrusage should succeed");
        assert!(b > 1_048_576, "peak RSS suspiciously small: {b}");
        assert!(b < 1_099_511_627_776, "peak RSS suspiciously large: {b}");
    }

    #[test]
    fn fmt_mb_rounds_to_megabytes() {
        assert_eq!(fmt_mb(150 * 1_048_576), "~150 MB");
        assert_eq!(fmt_mb(0), "~0 MB");
    }
}
