//! Pure encode/decode of the v1 WASM transform ABI. No wasmtime here — this
//! module is just the bit-twiddling and outcome classification, so it is
//! trivially unit-testable.

/// Packed return value meaning "drop this record" (filter it out).
pub(crate) const DROP: u64 = 0;

/// Packed return value meaning "the module signalled an error"; the host reads
/// the message from the optional `error_ptr()` / `error_len()` exports.
pub(crate) const ERROR: u64 = u64::MAX;

/// A packed transform return value, classified before touching memory.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RawOutcome {
    /// Drop the record.
    Drop,
    /// The module signalled an error.
    Error,
    /// Emit output JSON located at `[ptr, ptr + len)` in linear memory.
    Emit { ptr: u32, len: u32 },
}

/// Split a packed `u64` into `(ptr, len)` — high 32 bits are the pointer, low
/// 32 bits the length.
pub(crate) fn unpack(ret: u64) -> (u32, u32) {
    ((ret >> 32) as u32, (ret & 0xFFFF_FFFF) as u32)
}

/// Classify a raw packed return value from the `transform` export.
pub(crate) fn classify(ret: u64) -> RawOutcome {
    match ret {
        DROP => RawOutcome::Drop,
        ERROR => RawOutcome::Error,
        other => {
            let (ptr, len) = unpack(other);
            RawOutcome::Emit { ptr, len }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unpack_splits_hi_lo() {
        assert_eq!(unpack(0), (0, 0));
        assert_eq!(unpack((7u64 << 32) | 5), (7, 5));
        assert_eq!(unpack(0xFFFF_FFFE_0000_0003), (0xFFFF_FFFE, 3));
    }

    #[test]
    fn classify_zero_is_drop() {
        assert_eq!(classify(DROP), RawOutcome::Drop);
    }

    #[test]
    fn classify_max_is_error() {
        assert_eq!(classify(ERROR), RawOutcome::Error);
    }

    #[test]
    fn classify_other_is_emit() {
        assert_eq!(
            classify((1024u64 << 32) | 42),
            RawOutcome::Emit { ptr: 1024, len: 42 }
        );
    }

    #[test]
    fn classify_zero_len_nonzero_ptr_is_emit_empty() {
        // ptr set, len 0 → a legitimate empty-output emit (not the DROP
        // sentinel, which requires the whole word to be zero).
        assert_eq!(classify(64u64 << 32), RawOutcome::Emit { ptr: 64, len: 0 });
    }
}
