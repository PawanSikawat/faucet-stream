//! Read-time integrity verification for streaming object bodies.
//!
//! Object-store sources (S3, GCS) read an object body through an async reader
//! and trust the SDK to either deliver the whole body or surface an error. A
//! transfer that terminates early but *cleanly* (a truncated stream that still
//! yields EOF) would otherwise be parsed and emitted as a complete object —
//! silent data loss with no error (#161).
//!
//! [`VerifyingReader`] wraps the **raw** body reader (below any decompression
//! layer, so byte counts and checksums cover the *stored* bytes, not the
//! decoded ones) and runs a set of [`IntegrityCheck`]s once the underlying
//! reader reaches EOF. A failed check surfaces as an [`io::Error`] of kind
//! [`InvalidData`](std::io::ErrorKind::InvalidData) on the final read, which
//! the connector maps to [`FaucetError::Source`](crate::FaucetError::Source).
//!
//! The built-in [`LengthCheck`] guards against truncation. Connector crates
//! implement [`IntegrityCheck`] over their own hash libraries for opt-in
//! checksum verification.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// A check fed the raw bytes of an object body as they stream past, and asked
/// to validate once the underlying reader reaches EOF.
pub trait IntegrityCheck: Send {
    /// Observe a chunk of body bytes. Called zero or more times, in order,
    /// before [`finalize`](IntegrityCheck::finalize).
    fn update(&mut self, chunk: &[u8]);

    /// Validate at EOF. `total` is the number of bytes observed across every
    /// [`update`](IntegrityCheck::update) call. Return `Err(reason)` to fail
    /// the read; the reason is surfaced as an [`io::Error`] of kind
    /// [`InvalidData`](std::io::ErrorKind::InvalidData).
    fn finalize(self: Box<Self>, total: u64) -> Result<(), String>;
}

/// Built-in [`IntegrityCheck`] that fails when the number of bytes read does
/// not match the length advertised by the object store (`Content-Length` for
/// S3, object `size` for GCS). Catches a cleanly-truncated transfer that would
/// otherwise be accepted as a complete object.
#[derive(Debug, Clone, Copy)]
pub struct LengthCheck {
    expected: u64,
}

impl LengthCheck {
    /// Create a length check against the advertised body length in bytes.
    pub fn new(expected: u64) -> Self {
        Self { expected }
    }
}

impl IntegrityCheck for LengthCheck {
    fn update(&mut self, _chunk: &[u8]) {}

    fn finalize(self: Box<Self>, total: u64) -> Result<(), String> {
        if total == self.expected {
            Ok(())
        } else {
            Err(format!(
                "body length mismatch: object store advertised {} byte(s) but read {} \
                 (truncated or corrupted transfer)",
                self.expected, total
            ))
        }
    }
}

/// An [`AsyncRead`] adapter that observes every byte read from `inner` and runs
/// the configured [`IntegrityCheck`]s at EOF. Wrap the **raw** network reader
/// before any `BufReader` / decompression layer.
pub struct VerifyingReader<R> {
    inner: R,
    checks: Vec<Box<dyn IntegrityCheck>>,
    total: u64,
    finalized: bool,
}

impl<R> VerifyingReader<R> {
    /// Wrap `inner`, running `checks` at EOF. An empty `checks` vec is a
    /// transparent passthrough.
    pub fn new(inner: R, checks: Vec<Box<dyn IntegrityCheck>>) -> Self {
        Self {
            inner,
            checks,
            total: 0,
            finalized: false,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for VerifyingReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let filled = buf.filled();
                let n = filled.len() - before;
                if n > 0 {
                    // Bytes were produced: count them and feed every check the
                    // raw chunk, then yield to the caller as usual.
                    let chunk = &filled[before..];
                    this.total += n as u64;
                    for check in &mut this.checks {
                        check.update(chunk);
                    }
                    Poll::Ready(Ok(()))
                } else if this.finalized {
                    // EOF already validated on a prior poll; report clean EOF.
                    Poll::Ready(Ok(()))
                } else {
                    // First EOF: run every check exactly once. A failure is
                    // surfaced as an InvalidData error on this final read.
                    this.finalized = true;
                    for check in std::mem::take(&mut this.checks) {
                        if let Err(reason) = check.finalize(this.total) {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                reason,
                            )));
                        }
                    }
                    Poll::Ready(Ok(()))
                }
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt as _;

    /// Test-only digest check: sums observed bytes and compares the running
    /// total at EOF. Exercises the `update`/`finalize` path the way a real
    /// CRC/SHA check does, without pulling a hash crate into core's tests.
    struct SumCheck {
        expected: u64,
        running: u64,
    }
    impl SumCheck {
        fn new(expected: u64) -> Self {
            Self {
                expected,
                running: 0,
            }
        }
    }
    impl IntegrityCheck for SumCheck {
        fn update(&mut self, chunk: &[u8]) {
            self.running += chunk.iter().map(|b| *b as u64).sum::<u64>();
        }
        fn finalize(self: Box<Self>, _total: u64) -> Result<(), String> {
            if self.running == self.expected {
                Ok(())
            } else {
                Err(format!(
                    "checksum mismatch: expected {}, computed {}",
                    self.expected, self.running
                ))
            }
        }
    }

    /// A reader that yields exactly one byte per `poll_read`, to prove the
    /// verifier accumulates correctly across many partial reads.
    struct OneByteAtATime {
        data: Vec<u8>,
        pos: usize,
    }
    impl AsyncRead for OneByteAtATime {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.pos < this.data.len() {
                buf.put_slice(&this.data[this.pos..this.pos + 1]);
                this.pos += 1;
            }
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn digest_mismatch_fails() {
        let data: &[u8] = &[1, 2, 3]; // sum = 6
        let mut reader = VerifyingReader::new(data, vec![Box::new(SumCheck::new(99))]);
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("wrong checksum must fail the read");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("checksum mismatch"), "got: {err}");
    }

    #[tokio::test]
    async fn digest_match_passes() {
        let data: &[u8] = &[1, 2, 3]; // sum = 6
        let mut reader = VerifyingReader::new(data, vec![Box::new(SumCheck::new(6))]);
        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("matching checksum must pass");
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn accumulates_across_one_byte_reads() {
        // length + digest must both be correct even when the body arrives one
        // byte per poll (incremental `update`, not a single final chunk).
        let body = vec![10u8, 20, 30, 40]; // len 4, sum 100
        let reader = OneByteAtATime {
            data: body.clone(),
            pos: 0,
        };
        let mut reader = VerifyingReader::new(
            reader,
            vec![Box::new(LengthCheck::new(4)), Box::new(SumCheck::new(100))],
        );
        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("incremental reads must verify");
        assert_eq!(out, body);
    }

    #[tokio::test]
    async fn first_failing_check_surfaces() {
        // Length is correct (3) but the digest is wrong → the digest error is
        // what surfaces (length runs first and passes, digest second fails).
        let data: &[u8] = &[1, 2, 3];
        let mut reader = VerifyingReader::new(
            data,
            vec![Box::new(LengthCheck::new(3)), Box::new(SumCheck::new(0))],
        );
        let mut out = Vec::new();
        let err = reader.read_to_end(&mut out).await.expect_err("must fail");
        assert!(err.to_string().contains("checksum mismatch"), "got: {err}");
    }

    #[tokio::test]
    async fn empty_body_expecting_zero_passes() {
        let data: &[u8] = &[];
        let mut reader = VerifyingReader::new(data, vec![Box::new(LengthCheck::new(0))]);
        let mut out = Vec::new();
        let n = reader
            .read_to_end(&mut out)
            .await
            .expect("empty body, len 0");
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn empty_body_expecting_nonzero_fails() {
        let data: &[u8] = &[];
        let mut reader = VerifyingReader::new(data, vec![Box::new(LengthCheck::new(5))]);
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("an empty body advertised as 5 bytes must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn overlong_read_fails() {
        // More bytes than advertised is also a mismatch (store inconsistency).
        let data: &[u8] = b"helloworld"; // 10 bytes
        let mut reader = VerifyingReader::new(data, vec![Box::new(LengthCheck::new(4))]);
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("reading more than advertised must fail");
        assert!(err.to_string().contains("length mismatch"), "got: {err}");
    }

    #[tokio::test]
    async fn no_checks_is_transparent_passthrough() {
        let data: &[u8] = b"passthrough";
        let mut reader = VerifyingReader::new(data, Vec::new());
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("no checks = ok");
        assert_eq!(out, b"passthrough");
    }

    #[tokio::test]
    async fn short_read_fails_length_check() {
        let data: &[u8] = b"hello"; // 5 bytes
        let mut reader = VerifyingReader::new(data, vec![Box::new(LengthCheck::new(10))]);
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("a 5-byte body against an advertised 10 must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("length mismatch"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn exact_length_passes() {
        let data: &[u8] = b"helloworld"; // 10 bytes
        let mut reader = VerifyingReader::new(data, vec![Box::new(LengthCheck::new(10))]);
        let mut out = Vec::new();
        let n = reader
            .read_to_end(&mut out)
            .await
            .expect("exact-length body must succeed");
        assert_eq!(n, 10);
        assert_eq!(out, b"helloworld");
    }
}
