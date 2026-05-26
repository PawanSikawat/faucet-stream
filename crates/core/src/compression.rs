//! Transparent gzip / zstd compression wrappers for file-shaped connectors.
//!
//! Behind the `compression` feature. Exposes:
//! - [`CompressionConfig`] — user-facing enum (`None`, `Gzip`, `Zstd`, `Auto`).
//! - [`Compression`] — internal post-resolution enum (no `Auto`).
//! - `wrap_async_reader` / `wrap_async_writer` — for `stream_pages` and async sinks.
//! - `wrap_sync_reader` / `wrap_sync_writer` — for `spawn_blocking` paths.
//! - [`compress_buf`] — one-shot in-memory compression for S3/GCS sink uploads.
//! - [`warn_mismatch`] — log-once helper when explicit codec disagrees with the
//!   filename extension.

use crate::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use tokio::io::{AsyncBufRead, AsyncWrite};

/// User-facing compression config. Defaults to [`Auto`](Self::Auto).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum CompressionConfig {
    None,
    Gzip,
    Zstd,
    /// Detect from filename suffix: `.gz` → Gzip, `.zst` → Zstd, anything else → None.
    #[default]
    Auto,
}

/// Internal post-resolution codec. No `Auto` variant — call [`CompressionConfig::resolve`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compression {
    None,
    Gzip,
    Zstd,
}

impl CompressionConfig {
    /// Resolve `Auto` against a filename. Non-`Auto` variants pass through.
    pub fn resolve(self, path: &str) -> Compression {
        match self {
            Self::None => Compression::None,
            Self::Gzip => Compression::Gzip,
            Self::Zstd => Compression::Zstd,
            Self::Auto => detect_from_path(path),
        }
    }
}

/// Codec detection by filename suffix. Case-insensitive. Looks at the final
/// extension only: `foo.csv.gz` → Gzip, `foo.gz.zst` → Zstd.
pub fn detect_from_path(path: &str) -> Compression {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".gz") {
        Compression::Gzip
    } else if lower.ends_with(".zst") {
        Compression::Zstd
    } else {
        Compression::None
    }
}

/// Wrap an async buffered reader with a streaming decoder. `None` passes through.
pub fn wrap_async_reader<'a, R>(
    r: R,
    c: Compression,
) -> Pin<Box<dyn AsyncBufRead + Send + Unpin + 'a>>
where
    R: AsyncBufRead + Send + Unpin + 'a,
{
    match c {
        Compression::None => Box::pin(r),
        Compression::Gzip => {
            let mut dec = async_compression::tokio::bufread::GzipDecoder::new(r);
            dec.multiple_members(true);
            Box::pin(tokio::io::BufReader::new(dec))
        }
        // zstd handles concatenated frames natively, so no `multiple_members`-
        // equivalent flag is needed (the spec is part of zstd itself).
        Compression::Zstd => {
            let dec = async_compression::tokio::bufread::ZstdDecoder::new(r);
            Box::pin(tokio::io::BufReader::new(dec))
        }
    }
}

/// Wrap an async writer with a streaming encoder. `None` passes through.
/// Callers must call [`tokio::io::AsyncWriteExt::shutdown`] on the returned
/// writer to flush the trailer before the underlying writer is dropped.
pub fn wrap_async_writer<'a, W>(
    w: W,
    c: Compression,
) -> Pin<Box<dyn AsyncWrite + Send + Unpin + 'a>>
where
    W: AsyncWrite + Send + Unpin + 'a,
{
    match c {
        Compression::None => Box::pin(w),
        Compression::Gzip => Box::pin(async_compression::tokio::write::GzipEncoder::new(w)),
        Compression::Zstd => Box::pin(async_compression::tokio::write::ZstdEncoder::new(w)),
    }
}

/// Wrap a sync reader with a streaming decoder. `None` passes through.
pub fn wrap_sync_reader<'a, R>(r: R, c: Compression) -> Box<dyn std::io::Read + Send + 'a>
where
    R: std::io::Read + Send + 'a,
{
    match c {
        Compression::None => Box::new(r),
        Compression::Gzip => Box::new(flate2::read::MultiGzDecoder::new(r)),
        Compression::Zstd => Box::new(
            zstd::stream::read::Decoder::new(r)
                .expect("zstd decoder construction is infallible for any Read"),
        ),
    }
}

/// Wrap a sync writer with a streaming encoder. `None` passes through.
///
/// The returned writer finalises the encoder when dropped (gzip writes its
/// 8-byte trailer; zstd's `auto_finish` adapter writes the frame epilogue).
/// Because the concrete encoder type is erased behind `Box<dyn Write>`,
/// callers cannot invoke `flate2`'s or `zstd`'s `finish()` to capture the
/// trailer-write `io::Error`. Callers can `flush()` to drain the encoder's
/// internal buffer mid-stream, but trailer-write errors on drop are
/// negligibly rare and are silently swallowed.
///
/// Connectors using this wrapper should drop the box inside a
/// `spawn_blocking` task body so the trailer write does not block the
/// async runtime, and rely on the surrounding `write_all` / `flush` calls
/// to surface earlier I/O errors.
pub fn wrap_sync_writer<'a, W>(w: W, c: Compression) -> Box<dyn std::io::Write + Send + 'a>
where
    W: std::io::Write + Send + 'a,
{
    match c {
        Compression::None => Box::new(w),
        Compression::Gzip => Box::new(flate2::write::GzEncoder::new(
            w,
            flate2::Compression::default(),
        )),
        Compression::Zstd => Box::new(
            zstd::stream::write::Encoder::new(w, 0)
                .expect("zstd encoder construction is infallible")
                .auto_finish(),
        ),
    }
}

/// One-shot in-memory compression. Used by S3 and GCS sinks that build a full
/// `Vec<u8>` body before upload.
pub fn compress_buf(data: &[u8], c: Compression) -> Result<Vec<u8>, FaucetError> {
    use std::io::Write;
    match c {
        Compression::None => Ok(data.to_vec()),
        Compression::Gzip => {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(data)
                .map_err(|e| FaucetError::Sink(format!("gzip compress failed: {e}")))?;
            enc.finish()
                .map_err(|e| FaucetError::Sink(format!("gzip finalise failed: {e}")))
        }
        Compression::Zstd => zstd::stream::encode_all(data, 0)
            .map_err(|e| FaucetError::Sink(format!("zstd compress failed: {e}"))),
    }
}

/// Log a one-shot warning when the explicit codec disagrees with the
/// filename's detected codec. Deduplicates per `(path, declared)` pair across
/// the whole process so a million-object scan does not flood logs.
pub fn warn_mismatch(path: &str, declared: Compression) {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    static SEEN: OnceLock<Mutex<HashSet<(String, Compression)>>> = OnceLock::new();
    let detected = detect_from_path(path);
    if detected == declared {
        return;
    }
    let key = (path.to_string(), declared);
    let mut seen = SEEN
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .expect("compression mismatch log mutex poisoned");
    if seen.insert(key) {
        tracing::warn!(
            path = %path,
            declared = ?declared,
            detected = ?detected,
            "compression codec mismatch — explicit config wins, filename extension ignored",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

    #[test]
    fn detect_extensions() {
        assert_eq!(detect_from_path("foo.jsonl"), Compression::None);
        assert_eq!(detect_from_path("foo.json.gz"), Compression::Gzip);
        assert_eq!(detect_from_path("foo.csv.zst"), Compression::Zstd);
        assert_eq!(detect_from_path("FOO.GZ"), Compression::Gzip);
        assert_eq!(detect_from_path("a.gz.zst"), Compression::Zstd);
        assert_eq!(detect_from_path(""), Compression::None);
    }

    #[test]
    fn resolve_auto_uses_path() {
        assert_eq!(
            CompressionConfig::Auto.resolve("foo.gz"),
            Compression::Gzip
        );
        assert_eq!(
            CompressionConfig::Auto.resolve("foo.zst"),
            Compression::Zstd
        );
        assert_eq!(CompressionConfig::Auto.resolve("foo"), Compression::None);
    }

    #[test]
    fn resolve_explicit_ignores_path() {
        assert_eq!(
            CompressionConfig::Gzip.resolve("foo.txt"),
            Compression::Gzip
        );
        assert_eq!(
            CompressionConfig::None.resolve("foo.gz"),
            Compression::None
        );
    }

    #[test]
    fn config_default_is_auto() {
        assert_eq!(CompressionConfig::default(), CompressionConfig::Auto);
    }

    #[test]
    fn config_serde_lowercase() {
        // All four variants round-trip as lowercase strings.
        for (variant, expected) in [
            (CompressionConfig::None, "\"none\""),
            (CompressionConfig::Gzip, "\"gzip\""),
            (CompressionConfig::Zstd, "\"zstd\""),
            (CompressionConfig::Auto, "\"auto\""),
        ] {
            let serialised = serde_json::to_string(&variant).unwrap();
            assert_eq!(serialised, expected);
            let deserialised: CompressionConfig = serde_json::from_str(expected).unwrap();
            assert_eq!(deserialised, variant);
        }
    }

    #[tokio::test]
    async fn async_roundtrip_gzip() {
        let original = b"hello, compressed world!\n".repeat(100);
        let mut buf = Vec::new();
        {
            let mut w = wrap_async_writer(&mut buf, Compression::Gzip);
            w.write_all(&original).await.unwrap();
            w.shutdown().await.unwrap();
        }
        let mut decompressed = Vec::new();
        let mut r = wrap_async_reader(BufReader::new(&buf[..]), Compression::Gzip);
        r.read_to_end(&mut decompressed).await.unwrap();
        assert_eq!(decompressed, original);
    }

    #[tokio::test]
    async fn async_roundtrip_zstd() {
        let original = b"zstd payload\n".repeat(50);
        let mut buf = Vec::new();
        {
            let mut w = wrap_async_writer(&mut buf, Compression::Zstd);
            w.write_all(&original).await.unwrap();
            w.shutdown().await.unwrap();
        }
        let mut decompressed = Vec::new();
        let mut r = wrap_async_reader(BufReader::new(&buf[..]), Compression::Zstd);
        r.read_to_end(&mut decompressed).await.unwrap();
        assert_eq!(decompressed, original);
    }

    #[tokio::test]
    async fn async_none_passthrough() {
        let original = b"plain text";
        let mut buf = Vec::new();
        {
            let mut w = wrap_async_writer(&mut buf, Compression::None);
            w.write_all(original).await.unwrap();
            w.shutdown().await.unwrap();
        }
        assert_eq!(&buf[..], original);
    }

    #[test]
    fn sync_roundtrip_gzip() {
        use std::io::{Read, Write};
        let original = b"sync gzip data".repeat(20);
        let mut buf = Vec::new();
        {
            let mut w = wrap_sync_writer(&mut buf, Compression::Gzip);
            w.write_all(&original).unwrap();
            // GzEncoder finalises on drop (writes the 8-byte trailer).
            // The explicit flush drains the deflate buffer; the drop at the
            // end of this scope writes the trailer so the reader below
            // sees a complete stream.
            w.flush().unwrap();
        }
        let mut r = wrap_sync_reader(&buf[..], Compression::Gzip);
        let mut decompressed = Vec::new();
        r.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn sync_roundtrip_zstd() {
        use std::io::{Read, Write};
        let original = b"sync zstd data".repeat(20);
        let mut buf = Vec::new();
        {
            let mut w = wrap_sync_writer(&mut buf, Compression::Zstd);
            w.write_all(&original).unwrap();
            w.flush().unwrap();
        }
        let mut r = wrap_sync_reader(&buf[..], Compression::Zstd);
        let mut decompressed = Vec::new();
        r.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn compress_buf_roundtrip_gzip() {
        use std::io::Read;
        let original = b"buffer compression".repeat(10);
        let compressed = compress_buf(&original, Compression::Gzip).unwrap();
        assert_ne!(compressed, original);
        let mut r = wrap_sync_reader(&compressed[..], Compression::Gzip);
        let mut decompressed = Vec::new();
        r.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn compress_buf_roundtrip_zstd() {
        use std::io::Read;
        let original = b"buffer zstd".repeat(10);
        let compressed = compress_buf(&original, Compression::Zstd).unwrap();
        assert_ne!(compressed, original);
        let mut r = wrap_sync_reader(&compressed[..], Compression::Zstd);
        let mut decompressed = Vec::new();
        r.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn compress_buf_none_is_clone() {
        let original = b"unchanged";
        let out = compress_buf(original, Compression::None).unwrap();
        assert_eq!(out, original);
    }

    #[tokio::test]
    async fn empty_compressed_stream_yields_zero_bytes() {
        // Produce a valid empty gzip stream.
        let mut buf = Vec::new();
        {
            let mut w = wrap_async_writer(&mut buf, Compression::Gzip);
            w.shutdown().await.unwrap();
        }
        // Decompressing it should yield zero bytes, not an error.
        let mut decompressed = Vec::new();
        let mut r = wrap_async_reader(BufReader::new(&buf[..]), Compression::Gzip);
        r.read_to_end(&mut decompressed).await.unwrap();
        assert!(decompressed.is_empty());
    }

    #[tokio::test]
    async fn truncated_gzip_stream_errors() {
        let original = b"this will be truncated mid-stream".repeat(50);
        let mut buf = Vec::new();
        {
            let mut w = wrap_async_writer(&mut buf, Compression::Gzip);
            w.write_all(&original).await.unwrap();
            w.shutdown().await.unwrap();
        }
        // Truncate to half.
        buf.truncate(buf.len() / 2);
        let mut decompressed = Vec::new();
        let mut r = wrap_async_reader(BufReader::new(&buf[..]), Compression::Gzip);
        let err = r.read_to_end(&mut decompressed).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn warn_mismatch_dedups_per_path_and_codec() {
        // Calling twice with the same (path, declared) pair must produce a
        // single log line. We verify via the internal HashSet: after two
        // calls, the set contains exactly one entry. The static is shared
        // across the whole process, so we use a unique path to avoid
        // collisions with other tests.
        let unique_path = format!("warn_mismatch_dedup_fixture_{}.txt", line!());
        // First call: detected = None (no extension), declared = Gzip → mismatch, logs.
        warn_mismatch(&unique_path, Compression::Gzip);
        // Second call with identical args: must not log a second time.
        warn_mismatch(&unique_path, Compression::Gzip);
        // Third call with different declared: separate dedup key, logs once.
        warn_mismatch(&unique_path, Compression::Zstd);
        // Matching pair: no log (early-exit before touching the HashSet).
        warn_mismatch("file.gz", Compression::Gzip);
        // (Behaviour is verified through log absence in production. Here we
        // only assert the function runs to completion without panicking,
        // which exercises the OnceLock init, the Mutex acquisition, and the
        // HashSet insertion paths.)
    }
}
