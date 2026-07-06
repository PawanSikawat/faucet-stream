//! Read a DLQ location (a local JSONL file, a directory of `*.jsonl`, or a
//! glob) back into DLQ envelopes, and expose it as a [`Source`] so `faucet
//! dlq replay` can feed the unwrapped original payloads through the normal
//! pipeline path.
//!
//! The line-parsing core ([`classify_line`]) is pure and unit-tested; file
//! IO ([`scan_files`]) is a thin shim over it. A DLQ location may contain
//! arbitrary lines (blank lines, non-faucet output), so parsing is
//! tolerant: unparseable and non-envelope lines are **skipped and counted**,
//! never fatal.

use async_trait::async_trait;
use faucet_core::{FaucetError, Source, UnwrappedEnvelope, unwrap_envelope};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// A pre-built source attached to a single [`ExpandedNode`](crate::expand::ExpandedNode)
/// so the executor runs it instead of building one from the connector
/// registry. Used only by `faucet dlq replay`, which runs exactly one
/// invocation, so the source is taken once. `Clone` shares the same cell
/// (cloning the node does not duplicate the source).
#[derive(Clone)]
pub struct SourceOverride(Arc<Mutex<Option<Box<dyn Source>>>>);

impl SourceOverride {
    /// Wrap a pre-built source.
    pub fn new(source: Box<dyn Source>) -> Self {
        Self(Arc::new(Mutex::new(Some(source))))
    }

    /// Take the source out of the cell. Returns `None` if it was already
    /// taken (a second invocation would build from the registry instead).
    pub fn take(&self) -> Option<Box<dyn Source>> {
        self.0.lock().ok().and_then(|mut g| g.take())
    }
}

impl std::fmt::Debug for SourceOverride {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SourceOverride(..)")
    }
}

/// Outcome of classifying a single line of a DLQ location.
#[derive(Debug, Clone, PartialEq)]
pub enum LineOutcome {
    /// A blank / whitespace-only line — ignored, not counted as a skip.
    Blank,
    /// A line that is not valid JSON — skipped and counted.
    Malformed,
    /// Valid JSON that is not a DLQ envelope (no `payload`) — skipped and
    /// counted.
    NonEnvelope,
    /// A parsed DLQ envelope.
    Envelope(Box<UnwrappedEnvelope>),
}

/// Classify one raw line. Pure — no IO. Blank lines are ignored; anything
/// else is either an envelope, malformed JSON, or valid-but-not-an-envelope.
pub fn classify_line(line: &str) -> LineOutcome {
    if line.trim().is_empty() {
        return LineOutcome::Blank;
    }
    match serde_json::from_str::<Value>(line) {
        Ok(value) => match unwrap_envelope(&value) {
            Ok(env) => LineOutcome::Envelope(Box::new(env)),
            Err(_) => LineOutcome::NonEnvelope,
        },
        Err(_) => LineOutcome::Malformed,
    }
}

/// Envelopes read from a DLQ location plus the tolerant-parse tallies.
#[derive(Debug, Default, Clone)]
pub struct ScanResult {
    /// Every parsed envelope, in file order.
    pub envelopes: Vec<UnwrappedEnvelope>,
    /// Non-blank lines that were not valid JSON.
    pub malformed: usize,
    /// Valid-JSON lines that were not DLQ envelopes (no `payload`).
    pub non_envelope: usize,
    /// Files that were read.
    pub files_read: usize,
}

/// Expand a DLQ location into the concrete local files to read.
///
/// * a file path → just that file,
/// * a directory → every `*.jsonl` entry directly inside it (sorted),
/// * anything containing a glob metacharacter (`*?[`) → glob matches.
///
/// Returns an error only when the location resolves to nothing (a clear
/// signal the path is wrong), so callers never silently report an empty DLQ
/// for a typo'd path.
pub fn expand_location(location: &str) -> Result<Vec<PathBuf>, FaucetError> {
    let has_glob = location.contains(['*', '?', '[']);
    let mut files: Vec<PathBuf> = if has_glob {
        glob::glob(location)
            .map_err(|e| FaucetError::Config(format!("invalid DLQ glob '{location}': {e}")))?
            .filter_map(Result::ok)
            .filter(|p| p.is_file())
            .collect()
    } else {
        let path = Path::new(location);
        if path.is_dir() {
            std::fs::read_dir(path)
                .map_err(|e| FaucetError::Source(format!("reading DLQ dir '{location}': {e}")))?
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.is_file() && p.extension().is_some_and(|x| x == "jsonl"))
                .collect()
        } else if path.is_file() {
            vec![path.to_path_buf()]
        } else {
            Vec::new()
        }
    };
    files.sort();
    if files.is_empty() {
        return Err(FaucetError::Source(format!(
            "DLQ location '{location}' matched no files (expected a .jsonl file, a directory of \
             .jsonl files, or a glob)"
        )));
    }
    Ok(files)
}

/// Read and classify every line of every file, collecting envelopes and
/// tallies. Blank lines are ignored; malformed / non-envelope lines are
/// counted but never abort the scan.
pub fn scan_files(files: &[PathBuf]) -> Result<ScanResult, FaucetError> {
    let mut out = ScanResult::default();
    for file in files {
        let text = std::fs::read_to_string(file).map_err(|e| {
            FaucetError::Source(format!("reading DLQ file '{}': {e}", file.display()))
        })?;
        out.files_read += 1;
        for line in text.lines() {
            match classify_line(line) {
                LineOutcome::Blank => {}
                LineOutcome::Malformed => out.malformed += 1,
                LineOutcome::NonEnvelope => out.non_envelope += 1,
                LineOutcome::Envelope(env) => out.envelopes.push(*env),
            }
        }
    }
    Ok(out)
}

/// Whether an envelope matches an optional reason filter. `None` matches
/// everything; a legacy envelope with no `reason` field never matches an
/// explicit filter.
pub fn reason_matches(env: &UnwrappedEnvelope, filter: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(want) => env.reason.as_deref() == Some(want),
    }
}

/// A [`Source`] over a DLQ location that yields the **unwrapped original
/// payloads** (optionally filtered by reason), so a replay run feeds them
/// through the referenced config's transforms / quality / contract / sink.
///
/// It has no `state_key`, so the executor never wraps it for bookmarking —
/// a replay is a fresh, whole-location read.
pub struct DlqReaderSource {
    files: Vec<PathBuf>,
    reason: Option<String>,
}

impl DlqReaderSource {
    /// Build a reader over the already-expanded `files`, keeping only
    /// envelopes whose reason matches `reason` (if set).
    pub fn new(files: Vec<PathBuf>, reason: Option<String>) -> Self {
        Self { files, reason }
    }
}

#[async_trait]
impl Source for DlqReaderSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let files = self.files.clone();
        let reason = self.reason.clone();
        // Blocking file IO off the async runtime.
        let scan = tokio::task::spawn_blocking(move || scan_files(&files))
            .await
            .map_err(|e| FaucetError::Source(format!("DLQ reader task panicked: {e}")))??;
        Ok(scan
            .envelopes
            .into_iter()
            .filter(|env| reason_matches(env, reason.as_deref()))
            .map(|env| env.payload)
            .collect())
    }

    fn connector_name(&self) -> &'static str {
        "dlq-reader"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;

    fn envelope_line(reason: &str, payload: Value) -> String {
        json!({
            "error": { "kind": "Sink", "message": "boom" },
            "reason": reason,
            "payload": payload,
            "ts_ms": 1,
            "sink": "pg",
            "pipeline": "etl",
            "row": "",
            "record_index": 0,
        })
        .to_string()
    }

    #[test]
    fn classify_line_blank_is_ignored() {
        assert_eq!(classify_line(""), LineOutcome::Blank);
        assert_eq!(classify_line("   \t "), LineOutcome::Blank);
    }

    #[test]
    fn classify_line_malformed_json() {
        assert_eq!(classify_line("{not json"), LineOutcome::Malformed);
        assert_eq!(classify_line("just text"), LineOutcome::Malformed);
    }

    #[test]
    fn classify_line_valid_json_but_not_envelope() {
        assert_eq!(classify_line(r#"{"a":1}"#), LineOutcome::NonEnvelope);
        assert_eq!(classify_line("[1,2,3]"), LineOutcome::NonEnvelope);
    }

    #[test]
    fn classify_line_parses_envelope() {
        let line = envelope_line("quality", json!({"id": 7}));
        match classify_line(&line) {
            LineOutcome::Envelope(env) => {
                assert_eq!(env.payload, json!({"id": 7}));
                assert_eq!(env.reason.as_deref(), Some("quality"));
            }
            other => panic!("expected envelope, got {other:?}"),
        }
    }

    #[test]
    fn reason_matches_filter() {
        let env = UnwrappedEnvelope {
            payload: json!({}),
            reason: Some("contract".into()),
            error_kind: None,
            error_message: None,
            record_index: None,
            pipeline: None,
            row: None,
            sink: None,
            ts_ms: None,
        };
        assert!(reason_matches(&env, None));
        assert!(reason_matches(&env, Some("contract")));
        assert!(!reason_matches(&env, Some("quality")));
        // A legacy envelope with no reason never matches an explicit filter.
        let legacy = UnwrappedEnvelope {
            reason: None,
            ..env
        };
        assert!(reason_matches(&legacy, None));
        assert!(!reason_matches(&legacy, Some("quality")));
    }

    fn write_tmp(name: &str, body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        f.flush().unwrap();
        (dir, path)
    }

    #[test]
    fn scan_files_counts_skips_and_collects_envelopes() {
        let body = format!(
            "{}\n\n{}\nnot json\n{{\"a\":1}}\n",
            envelope_line("quality", json!({"id": 1})),
            envelope_line("contract", json!({"id": 2})),
        );
        let (_dir, path) = write_tmp("dlq.jsonl", &body);
        let scan = scan_files(&[path]).unwrap();
        assert_eq!(scan.envelopes.len(), 2);
        assert_eq!(scan.malformed, 1);
        assert_eq!(scan.non_envelope, 1);
        assert_eq!(scan.files_read, 1);
    }

    #[test]
    fn expand_location_file_dir_and_missing() {
        let (dir, path) = write_tmp("dlq.jsonl", "\n");
        // single file
        assert_eq!(
            expand_location(path.to_str().unwrap()).unwrap(),
            vec![path.clone()]
        );
        // directory → the .jsonl inside
        let got = expand_location(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(got, vec![path]);
        // missing path → error
        assert!(expand_location(dir.path().join("nope.jsonl").to_str().unwrap()).is_err());
    }

    #[tokio::test]
    async fn dlq_reader_source_yields_filtered_payloads() {
        let body = format!(
            "{}\n{}\n",
            envelope_line("quality", json!({"id": 1})),
            envelope_line("contract", json!({"id": 2})),
        );
        let (_dir, path) = write_tmp("dlq.jsonl", &body);
        // No filter → both payloads.
        let src = DlqReaderSource::new(vec![path.clone()], None);
        let all = src.fetch_all().await.unwrap();
        assert_eq!(all, vec![json!({"id": 1}), json!({"id": 2})]);
        // Reason filter → only matching payloads.
        let src = DlqReaderSource::new(vec![path], Some("contract".into()));
        let filtered = src.fetch_all().await.unwrap();
        assert_eq!(filtered, vec![json!({"id": 2})]);
    }

    #[test]
    fn source_override_takes_once() {
        struct Dummy;
        #[async_trait]
        impl Source for Dummy {
            async fn fetch_with_context(
                &self,
                _c: &HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(vec![])
            }
        }
        let ov = SourceOverride::new(Box::new(Dummy));
        assert!(ov.take().is_some());
        assert!(ov.take().is_none());
    }
}
