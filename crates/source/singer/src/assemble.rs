//! Pure page-assembly state machine.
//!
//! Extracted from the stream so the RECORD/STATE/EOF → [`StreamPage`] logic is
//! unit-testable without spawning a process. The stream feeds parsed messages
//! in; the process/error paths (idle timeout, non-zero exit, malformed-fail)
//! stay in `stream.rs`.

use faucet_core::{StreamPage, Value};

/// Accumulates RECORDs into pages and attaches the latest STATE as the page
/// bookmark, per the v0 single-stream rules.
pub struct PageAssembler {
    target_stream: String,
    batch_size: usize,
    flush_on_state: bool,
    buffer: Vec<Value>,
    /// Latest STATE `value` seen but not yet attached to a flushed page.
    pending_state: Option<Value>,
}

impl PageAssembler {
    /// `batch_size == 0` disables size-based flushing (flush only on STATE/EOF).
    pub fn new(target_stream: impl Into<String>, batch_size: usize, flush_on_state: bool) -> Self {
        Self {
            target_stream: target_stream.into(),
            batch_size,
            flush_on_state,
            buffer: Vec::new(),
            pending_state: None,
        }
    }

    /// Feed a RECORD. Records for a different stream are ignored (single-stream
    /// v0). Returns a page when the `batch_size` threshold is reached.
    pub fn on_record(&mut self, stream: &str, record: Value) -> Option<StreamPage> {
        if stream != self.target_stream {
            return None;
        }
        self.buffer.push(record);
        if self.batch_size != 0 && self.buffer.len() >= self.batch_size {
            Some(self.flush())
        } else {
            None
        }
    }

    /// Feed a STATE. Always becomes the pending checkpoint; flushes immediately
    /// when `flush_on_state` (yielding a page — possibly empty — that carries
    /// the STATE as its bookmark).
    pub fn on_state(&mut self, value: Value) -> Option<StreamPage> {
        self.pending_state = Some(value);
        if self.flush_on_state {
            Some(self.flush())
        } else {
            None
        }
    }

    /// Flush any trailing buffer + pending checkpoint at end-of-stream. Returns
    /// `None` when there is nothing to emit.
    pub fn on_eof(&mut self) -> Option<StreamPage> {
        if self.buffer.is_empty() && self.pending_state.is_none() {
            None
        } else {
            Some(self.flush())
        }
    }

    /// Drain the buffer + pending checkpoint into a page.
    fn flush(&mut self) -> StreamPage {
        StreamPage {
            records: std::mem::take(&mut self.buffer),
            bookmark: self.pending_state.take(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(id: i64) -> Value {
        json!({ "id": id })
    }

    #[test]
    fn flushes_at_batch_size_with_no_bookmark() {
        let mut a = PageAssembler::new("s", 2, true);
        assert!(a.on_record("s", rec(1)).is_none());
        let page = a.on_record("s", rec(2)).expect("flush at size 2");
        assert_eq!(page.records, vec![rec(1), rec(2)]);
        assert!(
            page.bookmark.is_none(),
            "no STATE covers a size-based flush"
        );
        // third record buffered until EOF
        assert!(a.on_record("s", rec(3)).is_none());
        let tail = a.on_eof().unwrap();
        assert_eq!(tail.records, vec![rec(3)]);
    }

    #[test]
    fn state_flush_attaches_bookmark() {
        let mut a = PageAssembler::new("s", 1000, true);
        assert!(a.on_record("s", rec(1)).is_none());
        assert!(a.on_record("s", rec(2)).is_none());
        let page = a.on_state(json!({"last_id": 2})).expect("flush on state");
        assert_eq!(page.records, vec![rec(1), rec(2)]);
        assert_eq!(page.bookmark, Some(json!({"last_id": 2})));
        // nothing left
        assert!(a.on_eof().is_none());
    }

    #[test]
    fn empty_run_with_trailing_state_yields_empty_page_with_bookmark() {
        let mut a = PageAssembler::new("s", 1000, true);
        let page = a.on_state(json!({"last_id": 0})).unwrap();
        assert!(page.records.is_empty());
        assert_eq!(page.bookmark, Some(json!({"last_id": 0})));
    }

    #[test]
    fn records_for_other_streams_are_ignored() {
        let mut a = PageAssembler::new("wanted", 1, true);
        assert!(a.on_record("other", rec(1)).is_none());
        assert!(a.on_record("other", rec(2)).is_none());
        // only the wanted stream flushes
        let page = a.on_record("wanted", rec(3)).unwrap();
        assert_eq!(page.records, vec![rec(3)]);
    }

    #[test]
    fn no_flush_on_state_defers_bookmark_to_next_flush() {
        let mut a = PageAssembler::new("s", 2, false);
        // STATE arrives first; not flushed because flush_on_state == false
        assert!(a.on_state(json!({"last_id": 0})).is_none());
        assert!(a.on_record("s", rec(1)).is_none());
        // batch flush picks up the pending bookmark
        let page = a.on_record("s", rec(2)).unwrap();
        assert_eq!(page.records, vec![rec(1), rec(2)]);
        assert_eq!(page.bookmark, Some(json!({"last_id": 0})));
    }

    #[test]
    fn eof_with_empty_buffer_and_no_state_yields_nothing() {
        let mut a = PageAssembler::new("s", 1000, true);
        assert!(a.on_eof().is_none());
    }
}
