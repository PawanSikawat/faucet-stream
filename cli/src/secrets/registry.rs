//! Process-global registry of resolved secret values + a redaction scrubber.
//!
//! Interpolation resolves secrets on raw config strings, so by the time the
//! config is a typed structure a secret value is an ordinary `String`. Rather
//! than tag fields, we track the resolved *values* and scrub any occurrence
//! from output the CLI emits (the [`RedactingWriter`]).

use std::borrow::Cow;
use std::collections::HashSet;
use std::io::{self, Write};
use std::sync::{OnceLock, RwLock};

/// Values shorter than this are not registered — masking 1–3 char strings
/// would over-redact unrelated output.
const MIN_REDACT_LEN: usize = 4;

fn registry() -> &'static RwLock<HashSet<String>> {
    static REG: OnceLock<RwLock<HashSet<String>>> = OnceLock::new();
    REG.get_or_init(|| RwLock::new(HashSet::new()))
}

/// Register a resolved secret value so it is scrubbed from future output.
pub fn register(secret: &str) {
    if secret.len() >= MIN_REDACT_LEN {
        registry()
            .write()
            .expect("secret registry lock poisoned")
            .insert(secret.to_owned());
    }
}

/// Replace every registered secret value in `input` with `***`.
pub fn redact(input: &str) -> Cow<'_, str> {
    let reg = registry().read().expect("secret registry lock poisoned");
    if reg.is_empty() {
        return Cow::Borrowed(input);
    }
    let mut out: Option<String> = None;
    for secret in reg.iter() {
        let current = out.as_deref().unwrap_or(input);
        if current.contains(secret.as_str()) {
            out = Some(current.replace(secret.as_str(), "***"));
        }
    }
    match out {
        Some(s) => Cow::Owned(s),
        None => Cow::Borrowed(input),
    }
}

/// An `io::Write` adapter that runs [`redact`] over every chunk before
/// forwarding it to the inner writer. Wrapping the tracing subscriber's
/// writer in this scrubs secret values out of *all* CLI log/diagnostic output
/// at the I/O boundary, regardless of which field carried the value.
pub struct RedactingWriter<W> {
    inner: W,
}

impl<W: Write> RedactingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl<W: Write> Write for RedactingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let text = String::from_utf8_lossy(buf);
        let scrubbed = redact(&text);
        self.inner.write_all(scrubbed.as_bytes())?;
        // Report the original length consumed — the tracing fmt layer treats a
        // short write as an error, and the scrubbed bytes are an internal detail.
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// `MakeWriter` that produces a [`RedactingWriter`] over stderr, for the
/// tracing fmt subscriber. Only needed when the `observability` feature wires
/// a subscriber (the sole place the CLI formats tracing output).
#[cfg(feature = "observability")]
pub struct RedactingMakeWriter;

#[cfg(feature = "observability")]
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RedactingMakeWriter {
    type Writer = RedactingWriter<std::io::Stderr>;
    fn make_writer(&'a self) -> Self::Writer {
        RedactingWriter::new(std::io::stderr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn clear() {
        registry().write().unwrap().clear();
    }

    #[test]
    #[serial]
    fn redacts_registered_value() {
        clear();
        register("supersecrettoken");
        assert_eq!(redact("Authorization: supersecrettoken"), "Authorization: ***");
    }

    #[test]
    #[serial]
    fn leaves_unregistered_text_untouched() {
        clear();
        register("supersecrettoken");
        assert_eq!(redact("nothing to see"), "nothing to see");
    }

    #[test]
    #[serial]
    fn does_not_register_short_values() {
        clear();
        register("abc"); // < MIN_REDACT_LEN
        assert_eq!(redact("abc def"), "abc def");
    }

    #[test]
    #[serial]
    fn writer_scrubs_secret_on_write() {
        clear();
        register("hunter2pass");
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = RedactingWriter::new(&mut buf);
            write!(w, "token={} done", "hunter2pass").unwrap();
            w.flush().unwrap();
        }
        assert_eq!(String::from_utf8(buf).unwrap(), "token=*** done");
    }
}
