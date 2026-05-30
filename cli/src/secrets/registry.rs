//! Process-global registry of resolved secret values + a redaction scrubber.
//!
//! Interpolation resolves secrets on raw config strings, so by the time the
//! config is a typed structure a secret value is an ordinary `String`. Rather
//! than tag fields, we track the resolved *values* and scrub any occurrence
//! from output the CLI emits (the [`RedactingWriter`]).

use std::borrow::Cow;
use std::collections::HashSet;
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
}
