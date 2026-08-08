//! A process-global redaction hook (#456 H5).
//!
//! Secrets are resolved on raw config text long before they become typed values,
//! so the CLI tracks the resolved *values* in `cli::secrets::registry` and scrubs
//! them from anything it emits. `faucet-core` cannot see that registry — it has
//! no secrets layer and must not gain one — yet core is where two outbound
//! surfaces are built:
//!
//! - the **DLQ envelope**'s `error.message` ([`crate::dlq::build_envelope`]),
//!   which is written to a file or object store, and
//! - any error text a host application forwards onward.
//!
//! An error string routinely embeds the material that produced it: `reqwest`'s
//! `Display` includes the request URL, so a REST source whose API key rides a
//! query parameter leaks the key; connection-string leakage in a CDC error has
//! already been a filed bug here (#84).
//!
//! So core exposes a hook: a host installs a scrubber once at startup, and core
//! routes outbound text through [`redact`]. With no hook installed, [`redact`] is
//! the identity function and costs one atomic load — library users who never
//! resolve secrets pay nothing and see no behaviour change.

use std::sync::OnceLock;

/// A scrubber: takes text, returns it with every known secret replaced.
pub type Redactor = Box<dyn Fn(&str) -> String + Send + Sync>;

fn hook() -> &'static OnceLock<Redactor> {
    static HOOK: OnceLock<Redactor> = OnceLock::new();
    &HOOK
}

/// Install the process-wide redactor. The **first** call wins; later calls are
/// ignored and return `false`, so a second `install_observability` (or a test
/// that runs after one) can never swap the scrubber out from under a run.
pub fn install(redactor: Redactor) -> bool {
    hook().set(redactor).is_ok()
}

/// Whether a redactor has been installed.
pub fn is_installed() -> bool {
    hook().get().is_some()
}

/// Scrub `text` with the installed redactor, or return it unchanged when none is
/// installed.
///
/// Call this on every string core hands to a destination outside the process.
pub fn redact(text: &str) -> String {
    match hook().get() {
        Some(f) => f(text),
        None => text.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hook is process-global and `install` is first-wins, so the whole
    /// contract is asserted in one test — two `#[test]` fns would race for the
    /// single `OnceLock`.
    #[test]
    fn install_is_first_wins_and_redact_applies_it() {
        // Before install: identity, and reported as absent.
        if !is_installed() {
            assert_eq!(redact("token=abcd"), "token=abcd");
        }

        assert!(install(Box::new(|s: &str| s.replace("abcd", "***"))));
        assert!(is_installed());
        assert_eq!(redact("token=abcd"), "token=***");
        assert_eq!(redact("nothing to do"), "nothing to do");

        // A second install is refused — the first redactor stays in force.
        assert!(!install(Box::new(|_: &str| "clobbered".to_owned())));
        assert_eq!(redact("token=abcd"), "token=***");
    }
}
