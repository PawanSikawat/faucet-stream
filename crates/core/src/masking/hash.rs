//! Deterministic hashing / tokenization for the masking layer (issue #206).
//!
//! Keyed digests use HMAC-SHA256 so the mapping is irreversible without the
//! key yet deterministic (equal inputs → equal outputs) so masked values stay
//! joinable across pipelines that share a key. Unkeyed digests fall back to a
//! plain SHA-256 — still deterministic, but recomputable by anyone, so it is
//! pseudonymization, not a secret.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// Resolved hashing strategy, compiled once from the policy `key`.
#[derive(Clone)]
pub enum Hasher {
    /// HMAC-SHA256 under the configured secret key.
    Keyed(Vec<u8>),
    /// Plain SHA-256 (no key configured).
    Unkeyed,
}

impl std::fmt::Debug for Hasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the key material.
        f.write_str(match self {
            Hasher::Keyed(_) => "Hasher::Keyed(<redacted>)",
            Hasher::Unkeyed => "Hasher::Unkeyed",
        })
    }
}

impl Hasher {
    /// Build from an optional policy key.
    pub fn from_key(key: Option<&str>) -> Self {
        match key {
            Some(k) => Hasher::Keyed(k.as_bytes().to_vec()),
            None => Hasher::Unkeyed,
        }
    }

    /// Full lowercase-hex digest of `input` (64 hex chars).
    pub fn digest_hex(&self, input: &str) -> String {
        let bytes = match self {
            Hasher::Keyed(key) => {
                // HMAC accepts a key of any length; construction is infallible.
                let mut mac =
                    Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
                mac.update(input.as_bytes());
                mac.finalize().into_bytes().to_vec()
            }
            Hasher::Unkeyed => {
                let mut h = Sha256::new();
                h.update(input.as_bytes());
                h.finalize().to_vec()
            }
        };
        to_hex(&bytes)
    }

    /// A short opaque token: `prefix` + the first 16 hex chars (64 bits) of the
    /// digest. Deterministic; collision-resistant enough to namespace a value.
    pub fn token(&self, input: &str, prefix: &str) -> String {
        let full = self.digest_hex(input);
        let short = &full[..16.min(full.len())];
        format!("{prefix}{short}")
    }
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_deterministic() {
        let h = Hasher::from_key(Some("k"));
        assert_eq!(h.digest_hex("alice"), h.digest_hex("alice"));
        assert_ne!(h.digest_hex("alice"), h.digest_hex("bob"));
    }

    #[test]
    fn keyed_differs_from_unkeyed_and_across_keys() {
        let unkeyed = Hasher::Unkeyed.digest_hex("x");
        let k1 = Hasher::from_key(Some("k1")).digest_hex("x");
        let k2 = Hasher::from_key(Some("k2")).digest_hex("x");
        assert_ne!(unkeyed, k1);
        assert_ne!(k1, k2);
    }

    #[test]
    fn digest_is_64_hex_chars() {
        let d = Hasher::Unkeyed.digest_hex("hello");
        assert_eq!(d.len(), 64);
        assert!(d.bytes().all(|b| b.is_ascii_hexdigit()));
        // Known SHA-256("hello").
        assert_eq!(
            d,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn token_has_prefix_and_short_body() {
        let h = Hasher::from_key(Some("k"));
        let t = h.token("alice", "tok_");
        assert!(t.starts_with("tok_"));
        assert_eq!(t.len(), "tok_".len() + 16);
        assert_eq!(t, h.token("alice", "tok_"), "deterministic");
    }

    #[test]
    fn debug_never_leaks_key() {
        let dbg = format!("{:?}", Hasher::from_key(Some("supersecret")));
        assert!(!dbg.contains("supersecret"));
        assert!(dbg.contains("redacted"));
    }
}
