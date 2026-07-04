//! Value-based PII detectors (issue #206). Conservative by design: every
//! detector fully anchors its match and the card detector additionally
//! verifies the Luhn checksum, so false positives — which would silently
//! rewrite non-PII data — stay rare.

use super::config::Detector;
use regex::Regex;
use std::sync::OnceLock;

/// Return `true` if `value` looks like an instance of `detector`'s PII class.
/// Only ever called on string values.
pub fn detects(detector: Detector, value: &str) -> bool {
    match detector {
        Detector::Email => email_re().is_match(value),
        Detector::Ssn => is_ssn(value),
        Detector::Phone => is_phone(value),
        Detector::Ipv4 => is_ipv4(value),
        Detector::CreditCard => is_credit_card(value),
    }
}

fn email_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // Conservative single-@ RFC-5322 subset, fully anchored.
    RE.get_or_init(|| Regex::new(r"(?i)^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$").unwrap())
}

/// US SSN `NNN-NN-NNNN`. The `regex` crate has no look-around, so the shape is
/// matched with a plain regex and the never-issued area/group/serial groups
/// (000 / 666 / 9xx area, 00 group, 0000 serial) are excluded numerically —
/// which keeps false positives low.
fn is_ssn(value: &str) -> bool {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^(\d{3})-(\d{2})-(\d{4})$").unwrap());
    let Some(caps) = re.captures(value) else {
        return false;
    };
    let area = &caps[1];
    let group = &caps[2];
    let serial = &caps[3];
    area != "000" && area != "666" && !area.starts_with('9') && group != "00" && serial != "0000"
}

/// E.164 / North-American phone number. Code-based (no regex): allow only
/// digits and the usual separators, `+` only at the front, and require 10–15
/// digits total so short numeric ids don't match.
fn is_phone(value: &str) -> bool {
    let mut digits = 0usize;
    for (i, ch) in value.chars().enumerate() {
        match ch {
            '0'..='9' => digits += 1,
            '+' if i == 0 => {}
            ' ' | '.' | '-' | '(' | ')' => {}
            _ => return false,
        }
    }
    (10..=15).contains(&digits)
}

fn is_ipv4(value: &str) -> bool {
    let mut octets = 0;
    for part in value.split('.') {
        octets += 1;
        if octets > 4 {
            return false;
        }
        // 1–3 digits, no leading zeros (except "0" itself), value ≤ 255.
        if part.is_empty() || part.len() > 3 || !part.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        if part.len() > 1 && part.starts_with('0') {
            return false;
        }
        if part.parse::<u16>().unwrap_or(256) > 255 {
            return false;
        }
    }
    octets == 4
}

/// A credit-card candidate: 13–19 digits (after stripping single space/dash
/// separators) that passes the Luhn checksum.
fn is_credit_card(value: &str) -> bool {
    // Reject anything that isn't digits + single space/dash separators.
    let mut digits: Vec<u8> = Vec::with_capacity(19);
    let mut prev_sep = true; // disallow leading separator
    for ch in value.chars() {
        match ch {
            '0'..='9' => {
                digits.push(ch as u8 - b'0');
                prev_sep = false;
            }
            ' ' | '-' => {
                if prev_sep {
                    return false; // no leading / doubled separators
                }
                prev_sep = true;
            }
            _ => return false,
        }
    }
    if prev_sep && !digits.is_empty() && (value.ends_with(' ') || value.ends_with('-')) {
        return false; // trailing separator
    }
    if digits.len() < 13 || digits.len() > 19 {
        return false;
    }
    luhn_valid(&digits)
}

/// Luhn (mod-10) checksum over already-parsed digits.
fn luhn_valid(digits: &[u8]) -> bool {
    let mut sum = 0u32;
    let mut double = false;
    for &d in digits.iter().rev() {
        let mut v = d as u32;
        if double {
            v *= 2;
            if v > 9 {
                v -= 9;
            }
        }
        sum += v;
        double = !double;
    }
    sum.is_multiple_of(10)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_detector() {
        assert!(detects(Detector::Email, "alice@example.com"));
        assert!(detects(Detector::Email, "a.b+tag@sub.domain.co"));
        assert!(!detects(Detector::Email, "not an email"));
        assert!(!detects(Detector::Email, "alice@localhost")); // no TLD
        assert!(!detects(Detector::Email, "prefix alice@x.com suffix")); // anchored
    }

    #[test]
    fn ssn_detector() {
        assert!(detects(Detector::Ssn, "123-45-6789"));
        assert!(!detects(Detector::Ssn, "000-45-6789")); // invalid area
        assert!(!detects(Detector::Ssn, "666-45-6789"));
        assert!(!detects(Detector::Ssn, "900-45-6789"));
        assert!(!detects(Detector::Ssn, "123-00-6789")); // invalid group
        assert!(!detects(Detector::Ssn, "123-45-0000")); // invalid serial
        assert!(!detects(Detector::Ssn, "123456789")); // needs dashes
    }

    #[test]
    fn credit_card_detector_uses_luhn() {
        assert!(detects(Detector::CreditCard, "4111111111111111")); // Visa test
        assert!(detects(Detector::CreditCard, "4111 1111 1111 1111"));
        assert!(detects(Detector::CreditCard, "4111-1111-1111-1111"));
        assert!(detects(Detector::CreditCard, "378282246310005")); // Amex 15
        assert!(!detects(Detector::CreditCard, "4111111111111112")); // bad luhn
        assert!(!detects(Detector::CreditCard, "1234567890")); // too short
        assert!(!detects(Detector::CreditCard, "4111 1111 1111 111a")); // non-digit
        assert!(!detects(Detector::CreditCard, "-4111111111111111")); // leading sep
    }

    #[test]
    fn phone_detector() {
        assert!(detects(Detector::Phone, "+14155552671"));
        assert!(detects(Detector::Phone, "415-555-2671"));
        assert!(detects(Detector::Phone, "(415) 555-2671"));
        assert!(!detects(Detector::Phone, "12345")); // too short
        assert!(!detects(Detector::Phone, "not a phone"));
    }

    #[test]
    fn ipv4_detector() {
        assert!(detects(Detector::Ipv4, "192.168.0.1"));
        assert!(detects(Detector::Ipv4, "0.0.0.0"));
        assert!(detects(Detector::Ipv4, "255.255.255.255"));
        assert!(!detects(Detector::Ipv4, "256.0.0.1")); // > 255
        assert!(!detects(Detector::Ipv4, "192.168.01.1")); // leading zero
        assert!(!detects(Detector::Ipv4, "192.168.0")); // 3 octets
        assert!(!detects(Detector::Ipv4, "1.2.3.4.5")); // 5 octets
    }
}
