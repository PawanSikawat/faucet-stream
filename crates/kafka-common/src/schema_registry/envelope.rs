//! Confluent wire envelope: `[0x00][schema_id: be u32][payload bytes...]`.

use faucet_core::FaucetError;

pub const MAGIC_BYTE: u8 = 0x00;

/// Prepend the Confluent wire envelope to a payload.
pub fn encode(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(MAGIC_BYTE);
    out.extend_from_slice(&schema_id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode the envelope. Returns `(schema_id, payload_slice)`.
pub fn decode(bytes: &[u8]) -> Result<(u32, &[u8]), FaucetError> {
    if bytes.len() < 5 {
        return Err(FaucetError::Source(format!(
            "schema-registry envelope is {} bytes, expected at least 5",
            bytes.len()
        )));
    }
    if bytes[0] != MAGIC_BYTE {
        return Err(FaucetError::Source(format!(
            "schema-registry magic byte is 0x{:02x}, expected 0x00",
            bytes[0]
        )));
    }
    let schema_id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    Ok((schema_id, &bytes[5..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let payload = b"hello world";
        let encoded = encode(42, payload);
        assert_eq!(encoded[0], 0x00);
        assert_eq!(&encoded[1..5], &42u32.to_be_bytes());
        let (id, body) = decode(&encoded).unwrap();
        assert_eq!(id, 42);
        assert_eq!(body, payload);
    }

    #[test]
    fn rejects_short_buffer() {
        assert!(decode(&[0x00, 0, 0, 0]).is_err());
    }

    #[test]
    fn rejects_wrong_magic() {
        let mut buf = encode(1, b"x");
        buf[0] = 0x01;
        assert!(decode(&buf).is_err());
    }

    #[test]
    fn decode_empty_payload() {
        let encoded = encode(7, b"");
        let (id, body) = decode(&encoded).unwrap();
        assert_eq!(id, 7);
        assert!(body.is_empty());
    }

    #[test]
    fn round_trip_max_schema_id() {
        let encoded = encode(u32::MAX, b"payload");
        let (id, body) = decode(&encoded).unwrap();
        assert_eq!(id, u32::MAX);
        assert_eq!(body, b"payload");
    }
}
