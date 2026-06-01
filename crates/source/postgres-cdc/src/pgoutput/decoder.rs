//! pgoutput wire-format decoder — turns raw bytes into `Message` values.

use super::messages::*;
use byteorder::{BigEndian, ReadBytesExt};
use faucet_core::FaucetError;
use std::io::{Cursor, Read};

/// XLogData envelope header that precedes every pgoutput payload coming over
/// the COPY BOTH stream (after the leading `'w'` byte stripped by the caller).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XLogDataHeader {
    pub wal_start: u64,
    pub wal_end: u64,
    pub server_ts: i64,
}

impl XLogDataHeader {
    pub const SIZE: usize = 24;

    /// Decode the 24-byte header. Caller has already stripped the leading
    /// `'w'` discriminator byte from the CopyData payload.
    pub fn decode(buf: &[u8]) -> Result<Self, FaucetError> {
        if buf.len() < Self::SIZE {
            return Err(FaucetError::Source(format!(
                "pgoutput: XLogData header truncated ({} < {})",
                buf.len(),
                Self::SIZE
            )));
        }
        let mut c = Cursor::new(buf);
        Ok(Self {
            wal_start: c.read_u64::<BigEndian>().map_err(io_err)?,
            wal_end: c.read_u64::<BigEndian>().map_err(io_err)?,
            server_ts: c.read_i64::<BigEndian>().map_err(io_err)?,
        })
    }
}

/// PrimaryKeepAlive message (CopyData discriminator `'k'`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryKeepAlive {
    pub wal_end: u64,
    pub server_ts: i64,
    pub reply_requested: bool,
}

impl PrimaryKeepAlive {
    pub const SIZE: usize = 17;

    pub fn decode(buf: &[u8]) -> Result<Self, FaucetError> {
        if buf.len() < Self::SIZE {
            return Err(FaucetError::Source(format!(
                "pgoutput: PrimaryKeepAlive truncated ({} < {})",
                buf.len(),
                Self::SIZE
            )));
        }
        let mut c = Cursor::new(buf);
        Ok(Self {
            wal_end: c.read_u64::<BigEndian>().map_err(io_err)?,
            server_ts: c.read_i64::<BigEndian>().map_err(io_err)?,
            reply_requested: c.read_u8().map_err(io_err)? != 0,
        })
    }
}

/// Decode a single pgoutput message from the start of `buf`.
///
/// The caller has already stripped the XLogData header. The first byte is
/// the message kind discriminator.
pub fn decode_message(buf: &[u8]) -> Result<Message, FaucetError> {
    let mut c = Cursor::new(buf);
    let kind = MessageKind::from_byte(c.read_u8().map_err(io_err_in("kind byte"))?)?;
    Ok(match kind {
        MessageKind::Begin => Message::Begin(decode_begin(&mut c)?),
        MessageKind::Commit => Message::Commit(decode_commit(&mut c)?),
        MessageKind::Origin => Message::Origin,
        MessageKind::Relation => Message::Relation(decode_relation(&mut c)?),
        MessageKind::Type => Message::Type,
        MessageKind::Insert => Message::Insert(decode_insert(&mut c)?),
        MessageKind::Update => Message::Update(decode_update(&mut c)?),
        MessageKind::Delete => Message::Delete(decode_delete(&mut c)?),
        MessageKind::Truncate => Message::Truncate(decode_truncate(&mut c)?),
    })
}

fn decode_begin(c: &mut Cursor<&[u8]>) -> Result<Begin, FaucetError> {
    Ok(Begin {
        final_lsn: c.read_u64::<BigEndian>().map_err(io_err_in("BEGIN"))?,
        commit_ts: c.read_i64::<BigEndian>().map_err(io_err_in("BEGIN"))?,
        xid: c.read_u32::<BigEndian>().map_err(io_err_in("BEGIN"))?,
    })
}

fn decode_commit(c: &mut Cursor<&[u8]>) -> Result<Commit, FaucetError> {
    Ok(Commit {
        flags: c.read_u8().map_err(io_err_in("COMMIT"))?,
        commit_lsn: c.read_u64::<BigEndian>().map_err(io_err_in("COMMIT"))?,
        end_lsn: c.read_u64::<BigEndian>().map_err(io_err_in("COMMIT"))?,
        commit_ts: c.read_i64::<BigEndian>().map_err(io_err_in("COMMIT"))?,
    })
}

fn decode_relation(c: &mut Cursor<&[u8]>) -> Result<Relation, FaucetError> {
    let oid = c.read_u32::<BigEndian>().map_err(io_err_in("RELATION"))?;
    let namespace = read_cstring(c)?;
    let name = read_cstring(c)?;
    let replica_identity = ReplicaIdentity::from_byte(c.read_u8().map_err(io_err_in("RELATION"))?)?;
    let n_columns = c.read_u16::<BigEndian>().map_err(io_err_in("RELATION"))?;
    let mut columns = Vec::with_capacity(n_columns as usize);
    for _ in 0..n_columns {
        columns.push(ColumnDesc {
            flags: c.read_u8().map_err(io_err_in("RELATION"))?,
            name: read_cstring(c)?,
            type_oid: c.read_u32::<BigEndian>().map_err(io_err_in("RELATION"))?,
            type_modifier: c.read_i32::<BigEndian>().map_err(io_err_in("RELATION"))?,
        });
    }
    Ok(Relation {
        oid,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn decode_insert(c: &mut Cursor<&[u8]>) -> Result<Insert, FaucetError> {
    let relation_oid = c.read_u32::<BigEndian>().map_err(io_err_in("INSERT"))?;
    let tag = c.read_u8().map_err(io_err_in("INSERT"))?;
    if tag != b'N' {
        return Err(FaucetError::Source(format!(
            "pgoutput INSERT: expected 'N' tuple tag, got {:?}",
            tag as char
        )));
    }
    Ok(Insert {
        relation_oid,
        new: decode_tuple(c)?,
    })
}

fn decode_update(c: &mut Cursor<&[u8]>) -> Result<Update, FaucetError> {
    let relation_oid = c.read_u32::<BigEndian>().map_err(io_err_in("UPDATE"))?;
    let first = c.read_u8().map_err(io_err_in("UPDATE"))?;
    let (old_kind, old) = match first {
        b'K' => (UpdateOldKind::Key, Some(decode_tuple(c)?)),
        b'O' => (UpdateOldKind::Full, Some(decode_tuple(c)?)),
        b'N' => {
            // No old tuple; the byte we just read is already the N tag, so
            // decode the new tuple directly without re-reading.
            return Ok(Update {
                relation_oid,
                old_kind: UpdateOldKind::None,
                old: None,
                new: decode_tuple(c)?,
            });
        }
        other => {
            return Err(FaucetError::Source(format!(
                "pgoutput UPDATE: invalid first tag byte {:?} (0x{other:02X}), \
                 expected 'K', 'O', or 'N'",
                other as char
            )));
        }
    };
    // After K or O old-tuple, the next byte must be 'N' for the new tuple.
    let n_tag = c.read_u8().map_err(io_err_in("UPDATE"))?;
    if n_tag != b'N' {
        return Err(FaucetError::Source(format!(
            "pgoutput UPDATE: expected 'N' new-tuple tag after old tuple, got {:?}",
            n_tag as char
        )));
    }
    Ok(Update {
        relation_oid,
        old_kind,
        old,
        new: decode_tuple(c)?,
    })
}

fn decode_delete(c: &mut Cursor<&[u8]>) -> Result<Delete, FaucetError> {
    let relation_oid = c.read_u32::<BigEndian>().map_err(io_err_in("DELETE"))?;
    let tag = c.read_u8().map_err(io_err_in("DELETE"))?;
    let old_kind = match tag {
        b'K' => DeleteOldKind::Key,
        b'O' => DeleteOldKind::Full,
        other => {
            return Err(FaucetError::Source(format!(
                "pgoutput DELETE: expected 'K' or 'O' tuple tag, got {:?}",
                other as char
            )));
        }
    };
    Ok(Delete {
        relation_oid,
        old_kind,
        old: decode_tuple(c)?,
    })
}

fn decode_truncate(c: &mut Cursor<&[u8]>) -> Result<Truncate, FaucetError> {
    let n = c.read_u32::<BigEndian>().map_err(io_err_in("TRUNCATE"))?;
    let flags = c.read_u8().map_err(io_err_in("TRUNCATE"))?;
    // Each relation OID is 4 bytes; a wire-controlled `n` can't exceed the
    // bytes that actually remain. Reject before reserving so a corrupt frame
    // can't drive a huge pre-allocation.
    let rem = remaining(c);
    if (n as usize).saturating_mul(4) > rem {
        return Err(FaucetError::Source(format!(
            "pgoutput TRUNCATE: declared relation count {n} exceeds {rem} remaining bytes"
        )));
    }
    let mut oids = Vec::with_capacity(n as usize);
    for _ in 0..n {
        oids.push(c.read_u32::<BigEndian>().map_err(io_err_in("TRUNCATE"))?);
    }
    Ok(Truncate {
        relation_oids: oids,
        cascade: flags & 0b01 != 0,
        restart_identity: flags & 0b10 != 0,
    })
}

fn decode_tuple(c: &mut Cursor<&[u8]>) -> Result<TupleData, FaucetError> {
    let n = c.read_u16::<BigEndian>().map_err(io_err_in("tuple"))?;
    let mut cells = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let kind = c.read_u8().map_err(io_err_in("tuple"))?;
        cells.push(match kind {
            b'n' => TupleCell::Null,
            b'u' => TupleCell::UnchangedToast,
            b't' => {
                let len = c.read_u32::<BigEndian>().map_err(io_err_in("tuple"))?;
                // Reject a wire-controlled length larger than the bytes that
                // remain before allocating a buffer for it.
                let rem = remaining(c);
                if len as usize > rem {
                    return Err(FaucetError::Source(format!(
                        "pgoutput tuple: declared text length {len} exceeds {rem} remaining bytes"
                    )));
                }
                let mut buf = vec![0u8; len as usize];
                c.read_exact(&mut buf).map_err(io_err_in("tuple"))?;
                TupleCell::Text(String::from_utf8(buf).map_err(|e| {
                    FaucetError::Source(format!("pgoutput tuple text not UTF-8: {e}"))
                })?)
            }
            b'b' => {
                return Err(FaucetError::Source(
                    "pgoutput tuple: binary-mode cells not supported in v1".into(),
                ));
            }
            other => {
                return Err(FaucetError::Source(format!(
                    "pgoutput tuple: unknown cell tag {:?}",
                    other as char
                )));
            }
        });
    }
    Ok(TupleData { cells })
}

fn read_cstring(c: &mut Cursor<&[u8]>) -> Result<String, FaucetError> {
    let mut out = Vec::new();
    loop {
        let b = c.read_u8().map_err(io_err_in("cstring"))?;
        if b == 0 {
            break;
        }
        out.push(b);
    }
    String::from_utf8(out).map_err(|e| FaucetError::Source(format!("pgoutput cstring: {e}")))
}

/// Bytes still unread in the cursor — used to bound wire-controlled
/// allocations against the data that actually remains.
fn remaining(c: &Cursor<&[u8]>) -> usize {
    c.get_ref().len().saturating_sub(c.position() as usize)
}

fn io_err(e: std::io::Error) -> FaucetError {
    FaucetError::Source(format!("pgoutput decode: {e}"))
}

fn io_err_in(ctx: &'static str) -> impl Fn(std::io::Error) -> FaucetError {
    move |e| FaucetError::Source(format!("pgoutput {ctx}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decode a hex-encoded fixture (whitespace ignored).
    fn hex(s: &str) -> Vec<u8> {
        let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        hex::decode(s).expect("valid hex")
    }

    #[test]
    fn decode_xlogdata_header() {
        // wal_start=0/16A4F88, wal_end=0/16A4FA0, server_ts=750000000000000
        let bytes = hex("00 00 00 00 01 6A 4F 88 \
             00 00 00 00 01 6A 4F A0 \
             00 02 A4 A6 4A 1B 80 00");
        let h = XLogDataHeader::decode(&bytes).unwrap();
        assert_eq!(h.wal_start, 0x0000_0000_016A_4F88);
        assert_eq!(h.wal_end, 0x0000_0000_016A_4FA0);
        assert_eq!(h.server_ts, 0x0002_A4A6_4A1B_8000);
    }

    #[test]
    fn decode_keepalive() {
        // wal_end=0/16A4F88, ts=750000000000000, reply_requested=1
        let bytes = hex("00 00 00 00 01 6A 4F 88 \
             00 02 A4 A6 4A 1B 80 00 \
             01");
        let k = PrimaryKeepAlive::decode(&bytes).unwrap();
        assert_eq!(k.wal_end, 0x0000_0000_016A_4F88);
        assert!(k.reply_requested);
    }

    #[test]
    fn decode_tuple_rejects_text_length_exceeding_remaining() {
        // n_cells=1, kind 't', declared text len=1000 (0x3E8), but only 2 bytes
        // ("AB") follow. The declared length must be rejected against the bytes
        // actually available *before* allocating a buffer for it.
        let bytes = hex("00 01 74 00 00 03 E8 41 42");
        let mut c = Cursor::new(bytes.as_slice());
        let Err(err) = decode_tuple(&mut c) else {
            panic!("an oversized declared text length must be rejected");
        };
        assert!(err.to_string().contains("exceeds"), "{err}");
    }

    #[test]
    fn decode_truncate_rejects_relation_count_exceeding_remaining() {
        // n=1_000_000 relations declared (4 MB of OIDs), flags=0, but only one
        // 4-byte OID follows. Must be rejected before reserving for `n`.
        let bytes = hex("00 0F 42 40 00 00 00 00 2A");
        let mut c = Cursor::new(bytes.as_slice());
        let Err(err) = decode_truncate(&mut c) else {
            panic!("an oversized declared relation count must be rejected");
        };
        assert!(err.to_string().contains("exceeds"), "{err}");
    }

    #[test]
    fn decode_begin_message() {
        // 'B', final_lsn=0/16A4FA0, ts=750000000000000, xid=0x4D2
        let bytes = hex("42 \
             00 00 00 00 01 6A 4F A0 \
             00 02 A4 A6 4A 1B 80 00 \
             00 00 04 D2");
        match decode_message(&bytes).unwrap() {
            Message::Begin(b) => {
                assert_eq!(b.final_lsn, 0x0000_0000_016A_4FA0);
                assert_eq!(b.xid, 0x4D2);
            }
            other => panic!("expected Begin, got {other:?}"),
        }
    }

    #[test]
    fn decode_commit_message() {
        // 'C', flags=0, commit_lsn=0/16A4FA0, end_lsn=0/16A4FB0, ts=750000000000000
        let bytes = hex("43 00 \
             00 00 00 00 01 6A 4F A0 \
             00 00 00 00 01 6A 4F B0 \
             00 02 A4 A6 4A 1B 80 00");
        match decode_message(&bytes).unwrap() {
            Message::Commit(c) => {
                assert_eq!(c.commit_lsn, 0x0000_0000_016A_4FA0);
                assert_eq!(c.end_lsn, 0x0000_0000_016A_4FB0);
            }
            other => panic!("expected Commit, got {other:?}"),
        }
    }

    #[test]
    fn decode_relation_message_two_columns() {
        // 'R', oid=16384, ns="public\0", name="users\0", ri='d', n_cols=2
        // col1: flags=1, name="id\0", type_oid=23 (int4), modifier=-1
        // col2: flags=0, name="name\0", type_oid=25 (text), modifier=-1
        let bytes = hex("52 \
             00 00 40 00 \
             70 75 62 6C 69 63 00 \
             75 73 65 72 73 00 \
             64 \
             00 02 \
             01 69 64 00 00 00 00 17 FF FF FF FF \
             00 6E 61 6D 65 00 00 00 00 19 FF FF FF FF");
        match decode_message(&bytes).unwrap() {
            Message::Relation(r) => {
                assert_eq!(r.oid, 16384);
                assert_eq!(r.namespace, "public");
                assert_eq!(r.name, "users");
                assert_eq!(r.replica_identity, ReplicaIdentity::Default);
                assert_eq!(r.columns.len(), 2);
                assert_eq!(r.columns[0].name, "id");
                assert_eq!(r.columns[0].type_oid, 23);
                assert_eq!(r.columns[0].flags & 1, 1);
                assert_eq!(r.columns[1].name, "name");
                assert_eq!(r.columns[1].type_oid, 25);
            }
            other => panic!("expected Relation, got {other:?}"),
        }
    }

    #[test]
    fn decode_insert_two_text_cells() {
        // 'I', relation=16384, 'N', n=2, ('t', len=1, "1"), ('t', len=5, "alice")
        let bytes = hex("49 \
             00 00 40 00 \
             4E \
             00 02 \
             74 00 00 00 01 31 \
             74 00 00 00 05 61 6C 69 63 65");
        match decode_message(&bytes).unwrap() {
            Message::Insert(i) => {
                assert_eq!(i.relation_oid, 16384);
                assert_eq!(i.new.cells.len(), 2);
                assert_eq!(i.new.cells[0], TupleCell::Text("1".into()));
                assert_eq!(i.new.cells[1], TupleCell::Text("alice".into()));
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn decode_insert_with_null_and_toast() {
        // 'I', relation=16384, 'N', n=3, ('t',1,"1"), ('n'), ('u')
        let bytes = hex("49 \
             00 00 40 00 \
             4E \
             00 03 \
             74 00 00 00 01 31 \
             6E \
             75");
        match decode_message(&bytes).unwrap() {
            Message::Insert(i) => {
                assert_eq!(i.new.cells[1], TupleCell::Null);
                assert_eq!(i.new.cells[2], TupleCell::UnchangedToast);
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn decode_update_with_key_old() {
        // 'U', relation=16384, 'K', old{1 cell, t,1,"1"}, 'N', new{2, t,1,"1", t,3,"bob"}
        let bytes = hex("55 \
             00 00 40 00 \
             4B \
             00 01 74 00 00 00 01 31 \
             4E \
             00 02 74 00 00 00 01 31 74 00 00 00 03 62 6F 62");
        match decode_message(&bytes).unwrap() {
            Message::Update(u) => {
                assert_eq!(u.old_kind, UpdateOldKind::Key);
                assert_eq!(u.old.unwrap().cells, vec![TupleCell::Text("1".into())]);
                assert_eq!(u.new.cells[1], TupleCell::Text("bob".into()));
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn decode_delete_key_only() {
        // 'D', relation=16384, 'K', old{1, t,1,"1"}
        let bytes = hex("44 \
             00 00 40 00 \
             4B \
             00 01 74 00 00 00 01 31");
        match decode_message(&bytes).unwrap() {
            Message::Delete(d) => {
                assert_eq!(d.old_kind, DeleteOldKind::Key);
                assert_eq!(d.old.cells.len(), 1);
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn decode_truncate_two_relations_cascade() {
        // 'T', n=2, flags=0b01 (cascade), oid=16384, oid=16385
        let bytes = hex("54 \
             00 00 00 02 \
             01 \
             00 00 40 00 \
             00 00 40 01");
        match decode_message(&bytes).unwrap() {
            Message::Truncate(t) => {
                assert_eq!(t.relation_oids, vec![16384, 16385]);
                assert!(t.cascade);
                assert!(!t.restart_identity);
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn decode_unknown_kind_errors() {
        let bytes = hex("5A 00 00"); // 'Z'
        assert!(decode_message(&bytes).is_err());
    }

    #[test]
    fn decode_truncated_input_errors() {
        let bytes = hex("42 00 00"); // 'B' with no body
        assert!(decode_message(&bytes).is_err());
    }

    #[test]
    fn decode_update_no_old_tuple() {
        // 'U', relation=16384, 'N' (no K/O old), new{2, t,1,"1", t,3,"bob"}
        let bytes = hex("55 \
             00 00 40 00 \
             4E \
             00 02 74 00 00 00 01 31 74 00 00 00 03 62 6F 62");
        match decode_message(&bytes).unwrap() {
            Message::Update(u) => {
                assert_eq!(u.old_kind, UpdateOldKind::None);
                assert!(u.old.is_none());
                assert_eq!(u.new.cells.len(), 2);
                assert_eq!(u.new.cells[0], TupleCell::Text("1".into()));
                assert_eq!(u.new.cells[1], TupleCell::Text("bob".into()));
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn decode_update_with_full_old_tuple() {
        // 'U', relation=16384, 'O', old{2, t,1,"1", t,5,"alice"}, 'N', new{2, t,1,"1", t,3,"bob"}
        let bytes = hex("55 \
             00 00 40 00 \
             4F \
             00 02 74 00 00 00 01 31 74 00 00 00 05 61 6C 69 63 65 \
             4E \
             00 02 74 00 00 00 01 31 74 00 00 00 03 62 6F 62");
        match decode_message(&bytes).unwrap() {
            Message::Update(u) => {
                assert_eq!(u.old_kind, UpdateOldKind::Full);
                let old = u.old.expect("old tuple present");
                assert_eq!(old.cells.len(), 2);
                assert_eq!(old.cells[1], TupleCell::Text("alice".into()));
                assert_eq!(u.new.cells[1], TupleCell::Text("bob".into()));
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn decode_truncate_restart_identity_only() {
        // 'T', n=1, flags=0b10 (restart identity, no cascade), oid=16384
        let bytes = hex("54 \
             00 00 00 01 \
             02 \
             00 00 40 00");
        match decode_message(&bytes).unwrap() {
            Message::Truncate(t) => {
                assert_eq!(t.relation_oids, vec![16384]);
                assert!(!t.cascade);
                assert!(t.restart_identity);
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn decode_insert_empty_text_cell() {
        // 'I', relation=16384, 'N', n=1, ('t', len=0)
        let bytes = hex("49 \
             00 00 40 00 \
             4E \
             00 01 \
             74 00 00 00 00");
        match decode_message(&bytes).unwrap() {
            Message::Insert(i) => {
                assert_eq!(i.new.cells.len(), 1);
                assert_eq!(i.new.cells[0], TupleCell::Text(String::new()));
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }
}
