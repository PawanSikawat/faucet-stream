//! In-process cache of `Relation` messages so subsequent `Insert`/`Update`/
//! `Delete` events can look up column names + type OIDs by relation OID.

use super::messages::Relation;
use faucet_core::FaucetError;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct RelationRegistry {
    by_oid: HashMap<u32, Relation>,
}

impl RelationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, rel: Relation) {
        // A re-sent Relation with a different column set means the table's
        // schema changed mid-stream (ALTER TABLE). Subsequent tuples decode
        // against the *new* descriptor, but a same-arity rename/type change
        // can silently bind values to the wrong column names — surface it so
        // an operator can correlate any downstream surprise (#78/#46).
        if let Some(prev) = self.by_oid.get(&rel.oid) {
            let prev_cols: Vec<(&str, u32)> = prev
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.type_oid))
                .collect();
            let new_cols: Vec<(&str, u32)> = rel
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.type_oid))
                .collect();
            if prev_cols != new_cols {
                tracing::warn!(
                    relation = %rel.name,
                    oid = rel.oid,
                    "postgres-cdc: relation column set changed mid-stream (schema change); \
                     subsequent rows decode against the new descriptor"
                );
            }
        }
        self.by_oid.insert(rel.oid, rel);
    }

    pub fn get(&self, oid: u32) -> Result<&Relation, FaucetError> {
        self.by_oid.get(&oid).ok_or_else(|| {
            FaucetError::Source(format!(
                "pgoutput: change event for unknown relation oid {oid} \
                 (Relation message must precede first change)"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::messages::ReplicaIdentity;

    fn rel(oid: u32, name: &str) -> Relation {
        Relation {
            oid,
            namespace: "public".into(),
            name: name.into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![],
        }
    }

    #[test]
    fn insert_then_get() {
        let mut r = RelationRegistry::new();
        r.insert(rel(16384, "users"));
        assert_eq!(r.get(16384).unwrap().name, "users");
    }

    #[test]
    fn second_insert_replaces() {
        let mut r = RelationRegistry::new();
        r.insert(rel(16384, "users_v1"));
        r.insert(rel(16384, "users_v2"));
        assert_eq!(r.get(16384).unwrap().name, "users_v2");
    }

    #[test]
    fn missing_oid_errors() {
        let r = RelationRegistry::new();
        let err = r.get(99999).unwrap_err();
        assert!(format!("{err}").contains("99999"));
    }
}
