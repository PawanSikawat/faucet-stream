//! Scoped cleanup — delete destination rows a run did not write (#478).
//!
//! An incremental sync into an upsert sink **cannot remove records deleted at the
//! source**: a record that disappears simply stops appearing in the
//! "updated since X" feed, so `write_mode: upsert` keeps it in the destination
//! forever. The destination looks healthy, the run reports success, and stale
//! rows accumulate indefinitely with nothing surfacing the divergence.
//!
//! Scoped cleanup closes that hole for the case where the source can make a
//! **completeness claim**: "for scope S, these are *all* the records". The
//! canonical shape is a parent/child incremental sync — a child row fetching one
//! contact's associations is authoritative for `contact_id = <that contact>`.
//!
//! ## Why the claim comes from the source
//!
//! A sink cannot make it. It observes a page of records and cannot distinguish a
//! complete set from page 1 of 3, or from a partial page preceding a failure. Two
//! further reasons the scope is declared upstream rather than on the sink:
//!
//! 1. **Drift safety.** The predicate already exists in the source config
//!    (`/contacts/${contacts.id}/associations`). Declaring it a second time on
//!    the sink means two copies of one predicate, and when they diverge the
//!    pipeline deletes the wrong rows.
//! 2. **The empty-result case, which is decisive.** A contact that had five
//!    associations and now has none produces a fetch returning **zero records**.
//!    Any design inferring scopes from observed records never learns the scope
//!    existed, so the five stale rows survive — i.e. it fails precisely the case
//!    the feature exists to fix. A scope declared by the invocation comes from
//!    the parent record and survives an empty result set.
//!
//! ## When it runs
//!
//! **Once per invocation, after a fully successful run — never per page.** Two
//! ways to get this wrong, both silent data loss:
//!
//! - *Per page*: a scope's records spanning two pages → page 2's
//!   "delete what I didn't see" wipes what page 1 just wrote.
//! - *After a partial run*: the fetch dies at 40% → the delete removes the 60%
//!   that had not yet arrived.
//!
//! So [`run_stream`](crate::run_stream) only invokes it when the stream reached
//! its natural end **uncancelled**, and the CLI additionally attaches the policy
//! only for real root invocations (never `--dry-run` / `--limit` / a shard).
//!
//! ## Key accumulation and the ceiling
//!
//! Deleting "what this run did not write" requires knowing what it wrote. The
//! pipeline accumulates the **key tuples only** (not records) as pages are
//! written — see [`SeenKeys`]. That is bounded by the scope's row count, which is
//! small for the per-parent shape this targets, so a ceiling
//! ([`CleanupPolicy::max_keys`]) guards the pathological case: on breach the
//! cleanup **aborts with a typed error and deletes nothing**, rather than issuing
//! a partial delete that would destroy rows it simply forgot about.

use crate::error::FaucetError;
use crate::write_mode::KeyTuple;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Default ceiling on accumulated keys for one invocation's cleanup.
///
/// Sized for the shape this feature targets (a scope per parent record — tens to
/// low thousands of rows). Above it the cleanup refuses rather than guessing.
pub const DEFAULT_MAX_KEYS: usize = 100_000;

/// What to do about destination rows inside the claimed scope that this run did
/// not write.
///
/// Serialized as the sink-config field `cleanup:` via
/// [`WriteSpec`](crate::write_mode::WriteSpec), so every upsert-capable sink
/// accepts it without a per-connector config change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CleanupMode {
    /// Delete rows in the scope whose key was not written by this invocation.
    DeleteMissing,
}

/// A compiled cleanup instruction for one invocation.
#[derive(Debug, Clone)]
pub struct CleanupPolicy {
    /// The completeness claim, in **destination column** terms: the rows this
    /// invocation is authoritative for. Every entry is an equality predicate,
    /// AND-ed together.
    ///
    /// Destination terms rather than source terms because the `DELETE` executes
    /// against destination columns, and a transform chain may rename fields
    /// between the two.
    pub scope: BTreeMap<String, Value>,
    /// Key columns identifying a row — mirrors the sink's `key`.
    pub key: Vec<String>,
    /// Ceiling on accumulated keys before the cleanup refuses (see module docs).
    pub max_keys: usize,
}

impl CleanupPolicy {
    /// Build a policy, validating it is actionable.
    pub fn new(
        scope: BTreeMap<String, Value>,
        key: Vec<String>,
        max_keys: usize,
    ) -> Result<Self, FaucetError> {
        if scope.is_empty() {
            // An empty scope is an every-row predicate. Refusing here is the
            // difference between "delete this contact's stale rows" and
            // "truncate the table".
            return Err(FaucetError::Config(
                "cleanup: the completeness claim (`complete_for`) must name at least one \
                 column — an empty scope would match every row in the destination"
                    .into(),
            ));
        }
        if key.is_empty() {
            return Err(FaucetError::Config(
                "cleanup: requires a non-empty `key` so a written row can be told apart \
                 from a stale one"
                    .into(),
            ));
        }
        if scope.values().any(Value::is_null) {
            return Err(FaucetError::Config(
                "cleanup: the completeness claim contains a null value — an unresolved \
                 scope token would delete the wrong rows"
                    .into(),
            ));
        }
        Ok(Self {
            scope,
            key,
            max_keys: max_keys.max(1),
        })
    }
}

/// The set of key tuples an invocation wrote, accumulated across pages.
///
/// Deliberately stores only keys, not records: memory is O(rows in scope × key
/// width) rather than O(payload).
#[derive(Debug, Default)]
pub struct SeenKeys {
    keys: Vec<KeyTuple>,
    /// Set once the ceiling is breached. Sticky: a cleanup that lost track of
    /// even one key must not run at all.
    overflowed: bool,
}

impl SeenKeys {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the keys written for one page. Rows missing a key column, or
    /// carrying a null there, are ignored — they cannot be matched by a keyed
    /// delete anyway, and the write path already routes them to the DLQ or fails
    /// the batch.
    pub fn record_page(&mut self, page: &[Value], key: &[String], max_keys: usize) {
        if self.overflowed {
            return;
        }
        for rec in page {
            let Some(obj) = rec.as_object() else { continue };
            let mut tuple = Vec::with_capacity(key.len());
            let mut complete = true;
            for k in key {
                match obj.get(k) {
                    Some(v) if !v.is_null() => tuple.push((k.clone(), v.clone())),
                    _ => {
                        complete = false;
                        break;
                    }
                }
            }
            if !complete {
                continue;
            }
            if self.keys.len() >= max_keys {
                self.overflowed = true;
                self.keys.clear(); // free the buffer; the cleanup will refuse
                return;
            }
            self.keys.push(KeyTuple(tuple));
        }
    }

    /// Whether the ceiling was breached, which makes the cleanup unsafe to run.
    pub fn overflowed(&self) -> bool {
        self.overflowed
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn keys(&self) -> &[KeyTuple] {
        &self.keys
    }

    /// The typed error a breached ceiling produces. Separate from the accumulator
    /// so the caller decides whether to fail the run or log — but never to
    /// delete.
    pub fn overflow_error(&self, max_keys: usize) -> FaucetError {
        FaucetError::Config(format!(
            "cleanup: this invocation wrote more than {max_keys} rows in the claimed scope, \
             so the set of written keys could not be tracked. Nothing was deleted — a \
             partial delete would remove rows the run actually wrote. Narrow the scope \
             (a smaller `complete_for`), or raise the ceiling if the destination can take \
             a delete of this size"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scope() -> BTreeMap<String, Value> {
        BTreeMap::from([("contact_id".to_string(), json!(123))])
    }

    #[test]
    fn policy_requires_a_non_empty_scope() {
        // An empty scope is a truncate, not a cleanup.
        let err = CleanupPolicy::new(BTreeMap::new(), vec!["id".into()], 10)
            .expect_err("empty scope must be refused");
        assert!(err.to_string().contains("at least one"), "{err}");
    }

    #[test]
    fn policy_requires_a_key() {
        let err = CleanupPolicy::new(scope(), vec![], 10).expect_err("no key must be refused");
        assert!(err.to_string().contains("`key`"), "{err}");
    }

    #[test]
    fn policy_refuses_a_null_scope_value() {
        // An unresolved `${parent.id}` would land here as null and delete the
        // wrong rows.
        let s = BTreeMap::from([("contact_id".to_string(), Value::Null)]);
        let err = CleanupPolicy::new(s, vec!["id".into()], 10).expect_err("null must be refused");
        assert!(err.to_string().contains("null"), "{err}");
    }

    #[test]
    fn policy_floors_max_keys_at_one() {
        let p = CleanupPolicy::new(scope(), vec!["id".into()], 0).unwrap();
        assert_eq!(p.max_keys, 1);
    }

    #[test]
    fn accumulates_keys_across_pages() {
        let mut seen = SeenKeys::new();
        let key = vec!["id".to_string()];
        seen.record_page(&[json!({"id": 1}), json!({"id": 2})], &key, 100);
        seen.record_page(&[json!({"id": 3})], &key, 100);
        assert_eq!(seen.len(), 3);
        assert!(!seen.overflowed());
    }

    #[test]
    fn accumulates_composite_keys_in_declared_order() {
        let mut seen = SeenKeys::new();
        let key = vec!["a".to_string(), "b".to_string()];
        seen.record_page(&[json!({"b": 2, "a": 1})], &key, 100);
        assert_eq!(seen.len(), 1);
        let t = &seen.keys()[0].0;
        assert_eq!(
            t[0].0, "a",
            "key order follows the declared `key`, not the record"
        );
        assert_eq!(t[1].0, "b");
    }

    #[test]
    fn skips_rows_with_a_missing_or_null_key() {
        let mut seen = SeenKeys::new();
        let key = vec!["id".to_string()];
        seen.record_page(
            &[
                json!({"id": 1}),
                json!({"other": 9}), // missing key
                json!({"id": null}), // null key
                json!("not an object"),
            ],
            &key,
            100,
        );
        assert_eq!(seen.len(), 1, "only the well-keyed row is tracked");
    }

    #[test]
    fn overflow_is_sticky_and_frees_the_buffer() {
        let mut seen = SeenKeys::new();
        let key = vec!["id".to_string()];
        let page: Vec<Value> = (0..5).map(|i| json!({"id": i})).collect();
        seen.record_page(&page, &key, 3);
        assert!(seen.overflowed(), "ceiling of 3 must trip on a 5-row page");
        assert!(seen.is_empty(), "buffer is freed — the cleanup will refuse");
        // Sticky: a later page cannot un-overflow it.
        seen.record_page(&[json!({"id": 99})], &key, 3);
        assert!(seen.overflowed());
        assert!(seen.is_empty());
    }

    #[test]
    fn overflow_error_explains_that_nothing_was_deleted() {
        let seen = SeenKeys::new();
        let msg = seen.overflow_error(50).to_string();
        assert!(msg.contains("Nothing was deleted"), "{msg}");
        assert!(msg.contains("50"), "{msg}");
    }
}
