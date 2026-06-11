//! `object_arrival` trigger: incremental S3/GCS prefix listing. The pure
//! `Cursor` decides which listed objects are new; the watcher (Task 13) does IO.

/// One listed object, decoupled from `object_store::ObjectMeta` so the cursor is
/// pure and testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    pub key: String,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub size: u64,
    pub etag: Option<String>,
}

/// Tracks a high-water `last_modified` + the set of keys seen exactly at that
/// timestamp (ties), so the same object is never re-emitted while genuinely new
/// objects (even at the same second) are.
#[derive(Debug, Default)]
pub struct Cursor {
    watermark: Option<chrono::DateTime<chrono::Utc>>,
    seen_at_watermark: std::collections::HashSet<String>,
}

impl Cursor {
    /// Seed for `start_at: now` — ignore everything at/below `now`.
    pub fn starting_now(now: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            watermark: Some(now),
            seen_at_watermark: std::collections::HashSet::new(),
        }
    }

    /// Seed for `start_at: beginning` — emit every existing object once.
    pub fn starting_beginning() -> Self {
        Self::default()
    }

    /// Return objects strictly newer than the watermark (or at-watermark but
    /// unseen). Does NOT advance the watermark — call [`Cursor::commit`] after a
    /// successful fire so a dropped fire is retried.
    pub fn new_objects(&self, listing: &[ListedObject]) -> Vec<ListedObject> {
        let mut out = Vec::new();
        for o in listing {
            match self.watermark {
                None => out.push(o.clone()),
                Some(w) if o.last_modified > w => out.push(o.clone()),
                Some(w) if o.last_modified == w && !self.seen_at_watermark.contains(&o.key) => {
                    out.push(o.clone())
                }
                _ => {}
            }
        }
        out
    }

    /// Mark an object committed (advance the watermark past it).
    pub fn commit(&mut self, o: &ListedObject) {
        match self.watermark {
            Some(w) if o.last_modified > w => {
                self.watermark = Some(o.last_modified);
                self.seen_at_watermark.clear();
                self.seen_at_watermark.insert(o.key.clone());
            }
            Some(w) if o.last_modified == w => {
                self.seen_at_watermark.insert(o.key.clone());
            }
            None => {
                self.watermark = Some(o.last_modified);
                self.seen_at_watermark.insert(o.key.clone());
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod cursor_tests {
    use super::*;
    use chrono::TimeZone;

    fn obj(key: &str, secs: i64) -> ListedObject {
        ListedObject {
            key: key.into(),
            last_modified: chrono::Utc.timestamp_opt(secs, 0).unwrap(),
            size: 1,
            etag: None,
        }
    }

    #[test]
    fn starting_now_ignores_existing() {
        let now = chrono::Utc.timestamp_opt(1000, 0).unwrap();
        let c = Cursor::starting_now(now);
        // Existing object at t=900 is older → not new.
        assert!(c.new_objects(&[obj("a", 900)]).is_empty());
        // Newer object at t=1100 → new.
        assert_eq!(c.new_objects(&[obj("b", 1100)]).len(), 1);
    }

    #[test]
    fn starting_beginning_emits_all_then_commits() {
        let mut c = Cursor::starting_beginning();
        let listing = vec![obj("a", 100), obj("b", 200)];
        let new = c.new_objects(&listing);
        assert_eq!(new.len(), 2);
        for o in &new {
            c.commit(o);
        }
        // After commit, none are new.
        assert!(c.new_objects(&listing).is_empty());
    }

    #[test]
    fn handles_ties_at_watermark() {
        let mut c = Cursor::starting_beginning();
        let a = obj("a", 100);
        c.commit(&c.new_objects(&[a.clone()])[0].clone());
        // A second object at the SAME timestamp is still new (unseen key).
        let b = obj("b", 100);
        let new = c.new_objects(&[a.clone(), b.clone()]);
        assert_eq!(new, vec![b.clone()]);
        c.commit(&b);
        assert!(c.new_objects(&[a, b]).is_empty());
    }

    #[test]
    fn dropped_fire_is_retried_until_committed() {
        let mut c = Cursor::starting_beginning();
        let a = obj("a", 100);
        // new_objects without commit → still new next time (simulating a drop).
        assert_eq!(c.new_objects(&[a.clone()]).len(), 1);
        assert_eq!(c.new_objects(&[a.clone()]).len(), 1);
        c.commit(&a);
        assert!(c.new_objects(&[a]).is_empty());
    }
}
