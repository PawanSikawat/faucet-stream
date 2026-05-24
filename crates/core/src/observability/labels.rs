//! Labels carried by every observability span and metric emitted by the
//! pipeline-internal decorators.

use std::sync::Arc;

/// Common labels carried by every span and metric.
///
/// `pipeline` and `row` are Prometheus labels (bounded cardinality). `run_id`
/// is a span attribute only — never a metric label — used for trace
/// correlation. `connector` is captured per-decorator at construction time,
/// not stored here.
#[derive(Debug, Clone)]
pub struct Labels {
    pub pipeline: Arc<str>,
    pub row: Arc<str>,
    pub run_id: Arc<str>,
}

impl Labels {
    pub fn new(
        pipeline: impl Into<Arc<str>>,
        row: impl Into<Arc<str>>,
        run_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            pipeline: pipeline.into(),
            row: row.into(),
            run_id: run_id.into(),
        }
    }

    /// Convenience constructor for non-CLI library callers: pipeline name only,
    /// empty row, freshly-minted UUIDv7 run id.
    pub fn for_named(pipeline: impl Into<Arc<str>>) -> Self {
        let run_id: Arc<str> = uuid::Uuid::now_v7().to_string().into();
        Self::new(pipeline, "", run_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_all_fields() {
        let l = Labels::new("p", "r", "rid");
        assert_eq!(&*l.pipeline, "p");
        assert_eq!(&*l.row, "r");
        assert_eq!(&*l.run_id, "rid");
    }

    #[test]
    fn for_named_mints_run_id() {
        let l = Labels::for_named("p");
        assert_eq!(&*l.pipeline, "p");
        assert_eq!(&*l.row, "");
        // UUIDv7 string is 36 chars (8-4-4-4-12 hex + 4 dashes).
        assert_eq!(l.run_id.len(), 36);
    }

    #[test]
    fn clone_is_arc_shallow() {
        let l = Labels::new("p", "r", "rid");
        let c = l.clone();
        // Cloning Arc<str> bumps the refcount; the underlying allocation is shared.
        assert!(Arc::ptr_eq(&l.pipeline, &c.pipeline));
        assert!(Arc::ptr_eq(&l.row, &c.row));
        assert!(Arc::ptr_eq(&l.run_id, &c.run_id));
    }
}
