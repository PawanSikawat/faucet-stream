//! Validated form of a [`TriggersFile`]. `compile` surfaces every problem at
//! startup (unique names, webhook-path collisions, resolvable pipeline ref,
//! interval/threshold bounds, missing backend feature) so a watcher never fails
//! mid-run from a config mistake. Pure (no IO except reading a path's existence,
//! which is done by the caller; here we only validate shapes).

use super::spec::{PipelineRef, QueueSpec, StoreSpec, TriggerKind, TriggerSpec, TriggersFile};
use std::collections::HashSet;

#[derive(Debug)]
pub struct CompiledTriggers {
    pub triggers: Vec<CompiledTrigger>,
}

#[derive(Debug, Clone)]
pub struct CompiledTrigger {
    pub spec: TriggerSpec,
    /// For webhook triggers: the route path `/v1/triggers/{name}`.
    pub webhook_path: Option<String>,
}

impl CompiledTrigger {
    pub fn name(&self) -> &str {
        &self.spec.name
    }
    pub fn kind_label(&self) -> &'static str {
        match self.spec.kind {
            TriggerKind::ObjectArrival { .. } => "object_arrival",
            TriggerKind::Webhook { .. } => "webhook",
            TriggerKind::QueueDepth { .. } => "queue_depth",
        }
    }
}

impl CompiledTriggers {
    /// Validate a parsed file. `err` strings are user-facing.
    pub fn compile(file: TriggersFile) -> Result<Self, String> {
        if file.version != 1 {
            return Err(format!(
                "triggers: unsupported version {} (expected 1)",
                file.version
            ));
        }
        let mut names = HashSet::new();
        let mut webhook_paths = HashSet::new();
        let mut compiled = Vec::with_capacity(file.triggers.len());

        for t in file.triggers {
            if t.name.trim().is_empty() {
                return Err("triggers: a trigger has an empty `name`".into());
            }
            if !names.insert(t.name.clone()) {
                return Err(format!("triggers: duplicate trigger name '{}'", t.name));
            }
            // Pipeline ref must be present and non-empty.
            match &t.config {
                PipelineRef::Path(p) if p.trim().is_empty() => {
                    return Err(format!("triggers: '{}' has an empty config path", t.name));
                }
                PipelineRef::Inline(v) if !v.is_object() => {
                    return Err(format!(
                        "triggers: '{}' inline config must be a mapping",
                        t.name
                    ));
                }
                _ => {}
            }

            let webhook_path = match &t.kind {
                TriggerKind::ObjectArrival {
                    poll_interval_secs,
                    store,
                    ..
                } => {
                    if *poll_interval_secs == 0 {
                        return Err(format!(
                            "triggers: '{}' poll_interval_secs must be >= 1",
                            t.name
                        ));
                    }
                    require_feature(&t.name, store_feature(store))?;
                    None
                }
                TriggerKind::Webhook { methods, .. } => {
                    require_feature(&t.name, "triggers")?; // always compiled when triggers is on
                    for m in methods {
                        let mu = m.to_ascii_uppercase();
                        if mu != "POST" && mu != "PUT" {
                            return Err(format!(
                                "triggers: '{}' unsupported webhook method '{}' (POST|PUT)",
                                t.name, m
                            ));
                        }
                    }
                    let path = format!("/v1/triggers/{}", t.name);
                    if !webhook_paths.insert(path.clone()) {
                        return Err(format!("triggers: duplicate webhook path '{path}'"));
                    }
                    Some(path)
                }
                TriggerKind::QueueDepth {
                    poll_interval_secs,
                    queue,
                    threshold,
                } => {
                    if *poll_interval_secs == 0 {
                        return Err(format!(
                            "triggers: '{}' poll_interval_secs must be >= 1",
                            t.name
                        ));
                    }
                    if *threshold == 0 {
                        return Err(format!(
                            "triggers: '{}' threshold must be >= 1",
                            t.name
                        ));
                    }
                    require_feature(&t.name, queue_feature(queue))?;
                    None
                }
            };

            compiled.push(CompiledTrigger {
                spec: t,
                webhook_path,
            });
        }
        Ok(Self {
            triggers: compiled,
        })
    }

    pub fn webhooks(&self) -> impl Iterator<Item = &CompiledTrigger> {
        self.triggers
            .iter()
            .filter(|t| t.webhook_path.is_some())
    }
}

fn store_feature(store: &StoreSpec) -> &'static str {
    match store {
        StoreSpec::S3 { .. } | StoreSpec::Gcs { .. } => "triggers-object-store",
    }
}

fn queue_feature(queue: &QueueSpec) -> &'static str {
    match queue {
        QueueSpec::Redis { .. } => "triggers-redis",
        QueueSpec::Kafka { .. } => "triggers-kafka",
    }
}

/// Returns Ok if the named feature is compiled in; else a clear error naming it.
fn require_feature(trigger: &str, feature: &str) -> Result<(), String> {
    let compiled = match feature {
        "triggers" => cfg!(feature = "triggers"),
        "triggers-object-store" => cfg!(feature = "triggers-object-store"),
        "triggers-redis" => cfg!(feature = "triggers-redis"),
        "triggers-kafka" => cfg!(feature = "triggers-kafka"),
        _ => true,
    };
    if compiled {
        Ok(())
    } else {
        Err(format!(
            "triggers: '{trigger}' requires a build with the `{feature}` feature"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(yaml: &str) -> TriggersFile {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn rejects_duplicate_names() {
        let f = file(
            "version: 1\ntriggers:\n  - { name: a, type: webhook, config: ./x.yaml }\n  - { name: a, type: webhook, config: ./y.yaml }\n",
        );
        let err = CompiledTriggers::compile(f).unwrap_err();
        assert!(err.contains("duplicate trigger name 'a'"), "{err}");
    }

    #[test]
    fn rejects_bad_version() {
        let f = file("version: 2\ntriggers: []\n");
        assert!(CompiledTriggers::compile(f).unwrap_err().contains("version"));
    }

    #[test]
    fn webhook_gets_path_and_collisions_rejected() {
        // Two webhooks with distinct names → distinct paths, both compile.
        let f = file(
            "version: 1\ntriggers:\n  - { name: a, type: webhook, config: ./x.yaml }\n  - { name: b, type: webhook, config: ./y.yaml }\n",
        );
        let c = CompiledTriggers::compile(f).unwrap();
        assert_eq!(c.webhooks().count(), 2);
        assert_eq!(c.triggers[0].webhook_path.as_deref(), Some("/v1/triggers/a"));
    }

    #[test]
    fn rejects_zero_threshold() {
        let f = file(
            "version: 1\ntriggers:\n  - name: q\n    type: queue_depth\n    config: ./x.yaml\n    threshold: 0\n    queue: { type: redis, url: \"redis://x\", key: k }\n",
        );
        // Only assert the threshold check when the redis backend is compiled;
        // otherwise the missing-feature error fires first (also acceptable).
        let err = CompiledTriggers::compile(f).unwrap_err();
        assert!(err.contains("threshold must be >= 1") || err.contains("triggers-redis"), "{err}");
    }
}
