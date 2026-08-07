//! Pipeline-template registry records (#444) — the persistent, versioned store
//! behind `faucet template …`, the `/v1/templates` endpoints, and the MCP
//! template tools.
//!
//! A template is a **config document registered once** plus the typed `params:`
//! it declares. Thereafter a caller triggers runs by `{id, params}` instead of
//! re-sending (and re-validating) the whole config. Storage rides the
//! `RunHistory` backends, like the Data Movement Catalog: an in-memory map for
//! the default backend, a `faucet_templates` table for the SQL ones, forwarded
//! by `FallbackHistory`.
//!
//! **Nothing secret is persisted.** The body is stored *verbatim* as submitted,
//! so `${env:…}` / `${vault:…}` remain unresolved tokens that are resolved at
//! trigger time, on the instance that runs the pipeline — the same privilege
//! surface as any other submitted config. Caller-supplied `secret: true` param
//! values are never written here at all: they exist only for the duration of one
//! trigger.

use crate::error::{CliError, CliResult};
use crate::params::ParamsSpec;
use crate::serve::load::ConfigFormat;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Maximum length of a template id. Long enough for `team-service-purpose`,
/// short enough to stay readable in a URL path and a CLI table.
pub const MAX_ID_LEN: usize = 64;

/// The reserved channel naming a template's newest version.
pub const LATEST_TAG: &str = "latest";

/// A **named version channel** — a movable pointer at one numeric version.
///
/// Versions themselves are numeric and auto-incrementing; channels are the
/// human-facing names you promote *between* them, exactly like container image
/// tags. The set is deliberately **closed**: an open-ended tag namespace turns
/// into a second, unreviewable naming system, and a typo (`prd`) would silently
/// create a new channel nobody watches. A caller who needs an arbitrary label
/// puts it in the run's `labels`, not in the registry.
///
/// [`Self::Latest`] is special: it is **derived**, always resolvable, and cannot
/// be assigned or moved — it is by definition the highest registered version.
/// Every other channel starts unset and points wherever it was last promoted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum VersionChannel {
    /// The newest registered version. Derived, never assigned.
    #[default]
    Latest,
    /// The version blessed as known-good — the usual promotion target.
    Stable,
    /// Production.
    Prod,
    /// Pre-production / release-candidate soak.
    PreProd,
    /// Staging.
    Staging,
    /// A partial-traffic canary ahead of `prod`.
    Canary,
    /// QA / integration testing.
    Test,
    /// Day-to-day development.
    Dev,
    /// The previously-blessed version, kept for a one-step rollback.
    Previous,
}

impl VersionChannel {
    /// Every channel, in promotion order (`dev` → … → `prod`), with the derived
    /// `latest` first. Drives help text, error messages, and `--tag` completion.
    pub const ALL: &'static [Self] = &[
        Self::Latest,
        Self::Dev,
        Self::Test,
        Self::Staging,
        Self::PreProd,
        Self::Canary,
        Self::Stable,
        Self::Prod,
        Self::Previous,
    ];

    /// The channels a caller may actually assign (everything except `latest`).
    pub const ASSIGNABLE: &'static [Self] = &[
        Self::Dev,
        Self::Test,
        Self::Staging,
        Self::PreProd,
        Self::Canary,
        Self::Stable,
        Self::Prod,
        Self::Previous,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Latest => LATEST_TAG,
            Self::Stable => "stable",
            Self::Prod => "prod",
            Self::PreProd => "pre-prod",
            Self::Staging => "staging",
            Self::Canary => "canary",
            Self::Test => "test",
            Self::Dev => "dev",
            Self::Previous => "previous",
        }
    }

    /// `latest` is computed from the version list, so it can never be promoted,
    /// moved, or deleted like the others.
    pub fn is_derived(self) -> bool {
        matches!(self, Self::Latest)
    }

    /// Parse a channel name. Case-insensitive, and `-`/`_`/`` separators are
    /// interchangeable, so `pre-prod`, `pre_prod`, `PreProd`, and `preprod` all
    /// name the same channel. Anything outside the closed set is rejected with
    /// the full list — the point of the enum.
    pub fn parse(raw: &str) -> CliResult<Self> {
        let normalized: String = raw
            .trim()
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .map(|c| c.to_ascii_lowercase())
            .collect();
        Self::ALL
            .iter()
            .copied()
            .find(|c| {
                c.as_str()
                    .chars()
                    .filter(char::is_ascii_alphanumeric)
                    .eq(normalized.chars())
            })
            .ok_or_else(|| {
                CliError::Config(format!(
                    "unknown template version channel '{raw}' — the named channels are fixed: {}",
                    Self::ALL
                        .iter()
                        .map(|c| c.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })
    }
}

impl std::fmt::Display for VersionChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for VersionChannel {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for VersionChannel {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// Which version of a template to use: a named [`VersionChannel`] or an exact
/// number.
///
/// Omitting the selector is the same as `latest`, so a caller that never
/// mentions versions always rides the newest registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionSelector {
    /// A named channel — `latest` (the default) or a promoted pointer.
    Channel(VersionChannel),
    /// An exact version, for a pin or a rollback.
    Pinned(u32),
}

impl Default for VersionSelector {
    fn default() -> Self {
        Self::Channel(VersionChannel::Latest)
    }
}

impl VersionSelector {
    /// The default selector: the newest version.
    pub const fn latest() -> Self {
        Self::Channel(VersionChannel::Latest)
    }

    /// Parse a channel name or a positive integer. A numeric string is a pin; a
    /// name must be one of the closed channel set.
    pub fn parse(raw: &str) -> CliResult<Self> {
        let s = raw.trim();
        // Digits are always a pin — never confused with a channel name.
        if s.chars().all(|c| c.is_ascii_digit()) && !s.is_empty() {
            return match s.parse::<u32>() {
                Ok(0) | Err(_) => Err(CliError::Config(format!(
                    "invalid template version '{raw}' — versions are numbered from 1"
                ))),
                Ok(n) => Ok(Self::Pinned(n)),
            };
        }
        VersionChannel::parse(s).map(Self::Channel)
    }

    /// True when this selector means "the newest version".
    pub fn is_latest(self) -> bool {
        matches!(self, Self::Channel(c) if c.is_derived())
    }

    /// The exact version, when the selector already names one. A non-`latest`
    /// channel needs a registry lookup — see
    /// [`crate::templates::resolve_version`].
    pub fn pinned(self) -> Option<u32> {
        match self {
            Self::Pinned(n) => Some(n),
            Self::Channel(_) => None,
        }
    }

    /// The channel this selector names, if any.
    pub fn channel(self) -> Option<VersionChannel> {
        match self {
            Self::Channel(c) => Some(c),
            Self::Pinned(_) => None,
        }
    }
}

impl std::fmt::Display for VersionSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Channel(c) => f.write_str(c.as_str()),
            Self::Pinned(n) => write!(f, "{n}"),
        }
    }
}

// Accepts a JSON/query value in any of the natural spellings — `"latest"`,
// `"pre-prod"`, `"3"`, or the bare number `3` — so an HTTP query string, a JSON
// body, and an MCP tool argument all deserialize the same way.
impl<'de> Deserialize<'de> for VersionSelector {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = VersionSelector;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "a named version channel or a version number")
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Self::Value, E> {
                VersionSelector::parse(s).map_err(serde::de::Error::custom)
            }
            fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<Self::Value, E> {
                VersionSelector::parse(&n.to_string()).map_err(serde::de::Error::custom)
            }
            fn visit_i64<E: serde::de::Error>(self, n: i64) -> Result<Self::Value, E> {
                VersionSelector::parse(&n.to_string()).map_err(serde::de::Error::custom)
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for VersionSelector {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

/// A validated template id: lowercase kebab/snake slug, `^[a-z0-9][a-z0-9_-]*$`.
///
/// Ids appear in URL paths (`/v1/templates/{id}`) and as CLI arguments, so the
/// charset is deliberately narrow — no slashes, dots, whitespace, or uppercase,
/// which rules out path traversal and case-collision surprises across backends.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TemplateId(String);

impl TemplateId {
    pub fn parse(raw: &str) -> CliResult<Self> {
        let s = raw.trim();
        if s.is_empty() {
            return Err(CliError::Config(
                "template id must not be empty — pass one with `--id`, or give the config a `name:`"
                    .into(),
            ));
        }
        if s.len() > MAX_ID_LEN {
            return Err(CliError::Config(format!(
                "template id '{s}' is longer than {MAX_ID_LEN} characters"
            )));
        }
        let mut chars = s.chars();
        let ok = match chars.next() {
            Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit() => {
                chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
            }
            _ => false,
        };
        if !ok {
            return Err(CliError::Config(format!(
                "invalid template id '{s}' — ids must match ^[a-z0-9][a-z0-9_-]*$ (lowercase \
                 letters, digits, `-`, `_`; first character alphanumeric)"
            )));
        }
        Ok(Self(s.to_string()))
    }

    /// Derive an id from a config's `name:` — lowercased, with runs of
    /// unsupported characters collapsed to `-`. Used when the caller registers
    /// without an explicit id.
    pub fn from_config_name(name: &str) -> CliResult<Self> {
        let mut slug = String::with_capacity(name.len());
        for c in name.chars() {
            if c.is_ascii_alphanumeric() {
                slug.push(c.to_ascii_lowercase());
            } else if !slug.ends_with('-') {
                slug.push('-');
            }
        }
        let trimmed = slug.trim_matches('-');
        let capped: String = trimmed.chars().take(MAX_ID_LEN).collect();
        Self::parse(capped.trim_matches('-'))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TemplateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for TemplateId {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value).map_err(|e| e.to_string())
    }
}

impl From<TemplateId> for String {
    fn from(value: TemplateId) -> Self {
        value.0
    }
}

/// One registered version of a template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateRecord {
    /// Stable registry id (a slug — see [`TemplateId`]).
    pub id: String,
    /// Monotonic version, starting at 1. `register` always appends a new one.
    pub version: u32,
    /// The config's own `name:`, if it has one (informational).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Free-text description supplied at registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The config document, stored **verbatim** (unresolved directives intact).
    pub body: String,
    /// Wire format of `body`, so the trigger path parses it the same way.
    pub format: ConfigFormat,
    /// The declared `params:` block, extracted at registration so callers can
    /// discover the trigger surface without parsing the body.
    #[serde(default)]
    pub params: ParamsSpec,
    pub created_at: DateTime<Utc>,
    /// Principal that registered this version (`None` for CLI registration).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
}

impl TemplateRecord {
    /// The summary view — everything except the (potentially large) body.
    pub fn summary(&self) -> TemplateSummary {
        TemplateSummary {
            id: self.id.clone(),
            version: self.version,
            name: self.name.clone(),
            description: self.description.clone(),
            params: self.params.clone(),
            created_at: self.created_at,
            created_by: self.created_by.clone(),
        }
    }
}

/// Body-free view of a template version, used by list endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateSummary {
    pub id: String,
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub params: ParamsSpec,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
}

/// A registration request, after validation. `version` is assigned by the store.
#[derive(Debug, Clone)]
pub struct TemplateDraft {
    pub id: TemplateId,
    pub name: Option<String>,
    pub description: Option<String>,
    pub body: String,
    pub format: ConfigFormat,
    pub params: ParamsSpec,
    pub created_by: Option<String>,
}

/// How many versions of one template the store keeps. Older versions are pruned
/// on register, so a template re-registered on every deploy can't grow the table
/// without bound while the recent history (for pinning / rollback) stays.
pub const VERSION_RETAIN: usize = 20;

/// Reduce a full version list to the latest version per id, preserving the
/// caller's ordering intent (newest-created first). Shared by the memory and SQL
/// backends so `template_list` can never disagree between them.
pub fn latest_per_id(mut records: Vec<TemplateRecord>) -> Vec<TemplateSummary> {
    // Highest version wins per id; ties are impossible (version is unique).
    records.sort_by(|a, b| a.id.cmp(&b.id).then(b.version.cmp(&a.version)));
    records.dedup_by(|a, b| a.id == b.id);
    let mut out: Vec<TemplateSummary> = records.iter().map(TemplateRecord::summary).collect();
    out.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(a.id.cmp(&b.id)));
    out
}

/// The version numbers to delete so at most [`VERSION_RETAIN`] remain for an id.
pub fn versions_to_prune(mut versions: Vec<u32>) -> Vec<u32> {
    if versions.len() <= VERSION_RETAIN {
        return Vec::new();
    }
    versions.sort_unstable_by(|a, b| b.cmp(a)); // newest first
    versions.split_off(VERSION_RETAIN)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str, version: u32, secs: i64) -> TemplateRecord {
        TemplateRecord {
            id: id.into(),
            version,
            name: None,
            description: None,
            body: "version: 1".into(),
            format: ConfigFormat::Yaml,
            params: ParamsSpec::new(),
            created_at: DateTime::from_timestamp(secs, 0).unwrap(),
            created_by: None,
        }
    }

    #[test]
    fn latest_per_id_keeps_highest_version_newest_first() {
        let out = latest_per_id(vec![
            rec("a", 1, 10),
            rec("a", 3, 30),
            rec("a", 2, 20),
            rec("b", 1, 40),
        ]);
        assert_eq!(out.len(), 2);
        // `b` was created most recently, so it leads.
        assert_eq!(out[0].id, "b");
        assert_eq!(out[1].id, "a");
        assert_eq!(out[1].version, 3);
    }

    #[test]
    fn latest_per_id_handles_empty() {
        assert!(latest_per_id(Vec::new()).is_empty());
    }

    #[test]
    fn summary_drops_the_body() {
        let s = rec("a", 1, 1).summary();
        let v = serde_json::to_value(&s).unwrap();
        assert!(v.get("body").is_none());
        assert_eq!(v["version"], 1);
    }

    #[test]
    fn prunes_only_beyond_the_retain_window() {
        assert!(versions_to_prune((1..=VERSION_RETAIN as u32).collect()).is_empty());
        let prune = versions_to_prune((1..=(VERSION_RETAIN as u32 + 3)).collect());
        // The three oldest go.
        assert_eq!(prune, vec![3, 2, 1]);
        assert!(versions_to_prune(vec![]).is_empty());
    }

    #[test]
    fn channels_are_a_closed_set_with_forgiving_spellings() {
        // Every spelling of the same channel normalizes identically.
        for raw in ["pre-prod", "pre_prod", "PreProd", "PRE-PROD", "  preprod "] {
            assert_eq!(VersionChannel::parse(raw).unwrap(), VersionChannel::PreProd);
        }
        for (raw, want) in [
            ("latest", VersionChannel::Latest),
            ("stable", VersionChannel::Stable),
            ("prod", VersionChannel::Prod),
            ("staging", VersionChannel::Staging),
            ("canary", VersionChannel::Canary),
            ("test", VersionChannel::Test),
            ("dev", VersionChannel::Dev),
            ("previous", VersionChannel::Previous),
        ] {
            assert_eq!(VersionChannel::parse(raw).unwrap(), want);
            assert_eq!(want.as_str(), raw);
        }
        // The point of the closed set: an invented or mistyped channel is
        // rejected, and the error lists the valid ones.
        for bad in ["", "prd", "production", "my-channel", "v2", "PROD1"] {
            let err = VersionChannel::parse(bad).unwrap_err().to_string();
            assert!(err.contains("fixed:"), "{bad:?}: {err}");
            assert!(err.contains("pre-prod"), "{bad:?}: {err}");
        }
    }

    #[test]
    fn only_latest_is_derived_and_assignable_excludes_it() {
        assert!(VersionChannel::Latest.is_derived());
        assert_eq!(VersionChannel::default(), VersionChannel::Latest);
        for c in VersionChannel::ASSIGNABLE {
            assert!(!c.is_derived(), "{c} must be assignable");
        }
        assert_eq!(
            VersionChannel::ALL.len(),
            VersionChannel::ASSIGNABLE.len() + 1,
            "ALL is ASSIGNABLE plus the derived `latest`"
        );
        assert!(!VersionChannel::ASSIGNABLE.contains(&VersionChannel::Latest));
    }

    #[test]
    fn channel_serde_round_trips_by_name() {
        use serde_json::json;
        assert_eq!(
            serde_json::to_value(VersionChannel::PreProd).unwrap(),
            json!("pre-prod")
        );
        assert_eq!(
            serde_json::from_value::<VersionChannel>(json!("pre_prod")).unwrap(),
            VersionChannel::PreProd
        );
        assert!(serde_json::from_value::<VersionChannel>(json!("nope")).is_err());
        assert!(serde_json::from_value::<VersionChannel>(json!(2)).is_err());
    }

    #[test]
    fn selector_distinguishes_channels_from_pins() {
        assert_eq!(
            VersionSelector::parse("prod").unwrap(),
            VersionSelector::Channel(VersionChannel::Prod)
        );
        assert_eq!(
            VersionSelector::parse("prod").unwrap().channel(),
            Some(VersionChannel::Prod)
        );
        assert!(VersionSelector::parse("prod").unwrap().pinned().is_none());
        assert!(!VersionSelector::parse("prod").unwrap().is_latest());
        assert!(VersionSelector::parse("latest").unwrap().is_latest());
        assert!(VersionSelector::default().is_latest());
        assert_eq!(VersionSelector::parse("4").unwrap().pinned(), Some(4));
        assert!(VersionSelector::parse("4").unwrap().channel().is_none());
        assert_eq!(
            VersionSelector::Channel(VersionChannel::Dev).to_string(),
            "dev"
        );
        // A channel-shaped typo is rejected, not silently treated as a pin.
        assert!(VersionSelector::parse("prd").is_err());
    }

    #[test]
    fn version_selector_parses_latest_and_numbers() {
        assert_eq!(
            VersionSelector::parse("latest").unwrap(),
            VersionSelector::latest()
        );
        assert_eq!(
            VersionSelector::parse("LATEST").unwrap(),
            VersionSelector::latest()
        );
        assert_eq!(
            VersionSelector::parse("  latest ").unwrap(),
            VersionSelector::latest()
        );
        assert_eq!(
            VersionSelector::parse("3").unwrap(),
            VersionSelector::Pinned(3)
        );
        // A version is 1-based; 0, negatives, and junk are all rejected.
        for bad in ["0", "-1", "", "v2", "newest", "1.5"] {
            assert!(
                VersionSelector::parse(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn version_selector_maps_to_a_lookup_and_back() {
        assert_eq!(VersionSelector::latest().pinned(), None);
        assert_eq!(VersionSelector::Pinned(7).pinned(), Some(7));
        assert_eq!(VersionSelector::default(), VersionSelector::latest());
        assert_eq!(VersionSelector::latest().to_string(), "latest");
        assert_eq!(VersionSelector::Pinned(2).to_string(), "2");
    }

    #[test]
    fn version_selector_serde_accepts_every_wire_spelling() {
        use serde_json::json;
        for wire in [json!("latest"), json!("LATEST")] {
            assert_eq!(
                serde_json::from_value::<VersionSelector>(wire).unwrap(),
                VersionSelector::latest()
            );
        }
        // A string (query string / CLI) and a bare number (JSON body) agree.
        assert_eq!(
            serde_json::from_value::<VersionSelector>(json!("4")).unwrap(),
            VersionSelector::Pinned(4)
        );
        assert_eq!(
            serde_json::from_value::<VersionSelector>(json!(4)).unwrap(),
            VersionSelector::Pinned(4)
        );
        assert!(serde_json::from_value::<VersionSelector>(json!(0)).is_err());
        assert!(serde_json::from_value::<VersionSelector>(json!("nope")).is_err());
        assert!(serde_json::from_value::<VersionSelector>(json!(true)).is_err());
        // Round-trips as the tag / the number, never as an enum variant name.
        assert_eq!(
            serde_json::to_value(VersionSelector::latest()).unwrap(),
            json!("latest")
        );
        assert_eq!(
            serde_json::to_value(VersionSelector::Pinned(9)).unwrap(),
            json!("9")
        );
    }

    #[test]
    fn id_parsing_accepts_slugs_and_rejects_the_rest() {
        for good in ["a", "tenant-sync", "t1_2", "9lives"] {
            assert_eq!(TemplateId::parse(good).unwrap().as_str(), good);
        }
        for bad in [
            "",
            "  ",
            "-lead",
            "_lead",
            "Upper",
            "has space",
            "has/slash",
            "has.dot",
            "../etc/passwd",
        ] {
            assert!(
                TemplateId::parse(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
        // Trimmed, and length-capped.
        assert_eq!(TemplateId::parse("  ok  ").unwrap().as_str(), "ok");
        assert!(TemplateId::parse(&"a".repeat(MAX_ID_LEN + 1)).is_err());
        assert!(TemplateId::parse(&"a".repeat(MAX_ID_LEN)).is_ok());
    }

    #[test]
    fn id_derives_from_a_config_name() {
        assert_eq!(
            TemplateId::from_config_name("Tenant Sync (prod)")
                .unwrap()
                .as_str(),
            "tenant-sync-prod"
        );
        assert_eq!(
            TemplateId::from_config_name("already-fine")
                .unwrap()
                .as_str(),
            "already-fine"
        );
        // Nothing usable → a clear error rather than an empty id.
        assert!(TemplateId::from_config_name("!!!").is_err());
        assert!(TemplateId::from_config_name("").is_err());
        // Over-long names are capped without leaving a trailing separator.
        let long = TemplateId::from_config_name(&format!("{} x", "a".repeat(MAX_ID_LEN))).unwrap();
        assert_eq!(long.as_str().len(), MAX_ID_LEN);
    }

    #[test]
    fn id_serde_round_trips_and_rejects_bad_values() {
        let id = TemplateId::parse("ok-id").unwrap();
        assert_eq!(
            serde_json::to_value(&id).unwrap(),
            serde_json::json!("ok-id")
        );
        let back: TemplateId = serde_json::from_value(serde_json::json!("ok-id")).unwrap();
        assert_eq!(back, id);
        assert!(serde_json::from_value::<TemplateId>(serde_json::json!("Bad Id")).is_err());
        assert_eq!(id.to_string(), "ok-id");
        assert_eq!(String::from(id), "ok-id");
    }

    #[test]
    fn record_round_trips_through_json() {
        let mut r = rec("a", 2, 5);
        r.params
            .insert("t".into(), crate::params::ParamSpec::string_default("v"));
        let text = serde_json::to_string(&r).unwrap();
        let back: TemplateRecord = serde_json::from_str(&text).unwrap();
        assert_eq!(back.id, "a");
        assert_eq!(back.version, 2);
        assert_eq!(back.params["t"].default, Some(serde_json::json!("v")));
    }
}
