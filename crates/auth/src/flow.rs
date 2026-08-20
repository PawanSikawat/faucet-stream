//! Composable multi-step auth **flow** provider (#511).
//!
//! Turns `AuthProvider` from a single-step credential source into a small
//! declarative program: an optional login / pre-flight request chain whose
//! responses are captured by JSONPath, arbitrary credential *placement*
//! (header / query / cookie / body), a pluggable HMAC request *signer*, and a
//! dynamic per-session base-URL — on top of the existing single-flight
//! machinery.
//!
//! ```yaml
//! auth:
//!   bullhorn:
//!     type: flow
//!     config:
//!       steps:
//!         - request: { method: POST, url: "https://auth/oauth/token",
//!                      form: { grant_type: refresh_token, refresh_token: "..." } }
//!           capture: { access_token: "$.access_token" }
//!         - request: { method: GET, url: "https://login/rest/login",
//!                      query: { access_token: "${access_token}" } }
//!           capture: { bh_rest_token: "$.BhRestToken", base_url: "$.restUrl" }
//!       apply:
//!         - { into: query, name: BhRestToken, value: "${bh_rest_token}" }
//!       base_url_from: "${base_url}"
//!       ttl_secs: 86400
//!       reauth_on: [401]
//! ```

use crate::auth_http_client;
use async_trait::async_trait;
use base64::Engine;
use faucet_core::{AuthProvider, Credential, CredentialPlacement, FaucetError, RequestAuth};
use hmac::{Hmac, Mac};
use jsonpath_rust::JsonPath;
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

type HmacSha256 = Hmac<Sha256>;

// ── Config ──────────────────────────────────────────────────────────────────

/// One HTTP request in the login / pre-flight chain.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlowRequest {
    #[serde(default = "default_method")]
    method: String,
    url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    query: HashMap<String, String>,
    /// `application/x-www-form-urlencoded` body.
    #[serde(default)]
    form: Option<HashMap<String, String>>,
    /// JSON body (mutually exclusive with `form`).
    #[serde(default)]
    json: Option<Value>,
}

fn default_method() -> String {
    "GET".to_owned()
}

/// A login step: a request plus the values to capture from its JSON response.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlowStep {
    request: FlowRequest,
    /// Captured-name → JSONPath into the response body.
    #[serde(default)]
    capture: HashMap<String, String>,
}

/// Where a captured credential is placed into data requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PlaceTarget {
    Header,
    Query,
    Cookie,
    Body,
}

/// HMAC algorithm for the request signer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SignAlg {
    HmacSha256,
}

/// Signature encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SigEncoding {
    #[default]
    Hex,
    Base64,
}

/// Where a computed signature is placed (a header, with a value template).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignInto {
    header: String,
    /// Value template; `${sig}` is the computed signature. Defaults to `${sig}`.
    #[serde(default = "default_sig_format")]
    format: String,
}

fn default_sig_format() -> String {
    "${sig}".to_owned()
}

/// A pluggable HMAC request signer.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignSpec {
    alg: SignAlg,
    /// HMAC key (resolve via `${env:...}` / `${vault:...}` in the catalog).
    key: String,
    /// The signature base string; `${captured}`, `${ts}`, `${nonce}` are
    /// substituted before signing.
    template: String,
    #[serde(default)]
    encoding: SigEncoding,
    into: SignInto,
}

/// One `apply` entry: either a static placement or a computed signature.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum ApplySpec {
    /// `{ into, name, value }` — place a (templated) value.
    Place {
        into: PlaceTarget,
        name: String,
        /// Value template; `${captured}` substituted.
        value: String,
    },
    /// `{ sign: { ... } }` — compute and place an HMAC signature.
    Sign { sign: SignSpec },
}

/// The `type: flow` provider config.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlowConfig {
    /// Login / pre-flight chain, run in order; each captures values for later
    /// steps and for `apply`.
    #[serde(default)]
    steps: Vec<FlowStep>,
    /// Credential placements applied to every data request.
    #[serde(default)]
    apply: Vec<ApplySpec>,
    /// Template (over captured values) yielding a per-session base-URL that
    /// overrides the connector's configured `base_url`.
    #[serde(default)]
    base_url_from: Option<String>,
    /// Re-run the login chain after this many seconds.
    #[serde(default)]
    ttl_secs: Option<u64>,
    /// HTTP statuses that trigger a re-login (via `invalidate`). Advisory —
    /// stored for connectors that wire status-based reauth.
    #[serde(default)]
    reauth_on: Vec<u16>,
}

impl FlowConfig {
    fn validate(&self) -> Result<(), FaucetError> {
        if self.steps.is_empty() && self.apply.is_empty() {
            return Err(FaucetError::Config(
                "flow auth: at least one of `steps` or `apply` is required".to_owned(),
            ));
        }
        for (i, step) in self.steps.iter().enumerate() {
            if step.request.url.trim().is_empty() {
                return Err(FaucetError::Config(format!(
                    "flow auth: step {i} has an empty `url`"
                )));
            }
            if step.request.form.is_some() && step.request.json.is_some() {
                return Err(FaucetError::Config(format!(
                    "flow auth: step {i} sets both `form` and `json`; pick one"
                )));
            }
        }
        for (i, a) in self.apply.iter().enumerate() {
            match a {
                ApplySpec::Place { name, .. } if name.trim().is_empty() => {
                    return Err(FaucetError::Config(format!(
                        "flow auth: apply[{i}] has an empty `name`"
                    )));
                }
                ApplySpec::Sign { sign } if sign.into.header.trim().is_empty() => {
                    return Err(FaucetError::Config(format!(
                        "flow auth: apply[{i}].sign.into.header must not be empty"
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

// ── Template rendering ───────────────────────────────────────────────────────

/// Substitute `${key}` tokens from `ctx`. Unknown tokens are left verbatim.
/// Single-pass — a substituted value is never re-scanned.
fn render(template: &str, ctx: &HashMap<String, String>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        if let Some(end) = after.find('}') {
            let key = &after[..end];
            match ctx.get(key) {
                Some(v) => out.push_str(v),
                None => {
                    out.push_str("${");
                    out.push_str(key);
                    out.push('}');
                }
            }
            rest = &after[end + 1..];
        } else {
            out.push_str(&rest[start..]);
            rest = "";
        }
    }
    out.push_str(rest);
    out
}

/// A captured JSON value rendered to its string form for templating.
fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn jsonpath_first(body: &Value, path: &str) -> Option<Value> {
    let results = body.query(path).ok()?;
    results.first().map(|v| (*v).clone())
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn hmac_sign(key: &str, message: &str, encoding: SigEncoding) -> String {
    let mut mac =
        HmacSha256::new_from_slice(key.as_bytes()).expect("HMAC accepts a key of any length");
    mac.update(message.as_bytes());
    let bytes = mac.finalize().into_bytes();
    match encoding {
        SigEncoding::Hex => to_hex(&bytes),
        SigEncoding::Base64 => base64::engine::general_purpose::STANDARD.encode(bytes),
    }
}

/// Build the placements + base-URL for one request from the captured context.
/// Pure given `captured` (and the clock values injected by the caller).
fn build_request_auth(
    apply: &[ApplySpec],
    base_url_from: &Option<String>,
    ctx: &HashMap<String, String>,
) -> RequestAuth {
    let mut out = RequestAuth::new();
    for a in apply {
        match a {
            ApplySpec::Place { into, name, value } => {
                let value = render(value, ctx);
                let name = name.clone();
                let placement = match into {
                    PlaceTarget::Header => CredentialPlacement::Header { name, value },
                    PlaceTarget::Query => CredentialPlacement::Query { name, value },
                    PlaceTarget::Cookie => CredentialPlacement::Cookie { name, value },
                    PlaceTarget::Body => CredentialPlacement::BodyField { name, value },
                };
                out = out.with_placement(placement);
            }
            ApplySpec::Sign { sign } => {
                let base = render(&sign.template, ctx);
                let sig = match sign.alg {
                    SignAlg::HmacSha256 => hmac_sign(&sign.key, &base, sign.encoding),
                };
                let mut sig_ctx = ctx.clone();
                sig_ctx.insert("sig".to_owned(), sig);
                let value = render(&sign.into.format, &sig_ctx);
                out = out.with_placement(CredentialPlacement::Header {
                    name: sign.into.header.clone(),
                    value,
                });
            }
        }
    }
    if let Some(tmpl) = base_url_from {
        let rendered = render(tmpl, ctx);
        if !rendered.is_empty() {
            out = out.with_base_url(rendered);
        }
    }
    out
}

// ── Provider ────────────────────────────────────────────────────────────────

struct Session {
    /// Captured values (as strings, ready for templating) plus a fresh clock.
    ctx: HashMap<String, String>,
    expires_at: Option<Instant>,
}

impl Session {
    fn valid(&self) -> bool {
        match self.expires_at {
            Some(exp) => Instant::now() < exp,
            None => true,
        }
    }
}

/// A composable multi-step auth flow provider.
pub struct FlowProvider {
    http: reqwest::Client,
    config: FlowConfig,
    state: Mutex<Option<Session>>,
}

impl std::fmt::Debug for FlowProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowProvider")
            .field("steps", &self.config.steps.len())
            .field("apply", &self.config.apply.len())
            .finish()
    }
}

impl FlowProvider {
    /// Build a flow provider from its `config` block.
    pub fn from_config(config: &Value) -> Result<Self, FaucetError> {
        let config: FlowConfig = serde_json::from_value(config.clone())
            .map_err(|e| FaucetError::Config(format!("flow auth: invalid config: {e}")))?;
        config.validate()?;
        Ok(Self {
            http: auth_http_client(),
            config,
            state: Mutex::new(None),
        })
    }

    /// A fresh clock context (`ts`, `nonce`) for signing.
    fn clock_ctx() -> HashMap<String, String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let mut ctx = HashMap::new();
        ctx.insert("ts".to_owned(), now.to_string());
        ctx.insert("nonce".to_owned(), format!("{now}{nanos}"));
        ctx
    }

    /// Run the login chain, returning the captured context.
    async fn run_login(&self) -> Result<HashMap<String, String>, FaucetError> {
        let mut ctx: HashMap<String, String> = HashMap::new();
        for (i, step) in self.config.steps.iter().enumerate() {
            let req = &step.request;
            let method = reqwest::Method::from_bytes(req.method.to_uppercase().as_bytes())
                .map_err(|_| {
                    FaucetError::Config(format!(
                        "flow auth: step {i} has an invalid HTTP method '{}'",
                        req.method
                    ))
                })?;
            let url = render(&req.url, &ctx);
            let mut builder = self.http.request(method, &url);
            for (k, v) in &req.headers {
                builder = builder.header(k.as_str(), render(v, &ctx));
            }
            if !req.query.is_empty() {
                let q: Vec<(String, String)> = req
                    .query
                    .iter()
                    .map(|(k, v)| (k.clone(), render(v, &ctx)))
                    .collect();
                builder = builder.query(&q);
            }
            if let Some(form) = &req.form {
                let f: Vec<(String, String)> = form
                    .iter()
                    .map(|(k, v)| (k.clone(), render(v, &ctx)))
                    .collect();
                builder = builder.form(&f);
            } else if let Some(json) = &req.json {
                let rendered = render(&serde_json::to_string(json).unwrap_or_default(), &ctx);
                let body: Value = serde_json::from_str(&rendered).unwrap_or_else(|_| json.clone());
                builder = builder.json(&body);
            }
            let resp = builder.send().await.map_err(|e| {
                FaucetError::Auth(format!("flow auth: step {i} request failed: {e}"))
            })?;
            let status = resp.status();
            if !status.is_success() {
                return Err(FaucetError::Auth(format!(
                    "flow auth: step {i} returned HTTP {}",
                    status.as_u16()
                )));
            }
            let body: Value = if step.capture.is_empty() {
                Value::Null
            } else {
                resp.json().await.map_err(|e| {
                    FaucetError::Auth(format!(
                        "flow auth: step {i} response was not valid JSON: {e}"
                    ))
                })?
            };
            for (name, path) in &step.capture {
                match jsonpath_first(&body, path) {
                    Some(v) => {
                        ctx.insert(name.clone(), value_to_string(&v));
                    }
                    None => {
                        return Err(FaucetError::Auth(format!(
                            "flow auth: step {i} capture '{name}' matched nothing at '{path}'"
                        )));
                    }
                }
            }
        }
        Ok(ctx)
    }

    /// Ensure a valid session, returning its captured context (single-flight:
    /// the lock is held across the login network calls).
    async fn ensure_ctx(&self) -> Result<HashMap<String, String>, FaucetError> {
        let mut guard = self.state.lock().await;
        if let Some(s) = guard.as_ref()
            && s.valid()
        {
            return Ok(s.ctx.clone());
        }
        let ctx = self.run_login().await?;
        let expires_at = self
            .config
            .ttl_secs
            .map(|s| Instant::now() + Duration::from_secs(s));
        *guard = Some(Session {
            ctx: ctx.clone(),
            expires_at,
        });
        Ok(ctx)
    }

    fn request_auth_from_ctx(&self, captured: &HashMap<String, String>) -> RequestAuth {
        let mut ctx = captured.clone();
        ctx.extend(Self::clock_ctx());
        build_request_auth(&self.config.apply, &self.config.base_url_from, &ctx)
    }
}

#[async_trait]
impl AuthProvider for FlowProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let ctx = self.ensure_ctx().await?;
        let auth = self.request_auth_from_ctx(&ctx);
        // Header/bearer fallback for connectors that only consume `credential()`
        // (xml, graphql): return the first header placement as a header
        // credential. Query/cookie/body placements need `request_auth`.
        for p in &auth.placements {
            if let CredentialPlacement::Header { name, value } = p {
                return Ok(Credential::Header {
                    name: name.clone(),
                    value: value.clone(),
                });
            }
        }
        Err(FaucetError::Auth(
            "flow auth: no header credential to apply via credential(); this flow places its \
             credential in a query/cookie/body, which requires a connector that consumes \
             request_auth() (e.g. the REST source)"
                .to_owned(),
        ))
    }

    async fn invalidate(&self, _stale: &Credential) -> Result<Credential, FaucetError> {
        // Force a re-login on the next ensure_ctx.
        *self.state.lock().await = None;
        self.credential().await
    }

    async fn request_auth(
        &self,
        _method: &str,
        _url: &str,
        _query: &std::collections::BTreeMap<String, String>,
    ) -> Result<RequestAuth, FaucetError> {
        let ctx = self.ensure_ctx().await?;
        Ok(self.request_auth_from_ctx(&ctx))
    }

    fn reauth_statuses(&self) -> &[u16] {
        &self.config.reauth_on
    }

    fn provider_name(&self) -> &'static str {
        "flow"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn render_substitutes_known_and_leaves_unknown() {
        let c = ctx(&[("token", "abc"), ("region", "eu")]);
        assert_eq!(render("Bearer ${token}", &c), "Bearer abc");
        assert_eq!(render("https://${region}.api", &c), "https://eu.api");
        assert_eq!(render("${missing}", &c), "${missing}");
        assert_eq!(render("no tokens", &c), "no tokens");
        assert_eq!(render("${token}${region}", &c), "abceu");
    }

    #[test]
    fn render_handles_unterminated_and_utf8() {
        let c = ctx(&[("x", "✓")]);
        assert_eq!(render("prefix ${x} ünïcode", &c), "prefix ✓ ünïcode");
        assert_eq!(render("dangling ${x", &c), "dangling ${x");
    }

    #[test]
    fn value_to_string_covers_kinds() {
        assert_eq!(value_to_string(&json!("s")), "s");
        assert_eq!(value_to_string(&json!(7)), "7");
        assert_eq!(value_to_string(&json!(true)), "true");
        assert_eq!(value_to_string(&json!(null)), "");
        assert_eq!(value_to_string(&json!([1, 2])), "[1,2]");
    }

    #[test]
    fn jsonpath_first_extracts_scalar() {
        let body = json!({"data": {"BhRestToken": "tok", "restUrl": "https://host/x"}});
        assert_eq!(
            jsonpath_first(&body, "$.data.BhRestToken"),
            Some(json!("tok"))
        );
        assert_eq!(
            jsonpath_first(&body, "$.data.restUrl"),
            Some(json!("https://host/x"))
        );
        assert_eq!(jsonpath_first(&body, "$.data.missing"), None);
    }

    #[test]
    fn hmac_hex_and_base64_are_deterministic() {
        let hex = hmac_sign("key", "message", SigEncoding::Hex);
        // Known HMAC-SHA256("key","message") hex vector.
        assert_eq!(
            hex,
            "6e9ef29b75fffc5b7abae527d58fdadb2fe42e7219011976917343065f58ed4a"
        );
        let b64 = hmac_sign("key", "message", SigEncoding::Base64);
        assert_eq!(b64, "bp7ym3X//Ft6uuUn1Y/a2y/kLnIZARl2kXNDBl9Y7Uo=");
    }

    #[test]
    fn build_request_auth_places_header_query_cookie_body() {
        let apply: Vec<ApplySpec> = serde_json::from_value(json!([
            { "into": "header", "name": "X-Tok", "value": "${tok}" },
            { "into": "query",  "name": "access_token", "value": "${tok}" },
            { "into": "cookie", "name": "sid", "value": "${sid}" },
            { "into": "body",   "name": "auth", "value": "${tok}" }
        ]))
        .unwrap();
        let c = ctx(&[("tok", "T"), ("sid", "S")]);
        let ra = build_request_auth(&apply, &None, &c);
        assert_eq!(ra.placements.len(), 4);
        assert!(
            matches!(&ra.placements[0], CredentialPlacement::Header { name, value } if name=="X-Tok" && value=="T")
        );
        assert!(
            matches!(&ra.placements[1], CredentialPlacement::Query { name, value } if name=="access_token" && value=="T")
        );
        assert!(
            matches!(&ra.placements[2], CredentialPlacement::Cookie { name, value } if name=="sid" && value=="S")
        );
        assert!(
            matches!(&ra.placements[3], CredentialPlacement::BodyField { name, value } if name=="auth" && value=="T")
        );
        assert!(ra.base_url.is_none());
    }

    #[test]
    fn build_request_auth_signs_and_sets_base_url() {
        let apply: Vec<ApplySpec> = serde_json::from_value(json!([
            { "sign": { "alg": "hmac_sha256", "key": "secret", "template": "${client}:${ts}",
                        "encoding": "hex", "into": { "header": "Authorization", "format": "SS ${sig}" } } }
        ]))
        .unwrap();
        let mut c = ctx(&[
            ("client", "abc"),
            ("ts", "100"),
            ("base_url", "https://eu.host"),
        ]);
        c.insert("base_url".to_owned(), "https://eu.host".to_owned());
        let ra = build_request_auth(&apply, &Some("${base_url}".to_owned()), &c);
        assert_eq!(ra.placements.len(), 1);
        let expected = format!("SS {}", hmac_sign("secret", "abc:100", SigEncoding::Hex));
        assert!(
            matches!(&ra.placements[0], CredentialPlacement::Header { name, value } if name=="Authorization" && *value==expected)
        );
        assert_eq!(ra.base_url.as_deref(), Some("https://eu.host"));
    }

    #[test]
    fn base_url_from_empty_render_is_ignored() {
        let ra = build_request_auth(&[], &Some("${missing_and_stripped}".to_owned()), &ctx(&[]));
        // The unknown token renders verbatim (non-empty), so it is set; an
        // actually-empty render (empty template) is ignored.
        assert!(ra.base_url.is_some());
        let ra2 = build_request_auth(&[], &Some("${e}".to_owned()), &ctx(&[("e", "")]));
        assert!(ra2.base_url.is_none());
    }

    #[test]
    fn config_validate_requires_steps_or_apply() {
        let empty: FlowConfig = serde_json::from_value(json!({})).unwrap();
        assert!(empty.validate().is_err());
    }

    #[test]
    fn config_rejects_form_and_json_together() {
        let cfg: FlowConfig = serde_json::from_value(json!({
            "steps": [ { "request": { "url": "https://x", "form": {"a":"b"}, "json": {"c":"d"} } } ]
        }))
        .unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_rejects_empty_place_name_and_sign_header() {
        let bad_place: FlowConfig = serde_json::from_value(json!({
            "apply": [ { "into": "query", "name": "", "value": "${x}" } ]
        }))
        .unwrap();
        assert!(bad_place.validate().is_err());

        let bad_sign: FlowConfig = serde_json::from_value(json!({
            "apply": [ { "sign": { "alg": "hmac_sha256", "key": "k", "template": "${ts}",
                                   "into": { "header": "" } } } ]
        }))
        .unwrap();
        assert!(bad_sign.validate().is_err());
    }

    #[test]
    fn from_config_rejects_unknown_field() {
        assert!(FlowProvider::from_config(&json!({ "bogus": 1 })).is_err());
    }

    #[test]
    fn build_provider_dispatches_flow() {
        let p = crate::build_provider(&json!({
            "type": "flow",
            "config": { "apply": [ { "into": "header", "name": "X", "value": "v" } ] }
        }))
        .unwrap();
        assert_eq!(p.provider_name(), "flow");
    }

    #[test]
    fn debug_redacts_and_summarizes() {
        let p = FlowProvider::from_config(&json!({
            "apply": [ { "into": "header", "name": "X", "value": "${t}" } ]
        }))
        .unwrap();
        let s = format!("{p:?}");
        assert!(s.contains("FlowProvider"));
        assert!(s.contains("apply"));
    }

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    #[tokio::test]
    async fn login_chain_captures_placements_and_base_url() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"access_token": "AT"})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/login"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    json!({"BhRestToken": "BRT", "restUrl": "https://data.example"}),
                ),
            )
            .mount(&server)
            .await;

        let cfg = json!({
            "steps": [
                { "request": { "method": "POST", "url": format!("{}/token", server.uri()),
                               "form": {"grant_type": "refresh_token"} },
                  "capture": { "access_token": "$.access_token" } },
                { "request": { "method": "GET", "url": format!("{}/login", server.uri()),
                               "query": {"access_token": "${access_token}"} },
                  "capture": { "bh_rest_token": "$.BhRestToken", "base_url": "$.restUrl" } }
            ],
            "apply": [ { "into": "query", "name": "BhRestToken", "value": "${bh_rest_token}" } ],
            "base_url_from": "${base_url}",
            "reauth_on": [401]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let ra = p
            .request_auth("GET", "https://data.example/x", &BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(ra.base_url.as_deref(), Some("https://data.example"));
        assert!(
            matches!(&ra.placements[0], CredentialPlacement::Query { name, value } if name == "BhRestToken" && value == "BRT")
        );
        assert_eq!(p.reauth_statuses(), &[401]);
        // A query placement has no header credential to apply via credential().
        assert!(p.credential().await.is_err());
    }

    struct CountingLogin(Arc<AtomicUsize>);
    impl Respond for CountingLogin {
        fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
            let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            ResponseTemplate::new(200).set_body_json(json!({ "sid": format!("S{n}") }))
        }
    }

    #[tokio::test]
    async fn header_credential_caches_then_reloads_on_invalidate() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(CountingLogin(hits.clone()))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "method": "POST", "url": format!("{}/login", server.uri()) },
                         "capture": { "sid": "$.sid" } } ],
            "apply": [ { "into": "header", "name": "X-Session", "value": "${sid}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();

        let c1 = p.credential().await.unwrap();
        assert!(
            matches!(&c1, Credential::Header { name, value } if name == "X-Session" && value == "S1")
        );
        // Cached: no ttl → second call does not re-login.
        let _ = p.credential().await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        // invalidate forces a fresh login.
        let c2 = p.invalidate(&c1).await.unwrap();
        assert!(matches!(&c2, Credential::Header { value, .. } if value == "S2"));
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn login_capture_miss_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/x"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"other": 1})))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "url": format!("{}/x", server.uri()) },
                         "capture": { "tok": "$.access_token" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "${tok}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(p.credential().await.is_err());
    }

    #[tokio::test]
    async fn login_non_success_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/x"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "url": format!("{}/x", server.uri()) } } ],
            "apply": [ { "into": "header", "name": "X", "value": "static" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(
            p.request_auth("GET", "https://x", &BTreeMap::new())
                .await
                .is_err()
        );
    }

    #[test]
    fn config_rejects_empty_step_url() {
        let cfg: FlowConfig = serde_json::from_value(json!({
            "steps": [ { "request": { "url": "  " } } ]
        }))
        .unwrap();
        assert!(cfg.validate().is_err());
    }

    #[tokio::test]
    async fn session_ttl_caches_within_window() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(CountingLogin(hits.clone()))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "method": "POST", "url": format!("{}/login", server.uri()) },
                         "capture": { "sid": "$.sid" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "${sid}" } ],
            "ttl_secs": 3600
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let _ = p.credential().await.unwrap();
        // Within the TTL window the session is reused (Session::valid == true).
        let _ = p.credential().await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn login_sends_headers_and_json_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/login"))
            .and(header("x-tenant", "acme"))
            .and(body_json(json!({"scope": "read"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"tok": "T"})))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "method": "POST", "url": format!("{}/login", server.uri()),
                         "headers": {"X-Tenant": "acme"}, "json": {"scope": "read"} },
                         "capture": { "t": "$.tok" } } ],
            "apply": [ { "into": "header", "name": "Authorization", "value": "Bearer ${t}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert_eq!(p.provider_name(), "flow");
        let c = p.credential().await.unwrap();
        assert!(
            matches!(&c, Credential::Header { name, value } if name == "Authorization" && value == "Bearer T")
        );
    }

    #[tokio::test]
    async fn login_invalid_method_errors() {
        let cfg = json!({
            "steps": [ { "request": { "method": "BAD METHOD", "url": "https://x/login" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "static" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(
            p.request_auth("GET", "https://x", &BTreeMap::new())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn login_send_failure_errors() {
        // Port 1 is unassignable → the send fails at the transport layer.
        let cfg = json!({
            "steps": [ { "request": { "url": "http://127.0.0.1:1/x" }, "capture": { "t": "$.t" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "${t}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(
            p.request_auth("GET", "https://x", &BTreeMap::new())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn login_non_json_response_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/x"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "url": format!("{}/x", server.uri()) }, "capture": { "t": "$.t" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "${t}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(
            p.request_auth("GET", "https://x", &BTreeMap::new())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn login_step_with_empty_capture_succeeds() {
        // A pre-flight step that captures nothing (e.g. establishes a cookie);
        // its non-JSON body is never parsed.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ping"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"t": "T"})))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [
                { "request": { "url": format!("{}/ping", server.uri()) } },
                { "request": { "url": format!("{}/token", server.uri()) }, "capture": { "t": "$.t" } }
            ],
            "apply": [ { "into": "header", "name": "X", "value": "${t}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let c = p.credential().await.unwrap();
        assert!(matches!(&c, Credential::Header { value, .. } if value == "T"));
    }

    #[tokio::test]
    async fn no_steps_apply_only_flow_works() {
        // A flow with only `apply` (a static signer/placement, no login) needs
        // no network.
        let cfg = json!({
            "apply": [ { "sign": { "alg": "hmac_sha256", "key": "k", "template": "msg",
                                   "into": { "header": "Authorization", "format": "HMAC ${sig}" } } } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let c = p.credential().await.unwrap();
        let expected = format!("HMAC {}", hmac_sign("k", "msg", SigEncoding::Hex));
        assert!(
            matches!(&c, Credential::Header { name, value } if name == "Authorization" && *value == expected)
        );
    }
}
