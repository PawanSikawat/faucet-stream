//! `/mcp` HTTP route for `faucet serve --mcp` (issue #420).
//!
//! A Streamable-HTTP MCP transport: one JSON-RPC 2.0 request per POST, the
//! response returned as the body. The route is mounted inside the
//! bearer-auth/RBAC `route_layer`, so every call is authenticated and lands in
//! the audit log like any other control-plane action. Mutating tools
//! additionally require the caller to hold the `RunWrite` scope — ANDed with
//! the server's `--mcp-allow-mutations` flag — so a `Viewer` token can never
//! mutate even on a mutation-enabled server.

use crate::serve::rbac::{AuthContext, Permission};
use crate::serve::state::ServerState;
use axum::Extension;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;

/// Per-route MCP flags injected in `build_router`.
#[derive(Clone, Copy)]
pub struct McpRouteFlags {
    pub allow_mutations: bool,
}

pub async fn handle(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Extension(flags): Extension<McpRouteFlags>,
    body: String,
) -> axum::response::Response {
    // Mutating tools require BOTH the server flag and the caller's RunWrite scope.
    let can_mutate = flags.allow_mutations && actor.role.grants(Permission::RunWrite);

    let auth = match crate::auth_catalog::build_auth_catalog(None) {
        Ok(a) => a,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to build auth catalog: {e}"),
            )
                .into_response();
        }
    };
    // The server's run-history backend doubles as the pipeline-template registry
    // (#444), so an agent on `/mcp` sees the same templates the `/v1/templates`
    // endpoints and `faucet template` do.
    let ctx = crate::mcp::McpContext::new(auth, can_mutate);
    #[cfg(feature = "templates")]
    let ctx = ctx.with_templates(state.history());
    let response = crate::mcp::handle_message(&ctx, &body).await;

    // Best-effort audit: record the MCP call under the caller's principal/role.
    crate::serve::audit::write(&state, &actor, "mcp", None, None, "ok").await;

    match response {
        Some(json) => ([(header::CONTENT_TYPE, "application/json")], json).into_response(),
        // A notification (no id) gets no body.
        None => StatusCode::ACCEPTED.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::AuditFilter;
    use crate::serve::rbac::Role;
    use crate::serve::test_support::test_state;

    fn actor(role: Role) -> AuthContext {
        AuthContext {
            principal: "tester".into(),
            role,
            source_ip: None,
        }
    }

    async fn body_text(resp: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn admin_with_flag_sees_run_pipeline() {
        let state = test_state();
        let resp = handle(
            State(state),
            Extension(actor(Role::Admin)),
            Extension(McpRouteFlags {
                allow_mutations: true,
            }),
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#.to_string(),
        )
        .await;
        assert!(body_text(resp).await.contains("run_pipeline"));
    }

    #[tokio::test]
    async fn viewer_cannot_see_run_pipeline_even_with_flag() {
        // RBAC gate: a Viewer lacks RunWrite, so the mutating tool stays hidden
        // even on a mutation-enabled server.
        let state = test_state();
        let resp = handle(
            State(state),
            Extension(actor(Role::Viewer)),
            Extension(McpRouteFlags {
                allow_mutations: true,
            }),
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#.to_string(),
        )
        .await;
        assert!(!body_text(resp).await.contains("run_pipeline"));
    }

    #[tokio::test]
    async fn flag_off_hides_run_pipeline_for_admin() {
        let state = test_state();
        let resp = handle(
            State(state),
            Extension(actor(Role::Admin)),
            Extension(McpRouteFlags {
                allow_mutations: false,
            }),
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#.to_string(),
        )
        .await;
        assert!(!body_text(resp).await.contains("run_pipeline"));
    }

    #[tokio::test]
    async fn tools_call_is_dispatched_and_audited() {
        let state = test_state();
        let resp = handle(
            State(state.clone()),
            Extension(actor(Role::Viewer)),
            Extension(McpRouteFlags {
                allow_mutations: false,
            }),
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_connectors","arguments":{"kind":"state"}}}"#.to_string(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_text(resp).await;
        assert!(body.contains("state_stores"));

        // The call was recorded in the audit log under action "mcp".
        let entries = state
            .history()
            .list_audit(&AuditFilter {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            entries
                .iter()
                .any(|e| e.action == "mcp" && e.principal == "tester")
        );
    }

    #[tokio::test]
    async fn notification_returns_202() {
        let state = test_state();
        let resp = handle(
            State(state),
            Extension(actor(Role::Admin)),
            Extension(McpRouteFlags {
                allow_mutations: false,
            }),
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#.to_string(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }
}
