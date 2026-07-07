//! `GET /v1/catalog/*` — browse the Data Movement Catalog (#279): the
//! accumulated cross-run picture of every dataset the server's pipelines have
//! touched. Read-only; all three routes require the `CatalogRead` permission
//! (granted to every role, `viewer` up), enforced by the auth middleware.

use crate::serve::error::ServeError;
use crate::serve::history::catalog::{
    CatalogDatasetDetail, CatalogDatasetPage, CatalogLineageEdge, CatalogListFilter,
    LINEAGE_DEFAULT_DEPTH,
};
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Path, Query, State};
use serde::{Deserialize, Serialize};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const MAX_DEPTH: u32 = 32;

/// `GET /v1/catalog/datasets` query string.
#[derive(Debug, Deserialize)]
pub struct DatasetsQuery {
    /// Exact connector-kind filter (`csv`, `postgres`, …).
    pub kind: Option<String>,
    /// Case-insensitive substring match on the dataset URI.
    pub q: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

/// `GET /v1/catalog/datasets` → 200.
pub async fn list_datasets(
    State(state): State<ServerState>,
    Query(query): Query<DatasetsQuery>,
) -> Result<Json<CatalogDatasetPage>, ServeError> {
    let filter = CatalogListFilter {
        kind: query.kind,
        q: query.q,
        limit: query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
        cursor: query.cursor,
    };
    let page = state
        .history()
        .catalog_list_datasets(&filter)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    Ok(Json(page))
}

/// `GET /v1/catalog/datasets/{id}` → 200 / 404.
pub async fn get_dataset(
    State(state): State<ServerState>,
    Path(id): Path<String>,
) -> Result<Json<CatalogDatasetDetail>, ServeError> {
    let detail = state
        .history()
        .catalog_get_dataset(&id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    Ok(Json(detail))
}

/// `GET /v1/catalog/lineage` query string.
#[derive(Debug, Deserialize)]
pub struct LineageQuery {
    /// Dataset id to root the graph at; omitted = the whole graph.
    pub root: Option<String>,
    /// BFS hop bound around `root` (ignored without one).
    pub depth: Option<u32>,
}

/// `GET /v1/catalog/lineage` response body.
#[derive(Debug, Serialize)]
pub struct LineageResponse {
    pub edges: Vec<CatalogLineageEdge>,
}

/// `GET /v1/catalog/lineage` → 200.
pub async fn lineage(
    State(state): State<ServerState>,
    Query(query): Query<LineageQuery>,
) -> Result<Json<LineageResponse>, ServeError> {
    let depth = query
        .depth
        .unwrap_or(LINEAGE_DEFAULT_DEPTH)
        .clamp(1, MAX_DEPTH);
    let edges = state
        .history()
        .catalog_lineage(query.root.as_deref(), depth)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    Ok(Json(LineageResponse { edges }))
}
