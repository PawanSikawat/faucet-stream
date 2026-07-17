//! Pagination strategies for REST APIs.

pub mod cursor;
pub mod link_header;
pub mod next_link_body;
pub mod offset;
pub mod page;

use faucet_core::FaucetError;
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Supported pagination strategies.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum PaginationStyle {
    None,
    Cursor {
        next_token_path: String,
        param_name: String,
    },
    LinkHeader,
    /// The full URL of the next page is embedded in the response body.
    /// `next_link_path` is a JSONPath expression pointing to that URL field
    /// (e.g. `"$.next_link"`).  Pagination stops when the field is absent,
    /// null, or an empty string.
    NextLinkInBody {
        next_link_path: String,
    },
    PageNumber {
        param_name: String,
        start_page: usize,
        page_size: Option<usize>,
        page_size_param: Option<String>,
    },
    Offset {
        offset_param: String,
        limit_param: String,
        limit: usize,
        total_path: Option<String>,
    },
}

/// Internal state tracked across pages.
#[derive(Debug, Default)]
pub struct PaginationState {
    pub page: usize,
    pub next_token: Option<String>,
    pub offset: usize,
    pub next_link: Option<String>,
    /// The previous page's token/link, used for loop detection.
    /// If `advance()` produces the same value twice in a row, pagination
    /// is stuck and we stop rather than looping forever.
    #[doc(hidden)]
    pub previous_token: Option<String>,
    /// Fingerprint of the previous page's body, used by `PageNumber` loop
    /// detection: APIs that clamp an out-of-range page to the last page and
    /// re-return it (non-empty) would otherwise loop until `max_pages`.
    #[doc(hidden)]
    pub previous_page_fingerprint: Option<u64>,
    /// Set by [`PaginationStyle::advance`] when the body-fingerprint stagnation
    /// guard fires: the page just handed to `advance` is a duplicate of the
    /// previous one and the caller must **drop** it rather than emit it a second
    /// time (audit #321 L1). Only the content-stagnation guards set it; a normal
    /// last-page stop leaves it `false` so the final page is still emitted.
    #[doc(hidden)]
    pub current_page_is_duplicate: bool,
}

/// Cheap, stable fingerprint of a response body for content-stagnation
/// loop detection.
fn body_fingerprint(body: &Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    body.to_string().hash(&mut h);
    h.finish()
}

impl PaginationStyle {
    pub fn apply_params(&self, params: &mut HashMap<String, String>, state: &PaginationState) {
        match self {
            PaginationStyle::None => {}
            PaginationStyle::Cursor { param_name, .. } => {
                cursor::apply_params(params, param_name, &state.next_token);
            }
            PaginationStyle::LinkHeader => {}
            PaginationStyle::NextLinkInBody { .. } => {}
            PaginationStyle::PageNumber {
                param_name,
                start_page,
                page_size,
                page_size_param,
            } => {
                page::apply_params(
                    params,
                    param_name,
                    *start_page,
                    state.page,
                    *page_size,
                    page_size_param.as_deref(),
                );
            }
            PaginationStyle::Offset {
                offset_param,
                limit_param,
                limit,
                ..
            } => {
                offset::apply_params(params, offset_param, limit_param, state.offset, *limit);
            }
        }
    }

    /// Advance pagination state based on the response body and headers.
    /// Returns `true` if there is a next page to fetch.
    ///
    /// Includes **loop detection**: if a cursor or next-link value is identical
    /// to the previous page's value, pagination stops with a warning instead of
    /// looping forever.
    pub fn advance(
        &self,
        body: &Value,
        headers: &HeaderMap,
        state: &mut PaginationState,
        record_count: usize,
    ) -> Result<bool, FaucetError> {
        match self {
            PaginationStyle::None => Ok(false),
            PaginationStyle::Cursor {
                next_token_path, ..
            } => {
                let has_next = cursor::advance(body, next_token_path, &mut state.next_token)?;
                if has_next {
                    if state.next_token == state.previous_token {
                        tracing::warn!(
                            "pagination loop detected: cursor {:?} repeated — stopping",
                            state.next_token
                        );
                        return Ok(false);
                    }
                    state.previous_token = state.next_token.clone();
                }
                Ok(has_next)
            }
            PaginationStyle::LinkHeader => match link_header::extract_next_link(headers) {
                Some(link) => {
                    if Some(&link) == state.previous_token.as_ref() {
                        tracing::warn!(
                            "pagination loop detected: link {link:?} repeated — stopping"
                        );
                        state.next_link = None;
                        return Ok(false);
                    }
                    state.previous_token = Some(link.clone());
                    state.next_link = Some(link);
                    Ok(true)
                }
                None => {
                    state.next_link = None;
                    Ok(false)
                }
            },
            PaginationStyle::NextLinkInBody { next_link_path } => {
                let has_next = next_link_body::advance(body, next_link_path, &mut state.next_link)?;
                if has_next {
                    if state.next_link == state.previous_token {
                        tracing::warn!(
                            "pagination loop detected: next_link {:?} repeated — stopping",
                            state.next_link
                        );
                        return Ok(false);
                    }
                    state.previous_token = state.next_link.clone();
                }
                Ok(has_next)
            }
            PaginationStyle::PageNumber { .. } => {
                state.page += 1;
                if record_count == 0 {
                    return Ok(false);
                }
                // Content-stagnation guard: some APIs clamp an out-of-range
                // page to the last page and return it again (non-empty), which
                // would loop until `max_pages` and duplicate records. Stop if
                // this page's body is identical to the previous one (#78/#15).
                let fp = body_fingerprint(body);
                if state.previous_page_fingerprint == Some(fp) {
                    tracing::warn!(
                        "pagination loop detected: PageNumber returned an identical page — stopping"
                    );
                    // The current page IS the duplicate — signal the caller to
                    // drop it rather than emit it a second time (#321 L1).
                    state.current_page_is_duplicate = true;
                    return Ok(false);
                }
                state.previous_page_fingerprint = Some(fp);
                Ok(true)
            }
            PaginationStyle::Offset {
                limit, total_path, ..
            } => {
                let has_next = offset::advance(
                    body,
                    &mut state.offset,
                    record_count,
                    *limit,
                    total_path.as_deref(),
                )?;
                // Content-stagnation guard (#264 F18): a server that ignores
                // the `offset` parameter re-returns the identical first page
                // forever. With `total_path` absent (commonly omitted) the
                // record-count heuristic keeps `has_next` true on every full
                // page, so the run would loop until `max_pages`, duplicating
                // records to the sink. Mirror the PageNumber guard: stop if
                // this page's body is identical to the previous one. A
                // zero-record / short page has already returned `false` above,
                // so this only fires on a genuinely repeated full page.
                //
                // Scoped to `total_path.is_none()`: when `total_path` is set,
                // `offset::advance` has an authoritative stop condition (offset
                // reaches total), and a paging-metadata body that legitimately
                // repeats (e.g. `{"total": N}` echoed on every page) must not
                // be mistaken for stagnation.
                if has_next && total_path.is_none() {
                    let fp = body_fingerprint(body);
                    if state.previous_page_fingerprint == Some(fp) {
                        tracing::warn!(
                            "pagination loop detected: Offset returned an identical page \
                             (server likely ignoring the offset parameter) — stopping"
                        );
                        // Drop this duplicate page rather than emit it (#321 L1).
                        state.current_page_is_duplicate = true;
                        return Ok(false);
                    }
                    state.previous_page_fingerprint = Some(fp);
                }
                Ok(has_next)
            }
        }
    }
}
