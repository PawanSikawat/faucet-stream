//! Pagination strategies for REST APIs.

pub mod cursor;
pub mod link_header;
pub mod next_link_body;
pub mod offset;
pub mod page;

use crate::error::FaucetError;
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::collections::HashMap;

/// Supported pagination strategies.
#[derive(Debug, Clone)]
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
                Ok(record_count > 0)
            }
            PaginationStyle::Offset {
                limit, total_path, ..
            } => offset::advance(
                body,
                &mut state.offset,
                record_count,
                *limit,
                total_path.as_deref(),
            ),
        }
    }
}
