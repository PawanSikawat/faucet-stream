//! Page number pagination.

use std::collections::HashMap;

pub fn apply_params(
    params: &mut HashMap<String, String>,
    param_name: &str,
    start_page: usize,
    current_page: usize,
    page_size: Option<usize>,
    page_size_param: Option<&str>,
) {
    params.insert(
        param_name.to_string(),
        (start_page + current_page).to_string(),
    );
    if let (Some(size), Some(size_param)) = (page_size, page_size_param) {
        params.insert(size_param.to_string(), size.to_string());
    }
}
