//! Helper to derive a friendly connector label from `std::any::type_name`.

/// Strip a Rust `type_name` string to its final path segment.
///
/// Examples (see tests):
/// - `"faucet_source_rest::stream::RestSource"` → `"RestSource"`
/// - `"my_crate::nested::module::MyConnector"` → `"MyConnector"`
/// - `"Foo"` → `"Foo"`
/// - `""` → `"unknown"`
/// - Generics (`"Foo<Bar>"`) keep the outer name: → `"Foo<Bar>"`
pub fn strip_type_name(s: &'static str) -> &'static str {
    if s.is_empty() {
        return "unknown";
    }
    match s.rsplit_once("::") {
        Some((_, tail)) => tail,
        None => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_nested_path() {
        assert_eq!(
            strip_type_name("faucet_source_rest::stream::RestSource"),
            "RestSource"
        );
    }

    #[test]
    fn strips_deeply_nested_path() {
        assert_eq!(strip_type_name("my_crate::a::b::c::Inner"), "Inner");
    }

    #[test]
    fn returns_unchanged_when_no_separator() {
        assert_eq!(strip_type_name("Foo"), "Foo");
    }

    #[test]
    fn empty_returns_unknown_sentinel() {
        assert_eq!(strip_type_name(""), "unknown");
    }

    #[test]
    fn preserves_generic_arguments() {
        assert_eq!(strip_type_name("Foo<Bar>"), "Foo<Bar>");
    }

    #[test]
    fn preserves_generics_after_path_strip() {
        assert_eq!(strip_type_name("crate::Foo<Bar>"), "Foo<Bar>");
    }
}
