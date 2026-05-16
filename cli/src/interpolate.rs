//! `${env:VAR}` / `${file:PATH}` interpolation.
//!
//! Runs over the raw config text *before* JSON/YAML parsing so the resolved
//! values can be substituted into any string value — including ones that
//! become structured types after parsing (numbers, durations, paths).
//!
//! Supported directives:
//!
//! | Form               | Resolves to |
//! |--------------------|-------------|
//! | `${env:VAR}`       | the value of environment variable `VAR` |
//! | `${file:PATH}`     | the contents of the file at `PATH` (trimmed of trailing whitespace) |
//! | `${secret:VAR}`    | reserved — currently aliased to `${env:VAR}`. A future secrets backend will own this prefix. |
//!
//! Escapes: a literal `${` can be written as `$${` and is decoded by the
//! interpolator. Anything that doesn't match `${prefix:body}` is left as-is.

use crate::error::{CliError, CliResult};
use std::path::PathBuf;

/// Resolve every `${prefix:body}` token in `input`. Returns the substituted
/// string or the first error encountered.
pub fn interpolate(input: &str) -> CliResult<String> {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Escape: $${ → ${
        if bytes[i] == b'$' && i + 2 < bytes.len() && bytes[i + 1] == b'$' && bytes[i + 2] == b'{' {
            out.push('$');
            i += 2;
            continue;
        }
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            // Locate matching `}` — interpolations cannot nest.
            let start = i + 2;
            let Some(rel_end) = input[start..].find('}') else {
                // No closing brace → emit literally and stop scanning for
                // directives so we don't quietly swallow content.
                out.push_str(&input[i..]);
                break;
            };
            let end = start + rel_end;
            let directive = &input[start..end];
            let resolved = resolve_directive(directive)?;
            out.push_str(&resolved);
            i = end + 1;
            continue;
        }
        out.push(input[i..].chars().next().unwrap());
        i += input[i..].chars().next().unwrap().len_utf8();
    }
    Ok(out)
}

fn resolve_directive(directive: &str) -> CliResult<String> {
    let (prefix, body) =
        directive
            .split_once(':')
            .ok_or_else(|| CliError::UnknownInterpolationPrefix {
                prefix: directive.to_owned(),
                full: format!("${{{directive}}}"),
            })?;
    match prefix {
        "env" | "secret" => std::env::var(body).map_err(|_| CliError::MissingEnvVar {
            var: body.to_owned(),
            location: format!("${{{directive}}}"),
        }),
        "file" => {
            let path = PathBuf::from(body);
            let bytes = std::fs::read(&path).map_err(|source| CliError::ReadInterpolatedFile {
                path: path.clone(),
                source,
            })?;
            let text = String::from_utf8_lossy(&bytes);
            Ok(text.trim_end().to_owned())
        }
        other => Err(CliError::UnknownInterpolationPrefix {
            prefix: other.to_owned(),
            full: format!("${{{directive}}}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passes_through_text_with_no_directives() {
        let out = interpolate("just a string").unwrap();
        assert_eq!(out, "just a string");
    }

    #[test]
    fn substitutes_env_var() {
        // SAFETY: tests run sequentially within this module via tokio's default
        // single-thread runtime when used with #[tokio::test], and these
        // synchronous tests don't share env state across cases.
        unsafe { std::env::set_var("FAUCET_TEST_VAR", "hello") };
        let out = interpolate("token=${env:FAUCET_TEST_VAR}").unwrap();
        assert_eq!(out, "token=hello");
        unsafe { std::env::remove_var("FAUCET_TEST_VAR") };
    }

    #[test]
    fn missing_env_var_is_an_error() {
        unsafe { std::env::remove_var("FAUCET_TEST_MISSING") };
        let err = interpolate("token=${env:FAUCET_TEST_MISSING}").unwrap_err();
        match err {
            CliError::MissingEnvVar { var, .. } => assert_eq!(var, "FAUCET_TEST_MISSING"),
            other => panic!("expected MissingEnvVar, got {other:?}"),
        }
    }

    #[test]
    fn secret_prefix_is_env_alias_for_now() {
        unsafe { std::env::set_var("FAUCET_SECRET_VAR", "shh") };
        let out = interpolate("${secret:FAUCET_SECRET_VAR}").unwrap();
        assert_eq!(out, "shh");
        unsafe { std::env::remove_var("FAUCET_SECRET_VAR") };
    }

    #[test]
    fn reads_file_directive_and_trims_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token.txt");
        std::fs::write(&path, "abcdef\n").unwrap();
        let raw = format!("token=${{file:{}}}", path.display());
        let out = interpolate(&raw).unwrap();
        assert_eq!(out, "token=abcdef");
    }

    #[test]
    fn unknown_prefix_is_reported() {
        let err = interpolate("${weird:thing}").unwrap_err();
        match err {
            CliError::UnknownInterpolationPrefix { prefix, .. } => assert_eq!(prefix, "weird"),
            other => panic!("expected UnknownInterpolationPrefix, got {other:?}"),
        }
    }

    #[test]
    fn dollar_dollar_brace_is_escaped() {
        // `$${env:VAR}` should pass through as `${env:VAR}` without resolving.
        let out = interpolate("path=$${env:VAR}").unwrap();
        assert_eq!(out, "path=${env:VAR}");
    }

    #[test]
    fn unclosed_directive_is_left_literal() {
        let out = interpolate("hello ${env:NOPE").unwrap();
        assert_eq!(out, "hello ${env:NOPE");
    }

    #[test]
    fn multiple_directives_resolve_in_order() {
        unsafe { std::env::set_var("FAUCET_A", "one") };
        unsafe { std::env::set_var("FAUCET_B", "two") };
        let out = interpolate("${env:FAUCET_A}-${env:FAUCET_B}").unwrap();
        assert_eq!(out, "one-two");
        unsafe { std::env::remove_var("FAUCET_A") };
        unsafe { std::env::remove_var("FAUCET_B") };
    }
}
