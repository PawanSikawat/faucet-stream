//! #465 Part 3 — the docs-site connector capability matrix is *generated* from
//! the registry allowlists (`faucet conformance --matrix`), not hand-maintained.
//! This test fails if the committed copy drifts from what the generator emits,
//! so a change to an allowlist forces regenerating the doc in the same PR.

/// The committed matrix must be byte-identical to the generator's output.
/// Feature-independent: the generator reads only the `const` allowlists, so this
/// holds under any feature set.
#[test]
fn committed_matrix_matches_generated() {
    let generated = faucet_cli::conformance::capability_matrix_markdown();
    let committed = include_str!("../../docs/book/src/reference/capability-matrix.md");
    assert_eq!(
        committed, generated,
        "docs/book/src/reference/capability-matrix.md is stale — regenerate it:\n  \
         cargo run -p faucet-cli -- conformance --matrix > \
         docs/book/src/reference/capability-matrix.md"
    );
}
