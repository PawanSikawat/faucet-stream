//! CLI test for `faucet schema triggers`. Gated on the `triggers` feature so
//! non-triggers builds skip it.
#![cfg(feature = "triggers")]

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn schema_triggers_prints_json_schema() {
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "triggers"])
        .assert()
        .success()
        .stdout(
            predicates::str::contains("object_arrival")
                .and(predicates::str::contains("webhook")),
        );
}
