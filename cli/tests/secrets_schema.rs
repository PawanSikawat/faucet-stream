use assert_cmd::Command;
use predicates::str::contains;

#[test]
fn schema_secrets_lists_all_four_schemes() {
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "secrets"])
        .assert()
        .success()
        .stdout(contains("vault"))
        .stdout(contains("aws-sm"))
        .stdout(contains("gcp-sm"))
        .stdout(contains("azure-kv"));
}
