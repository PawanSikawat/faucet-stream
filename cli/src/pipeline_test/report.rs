//! Rendering for `faucet test` results — the human checklist and the
//! machine-readable `--json` document.

use serde::Serialize;

/// One test case's final result.
#[derive(Debug, Serialize)]
pub struct CaseOutcome {
    /// Case name from the spec file.
    pub name: String,
    /// Spec file the case came from.
    pub spec: String,
    /// `"pass"` or `"fail"`.
    pub status: &'static str,
    /// One message per failed assertion (empty on pass).
    pub failures: Vec<String>,
}

impl CaseOutcome {
    pub fn new(name: String, spec: String, failures: Vec<String>) -> Self {
        Self {
            name,
            spec,
            status: if failures.is_empty() { "pass" } else { "fail" },
            failures,
        }
    }

    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }
}

/// The full report across every spec file.
#[derive(Debug, Serialize)]
pub struct TestReport {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub tests: Vec<CaseOutcome>,
}

impl TestReport {
    pub fn new(tests: Vec<CaseOutcome>) -> Self {
        let total = tests.len();
        let passed = tests.iter().filter(|t| t.passed()).count();
        Self {
            total,
            passed,
            failed: total - passed,
            tests,
        }
    }

    /// Render the human checklist, grouped by spec file in declared order.
    pub fn render_human(&self) -> String {
        let mut out = String::new();
        let mut current_spec: Option<&str> = None;
        for case in &self.tests {
            if current_spec != Some(case.spec.as_str()) {
                if current_spec.is_some() {
                    out.push('\n');
                }
                out.push_str(&case.spec);
                out.push('\n');
                current_spec = Some(case.spec.as_str());
            }
            if case.passed() {
                out.push_str(&format!("  ✓ {}\n", case.name));
            } else {
                out.push_str(&format!("  ✗ {}\n", case.name));
                for failure in &case.failures {
                    out.push_str(&format!("      - {failure}\n"));
                }
            }
        }
        out.push_str(&format!(
            "\n{} test{}, {} passed, {} failed\n",
            self.total,
            if self.total == 1 { "" } else { "s" },
            self.passed,
            self.failed
        ));
        out
    }

    /// Render the `--json` document.
    pub fn render_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("report serialization is infallible")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> TestReport {
        TestReport::new(vec![
            CaseOutcome::new("ok case".into(), "a.yaml".into(), vec![]),
            CaseOutcome::new(
                "bad case".into(),
                "a.yaml".into(),
                vec!["records[0].x: expected 1, got 2".into()],
            ),
            CaseOutcome::new("other".into(), "b.yaml".into(), vec![]),
        ])
    }

    #[test]
    fn counts_and_status() {
        let r = report();
        assert_eq!((r.total, r.passed, r.failed), (3, 2, 1));
        assert_eq!(r.tests[0].status, "pass");
        assert_eq!(r.tests[1].status, "fail");
    }

    #[test]
    fn human_rendering_groups_by_spec() {
        let text = report().render_human();
        assert!(
            text.contains("a.yaml\n  ✓ ok case\n  ✗ bad case\n"),
            "{text}"
        );
        assert!(
            text.contains("      - records[0].x: expected 1, got 2"),
            "{text}"
        );
        assert!(text.contains("b.yaml\n  ✓ other"), "{text}");
        assert!(text.contains("3 tests, 2 passed, 1 failed"), "{text}");
    }

    #[test]
    fn singular_summary_line() {
        let r = TestReport::new(vec![CaseOutcome::new(
            "one".into(),
            "s.yaml".into(),
            vec![],
        )]);
        assert!(r.render_human().contains("1 test, 1 passed, 0 failed"));
    }

    #[test]
    fn json_rendering_is_machine_readable() {
        let v: serde_json::Value = serde_json::from_str(&report().render_json()).unwrap();
        assert_eq!(v["total"], 3);
        assert_eq!(v["tests"][1]["status"], "fail");
        assert_eq!(
            v["tests"][1]["failures"][0],
            "records[0].x: expected 1, got 2"
        );
    }
}
