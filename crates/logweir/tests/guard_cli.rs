use std::process::Command;

fn run_with_spec(spec: &str) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("drill.yaml");
    std::fs::write(&p, spec).unwrap();
    Command::new(env!("CARGO_BIN_EXE_logweir"))
        .args(["drill", "run", "--spec"])
        .arg(&p)
        .args([
            "--approval",
            "../../examples/approval.json",
            "--approver-key",
            "../../e2e/fixtures/signed/public.pem",
            "--allowed-clusters",
            "../../examples/allowed-clusters.json",
            "--signing-key",
            "../../e2e/fixtures/signed/signing.pem",
        ])
        .output()
        .unwrap()
}

#[test]
fn purge_topics_in_the_spec_exits_3_at_either_value() {
    for v in ["true", "false"] {
        let spec = std::fs::read_to_string("../../examples/drill.yaml").unwrap()
            + &format!("\nengine_overrides:\n  purge_topics: {v}\n");
        let out = run_with_spec(&spec);
        assert_eq!(out.status.code(), Some(3), "purge_topics: {v}");
        let e = String::from_utf8(out.stderr).unwrap();
        assert!(
            e.contains("purge_topics"),
            "the offending path must be printed: {e}"
        );
    }
}

/// This asserts on the MESSAGE, not the code alone. `phase0_admit::run` performs
/// the two purely-local checks first (step 5 reorders it so), but this test runs
/// with no broker up, so a bare `assert_eq!(code, 3)` would pass even if
/// `check_topic_mapping_coverage` were deleted — a false green on the guard the
/// task exists to prove.
#[test]
fn an_unmapped_selected_topic_exits_3_on_the_mapping_check_not_on_a_dead_broker() {
    let spec = std::fs::read_to_string("../../examples/drill.yaml")
        .unwrap()
        .replace(
            "topic_mapping_prefix: \"drill-\"",
            "topic_mapping_prefix: \"\"",
        );
    let out = run_with_spec(&spec);
    let e = String::from_utf8(out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(3), "{e}");
    assert!(
        e.contains("topic_mapping entry") || e.contains("onto itself"),
        "must fail on the mapping check, not on an unreachable broker: {e}"
    );
}
