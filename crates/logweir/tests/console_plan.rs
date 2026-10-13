//! THE CONSOLE'S PLAN GOLDENS PARSE INTO THE RUNNER'S OWN TYPE (PROD-18.1 R8;
//! carried over from the retired `ui_lint.rs`, rows 18 to 18c and 22).
//!
//! `Restore.spec.planBytes` is opaque to the API server and to the controller,
//! and it is still not arbitrary: the controller writes those bytes VERBATIM
//! into the runner's plan ConfigMap, and the runner parses them with
//! `serde_yaml::from_str::<RestoreSpec>`. An invented shape passes the create,
//! every approval check (the hash matches) and fails only inside the runner
//! pod. A hash comparison cannot see it. So each golden the console renders
//! (`console/src/pages/restores/plan.test.ts` holds the renderer to the same
//! files, byte for byte) is deserialised here into `RestoreSpec`, and the
//! values that decide what the run does are compared with the fields file the
//! golden was rendered from.
//!
//! `RestoreSpec` has no `deny_unknown_fields`, so a golden that spelt a key
//! wrong would still parse into a plan WITHOUT the opt-in: each row asserts
//! its value ARRIVES, and that the plain goldens carry none of it.

use std::path::{Path, PathBuf};

use logweir_core::spec::RestoreSpec;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../console/fixtures")
}

fn read(name: &str) -> String {
    let path = fixtures().join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn spec(golden: &str) -> RestoreSpec {
    serde_yaml::from_str(&read(golden)).unwrap_or_else(|e| {
        panic!(
            "console/fixtures/{golden} does not deserialise into logweir_core::spec::RestoreSpec: \
             {e}. The console's renderPlanBytes emits the runner's document and nothing else; \
             regenerating the golden does not make it correct."
        )
    })
}

fn fields(name: &str) -> serde_json::Value {
    serde_json::from_str(&read(name)).expect("a fields file is JSON")
}

/// The goldens a runner of this tree parses; the clone golden is theme A's (below).
const GOLDENS: [&str; 5] = [
    "plan.golden.yaml",
    "plan-time-basis.golden.yaml",
    "plan-complete.golden.yaml",
    "plan-original-name.golden.yaml",
    "plan-point.golden.yaml",
];

#[test]
fn the_plain_golden_carries_the_wizard_values_and_no_opt_in() {
    let s = spec("plan.golden.yaml");
    let f = fields("plan-fields.json");
    assert_eq!(
        s.target.mode.to_string(),
        f["target"]["mode"].as_str().unwrap()
    );
    assert_eq!(
        s.target.topic_naming.as_ref().expect("topic_naming").prefix,
        f["target"]["topicPrefix"].as_str().unwrap()
    );
    let pit: chrono::DateTime<chrono::Utc> = f["pointInTime"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        s.restore.point_in_time,
        Some(pit),
        "the recovery point is the window's END"
    );
    assert_eq!(s.restore.time_basis, None);
    assert_eq!(s.sample.coverage, logweir_core::spec::Coverage::Sampled);
    assert!(s.target.original_name().is_none());
    assert!(
        s.source.point.is_none(),
        "a Backup-bound plan carries no point binding"
    );
}

#[test]
fn each_opt_in_arrives_under_the_key_the_runner_reads() {
    // Producer time.
    assert_eq!(
        spec("plan-time-basis.golden.yaml").restore.time_basis,
        Some(logweir_core::spec::TimeBasis::ProducerTime)
    );
    // Complete verification, bounded, with no partition bound beside it.
    let c = spec("plan-complete.golden.yaml");
    assert_eq!(c.sample.coverage, logweir_core::spec::Coverage::Complete);
    assert_eq!(c.sample.complete_max_records, Some(1_000_000));
    assert_eq!(c.sample.max_partitions, None);
    // The original names: the owner statement, the approval subject, condition 1.
    let o = spec("plan-original-name.golden.yaml");
    let block = o.target.original_name().expect("an original_name block");
    assert_eq!(block.owners.as_deref(), Some(&[][..]), "owners: []");
    assert!(logweir_core::original_name::is_original_name_restore(&o));
    assert_eq!(logweir_core::original_name::refuse_shape(&o), None);
    assert_eq!(
        logweir_core::original_name::ApprovalSubject::of_plan(&o),
        logweir_core::original_name::ApprovalSubject::OriginalName
    );
    assert_eq!(logweir_core::spec::target_topic_prefix(&o), "");
    // The catalog point binding, field by field, and the backup set pinned.
    let p = spec("plan-point.golden.yaml");
    let f = fields("plan-point-fields.json");
    let point = p.source.point.as_ref().expect("source.point");
    for (got, key) in [
        (&point.point_id, "pointId"),
        (&point.receipt_key, "receiptKey"),
        (&point.receipt_sha256, "receiptSha256"),
        (&point.manifest_sha256, "manifestSha256"),
    ] {
        assert_eq!(got, f["point"][key].as_str().unwrap(), "source.point.{key}");
    }
    assert_eq!(p.source.backup, f["backupSetRef"].as_str().unwrap());

    // NEGATIVE CONTROL: no other golden carries any of these.
    for name in GOLDENS {
        let s = spec(name);
        if name != "plan-time-basis.golden.yaml" {
            assert_eq!(s.restore.time_basis, None, "{name}");
        }
        if name != "plan-complete.golden.yaml" && name != "plan-original-name.golden.yaml" {
            assert_eq!(
                s.sample.coverage,
                logweir_core::spec::Coverage::Sampled,
                "{name}"
            );
        }
        if name != "plan-original-name.golden.yaml" {
            assert!(s.target.original_name().is_none(), "{name}");
        }
        if name != "plan-point.golden.yaml" {
            assert!(s.source.point.is_none(), "{name}");
        }
    }
}

/// THEME A'S CLONE (`target.teardown: {ttl_seconds}`), WHICH THIS TREE'S RUNNER
/// REFUSES. `teardown` is a string here, so the clone golden does not parse:
/// the documented behaviour of a runner from before theme A, which restores
/// nothing rather than keep a clone forever (decisions/PROD-04.2-cutover.md
/// section 5). When theme A's `RestoreSpec` lands, this row fails; it then
/// becomes the positive parse (`Teardown::Ttl(86400)`, `AfterVerification`,
/// `owners: []`), which a scratch build of theme A's `logweir-core` already
/// gives (prod-18-1-r8's report).
#[test]
fn a_runner_before_theme_a_refuses_the_console_clone_plan() {
    let golden = read("plan-clone.golden.yaml");
    assert!(golden.contains("  teardown:\n    ttl_seconds: 86400\n"));
    assert!(golden.contains("  topic_configuration:\n    transition: \"afterVerification\"\n"));
    let refused = serde_yaml::from_str::<RestoreSpec>(&golden)
        .expect_err("this tree's RestoreSpec refuses the clone form; see this row's comment");
    assert!(refused.to_string().contains("teardown"), "{refused}");
    // Control: the same plan with the string policy parses.
    let plain = golden.replace(
        "  teardown:\n    ttl_seconds: 86400\n",
        "  teardown: \"delete\"\n",
    );
    serde_yaml::from_str::<RestoreSpec>(&plain).expect("the string teardown parses");
}

/// THE PREFIX THE WIZARD PREFILLS IS THE ONE THE RUNNER COMPUTES, for the
/// instants `console/fixtures/legacy-console.json` records (the console's
/// `plan.test.ts` holds `defaultTopicPrefix` to the same file).
#[test]
fn the_default_prefix_agrees_with_the_rust_one() {
    let recorded = fields("legacy-console.json");
    let prefixes = recorded["defaultPrefixes"]
        .as_object()
        .expect("defaultPrefixes");
    assert_eq!(prefixes.len(), 3);
    for (at, want) in prefixes {
        let at: chrono::DateTime<chrono::Utc> = at.parse().unwrap();
        assert_eq!(
            logweir_core::spec::default_topic_prefix(at),
            want.as_str().unwrap(),
            "{at}"
        );
    }
}
