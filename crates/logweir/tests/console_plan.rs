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

/// OD-23 (`plan-registry.golden.yaml`): the target connection's Schema
/// Registry arrives as `target.schema_registry` (the controller holds the plan
/// to the connection's, `ConnectionPlanMismatch` otherwise), and the byte-rate
/// limit as `restore.rate_limit_bytes_per_sec`. CONTROL: the plain golden
/// carries neither.
#[test]
fn the_registry_and_the_rate_limit_arrive_under_the_keys_the_runner_reads() {
    let s = spec("plan-registry.golden.yaml");
    let f = fields("plan-registry-fields.json");
    let registry = s
        .target
        .schema_registry
        .as_ref()
        .expect("target.schema_registry");
    assert_eq!(
        registry.url,
        f["target"]["schemaRegistry"]["url"].as_str().unwrap()
    );
    assert_eq!(
        registry.username.as_deref(),
        f["target"]["schemaRegistry"]["username"].as_str()
    );
    assert_eq!(
        s.restore.rate_limit_bytes_per_sec,
        f["rateLimitBytesPerSec"].as_u64()
    );
    let plain = spec("plan.golden.yaml");
    assert!(plain.target.schema_registry.is_none());
    assert_eq!(plain.restore.rate_limit_bytes_per_sec, None);
}

/// THEME A'S CLONE WITH THE SOURCE'S CONFIGURATION (`plan-clone.golden.yaml`):
/// the runner reads the lifetime as the clone form, in scratch mode, and the
/// configuration choice with the requester's owner statement. A runner from
/// before theme A refuses the map form at parse rather than keep a clone forever
/// (decisions/PROD-04.2-cutover.md section 5); the string form is the control.
#[test]
fn the_console_clone_plan_is_one_the_runner_reads_as_a_clone() {
    let s = spec("plan-clone.golden.yaml");
    assert_eq!(s.target.mode.to_string(), "scratch");
    assert_eq!(s.target.teardown.ttl_seconds(), Some(86_400));
    let tc = s
        .target
        .topic_configuration
        .as_ref()
        .expect("the golden states target.topic_configuration");
    assert_eq!(
        tc.transition,
        logweir_core::topic_transition::Transition::AfterVerification
    );
    assert_eq!(tc.owners.as_deref().map(<[_]>::len), Some(0), "owners: []");
    // Controls: the plain goldens carry neither, and the string policy is no clone.
    for name in GOLDENS {
        let plain = spec(name);
        assert_eq!(plain.target.teardown.ttl_seconds(), None, "{name}");
        assert!(plain.target.topic_configuration.is_none(), "{name}");
    }
}

/// DUI-17: THE SOURCE'S CONFIGURATION WITH THE TOPICS' OWNERS NAMED
/// (`plan-owners.golden.yaml`). A point whose backup looked for no owner is
/// refused `TopicOwnerNotChecked` unless the plan states them; the wizard now
/// asks, and each owner it names arrives as the runner reads it and passes the
/// runner's own declaration rules.
#[test]
fn the_console_owners_plan_states_each_owner_as_the_runner_reads_it() {
    let s = spec("plan-owners.golden.yaml");
    let f = fields("plan-owners-fields.json");
    let tc = s
        .target
        .topic_configuration
        .as_ref()
        .expect("the golden states target.topic_configuration");
    let owners = tc.owners.as_deref().expect("the golden states owners");
    let want = f["target"]["topicConfiguration"]["owners"]
        .as_array()
        .expect("the fields file names owners");
    assert_eq!(owners.len(), want.len());
    assert!(
        !owners.is_empty(),
        "the row is about named owners, not owners: []"
    );
    for (owner, w) in owners.iter().zip(want) {
        assert_eq!(owner.topic, w["topic"].as_str().unwrap());
        assert_eq!(owner.kind, w["kind"].as_str().unwrap());
        assert_eq!(owner.reference, w["reference"].as_str().unwrap());
    }
    assert_eq!(
        logweir_core::topic_configuration::refuse_declarations(owners, &s.source.topics),
        None
    );
    // Control: the clone golden states owners: [], which is not this list.
    let clone = spec("plan-clone.golden.yaml");
    assert_eq!(
        clone
            .target
            .topic_configuration
            .as_ref()
            .and_then(|t| t.owners.as_deref())
            .map(<[_]>::len),
        Some(0)
    );
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
