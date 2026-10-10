//! **PROD-15.1 at the controller**: a `Restore` under the original topic names
//! is admitted only with an approval signed for its own approval subject,
//! `originalName`; its declaration (`spec.target.topicNaming.originalName`)
//! must say what its plan says; and a standing rehearsal authorization never
//! admits one. Pure functions over constructed objects — the same functions
//! the reconciler runs before its first `POST`.

use logweir_core::ids::sha256_prefixed;
use logweir_core::original_name::ApprovalSubject;
use serde_json::Value;
use weirkeeper::conditions::{
    TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH, TERMINAL_STATE_EXECUTION_SPEC_INVALID,
};
use weirkeeper::controllers::restore::{
    admit, approval_subject_of, original_name_agrees, plan_approval_subject, RestoreAdmission,
    RestoreError,
};
use weirkeeper::crds::restore::Restore;

const NS: &str = "team-a";
const NAME: &str = "rst-original";
const UID: &str = "5c2e7b91-0000-4000-8000-0000000000b1";
const APPROVAL: &str = "a1";

/// An original-name plan in the runner's grammar: `newTopic`, `prefix: ""`,
/// the block, and the COMPLETE verification such a restore requires.
const ORIGINAL_PLAN: &str = "\
source:
  storage:
    backend: s3
    bucket: kafka-backups
    prefix: prod
    region: us-east-1
  backup: latestCompleted
  topics: [orders, payments]
target:
  bootstrap_servers: [prod-0.kafka:9092]
  mode: newTopic
  topic_naming:
    prefix: \"\"
    original_name:
      owners: []
  topic_mapping_prefix: \"drill-\"
restore:
  point_in_time: \"2026-09-07T14:05:00Z\"
sample:
  window_start: \"2026-09-07T12:00:00Z\"
  window_end: \"2026-09-07T15:00:00Z\"
  coverage: complete
objectives: {}
evidence:
  backend: s3
  bucket: logweir-evidence
  prefix: logweir/
  region: us-east-1
";

/// The same restore under a new name: an ordinary plan (sampled, as an
/// ordinary plan may be).
fn ordinary_plan() -> String {
    ORIGINAL_PLAN
        .replace(
            "    prefix: \"\"\n    original_name:\n      owners: []\n",
            "    prefix: \"restore-\"\n",
        )
        .replace("  coverage: complete\n", "")
}

/// The original-name plan WITHOUT `sample.coverage: complete`.
fn sampled_original_plan() -> String {
    ORIGINAL_PLAN.replace("  coverage: complete\n", "")
}

fn restore(plan: &str, declared: Option<bool>, prefix: &str) -> Restore {
    let mut naming = serde_json::json!({ "prefix": prefix });
    if let Some(d) = declared {
        naming["originalName"] = serde_json::json!(d);
    }
    let mut object = serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": { "name": NAME, "namespace": NS, "uid": UID, "generation": 1,
                      "resourceVersion": "1" },
        "spec": {
            "planBytes": plan,
            "approvalRef": { "name": APPROVAL },
            "sourceArchive": { "url": "s3://kafka-backups/prod", "secretRef": { "name": "s3" } },
            "backupSetRef": "nightly",
            "pointInTime": "2026-09-07T14:05:00Z",
            "target": {
                "clusterRef": { "name": "prod" },
                "mode": "newTopic",
                "topicNaming": naming
            },
            "deadlineSeconds": 1800
        }
    });
    // `spec.coverage` declares what the plan says, as the controller holds it
    // to (`coverage_agrees`).
    if plan.contains("  coverage: complete\n") {
        object["spec"]["coverage"] = serde_json::json!("complete");
    }
    serde_json::from_value(object).expect("the fixture is a Restore")
}

fn original_restore() -> Restore {
    restore(ORIGINAL_PLAN, Some(true), "")
}

/// A verified v1 Approval of `plan`, whose signed document carries
/// `approval_subject` when given.
fn approval(plan: &str, subject: Option<&str>) -> weirkeeper::crds::approval::Approval {
    let mut doc = serde_json::json!({
        "approver": "sre-oncall@example.com",
        "ticket": "CHG-1",
        "plan_hash": sha256_prefixed(plan.as_bytes()),
        "approved_at": "2026-09-07T13:00:00Z",
        "subject_kind": "Restore"
    });
    if let Some(s) = subject {
        doc["approval_subject"] = serde_json::json!(s);
    }
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Approval",
        "metadata": { "name": APPROVAL, "namespace": NS, "uid": "aaaaaaaa-0000-4000-8000-0000000000b2" },
        "spec": {
            "subjectRef": { "kind": "Restore", "name": NAME },
            "planHash": sha256_prefixed(plan.as_bytes()),
            "approvalBytes": doc.to_string(),
            "sidecarBytes": "{}"
        },
        "status": {
            "verified": true,
            "verifiedSubjectRef": { "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                                    "name": NAME, "namespace": NS, "uid": UID },
            "conditions": [{ "type": "Verified", "status": "True", "reason": "Verified" }]
        }
    }))
    .expect("the fixture is an Approval")
}

fn cluster() -> weirkeeper::crds::kafka_cluster::KafkaCluster {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": { "name": "prod", "namespace": NS, "uid": "c" },
        "spec": { "bootstrapServers": ["prod-0.kafka:9092"], "auth": { "mode": "plaintext" },
                  "role": "target" },
        "status": { "reachable": true }
    }))
    .expect("the fixture is a KafkaCluster")
}

/// **The separate approval subject.** An original-name Restore is admitted
/// with an `originalName` approval and refused, terminally and before any
/// Job, with an ordinary one; an `originalName` approval admits nothing else.
/// KILLS: deleting step 4b; comparing in one direction only; reading the
/// subject from anything but the signed bytes.
#[test]
fn an_original_name_restore_needs_its_own_approval_subject() {
    let cluster = cluster();
    let restore = original_restore();
    assert_eq!(
        admit(
            &restore,
            Some(&approval(ORIGINAL_PLAN, Some("originalName"))),
            Some(&cluster),
            None
        ),
        RestoreAdmission::Ok
    );
    let ordinary = admit(
        &restore,
        Some(&approval(ORIGINAL_PLAN, None)),
        Some(&cluster),
        None,
    );
    assert!(
        matches!(
            ordinary,
            RestoreAdmission::ApprovalSubjectNotThePlans { .. }
        ),
        "{ordinary:?}"
    );
    assert!(ordinary.is_terminal());
    assert_eq!(ordinary.reason(), TERMINAL_STATE_APPROVAL_SUBJECT_MISMATCH);
    assert!(ordinary.to_string().contains("ordinary one"), "{ordinary}");

    let plain = ordinary_plan();
    let plain_restore = restore_with(&plain, None, "restore-");
    assert_eq!(
        admit(
            &plain_restore,
            Some(&approval(&plain, None)),
            Some(&cluster),
            None
        ),
        RestoreAdmission::Ok
    );
    let reverse = admit(
        &plain_restore,
        Some(&approval(&plain, Some("originalName"))),
        Some(&cluster),
        None,
    );
    assert!(
        matches!(reverse, RestoreAdmission::ApprovalSubjectNotThePlans { .. }),
        "{reverse:?}"
    );
    // A subject this build does not know is refused, never read as either.
    let unknown = admit(
        &restore,
        Some(&approval(ORIGINAL_PLAN, Some("everything"))),
        Some(&cluster),
        None,
    );
    assert!(
        matches!(unknown, RestoreAdmission::ApprovalSubjectNotThePlans { .. }),
        "{unknown:?}"
    );
}

fn restore_with(plan: &str, declared: Option<bool>, prefix: &str) -> Restore {
    restore(plan, declared, prefix)
}

/// The subject is read from the signed bytes in v2's spelling too.
#[test]
fn the_subject_is_read_from_the_signed_document_in_both_spellings() {
    let v1 = approval(ORIGINAL_PLAN, Some("originalName"));
    assert_eq!(approval_subject_of(&v1), Ok(ApprovalSubject::OriginalName));
    assert_eq!(
        approval_subject_of(&approval(ORIGINAL_PLAN, None)),
        Ok(ApprovalSubject::Ordinary)
    );
    let mut v2: Value = serde_json::to_value(&v1).unwrap();
    v2["spec"]["sidecarBytes"] = serde_json::json!(serde_json::json!({
        "payloadType": logweir_core::approval_policy::PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
        "signatures": []
    })
    .to_string());
    // Under a v2 payload type the snake_case key is NOT the subject.
    let v2: weirkeeper::crds::approval::Approval = serde_json::from_value(v2).unwrap();
    assert_eq!(approval_subject_of(&v2), Ok(ApprovalSubject::Ordinary));
    let mut camel: Value = serde_json::to_value(&v2).unwrap();
    camel["spec"]["approvalBytes"] = serde_json::json!(serde_json::json!({
        "approvalSubject": "originalName"
    })
    .to_string());
    let camel: weirkeeper::crds::approval::Approval = serde_json::from_value(camel).unwrap();
    assert_eq!(
        approval_subject_of(&camel),
        Ok(ApprovalSubject::OriginalName)
    );
    assert_eq!(
        plan_approval_subject(&original_restore()),
        ApprovalSubject::OriginalName
    );
}

/// **The declaration holds to the plan, both ways**, before an approver is
/// shown the wrong subject. KILLS: deleting `original_name_agrees`; checking
/// one direction.
#[test]
fn the_declaration_must_say_what_the_plan_says() {
    assert!(original_name_agrees(&original_restore()).is_ok());
    assert!(original_name_agrees(&restore(&ordinary_plan(), None, "restore-")).is_ok());
    assert!(original_name_agrees(&restore(&ordinary_plan(), Some(false), "restore-")).is_ok());
    for (label, r) in [
        (
            "an original-name plan declared ordinary",
            restore(ORIGINAL_PLAN, None, ""),
        ),
        (
            "an ordinary plan declared original-name",
            restore(&ordinary_plan(), Some(true), ""),
        ),
        (
            "a declaration over a plan that does not parse",
            restore("not: [a plan", Some(true), ""),
        ),
    ] {
        match original_name_agrees(&r) {
            Err(RestoreError::Refused(state, message)) => {
                assert_eq!(state, TERMINAL_STATE_EXECUTION_SPEC_INVALID, "{label}");
                assert!(message.contains("originalName"), "{label}: {message}");
            }
            other => panic!("{label}: expected ExecutionSpecInvalid, got {other:?}"),
        }
    }
    // An unparseable plan with nothing declared is the runner's to refuse.
    assert!(original_name_agrees(&restore("not: [a plan", None, "x-")).is_ok());
}

/// **An original-name restore requires complete verification, and the
/// controller says so before any Job** (the orchestrator's ruling of
/// 2026-10-09). A Restore whose plan carries the block without
/// `sample.coverage: complete` is refused terminally, `ExecutionSpecInvalid`,
/// its message opening with the runner's own token; the same plan asking for
/// complete coverage agrees. CONTROL: a sampled ORDINARY plan agrees as it
/// always did. KILLS: an admission that leaves the sampled plan to the
/// runner (a Job, an approval asked for, then exit 3); applying the rule to
/// a prefixed restore.
#[test]
fn a_sampled_original_name_restore_is_refused_before_any_job() {
    use weirkeeper::controllers::restore::coverage_agrees;

    let sampled = restore(&sampled_original_plan(), Some(true), "");
    // The declaration agrees with the plan on both counts...
    assert!(coverage_agrees(&sampled).is_ok());
    // ... and the plan itself is what is refused.
    match original_name_agrees(&sampled) {
        Err(RestoreError::Refused(state, message)) => {
            assert_eq!(state, TERMINAL_STATE_EXECUTION_SPEC_INVALID);
            assert!(
                message.starts_with("OriginalNameNeedsCompleteCoverage: "),
                "{message}"
            );
            assert!(message.contains("no Job was created"), "{message}");
        }
        other => panic!("expected ExecutionSpecInvalid, got {other:?}"),
    }
    let complete = original_restore();
    assert!(coverage_agrees(&complete).is_ok());
    assert!(original_name_agrees(&complete).is_ok());
    // CONTROL: an ordinary sampled restore is what it always was.
    let ordinary = restore(&ordinary_plan(), None, "restore-");
    assert!(coverage_agrees(&ordinary).is_ok());
    assert!(original_name_agrees(&ordinary).is_ok());
}

/// The CRD's CEL rules keep the declaration beside the only target it
/// describes and the only verification it may run with. KILLS: a rule that
/// admits `originalName` in scratch mode, beside a prefix, or without
/// `coverage: complete`.
#[test]
fn the_cel_rule_ties_the_declaration_to_new_topic_and_the_empty_prefix() {
    use weirkeeper::crds::restore::{ORIGINAL_NAME_COVERAGE_RULE, ORIGINAL_NAME_RULE, SPEC_RULES};
    assert!(SPEC_RULES.iter().any(|r| r.rule == ORIGINAL_NAME_RULE));
    assert!(ORIGINAL_NAME_RULE.contains("self.target.mode == 'newTopic'"));
    assert!(ORIGINAL_NAME_RULE.contains("self.target.topicNaming.prefix == ''"));
    assert!(ORIGINAL_NAME_RULE.starts_with("!has(self.target.topicNaming.originalName)"));
    assert!(SPEC_RULES
        .iter()
        .any(|r| r.rule == ORIGINAL_NAME_COVERAGE_RULE));
    assert_eq!(
        ORIGINAL_NAME_COVERAGE_RULE,
        "!has(self.target.topicNaming.originalName) || !self.target.topicNaming.originalName \
         || (has(self.coverage) && self.coverage == 'complete')"
    );
}
