//! PLAT-14.3b — the `Restore` reconciler's STANDING arm.
//!
//! The 2026-09-17 record named the gap exactly: `admit`, `get_approval`,
//! `triggered_by`, `runner_argv` and `runner_job_spec` resolved
//! `spec.approvalRef` only, so every rehearsal `Restore` D3 W7 created was
//! refused with `ApprovalNotReceived` before a Job existed.
//!
//! READ THESE THREE FIRST:
//!
//! 1. [`a_standing_restore_is_admitted_and_its_job_carries_the_bundle`] — the
//!    whole task in one row: admitted, the argv carries
//!    `--standing-authorization` and no `--approval`, and the Job projects the
//!    file table's five members.
//! 2. [`an_ordinary_restore_is_never_admitted_by_a_standing_document`] — the
//!    widening this arm is shaped to prevent. `admit` dispatches on the
//!    OBJECT's `spec.authorization`, never on what `Approval` exists.
//! 3. [`each_standing_refusal_is_named_and_reaches_no_job`] — D3 §4.3(c)'s
//!    four refusals, made again at this reconciler because a key can be
//!    withdrawn between the slot firing and this reconcile.

use chrono::{DateTime, TimeZone, Utc};
use weirkeeper::controllers::restore::{
    admit, runner_argv, runner_job_spec, triggered_by, RestoreAdmission, StandingAdmission,
    ALLOWED_CLUSTERS_FILE, APPROVAL_DOC_FILE, APPROVAL_SIG_FILE, APPROVER_KEY_FILE,
    AUTHORIZATION_KEYS_FILE, STANDING_AUTHORIZATION_FILE, STANDING_AUTHORIZATION_SIG_FILE,
};
use weirkeeper::crds::restore::Restore;

const NS: &str = "logweir-plat14-3b";
const SCHEDULE: &str = "weekly-orders";
const SCHEDULE_UID: &str = "3f2a91c7-1111-4222-8333-444444444444";
const APPROVAL: &str = "weekly-orders-standing";
const TARGET: &str = "kafka-target";
const TARGET_CLUSTER_ID: &str = "TARGET00000000000000000";
const SLOT: &str = "20260920T030000";
const KEY_ID: &str = "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6";
const KEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEApFpEU8uY5S8Lv43HL4DcXKKyM8WHurCPZIxvq8ZBfpY=\n-----END PUBLIC KEY-----\n";
const PREFIX: &str = "rehearsal-3f2a91c7-";

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 20, 3, 30, 0)
        .single()
        .expect("the fixture instant exists")
}

fn utc(y: i32, m: u32, d: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, 0, 0, 0)
        .single()
        .expect("a fixture instant")
}

/// The plan every row freezes: inside the scope below, and nothing else.
///
/// The exact `DrillSpec` grammar the runner parses — `plan_scope_facts` reads
/// `target.mode`, the topic prefix, `source.topics`, `sample.max_partitions`
/// and `sample.records_per_partition` out of it, so a fixture that drifted
/// from the real shape would make every scope row vacuous.
fn plan_bytes() -> String {
    format!(
        "source:\n  storage:\n    backend: s3\n    bucket: archives\n    prefix: logweir/\n\
         \n  backup: latestCompleted\n  topics: [orders]\ntarget:\n  bootstrap_servers: \
         [kafka-target:9092]\n  mode: scratch\n  topic_mapping_prefix: \"{PREFIX}\"\n  \
         marker_topic: logweir.scratch\n  default_replication_factor: 1\n  teardown: \
         delete\nsample:\n  window_start: \"2026-09-20T00:00:00Z\"\n  window_end: \
         \"2026-09-20T02:00:00Z\"\n  records_per_partition: 25\n  max_partitions: 200\n  \
         anchor: head\nobjectives: {{}}\nevidence:\n  backend: s3\n  bucket: archives\n  \
         prefix: logweir/\n"
    )
}

fn scope() -> serde_json::Value {
    serde_json::json!({
        "templateDigest": "sha256:aa",
        "targetClusterId": TARGET_CLUSTER_ID,
        "topicPrefix": PREFIX,
        "topics": ["orders"],
        "maxPartitions": 200,
        "recordsPerPartition": 25,
        "deadlineSeconds": 3600,
        "modes": ["scratch"],
    })
}

/// The SIGNED standing authorization, as the bytes `Approval.spec` carries.
fn envelope_with(scope: serde_json::Value, issued: DateTime<Utc>, days: i64) -> String {
    serde_json::to_string(&serde_json::json!({
        "formatVersion": "1.0.0",
        "kind": "StandingRehearsalAuthorization",
        "subjectRef": {
            "apiVersion": "logweir.dev/v1alpha1",
            "kind": "RehearsalSchedule",
            "namespace": NS,
            "name": SCHEDULE,
            "uid": SCHEDULE_UID,
        },
        "scope": scope,
        "issuedAt": issued.to_rfc3339(),
        "expiresAt": (issued + chrono::Duration::days(days)).to_rfc3339(),
    }))
    .expect("the fixture envelope serialises")
}

fn envelope() -> String {
    envelope_with(scope(), now() - chrono::Duration::days(2), 30)
}

/// A standing `Approval`: verified, bound to this schedule's UID, over
/// `envelope`.
fn approval_over(envelope: &str) -> weirkeeper::crds::approval::Approval {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Approval",
        "metadata": {"name": APPROVAL, "namespace": NS, "uid": "beef-1111"},
        "spec": {
            "subjectRef": {"kind": "RehearsalSchedule", "name": SCHEDULE},
            "planHash": "sha256:aa",
            "approvalBytes": envelope,
            "sidecarBytes": "{\"signatures\":[]}",
        },
        "status": {
            "verified": true,
            "matchedKeyId": KEY_ID,
            "verifiedSubjectRef": {
                "apiVersion": "logweir.dev/v1alpha1",
                "kind": "RehearsalSchedule",
                "name": SCHEDULE,
                "namespace": NS,
                "uid": SCHEDULE_UID,
            },
        },
    }))
    .expect("the fixture is an Approval")
}

fn approval() -> weirkeeper::crds::approval::Approval {
    approval_over(&envelope())
}

/// The rehearsal `Restore` a firing slot creates — `spec.authorization`, no
/// `spec.approvalRef`, and the slot label `triggered_by` reads.
fn standing_restore() -> Restore {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {
            "name": "logweir-rehearsal-weekly-orders-20260920-030000",
            "namespace": NS,
            "uid": "restore-uid-1",
            "labels": {
                "logweir.dev/rehearsal-schedule": SCHEDULE,
                "logweir.dev/rehearsal-slot": SLOT,
            },
        },
        "spec": {
            "planBytes": plan_bytes(),
            "authorization": {
                "kind": "Standing",
                "approvalRef": {"name": APPROVAL},
                "rehearsalScheduleRef": {"name": SCHEDULE},
            },
            "sourceArchive": {"url": "logweir-destination://primary"},
            "sourceDestinationRef": {"name": "primary"},
            "evidenceDestinationRef": {"name": "primary"},
            "backupSetRef": "nightly-7",
            "pointInTime": "2026-09-20T02:00:00Z",
            "target": {"clusterRef": {"name": TARGET}, "mode": "scratch", "topicNaming": {"prefix": PREFIX}},
            "deadlineSeconds": 3600,
        },
    }))
    .expect("the fixture is a Restore")
}

/// An ORDINARY `Restore`: `spec.approvalRef`, no `spec.authorization`.
fn ordinary_restore() -> Restore {
    let mut value: serde_json::Value =
        serde_json::to_value(standing_restore()).expect("the fixture serialises");
    value["spec"]["authorization"] = serde_json::Value::Null;
    value["spec"]["approvalRef"] = serde_json::json!({"name": APPROVAL});
    value["metadata"]["name"] = serde_json::json!("ordinary-restore");
    let mut restore: Restore = serde_json::from_value(value).expect("the fixture is a Restore");
    restore.spec.authorization = None;
    restore
}

fn cluster(reachable: bool) -> weirkeeper::crds::kafka_cluster::KafkaCluster {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": {"name": TARGET, "namespace": NS, "uid": "cluster-uid"},
        "spec": {"bootstrapServers": ["kafka-target:9092"], "auth": {"mode": "plaintext"}, "role": "scratch"},
        "status": {"reachable": reachable, "clusterId": TARGET_CLUSTER_ID},
    }))
    .expect("the fixture is a KafkaCluster")
}

/// One `TrustPolicy` key, mutated per row.
fn approver_key() -> weirkeeper::crds::trust_policy::TrustedKey {
    use weirkeeper::crds::trust_policy::{KeyAlgorithm, KeyPrincipal, KeyState, KeyUsage};
    weirkeeper::crds::trust_policy::TrustedKey {
        key_id: KEY_ID.to_string(),
        spki_pem: KEY_PEM.to_string(),
        algorithm: KeyAlgorithm::Ed25519,
        usages: vec![KeyUsage::GovernedApproval],
        principal: KeyPrincipal {
            id: format!("install:{KEY_ID}"),
            display: None,
        },
        not_before: utc(2026, 1, 1),
        not_after: utc(2099, 1, 1),
        state: KeyState::Active,
        retired_at: None,
        revoked_at: None,
        revocation_reason: None,
        revocation_effective_from: None,
    }
}

fn trust_with(key: weirkeeper::crds::trust_policy::TrustedKey) -> weirkeeper::trust::ResolvedTrust {
    use weirkeeper::crds::trust_policy::{TrustPolicy, TrustPolicySpec};
    weirkeeper::trust::from_policy(&TrustPolicy {
        metadata: kube::api::ObjectMeta {
            name: Some("org-default".to_string()),
            uid: Some("uid-org-default".to_string()),
            generation: Some(1),
            ..kube::api::ObjectMeta::default()
        },
        spec: TrustPolicySpec {
            default: false,
            namespaces: Some(vec![NS.to_string()]),
            allowed_target_cluster_ids: Some(vec![TARGET_CLUSTER_ID.to_string()]),
            keys: vec![key],
        },
        status: None,
    })
}

fn trust() -> weirkeeper::trust::ResolvedTrust {
    trust_with(approver_key())
}

// ---------------------------------------------------------------------------
// 1. The whole task, in one row
// ---------------------------------------------------------------------------

/// **THE ROW THIS TASK EXISTS FOR.** A rehearsal `Restore` is ADMITTED on
/// `spec.authorization`, its argv carries `--standing-authorization` and no
/// `--approval`, its trigger names the schedule and slot, and its Job projects
/// exactly the five members of the file table.
///
/// Before PLAT-14.3b every one of these was the ordinary approval shape and
/// the admission was `ApprovalNotReceived`.
#[test]
fn a_standing_restore_is_admitted_and_its_job_carries_the_bundle() {
    let trust = trust();
    let inputs = StandingAdmission {
        trust: &trust,
        now: now(),
    };
    let restore = standing_restore();

    assert_eq!(
        admit(
            &restore,
            Some(&approval()),
            Some(&cluster(true)),
            Some(&inputs)
        ),
        RestoreAdmission::Ok,
        "a verified, in-window, in-scope standing authorization admits the slot"
    );

    // ---- the argv ---------------------------------------------------------
    let argv = runner_argv(&restore, &[KEY_ID.to_string()]);
    assert!(
        argv.windows(2)
            .any(|w| w[0] == "--standing-authorization"
                && w[1].ends_with(STANDING_AUTHORIZATION_FILE)),
        "the argv points at the standing document: {argv:?}"
    );
    assert!(
        argv.windows(2)
            .any(|w| w[0] == "--authorization-keys" && w[1].ends_with(AUTHORIZATION_KEYS_FILE)),
        "and at the keyring its signature anchors in: {argv:?}"
    );
    assert!(
        !argv.iter().any(|a| a == "--approval"),
        "the standing document REPLACES the per-run approval; passing both is the pre-14.3b \
         shape the runner now refuses by name: {argv:?}"
    );
    // The sidecar is NOT a flag — the runner derives it by replacing the
    // extension, so there is one fewer path to get wrong.
    assert!(
        !argv
            .iter()
            .any(|a| a.ends_with(STANDING_AUTHORIZATION_SIG_FILE)),
        "the sidecar path is DERIVED, never passed: {argv:?}"
    );
    assert!(
        argv.windows(2)
            .any(|w| w[0] == "--approver-key" && w[1].ends_with(APPROVER_KEY_FILE)),
        "the approver public key is still mounted and named: {argv:?}"
    );

    // ---- the trigger ------------------------------------------------------
    assert_eq!(
        triggered_by(&restore),
        format!("rehearsal/{SCHEDULE}/{SLOT}"),
        "a rehearsal's reason is a SLOT; one standing document covers every slot of one \
         schedule, so `approval/<name>` would read identically on every run it ever makes"
    );
    assert!(
        argv.windows(2)
            .any(|w| w[0] == "--triggered-by" && w[1] == format!("rehearsal/{SCHEDULE}/{SLOT}")),
        "and the argv carries it: {argv:?}"
    );

    // ---- the Job's bundle mount, per the file table ----------------------
    //
    // PURE, so the Job a test builds is byte-identical to the one the
    // reconciler POSTs (see `runner_job_spec`'s own note).
    let spec = runner_job_spec(
        &restore,
        &cluster(true),
        &[KEY_ID.to_string()],
        &approval(),
        &trust,
        now(),
    )
    .expect("the Job renders for an admitted rehearsal");
    let mount = spec
        .config_map_mounts
        .iter()
        .find(|m| m.mount_path.ends_with("approval"))
        .expect("the bundle is mounted");
    let projected: Vec<(String, String)> = mount.items.clone();
    for (key, path) in [
        (STANDING_AUTHORIZATION_FILE, STANDING_AUTHORIZATION_FILE),
        (
            STANDING_AUTHORIZATION_SIG_FILE,
            STANDING_AUTHORIZATION_SIG_FILE,
        ),
        (AUTHORIZATION_KEYS_FILE, AUTHORIZATION_KEYS_FILE),
        (APPROVER_KEY_FILE, APPROVER_KEY_FILE),
        (ALLOWED_CLUSTERS_FILE, ALLOWED_CLUSTERS_FILE),
    ] {
        assert!(
            projected.iter().any(|(k, p)| k == key && p == path),
            "the file table projects {key} at {path}: {projected:?}"
        );
    }
    // **THE SIDECAR KEEPS ITS OWN NAME.** Projecting it at `approval.sig`
    // makes the runner read it into `bundle.approval_sidecar` and verify it
    // under `PAYLOAD_TYPE_APPROVAL`; `verify_detached` then returns
    // `payload_type mismatch`, a variant whose own doc comment calls it
    // EVIDENCE OF SUBSTITUTION. A correctly signed, correctly scoped rehearsal
    // would be reported to the operator as a TAMPERED APPROVAL.
    for forbidden in [APPROVAL_DOC_FILE, APPROVAL_SIG_FILE] {
        assert!(
            !projected
                .iter()
                .any(|(k, p)| k == forbidden || p == forbidden),
            "a rehearsal Job projects no per-run approval slot, and {forbidden} is in \
             {projected:?}"
        );
    }
    assert_eq!(
        projected.len(),
        5,
        "exactly the five members of the file table: {projected:?}"
    );
}

/// The ORDINARY argv and trigger are BYTE-UNCHANGED by this task.
///
/// MUTANT: making `runner_argv` emit `--standing-authorization` whenever a
/// standing bundle could be rendered breaks every ordinary Restore.
#[test]
fn an_ordinary_restore_keeps_its_approval_argv_and_trigger() {
    let restore = ordinary_restore();
    let argv = runner_argv(&restore, &[]);
    assert!(
        argv.windows(2)
            .any(|w| w[0] == "--approval" && w[1].ends_with(APPROVAL_DOC_FILE)),
        "{argv:?}"
    );
    for absent in ["--standing-authorization", "--authorization-keys"] {
        assert!(
            !argv.iter().any(|a| a == absent),
            "an ordinary Restore carries no standing material: {argv:?}"
        );
    }
    assert_eq!(triggered_by(&restore), format!("approval/{APPROVAL}"));
}

// ---------------------------------------------------------------------------
// 2. The widening this arm is shaped to prevent
// ---------------------------------------------------------------------------

/// **A standing document never admits an ORDINARY `Restore`.**
///
/// `admit` dispatches on the object's own `spec.authorization` — which CEL
/// makes mutually exclusive with `spec.approvalRef` on a sealed spec — and
/// never on what `Approval` happens to exist in the namespace.
///
/// MUTANT: dispatching on `approval.spec.subjectRef.kind ==
/// RehearsalSchedule` instead lets a schedule's standing document authorise
/// any restore whose `approvalRef` points at it. This row is the one that
/// fails.
#[test]
fn an_ordinary_restore_is_never_admitted_by_a_standing_document() {
    let trust = trust();
    let inputs = StandingAdmission {
        trust: &trust,
        now: now(),
    };
    // The ordinary Restore's `approvalRef` names the STANDING Approval.
    let verdict = admit(
        &ordinary_restore(),
        Some(&approval()),
        Some(&cluster(true)),
        Some(&inputs),
    );
    assert!(
        matches!(verdict, RestoreAdmission::ApprovalSubjectMismatch { .. }),
        "an ordinary Restore is judged on the ordinary path, where a RehearsalSchedule approval \
         is a subject mismatch; got {verdict:?}"
    );
    assert!(verdict.is_terminal(), "and it creates no Job");
}

/// And a rehearsal whose `spec.authorization` names nothing is terminal, not
/// a silent fall-through to the ordinary path.
#[test]
fn a_rehearsal_naming_no_approval_is_terminal() {
    let trust = trust();
    let inputs = StandingAdmission {
        trust: &trust,
        now: now(),
    };
    let mut restore = standing_restore();
    restore
        .spec
        .authorization
        .as_mut()
        .expect("the fixture is standing")
        .approval_ref
        .name = "   ".to_string();
    let verdict = admit(
        &restore,
        Some(&approval()),
        Some(&cluster(true)),
        Some(&inputs),
    );
    assert!(
        matches!(verdict, RestoreAdmission::ApprovalNotReceived { .. }),
        "got {verdict:?}"
    );
    assert!(verdict.is_terminal());
}

/// With no trust resolved, the authorization could not be JUDGED — and that is
/// a refusal, never an admission.
///
/// MUTANT: defaulting the absent inputs to "admitted" turns the one ordering
/// change this task makes into a hole.
#[test]
fn an_unjudgeable_standing_authorization_is_refused_and_never_admitted() {
    let verdict = admit(
        &standing_restore(),
        Some(&approval()),
        Some(&cluster(true)),
        None,
    );
    assert!(
        matches!(
            verdict,
            RestoreAdmission::StandingAuthorizationRefused { .. }
        ),
        "got {verdict:?}"
    );
    assert!(verdict.is_terminal(), "and it creates no Job");
}

// ---------------------------------------------------------------------------
// 3. D3 §4.3(c)'s refusals, made again at THIS reconciler
// ---------------------------------------------------------------------------

/// **Each refusal at admission, by name, and none of them reaches a Job.**
///
/// The `RehearsalSchedule` reconciler already made this chain before the
/// `Restore` existed. It is made again here because a key can be withdrawn
/// between the slot firing and this reconcile, and because a `Restore`
/// carrying `spec.authorization` can be created by anything with RBAC on the
/// kind — including one no schedule ever rendered.
#[test]
fn each_standing_refusal_is_named_and_reaches_no_job() {
    use weirkeeper::crds::trust_policy::{KeyState, KeyUsage};

    // ---- expired: the document's own window, re-checked here -------------
    let expired = approval_over(&envelope_with(
        scope(),
        now() - chrono::Duration::days(120),
        30,
    ));
    // ---- a life longer than D3 §4.3 permits ------------------------------
    let too_long = approval_over(&envelope_with(
        scope(),
        now() - chrono::Duration::days(1),
        400,
    ));
    // ---- not yet valid: issued in the future -----------------------------
    let not_yet_valid = approval_over(&envelope_with(
        scope(),
        now() + chrono::Duration::days(30),
        30,
    ));
    // ---- out of scope: the plan's prefix is not the signed one -----------
    let mut other_scope = scope();
    other_scope["topicPrefix"] = serde_json::json!("rehearsal-somethingelse-");
    let out_of_scope = approval_over(&envelope_with(
        other_scope,
        now() - chrono::Duration::days(1),
        30,
    ));
    // ---- wrong subject: verified against another schedule's UID ----------
    let mut wrong_subject: serde_json::Value =
        serde_json::to_value(approval()).expect("serialises");
    wrong_subject["status"]["verifiedSubjectRef"]["uid"] = serde_json::json!("another-uid");
    let wrong_subject: weirkeeper::crds::approval::Approval =
        serde_json::from_value(wrong_subject).expect("an Approval");
    // ---- wrong subject NAME ----------------------------------------------
    let mut wrong_name: serde_json::Value = serde_json::to_value(approval()).expect("serialises");
    wrong_name["status"]["verifiedSubjectRef"]["name"] = serde_json::json!("another-schedule");
    let wrong_name: weirkeeper::crds::approval::Approval =
        serde_json::from_value(wrong_name).expect("an Approval");
    // ---- a per-run Approval presented as a standing one -------------------
    let mut per_run: serde_json::Value = serde_json::to_value(approval()).expect("serialises");
    per_run["spec"]["subjectRef"]["kind"] = serde_json::json!("Restore");
    let per_run: weirkeeper::crds::approval::Approval =
        serde_json::from_value(per_run).expect("an Approval");

    let active = trust();
    // ---- a key withdrawn between one slot and the next -------------------
    let mut revoked_key = approver_key();
    revoked_key.state = KeyState::Revoked;
    revoked_key.revoked_at = Some(utc(2026, 9, 19));
    revoked_key.revocation_effective_from = Some(utc(2026, 9, 19));
    let revoked = trust_with(revoked_key);
    // ---- a key whose usage may not authorise (D3 §7.3) --------------------
    let mut evidence_key = approver_key();
    evidence_key.usages = vec![KeyUsage::EvidenceSigning];
    let evidence_only = trust_with(evidence_key);
    // ---- a key the namespace's trust does not carry at all ---------------
    let mut other_key = approver_key();
    other_key.key_id =
        "0000000000000000000000000000000000000000000000000000000000000000".to_string();
    let unknown_key = trust_with(other_key);

    struct Row {
        what: &'static str,
        approval: weirkeeper::crds::approval::Approval,
        trust: weirkeeper::trust::ResolvedTrust,
        expect_subject_mismatch: bool,
        detail_contains: &'static str,
    }
    let rows = vec![
        Row {
            what: "expired",
            approval: expired,
            trust: active.clone(),
            expect_subject_mismatch: false,
            detail_contains: "",
        },
        Row {
            what: "a life longer than 90 days",
            approval: too_long,
            trust: active.clone(),
            expect_subject_mismatch: false,
            detail_contains: "90",
        },
        // The notBefore half of the window, so BOTH edges are guarded: a
        // document minted for next month does not authorise this slot.
        Row {
            what: "not yet valid",
            approval: not_yet_valid,
            trust: active.clone(),
            expect_subject_mismatch: false,
            detail_contains: "not valid until",
        },
        Row {
            what: "out of scope",
            approval: out_of_scope,
            trust: active.clone(),
            expect_subject_mismatch: false,
            detail_contains: "outside the signed scope",
        },
        Row {
            what: "a revoked key",
            approval: approval(),
            trust: revoked,
            expect_subject_mismatch: false,
            detail_contains: "may no longer authorise anything new",
        },
        Row {
            what: "a wrong-usage key",
            approval: approval(),
            trust: evidence_only,
            expect_subject_mismatch: false,
            detail_contains: "never by EvidenceSigning",
        },
        Row {
            what: "a key the trust does not carry",
            approval: approval(),
            trust: unknown_key,
            expect_subject_mismatch: false,
            detail_contains: "does not carry",
        },
        // The UID binding is caught by `admit_standing_authorization` over the
        // SIGNED bytes — a stronger place than a status comparison, and so it
        // is reported as a standing refusal rather than a subject mismatch.
        Row {
            what: "another schedule's UID",
            approval: wrong_subject,
            trust: active.clone(),
            expect_subject_mismatch: false,
            detail_contains: "but this run is RehearsalSchedule UID another-uid",
        },
        Row {
            what: "another schedule's name",
            approval: wrong_name,
            trust: active.clone(),
            expect_subject_mismatch: true,
            detail_contains: "another-schedule",
        },
        Row {
            what: "a per-run Approval",
            approval: per_run,
            trust: active.clone(),
            expect_subject_mismatch: true,
            detail_contains: "RehearsalSchedule",
        },
    ];

    for row in rows {
        let inputs = StandingAdmission {
            trust: &row.trust,
            now: now(),
        };
        let verdict = admit(
            &standing_restore(),
            Some(&row.approval),
            Some(&cluster(true)),
            Some(&inputs),
        );
        assert!(
            verdict.is_terminal(),
            "{}: a recorded refusal and NO Job; got {verdict:?}",
            row.what
        );
        match (&verdict, row.expect_subject_mismatch) {
            (RestoreAdmission::ApprovalSubjectMismatch { detail, .. }, true) => {
                assert!(
                    detail.contains(row.detail_contains),
                    "{}: {detail}",
                    row.what
                );
            }
            (RestoreAdmission::StandingAuthorizationRefused { detail, .. }, false) => {
                assert!(
                    detail.contains(row.detail_contains),
                    "{}: the refusal says which check refused: {detail}",
                    row.what
                );
                assert_eq!(
                    verdict.reason(),
                    "StandingAuthorizationRefused",
                    "{}: its OWN reason — an operator told ApprovalSubjectMismatch goes looking \
                     at a binding that is correct",
                    row.what
                );
            }
            (other, expected) => panic!(
                "{}: expected {} refusal, got {other:?}",
                row.what,
                if expected {
                    "a subject-mismatch"
                } else {
                    "a standing"
                }
            ),
        }
    }
}

/// An unverified standing `Approval` is a HOLD, not a terminal refusal: it can
/// become verified without anybody touching this object (interface **I19**).
#[test]
fn an_unverified_standing_approval_is_held_and_not_refused() {
    let trust = trust();
    let inputs = StandingAdmission {
        trust: &trust,
        now: now(),
    };
    let mut value: serde_json::Value = serde_json::to_value(approval()).expect("serialises");
    value["status"]["verified"] = serde_json::json!(false);
    let unverified: weirkeeper::crds::approval::Approval =
        serde_json::from_value(value).expect("an Approval");

    let verdict = admit(
        &standing_restore(),
        Some(&unverified),
        Some(&cluster(true)),
        Some(&inputs),
    );
    assert!(
        matches!(verdict, RestoreAdmission::ApprovalNotVerified { .. }),
        "got {verdict:?}"
    );
    assert!(!verdict.is_terminal(), "a hold, requeued — interface I19");

    // And a missing one is the same hold: the Approval may not exist yet.
    let verdict = admit(
        &standing_restore(),
        None,
        Some(&cluster(true)),
        Some(&inputs),
    );
    assert!(
        matches!(verdict, RestoreAdmission::ApprovalNotVerified { .. }),
        "got {verdict:?}"
    );
    assert!(!verdict.is_terminal());
}

/// The target-reachable check is LAST on the standing path too: an
/// authorization problem is reported as one even when the cluster is also
/// down, so an operator is not sent to the broker for a withdrawn key.
#[test]
fn the_cluster_check_is_last_on_the_standing_path() {
    let trust = trust();
    let inputs = StandingAdmission {
        trust: &trust,
        now: now(),
    };
    assert!(
        matches!(
            admit(
                &standing_restore(),
                Some(&approval()),
                Some(&cluster(false)),
                Some(&inputs)
            ),
            RestoreAdmission::ClusterNotReachable { .. }
        ),
        "a good authorization and an unreachable target is a cluster refusal"
    );
    let expired = approval_over(&envelope_with(
        scope(),
        now() - chrono::Duration::days(120),
        30,
    ));
    assert!(
        matches!(
            admit(
                &standing_restore(),
                Some(&expired),
                Some(&cluster(false)),
                Some(&inputs)
            ),
            RestoreAdmission::StandingAuthorizationRefused { .. }
        ),
        "and an expired authorization is reported as one even when the target is also down"
    );
}

/// The file table, asserted as a set of NAMES: a rehearsal bundle carries the
/// five standing members and NEVER a per-run approval slot.
#[test]
fn the_file_table_names_are_the_five_standing_members() {
    for member in [
        STANDING_AUTHORIZATION_FILE,
        STANDING_AUTHORIZATION_SIG_FILE,
        AUTHORIZATION_KEYS_FILE,
        ALLOWED_CLUSTERS_FILE,
        APPROVER_KEY_FILE,
    ] {
        assert!(!member.is_empty());
    }
    // THE SIDECAR KEEPS ITS OWN NAME. Mounting it under the approval's name
    // makes the runner verify it under `PAYLOAD_TYPE_APPROVAL`, and a
    // correctly signed rehearsal is then reported as a SUBSTITUTED approval.
    assert_ne!(STANDING_AUTHORIZATION_SIG_FILE, APPROVAL_SIG_FILE);
    assert_ne!(STANDING_AUTHORIZATION_FILE, APPROVAL_DOC_FILE);
    assert_eq!(
        STANDING_AUTHORIZATION_SIG_FILE,
        STANDING_AUTHORIZATION_FILE.replace(".json", ".sig"),
        "the runner DERIVES the sidecar path by replacing the extension"
    );
}
