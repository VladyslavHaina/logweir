//! PLAT-19.2 at the `Restore` controller: admission re-checks the approval
//! policy against the namespace's CURRENT binding, and a v2-authorised run
//! freezes the policy snapshot and the console key into its bundle, its
//! environment and its argv. A v1 Restore's Job is byte-for-byte unchanged.
//!
//! Admission reads no signature (the Approval controller and the runner do),
//! so the v2 documents below are built from the core type and carry an empty
//! sidecar under the v2 payload type; the keys are real PUBLIC halves so the
//! resolved trust parses them.

use chrono::{DateTime, TimeZone, Utc};
use k8s_openapi::api::batch::v1::Job;
use logweir_core::approval_policy::{
    ApprovalMode, ApprovalPolicySet, AuthorizedSubject, EffectivePolicy, PolicyRef, Requester,
    RestoreAuthorization, PAYLOAD_TYPE_RESTORE_AUTHORIZATION, RESTORE_AUTHORIZATION_FORMAT_VERSION,
    RESTORE_AUTHORIZATION_KIND,
};
use logweir_core::execution_contract as wire;
use logweir_core::ids::sha256_prefixed;
use serde_json::Value;
use weirkeeper::controllers::restore::{
    admit, admit_with_policy, approval_bundle_config_map, approval_bundle_config_map_with_policy,
    carries_authorization_v2, reconcile_restore_with_policy, runner_job_spec,
    runner_job_spec_with_policy, unobserved_scorecard, PolicyAdmission, RestoreAdmission,
    RestoreOutcome, APPROVAL_POLICY_ARG, APPROVAL_POLICY_FILE,
    BUNDLE_APPROVAL_POLICY_DIGEST_ANNOTATION, CONFIRMATION_KEY_ARG, CONFIRMATION_KEY_FILE,
};
use weirkeeper::crds::approval::Approval;
use weirkeeper::crds::kafka_cluster::KafkaCluster;
use weirkeeper::crds::restore::Restore;
use weirkeeper::crds::trust_policy::{
    KeyAlgorithm, KeyPrincipal, KeyState, KeyUsage as SpecUsage, TrustPolicy, TrustPolicySpec,
    TrustedKey as SpecKey,
};
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};

const NS: &str = "logweir-p192";
const NAME: &str = "rst-policy-1";
const UID: &str = "5c2e7b91-0000-4000-8000-00000000192a";
const APPROVAL: &str = "a1";
const APPROVAL_UID: &str = "aaaaaaaa-0000-4000-8000-00000000192a";

const PLAN_BYTES: &str = "\
source:
  storage:
    backend: s3
    bucket: kafka-backups
    prefix: drill-demo
    region: us-east-1
  backup: latestCompleted
  topics: [orders]
target:
  bootstrap_servers: [scratch-0.logweir-p192:9092]
  mode: scratch
  topic_mapping_prefix: \"drill-\"
  marker_topic: logweir.scratch
  default_replication_factor: 1
  teardown: delete
restore:
  point_in_time: \"2026-09-07T14:05:00Z\"
sample:
  window_start: \"2026-09-07T12:00:00Z\"
  window_end: \"2026-09-07T15:00:00Z\"
  records_per_partition: 25
  anchor: head
objectives:
  rto_seconds: 900
  rpo_seconds: 300
  pass_rate: 1.0
evidence:
  backend: s3
  bucket: logweir-evidence
  prefix: logweir/
  region: us-east-1
";

const CONSOLE_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEASz9/MJSvCOl4EX5VeUj14ZG3ZMrUBJNcOc8B/UKcKSg=\n-----END PUBLIC KEY-----\n";
const CONSOLE_KEY_ID: &str = "b374de19128351f9efe05a18b057ddf7fff085e4e36173ab4dc93ecf954868cf";
const BOB_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAft4R9Sg5WWc2L0pwch6/nRiUCDBoX690uVzHz9p121M=\n-----END PUBLIC KEY-----\n";
const BOB_KEY_ID: &str = "8c6653a5c05dded175b7d260a033eaaf92952ab29d0fbc5f5d773f6167c2cf27";

const POLICY_DOC: &str = "allowOrdinaryConfirmation: true
policies:
  - name: team-ordinary
    mode: Ordinary
  - name: prod-governed
    mode: Governed
";

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 22, 12, 0, 0)
        .single()
        .expect("instant")
}

fn bind(policy: &str) -> ApprovalPolicySet {
    ApprovalPolicySet::parse(&format!("{POLICY_DOC}namespaces:\n  {NS}: {policy}\n"))
        .expect("valid")
}

fn effective(policy: &str) -> EffectivePolicy {
    bind(policy).resolve(NS)
}

fn plan_hash() -> String {
    sha256_prefixed(PLAN_BYTES.as_bytes())
}

fn restore() -> Restore {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {"name": NAME, "namespace": NS, "uid": UID, "generation": 1,
                     "resourceVersion": "4071"},
        "spec": {
            "planBytes": PLAN_BYTES,
            "approvalRef": {"name": APPROVAL},
            "sourceArchive": {"url": "s3://kafka-backups/logweir"},
            "backupSetRef": "drill-demo",
            "pointInTime": "2026-09-07T14:05:00Z",
            "target": {"clusterRef": {"name": "scratch"}, "mode": "scratch",
                       "topicNaming": {"prefix": "drill-"}},
            "deadlineSeconds": 1800
        }
    }))
    .expect("a Restore")
}

fn cluster() -> KafkaCluster {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": {"name": "scratch", "namespace": NS, "uid": "cccccccc-0000-4000-8000-0000000192cc"},
        "spec": {"bootstrapServers": ["scratch-0.logweir-p192:9092"],
                 "auth": {"mode": "plaintext", "tls": false},
                 "role": "scratch", "markerTopic": "logweir.scratch"},
        "status": {"reachable": true, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk"}
    }))
    .expect("a KafkaCluster")
}

fn document(policy: &str, mode: ApprovalMode) -> RestoreAuthorization {
    let bound = effective(policy);
    let bound = bound.bound().expect("bound");
    RestoreAuthorization {
        format_version: RESTORE_AUTHORIZATION_FORMAT_VERSION.into(),
        kind: RESTORE_AUTHORIZATION_KIND.into(),
        authorization_mode: mode,
        subject: AuthorizedSubject {
            api_version: "logweir.dev/v1alpha1".into(),
            kind: "Restore".into(),
            namespace: NS.into(),
            name: NAME.into(),
            uid: UID.into(),
        },
        plan_hash: plan_hash(),
        requester: Requester {
            issuer: "https://idp.example".into(),
            subject: "alice".into(),
        },
        policy: PolicyRef {
            name: bound.name.clone(),
            digest: bound.digest(),
        },
        issued_at: now() - chrono::Duration::minutes(5),
        expires_at: now() + chrono::Duration::minutes(5),
        // D0: required under Governed, optional under Ordinary.
        ticket: (mode == ApprovalMode::Governed).then(|| "CHG-4711".to_string()),
    }
}

/// A VERIFIED Approval carrying `doc`, with the provenance the Approval
/// controller writes for a v2 verdict under `provenance_policy`.
fn v2_approval(doc: &RestoreAuthorization, provenance_policy: &str, matched: &str) -> Approval {
    let bound = effective(provenance_policy);
    let bound = bound.bound().expect("bound").clone();
    let bytes = String::from_utf8(doc.to_bytes()).expect("utf-8");
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Approval",
        "metadata": {"name": APPROVAL, "namespace": NS, "uid": APPROVAL_UID},
        "spec": {
            "subjectRef": {"kind": "Restore", "name": NAME},
            "planHash": plan_hash(),
            "approvalBytes": bytes,
            "sidecarBytes": format!(r#"{{"payloadType":"{PAYLOAD_TYPE_RESTORE_AUTHORIZATION}","signatures":[]}}"#),
        },
        "status": {
            "verified": true,
            "matchedKeyId": matched,
            "verifiedSubjectRef": {"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                                   "name": NAME, "namespace": NS, "uid": UID},
            "authorization": {
                "mode": bound.mode.as_str(),
                "policyName": bound.name,
                "policyDigest": bound.digest(),
                "requester": "https://idp.example#alice",
                "confirmationKeyId": CONSOLE_KEY_ID
            },
            "conditions": [{"type": "Verified", "status": "True", "reason": "Verified"}]
        }
    }))
    .expect("an Approval")
}

fn ordinary_approval() -> Approval {
    v2_approval(
        &document("team-ordinary", ApprovalMode::Ordinary),
        "team-ordinary",
        CONSOLE_KEY_ID,
    )
}

fn governed_approval() -> Approval {
    v2_approval(
        &document("prod-governed", ApprovalMode::Governed),
        "prod-governed",
        BOB_KEY_ID,
    )
}

/// Today's v1 approval, verified — what every existing Approval is.
fn v1_approval() -> Approval {
    let doc = format!(
        r#"{{"approver":"ops@example.com","ticket":"CHG-1","plan_hash":"{}","subject_kind":"Restore"}}"#,
        plan_hash()
    );
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Approval",
        "metadata": {"name": APPROVAL, "namespace": NS, "uid": APPROVAL_UID},
        "spec": {
            "subjectRef": {"kind": "Restore", "name": NAME},
            "planHash": plan_hash(),
            "approvalBytes": doc,
            "sidecarBytes": r#"{"payloadType":"application/vnd.logweir.drill-approval+json;version=1.0.0","signatures":[]}"#,
        },
        "status": {
            "verified": true,
            "matchedKeyId": BOB_KEY_ID,
            "verifiedSubjectRef": {"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                                   "name": NAME, "namespace": NS, "uid": UID},
            "conditions": [{"type": "Verified", "status": "True", "reason": "Verified"}]
        }
    }))
    .expect("an Approval")
}

fn admit_under(approval: &Approval, policy: &str) -> RestoreAdmission {
    let bound = if policy.is_empty() {
        EffectivePolicy::Legacy
    } else {
        effective(policy)
    };
    admit_with_policy(
        &restore(),
        Some(approval),
        Some(&cluster()),
        None,
        Some(&PolicyAdmission {
            policy: &bound,
            now: now(),
        }),
    )
}

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

#[test]
fn both_modes_admit_a_verified_document_naming_the_current_binding() {
    assert_eq!(
        admit_under(&ordinary_approval(), "team-ordinary"),
        RestoreAdmission::Ok
    );
    assert_eq!(
        admit_under(&governed_approval(), "prod-governed"),
        RestoreAdmission::Ok
    );
}

/// **Existing Approvals stay valid**: an unbound namespace admits today's v1
/// approval exactly as `admit` always has.
#[test]
fn an_unbound_namespace_admits_todays_approval_unchanged() {
    assert_eq!(admit_under(&v1_approval(), ""), RestoreAdmission::Ok);
    assert_eq!(
        admit(&restore(), Some(&v1_approval()), Some(&cluster()), None),
        RestoreAdmission::Ok
    );
}

/// The binding decides, never what the document says: a v1 approval under
/// an explicit binding, and a v2 document in an unbound namespace, are both
/// refused before any Job — even with a (stale or forged) `verified: true`.
#[test]
fn the_format_must_match_the_binding() {
    for (approval, policy) in [
        (v1_approval(), "prod-governed"),
        (v1_approval(), "team-ordinary"),
        (ordinary_approval(), ""),
    ] {
        let admission = admit_under(&approval, policy);
        assert!(
            matches!(
                admission,
                RestoreAdmission::AuthorizationPolicyMismatch { .. }
            ),
            "{policy:?}: {admission:?}"
        );
        assert!(admission.is_terminal());
        assert_eq!(admission.reason(), "ApprovalPolicyMismatch");
    }
}

/// **The downgrade, at admission.** An ordinary confirmation (whatever its
/// status claims) for a Restore in a namespace bound Governed never runs.
#[test]
fn an_ordinary_confirmation_never_admits_a_governed_namespace() {
    let admission = admit_under(&ordinary_approval(), "prod-governed");
    assert_eq!(admission.reason(), "ApprovalPolicyMismatch", "{admission}");
    // …and a status that CLAIMS the governed policy does not rescue a document
    // that names the ordinary one.
    let forged = v2_approval(
        &document("team-ordinary", ApprovalMode::Ordinary),
        "prod-governed",
        BOB_KEY_ID,
    );
    assert_eq!(
        admit_under(&forged, "prod-governed").reason(),
        "ApprovalPolicyMismatch"
    );
}

/// A policy rollout between verification and admission: the document and the
/// status both name the OLD digest, the binding is new.
#[test]
fn a_policy_changed_since_verification_admits_nothing() {
    let edited = ApprovalPolicySet::parse(&format!(
        "{}namespaces:\n  {NS}: team-ordinary\n",
        POLICY_DOC.replace(
            "mode: Ordinary\n",
            "mode: Ordinary\n    maxAgeSeconds: 901\n"
        )
    ))
    .expect("valid")
    .resolve(NS);
    let admission = admit_with_policy(
        &restore(),
        Some(&ordinary_approval()),
        Some(&cluster()),
        None,
        Some(&PolicyAdmission {
            policy: &edited,
            now: now(),
        }),
    );
    assert_eq!(admission.reason(), "ApprovalPolicyMismatch", "{admission}");
}

/// The provenance must be the Approval controller's verdict under THIS
/// binding: a verified v2 Approval whose status carries none admits nothing.
#[test]
fn a_verdict_without_current_provenance_admits_nothing() {
    let mut approval = ordinary_approval();
    approval.status.as_mut().expect("status").authorization = None;
    assert_eq!(
        admit_under(&approval, "team-ordinary").reason(),
        "ApprovalPolicyMismatch"
    );
}

#[test]
fn expiry_before_admission_is_terminal() {
    let mut doc = document("team-ordinary", ApprovalMode::Ordinary);
    doc.issued_at = now() - chrono::Duration::minutes(15) - chrono::Duration::seconds(1);
    doc.expires_at = now() - chrono::Duration::seconds(1);
    let admission = admit_under(
        &v2_approval(&doc, "team-ordinary", CONSOLE_KEY_ID),
        "team-ordinary",
    );
    assert_eq!(admission.reason(), "AuthorizationExpired", "{admission}");
    assert!(admission.is_terminal());
}

/// The Approval controller's own permanent v2 refusals end the Restore
/// instead of holding it for ever; its pending state holds.
#[test]
fn the_approvals_permanent_refusals_end_the_restore_and_pending_holds() {
    for (reason, want) in [
        ("AuthorizationExpired", "AuthorizationExpired"),
        ("ApprovalPolicyMismatch", "ApprovalPolicyMismatch"),
        ("GovernedApprovalRequired", "ApprovalNotVerified"),
        ("SelfApprovalRefused", "ApprovalNotVerified"),
    ] {
        let mut approval = governed_approval();
        let status = approval.status.as_mut().expect("status");
        status.verified = Some(false);
        status.authorization = None;
        status.conditions = Some(vec![weirkeeper::crds::Condition {
            r#type: "Verified".into(),
            status: "False".into(),
            observed_generation: None,
            last_transition_time: None,
            reason: Some(reason.into()),
            message: Some(format!("the Approval says {reason}")),
        }]);
        let admission = admit_under(&approval, "prod-governed");
        assert_eq!(admission.reason(), want, "{reason}: {admission}");
    }
}

#[test]
fn a_changed_plan_or_a_recreated_subject_is_refused_from_the_signed_bytes() {
    let mut doc = document("team-ordinary", ApprovalMode::Ordinary);
    doc.plan_hash = sha256_prefixed(b"another plan");
    let mut approval = v2_approval(&doc, "team-ordinary", CONSOLE_KEY_ID);
    approval.spec.plan_hash = doc.plan_hash.clone();
    assert_eq!(
        admit_under(&approval, "team-ordinary").reason(),
        "PlanHashMismatch"
    );

    let mut doc = document("team-ordinary", ApprovalMode::Ordinary);
    doc.subject.uid = "an-older-restore".into();
    let admission = admit_under(
        &v2_approval(&doc, "team-ordinary", CONSOLE_KEY_ID),
        "team-ordinary",
    );
    assert_eq!(admission.reason(), "ApprovalSubjectMismatch", "{admission}");
}

// ---------------------------------------------------------------------------
// The frozen execution inputs
// ---------------------------------------------------------------------------

fn key(key_id: &str, pem: &str, usage: SpecUsage, principal: &str) -> SpecKey {
    SpecKey {
        key_id: key_id.into(),
        spki_pem: pem.into(),
        algorithm: KeyAlgorithm::Ed25519,
        usages: vec![usage],
        principal: KeyPrincipal {
            id: principal.into(),
            display: None,
        },
        not_before: Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("instant"),
        not_after: Utc
            .with_ymd_and_hms(2099, 1, 1, 0, 0, 0)
            .single()
            .expect("instant"),
        state: KeyState::Active,
        retired_at: None,
        revoked_at: None,
        revocation_reason: None,
        revocation_effective_from: None,
    }
}

fn trust_policy(keys: Vec<SpecKey>) -> TrustPolicy {
    TrustPolicy {
        metadata: kube::api::ObjectMeta {
            name: Some("org-default".into()),
            uid: Some("uid-org-default".into()),
            generation: Some(1),
            resource_version: Some("17".into()),
            ..kube::api::ObjectMeta::default()
        },
        spec: TrustPolicySpec {
            default: false,
            namespaces: Some(vec![NS.into()]),
            allowed_target_cluster_ids: Some(vec!["MkU3OEVBNTcwNTJENDM2Qk".into()]),
            keys,
        },
        status: None,
    }
}

fn keys() -> Vec<SpecKey> {
    vec![
        key(
            CONSOLE_KEY_ID,
            CONSOLE_PEM,
            SpecUsage::ConsoleConfirmation,
            "logweir-api:console",
        ),
        key(
            BOB_KEY_ID,
            BOB_PEM,
            SpecUsage::GovernedApproval,
            "https://idp.example#bob",
        ),
    ]
}

fn trust() -> weirkeeper::trust::ResolvedTrust {
    weirkeeper::trust::from_policy(&trust_policy(keys()))
}

fn env_map(spec: &weirkeeper::job::RunnerJobSpec) -> std::collections::BTreeMap<String, String> {
    spec.env_literal.iter().cloned().collect()
}

#[test]
fn an_ordinary_run_freezes_the_snapshot_and_the_console_key_as_its_authoriser() {
    let bound = effective("team-ordinary");
    let bundle = approval_bundle_config_map_with_policy(
        &restore(),
        &ordinary_approval(),
        &trust(),
        &bound,
        now(),
    )
    .expect("v2 materializes");
    let data = bundle.data.as_ref().expect("data");
    assert_eq!(data.len(), 6, "{:?}", data.keys().collect::<Vec<_>>());
    let policy = bound.bound().expect("bound");
    assert_eq!(
        data[APPROVAL_POLICY_FILE].as_bytes(),
        policy.snapshot_bytes().as_slice(),
        "the snapshot is exactly the bytes whose digest the document names"
    );
    assert_eq!(data[CONFIRMATION_KEY_FILE], CONSOLE_PEM);
    assert_eq!(
        data["approver.pub.pem"], CONSOLE_PEM,
        "under Ordinary the console key IS the authoriser"
    );
    assert_eq!(
        bundle.metadata.annotations.as_ref().expect("annotations")
            [BUNDLE_APPROVAL_POLICY_DIGEST_ANNOTATION],
        policy.digest()
    );
    assert!(data.values().all(|v| !v.contains("PRIVATE KEY")));
}

#[test]
fn a_governed_run_mounts_the_approver_and_the_console_key_and_pins_both() {
    let bound = effective("prod-governed");
    let restore = restore();
    let approval = governed_approval();
    assert!(carries_authorization_v2(&restore, &approval, &bound));
    let spec = runner_job_spec_with_policy(
        &restore,
        &cluster(),
        &[BOB_KEY_ID.to_string()],
        &approval,
        &trust(),
        &bound,
        now(),
        None,
    )
    .expect("a Job spec");
    let bundle =
        approval_bundle_config_map_with_policy(&restore, &approval, &trust(), &bound, now())
            .expect("bundle");
    let data = bundle.data.expect("data");
    assert_eq!(
        data["approver.pub.pem"], BOB_PEM,
        "the governed approver authorised it"
    );
    assert_eq!(data[CONFIRMATION_KEY_FILE], CONSOLE_PEM);

    let env = env_map(&spec);
    assert_eq!(
        env.get(wire::POLICY_SNAPSHOT_SHA256_ENV),
        Some(&sha256_prefixed(data[APPROVAL_POLICY_FILE].as_bytes()))
    );
    assert_eq!(
        env.get(wire::CONFIRMATION_KEY_SHA256_ENV),
        Some(&sha256_prefixed(data[CONFIRMATION_KEY_FILE].as_bytes()))
    );
    let argv = spec.args.join(" ");
    assert!(
        argv.contains(&format!(
            "{APPROVAL_POLICY_ARG} /approval/{APPROVAL_POLICY_FILE}"
        )),
        "{argv}"
    );
    assert!(
        argv.contains(&format!(
            "{CONFIRMATION_KEY_ARG} /approval/{CONFIRMATION_KEY_FILE}"
        )),
        "{argv}"
    );
    let mounted: Vec<&str> = spec.config_map_mounts[0]
        .items
        .iter()
        .map(|(k, _)| k.as_str())
        .collect();
    assert!(mounted.contains(&APPROVAL_POLICY_FILE) && mounted.contains(&CONFIRMATION_KEY_FILE));
}

/// A v1 Restore's Job is byte-for-byte what it was, whatever policy is
/// configured for OTHER namespaces.
#[test]
fn a_v1_run_is_unchanged_by_the_policy_feature() {
    let legacy = EffectivePolicy::Legacy;
    let old = runner_job_spec(
        &restore(),
        &cluster(),
        &[BOB_KEY_ID.to_string()],
        &v1_approval(),
        &trust(),
        now(),
    )
    .expect("old path");
    let new = runner_job_spec_with_policy(
        &restore(),
        &cluster(),
        &[BOB_KEY_ID.to_string()],
        &v1_approval(),
        &trust(),
        &legacy,
        now(),
        None,
    )
    .expect("new path");
    assert_eq!(old, new);
    let env = env_map(&new);
    assert!(!env.contains_key(wire::POLICY_SNAPSHOT_SHA256_ENV));
    assert!(!env.contains_key(wire::CONFIRMATION_KEY_SHA256_ENV));
    assert!(!new
        .args
        .iter()
        .any(|a| a == APPROVAL_POLICY_ARG || a == CONFIRMATION_KEY_ARG));
    let bundle =
        approval_bundle_config_map(&restore(), &v1_approval(), &trust(), now()).expect("bundle");
    assert_eq!(bundle.data.expect("data").len(), 4);
}

/// Each frozen key must still be able to sign something NEW under its own
/// usage when the bundle is written.
#[test]
fn a_withdrawn_or_wrong_usage_key_is_never_mounted() {
    let bound = effective("team-ordinary");
    let mut revoked = keys();
    revoked[0].state = KeyState::Retired;
    revoked[0].retired_at = Some(now() - chrono::Duration::hours(1));
    let refused = approval_bundle_config_map_with_policy(
        &restore(),
        &ordinary_approval(),
        &weirkeeper::trust::from_policy(&trust_policy(revoked)),
        &bound,
        now(),
    );
    assert!(
        refused.is_err(),
        "a retired console key confirms nothing new"
    );

    // Under Ordinary, an approval whose matched key is a GOVERNED approver's
    // is not an ordinary confirmation.
    let wrong = v2_approval(
        &document("team-ordinary", ApprovalMode::Ordinary),
        "team-ordinary",
        BOB_KEY_ID,
    );
    let refused =
        approval_bundle_config_map_with_policy(&restore(), &wrong, &trust(), &bound, now());
    assert!(
        refused.is_err(),
        "the authorising key must hold the bound mode's usage"
    );

    // UNDER GOVERNED, THE CONSOLE KEY IS THE SECOND FROZEN KEY and is
    // re-checked on its own: a valid approver does not carry a console key
    // retired since the confirmation was signed. The negative control is the
    // same approval under the unchanged trust, which mounts.
    let governed = effective("prod-governed");
    approval_bundle_config_map_with_policy(
        &restore(),
        &governed_approval(),
        &trust(),
        &governed,
        now(),
    )
    .expect("the governed control mounts under the unchanged trust");
    let refused = approval_bundle_config_map_with_policy(
        &restore(),
        &governed_approval(),
        &weirkeeper::trust::from_policy(&trust_policy(revoked_console())),
        &governed,
        now(),
    );
    let message = format!(
        "{:?}",
        refused.expect_err("a retired console key mounts nothing")
    );
    assert!(
        message.contains(CONSOLE_KEY_ID),
        "the refusal names the console key: {message}"
    );
}

/// [`keys`] with the console key retired an hour ago.
fn revoked_console() -> Vec<SpecKey> {
    let mut keys = keys();
    keys[0].state = KeyState::Retired;
    keys[0].retired_at = Some(now() - chrono::Duration::hours(1));
    keys
}

// ---------------------------------------------------------------------------
// The reconciler: direct-resource admission
// ---------------------------------------------------------------------------

fn not_found(kind: &str, name: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"{kind} \"{name}\" not found","reason":"NotFound","code":404}}"#
    )
}

fn routes(approval: &Approval) -> Vec<Route> {
    let list = serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustPolicyList",
        "metadata": {"resourceVersion": "1"}, "items": [trust_policy(keys())],
    });
    let job = serde_json::json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": NAME, "namespace": NS, "uid": "bbbbbbbb-0000-4000-8000-0000000192bb",
                     "ownerReferences": [{"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                                          "name": NAME, "uid": UID, "controller": true,
                                          "blockOwnerDeletion": true}]},
        "spec": {"template": {"spec": {"containers": []}}}
    });
    vec![
        Route {
            method: "GET",
            path_suffix: "/jobs/rst-policy-1",
            status: 404,
            body: not_found("jobs.batch", NAME),
        },
        Route {
            method: "GET",
            path_suffix: "/approvals/a1",
            status: 200,
            body: serde_json::to_string(approval).expect("json"),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/scratch",
            status: 200,
            body: serde_json::to_string(&cluster()).expect("json"),
        },
        Route {
            method: "GET",
            path_suffix: "/trustpolicies",
            status: 200,
            body: list.to_string(),
        },
        // Both creates answer with an object this Restore owns — the plan
        // ConfigMap is the one `has_exact_restore_owner_set` reads.
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: serde_json::to_string(
                &weirkeeper::controllers::restore::plan_config_map(&restore()).expect("plan"),
            )
            .expect("json"),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: job.to_string(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/restores/rst-policy-1/status",
            status: 200,
            body: serde_json::to_string(&restore()).expect("json"),
        },
    ]
}

async fn reconcile(
    approval: &Approval,
    policies: &ApprovalPolicySet,
) -> (RestoreOutcome, Vec<SeenBody>) {
    let (client, _recorder, bodies) = mock_client_recording_bodies(routes(approval));
    let outcome = reconcile_restore_with_policy(
        &restore(),
        &client,
        &unobserved_scorecard,
        &weirkeeper::verification::unverified_evidence,
        now(),
        &weirkeeper::job::RunnerImage::default(),
        policies,
    )
    .await
    .expect("an outcome");
    let seen = bodies.lock().expect("recorder").clone();
    (outcome, seen)
}

fn posts(bodies: &[SeenBody], suffix: &str) -> Vec<Value> {
    bodies
        .iter()
        .filter(|b| {
            b.method == "POST" && b.uri.split('?').next().is_some_and(|p| p.ends_with(suffix))
        })
        .map(|b| serde_json::from_str(&b.body).unwrap_or(Value::Null))
        .collect()
}

/// **Direct-resource admission.** A v1 approval written straight to the API
/// server for a Restore in a namespace bound to an explicit policy persists,
/// is refused terminally, and causes ZERO ConfigMap and Job POSTs.
#[tokio::test]
async fn a_directly_written_v1_approval_in_a_bound_namespace_creates_nothing() {
    let (outcome, bodies) = reconcile(&v1_approval(), &bind("prod-governed")).await;
    assert_eq!(
        outcome.terminal_state.as_deref(),
        Some("ApprovalPolicyMismatch")
    );
    assert!(!outcome.created);
    assert!(posts(&bodies, "/configmaps").is_empty());
    assert!(posts(&bodies, "/jobs").is_empty());
}

/// And the ordinary path, end to end at the reconciler: the Job it creates
/// pins the frozen policy snapshot and the console key.
#[tokio::test]
async fn an_ordinary_confirmation_creates_a_job_that_pins_the_frozen_policy() {
    let (outcome, bodies) = reconcile(&ordinary_approval(), &bind("team-ordinary")).await;
    assert!(outcome.created, "{outcome:?}");
    let jobs = posts(&bodies, "/jobs");
    assert_eq!(jobs.len(), 1);
    let job: Job = serde_json::from_value(jobs[0].clone()).expect("a Job");
    let container = &job
        .spec
        .expect("spec")
        .template
        .spec
        .expect("pod")
        .containers[0];
    let env: std::collections::BTreeMap<String, String> = container
        .env
        .clone()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|e| e.value.map(|v| (e.name, v)))
        .collect();
    let snapshot = effective("team-ordinary")
        .bound()
        .expect("bound")
        .snapshot_bytes();
    assert_eq!(
        env.get(wire::POLICY_SNAPSHOT_SHA256_ENV),
        Some(&sha256_prefixed(&snapshot))
    );
    assert_eq!(
        env.get(wire::CONFIRMATION_KEY_SHA256_ENV),
        Some(&sha256_prefixed(CONSOLE_PEM.as_bytes()))
    );
    let args = container.args.clone().unwrap_or_default().join(" ");
    assert!(
        args.contains(APPROVAL_POLICY_ARG) && args.contains(CONFIRMATION_KEY_ARG),
        "{args}"
    );
}
