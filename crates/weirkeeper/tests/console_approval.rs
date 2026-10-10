//! PROD-16.2 at the controller — **two-person approval in the console**, at
//! both of its checkpoints: the `Approval` verdict
//! (`controllers::approval::evaluate_authorization_v2`) and `Restore`
//! admission (`controllers::restore::admit_with_policy`, then the bundle the
//! runner is given).
//!
//! Under a policy whose `approverSignature` is `Console` there is no second
//! key. The second PERSON is inside the bytes the console signed, and each
//! checkpoint re-derives, from those bytes and nothing else: the console's
//! signature, an approver in `<issuer>#<subject>` form who is a second person
//! of the requester's own issuer (never the local administrator, never a
//! service account), and an `approvedAt` inside the request's window.
//!
//! # Where the signed material comes from
//!
//! This crate cannot sign (`scripts/check-one-signer.sh`). The documents and
//! signatures are `tests/fixtures/console-approval.json`, written by
//! `crates/logweir/tests/authorization_v2.rs`
//! (`write_the_console_approval_fixture_for_the_controllers_rows`) with
//! throwaway keys that existed only in that process; the file holds public
//! halves, key ids, documents and signatures. That crate's own row reads the
//! same file through the runner's verifier, so the runner and the controller
//! are held to one set of bytes.

use chrono::{DateTime, Duration, Utc};
use kube::api::ObjectMeta;
use logweir_core::approval_policy::{
    ApprovalPolicy, ApprovalPolicySet, ApproverSignature, EffectivePolicy, ExpectedSubject,
};
use weirkeeper::controllers::approval::{self, ApprovalRefusal, Verified};
use weirkeeper::controllers::restore::{
    admit_with_policy, approval_bundle_config_map_with_policy, PolicyAdmission, RestoreAdmission,
    APPROVAL_POLICY_FILE, CONFIRMATION_KEY_FILE,
};
use weirkeeper::crds::approval::Approval;
use weirkeeper::crds::kafka_cluster::KafkaCluster;
use weirkeeper::crds::restore::Restore;
use weirkeeper::crds::trust_policy::{
    KeyAlgorithm, KeyPrincipal, KeyState, KeyUsage as SpecUsage, RevocationReason, TrustPolicy,
    TrustPolicySpec, TrustedKey as SpecKey,
};

const FIXTURE: &str = include_str!("fixtures/console-approval.json");

/// The fixture, parsed once per use.
struct Fixture(serde_json::Value);

fn fixture() -> Fixture {
    Fixture(serde_json::from_str(FIXTURE).expect("the fixture is JSON"))
}

impl Fixture {
    fn text(&self, field: &str) -> String {
        self.0[field]
            .as_str()
            .unwrap_or_else(|| panic!("the fixture has a string {field}"))
            .to_string()
    }

    fn instant(&self, field: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(&self.text(field))
            .expect("a fixture instant")
            .with_timezone(&Utc)
    }

    fn document(&self, case: &str) -> String {
        self.0["cases"][case]["document"]
            .as_str()
            .unwrap_or_else(|| panic!("the fixture has case {case}"))
            .to_string()
    }

    fn sidecar(&self, case: &str) -> String {
        self.0["cases"][case]["sidecar"]
            .as_str()
            .unwrap_or_else(|| panic!("the fixture has case {case}"))
            .to_string()
    }

    /// The installation document with the namespace bound to `policy`.
    fn bind(&self, policy: &str) -> ApprovalPolicySet {
        ApprovalPolicySet::parse(&format!(
            "{}namespaces:\n  {}: {policy}\n",
            self.text("policyDocument"),
            self.text("namespace")
        ))
        .expect("the fixture's policy document validates")
    }

    fn policy(&self, name: &str) -> ApprovalPolicy {
        self.bind(name)
            .resolve(&self.text("namespace"))
            .bound()
            .cloned()
            .expect("bound")
    }

    fn pair(&self) -> ApprovalPolicy {
        self.policy(&self.text("twoPersonPolicy"))
    }

    fn strict(&self) -> ApprovalPolicy {
        self.policy(&self.text("strictPolicy"))
    }

    fn now(&self) -> DateTime<Utc> {
        self.instant("now")
    }

    fn expected(&self, plan: &str) -> ExpectedSubject {
        ExpectedSubject {
            namespace: self.text("namespace"),
            name: self.text("restore"),
            uid: self.text("restoreUid"),
            plan_hash: logweir_core::ids::sha256_prefixed(self.text(plan).as_bytes()),
        }
    }

    fn console_key(&self) -> SpecKey {
        key(
            &self.text("consoleKeyId"),
            &self.text("consolePublicPem"),
            SpecUsage::ConsoleConfirmation,
            "console:logweir-system/logweir-console-confirmation",
        )
    }

    /// A personal `GovernedApproval` key, held by `bob`.
    fn personal_key(&self) -> SpecKey {
        key(
            &self.text("approverKeyId"),
            &self.text("approverPublicPem"),
            SpecUsage::GovernedApproval,
            "https://idp.example#bob",
        )
    }
}

fn algorithm(pem: &str) -> KeyAlgorithm {
    // The fixture's console key is Ed25519 and its personal key is P-256;
    // the SPKI's length tells them apart without parsing anything.
    if pem.len() < 130 {
        KeyAlgorithm::Ed25519
    } else {
        KeyAlgorithm::P256
    }
}

fn key(key_id: &str, pem: &str, usage: SpecUsage, principal: &str) -> SpecKey {
    let at = |s: &str| {
        DateTime::parse_from_rfc3339(s)
            .expect("an instant")
            .with_timezone(&Utc)
    };
    SpecKey {
        key_id: key_id.to_string(),
        spki_pem: pem.to_string(),
        algorithm: algorithm(pem),
        usages: vec![usage],
        principal: KeyPrincipal {
            id: principal.to_string(),
            display: None,
        },
        not_before: at("2026-01-01T00:00:00Z"),
        not_after: at("2099-01-01T00:00:00Z"),
        state: KeyState::Active,
        retired_at: None,
        revoked_at: None,
        revocation_reason: None,
        revocation_effective_from: None,
    }
}

fn trust(f: &Fixture, keys: Vec<SpecKey>) -> weirkeeper::trust::ResolvedTrust {
    weirkeeper::trust::from_policy(&TrustPolicy {
        metadata: ObjectMeta {
            name: Some("org-default".to_string()),
            uid: Some("uid-org-default".to_string()),
            generation: Some(1),
            resource_version: Some("17".to_string()),
            ..ObjectMeta::default()
        },
        spec: TrustPolicySpec {
            default: false,
            namespaces: Some(vec![f.text("namespace")]),
            allowed_target_cluster_ids: Some(vec!["scratch-cluster-id".to_string()]),
            keys,
        },
        status: None,
    })
}

/// The Approval verdict over one fixture case, under `bound`, at `now`, with
/// only the console key trusted unless `keys` says otherwise.
fn verdict_with(
    f: &Fixture,
    case: &str,
    bound: &ApprovalPolicy,
    now: DateTime<Utc>,
    keys: Vec<SpecKey>,
) -> Result<Verified, ApprovalRefusal> {
    approval::evaluate_authorization_v2(
        f.document(case).as_bytes(),
        f.sidecar(case).as_bytes(),
        &trust(f, keys),
        now,
        &f.expected("planBytes"),
        bound,
    )
}

fn verdict(f: &Fixture, case: &str) -> Result<Verified, ApprovalRefusal> {
    verdict_with(f, case, &f.pair(), f.now(), vec![f.console_key()])
}

fn reason(result: &Result<Verified, ApprovalRefusal>) -> &'static str {
    match result {
        Ok(_) => "Verified",
        Err(r) => r.reason(),
    }
}

fn words(result: &Result<Verified, ApprovalRefusal>) -> String {
    match result {
        Ok(_) => String::new(),
        Err(r) => r.to_string(),
    }
}

// ---------------------------------------------------------------------------
// The fixture is what it says
// ---------------------------------------------------------------------------

#[test]
fn the_fixture_names_the_policies_this_build_computes_and_holds_no_private_key() {
    let f = fixture();
    assert!(!FIXTURE.contains("PRIVATE"), "public halves only");
    assert_eq!(f.pair().approver_signature, ApproverSignature::Console);
    assert_eq!(
        f.strict().approver_signature,
        ApproverSignature::PersonalKey
    );
    assert!(f.document("approved").contains(&f.pair().digest()));
    assert!(f.document("request").contains(&f.pair().digest()));
    assert!(f.document("strict-request").contains(&f.strict().digest()));
    assert!(f
        .document("approved")
        .starts_with("{\"formatVersion\":\"2.2.0\","));
    assert!(f
        .document("request")
        .starts_with("{\"formatVersion\":\"2.0.0\","));
    assert!(f
        .document("original-name-request")
        .starts_with("{\"formatVersion\":\"2.1.0\","));
}

// ---------------------------------------------------------------------------
// The Approval verdict
// ---------------------------------------------------------------------------

/// **A second person's approval in the console verifies on the console's
/// signature alone**, under its `ConsoleConfirmation` usage, and the verdict
/// names the APPROVER's principal — what `status.approver` and the scorecard
/// carry. No `GovernedApproval` key is trusted here at all.
///
/// KILLS: a controller that still asks a two-person policy for a personal
/// countersignature; one that records the requester as the approver.
#[test]
fn a_console_approval_verifies_on_the_consoles_signature_and_names_the_approver() {
    let f = fixture();
    let verified = verdict(&f, "approved").expect("a console approval verifies");
    assert_eq!(verified.matched_key_id, f.text("consoleKeyId"));
    assert_eq!(
        verified.authorization_usage,
        logweir_core::trust::KeyUsage::ConsoleConfirmation
    );
    assert_eq!(verified.approver, "https://idp.example#bob");
    assert_eq!(verified.ticket, "CHG-4711");
    assert!(!verified.self_attested_risk);
    let provenance = verified.authorization.expect("v2 publishes its provenance");
    assert_eq!(provenance.mode, "Governed");
    assert_eq!(provenance.policy_name, f.text("twoPersonPolicy"));
    assert_eq!(provenance.policy_digest, f.pair().digest());
    assert_eq!(provenance.requester, "https://idp.example#alice");
    assert_eq!(provenance.confirmation_key_id, f.text("consoleKeyId"));
    assert_eq!(verified.document_expires_at, Some(f.instant("expiresAt")));
    // An issuer spelled with a trailing slash is the same issuer, so its
    // second subject is still a second person.
    assert_eq!(
        reason(&verdict(
            &f,
            "approved-by-a-second-person-behind-a-trailing-slash"
        )),
        "Verified"
    );
}

/// **The request is not an approval.** The document the approver was shown,
/// written straight into the Approval the Restore references, stays pending
/// for ever — and a personal-key countersignature on it changes nothing: a
/// two-person namespace takes no personal-key document.
#[test]
fn a_two_person_request_authorises_nothing_with_or_without_a_personal_countersignature() {
    let f = fixture();
    let pending = verdict(&f, "request");
    assert_eq!(reason(&pending), "GovernedApprovalRequired", "{pending:?}");
    assert!(
        words(&pending).contains("nobody has approved")
            && words(&pending).contains("personal-key countersignature is not accepted"),
        "{}",
        words(&pending)
    );
    // The same request, countersigned by a TRUSTED GovernedApproval key whose
    // principal is not the requester's: under a personal-key policy this is a
    // complete approval. Here it is still the request.
    let countersigned = verdict_with(
        &f,
        "request-countersigned-by-a-personal-key",
        &f.pair(),
        f.now(),
        vec![f.console_key(), f.personal_key()],
    );
    assert_eq!(
        reason(&countersigned),
        "GovernedApprovalRequired",
        "{countersigned:?}"
    );
    // An expired request reads as expired, as a strict one does.
    let expired = verdict_with(
        &f,
        "request",
        &f.pair(),
        f.instant("expiresAt"),
        vec![f.console_key()],
    );
    assert_eq!(reason(&expired), "AuthorizationExpired", "{expired:?}");
}

/// **THE IDENTITY RULE, AT THE APPROVAL VERDICT.** Each case carries the
/// console's valid signature, so only the rule can refuse it.
#[test]
fn the_approval_verdict_refuses_an_approver_who_is_not_a_second_person() {
    let f = fixture();
    for (case, needle) in [
        ("approved-by-requester", "is the requester"),
        ("approved-by-requester-in-another-case", "is the requester"),
        (
            "approved-by-requester-behind-a-trailing-slash",
            "is the requester",
        ),
        (
            "approved-by-the-same-subject-of-another-issuer",
            "two issuers",
        ),
        (
            "approved-by-a-subject-with-a-trailing-space",
            "not in a form that can be compared",
        ),
        (
            "approved-by-a-decomposed-spelling",
            "not in a form that can be compared",
        ),
        ("approved-by-the-local-admin", "administrator console"),
        ("requested-by-the-local-admin", "administrator console"),
        (
            "requested-by-a-service-account",
            "Kubernetes system identity",
        ),
    ] {
        let refused = verdict(&f, case);
        assert_eq!(
            reason(&refused),
            "SelfApprovalRefused",
            "{case}: {refused:?}"
        );
        assert!(
            words(&refused).contains(needle),
            "{case}: {}",
            words(&refused)
        );
    }
    // THE CONTROL is `a_console_approval_verifies_...` above: the same
    // document with a second person.
}

/// **THE CLOCK RULE, AT THE APPROVAL VERDICT**: `issuedAt <= approvedAt <
/// expiresAt` inside the signed bytes, an `approvedAt` no more than 60 s
/// ahead of the controller's clock, and the request's own expiry.
#[test]
fn the_approval_verdict_holds_approved_at_to_the_window_and_the_clock() {
    let f = fixture();
    for case in [
        "approved-before-the-request",
        "approved-at-the-expiry",
        "approved-ahead-of-the-controllers-clock",
    ] {
        let refused = verdict(&f, case);
        assert_eq!(
            reason(&refused),
            "AuthorizationWindowInvalid",
            "{case}: {refused:?}"
        );
    }
    // The approval that is 61 s ahead of the clock at `now` is fine two
    // seconds later: what refused it was the clock, not the document.
    let later = verdict_with(
        &f,
        "approved-ahead-of-the-controllers-clock",
        &f.pair(),
        f.now() + Duration::seconds(2),
        vec![f.console_key()],
    );
    assert_eq!(reason(&later), "Verified", "{later:?}");
    // The approved document itself: inside the window, then expired.
    let last_second = verdict_with(
        &f,
        "approved",
        &f.pair(),
        f.instant("expiresAt") - Duration::seconds(1),
        vec![f.console_key()],
    );
    assert_eq!(reason(&last_second), "Verified", "{last_second:?}");
    let expired = verdict_with(
        &f,
        "approved",
        &f.pair(),
        f.instant("expiresAt"),
        vec![f.console_key()],
    );
    assert_eq!(reason(&expired), "AuthorizationExpired", "{expired:?}");
}

/// **The fields are format 2.2.0 at the controller**, and come together.
#[test]
fn the_approval_verdict_refuses_the_approver_fields_under_an_older_version() {
    let f = fixture();
    for (case, needle) in [
        ("approver-under-2-1-0", "defined from formatVersion 2.2.0"),
        ("approver-under-2-0-0", "defined from formatVersion 2.2.0"),
        ("approver-without-the-instant", "without the other"),
    ] {
        let refused = verdict(&f, case);
        assert_eq!(
            reason(&refused),
            "AuthorizationDocumentInvalid",
            "{case}: {refused:?}"
        );
        assert!(
            words(&refused).contains(needle),
            "{case}: {}",
            words(&refused)
        );
    }
}

/// **An approval binds one Restore, one plan, one policy** — the UID
/// included, so it never follows a Restore deleted and recreated under the
/// same name.
#[test]
fn the_approval_verdict_binds_the_restore_its_uid_its_plan_and_the_policy() {
    let f = fixture();
    for (case, want) in [
        ("approved-for-another-uid", "AuthorizationSubjectMismatch"),
        (
            "approved-for-another-namespace",
            "AuthorizationSubjectMismatch",
        ),
        (
            "approved-for-another-restore",
            "AuthorizationSubjectMismatch",
        ),
        ("approved-for-another-plan", "PlanHashMismatch"),
    ] {
        let refused = verdict(&f, case);
        assert_eq!(reason(&refused), want, "{case}: {refused:?}");
    }
    // The namespace's policy changed after the request: the same document
    // under the SAME policy with a personal key (another digest).
    let mut personal = f.pair();
    personal.approver_signature = ApproverSignature::PersonalKey;
    for case in ["approved", "request"] {
        let refused = verdict_with(&f, case, &personal, f.now(), vec![f.console_key()]);
        assert_eq!(
            reason(&refused),
            "ApprovalPolicyMismatch",
            "{case}: {refused:?}"
        );
    }
}

/// **A strict namespace is what it was.** Its request countersigned by a
/// personal key verifies under that key; console only, it is pending; and a
/// console-approved document is refused there twice over — one that names the
/// two-person policy by its digest, and one that names the STRICT policy's
/// own digest and carries an approver all the same (countersigned, too).
#[test]
fn a_strict_namespace_is_unchanged_and_refuses_a_console_approved_document() {
    let f = fixture();
    let strict = f.strict();
    let keys = || vec![f.console_key(), f.personal_key()];
    let verified = verdict_with(&f, "strict-request-countersigned", &strict, f.now(), keys())
        .expect("a strict request with its countersignature verifies");
    assert_eq!(verified.matched_key_id, f.text("approverKeyId"));
    assert_eq!(
        verified.authorization_usage,
        logweir_core::trust::KeyUsage::GovernedApproval
    );
    assert_eq!(verified.approver, "https://idp.example#bob");
    let pending = verdict_with(&f, "strict-request", &strict, f.now(), keys());
    assert_eq!(reason(&pending), "GovernedApprovalRequired", "{pending:?}");
    assert!(
        words(&pending).contains("GovernedApproval key"),
        "the strict pending sentence is the one it always was: {}",
        words(&pending)
    );

    let other_policy = verdict_with(&f, "approved", &strict, f.now(), keys());
    assert_eq!(
        reason(&other_policy),
        "ApprovalPolicyMismatch",
        "{other_policy:?}"
    );
    let forged = verdict_with(
        &f,
        "strict-with-a-console-approver",
        &strict,
        f.now(),
        keys(),
    );
    assert_eq!(reason(&forged), "ApprovalPolicyMismatch", "{forged:?}");
    assert!(
        words(&forged).contains("approverSignature is Console"),
        "{}",
        words(&forged)
    );
    // And the strict request offered to the two-person namespace.
    let downgrade = verdict_with(
        &f,
        "strict-request-countersigned",
        &f.pair(),
        f.now(),
        keys(),
    );
    assert_eq!(
        reason(&downgrade),
        "ApprovalPolicyMismatch",
        "{downgrade:?}"
    );
}

/// **The signature is the console's, over these bytes.** A document signed
/// by a key nobody trusts, the approved sidecar beside other bytes, a console
/// key trusted for another usage, and a revoked console key: each refused.
#[test]
fn the_approval_verdict_needs_the_consoles_own_signature_over_these_bytes() {
    let f = fixture();
    let foreign = verdict(&f, "approved-signed-by-another-key");
    assert_eq!(reason(&foreign), "KeyIdNotInRoster", "{foreign:?}");
    // The approved document's sidecar beside the requester's own approval.
    let swapped = approval::evaluate_authorization_v2(
        f.document("approved-by-requester").as_bytes(),
        f.sidecar("approved").as_bytes(),
        &trust(&f, vec![f.console_key()]),
        f.now(),
        &f.expected("planBytes"),
        &f.pair(),
    );
    assert_eq!(reason(&swapped), "SignatureInvalid", "{swapped:?}");
    // The console key trusted only as a governed approver: not a
    // confirmation, so not a console approval either.
    let mut wrong_usage = f.console_key();
    wrong_usage.usages = vec![SpecUsage::GovernedApproval];
    let refused = verdict_with(&f, "approved", &f.pair(), f.now(), vec![wrong_usage]);
    assert_eq!(reason(&refused), "KeyIdNotInRoster", "{refused:?}");
    let mut revoked = f.console_key();
    revoked.state = KeyState::Revoked;
    revoked.revoked_at = Some(f.now() - Duration::hours(1));
    revoked.revocation_reason = Some(RevocationReason::KeyCompromise);
    let refused = verdict_with(&f, "approved", &f.pair(), f.now(), vec![revoked]);
    assert_eq!(reason(&refused), "KeyRevoked", "{refused:?}");
}

// ---------------------------------------------------------------------------
// Restore admission
// ---------------------------------------------------------------------------

/// The Restore the fixture's documents are for, over `plan`.
fn restore(f: &Fixture, plan: &str, original_name: bool) -> Restore {
    let mut target = serde_json::json!({
        "clusterRef": {"name": "scratch"},
        "mode": if original_name { "newTopic" } else { "scratch" },
        "topicNaming": {"prefix": if original_name { "" } else { "drill-" }},
    });
    if original_name {
        target["topicNaming"]["originalName"] = serde_json::json!(true);
    }
    let mut spec = serde_json::json!({
        "planBytes": f.text(plan),
        "approvalRef": {"name": "a1"},
        "sourceArchive": {"url": "s3://kafka-backups/logweir"},
        "backupSetRef": "drill-demo",
        "pointInTime": "2026-09-07T14:05:00Z",
        "target": target,
        "deadlineSeconds": 1800
    });
    if original_name {
        spec["coverage"] = serde_json::json!("complete");
    }
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Restore",
        "metadata": {"name": f.text("restore"), "namespace": f.text("namespace"),
                     "uid": f.text("restoreUid"), "generation": 1, "resourceVersion": "4071"},
        "spec": spec
    }))
    .expect("a Restore")
}

fn cluster(f: &Fixture) -> KafkaCluster {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": {"name": "scratch", "namespace": f.text("namespace"),
                     "uid": "cccccccc-0000-4000-8000-0000000162cc"},
        "spec": {"bootstrapServers": ["scratch-0:9092"],
                 "auth": {"mode": "plaintext", "tls": false},
                 "role": "scratch", "markerTopic": "logweir.scratch"},
        "status": {"reachable": true, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk"}
    }))
    .expect("a KafkaCluster")
}

/// An Approval carrying one fixture case, WITH THE STATUS A VERIFIED VERDICT
/// UNDER `provenance` WOULD HAVE — whatever the Approval controller would in
/// fact have said of it. Admission must re-derive its own verdict from the
/// signed bytes and not take that status's word.
fn approval_object(
    f: &Fixture,
    case: &str,
    plan: &str,
    provenance: &ApprovalPolicy,
    matched: &str,
) -> Approval {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Approval",
        "metadata": {"name": "a1", "namespace": f.text("namespace"),
                     "uid": "aaaaaaaa-0000-4000-8000-00000000162a"},
        "spec": {
            "subjectRef": {"kind": "Restore", "name": f.text("restore")},
            "planHash": logweir_core::ids::sha256_prefixed(f.text(plan).as_bytes()),
            "approvalBytes": f.document(case),
            "sidecarBytes": f.sidecar(case),
        },
        "status": {
            "verified": true,
            "matchedKeyId": matched,
            "approver": "https://idp.example#bob",
            "verifiedSubjectRef": {"apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
                                   "name": f.text("restore"), "namespace": f.text("namespace"),
                                   "uid": f.text("restoreUid")},
            "authorization": {
                "mode": provenance.mode.as_str(),
                "policyName": provenance.name,
                "policyDigest": provenance.digest(),
                "requester": "https://idp.example#alice",
                "confirmationKeyId": f.text("consoleKeyId")
            },
            "conditions": [{"type": "Verified", "status": "True", "reason": "Verified"}]
        }
    }))
    .expect("an Approval")
}

fn admit(f: &Fixture, case: &str, bound: &ApprovalPolicy, now: DateTime<Utc>) -> RestoreAdmission {
    let approval = approval_object(f, case, "planBytes", bound, &f.text("consoleKeyId"));
    admit_with_policy(
        &restore(f, "planBytes", false),
        Some(&approval),
        Some(&cluster(f)),
        None,
        Some(&PolicyAdmission {
            policy: &EffectivePolicy::Bound(bound.clone()),
            now,
        }),
    )
}

/// **Restore admission admits a console-approved Restore** — and re-derives,
/// from the signed bytes, every refusal the Approval verdict makes, even when
/// the Approval's status says `verified: true` under the right policy. A
/// stale, defective or forged verdict is never what a Job is created on.
///
/// KILLS: an admission that trusts `status.verified` for the second person.
#[test]
fn restore_admission_re_checks_the_console_approval_from_the_signed_bytes() {
    let f = fixture();
    assert_eq!(
        admit(&f, "approved", &f.pair(), f.now()),
        RestoreAdmission::Ok
    );
    assert_eq!(
        admit(
            &f,
            "approved-by-a-second-person-behind-a-trailing-slash",
            &f.pair(),
            f.now()
        ),
        RestoreAdmission::Ok
    );
    for (case, needle) in [
        ("request", "nobody has approved"),
        (
            "request-countersigned-by-a-personal-key",
            "nobody has approved",
        ),
        ("approved-by-requester", "is the requester"),
        ("approved-by-requester-in-another-case", "is the requester"),
        (
            "approved-by-requester-behind-a-trailing-slash",
            "is the requester",
        ),
        (
            "approved-by-the-same-subject-of-another-issuer",
            "two issuers",
        ),
        (
            "approved-by-a-subject-with-a-trailing-space",
            "not in a form that can be compared",
        ),
        (
            "approved-by-a-decomposed-spelling",
            "not in a form that can be compared",
        ),
        ("approved-by-the-local-admin", "administrator console"),
        ("requested-by-the-local-admin", "administrator console"),
        (
            "requested-by-a-service-account",
            "Kubernetes system identity",
        ),
        (
            "approved-before-the-request",
            "outside the request's own window",
        ),
        ("approved-at-the-expiry", "outside the request's own window"),
        (
            "approved-ahead-of-the-controllers-clock",
            "ahead of this verifier's clock",
        ),
        ("approver-under-2-1-0", "defined from formatVersion 2.2.0"),
        ("approver-under-2-0-0", "defined from formatVersion 2.2.0"),
        ("approver-without-the-instant", "without the other"),
    ] {
        let admission = admit(&f, case, &f.pair(), f.now());
        assert!(
            matches!(
                admission,
                RestoreAdmission::AuthorizationPolicyMismatch { .. }
            ),
            "{case}: {admission:?}"
        );
        assert!(admission.is_terminal(), "{case}");
        assert!(
            admission.to_string().contains(needle),
            "{case}: {admission}"
        );
    }
    // Expired at admission: the Approval was verified in time, the Job was
    // not created in time.
    let expired = admit(&f, "approved", &f.pair(), f.instant("expiresAt"));
    assert_eq!(expired.reason(), "AuthorizationExpired", "{expired}");
    // The UID, the plan.
    let other_uid = admit(&f, "approved-for-another-uid", &f.pair(), f.now());
    assert!(
        matches!(other_uid, RestoreAdmission::ApprovalSubjectMismatch { .. }),
        "{other_uid:?}"
    );
    let other_plan = admit(&f, "approved-for-another-plan", &f.pair(), f.now());
    assert!(
        matches!(other_plan, RestoreAdmission::PlanHashMismatch { .. }),
        "{other_plan:?}"
    );
}

/// **A policy change between the approval and the Job**: the namespace is
/// rebound to the same policy with a personal key, or to the strict policy.
/// The console-approved document names a digest the namespace no longer has.
/// And the reverse: a strict namespace's own document carrying an approver.
#[test]
fn restore_admission_refuses_a_console_approval_under_any_other_policy() {
    let f = fixture();
    let mut personal = f.pair();
    personal.approver_signature = ApproverSignature::PersonalKey;
    for bound in [personal, f.strict()] {
        let admission = admit(&f, "approved", &bound, f.now());
        assert_eq!(admission.reason(), "ApprovalPolicyMismatch", "{admission}");
        assert!(admission.is_terminal());
    }
    let forged = admit(&f, "strict-with-a-console-approver", &f.strict(), f.now());
    assert_eq!(forged.reason(), "ApprovalPolicyMismatch", "{forged}");
    assert!(
        forged.to_string().contains("approverSignature is Console"),
        "{forged}"
    );
    // NEGATIVE CONTROL: the strict namespace's own countersigned request is
    // admitted exactly as before (admission reads no signature for it).
    let strict_ok = admit_with_policy(
        &restore(&f, "planBytes", false),
        Some(&approval_object(
            &f,
            "strict-request-countersigned",
            "planBytes",
            &f.strict(),
            &f.text("approverKeyId"),
        )),
        Some(&cluster(&f)),
        None,
        Some(&PolicyAdmission {
            policy: &EffectivePolicy::Bound(f.strict()),
            now: f.now(),
        }),
    );
    assert_eq!(strict_ok, RestoreAdmission::Ok);
}

/// **THE COMBINATION WITH PROD-15.1, AT ADMISSION.** An original-name restore
/// approved by a second person is admitted on the subject and the approver;
/// the request alone is not; an ORDINARY console approval never authorises an
/// original-name plan; and typed names beside a second person are refused.
#[test]
fn restore_admission_holds_an_original_name_console_approval_to_the_plans_subject() {
    let f = fixture();
    let admit_original = |case: &str, plan: &str, original: bool| {
        admit_with_policy(
            &restore(&f, plan, original),
            Some(&approval_object(
                &f,
                case,
                plan,
                &f.pair(),
                &f.text("consoleKeyId"),
            )),
            Some(&cluster(&f)),
            None,
            Some(&PolicyAdmission {
                policy: &EffectivePolicy::Bound(f.pair()),
                now: f.now(),
            }),
        )
    };
    assert_eq!(
        admit_original("original-name-approved", "originalPlanBytes", true),
        RestoreAdmission::Ok
    );
    let pending = admit_original("original-name-request", "originalPlanBytes", true);
    assert_eq!(pending.reason(), "ApprovalPolicyMismatch", "{pending}");
    assert!(
        pending.to_string().contains("nobody has approved"),
        "{pending}"
    );
    let typed = admit_original(
        "original-name-approved-with-typed-names",
        "originalPlanBytes",
        true,
    );
    assert!(
        typed
            .to_string()
            .contains(logweir_core::original_name::ORIGINAL_NAME_CONFIRMATION_NOT_ACCEPTED),
        "{typed}"
    );
    // The original-name approval is for another plan than the ordinary one,
    // so it never authorises the ordinary Restore either.
    let wrong_plan = admit_original("original-name-approved", "planBytes", false);
    assert!(
        matches!(wrong_plan, RestoreAdmission::PlanHashMismatch { .. }),
        "{wrong_plan:?}"
    );
}

// ---------------------------------------------------------------------------
// The bundle: the console's signature, verified once more
// ---------------------------------------------------------------------------

/// **The bundle of a console-approved Restore mounts the console key as the
/// approver key, beside the `Console` snapshot — and is written only after
/// the console's signature verified once more**, against the console key the
/// namespace's trust carries now, over the exact bytes it mounts.
///
/// KILLS: materialising on `status.verified` alone; asking the trust for a
/// `GovernedApproval` usage on the console key (which it never has).
#[test]
fn the_bundle_is_written_only_over_a_console_signature_that_verifies_now() {
    let f = fixture();
    let bound = EffectivePolicy::Bound(f.pair());
    let run = |case: &str, matched: &str, keys: Vec<SpecKey>| {
        approval_bundle_config_map_with_policy(
            &restore(&f, "planBytes", false),
            &approval_object(&f, case, "planBytes", &f.pair(), matched),
            &trust(&f, keys),
            &bound,
            f.now(),
        )
    };
    let console_id = f.text("consoleKeyId");
    let bundle = run("approved", &console_id, vec![f.console_key()]).expect("a bundle");
    let data = bundle.data.expect("data");
    assert_eq!(
        data.get("approver.pub.pem"),
        Some(&f.text("consolePublicPem")),
        "the console key authorised the run"
    );
    assert_eq!(
        data.get(CONFIRMATION_KEY_FILE),
        Some(&f.text("consolePublicPem"))
    );
    assert_eq!(
        data.get(APPROVAL_POLICY_FILE).map(String::as_bytes),
        Some(f.pair().snapshot_bytes().as_slice()),
        "the frozen snapshot is the two-person one, approverSignature and all"
    );
    assert_eq!(data.get("approval.json"), Some(&f.document("approved")));

    // A status that says verified over a signature that is NOT the console's.
    let forged = run(
        "approved-signed-by-another-key",
        &console_id,
        vec![f.console_key()],
    )
    .expect_err("no bundle over a signature that does not verify");
    assert!(forged.to_string().contains("does not verify"), "{forged}");
    // A status that names a personal key as the authoriser under a policy
    // whose approval the console signs.
    let personal = run(
        "approved",
        &f.text("approverKeyId"),
        vec![f.console_key(), f.personal_key()],
    )
    .expect_err("no bundle");
    assert!(
        personal.to_string().contains("no longer accepts")
            || personal.to_string().contains("not the console key"),
        "{personal}"
    );
    // A console key withdrawn between the verdict and the bundle.
    let mut retired = f.console_key();
    retired.state = KeyState::Retired;
    retired.retired_at = Some(f.now() - Duration::minutes(1));
    let withdrawn = run("approved", &console_id, vec![retired]).expect_err("no bundle");
    assert!(
        withdrawn.to_string().contains("no longer accepts"),
        "{withdrawn}"
    );

    // NEGATIVE CONTROL, A STRICT NAMESPACE: its bundle is what it always was
    // — the personal key as the approver, the snapshot without the setting —
    // and no signature is re-verified here for it.
    let strict = approval_bundle_config_map_with_policy(
        &restore(&f, "planBytes", false),
        &approval_object(
            &f,
            "strict-request-countersigned",
            "planBytes",
            &f.strict(),
            &f.text("approverKeyId"),
        ),
        &trust(&f, vec![f.console_key(), f.personal_key()]),
        &EffectivePolicy::Bound(f.strict()),
        f.now(),
    )
    .expect("a strict bundle");
    let data = strict.data.expect("data");
    assert_eq!(
        data.get("approver.pub.pem"),
        Some(&f.text("approverPublicPem"))
    );
    assert!(!data
        .get(APPROVAL_POLICY_FILE)
        .expect("snapshot")
        .contains("approverSignature"));
}

// ---------------------------------------------------------------------------
// The coordinator's addition 4: the controller decides from the table
// ---------------------------------------------------------------------------

/// **The controller decides from `ApprovalPolicy::route`, at the verdict AND
/// at admission, and refuses the pair that is no row.** A bound policy with
/// `mode: Ordinary` and `approverSignature: Console` cannot be read from an
/// installation document (`ApprovalPolicySet::parse` refuses it, so the
/// controller refuses to start on one written with `kubectl`); built here in
/// memory, as a later code path might build one, it verifies and admits
/// NOTHING, whatever document is offered under it — the console-approved one,
/// the request, a strict request with its countersignature.
///
/// And the three rows, each by its own document and no other row's:
///
/// | bound policy | the document it admits | every other fixture document |
/// |---|---|---|
/// | `Governed` + `Console` | `approved` | refused |
/// | `Governed` + personal key | `strict-request-countersigned` | refused |
///
/// (`Ordinary` + personal key, the one-person confirmation, is PROD-16.1's
/// and has its own rows in `approval_policy.rs`.)
///
/// KILLS: a `route()` that maps the fourth pair to any row; a verdict or an
/// admission that decides from `mode` alone, or from `approver_signature`
/// alone.
#[test]
fn the_controller_decides_from_the_table_and_refuses_a_pair_that_is_no_row() {
    let f = fixture();
    let mut not_a_row = f.pair();
    not_a_row.mode = logweir_core::approval_policy::ApprovalMode::Ordinary;
    not_a_row.require_distinct_principal = false;
    assert!(not_a_row.route().is_err(), "the fourth pair");
    for case in ["approved", "request", "strict-request-countersigned"] {
        let refused = verdict_with(
            &f,
            case,
            &not_a_row,
            f.now(),
            vec![f.console_key(), f.personal_key()],
        );
        assert_eq!(
            reason(&refused),
            "ApprovalPolicyMismatch",
            "{case}: {refused:?}"
        );
        assert!(
            words(&refused).contains("not a policy anything is confirmed, approved or run under"),
            "{case}: {}",
            words(&refused)
        );
        let admission = admit(&f, case, &not_a_row, f.now());
        assert_eq!(
            admission.reason(),
            "ApprovalPolicyMismatch",
            "{case}: {admission}"
        );
        assert!(admission.is_terminal(), "{case}");
    }
    // An installation document that says so is refused where it is read.
    for document in [
        "allowOrdinaryConfirmation: true\npolicies:\n  - name: p\n    mode: Ordinary\n    approverSignature: Console\nnamespaces:\n  team-a: p\n",
        "allowOrdinaryConfirmation: true\npolicies:\n  - name: p\n    mode: confirm\n    approverSignature: Console\nnamespaces:\n  team-a: p\n",
    ] {
        let refused = weirkeeper::approval_policy::configured_policy(
            Ok("/etc/logweir/approval-policy.yaml".to_string()),
            |_| Ok(document.to_string()),
        )
        .expect_err("the controller does not start on it");
        assert!(
            refused.contains("/etc/logweir/approval-policy.yaml") && refused.contains("approver"),
            "{refused}"
        );
    }

    // THE ROWS, each admitting its own document and refusing the others'.
    let mut personal = f.pair();
    personal.approver_signature = ApproverSignature::PersonalKey;
    let keys = || vec![f.console_key(), f.personal_key()];
    let rows: [(&str, ApprovalPolicy, &str); 2] = [
        ("Governed + Console", f.pair(), "approved"),
        (
            "Governed + personal key",
            f.strict(),
            "strict-request-countersigned",
        ),
    ];
    let documents = [
        "approved",
        "request",
        "request-countersigned-by-a-personal-key",
        "strict-request",
        "strict-request-countersigned",
        "strict-with-a-console-approver",
    ];
    for (label, bound, admitted) in &rows {
        for case in documents {
            let result = verdict_with(&f, case, bound, f.now(), keys());
            assert_eq!(
                result.is_ok(),
                case == *admitted,
                "{label} over {case}: {result:?}"
            );
        }
    }
    // The same two-person policy with a personal key is another policy (its
    // digest differs): it admits neither row's document.
    for case in documents {
        let result = verdict_with(&f, case, &personal, f.now(), keys());
        assert!(
            result.is_err(),
            "the rebound policy over {case}: {result:?}"
        );
    }
}
