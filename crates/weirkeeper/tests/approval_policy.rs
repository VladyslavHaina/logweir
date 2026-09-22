//! PLAT-19.2 — ordinary confirmation and governed approval at the `Approval`
//! controller: both policies, self-approval, expiry, a changed plan, a policy
//! downgrade and a directly written object.
//!
//! # Where the signed material comes from
//!
//! This crate cannot sign (`scripts/check-one-signer.sh`, check 3), so, as in
//! `approval_controller.rs`, the documents and signatures below were produced
//! OUT OF TREE by throwaway Ed25519 key pairs that existed only in one Python
//! process's memory. Only the public halves, the key ids, the documents and
//! the base64 signatures were written down. Ed25519 because its signatures are
//! deterministic, so the constants are reproducible.
//!
//! * `CONSOLE_*` — the console's `ConsoleConfirmation` key.
//! * `BOB_*` — a governed approver, principal `https://idp.example#bob`.
//! * `ALICE_*` — a governed-approver key held by the REQUESTER herself,
//!   principal `https://idp.example#alice`: the self-approval case.
//!
//! The two documents name policy digests computed from the snapshot bytes of
//! the two policies in [`POLICY_DOC`]; `the_fixture_digests_are_the_policies`
//! asserts that against the live function, so a snapshot change is a red test
//! here rather than a silently stale fixture.

use chrono::{DateTime, Duration, Utc};
use kube::api::ObjectMeta;
use logweir_core::approval_policy::{
    ApprovalMode, ApprovalPolicy, ApprovalPolicySet, ExpectedSubject,
};
use weirkeeper::controllers::approval::{
    self, ApprovalOutcome, ApprovalRefusal, Verified, CLEARABLE_STATUS_FIELDS,
};
use weirkeeper::crds::approval::{Approval, ApprovalSpec, SubjectKind, SubjectRef};
use weirkeeper::crds::trust_policy::{
    KeyAlgorithm, KeyPrincipal, KeyState, KeyUsage as SpecUsage, RevocationReason, TrustPolicy,
    TrustPolicySpec, TrustedKey as SpecKey,
};
use weirkeeper::testing::{mock_client_recording, Route};

const NS: &str = "logweir-t16";
const PLAN_BYTES: &str = "apiVersion: logweir.dev/v1alpha1\nkind: RestorePlan\ntopics:\n  - orders\nwindow_start: 2026-09-01T00:00:00Z\nwindow_end: 2026-09-02T00:00:00Z\n";
const PLAN_HASH: &str = "sha256:742778e4f9dc02eced0b9d0b9dc35f3a3ab5dbef5a559375b3aec76e73006091";
const RESTORE_UID: &str = "restore-uid-1";

const CONSOLE_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEATMlQktU/lSpsDha1ZlgAW5JtnXqOLAQgQAZI8IFBlRI=\n-----END PUBLIC KEY-----\n";
const CONSOLE_KEY_ID: &str = "85551a95543b7e54ffd8a7ce17560f6f850c993b7a35e5ab23987f04c946ebd8";
const BOB_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEALdRlkK//9cPdrQEiKldQ80TZ6nvN/KwbaBVFyG2Xjq0=\n-----END PUBLIC KEY-----\n";
const BOB_KEY_ID: &str = "74dd2b54804995b672352b96d7d3ab2a96704a5d0d0aa8abed0b8bae5d87d6af";
const ALICE_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAwLTK/p5eg/WiGxgKGAlztjerEHGiFR8Fbd/6WSYv4BI=\n-----END PUBLIC KEY-----\n";
const ALICE_KEY_ID: &str = "ce550e00f743e956380078ce7dfc27c9a8e19602c467725d051792ce260fc37b";

const ORDINARY_DOC: &str = r#"{"formatVersion":"2.0.0","kind":"RestoreAuthorization","authorizationMode":"Ordinary","subject":{"apiVersion":"logweir.dev/v1alpha1","kind":"Restore","namespace":"logweir-t16","name":"r1","uid":"restore-uid-1"},"planHash":"sha256:742778e4f9dc02eced0b9d0b9dc35f3a3ab5dbef5a559375b3aec76e73006091","requester":{"issuer":"https://idp.example","subject":"alice"},"policy":{"name":"team-ordinary","digest":"sha256:fcfaf96141cda0c8ec9f856520d6f2989d1eccef73b307dcfdabbc6ac67490a0"},"issuedAt":"2026-09-09T12:55:00Z","expiresAt":"2026-09-09T13:10:00Z"}"#;
const ORDINARY_CONSOLE_SIG: &str =
    "GBZ9x6CrkCziVApvX5wZYpFqh9RFr61WATjPOIZ3LIxUpK6n5z9F3I+l2/iz1BOu+9FuSz+gGp6cCSkLBYejAA==";
const ORDINARY_BOB_SIG: &str =
    "yPutrFQGtzyTBp6nXQK4xrG8jxA9aobEYtNmVRxRj4U4JU6wXksNd1WYndXA29FarAZKQMVUn+i6On6a6l2dAg==";
const GOVERNED_DOC: &str = r#"{"formatVersion":"2.0.0","kind":"RestoreAuthorization","authorizationMode":"Governed","subject":{"apiVersion":"logweir.dev/v1alpha1","kind":"Restore","namespace":"logweir-t16","name":"r1","uid":"restore-uid-1"},"planHash":"sha256:742778e4f9dc02eced0b9d0b9dc35f3a3ab5dbef5a559375b3aec76e73006091","requester":{"issuer":"https://idp.example","subject":"alice"},"policy":{"name":"prod-governed","digest":"sha256:ff926e0fc3a20288911e296cc5dbd81f2ccced1dbc1fb03e265647c56eb9f0fa"},"issuedAt":"2026-09-09T12:00:00Z","expiresAt":"2026-09-10T12:00:00Z","ticket":"CHG-4711"}"#;
const GOVERNED_CONSOLE_SIG: &str =
    "ZC1ZnoTGVQsyl4dHbnDCvMnFzR5MDembbSuHSADoJUwfsv+KclBg9/rWSOJ7Nlp0A/GdQW72IsdXe906EILjBQ==";
const GOVERNED_BOB_SIG: &str =
    "K+NjfkRjij4qCgxJrtwFso3SxSsulxUCMXA8oAIVw0ZdTibbkXBc1ptlwRBoVt0Gm02soIvrW8gsxgsPsr//Cw==";
const GOVERNED_ALICE_SIG: &str =
    "iP/fjpY83nqZbA3Ev+iEOJe1hR/V6GzpDUDXwM6WMzUUqFfm+JLe+VVhNT5C4ep7V58YevfYsvONAQAyB4JVCA==";
/// The governed document WITHOUT a change ticket, signed by the console and
/// bob: D0 requires a ticket under Governed, so it never verifies.
const GOVERNED_NO_TICKET_DOC: &str = r#"{"formatVersion":"2.0.0","kind":"RestoreAuthorization","authorizationMode":"Governed","subject":{"apiVersion":"logweir.dev/v1alpha1","kind":"Restore","namespace":"logweir-t16","name":"r1","uid":"restore-uid-1"},"planHash":"sha256:742778e4f9dc02eced0b9d0b9dc35f3a3ab5dbef5a559375b3aec76e73006091","requester":{"issuer":"https://idp.example","subject":"alice"},"policy":{"name":"prod-governed","digest":"sha256:ff926e0fc3a20288911e296cc5dbd81f2ccced1dbc1fb03e265647c56eb9f0fa"},"issuedAt":"2026-09-09T12:00:00Z","expiresAt":"2026-09-10T12:00:00Z"}"#;
const GOVERNED_NO_TICKET_CONSOLE_SIG: &str =
    "Ps2BhsZsi204f15hN5SNe31B5dTUU5glSSHPvxJ9ciGqzn9U+2ALeil6dcPxlU6F0lcP5ItLAx7phH+KEUQ9Dg==";
const GOVERNED_NO_TICKET_BOB_SIG: &str =
    "NQGZ4cQe6X4lDslOqflTDgIGfj6N7bWJEEyKgHWbT1auqM9eUrsD9g/eAraZQ6vmGvy8ot3gMQ19RxhdkvirBA==";

/// The two v1 constants from `approval_controller.rs`: a genuine v1 approval
/// by a `GovernedApproval` key, for the rows that present one under a binding.
const V1_DOC: &str = r#"{"approver":"ops@example.com","ticket":"CHG-4711","plan_hash":"sha256:742778e4f9dc02eced0b9d0b9dc35f3a3ab5dbef5a559375b3aec76e73006091","approved_at":"2026-09-09T12:00:00Z","subject_kind":"Restore"}"#;
const V1_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEApFpEU8uY5S8Lv43HL4DcXKKyM8WHurCPZIxvq8ZBfpY=\n-----END PUBLIC KEY-----\n";
const V1_KEY_ID: &str = "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6";
const V1_SIG: &str =
    "afLmiRCAVRJGg0IfHJTDWHWQQE+PXZWqryC5ATTC2GUHcvriC4RRyy+4ZzhONWM1V5HmBeSO52eMH+6I75AMBQ==";

const V2_PAYLOAD: &str = logweir_core::approval_policy::PAYLOAD_TYPE_RESTORE_AUTHORIZATION;

/// The installation document every row resolves against. `logweir-t16` is
/// bound per row by [`bind`].
const POLICY_DOC: &str = "allowOrdinaryConfirmation: true
policies:
  - name: team-ordinary
    mode: Ordinary
    maxAgeSeconds: 900
  - name: prod-governed
    mode: Governed
    maxAgeSeconds: 86400
";

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("a fixture instant")
        .with_timezone(&Utc)
}

/// Inside both documents' windows.
fn now() -> DateTime<Utc> {
    at("2026-09-09T13:00:00Z")
}

fn bind(policy: &str) -> ApprovalPolicySet {
    ApprovalPolicySet::parse(&format!("{POLICY_DOC}namespaces:\n  {NS}: {policy}\n"))
        .expect("the fixture document validates")
}

fn policy(name: &str) -> ApprovalPolicy {
    bind(name).resolve(NS).bound().cloned().expect("bound")
}

fn key(key_id: &str, pem: &str, usage: SpecUsage, principal: &str) -> SpecKey {
    SpecKey {
        key_id: key_id.to_string(),
        spki_pem: pem.to_string(),
        algorithm: KeyAlgorithm::Ed25519,
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

fn console() -> SpecKey {
    key(
        CONSOLE_KEY_ID,
        CONSOLE_PEM,
        SpecUsage::ConsoleConfirmation,
        "logweir-api:console",
    )
}
fn bob() -> SpecKey {
    key(
        BOB_KEY_ID,
        BOB_PEM,
        SpecUsage::GovernedApproval,
        "https://idp.example#bob",
    )
}
fn alice() -> SpecKey {
    key(
        ALICE_KEY_ID,
        ALICE_PEM,
        SpecUsage::GovernedApproval,
        "https://idp.example#alice",
    )
}
fn v1_approver() -> SpecKey {
    key(
        V1_KEY_ID,
        V1_PEM,
        SpecUsage::GovernedApproval,
        "ops@example.com",
    )
}

fn trust_policy(keys: Vec<SpecKey>) -> TrustPolicy {
    TrustPolicy {
        metadata: ObjectMeta {
            name: Some("org-default".to_string()),
            uid: Some("uid-org-default".to_string()),
            generation: Some(1),
            resource_version: Some("17".to_string()),
            ..ObjectMeta::default()
        },
        spec: TrustPolicySpec {
            default: false,
            namespaces: Some(vec![NS.to_string()]),
            allowed_target_cluster_ids: Some(vec!["scratch-cluster-id".to_string()]),
            keys,
        },
        status: None,
    }
}

fn trust(keys: Vec<SpecKey>) -> weirkeeper::trust::ResolvedTrust {
    weirkeeper::trust::from_policy(&trust_policy(keys))
}

fn sidecar(payload_type: &str, signatures: &[(&str, &str)]) -> String {
    let sigs = signatures
        .iter()
        .map(|(keyid, sig)| format!(r#"{{"keyid":"{keyid}","sig":"{sig}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"payloadType":"{payload_type}","signatures":[{sigs}]}}"#)
}

fn expected() -> ExpectedSubject {
    ExpectedSubject {
        namespace: NS.to_string(),
        name: "r1".to_string(),
        uid: RESTORE_UID.to_string(),
        plan_hash: PLAN_HASH.to_string(),
    }
}

fn v2(
    doc: &str,
    signatures: &[(&str, &str)],
    keys: Vec<SpecKey>,
    bound: &str,
    now: DateTime<Utc>,
    expected: &ExpectedSubject,
) -> Result<Verified, ApprovalRefusal> {
    approval::evaluate_authorization_v2(
        doc.as_bytes(),
        sidecar(V2_PAYLOAD, signatures).as_bytes(),
        &trust(keys),
        now,
        expected,
        &policy(bound),
    )
}

fn ordinary(keys: Vec<SpecKey>) -> Result<Verified, ApprovalRefusal> {
    v2(
        ORDINARY_DOC,
        &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)],
        keys,
        "team-ordinary",
        now(),
        &expected(),
    )
}

fn governed(signatures: &[(&str, &str)], keys: Vec<SpecKey>) -> Result<Verified, ApprovalRefusal> {
    v2(
        GOVERNED_DOC,
        signatures,
        keys,
        "prod-governed",
        now(),
        &expected(),
    )
}

fn reason(result: Result<Verified, ApprovalRefusal>) -> &'static str {
    match result {
        Ok(_) => "Verified",
        Err(r) => r.reason(),
    }
}

// ---------------------------------------------------------------------------
// The fixtures are what they say
// ---------------------------------------------------------------------------

#[test]
fn the_fixture_digests_are_the_policies() {
    assert!(ORDINARY_DOC.contains(&policy("team-ordinary").digest()));
    assert!(GOVERNED_DOC.contains(&policy("prod-governed").digest()));
    assert_eq!(
        logweir_core::ids::sha256_prefixed(PLAN_BYTES.as_bytes()),
        PLAN_HASH
    );
}

// ---------------------------------------------------------------------------
// Both policies
// ---------------------------------------------------------------------------

/// **The ordinary path.** One console signature over a v2 document naming the
/// namespace's Ordinary policy is the whole authorization.
///
/// KILLS: "`ConsoleConfirmation` fails closed for every mode" — the pre-19.2
/// behaviour, which leaves ordinary mode unusable.
#[test]
fn an_ordinary_confirmation_verifies_under_an_ordinary_binding() {
    let verified = ordinary(vec![console()]).expect("an ordinary confirmation verifies");
    assert_eq!(verified.matched_key_id, CONSOLE_KEY_ID);
    assert_eq!(
        verified.authorization_usage,
        logweir_core::trust::KeyUsage::ConsoleConfirmation
    );
    assert_eq!(verified.approver, "https://idp.example#alice");
    let provenance = verified.authorization.expect("v2 publishes its provenance");
    assert_eq!(provenance.mode, "Ordinary");
    assert_eq!(provenance.policy_name, "team-ordinary");
    assert_eq!(provenance.policy_digest, policy("team-ordinary").digest());
    assert_eq!(provenance.requester, "https://idp.example#alice");
    assert_eq!(provenance.confirmation_key_id, CONSOLE_KEY_ID);
    assert_eq!(
        verified.document_expires_at,
        Some(at("2026-09-09T13:10:00Z")),
        "the verdict stops being true at the document's own expiry"
    );
}

/// **The governed path.** The console's confirmation AND a distinct
/// principal's `GovernedApproval` countersignature over the same bytes.
#[test]
fn a_governed_request_verifies_when_a_distinct_principal_countersigns() {
    let verified = governed(
        &[
            (CONSOLE_KEY_ID, GOVERNED_CONSOLE_SIG),
            (BOB_KEY_ID, GOVERNED_BOB_SIG),
        ],
        vec![console(), bob()],
    )
    .expect("console + distinct approver verifies");
    assert_eq!(
        verified.matched_key_id, BOB_KEY_ID,
        "the approver's key authorised it"
    );
    assert_eq!(
        verified.authorization_usage,
        logweir_core::trust::KeyUsage::GovernedApproval
    );
    assert_eq!(verified.approver, "https://idp.example#bob");
    let provenance = verified.authorization.expect("provenance");
    assert_eq!(provenance.mode, "Governed");
    assert_eq!(provenance.confirmation_key_id, CONSOLE_KEY_ID);
    assert_eq!(provenance.requester, "https://idp.example#alice");
}

/// A console confirmation under a Governed policy is the PENDING state of a
/// governed request, never an authorization — and it is exactly what a
/// directly written console-only object stays at.
///
/// KILLS: "accept the console signature as sufficient in every mode", the
/// downgrade the whole policy exists to prevent.
#[test]
fn a_console_only_document_under_a_governed_binding_awaits_an_approver() {
    let refused = governed(
        &[(CONSOLE_KEY_ID, GOVERNED_CONSOLE_SIG)],
        vec![console(), bob()],
    );
    assert_eq!(reason(refused.clone()), "GovernedApprovalRequired");
    let message = refused.err().map(|r| r.to_string()).unwrap_or_default();
    assert!(message.contains("https://idp.example#alice"), "{message}");
    assert!(message.contains("prod-governed"), "{message}");
    // The console's key signing twice is still one principal: the second
    // signature must be a DIFFERENT key, of the approver usage.
    assert_eq!(
        reason(governed(
            &[
                (CONSOLE_KEY_ID, GOVERNED_CONSOLE_SIG),
                (CONSOLE_KEY_ID, GOVERNED_CONSOLE_SIG)
            ],
            vec![console(), bob()]
        )),
        "GovernedApprovalRequired"
    );
}

/// **Self-approval is refused**: the approver key belongs to the requester's
/// own principal. An administrator role changes nothing — the comparison is
/// between principals, never roles.
///
/// KILLS: "skip the principal comparison", and "compare key ids" (alice's key
/// id is not the console's, so a key-id comparison would pass it).
#[test]
fn a_requester_countersigning_their_own_request_is_self_approval() {
    let refused = governed(
        &[
            (CONSOLE_KEY_ID, GOVERNED_CONSOLE_SIG),
            (ALICE_KEY_ID, GOVERNED_ALICE_SIG),
        ],
        vec![console(), alice()],
    );
    assert_eq!(reason(refused.clone()), "SelfApprovalRefused");
    let message = refused.err().map(|r| r.to_string()).unwrap_or_default();
    assert!(
        message.contains(ALICE_KEY_ID) && message.contains("#alice"),
        "{message}"
    );
}

/// The approver's signature never substitutes for the console's: without the
/// console attesting the requester, separation of duties is not checkable.
#[test]
fn a_governed_approver_signature_alone_is_not_a_confirmation() {
    assert_eq!(
        reason(governed(
            &[(BOB_KEY_ID, GOVERNED_BOB_SIG)],
            vec![console(), bob()]
        )),
        "KeyIdNotInRoster",
        "no ConsoleConfirmation signature → refused before the document is read"
    );
    assert_eq!(
        reason(v2(
            ORDINARY_DOC,
            &[(BOB_KEY_ID, ORDINARY_BOB_SIG)],
            vec![console(), bob()],
            "team-ordinary",
            now(),
            &expected()
        )),
        "KeyIdNotInRoster",
        "an approver key cannot pose as the console in ordinary mode either"
    );
}

/// Key usage is checked by name: a key declared `GovernedApproval` in the
/// console slot, or `EvidenceSigning` in the approver slot, authorises nothing.
#[test]
fn each_signature_needs_its_own_usage() {
    let mut not_console = console();
    not_console.usages = vec![SpecUsage::GovernedApproval];
    assert_eq!(reason(ordinary(vec![not_console])), "KeyIdNotInRoster");
    let mut evidence_bob = bob();
    evidence_bob.usages = vec![SpecUsage::EvidenceSigning];
    assert_eq!(
        reason(governed(
            &[
                (CONSOLE_KEY_ID, GOVERNED_CONSOLE_SIG),
                (BOB_KEY_ID, GOVERNED_BOB_SIG)
            ],
            vec![console(), evidence_bob]
        )),
        "KeyIdNotInRoster"
    );
}

/// The legacy synthesis carries no `ConsoleConfirmation` key (D3 §7.3), so a
/// v2 document can never verify against a roster-only namespace.
#[test]
fn a_roster_only_namespace_offers_no_console_key() {
    let roster = weirkeeper::crds::trust_roster::TrustRosterSpec {
        approver_keys: vec![weirkeeper::crds::trust_roster::KeyEntry {
            key_id: CONSOLE_KEY_ID.to_string(),
            spki_pem: CONSOLE_PEM.to_string(),
            subject: None,
            not_after: None,
        }],
        signing_keys: Vec::new(),
        allowed_cluster_ids: Vec::new(),
    };
    let refused = approval::evaluate_authorization_v2(
        ORDINARY_DOC.as_bytes(),
        sidecar(V2_PAYLOAD, &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)]).as_bytes(),
        &weirkeeper::trust::synthesize_legacy(&roster),
        now(),
        &expected(),
        &policy("team-ordinary"),
    );
    assert_eq!(
        reason(refused),
        "KeyIdNotInRoster",
        "the same key rostered as an approver is NOT a console key"
    );
}

/// A withdrawn console key confirms nothing new.
#[test]
fn a_revoked_console_key_confirms_nothing() {
    let mut revoked = console();
    revoked.state = KeyState::Revoked;
    revoked.revoked_at = Some(at("2026-09-01T00:00:00Z"));
    revoked.revocation_effective_from = Some(at("2026-09-01T00:00:00Z"));
    revoked.revocation_reason = Some(RevocationReason::KeyCompromise);
    assert_eq!(reason(ordinary(vec![revoked])), "KeyRevoked");
}

/// **A withdrawn GOVERNED APPROVER key authorises nothing new** (review M2):
/// retired, revoked, or past its `notAfter`, bob's countersignature over a
/// valid governed document leaves the Approval unverified, each with its own
/// reason. The control is the same signatures under bob's active key.
///
/// KILLS: "skip `may_sign_new_for` for the GovernedApproval usage".
#[test]
fn a_withdrawn_approver_key_authorises_no_governed_request() {
    let signatures = [
        (CONSOLE_KEY_ID, GOVERNED_CONSOLE_SIG),
        (BOB_KEY_ID, GOVERNED_BOB_SIG),
    ];
    assert_eq!(
        reason(governed(&signatures, vec![console(), bob()])),
        "Verified",
        "the control: bob's active key verifies"
    );

    let mut retired = bob();
    retired.state = KeyState::Retired;
    retired.retired_at = Some(at("2026-09-01T00:00:00Z"));
    assert_eq!(
        reason(governed(&signatures, vec![console(), retired])),
        "KeyRetired"
    );

    let mut revoked = bob();
    revoked.state = KeyState::Revoked;
    revoked.revoked_at = Some(at("2026-09-01T00:00:00Z"));
    revoked.revocation_effective_from = Some(at("2026-09-01T00:00:00Z"));
    revoked.revocation_reason = Some(RevocationReason::KeyCompromise);
    assert_eq!(
        reason(governed(&signatures, vec![console(), revoked])),
        "KeyRevoked"
    );

    let mut expired = bob();
    expired.not_after = at("2026-09-09T12:30:00Z");
    assert_eq!(
        reason(governed(&signatures, vec![console(), expired])),
        "KeyIdExpired"
    );
}

/// **Separation fails closed on a principal that cannot be compared**
/// (review M4). bob's key recorded as `bob@example.com` — the "email" the
/// TrustPolicy description once invited — could be the requester under
/// another spelling, so it establishes nothing: `SelfApprovalRefused`, with
/// the form named. The control is the same key as `https://idp.example#bob`.
///
/// KILLS: "compare the strings and call any difference separation".
#[test]
fn an_approver_principal_not_in_issuer_subject_form_is_refused() {
    let signatures = [
        (CONSOLE_KEY_ID, GOVERNED_CONSOLE_SIG),
        (BOB_KEY_ID, GOVERNED_BOB_SIG),
    ];
    for other_form in ["alice@example.com", "bob", "install:sha256:0f"] {
        let mut email = bob();
        email.principal.id = other_form.to_string();
        let refused = governed(&signatures, vec![console(), email]);
        assert_eq!(
            reason(refused.clone()),
            "SelfApprovalRefused",
            "{other_form}"
        );
        let message = refused.err().map(|r| r.to_string()).unwrap_or_default();
        assert!(message.contains("<issuer>#<subject>"), "{message}");
    }
    assert_eq!(
        reason(governed(&signatures, vec![console(), bob()])),
        "Verified"
    );
}

/// **Governed requires a change ticket** (D0: "ticket (required in Governed,
/// optional in Ordinary)"): the same governed document without one, signed by
/// the console AND a distinct approver, does not verify. The Ordinary fixture
/// carries none and verifies (`an_ordinary_confirmation_verifies_...`).
#[test]
fn a_governed_document_without_a_ticket_authorises_nothing() {
    let refused = v2(
        GOVERNED_NO_TICKET_DOC,
        &[
            (CONSOLE_KEY_ID, GOVERNED_NO_TICKET_CONSOLE_SIG),
            (BOB_KEY_ID, GOVERNED_NO_TICKET_BOB_SIG),
        ],
        vec![console(), bob()],
        "prod-governed",
        now(),
        &expected(),
    );
    assert_eq!(reason(refused.clone()), "AuthorizationDocumentInvalid");
    let message = refused.err().map(|r| r.to_string()).unwrap_or_default();
    assert!(message.contains("ticket"), "{message}");
    assert!(
        GOVERNED_DOC.contains(r#""ticket":"CHG-4711""#),
        "the control carries one"
    );
}

// ---------------------------------------------------------------------------
// Expiry, changed plan, changed subject
// ---------------------------------------------------------------------------

#[test]
fn an_expired_document_authorises_nothing() {
    let refused = v2(
        ORDINARY_DOC,
        &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)],
        vec![console()],
        "team-ordinary",
        at("2026-09-09T13:10:00Z"),
        &expected(),
    );
    assert_eq!(
        reason(refused),
        "AuthorizationExpired",
        "expiresAt is exclusive"
    );
    let governed_late = v2(
        GOVERNED_DOC,
        &[
            (CONSOLE_KEY_ID, GOVERNED_CONSOLE_SIG),
            (BOB_KEY_ID, GOVERNED_BOB_SIG),
        ],
        vec![console(), bob()],
        "prod-governed",
        at("2026-09-10T12:00:01Z"),
        &expected(),
    );
    assert_eq!(reason(governed_late), "AuthorizationExpired");
}

/// The console-key window bounds the verdict too: a verdict must be re-made
/// no later than the console key's `notAfter`.
#[test]
fn the_verdict_expires_at_the_earlier_of_the_document_and_the_console_key() {
    let mut short = console();
    short.not_after = at("2026-09-09T13:05:00Z");
    let verified = ordinary(vec![short]).expect("still inside both windows");
    assert_eq!(
        verified.document_expires_at,
        Some(at("2026-09-09T13:05:00Z"))
    );
    assert_eq!(
        ApprovalOutcome::Verified(verified).valid_until(),
        Some(at("2026-09-09T13:05:00Z"))
    );
}

#[test]
fn a_changed_plan_needs_a_new_confirmation() {
    let mut other = expected();
    other.plan_hash = logweir_core::ids::sha256_prefixed(b"another plan");
    assert_eq!(
        reason(v2(
            ORDINARY_DOC,
            &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)],
            vec![console()],
            "team-ordinary",
            now(),
            &other
        )),
        "PlanHashMismatch"
    );
}

#[test]
fn a_recreated_subject_is_another_subject() {
    let mut recreated = expected();
    recreated.uid = "restore-uid-2".to_string();
    assert_eq!(
        reason(v2(
            ORDINARY_DOC,
            &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)],
            vec![console()],
            "team-ordinary",
            now(),
            &recreated
        )),
        "AuthorizationSubjectMismatch"
    );
}

// ---------------------------------------------------------------------------
// Unauthorized policy downgrade
// ---------------------------------------------------------------------------

/// **The downgrade.** A genuine ordinary confirmation presented in a namespace
/// bound Governed is refused on the POLICY — it names another policy, digest
/// and mode — before any approver signature is even looked for.
#[test]
fn an_ordinary_confirmation_in_a_governed_namespace_is_refused() {
    assert_eq!(
        reason(v2(
            ORDINARY_DOC,
            &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)],
            vec![console(), bob()],
            "prod-governed",
            now(),
            &expected()
        )),
        "ApprovalPolicyMismatch"
    );
}

/// Editing a policy is a new policy: the old digest no longer matches, so a
/// document issued under it must be re-confirmed.
#[test]
fn an_edited_policy_invalidates_documents_issued_under_the_old_one() {
    let edited = ApprovalPolicySet::parse(&format!(
        "{}namespaces:\n  {NS}: team-ordinary\n",
        POLICY_DOC.replace("maxAgeSeconds: 900", "maxAgeSeconds: 901")
    ))
    .expect("valid");
    let edited = edited.resolve(NS).bound().cloned().expect("bound");
    assert_eq!(edited.mode, ApprovalMode::Ordinary);
    let refused = approval::evaluate_authorization_v2(
        ORDINARY_DOC.as_bytes(),
        sidecar(V2_PAYLOAD, &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)]).as_bytes(),
        &trust(vec![console()]),
        now(),
        &expected(),
        &edited,
    );
    assert_eq!(reason(refused), "ApprovalPolicyMismatch");
}

#[test]
fn a_v1_payload_type_is_not_a_v2_document() {
    let refused = approval::evaluate_authorization_v2(
        ORDINARY_DOC.as_bytes(),
        sidecar(
            weirkeeper::controllers::approval::PAYLOAD_TYPE_APPROVAL,
            &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)],
        )
        .as_bytes(),
        &trust(vec![console()]),
        now(),
        &expected(),
        &policy("team-ordinary"),
    );
    assert_eq!(reason(refused), "PayloadTypeMismatch");
}

// ---------------------------------------------------------------------------
// The reconciler: the binding decides, never the document
// ---------------------------------------------------------------------------

fn approval_object(doc: &str, sidecar_bytes: &str) -> Approval {
    Approval {
        metadata: ObjectMeta {
            name: Some("a1".to_string()),
            namespace: Some(NS.to_string()),
            generation: Some(1),
            resource_version: Some("4071".to_string()),
            ..ObjectMeta::default()
        },
        spec: ApprovalSpec {
            subject_ref: SubjectRef {
                kind: SubjectKind::Restore,
                name: "r1".to_string(),
            },
            plan_hash: PLAN_HASH.to_string(),
            approval_bytes: doc.to_string(),
            sidecar_bytes: sidecar_bytes.to_string(),
        },
        status: None,
    }
}

fn restore_body() -> String {
    let plan = PLAN_BYTES.replace('\n', "\\n");
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"Restore",
             "metadata":{{"name":"r1","namespace":"{NS}","uid":"{RESTORE_UID}"}},
             "spec":{{"planBytes":"{plan}","approvalRef":{{"name":"a1"}},
                      "sourceArchive":{{"url":"s3://archive/logweir"}},
                      "backupSetRef":"bk-1","pointInTime":"2026-09-02T00:00:00Z",
                      "target":{{"clusterRef":{{"name":"scratch"}},"mode":"scratch",
                                 "topicNaming":{{"prefix":"restored-"}}}},
                      "deadlineSeconds":900}}}}"#
    )
}

fn routes(keys: Vec<SpecKey>) -> Vec<Route> {
    let list = serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "TrustPolicyList",
        "metadata": {"resourceVersion": "1"},
        "items": [trust_policy(keys)],
    });
    vec![
        Route {
            method: "GET",
            path_suffix: "/trustpolicies",
            status: 200,
            body: list.to_string(),
        },
        Route {
            method: "GET",
            path_suffix: "/restores/r1",
            status: 200,
            body: restore_body(),
        },
    ]
}

async fn decide(approval: &Approval, policies: &ApprovalPolicySet) -> ApprovalOutcome {
    let (client, _recorder) = mock_client_recording(routes(vec![console(), bob(), v1_approver()]));
    approval::decide_with_policy_at(approval, &client, policies, now())
        .await
        .expect("a verdict")
}

fn outcome_reason(outcome: &ApprovalOutcome) -> &'static str {
    outcome.reason()
}

/// An unbound namespace is `legacy-governed-v1`: today's v1 approval still
/// verifies (existing Approvals stay valid) and a v2 document is refused on the
/// policy, never verified by accident.
#[tokio::test]
async fn an_unbound_namespace_keeps_todays_approval_and_refuses_v2() {
    let legacy = ApprovalPolicySet::default();
    let v1 = approval_object(
        V1_DOC,
        &sidecar(approval::PAYLOAD_TYPE_APPROVAL, &[(V1_KEY_ID, V1_SIG)]),
    );
    let outcome = decide(&v1, &legacy).await;
    assert!(
        outcome.is_verified(),
        "an existing v1 approval stays valid: {outcome:?}"
    );
    let ApprovalOutcome::Verified(v) = &outcome else {
        unreachable!()
    };
    assert_eq!(
        v.authorization, None,
        "a v1 approval publishes no policy provenance"
    );

    let v2_doc = approval_object(
        ORDINARY_DOC,
        &sidecar(V2_PAYLOAD, &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)]),
    );
    let outcome = decide(&v2_doc, &legacy).await;
    assert_eq!(outcome_reason(&outcome), "ApprovalPolicyMismatch");
    assert!(
        outcome.message().contains("legacy-governed-v1"),
        "{}",
        outcome.message()
    );
}

/// A bound namespace accepts v2 only: a genuine v1 approval — which carries no
/// console-attested requester — is refused on the policy.
#[tokio::test]
async fn a_bound_namespace_refuses_a_v1_approval() {
    for bound in ["prod-governed", "team-ordinary"] {
        let v1 = approval_object(
            V1_DOC,
            &sidecar(approval::PAYLOAD_TYPE_APPROVAL, &[(V1_KEY_ID, V1_SIG)]),
        );
        let outcome = decide(&v1, &bind(bound)).await;
        assert_eq!(
            outcome_reason(&outcome),
            "ApprovalPolicyMismatch",
            "{bound}"
        );
    }
}

/// Through the reconciler, end to end: the ordinary confirmation verifies in
/// the Ordinary namespace and the status carries the provenance; the same
/// object in a Governed namespace is refused and the status CLEARS it.
#[tokio::test]
async fn the_reconciler_verifies_by_the_binding_and_clears_provenance_on_refusal() {
    let object = approval_object(
        ORDINARY_DOC,
        &sidecar(V2_PAYLOAD, &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)]),
    );
    let outcome = decide(&object, &bind("team-ordinary")).await;
    assert!(outcome.is_verified(), "{outcome:?}");
    assert!(
        outcome.message().contains("approvalPolicy=team-ordinary"),
        "{}",
        outcome.message()
    );

    let governed_ns = decide(&object, &bind("prod-governed")).await;
    assert_eq!(outcome_reason(&governed_ns), "ApprovalPolicyMismatch");

    // A verdict's provenance is published, and a refusal nulls it.
    let verified = ordinary(vec![console()]).expect("verified");
    let status = approval::status_for(&object, &ApprovalOutcome::Verified(verified), now());
    assert!(status.authorization.is_some());
    let refused = approval::status_for(&object, &governed_ns, now() + Duration::seconds(1));
    assert_eq!(refused.authorization, None);
    let body = approval::status_patch_body(&refused);
    assert!(
        body["status"]
            .as_object()
            .is_some_and(|m| m.get("authorization") == Some(&serde_json::Value::Null)),
        "a refusal must null the provenance a merge patch would otherwise leave: {body}"
    );
    assert!(CLEARABLE_STATUS_FIELDS.contains(&"authorization"));
}
