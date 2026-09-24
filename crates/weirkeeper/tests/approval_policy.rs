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

/// This Approval object's UID — what the Restore's approval bundle records
/// when the Restore controller admits under it (review L3).
const APPROVAL_UID: &str = "approval-uid-1";

fn approval_object(doc: &str, sidecar_bytes: &str) -> Approval {
    Approval {
        metadata: ObjectMeta {
            name: Some("a1".to_string()),
            namespace: Some(NS.to_string()),
            uid: Some(APPROVAL_UID.to_string()),
            creation_timestamp: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(at(
                "2026-09-09T12:55:30Z",
            ))),
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

// ---------------------------------------------------------------------------
// P9 — a verdict its Restore's admission consumed is a record, not a gate
// ---------------------------------------------------------------------------
//
// poc-install, 2026-09-24: 900 s (`maxAgeSeconds`) after an Ordinary
// confirmation the controller re-judged the Approval of an ALREADY-ADMITTED,
// SUCCEEDED Restore, rewrote it `Verified=False/AuthorizationExpired`, nulled
// the `authorization` it had recorded, and — because the expiry message named
// the clock — wrote a new status every pass and logged `approval refused`
// every ~2 s for ever. D0: expiry bounds the time to ADMISSION.

/// Past the Ordinary document's `expiresAt` (13:10), so past `maxAgeSeconds`.
fn after_expiry() -> DateTime<Utc> {
    at("2026-09-09T13:20:00Z")
}

/// The Restore `r1`, carrying the status its controller wrote.
fn restore_with_status(status: serde_json::Value) -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(&restore_body()).expect("the fixture Restore parses");
    value["status"] = status;
    value.to_string()
}

/// ADMITTED at `when`: the Job was created under this Approval.
fn restore_admitted(when: &str) -> String {
    restore_with_status(serde_json::json!({
        "phase": "Succeeded",
        "conditions": [{
            "type": "Admitted", "status": "True", "reason": "Admitted",
            "lastTransitionTime": when,
            "message": "the approval is Verified=True, the recomputed plan hash matches"
        }]
    }))
}

/// HELD, not admitted: `Admitted=False` is a Restore still waiting.
fn restore_held() -> String {
    restore_with_status(serde_json::json!({
        "phase": "Pending",
        "conditions": [{
            "type": "Admitted", "status": "False", "reason": "DestinationNotValid",
            "lastTransitionTime": "2026-09-09T13:01:00Z",
            "message": "held for the destination"
        }]
    }))
}

/// The Restore's own approval bundle, as the Restore controller writes it
/// before the Job: controlled by the Restore UID and naming the Approval UID it
/// admitted under (review L3).
fn bundle_body(approval_uid: &str) -> String {
    serde_json::json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {
            "name": "r1-approval-bundle", "namespace": NS, "uid": "bundle-uid",
            "annotations": {
                "logweir.dev/restore-uid": RESTORE_UID,
                "logweir.dev/approval-name": "a1",
                "logweir.dev/approval-uid": approval_uid,
            },
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore", "name": "r1",
                "uid": RESTORE_UID, "controller": true, "blockOwnerDeletion": true,
            }],
        },
        "immutable": true,
        "data": {},
    })
    .to_string()
}

/// A Kubernetes `Status` 404.
fn not_found(message: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure","message":"{message}",
             "reason":"NotFound","code":404}}"#
    )
}

/// The routes a pass needs: the trust (with `keys`), the Restore, and its
/// bundle (`None` answers 404 — a Restore admitted before per-Restore bundles).
fn routes_with(restore: String, keys: Vec<SpecKey>, bundle: Option<String>) -> Vec<Route> {
    let mut table: Vec<Route> = routes(keys)
        .into_iter()
        .map(|mut route| {
            if route.path_suffix == "/restores/r1" {
                route.body = restore.clone();
            }
            route
        })
        .collect();
    table.push(match bundle {
        Some(body) => Route {
            method: "GET",
            path_suffix: "/configmaps/r1-approval-bundle",
            status: 200,
            body,
        },
        None => Route {
            method: "GET",
            path_suffix: "/configmaps/r1-approval-bundle",
            status: 404,
            body: not_found("configmaps r1-approval-bundle not found"),
        },
    });
    table
}

fn routes_over(restore: String) -> Vec<Route> {
    routes_with(
        restore,
        vec![console(), bob(), v1_approver()],
        Some(bundle_body(APPROVAL_UID)),
    )
}

fn ordinary_object() -> Approval {
    approval_object(
        ORDINARY_DOC,
        &sidecar(V2_PAYLOAD, &[(CONSOLE_KEY_ID, ORDINARY_CONSOLE_SIG)]),
    )
}

/// The Ordinary Approval carrying the status this controller wrote when it
/// verified it at 13:00 — before the Restore was admitted.
async fn verified_ordinary() -> Approval {
    let mut object = ordinary_object();
    let outcome = decide(&object, &bind("team-ordinary")).await;
    assert!(
        matches!(outcome, ApprovalOutcome::Verified(_)),
        "the fixture verifies inside its window: {outcome:?}"
    );
    object.status = Some(approval::status_for(&object, &outcome, now()));
    object
}

async fn decide_over(
    approval: &Approval,
    restore: String,
    when: DateTime<Utc>,
) -> (ApprovalOutcome, Vec<String>) {
    let (client, recorder) = mock_client_recording(routes_over(restore));
    let outcome = approval::decide_with_policy_at(approval, &client, &bind("team-ordinary"), when)
        .await
        .expect("a verdict");
    let seen = recorder
        .lock()
        .expect("the recorder is never poisoned")
        .iter()
        .map(|r| format!("{} {}", r.method, r.uri))
        .collect();
    (outcome, seen)
}

fn condition<'a>(
    status: &'a weirkeeper::crds::approval::ApprovalStatus,
    kind: &str,
) -> &'a weirkeeper::crds::Condition {
    status
        .conditions
        .as_ref()
        .and_then(|cs| cs.iter().find(|c| c.r#type == kind))
        .unwrap_or_else(|| panic!("no {kind} condition in {status:?}"))
}

#[tokio::test]
async fn an_admitted_restore_keeps_its_approval_verified_past_its_expiry() {
    let object = verified_ordinary().await;
    let stored = object.status.clone().expect("verified status");
    assert!(
        stored.authorization.is_some(),
        "the fixture recorded provenance"
    );

    let (outcome, seen) = decide_over(
        &object,
        restore_admitted("2026-09-09T13:01:00Z"),
        after_expiry(),
    )
    .await;
    assert!(
        outcome.is_consumed() && outcome.is_verified(),
        "a Restore admitted at 13:01 consumed the verdict; at 13:20 it is kept, not re-judged: \
         {outcome:?}"
    );
    // ONE TRUST READ (review M1): a compromise revocation is the one key event
    // that still reaches a consumed record, so its keys are looked up — and
    // nothing about the document's window or the binding is asked again.
    assert_eq!(
        seen.iter().filter(|r| r.contains("trustpolicies")).count(),
        1,
        "{seen:?}"
    );
    assert!(
        seen.iter()
            .any(|r| r.contains("/configmaps/r1-approval-bundle")),
        "the admission is bound to this Approval OBJECT through the Restore's bundle: {seen:?}"
    );

    // THE RECORD IS KEPT: every field the verdict wrote, and the Verified
    // condition with its ORIGINAL transition time and message.
    let status = approval::status_for(&object, &outcome, after_expiry());
    assert_eq!(status.verified, Some(true));
    assert_eq!(
        status.authorization, stored.authorization,
        "mode, requester, key kept"
    );
    assert_eq!(status.matched_key_id, stored.matched_key_id);
    assert_eq!(status.approver, stored.approver);
    assert_eq!(status.approver_key_window, stored.approver_key_window);
    assert_eq!(status.verified_subject_ref, stored.verified_subject_ref);
    assert_eq!(
        condition(&status, "Verified"),
        condition(&stored, "Verified"),
        "the Verified condition is carried verbatim"
    );
    let consumed = condition(&status, approval::CONDITION_CONSUMED);
    assert_eq!(consumed.status, "True");
    assert_eq!(
        consumed.last_transition_time,
        Some(at("2026-09-09T13:01:00Z")),
        "Consumed's transition is the ADMISSION instant the Restore controller recorded — the \
         independent observation a later compromise is compared against"
    );
    assert_eq!(
        consumed.reason.as_deref(),
        Some(approval::REASON_RESTORE_ADMITTED)
    );
    let message = consumed.message.clone().unwrap_or_default();
    assert!(
        message.contains(RESTORE_UID) && message.contains("2026-09-09T13:01:00"),
        "the condition names the Restore UID and the admission instant: {message}"
    );
    let body = approval::status_patch_body(&status);
    assert!(
        body["status"]["authorization"].is_object(),
        "the patch keeps the provenance instead of nulling it: {body}"
    );

    // THE NEXT PASS READS ONLY THE TRUST AND WRITES NOTHING, a day later.
    let mut written = object.clone();
    written.status = Some(status.clone());
    let (client, recorder) = mock_client_recording(routes(vec![console(), bob()]));
    let later = at("2026-09-10T13:20:00Z");
    let again = approval::decide_with_policy_at(&written, &client, &bind("prod-governed"), later)
        .await
        .expect("a verdict");
    assert!(
        again.is_consumed() && again.is_verified(),
        "even under a changed binding a consumed record stands: {again:?}"
    );
    let reads: Vec<String> = recorder
        .lock()
        .expect("recorder")
        .iter()
        .map(|r| r.uri.clone())
        .collect();
    assert!(
        reads.iter().all(|u| u.contains("trustpolicies")) && reads.len() == 1,
        "a recorded consumption reads the namespace's trust and nothing else: {reads:?}"
    );
    let next = approval::status_patch_body(&approval::status_for(&written, &again, later));
    assert!(
        weirkeeper::conditions::status_unchanged(
            serde_json::to_value(&status).ok().as_ref(),
            &next
        ),
        "the second pass computes the same status byte for byte: {next}"
    );
    assert_eq!(
        approval::requeue_for(&again, later),
        None,
        "a consumed record waits for a change; no timer re-reads it"
    );
}

#[tokio::test]
async fn a_not_admitted_restore_still_expires() {
    // FAIL CLOSED, UNCHANGED: a Restore with no Job yet — never admitted, or
    // held `Admitted=False` — is a request the expiry still refuses.
    for (label, restore) in [("absent", restore_body()), ("held", restore_held())] {
        let object = verified_ordinary().await;
        let (outcome, _) = decide_over(&object, restore, after_expiry()).await;
        assert_eq!(
            outcome.reason(),
            "AuthorizationExpired",
            "{label}: an unadmitted request past its expiry authorises nothing"
        );
        assert!(!outcome.is_consumed(), "{label}");
        let status = approval::status_for(&object, &outcome, after_expiry());
        assert_eq!(status.authorization, None, "{label}");
        assert_eq!(status.verified, Some(false), "{label}");
    }
}

#[tokio::test]
async fn an_expired_refusal_is_written_once_and_the_requeue_is_the_heartbeat() {
    let object = verified_ordinary().await;
    let mut written = object.clone();
    written.metadata.resource_version = Some("4072".to_string());
    let mut routes = routes_over(restore_body());
    routes.push(Route {
        method: "PATCH",
        path_suffix: "/approvals/a1/status",
        status: 200,
        body: serde_json::to_string(&written).expect("serialises"),
    });
    let (client, recorder) = mock_client_recording(routes);
    let policies = bind("team-ordinary");
    let first =
        approval::reconcile_approval_with_policy_at(&object, &client, &policies, after_expiry())
            .await
            .expect("a verdict");
    assert_eq!(first.reason(), "AuthorizationExpired");
    written.status = Some(approval::status_for(&object, &first, after_expiry()));

    // FIVE MINUTES LATER, over the object the first pass wrote.
    let later = after_expiry() + Duration::minutes(5);
    let second = approval::reconcile_approval_with_policy_at(&written, &client, &policies, later)
        .await
        .expect("a verdict");
    assert_eq!(
        first.message(),
        second.message(),
        "the refusal names no clock, so two instants compute one message"
    );
    let patches = recorder
        .lock()
        .expect("recorder")
        .iter()
        .filter(|r| r.method == "PATCH")
        .count();
    assert_eq!(
        patches, 1,
        "the withdrawal is written once; an unchanged refusal writes nothing, so no write wakes \
         the watch (the P9 storm was one PATCH per pass)"
    );
    let requeue = approval::requeue_for(&second, later).expect("a refusal is re-read on a timer");
    assert!(
        requeue >= std::time::Duration::from_secs(300),
        "a refusal is re-read at the heartbeat, never sooner: {requeue:?}"
    );
}

#[tokio::test]
async fn a_verdict_withdrawn_after_admission_is_re_established_at_the_admission_instant() {
    // THE P9 OBJECTS THEMSELVES: an earlier build rewrote the Approval of an
    // admitted Restore `AuthorizationExpired` and nulled its provenance. This
    // build asks the verdict at the instant the Restore was admitted.
    let mut object = verified_ordinary().await;
    let original = object.status.clone().expect("verified");
    let (withdrawn, _) = decide_over(&object, restore_body(), after_expiry()).await;
    object.status = Some(approval::status_for(&object, &withdrawn, after_expiry()));
    assert_eq!(
        object.status.as_ref().and_then(|s| s.authorization.clone()),
        None
    );

    let late = at("2026-09-09T13:40:00Z");
    let (repaired, _) = decide_over(&object, restore_admitted("2026-09-09T13:01:00Z"), late).await;
    let ApprovalOutcome::Consumed(consumption) = &repaired else {
        panic!("admitted at 13:01, inside the window: {repaired:?}");
    };
    assert!(
        consumption.reverified.is_some(),
        "re-established, not copied"
    );
    let status = approval::status_for(&object, &repaired, late);
    assert_eq!(status.verified, Some(true));
    assert_eq!(status.authorization, original.authorization);
    let verified = condition(&status, "Verified");
    assert_eq!(verified.status, "True");
    assert!(
        verified
            .message
            .as_deref()
            .is_some_and(|m| m.contains("as of the admission at 2026-09-09T13:01:00")),
        "{verified:?}"
    );
    assert_eq!(
        condition(&status, approval::CONDITION_CONSUMED).status,
        "True"
    );

    // AND AN ADMISSION THAT CLAIMS AN INSTANT OUTSIDE THE WINDOW RE-ESTABLISHES
    // NOTHING: the clock moved, the verdict at it is still asked.
    let (outside, _) = decide_over(&object, restore_admitted("2026-09-09T13:15:00Z"), late).await;
    assert_eq!(outside.reason(), "AuthorizationExpired", "{outside:?}");
}

// ---------------------------------------------------------------------------
// Review round (poc-fixes-2): M1 compromise, I1, L3 and the R1-R5 fences
// ---------------------------------------------------------------------------

/// The console key, REVOKED with `reason` effective at `effective`.
fn revoked_console(reason: RevocationReason, effective: &str) -> SpecKey {
    SpecKey {
        state: KeyState::Revoked,
        revoked_at: Some(at("2026-09-10T09:00:00Z")),
        revocation_reason: Some(reason),
        revocation_effective_from: Some(at(effective)),
        ..console()
    }
}

/// The Ordinary Approval as it stands after its Restore was admitted at 13:01
/// and this controller recorded the consumption at 13:20.
async fn consumed_ordinary() -> Approval {
    let mut object = verified_ordinary().await;
    let (outcome, _) = decide_over(
        &object,
        restore_admitted("2026-09-09T13:01:00Z"),
        after_expiry(),
    )
    .await;
    assert!(outcome.is_consumed(), "{outcome:?}");
    object.status = Some(approval::status_for(&object, &outcome, after_expiry()));
    object.metadata.resource_version = Some("4080".to_string());
    object
}

/// One pass over `object` with the namespace trusting `keys` — the only read
/// a recorded consumption makes.
async fn decide_trusting(object: &Approval, keys: Vec<SpecKey>) -> ApprovalOutcome {
    let (client, _) = mock_client_recording(routes(keys));
    approval::decide_with_policy_at(
        object,
        &client,
        &bind("team-ordinary"),
        at("2026-09-11T08:00:00Z"),
    )
    .await
    .expect("a verdict")
}

/// **M1.** A `KeyCompromise` revocation of the console key that confirmed an
/// ADMITTED Restore, effective after the admission, turns the consumed
/// verdict `RecordedBeforeRevocation` — never green — and keeps the record:
/// the authorization, the key id, the approver and `Consumed`. Before this
/// round the consumed record stayed `Verified=True` for ever.
#[tokio::test]
async fn a_compromise_after_the_admission_is_recorded_before_revocation_and_never_green() {
    let object = consumed_ordinary().await;
    let stored = object.status.clone().expect("consumed status");
    let outcome = decide_trusting(
        &object,
        vec![
            revoked_console(RevocationReason::KeyCompromise, "2026-09-09T13:05:00Z"),
            bob(),
        ],
    )
    .await;
    assert!(outcome.is_consumed(), "{outcome:?}");
    assert!(
        !outcome.is_verified(),
        "a compromised signer is never green"
    );
    assert_eq!(
        outcome.reason(),
        approval::REASON_RECORDED_BEFORE_REVOCATION
    );

    let t = at("2026-09-11T08:00:00Z");
    let status = approval::status_for(&object, &outcome, t);
    assert_eq!(status.verified, Some(false));
    let verified = condition(&status, "Verified");
    assert_eq!(verified.status, "False");
    assert_eq!(verified.reason.as_deref(), Some("RecordedBeforeRevocation"));
    let message = verified.message.clone().unwrap_or_default();
    for fact in [
        CONSOLE_KEY_ID,
        "KeyCompromise",
        "2026-09-09T13:05:00",
        "2026-09-09T13:01:00",
        RESTORE_UID,
        "never green",
    ] {
        assert!(
            message.contains(fact),
            "the message names {fact}: {message}"
        );
    }
    // THE RECORD IS KEPT.
    assert_eq!(status.authorization, stored.authorization);
    assert_eq!(status.matched_key_id, stored.matched_key_id);
    assert_eq!(status.approver, stored.approver);
    assert_eq!(status.verified_subject_ref, stored.verified_subject_ref);
    assert_eq!(
        condition(&status, approval::CONDITION_CONSUMED),
        condition(&stored, approval::CONDITION_CONSUMED),
        "Consumed is carried verbatim"
    );
    let body = approval::status_patch_body(&status);
    assert!(body["status"]["authorization"].is_object(), "{body}");
    assert!(body["status"]["matchedKeyId"].is_string(), "{body}");

    // THE SHARED FIXTURE IS WHAT THIS CONTROLLER WRITES: the console renders
    // it never green (`ui/tests/approval-policy.spec.js`) and the API projects
    // into it (`crates/logweir-api/tests/approval_revoked_after_use.rs`).
    let shared: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../ui/tests/fixtures/console/approval-revoked-after-use.json"
        ))
        .expect("the shared fixture is readable"),
    )
    .expect("JSON");
    let written = serde_json::to_value(&status).expect("status");
    let strip = |conditions: &serde_json::Value| -> Vec<serde_json::Value> {
        conditions
            .as_array()
            .expect("conditions")
            .iter()
            .map(|c| {
                serde_json::json!({
                    "type": c["type"], "status": c["status"], "reason": c["reason"],
                    "message": c["message"], "lastTransitionTime": c["lastTransitionTime"],
                })
            })
            .collect()
    };
    assert_eq!(
        strip(&written["conditions"]),
        strip(&shared["item"]["conditions"]),
        "the controller's conditions ARE the fixture's"
    );
    assert_eq!(written["verified"], shared["item"]["verified"]);
    assert_eq!(written["matchedKeyId"], shared["item"]["matchedKeyId"]);
    assert_eq!(
        written["authorization"]["confirmationKeyId"],
        shared["item"]["authorization"]["confirmationKeyId"]
    );

    // WRITTEN ONCE: the next pass over the written object computes the same
    // bytes (no clock in the message) ...
    let mut written = object.clone();
    written.status = Some(status.clone());
    let again = decide_trusting(
        &written,
        vec![
            revoked_console(RevocationReason::KeyCompromise, "2026-09-09T13:05:00Z"),
            bob(),
        ],
    )
    .await;
    let next = approval::status_patch_body(&approval::status_for(
        &written,
        &again,
        t + Duration::hours(1),
    ));
    assert!(
        weirkeeper::conditions::status_unchanged(
            serde_json::to_value(&status).ok().as_ref(),
            &next
        ),
        "an unchanged compromise is never rewritten: {next}"
    );
    // ... AND STICKY: the object recording the revocation disappearing (the
    // namespace now resolves a policy where the key is Active) exonerates
    // nothing. NEGATIVE CONTROL for the sticky rule.
    let lifted = decide_trusting(&written, vec![console(), bob()]).await;
    assert!(!lifted.is_verified(), "{lifted:?}");
    assert_eq!(lifted.reason(), approval::REASON_RECORDED_BEFORE_REVOCATION);
    let kept = approval::status_patch_body(&approval::status_for(&written, &lifted, t));
    assert!(
        weirkeeper::conditions::status_unchanged(
            serde_json::to_value(&status).ok().as_ref(),
            &kept
        ),
        "the withdrawal is kept verbatim: {kept}"
    );
}

/// **M1, the other row.** A compromise effective AT or BEFORE the recorded
/// admission leaves no observation that separates the signature from the
/// compromise: `KeyRevoked`, never green. The boundary is D3's own (`observed
/// < effective`), decided by `logweir_core::trust::decide` — the equality case
/// pins that the approval side calls the same rule.
#[tokio::test]
async fn a_compromise_effective_before_the_admission_is_key_revoked() {
    let object = consumed_ordinary().await;
    for effective in ["2026-09-09T12:59:00Z", "2026-09-09T13:01:00Z"] {
        let outcome = decide_trusting(
            &object,
            vec![revoked_console(RevocationReason::KeyCompromise, effective)],
        )
        .await;
        assert!(!outcome.is_verified(), "{effective}: {outcome:?}");
        assert_eq!(outcome.reason(), "KeyRevoked", "{effective}");
        let key = weirkeeper::trust::from_policy(&trust_policy(vec![revoked_console(
            RevocationReason::KeyCompromise,
            effective,
        )]));
        let core = logweir_core::trust::decide(
            key.key(CONSOLE_KEY_ID).map(|k| &k.trust),
            logweir_core::trust::KeyUsage::ConsoleConfirmation,
            &logweir_core::trust::EvidenceClaim::absent(
                logweir_core::trust::ClaimAbsence::FieldAbsent,
            ),
            &logweir_core::trust::IndependentObservation::at(at("2026-09-09T13:01:00Z")),
            at("2026-09-11T08:00:00Z"),
        );
        assert_eq!(
            core.reason,
            Some(logweir_core::trust::UntrustReason::Revoked),
            "parity: D3's rule answers the same for {effective}"
        );
    }
}

/// **M1, the freeze it keeps.** A `Superseded` revocation and a retirement are
/// time-based boundaries on NEW uses: the consumed record stays green. Only a
/// compromise reaches it.
#[tokio::test]
async fn a_superseded_or_retired_key_leaves_a_consumed_record_green() {
    let object = consumed_ordinary().await;
    let retired = SpecKey {
        state: KeyState::Retired,
        retired_at: Some(at("2026-09-10T09:00:00Z")),
        ..console()
    };
    for keys in [
        vec![revoked_console(
            RevocationReason::Superseded,
            "2026-09-09T12:00:00Z",
        )],
        vec![retired],
    ] {
        let outcome = decide_trusting(&object, keys).await;
        assert!(
            outcome.is_consumed() && outcome.is_verified(),
            "{outcome:?}"
        );
    }
}

/// **I1's guard.** A `Consumed=True` status planted on an Approval whose
/// recorded key never signed its bytes is not a record of anything: it is
/// judged from scratch, refused, and the planted `Consumed` is dropped.
#[tokio::test]
async fn a_planted_consumption_over_a_signature_that_does_not_verify_is_judged_again() {
    let genuine = consumed_ordinary().await;
    let mut planted = approval_object(
        ORDINARY_DOC,
        // bob's signature, filed under the CONSOLE key id: not the console's.
        &sidecar(V2_PAYLOAD, &[(CONSOLE_KEY_ID, ORDINARY_BOB_SIG)]),
    );
    planted.status = genuine.status.clone();
    let (outcome, _) = decide_over(
        &planted,
        restore_admitted("2026-09-09T13:01:00Z"),
        after_expiry(),
    )
    .await;
    assert!(
        !outcome.is_consumed() && !outcome.is_verified(),
        "NEGATIVE CONTROL: the planted record is not kept: {outcome:?}"
    );
    let status = approval::status_for(&planted, &outcome, after_expiry());
    assert!(
        status
            .conditions
            .as_ref()
            .is_some_and(|cs| cs.iter().all(|c| c.r#type != approval::CONDITION_CONSUMED)),
        "{status:?}"
    );
}

/// **L3.** The admission binds the Approval OBJECT. An Approval deleted and
/// re-created under the same name is another object: the Restore's bundle
/// names the old UID, so the new object's verdict was consumed by nothing and
/// still expires. Without a bundle (a Restore admitted before per-Restore
/// bundles), the object must have existed at the admission.
#[tokio::test]
async fn an_approval_recreated_under_the_same_name_was_admitted_under_nothing() {
    let mut recreated = verified_ordinary().await;
    recreated.metadata.uid = Some("approval-uid-2".to_string());
    let keys = || vec![console(), bob()];
    let (client, _) = mock_client_recording(routes_with(
        restore_admitted("2026-09-09T13:01:00Z"),
        keys(),
        Some(bundle_body(APPROVAL_UID)),
    ));
    let outcome = approval::decide_with_policy_at(
        &recreated,
        &client,
        &bind("team-ordinary"),
        after_expiry(),
    )
    .await
    .expect("a verdict");
    assert_eq!(
        outcome.reason(),
        "AuthorizationExpired",
        "the bundle names approval-uid-1: {outcome:?}"
    );

    // NO BUNDLE: the fallback is "this object existed at the admission".
    let mut late = verified_ordinary().await;
    late.metadata.creation_timestamp = Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
        at("2026-09-09T13:02:00Z"),
    ));
    let (client, _) = mock_client_recording(routes_with(
        restore_admitted("2026-09-09T13:01:00Z"),
        keys(),
        None,
    ));
    let outcome =
        approval::decide_with_policy_at(&late, &client, &bind("team-ordinary"), after_expiry())
            .await
            .expect("a verdict");
    assert_eq!(outcome.reason(), "AuthorizationExpired", "{outcome:?}");
    // NEGATIVE CONTROL: the same object, created before the admission.
    let early = verified_ordinary().await;
    let (client, _) = mock_client_recording(routes_with(
        restore_admitted("2026-09-09T13:01:00Z"),
        keys(),
        None,
    ));
    let outcome =
        approval::decide_with_policy_at(&early, &client, &bind("team-ordinary"), after_expiry())
            .await
            .expect("a verdict");
    assert!(outcome.is_consumed(), "{outcome:?}");
}

/// **R2 (pure).** A Restore that names ANOTHER Approval did not consume this
/// one, whatever its bundle claims.
#[tokio::test]
async fn a_restore_naming_another_approval_did_not_consume_this_one() {
    let object = verified_ordinary().await;
    let mut restore: weirkeeper::crds::restore::Restore =
        serde_json::from_str(&restore_admitted("2026-09-09T13:01:00Z")).expect("a Restore");
    let bundle: k8s_openapi::api::core::v1::ConfigMap =
        serde_json::from_str(&bundle_body(APPROVAL_UID)).expect("a ConfigMap");
    assert!(
        approval::admitted_under(&restore, &object, Some(&bundle)).is_some(),
        "the fixture itself is admitted under a1"
    );
    restore.spec.approval_ref = Some(weirkeeper::crds::LocalRef {
        name: "a2".to_string(),
    });
    assert_eq!(
        approval::admitted_under(&restore, &object, Some(&bundle)),
        None
    );
    // And a bundle that is not demonstrably this Restore's answers nothing.
    let mut foreign = bundle.clone();
    foreign.metadata.owner_references = None;
    assert_eq!(approval::admitted_approval_uid(&foreign, RESTORE_UID), None);
}

/// **R1.** The freeze is for the SAME Restore UID. The name now answers with
/// another object that was admitted: the verdict was recorded for the old
/// one, so it is `ReferentUidChanged`, never `Consumed`.
#[tokio::test]
async fn a_recreated_restore_does_not_consume_the_old_verdict() {
    let object = verified_ordinary().await;
    let mut other: serde_json::Value =
        serde_json::from_str(&restore_admitted("2026-09-09T13:01:00Z")).expect("json");
    other["metadata"]["uid"] = serde_json::json!("restore-uid-2");
    let mut bundle: serde_json::Value =
        serde_json::from_str(&bundle_body(APPROVAL_UID)).expect("json");
    bundle["metadata"]["annotations"]["logweir.dev/restore-uid"] =
        serde_json::json!("restore-uid-2");
    bundle["metadata"]["ownerReferences"][0]["uid"] = serde_json::json!("restore-uid-2");
    let (client, _) = mock_client_recording(routes_with(
        other.to_string(),
        vec![console(), bob()],
        Some(bundle.to_string()),
    ));
    let outcome =
        approval::decide_with_policy_at(&object, &client, &bind("team-ordinary"), after_expiry())
            .await
            .expect("a verdict");
    assert_eq!(outcome.reason(), "ReferentUidChanged", "{outcome:?}");
}

/// **R3.** The repair (a verdict asked at the admission instant) is only for a
/// verdict once BOUND to this Restore UID. A fresh Approval with no status —
/// re-applied from captured JSON, say — whose document expired is refused at
/// `now`, even though its Restore was admitted while it was valid.
#[tokio::test]
async fn a_never_bound_approval_is_not_repaired_at_the_admission_instant() {
    let fresh = ordinary_object();
    let (outcome, _) = decide_over(
        &fresh,
        restore_admitted("2026-09-09T13:01:00Z"),
        after_expiry(),
    )
    .await;
    assert_eq!(outcome.reason(), "AuthorizationExpired", "{outcome:?}");
}

/// **R4.** A verified, NOT-admitted Approval is re-read AT its own expiry
/// (TRUST-EXPIRY-LAG's rule), never left to wait for a change.
#[tokio::test]
async fn a_verified_unadmitted_approval_requeues_at_its_expiry() {
    let object = ordinary_object();
    let (client, _) = mock_client_recording(routes_over(restore_body()));
    let when = at("2026-09-09T13:08:00Z");
    let outcome = approval::decide_with_policy_at(&object, &client, &bind("team-ordinary"), when)
        .await
        .expect("a verdict");
    assert!(
        outcome.is_verified() && !outcome.is_consumed(),
        "{outcome:?}"
    );
    assert_eq!(
        approval::requeue_for(&outcome, when),
        Some(std::time::Duration::from_secs(120)),
        "the document expires at 13:10, 120 s away"
    );
}

/// **R5.** A Restore that is gone is not an admission: the stored verdict is
/// judged, and the referent is reported missing.
#[tokio::test]
async fn a_deleted_restore_is_not_an_admission() {
    let object = verified_ordinary().await;
    let mut table = routes_over(restore_body());
    for route in &mut table {
        if route.path_suffix == "/restores/r1" {
            route.status = 404;
            route.body = not_found("restores r1 not found");
        }
    }
    let (client, _) = mock_client_recording(table);
    let outcome =
        approval::decide_with_policy_at(&object, &client, &bind("team-ordinary"), after_expiry())
            .await
            .expect("a verdict");
    assert_eq!(outcome.reason(), "ReferentNotFound", "{outcome:?}");
}

// ---------------------------------------------------------------------------
// TRUSTPOLICY-DELETE-DROPS-REVOCATION: a compromise recorded on ANOTHER policy
// ---------------------------------------------------------------------------

/// **A compromise is a fact about the key, not about the policy.** The
/// namespace's own policy lists the console key `Active` (a successor applied
/// from an export taken before the revocation, or the namespace re-bound), and
/// a DIFFERENT policy records its `KeyCompromise` revocation. A consumed record
/// whose stored status is still CLEAN — so the sticky rule is not what answers
/// — reads the compromise all the same: `RecordedBeforeRevocation`, never
/// green.
///
/// NEGATIVE CONTROL beside it: without the recording policy the same pass is
/// `Verified`, so the refusal is the record's and nothing else's.
#[tokio::test]
async fn a_compromise_recorded_on_another_policy_reaches_a_consumed_record() {
    let object = consumed_ordinary().await;
    assert_eq!(
        object.status.as_ref().and_then(|s| s.verified),
        Some(true),
        "the stored record is clean: nothing sticky answers this row"
    );
    let recorder = TrustPolicy {
        metadata: ObjectMeta {
            name: Some("incident-record".to_string()),
            uid: Some("uid-incident-record".to_string()),
            generation: Some(1),
            resource_version: Some("23".to_string()),
            ..ObjectMeta::default()
        },
        spec: TrustPolicySpec {
            default: false,
            namespaces: None,
            allowed_target_cluster_ids: None,
            keys: vec![revoked_console(
                RevocationReason::KeyCompromise,
                "2026-09-09T13:05:00Z",
            )],
        },
        status: None,
    };
    let mut table = routes(vec![console(), bob()]);
    table[0].body = serde_json::json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "TrustPolicyList",
        "metadata": {"resourceVersion": "1"},
        "items": [trust_policy(vec![console(), bob()]), recorder],
    })
    .to_string();
    let (client, _) = mock_client_recording(table);
    let outcome = approval::decide_with_policy_at(
        &object,
        &client,
        &bind("team-ordinary"),
        at("2026-09-11T08:00:00Z"),
    )
    .await
    .expect("a verdict");
    assert!(outcome.is_consumed(), "{outcome:?}");
    assert!(!outcome.is_verified(), "never green: {outcome:?}");
    assert_eq!(
        outcome.reason(),
        approval::REASON_RECORDED_BEFORE_REVOCATION
    );

    let clean = decide_trusting(&object, vec![console(), bob()]).await;
    assert!(clean.is_verified(), "{clean:?}");
}
