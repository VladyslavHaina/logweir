//! `TrustPolicy` resolution, the synthesised `legacy-roster-v1`, and the
//! reconciler around them (PLAT-19.1, decision D3 §7.1 and §7.5).
//!
//! # Where the key material comes from
//!
//! The same two **public** halves `tests/approval_controller.rs` uses, and for
//! the same reason: `scripts/check-one-signer.sh`'s check 3 greps
//! `crates/*/tests` for the signing API and `weirkeeper` is on neither
//! `ALLOWED_LINK` nor `ALLOWED_SOURCE`, so a test here that minted a key pair
//! would put the controller crate's test target on the signing side of the one
//! boundary this crate exists to respect. These are public keys and key ids;
//! no private half was ever written down.
//!
//! # What each test is allowed to prove
//!
//! The resolution and evaluation tests call the pure functions directly — the
//! property is a mapping from objects and a clock to a verdict, and a test that
//! needs a route table to reach it is a test about `kube`. The reconciler tests
//! go through `weirkeeper::testing::mock_client_recording`, whose double panics
//! on a request it was not given a route for; that panic is what makes "the
//! reconciler asked for nothing else" an assertion rather than a hope.

use chrono::{DateTime, Duration, Utc};
use kube::api::ObjectMeta;
use logweir_core::trust::{
    EvidenceClaim, IndependentObservation, KeyUsage, SigningRefusal, TrustBasis, TrustResult,
    UntrustReason,
};
use weirkeeper::controllers::trust_policy::{
    self, CONDITION_BOUND, CONDITION_EXPIRING_SOON, CONDITION_LOADED, CONDITION_SUPERSEDED,
    REASON_ALGORITHM_MISMATCH, REASON_BOUND, REASON_CONFLICT, REASON_DEFAULT_POLICY,
    REASON_EXPIRING_SOON, REASON_KEY_ID_MISMATCH, REASON_LOADED, REASON_NOT_BOUND,
    REASON_NOT_EXPIRING, REASON_ROSTER_STILL_CONSULTED, REASON_SUPERSEDED_BY_TRUST_POLICY,
    REASON_UNPARSEABLE_KEY,
};
use weirkeeper::crds::trust_policy::{
    KeyAlgorithm, KeyPrincipal, KeyState, RevocationReason, TrustPolicy, TrustPolicySpec,
    TrustPolicyStatus, TrustedKey,
};
use weirkeeper::crds::trust_roster::{KeyEntry, TrustRoster as TrustRosterObject, TrustRosterSpec};
use weirkeeper::testing::{mock_client_recording, Recorder, Route, SeenRequest};
use weirkeeper::trust::{
    self, Resolution, TrustSource, DEFAULT_SENTINEL, LEGACY_NOT_BEFORE_RFC3339, LEGACY_POLICY_NAME,
    REASON_TRUST_POLICY_CONFLICT,
};

// ---------------------------------------------------------------------------
// Public material, out of tree. See the module header.
// ---------------------------------------------------------------------------

/// An Ed25519 **public** key, SubjectPublicKeyInfo PEM.
const KEY_A_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEApFpEU8uY5S8Lv43HL4DcXKKyM8WHurCPZIxvq8ZBfpY=\n-----END PUBLIC KEY-----\n";
/// `sha256(KEY_A_PEM's SPKI DER)`, lowercase hex.
const KEY_A_ID: &str = "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6";

/// A second Ed25519 **public** key.
const KEY_B_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAKTrTSpPTt1d9M1kMim3Imkt2s1OjRm48GfqVh+fjvyk=\n-----END PUBLIC KEY-----\n";
/// `sha256(KEY_B_PEM's SPKI DER)`, lowercase hex.
const KEY_B_ID: &str = "067bf4d360d3c0658620a75a225d4d3e2e038cdb12cdf38ef6611241b9d380d2";

/// PEM armour around bytes that are not a key at all.
const UNPARSEABLE_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nbm90IGEgcHVibGljIGtleQ==\n-----END PUBLIC KEY-----\n";
/// The `keyId` the unparseable entry declares.
const BAD_KEY_ID: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

/// The one instant every test in this file is written against.
fn now() -> DateTime<Utc> {
    at("2026-09-16T12:00:00Z")
}

/// An RFC 3339 instant.
fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("a fixture instant")
        .with_timezone(&Utc)
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

/// An `Active` key entry with `usage`, valid through 2027.
fn key(
    key_id: &str,
    pem: &str,
    usages: Vec<weirkeeper::crds::trust_policy::KeyUsage>,
) -> TrustedKey {
    TrustedKey {
        key_id: key_id.to_string(),
        spki_pem: pem.to_string(),
        algorithm: KeyAlgorithm::Ed25519,
        usages,
        principal: KeyPrincipal {
            id: format!("install:{key_id}"),
            display: None,
        },
        not_before: at("2026-01-01T00:00:00Z"),
        not_after: at("2027-06-01T00:00:00Z"),
        state: KeyState::Active,
        retired_at: None,
        revoked_at: None,
        revocation_reason: None,
        revocation_effective_from: None,
    }
}

/// A policy naming `namespaces`, with `keys`.
fn policy(name: &str, namespaces: &[&str], default: bool, keys: Vec<TrustedKey>) -> TrustPolicy {
    TrustPolicy {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            uid: Some(format!("uid-{name}")),
            generation: Some(1),
            // SEAM S7 (review finding F6): every status write carries it, so a
            // fixture without one is a fixture the reconciler refuses.
            resource_version: Some("17".to_string()),
            ..ObjectMeta::default()
        },
        spec: TrustPolicySpec {
            default,
            namespaces: if namespaces.is_empty() {
                None
            } else {
                Some(namespaces.iter().map(|n| (*n).to_string()).collect())
            },
            allowed_target_cluster_ids: Some(vec!["scratch-cluster-id".to_string()]),
            keys,
        },
        status: None,
    }
}

/// The one key set most tests use.
fn evidence_key() -> TrustedKey {
    key(
        KEY_A_ID,
        KEY_A_PEM,
        vec![weirkeeper::crds::trust_policy::KeyUsage::EvidenceSigning],
    )
}

/// A roster with `approver_keys` and `signing_keys`.
fn roster(approver: Vec<KeyEntry>, signing: Vec<KeyEntry>) -> TrustRosterSpec {
    TrustRosterSpec {
        approver_keys: approver,
        signing_keys: signing,
        allowed_cluster_ids: vec!["scratch-cluster-id".to_string()],
    }
}

/// One roster entry.
fn entry(key_id: &str, pem: &str, not_after: Option<&str>) -> KeyEntry {
    KeyEntry {
        key_id: key_id.to_string(),
        spki_pem: pem.to_string(),
        subject: None,
        not_after: not_after.map(at),
    }
}

/// The resolved trust, or a panic naming what came back instead.
fn expect_trust(resolution: Resolution) -> weirkeeper::trust::ResolvedTrust {
    match resolution {
        Resolution::Trust(t) => *t,
        other => panic!("expected resolved trust; got {other:?}"),
    }
}

/// Every request the double saw, method and path, query string dropped.
fn seen(recorder: &Recorder) -> Vec<(String, String)> {
    recorder
        .lock()
        .expect("the recorder mutex")
        .iter()
        .map(|SeenRequest { method, uri }| {
            (
                method.clone(),
                uri.split('?').next().unwrap_or(uri).to_string(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Resolution — D3 §7.1
// ---------------------------------------------------------------------------

/// An explicit `spec.namespaces` match wins over the default.
#[test]
fn an_explicit_namespace_match_beats_the_default() {
    let explicit = policy("team-a", &["team-a"], false, vec![evidence_key()]);
    let fallback = policy(
        "org-default",
        &[],
        true,
        vec![key(
            KEY_B_ID,
            KEY_B_PEM,
            vec![weirkeeper::crds::trust_policy::KeyUsage::EvidenceSigning],
        )],
    );
    let resolved = expect_trust(trust::resolve_in(
        "team-a",
        &[fallback.clone(), explicit],
        None,
    ));
    assert_eq!(resolved.source.name(), "team-a");

    // And a namespace nobody names falls to the default.
    let other = expect_trust(trust::resolve_in("team-z", &[fallback], None));
    assert_eq!(other.source.name(), "org-default");
}

/// With no policy at all, the roster is synthesised.
#[test]
fn with_no_policy_the_roster_is_synthesised() {
    let spec = roster(
        vec![entry(KEY_A_ID, KEY_A_PEM, None)],
        vec![entry(KEY_B_ID, KEY_B_PEM, None)],
    );
    let resolved = expect_trust(trust::resolve_in("team-a", &[], Some(&spec)));
    assert_eq!(resolved.source, TrustSource::LegacyRoster);
    assert_eq!(resolved.source.name(), LEGACY_POLICY_NAME);
    assert!(resolved.source.is_legacy());
}

/// With no policy and no roster, nothing is configured — today's
/// `RosterNotFound`.
#[test]
fn with_no_policy_and_no_roster_nothing_is_configured() {
    assert_eq!(
        trust::resolve_in("team-a", &[], None),
        Resolution::Unconfigured
    );
}

/// **MUTANT 3 — a conflict resolving to the first policy.**
///
/// The planted mutation is `resolve_in` taking `explicit.first()` whenever one
/// exists instead of refusing a contested namespace — the "pick one and move
/// on" shape a reconciler falls into naturally. It is a mutation a coverage
/// number cannot see: `policy-a` genuinely governs `team-a` under one reading,
/// so a test that only asserted "some trust resolves" would stay green while
/// the namespace silently took whichever policy the list happened to hold
/// first.
///
/// Three assertions kill it: the resolution is a `Conflict`, the conflict
/// NAMES BOTH policies (so a mutant that refused while reporting one dies
/// too), and the uncontested namespace of the same policy still resolves — so
/// a mutant that "fixed" the conflict by refusing everything dies as well.
#[test]
fn mutant_a_namespace_two_policies_claim_resolves_to_nothing() {
    let a = policy("policy-a", &["team-a"], false, vec![evidence_key()]);
    let b = policy(
        "policy-b",
        &["team-a", "team-b"],
        false,
        vec![evidence_key()],
    );
    match trust::resolve_in("team-a", &[a.clone(), b.clone()], None) {
        Resolution::Conflict {
            namespace,
            policies,
        } => {
            assert_eq!(namespace, "team-a");
            assert_eq!(policies, vec!["policy-a", "policy-b"]);
        }
        other => panic!("a contested namespace must resolve to nothing; got {other:?}"),
    }
    // The UNCONTESTED namespace of the same policy still resolves.
    let resolved = expect_trust(trust::resolve_in("team-b", &[a, b], None));
    assert_eq!(
        resolved.source.name(),
        "policy-b",
        "a conflict in one namespace must not take away a namespace nobody contests"
    );
}

/// Two `default: true` policies are a conflict for every namespace that falls
/// to them, and for no namespace an explicit match already answered.
#[test]
fn two_defaults_are_a_conflict_for_every_fallback_namespace() {
    let one = policy("default-one", &[], true, vec![evidence_key()]);
    let two = policy("default-two", &[], true, vec![evidence_key()]);
    let explicit = policy("team-a", &["team-a"], false, vec![evidence_key()]);
    let all = vec![one, two, explicit];
    match trust::resolve_in("team-z", &all, None) {
        Resolution::Conflict { policies, .. } => {
            assert_eq!(policies, vec!["default-one", "default-two"]);
        }
        other => panic!("two defaults contest every fallback namespace; got {other:?}"),
    }
    let resolved = expect_trust(trust::resolve_in("team-a", &all, None));
    assert_eq!(resolved.source.name(), "team-a");
}

/// A conflict falls through to NEITHER the default NOR the roster: it is a
/// refusal, not a miss.
#[test]
fn a_conflict_never_falls_through_to_the_roster() {
    let a = policy("policy-a", &["team-a"], false, vec![evidence_key()]);
    let b = policy("policy-b", &["team-a"], false, vec![evidence_key()]);
    let spec = roster(vec![entry(KEY_A_ID, KEY_A_PEM, None)], vec![]);
    assert!(
        matches!(
            trust::resolve_in("team-a", &[a, b], Some(&spec)),
            Resolution::Conflict { .. }
        ),
        "a contested namespace resolving to the roster would silently answer with the very \
         trust the two policies were written to replace"
    );
}

/// `conflicts` reports the whole cluster's contested namespaces once, sorted.
#[test]
fn conflicts_reports_every_contested_namespace() {
    let a = policy("policy-a", &["team-a", "team-b"], false, vec![]);
    let b = policy("policy-b", &["team-b"], false, vec![]);
    let c = policy("policy-c", &["team-c"], false, vec![]);
    let map = trust::conflicts(&[a, b, c]);
    assert_eq!(map.len(), 1);
    assert_eq!(map["team-b"], vec!["policy-a", "policy-b"]);
    assert!(!map.contains_key("team-a"));
    assert!(!map.contains_key("team-c"));
}

/// Two defaults are reported under a sentinel that cannot be a namespace name.
#[test]
fn two_defaults_are_reported_under_a_sentinel_no_namespace_can_be_called() {
    let map = trust::conflicts(&[
        policy("default-one", &[], true, vec![]),
        policy("default-two", &[], true, vec![]),
    ]);
    assert_eq!(map[DEFAULT_SENTINEL], vec!["default-one", "default-two"]);
    assert_eq!(
        DEFAULT_SENTINEL, "*",
        "`*` is not a DNS-1123 label, so no namespace can ever be named this and no reader can \
         mistake the sentinel for one"
    );
}

/// `boundNamespaces` is `spec.namespaces` minus the contested ones.
#[test]
fn bound_namespaces_drops_the_contested_ones() {
    let a = policy("policy-a", &["team-a", "team-b"], false, vec![]);
    let b = policy("policy-b", &["team-b"], false, vec![]);
    let all = vec![a.clone(), b];
    assert_eq!(trust::bound_namespaces(&a, &all), vec!["team-a"]);
}

// ---------------------------------------------------------------------------
// The synthesised legacy policy — D3 §7.5
// ---------------------------------------------------------------------------

/// The mapping, field by field.
#[test]
fn the_legacy_synthesis_maps_every_roster_field() {
    let spec = roster(
        vec![entry(KEY_A_ID, KEY_A_PEM, Some("2027-01-01T00:00:00Z"))],
        vec![entry(KEY_B_ID, KEY_B_PEM, None)],
    );
    let resolved = trust::synthesize_legacy(&spec);

    assert_eq!(
        resolved.allowed_target_cluster_ids,
        vec!["scratch-cluster-id"],
        "allowedClusterIds becomes allowedTargetClusterIds, at the same scope"
    );

    let approver = resolved.key(KEY_A_ID).expect("the approver key");
    assert_eq!(approver.trust.usages, vec![KeyUsage::GovernedApproval]);
    assert_eq!(approver.trust.state, logweir_core::trust::KeyState::Active);
    assert_eq!(
        approver.trust.not_after,
        at("2027-01-01T00:00:00Z"),
        "notAfter verbatim"
    );
    assert_eq!(approver.trust.principal_id, format!("legacy:{KEY_A_ID}"));
    assert_eq!(approver.trust.retired_at, None);
    assert_eq!(approver.trust.revoked_at, None);

    let signer = resolved.key(KEY_B_ID).expect("the signing key");
    assert_eq!(signer.trust.usages, vec![KeyUsage::EvidenceSigning]);
    assert_eq!(
        signer.trust.not_after,
        trust::legacy_not_after(),
        "an entry with no notAfter does not expire, which is how every consumer reads it today"
    );

    assert_eq!(
        approver.trust.not_before,
        at(LEGACY_NOT_BEFORE_RFC3339),
        "a synthesised notBefore of `now` would retroactively invalidate every archive the \
         roster signed"
    );

    assert!(
        resolved
            .keys
            .iter()
            .all(|k| !k.trust.has_usage(KeyUsage::ConsoleConfirmation)),
        "D3 §7.3: no ConsoleConfirmation key is ever synthesised, so an old controller reached \
         by rollback cannot mistake an ordinary confirmation for a governed approval"
    );
}

/// A key on **both** roster lists becomes ONE entry with BOTH usages — the
/// overlap `selfAttestedRisk` labels rather than refuses.
#[test]
fn a_key_on_both_roster_lists_becomes_one_entry_with_both_usages() {
    let spec = roster(
        vec![entry(KEY_A_ID, KEY_A_PEM, None)],
        vec![entry(KEY_A_ID, KEY_A_PEM, None)],
    );
    let resolved = trust::synthesize_legacy(&spec);
    assert_eq!(
        resolved.keys.len(),
        1,
        "spec.keys is an associative list keyed by keyId, so two entries with the same id is a \
         document the API server refuses"
    );
    let only = &resolved.keys[0];
    assert!(only.trust.has_usage(KeyUsage::GovernedApproval));
    assert!(only.trust.has_usage(KeyUsage::EvidenceSigning));
}

/// **MUTANT 6 — a partially parseable roster loading.**
///
/// The planted mutation is the legacy synthesis skipping an unparseable
/// `approverKeys` entry instead of blocking the usage — which would let the
/// OTHER approver key on the same roster authorise a restore, on a roster
/// today's `evaluate` check 2 refuses outright. Both halves are asserted: the
/// block exists, and its message is the one today's refusal carries.
#[test]
fn mutant_a_partially_parseable_roster_is_not_a_roster() {
    let spec = roster(
        vec![
            entry(BAD_KEY_ID, UNPARSEABLE_PEM, None),
            entry(KEY_A_ID, KEY_A_PEM, None),
        ],
        vec![entry(KEY_B_ID, KEY_B_PEM, None)],
    );
    let resolved = trust::synthesize_legacy(&spec);
    let blocked = resolved
        .blocked_for(KeyUsage::GovernedApproval)
        .expect("a roster with an unparseable approver key blocks every approval");
    assert!(
        blocked.contains(BAD_KEY_ID),
        "the refusal names the bad keyId: {blocked}"
    );
    assert!(
        blocked.contains("a partially loaded roster is not a roster"),
        "byte-for-byte today's message, so the refusal an operator reads does not change on the \
         day this path replaces controllers::approval::evaluate check 2: {blocked}"
    );
    assert!(
        resolved.key(KEY_A_ID).is_some(),
        "the good key is still RESOLVED — the refusal is a property of the roster, not a hole \
         in the key set"
    );

    // AND THE RULE STOPS AT THE APPROVAL SIDE. `verify_evidence` step 4 skips
    // an unparseable signing key and keeps trying the rest, so a bad APPROVER
    // key must not stop evidence verifying.
    assert_eq!(
        resolved.blocked_for(KeyUsage::EvidenceSigning),
        None,
        "today's verify_evidence skips an unparseable signingKeys entry and keeps going; a \
         block here would change behaviour §7.5 says is byte-for-byte"
    );
    assert_eq!(
        resolved.keys_for(KeyUsage::EvidenceSigning).count(),
        1,
        "and the usable signing key is still offered to the verifier"
    );
}

/// An unparseable key is excluded from the key lists a verifier tries.
#[test]
fn an_unparseable_key_is_never_offered_to_a_verifier() {
    let spec = roster(vec![], vec![entry(BAD_KEY_ID, UNPARSEABLE_PEM, None)]);
    let resolved = trust::synthesize_legacy(&spec);
    assert_eq!(resolved.keys_for(KeyUsage::EvidenceSigning).count(), 0);
    assert!(
        resolved.key(BAD_KEY_ID).is_some_and(|k| !k.is_usable()),
        "it is present and reported unusable, never silently absent"
    );
}

/// A roster entry whose declared id is not the hash of its own material is
/// reported separately from an unparseable one — today's `KeyIdNotInRoster`
/// and `SignatureInvalid` are different refusals.
#[test]
fn a_declared_id_that_does_not_match_its_material_is_its_own_fault() {
    let spec = roster(vec![], vec![entry(KEY_B_ID, KEY_A_PEM, None)]);
    let resolved = trust::synthesize_legacy(&spec);
    let only = &resolved.keys[0];
    assert!(only.parsed.is_ok(), "the PEM is a perfectly good key");
    assert_eq!(
        only.declared_id_matches.as_ref().unwrap_err(),
        KEY_A_ID,
        "and the mismatch names what it actually hashes to"
    );
    assert!(!only.is_usable());
}

/// **A real `TrustPolicy` blocks no usage for an unparseable key** — and the
/// reason is written down rather than assumed.
#[test]
fn a_real_policy_reports_one_unparseable_key_without_refusing_the_others() {
    let bad = TrustedKey {
        key_id: BAD_KEY_ID.to_string(),
        spki_pem: UNPARSEABLE_PEM.to_string(),
        ..evidence_key()
    };
    let p = policy("org-default", &["team-a"], false, vec![bad, evidence_key()]);
    let resolved = trust::from_policy(&p);
    assert!(resolved.blocked.is_empty());
    assert_eq!(
        resolved.keys_for(KeyUsage::EvidenceSigning).count(),
        1,
        "the good key still verifies: an unparseable key can verify nothing, so leaving it out \
         widens no trust, while refusing the whole policy would stop every restore in every \
         bound namespace over one bad paste"
    );
}

// ---------------------------------------------------------------------------
// The seams W10 wires in
// ---------------------------------------------------------------------------

/// `decide_for` resolves by stored `matchedKeyId` and refuses an id the policy
/// does not carry.
#[test]
fn the_decide_seam_resolves_by_stored_key_id() {
    let p = policy("org-default", &["team-a"], false, vec![evidence_key()]);
    let resolved = trust::from_policy(&p);
    let good = resolved.decide_for(
        KEY_A_ID,
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(at("2026-06-01T00:00:00Z")),
        &IndependentObservation::none(),
        now(),
    );
    assert_eq!(good.result, TrustResult::Valid);
    assert_eq!(good.basis, TrustBasis::Current);

    let stranger = resolved.decide_for(
        KEY_B_ID,
        KeyUsage::EvidenceSigning,
        &EvidenceClaim::at(at("2026-06-01T00:00:00Z")),
        &IndependentObservation::none(),
        now(),
    );
    assert_eq!(stranger.reason, Some(UntrustReason::UntrustedSigner));
}

/// `may_sign_new_for` is the admission-time half, and it disagrees with
/// `decide_for` exactly where D3 §7.4 says it should.
#[test]
fn the_signing_seam_refuses_what_the_verification_seam_still_trusts() {
    let retired = TrustedKey {
        state: KeyState::Retired,
        retired_at: Some(at("2026-05-01T00:00:00Z")),
        usages: vec![weirkeeper::crds::trust_policy::KeyUsage::GovernedApproval],
        ..evidence_key()
    };
    let p = policy("org-default", &["team-a"], false, vec![retired]);
    let resolved = trust::from_policy(&p);
    assert_eq!(
        resolved.may_sign_new_for(KEY_A_ID, KeyUsage::GovernedApproval, now()),
        Err(SigningRefusal::KeyRetired)
    );
    assert_eq!(
        resolved
            .decide_for(
                KEY_A_ID,
                KeyUsage::GovernedApproval,
                &EvidenceClaim::at(at("2026-04-01T00:00:00Z")),
                &IndependentObservation::none(),
                now(),
            )
            .basis,
        TrustBasis::Historical
    );
}

/// The conflict refusal string is one word, used on both surfaces.
#[test]
fn the_conflict_refusal_has_one_spelling() {
    assert_eq!(REASON_TRUST_POLICY_CONFLICT, "TrustPolicyConflict");
    assert_eq!(REASON_CONFLICT, REASON_TRUST_POLICY_CONFLICT);
}

// ---------------------------------------------------------------------------
// The reconciler's verdict — D3 §7.1 status
// ---------------------------------------------------------------------------

/// A healthy policy: `Loaded=True`, per-key verdicts, `Bound=True`.
#[test]
fn a_healthy_policy_loads_and_binds() {
    let p = policy(
        "org-default",
        &["team-a", "team-b"],
        false,
        vec![evidence_key()],
    );
    let verdict = trust_policy::evaluate(&p, std::slice::from_ref(&p), now());
    assert!(verdict.loaded);
    assert_eq!(verdict.bound, vec!["team-a", "team-b"]);
    assert!(verdict.conflicts.is_empty());
    assert_eq!(verdict.keys.len(), 1);
    assert_eq!(verdict.keys[0].effective_state, "Active");
    assert_eq!(verdict.keys[0].usable_for_new_signatures, Some(true));
    assert_eq!(
        verdict.keys[0].usable_for_verification.as_deref(),
        Some("Full")
    );
    let loaded = condition(&verdict, CONDITION_LOADED);
    assert_eq!(loaded.status, "True");
    assert_eq!(loaded.reason.as_deref(), Some(REASON_LOADED));
    let bound = condition(&verdict, CONDITION_BOUND);
    assert_eq!(bound.status, "True");
    assert_eq!(bound.reason.as_deref(), Some(REASON_BOUND));
}

/// Each way a key can fail to load has its own `reason`, and the first fault
/// decides.
#[test]
fn each_load_fault_has_its_own_reason() {
    let unparseable = TrustedKey {
        key_id: BAD_KEY_ID.to_string(),
        spki_pem: UNPARSEABLE_PEM.to_string(),
        ..evidence_key()
    };
    let wrong_id = TrustedKey {
        key_id: KEY_B_ID.to_string(),
        ..evidence_key()
    };
    let wrong_algorithm = TrustedKey {
        algorithm: KeyAlgorithm::P256,
        ..evidence_key()
    };
    for (key, reason) in [
        (unparseable, REASON_UNPARSEABLE_KEY),
        (wrong_id, REASON_KEY_ID_MISMATCH),
        (wrong_algorithm, REASON_ALGORITHM_MISMATCH),
    ] {
        let p = policy("org-default", &["team-a"], false, vec![key]);
        let verdict = trust_policy::evaluate(&p, std::slice::from_ref(&p), now());
        assert!(!verdict.loaded, "{reason}");
        assert_eq!(
            condition(&verdict, CONDITION_LOADED).reason.as_deref(),
            Some(reason)
        );
    }
}

/// An unparseable key is `Unparseable`, usable for nothing — the clock does
/// not get to call it `Active`.
#[test]
fn an_unparseable_key_is_unparseable_not_active() {
    let bad = TrustedKey {
        key_id: BAD_KEY_ID.to_string(),
        spki_pem: UNPARSEABLE_PEM.to_string(),
        ..evidence_key()
    };
    let p = policy("org-default", &["team-a"], false, vec![bad]);
    let verdict = trust_policy::evaluate(&p, std::slice::from_ref(&p), now());
    assert_eq!(verdict.keys[0].effective_state, "Unparseable");
    assert_eq!(verdict.keys[0].usable_for_new_signatures, Some(false));
    assert_eq!(
        verdict.keys[0].usable_for_verification.as_deref(),
        Some("None")
    );
}

/// The key lifecycle, projected onto `status.keys[]`.
#[test]
fn the_per_key_status_reports_the_lifecycle() {
    let retired = TrustedKey {
        key_id: KEY_B_ID.to_string(),
        spki_pem: KEY_B_PEM.to_string(),
        state: KeyState::Retired,
        retired_at: Some(at("2026-05-01T00:00:00Z")),
        ..evidence_key()
    };
    let compromised = TrustedKey {
        state: KeyState::Revoked,
        revoked_at: Some(at("2026-05-01T00:00:00Z")),
        revocation_reason: Some(RevocationReason::KeyCompromise),
        revocation_effective_from: Some(at("2026-05-01T00:00:00Z")),
        ..evidence_key()
    };
    let p = policy(
        "org-default",
        &["team-a"],
        false,
        vec![compromised, retired],
    );
    let verdict = trust_policy::evaluate(&p, std::slice::from_ref(&p), now());
    assert_eq!(verdict.keys[0].effective_state, "Revoked");
    assert_eq!(
        verdict.keys[0].usable_for_verification.as_deref(),
        Some("None")
    );
    assert_eq!(verdict.keys[1].effective_state, "Retired");
    assert_eq!(
        verdict.keys[1].usable_for_verification.as_deref(),
        Some("Historical"),
        "`Historical` is not a downgrade of `Full` — it is the honest answer for a key that was \
         valid when it signed"
    );
    assert_eq!(verdict.keys[1].usable_for_new_signatures, Some(false));
}

/// A contested policy reports the conflict and drops the namespace from
/// `boundNamespaces`.
#[test]
fn a_contested_policy_reports_it_and_governs_nothing_there() {
    let a = policy("policy-a", &["team-a"], false, vec![evidence_key()]);
    let b = policy("policy-b", &["team-a"], false, vec![evidence_key()]);
    let all = vec![a.clone(), b];
    let verdict = trust_policy::evaluate(&a, &all, now());
    assert!(verdict.bound.is_empty());
    assert_eq!(verdict.conflicts.len(), 1);
    assert_eq!(verdict.conflicts[0].namespace, "team-a");
    assert_eq!(verdict.conflicts[0].policies, vec!["policy-a", "policy-b"]);
    let bound = condition(&verdict, CONDITION_BOUND);
    assert_eq!(bound.status, "False");
    assert_eq!(bound.reason.as_deref(), Some(REASON_CONFLICT));
    assert!(bound
        .message
        .as_deref()
        .expect("a message")
        .contains("TrustPolicyConflict"));
}

/// A policy that names nothing and is not the default is `Bound=False`.
#[test]
fn a_policy_that_names_nothing_governs_nothing() {
    let p = policy("orphan", &[], false, vec![evidence_key()]);
    let verdict = trust_policy::evaluate(&p, std::slice::from_ref(&p), now());
    let bound = condition(&verdict, CONDITION_BOUND);
    assert_eq!(bound.status, "False");
    assert_eq!(bound.reason.as_deref(), Some(REASON_NOT_BOUND));
}

/// The default policy is `Bound=True/DefaultPolicy` and says in words why it
/// enumerates no namespace.
#[test]
fn the_default_policy_is_bound_without_enumerating_namespaces() {
    let p = policy("org-default", &[], true, vec![evidence_key()]);
    let verdict = trust_policy::evaluate(&p, std::slice::from_ref(&p), now());
    let bound = condition(&verdict, CONDITION_BOUND);
    assert_eq!(bound.status, "True");
    assert_eq!(bound.reason.as_deref(), Some(REASON_DEFAULT_POLICY));
    assert!(
        verdict.bound.is_empty(),
        "enumerating `every namespace nobody else names` would need `list` on namespaces, a verb \
         this controller does not have and must not acquire to fill in a status field"
    );
}

/// `ExpiringSoon` fires inside the horizon and not outside it.
#[test]
fn expiring_soon_fires_inside_the_horizon_only() {
    let far = policy("org-default", &["team-a"], false, vec![evidence_key()]);
    assert_eq!(
        condition(
            &trust_policy::evaluate(&far, std::slice::from_ref(&far), now()),
            CONDITION_EXPIRING_SOON
        )
        .reason
        .as_deref(),
        Some(REASON_NOT_EXPIRING)
    );

    let soon_key = TrustedKey {
        not_after: now() + Duration::days(trust_policy::EXPIRING_SOON_DAYS - 1),
        ..evidence_key()
    };
    let soon = policy("org-default", &["team-a"], false, vec![soon_key]);
    let verdict = trust_policy::evaluate(&soon, std::slice::from_ref(&soon), now());
    let c = condition(&verdict, CONDITION_EXPIRING_SOON);
    assert_eq!(c.status, "True");
    assert_eq!(c.reason.as_deref(), Some(REASON_EXPIRING_SOON));
    assert!(c.message.as_deref().expect("a message").contains(KEY_A_ID));
    assert!(
        c.message
            .as_deref()
            .expect("a message")
            .contains("Nothing is blocked"),
        "protection availability outranks verification display (D3 §7.6): the condition is a \
         planning signal, never a gate"
    );
}

/// A key already past `notAfter` is `Expired` and does not also raise
/// `ExpiringSoon` — it is not approaching anything.
#[test]
fn an_already_expired_key_is_not_expiring_soon() {
    let past = TrustedKey {
        not_after: at("2026-03-01T00:00:00Z"),
        ..evidence_key()
    };
    let p = policy("org-default", &["team-a"], false, vec![past]);
    let verdict = trust_policy::evaluate(&p, std::slice::from_ref(&p), now());
    assert_eq!(verdict.keys[0].effective_state, "Expired");
    assert_eq!(
        condition(&verdict, CONDITION_EXPIRING_SOON)
            .reason
            .as_deref(),
        Some(REASON_NOT_EXPIRING)
    );
}

/// One condition off a verdict, by type.
fn condition<'a>(
    verdict: &'a trust_policy::PolicyVerdict,
    r#type: &str,
) -> &'a weirkeeper::crds::Condition {
    verdict
        .conditions
        .iter()
        .find(|c| c.r#type == r#type)
        .unwrap_or_else(|| panic!("the verdict carries no `{type}` condition", type = r#type))
}

// ---------------------------------------------------------------------------
// The `evaluatedAt` heartbeat — D3 §7.1 and §7.7
// ---------------------------------------------------------------------------

/// A steady policy keeps the stored `evaluatedAt` until [`HEARTBEAT`] has
/// elapsed, and then moves it.
#[test]
fn the_evaluated_at_heartbeat_is_debounced_to_five_minutes() {
    let base = policy("org-default", &["team-a"], false, vec![evidence_key()]);
    let verdict = trust_policy::evaluate(&base, std::slice::from_ref(&base), now());
    let first = trust_policy::status_for(&base, &verdict, now());
    assert_eq!(first.evaluated_at, Some(now()));

    let steady = TrustPolicy {
        status: Some(first),
        ..base.clone()
    };
    let soon = now() + Duration::seconds(120);
    let unchanged = trust_policy::status_for(
        &steady,
        &trust_policy::evaluate(&steady, std::slice::from_ref(&steady), soon),
        soon,
    );
    assert_eq!(
        unchanged.evaluated_at,
        Some(now()),
        "two minutes later nothing has changed, so the stored instant is written back and \
         `status_unchanged` skips the patch — the shape that spun TrustRoster at 133 reconciles \
         a second (E11(d))"
    );

    let later = now() + Duration::seconds(301);
    let beat = trust_policy::status_for(
        &steady,
        &trust_policy::evaluate(&steady, std::slice::from_ref(&steady), later),
        later,
    );
    assert_eq!(
        beat.evaluated_at,
        Some(later),
        "past five minutes the heartbeat moves, which is what D3 §7.7's fifteen-minute \
         staleness rule measures against"
    );
}

/// A CHANGED verdict moves `evaluatedAt` immediately, whatever the debounce
/// says.
#[test]
fn a_changed_verdict_moves_evaluated_at_at_once() {
    let base = policy("org-default", &["team-a"], false, vec![evidence_key()]);
    let stored = trust_policy::status_for(
        &base,
        &trust_policy::evaluate(&base, std::slice::from_ref(&base), now()),
        now(),
    );
    let edited = TrustPolicy {
        status: Some(stored),
        spec: TrustPolicySpec {
            namespaces: Some(vec!["team-a".to_string(), "team-c".to_string()]),
            ..base.spec.clone()
        },
        ..base
    };
    let soon = now() + Duration::seconds(10);
    let next = trust_policy::status_for(
        &edited,
        &trust_policy::evaluate(&edited, std::slice::from_ref(&edited), soon),
        soon,
    );
    assert_eq!(next.evaluated_at, Some(soon));
}

/// `observedGeneration` is carried, so §7.7's generation-skew rule can fire.
#[test]
fn the_status_carries_the_generation_it_was_computed_from() {
    let mut p = policy("org-default", &["team-a"], false, vec![evidence_key()]);
    p.metadata.generation = Some(7);
    let status = trust_policy::status_for(
        &p,
        &trust_policy::evaluate(&p, std::slice::from_ref(&p), now()),
        now(),
    );
    assert_eq!(status.observed_generation, Some(7));
    assert_eq!(status.key_count, Some(1));
}

// ---------------------------------------------------------------------------
// The reconciler, over a route table
// ---------------------------------------------------------------------------

/// The reconcile lists policies, patches its own status, and writes the
/// `Superseded` condition on the roster — and asks for nothing else.
#[tokio::test]
async fn the_reconcile_patches_its_status_and_writes_the_roster_condition() {
    let p = policy("org-default", &[], true, vec![evidence_key()]);
    let (client, recorder) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/trustpolicies",
            status: 200,
            body: policy_list_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/trustpolicies/org-default/status",
            status: 200,
            body: policy_body(),
        },
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 200,
            body: roster_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/trustrosters/default/status",
            status: 200,
            body: roster_body(),
        },
    ]);
    let verdict = trust_policy::reconcile_policy(&p, &client)
        .await
        .expect("the reconcile completes");
    assert!(verdict.loaded);
    let requests = seen(&recorder);
    assert!(
        requests
            .iter()
            .any(|(m, u)| m == "PATCH" && u.ends_with("/trustpolicies/org-default/status")),
        "{requests:?}"
    );
    assert!(
        requests
            .iter()
            .any(|(m, u)| m == "PATCH" && u.ends_with("/trustrosters/default/status")),
        "D3 §7.5: a matching (default) policy makes the roster Superseded=True. {requests:?}"
    );
    assert!(
        !requests.iter().any(|(m, _)| m == "DELETE" || m == "PUT"),
        "the roster is NOT deleted and its spec is NOT replaced — a rollback reads it \
         unchanged. {requests:?}"
    );
}

/// The `Superseded` condition names the reason D3 §7.5 fixes, and says the
/// roster is untouched.
#[test]
fn the_superseded_reason_is_the_one_the_decision_names() {
    assert_eq!(CONDITION_SUPERSEDED, "Superseded");
    assert_eq!(REASON_SUPERSEDED_BY_TRUST_POLICY, "SupersededByTrustPolicy");
}

/// A bare `TrustRoster` object with `conditions`, for the F2 tests.
fn roster_object(conditions: Vec<weirkeeper::crds::Condition>) -> TrustRosterObject {
    use weirkeeper::crds::trust_roster::TrustRosterStatus;
    TrustRosterObject {
        metadata: ObjectMeta {
            name: Some("default".to_string()),
            generation: Some(1),
            resource_version: Some("41".to_string()),
            ..ObjectMeta::default()
        },
        spec: roster(vec![entry(KEY_A_ID, KEY_A_PEM, None)], vec![]),
        status: Some(TrustRosterStatus {
            loaded: Some(true),
            expired_key_ids: Some(vec![]),
            conditions: Some(conditions),
        }),
    }
}

/// **F2, first half: only a MATCHING policy supersedes the roster.**
///
/// A policy governing `team-a` leaves every other namespace resolving to
/// `legacy-roster-v1`, synthesised from this very roster. Marking it superseded
/// invites an operator to stop maintaining or delete it, which silently
/// un-trusts every unbound namespace. The only policy that displaces the roster
/// for EVERY namespace is the cluster default.
#[test]
fn only_a_default_policy_supersedes_the_roster() {
    let roster = roster_object(vec![]);
    let cases: Vec<(&str, Vec<TrustPolicy>, &str, &str)> = vec![
        (
            "no policy at all",
            vec![],
            "False",
            REASON_ROSTER_STILL_CONSULTED,
        ),
        (
            "a policy naming only team-a",
            vec![policy("team-a", &["team-a"], false, vec![evidence_key()])],
            "False",
            REASON_ROSTER_STILL_CONSULTED,
        ),
        (
            "the cluster default",
            vec![policy("org-default", &[], true, vec![evidence_key()])],
            "True",
            REASON_SUPERSEDED_BY_TRUST_POLICY,
        ),
        (
            "two defaults, which contest each other",
            vec![
                policy("default-one", &[], true, vec![evidence_key()]),
                policy("default-two", &[], true, vec![evidence_key()]),
            ],
            "False",
            REASON_ROSTER_STILL_CONSULTED,
        ),
    ];
    for (name, policies, status, reason) in cases {
        let c = trust_policy::superseded_condition(&roster, &policies, now());
        assert_eq!(c.status, status, "{name}");
        assert_eq!(c.reason.as_deref(), Some(reason), "{name}");
    }
}

/// **F2, second half: the condition CLEARS when the policy goes away.**
///
/// `reconcile_policy` runs only for a policy that exists, so deleting every
/// policy would otherwise leave `Superseded=True` forever — rollback in place
/// advertising the opposite of what is happening. The roster's own reconciler
/// recomputes it from its 300 s requeue, which runs whether or not a policy
/// exists.
#[test]
fn the_superseded_condition_clears_when_the_default_policy_goes_away() {
    use weirkeeper::controllers::trust_roster;

    let defaults = vec![policy("org-default", &[], true, vec![evidence_key()])];
    let marked = trust_policy::superseded_condition(&roster_object(vec![]), &defaults, now());
    assert_eq!(marked.status, "True");

    // Every policy is gone. The ROSTER's reconciler is what notices.
    let roster = roster_object(vec![marked]);
    let verdict = trust_roster::evaluate(&roster.spec, now());
    let status = trust_roster::status_for(&roster, &verdict, &[], now());
    let conditions = status.conditions.expect("conditions");
    let superseded = conditions
        .iter()
        .find(|c| c.r#type == CONDITION_SUPERSEDED)
        .expect("the condition is recomputed, not dropped");
    assert_eq!(superseded.status, "False");
    assert_eq!(
        superseded.reason.as_deref(),
        Some(REASON_ROSTER_STILL_CONSULTED)
    );
    assert!(
        conditions.iter().any(|c| c.r#type == CONDITION_LOADED),
        "and the roster's own condition is still there"
    );
    assert_eq!(
        conditions.len(),
        2,
        "exactly Loaded and Superseded — a duplicate would mean the carry and the recompute \
         both ran"
    );
}

/// **F6: both status writes carry seam S7's precondition.**
#[test]
fn a_status_patch_carries_the_resource_version_precondition() {
    let patch = trust_policy::with_precondition(
        &ObjectMeta {
            name: Some("org-default".to_string()),
            resource_version: Some("99".to_string()),
            ..ObjectMeta::default()
        },
        "org-default",
        serde_json::json!({ "status": { "loaded": true } }),
    )
    .expect("an object from the API server always carries one");
    assert_eq!(patch["metadata"]["resourceVersion"], "99");
    assert_eq!(patch["metadata"]["name"], "org-default");
    assert_eq!(patch["status"]["loaded"], true);

    assert!(
        trust_policy::with_precondition(
            &ObjectMeta::default(),
            "org-default",
            serde_json::json!({ "status": {} })
        )
        .is_err(),
        "an object with no resourceVersion is named rather than patched without a precondition"
    );
}

/// A cluster with no roster at all reconciles without an error — there is
/// nothing to supersede.
#[tokio::test]
async fn a_cluster_with_no_roster_still_reconciles() {
    let p = policy("org-default", &[], true, vec![evidence_key()]);
    let (client, recorder) = mock_client_recording(vec![
        Route {
            method: "GET",
            path_suffix: "/trustpolicies",
            status: 200,
            body: policy_list_body(),
        },
        Route {
            method: "PATCH",
            path_suffix: "/trustpolicies/org-default/status",
            status: 200,
            body: policy_body(),
        },
        Route {
            method: "GET",
            path_suffix: "/trustrosters/default",
            status: 404,
            body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}"#.to_string(),
        },
    ]);
    trust_policy::reconcile_policy(&p, &client)
        .await
        .expect("a missing roster is not a reconcile error");
    assert!(seen(&recorder)
        .iter()
        .all(|(m, u)| !(m == "PATCH" && u.contains("trustrosters"))));
}

/// The roster reconciler carries conditions it does not own, so the two
/// controllers do not delete each other's work.
#[test]
fn the_roster_reconciler_carries_conditions_it_does_not_own() {
    use weirkeeper::controllers::trust_roster;
    use weirkeeper::crds::trust_roster::TrustRosterStatus;

    let default_policy = policy("org-default", &[], true, vec![evidence_key()]);
    let spec = roster(vec![entry(KEY_A_ID, KEY_A_PEM, None)], vec![]);
    let existing = TrustRosterObject {
        metadata: ObjectMeta {
            name: Some("default".to_string()),
            generation: Some(1),
            resource_version: Some("41".to_string()),
            ..ObjectMeta::default()
        },
        spec,
        status: Some(TrustRosterStatus {
            loaded: Some(true),
            expired_key_ids: Some(vec![]),
            conditions: Some(vec![weirkeeper::crds::Condition {
                r#type: CONDITION_SUPERSEDED.to_string(),
                status: "True".to_string(),
                observed_generation: Some(1),
                last_transition_time: Some(now()),
                reason: Some(REASON_SUPERSEDED_BY_TRUST_POLICY.to_string()),
                message: Some("superseded".to_string()),
            }]),
        }),
    };
    let verdict = trust_roster::evaluate(&existing.spec, now());
    let status = trust_roster::status_for(
        &existing,
        &verdict,
        std::slice::from_ref(&default_policy),
        now(),
    );
    let types: Vec<&str> = status
        .conditions
        .as_ref()
        .expect("conditions")
        .iter()
        .map(|c| c.r#type.as_str())
        .collect();
    assert!(
        types.contains(&CONDITION_SUPERSEDED),
        "a JSON merge patch REPLACES arrays, so a roster status carrying only [Loaded] deletes \
         the Superseded condition the TrustPolicy controller wrote — and the two would then \
         delete each other's on every pass. Got {types:?}"
    );
    assert!(types.contains(&CONDITION_LOADED));
}

/// A `TrustPolicy` list with one item, for the route table.
fn policy_list_body() -> String {
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"TrustPolicyList",
             "metadata":{{"resourceVersion":"1"}},"items":[{}]}}"#,
        policy_body()
    )
}

/// One `TrustPolicy` object, for the route table.
fn policy_body() -> String {
    let pem = KEY_A_PEM.replace('\n', "\\n");
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"TrustPolicy",
             "metadata":{{"name":"org-default","uid":"uid-org-default","generation":1,
                          "resourceVersion":"17"}},
             "spec":{{"default":true,
                      "allowedTargetClusterIds":["scratch-cluster-id"],
                      "keys":[{{"keyId":"{KEY_A_ID}","spkiPem":"{pem}","algorithm":"ed25519",
                                "usages":["EvidenceSigning"],
                                "principal":{{"id":"install:{KEY_A_ID}"}},
                                "notBefore":"2026-01-01T00:00:00Z",
                                "notAfter":"2027-06-01T00:00:00Z","state":"Active"}}]}}}}"#
    )
}

/// A `TrustRoster` object, for the route table.
fn roster_body() -> String {
    let pem = KEY_A_PEM.replace('\n', "\\n");
    format!(
        r#"{{"apiVersion":"logweir.dev/v1alpha1","kind":"TrustRoster",
             "metadata":{{"name":"default","generation":1,"resourceVersion":"41"}},
             "spec":{{"approverKeys":[{{"keyId":"{KEY_A_ID}","spkiPem":"{pem}"}}],
                      "signingKeys":[],"allowedClusterIds":["scratch-cluster-id"]}}}}"#
    )
}

/// The status type the reconciler writes is the CRD's, unchanged.
#[test]
fn the_status_shape_is_the_crds_own() {
    let status = TrustPolicyStatus::default();
    assert!(status.keys.is_none());
    assert!(status.conflicts.is_none());
}
