mod fixtures; // crates/logweir/tests/fixtures/mod.rs — Task 14 step 5c
use logweir::drill::phase1_approval;
use logweir::drill::DrillError;
use logweir_core::guard::GuardRefusal;

#[test]
fn a_matching_plan_hash_and_valid_signature_is_approved() {
    let f = fixtures::approval_ok();
    let a = phase1_approval::verify(&f.spec_text, &f.approval, &f.approver_pub, &f.signing_key)
        .unwrap();
    assert_eq!(a.approval.ticket, "CHG-40881");
    assert!(!a.approval.self_attested);
    assert!(a.validated_at >= f.before);
}

#[test]
fn a_plan_hash_over_different_spec_bytes_is_refused() {
    let f = fixtures::approval_ok();
    let tampered = f
        .spec_text
        .replace("records_per_partition: 25", "records_per_partition: 1");
    let e = phase1_approval::verify(&tampered, &f.approval, &f.approver_pub, &f.signing_key)
        .unwrap_err();
    match e {
        DrillError::Guard(GuardRefusal(msg)) => assert!(msg.contains("plan_hash"), "{msg}"),
        other => panic!("expected DrillError::Guard (exit 3), got {other:?}"),
    }
}

#[test]
fn an_approval_signed_by_the_wrong_key_is_refused() {
    let f = fixtures::approval_ok();
    let e = phase1_approval::verify(&f.spec_text, &f.approval, &f.other_pub, &f.signing_key)
        .unwrap_err();
    match e {
        // "no signature by key ... in the sidecar" is produced ONLY when the
        // presented key's id matches no signature in the sidecar — distinct
        // from a tampered-payload or payload_type-substitution message, which
        // would also contain the word "signature" but not this phrase.
        DrillError::Guard(GuardRefusal(msg)) => {
            assert!(msg.contains("no signature by key"), "{msg}")
        }
        other => panic!("expected DrillError::Guard (exit 3), got {other:?}"),
    }
}

#[test]
fn an_approval_key_equal_to_the_signing_key_is_labelled_not_refused() {
    let f = fixtures::approval_self();
    let a = phase1_approval::verify(&f.spec_text, &f.approval, &f.approver_pub, &f.signing_key)
        .unwrap();
    assert!(
        a.approval.self_attested,
        "R13: the artifact must survive this reading, not hide it"
    );
}

/// An approval file that cannot be read at all (missing, permission denied,
/// a briefly-unavailable mount) says nothing about whether the plan is
/// authorised — it is an OPERATIONAL failure (exit 1, retry), never a guard
/// refusal (exit 3, "the plan is refused"). Style follows
/// `phase0_admit.rs`'s `a_cluster_id_read_failure_is_operational_not_a_guard_refusal`.
#[test]
fn a_missing_approval_file_is_operational_not_a_guard_refusal() {
    let f = fixtures::approval_ok();
    let missing = f.approval.with_file_name("does-not-exist.json");
    let e = phase1_approval::verify(&f.spec_text, &missing, &f.approver_pub, &f.signing_key)
        .unwrap_err();
    match e {
        DrillError::Operational(msg) => assert!(msg.contains("does-not-exist.json"), "{msg}"),
        other => panic!("expected DrillError::Operational (exit 1), got {other:?}"),
    }
}

/// A missing (or, equally, corrupt/non-JSON) `.sig` sidecar is the tool
/// failing to find evidence, not evidence that the plan was refused.
#[test]
fn a_missing_sidecar_is_operational_not_a_guard_refusal() {
    let f = fixtures::approval_ok();
    std::fs::remove_file(f.approval.with_extension("sig")).unwrap();
    let e = phase1_approval::verify(&f.spec_text, &f.approval, &f.approver_pub, &f.signing_key)
        .unwrap_err();
    match e {
        DrillError::Operational(msg) => assert!(msg.contains("DSSE sidecar"), "{msg}"),
        other => panic!("expected DrillError::Operational (exit 1), got {other:?}"),
    }
}

/// A malformed approver-key PEM is a broken input to the tool, not a
/// statement about the plan's authorisation.
#[test]
fn a_malformed_approver_key_is_operational_not_a_guard_refusal() {
    let f = fixtures::approval_ok();
    let dir = tempfile::tempdir().unwrap();
    let broken = dir.path().join("broken.pub.pem");
    std::fs::write(
        &broken,
        b"-----BEGIN PUBLIC KEY-----\nnot a key\n-----END PUBLIC KEY-----\n",
    )
    .unwrap();
    let e =
        phase1_approval::verify(&f.spec_text, &f.approval, &broken, &f.signing_key).unwrap_err();
    match e {
        DrillError::Operational(_) => {}
        other => panic!("expected DrillError::Operational (exit 1), got {other:?}"),
    }
}
