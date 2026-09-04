mod fixtures; // crates/logweir/tests/fixtures/mod.rs — Task 14 step 5c
use logweir::drill::phase1_approval;

#[test]
fn a_matching_plan_hash_and_valid_signature_is_approved() {
    let f = fixtures::approval_ok();
    let a = phase1_approval::verify(&f.spec_text, &f.approval, &f.approver_pub, "signer-key-id")
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
    let e = phase1_approval::verify(&tampered, &f.approval, &f.approver_pub, "signer").unwrap_err();
    assert!(e.to_string().contains("plan_hash"), "{e}");
}

#[test]
fn an_approval_signed_by_the_wrong_key_is_refused() {
    let f = fixtures::approval_ok();
    let e = phase1_approval::verify(&f.spec_text, &f.approval, &f.other_pub, "signer").unwrap_err();
    assert!(e.to_string().contains("signature"), "{e}");
}

#[test]
fn an_approval_key_equal_to_the_signing_key_is_labelled_not_refused() {
    let f = fixtures::approval_self();
    let a = phase1_approval::verify(
        &f.spec_text,
        &f.approval,
        &f.approver_pub,
        &f.approver_key_id,
    )
    .unwrap();
    assert!(
        a.approval.self_attested,
        "R13: the artifact must survive this reading, not hide it"
    );
}
