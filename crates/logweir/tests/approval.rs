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

// ---------------------------------------------------------------------------
// Task 22 — `--approver-key-ids`, the pinned approver set
// ---------------------------------------------------------------------------
//
// EVERYTHING ABOVE THIS LINE IS UNTOUCHED, and that is an acceptance criterion
// rather than a courtesy: "omitting `--approver-key-ids` preserves today's
// behaviour exactly" is proved by the existing suite compiling and passing
// against the new code with a zero-deletion test-name diff.
// `phase1_approval::verify` keeps its four-argument signature for the same
// reason — the pinned check is a separate entry point, hoisted ahead of phase
// 0, not a fifth parameter every caller must pass.

use std::path::Path;
use std::process::Command;

/// `(exit code, stdout, stderr)` from the COMPILED binary, with the code read
/// from the process status directly and never through a pipe (STANDING RULE
/// 20).
fn run_restore(args: &[String]) -> (Option<i32>, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_logweir"))
        .args(["restore", "run"])
        .args(args)
        .output()
        .expect("the compiled logweir binary runs");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// The five flags every `restore run` in this file shares, over the shipped
/// example plan and the checked-in fixture keypair.
///
/// `e2e/fixtures/signed/public.pem` is the PUBLIC half; no private key is read
/// here (erratum **E10e**: `signing.pem` is a stage-1 throwaway and is the only
/// private key any test may name, and it is named as the SCORECARD signing key,
/// which is what `restore run` requires).
fn base_args() -> Vec<String> {
    [
        "--spec",
        "../../examples/drill.yaml",
        "--approval",
        "../../examples/approval.json",
        "--approver-key",
        "../../e2e/fixtures/signed/public.pem",
        "--allowed-clusters",
        "../../examples/allowed-clusters.json",
        "--signing-key",
        "../../e2e/fixtures/signed/signing.pem",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect()
}

/// `base_args` with `--spec` repointed at `spec` — the index is the one the
/// literal above fixes, asserted rather than assumed.
fn base_args_over(spec: &Path) -> Vec<String> {
    let mut a = base_args();
    assert_eq!(a[0], "--spec");
    a[1] = spec.to_string_lossy().to_string();
    a
}

/// `--approver-key-ids <id>` for each id, appended.
fn pin(args: &mut Vec<String>, ids: &[&str]) {
    for id in ids {
        args.push("--approver-key-ids".to_string());
        args.push((*id).to_string());
    }
}

/// The fixture approver key's ACTUAL id, computed the way the guard computes
/// it, so no test here hard-codes a hash that could drift from the file.
fn fixture_key_id() -> String {
    logweir_evidence::keys::VerifyingKey::from_pem_file(Path::new(
        "../../e2e/fixtures/signed/public.pem",
    ))
    .expect("the checked-in fixture public key parses")
    .key_id()
}

/// A plan whose phase 0 refuses on a PURELY LOCAL check — the same spec
/// `guard_cli.rs` uses for the mapping guard.
///
/// It is the discriminator for "the pinned guard let this run through": phase 0
/// is reached, refuses locally, and says so in its own words. A plan that
/// passed phase 0's local checks would go on to DIAL, which no test in the
/// default suite may depend on (Global Constraint 22).
fn spec_refused_by_phase_0(dir: &Path) -> std::path::PathBuf {
    let text = std::fs::read_to_string("../../examples/drill.yaml")
        .unwrap()
        .replace(
            "topic_mapping_prefix: \"drill-\"",
            "topic_mapping_prefix: \"\"",
        );
    let p = dir.join("phase0-refuses.yaml");
    std::fs::write(&p, text).unwrap();
    p
}

/// An approval whose approver key id is outside `--approver-key-ids` exits 3,
/// on the exact message, **before phase 0 dials anything**.
///
/// Global Constraint 11: exit 3 is "refused by a guard, before anything ran".
/// There is no broker up for this test and there does not need to be — which is
/// the property, not a convenience.
#[test]
fn phase1_refuses_approver_key_outside_pinned_ids() {
    let mut args = base_args();
    pin(&mut args, &["sha256:deadbeef", "sha256:cafebabe"]);
    let (code, stdout, stderr) = run_restore(&args);
    assert_eq!(code, Some(3), "stderr: {stderr}");

    let want = phase1_approval::pinned_set_refusal(
        &fixture_key_id(),
        &["sha256:deadbeef".to_string(), "sha256:cafebabe".to_string()],
    );
    assert!(
        stderr.contains(&want),
        "the refusal must be this message verbatim.\nwant: {want}\ngot: {stderr}"
    );
    assert!(
        want.contains("is not in the pinned set {sha256:deadbeef, sha256:cafebabe}"),
        "the pinned set is rendered in braces, comma-and-space separated, in flag order: {want}"
    );
    assert!(
        stdout.trim_end().ends_with("refusal-reason=GuardRefused"),
        "a guard refusal prints its reason as the final stdout line (interface I9): {stdout}"
    );
}

/// The same approval with its id pinned proceeds PAST phase 1's pinned check.
///
/// The discriminator is the message: phase 0's own local refusal, which is
/// reached only because the pinned guard returned `Ok`. Asserting merely "not
/// exit 3" would be satisfied by a broken guard that never refuses anything.
#[test]
fn phase1_accepts_approver_key_inside_pinned_ids() {
    // In process, on the guard itself.
    let id = fixture_key_id();
    phase1_approval::admit_pinned_approver_key_id(
        Path::new("../../e2e/fixtures/signed/public.pem"),
        &["sha256:deadbeef".to_string(), id.clone()],
    )
    .expect("a pinned id that matches the key is admitted");

    // And through the binary, where the run must get past the guard.
    let dir = tempfile::tempdir().unwrap();
    let spec = spec_refused_by_phase_0(dir.path());
    let mut args = base_args_over(&spec);
    pin(&mut args, &["sha256:deadbeef", &id]);
    let (code, _stdout, stderr) = run_restore(&args);
    assert_eq!(code, Some(3), "stderr: {stderr}");
    assert!(
        !stderr.contains("is not in the pinned set"),
        "the pinned guard must have admitted this key: {stderr}"
    );
    assert!(
        stderr.contains("topic_mapping entry") || stderr.contains("onto itself"),
        "and the run must have reached PHASE 0, whose local refusal this is: {stderr}"
    );
}

/// Omitting `--approver-key-ids` reproduces today's behaviour exactly.
///
/// Two halves. (a) The guard itself is a no-op on an empty set, for a key id
/// that is in no set at all — so nothing can be refused for want of a flag.
/// (b) The same command line WITHOUT the flag reaches the same phase-0 refusal
/// as the one WITH a matching id, byte for byte on the discriminating message.
/// The third half is the test-name diff: every test above this section runs
/// unmodified, and the mutant "make `--approver-key-ids` mandatory" is killed
/// by the binary refusing this very invocation.
#[test]
fn phase1_without_the_flag_is_unchanged() {
    phase1_approval::admit_pinned_approver_key_id(
        Path::new("../../e2e/fixtures/signed/public.pem"),
        &[],
    )
    .expect("an empty pinned set pins nothing");

    let dir = tempfile::tempdir().unwrap();
    let spec = spec_refused_by_phase_0(dir.path());
    let args = base_args_over(&spec);
    let (code, _out, stderr) = run_restore(&args);
    assert_eq!(
        code,
        Some(3),
        "no flag, and the run behaves as it always did: {stderr}"
    );
    assert!(
        !stderr.contains("pinned set"),
        "an absent flag pins nothing and names nothing: {stderr}"
    );
    assert!(
        stderr.contains("topic_mapping entry") || stderr.contains("onto itself"),
        "the pre-existing phase-0 refusal, unchanged: {stderr}"
    );
}

/// `--subject-kind` is written INSIDE the signed bytes.
///
/// Proved by mutation rather than by reading the file: the minted
/// `approval.json` contains `"subject_kind":"Backup"`, and changing that field
/// **after** signing makes `phase1_approval::verify` return
/// `DrillError::Guard(GuardRefusal(_))` — which `crates/logweir/src/exit.rs`
/// maps to exit 3, asserted process-level by
/// `cli_exit_codes.rs::a_mutated_subject_kind_is_a_guard_refusal_exit_3`.
///
/// If the field lived in the DSSE SIDECAR instead, the same mutation would
/// verify happily, and that is exactly the mutant this test kills.
///
/// **The assertion is against `verify`, not against a `drill run` invocation**:
/// `drill/mod.rs` dials in phase 0 before it reaches phase 1 (and
/// `assert_engine_identity` exits 1 on an unset engine environment), so a
/// `drill run` with no broker up exits 1 and never observes this refusal. This
/// task therefore needs neither the compose stack nor a
/// `no_network_in_unit_tests.rs` allow-list entry.
#[test]
fn subject_kind_is_inside_the_signed_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let spec_text = std::fs::read_to_string("../../examples/drill.yaml").unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, &spec_text).unwrap();

    let key = dir.path().join("approver.pem");
    let sk = logweir_evidence::keys::SigningKey::generate_p256();
    std::fs::write(&key, sk.to_pkcs8_pem().unwrap()).unwrap();
    let approver_pub = dir.path().join("approver.pub.pem");
    std::fs::write(
        &approver_pub,
        sk.verifying_key().to_public_key_pem().unwrap(),
    )
    .unwrap();
    let approval = dir.path().join("approval.json");

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_logweir"))
        .args(["drill", "approve"])
        .arg("--spec")
        .arg(&spec)
        .arg("--key")
        .arg(&key)
        .args(["--approver", "sre-oncall@example.com"])
        .args(["--ticket", "CHG-40881"])
        .args(["--subject-kind", "Backup"])
        .arg("--out")
        .arg(&approval)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let minted = std::fs::read_to_string(&approval).unwrap();
    let compact: String = minted.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(
        compact.contains(r#""subject_kind":"Backup""#),
        "the flag's value must be in the document: {minted}"
    );

    // Unmutated, it verifies.
    let signing = logweir_evidence::keys::SigningKey::generate_p256().verifying_key();
    phase1_approval::verify(&spec_text, &approval, &approver_pub, &signing)
        .expect("the minted approval verifies before the mutation");

    // Mutate the field AFTER signing. Inside the signed bytes, this is a
    // guard refusal; in the sidecar it would be invisible.
    std::fs::write(
        &approval,
        minted.replace(
            r#""subject_kind": "Backup""#,
            r#""subject_kind": "Restore""#,
        ),
    )
    .unwrap();
    assert_ne!(
        std::fs::read_to_string(&approval).unwrap(),
        minted,
        "the mutation must actually change the file — if the field is not there in this \
         spelling, this test proves nothing"
    );
    let e = phase1_approval::verify(&spec_text, &approval, &approver_pub, &signing).unwrap_err();
    match e {
        DrillError::Guard(GuardRefusal(msg)) => assert!(
            msg.contains("signature"),
            "the signature no longer covers these bytes: {msg}"
        ),
        other => panic!("expected DrillError::Guard (exit 3), got {other:?}"),
    }
}

/// An approval with NO `subject_kind` — every approval minted before the flag
/// existed — is read as `Restore` by the runner and verifies unchanged.
#[test]
fn an_approval_with_no_subject_kind_is_read_as_restore() {
    let f = fixtures::approval_ok();
    let raw = std::fs::read_to_string(&f.approval).unwrap();
    assert!(
        !raw.contains("subject_kind"),
        "the fixture must be a pre-Task-22 document for this to mean anything: {raw}"
    );
    phase1_approval::verify(&f.spec_text, &f.approval, &f.approver_pub, &f.signing_key)
        .expect("an approval with no subject_kind still verifies");

    let doc: logweir_core::spec::ApprovalDoc = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        doc.subject_kind,
        logweir_core::spec::SUBJECT_KIND_RESTORE,
        "absent means Restore on the RUNNER path; the controller defaults it to \"\" instead \
         and refuses with check 8, which is the fail-closed half"
    );
}
