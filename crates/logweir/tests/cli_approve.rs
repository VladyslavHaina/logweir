//! `logweir drill approve`, driven through the COMPILED BINARY.
//!
//! The defect this file exists for was not "the signing code is wrong" — the
//! signing code was correct and tested. It was that the only producer of an
//! approval was a cargo EXAMPLE, which ships in neither the container image
//! nor the release tarballs, so `drill run`'s mandatory `--approval` could not
//! be satisfied from Logweir's own artifacts. A library-level test would have
//! passed throughout. Every test here therefore spawns the binary and then
//! feeds the result to the REAL phase-1 verifier.
use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
}

fn write_key(dir: &Path) -> PathBuf {
    let p = dir.join("approver.pem");
    let sk = logweir_evidence::keys::SigningKey::generate_p256();
    std::fs::write(&p, sk.to_pkcs8_pem().unwrap()).unwrap();
    p
}

fn write_pub(private: &Path, to: &Path) {
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(private).unwrap();
    std::fs::write(to, sk.verifying_key().to_public_key_pem().unwrap()).unwrap();
}

struct Minted {
    dir: tempfile::TempDir,
    spec_text: String,
    approval: PathBuf,
    approver_pub: PathBuf,
    key: PathBuf,
}

fn approve_over(spec_text: &str) -> Minted {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, spec_text).unwrap();
    let key = write_key(dir.path());
    let approver_pub = dir.path().join("approver.pub.pem");
    write_pub(&key, &approver_pub);
    let approval = dir.path().join("approval.json");

    let out = bin()
        .args(["drill", "approve"])
        .arg("--spec")
        .arg(&spec)
        .arg("--key")
        .arg(&key)
        .args(["--approver", "sre-oncall@example.com"])
        .args(["--ticket", "CHG-40881"])
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
    Minted {
        spec_text: spec_text.to_string(),
        approval,
        approver_pub,
        key,
        dir,
    }
}

fn example_spec() -> String {
    std::fs::read_to_string("../../examples/drill.yaml").unwrap()
}

/// THE HEADLINE. The shipped binary produces an approval the shipped binary's
/// own phase 1 accepts, with no cargo example, no `jq` and no `shasum` in the
/// path.
#[test]
fn the_shipped_binary_mints_an_approval_that_phase_1_accepts() {
    let m = approve_over(&example_spec());
    let signing = logweir_evidence::keys::SigningKey::generate_p256().verifying_key();
    let a = logweir::drill::phase1_approval::verify(
        &m.spec_text,
        &m.approval,
        &m.approver_pub,
        &signing,
    )
    .expect("phase 1 must accept the approval `drill approve` just minted");
    assert_eq!(a.approval.ticket, "CHG-40881");
    assert_eq!(a.approval.approver, "sre-oncall@example.com");
    assert!(
        !a.approval.self_attested,
        "the approver key and the scorecard signing key differ here"
    );
    drop(m.dir);
}

/// The sidecar must land at exactly the path `phase1_approval::verify` derives
/// (`approval.json` -> `approval.sig`). A sidecar written anywhere else is an
/// approval `drill run` reports as missing.
#[test]
fn the_sidecar_is_written_beside_the_approval_where_drill_run_looks() {
    let m = approve_over(&example_spec());
    let sig = m.approval.with_extension("sig");
    assert!(sig.exists(), "no sidecar at {}", sig.display());
    let side: logweir_evidence::Sidecar =
        serde_json::from_slice(&std::fs::read(&sig).unwrap()).unwrap();
    assert_eq!(
        side.payload_type,
        logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        "a sidecar under any other payload type is refused by phase 1"
    );
    drop(m.dir);
}

/// `plan_hash` binds the EXACT spec bytes. This is the property that forces a
/// re-approval on every spec edit, and the reason a shipped minting command is
/// a requirement rather than a convenience: the sample window moves.
#[test]
fn an_approval_minted_over_one_spec_is_refused_against_an_edited_one() {
    let m = approve_over(&example_spec());
    let edited = m
        .spec_text
        .replace("records_per_partition: 25", "records_per_partition: 26");
    assert_ne!(
        edited, m.spec_text,
        "the substitution must actually change the spec, or this test proves nothing"
    );
    let signing = logweir_evidence::keys::SigningKey::generate_p256().verifying_key();
    let e =
        logweir::drill::phase1_approval::verify(&edited, &m.approval, &m.approver_pub, &signing)
            .unwrap_err();
    match e {
        logweir::drill::DrillError::Guard(logweir_core::guard::GuardRefusal(msg)) => {
            assert!(msg.contains("plan_hash"), "{msg}")
        }
        other => panic!("expected a guard refusal (exit 3), got {other:?}"),
    }
    drop(m.dir);
}

/// An approver key equal to the scorecard signing key is LABELLED, never
/// refused — the same rule phase 1 already applies, reached now through the
/// shipped minting path.
#[test]
fn an_approval_minted_with_the_signing_key_is_labelled_self_attested() {
    let m = approve_over(&example_spec());
    let signing = logweir_evidence::keys::SigningKey::from_pem_file(&m.key)
        .unwrap()
        .verifying_key();
    let a = logweir::drill::phase1_approval::verify(
        &m.spec_text,
        &m.approval,
        &m.approver_pub,
        &signing,
    )
    .unwrap();
    assert!(a.approval.self_attested);
    drop(m.dir);
}

/// An unreadable key is exit 1 (operational), never 3: nothing was refused,
/// the tool could not do its own job. And the error must name the PATH only —
/// no key material in any message (Global Constraint on key handling).
#[test]
fn an_unreadable_key_is_operational_and_never_echoes_key_material() {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, example_spec()).unwrap();
    let key = dir.path().join("not-a-key.pem");
    std::fs::write(
        &key,
        "-----BEGIN PRIVATE KEY-----\nSUPERSECRET\n-----END PRIVATE KEY-----\n",
    )
    .unwrap();

    let out = bin()
        .args(["drill", "approve"])
        .arg("--spec")
        .arg(&spec)
        .arg("--key")
        .arg(&key)
        .args(["--approver", "a@example.com", "--ticket", "T-1"])
        .arg("--out")
        .arg(dir.path().join("approval.json"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        !err.contains("SUPERSECRET"),
        "the error echoed the key file's contents: {err}"
    );
    assert!(
        !dir.path().join("approval.json").exists(),
        "nothing may be written when the key could not be loaded"
    );
}

/// `drill approve` is reachable from the compiled binary's own help. The
/// example it replaces was invisible to `--help` by construction, which is
/// how an operator failed to find it.
#[test]
fn approve_is_listed_in_drill_help() {
    let out = bin().args(["drill", "--help"]).output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("approve"),
        "`drill --help` omits approve:\n{text}"
    );
}

// ---------------------------------------------------------------------------
// Task 22 — `--subject-kind`
// ---------------------------------------------------------------------------

/// The two values `--subject-kind` accepts are the WIRE spellings, capitalised
/// — and the help text states both halves of what the field means.
///
/// clap's default `ValueEnum` rendering is kebab-case (`restore`, `backup`).
/// The string this flag produces goes inside the signed bytes and is compared
/// by the controller's check 8 against a Kubernetes `kind`, so a lower-cased
/// value would mint approvals that verify on the runner and are refused by
/// every controller with a message naming two strings that differ only in
/// case. `cli::SubjectKindArg` therefore pins both with `#[value(name = ...)]`,
/// and this is the test that fails if someone drops them.
#[test]
fn the_subject_kind_values_are_the_wire_spellings() {
    let out = bin().args(["drill", "approve", "--help"]).output().unwrap();
    assert_eq!(out.status.code(), Some(0));
    let help = String::from_utf8(out.stdout).unwrap();
    assert!(
        help.contains("[possible values: Restore, Backup, RehearsalSchedule]"),
        "the capitalised wire spellings, in the order the enum declares them; \
         `RehearsalSchedule` joined them with PLAT-14.3b's standing signer: {help}"
    );
    assert!(
        help.contains("[default: Restore]"),
        "absent means Restore: {help}"
    );
    // BOTH HALVES, in the flag's own help text.
    let flat: String = help.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        flat.contains("RUNNER: an approval with no `subject_kind` at all")
            && flat.contains("is treated as `Restore` and verifies unchanged"),
        "the RUNNER half — an approval with no subject_kind verifies as Restore: {help}"
    );
    assert!(
        flat.contains(
            "CONTROLLER: the same approval is refused by the `Approval` reconciler's check 8 \
             when the referent it names is not a `Restore`"
        ),
        "the CONTROLLER half — refused by check 8 when the referent is not a Restore: {help}"
    );

    // And the constants the rest of the workspace compares against.
    assert_eq!(logweir::cli::SubjectKindArg::Restore.as_str(), "Restore");
    assert_eq!(logweir::cli::SubjectKindArg::Backup.as_str(), "Backup");
    assert_eq!(
        logweir::cli::SubjectKindArg::RehearsalSchedule.as_str(),
        "RehearsalSchedule",
        "the spelling the Approval controller compares its referent's kind against"
    );
    assert_eq!(
        logweir::cli::SubjectKindArg::RehearsalSchedule.as_str(),
        logweir_core::execution_contract::REHEARSAL_SCHEDULE_KIND,
        "the flag and the SIGNED document name the same kind"
    );
    assert_eq!(
        logweir_core::spec::SUBJECT_KIND_RESTORE,
        logweir::cli::SubjectKindArg::Restore.as_str(),
        "the runner's compatibility default and the flag's default are one string"
    );
}

/// A lower-cased value is REFUSED, not silently accepted — the mutant "drop
/// the `#[value(name = ...)]` attributes" is killed here as well as by the
/// help-text assertion above.
#[test]
fn a_lower_cased_subject_kind_is_a_usage_error() {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, example_spec()).unwrap();
    let key = write_key(dir.path());
    let out = bin()
        .args(["drill", "approve"])
        .arg("--spec")
        .arg(&spec)
        .arg("--key")
        .arg(&key)
        .args(["--approver", "a@example.com"])
        .args(["--ticket", "CHG-1"])
        .args(["--subject-kind", "restore"])
        .arg("--out")
        .arg(dir.path().join("approval.json"))
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "a usage error is exit 1, never 2 (Global Constraint 11): {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
