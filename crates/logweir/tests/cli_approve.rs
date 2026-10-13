//! `logweir drill approve`, driven through the COMPILED BINARY.
//!
//! The only producer of an approval must ship in the container image and the
//! release tarballs, or `drill run`'s mandatory `--approval` cannot be
//! satisfied from Logweir's own artifacts, and a library-level test cannot
//! see that. Every test here spawns the binary and feeds the result to the
//! REAL phase-1 verifier.
mod support;
use support::bin;

use std::path::{Path, PathBuf};

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

// `--subject-kind`.

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
        help.contains("[possible values: Restore, Backup, Switchover]"),
        "the capitalised wire spellings, in the order the enum declares them \
         (`RehearsalSchedule` left with the withdrawn standing signer): {help}"
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

// The standing mint is withdrawn (OD-12).

/// **`drill approve --standing` is refused by clap, and so is
/// `--subject-kind RehearsalSchedule`.** Standing rehearsal authorizations are
/// withdrawn in v1, so this binary mints none: the flag is unknown and the
/// value is not one of `--subject-kind`'s. Both are usage errors (exit 1,
/// never 2 — Global Constraint 11) that name what was refused, and nothing is
/// written.
///
/// NEGATIVE CONTROL: the same command line without the refused word mints
/// both files, so the refusal is the flag's or the value's and not the rest of
/// the line's. Restoring `--standing` to the CLI makes the first half fail.
#[test]
fn the_withdrawn_standing_mint_is_refused_by_clap() {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, example_spec()).unwrap();
    let key = write_key(dir.path());
    let attempt = |name: &str, extra: &[&str]| {
        let out = dir.path().join(format!("{name}.json"));
        let done = bin()
            .args(["drill", "approve"])
            .arg("--spec")
            .arg(&spec)
            .arg("--key")
            .arg(&key)
            .args(["--approver", "a@example.com", "--ticket", "CHG-1"])
            .args(extra)
            .arg("--out")
            .arg(&out)
            .output()
            .unwrap();
        let transcript = format!(
            "{}{}",
            String::from_utf8_lossy(&done.stdout),
            String::from_utf8_lossy(&done.stderr)
        );
        (done.status.code(), transcript, out)
    };

    let (code, transcript, out) = attempt("standing", &["--standing"]);
    assert_eq!(code, Some(1), "{transcript}");
    assert!(
        transcript.contains("unexpected argument '--standing'"),
        "clap names the withdrawn flag: {transcript}"
    );
    assert!(!out.exists() && !out.with_extension("sig").exists());

    let (code, transcript, out) = attempt("kind", &["--subject-kind", "RehearsalSchedule"]);
    assert_eq!(code, Some(1), "{transcript}");
    assert!(
        transcript.contains("invalid value 'RehearsalSchedule'"),
        "clap names the withdrawn value: {transcript}"
    );
    assert!(!out.exists() && !out.with_extension("sig").exists());

    // The control.
    let (code, transcript, out) = attempt("control", &[]);
    assert_eq!(code, Some(0), "{transcript}");
    assert!(out.exists() && out.with_extension("sig").exists());
}

/// **The blank-approver refusal is `mint`'s own, not clap's.**
///
/// `--approver` is required by clap, so a mutant deleting the check inside
/// `mint` survives every subprocess row here. This one calls the library directly,
/// the way any future caller that builds `ApproveArgs` itself would, and a
/// whitespace-only value is refused for the same reason an absent one is: the
/// value is signed and copied verbatim into the scorecard's accountability
/// record.
#[test]
fn a_blank_approver_or_ticket_is_refused_by_mint_itself() {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, example_spec()).unwrap();
    let key = write_key(dir.path());

    let args = |approver: &str, ticket: &str| logweir::approve::ApproveArgs {
        spec: spec.clone(),
        key: key.clone(),
        approver: approver.to_string(),
        ticket: ticket.to_string(),
        out: dir.path().join("blank.json"),
        subject_kind: "Restore".to_string(),
        approval_subject: None,
    };

    for (approver, ticket, flag) in [
        ("", "CHG-1", "--approver"),
        ("   ", "CHG-1", "--approver"),
        ("me", "", "--ticket"),
        ("me", "\t", "--ticket"),
    ] {
        let error = logweir::approve::mint(&args(approver, ticket), fixed_now())
            .expect_err("a blank accountability field is refused");
        assert!(
            error.contains(flag) && error.contains("must not be blank"),
            "{approver:?}/{ticket:?}: {error}"
        );
    }
    assert!(
        !dir.path().join("blank.json").exists(),
        "and nothing was signed"
    );

    // The control: a named approver and ticket still mint.
    logweir::approve::mint(&args("operator@example.com", "CHG-42"), fixed_now())
        .expect("the ordinary case");
    assert!(dir.path().join("blank.json").exists());
}

// The clock is read at the entry point, and only there.

fn at(rfc3339: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .unwrap()
        .with_timezone(&chrono::Utc)
}

/// The `now` the library rows hand `mint`: fixed, so no row depends on the
/// date it runs.
fn fixed_now() -> chrono::DateTime<chrono::Utc> {
    at("2026-09-01T00:00:00Z")
}

/// **`mint` signs the CALLER's `now` as `approved_at` and reads no clock.**
///
/// `approved_at` is not decoration: it is the approval's claimed signing time,
/// which the trust lifecycle judges against the signing key's window
/// (`logweir_core::trust::claimed_signing_time`). Two instants centuries
/// apart, both far from any date this suite runs on, so a build that stamps
/// the wall clock fails this row on every date.
#[test]
fn mint_signs_the_callers_now_as_approved_at() {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, example_spec()).unwrap();
    let key = write_key(dir.path());

    for now in [at("2001-01-01T00:00:00Z"), at("2999-06-01T00:00:00Z")] {
        let out = dir
            .path()
            .join(format!("approval-{}.json", now.timestamp()));
        logweir::approve::mint(
            &logweir::approve::ApproveArgs {
                spec: spec.clone(),
                key: key.clone(),
                approver: "operator@example.com".to_string(),
                ticket: "CHG-42".to_string(),
                out: out.clone(),
                subject_kind: "Restore".to_string(),
                approval_subject: None,
            },
            now,
        )
        .expect("the approval mints");
        let doc: logweir_core::spec::ApprovalDoc =
            serde_json::from_slice(&std::fs::read(&out).unwrap()).expect("the approval parses");
        assert_eq!(
            doc.approved_at, now,
            "approved_at is the caller's now, never the wall clock"
        );
    }
}

/// **The binary's half: `drill approve` hands the mint the WALL CLOCK.**
///
/// The library rows prove that `mint` stamps the `now` it is handed. This row
/// proves `run` hands it the wall clock. It compares the stamp with this
/// test's own clock, read around the subprocess, so it holds on any date. A
/// `run` that passed a fixed instant fails.
#[test]
fn drill_approve_hands_the_mint_the_wall_clock() {
    // Generous, because the row is about WHICH clock, not about latency: a
    // fixed instant misses this window by years.
    let slack = chrono::Duration::minutes(2);

    // (a) The per-run approval's `approved_at`.
    let before = chrono::Utc::now();
    let m = approve_over(&example_spec());
    let after = chrono::Utc::now();
    let doc: logweir_core::spec::ApprovalDoc =
        serde_json::from_slice(&std::fs::read(&m.approval).unwrap()).unwrap();
    assert!(
        before - slack <= doc.approved_at && doc.approved_at <= after + slack,
        "approved_at {} is not the wall clock ({before} .. {after})",
        doc.approved_at
    );
}

// No approval over a subset plan this release refuses.

/// The example drill spec with a `restore` block naming a partition subset,
/// its point in time written as `point_in_time`.
fn subset_spec(point_in_time: &str) -> String {
    let base = example_spec();
    assert!(
        !base.contains("\nrestore:"),
        "the example states no restore block"
    );
    format!(
        "{base}\nrestore:\n  point_in_time: \"{point_in_time}\"\n  partitions:\n    orders: [0, 2]\n"
    )
}

/// **`drill approve` refuses an unparseable subset plan, by name, and signs
/// nothing.** A subset beside a plain instant is refused at parse by this
/// release's runner and preview (`PlanUnparseable`), but an approval binds
/// bytes: minted, it could be run by a runner that predates partition subsets,
/// which ignores the key and restores every partition. THE CONTROL: the same
/// subset beside the open-start interval mints, and a plan with no subset is
/// minted as before (every other row in this file).
///
/// KILLS: the parse check removed from `mint`; `refuse_unparseable_subset_plan`
/// reading a plan with the key as one without it.
#[test]
fn approve_refuses_an_unparseable_subset_plan_by_name_and_signs_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let key = write_key(dir.path());
    let run = |text: &str, name: &str| {
        let spec = dir.path().join(format!("{name}.yaml"));
        std::fs::write(&spec, text).unwrap();
        let out = dir.path().join(format!("{name}.approval.json"));
        let o = bin()
            .args(["drill", "approve"])
            .arg("--spec")
            .arg(&spec)
            .arg("--key")
            .arg(&key)
            .args(["--approver", "sre-oncall@example.com"])
            .args(["--ticket", "CHG-40881"])
            .arg("--out")
            .arg(&out)
            .output()
            .unwrap();
        (
            o.status.code(),
            String::from_utf8_lossy(&o.stderr).to_string(),
            out,
        )
    };
    let (code, stderr, out) = run(&subset_spec("2026-09-07T14:05:00Z"), "instant");
    assert_eq!(code, Some(1), "{stderr}");
    assert!(
        stderr.contains(logweir::approve::SUBSET_PLAN_UNPARSEABLE)
            && stderr.contains("restore.partitions"),
        "{stderr}"
    );
    assert!(!out.exists(), "nothing was signed");
    assert!(!out.with_extension("sig").exists());

    // THE CONTROL: the open-start interval is this release's spelling.
    let (code, stderr, out) = run(&subset_spec("../2026-09-07T14:05:00Z"), "interval");
    assert_eq!(code, Some(0), "{stderr}");
    assert!(out.exists());

    // The predicate itself: no key, a null key or bytes that are not YAML are
    // not judged; a subset beside an instant is.
    use logweir::approve::refuse_unparseable_subset_plan as judge;
    assert_eq!(judge(&example_spec()), Ok(()));
    assert_eq!(judge("restore:\n  partitions: null\n"), Ok(()));
    assert_eq!(judge(": : not yaml : ["), Ok(()));
    assert!(judge(&subset_spec("2026-09-07T14:05:00Z"))
        .unwrap_err()
        .starts_with("SubsetPlanUnparseable: "));
    assert_eq!(judge(&subset_spec("../2026-09-07T14:05:00Z")), Ok(()));
}
