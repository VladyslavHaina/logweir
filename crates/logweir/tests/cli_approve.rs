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

// ===========================================================================
// PLAT-14.3b fix round 2 — the SHIPPED `--standing` command line
// ===========================================================================
//
// The P0 rows in `standing_approve.rs` call `approve::mint_standing` directly,
// so none of the clap plumbing was covered: not `main.rs`'s
// "--subject-kind RehearsalSchedule requires --standing" refusal — the only
// thing stopping a RehearsalSchedule referent being minted under
// `PAYLOAD_TYPE_APPROVAL` — and not the required/conflicts constraints. These
// rows spawn the binary, exactly as the rest of this file does.

fn standing_scope(dir: &Path) -> PathBuf {
    let p = dir.join("scope.json");
    std::fs::write(
        &p,
        serde_json::to_string_pretty(&serde_json::json!({
            "templateDigest": "sha256:aa",
            "targetClusterId": "TARGET00000000000000000",
            "topicPrefix": "rehearsal-3f2a91c7-",
            "topics": ["orders"],
            "maxPartitions": 200,
            "recordsPerPartition": 25,
            "deadlineSeconds": 3600,
            "modes": ["scratch"],
        }))
        .unwrap(),
    )
    .unwrap();
    p
}

/// The documented command line runs, exits 0, and writes both files.
///
/// It is the block `docs/kubernetes.md` §7g gives an operator, and the review
/// found that block naming `logweir approve` — a subcommand that does not
/// exist. This row is what makes the documented spelling a tested one.
#[test]
fn the_documented_standing_command_line_mints_both_files() {
    let dir = tempfile::tempdir().unwrap();
    let key = write_key(dir.path());
    let scope = standing_scope(dir.path());
    let out = dir.path().join("standing-authorization.json");

    let done = bin()
        .args(["drill", "approve", "--standing", "--key"])
        .arg(&key)
        .args([
            "--schedule-namespace",
            "team-a",
            "--schedule-name",
            "weekly-orders",
            "--schedule-uid",
            "3f2a91c7-1111-4222-8333-444444444444",
        ])
        .arg("--scope")
        .arg(&scope)
        .args(["--valid-days", "30"])
        .arg("--out")
        .arg(&out)
        .output()
        .unwrap();
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&done.stdout),
        String::from_utf8_lossy(&done.stderr)
    );
    assert_eq!(done.status.code(), Some(0), "{transcript}");
    let stdout = String::from_utf8_lossy(&done.stdout);
    assert!(
        stdout.contains(
            "The standing format accepts GovernedApproval only, under\nevery approval policy"
        ),
        "success guidance pins the governed-only standing contract: {stdout}"
    );
    assert!(
        stdout.contains("ConsoleConfirmation authorises no rehearsal"),
        "success guidance says a console key cannot authorize a rehearsal: {stdout}"
    );
    assert!(
        !stdout.contains("GovernedApproval or ConsoleConfirmation"),
        "the superseded authority claim must not return: {stdout}"
    );
    assert!(out.exists(), "the envelope: {transcript}");
    assert!(
        out.with_extension("sig").exists(),
        "and its DERIVED sidecar: {transcript}"
    );

    // Signed under the STANDING payload type, and readable as the document
    // both the controller and the runner parse.
    let sidecar: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.with_extension("sig")).unwrap()).unwrap();
    assert_eq!(
        sidecar["payloadType"].as_str(),
        Some(logweir_core::execution_contract::PAYLOAD_TYPE_STANDING_AUTHORIZATION),
        "a per-run payload type here makes a rehearsal read as a substituted approval"
    );
    let doc: logweir_core::execution_contract::StandingAuthorization =
        serde_json::from_slice(&std::fs::read(&out).unwrap()).expect("the envelope parses");
    assert_eq!(doc.subject_ref.uid, "3f2a91c7-1111-4222-8333-444444444444");
    assert_eq!(doc.scope.deadline_seconds, 3600);

    // And no private material reached either file.
    for written in [out.clone(), out.with_extension("sig")] {
        let bytes = std::fs::read(&written).unwrap();
        assert!(
            !String::from_utf8_lossy(&bytes).contains("PRIVATE"),
            "{}: a signed artifact never carries key material",
            written.display()
        );
    }
}

/// `--subject-kind RehearsalSchedule` WITHOUT `--standing` is refused by name.
///
/// It is the guard in `main.rs`: those bytes would be signed under
/// `PAYLOAD_TYPE_APPROVAL`, and the `Approval` controller refuses an ordinary
/// approval for a `RehearsalSchedule` referent — so the operator would get a
/// signed document, spend a key, and learn nothing until the object was
/// rejected.
#[test]
fn a_rehearsal_schedule_subject_without_standing_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let key = write_key(dir.path());
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, example_spec()).unwrap();
    let out = dir.path().join("approval.json");

    let done = bin()
        .args([
            "drill",
            "approve",
            "--subject-kind",
            "RehearsalSchedule",
            "--spec",
        ])
        .arg(&spec)
        .arg("--key")
        .arg(&key)
        .args(["--approver", "me", "--ticket", "CHG-1"])
        .arg("--out")
        .arg(&out)
        .output()
        .unwrap();
    let transcript = format!(
        "{}{}",
        String::from_utf8_lossy(&done.stdout),
        String::from_utf8_lossy(&done.stderr)
    );
    assert_ne!(done.status.code(), Some(0), "{transcript}");
    assert!(
        transcript.contains("requires --standing"),
        "the refusal says what to do: {transcript}"
    );
    assert!(!out.exists(), "and nothing was signed: {transcript}");
}

/// The rest of the shipped constraints, each over the binary: the ninety-day
/// cap, `--standing` with no `--scope`, `--standing` with an explicit
/// `--subject-kind`, and a PER-RUN approval with no `--approver`.
#[test]
fn the_standing_command_line_refuses_what_the_flags_promise() {
    let dir = tempfile::tempdir().unwrap();
    let key = write_key(dir.path());
    let scope = standing_scope(dir.path());
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, example_spec()).unwrap();

    let standing_base = |out: &Path| -> Vec<String> {
        vec![
            "drill".into(),
            "approve".into(),
            "--standing".into(),
            "--key".into(),
            key.display().to_string(),
            "--schedule-namespace".into(),
            "team-a".into(),
            "--schedule-name".into(),
            "weekly-orders".into(),
            "--schedule-uid".into(),
            "u-1".into(),
            "--out".into(),
            out.display().to_string(),
        ]
    };

    // (a) D3 §4.3's ninety-day cap, refused at minting time.
    let out = dir.path().join("a.json");
    let mut argv = standing_base(&out);
    argv.extend([
        "--scope".to_string(),
        scope.display().to_string(),
        "--valid-days".to_string(),
        "91".to_string(),
    ]);
    let done = bin().args(&argv).output().unwrap();
    let t = format!(
        "{}{}",
        String::from_utf8_lossy(&done.stdout),
        String::from_utf8_lossy(&done.stderr)
    );
    assert_ne!(done.status.code(), Some(0), "{t}");
    assert!(t.contains("90"), "the cap is named: {t}");
    assert!(!out.exists(), "nothing was signed: {t}");

    // (b) `--standing` with no `--scope` names the flag, not a blank path.
    let out = dir.path().join("b.json");
    let done = bin().args(standing_base(&out)).output().unwrap();
    let t = format!(
        "{}{}",
        String::from_utf8_lossy(&done.stdout),
        String::from_utf8_lossy(&done.stderr)
    );
    assert_ne!(done.status.code(), Some(0), "{t}");
    assert!(t.contains("--scope"), "the missing flag is named: {t}");

    // (c) an explicit `--subject-kind` under `--standing` is discarded by the
    //     document (its kind is always RehearsalSchedule), so it is refused
    //     rather than silently ignored.
    let out = dir.path().join("c.json");
    let mut argv = standing_base(&out);
    argv.extend([
        "--scope".to_string(),
        scope.display().to_string(),
        "--subject-kind".to_string(),
        "Backup".to_string(),
    ]);
    let done = bin().args(&argv).output().unwrap();
    let t = format!(
        "{}{}",
        String::from_utf8_lossy(&done.stdout),
        String::from_utf8_lossy(&done.stderr)
    );
    assert_ne!(done.status.code(), Some(0), "{t}");
    assert!(t.contains("--subject-kind"), "{t}");
    assert!(!out.exists(), "nothing was signed: {t}");

    // (d) **THE PER-RUN PATH IS STILL REQUIRED TO NAME A HUMAN.** Fix round 1
    //     made `--approver`/`--ticket` default to "" so `--standing` need not
    //     supply them, which let a drill be approved by nobody under no ticket
    //     — signed bytes that land verbatim in the scorecard.
    let out = dir.path().join("d.json");
    let done = bin()
        .args(["drill", "approve", "--spec"])
        .arg(&spec)
        .arg("--key")
        .arg(&key)
        .arg("--out")
        .arg(&out)
        .output()
        .unwrap();
    let t = format!(
        "{}{}",
        String::from_utf8_lossy(&done.stdout),
        String::from_utf8_lossy(&done.stderr)
    );
    assert_ne!(done.status.code(), Some(0), "{t}");
    assert!(t.contains("--approver"), "the missing flag is named: {t}");
    assert!(!out.exists(), "and no approval was signed by nobody: {t}");
}

/// **The runbook's command line is a TESTED command line.**
///
/// `docs/kubernetes.md` §7g gives an operator one copy-pasteable block for the
/// standing signer, and review 2 found it naming `logweir approve` — a
/// subcommand that does not exist (`error: unrecognized subcommand 'approve'`).
/// No gate caught it: `doc_lint` validates notices, footers, pointers and
/// digests, never a command line.
///
/// This reads the block out of the document and requires (a) that it invokes
/// `logweir drill approve` and (b) that every long flag it uses is one this
/// binary actually accepts, taken from `--help`. A flag renamed here or a
/// command renamed there fails this row.
#[test]
fn the_documented_standing_command_line_uses_flags_this_binary_has() {
    let doc = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/kubernetes.md"),
    )
    .expect("docs/kubernetes.md");

    // The fenced block that starts with the standing invocation.
    // The FENCED occurrence, not the inline mention in the sentence above it.
    let start = doc
        .find("```\nlogweir drill approve --standing")
        .map(|i| i + 4)
        .unwrap_or_else(|| {
            panic!(
                "§7g must show `logweir drill approve --standing`; if it says `logweir approve` \
                 the runbook names a subcommand that does not exist"
            )
        });
    let block: String = doc[start..]
        .split("```")
        .next()
        .expect("the fenced block ends")
        .to_string();
    assert!(
        !block.contains("\nlogweir approve"),
        "the block must not also teach the non-existent spelling: {block}"
    );

    let help = {
        let out = bin().args(["drill", "approve", "--help"]).output().unwrap();
        assert_eq!(out.status.code(), Some(0));
        String::from_utf8(out.stdout).unwrap()
    };

    let mut checked = 0;
    for token in block.split_whitespace() {
        // Long flags only; `$(kubectl … -o jsonpath=…)` contributes short ones
        // and values, which this row deliberately does not police.
        let Some(flag) = token.strip_prefix("--") else {
            continue;
        };
        let flag = flag.trim_end_matches('\\');
        if flag.is_empty() || flag.starts_with('-') {
            continue;
        }
        // The `kubectl` substitution inside the block is not this binary's.
        if ["context", "help"].contains(&flag) {
            continue;
        }
        assert!(
            help.contains(&format!("--{flag}")),
            "the runbook passes `--{flag}`, which `logweir drill approve --help` does not list:\n{help}"
        );
        checked += 1;
    }
    assert!(
        checked >= 7,
        "the documented block should exercise the standing flags; only {checked} were checked"
    );
}

/// **The blank-approver refusal is `mint`'s own, not clap's.**
///
/// `--approver` is `required_unless_present = "standing"`, so the command line
/// cannot omit it — which means a mutant deleting the check inside `mint`
/// survives every subprocess row here. This one calls the library directly,
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
        spec: Some(spec.clone()),
        key: key.clone(),
        approver: approver.to_string(),
        ticket: ticket.to_string(),
        out: dir.path().join("blank.json"),
        subject_kind: "Restore".to_string(),
        standing: None,
    };

    for (approver, ticket, flag) in [
        ("", "CHG-1", "--approver"),
        ("   ", "CHG-1", "--approver"),
        ("me", "", "--ticket"),
        ("me", "\t", "--ticket"),
    ] {
        let error = logweir::approve::mint(&args(approver, ticket))
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
    logweir::approve::mint(&args("operator@example.com", "CHG-42")).expect("the ordinary case");
    assert!(dir.path().join("blank.json").exists());
}
