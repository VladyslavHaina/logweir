//! **PROD-16.2: what a run a SECOND PERSON APPROVED IN THE CONSOLE signs,
//! through the runner's own phase sequence, over the orchestrator fixture.**
//! CI-run (no broker, no bucket, no engine binary).
//!
//! `authorization_v2.rs` holds phase 1 (the bundle is verified and the
//! `Approved` it mints names the approver's principal); these rows hold what
//! the phases DO with it: the signed scorecard carries `approval.console`
//! under the first version of its major that defines it (1.9.0, and 2.1.0
//! for a partition subset), an original-name restore says `consoleApproval`
//! in its own block as well, and both readers' lines say who approved and
//! how. Each row has its control: the same run without the block is the
//! document it always was.
//!
//! The signed bytes are also what an OLDER reader is measured over: with
//! `LOGWEIR_WRITE_CONSOLE_APPROVED_SCORECARDS=<dir>` each row writes its
//! scorecard, sidecar and public key there (throwaway keys the fixture
//! mints; no key of any install).

mod fixtures;

use fixtures::Drill;
use logweir::drill::{execute_with, execute_with_approved, phase1_approval};
use logweir_core::original_name::ApprovalSubject;
use logweir_core::scorecard::{
    ConsoleApprovalInfo, ConsolePrincipal, Scorecard, APPROVAL_MODE_CONSOLE,
    FORMAT_VERSION_SUBSET_WITH_CONSOLE_APPROVAL, FORMAT_VERSION_WITH_CONSOLE_APPROVAL,
    FORMAT_VERSION_WITH_ORIGINAL_NAME, FORMAT_VERSION_WITH_PARTITION_SUBSETS,
};

const ISSUER: &str = "https://idp.example";
const CONSOLE_KEY_ID: &str = "c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0";

fn at(text: &str) -> chrono::DateTime<chrono::Utc> {
    text.parse().expect("a fixed instant")
}

/// What phase 1 mints for a bundle approved in the console
/// (`phase1_approval::verify_authorization_v2_bytes`, row
/// `SecondPersonInConsole`): alice asked, bob approved four minutes later,
/// inside the hour the request was good for, and the console key signed both.
fn approved_in_the_console(subject: ApprovalSubject) -> phase1_approval::Approved {
    let approved_at = at("2026-10-10T12:04:00Z");
    let approver = ConsolePrincipal {
        issuer: ISSUER.into(),
        subject: "bob".into(),
    };
    phase1_approval::Approved {
        approval: logweir_core::scorecard::ApprovalInfo {
            approver: approver.principal_id(),
            ticket: "CHG-1042".into(),
            plan_hash: "sha256:00".into(),
            approved_at,
            key_id: CONSOLE_KEY_ID.into(),
            self_attested: false,
            console: Some(ConsoleApprovalInfo {
                mode: APPROVAL_MODE_CONSOLE.into(),
                requester: ConsolePrincipal {
                    issuer: ISSUER.into(),
                    subject: "alice".into(),
                },
                approver,
                requested_at: at("2026-10-10T12:00:00Z"),
                approved_at,
                request_expires_at: at("2026-10-10T13:00:00Z"),
                confirmation_key_id: CONSOLE_KEY_ID.into(),
            }),
        },
        validated_at: chrono::Utc::now(),
        approval_subject: subject,
        approval_mode: phase1_approval::APPROVAL_MODE_CONSOLE,
        original_name_confirmation: None,
    }
}

/// The SIGNED bytes the run uploaded, its sidecar, and the scorecard read
/// back from them: held to every invariant, and to its signature.
fn signed(f: &fixtures::OrchestratorFixture) -> (Vec<u8>, Vec<u8>, Scorecard) {
    let get = |key: String| {
        f.ctx
            .store
            .get_capped(&key, logweir_engine_oso::storage::caps::SIGNED_DOCUMENT)
            .expect("phase 8 uploaded it")
            .0
    };
    let bytes = get(format!("logweir/drills/{}.json", f.run_id));
    let sidecar = get(format!("logweir/drills/{}.sig", f.run_id));
    let sc: Scorecard = serde_json::from_slice(&bytes).expect("a scorecard");
    sc.validate_invariants().expect("the readers accept it");
    (bytes, sidecar, sc)
}

/// Runs THE READER (`logweir drill verify`'s own function) over the signed
/// bytes and returns its report; and, when asked, leaves the three files for
/// an older reader to be measured over.
fn verified_by_the_reader(
    f: &fixtures::OrchestratorFixture,
    name: &str,
) -> logweir::verify::VerifyReport {
    let (bytes, sidecar, _) = signed(f);
    let key = logweir_evidence::keys::SigningKey::from_pem_file(&f.args.signing_key)
        .expect("the fixture's throwaway signing key");
    let dir = tempfile::tempdir().unwrap();
    let write = |dir: &std::path::Path| {
        std::fs::create_dir_all(dir).unwrap();
        let (sc, sig, public) = (
            dir.join(format!("{name}.json")),
            dir.join(format!("{name}.sig")),
            dir.join(format!("{name}.pub.pem")),
        );
        std::fs::write(&sc, &bytes).unwrap();
        std::fs::write(&sig, &sidecar).unwrap();
        fixtures::write_pub(&key, &public);
        (sc, sig, public)
    };
    if let Ok(out) = std::env::var("LOGWEIR_WRITE_CONSOLE_APPROVED_SCORECARDS") {
        write(std::path::Path::new(&out));
    }
    let (sc, sig, public) = write(dir.path());
    match logweir::verify::verify_scorecard(
        &sc,
        &sig,
        &public,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
    ) {
        Ok(logweir::verify::Verdict::Scorecard(report)) => report,
        other => panic!("the reader accepts the signed scorecard: {other:?}"),
    }
}

const THE_LINE: &str = "console approval: mode consoleApproval; requested by \
     https://idp.example#alice at 2026-10-10T12:00:00Z; approved in the console by \
     https://idp.example#bob at 2026-10-10T12:04:00Z (the request expired at \
     2026-10-10T13:00:00Z); the console key \
     c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0 signed the request and the \
     approval, which is expected in this mode: no personal key is involved";

/// **An ordinary restore approved in the console signs who approved, as
/// scorecard 1.9.0, and both of the runner's readers say it.** KILLS: the
/// version step not applied (the document would carry the block under the
/// version it had, which arm CA-1 refuses to sign: exit 4, no scorecard); the
/// block dropped between phase 1 and the signed bytes; a reader that prints
/// the approver and not how they approved.
///
/// NEGATIVE CONTROL: the same drill with its own v1 approval carries no
/// block, keeps the version it always had, and prints no such line.
#[test]
fn a_restore_approved_in_the_console_signs_who_approved_as_1_9_0() {
    let f = fixtures::orchestrator_fixture(Drill::Passes);
    let outcome = execute_with_approved(
        &f.args,
        &f.run_id,
        &f.ctx,
        approved_in_the_console(ApprovalSubject::Ordinary),
    )
    .expect("the drill passes");
    assert_eq!(
        outcome.scorecard.format_version,
        FORMAT_VERSION_WITH_CONSOLE_APPROVAL
    );
    let (_, _, sc) = signed(&f);
    assert_eq!(sc.format_version, "1.9.0");
    let console = sc.approval.console.as_ref().expect("the block is signed");
    assert_eq!(console.mode, "consoleApproval");
    assert_eq!(
        console.requester.principal_id(),
        "https://idp.example#alice"
    );
    assert_eq!(console.approver.principal_id(), "https://idp.example#bob");
    assert_eq!(sc.approval.approver, "https://idp.example#bob");
    assert_eq!(sc.approval.key_id, console.confirmation_key_id);
    assert_eq!(sc.approval.approved_at, console.approved_at);
    assert!(!sc.approval.self_attested);
    assert!(sc.target.original_name.is_none());

    let report = verified_by_the_reader(&f, "console-approved-1.9.0");
    assert_eq!(
        logweir::verify::console_approval_lines(report.console_approval.as_deref()),
        vec![THE_LINE.to_string()]
    );
    let table = logweir::show::render_table(&sc);
    assert!(
        table.contains(&format!("    approval.console          {THE_LINE}\n")),
        "{table}"
    );

    // NEGATIVE CONTROL.
    let f = fixtures::orchestrator_fixture(Drill::Passes);
    let plain = execute_with(&f.args, &f.run_id, &f.ctx).expect("the drill passes");
    let (_, _, sc) = signed(&f);
    assert!(sc.approval.console.is_none());
    assert_eq!(sc.format_version, plain.format_version);
    assert_ne!(sc.format_version, FORMAT_VERSION_WITH_CONSOLE_APPROVAL);
    assert!(
        !logweir_core::scorecard::defines_console_approval(&sc.format_version),
        "a run nobody approved in the console keeps the version it had: {}",
        sc.format_version
    );
    let report = verified_by_the_reader(&f, "control-v1-approval");
    assert!(logweir::verify::console_approval_lines(report.console_approval.as_deref()).is_empty());
    assert!(!logweir::show::render_table(&sc).contains("approval.console"));
}

/// **A PARTITION-SUBSET restore approved in the console is ALLOWED, and it
/// is scorecard 2.1.0**: format 2's fields and the same block. The row's
/// decision (the coordinator's addition 1): a subset is narrower than a whole
/// restore, never more dangerous, so there is no reason to refuse the pair;
/// the block is defined in both lines, and the version step picks the one of
/// the major the selection chose. KILLS: a version step that runs before the
/// selection step only (the document would be 2.0.0 carrying the block, which
/// CA-1 refuses to sign), or that writes 1.9.0 for a subset (PS-1).
///
/// NEGATIVE CONTROL: the same subset under its own v1 approval is the 2.0.0
/// document PROD-11.1b signs.
#[test]
fn a_partition_subset_approved_in_the_console_is_format_2_1_0() {
    let f = fixtures::orchestrator_fixture(Drill::StatesAPartitionSelection);
    execute_with_approved(
        &f.args,
        &f.run_id,
        &f.ctx,
        approved_in_the_console(ApprovalSubject::Ordinary),
    )
    .expect("the subset runs");
    let (_, _, sc) = signed(&f);
    assert_eq!(
        sc.format_version,
        FORMAT_VERSION_SUBSET_WITH_CONSOLE_APPROVAL
    );
    assert_eq!(sc.format_version, "2.1.0");
    assert!(sc
        .source
        .selection
        .as_ref()
        .is_some_and(|s| s.partitions.is_some()));
    assert!(sc.approval.console.is_some());
    let report = verified_by_the_reader(&f, "console-approved-subset-2.1.0");
    assert_eq!(
        logweir::verify::console_approval_lines(report.console_approval.as_deref()),
        vec![THE_LINE.to_string()]
    );

    // NEGATIVE CONTROL.
    let f = fixtures::orchestrator_fixture(Drill::StatesAPartitionSelection);
    execute_with(&f.args, &f.run_id, &f.ctx).expect("the subset runs");
    let (_, _, sc) = signed(&f);
    assert_eq!(sc.format_version, FORMAT_VERSION_WITH_PARTITION_SUBSETS);
    assert!(sc.approval.console.is_none());
}

/// **PROD-15.1 and PROD-16.2 together: an original-name restore approved in
/// the console says so in BOTH places.** `target.original_name.approval_mode`
/// is `consoleApproval` (never `governed`: no personal key countersigned),
/// `approval.console` names who approved, the document is 1.9.0, and no typed
/// names are asked for (a second person approved; OD-10's typing is the
/// one-person confirmation's). KILLS: the original-name block written with
/// `governed` under a console approval (arm CA-8 refuses to sign it); a
/// version left at 1.8.0 with the block (CA-1).
///
/// NEGATIVE CONTROL: the same restore under its own v1 approval is the 1.8.0
/// document PROD-15.1 signs.
#[test]
fn an_original_name_restore_approved_in_the_console_says_so_in_both_places() {
    let f = fixtures::orchestrator_fixture(Drill::RestoresUnderTheOriginalNames);
    execute_with_approved(
        &f.args,
        &f.run_id,
        &f.ctx,
        approved_in_the_console(ApprovalSubject::OriginalName),
    )
    .expect("the original-name restore runs");
    let (_, _, sc) = signed(&f);
    assert_eq!(sc.format_version, "1.9.0");
    let on = sc.target.original_name.as_ref().expect("the block");
    assert_eq!(on.approval_subject, "originalName");
    assert_eq!(on.approval_mode, "consoleApproval");
    assert_eq!(on.confirmation, None, "a second person approved");
    assert!(sc.approval.console.is_some());
    let report = verified_by_the_reader(&f, "console-approved-original-name-1.9.0");
    assert_eq!(
        logweir::verify::console_approval_lines(report.console_approval.as_deref()),
        vec![THE_LINE.to_string()]
    );
    assert!(report.original_name.is_some());

    // NEGATIVE CONTROL.
    let f = fixtures::orchestrator_fixture(Drill::RestoresUnderTheOriginalNames);
    execute_with(&f.args, &f.run_id, &f.ctx).expect("the original-name restore runs");
    let (_, _, sc) = signed(&f);
    assert_eq!(sc.format_version, FORMAT_VERSION_WITH_ORIGINAL_NAME);
    assert_eq!(
        sc.target.original_name.as_ref().unwrap().approval_mode,
        "v1Approval"
    );
    assert!(sc.approval.console.is_none());
}

/// **A console approval under the ORDINARY subject never reaches the
/// creation step of an original-name plan** — the subject is the signed
/// document's, and the second person approving an ordinary restore did not
/// approve writing under the original names (PROD-15.1's rule, unchanged by
/// the way the approval was given). KILLS: a console route that skips
/// `check_original_name_subject`.
#[test]
fn a_console_approval_of_the_ordinary_subject_does_not_run_an_original_name_plan() {
    let f = fixtures::orchestrator_fixture(Drill::RestoresUnderTheOriginalNames);
    let e = execute_with_approved(
        &f.args,
        &f.run_id,
        &f.ctx,
        approved_in_the_console(ApprovalSubject::Ordinary),
    )
    .expect_err("the ordinary subject does not authorise original names");
    assert_eq!(e.exit_code(), logweir::exit::ExitCode::GuardRefused, "{e}");
    assert!(
        e.to_string()
            .contains(logweir_core::original_name::APPROVAL_SUBJECT_MISMATCH),
        "{e}"
    );
    assert!(fixtures::created_topics(&f).is_empty());
}
