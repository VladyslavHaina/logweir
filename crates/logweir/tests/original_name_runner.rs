//! **PROD-15.1 review M3 and L5: the RUNNER's own hold of an original-name
//! restore's approval, through its call sites, over the orchestrator
//! fixture.** CI-run (no broker, no bucket, no engine binary).
//!
//! The function `check_original_name_subject` has its own unit rows; these
//! rows exist because removing its CALL SITES survived every CI suite in the
//! review, and live an ordinary approval then wrote under the original name
//! before signing refused (exit 4). Each row below fails when one call site
//! is gone:
//!
//! - after phase 1, on the path-based `execute_with`
//!   ([`an_ordinary_v1_approval_never_reaches_the_creation_step`]);
//! - before phase 0, on a pre-validated approval
//!   ([`a_prevalidated_ordinary_approval_is_refused_before_phase_0`]);
//! - at startup, before any runner input is read: `original_name_cli.rs`.
//!
//! The pass row is also the one that proves the signed block is WRITTEN
//! (review L5, R21).

mod fixtures;

use fixtures::Drill;
use logweir::drill::{execute_with, execute_with_approved, phase1_approval, DrillError};
use logweir::exit::ExitCode;
use logweir_core::original_name::{
    ApprovalSubject, OriginalNameConfirmation, APPROVAL_SUBJECT_MISMATCH,
    ORIGINAL_NAME_CONFIRMATION_MISMATCH, ORIGINAL_NAME_CONFIRMATION_MISSING,
};

fn guard_message(e: DrillError) -> String {
    assert_eq!(e.exit_code(), ExitCode::GuardRefused, "{e}");
    e.to_string()
}

/// The SIGNED bytes the run wrote (`--out`), read back and held to every
/// invariant both readers enforce, ON-1 to ON-12 included.
fn signed_document_is_accepted(f: &fixtures::OrchestratorFixture) {
    let bytes = std::fs::read(&f.out).expect("the run wrote its scorecard");
    let sc: logweir_core::scorecard::Scorecard =
        serde_json::from_slice(&bytes).expect("a scorecard");
    sc.validate_invariants().expect("the readers accept it");
    assert!(sc.target.original_name.is_some());
}

fn approved(subject: ApprovalSubject, mode: &'static str) -> phase1_approval::Approved {
    phase1_approval::Approved {
        approval: logweir_core::scorecard::ApprovalInfo {
            approver: "requester@example.com".into(),
            ticket: String::new(),
            plan_hash: "sha256:00".into(),
            approved_at: chrono::Utc::now(),
            key_id: "k".into(),
            self_attested: false,
        },
        validated_at: chrono::Utc::now(),
        approval_subject: subject,
        approval_mode: mode,
        original_name_confirmation: None,
    }
}

/// The plan the original-name fixture restores, approved with the separate
/// subject: it runs to a pass, creates `orders` under its own name, and SIGNS
/// `target.original_name` (version at least 1.8.0). KILLS (review L5, R21):
/// never writing the block; writing it without the subject or the condition.
#[test]
fn an_original_name_restore_runs_and_signs_its_block() {
    let f = fixtures::orchestrator_fixture(Drill::RestoresUnderTheOriginalNames);
    let sc = execute_with(&f.args, &f.run_id, &f.ctx).expect("the original-name drill passes");
    let created: Vec<String> = fixtures::created_topics(&f)
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert_eq!(created, vec!["orders".to_string()]);
    let on = sc
        .target
        .original_name
        .as_ref()
        .expect("the signed document carries target.original_name");
    assert_eq!(on.approval_subject, "originalName");
    assert_eq!(on.approval_mode, "v1Approval");
    assert_eq!(on.cluster_condition, "autoCreateDisabled");
    assert_eq!(on.owner_detection, vec!["plan".to_string()]);
    assert_eq!(on.confirmation, None, "a v1 approval has a second person");
    // At least the version that defines the block, never pinned exactly.
    let minor = sc
        .format_version
        .split('.')
        .nth(1)
        .and_then(|m| m.parse::<u64>().ok());
    assert!(
        sc.format_version.starts_with("1.") && minor.is_some_and(|m| m >= 8),
        "{}",
        sc.format_version
    );
    assert_eq!(sc.target.topic_mapping_prefix, "");
    signed_document_is_accepted(&f);
}

/// Review M3, the call site AFTER PHASE 1: an ordinary v1 approval for an
/// original-name plan is refused by name once phase 1 has verified it, and
/// NOTHING is created. KILLS: removing that call site (the run would create
/// `orders`, restore into it, and stop only at signing, exit 4).
#[test]
fn an_ordinary_v1_approval_never_reaches_the_creation_step() {
    let f =
        fixtures::orchestrator_fixture(Drill::RestoresUnderTheOriginalNamesWithAnOrdinaryApproval);
    let e = execute_with(&f.args, &f.run_id, &f.ctx).expect_err("an ordinary approval");
    let message = guard_message(e);
    assert!(message.contains(APPROVAL_SUBJECT_MISMATCH), "{message}");
    assert!(
        fixtures::created_topics(&f).is_empty(),
        "an ordinary approval created {:?}",
        fixtures::created_topics(&f)
    );
}

/// Review M3, the call site BEFORE PHASE 0 on a pre-validated approval: the
/// plan here would ALSO be refused at phase 0 (its owner statement is removed,
/// `OriginalNameOwnerNotChecked`), so the refusal this row reads proves which
/// check ran first. KILLS: removing that call site (phase 0's refusal would
/// answer instead).
#[test]
fn a_prevalidated_ordinary_approval_is_refused_before_phase_0() {
    let mut f = fixtures::orchestrator_fixture(Drill::RestoresUnderTheOriginalNames);
    if let Some(block) = f
        .ctx
        .spec
        .target
        .topic_naming
        .as_mut()
        .and_then(|n| n.original_name.as_mut())
    {
        block.owners = None;
    }
    let e = execute_with_approved(
        &f.args,
        &f.run_id,
        &f.ctx,
        approved(ApprovalSubject::Ordinary, phase1_approval::APPROVAL_MODE_V1),
    )
    .expect_err("an ordinary approval");
    let message = guard_message(e);
    assert!(message.contains(APPROVAL_SUBJECT_MISMATCH), "{message}");
    assert!(fixtures::created_topics(&f).is_empty());
}

/// OD-10, the runner's half through its call sites: a one-person
/// confirmation (an authorization document v2 under `Ordinary`) of an
/// original-name plan runs only with every original topic name typed —
/// missing or mistyped is refused by name before phase 0 — and the signed
/// block says it was confirmed with the names typed. KILLS: a runner that
/// admits a one-person confirmation without the typed names; a document that
/// does not record how it was confirmed.
#[test]
fn a_one_person_confirmation_runs_only_with_the_names_typed_and_says_so() {
    let ordinary = phase1_approval::APPROVAL_MODE_ORDINARY;
    let typed = |names: &[&str]| {
        let mut a = approved(ApprovalSubject::OriginalName, ordinary);
        a.original_name_confirmation = Some(OriginalNameConfirmation {
            typed_topics: names.iter().map(|n| (*n).to_string()).collect(),
        });
        a
    };
    let f = fixtures::orchestrator_fixture(Drill::RestoresUnderTheOriginalNames);
    let message = guard_message(
        execute_with_approved(
            &f.args,
            &f.run_id,
            &f.ctx,
            approved(ApprovalSubject::OriginalName, ordinary),
        )
        .expect_err("no typed names"),
    );
    assert!(
        message.contains(ORIGINAL_NAME_CONFIRMATION_MISSING),
        "{message}"
    );
    assert!(fixtures::created_topics(&f).is_empty());

    let f = fixtures::orchestrator_fixture(Drill::RestoresUnderTheOriginalNames);
    let message = guard_message(
        execute_with_approved(&f.args, &f.run_id, &f.ctx, typed(&["Orders"]))
            .expect_err("a mistyped name"),
    );
    assert!(
        message.contains(ORIGINAL_NAME_CONFIRMATION_MISMATCH),
        "{message}"
    );
    assert!(fixtures::created_topics(&f).is_empty());

    let f = fixtures::orchestrator_fixture(Drill::RestoresUnderTheOriginalNames);
    let outcome = execute_with_approved(&f.args, &f.run_id, &f.ctx, typed(&["orders"]))
        .expect("typed exactly, the confirmation runs");
    let on = outcome
        .scorecard
        .target
        .original_name
        .as_ref()
        .expect("the block is signed");
    assert_eq!(on.approval_mode, "ordinary");
    assert_eq!(
        on.confirmation.as_deref(),
        Some(logweir_core::original_name::CONFIRMATION_TYPED_TOPIC_NAMES)
    );
    signed_document_is_accepted(&f);
}
