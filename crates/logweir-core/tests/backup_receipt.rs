//! `BackupReceipt::validate_invariants` has exactly four arms, and each one
//! refuses with an exact message.
//!
//! # Why the messages are asserted in FULL and not by `contains`
//!
//! The refusal text is the interface. `docs/verify_scorecard.py`'s mirrored
//! block (Task 5b) is required to produce the same string byte-for-byte, and
//! `scripts/check-verifier-parity.sh` compares the two readers' refusal TEXT
//! rather than merely that both refused. A `contains("format_version")` here
//! would let one reader say something the other does not and still go green —
//! which is the exact defect the scorecard's parity gate exists because of.
//!
//! # Why there is an aggregate test AND four per-arm tests
//!
//! The four per-arm tests are what a failure should be NAMED after: a
//! reviewer applying "delete arm 2" wants to read `arm_2_…` in the failure
//! list, not scan a table. The aggregate `backup_receipt_invariants_have_
//! exactly_four_arms` is what makes "exactly four" a claim rather than a
//! comment: it walks the same case list, asserts every case is refused with
//! its exact message, asserts the list has four entries, and asserts the
//! pristine receipt is `Ok(())`. Both read from one `arm_cases()`, so the two
//! cannot come apart.
//!
//! Global Constraint 1: an integration-test target, not `logweir-core`
//! source. Pure — no clock, no I/O except the one checked-in fixture read at
//! the bottom, and well inside the per-test budget.

use logweir_core::backup_receipt::{
    BackupReceipt, ReceiptArchive, ReceiptAuth, ReceiptCovered, ReceiptEngine, ReceiptSource,
};
use std::collections::BTreeMap;

/// A receipt that satisfies all four arms. Every case below mutates exactly
/// ONE thing about it, so a refusal is provably about that one thing.
fn pristine() -> BackupReceipt {
    let mut records = BTreeMap::new();
    records.insert("orders".to_string(), 12_u64);
    records.insert("payments".to_string(), 7_u64);
    BackupReceipt {
        format_version: "1.0.0".to_string(),
        run_id: "01J8Z9QK7V6M3F2R5T8W1XB0CD".to_string(),
        backup_id: "logweir-backup-01J8Z9QK7V".to_string(),
        requested_at: "2026-09-09T11:02:14Z".parse().unwrap(),
        started_at: "2026-09-09T11:02:19Z".parse().unwrap(),
        finished_at: "2026-09-09T11:04:46Z".parse().unwrap(),
        exit_code: 0,
        triggered_by: "a-test".to_string(),
        source: ReceiptSource {
            cluster_id: "kRtQ7yZ1S0uPq9AeVxN2Lg".to_string(),
            bootstrap_servers: vec!["kafka-broker-1:9094".to_string()],
            auth: ReceiptAuth {
                mode: "plaintext".to_string(),
                username: None,
            },
            topics: vec!["orders".to_string(), "payments".to_string()],
        },
        engine: ReceiptEngine {
            id: "oso-cli".to_string(),
            version: "v0.21.0".to_string(),
            digest: "sha256:00".to_string(),
        },
        archive: ReceiptArchive {
            manifest_key: "logweir/backups/b/manifest.json".to_string(),
            manifest_sha256: "sha256:11".to_string(),
            prefix: "logweir/backups/b/".to_string(),
        },
        records,
        covered: ReceiptCovered {
            from_ms: 1_757_415_734_000,
            to_ms: 1_757_419_486_000,
        },
    }
}

/// One case per arm: the arm's number, a one-line label, the mutated receipt
/// and the EXACT message it must produce.
fn arm_cases() -> Vec<(u8, &'static str, BackupReceipt, String)> {
    // Arm 1: a major this reader has never seen.
    let mut arm1 = pristine();
    arm1.format_version = "2.0.0".to_string();

    // Arm 2: exit 0 with no manifest named — a successful backup that wrote
    // nothing an auditor can go and look at.
    let mut arm2 = pristine();
    arm2.archive.manifest_key = String::new();

    // Arm 3: a topic counted that the run was never asked to back up.
    let mut arm3 = pristine();
    arm3.records.insert("invoices".to_string(), 1);

    // Arm 4: a window that ends before it begins. The END IS EXCLUSIVE since
    // Task 5b, so `from_ms == to_ms` is refused by the same arm — that
    // direction is `arm_4_refuses_an_empty_window` below.
    let mut arm4 = pristine();
    arm4.covered.from_ms = 2;
    arm4.covered.to_ms = 1;

    vec![
        (
            1,
            "format_version's major is not 1",
            arm1,
            "format_version \"2.0.0\" is not a 1.x version this reader understands".to_string(),
        ),
        (
            2,
            "exit 0 and no manifest disagree",
            arm2,
            "exit_code 0 and manifest_key absent disagree: a receipt names a manifest \
             if and only if the backup exited 0"
                .to_string(),
        ),
        (
            3,
            "records covers a topic the named set does not",
            arm3,
            "records covers {\"invoices\", \"orders\", \"payments\"} but the named topic \
             set is {\"orders\", \"payments\"}"
                .to_string(),
        ),
        (
            4,
            "the covered window ends before it begins",
            arm4,
            "covered.from_ms 2 is not before covered.to_ms 1: the covered window's end is \
             EXCLUSIVE, so an empty range covers no record"
                .to_string(),
        ),
    ]
}

fn assert_arm(n: u8) {
    let cases = arm_cases();
    let (_, label, doc, want) = cases
        .into_iter()
        .find(|(i, ..)| *i == n)
        .unwrap_or_else(|| panic!("arm_cases() has no arm {n}"));
    match doc.validate_invariants() {
        Ok(()) => {
            panic!("arm {n} ({label}) accepted a document it must refuse; expected:\n  {want}")
        }
        Err(got) => assert_eq!(
            got, want,
            "arm {n} ({label}) refused with the wrong message. The refusal TEXT is the \
             interface docs/verify_scorecard.py's mirrored block has to reproduce \
             byte-for-byte; do not reword it, and do not relax this assertion to a \
             `contains`."
        ),
    }
}

#[test]
fn arm_1_refuses_a_format_version_whose_major_is_not_one() {
    assert_arm(1);
}

#[test]
fn arm_2_refuses_an_exit_code_and_manifest_key_that_disagree() {
    assert_arm(2);
}

#[test]
fn arm_3_refuses_records_that_do_not_cover_the_named_topic_set() {
    assert_arm(3);
}

#[test]
fn arm_4_refuses_a_covered_window_that_ends_before_it_begins() {
    assert_arm(4);
}

/// The acceptance criterion, and the test every mutant in Task 5's brief is
/// named against: four arms, each refusing with its exact message, and a
/// pristine receipt accepted.
#[test]
fn backup_receipt_invariants_have_exactly_four_arms() {
    let cases = arm_cases();
    assert_eq!(
        cases.len(),
        4,
        "the invariant has four arms and this test walks all of them; adding an arm \
         without a case here would leave it unasserted, which is how a guard becomes \
         a comment"
    );
    let mut seen = Vec::new();
    for (n, label, doc, want) in cases {
        seen.push(n);
        match doc.validate_invariants() {
            Ok(()) => panic!("arm {n} ({label}) accepted a document it must refuse"),
            Err(got) => assert_eq!(
                got, want,
                "arm {n} ({label}) refused with the wrong message"
            ),
        }
    }
    assert_eq!(
        seen,
        vec![1, 2, 3, 4],
        "the arms are numbered 1..=4, in order"
    );

    // …and the arms are not simply always refusing.
    pristine()
        .validate_invariants()
        .expect("an unmodified receipt must satisfy every arm");
}

#[test]
fn an_unmodified_receipt_satisfies_every_invariant() {
    assert_eq!(pristine().validate_invariants(), Ok(()));
}

/// Arm 2 is a BICONDITIONAL, so the other direction needs its own case: a
/// receipt that names a manifest while reporting a non-zero exit. The
/// aggregate above covers `exit 0 && absent`; this covers `exit != 0 &&
/// present`, and a one-directional implementation passes one and fails the
/// other.
#[test]
fn arm_2_refuses_a_named_manifest_on_a_failed_backup() {
    let mut doc = pristine();
    doc.exit_code = 1;
    assert_eq!(
        doc.validate_invariants(),
        Err(
            "exit_code 1 and manifest_key \"logweir/backups/b/manifest.json\" disagree: \
             a receipt names a manifest if and only if the backup exited 0"
                .to_string()
        )
    );

    // And the legitimate failure receipt — non-zero exit, no manifest — is
    // ACCEPTED. Without this half, an implementation that refused every
    // failed backup's receipt would pass every other assertion here while
    // making the one document a failed backup can produce unwritable.
    let mut failed = pristine();
    failed.exit_code = 1;
    failed.archive.manifest_key = String::new();
    failed
        .validate_invariants()
        .expect("a receipt for a failed backup names no manifest, and that is legal");
}

/// A whitespace-only `manifest_key` is ABSENT, not "a manifest made of
/// spaces" (ruling R-A, the same predicate the scorecard's `partial_reason`
/// uses). `.is_empty()` alone accepts this document at exit 0.
#[test]
fn arm_2_treats_a_blank_manifest_key_as_absent() {
    let mut doc = pristine();
    doc.archive.manifest_key = "   ".to_string();
    assert_eq!(
        doc.validate_invariants(),
        Err(
            "exit_code 0 and manifest_key absent disagree: a receipt names a manifest \
             if and only if the backup exited 0"
                .to_string()
        )
    );
}

/// Arm 1 is checked FIRST, like `Scorecard::refuse_unreadable_major`: a
/// document from a future major is refused before any other arm is evaluated
/// against fields that build may have redefined.
#[test]
fn arm_1_is_evaluated_before_the_other_three() {
    let mut doc = pristine();
    doc.format_version = "2.0.0".to_string();
    // Every other arm is ALSO violated.
    doc.archive.manifest_key = String::new();
    doc.records.insert("invoices".to_string(), 1);
    doc.covered.from_ms = 2;
    doc.covered.to_ms = 1;
    assert_eq!(
        doc.validate_invariants(),
        Err("format_version \"2.0.0\" is not a 1.x version this reader understands".to_string()),
        "a document from an unreadable major must be refused for THAT, not for whichever \
         other arm happens to be evaluated first"
    );
}

/// "Parses as semver" is the whole string, not just the leading component.
/// `Scorecard::major_version` reads the leading component alone, which is
/// right for its arm and wrong for this one: arm 1 claims the version parses.
#[test]
fn arm_1_refuses_a_format_version_that_is_not_three_integers() {
    for bad in ["1", "1.0", "1.0.0.0", "1.0.0-rc1", "v1.0.0", "", "one.0.0"] {
        let mut doc = pristine();
        doc.format_version = bad.to_string();
        assert_eq!(
            doc.validate_invariants(),
            Err(format!(
                "format_version {bad:?} is not a 1.x version this reader understands"
            )),
            "{bad:?} does not parse as three dot-separated integers and must be refused"
        );
    }
    // …and every 1.x.y DOES parse: a MINOR bump adds optional fields only,
    // and a 1.0.0 reader must still read it (Global Constraint 12).
    for good in ["1.0.0", "1.0.9", "1.12.345"] {
        let mut doc = pristine();
        doc.format_version = good.to_string();
        assert_eq!(
            doc.validate_invariants(),
            Ok(()),
            "{good} is a 1.x version this reader must accept"
        );
    }
}

/// Arm 3 refuses the OMISSION as well as the addition: `records` missing a
/// named topic is the same defect from the other side, and a subset check
/// would pass one and fail the other.
#[test]
fn arm_3_refuses_records_that_omit_a_named_topic() {
    let mut doc = pristine();
    doc.records.remove("payments");
    assert_eq!(
        doc.validate_invariants(),
        Err("records covers {\"orders\"} but the named topic set is \
             {\"orders\", \"payments\"}"
            .to_string())
    );
}

/// Arm 4 is `<`, not `<=`, since Task 5b (Task 5's review, F3): `to_ms` is the
/// EXCLUSIVE end of the window, so `from_ms == to_ms` describes a range that
/// contains nothing and cannot be the window of an archive holding a record.
///
/// Task 5 asserted the opposite here — `arm_4_accepts_an_instantaneous_window`
/// — while `config/crd/backups.yaml` already documented the same window's
/// `toMs` as exclusive (I22). This test is that assertion turned round, and it
/// is the mutant guard for the change: reverting the arm to `>` makes it fail
/// at assertion time.
///
/// The single-record backup that motivated the old test is still legal, and
/// still a window: `crates/logweir/src/backup/phase_run.rs` converts the
/// manifest's inclusive newest timestamp to an exclusive bound, so that run
/// produces `[t, t+1)` rather than `[t, t]`.
#[test]
fn arm_4_refuses_an_empty_window() {
    let mut doc = pristine();
    doc.covered.from_ms = 1_757_415_734_000;
    doc.covered.to_ms = 1_757_415_734_000;
    assert_eq!(
        doc.validate_invariants(),
        Err(
            "covered.from_ms 1757415734000 is not before covered.to_ms 1757415734000: the \
             covered window's end is EXCLUSIVE, so an empty range covers no record"
                .to_string()
        ),
        "from_ms == to_ms is an EMPTY half-open range, not an instantaneous window"
    );
    // …and the one-millisecond window a single-record backup really produces
    // is accepted, which is what makes the arm a bound and not a ban.
    let mut ok = pristine();
    ok.covered.from_ms = 1_757_415_734_000;
    ok.covered.to_ms = 1_757_415_734_001;
    ok.validate_invariants()
        .expect("[t, t+1) contains exactly one millisecond and is a real window");
}

/// The checked-in signed fixture is a document THIS type parses and THIS
/// reader accepts.
///
/// It is minted by `crates/logweir-evidence/examples/
/// mint_backup_receipt_fixture.rs`, which validates before it signs — so this
/// test is what catches a hand-edit of the tracked document, the one change
/// that would leave a signed fixture the reader refuses.
#[test]
fn the_checked_in_receipt_fixture_parses_and_satisfies_its_invariants() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../e2e/fixtures/signed/backup-receipt.json"
    );
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let doc: BackupReceipt = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("{path} must parse as a BackupReceipt: {e}"));
    doc.validate_invariants()
        .unwrap_or_else(|e| panic!("{path} must satisfy every invariant: {e}"));
    assert_eq!(
        doc.format_version, "1.0.0",
        "the fixture pins the format version this task ships"
    );
}
