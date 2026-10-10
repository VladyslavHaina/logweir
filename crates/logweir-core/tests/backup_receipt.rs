//! `BackupReceipt::validate_invariants` has exactly FORTY arms — the four
//! SELF-CONTRADICTION invariants and, since Task 5b fix round 1, the one
//! CLOSED VALUE SET (`source.auth.mode`), plus since FX-4 the six arms (6-11)
//! that read ONLY the 1.1.0 `config_coverage` block, plus since PROD-05.1 the
//! eight (12-19) that run only on the 1.3.0 `topic_configuration` block and
//! the two (20-21) over its `owner_detection`, plus since PROD-03.0 the eight
//! (22-29) that run only on the 1.5.0 `schema_dependency` block, plus since
//! PROD-04.1 the six (30-35) that run only on the 1.7.0 `consumer_positions`
//! block, and since PROD-01.4a the five (36-40) that read only the 1.6.0
//! `generations` block — and each one refuses with an exact message. The
//! positions document the `consumer_positions` block binds has its own fourteen
//! (CP-1 to CP-14, `validate_consumer_positions_document`), asserted here the
//! same way.
//!
//! The four and the five are asserted SEPARATELY and on purpose:
//! `arm_cases()` carries the four self-contradiction arms and
//! `backup_receipt_refuses_each_self_contradiction_arm_with_its_exact_message` closes over them, while
//! `validate_invariants_has_exactly_forty_two_return_err_statements` closes over
//! the function's TOTAL by reading its source text. So an arm added to the
//! function without a case here fails the second test, and a case deleted
//! from `arm_cases()` fails the first — neither number can go stale under
//! cover of the other.
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
//! # Why there is an aggregate test AND a per-arm test for every arm
//!
//! The per-arm tests are what a failure should be NAMED after: a
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
    BackupReceipt, ConfigCoverage, ConfigEntry, EffectiveConfigValue, ReceiptArchive, ReceiptAuth,
    ReceiptCovered, ReceiptEngine, ReceiptSource, SourceConfigCoverage, TopicConfigCoverage,
    TopicConfiguration, TopicIdentity, TopicOwner,
};
use logweir_core::consumer_positions::{
    DocumentRef, GroupSnapshot, PartitionFacts, PositionCounts, PositionEntry, PositionsDocument,
};
use std::collections::BTreeMap;

/// A receipt that satisfies all five arms. Every case below mutates exactly
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
            manifest_version_id: None,
            prefix: "logweir/backups/b/".to_string(),
        },
        records,
        covered: ReceiptCovered {
            from_ms: 1_757_415_734_000,
            to_ms: 1_757_419_486_000,
        },
        config_coverage: None,
        topic_configuration: None,
        owner_detection: None,
        schema_dependency: None,
        generations: None,
        consumer_positions: None,
    }
}

/// One case per SELF-CONTRADICTION arm (1..=4): the arm's number, a one-line
/// label, the mutated receipt and the EXACT message it must produce.
///
/// Arm 5 is deliberately NOT here. It is the only arm that is not a claim the
/// document makes against itself — it refuses a value the format has no
/// spelling for — and it has its own case and its own named test
/// (`arm_5_refuses_an_auth_mode_outside_the_closed_two`) so that
/// `backup_receipt_refuses_each_self_contradiction_arm_with_its_exact_message` keeps saying exactly
/// what its name says while
/// `validate_invariants_has_exactly_forty_two_return_err_statements` pins the
/// total.
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

/// **ARM 5 — the closed value set.** `source.auth.mode` is `"plaintext"` or
/// `"scramSha512"` and nothing else (controller ruling, Task 5b fix round 1).
///
/// Three cases, because the arm has three distinct jobs. `scram-sha-512` is
/// the LEGACY spelling this product used to write and now refuses, which is
/// the whole point of closing the set: the value Task 17 copies into
/// `Backup.status.auth.mode` must be the one that field's CRD description
/// promises. `totally-made-up` is the value Task 5b's review signed and
/// verified 0/0 at both readers, i.e. the defect this arm removes. And the
/// two accepted values are asserted too, so an arm that refused everything
/// could not pass this test.
#[test]
fn arm_5_refuses_an_auth_mode_outside_the_closed_two() {
    for (mode, want) in [
        (
            "scram-sha-512",
            "source.auth.mode \"scram-sha-512\" is not one of the two values this format \
             defines: \"plaintext\" or \"scramSha512\"",
        ),
        (
            "totally-made-up",
            "source.auth.mode \"totally-made-up\" is not one of the two values this format \
             defines: \"plaintext\" or \"scramSha512\"",
        ),
        (
            " plaintext ",
            "source.auth.mode \" plaintext \" is not one of the two values this format \
             defines: \"plaintext\" or \"scramSha512\"",
        ),
    ] {
        let mut doc = pristine();
        doc.source.auth.mode = mode.to_string();
        match doc.validate_invariants() {
            Ok(()) => panic!("arm 5 accepted source.auth.mode {mode:?}"),
            Err(got) => assert_eq!(
                got, want,
                "arm 5 refused {mode:?} with the wrong message. The refusal TEXT is the \
                 interface docs/verify_scorecard.py's mirrored arm reproduces byte for \
                 byte, and e2e/fixtures/invariants/backup-receipt-index.json records it \
                 verbatim; do not reword it."
            ),
        }
    }
    // …and the two values the format DOES define are accepted, so this arm
    // cannot pass by refusing every mode.
    for mode in ["plaintext", "scramSha512"] {
        let mut doc = pristine();
        doc.source.auth.mode = mode.to_string();
        doc.source.auth.username = match mode {
            "plaintext" => None,
            _ => Some("logweir-backup".to_string()),
        };
        assert_eq!(
            doc.validate_invariants(),
            Ok(()),
            "{mode:?} is one of the two values this format defines and must be accepted"
        );
    }
}

/// **PROD-01.3, arms 5b and 5c.** The three modes PROD-01.3 adds are values of
/// format 1.4.0 and later: under an older minor they are refused as a value
/// no writer of that version could have produced (5b), from 1.4.0 they are
/// accepted, and the closed set there is five (5c). Below 1.4.0 an unknown
/// value is still refused by arm 5a with its unchanged message — which is
/// exactly what an older reader says about a 1.4.0 receipt naming a new mode.
#[test]
fn arm_5_is_versioned_by_the_prod_01_3_modes() {
    for mode in ["scramSha256", "plain", "mtls"] {
        for version in ["1.0.0", "1.1.0", "1.2.0", "1.3.0"] {
            let mut doc = pristine();
            doc.format_version = version.to_string();
            doc.source.auth.mode = mode.to_string();
            assert_eq!(
                doc.validate_invariants(),
                Err(format!(
                    "source.auth.mode {mode:?} is defined from 1.4.0 and format_version \
                     {version:?} predates it"
                )),
                "arm 5b must refuse {mode} under {version}"
            );
        }
        for version in ["1.4.0", "1.4.2", "1.12.0"] {
            let mut doc = pristine();
            doc.format_version = version.to_string();
            doc.source.auth.mode = mode.to_string();
            doc.source.auth.username = (mode != "mtls").then(|| "logweir".to_string());
            assert_eq!(
                doc.validate_invariants(),
                Ok(()),
                "{mode} under {version} is one of the five values and must be accepted"
            );
        }
    }
    // The two original values stay accepted under 1.4.0.
    for mode in ["plaintext", "scramSha512"] {
        let mut doc = pristine();
        doc.format_version = "1.4.0".to_string();
        doc.source.auth.mode = mode.to_string();
        assert_eq!(doc.validate_invariants(), Ok(()), "{mode}");
    }
    // Arm 5c: the closed five, from 1.4.0.
    for mode in ["scram-sha-256", "PLAIN", "oauthbearer", ""] {
        let mut doc = pristine();
        doc.format_version = "1.4.0".to_string();
        doc.source.auth.mode = mode.to_string();
        assert_eq!(
            doc.validate_invariants(),
            Err(format!(
                "source.auth.mode {mode:?} is not one of the five values this format defines: \
                 \"plaintext\", \"scramSha512\", \"scramSha256\", \"plain\" or \"mtls\""
            )),
            "arm 5c must refuse {mode:?}"
        );
    }
}

/// The written version follows the mode: a PROD-01.3 mode is 1.4.0 whatever
/// the archive pins and whether or not the receipt carries PROD-05.1's
/// `topic_configuration` (1.4.0 defines both); the two original modes keep the
/// documents they always were (1.1.0, 1.2.0 pinned, 1.3.0 with the topic
/// configuration), so no receipt a plaintext or SCRAM-SHA-512 backup writes
/// changes by a byte.
#[test]
fn the_written_version_follows_the_auth_mode() {
    use logweir_core::backup_receipt::{
        format_version_for, FORMAT_VERSION_WITH_AUTH_MODES, FORMAT_VERSION_WITH_MANIFEST_VERSION,
        FORMAT_VERSION_WITH_TOPIC_CONFIGURATION, RECEIPT_FORMAT_VERSION,
    };
    let doc = pristine();
    let mut pinned = doc.archive.clone();
    pinned.manifest_version_id = Some("v1".to_string());
    for mode in ["plaintext", "scramSha512"] {
        let auth = ReceiptAuth {
            mode: mode.to_string(),
            username: None,
        };
        assert_eq!(
            format_version_for(&doc.archive, false, false, &auth, false, false),
            RECEIPT_FORMAT_VERSION
        );
        assert_eq!(
            format_version_for(&pinned, false, false, &auth, false, false),
            FORMAT_VERSION_WITH_MANIFEST_VERSION
        );
        for archive in [&doc.archive, &pinned] {
            assert_eq!(
                format_version_for(archive, true, false, &auth, false, false),
                FORMAT_VERSION_WITH_TOPIC_CONFIGURATION
            );
        }
    }
    for mode in ["scramSha256", "plain", "mtls"] {
        let auth = ReceiptAuth {
            mode: mode.to_string(),
            username: None,
        };
        for archive in [&doc.archive, &pinned] {
            for topic_configuration in [false, true] {
                assert_eq!(
                    format_version_for(archive, topic_configuration, false, &auth, false, false),
                    FORMAT_VERSION_WITH_AUTH_MODES,
                    "{mode}, topic_configuration={topic_configuration}"
                );
            }
        }
    }
    assert_eq!(FORMAT_VERSION_WITH_AUTH_MODES, "1.4.0");
    assert_eq!(
        logweir_core::backup_receipt::AUTH_MODES_SINCE_MINOR,
        4,
        "the constant and the version move together"
    );
}

/// **THE TOTAL.** `validate_invariants` has exactly thirty-six refusing statements
/// (arm 5 is three statements, so the twenty-nine arms through PROD-03.0's are
/// thirty-one, PROD-04.1's six (30-35) make thirty-seven, and PROD-01.4a's five
/// (36-40) make forty-two).
///
/// Read out of the SOURCE TEXT, which is the only way to make the count a
/// claim about the function rather than about this file's case list: an arm
/// added without a case, or a sixth arm added without updating this number,
/// fails here. It is the same slice
/// `scripts/check-invariant-corpus.sh` takes — from the signature line to the
/// first line that is exactly four spaces and a closing brace — so the two
/// gates cannot disagree about where the function ends, and comments are
/// required not to inflate the count (the same rule
/// `crates/logweir/tests/two_reader_parity.rs::
/// every_invariant_arm_has_a_corpus_case` applies to the scorecard's arms).
#[test]
fn validate_invariants_has_exactly_forty_two_return_err_statements() {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/backup_receipt.rs"
    ))
    .expect("read crates/logweir-core/src/backup_receipt.rs");
    let lines: Vec<&str> = src.lines().collect();
    let start = lines
        .iter()
        .position(|l| l.contains("pub fn validate_invariants"))
        .expect("backup_receipt.rs declares validate_invariants");
    let end = start
        + 1
        + lines[start + 1..]
            .iter()
            .position(|l| *l == "    }")
            .expect("the function closes on a line that is exactly four spaces and a brace");
    let body = lines[start..=end].join("\n");

    let total = body.matches("return Err(format!(").count();
    assert_eq!(
        total, 42,
        "BackupReceipt::validate_invariants has {total} `return Err(format!(` \
         statement(s), not 42. Every one of them needs a per-arm test in this file with \
         its exact message AND a case in \
         e2e/fixtures/invariants/backup-receipt-index.json — \
         scripts/check-invariant-corpus.sh derives the list from this same slice and \
         from docs/verify_scorecard.py and refuses to balance otherwise."
    );
    // A COMMENT quoting the marker would inflate the count, which is how a
    // deleted arm hides behind prose.
    let code_only: String = body
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        code_only.matches("return Err(format!(").count(),
        total,
        "a COMMENT in validate_invariants contains `return Err(format!(`, so the \
         statement count above is inflated by prose"
    );
}

/// The acceptance criterion, and the test every mutant in Task 5's brief is
/// named against: the four SELF-CONTRADICTION arms, each refusing with its
/// exact message, and a pristine receipt accepted. Arm 5 — the closed value
/// set — is `arm_5_refuses_an_auth_mode_outside_the_closed_two`, and the
/// function's total is
/// `validate_invariants_has_exactly_forty_two_return_err_statements`.
///
/// **RENAMED, Task 12 closeout carry (c).** It was
/// `backup_receipt_invariants_have_exactly_four_arms`, which Task 5b's fix
/// round made false: the invariant gained a fifth arm and the name went on
/// claiming there were four. The body was always exact and was always scoped
/// to the four self-contradiction arms in writing; only the name said
/// otherwise, and a name is what a reviewer reads first.
#[test]
fn backup_receipt_refuses_each_self_contradiction_arm_with_its_exact_message() {
    let cases = arm_cases();
    assert_eq!(
        cases.len(),
        4,
        "this test walks the four self-contradiction arms (arm 5, the auth-mode enum, has \
         its own test); adding an arm \
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
///
/// **RENAMED, Task 12 closeout carry (c).** It was
/// `arm_1_is_evaluated_before_the_other_three`, a name that predated arm 5 and
/// counted one arm too few. The property asserted was always the general one —
/// before EVERY other arm — and the body below violates all four of them, arm
/// 5 included; the name now says so.
#[test]
fn arm_1_is_evaluated_before_every_other_arm() {
    let mut doc = pristine();
    doc.format_version = "2.0.0".to_string();
    // Every other arm is ALSO violated.
    doc.archive.manifest_key = String::new();
    doc.records.insert("invoices".to_string(), 1);
    doc.covered.from_ms = 2;
    doc.covered.to_ms = 1;
    doc.source.auth.mode = "scram-sha-512".to_string();
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
        "the signed fixture is a 1.0.0 receipt and stays one after FX-4's 1.1.0: old \
         evidence verifies unchanged, and ruling R-G reserves the fixture re-mint"
    );
    assert_eq!(
        doc.config_coverage, None,
        "a 1.0.0 receipt carries no config_coverage"
    );
    assert_eq!(
        SourceConfigCoverage::from_receipt(&doc).of("orders"),
        ConfigCoverage::Unknown,
        "the 1.0.0 fixture must read as coverage UNKNOWN, never captured"
    );
}

// ---------------------------------------------------------------------------
// FX-4: format 1.1.0's `config_coverage` block and arms 6-11
// ---------------------------------------------------------------------------

fn coverage(value: &str, reason: Option<&str>, ts: Option<(&str, &str)>) -> TopicConfigCoverage {
    TopicConfigCoverage {
        coverage: value.to_string(),
        reason: reason.map(str::to_string),
        timestamp_type: ts.map(|(value, source)| EffectiveConfigValue {
            value: value.to_string(),
            source: source.to_string(),
        }),
    }
}

/// A 1.1.0 receipt that satisfies all eleven arms: `orders` captured with a
/// broker-default `LogAppendTime`, `payments` captured with a topic-override
/// `CreateTime`.
fn pristine_1_1() -> BackupReceipt {
    let mut doc = pristine();
    doc.format_version = "1.1.0".to_string();
    let mut block = BTreeMap::new();
    block.insert(
        "orders".to_string(),
        coverage(
            "captured",
            None,
            Some(("LogAppendTime", "dynamicDefaultBrokerConfig")),
        ),
    );
    block.insert(
        "payments".to_string(),
        coverage("captured", None, Some(("CreateTime", "dynamicTopicConfig"))),
    );
    doc.config_coverage = Some(block);
    doc
}

fn refused_with(doc: &BackupReceipt, want: &str, what: &str) {
    match doc.validate_invariants() {
        Ok(()) => panic!("{what}: accepted a document it must refuse; expected:\n  {want}"),
        Err(got) => assert_eq!(
            got, want,
            "{what}: refused with the wrong message. The refusal TEXT is the interface \
             docs/verify_scorecard.py's mirrored arm reproduces byte for byte and \
             e2e/fixtures/invariants/backup-receipt-index.json records verbatim; do not \
             reword it, and do not relax this to a `contains`."
        ),
    }
}

#[test]
fn a_1_1_0_receipt_with_every_coverage_kind_satisfies_every_invariant() {
    let mut doc = pristine_1_1();
    assert_eq!(doc.validate_invariants(), Ok(()));
    // Every legal shape of an entry, on one document: a denied read (no
    // timestamp type), a failed read (none either), and a read whose manifest
    // disagreed (the timestamp type WAS observed).
    doc.source.topics = vec![
        "a-denied".into(),
        "b-failed".into(),
        "c-differs".into(),
        "orders".into(),
    ];
    doc.records = doc
        .source
        .topics
        .iter()
        .map(|t| (t.clone(), 1_u64))
        .collect();
    let block = doc.config_coverage.as_mut().unwrap();
    block.remove("payments");
    block.insert("a-denied".into(), coverage("captureDenied", None, None));
    block.insert(
        "b-failed".into(),
        coverage("notCaptured", Some("describeFailed"), None),
    );
    block.insert(
        "c-differs".into(),
        coverage(
            "notCaptured",
            Some("manifestDiffers"),
            Some(("CreateTime", "defaultConfig")),
        ),
    );
    assert_eq!(doc.validate_invariants(), Ok(()));
}

/// Absent is UNKNOWN, and a 1.1.0 receipt may omit the block: that is the
/// weaker claim, never a contradiction.
#[test]
fn a_receipt_without_config_coverage_is_accepted_at_1_0_0_and_at_1_1_0() {
    assert_eq!(pristine().validate_invariants(), Ok(()));
    let mut doc = pristine_1_1();
    doc.config_coverage = None;
    assert_eq!(doc.validate_invariants(), Ok(()));
}

#[test]
fn arm_6_refuses_config_coverage_under_a_1_0_x_format_version() {
    for version in ["1.0.0", "1.0.7"] {
        let mut doc = pristine_1_1();
        doc.format_version = version.to_string();
        refused_with(
            &doc,
            &format!(
                "config_coverage is present but format_version \"{version}\" predates it: the \
                 field is defined from 1.1.0"
            ),
            "arm 6",
        );
    }
    // A later minor carries it too.
    let mut doc = pristine_1_1();
    doc.format_version = "1.2.0".to_string();
    assert_eq!(doc.validate_invariants(), Ok(()));
}

#[test]
fn arm_7_refuses_config_coverage_that_does_not_cover_the_named_topic_set() {
    let mut doc = pristine_1_1();
    doc.config_coverage.as_mut().unwrap().remove("payments");
    refused_with(
        &doc,
        "config_coverage covers {\"orders\"} but the named topic set is {\"orders\", \"payments\"}",
        "arm 7 (a named topic missing)",
    );
    let mut doc = pristine_1_1();
    doc.config_coverage
        .as_mut()
        .unwrap()
        .insert("invoices".into(), coverage("captured", None, None));
    refused_with(
        &doc,
        "config_coverage covers {\"invoices\", \"orders\", \"payments\"} but the named topic set \
         is {\"orders\", \"payments\"}",
        "arm 7 (an unnamed topic present)",
    );
}

#[test]
fn arm_8_refuses_a_coverage_outside_the_closed_three() {
    for value in ["unknown", "Captured", "notAssessed", ""] {
        let mut doc = pristine_1_1();
        doc.config_coverage
            .as_mut()
            .unwrap()
            .get_mut("orders")
            .unwrap()
            .coverage = value.to_string();
        refused_with(
            &doc,
            &format!(
                "config_coverage[\"orders\"].coverage \"{value}\" is not one of the three values \
                 this format defines: \"captured\", \"notCaptured\" or \"captureDenied\""
            ),
            "arm 8",
        );
    }
}

#[test]
fn arm_9_refuses_a_reason_that_does_not_fit_the_coverage() {
    // notCaptured with no reason.
    let mut doc = pristine_1_1();
    *doc.config_coverage
        .as_mut()
        .unwrap()
        .get_mut("orders")
        .unwrap() = coverage("notCaptured", None, None);
    refused_with(
        &doc,
        "config_coverage[\"orders\"].reason absent does not fit coverage \"notCaptured\": a \
         reason is present exactly when coverage is \"notCaptured\", and is \"describeFailed\" \
         or \"manifestDiffers\"",
        "arm 9 (notCaptured without a reason)",
    );
    // A reason beside `captured`.
    let mut doc = pristine_1_1();
    doc.config_coverage
        .as_mut()
        .unwrap()
        .get_mut("orders")
        .unwrap()
        .reason = Some("manifestDiffers".into());
    refused_with(
        &doc,
        "config_coverage[\"orders\"].reason \"manifestDiffers\" does not fit coverage \
         \"captured\": a reason is present exactly when coverage is \"notCaptured\", and is \
         \"describeFailed\" or \"manifestDiffers\"",
        "arm 9 (captured with a reason)",
    );
    // A reason outside the closed two.
    let mut doc = pristine_1_1();
    *doc.config_coverage
        .as_mut()
        .unwrap()
        .get_mut("orders")
        .unwrap() = coverage("notCaptured", Some("timeout"), None);
    refused_with(
        &doc,
        "config_coverage[\"orders\"].reason \"timeout\" does not fit coverage \"notCaptured\": a \
         reason is present exactly when coverage is \"notCaptured\", and is \"describeFailed\" \
         or \"manifestDiffers\"",
        "arm 9 (an unknown reason)",
    );
}

#[test]
fn arm_10_refuses_a_timestamp_type_observed_by_a_read_that_did_not_succeed() {
    for entry in [
        coverage("captureDenied", None, Some(("CreateTime", "defaultConfig"))),
        coverage(
            "notCaptured",
            Some("describeFailed"),
            Some(("LogAppendTime", "dynamicTopicConfig")),
        ),
    ] {
        let mut doc = pristine_1_1();
        *doc.config_coverage
            .as_mut()
            .unwrap()
            .get_mut("orders")
            .unwrap() = entry;
        refused_with(
            &doc,
            "config_coverage[\"orders\"] records a timestamp_type, but a topic whose \
             configuration read was denied or failed cannot have observed one",
            "arm 10",
        );
    }
}

#[test]
fn arm_11_refuses_a_timestamp_type_value_or_source_outside_the_closed_sets() {
    for (value, source) in [
        ("createTime", "defaultConfig"),
        ("LogAppendTime", "DYNAMIC_DEFAULT_BROKER_CONFIG"),
        ("LogAppendTime", ""),
    ] {
        let mut doc = pristine_1_1();
        doc.config_coverage
            .as_mut()
            .unwrap()
            .get_mut("orders")
            .unwrap()
            .timestamp_type = Some(EffectiveConfigValue {
            value: value.into(),
            source: source.into(),
        });
        refused_with(
            &doc,
            &format!(
                "config_coverage[\"orders\"].timestamp_type \"{value}\" from \"{source}\" is not \
                 a value and source this format defines: the value is \"CreateTime\" or \
                 \"LogAppendTime\", and the source is \"dynamicTopicConfig\", \
                 \"dynamicBrokerConfig\", \"dynamicDefaultBrokerConfig\", \
                 \"staticBrokerConfig\", \"defaultConfig\" or \"unknown\""
            ),
            "arm 11",
        );
    }
    // Every value and source the format defines is accepted.
    for value in logweir_core::backup_receipt::TIMESTAMP_TYPES {
        for source in logweir_core::backup_receipt::CONFIG_SOURCES {
            let mut doc = pristine_1_1();
            doc.config_coverage
                .as_mut()
                .unwrap()
                .get_mut("orders")
                .unwrap()
                .timestamp_type = Some(EffectiveConfigValue {
                value: value.into(),
                source: source.into(),
            });
            assert_eq!(doc.validate_invariants(), Ok(()), "{value} from {source}");
        }
    }
}

/// The 1.0.0 arms still come FIRST: a document that violates arm 5 and arm 8
/// reports arm 5, and one that violates arm 3 and arm 7 reports arm 3 — so a
/// 1.1.0 block can never mask what an auditor reads first.
#[test]
fn the_1_0_0_arms_are_evaluated_before_the_config_coverage_arms() {
    let mut doc = pristine_1_1();
    doc.source.auth.mode = "scram-sha-512".into();
    doc.config_coverage
        .as_mut()
        .unwrap()
        .get_mut("orders")
        .unwrap()
        .coverage = "bogus".into();
    refused_with(
        &doc,
        "source.auth.mode \"scram-sha-512\" is not one of the two values this format defines: \
         \"plaintext\" or \"scramSha512\"",
        "arm 5 before arm 8",
    );
    let mut doc = pristine_1_1();
    doc.records.remove("payments");
    doc.config_coverage.as_mut().unwrap().remove("payments");
    refused_with(
        &doc,
        "records covers {\"orders\"} but the named topic set is {\"orders\", \"payments\"}",
        "arm 3 before arm 7",
    );
}

/// **The version pair a renumber must move together.** The receipt this build
/// WRITES must be one that may carry `config_coverage` (its minor is at least
/// `CONFIG_COVERAGE_SINCE_MINOR`), or every receipt the backup signs would be
/// refused by its own arm 6. Should another 1.1.0 field merge first, FX-4
/// becomes 1.2.0: both constants move, and this keeps them coherent.
#[test]
fn the_written_version_defines_config_coverage() {
    let mut parts = logweir_core::backup_receipt::RECEIPT_FORMAT_VERSION
        .split('.')
        .map(|p| p.parse::<u64>().expect("a numeric semver part"));
    let (major, minor) = (parts.next().unwrap(), parts.next().unwrap());
    assert_eq!(major, 1);
    assert!(
        minor >= logweir_core::backup_receipt::CONFIG_COVERAGE_SINCE_MINOR,
        "RECEIPT_FORMAT_VERSION {} predates CONFIG_COVERAGE_SINCE_MINOR {}",
        logweir_core::backup_receipt::RECEIPT_FORMAT_VERSION,
        logweir_core::backup_receipt::CONFIG_COVERAGE_SINCE_MINOR
    );
}

/// **The reader's half of "absent is unknown, never captured"** — the mutant
/// the FX-4 brief names "an absent field read as `captured`".
#[test]
fn source_config_coverage_reads_absent_as_unknown_never_captured() {
    // No receipt at all: a plan not bound to a recovery point.
    assert_eq!(
        SourceConfigCoverage::unknown().of("orders"),
        ConfigCoverage::Unknown
    );
    // A 1.0.0 receipt: no block.
    assert_eq!(
        SourceConfigCoverage::from_receipt(&pristine()).of("orders"),
        ConfigCoverage::Unknown
    );
    // A 1.1.0 receipt: its own answers, and UNKNOWN for a topic it does not name.
    let doc = pristine_1_1();
    let seen = SourceConfigCoverage::from_receipt(&doc);
    assert_eq!(seen.of("orders"), ConfigCoverage::Captured);
    assert_eq!(seen.of("not-in-the-receipt"), ConfigCoverage::Unknown);
    assert_eq!(
        seen.entry("orders").and_then(|e| e.timestamp_type.as_ref()),
        Some(&EffectiveConfigValue {
            value: "LogAppendTime".into(),
            source: "dynamicDefaultBrokerConfig".into(),
        }),
        "FX-8 reads the effective timestamp type and its source from here"
    );
    // A value outside the closed set (a document no verifier accepts, read by
    // a caller that skipped verification) is UNKNOWN too, never captured.
    let mut doc = pristine_1_1();
    doc.config_coverage
        .as_mut()
        .unwrap()
        .get_mut("orders")
        .unwrap()
        .coverage = "totallyCaptured".into();
    assert_eq!(
        SourceConfigCoverage::from_receipt(&doc).of("orders"),
        ConfigCoverage::Unknown
    );
    for (wire, want) in [
        ("captured", ConfigCoverage::Captured),
        ("notCaptured", ConfigCoverage::NotCaptured),
        ("captureDenied", ConfigCoverage::CaptureDenied),
    ] {
        assert_eq!(ConfigCoverage::from_wire(wire), want);
        assert_eq!(want.wire_name(), wire);
    }
    assert_eq!(ConfigCoverage::Unknown.wire_name(), "unknown");
}

/// A 1.0.0 document round-trips byte-for-byte through the 1.1.0 type: the
/// block is skipped when absent, so an old receipt's bytes — the ones its
/// signature covers — are exactly what this build would write for it.
#[test]
fn a_1_0_0_receipt_round_trips_byte_for_byte_through_the_1_1_0_type() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../e2e/fixtures/signed/backup-receipt.json"
    );
    let bytes = std::fs::read(path).unwrap();
    let doc: BackupReceipt = serde_json::from_slice(&bytes).unwrap();
    let again = logweir_core::det_json::to_deterministic_json(&doc).unwrap();
    assert_eq!(
        String::from_utf8(again).unwrap(),
        String::from_utf8(bytes).unwrap(),
        "re-serialising a 1.0.0 receipt must not add a config_coverage key"
    );
}

// ---------------------------------------------------------------------------
// PROD-05.1: format 1.3.0's `topic_configuration` block and arms 12-21
// ---------------------------------------------------------------------------

fn entry(value: Option<&str>, source: &str, portability: &str) -> ConfigEntry {
    ConfigEntry {
        value: value.map(str::to_string),
        source: source.to_string(),
        portability: portability.to_string(),
    }
}

/// A 1.3.0 receipt that satisfies all twenty-one arms: `orders` a compacted,
/// min-in-sync-2 topic with an inherited retention and a removed-in-4.0
/// override, owned by a Strimzi `KafkaTopic`; `payments` with a secret and a
/// provider-only override, declared externally owned; and the run looked in
/// both places (`owner_detection`).
fn pristine_1_3() -> BackupReceipt {
    let mut doc = pristine_1_1();
    doc.format_version = "1.3.0".to_string();
    let mut orders = BTreeMap::new();
    orders.insert(
        "cleanup.policy".to_string(),
        entry(Some("compact"), "dynamicTopicConfig", "portable"),
    );
    orders.insert(
        "min.insync.replicas".to_string(),
        entry(Some("2"), "dynamicTopicConfig", "portable"),
    );
    orders.insert(
        "retention.ms".to_string(),
        entry(Some("604800000"), "defaultConfig", "inherited"),
    );
    orders.insert(
        "message.format.version".to_string(),
        entry(Some("3.0-IV1"), "dynamicTopicConfig", "removedInKafka4"),
    );
    let mut payments = BTreeMap::new();
    payments.insert(
        "vendor.token".to_string(),
        entry(None, "dynamicTopicConfig", "secret"),
    );
    payments.insert(
        "confluent.placement.constraints".to_string(),
        entry(Some("{}"), "dynamicTopicConfig", "providerOnly"),
    );
    let mut model = BTreeMap::new();
    model.insert(
        "orders".to_string(),
        TopicConfiguration {
            partitions: Some(3),
            replication_factor: Some(3),
            entries: Some(orders),
            owner: Some(TopicOwner {
                kind: "strimzi".into(),
                basis: "kafkaTopicResource".into(),
                reference: "kafka/orders".into(),
            }),
        },
    );
    model.insert(
        "payments".to_string(),
        TopicConfiguration {
            partitions: Some(1),
            replication_factor: Some(1),
            entries: Some(payments),
            owner: Some(TopicOwner {
                kind: "external".into(),
                basis: "declared".into(),
                reference: "terraform: kafka_topic.payments".into(),
            }),
        },
    );
    doc.topic_configuration = Some(model);
    doc.owner_detection = Some(vec!["declared".into(), "kafkaTopicResources".into()]);
    doc
}

fn topic<'a>(doc: &'a mut BackupReceipt, name: &str) -> &'a mut TopicConfiguration {
    doc.topic_configuration
        .as_mut()
        .unwrap()
        .get_mut(name)
        .unwrap()
}

#[test]
fn a_1_3_0_receipt_with_every_class_and_owner_satisfies_every_invariant() {
    assert_eq!(pristine_1_3().validate_invariants(), Ok(()));
    // A denied and a failed read record no entries, and a read whose manifest
    // differed records them; partitions and the factor are the archive's and
    // stand either way.
    let mut doc = pristine_1_3();
    doc.config_coverage
        .as_mut()
        .unwrap()
        .insert("orders".into(), coverage("captureDenied", None, None));
    topic(&mut doc, "orders").entries = None;
    doc.config_coverage.as_mut().unwrap().insert(
        "payments".into(),
        coverage("notCaptured", Some("manifestDiffers"), None),
    );
    assert_eq!(doc.validate_invariants(), Ok(()));
    let mut doc = pristine_1_3();
    doc.config_coverage.as_mut().unwrap().insert(
        "orders".into(),
        coverage("notCaptured", Some("describeFailed"), None),
    );
    let t = topic(&mut doc, "orders");
    t.entries = None;
    t.owner = None;
    t.partitions = None;
    t.replication_factor = None;
    assert_eq!(doc.validate_invariants(), Ok(()));
    // Every class the table defines, as an override; and `inherited` from
    // every broker source.
    let mut doc = pristine_1_3();
    let e = topic(&mut doc, "orders").entries.as_mut().unwrap();
    for class in logweir_core::topic_configuration::PORTABILITY_CLASSES {
        if class != "inherited" && class != "secret" {
            e.insert(
                format!("k.{class}"),
                entry(Some("v"), "dynamicTopicConfig", class),
            );
        }
    }
    for source in [
        "dynamicBrokerConfig",
        "dynamicDefaultBrokerConfig",
        "staticBrokerConfig",
        "defaultConfig",
        "unknown",
    ] {
        e.insert(format!("i.{source}"), entry(Some("v"), source, "inherited"));
    }
    assert_eq!(doc.validate_invariants(), Ok(()));
}

#[test]
fn a_receipt_without_topic_configuration_is_decided_as_before() {
    // Every earlier minor, and a 1.3.0 that omits the block (weaker: NOT
    // RECORDED), all accepted.
    for version in ["1.1.0", "1.2.0", "1.3.0"] {
        let mut doc = pristine_1_1();
        doc.format_version = version.into();
        assert_eq!(doc.validate_invariants(), Ok(()), "{version}");
    }
}

#[test]
fn arm_12_refuses_topic_configuration_under_a_minor_before_3() {
    for version in ["1.0.0", "1.1.0", "1.2.9"] {
        let mut doc = pristine_1_3();
        doc.format_version = version.to_string();
        // 1.0.x would be arm 6's first; drop the 1.1 block so arm 12 is reached.
        let want = if version == "1.0.0" {
            doc.config_coverage = None;
            format!(
                "topic_configuration is present but format_version \"{version}\" predates it: \
                 the field is defined from 1.3.0"
            )
        } else {
            format!(
                "topic_configuration is present but format_version \"{version}\" predates it: \
                 the field is defined from 1.3.0"
            )
        };
        refused_with(&doc, &want, "arm 12");
    }
}

#[test]
fn arm_13_refuses_topic_configuration_without_config_coverage() {
    let mut doc = pristine_1_3();
    doc.config_coverage = None;
    refused_with(
        &doc,
        "topic_configuration is present under format_version \"1.3.0\" but config_coverage \
         is not: a topic's configuration entries cannot be judged without the read that \
         produced them",
        "arm 13",
    );
}

#[test]
fn arm_14_refuses_topic_configuration_that_does_not_cover_the_named_topic_set() {
    let mut doc = pristine_1_3();
    doc.topic_configuration.as_mut().unwrap().remove("payments");
    refused_with(
        &doc,
        "topic_configuration covers {\"orders\"} but the named topic set is {\"orders\", \
         \"payments\"}",
        "arm 14, a missing topic",
    );
    let mut doc = pristine_1_3();
    let extra = topic(&mut doc, "orders").clone();
    doc.topic_configuration
        .as_mut()
        .unwrap()
        .insert("invoices".into(), extra);
    refused_with(
        &doc,
        "topic_configuration covers {\"invoices\", \"orders\", \"payments\"} but the named \
         topic set is {\"orders\", \"payments\"}",
        "arm 14, an extra topic",
    );
}

#[test]
fn arm_15_refuses_entries_that_do_not_fit_the_read() {
    // Entries beside a denied read: an observation that could not exist.
    let mut doc = pristine_1_3();
    doc.config_coverage
        .as_mut()
        .unwrap()
        .insert("orders".into(), coverage("captureDenied", None, None));
    refused_with(
        &doc,
        "topic_configuration[\"orders\"].entries present does not fit its config_coverage \
         \"captureDenied\": entries are recorded exactly when the configuration read \
         succeeded (\"captured\", or \"notCaptured\" with reason \"manifestDiffers\")",
        "arm 15, entries beside a denied read",
    );
    let mut doc = pristine_1_3();
    doc.config_coverage.as_mut().unwrap().insert(
        "orders".into(),
        coverage("notCaptured", Some("describeFailed"), None),
    );
    refused_with(
        &doc,
        "topic_configuration[\"orders\"].entries present does not fit its config_coverage \
         \"notCaptured/describeFailed\": entries are recorded exactly when the configuration \
         read succeeded (\"captured\", or \"notCaptured\" with reason \"manifestDiffers\")",
        "arm 15, entries beside a failed read",
    );
    // No entries beside a read that succeeded: "not recorded" masquerading.
    let mut doc = pristine_1_3();
    topic(&mut doc, "payments").entries = None;
    refused_with(
        &doc,
        "topic_configuration[\"payments\"].entries absent does not fit its config_coverage \
         \"captured\": entries are recorded exactly when the configuration read succeeded \
         (\"captured\", or \"notCaptured\" with reason \"manifestDiffers\")",
        "arm 15, no entries beside a successful read",
    );
}

#[test]
fn arm_16_refuses_a_source_or_class_outside_the_closed_sets() {
    let mut doc = pristine_1_3();
    topic(&mut doc, "orders").entries.as_mut().unwrap().insert(
        "segment.ms".into(),
        entry(Some("1"), "dynamicTopicConfig", "portableish"),
    );
    let tail = " are not a source and class this format defines: the source is \
                \"dynamicTopicConfig\", \"dynamicBrokerConfig\", \"dynamicDefaultBrokerConfig\", \
                \"staticBrokerConfig\", \"defaultConfig\" or \"unknown\", and the class is \
                \"portable\", \"inherited\", \"removedInKafka4\", \"clusterBound\", \
                \"requiresTieredStorage\", \"providerOnly\" or \"secret\"";
    refused_with(
        &doc,
        &format!(
            "topic_configuration[\"orders\"].entries[\"segment.ms\"] source \
             \"dynamicTopicConfig\" and portability \"portableish\"{tail}"
        ),
        "arm 16, a class",
    );
    let mut doc = pristine_1_3();
    topic(&mut doc, "orders").entries.as_mut().unwrap().insert(
        "segment.ms".into(),
        entry(Some("1"), "DEFAULT_CONFIG", "inherited"),
    );
    refused_with(
        &doc,
        &format!(
            "topic_configuration[\"orders\"].entries[\"segment.ms\"] source \
             \"DEFAULT_CONFIG\" and portability \"inherited\"{tail}"
        ),
        "arm 16, a source",
    );
}

#[test]
fn arm_17_refuses_a_class_that_does_not_fit_its_source_or_value() {
    let tail = ": an entry is \"secret\" exactly when it carries no value, and otherwise \
                \"inherited\" exactly when its source is not \"dynamicTopicConfig\"";
    let cases = [
        // A broker default passed off as the topic's own portable override.
        (
            entry(Some("604800000"), "defaultConfig", "portable"),
            "is \"portable\" from \"defaultConfig\" with a value",
        ),
        // The topic's own override passed off as inherited.
        (
            entry(Some("compact"), "dynamicTopicConfig", "inherited"),
            "is \"inherited\" from \"dynamicTopicConfig\" with a value",
        ),
        // A secret that carries its value.
        (
            entry(Some("hunter2"), "dynamicTopicConfig", "secret"),
            "is \"secret\" from \"dynamicTopicConfig\" with a value",
        ),
        // A value missing from something that is not a secret.
        (
            entry(None, "dynamicTopicConfig", "portable"),
            "is \"portable\" from \"dynamicTopicConfig\" with no value",
        ),
    ];
    for (bad, said) in cases {
        let mut doc = pristine_1_3();
        topic(&mut doc, "orders")
            .entries
            .as_mut()
            .unwrap()
            .insert("k".into(), bad);
        refused_with(
            &doc,
            &format!("topic_configuration[\"orders\"].entries[\"k\"] {said}{tail}"),
            "arm 17",
        );
    }
}

#[test]
fn arm_18_refuses_an_owner_outside_the_closed_sets() {
    let tail = " is not an owner this format defines: the kind is \"strimzi\" or \
                \"external\", the basis is \"kafkaTopicResource\" (for \"strimzi\" only) or \
                \"declared\", and the reference is 1 to 256 characters with no control \
                character";
    let cases = [
        ("terraform", "declared", "x".to_string()),
        ("external", "kafkaTopicResource", "kafka/orders".to_string()),
        ("strimzi", "label", "kafka/orders".to_string()),
        ("strimzi", "declared", "  ".to_string()),
        ("strimzi", "declared", "a\u{7}b".to_string()),
        ("external", "declared", "x".repeat(257)),
    ];
    for (kind, basis, reference) in cases {
        let mut doc = pristine_1_3();
        topic(&mut doc, "orders").owner = Some(TopicOwner {
            kind: kind.into(),
            basis: basis.into(),
            reference,
        });
        refused_with(
            &doc,
            &format!("topic_configuration[\"orders\"].owner \"{kind}\" by \"{basis}\"{tail}"),
            "arm 18",
        );
    }
    // The boundary: 256 characters is a reference.
    let mut doc = pristine_1_3();
    topic(&mut doc, "orders").owner.as_mut().unwrap().reference = "x".repeat(256);
    assert_eq!(doc.validate_invariants(), Ok(()));
}

#[test]
fn arm_19_refuses_a_zero_count() {
    let mut doc = pristine_1_3();
    topic(&mut doc, "payments").partitions = Some(0);
    refused_with(
        &doc,
        "topic_configuration[\"payments\"] records partitions 0 and replication_factor 1: a \
         recorded count is at least 1",
        "arm 19, partitions",
    );
    let mut doc = pristine_1_3();
    let t = topic(&mut doc, "payments");
    t.partitions = None;
    t.replication_factor = Some(0);
    refused_with(
        &doc,
        "topic_configuration[\"payments\"] records partitions absent and replication_factor \
         0: a recorded count is at least 1",
        "arm 19, the factor",
    );
}

const ARM_20_TAIL: &str = " is not a detection this format defines: it is present only \
                           beside topic_configuration, and lists \"declared\" and \
                           \"kafkaTopicResources\" each at most once";

/// **Arm 20 (fix round, M2).** `owner_detection` is the closed set, each word
/// at most once, and only beside the model it qualifies. An EMPTY list is a
/// run that looked nowhere — legal — and so is an absent one.
#[test]
fn arm_20_refuses_a_detection_outside_the_closed_set_or_beside_no_model() {
    for bad in [
        vec!["labels".to_string()],
        vec!["declared".to_string(), "declared".to_string()],
        vec!["Declared".to_string()],
        vec!["kafkaTopicResource".to_string()],
    ] {
        let mut doc = pristine_1_3();
        doc.owner_detection = Some(bad.clone());
        refused_with(
            &doc,
            &format!("owner_detection {bad:?}{ARM_20_TAIL}"),
            "arm 20",
        );
    }
    // Beside no model: a 1.1.0 document claiming where it looked for owners
    // it records none of.
    let mut doc = pristine_1_1();
    doc.owner_detection = Some(Vec::new());
    refused_with(
        &doc,
        &format!("owner_detection []{ARM_20_TAIL}"),
        "arm 20, no model",
    );
    // Legal: empty, absent, one, both — with the owners arm 21 then allows.
    let mut doc = pristine_1_3();
    for t in ["orders", "payments"] {
        topic(&mut doc, t).owner = None;
    }
    for ok in [None, Some(vec![]), Some(vec!["declared".to_string()])] {
        doc.owner_detection = ok.clone();
        assert_eq!(doc.validate_invariants(), Ok(()), "{ok:?}");
    }
}

/// **Arm 21 (fix round, M2).** An owner is recorded only from a source the
/// run looked in: a `declared` owner needs `declared`, a `kafkaTopicResource`
/// owner `kafkaTopicResources`. An absent detection reads as empty.
#[test]
fn arm_21_refuses_an_owner_from_a_source_the_run_did_not_look_in() {
    let tail = ": a \"declared\" owner needs \"declared\", a \"kafkaTopicResource\" owner \
                \"kafkaTopicResources\"";
    let mut doc = pristine_1_3();
    doc.owner_detection = Some(vec!["declared".into()]);
    refused_with(
        &doc,
        &format!(
            "topic_configuration[\"orders\"].owner by \"kafkaTopicResource\" names no source \
             owner_detection [\"declared\"] lists{tail}"
        ),
        "arm 21, resources not read",
    );
    let mut doc = pristine_1_3();
    doc.owner_detection = Some(vec!["kafkaTopicResources".into()]);
    refused_with(
        &doc,
        &format!(
            "topic_configuration[\"payments\"].owner by \"declared\" names no source \
             owner_detection [\"kafkaTopicResources\"] lists{tail}"
        ),
        "arm 21, nothing declared",
    );
    let mut doc = pristine_1_3();
    doc.owner_detection = None;
    refused_with(
        &doc,
        &format!(
            "topic_configuration[\"orders\"].owner by \"kafkaTopicResource\" names no source \
             owner_detection [] lists{tail}"
        ),
        "arm 21, absent reads as empty",
    );
}

/// The 1.0.0-1.1.0 arms run before the 1.3.0 ones: a document with both a
/// coverage fault and a model fault is refused for the coverage fault.
#[test]
fn the_config_coverage_arms_are_evaluated_before_the_topic_configuration_arms() {
    let mut doc = pristine_1_3();
    doc.config_coverage
        .as_mut()
        .unwrap()
        .get_mut("orders")
        .unwrap()
        .coverage = "nope".into();
    topic(&mut doc, "orders").partitions = Some(0);
    let got = doc.validate_invariants().unwrap_err();
    assert!(
        got.starts_with("config_coverage[\"orders\"].coverage"),
        "{got}"
    );
}

/// The written version and the first minor that defines the block move
/// together, or every receipt this build signs refuses itself at arm 12.
#[test]
fn the_written_version_defines_topic_configuration() {
    use logweir_core::backup_receipt::{
        format_version_for, FORMAT_VERSION_WITH_TOPIC_CONFIGURATION,
        TOPIC_CONFIGURATION_SINCE_MINOR,
    };
    let minor: u64 = FORMAT_VERSION_WITH_TOPIC_CONFIGURATION
        .split('.')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(minor, TOPIC_CONFIGURATION_SINCE_MINOR);
    let mut archive = pristine().archive;
    let auth = pristine().source.auth;
    assert_eq!(
        format_version_for(&archive, true, false, &auth, false, false),
        "1.3.0"
    );
    assert_eq!(
        format_version_for(&archive, false, false, &auth, false, false),
        "1.1.0"
    );
    archive.manifest_version_id = Some("v1".into());
    assert_eq!(
        format_version_for(&archive, true, false, &auth, false, false),
        "1.3.0"
    );
    assert_eq!(
        format_version_for(&archive, false, false, &auth, false, false),
        "1.2.0"
    );
}

// ===========================================================================
// PROD-03.0: format 1.5.0, `schema_dependency` (arms 22-29)
// ===========================================================================

use logweir_core::backup_receipt::{SchemaDependency, SideFraming, TopicSchemaDependency};

fn side(dependent: bool, framed: u64, unframed: u64, nulls: u64, ids: &[u32]) -> SideFraming {
    SideFraming {
        dependent,
        framed,
        unframed,
        nulls,
        schema_ids: ids.to_vec(),
        schema_id_count: ids.len() as u64,
    }
}

fn judged(
    verdict: &str,
    basis: &str,
    key: SideFraming,
    value: SideFraming,
) -> TopicSchemaDependency {
    TopicSchemaDependency {
        verdict: verdict.to_string(),
        reason: None,
        basis: Some(basis.to_string()),
        key: Some(key),
        value: Some(value),
    }
}

/// A 1.5.0 receipt that satisfies all twenty-nine arms: `orders` (12 records)
/// judged completely, its values framed with ids 1 and 7 beside two
/// tombstones and its keys unframed; `payments` (7 records) sampled, five
/// records with null keys and unframed values.
fn pristine_1_5() -> BackupReceipt {
    let mut doc = pristine_1_3();
    doc.format_version = "1.5.0".to_string();
    let mut block = BTreeMap::new();
    block.insert(
        "orders".to_string(),
        judged(
            "schemaDependent",
            "complete",
            side(false, 0, 12, 0, &[]),
            side(true, 10, 0, 2, &[1, 7]),
        ),
    );
    block.insert(
        "payments".to_string(),
        judged(
            "notDetected",
            "sampled",
            side(false, 0, 0, 5, &[]),
            side(false, 0, 5, 0, &[]),
        ),
    );
    doc.schema_dependency = Some(block);
    doc
}

fn dep<'a>(doc: &'a mut BackupReceipt, name: &str) -> &'a mut TopicSchemaDependency {
    doc.schema_dependency
        .as_mut()
        .unwrap()
        .get_mut(name)
        .unwrap()
}

#[test]
fn a_1_5_0_receipt_satisfies_every_invariant() {
    pristine_1_5()
        .validate_invariants()
        .expect("the pristine 1.5.0 receipt satisfies every arm");
    // Every not-assessed shape is accepted too.
    for reason in [
        "segmentUnreadable",
        "segmentTooLargeForDetection",
        "detectionTimeBudgetExceeded",
    ] {
        let mut doc = pristine_1_5();
        *dep(&mut doc, "payments") = logweir_core::schema_dependency::not_assessed(reason);
        doc.validate_invariants().expect(reason);
    }
    let mut doc = pristine_1_5();
    doc.records.insert("payments".into(), 0);
    *dep(&mut doc, "payments") = logweir_core::schema_dependency::not_assessed("noRecords");
    doc.validate_invariants()
        .expect("noRecords beside a count of 0");
}

#[test]
fn a_receipt_without_schema_dependency_is_decided_as_before() {
    let mut doc = pristine_1_5();
    doc.schema_dependency = None;
    doc.validate_invariants()
        .expect("absent is not assessed, never refused");
    pristine_1_3().validate_invariants().expect("1.3.0");
}

#[test]
fn arm_22_refuses_schema_dependency_under_a_minor_before_5() {
    let mut doc = pristine_1_5();
    doc.format_version = "1.4.0".to_string();
    refused_with(
        &doc,
        "schema_dependency is present but format_version \"1.4.0\" predates it: the field is \
         defined from 1.5.0",
        "arm 22",
    );
}

#[test]
fn arm_23_refuses_schema_dependency_that_does_not_cover_the_named_topic_set() {
    let mut doc = pristine_1_5();
    doc.schema_dependency.as_mut().unwrap().remove("payments");
    refused_with(
        &doc,
        "schema_dependency covers {\"orders\"} but the named topic set is {\"orders\", \"payments\"}",
        "arm 23, a topic missing",
    );
    let mut doc = pristine_1_5();
    let extra = dep(&mut doc, "payments").clone();
    doc.schema_dependency
        .as_mut()
        .unwrap()
        .insert("invoices".into(), extra);
    refused_with(
        &doc,
        "schema_dependency covers {\"invoices\", \"orders\", \"payments\"} but the named topic set \
         is {\"orders\", \"payments\"}",
        "arm 23, a topic not named",
    );
}

const ARM_24_TAIL: &str = " is not a verdict this format defines: the verdict is \
     \"schemaDependent\", \"notDetected\" or \"notAssessed\"; a \"notAssessed\" topic has a \
     reason, \"noRecords\", \"segmentUnreadable\", \"segmentTooLargeForDetection\" or \
     \"detectionTimeBudgetExceeded\", and no basis, and any other topic has a basis, \
     \"sampled\" or \"complete\", and no reason";

#[test]
fn arm_24_refuses_a_verdict_reason_or_basis_outside_the_closed_sets() {
    type Mutation = Box<dyn Fn(&mut TopicSchemaDependency)>;
    let cases: Vec<(&str, Mutation, &str)> = vec![
        (
            "a verdict outside the three",
            Box::new(|e| e.verdict = "registryNeeded".into()),
            "schema_dependency[\"orders\"] verdict \"registryNeeded\" with reason absent and basis \"complete\"",
        ),
        (
            "a judged topic with a reason",
            Box::new(|e| e.reason = Some("noRecords".into())),
            "schema_dependency[\"orders\"] verdict \"schemaDependent\" with reason \"noRecords\" and basis \"complete\"",
        ),
        (
            "a judged topic with no basis",
            Box::new(|e| e.basis = None),
            "schema_dependency[\"orders\"] verdict \"schemaDependent\" with reason absent and basis absent",
        ),
        (
            "a basis outside the two",
            Box::new(|e| e.basis = Some("partial".into())),
            "schema_dependency[\"orders\"] verdict \"schemaDependent\" with reason absent and basis \"partial\"",
        ),
        (
            "a not-assessed topic with a basis",
            Box::new(|e| {
                *e = logweir_core::schema_dependency::not_assessed("noRecords");
                e.basis = Some("sampled".into());
            }),
            "schema_dependency[\"orders\"] verdict \"notAssessed\" with reason \"noRecords\" and basis \"sampled\"",
        ),
        (
            "a not-assessed topic with no reason",
            Box::new(|e| {
                *e = logweir_core::schema_dependency::not_assessed("noRecords");
                e.reason = None;
            }),
            "schema_dependency[\"orders\"] verdict \"notAssessed\" with reason absent and basis absent",
        ),
        (
            "a reason outside the two",
            Box::new(|e| {
                *e = logweir_core::schema_dependency::not_assessed("registryDown");
            }),
            "schema_dependency[\"orders\"] verdict \"notAssessed\" with reason \"registryDown\" and basis absent",
        ),
    ];
    for (what, mutate, head) in cases {
        let mut doc = pristine_1_5();
        mutate(dep(&mut doc, "orders"));
        refused_with(&doc, &format!("{head}{ARM_24_TAIL}"), what);
    }
}

#[test]
fn arm_25_refuses_sides_that_do_not_fit_the_verdict() {
    let tail = ": a judged topic records a key side and a value side over the same records, at \
                least one, and a \"notAssessed\" topic records neither";
    let mut doc = pristine_1_5();
    dep(&mut doc, "orders").value = None;
    refused_with(
        &doc,
        &format!("schema_dependency[\"orders\"] verdict \"schemaDependent\" records key 12 records and value absent{tail}"),
        "a judged topic missing a side",
    );
    let mut doc = pristine_1_5();
    dep(&mut doc, "orders").key.as_mut().unwrap().unframed = 11;
    refused_with(
        &doc,
        &format!("schema_dependency[\"orders\"] verdict \"schemaDependent\" records key 11 records and value 12 records{tail}"),
        "sides over different records",
    );
    let mut doc = pristine_1_5();
    let e = dep(&mut doc, "payments");
    e.key = Some(side(false, 0, 0, 0, &[]));
    e.value = Some(side(false, 0, 0, 0, &[]));
    refused_with(
        &doc,
        &format!("schema_dependency[\"payments\"] verdict \"notDetected\" records key 0 records and value 0 records{tail}"),
        "a judged topic that judged nothing",
    );
    let mut doc = pristine_1_5();
    let e = dep(&mut doc, "payments");
    let key = e.key.clone();
    *e = logweir_core::schema_dependency::not_assessed("segmentUnreadable");
    e.key = key;
    refused_with(
        &doc,
        &format!("schema_dependency[\"payments\"] verdict \"notAssessed\" records key 5 records and value absent{tail}"),
        "a not-assessed topic with a side",
    );
}

#[test]
fn arm_26_refuses_a_judged_count_the_receipt_does_not_back() {
    let tail = ": a \"complete\" basis judges every record the receipt counts, a \"sampled\" one \
                at most that many, and \"noRecords\" is said only of a topic that counts none";
    let mut doc = pristine_1_5();
    doc.records.insert("orders".into(), 13);
    refused_with(
        &doc,
        &format!("schema_dependency[\"orders\"] judges 12 records under \"complete\" and records counts 13{tail}"),
        "complete, but a record was not judged",
    );
    let mut doc = pristine_1_5();
    doc.records.insert("payments".into(), 4);
    refused_with(
        &doc,
        &format!("schema_dependency[\"payments\"] judges 5 records under \"sampled\" and records counts 4{tail}"),
        "sampled more than the receipt counts",
    );
    let mut doc = pristine_1_5();
    *dep(&mut doc, "payments") = logweir_core::schema_dependency::not_assessed("noRecords");
    refused_with(
        &doc,
        &format!("schema_dependency[\"payments\"] judges 0 records under \"noRecords\" and records counts 7{tail}"),
        "noRecords beside a topic with records",
    );
    // The boundary holds: sampled may judge exactly what is counted.
    let mut doc = pristine_1_5();
    doc.records.insert("payments".into(), 5);
    doc.validate_invariants().expect("sampled == counted");
}

#[test]
fn arm_27_refuses_ids_that_do_not_fit_the_count_or_the_framing() {
    let tail = ": the ids are distinct, ascending and from 1 to 16777215, all of them when the \
                count is 16 or fewer and 16 otherwise, and the count is at least 1 exactly when \
                a record is framed and never above the framed count";
    let cases: Vec<(&str, Vec<u32>, u64, u64, &str)> = vec![
        (
            "descending",
            vec![7, 1],
            2,
            10,
            "[7, 1] with schema_id_count 2 and framed 10",
        ),
        (
            "a repeat",
            vec![7, 7],
            2,
            10,
            "[7, 7] with schema_id_count 2 and framed 10",
        ),
        (
            "id 0",
            vec![0, 7],
            2,
            10,
            "[0, 7] with schema_id_count 2 and framed 10",
        ),
        (
            "id 2^24",
            vec![1, 16_777_216],
            2,
            10,
            "[1, 16777216] with schema_id_count 2 and framed 10",
        ),
        (
            "fewer listed than counted",
            vec![1],
            2,
            10,
            "[1] with schema_id_count 2 and framed 10",
        ),
        (
            "a count above the framed records",
            vec![1, 7],
            2,
            1,
            "[1, 7] with schema_id_count 2 and framed 1",
        ),
    ];
    for (what, ids, count, framed, said) in cases {
        let mut doc = pristine_1_5();
        let v = dep(&mut doc, "orders").value.as_mut().unwrap();
        v.schema_ids = ids;
        v.schema_id_count = count;
        v.framed = framed;
        v.unframed = 10 - framed;
        refused_with(
            &doc,
            &format!("schema_dependency[\"orders\"].value lists schema_ids {said}{tail}"),
            what,
        );
    }
    // Framed records with no id, on the key side.
    let mut doc = pristine_1_5();
    let k = dep(&mut doc, "orders").key.as_mut().unwrap();
    k.framed = 1;
    k.unframed = 11;
    refused_with(
        &doc,
        &format!("schema_dependency[\"orders\"].key lists schema_ids [] with schema_id_count 0 and framed 1{tail}"),
        "framed, and no id",
    );
    // More than 16: 16 listed, the count above them. `orders` grows to 30
    // records so 20 ids fit inside its framed count.
    let thirty = |listed: u32| {
        let mut doc = pristine_1_5();
        doc.records.insert("orders".into(), 30);
        let e = dep(&mut doc, "orders");
        e.key = Some(side(false, 0, 30, 0, &[]));
        let v = e.value.as_mut().unwrap();
        v.framed = 28;
        v.nulls = 2;
        v.schema_ids = (1..=listed).collect();
        v.schema_id_count = 20;
        doc
    };
    thirty(16)
        .validate_invariants()
        .expect("16 listed of 20 is the cap");
    refused_with(
        &thirty(17),
        &format!(
            "schema_dependency[\"orders\"].value lists schema_ids {:?} with schema_id_count 20 \
             and framed 28{tail}",
            (1..=17).collect::<Vec<u32>>()
        ),
        "17 listed",
    );
    refused_with(
        &thirty(15),
        &format!(
            "schema_dependency[\"orders\"].value lists schema_ids {:?} with schema_id_count 20 \
             and framed 28{tail}",
            (1..=15).collect::<Vec<u32>>()
        ),
        "15 listed of 20",
    );
}

#[test]
fn arm_28_refuses_a_dependent_flag_the_counts_do_not_give() {
    let tail = ": a side is dependent exactly when at least one record and at least one in ten \
                of its non-null records are framed";
    // One in ten exactly is dependent; one in eleven is not.
    let mut doc = pristine_1_5();
    let v = dep(&mut doc, "orders").value.as_mut().unwrap();
    v.framed = 1;
    v.unframed = 9;
    v.nulls = 2;
    v.schema_ids = vec![1];
    v.schema_id_count = 1;
    doc.validate_invariants().expect("1 of 10 is dependent");
    let v = dep(&mut doc, "orders").value.as_mut().unwrap();
    v.unframed = 10;
    v.nulls = 1;
    refused_with(
        &doc,
        &format!(
            "schema_dependency[\"orders\"].value is dependent with framed 1 and unframed 10{tail}"
        ),
        "1 of 11 claimed dependent",
    );
    // Nulls never count toward the share.
    let mut doc = pristine_1_5();
    let v = dep(&mut doc, "orders").value.as_mut().unwrap();
    v.framed = 1;
    v.unframed = 0;
    v.nulls = 11;
    v.schema_ids = vec![1];
    v.schema_id_count = 1;
    doc.validate_invariants()
        .expect("1 framed beside 11 tombstones is dependent");
    // A side that is not claimed dependent when its counts say it is.
    let mut doc = pristine_1_5();
    let k = dep(&mut doc, "payments").key.as_mut().unwrap();
    k.framed = 5;
    k.nulls = 0;
    k.schema_ids = vec![3];
    k.schema_id_count = 1;
    refused_with(
        &doc,
        &format!("schema_dependency[\"payments\"].key is not dependent with framed 5 and unframed 0{tail}"),
        "all framed, claimed not dependent",
    );
}

#[test]
fn arm_29_refuses_a_verdict_its_sides_do_not_give() {
    let tail =
        " does not fit its sides: a judged topic is \"schemaDependent\" exactly when its key \
                side or its value side is dependent";
    let mut doc = pristine_1_5();
    dep(&mut doc, "orders").verdict = "notDetected".into();
    refused_with(
        &doc,
        &format!("schema_dependency[\"orders\"] verdict \"notDetected\"{tail}"),
        "a dependent value side under notDetected",
    );
    let mut doc = pristine_1_5();
    dep(&mut doc, "payments").verdict = "schemaDependent".into();
    refused_with(
        &doc,
        &format!("schema_dependency[\"payments\"] verdict \"schemaDependent\"{tail}"),
        "schemaDependent with no dependent side",
    );
    // A dependent KEY side alone makes the topic dependent.
    let mut doc = pristine_1_5();
    let e = dep(&mut doc, "payments");
    e.verdict = "schemaDependent".into();
    e.key = Some(side(true, 5, 0, 0, &[4]));
    doc.validate_invariants().expect("a dependent key side");
}

#[test]
fn the_owner_arms_are_evaluated_before_the_schema_dependency_arms() {
    let mut doc = pristine_1_5();
    doc.owner_detection = Some(vec!["declared".into()]);
    dep(&mut doc, "orders").verdict = "nope".into();
    let got = doc.validate_invariants().unwrap_err();
    assert!(
        got.starts_with("topic_configuration[\"orders\"].owner by"),
        "{got}"
    );
}

/// The written version and the first minor that defines the block move
/// together, or every receipt this build signs refuses itself at arm 22; and
/// a receipt carrying the block is 1.5.0 whatever else it carries.
#[test]
fn the_written_version_defines_schema_dependency() {
    use logweir_core::backup_receipt::{
        format_version_for, FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY, SCHEMA_DEPENDENCY_SINCE_MINOR,
    };
    let minor: u64 = FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY
        .split('.')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(minor, SCHEMA_DEPENDENCY_SINCE_MINOR);
    assert_eq!(FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY, "1.5.0");
    let mut pinned = pristine().archive;
    pinned.manifest_version_id = Some("v1".into());
    for mode in ["plaintext", "scramSha512", "scramSha256", "plain", "mtls"] {
        let auth = ReceiptAuth {
            mode: mode.into(),
            username: None,
        };
        for archive in [&pristine().archive, &pinned] {
            for topic_configuration in [false, true] {
                assert_eq!(
                    format_version_for(archive, topic_configuration, true, &auth, false, false),
                    "1.5.0",
                    "{mode}"
                );
            }
        }
    }
    // 1.5.0 defines the PROD-01.3 modes: a 1.5.0 receipt naming one is
    // accepted by arm 5.
    let mut doc = pristine_1_5();
    doc.source.auth.mode = "mtls".into();
    doc.validate_invariants().expect("1.5.0 defines mtls");
}

#[test]
fn schema_dependency_of_reads_absent_as_not_assessed_never_not_detected() {
    let doc = pristine_1_5();
    assert_eq!(
        SchemaDependency::of(&doc, "orders"),
        SchemaDependency::SchemaDependent
    );
    assert_eq!(
        SchemaDependency::of(&doc, "payments"),
        SchemaDependency::NotDetected
    );
    assert_eq!(
        SchemaDependency::of(&doc, "invoices"),
        SchemaDependency::NotAssessed
    );
    assert_eq!(
        SchemaDependency::of(&pristine_1_3(), "payments"),
        SchemaDependency::NotAssessed
    );
    assert_eq!(
        SchemaDependency::from_wire("notdetected"),
        SchemaDependency::NotAssessed
    );
}

#[test]
fn a_1_4_0_receipt_round_trips_byte_for_byte_through_the_1_5_0_type() {
    let mut doc = pristine_1_3();
    doc.format_version = "1.4.0".into();
    let bytes = logweir_core::det_json::to_deterministic_json(&doc).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("schema_dependency"));
    let back: BackupReceipt = serde_json::from_slice(&bytes).unwrap();
    assert!(back.schema_dependency.is_none());
    assert_eq!(
        logweir_core::det_json::to_deterministic_json(&back).unwrap(),
        bytes
    );
}

// ---------------------------------------------------------------------------
// PROD-01.4a: format 1.6.0's `generations` block and arms 36-40
// ---------------------------------------------------------------------------

/// Two topic IDs as `kafka-topics.sh --describe` printed them (PROD-01.4's
/// `ffi-route-evidence.txt`).
const ID_A: &str = "gtOq2VXiTCK1QM2UtERijA";
const ID_B: &str = "tpWwuKExQo2lN9NziDMpYg";

fn identity(
    before: Option<&str>,
    after: Option<&str>,
    source: Option<&str>,
    reasons: (Option<&str>, Option<&str>),
) -> TopicIdentity {
    TopicIdentity {
        topic_id: before.map(str::to_string),
        topic_id_after: after.map(str::to_string),
        topic_id_source: source.map(str::to_string),
        topic_id_reason: reasons.0.map(str::to_string),
        topic_id_after_reason: reasons.1.map(str::to_string),
    }
}

/// A 1.6.0 receipt that satisfies every arm: `orders` read with the
/// same ID before and after the engine, `payments` on a broker that has no IDs.
fn pristine_1_6() -> BackupReceipt {
    let mut doc = pristine_1_3();
    doc.format_version = "1.6.0".to_string();
    let mut block = BTreeMap::new();
    block.insert(
        "orders".to_string(),
        identity(Some(ID_A), Some(ID_A), Some("describeTopics"), (None, None)),
    );
    block.insert(
        "payments".to_string(),
        identity(None, None, None, (Some("noTopicId"), Some("noTopicId"))),
    );
    doc.generations = Some(block);
    doc
}

fn generation<'a>(doc: &'a mut BackupReceipt, name: &str) -> &'a mut TopicIdentity {
    doc.generations.as_mut().unwrap().get_mut(name).unwrap()
}

#[test]
fn a_1_6_0_receipt_with_every_identity_shape_satisfies_every_invariant() {
    let mut doc = pristine_1_6();
    assert_eq!(doc.validate_invariants(), Ok(()));
    for (label, shape) in [
        (
            "recreated during the capture: a fact, not a contradiction",
            identity(Some(ID_A), Some(ID_B), Some("describeTopics"), (None, None)),
        ),
        (
            "deleted during the capture",
            identity(
                Some(ID_A),
                None,
                Some("describeTopics"),
                (None, Some("topicNotFound")),
            ),
        ),
        (
            "created during the capture",
            identity(
                None,
                Some(ID_B),
                Some("describeTopics"),
                (Some("topicNotFound"), None),
            ),
        ),
        (
            "refused, then failed",
            identity(
                None,
                None,
                None,
                (Some("notAuthorized"), Some("readFailed")),
            ),
        ),
        (
            "a reader that reads no IDs",
            identity(None, None, None, (Some("notRead"), Some("notRead"))),
        ),
        (
            "a broker that answered Kafka's reserved ID",
            identity(
                None,
                None,
                None,
                (Some("reservedTopicId"), Some("reservedTopicId")),
            ),
        ),
        (
            "the engine route's source",
            identity(Some(ID_A), Some(ID_A), Some("engineManifest"), (None, None)),
        ),
    ] {
        *generation(&mut doc, "payments") = shape;
        assert_eq!(doc.validate_invariants(), Ok(()), "{label}");
    }
}

/// A null ID is written as `null`, never omitted, and its reason beside it.
#[test]
fn a_null_topic_id_is_written_as_null_with_its_reason() {
    let doc = pristine_1_6();
    let v = serde_json::to_value(&doc).expect("serialises");
    assert_eq!(
        v["generations"]["payments"],
        serde_json::json!({
            "topic_id": null,
            "topic_id_after": null,
            "topic_id_reason": "noTopicId",
            "topic_id_after_reason": "noTopicId",
        })
    );
    assert_eq!(
        v["generations"]["orders"],
        serde_json::json!({
            "topic_id": ID_A,
            "topic_id_after": ID_A,
            "topic_id_source": "describeTopics",
        })
    );
    // A reader reads absent IDs as null.
    let back: TopicIdentity =
        serde_json::from_value(serde_json::json!({"topic_id_reason": "notRead"}))
            .expect("absent IDs read as null");
    assert_eq!(back.topic_id, None);
    assert_eq!(back.topic_id_after, None);
}

#[test]
fn a_receipt_without_generations_is_decided_as_before() {
    assert_eq!(pristine().validate_invariants(), Ok(()));
    assert_eq!(pristine_1_1().validate_invariants(), Ok(()));
    assert_eq!(pristine_1_3().validate_invariants(), Ok(()));
    // A 1.6.0 document without the block is legal too (a writer that read no
    // IDs would omit it; this build always writes it).
    let mut doc = pristine_1_6();
    doc.generations = None;
    assert_eq!(doc.validate_invariants(), Ok(()));
}

#[test]
fn arm_36_refuses_generations_under_a_minor_before_6() {
    for version in ["1.0.0", "1.2.0", "1.4.0", "1.5.0"] {
        // The 1.0.0 document, so no earlier block's own version arm fires.
        let mut doc = pristine();
        doc.generations = pristine_1_6().generations;
        doc.format_version = version.to_string();
        refused_with(
            &doc,
            &format!(
                "generations is present but format_version \"{version}\" predates it: the \
                 field is defined from 1.6.0"
            ),
            "arm 36",
        );
    }
    let mut doc = pristine_1_6();
    doc.format_version = "1.7.0".to_string();
    assert_eq!(
        doc.validate_invariants(),
        Ok(()),
        "a later minor defines it"
    );
}

#[test]
fn arm_37_refuses_generations_that_do_not_cover_the_named_topic_set() {
    let mut doc = pristine_1_6();
    doc.generations.as_mut().unwrap().remove("payments");
    refused_with(
        &doc,
        "generations covers {\"orders\"} but the named topic set is {\"orders\", \"payments\"}",
        "arm 37, a missing topic",
    );
    let mut doc = pristine_1_6();
    doc.generations.as_mut().unwrap().insert(
        "invoices".to_string(),
        identity(None, None, None, (Some("notRead"), Some("notRead"))),
    );
    refused_with(
        &doc,
        "generations covers {\"invoices\", \"orders\", \"payments\"} but the named topic set \
         is {\"orders\", \"payments\"}",
        "arm 37, an unlisted topic",
    );
}

#[test]
fn arm_38_refuses_an_id_that_is_not_canonical() {
    let tail = " is not a topic ID this format defines: 22 characters of URL-safe base64 \
                without padding over the ID's 16 bytes, and never one of Kafka's reserved IDs \
                (AAAAAAAAAAAAAAAAAAAAAA, AAAAAAAAAAAAAAAAAAAAAQ)";
    for bad in [
        // Kafka's "no ID" is never an identity.
        "AAAAAAAAAAAAAAAAAAAAAA",
        // Nor its reserved ONE_UUID / METADATA_TOPIC_ID (review M1).
        "AAAAAAAAAAAAAAAAAAAAAQ",
        // C4: librdkafka's helper's STANDARD alphabet for a real ID.
        "Cf6zT/mcTNCoxuPmv1Ztxw",
        "gtOq2VXiTCK1QM2UtERijA==",
        "gtOq2VXiTCK1QM2UtERij",
        "gtOq2VXiTCK1QM2UtERijB",
        "orders",
    ] {
        let mut doc = pristine_1_6();
        generation(&mut doc, "orders").topic_id = Some(bad.to_string());
        refused_with(
            &doc,
            &format!("generations[\"orders\"].topic_id \"{bad}\"{tail}"),
            "arm 38, before",
        );
        let mut doc = pristine_1_6();
        generation(&mut doc, "orders").topic_id_after = Some(bad.to_string());
        refused_with(
            &doc,
            &format!("generations[\"orders\"].topic_id_after \"{bad}\"{tail}"),
            "arm 38, after",
        );
    }
    // The URL-safe twin of C4's text is a real ID.
    let mut doc = pristine_1_6();
    let e = generation(&mut doc, "orders");
    e.topic_id = Some("Cf6zT_mcTNCoxuPmv1Ztxw".into());
    e.topic_id_after = Some("Cf6zT_mcTNCoxuPmv1Ztxw".into());
    assert_eq!(doc.validate_invariants(), Ok(()));
}

#[test]
fn arm_39_refuses_a_reason_that_does_not_fit_its_id() {
    let tail = ": a reason is present exactly when the ID is null, and is \"noTopicId\", \
                \"notAuthorized\", \"topicNotFound\", \"readFailed\", \"notRead\" or \
                \"reservedTopicId\"";
    // A reason beside a recorded ID.
    let mut doc = pristine_1_6();
    generation(&mut doc, "orders").topic_id_reason = Some("readFailed".into());
    refused_with(
        &doc,
        &format!(
            "generations[\"orders\"].topic_id_reason \"readFailed\" does not fit a recorded \
             topic_id{tail}"
        ),
        "arm 39, a reason beside an ID",
    );
    // No reason beside null: an unknown that does not say why.
    let mut doc = pristine_1_6();
    generation(&mut doc, "payments").topic_id_after_reason = None;
    refused_with(
        &doc,
        &format!(
            "generations[\"payments\"].topic_id_after_reason absent does not fit a null \
             topic_id_after{tail}"
        ),
        "arm 39, null without a reason",
    );
    // A reason outside the closed set.
    for bad in ["absent", "NotAuthorized", "transport"] {
        let mut doc = pristine_1_6();
        generation(&mut doc, "payments").topic_id_reason = Some(bad.into());
        refused_with(
            &doc,
            &format!(
                "generations[\"payments\"].topic_id_reason \"{bad}\" does not fit a null \
                 topic_id{tail}"
            ),
            "arm 39, an unknown reason",
        );
    }
}

#[test]
fn arm_40_refuses_a_source_that_does_not_fit_the_ids() {
    let tail = " does not fit its IDs: a source is present exactly when an ID is recorded, \
                and is \"describeTopics\" or \"engineManifest\"";
    let mut doc = pristine_1_6();
    generation(&mut doc, "orders").topic_id_source = None;
    refused_with(
        &doc,
        &format!("generations[\"orders\"].topic_id_source absent{tail}"),
        "arm 40, IDs without a source",
    );
    let mut doc = pristine_1_6();
    generation(&mut doc, "payments").topic_id_source = Some("describeTopics".into());
    refused_with(
        &doc,
        &format!("generations[\"payments\"].topic_id_source \"describeTopics\"{tail}"),
        "arm 40, a source without an ID",
    );
    let mut doc = pristine_1_6();
    generation(&mut doc, "orders").topic_id_source = Some("metadata".into());
    refused_with(
        &doc,
        &format!("generations[\"orders\"].topic_id_source \"metadata\"{tail}"),
        "arm 40, an unknown source",
    );
}

/// Per topic, arm 38 before 39 before 40, and `topic_id` before
/// `topic_id_after`; the earlier blocks' arms before all of them.
#[test]
fn the_generations_arms_run_in_their_order() {
    let mut doc = pristine_1_6();
    let e = generation(&mut doc, "orders");
    e.topic_id_after = Some("AAAAAAAAAAAAAAAAAAAAAA".into());
    e.topic_id_reason = Some("readFailed".into());
    e.topic_id_source = None;
    let got = doc.validate_invariants().unwrap_err();
    assert!(
        got.starts_with("generations[\"orders\"].topic_id_after \"AAAA"),
        "{got}"
    );
    let mut doc = pristine_1_6();
    let e = generation(&mut doc, "orders");
    e.topic_id_reason = Some("readFailed".into());
    e.topic_id_source = None;
    let got = doc.validate_invariants().unwrap_err();
    assert!(
        got.starts_with("generations[\"orders\"].topic_id_reason"),
        "{got}"
    );
    let mut doc = pristine_1_6();
    topic(&mut doc, "orders").partitions = Some(0);
    generation(&mut doc, "orders").topic_id_source = None;
    let got = doc.validate_invariants().unwrap_err();
    assert!(got.starts_with("topic_configuration[\"orders\"]"), "{got}");
}

/// The written version and the first minor that defines the block move
/// together, or every receipt this build signs refuses itself at arm 36.
#[test]
fn the_written_version_defines_generations() {
    use logweir_core::backup_receipt::{
        format_version_for, FORMAT_VERSION_WITH_GENERATIONS, GENERATIONS_SINCE_MINOR,
    };
    assert_eq!(FORMAT_VERSION_WITH_GENERATIONS, "1.6.0");
    let minor: u64 = FORMAT_VERSION_WITH_GENERATIONS
        .split('.')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(minor, GENERATIONS_SINCE_MINOR);
    let mut archive = pristine().archive;
    for mode in ["plaintext", "scramSha512", "scramSha256", "plain", "mtls"] {
        let auth = ReceiptAuth {
            mode: mode.to_string(),
            username: None,
        };
        for topic_configuration in [false, true] {
            for schema_dependency in [false, true] {
                assert_eq!(
                    format_version_for(
                        &archive,
                        topic_configuration,
                        schema_dependency,
                        &auth,
                        true,
                        false
                    ),
                    "1.6.0",
                    "{mode}"
                );
            }
        }
    }
    archive.manifest_version_id = Some("v1".into());
    assert_eq!(
        format_version_for(&archive, true, true, &pristine().source.auth, true, false),
        "1.6.0"
    );
}

/// **The generation rule over two RECEIPTS** (`topic_identity::between`): the
/// lineage key is (source cluster, topic), so a previous point from another
/// cluster is no predecessor, whatever its IDs say; a receipt without the
/// block (every one before 1.6.0) establishes nothing; and only then do the
/// two pre-capture IDs decide.
#[test]
fn the_generation_rule_reads_two_receipts_of_one_source_cluster() {
    use logweir_core::topic_identity::{between, Generation, Unestablished};
    let previous = pristine_1_6();
    let mut current = pristine_1_6();
    // Same cluster, same ID: the same generation.
    assert_eq!(
        between(Some(&previous), &current, "orders"),
        Generation::Same {
            topic_id: ID_A.into()
        }
    );
    // Same cluster, recreated between the two points: a NEW generation.
    let e = generation(&mut current, "orders");
    e.topic_id = Some(ID_B.into());
    e.topic_id_after = Some(ID_B.into());
    assert_eq!(
        between(Some(&previous), &current, "orders"),
        Generation::New {
            previous: ID_A.into(),
            current: ID_B.into()
        }
    );
    // Another source cluster with the SAME ID is still no predecessor.
    let mut elsewhere = pristine_1_6();
    elsewhere.source.cluster_id = "ANOTHER-CLUSTER-000000001".into();
    assert_eq!(
        between(Some(&elsewhere), &pristine_1_6(), "orders"),
        Generation::NotEstablished(Unestablished::OtherCluster)
    );
    // A previous receipt before 1.6.0, and a current one without the block.
    assert_eq!(
        between(Some(&pristine_1_3()), &pristine_1_6(), "orders"),
        Generation::NotEstablished(Unestablished::PreviousNotRecorded)
    );
    assert_eq!(
        between(Some(&pristine_1_6()), &pristine_1_3(), "orders"),
        Generation::NotEstablished(Unestablished::CurrentNotRecorded)
    );
    // No previous point at all.
    assert_eq!(
        between(None, &pristine_1_6(), "orders"),
        Generation::NotEstablished(Unestablished::NoPredecessor)
    );
    // A broker with no IDs on both sides: unknown, never the same.
    assert!(matches!(
        between(Some(&pristine_1_6()), &pristine_1_6(), "payments"),
        Generation::NotEstablished(Unestablished::PreviousUnread(_))
    ));
}

/// **Review M1 and M2 over two receipts.** A recreated topic can never read
/// as the same generation through Kafka's reserved ID: a receipt that records
/// `AAAAAAAAAAAAAAAAAAAAAQ` is refused (arm 38), and even read unverified the
/// rule does not take it as an identity. And a capture whose after-read
/// recorded no ID is never `Same` with its predecessor. The control: two
/// receipts with one real ID, read before and after, are `Same`.
#[test]
fn neither_a_sentinel_nor_a_missing_after_read_makes_two_points_the_same() {
    use logweir_core::topic_identity::{between, Generation, Unestablished};
    const SENTINEL: &str = "AAAAAAAAAAAAAAAAAAAAAQ";
    let sentinel = |doc: &mut BackupReceipt| {
        let e = generation(doc, "orders");
        e.topic_id = Some(SENTINEL.into());
        e.topic_id_after = Some(SENTINEL.into());
    };
    let mut before_recreation = pristine_1_6();
    sentinel(&mut before_recreation);
    let mut after_recreation = pristine_1_6();
    sentinel(&mut after_recreation);
    for doc in [&before_recreation, &after_recreation] {
        assert!(
            doc.validate_invariants()
                .unwrap_err()
                .starts_with("generations[\"orders\"].topic_id \"AAAAAAAAAAAAAAAAAAAAAQ\""),
            "arm 38 refuses the sentinel"
        );
    }
    assert!(matches!(
        between(Some(&before_recreation), &after_recreation, "orders"),
        Generation::NotEstablished(Unestablished::PreviousUnread(_))
    ));
    // M2: this capture's after-read failed.
    let mut current = pristine_1_6();
    let e = generation(&mut current, "orders");
    e.topic_id_after = None;
    e.topic_id_after_reason = Some("readFailed".into());
    assert_eq!(current.validate_invariants(), Ok(()));
    assert_eq!(
        between(Some(&pristine_1_6()), &current, "orders"),
        Generation::NotEstablished(Unestablished::CurrentAfterUnread("readFailed".into()))
    );
    // The control.
    assert_eq!(
        between(Some(&pristine_1_6()), &pristine_1_6(), "orders"),
        Generation::Same {
            topic_id: ID_A.into()
        }
    );
}

// ===========================================================================
// PROD-04.1: format 1.7.0, `consumer_positions` (arms 30-35) and the
// positions document it binds (CP-1 to CP-14)
// ===========================================================================

/// A partition's facts, as JSON.
fn facts(
    p: u32,
    marks: Option<(i64, i64)>,
    after: Option<(i64, i64)>,
    archived: Option<(i64, i64)>,
) -> serde_json::Value {
    let mut v = serde_json::json!({"partition": p, "observed": marks.is_some()});
    if let Some((a, b)) = marks {
        v["log_start"] = a.into();
        v["high_watermark"] = b.into();
    }
    if let Some((a, b)) = after {
        v["log_start_after"] = a.into();
        v["high_watermark_after"] = b.into();
    }
    if let Some((a, b)) = archived {
        v["archived_first"] = a.into();
        v["archived_last"] = b.into();
    }
    v
}

/// The positions document of [`pristine_cp`]. `orders` has three partitions,
/// the third added during the capture; `payments` one. Group `billing` is
/// captured (a committed position on each topic, `orders` 1 never committed,
/// `orders` 2 not observed), `audit` captured with a position beyond the end,
/// one before the log start and a failed read.
fn pristine_document() -> PositionsDocument {
    serde_json::from_value(serde_json::json!({
        "format_version": "1.0.0",
        "backup_id": "logweir-backup-01J8Z9QK7V",
        "run_id": "01J8Z9QK7V6M3F2R5T8W1XB0CD",
        "topics": {
            "orders": {
                "partitions": [
                    facts(0, Some((0, 20)), Some((0, 24)), Some((0, 23))),
                    facts(1, Some((5, 9)), Some((5, 9)), Some((5, 8))),
                    facts(2, None, Some((0, 3)), Some((0, 2))),
                ],
                "changed_during_capture": false
            },
            "payments": {
                "partitions": [facts(0, Some((0, 7)), None, Some((0, 6)))],
                "changed_during_capture": false
            }
        },
        "groups": {
            "audit": {
                "positions": [
                    {"topic": "orders", "partition": 0, "status": "excluded", "position": 21, "reason": "PositionBeyondEnd"},
                    {"topic": "orders", "partition": 1, "status": "captured", "position": 2, "coverage": "beforeLogStart"},
                    {"topic": "orders", "partition": 2, "status": "notObserved", "reason": "PartitionAddedDuringCapture"},
                    {"topic": "payments", "partition": 0, "status": "failed", "reason": "Unstable"}
                ],
                "no_committed_position": 0
            },
            "billing": {
                "positions": [
                    {"topic": "orders", "partition": 0, "status": "captured", "position": 12, "coverage": "withinArchive"},
                    {"topic": "orders", "partition": 2, "status": "notObserved", "reason": "PartitionAddedDuringCapture"},
                    {"topic": "payments", "partition": 0, "status": "captured", "position": 7, "coverage": "atArchiveEnd"}
                ],
                "no_committed_position": 1
            }
        }
    }))
    .expect("a positions document")
}

/// A 1.7.0 receipt that satisfies every arm, bound to
/// [`pristine_document`]: `billing` and `audit` captured with the counts the
/// document's positions give, `share-1` excluded GroupTypeNotCaptured, `gone`
/// excluded GroupNotFound, `hidden` failed NotVisibleToPrincipal.
fn pristine_cp() -> BackupReceipt {
    let mut doc = pristine_1_3();
    doc.format_version =
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_CONSUMER_POSITIONS.to_string();
    let block = serde_json::json!({
        "observed_from": "2026-09-09T11:02:15Z",
        "observed_to": "2026-09-09T11:02:16Z",
        "listing": "complete",
        "document": {"key": "", "sha256": "", "bytes": 0},
        "groups": {
            "billing": {
                "outcome": "captured", "group_type": "classic", "state": "Stable",
                "listed_state": "Stable", "members": 2, "active": true,
                "counts": {"related": 2, "not_related": 0, "never_committed": 1,
                           "beyond_end": 0, "failed": 0, "not_observed": 1}
            },
            "audit": {
                "outcome": "captured", "group_type": "consumer", "state": "Empty",
                "listed_state": "Empty", "members": 0, "active": false,
                "counts": {"related": 0, "not_related": 1, "never_committed": 0,
                           "beyond_end": 1, "failed": 1, "not_observed": 1}
            },
            "share-1": {"outcome": "excluded", "reason": "GroupTypeNotCaptured", "group_type": "other"},
            "gone": {"outcome": "excluded", "reason": "GroupNotFound"},
            "hidden": {"outcome": "failed", "reason": "NotVisibleToPrincipal"}
        }
    });
    doc.consumer_positions = Some(serde_json::from_value(block).expect("a block"));
    bind(&mut doc, &pristine_document());
    doc
}

/// Bind `positions` into `doc`'s block, as the runner does, and return the
/// document's exact bytes.
fn bind(doc: &mut BackupReceipt, positions: &PositionsDocument) -> Vec<u8> {
    let bytes = positions.to_bytes().expect("serialises");
    let key = logweir_core::consumer_positions::document_key(&doc.backup_id, &doc.run_id);
    cp(doc).document = DocumentRef {
        key,
        sha256: logweir_core::ids::sha256_prefixed(&bytes),
        bytes: bytes.len() as u64,
    };
    bytes
}

/// The document arms over `positions`, bound into `doc` first.
fn check(mut doc: BackupReceipt, positions: &PositionsDocument) -> Result<(), String> {
    let bytes = bind(&mut doc, positions);
    assert_eq!(
        doc.validate_invariants(),
        Ok(()),
        "the receipt itself holds"
    );
    doc.validate_consumer_positions_document(&bytes, positions)
}

fn cp(doc: &mut BackupReceipt) -> &mut logweir_core::consumer_positions::ConsumerPositions {
    doc.consumer_positions.as_mut().expect("a 1.7.0 block")
}

fn group<'a>(doc: &'a mut BackupReceipt, id: &str) -> &'a mut GroupSnapshot {
    cp(doc).groups.get_mut(id).unwrap()
}

fn pentry<'a>(d: &'a mut PositionsDocument, group: &str, i: usize) -> &'a mut PositionEntry {
    &mut d.groups.get_mut(group).unwrap().positions[i]
}

#[test]
fn a_1_7_0_receipt_and_its_document_satisfy_every_invariant() {
    assert_eq!(pristine_cp().validate_invariants(), Ok(()));
    assert_eq!(check(pristine_cp(), &pristine_document()), Ok(()));
    // Without the block the document is decided exactly as 1.3.0 was.
    let mut doc = pristine_cp();
    doc.consumer_positions = None;
    assert_eq!(doc.validate_invariants(), Ok(()));
}

#[test]
fn arm_30_refuses_consumer_positions_under_the_minor_before_it() {
    use logweir_core::backup_receipt::CONSUMER_POSITIONS_SINCE_MINOR as SINCE;
    let mut doc = pristine_cp();
    let before = format!("1.{}.0", SINCE - 1);
    doc.format_version = before.clone();
    assert_eq!(
        doc.validate_invariants().unwrap_err(),
        format!(
            "consumer_positions is present but format_version {before:?} predates it: the field \
             is defined from 1.{SINCE}.0"
        )
    );
}

/// Arm 31, including the review's L1: a capture that ends before it starts.
#[test]
fn arm_31_refuses_a_backwards_window_a_listing_outside_the_set_or_no_group() {
    let tail = "the capture ends at or after it starts, the listing is \"complete\" or \
                \"notComplete\", and at least one group is recorded";
    let mut doc = pristine_cp();
    cp(&mut doc).listing = "partial".into();
    assert_eq!(
        doc.validate_invariants().unwrap_err(),
        format!(
            "consumer_positions records listing \"partial\", 5 group(s) and a capture that ends \
             at or after it starts: {tail}"
        )
    );
    let mut doc = pristine_cp();
    cp(&mut doc).groups.clear();
    assert_eq!(
        doc.validate_invariants().unwrap_err(),
        format!(
            "consumer_positions records listing \"complete\", 0 group(s) and a capture that ends \
             at or after it starts: {tail}"
        )
    );
    let mut doc = pristine_cp();
    cp(&mut doc).observed_to = "2000-01-01T00:00:00Z".parse().unwrap();
    assert_eq!(
        doc.validate_invariants().unwrap_err(),
        format!(
            "consumer_positions records listing \"complete\", 5 group(s) and a capture that ends \
             before it starts: {tail}"
        )
    );
    // The boundary: a capture that ends as it starts.
    let mut doc = pristine_cp();
    cp(&mut doc).observed_to = cp(&mut doc).observed_from;
    assert_eq!(doc.validate_invariants(), Ok(()));
}

#[test]
fn arm_32_refuses_a_document_reference_that_is_not_this_runs() {
    let want = |key: &str, sha: &str, bytes: u64| {
        format!(
            "consumer_positions.document is {key:?} with sha256 {sha:?} over {bytes} bytes: the \
             positions document is \"logweir/backups/logweir-backup-01J8Z9QK7V/\
             01J8Z9QK7V6M3F2R5T8W1XB0CD.consumer-positions.json\", its digest \"sha256:\" and 64 \
             lowercase hex digits, over at least one byte"
        )
    };
    let mut doc = pristine_cp();
    let good = cp(&mut doc).document.clone();
    let other = "logweir/backups/logweir-backup-01J8Z9QK7V/another-run.consumer-positions.json";
    cp(&mut doc).document.key = other.into();
    assert_eq!(
        doc.validate_invariants().unwrap_err(),
        want(other, &good.sha256, good.bytes)
    );
    let mut doc = pristine_cp();
    let upper = good.sha256.to_uppercase().replace("SHA256:", "sha256:");
    cp(&mut doc).document.sha256 = upper.clone();
    assert_eq!(
        doc.validate_invariants().unwrap_err(),
        want(&good.key, &upper, good.bytes)
    );
    let mut doc = pristine_cp();
    cp(&mut doc).document.bytes = 0;
    assert_eq!(
        doc.validate_invariants().unwrap_err(),
        want(&good.key, &good.sha256, 0)
    );
}

#[test]
fn arm_33_refuses_an_outcome_or_reason_that_does_not_fit() {
    let want = |id: &str, outcome: &str, reason: &str| {
        format!(
            "consumer_positions.groups[{id:?}] has outcome {outcome} and reason {reason}: the \
             outcome is \"captured\", \"excluded\" or \"failed\", a reason is present exactly \
             when it is not \"captured\", and it is one this format defines for that outcome"
        )
    };
    for (id, outcome, reason, why) in [
        (
            "gone",
            "\"excluded\"",
            Some("NotVisibleToPrincipal"),
            "a failure reason",
        ),
        (
            "hidden",
            "\"failed\"",
            Some("GroupNotFound"),
            "an exclusion reason",
        ),
        ("hidden", "\"failed\"", None, "no reason"),
        (
            "billing",
            "\"captured\"",
            Some("GroupNotFound"),
            "a reason on a capture",
        ),
        (
            "gone",
            "\"lost\"",
            Some("GroupNotFound"),
            "an outcome outside the set",
        ),
    ] {
        let mut doc = pristine_cp();
        let g = group(&mut doc, id);
        g.outcome = outcome.trim_matches('"').into();
        g.reason = reason.map(str::to_string);
        assert_eq!(
            doc.validate_invariants().unwrap_err(),
            want(
                id,
                outcome,
                &reason.map_or("absent".to_string(), |r| format!("{r:?}"))
            ),
            "{why}"
        );
    }
}

/// Arm 34, with one row per clause of the captured branch (the review's A6:
/// a captured group missing `active`, `members`, `listed_state` or its
/// counts), the review's M2 (a captured group described `Dead` with no
/// member), and the two other branches.
#[test]
fn arm_34_refuses_fields_that_do_not_fit_the_outcome() {
    let tail = "a captured group records a type of \"classic\" or \"consumer\", both states \
                from the closed set, its members, active and counts over at least one \
                partition, and is never \"Dead\" with no member; a GroupTypeNotCaptured group \
                records group_type \"other\" and nothing else; any other group records none of \
                them";
    let billing =
        |ty: &str, state: &str, listed: &str, members: &str, active: &str, counts: &str| {
            format!(
            "consumer_positions.groups[\"billing\"] is \"captured\" with group_type {ty}, state \
             {state}, listed_state {listed}, members {members}, active {active} and counts \
             {counts}: {tail}"
        )
        };
    type Mutate = fn(&mut GroupSnapshot);
    let rows: [(Mutate, String); 8] = [
        (
            |g| g.group_type = Some("share".into()),
            billing(
                "\"share\"",
                "\"Stable\"",
                "\"Stable\"",
                "2",
                "true",
                "over 4 partition(s)",
            ),
        ),
        (
            |g| g.state = Some("Assigning".into()),
            billing(
                "\"classic\"",
                "\"Assigning\"",
                "\"Stable\"",
                "2",
                "true",
                "over 4 partition(s)",
            ),
        ),
        (
            |g| g.listed_state = None,
            billing(
                "\"classic\"",
                "\"Stable\"",
                "absent",
                "2",
                "true",
                "over 4 partition(s)",
            ),
        ),
        (
            |g| g.members = None,
            billing(
                "\"classic\"",
                "\"Stable\"",
                "\"Stable\"",
                "absent",
                "true",
                "over 4 partition(s)",
            ),
        ),
        (
            |g| g.active = None,
            billing(
                "\"classic\"",
                "\"Stable\"",
                "\"Stable\"",
                "2",
                "absent",
                "over 4 partition(s)",
            ),
        ),
        (
            |g| g.counts = None,
            billing(
                "\"classic\"",
                "\"Stable\"",
                "\"Stable\"",
                "2",
                "true",
                "absent",
            ),
        ),
        (
            |g| g.counts = Some(PositionCounts::default()),
            billing(
                "\"classic\"",
                "\"Stable\"",
                "\"Stable\"",
                "2",
                "true",
                "over 0 partition(s)",
            ),
        ),
        (
            |g| {
                g.state = Some("Dead".into());
                g.members = Some(0);
            },
            billing(
                "\"classic\"",
                "\"Dead\"",
                "\"Stable\"",
                "0",
                "true",
                "over 4 partition(s)",
            ),
        ),
    ];
    for (mutate, want) in rows {
        let mut doc = pristine_cp();
        mutate(group(&mut doc, "billing"));
        assert_eq!(doc.validate_invariants().unwrap_err(), want);
    }
    // The control of the vanished clause: `Dead` WITH a member is described
    // as the broker answered, and the receipt holds.
    let mut doc = pristine_cp();
    let g = group(&mut doc, "billing");
    g.state = Some("Dead".into());
    assert_eq!(doc.validate_invariants(), Ok(()));
    // A failed group carrying counts; an excluded one without `other`.
    let mut doc = pristine_cp();
    group(&mut doc, "hidden").counts = Some(PositionCounts::default());
    assert_eq!(
        doc.validate_invariants().unwrap_err(),
        format!(
            "consumer_positions.groups[\"hidden\"] is \"failed\" with group_type absent, state \
             absent, listed_state absent, members absent, active absent and counts over 0 \
             partition(s): {tail}"
        )
    );
    let mut doc = pristine_cp();
    group(&mut doc, "share-1").group_type = None;
    assert!(doc.validate_invariants().unwrap_err().starts_with(
        "consumer_positions.groups[\"share-1\"] is \"excluded\" with group_type absent"
    ));
}

#[test]
fn arm_35_refuses_an_active_flag_the_states_do_not_derive() {
    let mut doc = pristine_cp();
    let g = group(&mut doc, "audit");
    g.listed_state = Some("PreparingRebalance".into());
    assert_eq!(
        doc.validate_invariants().unwrap_err(),
        "consumer_positions.groups[\"audit\"].active is false but its states \"Empty\" and \
         \"PreparingRebalance\" say true: a group is active unless both its states are \"Empty\" \
         or \"Dead\""
    );
}

#[test]
fn the_consumer_positions_arms_run_after_every_earlier_arm() {
    let mut doc = pristine_cp();
    cp(&mut doc).listing = "partial".into();
    topic(&mut doc, "orders").partitions = Some(0);
    let got = doc.validate_invariants().unwrap_err();
    assert!(
        got.starts_with("topic_configuration[\"orders\"] records partitions 0"),
        "{got}"
    );
}

/// The written version and the first minor that defines the block move
/// together, or every receipt that selects groups refuses itself at arm 30.
#[test]
fn the_written_version_defines_consumer_positions() {
    use logweir_core::backup_receipt::{
        format_version_for, CONSUMER_POSITIONS_SINCE_MINOR, FORMAT_VERSION_WITH_CONSUMER_POSITIONS,
    };
    let minor: u64 = FORMAT_VERSION_WITH_CONSUMER_POSITIONS
        .split('.')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(minor, CONSUMER_POSITIONS_SINCE_MINOR);
    let mut archive = pristine().archive;
    let mut auth = pristine().source.auth;
    assert_eq!(
        format_version_for(&archive, true, false, &auth, false, true),
        FORMAT_VERSION_WITH_CONSUMER_POSITIONS
    );
    archive.manifest_version_id = Some("v1".into());
    auth.mode = "mtls".into();
    assert_eq!(
        format_version_for(&archive, true, false, &auth, false, true),
        FORMAT_VERSION_WITH_CONSUMER_POSITIONS
    );
    assert_eq!(
        format_version_for(&archive, true, false, &auth, false, false),
        "1.4.0"
    );
    // A run with every block (integration batch 5): the highest version that
    // defines them all, never a lower one — PROD-04.1's over PROD-01.4a's
    // `generations` and PROD-03.0's `schema_dependency`.
    assert_eq!(
        format_version_for(&archive, true, true, &auth, true, true),
        FORMAT_VERSION_WITH_CONSUMER_POSITIONS
    );
    assert_eq!(
        format_version_for(&archive, true, true, &auth, true, false),
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_GENERATIONS
    );
}

// --------------------------------------------- the positions document, CP-1..14

/// `validate_consumer_positions_document` has exactly fourteen `return
/// Err(format!(` statements, one per arm — the slice
/// `scripts/check-invariant-corpus.sh` takes for the document's arm list.
#[test]
fn validate_consumer_positions_document_has_exactly_fourteen_return_err_statements() {
    let src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/backup_receipt.rs"
    ))
    .expect("read crates/logweir-core/src/backup_receipt.rs");
    let lines: Vec<&str> = src.lines().collect();
    let start = lines
        .iter()
        .position(|l| l.contains("pub fn validate_consumer_positions_document"))
        .expect("backup_receipt.rs declares validate_consumer_positions_document");
    let end = start
        + 1
        + lines[start + 1..]
            .iter()
            .position(|l| *l == "    }")
            .expect("the function closes on a line that is exactly four spaces and a brace");
    let body: Vec<&str> = lines[start..=end]
        .iter()
        .copied()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect();
    assert_eq!(body.join("\n").matches("return Err(format!(").count(), 14);
}

#[test]
fn cp_1_refuses_a_document_for_a_receipt_that_selected_no_group() {
    let mut doc = pristine_1_3();
    doc.format_version =
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_CONSUMER_POSITIONS.into();
    let positions = pristine_document();
    let bytes = positions.to_bytes().unwrap();
    assert_eq!(
        doc.validate_consumer_positions_document(&bytes, &positions)
            .unwrap_err(),
        "the receipt of run \"01J8Z9QK7V6M3F2R5T8W1XB0CD\" records no consumer_positions block, \
         so it binds no positions document: only a backup that selected consumer groups writes \
         one"
    );
}

/// CP-2, and the review's A8: the digest moves with ONE position, though the
/// counts do not, so a document whose counts agree is still not the one the
/// receipt signed.
#[test]
fn cp_2_refuses_a_document_the_receipt_did_not_sign_even_with_equal_counts() {
    let mut doc = pristine_cp();
    let bytes = bind(&mut doc, &pristine_document());
    let signed = cp(&mut doc).document.clone();
    let mut other = pristine_document();
    pentry(&mut other, "billing", 0).position = Some(13);
    let other_bytes = other.to_bytes().unwrap();
    assert_ne!(
        logweir_core::ids::sha256_prefixed(&other_bytes),
        signed.sha256,
        "one position moved the digest"
    );
    // The counts are the same: both are `withinArchive`.
    assert_eq!(
        PositionCounts::of(&other.groups["billing"]),
        PositionCounts::of(&pristine_document().groups["billing"])
    );
    assert_eq!(
        doc.validate_consumer_positions_document(&other_bytes, &other)
            .unwrap_err(),
        format!(
            "the positions document is {} over {} bytes but the receipt binds {} over {} bytes: \
             it is not the document this receipt signed",
            logweir_core::ids::sha256_prefixed(&other_bytes),
            other_bytes.len(),
            signed.sha256,
            signed.bytes
        )
    );
    // A trailing byte the parse does not see is not the signed document.
    let mut padded = bytes.clone();
    padded.push(b'\n');
    assert!(doc
        .validate_consumer_positions_document(&padded, &pristine_document())
        .unwrap_err()
        .contains("it is not the document this receipt signed"));
    assert_eq!(
        doc.validate_consumer_positions_document(&bytes, &pristine_document()),
        Ok(())
    );
}

#[test]
fn cp_3_refuses_a_document_for_another_run_or_format() {
    let mut other = pristine_document();
    other.run_id = "another-run".into();
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        "the positions document is format \"1.0.0\" for backup \"logweir-backup-01J8Z9QK7V\" run \
         \"another-run\" but the receipt is backup \"logweir-backup-01J8Z9QK7V\" run \
         \"01J8Z9QK7V6M3F2R5T8W1XB0CD\": a format-1 positions document names its receipt's own \
         backup and run"
    );
    let mut other = pristine_document();
    other.format_version = "2.0.0".into();
    assert!(check(pristine_cp(), &other)
        .unwrap_err()
        .starts_with("the positions document is format \"2.0.0\""));
}

#[test]
fn cp_4_refuses_topics_that_are_not_the_named_set() {
    let mut other = pristine_document();
    other.topics.remove("payments");
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        "the positions document's topics cover {\"orders\"} but the named topic set is \
         {\"orders\", \"payments\"}"
    );
}

#[test]
fn cp_5_refuses_partitions_out_of_order() {
    let mut other = pristine_document();
    other.topics.get_mut("orders").unwrap().partitions[1].partition = 2;
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        "the positions document's topics[\"orders\"].partitions[1] is partition 2: each topic \
         lists its partitions from 0, one entry each, in order"
    );
}

#[test]
fn cp_6_refuses_marks_that_are_not_well_formed() {
    let want = "the positions document's topics[\"orders\"].partitions[0] records marks that \
                are not well formed: a log start and its high watermark are recorded together \
                with 0 <= log start <= high watermark, the archived range is recorded whole with \
                0 <= first <= last, and a partition the capture did not observe has no \
                group-capture marks";
    type Mutate = fn(&mut PartitionFacts);
    let rows: [Mutate; 5] = [
        |p| p.high_watermark = None,
        |p| p.log_start = Some(-1),
        |p| p.log_start_after = Some(30),
        |p| p.archived_first = Some(24),
        |p| p.observed = false,
    ];
    for mutate in rows {
        let mut other = pristine_document();
        mutate(&mut other.topics.get_mut("orders").unwrap().partitions[0]);
        assert_eq!(check(pristine_cp(), &other).unwrap_err(), want);
    }
}

#[test]
fn cp_7_refuses_a_changed_flag_the_marks_do_not_derive() {
    let mut other = pristine_document();
    other
        .topics
        .get_mut("payments")
        .unwrap()
        .changed_during_capture = true;
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        "the positions document's topics[\"payments\"].changed_during_capture is true but its \
         marks say false: a topic changed during the capture exactly when a mark read after the \
         engine is below the one read at group capture"
    );
    let mut other = pristine_document();
    other.topics.get_mut("orders").unwrap().partitions[0].high_watermark_after = Some(19);
    assert!(check(pristine_cp(), &other).unwrap_err().starts_with(
        "the positions document's topics[\"orders\"].changed_during_capture is false"
    ));
}

#[test]
fn cp_8_refuses_positions_for_a_group_that_was_not_captured_or_none_for_one_that_was() {
    let mut other = pristine_document();
    let billing = other.groups["billing"].clone();
    other.groups.insert("hidden".into(), billing);
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        "the positions document records positions for the groups {\"audit\", \"billing\", \
         \"hidden\"} but the receipt's captured groups are {\"audit\", \"billing\"}: it records \
         exactly the captured groups"
    );
    let mut other = pristine_document();
    other.groups.remove("audit");
    assert!(check(pristine_cp(), &other)
        .unwrap_err()
        .starts_with("the positions document records positions for the groups {\"billing\"}"));
}

#[test]
fn cp_9_refuses_a_position_on_a_changed_topic_and_a_blame_without_a_change() {
    let want = |id: &str, outcome: &str, reason: &str, changed: &str| {
        format!(
            "consumer_positions.groups[{id:?}] is {outcome:?} with reason {reason} while the \
             topics that changed during the capture are {changed}: a group holding a position \
             on such a topic fails GenerationChangedDuringCapture, and no group fails so when \
             none changed"
        )
    };
    // `payments` regressed; `audit`'s only `payments` entry is a failed read,
    // `billing` holds a position there.
    let mut other = pristine_document();
    let t = other.topics.get_mut("payments").unwrap();
    t.partitions[0].high_watermark_after = Some(3);
    t.partitions[0].log_start_after = Some(0);
    t.changed_during_capture = true;
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want("billing", "captured", "absent", "{\"payments\"}")
    );
    // The review's A3: a kept EXCLUDED position on a changed topic counts too.
    let mut other = pristine_document();
    let t = other.topics.get_mut("orders").unwrap();
    t.partitions[1].high_watermark_after = Some(8);
    t.changed_during_capture = true;
    let mut doc = pristine_cp();
    let mut only_excluded = other.clone();
    only_excluded.groups.remove("billing");
    group(&mut doc, "billing").outcome = "failed".into();
    group(&mut doc, "billing").reason = Some("GenerationChangedDuringCapture".into());
    let g = group(&mut doc, "billing");
    (
        g.group_type,
        g.state,
        g.listed_state,
        g.members,
        g.active,
        g.counts,
    ) = (None, None, None, None, None, None);
    let a = only_excluded.groups.get_mut("audit").unwrap();
    a.positions.remove(1);
    a.no_committed_position = 1;
    group(&mut doc, "audit").counts = Some(PositionCounts::of(&only_excluded.groups["audit"]));
    assert_eq!(
        check(doc, &only_excluded).unwrap_err(),
        want("audit", "captured", "absent", "{\"orders\"}")
    );
    // Blamed with no topic changed.
    let mut doc = pristine_cp();
    group(&mut doc, "hidden").reason = Some("GenerationChangedDuringCapture".into());
    assert_eq!(
        check(doc, &pristine_document()).unwrap_err(),
        want(
            "hidden",
            "failed",
            "\"GenerationChangedDuringCapture\"",
            "{}"
        )
    );
}

/// CP-10, the review's L4: a topic no read gave a partition for.
#[test]
fn cp_10_refuses_a_capture_over_an_unread_topic_and_a_blame_with_every_topic_read() {
    let want = |id: &str, outcome: &str, reason: &str, unread: &str| {
        format!(
            "consumer_positions.groups[{id:?}] is {outcome:?} with reason {reason} while the \
             topics whose partitions were never read are {unread}: a group is captured only when \
             every named topic's partitions were read, and fails PartitionsNotRead only when one \
             was not"
        )
    };
    let mut other = pristine_document();
    other.topics.get_mut("payments").unwrap().partitions.clear();
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want("audit", "captured", "absent", "{\"payments\"}")
    );
    let mut doc = pristine_cp();
    group(&mut doc, "hidden").reason = Some("PartitionsNotRead".into());
    assert_eq!(
        check(doc, &pristine_document()).unwrap_err(),
        want("hidden", "failed", "\"PartitionsNotRead\"", "{}")
    );
}

#[test]
fn cp_11_refuses_entries_out_of_place_a_dropped_unobserved_partition_or_a_wrong_count() {
    let want = |listed: usize, first: &str, missing: usize, none: u32| {
        format!(
            "the positions document's groups[\"billing\"] lists {listed} position(s) (first out \
             of place: {first}), leaves {missing} unobserved partition(s) out and counts {none} \
             without a committed position over 4 partition(s): a captured group lists, topics \
             in name order and partitions in order, each partition of a named topic at most once \
             and every one the capture did not observe, and counts every other partition as \
             without a committed position"
        )
    };
    // The never-committed partition dropped from the count: absence would
    // otherwise read as nothing at all.
    let mut other = pristine_document();
    other
        .groups
        .get_mut("billing")
        .unwrap()
        .no_committed_position = 0;
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(3, "none", 0, 0)
    );
    // Out of order.
    let mut other = pristine_document();
    other
        .groups
        .get_mut("billing")
        .unwrap()
        .positions
        .swap(0, 1);
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(3, "1, \"orders\":0", 0, 1)
    );
    // A partition no topic has.
    let mut other = pristine_document();
    pentry(&mut other, "billing", 2).partition = 1;
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(3, "2, \"payments\":1", 0, 1)
    );
    // The unobserved partition left out (and counted instead).
    let mut other = pristine_document();
    let b = other.groups.get_mut("billing").unwrap();
    b.positions.remove(1);
    b.no_committed_position = 2;
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(2, "none", 1, 2)
    );
}

#[test]
fn cp_12_refuses_a_status_position_or_reason_that_does_not_fit() {
    let want = |i: usize, status: &str, position: &str, reason: &str| {
        format!(
            "the positions document's groups[\"billing\"].positions[{i}] has status {status:?}, \
             position {position} and reason {reason}: the status is \"captured\", \"excluded\", \
             \"failed\" or \"notObserved\", a position is present exactly when it is \"captured\" \
             or \"excluded\" and is never negative, a reason exactly when it is not \"captured\" \
             and from that status's set, and \"notObserved\" is exactly a partition the capture \
             did not observe"
        )
    };
    // The old dense status: absence is no longer an entry, so it is no word.
    let mut other = pristine_document();
    let e = pentry(&mut other, "billing", 0);
    (e.status, e.position, e.coverage) = ("noCommittedPosition".into(), Some(0), None);
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(0, "noCommittedPosition", "0", "absent")
    );
    let mut other = pristine_document();
    pentry(&mut other, "billing", 0).position = Some(-1);
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(0, "captured", "-1", "absent")
    );
    let mut other = pristine_document();
    pentry(&mut other, "billing", 0).reason = Some("Unstable".into());
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(0, "captured", "12", "\"Unstable\"")
    );
    // `notObserved` on a partition the capture read.
    let mut other = pristine_document();
    let e = pentry(&mut other, "billing", 2);
    (e.status, e.position, e.coverage, e.reason) = (
        "notObserved".into(),
        None,
        None,
        Some("TopicNotObserved".into()),
    );
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(2, "notObserved", "absent", "\"TopicNotObserved\"")
    );
}

#[test]
fn cp_13_refuses_a_coverage_the_facts_do_not_derive() {
    let want = |g: &str, i: usize, status: &str, coverage: &str, position: &str, derived: &str| {
        format!(
            "the positions document's groups[{g:?}].positions[{i}] is {status:?} with coverage \
             {coverage} at position {position}, but its partition's facts make it {derived}: a \
             coverage word is recorded exactly on a captured position, and a kept position's \
             coverage, or its PositionBeyondEnd, follows from the marks and the archived range"
        )
    };
    // A position beyond the end recorded as captured.
    let mut other = pristine_document();
    let e = pentry(&mut other, "audit", 0);
    (e.status, e.reason, e.coverage) = ("captured".into(), None, Some("beyondArchive".into()));
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(
            "audit",
            0,
            "captured",
            "\"beyondArchive\"",
            "21",
            "PositionBeyondEnd"
        )
    );
    // A relation the facts do not support.
    let mut other = pristine_document();
    pentry(&mut other, "billing", 0).coverage = Some("atArchiveEnd".into());
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(
            "billing",
            0,
            "captured",
            "\"atArchiveEnd\"",
            "12",
            "withinArchive"
        )
    );
    // A coverage word on an excluded position.
    let mut other = pristine_document();
    pentry(&mut other, "audit", 0).coverage = Some("beyondArchive".into());
    assert_eq!(
        check(pristine_cp(), &other).unwrap_err(),
        want(
            "audit",
            0,
            "excluded",
            "\"beyondArchive\"",
            "21",
            "PositionBeyondEnd"
        )
    );
}

#[test]
fn cp_14_refuses_counts_the_positions_do_not_give() {
    let mut doc = pristine_cp();
    let c = group(&mut doc, "billing").counts.as_mut().unwrap();
    c.related = 1;
    c.not_related = 1;
    assert_eq!(
        check(doc, &pristine_document()).unwrap_err(),
        "consumer_positions.groups[\"billing\"].counts are 1 related, 1 not related, 1 never \
         committed, 0 beyond the end, 0 failed, 1 not observed but its positions count 2 \
         related, 0 not related, 1 never committed, 0 beyond the end, 0 failed, 1 not observed: \
         the receipt counts what the positions document records"
    );
}
