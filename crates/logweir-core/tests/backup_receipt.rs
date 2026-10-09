//! `BackupReceipt::validate_invariants` has exactly TWENTY-ONE arms — the four
//! SELF-CONTRADICTION invariants and, since Task 5b fix round 1, the one
//! CLOSED VALUE SET (`source.auth.mode`), plus since FX-4 the six arms (6-11)
//! that read ONLY the 1.1.0 `config_coverage` block, plus since PROD-05.1 the
//! eight (12-19) that run only on the 1.3.0 `topic_configuration` block and
//! the two (20-21) over its `owner_detection` — and each one refuses with an
//! exact message.
//!
//! The four and the five are asserted SEPARATELY and on purpose:
//! `arm_cases()` carries the four self-contradiction arms and
//! `backup_receipt_refuses_each_self_contradiction_arm_with_its_exact_message` closes over them, while
//! `validate_invariants_has_exactly_twenty_three_return_err_statements` closes over
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
    TopicConfiguration, TopicOwner,
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
/// `validate_invariants_has_exactly_twenty_three_return_err_statements` pins the
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
            format_version_for(&doc.archive, false, &auth, false),
            RECEIPT_FORMAT_VERSION
        );
        assert_eq!(
            format_version_for(&pinned, false, &auth, false),
            FORMAT_VERSION_WITH_MANIFEST_VERSION
        );
        for archive in [&doc.archive, &pinned] {
            assert_eq!(
                format_version_for(archive, true, &auth, false),
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
                    format_version_for(archive, topic_configuration, &auth, false),
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

/// **THE TOTAL.** `validate_invariants` has exactly twenty-three refusing statements.
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
fn validate_invariants_has_exactly_twenty_three_return_err_statements() {
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
        total, 23,
        "BackupReceipt::validate_invariants has {total} `return Err(format!(` \
         statement(s), not 23. Every one of them needs a per-arm test in this file with \
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
/// `validate_invariants_has_exactly_twenty_three_return_err_statements`.
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
    assert_eq!(format_version_for(&archive, true, &auth, false), "1.3.0");
    assert_eq!(format_version_for(&archive, false, &auth, false), "1.1.0");
    archive.manifest_version_id = Some("v1".into());
    assert_eq!(format_version_for(&archive, true, &auth, false), "1.3.0");
    assert_eq!(format_version_for(&archive, false, &auth, false), "1.2.0");
}
