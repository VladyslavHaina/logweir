//! **Task 11's default-set half: the two claims about G-PITR that can be
//! checked without Docker, checked as tests rather than by eye** (critique A
//! F26).
//!
//! `e2e/tests/pitr_boundary.rs` is `#![cfg(feature = "e2e")]`, so it is not
//! compiled into the default set at all and a `#[test]` asserting on its `T`
//! constant cannot live beside it. Both tests here therefore read their
//! subject **as text**: that is the only device available, and it is the same
//! device `crates/logweir/tests/two_reader_parity.rs` already uses to keep the
//! four interpreter resolvers in agreement and
//! `crates/logweir/tests/windowed_reconciliation.rs` uses to count code sites
//! of `x-original-offset`.
//!
//! Neither test dials anything and neither reads a clock; each opens one file.
//! Global Constraint 22's 15 s per-test bound is not in danger, and the two
//! rows below are the ONLY additions Task 11 makes to
//! `cargo test --workspace` (everything else it lands is `e2e`-gated).
//!
//! # Why a document claim is a test here
//!
//! `docs/stability.md` is where this project records what it has measured, and
//! a recorded measurement nobody checks is how a ledger comes to say "closed"
//! about something that regressed (STANDING RULE 21's own reasoning, and spec
//! §17 round two). `stability_md_records_the_pitr_result_and_residual_3` is
//! therefore an assertion with a needle, not an eyeball check — and it covers
//! **two** obligations, because `docs/stability.md` admits exactly one editor
//! per dispatch slot (STANDING RULE 17, chain S) and Task 11 is slot 12's
//! owner: G-PITR's own recorded result, and the one-sentence residual-3 answer
//! Task 8 filed here.

use std::path::{Path, PathBuf};

/// The workspace root — `crates/logweir/` up two.
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root resolves")
}

fn read(rel: &str) -> String {
    let p: PathBuf = root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", Path::display(&p)))
}

/// The G-PITR fixture's epoch is a FIXED LITERAL and is never read from the
/// clock.
///
/// `1_760_000_000_000` ms is 2025-10-09T08:53:20Z. A fixture bound to the wall
/// clock asserts a different thing on every run: the boundary record, the
/// window floor and the restored set would all move, the expected target topic
/// name (`restore-<YYYYmmddTHHMMSSZ>-…`) would change run to run, and a
/// failure could never be reproduced from the report of it. This is the mutant
/// the brief names — replace the fixed `T` with a clock read — and this test is
/// what kills it.
///
/// # Why the DECLARATION and not the literal
///
/// Asserting that the file merely *contains* `1_760_000_000_000` is not an
/// oracle: the literal survives in a doc comment, so `T` can be derived from a
/// clock — `SystemTime::now()`, say, which this file cannot forbid because
/// `pitr_boundary.rs` needs it for the `backup_id` nonce — while both needles
/// still pass. That mutant was run and it SURVIVED (Task 11 review, mutant
/// M6b, 2 passed, rc 0). The needle is therefore the declaration line itself,
/// copied byte-for-byte out of `pitr_boundary.rs`: no clock read can be
/// spelled on the right-hand side of `const T: i64 = 1_760_000_000_000;`,
/// because a `const` is evaluated at compile time and `SystemTime::now()` is
/// not a `const fn`.
///
/// The one remaining evasion — keep the declaration, add a clock-reading
/// helper, and repoint every *use* at it — leaves `T` unused. Nothing in this
/// workspace denies warnings in a test build, so `dead_code` alone does not
/// fail it; that evasion is caught by `cargo clippy --features e2e -- -D
/// warnings`, the gate every task and review runs by hand, not by this test.
///
/// The `Utc::now` needle stays: it is the spelling a clock read would most
/// likely take (`pitr_boundary.rs` already has `chrono` through the harness,
/// and `harness::rfc3339` is the only other place in that suite that renders
/// an instant), and it also covers the fixture's other instants —
/// `PIT_RFC3339` and the two 1 ms neighbours — which are literals for the same
/// reason `T` is.
#[test]
fn pitr_fixture_uses_a_fixed_epoch() {
    const FIXTURE: &str = "e2e/tests/pitr_boundary.rs";
    /// The declaration as `pitr_boundary.rs` spells it, copied from the file.
    const DECLARATION: &str = "const T: i64 = 1_760_000_000_000;";
    let src = read(FIXTURE);

    assert!(
        src.contains(DECLARATION),
        "{FIXTURE} must DECLARE its point in time as `{DECLARATION}` \
         (2025-10-09T08:53:20Z), spelled exactly so. The literal appearing somewhere in the \
         file is not enough — it survives in a comment while `T` is derived from a clock — \
         and a boundary fixture whose boundary moves proves nothing reproducible"
    );
    assert!(
        !src.contains("Utc::now()"),
        "{FIXTURE} reads the wall clock. The G-PITR fixture's instants are literals: \
         `point_in_time`, the record one millisecond before it and the record one \
         millisecond after it are the assertion, and a clock read makes every one of them \
         unreproducible"
    );
}

/// `docs/stability.md` records the G-PITR result — with the engine version and
/// digest it was measured against — and carries Task 8's one-sentence
/// residual-3 answer.
///
/// Each needle is its own assertion so a reviewer can see which claim went
/// missing, and the digest needle is read from
/// `third_party/kafka-backup-binary.digest` rather than written out here: the
/// document must name the engine THIS tree pins (Global Constraint 7 — pinning
/// is by digest, never by tag), so a digest bump that leaves the recorded
/// measurement behind fails this test instead of silently ageing.
#[test]
fn stability_md_records_the_pitr_result_and_residual_3() {
    const DOC: &str = "docs/stability.md";
    let doc = read(DOC);

    // 1 — WHY the guard exists: upstream's six assertion-free tests, cited by
    //     file and line.
    assert!(
        doc.contains("pitr_accuracy.rs:25,40,54,66,78,86"),
        "{DOC} must cite upstream's six PITR tests by line — that citation is the whole \
         reason G-PITR is Logweir's to prove"
    );
    assert!(
        doc.contains("contains zero assertions"),
        "{DOC} must say what is wrong with those six tests: the file contains zero \
         assertions, so the boundary has no executable evidence upstream"
    );

    // 2 — the recorded RESULT, by the name of the test that produced it, and
    //     the measured verdict.
    assert!(
        doc.contains("pitr_boundary_includes_the_record_whose_timestamp_equals_point_in_time"),
        "{DOC} must name the test whose transcript it records, so a reader can re-run it"
    );
    assert!(
        doc.contains("six of the nine records"),
        "{DOC} must record the measured verdict: six of the nine records are at or before \
         the recovery point and exactly those six came back"
    );

    // 3 — the engine it was measured against: version AND digest (GC7, GC8).
    let digest = read("third_party/kafka-backup-binary.digest")
        .trim()
        .to_string();
    assert!(
        doc.contains(&digest),
        "{DOC} must record the engine DIGEST the G-PITR result was measured against \
         ({digest}); a tag would not identify the binary"
    );
    assert!(
        doc.contains("0.21.0"),
        "{DOC} must record the engine version the G-PITR result was measured against"
    );

    // 4 — Task 8's residual 3, in one sentence, filed here because chain S
    //     admits one editor per slot and Task 11 owns slot 12.
    assert!(
        doc.contains("honours a per-topic `message.timestamp.type=CreateTime` override"),
        "{DOC} must carry Task 8's one-sentence residual-3 answer: a broker on \
         log.message.timestamp.type=LogAppendTime honours a per-topic CreateTime override"
    );
    assert!(
        doc.contains("LogAppendTime"),
        "{DOC}'s residual-3 sentence must name the broker setting the question was about"
    );
}
