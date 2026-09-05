#![cfg(feature = "e2e")]
//! The refusal table as code, against the live stack. This is the artifact that
//! proves Global Constraints 4, 5 and 11, so it gets one `#[test]` per row.
//!
//! Exit 3 means the plan was refused BEFORE anything ran, so every row here
//! also asserts the two consequences that make that claim true: no scorecard
//! file, and nothing new in the evidence bucket.
mod harness;
use harness::*;

/// Every row that ends in exit 3 goes through here, so the message is asserted
/// too — a bare code assertion can pass for the wrong reason.
fn expect_exit_3(mutate: impl Fn(&mut serde_yaml::Value), needle: &str) {
    let mut spec = spec_default();
    mutate(&mut spec);
    let before = list_evidence_bucket();
    let r = drill_run(&spec);
    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(3), "{e}");
    assert!(e.contains(needle), "expected `{needle}` in:\n{e}");
    assert!(
        !r.scorecard.exists(),
        "a guard refusal must write NO scorecard: {} exists",
        r.scorecard.display()
    );
    assert_eq!(
        before,
        list_evidence_bucket(),
        "a guard refusal must upload nothing"
    );
}

// --- the three forbidden keys, at BOTH values: the guard is on the KEY -------
#[test]
fn purge_topics_true_is_refused() {
    expect_exit_3(
        |s| s["engine_overrides"]["purge_topics"] = true.into(),
        "purge_topics",
    );
}
#[test]
fn purge_topics_false_is_refused_exactly_the_same() {
    expect_exit_3(
        |s| s["engine_overrides"]["purge_topics"] = false.into(),
        "purge_topics",
    );
}
#[test]
fn dry_run_true_is_refused() {
    expect_exit_3(
        |s| s["engine_overrides"]["dry_run"] = true.into(),
        "dry_run",
    );
}
#[test]
fn dry_run_false_is_refused_exactly_the_same() {
    expect_exit_3(
        |s| s["engine_overrides"]["dry_run"] = false.into(),
        "dry_run",
    );
}
#[test]
fn header_preflight_external_true_is_refused() {
    expect_exit_3(
        |s| s["engine_overrides"]["header_preflight_external"] = true.into(),
        "header_preflight_external",
    );
}
#[test]
fn header_preflight_external_false_is_refused_exactly_the_same() {
    expect_exit_3(
        |s| s["engine_overrides"]["header_preflight_external"] = false.into(),
        "header_preflight_external",
    );
}
#[test]
fn a_forbidden_key_nested_deep_in_the_spec_is_still_found() {
    expect_exit_3(
        |s| s["engine_overrides"]["a"]["b"]["purge_topics"] = true.into(),
        "purge_topics",
    );
}

// --- mapping, allowlist, marker topic ---------------------------------------

/// The topic-mapping row. RENAMED from the brief's
/// `a_selected_topic_with_no_mapping_entry_is_refused`, applying addendum
/// ruling A5's own standard: `phase0_admit::run` BUILDS the mapping from
/// `spec.source.topics` itself, one entry per selected topic, so
/// `check_topic_mapping_coverage`'s `None` arm is unreachable from a spec and
/// the brief's name could never describe what its body does. What an empty
/// prefix actually produces is the OTHER refusal in the same guard — a mapping
/// that would restore each topic over itself — and that is what this asserts.
#[test]
fn a_topic_mapping_that_maps_a_selected_topic_onto_itself_is_refused() {
    expect_exit_3(
        |s| s["target"]["topic_mapping_prefix"] = "".into(),
        "topic_mapping maps `orders` onto itself",
    );
}

/// The allowlist row of the refusal table: a target whose cluster_id is not in
/// allowed_cluster_ids is refused at phase 0. Driven by swapping the allowlist
/// fixture, which is the only thing that actually changes the guard's input
/// (addendum ruling A5).
#[test]
fn a_cluster_id_absent_from_allowed_cluster_ids_is_refused() {
    let spec = spec_default();
    let before = list_evidence_bucket();
    let r = drill_run_with_allowlist(
        &spec,
        &root().join("e2e/fixtures/allowed-clusters-empty.json"),
    );
    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(3), "{e}");
    assert!(e.contains("allowedClusterIds"), "{e}");
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

#[test]
fn a_missing_marker_topic_is_refused() {
    delete_marker_topic();
    let before = list_evidence_bucket();
    let r = drill_run(&spec_default());
    // Put the cluster back before anything can panic out of this test.
    recreate_marker_topic();

    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(3), "{e}");
    assert!(e.contains("marker topic"), "{e}");
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

// --- the sample anchor ------------------------------------------------------

/// `tail` and `random` select archive records phase 7's leading-range read
/// cannot reach. v0.1 REFUSES them at phase 0 rather than silently sampling
/// `head` under a plan that asked for something else — the scorecard would
/// otherwise record an anchor the drill never applied. See
/// `logweir_core::spec::Anchor` for the measurement that forced this.
#[test]
fn a_sample_anchor_of_random_is_refused_before_anything_runs() {
    expect_exit_3(|s| s["sample"]["anchor"] = "random".into(), "sample.anchor");
}

#[test]
fn a_sample_anchor_of_tail_is_refused_exactly_the_same() {
    expect_exit_3(|s| s["sample"]["anchor"] = "tail".into(), "sample.anchor");
}

/// An anchor that is not one of the three does not reach a guard at all: it is
/// unspellable, so the spec does not parse. That is exit 1 (Logweir could not
/// read its own input), not exit 3, and it is loud either way — the point of
/// the closed enum is that it can never degrade to head-like behaviour the way
/// a free-form string did.
#[test]
fn an_unspellable_sample_anchor_fails_to_parse_and_never_degrades_silently() {
    let mut spec = spec_default();
    spec["sample"]["anchor"] = "sideways".into();
    let before = list_evidence_bucket();
    let r = drill_run(&spec);
    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(1), "{e}");
    assert!(e.contains("drill spec does not parse"), "{e}");
    assert!(e.contains("sideways"), "{e}");
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

// --- approval ----------------------------------------------------------------
#[test]
fn an_approval_over_different_spec_bytes_is_refused() {
    let before = list_evidence_bucket();
    let r = drill_run_with_stale_approval();
    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(3), "{e}");
    assert!(e.contains("plan_hash"), "{e}");
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

#[test]
fn an_approval_signed_by_the_wrong_key_is_refused() {
    let before = list_evidence_bucket();
    let r = drill_run_with_wrong_approver_key();
    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(3), "{e}");
    assert!(e.contains("signature"), "{e}");
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

// --- the exit-4 backstop -----------------------------------------------------

/// Exit 4 leaves the bucket EMPTY of this run's artifacts — signing precedes
/// every put, and a put that fails retracts nothing because nothing was
/// written.
///
/// DRIVEN BY AN UNWRITABLE EVIDENCE SINK, not by the brief's unreadable signing
/// key. Measured: an unreadable signing key exits **1**, not 4, and that is the
/// product's own design rather than a defect — `execute_with` loads the signing
/// key immediately after phase 0 (it needs the public half to decide
/// `approval.self_attested`), and a key that cannot be read there means NO
/// DRILL RAN. Exit 4's own contract, in `DrillError::SigningOrLock`'s doc
/// comment, is "the drill RAN, and its result could not be signed"; reporting
/// that for a mistyped `--signing-key` path would be a false claim. See the
/// sibling test below, which pins the exit-1 behaviour so it cannot drift
/// silently either.
///
/// An evidence bucket that does not exist reaches the genuine exit-4 path:
/// `Store::from_url` builds without a round trip, phase 8 validates, zeroes,
/// serialises and SIGNS, and only then does `put_create_only` fail.
#[test]
fn an_unwritable_evidence_sink_exits_4_and_uploads_nothing() {
    let mut spec = spec_default();
    spec["evidence"]["bucket"] = "logweir-evidence-does-not-exist".into();
    let before = list_evidence_bucket();
    let r = drill_run(&spec);
    assert_eq!(r.out.status.code(), Some(4), "{}", r.out.stderr_utf8());
    assert!(
        !r.scorecard.exists(),
        "the artifact is written from the bytes phase 8 stored; a failed put writes none"
    );
    // The whole-bucket before/after comparison, not `evidence_for_run(run_id)`:
    // this run wrote no scorecard and its structured error line carries no
    // `run_id` field, so there is no id to scope by — and the unscoped
    // comparison is the stronger claim anyway. It says nothing at all was
    // added, which covers both this run's artifacts and any it might have
    // written under someone else's key.
    assert_eq!(
        before,
        list_evidence_bucket(),
        "OSO's own rule, adopted verbatim: a signing/lock failure aborts before anything \
         is uploaded"
    );
    let e = r.out.stderr_utf8();
    assert!(
        e.contains("signing or lock proof failed"),
        "exit 4 must say which contract it broke:\n{e}"
    );
}

/// The brief's `an_unreadable_signing_key_exits_4` row, asserting what the
/// product actually does and why that is right: exit 1, no artifact, and a
/// message naming the key. Pinned so the routing cannot change unnoticed in
/// either direction.
#[test]
fn an_unreadable_signing_key_exits_1_because_no_drill_ran() {
    let bad = demo_dir().join("broken-signing-key.pem");
    std::fs::write(
        &bad,
        b"-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----\n",
    )
    .unwrap();
    let before = list_evidence_bucket();
    let r = drill_run_with_signing_key(&bad);
    let e = r.out.stderr_utf8();
    assert_eq!(
        r.out.status.code(),
        Some(1),
        "an unreadable signing key means no drill ran, so exit 1 (no artifact), \
         never exit 2 (a drill result) and never exit 4 (a drill that ran but is \
         unattested):\n{e}"
    );
    assert!(
        e.contains("not a P-256 or Ed25519 PKCS#8 key"),
        "the failure must name the key, not something else:\n{e}"
    );
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

// --- the schema surface ------------------------------------------------------
#[test]
fn schema_scorecard_prints_the_schema() {
    let o = std::process::Command::new(bin())
        .args(["schema", "scorecard"])
        .output()
        .unwrap();
    assert!(o.status.success());
    assert!(o
        .stdout_utf8()
        .contains("logweir-drill-scorecard-1.0.0.json"));
}

#[test]
fn schema_plan_exits_1_naming_sp3() {
    let o = std::process::Command::new(bin())
        .args(["schema", "plan"])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(1));
    assert!(o.stderr_utf8().contains("SP3"));
}
