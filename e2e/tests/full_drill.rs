#![cfg(feature = "e2e")]
//! The end-to-end suite, against the Task 7b compose stack and a REAL archive
//! produced by `scripts/e2e-seed.sh`.
//!
//! Read `harness/mod.rs`'s module doc first: it says which three things this
//! suite binds at run time (the allowlist, the sample window, the engine's
//! execution route) and why a checked-in value cannot serve for any of them.
mod harness;
use harness::*;

/// The Part C exit criterion, mechanised.
#[test]
fn a_full_drill_produces_a_signed_scorecard_with_real_numbers() {
    let out = drill_run(&spec_default());
    assert_eq!(out.out.status.code(), Some(0), "{}", out.out.stderr_utf8());

    let sc = read_scorecard(&out);
    assert_eq!(sc["outcome"], "pass");
    // 7, NOT 9. `sign_and_publish` clones the scorecard BEFORE pushing phase
    // 8's own record, and phase 8 signs that clone — a document cannot contain
    // the record of its own signing, and phase 9 runs after the bytes are
    // frozen. So the last phase a SIGNED scorecard can attest to is 7. The
    // brief's `9` describes the in-memory document, which is not what is
    // signed, stored, or written to `--out`. Phase 9 attests to itself in a
    // separate signed document (`phase9_teardown::persist`).
    assert_eq!(sc["last_phase_completed"], 7);
    assert!(
        sc["measured"]["rto_seconds"].as_u64().unwrap() > 0,
        "the whole product is that this is a number"
    );
    assert!(sc["measured"]["rpo_seconds"].is_number());
    assert!(
        sc["measured"]["rto_excluding_preflight_seconds"]
            .as_u64()
            .unwrap()
            <= sc["measured"]["rto_seconds"].as_u64().unwrap()
    );
    assert_eq!(sc["engine"]["levers"]["header_preflight"], "honoured");
    assert!(sc["engine"]["digest"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    assert_eq!(sc["source"]["captured_by_logweir"], false);
    assert_eq!(
        sc["measured"]["rpo_source_relative_seconds"],
        serde_json::Value::Null
    );

    // integrity is honest about which level was achieved
    let level = sc["integrity"]["level"].as_str().unwrap();
    assert!(
        level == "byte-fingerprint" || level == "consume-only",
        "{level}"
    );
    // …and this run really did reconcile records, rather than reaching `pass`
    // through an empty sample. 150 = 25 records x 6 partitions.
    assert_eq!(sc["integrity"]["result"], "pass");
    assert!(sc["integrity"]["records_sampled"].as_u64().unwrap() > 0);
    assert_eq!(
        sc["integrity"]["records_sampled"],
        sc["integrity"]["records_sampled_matching"]
    );
    assert_eq!(sc["integrity"]["mismatches"], 0);
    assert_eq!(
        sc["sample"]["records_restored"],
        sc["sample"]["records_expected"]
    );
    assert_eq!(sc["objectives"]["met"], true);

    // both verifiers agree
    assert!(logweir_verify(&out).success());
    assert!(python_verify(&out).success());

    // Topics were auto-created at the MANIFEST partition count. The broker runs
    // with KAFKA_AUTO_CREATE_TOPICS_ENABLE=false and num.partitions=1, so a 3
    // here can only come from `create_topics: true` plus the rendered partition
    // count actually being honoured — implicit creation would give 1.
    assert_eq!(count_partitions("drill-orders"), 3);
    assert_eq!(sc["target_diff"]["level"], "full");
    assert!(sc["target_diff"]["would_create"].is_array());
}

/// The signed artifact must be exactly the bytes that were signed. A verifier
/// that passes over a re-serialised document proves nothing about what the
/// bucket holds.
#[test]
fn the_signed_scorecard_verifies_and_one_flipped_byte_makes_it_stop_verifying() {
    let out = drill_run(&spec_default());
    assert_eq!(out.out.status.code(), Some(0), "{}", out.out.stderr_utf8());
    assert!(logweir_verify(&out).success());
    assert!(python_verify(&out).success());

    let original = std::fs::read_to_string(&out.scorecard).unwrap();
    let tampered = original.replace("\"outcome\": \"pass\"", "\"outcome\": \"fail-objective\"");
    assert_ne!(
        tampered, original,
        "the substitution must actually change the document, or this test proves nothing"
    );
    std::fs::write(&out.scorecard, tampered.as_bytes()).unwrap();
    assert!(
        !logweir_verify(&out).success(),
        "a scorecard whose outcome was edited after signing must not verify"
    );
    assert!(
        !python_verify(&out).success(),
        "the auditor's own verifier must reject it too"
    );
    std::fs::write(&out.scorecard, original.as_bytes()).unwrap();
}

/// A drill against an archive that genuinely cannot restore. Exit 2 with a
/// SIGNED scorecard recording it — never exit 0, never exit 1.
#[test]
fn a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard() {
    let key = corrupt_a_non_oldest_segment();
    let out = drill_run(&spec_default());
    // The archive is put back before any assertion can panic out of the test,
    // so one failing assertion cannot leave every later test running against a
    // broken archive.
    restore_the_corrupted_segment(&key);

    assert_eq!(
        out.out.status.code(),
        Some(2),
        "a drill RESULT, never exit 1: {}",
        out.out.stderr_utf8()
    );
    let sc = read_scorecard(&out);
    assert_eq!(sc["outcome"], "preflight-failed");
    assert_eq!(sc["last_phase_completed"], 5);
    assert_eq!(sc["integrity"]["level"], "not-attempted");
    assert_eq!(sc["measured"]["rto_seconds"], serde_json::Value::Null);
    // The preflight finding itself is INSIDE the signed bytes, not only in the
    // exit code: a `preflight-failed` scorecard that does not say which ground
    // blocked the restore is not evidence.
    let notes = phase(&sc, 5)["notes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(!notes.is_empty(), "phase 5 recorded no findings: {sc}");
    assert!(
        logweir_verify(&out).success(),
        "the failure artifact is still signed and verifiable"
    );
    assert!(python_verify(&out).success());
    // The restore never ran, so nothing was created on the target.
    assert!(
        !topic_exists("drill-orders"),
        "a blocked preflight must not have written to the cluster"
    );
}

/// THE FAILURE THIS SUITE EXISTS FOR: a drill with nothing to restore must
/// never report success.
///
/// This replaces TWO rows of the brief, and says exactly what it does and does
/// not cover.
///
/// It replaces `a_compacted_topic_is_partial_with_a_reason_and_never_a_pass`
/// because that row cannot be driven end to end: `phase7_verify`'s own module
/// doc records that the compacted-topic -> `Partial` path is DELIBERATELY not
/// given a reachable code path in v0.1 ("a known limitation, not a defect, and
/// out of scope to resolve algorithmically"), and
/// `fixtures::verify_outcome_for_compacted_topic` is explicitly "a SHAPE test
/// only … it does not, and cannot yet, describe a path `run` itself takes". A
/// test whose name asserts one thing and whose body can only pass for another
/// is worse than no test (addendum ruling A5, applied).
///
/// It also stands in for the deleted `a_smuggled_dry_run_fails_the_phase_6_
/// post_condition` row. `DrillError::RestoreNoOp` — the phase-6 backstop that
/// catches a restore which ran, exited 0 and left every target partition empty
/// — has NO reachable end-to-end trigger once GR7 removed the injection hook,
/// and Global Constraint 4 admits no debug exception for reintroducing one. So
/// that backstop is covered at unit level only (`crates/logweir/tests/
/// restore_phase.rs`, plus the guard-predicate tests in
/// `logweir-core::guard`), and this test covers the reachable sibling: the
/// drill refuses BEFORE restoring rather than restoring nothing and scoring it.
///
/// MEASURED: exit 1 with NO scorecard, not exit 2. That is the product's own
/// routing — `phase4_sample` raises an operational error, because a plan whose
/// window matches no segment is a plan Logweir cannot execute, not a fact about
/// a restore that ran. What matters for this suite is the invariant in the
/// name: never 0, and never a signed document claiming a pass.
#[test]
fn a_drill_with_nothing_to_restore_is_refused_and_never_reports_a_pass() {
    let out = drill_run(&spec_with_unrestorable_topic());
    let code = out.out.status.code();
    assert_ne!(
        code,
        Some(0),
        "a drill with nothing to restore must never exit 0"
    );
    assert_eq!(
        code,
        Some(1),
        "expected the phase-4 refusal (operational, no artifact): {}",
        out.out.stderr_utf8()
    );
    let e = out.out.stderr_utf8();
    assert!(
        e.contains("overlaps the window") && e.contains("would report a pass that means nothing"),
        "the refusal must say WHY, in the words the guard was written for:\n{e}"
    );
    assert!(
        !out.scorecard.exists(),
        "exit 1 means Logweir could not do its job: NO scorecard may be written"
    );
}

/// THE TEST WHOSE ABSENCE LET D4 THROUGH: the shipped example config, run as
/// written, must either PASS or be REFUSED — it must never quietly report
/// `fail-integrity` about a healthy backup.
///
/// Only `sample.window_start`/`window_end` are bound, because those are the one
/// pair that cannot be checked in: `scripts/e2e-seed.sh` produces records at
/// run time, so any fixed window in a repository file matches no segment.
/// EVERY OTHER FIELD — including `sample.anchor`, which is what this test is
/// really about — comes from `examples/drill.yaml` verbatim, and the anchor the
/// scorecard reports is asserted equal to the value read out of that file.
///
/// Before fix round 1 this failed: the example shipped `anchor: random`, phase
/// 4 fingerprinted archive records spread across the window while phase 7 read
/// the target's first 25, and a byte-for-byte correct restore scored
/// `records_sampled_matching: 12 / 150`, `pass_rate_measured: 0.08`,
/// `outcome: fail-integrity`.
#[test]
fn the_shipped_example_spec_runs_as_written_and_never_reports_a_false_fail() {
    let out = drill_run(&spec_example_with_only_the_window_bound());
    let code = out.out.status.code();
    assert_ne!(
        code,
        Some(2),
        "the shipped example reported a DRILL FAILURE against a healthy archive: {}",
        out.out.stderr_utf8()
    );
    assert_eq!(
        code,
        Some(0),
        "the shipped example must pass (or be refused at 3 — never a false fail): {}",
        out.out.stderr_utf8()
    );

    let sc = read_scorecard(&out);
    assert_eq!(sc["outcome"], "pass");
    assert_eq!(sc["integrity"]["result"], "pass");
    assert_eq!(sc["integrity"]["mismatches"], 0);
    assert_eq!(
        sc["integrity"]["pass_rate_measured"], 1.0,
        "0.08 here is the exact signature of the anchor/consume-range mismatch"
    );
    // The anchor really came from the file, not from this test.
    assert_eq!(
        sc["sample"]["anchor"].as_str().unwrap(),
        example_anchor(),
        "the scorecard must report the anchor examples/drill.yaml actually states"
    );
    assert!(logweir_verify(&out).success());
}

/// The same guarantee for an adopter who OMITS `sample.anchor` entirely — which
/// is how the defect reached everyone who never thought about the field, since
/// the serde default used to be `random`.
#[test]
fn a_spec_that_omits_the_sample_anchor_gets_head_and_passes() {
    let mut spec = spec_default();
    let sample = spec["sample"].as_mapping_mut().expect("sample block");
    sample.remove(serde_yaml::Value::from("anchor"));
    assert!(
        spec["sample"].get("anchor").is_none(),
        "the field must actually be gone for this test to mean anything"
    );
    let out = drill_run(&spec);
    assert_eq!(out.out.status.code(), Some(0), "{}", out.out.stderr_utf8());
    let sc = read_scorecard(&out);
    assert_eq!(sc["sample"]["anchor"], "head", "the default must be head");
    assert_eq!(sc["outcome"], "pass");
}

/// THE MAINLINE EXIT-2 GATE, end to end. Every other non-pass row in this file
/// returns EARLY — the phase-5 block, the phase-4 refusal — so none of them
/// reaches `execute_with`'s final `if sc.outcome != Outcome::Pass`. That
/// comparison is the one that decides whether a drill which ran every phase,
/// was measured and MISSED ITS OBJECTIVE is reported as a success. Task 21a's
/// review found it surviving the whole suite as a mutant; this drives it with a
/// real restore, a real measurement and an impossible RTO.
#[test]
fn a_drill_that_runs_every_phase_and_misses_its_rto_exits_2_with_a_signed_scorecard() {
    let mut spec = spec_default();
    // 0 seconds is unreachable for any real restore, and it is compared against
    // `rto_excluding_preflight_seconds`, which this stack measures at ~6.
    spec["objectives"]["rto_seconds"] = 0.into();
    let out = drill_run(&spec);
    assert_eq!(
        out.out.status.code(),
        Some(2),
        "a measured drill that missed its objective is a RESULT, not a success: {}",
        out.out.stderr_utf8()
    );
    let sc = read_scorecard(&out);
    assert_eq!(sc["outcome"], "fail-objective");
    assert_eq!(sc["objectives"]["met"], false);
    // It really did run the whole way: the restore happened and reconciled.
    assert_eq!(sc["last_phase_completed"], 7);
    assert_eq!(sc["integrity"]["result"], "pass");
    assert!(sc["measured"]["rto_seconds"].is_number());
    assert!(
        logweir_verify(&out).success(),
        "exit 2 promises a SIGNED scorecard, and this is that promise"
    );
    assert!(python_verify(&out).success());
    let run_id = out.run_id();
    assert!(
        !evidence_for_run(&run_id).is_empty(),
        "exit 2 also promises the scorecard is in the bucket"
    );
}

#[test]
fn a_restore_into_an_empty_scratch_target_succeeds() {
    delete_all_drill_topics();
    let out = drill_run(&spec_default());
    assert_eq!(
        out.out.status.code(),
        Some(0),
        "proves create_topics: true and the explicit replication factor are actually rendered: {}",
        out.out.stderr_utf8()
    );
    assert_eq!(count_partitions("drill-payments"), 3);
}

#[test]
fn an_approver_key_equal_to_the_signing_key_succeeds_but_is_labelled() {
    let out = drill_run_with_same_key();
    assert_eq!(out.out.status.code(), Some(0), "{}", out.out.stderr_utf8());
    assert_eq!(read_scorecard(&out)["approval"]["self_attested"], true);
}

/// Phase 9's `delete` policy, end to end. `spec_default` runs with
/// `teardown: keep` so the suite can inspect what the drill built; this is the
/// row that proves the shipped policy actually removes it.
#[test]
fn teardown_delete_removes_the_scratch_topics_the_drill_created() {
    let mut spec = spec_default();
    spec["target"]["teardown"] = "delete".into();
    let out = drill_run(&spec);
    assert_eq!(out.out.status.code(), Some(0), "{}", out.out.stderr_utf8());
    for t in ["drill-orders", "drill-payments"] {
        assert!(
            !topic_exists(t),
            "teardown: delete left {t} behind on the cluster"
        );
    }
    // The teardown attestation is a SECOND signed document, put beside the
    // scorecard; its absence would mean the segregation evidence was never
    // persisted.
    let keys = evidence_for_run(&out.run_id());
    assert!(
        keys.iter().any(|k| k.contains("teardown")),
        "no teardown attestation among {keys:?}"
    );
}

/// Every signed artifact this run produced is in the evidence bucket, under
/// `logweir/` and nowhere else (Global Constraint 6).
#[test]
fn a_passing_drill_puts_its_signed_artifacts_under_the_logweir_prefix_only() {
    let out = drill_run(&spec_default());
    assert_eq!(out.out.status.code(), Some(0), "{}", out.out.stderr_utf8());
    let run_id = out.run_id();
    let keys = evidence_for_run(&run_id);
    assert!(
        keys.iter().any(|k| k.ends_with(&format!("{run_id}.json"))),
        "the scorecard is not in the bucket: {keys:?}"
    );
    assert!(
        keys.iter().any(|k| k.ends_with(&format!("{run_id}.sig"))),
        "the DSSE sidecar is not in the bucket: {keys:?}"
    );
    for k in list_evidence_bucket() {
        assert!(
            k.starts_with("logweir/"),
            "Global Constraint 6: `{k}` is outside the logweir/ prefix"
        );
    }
    // The archive bucket is untouched by Logweir: `manifest.json` is never
    // written by it, and neither is anything else.
    assert!(
        !list_evidence_bucket()
            .iter()
            .any(|k| k.contains("manifest.json")),
        "Logweir must never write a manifest.json"
    );
}

// ---------------------------------------------------------------------------
// The two engine readbacks Task 3's addendum deferred here.
// ---------------------------------------------------------------------------
// progress.md, "Deferred items raised by the addenda pass":
//   "Task 3 -> Task 21c: two engine-readback assertions (an unknown-key warning
//    is read back from whichever stream the engine uses; `validate-restore
//    --format json` stdout parses despite the command logging first) lost their
//    home when Task 3's addendum removed them. Task 21c owns the harness that
//    can host them."
// Both are claims `logweir-engine-oso` marks [VERIFIED] against upstream SOURCE.
// These two tests check them against the running BINARY, which is the only
// thing that can actually settle them.

/// `subprocess::run_engine` scans BOTH streams for
/// "Ignoring unknown config key `<path>`" and its doc comment says stdout is
/// the load-bearing one, because upstream's single `fmt::layer()` has no
/// `with_writer` and defaults to stdout. That is an inference from
/// tracing-subscriber's documented default; this measures it.
///
/// If the warning ever moved to stderr this test still passes (both streams are
/// scanned) but the assertion below names which stream carried it, so the
/// failure message tells the next reader what actually changed.
#[test]
fn an_unknown_config_key_warning_is_read_back_from_the_stream_the_engine_uses() {
    let doc = rendered_restore_yaml();
    // A key Logweir never renders, and NOT one of the three Global Constraint 4
    // forbids — the point is the readback mechanism, not the guard.
    let probe = doc.replace(
        "  create_topics: true\n",
        "  create_topics: true\n  logweir_e2e_probe_unknown_key: 1\n",
    );
    assert_ne!(probe, doc, "the probe key was not inserted");
    let (code, stdout, stderr) = engine_validate_restore(&probe);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    const NEEDLE: &str = "Ignoring unknown config key `restore.logweir_e2e_probe_unknown_key`";
    let on_stdout = stdout.contains(NEEDLE);
    let on_stderr = stderr.contains(NEEDLE);
    assert!(
        on_stdout || on_stderr,
        "the engine emitted no unknown-key warning at all, so \
         `assert_no_dropped_logweir_key` can never fire against the real binary.\n\
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        on_stdout,
        "the warning moved off stdout (stderr: {on_stderr}); \
         `subprocess::run_engine`'s doc comment says stdout is where the real \
         binary puts it, and a scanner that only read stderr would return an \
         empty vector forever"
    );

    // …and the scanner Logweir actually ships extracts the path from it.
    let found = logweir_engine_oso::subprocess::scan_unknown_key_warnings(&stdout);
    assert!(
        found
            .iter()
            .any(|p| p == "restore.logweir_e2e_probe_unknown_key"),
        "scan_unknown_key_warnings did not extract the path from the real \
         binary's own line: {found:?}"
    );
}

/// `OsoCliEngine::preflight` slices `validate-restore`'s stdout from the first
/// `{` because the command LOGS FIRST and prints the JSON report afterwards.
/// A naive `serde_json::from_str(&stdout)` would fail on every real run.
#[test]
fn validate_restore_json_stdout_parses_even_though_the_command_logs_first() {
    let doc = rendered_restore_yaml();
    let (code, stdout, stderr) = engine_validate_restore(&doc);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    let brace = stdout.find('{').expect("a JSON object on stdout");
    assert!(
        brace > 0,
        "this run printed nothing before the JSON, so it cannot demonstrate the \
         hazard the slice exists for; stdout:\n{stdout}"
    );
    assert!(
        serde_json::from_str::<serde_json::Value>(&stdout).is_err(),
        "the whole of stdout parsed as JSON, which would mean the leading log \
         lines are gone and this assertion no longer measures anything"
    );
    let report: serde_json::Value = serde_json::from_str(&stdout[brace..])
        .expect("stdout from the first `{` is a DryRunReport");
    assert_eq!(report["valid"], true);
    assert!(report["segments_to_process"].as_u64().unwrap() > 0);
}

// ---------------------------------------------------------------------------
// A declared gap, pinned so it cannot be forgotten.
// ---------------------------------------------------------------------------

/// `engine_subreport` is NULL on every run today, and the brief's three
/// assertions over it (`oso_evidence_verify`, `body_sha256`'s length,
/// `retained_verbatim`) therefore cannot be met by this task.
///
/// The cause is named and tracked: `OsoCliEngine` does not override
/// `DataEngine::validation_run`, so nothing ever writes a report under
/// `logweir/<run_id>/engine-validation/` for `phase8_score::run` to retain.
/// `crates/logweir-engine-oso/tests/engine.rs` carries an `#[ignore]`d marker
/// test (`oso_cli_engine_must_override_validation_run_once_docker_is_available`)
/// that CI runs on every build, and the override is owed to whoever adds it —
/// not to this task, which owns no file in that crate's engine module.
///
/// The gap is also stated where a READER meets it, not only here:
/// `docs/stability.md` ("`engine_subreport` is always null in v0.1"),
/// `docs/verify-a-scorecard.md`'s `engine_subreport` section, and
/// `e2e/fixtures/signed/README.md` — the last because the checked-in example
/// documents carry a POPULATED block the shipping code cannot emit.
///
/// This test pins the gap so it cannot be mistaken for coverage: it goes RED the
/// day the override lands, at which point the brief's real round-trip assertions
/// belong here, driven by `harness::oso_evidence_verify`.
#[test]
fn the_engine_subreport_is_absent_until_oso_cli_engine_overrides_validation_run() {
    let out = drill_run(&spec_default());
    assert_eq!(out.out.status.code(), Some(0), "{}", out.out.stderr_utf8());
    let sc = read_scorecard(&out);
    // The KEY must be present and null, not merely absent — otherwise this
    // assertion would also hold for a scorecard that dropped the field
    // entirely, and would be pinning nothing.
    assert!(
        sc.as_object().unwrap().contains_key("engine_subreport"),
        "the scorecard no longer carries an engine_subreport field at all"
    );
    assert_eq!(
        sc["engine_subreport"],
        serde_json::Value::Null,
        "an engine sub-report appeared: OsoCliEngine::validation_run must now be \
         implemented. Replace this test with the brief's round-trip assertions — \
         oso_evidence_verify(&sc[\"engine_subreport\"]).success() and a 71-character \
         body_sha256 — and delete the #[ignore]d marker test in \
         crates/logweir-engine-oso/tests/engine.rs."
    );
    // Phase 8's own "no engine validation report under the per-run prefix" note
    // is NOT assertable from the signed document: `sign_and_publish` freezes
    // the scorecard before phase 8's record exists, so the note lands only on
    // the in-memory copy. The absence above is the observable fact.
    assert!(
        phase_outcomes(&sc).keys().max() == Some(&7),
        "the signed document should end at phase 7: {:?}",
        phase_outcomes(&sc)
    );
}
