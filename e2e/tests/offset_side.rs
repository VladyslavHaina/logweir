#![cfg(feature = "e2e")]
//! **Task 9b against the live stack** — the four rendered offset-side keys, the
//! offset report's upload, interface I8's three stdout lines, and one
//! `mode: newTopic` round trip.
//!
//! # Why these rows exist at all, and why they are not optional
//!
//! Plan errata **E2** and **E3** are the same defect twice: a key Logweir
//! rendered into a document the ENGINE loads, which the engine came back and
//! called an unknown key — once at the document's top indent
//! (`strip_offset_headers` on the backup side, E2) and once one indent too
//! shallow (the SASL block, E3, which ran UNAUTHENTICATED behind four "Ignoring
//! unknown config key" lines). Both were invisible to every stub in the tree
//! and both were found by driving the real engine. The rule the controller drew
//! from them is binding here: **any document rendered for the engine gets at
//! least one real-engine end-to-end row.** Task 9b renders four new keys, so
//! `the_engine_accepts_the_four_offset_side_keys` is that row.
//!
//! The four keys were ALSO verified against the pinned engine's own source
//! before this file was written — `third_party/kafka-backup-v0.21.0.tar.gz`,
//! `crates/kafka-backup-core/src/config.rs`: `consumer_group_strategy` at
//! `:773-774` (an `OffsetStrategy`, `#[serde(rename_all = "kebab-case")]` at
//! `:719-726`, so `Skip` is `skip`), `reset_consumer_offsets` at `:853-854`,
//! `offset_report: Option<PathBuf>` at `:857-858` and `auto_consumer_groups`
//! at `:899-900`, all four fields of `RestoreOptions` and therefore under
//! `restore:`. That is a strong static check and it is not a substitute for
//! this file: the engine binary is what decides, and E2's key parsed fine as
//! Rust too.
mod harness;
use harness::*;

/// The four keys, in the document the drill ACTUALLY rendered on this host —
/// not in the golden, which `crates/logweir/tests/restore_mode.rs` reads.
///
/// Each as its own assertion, and each read as a LINE at two spaces, because
/// indentation is half of what erratum E3 was about.
#[test]
fn the_rendered_document_carries_the_four_offset_side_keys() {
    let doc = rendered_restore_yaml();
    for want in [
        "  consumer_group_strategy: skip",
        "  reset_consumer_offsets: false",
        "  auto_consumer_groups: false",
    ] {
        assert!(
            doc.lines().any(|l| l == want),
            "the rendered restore.yaml is missing the exact line {want:?}:\n{doc}"
        );
    }
    let offset_line = doc
        .lines()
        .find(|l| l.trim_start().starts_with("offset_report:"))
        .unwrap_or_else(|| panic!("no offset_report line in:\n{doc}"));
    assert!(
        offset_line.starts_with("  offset_report: \"") && offset_line.ends_with("offsets.json\""),
        "offset_report must be a quoted path at two spaces, got {offset_line:?}"
    );
    // Global Constraint 20 is an ASSERTION in this document now, so the two
    // levers `offset_recovery_requested()` keys off are pinned false and
    // nothing is inherited.
    assert!(!doc.contains("reset_consumer_offsets: true"));
    assert!(!doc.contains("auto_consumer_groups: true"));
}

/// **THE ERRATA-E2/E3 ROW.** The engine LOADS the document Logweir rendered and
/// does not report a single one of the four keys as unknown.
///
/// `validate-restore` reads the config before it does anything else, so a
/// misspelled key or a wrong indent surfaces here as `Ignoring unknown config
/// key <name>` — the exact string E2 and E3 were both found by. A serde TYPE
/// error would instead abort the config load outright, which the exit code
/// below would also catch.
#[test]
fn the_engine_accepts_the_four_offset_side_keys() {
    let doc = rendered_restore_yaml();
    let (code, stdout, stderr) = engine_validate_restore(&doc);
    let both = format!("{stdout}\n{stderr}");
    for key in [
        "consumer_group_strategy",
        "reset_consumer_offsets",
        "auto_consumer_groups",
        "offset_report",
    ] {
        assert!(
            !both.contains(&format!("Ignoring unknown config key {key}"))
                && !both.contains(&format!("Ignoring unknown config key restore.{key}")),
            "the engine dropped `{key}` — plan errata E2/E3 are this exact failure. \
             exit {code:?}\n{both}"
        );
    }
    // A dropped key is the finding this row exists for; a config the engine
    // could not load at all is a stronger one, so the load itself is asserted
    // too. `validate-restore` force-sets `dry_run = true` and writes nothing.
    assert_ne!(
        code,
        Some(2),
        "the engine could not load the rendered config:\n{both}"
    );
}

/// **I8, against the real binary.** A successful `logweir restore run` prints
/// `scorecard-key=`, `sidecar-key=`, `offset-report-key=` as its final three
/// stdout lines, and the third one names an object that is really in the
/// bucket.
///
/// `crates/logweir/tests/restore_mode.rs::
/// the_runner_prints_its_three_evidence_keys_last` proves the same contract
/// over doubles by re-execing its own test binary; this row proves it over the
/// shipped binary, a real broker, a real engine and a real bucket.
#[test]
fn a_successful_restore_prints_three_keys_and_uploads_the_offset_report() {
    let r = drill_run(&spec_default());
    let stdout = r.out.stdout_utf8();
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "the drill must pass for its stdout to be a successful run's:\n{}",
        r.out.stderr_utf8()
    );
    let lines: Vec<&str> = stdout.lines().collect();
    let last3 = &lines[lines.len().saturating_sub(3)..];
    assert!(
        last3[0].starts_with("scorecard-key=logweir/drills/"),
        "line 1 of 3, got {:?} in:\n{stdout}",
        last3[0]
    );
    assert!(
        last3[1].starts_with("sidecar-key=logweir/drills/"),
        "line 2 of 3, got {:?} in:\n{stdout}",
        last3[1]
    );
    assert!(
        last3[2].starts_with("offset-report-key=logweir/drills/")
            && last3[2].ends_with(".offsets.json"),
        "line 3 of 3, got {:?} in:\n{stdout}",
        last3[2]
    );
    assert!(
        stdout.ends_with(&format!("{}\n{}\n{}\n", last3[0], last3[1], last3[2])),
        "nothing may follow the three keys (plan erratum E4: a controller reads a bounded \
         TAIL of the pod log):\n{stdout}"
    );

    // The third key names an object that EXISTS, and the signed scorecard
    // binds its bytes.
    let run_id = r.run_id();
    let keys = evidence_for_run(&run_id);
    let want = format!("logweir/drills/{run_id}.offsets.json");
    assert!(
        keys.contains(&want),
        "the offset report must be in the bucket at {want}; the pod is deleted and this is the \
         only durable copy. got: {keys:?}"
    );
    let sc = read_scorecard(&r);
    assert_eq!(
        sc["evidence"]["offset_report_key"].as_str(),
        Some(want.as_str()),
        "the SIGNED document names the key"
    );
    assert!(
        sc["evidence"]["offset_report_sha256"]
            .as_str()
            .is_some_and(|d| d.starts_with("sha256:")),
        "and its digest: {}",
        sc["evidence"]
    );
    // Both readers accept the document that carries the pair.
    assert!(logweir_verify(&r).success());
    assert!(python_verify(&r).success());

    // THE SCRATCH CONTROL for review F1, on a real broker: this run's
    // document names the marker topic phase 0 actually verified, and carries
    // no `mode` key at all — `Scratch` is the default and is
    // `skip_serializing_if`-absent on the wire, which is what keeps the three
    // checked-in signed fixtures under `e2e/fixtures/signed/` byte-identical.
    // The `newTopic` arm of the same pair is asserted by
    // `a_new_topic_restore_names_its_topics_and_tears_nothing_down` below.
    assert_eq!(
        sc["target"]["marker_topic"].as_str(),
        Some(MARKER_TOPIC),
        "a scratch run records the marker topic it checked: {}",
        sc["target"]
    );
    assert!(
        sc["target"].get("mode").is_none(),
        "scratch is the default and adds no key: {}",
        sc["target"]
    );
}

/// **One `mode: newTopic` round trip on a real cluster.**
///
/// Four claims, all of which are meaningless against a double: the topics are
/// named `restore-<YYYYmmddTHHMMSSZ>-<topic>`; the marker topic is not
/// required (it is deleted first); phase 9 tears NOTHING down; and a second
/// run of the same plan is refused, exit 3, because the topics it would write
/// now exist.
///
/// It cleans up after itself with `kafka-topics --delete`, by exact name.
/// Nothing else in the suite would: `delete_all_drill_topics` is scoped to the
/// `drill-` prefix, and phase 9 deliberately leaves these behind.
///
/// Cleanup is a `Drop` guard, `RestoredBrokerState`, so it runs on EVERY exit
/// path including a panicking assertion. See that type for why the alternative
/// — never deleting the marker topic — is not open to this row.
#[test]
fn a_new_topic_restore_names_its_topics_and_tears_nothing_down() {
    let mut spec = spec_default();
    spec["target"]["mode"] = "newTopic".into();
    // The recovery point IS the naming input, so it has to be inside the
    // archive's window — `spec_default` has just bound that window to the
    // newest source record.
    let point_in_time = spec["sample"]["window_end"].clone();
    // The `restore:` block does not exist in `examples/drill.yaml` at all, so
    // it is built whole rather than indexed into: `serde_yaml::Value`'s
    // `IndexMut` on a Null node would panic.
    spec["restore"] = serde_yaml::from_str::<serde_yaml::Value>(&format!(
        "point_in_time: {}",
        point_in_time.as_str().expect("an RFC 3339 window end")
    ))
    .expect("a one-field restore block");
    // The expected names come from the PRODUCT's own rule — the spec goes
    // through the very `DrillSpec` the binary parses and the very
    // `target_topic_prefix` phase 0 maps through, which for this spec (mode
    // `newTopic`, no `topic_naming`) is `default_topic_prefix` of the recovery
    // point. Fix round 1, F5a: this was derived by `replace(['-', ':'], "")`
    // plus `replace(".000", "")` over the RFC 3339 string, and `harness::rfc3339`
    // emits `SecondsFormat::Millis` UNCONDITIONALLY, so the guard stripped a
    // literal `.000` that is present about one run in a thousand while the
    // product formats `%Y%m%dT%H%M%SZ` and never emits a fraction. A test that
    // re-implements the rule it is checking asserts a property of itself.
    let parsed: logweir_core::spec::DrillSpec = serde_yaml::from_value(spec.clone())
        .expect("the harness spec is the one the binary parses, as a DrillSpec");
    let prefix = logweir_core::spec::target_topic_prefix(&parsed);
    let expected: Vec<String> = ["orders", "payments"]
        .iter()
        .map(|t| format!("{prefix}{t}"))
        .collect();

    // Armed BEFORE the marker is deleted, so even a panic inside
    // `delete_marker_topic` itself puts the broker back.
    let _restored = RestoredBrokerState {
        targets: expected.clone(),
    };
    // `newTopic` skips the marker check, so prove it by removing the marker.
    delete_marker_topic();

    let r = drill_run(&spec);
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "a newTopic restore must pass with no marker topic:\n{}",
        r.out.stderr_utf8()
    );

    // 1 — the names.
    for t in &expected {
        assert!(
            topic_exists(t),
            "expected the newTopic restore to create `{t}`; it names topics \
             restore-<YYYYmmddTHHMMSSZ>-<topic>"
        );
    }
    // 2 — the SIGNED document says which prefix it mapped through.
    let sc = read_scorecard(&r);
    assert_eq!(
        sc["target"]["topic_mapping_prefix"].as_str(),
        Some(prefix.as_str()),
        "the scorecard records the prefix this run mapped through, not the scratch one"
    );
    // …and it says WHICH MODE it was in, and names NO marker topic (review
    // F1). This is the reviewer's live counterexample turned into a permanent
    // row: `delete_marker_topic()` above removed `logweir.scratch` from this
    // very cluster, and the shipped document still reported
    // `target.marker_topic: "logweir.scratch"` — a field whose own doc comment
    // means "phase 0 verified this topic exists on an allowlisted cluster".
    assert_eq!(
        sc["target"]["mode"].as_str(),
        Some("newTopic"),
        "the signed document names the mode it ran in: {}",
        sc["target"]
    );
    assert!(
        sc["target"].get("marker_topic").is_none(),
        "a newTopic run verified no marker topic, so its document carries no such KEY \
         (not a null one): {}",
        sc["target"]
    );

    // 3 — phase 9 tore nothing down: the topics are still there, and the
    //     teardown attestation says so with the mode on it.
    let att_key = format!("logweir/drills/{}.teardown.json", r.run_id());
    assert!(
        evidence_for_run(&r.run_id()).contains(&att_key),
        "phase 9 still ATTESTS in newTopic mode; it just deletes nothing"
    );

    // 4 — a second run of the same plan is refused, exit 3, naming the topic
    //     and the cluster.
    let again = drill_run(&spec);
    let e = again.out.stderr_utf8();
    let code = again.out.status.code();
    assert_eq!(code, Some(3), "the second run must be REFUSED:\n{e}");
    assert!(
        e.contains("already exists on cluster")
            && e.contains(
                "appending into a half-populated topic produces a restore that reconciles \
                 against records it did not write"
            ),
        "spec §6.1's own sentence, with the cluster named:\n{e}"
    );
}

/// Puts the SHARED broker state `a_new_topic_restore_names_its_topics_and_tears_nothing_down`
/// perturbs back, on **every** exit path.
///
/// **Why a `Drop` guard and not simply never deleting the marker topic:**
/// deleting `logweir.scratch` IS one of that row's four claims — `newTopic`
/// skips phase 0's marker check, and the only way to prove a check was skipped
/// is to make it fail if it ran. So the row must delete, and therefore it must
/// restore unconditionally. Fix round 1, F5b: cleanup used to be a closure
/// called on the early-return path and again just before the last assertion,
/// with three assertions in between it did not cover. One panicking assertion
/// left the broker with `logweir.scratch` absent and two `restore-<pit>-*`
/// topics behind (`delete_all_drill_topics` is `drill-` scoped), which turned
/// one red row into four in the review's first run of this file. `#[test]`
/// unwinds — this workspace sets no `panic = "abort"` — so `Drop` runs on the
/// panicking path too, and no later e2e row depends on this row reaching its
/// end.
struct RestoredBrokerState {
    targets: Vec<String>,
}

impl Drop for RestoredBrokerState {
    fn drop(&mut self) {
        for t in &self.targets {
            let _ = kafka_topics(&[
                "--bootstrap-server",
                "kafka-broker-1:9094",
                "--delete",
                "--topic",
                t,
            ]);
        }
        // `recreate_marker_topic` asserts, and a panic escaping `drop` DURING
        // an unwind aborts the process — which would destroy the test report
        // that says why the row failed. So it is caught here and reported.
        if std::panic::catch_unwind(recreate_marker_topic).is_err() {
            eprintln!(
                "[e2e] FAILED to restore the marker topic `{MARKER_TOPIC}` — later rows in \
                 this suite will refuse at phase 0 until `just e2e-up` recreates it"
            );
        }
    }
}
