//! `render_backup` and the four rails of GC18(c).
//!
//! Every fixture in this file dials `kafka-broker-1:9094` and
//! `http://minio:9000` — the CONTAINER-side names — and never a dial token
//! (`crates/logweir/tests/no_network_in_unit_tests.rs`'s `DIAL_TOKENS`). This
//! file constructs no client of any kind, so it is deliberately not a member
//! of chain N and belongs in no `ALLOWED` entry (STANDING RULE 18). The
//! host-side spelling is asserted ABSENT below by looking for the substring
//! `localhost`, rather than by writing the endpoint out, for exactly that
//! reason.
use logweir_core::engine::{AuthRender, BackupPlan, StorageUrl};
use logweir_engine_oso::render_backup::{self, RenderError};

/// The container-side shape, field-for-field the harness's checked-in
/// `e2e/compose/config/backup-drill.yaml:14-39` — the config that actually
/// manufactured the archive every drill in this tree restores from.
fn plan() -> BackupPlan {
    BackupPlan {
        backup_id: "drill-demo".into(),
        // The INTERNAL listener. Under `e2e/compose/docker-compose.yml`, port
        // 9092 on the broker is the EXTERNAL listener and advertises a
        // host-side name, so a container on `kafka-net` would resolve it to
        // itself and never reach the broker — see backup-drill.yaml:6-13,
        // which writes that lesson out in full.
        source_bootstrap: vec!["kafka-broker-1:9094".into()],
        source_auth: AuthRender::Plaintext,
        topics: vec!["orders".into(), "payments".into()],
        storage: StorageUrl::S3 {
            bucket: "kafka-backups".into(),
            prefix: "drill-demo".into(),
            region: Some("us-east-1".into()),
            endpoint: Some("http://minio:9000".into()),
            path_style: true,
            allow_http: true,
        },
        compression: "zstd".into(),
        segment_max_records: 1000,
        segment_max_bytes: 10_485_760,
        max_concurrent_partitions: 3,
    }
}

const GLOBBED: [&str; 6] = ["orders*", "orders?", "events[1]", "a]b", "x{1}", "y}z"];
const PLAIN: [&str; 4] = ["orders", "payments", "orders.v2", "a-b_c"];

/// **G-GLOB** — GC18(c) rail 1, "a mandatory named-topic allowlist with no
/// wildcard". *No wildcard is not enforced by not writing one* (spec §5 M4):
/// upstream's `TopicSelection.include` "supports glob patterns"
/// [U/kafka-backup/crates/kafka-backup-core/src/config.rs:334-343], and this
/// renderer puts `plan.topics` straight into `source.topics.include`, so a
/// topic legitimately NAMED `orders*` or `events[1]` is handed over as a
/// PATTERN and one named entry silently widens to a set.
///
/// All six metacharacters, including the CLOSING halves: `a]b` and `y}z` are
/// refused even though no class or brace was opened, because the question is
/// whether the string is a plain name, not whether it is a well-formed glob.
#[test]
fn topic_include_entries_reject_glob_metacharacters() {
    for bad in GLOBBED {
        let mut p = plan();
        p.topics = vec![bad.to_string()];
        assert_eq!(
            render_backup::render(&p).unwrap_err(),
            RenderError::GlobMetacharacter(bad.to_string()),
            "`{bad}` must be refused as a glob pattern"
        );

        // And it is refused wherever in the list it sits — a clean first
        // entry must not shadow a later one.
        let mut p = plan();
        p.topics = vec!["orders".to_string(), bad.to_string()];
        assert_eq!(
            render_backup::render(&p).unwrap_err(),
            RenderError::GlobMetacharacter(bad.to_string()),
            "`{bad}` must be refused in a trailing position too"
        );
    }

    for good in PLAIN {
        let mut p = plan();
        p.topics = vec![good.to_string()];
        let doc = render_backup::render(&p)
            .unwrap_or_else(|e| panic!("`{good}` is a plain topic name and must render: {e}"));
        assert!(doc.contains(&format!("      - \"{good}\"\n")), "{doc}");
    }

    // The whole plain set together, and through `render_and_digest` as well —
    // the refusal must not live only on the `render` path.
    let mut p = plan();
    p.topics = PLAIN.iter().map(|s| s.to_string()).collect();
    assert!(render_backup::render_and_digest(&p).is_ok());
    p.topics.push("orders*".into());
    assert_eq!(
        render_backup::render_and_digest(&p).unwrap_err(),
        RenderError::GlobMetacharacter("orders*".to_string())
    );
}

/// **GC18(c) rail 2 — the read-only assertion.** A `backup` config that could
/// create a topic or commit a consumer-group offset on the SOURCE cluster is
/// the one thing a backup must never be able to do (Global Constraints 19 and
/// 20). `BackupPlan` carries no field that could produce any of these keys, so
/// the property is structural; this test is what stops the next editor from
/// adding one "for symmetry with restore.yaml".
///
/// Whole-key, per-physical-line, never a substring: the file is scanned the
/// same way `tests/render.rs::assert_no_forbidden_key_line` scans, so a
/// legitimately-rendered key that merely CONTAINS one of these words would not
/// trip it (none does today, and the discipline is what keeps this test and a
/// future explicit-value test from being mutually unsatisfiable).
#[test]
fn backup_document_names_no_write_key() {
    const WRITE_KEYS: [&str; 5] = [
        "reset_consumer_offsets",
        "auto_consumer_groups",
        "create_topics",
        "consumer_group_strategy",
        "topic_mapping",
    ];
    let doc = render_backup::render(&plan()).expect("the fixture renders");
    for line in doc.lines() {
        let key = line
            .trim_start()
            .split(':')
            .next()
            .unwrap_or("")
            .trim()
            .trim_start_matches("- ");
        for k in WRITE_KEYS {
            assert_ne!(key, k, "the backup document emits write key `{k}`:\n{doc}");
        }
    }
    // Belt and braces, and the form the mutant is stated in ("emit
    // `create_topics: false`"): none of the five tokens appears ANYWHERE in
    // this document, at any nesting, in any value. Nothing this renderer
    // legitimately emits contains them, so the stronger scan costs nothing.
    for k in WRITE_KEYS {
        assert!(
            !doc.contains(k),
            "the backup document names `{k}` somewhere:\n{doc}"
        );
    }
    // No consumer-group key of ANY kind.
    assert!(!doc.contains("consumer_group"), "{doc}");
    assert!(!doc.contains("consumer-group"), "{doc}");
}

/// Spec §6.1 M5/N1: `include_offset_headers: true` is rendered EXPLICITLY,
/// never inherited. It stamps `x-original-offset`/`x-original-timestamp` on
/// every archived record, and that header is the ONLY key phase 7 reconciles
/// on (`crates/logweir/src/drill/phase7_verify.rs:243-265`); an archive
/// written without it makes a windowed restore unverifiable while still
/// exiting 0.
///
/// The EXACT line, indentation included, so a key emitted at the wrong nesting
/// level fails too.
///
/// **RENAMED, Task 12 closeout carry (c).** Task 4's fix round 1 (review
/// finding F-1) removed `backup.strip_offset_headers` from this document — it
/// is a RESTORE key the pinned engine drops as unknown, see
/// `the_backup_document_names_no_restore_side_offset_key` below — and the row
/// kept the name it had been landed under, `backup_document_renders_strip_offset_headers_false`,
/// because the test-name gate is additions-only against the base commit. That
/// name described a key this document no longer emits, at a value it no longer
/// has: a reader looking for the `strip_offset_headers` behaviour would have
/// found a test asserting `include_offset_headers`. The closeout ruled the
/// rename in; the assertions are unchanged.
#[test]
fn backup_document_renders_include_offset_headers_true() {
    let doc = render_backup::render(&plan()).expect("the fixture renders");
    assert!(
        doc.lines().any(|l| l == "  include_offset_headers: true"),
        "expected the exact line `  include_offset_headers: true`; got:\n{doc}"
    );
    assert!(
        !doc.contains("include_offset_headers: false"),
        "the only permitted value is true:\n{doc}"
    );
    // The restore-side twin, at ANY value, is refused by the row below; the
    // value bound is kept here so a re-added key at the wrong value fails on
    // both rows rather than only on the golden.
    assert!(
        !doc.contains("strip_offset_headers: true"),
        "`strip_offset_headers` is not a key of the engine's BACKUP config at \
         any value:\n{doc}"
    );
}

/// **Task 4 review, F-1 — the key this document must NOT carry.**
///
/// `strip_offset_headers` is a field of `RestoreOptions`
/// [U:crates/kafka-backup-core/src/config.rs:793-801] and of nothing else.
/// `BackupOptions` (`:404-541`) has exactly ONE offset-header field,
/// `include_offset_headers` (`:455-458`). So the engine at the GC8 floor
/// (`kafka-backup` 0.21.0, the digest in
/// `third_party/kafka-backup-binary.digest`) reads `backup.strip_offset_headers`
/// as an unknown key and drops it with *"Ignoring unknown config key"*
/// [U:crates/kafka-backup-cli/src/commands/config.rs:46] — whereupon
/// `OsoCliEngine`'s `assert_no_dropped_logweir_key` correctly aborts, and
/// `logweir backup run` exited **1 after a complete archive had been
/// written**. Spec §6.1's "kept in both modes" is about the RESTORE document,
/// which still renders it (`render_restore.rs:131-143`) at a key the engine
/// accepts.
///
/// Asserted over the KEY at any nesting and any value, because the failure was
/// never about the value. The end-to-end half — the real, digest-pinned engine
/// accepting this exact document — is
/// `e2e/tests/backup_argv.rs::the_real_engine_accepts_the_rendered_backup_document`;
/// no stub can reject a document, so no stub-based row could ever close this
/// class.
#[test]
fn the_backup_document_names_no_restore_side_offset_key() {
    let doc = render_backup::render(&plan()).expect("the fixture renders");
    assert!(
        !doc.contains("strip_offset_headers"),
        "the backup document names `strip_offset_headers`, which is a RESTORE key: the pinned \
         engine drops it as unknown and assert_no_dropped_logweir_key then aborts the run AFTER \
         the archive has been written (review F-1):\n{doc}"
    );
    // And the restore document is unaffected — the key belongs there, so this
    // is a MOVE of one invariant's other end, not its deletion.
    assert!(
        render_backup::render(&plan())
            .expect("the fixture renders")
            .contains("include_offset_headers: true"),
        "the backup-side half of the invariant must still be rendered explicitly"
    );
}

/// **GC18(c) rail 3** — `purge_topics` and `dry_run` refused IN the rendered
/// `backup.yaml`, at any value (Global Constraint 4).
///
/// Global ruling GR7's shape, and the only shape available: there is no
/// `LOGWEIR_TEST_INJECT_DRY_RUN` and no code path capable of emitting one of
/// the three keys, so the backstop is exercised by handing the post-render
/// scan a CONSTRUCTED document as input. Proving it through `render` is
/// impossible by construction — which is the point, and also why the scan is
/// a separately callable function rather than an inlined block.
#[test]
fn a_forbidden_key_in_a_constructed_backup_document_is_refused() {
    let constructed = "\
mode: backup
backup_id: \"drill-demo\"
dry_run: true
";
    assert_eq!(
        render_backup::scan_rendered_document(constructed).unwrap_err(),
        RenderError::ForbiddenKey("dry_run".into())
    );

    // At any VALUE — the guard refuses on the KEY, never on the value.
    assert_eq!(
        render_backup::scan_rendered_document("dry_run: false\n").unwrap_err(),
        RenderError::ForbiddenKey("dry_run".into())
    );
    // All three keys, and the reported payload is where it was FOUND, which is
    // the only form an operator can act on.
    assert_eq!(
        render_backup::scan_rendered_document("purge_topics: true\n").unwrap_err(),
        RenderError::ForbiddenKey("purge_topics".into())
    );
    assert_eq!(
        render_backup::scan_rendered_document("backup:\n  header_preflight_external: true\n")
            .unwrap_err(),
        RenderError::ForbiddenKey("backup.header_preflight_external".into())
    );

    // FAILS CLOSED. A document the scan could not parse has not been cleared,
    // and must be refused exactly as a hit is — never accepted because the
    // scanner said nothing.
    let err = render_backup::scan_rendered_document("\tnot: [valid, yaml\n").unwrap_err();
    assert!(
        matches!(&err, RenderError::ForbiddenKey(p) if p.starts_with("<not scanned:")),
        "an unparseable document must be refused, not accepted: {err:?}"
    );

    // And the renderer's own output passes it — a backstop that refused the
    // real document would be indistinguishable from a broken renderer.
    let doc = render_backup::render(&plan()).expect("the fixture renders");
    assert_eq!(render_backup::scan_rendered_document(&doc), Ok(()));

    // THE RAIL IS "inside `render_and_digest`", and that half cannot be
    // reached from a value: GR7 forbids an injection hook, and no `BackupPlan`
    // can make `render` emit one of the three keys, so every behavioural probe
    // of `render_and_digest`'s scan returns `Ok` whether the call is there or
    // not. Deleting the call would therefore pass every assertion above. The
    // call site is asserted structurally instead — the same technique
    // `there_is_exactly_one_yaml_escaper` uses, and for the same reason.
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/render_backup.rs"),
    )
    .expect("src/render_backup.rs");
    let body = src
        .split_once("pub fn render_and_digest(")
        .expect("render_and_digest must exist")
        .1;
    let body = &body[..body.find("\npub fn ").unwrap_or(body.len())];
    assert!(
        body.contains("scan_rendered_document(&doc)?"),
        "`render_and_digest` must run the forbidden-key scan over its own output \
         (GC18(c) rail 3); found:\n{body}"
    );
}

/// **One escaper, one place to fix it.** `yaml_scalar` moved out of
/// `render_restore.rs` into `src/yaml.rs`; the failure mode this pins is not a
/// missing function but a THIRD copy — the path of least resistance for a new
/// renderer is to paste the sixteen-line escaper rather than import it, and
/// the copy then does not receive the fix the original gets (Task 3 adds
/// `yaml_scalar_checked` beside the original).
///
/// Source-reading, deliberately: nothing about a *call* can distinguish one
/// definition from three identical ones.
#[test]
fn there_is_exactly_one_yaml_escaper() {
    /// A DEFINITION, not a mention. Comment lines are skipped, because
    /// `src/yaml.rs`'s own module doc and every renderer's prose refer to the
    /// function by name on purpose — a test that made naming the escaper in a
    /// comment a failure would be paid for by deleting the explanation, which
    /// is the opposite of what this pins.
    ///
    /// The token is `fn yaml_scalar(`, WITH the opening parenthesis, and Task
    /// 3 sharpened it from the bare `fn yaml_scalar` for a reason worth
    /// stating: Task 3 added `yaml_scalar_checked` in `src/yaml.rs`, which the
    /// looser token counted as a second escaper. It is not one — it is a
    /// **G-EXP** pre-check that delegates to the single escaper below, and
    /// `tests/expansion.rs::every_renderer_calls_the_checked_escaper` is what
    /// pins the other half (no renderer may call the unchecked
    /// `yaml_scalar(` any more). Widening the expected count to 2 was the
    /// alternative and was rejected: the number would then no longer mean
    /// "there is one escaper", and a genuine copy of `yaml_scalar` would be
    /// absorbed by the allowance.
    fn definitions(body: &str) -> usize {
        body.lines()
            .filter(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && t.contains("fn yaml_scalar(")
            })
            .count()
    }

    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for renderer in [
        "render_restore.rs",
        "render_validation.rs",
        "render_backup.rs",
    ] {
        let body = std::fs::read_to_string(src.join(renderer))
            .unwrap_or_else(|e| panic!("{renderer}: {e}"));
        assert_eq!(
            definitions(&body),
            0,
            "{renderer} defines its own YAML escaper; there is exactly one, in src/yaml.rs, \
             and every renderer imports it with `use crate::yaml::…`"
        );
    }
    let yaml = std::fs::read_to_string(src.join("yaml.rs")).expect("src/yaml.rs must exist");
    assert!(
        yaml.contains("pub(crate) fn yaml_scalar(value: &str) -> String {"),
        "src/yaml.rs must hold the one definition"
    );
    // Exactly one definition in the whole crate, not merely none in the three
    // renderers: a copy in `engine.rs` or `kbak.rs` would be the same defect.
    let mut total = 0;
    let mut stack = vec![src.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "rs") {
                total += definitions(&std::fs::read_to_string(&p).unwrap());
            }
        }
    }
    assert_eq!(
        total, 1,
        "exactly one `fn yaml_scalar(` may exist under crates/logweir-engine-oso/src/"
    );
}

/// The golden is the CONTAINER-side shape, and that is a property of the
/// golden file, not of a comment beside it.
///
/// `logweir` and the engine both run on the HOST in the drill path, so the
/// host-side spelling of this same archive is what `examples/backup.yaml`
/// ships (Task 4) — `examples/drill.yaml:17` and `:71` already use a host-side
/// MinIO endpoint for exactly that reason. Confusing the two is the defect
/// `e2e/compose/config/backup-drill.yaml:6-13` documents at length: a
/// container resolving the host-side name reaches itself and never the broker.
///
/// The absence half is asserted as "no `localhost` anywhere" rather than by
/// writing the host-side endpoint out: that literal is a dial token
/// (STANDING RULE 18) and this file may not name one.
///
/// # It asserts the FIXTURE as well as the FILE, and that is not belt-and-braces
///
/// Measured while applying this task's mutants: with the fixture changed to
/// the host-side endpoint and `just golden` (`INSTA_UPDATE=always`) run, the
/// whole binary went GREEN — insta rewrites `render_backup__backup_s3.snap`
/// in place, and within one `cargo test` process this test can read the file
/// before the golden test has written it. The mutant was caught only on the
/// NEXT run. An order-dependent kill is the shape STANDING RULE 21 calls worse
/// than no guard, so the same property is asserted over the freshly rendered
/// document, which no snapshot-acceptance ordering can move.
#[test]
fn the_backup_golden_is_the_container_side_shape() {
    // (a) The FIXTURE — order-independent, and what an accepted golden is
    // regenerated FROM.
    let doc = render_backup::render(&plan()).expect("the fixture renders");
    assert!(
        doc.contains("http://minio:9000"),
        "the golden's fixture must name the container-side MinIO endpoint:\n{doc}"
    );
    assert!(
        !doc.contains("localhost"),
        "the golden's fixture must name no host-side endpoint — the host-side \
         shape is examples/backup.yaml's (Task 4):\n{doc}"
    );
    assert!(
        doc.contains("kafka-broker-1:9094"),
        "the golden's fixture must name the broker's INTERNAL listener:\n{doc}"
    );

    // (b) The committed FILE, which is what a reviewer reads and what a
    // hand-edit would change without touching the fixture.
    let snap = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/snapshots/render_backup__backup_s3.snap"),
    )
    .expect("the backup_s3 golden must be committed");
    assert!(
        snap.contains("http://minio:9000"),
        "the golden must carry the container-side MinIO endpoint:\n{snap}"
    );
    assert!(
        !snap.contains("localhost"),
        "the golden must carry no host-side name — the host-side shape is \
         examples/backup.yaml's (Task 4):\n{snap}"
    );
    assert!(
        snap.contains("kafka-broker-1:9094"),
        "the golden must carry the broker's INTERNAL listener:\n{snap}"
    );
}

/// The S3 golden. `description` puts the container-side/host-side note in the
/// snapshot's own header, where a reviewer diffing the file sees it.
#[test]
fn backup_yaml_matches_the_golden_for_s3_storage() {
    insta::with_settings!({description => "THE CONTAINER-SIDE SHAPE: field-for-field \
    e2e/compose/config/backup-drill.yaml:14-39, the config that manufactured the drill archive. \
    The HOST-SIDE shape of the same archive is what examples/backup.yaml ships (Task 4) — logweir \
    and the engine run on the host in the drill path, which is why examples/drill.yaml:17 and :71 \
    use a host-side MinIO endpoint. See backup-drill.yaml:6-13."}, {
        insta::assert_snapshot!(
            "backup_s3",
            render_backup::render(&plan()).expect("the fixture renders")
        );
    });
}

/// The filesystem golden. `render_storage_block`'s own doc promises "one arm
/// set, one golden per backend"; the backup document reaches the same shared
/// function, so a second backend here proves the sharing rather than
/// re-pinning S3's arm.
#[test]
fn backup_yaml_matches_the_golden_for_filesystem_storage() {
    let mut p = plan();
    p.storage = StorageUrl::Filesystem {
        path: "/var/backups/drill-demo".into(),
    };
    insta::with_settings!({description => "The filesystem backend, through the SAME \
    render_storage_block the restore and validation documents use — one arm set, one golden per \
    backend."}, {
        insta::assert_snapshot!(
            "backup_filesystem",
            render_backup::render(&p).expect("the fixture renders")
        );
    });
}

/// `render_and_digest` returns the render VERBATIM and hashes the bytes, not a
/// re-serialisation of the plan — the same property `tests/render_equality.rs`
/// pins for the restore document, and for the same reason: the bytes are what
/// the engine loads.
#[test]
fn backup_render_and_digest_is_over_the_rendered_bytes() {
    let p = plan();
    let (doc, digest) = render_backup::render_and_digest(&p).expect("the fixture renders");
    assert_eq!(doc, render_backup::render(&p).unwrap());
    assert_eq!(
        digest,
        logweir_core::ids::sha256_prefixed(render_backup::render(&p).unwrap().as_bytes())
    );
    assert!(digest.starts_with("sha256:"));
}

/// Every free-text value reaches the document through `crate::yaml::
/// yaml_scalar` (Task 3 swaps these sites to `yaml_scalar_checked`). The
/// adversarial payload is a raw newline followed by text that reads as a
/// `dry_run: true` key-line at column 0 — the exact shape that produced a
/// physical forbidden-key line in the restore renderer before it was escaped.
/// One interpolation site at a time, so a regression is individually
/// attributable.
#[test]
fn every_interpolated_backup_value_is_escaped() {
    const PAYLOAD: &str = "a\ndry_run: true";
    const ESCAPED: &str = "a\\ndry_run: true"; // backslash + 'n', not a newline

    let mut cases: Vec<(&str, BackupPlan)> = Vec::new();

    let mut p = plan();
    p.backup_id = PAYLOAD.into();
    cases.push(("backup_id", p));

    let mut p = plan();
    p.source_bootstrap = vec![PAYLOAD.into()];
    cases.push(("bootstrap_servers", p));

    let mut p = plan();
    p.topics = vec![PAYLOAD.into()];
    cases.push(("topics.include", p));

    let mut p = plan();
    p.compression = PAYLOAD.into();
    cases.push(("compression", p));

    let mut p = plan();
    p.storage = StorageUrl::S3 {
        bucket: PAYLOAD.into(),
        prefix: PAYLOAD.into(),
        region: Some(PAYLOAD.into()),
        endpoint: Some(PAYLOAD.into()),
        path_style: false,
        allow_http: false,
    };
    cases.push(("storage", p));

    for (label, p) in cases {
        let doc =
            render_backup::render(&p).unwrap_or_else(|e| panic!("case `{label}` must render: {e}"));
        assert!(
            doc.contains(ESCAPED),
            "case `{label}`: the newline must survive as the two-character YAML escape, \
             not be dropped:\n{doc}"
        );
        assert_eq!(
            render_backup::scan_rendered_document(&doc),
            Ok(()),
            "case `{label}`: an escaped payload must not forge a forbidden key-line:\n{doc}"
        );
    }
}
