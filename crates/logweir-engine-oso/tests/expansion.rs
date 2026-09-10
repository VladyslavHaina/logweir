//! Guard **G-EXP**: no unnamed `${` in any rendered document.
//!
//! # The hazard, and why an escaper cannot close it
//!
//! Before parsing, the engine runs a blunt TEXTUAL environment expansion over
//! the whole config: `expand_env_vars` replaces every `${NAME}` with the
//! environment value, and an **unset variable is replaced with the empty
//! string** behind nothing but a `tracing::warn!`
//! [U:crates/kafka-backup-cli/src/commands/config.rs:1-34], wired into every
//! command Logweir drives (`backup.rs:11`, `restore.rs:12`,
//! `validate_restore.rs:8`). So the document Logweir hashes is a *template*
//! and the document the engine executes is its *expansion*:
//!
//! - **With no attacker at all**, a topic legitimately named `orders${X}`
//!   becomes `orders`, and the drill restores a topic the approved plan never
//!   named while every hash still matches.
//! - **With one**, anything able to set an environment variable on the runner
//!   pod changes topics, prefixes, bootstrap servers or the storage prefix
//!   AFTER the hash check passed — `plan_hash`
//!   (`crates/logweir/src/drill/phase1_approval.rs:87`) covers bytes the engine
//!   never executes.
//!
//! `crate::yaml::yaml_scalar`'s own doc comment already said this and then
//! said no YAML-scalar escaper "this one or any other" can close it, because
//! the expansion happens a layer below YAML syntax: `"orders${X}"` is expanded
//! inside its own quotes. Nothing closed it before this file existed. The
//! closure is a REFUSAL, in two legs — `yaml_scalar_checked` over every
//! interpolated VALUE, and `assert_no_unnamed_dollar_brace` over the finished
//! DOCUMENT, before the digest is taken.
//!
//! Every fixture here names the container-side endpoints
//! (`kafka-broker-1:9094`, `http://minio:9000`) and never a dial token
//! (`crates/logweir/tests/no_network_in_unit_tests.rs`'s `DIAL_TOKENS`); this
//! file constructs no client of any kind and belongs in no `ALLOWED` entry
//! (STANDING RULE 18).

use logweir_core::engine::{
    AuthRender, BackupPlan, BackupSetRef, RestorePlan, StorageUrl, WindowFloorSource,
};
use logweir_engine_oso::render_backup::RenderError;
use logweir_engine_oso::yaml::{
    assert_no_unnamed_dollar_brace, PLACEHOLDER_SOURCE_PASSWORD, PLACEHOLDER_TARGET_PASSWORD,
};
use logweir_engine_oso::{render_backup, render_restore, render_validation};
use std::collections::BTreeMap;

/// The adversarial payload, used at every site: a `${X}` naming a variable
/// that is almost certainly unset, which is the case that expands to the
/// empty string rather than to an attacker's value.
const PAYLOAD_SUFFIX: &str = "${X}";

fn backup_plan() -> BackupPlan {
    BackupPlan {
        backup_id: "drill-demo".into(),
        source_bootstrap: vec!["kafka-broker-1:9094".into()],
        source_auth: AuthRender::Plaintext,
        topics: vec!["orders".into()],
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

fn restore_plan() -> RestorePlan {
    let mut topic_mapping = BTreeMap::new();
    topic_mapping.insert("orders".to_string(), "drill-orders".to_string());
    RestorePlan {
        set: BackupSetRef {
            backup_id: "drill-demo".into(),
            manifest_key: "drill-demo/manifest.json".into(),
        },
        storage: StorageUrl::S3 {
            bucket: "kafka-backups".into(),
            prefix: "drill-demo".into(),
            region: Some("us-east-1".into()),
            endpoint: Some("http://minio:9000".into()),
            path_style: true,
            allow_http: true,
        },
        target_bootstrap: vec!["kafka-broker-1:9094".into()],
        target_auth: logweir_core::engine::AuthRender::Plaintext,
        topic_mapping,
        time_window: (
            chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .into(),
            chrono::DateTime::parse_from_rfc3339("2026-01-02T00:00:00Z")
                .unwrap()
                .into(),
        ),
        window_floor_source: WindowFloorSource::ArchiveManifest,
        default_replication_factor: 1,
        checkpoint_state: "/var/lib/logweir/checkpoint".into(),
        checkpoint_interval_secs: 30,
        offset_report: "/var/lib/logweir/01J9X/offsets.json".into(),
    }
}

/// **G-EXP.** Four arms, one module — the brief's named acceptance.
mod rendered_engine_config_has_no_unnamed_dollar_brace {
    use super::*;

    /// Every interpolated value, in all three documents, one site at a time so
    /// a regression is individually attributable. The refusal carries the
    /// offending INPUT VALUE, which is what an operator has to go and fix.
    #[test]
    fn a_topic_named_with_a_dollar_brace_is_refused() {
        // ---- backup: topics.include
        let mut p = backup_plan();
        p.topics = vec![format!("orders{PAYLOAD_SUFFIX}")];
        assert_eq!(
            render_backup::render(&p).unwrap_err(),
            RenderError::DollarBrace(format!("orders{PAYLOAD_SUFFIX}")),
            "a topic named `orders${{X}}` expands to `orders` before the engine parses anything"
        );

        // ---- backup: backup_id
        let mut p = backup_plan();
        p.backup_id = format!("drill{PAYLOAD_SUFFIX}");
        assert_eq!(
            render_backup::render(&p).unwrap_err(),
            RenderError::DollarBrace(format!("drill{PAYLOAD_SUFFIX}"))
        );

        // ---- backup: source.bootstrap_servers
        let mut p = backup_plan();
        p.source_bootstrap = vec![format!("kafka-broker-1{PAYLOAD_SUFFIX}:9094")];
        assert_eq!(
            render_backup::render(&p).unwrap_err(),
            RenderError::DollarBrace(format!("kafka-broker-1{PAYLOAD_SUFFIX}:9094")),
            "a bootstrap server is the field that redirects the whole run"
        );

        // ---- backup: compression, and the shared storage block's prefix
        let mut p = backup_plan();
        p.compression = format!("zstd{PAYLOAD_SUFFIX}");
        assert_eq!(
            render_backup::render(&p).unwrap_err(),
            RenderError::DollarBrace(format!("zstd{PAYLOAD_SUFFIX}"))
        );

        // ---- restore: BOTH SIDES of topic_mapping. The keys become
        // `target.topics.include` entries and the values become
        // `restore.topic_mapping` targets; checking only the keys would leave
        // the half an operator is more likely to template.
        let mut p = restore_plan();
        p.topic_mapping = BTreeMap::from([(
            format!("orders{PAYLOAD_SUFFIX}"),
            "drill-orders".to_string(),
        )]);
        assert_eq!(
            render_restore::render(&p).unwrap_err(),
            RenderError::DollarBrace(format!("orders{PAYLOAD_SUFFIX}")),
            "the topic_mapping KEY side"
        );

        let mut p = restore_plan();
        p.topic_mapping = BTreeMap::from([(
            "orders".to_string(),
            format!("drill-orders{PAYLOAD_SUFFIX}"),
        )]);
        assert_eq!(
            render_restore::render(&p).unwrap_err(),
            RenderError::DollarBrace(format!("drill-orders{PAYLOAD_SUFFIX}")),
            "the topic_mapping VALUE side"
        );

        // ---- restore: target.bootstrap_servers
        let mut p = restore_plan();
        p.target_bootstrap = vec![format!("kafka-broker-1{PAYLOAD_SUFFIX}:9094")];
        assert_eq!(
            render_restore::render(&p).unwrap_err(),
            RenderError::DollarBrace(format!("kafka-broker-1{PAYLOAD_SUFFIX}:9094"))
        );

        // ---- restore: storage.prefix, through the SHARED
        // `render_storage_block` all three documents reach.
        let mut p = restore_plan();
        p.storage = StorageUrl::S3 {
            bucket: "kafka-backups".into(),
            prefix: format!("drill-demo{PAYLOAD_SUFFIX}"),
            region: Some("us-east-1".into()),
            endpoint: Some("http://minio:9000".into()),
            path_style: true,
            allow_http: true,
        };
        assert_eq!(
            render_restore::render(&p).unwrap_err(),
            RenderError::DollarBrace(format!("drill-demo{PAYLOAD_SUFFIX}")),
            "the storage prefix decides WHICH archive is read"
        );
        // The same value, through the backup and validation documents, so the
        // shared block is proved shared rather than assumed.
        let mut b = backup_plan();
        b.storage = p.storage.clone();
        assert_eq!(
            render_backup::render(&b).unwrap_err(),
            RenderError::DollarBrace(format!("drill-demo{PAYLOAD_SUFFIX}"))
        );
        assert_eq!(
            render_validation::render(&p, "r", None).unwrap_err(),
            RenderError::DollarBrace(format!("drill-demo{PAYLOAD_SUFFIX}"))
        );

        // ---- restore: restore.checkpoint_state, a PathBuf rendered as a
        // string like any other.
        let mut p = restore_plan();
        p.checkpoint_state = format!("/var/lib/logweir/{PAYLOAD_SUFFIX}/checkpoint").into();
        assert_eq!(
            render_restore::render(&p).unwrap_err(),
            RenderError::DollarBrace(format!("/var/lib/logweir/{PAYLOAD_SUFFIX}/checkpoint"))
        );

        // ---- and the clean fixtures still render, in all three documents.
        assert!(render_backup::render(&backup_plan()).is_ok());
        assert!(render_restore::render(&restore_plan()).is_ok());
        assert!(render_validation::render(&restore_plan(), "r", Some("KPMG Q3")).is_ok());
    }

    /// The post-render sweep, over a hand-written document no renderer
    /// produced — which is the only way to exercise a `${…}` that arrives from
    /// a renderer's own format string rather than from an interpolated value.
    #[test]
    fn a_third_unnamed_placeholder_is_refused() {
        assert_eq!(
            assert_no_unnamed_dollar_brace("a: ${LOGWEIR_SOURCE_PASSWORD}\nb: ${OTHER}\n"),
            Err(RenderError::UnnamedPlaceholder("${OTHER}".to_string())),
            "one named placeholder does not license a second, unnamed one"
        );
        assert_eq!(
            assert_no_unnamed_dollar_brace("a: ${LOGWEIR_SOURCE_PASSWORD}\n"),
            Ok(()),
            "the same document without the `b:` line passes"
        );

        // A `${` with no closing brace is read to the end of its physical line
        // and refused as what it is, rather than swallowing the rest of the
        // file or being waved through as "not a placeholder".
        assert_eq!(
            assert_no_unnamed_dollar_brace("a: ${OTHER\nb: 1\n"),
            Err(RenderError::UnnamedPlaceholder("${OTHER".to_string()))
        );
        // An unterminated `${` at end of input, with no newline at all.
        assert_eq!(
            assert_no_unnamed_dollar_brace("a: ${"),
            Err(RenderError::UnnamedPlaceholder("${".to_string()))
        );
        // A NEAR MISS is refused: the sweep is byte-equality against the two
        // whole placeholders, never a prefix or a substring, so a variable
        // whose name merely starts with a permitted one does not pass.
        assert_eq!(
            assert_no_unnamed_dollar_brace("a: ${LOGWEIR_SOURCE_PASSWORD_2}\n"),
            Err(RenderError::UnnamedPlaceholder(
                "${LOGWEIR_SOURCE_PASSWORD_2}".to_string()
            ))
        );
        // A lone `$` or a lone `{` is not an expansion and is not refused —
        // `$100` and a JSON-ish `{}` are ordinary text.
        assert_eq!(assert_no_unnamed_dollar_brace("a: \"$100 {}\"\n"), Ok(()));
        // The empty document, and a document with no `$` at all.
        assert_eq!(assert_no_unnamed_dollar_brace(""), Ok(()));
        assert_eq!(assert_no_unnamed_dollar_brace("mode: restore\n"), Ok(()));
    }

    /// The two named password placeholders are the ONE exception to G-EXP, and
    /// the exception is deliberate: the password must not be in the bytes
    /// Logweir hashes, so the document names the variable and the projected
    /// Secret supplies the value. The SASL block that emits them is Task 6's;
    /// this arm pins that the sweep already permits exactly these two and
    /// nothing else, so Task 6 cannot land a document this guard would refuse.
    #[test]
    fn both_named_placeholders_pass() {
        let doc = format!(
            "source:\n  sasl_password: \"{PLACEHOLDER_SOURCE_PASSWORD}\"\n\
             target:\n  sasl_password: \"{PLACEHOLDER_TARGET_PASSWORD}\"\n"
        );
        assert_eq!(assert_no_unnamed_dollar_brace(&doc), Ok(()));
        assert_eq!(PLACEHOLDER_SOURCE_PASSWORD, "${LOGWEIR_SOURCE_PASSWORD}");
        assert_eq!(PLACEHOLDER_TARGET_PASSWORD, "${LOGWEIR_TARGET_PASSWORD}");
        // Each on its own, and adjacent with nothing between them.
        assert_eq!(
            assert_no_unnamed_dollar_brace(PLACEHOLDER_SOURCE_PASSWORD),
            Ok(())
        );
        assert_eq!(
            assert_no_unnamed_dollar_brace(PLACEHOLDER_TARGET_PASSWORD),
            Ok(())
        );
        assert_eq!(
            assert_no_unnamed_dollar_brace(&format!(
                "{PLACEHOLDER_SOURCE_PASSWORD}{PLACEHOLDER_TARGET_PASSWORD}"
            )),
            Ok(())
        );
    }

    /// **The third renderer, which had no sweep at all before this task.**
    /// `render_and_digest` existed in only two of the three renderers, so
    /// G-EXP's document leg missed a third of its surface (critique A F11).
    /// `triggered_by` is the field a cron trigger or a ticket reference lands
    /// in, i.e. the most operator-templated string in the document.
    #[test]
    fn a_validation_document_with_an_unnamed_placeholder_is_refused() {
        let p = restore_plan();
        let err = render_validation::render_and_digest(&p, "r", Some("run-${X}")).unwrap_err();
        assert_eq!(err, RenderError::DollarBrace("run-${X}".to_string()));
        // AND NO DIGEST. The whole point of the ordering is that a caller
        // never receives a digest over a document that did not pass, so the
        // `Err` is asserted to be the ONLY thing returned — there is no tuple
        // to unpack on this path, which is a property of the signature and is
        // re-asserted by value in `the_digest_is_taken_only_over_a_checked_document`.
        assert!(render_validation::render_and_digest(&p, "r", Some("run-${X}")).is_err());
        // The clean case does yield a digest, over the exact rendered bytes.
        let (doc, digest) = render_validation::render_and_digest(&p, "r", Some("KPMG Q3"))
            .expect("the clean fixture renders");
        assert_eq!(
            doc,
            render_validation::render(&p, "r", Some("KPMG Q3")).unwrap()
        );
        assert_eq!(digest, logweir_core::ids::sha256_prefixed(doc.as_bytes()));
        assert!(digest.starts_with("sha256:"));
    }
}

/// For each of the THREE renderers, a document containing `${OTHER}` makes
/// `render_and_digest` return `Err` and produce no digest.
///
/// The `${OTHER}` cannot be injected through an interpolated value — the
/// per-value leg refuses those first, which is the point of having two legs —
/// so this drives the sweep directly over each renderer's own output plus the
/// offending sequence, and separately asserts that each `render_and_digest`
/// calls the sweep BEFORE hashing. That second half is what the "sweep after
/// hashing" mutant breaks: with the calls transposed, a digest comes back for
/// a document carrying `${OTHER}`.
#[test]
fn the_digest_is_taken_only_over_a_checked_document() {
    // (a) The sweep itself refuses the sequence in each of the three real
    // documents, wherever it is appended.
    let b = render_backup::render(&backup_plan()).expect("the fixture renders");
    let r = render_restore::render(&restore_plan()).expect("the fixture renders");
    let v = render_validation::render(&restore_plan(), "r", None).expect("the fixture renders");
    for (label, doc) in [("backup", b), ("restore", r), ("validation", v)] {
        assert_eq!(
            assert_no_unnamed_dollar_brace(&doc),
            Ok(()),
            "`{label}`: the clean document must pass"
        );
        let poisoned = format!("{doc}poisoned: ${{OTHER}}\n");
        assert_eq!(
            assert_no_unnamed_dollar_brace(&poisoned),
            Err(RenderError::UnnamedPlaceholder("${OTHER}".to_string())),
            "`{label}`: a `${{OTHER}}` anywhere in the finished document must be refused"
        );
    }

    // (b) Each `render_and_digest` refuses, and returns NO digest, for a plan
    // that would put a `${…}` in the document. `Result` gives us the strong
    // form of "no digest": there is no value to read on the error path, and
    // `is_err()` over the whole call is the assertion.
    let mut bp = backup_plan();
    bp.topics = vec!["orders${OTHER}".into()];
    assert_eq!(
        render_backup::render_and_digest(&bp).unwrap_err(),
        RenderError::DollarBrace("orders${OTHER}".to_string())
    );
    assert!(render_backup::render_and_digest(&bp).is_err());

    let mut rp = restore_plan();
    rp.topic_mapping = BTreeMap::from([("orders${OTHER}".to_string(), "drill-orders".to_string())]);
    assert_eq!(
        render_restore::render_and_digest(&rp).unwrap_err(),
        RenderError::DollarBrace("orders${OTHER}".to_string())
    );
    assert!(render_restore::render_and_digest(&rp).is_err());

    assert_eq!(
        render_validation::render_and_digest(&restore_plan(), "r", Some("${OTHER}")).unwrap_err(),
        RenderError::DollarBrace("${OTHER}".to_string())
    );
    assert!(render_validation::render_and_digest(&restore_plan(), "r", Some("${OTHER}")).is_err());

    // (c) The ORDER, read from the source. A sweep that runs after
    // `sha256_prefixed` satisfies every assertion above — the refusal still
    // happens — while having already handed a digest over a template to a
    // caller in the passing case. Nothing about a return value can see that,
    // so it is asserted structurally: in each `render_and_digest`, the sweep's
    // line comes before the hash's.
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for renderer in [
        "render_backup.rs",
        "render_restore.rs",
        "render_validation.rs",
    ] {
        let body = std::fs::read_to_string(src.join(renderer))
            .unwrap_or_else(|e| panic!("{renderer}: {e}"));
        let sweep = body
            .lines()
            .position(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && t.contains("assert_no_unnamed_dollar_brace(&doc)")
            })
            .unwrap_or_else(|| panic!("{renderer}: render_and_digest must call the sweep"));
        let hash = body
            .lines()
            .position(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && t.contains("sha256_prefixed(doc.as_bytes())")
            })
            .unwrap_or_else(|| panic!("{renderer}: render_and_digest must hash the document"));
        assert!(
            sweep < hash,
            "{renderer}: the G-EXP sweep must run BEFORE the digest is taken, not after \
             (sweep at line {sweep}, hash at line {hash})"
        );
    }
}

/// The structural half of "every rendered document is covered": no renderer
/// may call the UNCHECKED escaper any more.
///
/// A per-site test can only prove the sites it enumerates. This reads the
/// three renderer sources and asserts that `yaml_scalar(` — the unchecked
/// function, with its opening parenthesis, so `yaml_scalar_checked(` does not
/// match — appears at no non-comment line. A new field added by a later task
/// and escaped with the wrong function is caught here even though no test
/// enumerates it.
#[test]
fn every_renderer_calls_the_checked_escaper() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for renderer in [
        "render_backup.rs",
        "render_restore.rs",
        "render_validation.rs",
    ] {
        let body = std::fs::read_to_string(src.join(renderer))
            .unwrap_or_else(|e| panic!("{renderer}: {e}"));
        let offenders: Vec<(usize, &str)> = body
            .lines()
            .enumerate()
            .filter(|(_, l)| {
                let t = l.trim_start();
                !t.starts_with("//") && t.contains("yaml_scalar(")
            })
            .map(|(i, l)| (i + 1, l.trim()))
            .collect();
        assert!(
            offenders.is_empty(),
            "{renderer} still reaches the UNCHECKED escaper; every interpolation site must call \
             `yaml_scalar_checked` so a `${{` in the value is refused: {offenders:?}"
        );
        assert!(
            body.contains("yaml_scalar_checked("),
            "{renderer} must escape through `yaml_scalar_checked`"
        );
    }
}

/// Task 2's review finding **F1**, as it stands after Task 6:
/// `AuthRender::ScramSha512` was a public enum variant whose render arm was
/// `todo!()` — the workspace's only shipped `todo!()`. Task 3 replaced it with
/// a typed `RenderError::UnsupportedAuthMode`; **Task 6 replaced the refusal
/// with the real render**, so the assertion here inverts: the arm no longer
/// refuses, and what F1 was really about — that no reachable arm panics, and
/// that no plan naming SASL is ever rendered unauthenticated — is what is
/// asserted now.
///
/// The three wrong answers this pins against:
///   * a `todo!()` PANICS, which aborts where Global Constraint 11 requires a
///     refusal and prints no `refusal-reason=` line at all;
///   * falling through to the `Plaintext` arm emits a document the engine runs
///     UNAUTHENTICATED, from a plan that named SCRAM — an authentication
///     downgrade performed on the operator's behalf. That is still the mutant
///     for this test, and it is the one a tired editor would actually write;
///   * refusing again, now that the capability exists, would take a whole
///     mode back out of the product silently.
#[test]
fn a_scram_plan_renders_and_never_panics_and_is_never_downgraded() {
    let mut p = backup_plan();
    p.source_auth = AuthRender::ScramSha512 {
        username: "logweir-drill".into(),
        tls: true,
    };
    let doc = render_backup::render(&p).expect("the SCRAM arm renders since Task 6");
    // NOT a downgrade: the document authenticates, and says so with the
    // engine's own spellings.
    assert!(doc.contains("  security:\n"), "{doc}");
    assert!(doc.contains("security_protocol: \"SASL_SSL\""), "{doc}");
    assert!(doc.contains("sasl_mechanism: \"SCRAM-SHA512\""), "{doc}");
    assert!(doc.contains("sasl_username: \"logweir-drill\""), "{doc}");
    // And the placeholder, never a value: the whole point of the exception.
    assert!(
        doc.contains("sasl_password: ${LOGWEIR_SOURCE_PASSWORD}"),
        "{doc}"
    );
    // Through `render_and_digest` too, so the post-render `${` sweep accepts
    // the one named placeholder rather than refusing the document it is in.
    let (doc2, digest) = render_backup::render_and_digest(&p)
        .expect("the named placeholder is what `assert_no_unnamed_dollar_brace` permits");
    assert_eq!(doc2, doc);
    assert!(digest.starts_with("sha256:"), "{digest}");

    // The `UnsupportedAuthMode` rail still exists, still carries the mode, and
    // still classifies as a plain guard refusal rather than
    // `CredentialNotRenderable` — nothing about a projected credential is
    // observed by a renderer, and telling a controller otherwise would send an
    // operator to look at a Secret. Constructed directly: no `AuthRender` arm
    // reaches it any more, which is the point of keeping it typed rather than
    // deleting it (see the variant's own doc comment).
    let err = RenderError::UnsupportedAuthMode("oauthbearer".to_string());
    assert_eq!(
        logweir_core::guard::refusal_reason_line(&err.to_string()),
        "refusal-reason=GuardRefused"
    );
    let msg = err.to_string();
    assert!(msg.contains("oauthbearer"), "{msg}");
    assert!(!msg.contains("logweir-drill"), "{msg}");

    // There is no `todo!()` anywhere in the crate's sources any more.
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut stack = vec![src];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|x| x == "rs") {
                let body = std::fs::read_to_string(&path).unwrap();
                for (i, line) in body.lines().enumerate() {
                    let t = line.trim_start();
                    assert!(
                        t.starts_with("//") || !t.contains("todo!("),
                        "{}:{}: a `todo!()` on a reachable arm is a panic where Global \
                         Constraint 11 requires a refusal",
                        path.display(),
                        i + 1
                    );
                }
            }
        }
    }
}
