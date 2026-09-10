//! The SASL/SCRAM-SHA-512 half of all three rendered documents (Task 6,
//! interface **I1**), and the four goldens that pin its bytes.
//!
//! # Why these goldens exist beside the eight that predate them
//!
//! The eight goldens at `63066d2` all carry `AuthRender::Plaintext`, whose arm
//! emits **nothing at all** — so they are byte-identical after this task, and
//! that identity is itself an assertion (critique A F16): it is what proves
//! the `Plaintext` arm did not gain an explicit `security_protocol:
//! PLAINTEXT`. The four here are the other half: the bytes an authenticating
//! document actually has.
//!
//! # Everything in these documents was measured against the pinned engine
//!
//! Two things about the SASL block cannot be got right by reading upstream's
//! examples, and both were settled by running the digest-pinned engine
//! (`third_party/kafka-backup-binary.digest`) over a crafted config:
//!
//! 1. **The nesting is `security:`.** The four keys are fields of
//!    `SecurityConfig`, reached through `KafkaConfig.security`
//!    [U:crates/kafka-backup-core/src/config.rs:173-208]. Rendered one level
//!    too high the engine answers with four *"Ignoring unknown config key
//!    `source.security_protocol`"* lines — and
//!    `OsoCliEngine::assert_no_dropped_logweir_key` then aborts the run at
//!    exit 1 after the document was written, which is the
//!    `strip_offset_headers` defect exactly (Task 4 review F-1). Under
//!    `security:` the same engine emits no warning at all.
//! 2. **The mechanism has ONE hyphen.** `sasl_mechanism: "SCRAM-SHA-512"` —
//!    librdkafka's spelling, and the one upstream's own
//!    `config/example-backup.yaml` comments — is a serde TYPE error that
//!    aborts config load: *"unknown variant `SCRAM-SHA-512`, expected one of
//!    `PLAIN`, `SCRAM-SHA256`, `SCRAM-SHA512`, `GSSAPI`"*. A wrong-but-typed
//!    value is not an unknown key, so `subprocess`'s unknown-key readback
//!    cannot see it.
//!
//! # This file dials nothing
//!
//! Every test is a renderer call or a source read over in-memory plans. The
//! bootstrap strings are `kafka-broker-1:9096`/`:9094` — deliberately not one
//! of the loopback endpoints in `crates/logweir/tests/
//! no_network_in_unit_tests.rs`'s `DIAL_TOKENS`, so that gate needs no entry
//! for this path at all. (Naming one here, even in prose, would trip it: the
//! scan is a plain `contains` over the file, by design.)
use logweir_core::engine::{AuthRender, BackupPlan, BackupSetRef, RestorePlan, StorageUrl};
use logweir_engine_oso::render_backup::RenderError;
use logweir_engine_oso::{render_backup, render_restore, render_validation};

/// The SCRAM principal every document below binds. A NAME, never a secret:
/// `AuthRender` has no field that could hold one.
const PRINCIPAL: &str = "logweir-backup";

fn storage() -> StorageUrl {
    StorageUrl::S3 {
        bucket: "kafka-backups".into(),
        prefix: "scram-demo".into(),
        region: Some("us-east-1".into()),
        endpoint: Some("http://minio:9000".into()),
        path_style: true,
        allow_http: true,
    }
}

fn restore_plan(auth: AuthRender) -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: "backup-2026-08-30T02:00:00Z".into(),
            manifest_key: "drills/backup-2026-08-30T02:00:00Z/manifest.json".into(),
        },
        storage: storage(),
        target_bootstrap: vec!["kafka-broker-1:9096".into()],
        target_auth: auth,
        topic_mapping: [("orders".to_string(), "drill-20260903-orders".to_string())]
            .into_iter()
            .collect(),
        time_window: (
            "2026-08-29T00:00:00Z".parse().unwrap(),
            "2026-08-30T02:00:00Z".parse().unwrap(),
        ),
        default_replication_factor: 1,
        checkpoint_state: "/var/lib/logweir/01J9X/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
    }
}

fn backup_plan(auth: AuthRender) -> BackupPlan {
    BackupPlan {
        backup_id: "scram-demo".into(),
        source_bootstrap: vec!["kafka-broker-1:9094".into()],
        source_auth: auth,
        topics: vec!["orders".into()],
        storage: storage(),
        compression: "zstd".into(),
        segment_max_records: 1000,
        segment_max_bytes: 10_485_760,
        max_concurrent_partitions: 3,
    }
}

fn scram(tls: bool) -> AuthRender {
    AuthRender::ScramSha512 {
        username: PRINCIPAL.into(),
        tls,
    }
}

// ---------------------------------------------------------------------------
// THE FOUR GOLDENS
// ---------------------------------------------------------------------------

#[test]
fn restore_scram_tls_matches_the_golden() {
    insta::assert_snapshot!(
        "restore_scram_tls",
        render_restore::render(&restore_plan(scram(true))).expect("no glob metacharacter, no `${`")
    );
}

#[test]
fn restore_scram_plaintext_matches_the_golden() {
    insta::assert_snapshot!(
        "restore_scram_plaintext",
        render_restore::render(&restore_plan(scram(false)))
            .expect("no glob metacharacter, no `${`")
    );
}

#[test]
fn backup_scram_tls_matches_the_golden() {
    insta::assert_snapshot!(
        "backup_scram_tls",
        render_backup::render(&backup_plan(scram(true))).expect("no glob metacharacter, no `${`")
    );
}

#[test]
fn backup_scram_plaintext_matches_the_golden() {
    insta::assert_snapshot!(
        "backup_scram_plaintext",
        render_backup::render(&backup_plan(scram(false))).expect("no glob metacharacter, no `${`")
    );
}

// ---------------------------------------------------------------------------
// THE PROPERTIES THE GOLDENS ALONE DO NOT STATE
// ---------------------------------------------------------------------------

/// **The two spellings, and neither can drift alone.**
///
/// The engine's wire value is `SCRAM-SHA512` (ONE hyphen), librdkafka's is
/// `SCRAM-SHA-512` (two). Both halves are read here — one from a rendered
/// document, one from `rdkafka_reader.rs`'s own source — so a next editor who
/// "fixes" either to match the other fails this test rather than shipping a
/// config the engine refuses to parse or a client librdkafka refuses to build.
///
/// The two-hyphen form's cost, measured against the pinned engine:
/// `Failed to parse config: source.security.sasl_mechanism: unknown variant
/// \`SCRAM-SHA-512\`` — config load aborts, so no backup and no restore runs
/// at all.
#[test]
fn the_engine_spelling_is_one_hyphen_and_librdkafkas_is_two() {
    for doc in [
        render_backup::render(&backup_plan(scram(true))).unwrap(),
        render_restore::render(&restore_plan(scram(true))).unwrap(),
        render_validation::render(&restore_plan(scram(true)), "01J9X", None).unwrap(),
    ] {
        assert!(
            doc.contains("sasl_mechanism: \"SCRAM-SHA512\""),
            "the ENGINE's spelling has one hyphen:\n{doc}"
        );
        assert!(
            !doc.contains("SCRAM-SHA-512"),
            "librdkafka's two-hyphen spelling is a serde TYPE error in an engine config, not an \
             unknown key, so the stderr readback cannot catch it:\n{doc}"
        );
    }

    // The client half, read from source so it cannot drift silently either.
    let rdkafka = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../logweir-kafka/src/rdkafka_reader.rs"),
    )
    .expect("crates/logweir-kafka/src/rdkafka_reader.rs");
    assert!(
        rdkafka.contains("\"SCRAM-SHA-512\""),
        "librdkafka spells the mechanism with TWO hyphens; if this line changed, the client and \
         the engine now disagree about which mechanism they are asking for"
    );

    // And the constant the renderers share is the engine's, spelled once.
    assert_eq!(
        logweir_engine_oso::yaml::ENGINE_SCRAM_SHA_512,
        "SCRAM-SHA512"
    );
}

/// **`Plaintext` renders NO security keys, in any of the three documents.**
///
/// This is the property that keeps the eight pre-SCRAM goldens byte-identical,
/// and it is asserted directly as well as by their identity: the mutant —
/// emitting `security_protocol: "PLAINTEXT"` from the `Plaintext` arm — is
/// caught here at assertion time even before `insta` reports eight changed
/// goldens.
#[test]
fn a_plaintext_spec_renders_no_security_keys() {
    let restore = render_restore::render(&restore_plan(AuthRender::Plaintext)).unwrap();
    let backup = render_backup::render(&backup_plan(AuthRender::Plaintext)).unwrap();
    let validation =
        render_validation::render(&restore_plan(AuthRender::Plaintext), "01J9X", None).unwrap();
    for (name, doc) in [
        ("restore.yaml", &restore),
        ("backup.yaml", &backup),
        ("validation.yaml", &validation),
    ] {
        for key in [
            "security:",
            "security_protocol",
            "sasl_mechanism",
            "sasl_username",
            "sasl_password",
            "PLAINTEXT",
        ] {
            assert!(
                !doc.contains(key),
                "{name}: the Plaintext arm emits NOTHING AT ALL — the engine's own
                 `SecurityProtocol` default is PLAINTEXT, so an explicit key would be a fourth \
                 spelling to keep in step with upstream for no behavioural gain, and it would \
                 change every golden that predates SCRAM. Found `{key}`:\n{doc}"
            );
        }
    }
}

/// **The four keys are nested under `security:`, and the nesting is the whole
/// finding.**
///
/// A source read as well as a document read: the shipped renderers must reach
/// the SHARED block rather than each spelling the keys themselves, because
/// three copies of this nesting is three places for it to be got wrong again.
#[test]
fn the_sasl_keys_are_nested_under_the_engines_security_map() {
    for (name, doc) in [
        (
            "backup.yaml",
            render_backup::render(&backup_plan(scram(false))).unwrap(),
        ),
        (
            "restore.yaml",
            render_restore::render(&restore_plan(scram(false))).unwrap(),
        ),
        (
            "validation.yaml",
            render_validation::render(&restore_plan(scram(false)), "01J9X", None).unwrap(),
        ),
    ] {
        // The block header at two spaces (inside `source:`/`target:`) and its
        // four keys at four. Rendered at two, the pinned engine drops all four
        // as `source.security_protocol` &c. — measured, not assumed.
        assert!(doc.contains("\n  security:\n"), "{name}:\n{doc}");
        for key in [
            "    security_protocol: \"SASL_PLAINTEXT\"\n",
            "    sasl_mechanism: \"SCRAM-SHA512\"\n",
            "    sasl_username: \"logweir-backup\"\n",
        ] {
            assert!(doc.contains(key), "{name} is missing `{key}`:\n{doc}");
        }
        // And never at the wrong depth.
        assert!(
            !doc.contains("\n  security_protocol:"),
            "{name}: two-space `security_protocol` is an UNKNOWN KEY to the engine:\n{doc}"
        );
    }

    // One implementation, three callers.
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for renderer in [
        "render_backup.rs",
        "render_restore.rs",
        "render_validation.rs",
    ] {
        let body = std::fs::read_to_string(dir.join(renderer)).unwrap();
        assert!(
            body.contains("render_security_block("),
            "{renderer} must reach the ONE shared SASL block, not spell the keys itself"
        );
    }
    let yaml = std::fs::read_to_string(dir.join("yaml.rs")).unwrap();
    assert_eq!(
        yaml.matches("fn render_security_block(").count(),
        1,
        "there is exactly one implementation of the SASL block"
    );
}

/// `tls` flips exactly one value — the `security.protocol` — and nothing else.
/// It is separate from the mechanism because SASL/SCRAM over PLAINTEXT and
/// over SSL are two `security.protocol` values for ONE mechanism
/// [U:crates/kafka-backup-core/src/config.rs:261-269, SCREAMING_SNAKE_CASE
/// over Plaintext|Ssl|SaslPlaintext|SaslSsl].
#[test]
fn tls_selects_sasl_ssl_and_changes_nothing_else() {
    let with = render_backup::render(&backup_plan(scram(true))).unwrap();
    let without = render_backup::render(&backup_plan(scram(false))).unwrap();
    assert!(with.contains("security_protocol: \"SASL_SSL\""), "{with}");
    assert!(
        without.contains("security_protocol: \"SASL_PLAINTEXT\""),
        "{without}"
    );
    assert_eq!(
        with.replace("SASL_SSL", "SASL_PLAINTEXT"),
        without,
        "`tls` is one match arm; anything else that differs is a second effect nobody asked for"
    );
}

/// **The password placeholder, and the two things about it that are the
/// point.**
///
/// It is the RAW literal, unquoted, never through `yaml_scalar_checked` (which
/// would refuse it); and the backup document names the SOURCE variable while
/// the restore and validation documents name the TARGET one. A renderer that
/// crossed them would have the engine authenticate to the scratch cluster with
/// the production cluster's credential.
#[test]
fn the_password_is_a_named_placeholder_and_the_two_sides_do_not_cross() {
    let backup = render_backup::render(&backup_plan(scram(true))).unwrap();
    assert!(
        backup.contains("    sasl_password: ${LOGWEIR_SOURCE_PASSWORD}\n"),
        "{backup}"
    );
    assert!(!backup.contains("LOGWEIR_TARGET_PASSWORD"), "{backup}");

    for (name, doc) in [
        (
            "restore.yaml",
            render_restore::render(&restore_plan(scram(true))).unwrap(),
        ),
        (
            "validation.yaml",
            render_validation::render(&restore_plan(scram(true)), "01J9X", None).unwrap(),
        ),
    ] {
        assert!(
            doc.contains("    sasl_password: ${LOGWEIR_TARGET_PASSWORD}\n"),
            "{name}:\n{doc}"
        );
        assert!(!doc.contains("LOGWEIR_SOURCE_PASSWORD"), "{name}:\n{doc}");
    }

    // Unquoted, deliberately: the expanded value lands as a YAML PLAIN scalar,
    // whose hazards produce a parse error or a truncated password (a failed
    // authentication) and CANNOT open a new key, since `\n`/`\r` are refused
    // by `credential_is_renderable` before projection. Double-quoting would
    // make `\` an escape introducer and silently rewrite a password containing
    // one — see `yaml::render_security_block`'s doc comment.
    assert!(
        !backup.contains("sasl_password: \"${"),
        "the placeholder is emitted raw:\n{backup}"
    );
}

/// **The digest covers a document with the placeholder in it, and the
/// post-render `${` sweep permits exactly the two named ones.**
///
/// `render_and_digest` runs `assert_no_unnamed_dollar_brace` BEFORE
/// `sha256_prefixed`, so a document carrying anything else would be refused
/// and never digested. Both SCRAM documents go through it here, because the
/// SASL block is the only thing in the product that puts a `${…}` into a
/// hashed document at all.
#[test]
fn a_scram_document_passes_the_unnamed_placeholder_sweep_and_is_digested() {
    let (bdoc, bdig) = render_backup::render_and_digest(&backup_plan(scram(true))).unwrap();
    let (rdoc, rdig) = render_restore::render_and_digest(&restore_plan(scram(true))).unwrap();
    for (doc, dig) in [(&bdoc, &bdig), (&rdoc, &rdig)] {
        assert!(dig.starts_with("sha256:"), "{dig}");
        assert_eq!(
            dig,
            &logweir_core::ids::sha256_prefixed(doc.as_bytes()),
            "the digest is over the EXACT rendered bytes, never a re-serialisation of the plan"
        );
        assert_eq!(
            doc.matches("${").count(),
            1,
            "exactly one placeholder:\n{doc}"
        );
    }
    // A third, unnamed placeholder in the same document IS refused — the sweep
    // has teeth, and the SASL block did not blunt it.
    let mut tampered = bdoc.clone();
    tampered.push_str("tampered: ${SOMETHING_ELSE}\n");
    assert_eq!(
        logweir_engine_oso::yaml::assert_no_unnamed_dollar_brace(&tampered).unwrap_err(),
        RenderError::UnnamedPlaceholder("${SOMETHING_ELSE}".to_string())
    );
}

/// The `UnsupportedAuthMode` rail is unreachable from both `AuthRender` arms —
/// which is the claim its own doc comment makes, so it is the claim that gets
/// a test rather than a comment.
///
/// It is kept rather than deleted because the THIRD mode
/// (`logweir_kafka::reader::AuthConfig::Token`, OAUTHBEARER / MSK IAM, SP4)
/// has no `AuthRender` twin yet, and the task that adds one must decide in the
/// type system whether the engine can render it. A kept variant is cheaper
/// than the `todo!()` its absence invites (Task 2 review F1).
#[test]
fn the_unsupported_auth_mode_rail_is_unreachable_from_both_arms() {
    for auth in [AuthRender::Plaintext, scram(false), scram(true)] {
        render_backup::render(&backup_plan(auth.clone())).expect("both arms render");
        render_restore::render(&restore_plan(auth.clone())).expect("both arms render");
        render_validation::render(&restore_plan(auth), "01J9X", None).expect("both arms render");
    }
    // Still typed, still constructible, still a plain guard refusal.
    assert!(RenderError::UnsupportedAuthMode("gssapi".into())
        .to_string()
        .contains("is not supported by this build"));
}
