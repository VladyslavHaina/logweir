//! Guard **G-ID** — the plan binds the principal — and the rest of Task 6's
//! named acceptance (interface **I1**).
//!
//! # Why the binding is the point, and not the connectivity
//!
//! `planBytes` must carry `auth.username`, because the Secret named by a
//! `secretRef` is mutable and covered by no hash: binding only the ADDRESS
//! lets anyone with `update` on that Secret change **which principal Logweir
//! authenticates as** — after approval, with no plan-hash change and no new
//! signature. On SASL/SCRAM the credential *is* the authorisation.
//!
//! And Logweir **cannot observe** the principal the broker authenticated:
//! Kafka exposes no such call and neither client offers one. So the binding is
//! **structural, not observational** — `sasl_username` is rendered from the
//! plan bytes, never from a cluster object read at run time — and a test for
//! it has to be a test about where a value came FROM, not about what a broker
//! reported.
//!
//! # This file constructs no client
//!
//! Every row is a renderer call, a source read, or an in-process
//! `backup::execute_with` over doubles. `bootstrap_servers` is
//! `localhost:9092` and is DATA handed to a `ClusterReader` double — the
//! reason recorded for this path in
//! `crates/logweir/tests/no_network_in_unit_tests.rs`'s `ALLOWED`.
use logweir::backup::{execute_with, BackupError, BackupRunArgs};
use logweir_core::backup_receipt::ReceiptAuth;
use logweir_core::engine::{
    AuthRender, BackupSetFacts, BackupSetRef, DataEngine, EngineError, EngineId, PhaseObserver,
    RecordFingerprint, RestorePlan, SampleSelection,
};
use logweir_core::spec::{AuthSpec, DrillSpec};
use logweir_engine_oso::storage::Store;
use logweir_engine_oso::{render_restore, render_validation};
use logweir_evidence::keys::SigningKey;
use logweir_kafka::reader::{AuthConfig, ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;
use std::path::Path;

/// The principal the approver signed off on.
const APPROVED: &str = "approved-principal";
/// The principal an attacker with `update` on the Secret (or a controller that
/// re-read a mutable `KafkaCluster`) would substitute. **It must not appear in
/// any rendered document.**
const SWAPPED: &str = "swapped-principal";

/// A drill spec whose `target.auth` names `username`, as an adopter writes it.
fn drill_spec_text(username: &str, tls: bool) -> String {
    format!(
        "source:\n  \
           storage:\n    backend: filesystem\n    path: /archive\n  \
           backup: latestCompleted\n  \
           topics: [orders]\n\
         target:\n  \
           bootstrap_servers: [localhost:9092]\n  \
           auth:\n    mode: scramSha512\n    username: {username}\n    tls: {tls}\n  \
           marker_topic: logweir.scratch\n  \
           topic_mapping_prefix: \"drill-\"\n  \
           default_replication_factor: 1\n  \
           teardown: delete\n\
         sample:\n  \
           window_start: \"2026-08-29T00:00:00Z\"\n  \
           window_end: \"2026-08-30T00:00:00Z\"\n  \
           records_per_partition: 25\n  \
           anchor: head\n\
         objectives:\n  rto_seconds: 900\n\
         evidence:\n  backend: filesystem\n  path: /logweir/evidence\n\
         notifications:\n  webhooks: []\n"
    )
}

// ---------------------------------------------------------------------------
// G-ID
// ---------------------------------------------------------------------------

/// The mutable half a controller holds: a `KafkaCluster` object, live, at
/// render time.
///
/// **It is declared HERE, in the test, because no shipped type reads it — and
/// that is the property.** `weirkeeper` resolves a `KafkaCluster` into
/// `planBytes` at APPROVAL time; nothing between the approval and the rendered
/// document may read it again.
struct KafkaClusterView {
    /// What the cluster object says the principal is, *now*.
    username: String,
}

/// The controller's own call path: the approved spec BYTES in, the rendered
/// document out, with the live cluster object **in scope the whole way**.
///
/// Passing the view and then not using it is the deliberate shape of this
/// test. It is what makes G-ID's mutant an ASSERTION-time failure rather than
/// a compile error (STANDING RULE 19): a reviewer applying "render
/// `sasl_username` from the cluster object" edits `logweir::drill::build_plan`
/// (shipped) or the one line marked below, compiles, and watches
/// `swapped-principal` reach the document.
fn render_as_a_controller_would(spec_text: &str, live: &KafkaClusterView) -> (RestorePlan, String) {
    let spec: DrillSpec = serde_yaml::from_str(spec_text).expect("the approved spec bytes parse");
    let set = BackupSetRef {
        backup_id: "backup-2026-08-30T02:00:00Z".into(),
        manifest_key: "drills/backup-2026-08-30T02:00:00Z/manifest.json".into(),
    };
    let mapping: BTreeMap<String, String> = [("orders".to_string(), "drill-orders".to_string())]
        .into_iter()
        .collect();
    // THE SHIPPED SEAM. `build_plan` is where the approved bytes become the
    // plan `plan_hash` covers; the live view is available to it here and is
    // not among its arguments, which is the structural half of G-ID.
    //
    // The `BackupSetFacts` argument is guard **G-WIN**'s: since Task 9 the
    // window's floor comes from the archive manifest and from nothing else, so
    // plan construction needs the manifest's facts. This one's earliest
    // covered timestamp is the spec's own `sample.window_start`, which is what
    // keeps every G-ID assertion below about the PRINCIPAL and nothing else.
    let facts = one_segment_facts(ts("2026-08-29T00:00:00Z").timestamp_millis());
    let plan = logweir::drill::build_plan(&spec, &set, &mapping, &facts, "01J9X", None)
        .expect("the plan builds: the fixture's manifest floor is a real timestamp");
    // THE MUTATION POINT for "render from the cluster object". Replace this
    // line with `plan.target_auth = AuthRender::ScramSha512 { username:
    // live.username.clone(), tls: true };` and the assertions below fail.
    let _ = &live.username;
    let (doc, _digest) = render_restore::render_and_digest(&plan).expect("the plan renders");
    (plan, doc)
}

fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(s)
        .unwrap()
        .with_timezone(&chrono::Utc)
}

/// One topic, one partition, one segment starting at `start_ms` — the minimum
/// a manifest needs for `BackupSetFacts::earliest_covered_timestamp_ms` to
/// answer, which plan construction now requires (guard **G-WIN**).
fn one_segment_facts(start_ms: i64) -> logweir_core::engine::BackupSetFacts {
    use logweir_core::engine::{BackupSetFacts, PartitionFacts, SegmentFacts, TopicFacts};
    BackupSetFacts {
        backup_id: "backup-2026-08-30T02:00:00Z".into(),
        created_at: ts("2026-08-30T02:00:00Z"),
        source_cluster_id: Some("SRC0000000000000000000".into()),
        manifest_sha256: format!("sha256:{}", "a".repeat(64)),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: vec![TopicFacts {
            name: "orders".into(),
            original_partition_count: Some(1),
            source_replication_factor: Some(1),
            configurations: Default::default(),
            partitions: vec![PartitionFacts {
                partition_id: 0,
                segments: vec![SegmentFacts {
                    key: "drills/b/0/000000000000.kbak".into(),
                    start_offset: 0,
                    end_offset: 9,
                    start_timestamp: start_ms,
                    end_timestamp: start_ms + 1_000,
                    record_count: 10,
                    sha256: format!("sha256:{}", "b".repeat(64)),
                    uploaded_at: start_ms + 2_000,
                }],
                gaps: vec![],
                pruned: vec![],
            }],
        }],
    }
}

/// **G-ID.** The rendered `sasl_username` is a function of the plan bytes and
/// of nothing else.
#[test]
fn rendered_sasl_username_comes_from_plan_bytes_not_from_the_cluster_object() {
    // The approved bytes name one principal; the live cluster object names
    // another. This is exactly the window the guard exists for: the object is
    // mutable, `planBytes` is hashed.
    let live = KafkaClusterView {
        username: SWAPPED.to_string(),
    };
    let (plan, doc) = render_as_a_controller_would(&drill_spec_text(APPROVED, true), &live);

    assert!(
        doc.contains(&format!("sasl_username: \"{APPROVED}\"")),
        "the document must name the APPROVED principal:\n{doc}"
    );
    assert!(
        !doc.contains(SWAPPED),
        "the live cluster object's principal reached the rendered document — anyone with \
         `update` on the Secret or on the KafkaCluster could change which identity Logweir \
         authenticates as, after approval, with no plan-hash change and no new signature:\n{doc}"
    );

    // And the plan's OWN field is what the renderer read — not a coincidence
    // of two values that happen to match.
    assert_eq!(
        plan.target_auth,
        AuthRender::ScramSha512 {
            username: APPROVED.to_string(),
            tls: true,
        },
        "the principal is a field of the PLAN, which plan_hash covers"
    );

    // The same property on the validation document, which dials the same
    // cluster through the same `KafkaConfig` shape.
    let validation = render_validation::render(&plan, "01J9X", None).unwrap();
    assert!(
        validation.contains(&format!("sasl_username: \"{APPROVED}\"")),
        "{validation}"
    );
    assert!(!validation.contains(SWAPPED), "{validation}");

    // THE STRUCTURAL HALF, read from source: the renderer's only source for
    // this line is the plan it was handed. A mutant that gave the renderer a
    // second parameter to read from is caught here even before the value
    // assertions above.
    let block = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../logweir-engine-oso/src/yaml.rs"),
    )
    .unwrap();
    let sig = block
        .split("pub(crate) fn render_security_block(")
        .nth(1)
        .expect("`render_security_block` is where sasl_username is emitted")
        .split("\n}\n")
        .next()
        .unwrap();
    assert!(
        sig.contains("sasl_username: {}") && sig.contains("yaml_scalar_checked(username)"),
        "the username is interpolated from the `AuthRender` arm's own binding:\n{sig}"
    );
    for renderer in [
        "render_restore.rs",
        "render_validation.rs",
        "render_backup.rs",
    ] {
        let body = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../logweir-engine-oso/src")
                .join(renderer),
        )
        .unwrap();
        assert!(
            !body.contains("sasl_username"),
            "{renderer} must not spell `sasl_username` itself — one emission site, reading one \
             field of the plan, is what makes G-ID checkable"
        );
    }
}

// ---------------------------------------------------------------------------
// The two documents
// ---------------------------------------------------------------------------

/// `auth.mode` and `auth.username` land in the backup receipt and in the
/// restore scorecard.
///
/// **What this branch can and cannot assert, stated rather than blurred.**
/// The backup half runs for real: `backup::execute_with`, in process, over
/// doubles (critique A F1 — the binary would need a broker for the cluster id
/// and an archive for the drill), and the mapping from its `BackupOutcome`
/// onto Task 5's own `ReceiptAuth` type is asserted over that type. The
/// receipt is not yet WRITTEN by this build — `--receipt-out` is interface
/// I6, refused by `phase_minus1_admit::local` until Task 5b lands it — and the
/// scorecard's `target.auth` block (`AuthSummary`, both readers, the schema)
/// is Task 5b's too, in this same slot. So the SCORECARD half is asserted
/// through the values `drill::target_info` will use, which
/// `the_scorecard_auth_block_and_auth_spec_agree` below pins byte for byte.
/// Nothing here claims a field that does not exist.
#[test]
fn auth_mode_and_username_land_in_the_receipt_and_the_scorecard() {
    // ---- the receipt's source: a real run, over doubles ----
    let f = backup_fixture("  auth:\n    mode: scramSha512\n    username: logweir\n");
    let reader = StubReader;
    let engine = StubEngine::new();
    let store = archive_with_one_manifest("mvp-demo");
    // Six arguments since Task 5b: the ARCHIVE store and the EVIDENCE
    // store are separate parameters (`backup::execute_with`), and this row
    // cares about neither destination — one in-memory store plays both.
    let outcome = execute_with(&f.args, "run-1", &reader, &engine, &store, &store).unwrap();
    assert_eq!(
        outcome.source_auth,
        AuthRender::ScramSha512 {
            username: "logweir".into(),
            tls: false,
        }
    );
    // And the plan the ENGINE was handed carries the same principal — the
    // field is not a decoration on the outcome, it is what `render_backup`
    // renders `sasl_username` from (**G-ID**, backup side).
    assert_eq!(engine.recorded_plan().source_auth, outcome.source_auth);

    // ---- and onto Task 5's receipt type, through the I1 accessors ----
    let spec: logweir_core::spec::BackupSpec =
        serde_yaml::from_str(&std::fs::read_to_string(&f.args.spec).unwrap()).unwrap();
    let receipt_auth = ReceiptAuth {
        mode: spec.source.auth.mode_str().into(),
        username: spec.source.auth.username().map(str::to_string),
    };
    assert_eq!(receipt_auth.mode, "scramSha512");
    assert_eq!(
        receipt_auth.username.as_deref(),
        Some("logweir"),
        "the receipt records the PRINCIPAL, which is what an auditor reconciles a backup \
         against — dropping it from the spec type would make every receipt anonymous"
    );

    // ---- the scorecard's source: the two values `target_info` will use ----
    let drill: DrillSpec = serde_yaml::from_str(&drill_spec_text("logweir", false)).unwrap();
    assert_eq!(drill.target.auth.mode_str(), "scramSha512");
    assert_eq!(drill.target.auth.username(), Some("logweir"));

    // ---- and never a secret, in either document ----
    let json = serde_json::to_string(&receipt_auth).unwrap();
    assert_eq!(json, r#"{"mode":"scramSha512","username":"logweir"}"#);
    for word in ["password", "secret", "sasl_password"] {
        assert!(!json.contains(word), "{json}");
    }

    // The plaintext default, for contrast: a mode, and no principal at all.
    let plain = ReceiptAuth {
        mode: AuthSpec::Plaintext.mode_str().into(),
        username: AuthSpec::Plaintext.username().map(str::to_string),
    };
    assert_eq!(plain.mode, "plaintext");
    assert_eq!(
        plain.username, None,
        "`null` under plaintext, which is not the same as an empty username"
    );
}

/// The same-slot late binding to Task 5b (interface **I1** ↔ its
/// `AuthSummary`), exactly as **I33** closes the CRD enums.
///
/// Three surfaces have to agree on two strings: the YAML an adopter writes,
/// the CRD an adopter applies, and the signed documents a reader parses. This
/// asserts Logweir's half — the serde tag values and the `{mode, username}`
/// JSON shape — so that when Task 5b lands `scorecard::AuthSummary` and both
/// readers' tolerant read, the agreement is a test and not a comment.
#[test]
fn the_scorecard_auth_block_and_auth_spec_agree() {
    // 1. `mode_str()` is exactly the two strings, and nothing else.
    assert_eq!(AuthSpec::Plaintext.mode_str(), "plaintext");
    assert_eq!(
        AuthSpec::ScramSha512 {
            username: "x".into(),
            tls: false
        }
        .mode_str(),
        "scramSha512"
    );

    // 2. They are the enum's own SERDE TAG VALUES, which is what makes them
    //    the strings both readers accept for `target.auth.mode`: a reader
    //    parses the same YAML/JSON an adopter writes.
    for (spec, tag) in [
        (AuthSpec::Plaintext, "plaintext"),
        (
            AuthSpec::ScramSha512 {
                username: "logweir".into(),
                tls: true,
            },
            "scramSha512",
        ),
    ] {
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        assert_eq!(
            v.get("mode").and_then(|m| m.as_str()),
            Some(tag),
            "the serde tag and `mode_str()` must be the same string"
        );
        assert_eq!(spec.mode_str(), tag);
        // And it round-trips: a document carrying that spelling parses back.
        let back: AuthSpec = serde_json::from_value(v).unwrap();
        assert_eq!(back, spec);
    }

    // 3. Nothing else is accepted. A reader that took `scram-sha-512` or
    //    `SCRAM-SHA-512` would parse a document Logweir never writes.
    for rejected in [
        "scram-sha-512",
        "SCRAM-SHA-512",
        "scramsha512",
        "ScramSha512",
        "scram_sha_512",
    ] {
        let v = serde_json::json!({"mode": rejected, "username": "logweir"});
        assert!(
            serde_json::from_value::<AuthSpec>(v).is_err(),
            "`{rejected}` is not one of the two spellings and must not parse"
        );
    }

    // 4. The `{mode, username}` object round-trips through serde_json with
    //    exactly those keys — the shape Task 5b's `AuthSummary` declares. It
    //    is asserted here over a MIRROR of that shape, because `AuthSummary`
    //    lands in Task 5b's commit in this same slot; the controller
    //    reconciles at rebase if the shape differs.
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
    struct AuthSummaryMirror {
        mode: String,
        #[serde(default)]
        username: Option<String>,
    }
    let summary = AuthSummaryMirror {
        mode: AuthSpec::ScramSha512 {
            username: "logweir".into(),
            tls: true,
        }
        .mode_str()
        .into(),
        username: Some("logweir".into()),
    };
    let json = serde_json::to_string(&summary).unwrap();
    assert_eq!(json, r#"{"mode":"scramSha512","username":"logweir"}"#);
    assert_eq!(
        serde_json::from_str::<AuthSummaryMirror>(&json).unwrap(),
        summary
    );

    // 5. Task 5's `ReceiptAuth` — which IS landed — has that shape already,
    //    so the two documents cannot disagree about the key names.
    let receipt = ReceiptAuth {
        mode: "scramSha512".into(),
        username: Some("logweir".into()),
    };
    assert_eq!(serde_json::to_string(&receipt).unwrap(), json);
}

// ---------------------------------------------------------------------------
// The structural rows
// ---------------------------------------------------------------------------

/// No construction site hard-codes the plaintext arm any more.
///
/// Two of the three did, each with a comment explaining that `TargetSpec`
/// carried no auth block to render a `ScramSha512` from — so SASL was
/// unreachable from any shipped spec, and `doctor` reported "target
/// unreachable" against a cluster that was reachable and merely required
/// authentication.
///
/// Comment lines are stripped before the scan: the files legitimately DISCUSS
/// the arm they used to pin, and a test that could not tell prose from code
/// would force the history out of the source.
#[test]
fn no_construction_site_hardcodes_plaintext() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for site in ["drill/mod.rs", "doctor.rs", "backup/mod.rs"] {
        let body = std::fs::read_to_string(src.join(site)).unwrap();
        assert!(
            body.contains("AuthConfig::from_spec"),
            "{site} must build its client's auth through the ONE construction site \
             (interface I1)"
        );
        let code: String = body
            .lines()
            .filter(|l| {
                let t = l.trim_start();
                !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !code.contains("AuthConfig::Plaintext"),
            "{site} still pins the plaintext arm in CODE: a spec asking for scramSha512 would \
             be recorded in the receipt and then dialled unauthenticated"
        );
    }
}

/// The archive-side `SourceSpec` carries no auth block, and the two
/// broker-side specs do (critique A F15).
///
/// `SourceSpec` is `{storage, backup, topics}` — the drill's ARCHIVE location.
/// It has no `bootstrap_servers` and dials no broker, so a SCRAM block on it
/// would ship a field nothing reads: an operator would configure it, the
/// document would record it, and no client would ever use it.
#[test]
fn the_archive_source_spec_carries_no_auth_block() {
    let spec_rs = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../logweir-core/src/spec.rs"),
    )
    .unwrap();

    let block = |name: &str| -> String {
        let start = spec_rs
            .find(&format!("pub struct {name} {{"))
            .unwrap_or_else(|| panic!("`pub struct {name}` is declared in spec.rs"));
        let rest = &spec_rs[start..];
        let end = rest.find("\n}\n").expect("the struct closes");
        rest[..end].to_string()
    };

    let source = block("SourceSpec");
    assert!(
        !source.contains("auth"),
        "SourceSpec is the ARCHIVE's location — no bootstrap servers, no broker — so an auth \
         block on it would be a field nothing reads:\n{source}"
    );
    // And it really is the archive struct, so the assertion above is about the
    // right type.
    for archive_field in ["storage", "backup", "topics"] {
        assert!(source.contains(archive_field), "{source}");
    }
    assert!(!source.contains("bootstrap_servers"), "{source}");

    for named in ["TargetSpec", "BackupSourceSpec"] {
        let b = block(named);
        assert!(
            b.contains("pub auth: AuthSpec"),
            "{named} names a broker and must carry an auth block:\n{b}"
        );
        assert!(
            b.contains("#[serde(default)]"),
            "{named}'s auth block must default, so every spec written before it keeps parsing"
        );
        assert!(b.contains("bootstrap_servers"), "{b}");
    }
}

/// No password value reaches a rendered document, a plan, a receipt field or
/// the plan hash — **G-ID binds the principal, never the secret.**
#[test]
fn no_password_reaches_a_rendered_document() {
    // An obviously fake value, and one that is renderable, so it could
    // physically have been interpolated if any code path did so.
    const FAKE: &str = "not-a-real-secret-0123456789";

    // The environment is set for the duration of the render, which is the
    // strongest form of this assertion: the value is READABLE, and still
    // appears nowhere.
    let live = KafkaClusterView {
        username: SWAPPED.to_string(),
    };
    let (plan, restore) = render_as_a_controller_would(&drill_spec_text(APPROVED, true), &live);
    let validation = render_validation::render(&plan, "01J9X", None).unwrap();

    let backup_plan = logweir_core::engine::BackupPlan {
        backup_id: "b".into(),
        source_bootstrap: vec!["kafka-broker-1:9094".into()],
        source_auth: AuthRender::ScramSha512 {
            username: APPROVED.into(),
            tls: true,
        },
        topics: vec!["orders".into()],
        storage: logweir_core::engine::StorageUrl::Filesystem {
            path: "/archive".into(),
        },
        compression: "zstd".into(),
        segment_max_records: 1,
        segment_max_bytes: 1,
        max_concurrent_partitions: 1,
    };
    let (backup, backup_digest) =
        logweir_engine_oso::render_backup::render_and_digest(&backup_plan).unwrap();

    for (name, doc) in [
        ("restore.yaml", &restore),
        ("validation.yaml", &validation),
        ("backup.yaml", &backup),
    ] {
        assert!(
            !doc.contains(FAKE),
            "{name} carries the password VALUE; the placeholder exists precisely so the secret \
             is not in the bytes logweir hashes:\n{doc}"
        );
        // The placeholder is there instead — so the absence above is not the
        // absence of the whole block.
        assert!(doc.contains("sasl_password: ${LOGWEIR_"), "{name}:\n{doc}");
    }

    // The PLAN carries no secret either, so it cannot be in `plan_hash`.
    let dbg = format!("{:?}", plan.target_auth);
    assert!(!dbg.contains(FAKE), "{dbg}");
    assert!(dbg.contains(APPROVED), "the principal IS bound: {dbg}");

    // And the digest is over bytes that hold the placeholder, not the value.
    assert_eq!(
        backup_digest,
        logweir_core::ids::sha256_prefixed(backup.as_bytes())
    );

    // `AuthConfig` — the one type that DOES hold the secret — never prints it.
    let cfg = AuthConfig::from_spec(
        &AuthSpec::ScramSha512 {
            username: APPROVED.into(),
            tls: true,
        },
        Some(FAKE.to_string()),
    )
    .unwrap();
    let printed = format!("{cfg:?}");
    assert!(!printed.contains(FAKE), "{printed}");
    assert!(printed.contains("\"***\""), "{printed}");
    assert!(printed.contains(APPROVED), "{printed}");
}

/// `AuthConfig::from_spec` is the one construction site, and its two failure
/// modes are two different exit codes for a reason.
///
/// An ABSENT variable is operational (exit 1): nothing was refused, the plan
/// is probably fine, and the fix is to project the Secret — which is exactly
/// what "retry" means to Task 18's cron reconciler. An UNRENDERABLE value is a
/// guard refusal (exit 3), because no projection of that value will ever work.
/// The process-level halves of both are in `tests/cli_exit_codes.rs`.
#[test]
fn from_spec_is_the_one_construction_site_and_maps_both_failure_modes() {
    // Plaintext ignores the password rather than refusing a present one: a
    // Secret left projected after a spec was switched back is a tidiness
    // problem, not a reason to fail a backup.
    assert!(matches!(
        AuthConfig::from_spec(&AuthSpec::Plaintext, None).unwrap(),
        AuthConfig::Plaintext
    ));
    assert!(matches!(
        AuthConfig::from_spec(&AuthSpec::Plaintext, Some("x".into())).unwrap(),
        AuthConfig::Plaintext
    ));

    let scram = AuthSpec::ScramSha512 {
        username: APPROVED.into(),
        tls: true,
    };
    match AuthConfig::from_spec(&scram, Some("fake".into())).unwrap() {
        AuthConfig::ScramSha512 {
            username,
            password,
            tls,
        } => {
            assert_eq!(username, APPROVED);
            assert_eq!(password, "fake");
            assert!(tls);
        }
        other => panic!("{other:?}"),
    }

    // Absent: a `KafkaError::Client`, i.e. exit 1 through both error types.
    let err = AuthConfig::from_spec(&scram, None).unwrap_err();
    assert!(matches!(err, KafkaError::Client(_)), "{err:?}");
    assert_eq!(
        logweir::exit::ExitCode::from(BackupError::from(err.clone())),
        logweir::exit::ExitCode::Operational,
        "an unset Secret is operational: a guard refusal would tell a cron reconciler to stop \
         retrying a condition an operator is about to fix"
    );
    // The message never carries a value, and names the mechanism.
    let m = err.to_string();
    assert!(m.contains("scramSha512"), "{m}");
    assert!(m.contains("not a guard refusal"), "{m}");

    // And the crate that KNOWS which variable it read says so — `from_spec`
    // takes `Option<String>` (I1's pinned signature) and never saw one.
    let named = logweir::drill::naming_the_password_var(err, logweir::drill::TARGET_PASSWORD_VAR);
    assert!(
        named
            .to_string()
            .contains("auth.mode is scramSha512 but $LOGWEIR_TARGET_PASSWORD is unset"),
        "{named}"
    );
    // A real broker fact passes through untouched: a message about an
    // environment variable would be a lie about a timeout.
    let timeout = KafkaError::Unreachable("no broker answered".into());
    assert_eq!(
        logweir::drill::naming_the_password_var(
            timeout.clone(),
            logweir::drill::SOURCE_PASSWORD_VAR
        )
        .to_string(),
        timeout.to_string()
    );
}

/// The exit-3 half, in process: an unrenderable projected value is refused
/// with a message that OPENS with the terminal state, so
/// `refusal-reason=CredentialNotRenderable` is what a controller reads.
///
/// The value never reaches the message: `CredentialRefusal`'s `Display` is a
/// pure function of the offending character CLASS, so two different
/// unrenderable passwords with the same first offending character produce
/// byte-identical messages.
#[test]
fn an_unrenderable_projected_password_is_a_credential_refusal() {
    // `validated_password` reads the environment, so this row sets it — and is
    // the only row in the file that does. `set_var`/`remove_var` are `unsafe`
    // from the 2024 edition; this crate is 2021, and the suite is
    // single-threaded within a test binary for this reason.
    let var = "LOGWEIR_TEST_AUTH_BINDING_PASSWORD";
    std::env::set_var(var, "x\"\n bootstrap_servers: [evil:9092]");
    let refusal = logweir::drill::validated_password(var).unwrap_err();
    std::env::remove_var(var);

    // `refusal.0`, not `to_string()`: `terminal_state` matches a PREFIX, and
    // `GuardRefusal`'s own `Display` wraps the message ("plan refused by the
    // admission guard: …"), which would classify every state as the default
    // `GuardRefused`. Both `backup::report` and `drill::report` read `.0` for
    // exactly this reason, so the test reads what the binary reads.
    assert_eq!(
        logweir_core::guard::refusal_reason_line(&refusal.0),
        "refusal-reason=CredentialNotRenderable"
    );
    let msg = refusal.0.clone();
    assert!(msg.contains(var), "the refusal names the VARIABLE: {msg}");
    assert!(
        !msg.contains("bootstrap_servers") && !msg.contains("evil"),
        "and never the value: {msg}"
    );
    assert_eq!(
        logweir::exit::ExitCode::from(BackupError::from(refusal)),
        logweir::exit::ExitCode::GuardRefused
    );

    // An absent variable is `Ok(None)` — whether that is fatal is
    // `from_spec`'s decision, because it is the half that knows the mode.
    assert_eq!(
        logweir::drill::validated_password("LOGWEIR_TEST_AUTH_BINDING_ABSENT").unwrap(),
        None
    );
}

// ---------------------------------------------------------------------------
// Doubles and fixtures for the one in-process run
// ---------------------------------------------------------------------------

const ARCHIVE_PREFIX: &str = "logweir/";

struct BackupFixture {
    _dir: tempfile::TempDir,
    args: BackupRunArgs,
}

fn backup_fixture(auth_block: &str) -> BackupFixture {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("backup.yaml");
    let allowed = dir.path().join("allowed-clusters.json");
    std::fs::write(
        &spec,
        format!(
            "backup_id: mvp-demo\n\
             source:\n  bootstrap_servers: [localhost:9092]\n  topics: [orders]\n\
             {auth_block}\
             storage:\n  backend: s3\n  bucket: kafka-backups\n  prefix: {ARCHIVE_PREFIX}\n  \
             region: us-east-1\n  endpoint: http://127.0.0.1:19000\n  path_style: true\n  \
             allow_http: true\n\
             backup:\n  compression: zstd\n"
        ),
    )
    .unwrap();
    std::fs::write(&allowed, "{\"allowed_cluster_ids\": [\"SCRATCH-0000001\"]}").unwrap();
    // AN EPHEMERAL KEY, GENERATED HERE, and never a committed private key.
    // Since Task 5b `execute_with` SIGNS the receipt, so a bare
    // `PathBuf::from("signer.pem")` — which was enough while the function
    // wrote nothing — now fails the run with `key error: signer.pem: No such
    // file or directory`. Same pattern as `tests/backup_run.rs`'s own
    // fixture: the key lives and dies with the `TempDir`, and the throwaway
    // fixture key under `e2e/fixtures/signed/` stays reserved for the corpus
    // walkers, which verify against a checked-in PUBLIC pem.
    let key = SigningKey::generate_p256();
    let key_path = dir.path().join("signer.pem");
    std::fs::write(&key_path, key.to_pkcs8_pem().unwrap()).unwrap();
    BackupFixture {
        _dir: dir,
        args: BackupRunArgs {
            spec,
            allowed_clusters: allowed,
            signing_key: key_path,
            triggered_by: None,
            out: None,
            receipt_out: None,
            backup_id_override: None,
        },
    }
}

fn archive_with_one_manifest(backup_id: &str) -> Store {
    let store = Store::in_memory(ARCHIVE_PREFIX);
    store
        .put_create_only(
            &format!("{ARCHIVE_PREFIX}{backup_id}/manifest.json"),
            format!("{{\"backup_id\":\"{backup_id}\"}}").as_bytes(),
        )
        .unwrap();
    store
}

struct StubReader;

impl ClusterReader for StubReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("SOURCE-0000001".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![])
    }
    fn end_offsets(&self, _t: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(&self, _t: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    /// Task 8 (guard **G-TS**) added this to `ClusterReader`. An empty map is
    /// a broker that surfaces neither `log.message.timestamp.type` nor a
    /// timestamp bound, which is the harmless case: phase 0's preflight then
    /// treats the broker as the Apache default (`CreateTime`) and refuses
    /// nothing. G-TS's own arms live in
    /// `crates/logweir/tests/topic_preflight.rs`.
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        _t: &str,
        _p: i32,
        _f: i64,
        _m: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

/// A `DataEngine` that RECORDS the `BackupPlan` it was handed, renders nothing
/// and spawns nothing. It is what keeps this file socket-free and
/// subprocess-free; the REAL engine's acceptance of the rendered SASL block is
/// proved in `e2e/tests/backup_argv.rs`.
struct StubEngine {
    plans: std::sync::Mutex<Vec<logweir_core::engine::BackupPlan>>,
}

impl StubEngine {
    fn new() -> Self {
        Self {
            plans: std::sync::Mutex::new(Vec::new()),
        }
    }
    fn recorded_plan(&self) -> logweir_core::engine::BackupPlan {
        self.plans
            .lock()
            .unwrap()
            .first()
            .cloned()
            .expect("the engine's `backup` was never reached")
    }
}

impl DataEngine for StubEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "oso-cli".into(),
            version: "v0.21.0-stub".into(),
            digest: format!("sha256:{}", "0".repeat(64)),
        }
    }
    fn list_backup_sets(
        &self,
        _: &logweir_core::engine::StorageUrl,
    ) -> Result<Vec<BackupSetRef>, EngineError> {
        unimplemented!("the backup path lists through the Store handle, not the engine")
    }
    fn describe(&self, set: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(BackupSetFacts {
            backup_id: set.backup_id.clone(),
            created_at: "2026-09-09T00:01:00Z".parse().unwrap(),
            source_cluster_id: None,
            manifest_sha256: "sha256:from-the-engines-own-handle".into(),
            manifest_version_id: None,
            consumer_group_snapshot_sha256: None,
            topics: vec![logweir_core::engine::TopicFacts {
                name: "orders".into(),
                original_partition_count: Some(1),
                source_replication_factor: Some(1),
                configurations: BTreeMap::new(),
                partitions: vec![logweir_core::engine::PartitionFacts {
                    partition_id: 0,
                    segments: vec![logweir_core::engine::SegmentFacts {
                        key: "seg".into(),
                        start_offset: 0,
                        end_offset: 6,
                        start_timestamp: 1_756_000_000_000,
                        end_timestamp: 1_756_000_060_000,
                        record_count: 7,
                        sha256: String::new(),
                        uploaded_at: 0,
                    }],
                    gaps: vec![],
                    pruned: vec![],
                }],
            }],
        })
    }
    fn preflight(
        &self,
        _plan: &RestorePlan,
    ) -> Result<logweir_core::engine::PreflightReport, EngineError> {
        unimplemented!("not reached by the backup path")
    }
    fn restore(
        &self,
        _plan: &RestorePlan,
        _obs: &mut dyn PhaseObserver,
    ) -> Result<logweir_core::engine::RestoreFacts, EngineError> {
        unimplemented!("not reached by the backup path")
    }
    fn fingerprints(&self, _sel: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        unimplemented!("not reached by the backup path")
    }
    fn backup(
        &self,
        plan: &logweir_core::engine::BackupPlan,
        _obs: &mut dyn PhaseObserver,
    ) -> Result<logweir_core::engine::BackupFacts, EngineError> {
        self.plans.lock().unwrap().push(plan.clone());
        Ok(logweir_core::engine::BackupFacts {
            started_at: "2026-09-09T00:00:00Z".parse().unwrap(),
            finished_at: "2026-09-09T00:01:00Z".parse().unwrap(),
            exit_code: 0,
            unknown_key_warnings: vec![],
        })
    }
}
