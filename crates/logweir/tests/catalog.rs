//! **The recovery catalog's named tests (PLAT-15.1, decision D3 §5).**
//!
//! Every test runs in process against `Store::in_memory` or a tempdir
//! filesystem store — no broker, no bucket, no subprocess — for the reason
//! `crates/logweir/tests/backup_run.rs` records in full: with the compose
//! stack down an rdkafka metadata call blocks for 20 s, and Global Constraint
//! 22's bound is 15 s per `#[test]`. The one leg that needs a real
//! S3-compatible server is `crates/logweir/tests/catalog_minio.rs`, which
//! carries the repository's e2e gating attribute.
//!
//! This file names `Store::from_url` once, over a TEMPDIR filesystem URL, and
//! carries an entry on `no_network_in_unit_tests.rs`'s allow-list saying so.
//! The attribute that exempts a whole file is deliberately NOT quoted anywhere
//! here: a doc comment mentioning it would exempt this file from the dial-token
//! audit by accident, which is the quietest possible way to lose that gate.
//!
//! # Map of what is asserted where
//!
//! | claim | test |
//! |---|---|
//! | identity is the receipt's digest, and two receipts under one `backup_id` are two points (defect RECEIPT-DUP) | `the_point_id_is_derived_from_the_receipt_bytes`, `two_receipts_for_one_backup_id_are_two_points` |
//! | the record's bytes are deterministic and are what gets signed | `the_record_bytes_are_deterministic_and_are_what_is_signed` |
//! | reader rule 1 — a higher major is per-entry, never fatal | `a_record_from_a_future_major_is_unsupported_not_corrupt` |
//! | reader rule 2 — unknown fields ignored, absent optional fields are UNKNOWN | `an_unknown_field_inside_major_1_is_ignored`, `absent_optional_fields_read_as_unknown_never_zero` |
//! | reader rule 3 — the receipt-derived facts are recomputed and a disagreement is `RecordMismatch` | `a_record_that_contradicts_its_receipt_is_a_record_mismatch` |
//! | duplicate identity — one point in two places, or a conflict | `one_receipt_in_two_buckets_is_one_point_with_two_locations`, `two_records_that_disagree_for_one_identity_are_a_conflict` |
//! | rule 4 — create-only, never rewritten | `the_three_catalog_objects_are_create_only`, `a_second_write_of_one_point_rewrites_nothing` |
//! | the fifth put and the `catalog-key=` line | `a_successful_backup_writes_its_catalog_point`, `the_catalog_key_line_comes_before_interface_i7s_pair` |
//! | a failed catalog write is a warning | `a_catalog_write_that_fails_leaves_the_run_and_its_receipt_untouched`, `the_catalog_write_cannot_become_a_failure_by_one_question_mark` |
//! | `catalog sync` backfills, is idempotent, and refuses to record what it cannot verify | the `sync_*` rows |
//! | CLI exit codes and the URL guard | the `catalog_*_exits` and `evidence_url_*` rows |
//! | the published schema still describes the type | `the_checked_in_catalog_point_schema_is_the_one_the_type_generates` |

use logweir::catalog::cli::{ListArgs, Location, PointOutcome, SyncArgs};
use logweir::catalog::reader::{self, CrossCheck, Duplicate, RecordVerdict};
use logweir::catalog::record::*;
use logweir::catalog::writer::{self, PutState, RecordInputs};
use logweir_core::backup_receipt::*;
use logweir_engine_oso::storage::Store;
use logweir_evidence::keys::SigningKey;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
    s.parse().unwrap()
}

/// A receipt as `logweir backup run` writes one. `run_id` is a parameter
/// because the RECEIPT-DUP row needs two receipts for one `backup_id`.
fn receipt(backup_id: &str, run_id: &str) -> BackupReceipt {
    BackupReceipt {
        format_version: "1.0.0".into(),
        run_id: run_id.into(),
        backup_id: backup_id.into(),
        requested_at: ts("2026-09-15T02:59:00Z"),
        started_at: ts("2026-09-15T03:00:00Z"),
        finished_at: ts("2026-09-15T03:04:00Z"),
        exit_code: 0,
        triggered_by: "schedule".into(),
        source: ReceiptSource {
            cluster_id: "SOURCE-CLUSTER-000001".into(),
            bootstrap_servers: vec!["kafka-source:9092".into()],
            auth: ReceiptAuth {
                mode: "scramSha512".into(),
                username: Some("logweir".into()),
            },
            topics: vec!["orders".into()],
        },
        engine: ReceiptEngine {
            id: "oso".into(),
            version: "0.21.0".into(),
            digest: "sha256:".to_string() + &"d".repeat(64),
        },
        archive: ReceiptArchive {
            manifest_key: format!("prod/{backup_id}/manifest.json"),
            manifest_sha256: "sha256:".to_string() + &"b".repeat(64),
            prefix: "prod".into(),
        },
        records: BTreeMap::from([("orders".to_string(), 1234u64)]),
        covered: ReceiptCovered {
            from_ms: 1_757_980_800_000,
            to_ms: 1_757_984_400_000,
        },
    }
}

fn receipt_bytes(r: &BackupReceipt) -> Vec<u8> {
    logweir_core::det_json::to_deterministic_json(r).unwrap()
}

fn inputs_for(r: &BackupReceipt, location: &str, key: &SigningKey) -> RecordInputs {
    let keys = logweir::backup::phase_run::receipt_keys(&r.backup_id, &r.run_id);
    RecordInputs {
        receipt_key: keys.receipt_key,
        sidecar_key: keys.sidecar_key,
        location_id: location.to_string(),
        recorded_at: ts("2026-09-16T00:00:00Z"),
        signing: logweir::catalog::signing_of(&key.verifying_key()),
        installation: Some(logweir::catalog::RecordInstallation {
            key_id: key.verifying_key().key_id(),
        }),
        execution: None,
    }
}

fn point_for(r: &BackupReceipt, location: &str, key: &SigningKey) -> CatalogPoint {
    let bytes = receipt_bytes(r);
    writer::from_receipt(r, &bytes, &inputs_for(r, location, key)).unwrap()
}

/// A validated signer over an ephemeral key, written to (and living in) `dir`.
///
/// Ephemeral and never a committed private key: the throwaway fixture key under
/// `e2e/fixtures/signed/` is reserved for the corpus walkers.
fn signer_in(dir: &Path) -> (logweir::signer::ValidatedSigner, SigningKey, PathBuf) {
    let key = SigningKey::generate_p256();
    let path = dir.join("signer.pem");
    std::fs::write(&path, key.to_pkcs8_pem().unwrap()).unwrap();
    let validated = logweir::backup::phase_run::load_signer(&path).unwrap();
    (validated, key, path)
}

// ---------------------------------------------------------------------------
// §5.1 — identity
// ---------------------------------------------------------------------------

#[test]
fn the_point_id_is_derived_from_the_receipt_bytes() {
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let bytes = receipt_bytes(&r);
    let id = point_id(&bytes);

    // The shape: `lwp1-` plus 32 lowercase hex characters (128 bits).
    assert!(id.starts_with("lwp1-"), "{id}");
    let hex = &id["lwp1-".len()..];
    assert_eq!(hex.len(), 32, "{id}");
    assert!(
        hex.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "the id is LOWERCASE hex, so two writers cannot spell one identity two ways: {id}"
    );

    // It is the receipt's own digest, not a re-hash of a re-serialisation.
    let digest = logweir_core::ids::sha256_hex(&bytes);
    assert_eq!(hex, &digest[..32]);

    // Deterministic: the same bytes are the same point, every time. This is
    // what makes repeated import idempotent (D3 §5.5 step 2).
    assert_eq!(id, point_id(&bytes));

    // …and one flipped byte is a different point. The mutant this kills is an
    // id taken over the MANIFEST digest (tracker defect RECEIPT-DUP): the
    // manifest is identical across the two runs below, so such an id would be
    // stable here and would collapse them.
    let mut flipped = bytes.clone();
    let at = flipped.iter().position(|b| *b == b'0').unwrap();
    flipped[at] = b'9';
    assert_ne!(id, point_id(&flipped));
}

#[test]
fn two_receipts_for_one_backup_id_are_two_points() {
    // **Tracker defect RECEIPT-DUP, and the reason identity is content-derived.**
    // A Backup Job re-created from its frozen inputs writes a SECOND run-id
    // receipt under the same execution id while overwriting the manifest at
    // the same key. Run identity is idempotent; signed evidence is not. Both
    // receipts below name the same `backup_id` AND the same manifest key and
    // digest — so an identity derived from either of those would make one run
    // disappear.
    let a = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let b = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5B");
    assert_eq!(a.backup_id, b.backup_id);
    assert_eq!(a.archive.manifest_key, b.archive.manifest_key);
    assert_eq!(a.archive.manifest_sha256, b.archive.manifest_sha256);

    let id_a = point_id(&receipt_bytes(&a));
    let id_b = point_id(&receipt_bytes(&b));
    assert_ne!(
        id_a, id_b,
        "two receipts under one backup_id must be TWO points: that distinction is what \
         `recovery point` means, and collapsing them is defect RECEIPT-DUP"
    );

    // …and they share the `backup_id`, which is the archive SET identifier.
    let dir = tempfile::tempdir().unwrap();
    let (_, key, _) = signer_in(dir.path());
    let pa = point_for(&a, "s3://kafka-backups/prod", &key);
    let pb = point_for(&b, "s3://kafka-backups/prod", &key);
    assert_eq!(pa.backup_id, pb.backup_id);
    assert_ne!(pa.point_id, pb.point_id);

    // Two points, two log keys — so the day shard lists both.
    assert_ne!(pa.log_key(), pb.log_key());
}

// ---------------------------------------------------------------------------
// The record document
// ---------------------------------------------------------------------------

#[test]
fn the_record_bytes_are_deterministic_and_are_what_is_signed() {
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let point = point_for(&r, "s3://kafka-backups/prod", &key);

    let a = point.canonical_bytes().unwrap();
    let b = point.canonical_bytes().unwrap();
    assert_eq!(a, b, "the canonical encoding is a function of the value");
    assert_eq!(
        *a.last().unwrap(),
        b'\n',
        "deterministic JSON ends in a newline"
    );

    // Declaration order IS byte order, which is the property a reviewer reads
    // a drift diff against.
    let text = String::from_utf8(a.clone()).unwrap();
    let order: Vec<usize> = [
        "\"format_version\"",
        "\"point_id\"",
        "\"recorded_at\"",
        "\"receipt\"",
        "\"backup_id\"",
        "\"run_id\"",
        "\"archive\"",
        "\"covered\"",
        "\"capture\"",
        "\"topics\"",
        "\"source\"",
        "\"signing\"",
    ]
    .iter()
    .map(|f| {
        text.find(f)
            .unwrap_or_else(|| panic!("{f} is in the document:\n{text}"))
    })
    .collect();
    let mut sorted = order.clone();
    sorted.sort_unstable();
    assert_eq!(
        order, sorted,
        "the fields serialise in declaration order:\n{text}"
    );

    // And those exact bytes are what verifies. A document re-rendered after
    // signing does not verify, which is why `put_point` serialises once.
    let sidecar = signer
        .sign(logweir::catalog::PAYLOAD_TYPE_CATALOG_POINT, &a)
        .unwrap();
    logweir_evidence::verify::verify_detached(
        &key.verifying_key(),
        logweir::catalog::PAYLOAD_TYPE_CATALOG_POINT,
        &a,
        &sidecar,
    )
    .expect("the sidecar verifies over the canonical bytes");
}

#[test]
fn the_record_carries_no_credential_and_no_principal() {
    let dir = tempfile::tempdir().unwrap();
    let (_, key, _) = signer_in(dir.path());
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    assert_eq!(
        r.source.auth.username.as_deref(),
        Some("logweir"),
        "the fixture receipt DOES carry a principal, so the assertion below is about a \
         value that was available and was not copied"
    );
    let text = String::from_utf8(
        point_for(&r, "s3://kafka-backups/prod", &key)
            .canonical_bytes()
            .unwrap(),
    )
    .unwrap();
    assert!(
        !text.contains("username") && !text.contains("\"logweir\""),
        "a catalog is the surface an operator lists in bulk; the SASL principal is not a \
         fact a recovery point needs:\n{text}"
    );
    // The auth MECHANISM is carried, because "how was this captured" is a fact
    // about the backup. One spelling in this product.
    assert!(text.contains("\"scramSha512\""), "{text}");
}

#[test]
fn the_recovery_point_is_the_capture_start_and_the_log_key_shards_on_it() {
    // **D3 §3.2, the decision that matters.** Freshness is measured from the
    // capture START, never from `covered.to_ms` — that is the newest RECORD
    // instant, so an idle topic would look stale forever — and never from
    // `finished_at`, which under-reports the gap for a long capture.
    let dir = tempfile::tempdir().unwrap();
    let (_, key, _) = signer_in(dir.path());
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let point = point_for(&r, "s3://kafka-backups/prod", &key);
    assert_eq!(point.recovery_point_at(), r.started_at);
    assert_ne!(point.recovery_point_at(), r.finished_at);

    let key_str = point.log_key();
    assert!(
        key_str.starts_with("logweir/catalog/v1/log/2026/09/15/"),
        "the shard is the recovery point's UTC day: {key_str}"
    );
    let stem = key_str.rsplit('/').next().unwrap();
    let (ms, rest) = stem.split_once('-').unwrap();
    assert_eq!(
        ms.len(),
        13,
        "the millisecond is zero-padded to 13 so a shard sorts by time: {stem}"
    );
    assert_eq!(ms.parse::<i64>().unwrap(), r.started_at.timestamp_millis());
    assert!(
        rest.starts_with("lwp1-") && rest.ends_with(".json"),
        "{stem}"
    );

    // The pairing that makes a listing meaningful: within one shard, an
    // earlier capture sorts before a later one.
    let mut earlier = r.clone();
    earlier.started_at = ts("2026-09-15T01:00:00Z");
    earlier.run_id = "01J9X2QK7C4V0R8YB3ZP6MTS5B".into();
    let earlier_point = point_for(&earlier, "s3://kafka-backups/prod", &key);
    assert!(
        earlier_point.log_key() < key_str,
        "{} must sort before {key_str}",
        earlier_point.log_key()
    );
}

#[test]
fn the_index_entry_is_a_copy_of_the_record_and_carries_no_signature() {
    let dir = tempfile::tempdir().unwrap();
    let (_, key, _) = signer_in(dir.path());
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let point = point_for(&r, "s3://kafka-backups/prod", &key);
    let entry = CatalogLogEntry::of(&point);

    assert_eq!(entry.point_id, point.point_id);
    assert_eq!(entry.backup_id, point.backup_id);
    assert_eq!(entry.run_id, point.run_id);
    assert_eq!(entry.covered, point.covered);
    assert_eq!(entry.receipt_sha256, point.receipt.sha256);
    assert_eq!(entry.record_key, record_key(&point.point_id));
    assert_eq!(
        entry.recovery_point_at_ms,
        point.recovery_point_at().timestamp_millis()
    );

    // The index is NOT evidence, and the shape says so: no signature, no
    // sidecar key, and a `record_key` that names the document a reader must go
    // and verify instead.
    let text = String::from_utf8(entry.canonical_bytes().unwrap()).unwrap();
    assert!(!text.contains("signature"), "{text}");
    assert!(!text.contains("sidecar"), "{text}");
    assert!(text.contains(&record_key(&point.point_id)), "{text}");
}

#[test]
fn the_location_id_is_bucket_and_prefix_and_nothing_else() {
    use logweir_core::engine::StorageUrl as U;
    // NO endpoint, NO region, NO addressing style — and no userinfo, because a
    // `StorageUrl` has none and this function reads two fields.
    assert_eq!(
        location_id(&U::S3 {
            bucket: "kafka-backups".into(),
            prefix: "prod".into(),
            region: Some("eu-west-1".into()),
            endpoint: Some("https://minio.example:9000".into()),
            path_style: true,
            allow_http: false,
        }),
        "s3://kafka-backups/prod"
    );
    assert_eq!(
        location_id(&U::S3 {
            bucket: "kafka-backups".into(),
            prefix: String::new(),
            region: None,
            endpoint: None,
            path_style: false,
            allow_http: false,
        }),
        "s3://kafka-backups"
    );
    assert_eq!(
        location_id(&U::Gcs {
            bucket: "b".into(),
            prefix: "p".into()
        }),
        "gs://b/p"
    );
    assert_eq!(
        location_id(&U::Azure {
            account_name: "acct".into(),
            container_name: "c".into(),
            prefix: "p".into()
        }),
        "az://acct/c/p"
    );
    assert_eq!(
        location_id(&U::Filesystem {
            path: "/var/archive".into()
        }),
        "file:///var/archive"
    );
}

// ---------------------------------------------------------------------------
// §5.2 reading rules
// ---------------------------------------------------------------------------

fn sample_point() -> CatalogPoint {
    let dir = tempfile::tempdir().unwrap();
    let (_, key, _) = signer_in(dir.path());
    point_for(
        &receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A"),
        "s3://kafka-backups/prod",
        &key,
    )
}

#[test]
fn a_record_from_a_future_major_is_unsupported_not_corrupt() {
    // **Rule 1.** Per-ENTRY and never fatal for a sync: a catalog written by a
    // newer Logweir still lists, with this entry marked.
    let mut v: serde_json::Value =
        serde_json::from_slice(&sample_point().canonical_bytes().unwrap()).unwrap();
    v["format_version"] = serde_json::json!("2.0.0");
    // …and a shape major 1 could not hold, to prove the version is read BEFORE
    // the typed deserialisation. A reader that parsed first would report "not
    // a record" for a document it merely does not implement yet.
    v["topics"] = serde_json::json!({"orders": {"segments": 4}});
    match reader::read_record(&serde_json::to_vec(&v).unwrap()) {
        RecordVerdict::UnsupportedFormat { format_version } => {
            assert_eq!(format_version, "2.0.0")
        }
        other => panic!("a major-2 record is UnsupportedFormat, got {other:?}"),
    }
    // A HIGHER MINOR inside major 1 is readable: a minor bump adds optional
    // fields and a 1.0.0 reader must still read it.
    let mut v: serde_json::Value =
        serde_json::from_slice(&sample_point().canonical_bytes().unwrap()).unwrap();
    v["format_version"] = serde_json::json!("1.7.3");
    assert!(matches!(
        reader::read_record(&serde_json::to_vec(&v).unwrap()),
        RecordVerdict::Point(_)
    ));
}

#[test]
fn bytes_that_are_not_a_record_are_unreadable_and_named_as_such() {
    for (bytes, why) in [
        (&b"not json at all"[..], "not valid JSON"),
        (&b"{}"[..], "no `format_version`"),
        (&br#"{"format_version": "1"}"#[..], "is not a semver"),
        (
            &br#"{"format_version": "one.two.three"}"#[..],
            "is not a semver",
        ),
        (
            &br#"{"format_version": "1.0.0"}"#[..],
            "is not a major-1 catalog point record",
        ),
    ] {
        match reader::read_record(bytes) {
            RecordVerdict::Unreadable(msg) => assert!(
                msg.contains(why),
                "reading {:?} should say {why:?}, said {msg:?}",
                String::from_utf8_lossy(bytes)
            ),
            other => panic!(
                "{:?} must be Unreadable, got {other:?}",
                String::from_utf8_lossy(bytes)
            ),
        }
    }
}

#[test]
fn an_unknown_field_inside_major_1_is_ignored() {
    // **Rule 2, first half.** There is deliberately no `deny_unknown_fields`:
    // a 1.0.0 reader must read a 1.1.0 record. The mutant this kills is
    // exactly that attribute added to `CatalogPoint`.
    let mut v: serde_json::Value =
        serde_json::from_slice(&sample_point().canonical_bytes().unwrap()).unwrap();
    v["something_a_later_minor_added"] = serde_json::json!({"deep": [1, 2, 3]});
    v["archive"]["a_nested_unknown"] = serde_json::json!("x");
    match reader::read_record(&serde_json::to_vec(&v).unwrap()) {
        RecordVerdict::Point(p) => {
            assert_eq!(p.backup_id, "nightly-20260915");
            assert_eq!(p.archive.location_id, "s3://kafka-backups/prod");
        }
        other => panic!("an unknown field inside major 1 is ignored, got {other:?}"),
    }
}

#[test]
fn absent_optional_fields_read_as_unknown_never_zero() {
    // **Rule 2, second half — the one that matters most.** An absent
    // `topics[].partitions` blocks D3 §4.2's `maxPartitions` filter; a `0`
    // would let that filter accept a point it knows nothing about. An absent
    // `execution` is an archive imported from another installation.
    let p = sample_point();
    assert_eq!(
        p.topics[0].partitions, None,
        "a backup receipt records no partition count, so the record must say UNKNOWN"
    );

    // **The `execution` block carries exactly what the writer had in hand**
    // (review finding F4). The Backup Job's argv passes the runner no
    // namespace, name, UID or `inputsSha256`, so those stay UNKNOWN — but
    // `triggered_by` is in the receipt and dropping it was discarding
    // provenance the writer held.
    let execution = p
        .execution
        .as_ref()
        .expect("the receipt's triggered_by is carried");
    assert_eq!(execution.triggered_by.as_deref(), Some("schedule"));
    assert_eq!(execution.namespace, None);
    assert_eq!(execution.name, None);
    assert_eq!(execution.uid, None);
    assert_eq!(execution.kind, None);
    assert_eq!(execution.schedule, None);
    assert_eq!(
        execution.inputs_sha256, None,
        "D-SEAMS S4: this record may CITE PLAT-06.1's inputs_sha256 and may never \
         invent one — the Backup Job's argv does not carry it"
    );
    assert_eq!(
        execution.execution_id, None,
        "`backup_id` happens to equal the execution id on a controller-driven run, but the \
         runner cannot tell an execution id from a schedule slot, and a field that is right \
         on one path and a fabrication on the other is worse than an absent one"
    );

    // And the absent fields are ABSENT from the bytes, not `null`: a reader in
    // another language must see nothing, not a value.
    let text = String::from_utf8(p.canonical_bytes().unwrap()).unwrap();
    assert!(!text.contains("partitions"), "{text}");
    assert!(!text.contains("inputs_sha256"), "{text}");
    assert!(!text.contains("namespace"), "{text}");

    // Round-tripping keeps them unknown rather than filling them in.
    match reader::read_record(p.canonical_bytes().unwrap().as_slice()) {
        RecordVerdict::Point(back) => {
            assert_eq!(back.topics[0].partitions, None);
            assert_eq!(back.execution, p.execution);
            assert_eq!(back.topics[0].records, 1234, "a KNOWN count still reads");
        }
        other => panic!("{other:?}"),
    }

    // A receipt that says nothing about what triggered it leaves the WHOLE
    // block absent. `""` is "the operator said nothing", and writing it would
    // turn an absence into a value — and an object of eight nulls is not
    // "unknown" written down, it is an empty object pretending to be a fact.
    let dir = tempfile::tempdir().unwrap();
    let (_, key, _) = signer_in(dir.path());
    let mut untriggered = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    untriggered.triggered_by = String::new();
    let bare = point_for(&untriggered, "s3://kafka-backups/prod", &key);
    assert_eq!(bare.execution, None);
    let text = String::from_utf8(bare.canonical_bytes().unwrap()).unwrap();
    assert!(!text.contains("execution"), "{text}");
}

#[test]
fn a_record_that_contradicts_its_receipt_is_a_record_mismatch() {
    // **Rule 3.** Everything except the receipt-derived facts is
    // informational; those are recomputed from the VERIFIED receipt and a
    // disagreeing copy is `RecordMismatch` (availability `Conflict`, D3 §5.4).
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let bytes = receipt_bytes(&r);
    let good = sample_point();
    assert_eq!(reader::cross_check(&good, &r, &bytes), CrossCheck::Agrees);

    for (label, mutate) in [
        (
            "covered.to_ms",
            Box::new(|p: &mut CatalogPoint| p.covered.to_ms = 1) as Box<dyn Fn(&mut CatalogPoint)>,
        ),
        (
            "backup_id",
            Box::new(|p: &mut CatalogPoint| p.backup_id = "someone-elses-set".into()),
        ),
        (
            "run_id",
            Box::new(|p: &mut CatalogPoint| p.run_id = "01OTHER".into()),
        ),
        (
            "capture.started_at",
            Box::new(|p: &mut CatalogPoint| p.capture.started_at = ts("1999-01-01T00:00:00Z")),
        ),
        (
            "archive.manifest_sha256",
            Box::new(|p: &mut CatalogPoint| {
                p.archive.manifest_sha256 = "sha256:".to_string() + &"0".repeat(64)
            }),
        ),
    ] {
        let mut bad = good.clone();
        mutate(&mut bad);
        match reader::cross_check(&bad, &r, &bytes) {
            CrossCheck::RecordMismatch(d) => assert!(
                d.iter().any(|m| m.starts_with(label)),
                "a record disagreeing on {label} must name it: {d:?}"
            ),
            other => panic!("{label}: expected RecordMismatch, got {other:?}"),
        }
    }

    // The INFORMATIONAL fields do NOT make a mismatch: the same point written
    // by two installations into two buckets differs in all of them.
    let mut elsewhere = good.clone();
    elsewhere.archive.location_id = "s3://dr-copy/prod".into();
    elsewhere.recorded_at = ts("2027-01-01T00:00:00Z");
    elsewhere.signing.key_id = "f".repeat(64);
    elsewhere.source.bootstrap_servers = vec!["renamed:9092".into()];
    assert_eq!(
        reader::cross_check(&elsewhere, &r, &bytes),
        CrossCheck::Agrees,
        "location, recording time, signer and addressing are informational (rule 3)"
    );
}

#[test]
fn a_record_that_names_other_bytes_is_not_compared_at_all() {
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let other = receipt_bytes(&receipt("nightly-20260915", "01OTHER"));
    match reader::cross_check(&sample_point(), &r, &other) {
        CrossCheck::WrongReceipt { claimed, actual } => assert_ne!(claimed, actual),
        other => panic!(
            "a record whose digest is not these bytes' is WrongReceipt, not a field \
             comparison: {other:?}"
        ),
    }
}

#[test]
fn one_receipt_in_two_buckets_is_one_point_with_two_locations() {
    // D3 §5.1: "the same archive copied to a second bucket yields the same
    // point with two `locations[]` rather than a duplicate".
    let a = sample_point();
    let mut b = a.clone();
    b.archive.location_id = "s3://dr-copy/prod".into();
    b.recorded_at = ts("2027-01-01T00:00:00Z");
    match reader::reconcile(&a, &b) {
        Duplicate::SameIdentity { locations } => assert_eq!(
            locations,
            vec![
                "s3://dr-copy/prod".to_string(),
                "s3://kafka-backups/prod".to_string()
            ]
        ),
        other => panic!("{other:?}"),
    }
}

#[test]
fn two_records_that_disagree_for_one_identity_are_a_conflict() {
    let a = sample_point();
    let mut b = a.clone();
    b.covered.from_ms = 0;
    match reader::reconcile(&a, &b) {
        Duplicate::Conflict(d) => {
            assert!(d.iter().any(|m| m.starts_with("covered.from_ms")), "{d:?}")
        }
        other => panic!("{other:?}"),
    }

    // Different ids are not a duplicate at all, and saying so is not the same
    // as saying they agree.
    let mut c = a.clone();
    c.point_id = "lwp1-".to_string() + &"e".repeat(32);
    assert_eq!(reader::reconcile(&a, &c), Duplicate::DifferentPoints);
}

// ---------------------------------------------------------------------------
// §5.2 rule 4 — create-only
// ---------------------------------------------------------------------------

#[test]
fn the_three_catalog_objects_are_create_only() {
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    let point = point_for(
        &receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A"),
        "s3://kafka-backups/prod",
        &key,
    );
    let entry = CatalogLogEntry::of(&point);
    let out = writer::put_point(&point, &entry, &signer, &store).unwrap();

    assert_eq!(out.record, PutState::Created);
    assert_eq!(out.sidecar, PutState::Created);
    assert_eq!(out.log, PutState::Created);
    assert!(out.created_record());

    // All three under `logweir/catalog/v1/`, and nowhere else.
    for k in [
        &out.keys.record_key,
        &out.keys.sidecar_key,
        &out.keys.log_key,
    ] {
        assert!(
            k.starts_with("logweir/catalog/v1/"),
            "a catalog write outside its own prefix: {k}"
        );
    }
    assert_eq!(out.keys.record_key, record_key(&point.point_id));
    assert_eq!(out.keys.sidecar_key, record_sidecar_key(&point.point_id));

    // The sidecar verifies over the stored record bytes, under the catalog's
    // own media type — not the receipt's, which would make a record verifiable
    // as a receipt.
    let stored = store.get(&out.keys.record_key).unwrap().0;
    let sidecar: logweir_evidence::Sidecar =
        serde_json::from_slice(&store.get(&out.keys.sidecar_key).unwrap().0).unwrap();
    assert_eq!(
        sidecar.payload_type,
        logweir::catalog::PAYLOAD_TYPE_CATALOG_POINT
    );
    logweir_evidence::verify::verify_detached(
        &key.verifying_key(),
        logweir::catalog::PAYLOAD_TYPE_CATALOG_POINT,
        &stored,
        &sidecar,
    )
    .expect("the stored sidecar verifies over the stored bytes");
}

#[test]
fn a_second_write_of_one_point_rewrites_nothing() {
    // Rule 4, and D3 §5.5 step 2's "repeated import is idempotent": a second
    // write is `AlreadyPresent`, not an error and not an overwrite.
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let point = point_for(&r, "s3://kafka-backups/prod", &key);
    let entry = CatalogLogEntry::of(&point);
    let first = writer::put_point(&point, &entry, &signer, &store).unwrap();
    let before = store.get(&first.keys.record_key).unwrap().0;

    // A SECOND record for the same point, differing in an informational field
    // — which is what a second installation backfilling the same archive
    // produces.
    let mut second_point = point.clone();
    second_point.archive.location_id = "s3://dr-copy/prod".into();
    second_point.recorded_at = ts("2027-01-01T00:00:00Z");
    let second = writer::put_point(
        &second_point,
        &CatalogLogEntry::of(&second_point),
        &signer,
        &store,
    )
    .unwrap();
    assert_eq!(second.record, PutState::AlreadyPresent);
    assert_eq!(second.sidecar, PutState::AlreadyPresent);
    assert_eq!(second.log, PutState::AlreadyPresent);
    assert!(!second.created_record());
    assert_eq!(
        store.get(&first.keys.record_key).unwrap().0,
        before,
        "nothing under logweir/ is ever rewritten (D3 §5.2 rule 4)"
    );
}

#[test]
fn a_record_whose_id_is_not_its_digest_is_never_signed() {
    // The one claim in the document a reader cannot recompute without fetching
    // the receipt. A record where they disagree would answer a lookup by id
    // with a different backup's window, so it is refused BEFORE signing and
    // nothing is put.
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    let mut point = point_for(
        &receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A"),
        "s3://kafka-backups/prod",
        &key,
    );
    point.point_id = "lwp1-".to_string() + &"0".repeat(32);
    let err = writer::put_point(&point, &CatalogLogEntry::of(&point), &signer, &store)
        .expect_err("a record whose id contradicts its digest must be refused");
    assert!(
        err.nothing_was_uploaded(),
        "a refusal that precedes every put may truthfully say nothing was uploaded: {err:?}"
    );
    assert!(
        matches!(err, writer::PutError::SelfContradicting(_)),
        "{err:?}"
    );
    assert!(
        err.to_string()
            .contains("is not the one its receipt digest"),
        "{err}"
    );
    assert!(
        store.list_keys("logweir/catalog/").unwrap().is_empty(),
        "the refusal happens before any put"
    );
}

// ---------------------------------------------------------------------------
// The fifth put, in `backup run`
// ---------------------------------------------------------------------------

mod backup_seam;

#[test]
fn a_successful_backup_writes_its_catalog_point() {
    let f = backup_seam::Fixture::new();
    let evidence = Store::in_memory("logweir/");
    let outcome = f.execute(&evidence).expect("the backup succeeds");

    let catalog_key = outcome
        .catalog_key
        .as_deref()
        .expect("a successful backup writes its catalog point");
    assert_eq!(
        catalog_key,
        record_key(&point_id(&evidence.get(&outcome.receipt_key).unwrap().0))
    );

    // The record is derived from the receipt that was actually PUT — not from
    // a re-serialisation — so its facts and the receipt's agree by
    // construction.
    let receipt_bytes = evidence.get(&outcome.receipt_key).unwrap().0;
    let parsed: BackupReceipt = serde_json::from_slice(&receipt_bytes).unwrap();
    let record = evidence.get(catalog_key).unwrap().0;
    match reader::read_record(&record) {
        RecordVerdict::Point(p) => {
            assert_eq!(
                reader::cross_check(&p, &parsed, &receipt_bytes),
                CrossCheck::Agrees
            );
            assert_eq!(p.receipt.key, outcome.receipt_key);
            assert_eq!(p.receipt.sidecar_key, outcome.sidecar_key);
            // The installation is established, not assumed: the key that
            // signed the receipt is the key that signed this record.
            assert_eq!(
                p.installation.as_ref().map(|i| i.key_id.clone()),
                Some(p.signing.key_id.clone())
            );
            // ONE SPELLING IN THIS PRODUCT — the same string
            // `logweir identity bootstrap` publishes for the same key.
            assert_eq!(p.signing.algorithm, "ecdsa-p256-sha256");

            // …and the log entry landed in the shard the record's own
            // recovery point names.
            let entry_bytes = evidence.get(&p.log_key()).unwrap().0;
            let entry: CatalogLogEntry = serde_json::from_slice(&entry_bytes).unwrap();
            assert_eq!(entry.point_id, p.point_id);
        }
        other => panic!("{other:?}"),
    }

    // Exactly six objects: the execution claim (RECEIPT-DUP, taken before the
    // engine), receipt, sidecar, record, record.sig, log entry.
    let mut keys = evidence.list_keys("logweir/").unwrap();
    keys.retain(|k| !k.starts_with("logweir/archive-fixture/"));
    assert_eq!(keys.len(), 6, "{keys:?}");
    assert!(
        keys.contains(&logweir::backup::phase_run::claim_key(
            backup_seam::BACKUP_ID
        )),
        "{keys:?}"
    );
}

#[test]
fn the_catalog_key_line_comes_before_interface_i7s_pair() {
    // **Interface I7 is `receipt-key=` then `sidecar-key=` as the FINAL two
    // stdout lines, with nothing after them.** Two landed tests read it that
    // way (`backup_run.rs`'s I7 row takes the last two lines,
    // `e2e/tests/backup_argv.rs` asserts the final line is `sidecar-key=`), so
    // the optional catalog line goes in FRONT of them. A reader finds it by
    // prefix, which is how every controller reads these lines.
    //
    // Asserted on the SOURCE because the printing happens in `exiting`, which
    // is private and prints to the process's own stdout; the behavioural half
    // is `e2e/tests/backup_argv.rs`'s existing final-line assertion, which
    // this ordering is what keeps green.
    let src =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/backup/mod.rs"))
            .unwrap();
    let catalog_at = src
        .find("println!(\"catalog-key=")
        .expect("the catalog line is printed");
    let receipt_at = src
        .find("println!(\"receipt-key=")
        .expect("I7's first line");
    let sidecar_at = src
        .find("println!(\"sidecar-key=")
        .expect("I7's second line");
    assert!(
        catalog_at < receipt_at && receipt_at < sidecar_at,
        "`catalog-key=` must be printed BEFORE interface I7's pair, or the two landed \
         final-line assertions break"
    );
}

#[test]
fn a_catalog_write_that_fails_leaves_the_run_and_its_receipt_untouched() {
    // THE FAILURE INJECTION. A tempdir filesystem evidence store whose
    // `logweir/catalog/v1/points` path is occupied by a regular FILE: creating
    // an object beneath it cannot make the directory, so the catalog put fails
    // with a store error that is NOT `AlreadyExists` — while
    // `logweir/backups/...` stays perfectly writable.
    //
    // A read-only store would have failed the receipt puts too and proved
    // nothing about the ORDER; pre-creating the record key would have produced
    // `AlreadyPresent`, which is a success.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("archive");
    std::fs::create_dir_all(root.join("logweir/catalog/v1")).unwrap();
    std::fs::write(root.join("logweir/catalog/v1/points"), b"not a directory").unwrap();
    let evidence =
        Store::from_url(&logweir_core::engine::StorageUrl::Filesystem { path: root.clone() })
            .unwrap();

    let f = backup_seam::Fixture::new();
    let outcome = f
        .execute(&evidence)
        .expect("a failed catalog write is NOT a failed backup");

    assert_eq!(
        outcome.catalog_key, None,
        "no `catalog-key=` line may name a key nothing was written to"
    );
    // The run's result is unchanged: both evidence keys, and a receipt that
    // still verifies.
    assert!(outcome.receipt_key.starts_with("logweir/backups/"));
    assert!(outcome.sidecar_key.ends_with(".receipt.sig"));
    let bytes = evidence.get(&outcome.receipt_key).unwrap().0;
    let sidecar: logweir_evidence::Sidecar =
        serde_json::from_slice(&evidence.get(&outcome.sidecar_key).unwrap().0).unwrap();
    logweir_evidence::verify::verify_detached(
        &f.public_key(),
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &bytes,
        &sidecar,
    )
    .expect("the receipt is signed and intact although the catalog write failed");
    let parsed: BackupReceipt = serde_json::from_slice(&bytes).unwrap();
    parsed.validate_invariants().unwrap();
}

#[test]
fn the_catalog_write_cannot_become_a_failure_by_one_question_mark() {
    // The structural half of the row above. `write_catalog_point` returns
    // `Option<String>` and not `Result`, so there is no `?` for a later edit
    // to add: the type is the guarantee that a bucket refusing a metadata put
    // cannot turn a successful backup — whose archive and signed receipt are
    // already in that bucket — into a failure an operator has to investigate.
    let src = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/backup/phase_run.rs"),
    )
    .unwrap();
    assert!(
        src.contains(") -> Option<String> {"),
        "write_catalog_point must return Option<String>; a Result here invites the `?` \
         that makes a metadata failure a run failure"
    );
    assert!(
        src.contains("keys.catalog_key = write_catalog_point("),
        "the call must bind the value, never propagate it"
    );
    assert!(
        !src.contains("write_catalog_point(outcome, &receipt, &bytes, &keys, signer, store)?"),
        "the catalog write must not be propagated with `?`"
    );
}

// ---------------------------------------------------------------------------
// `logweir catalog sync`
// ---------------------------------------------------------------------------

/// Put a receipt and its sidecar into `store` exactly as `backup run` does.
fn seed_receipt(store: &Store, r: &BackupReceipt, key: &SigningKey) -> (String, Vec<u8>) {
    let bytes = receipt_bytes(r);
    let keys = logweir::backup::phase_run::receipt_keys(&r.backup_id, &r.run_id);
    let sidecar = logweir_evidence::sign::sign_detached(
        key,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &bytes,
    )
    .unwrap();
    store.put_create_only(&keys.receipt_key, &bytes).unwrap();
    store
        .put_create_only(&keys.sidecar_key, &serde_json::to_vec(&sidecar).unwrap())
        .unwrap();
    (keys.receipt_key, bytes)
}

fn sync_args(max: usize, since: Option<&str>) -> SyncArgs {
    SyncArgs {
        location: Location {
            url: "s3://kafka-backups".into(),
            region: None,
            endpoint: None,
            path_style: false,
            allow_http: false,
        },
        signing_key: PathBuf::from("unused-by-the-seam"),
        public_keys: Vec::new(),
        since: since.map(str::to_string),
        max,
    }
}

#[test]
fn sync_backfills_a_receipt_that_has_no_record_and_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let (receipt_key, bytes) = seed_receipt(&store, &r, &key);
    let trust = vec![key.verifying_key()];

    let report = logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &trust,
        ts("2026-09-16T00:00:00Z"),
        "s3://kafka-backups/prod",
    )
    .unwrap();
    assert_eq!(report.scanned, 1);
    assert_eq!(report.written, 1);
    assert_eq!(report.next, None, "a completed walk carries no cursor");
    assert_eq!(
        report.points,
        vec![(receipt_key, point_id(&bytes), PointOutcome::Written)]
    );
    assert!(store.get(&record_key(&point_id(&bytes))).is_ok());

    // REPEATED IMPORT IS IDEMPOTENT (D3 §5.5 step 2): identity is
    // content-derived, so a second sync produces the same id and reports it as
    // already present rather than writing a second document or failing.
    let again = logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &trust,
        ts("2027-01-01T00:00:00Z"),
        "s3://dr-copy/prod",
    )
    .unwrap();
    assert_eq!(again.written, 0);
    assert_eq!(again.already_present, 1);
}

#[test]
fn sync_writes_no_record_for_a_receipt_it_cannot_verify() {
    // The receipt's signature is the verification root. A record derived from
    // bytes nobody could authenticate would assert facts this command never
    // established — so there is no record at all, and the count says so.
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    let stranger = SigningKey::generate_ed25519();
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let (_, bytes) = seed_receipt(&store, &r, &stranger);

    let report = logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &[key.verifying_key()],
        ts("2026-09-16T00:00:00Z"),
        "s3://kafka-backups/prod",
    )
    .unwrap();
    assert_eq!(report.unverified_signer, 1);
    assert_eq!(report.written, 0);
    assert!(
        store.get(&record_key(&point_id(&bytes))).is_err(),
        "no record may exist for a receipt no configured key verifies"
    );

    // …and with the stranger's key configured, the same archive syncs. The
    // control matters: without it, a sync that refused everything would pass
    // the row above.
    let report = logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &[stranger.verifying_key()],
        ts("2026-09-16T00:00:00Z"),
        "s3://kafka-backups/prod",
    )
    .unwrap();
    assert_eq!(report.written, 1);
    // The installation recorded is the key that verified the RECEIPT, which is
    // not the key that signed the record.
    match reader::read_record(&store.get(&record_key(&point_id(&bytes))).unwrap().0) {
        RecordVerdict::Point(p) => {
            assert_eq!(
                p.installation.unwrap().key_id,
                stranger.verifying_key().key_id()
            );
            assert_eq!(p.signing.key_id, key.verifying_key().key_id());
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn sync_with_no_public_key_refuses_rather_than_trusting_the_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    seed_receipt(
        &store,
        &receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A"),
        &key,
    );
    let err = logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &[],
        ts("2026-09-16T00:00:00Z"),
        "s3://kafka-backups/prod",
    )
    .expect_err("a sync with no trust material must refuse");
    let msg = format!("{err:?}");
    assert!(msg.contains("never trusted merely by proximity"), "{msg}");
}

#[test]
fn sync_reports_a_record_that_contradicts_its_receipt_as_a_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let (_, bytes) = seed_receipt(&store, &r, &key);

    // A record at the right key whose copied facts are wrong. Nothing rewrites
    // it: a correction is a new record under a new point id.
    let mut forged = point_for(&r, "s3://kafka-backups/prod", &key);
    forged.covered.to_ms = 1;
    store
        .put_create_only(
            &record_key(&point_id(&bytes)),
            &forged.canonical_bytes().unwrap(),
        )
        .unwrap();
    let before = store.get(&record_key(&point_id(&bytes))).unwrap().0;

    let report = logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &[key.verifying_key()],
        ts("2026-09-16T00:00:00Z"),
        "s3://kafka-backups/prod",
    )
    .unwrap();
    assert_eq!(report.conflict, 1);
    assert_eq!(report.written, 0);
    assert_eq!(
        store.get(&record_key(&point_id(&bytes))).unwrap().0,
        before,
        "a conflict is REPORTED, never corrected in place"
    );
}

#[test]
fn sync_reports_a_future_major_record_per_entry_and_keeps_walking() {
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    let a = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let b = receipt("nightly-20260916", "01J9X2QK7C4V0R8YB3ZP6MTS5B");
    let (_, a_bytes) = seed_receipt(&store, &a, &key);
    seed_receipt(&store, &b, &key);
    store
        .put_create_only(
            &record_key(&point_id(&a_bytes)),
            br#"{"format_version":"2.0.0","anything":true}"#,
        )
        .unwrap();

    let report = logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &[key.verifying_key()],
        ts("2026-09-16T00:00:00Z"),
        "s3://kafka-backups/prod",
    )
    .unwrap();
    assert_eq!(report.unsupported_format, 1);
    assert_eq!(
        report.written, 1,
        "a record this build cannot read is a per-ENTRY state, never fatal for the walk"
    );
    assert_eq!(report.scanned, 2);
}

#[test]
fn sync_is_bounded_and_resumable_from_the_cursor_it_prints() {
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    for i in 0..5 {
        seed_receipt(
            &store,
            &receipt(
                &format!("nightly-2026091{i}"),
                &format!("01J9X2QK7C4V0R8YB3ZP6MTS5{i}"),
            ),
            &key,
        );
    }
    let trust = [key.verifying_key()];
    let mut written = 0usize;
    let mut cursor: Option<String> = None;
    let mut rounds = 0;
    loop {
        let report = logweir::catalog::cli::sync_with(
            &sync_args(2, cursor.as_deref()),
            &store,
            &signer,
            &trust,
            ts("2026-09-16T00:00:00Z"),
            "s3://kafka-backups/prod",
        )
        .unwrap();
        written += report.written;
        rounds += 1;
        assert!(rounds < 20, "the resumable walk did not terminate");
        match report.next {
            Some(n) => cursor = Some(n),
            None => break,
        }
    }
    assert_eq!(
        written, 5,
        "every receipt was reached exactly once across {rounds} page(s)"
    );
    assert!(
        rounds > 1,
        "a bound of 2 over 10 objects must take more than one page"
    );
    let points = store.list_keys("logweir/catalog/v1/points/").unwrap();
    assert_eq!(points.len(), 10, "five points, each a record and a sidecar");
}

#[test]
fn list_returns_the_newest_points_first() {
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    // Three captures on three days, seeded out of order so the result cannot
    // be the insertion order by accident.
    for (day, run) in [("17", "5C"), ("15", "5A"), ("16", "5B")] {
        let mut r = receipt(
            &format!("nightly-202609{day}"),
            &format!("01J9X2QK7C4V0R8YB3ZP6MTS{run}"),
        );
        r.started_at = ts(&format!("2026-09-{day}T03:00:00Z"));
        r.finished_at = ts(&format!("2026-09-{day}T03:04:00Z"));
        seed_receipt(&store, &r, &key);
    }
    logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &[key.verifying_key()],
        ts("2026-09-18T00:00:00Z"),
        "s3://kafka-backups/prod",
    )
    .unwrap();

    // `today` is a PARAMETER, so this walks the same window on every machine
    // and on every future date. The seeded captures are 2026-09-15..17.
    let today = chrono::NaiveDate::from_ymd_opt(2026, 9, 18).unwrap();
    let report = logweir::catalog::cli::list_with(&list_args(10, None), &store, today).unwrap();
    assert_eq!(
        (
            report.unsupported_format,
            report.unreadable,
            report.inconsistent
        ),
        (0, 0, 0),
        "a clean catalog skips nothing"
    );
    assert!(!report.truncated, "ten rows asked for, three exist");
    let days: Vec<String> = report
        .rows
        .iter()
        .map(|e| {
            chrono::DateTime::from_timestamp_millis(e.recovery_point_at_ms)
                .unwrap()
                .format("%Y-%m-%d")
                .to_string()
        })
        .collect();
    assert_eq!(
        days,
        vec!["2026-09-17", "2026-09-16", "2026-09-15"],
        "newest first"
    );

    // …and `--max` really bounds the page, keeping the NEWEST rows rather than
    // the first ones the backend happened to stream.
    let one = logweir::catalog::cli::list_with(&list_args(1, None), &store, today).unwrap();
    assert_eq!(one.rows.len(), 1);
    assert_eq!(one.rows[0].point_id, report.rows[0].point_id);
    assert!(
        one.truncated,
        "a page that filled with days still unlooked-at must say so, or a short listing reads \
         as `these are all the points`"
    );

    // THE WINDOW IS REPORTED, so an empty page says which window it is empty
    // for. A one-day lookback from a day with nothing in it finds nothing —
    // and says how far it looked.
    let narrow = logweir::catalog::cli::list_with(
        &ListArgs {
            days: 1,
            ..list_args(10, None)
        },
        &store,
        today,
    )
    .unwrap();
    assert!(narrow.rows.is_empty());
    assert_eq!(narrow.days_searched, 1);
    assert_eq!(narrow.oldest_day_searched, "2026-09-18");
    assert!(
        !narrow.truncated,
        "the lookback ran out rather than the page filling; `truncated` is about `--max`"
    );
}

// ---------------------------------------------------------------------------
// The URL guard and the CLI's exit codes
// ---------------------------------------------------------------------------

/// `ListArgs` over the in-memory fixture store, with the full default window.
fn list_args(max: usize, since: Option<&str>) -> ListArgs {
    ListArgs {
        location: sync_args(0, None).location,
        since: since.map(str::to_string),
        max,
        days: 400,
    }
}

fn location(url: &str) -> Location {
    Location {
        url: url.into(),
        region: None,
        endpoint: None,
        path_style: false,
        allow_http: false,
    }
}

#[test]
fn evidence_url_imposes_the_logweir_root_and_never_reads_it_off_the_url() {
    use logweir::catalog::cli::evidence_url;
    use logweir_core::engine::StorageUrl as U;
    for url in ["s3://kafka-backups", "s3://kafka-backups/logweir"] {
        match evidence_url(&location(url)).unwrap() {
            U::S3 { bucket, prefix, .. } => {
                assert_eq!(bucket, "kafka-backups");
                assert_eq!(prefix, "logweir/", "{url}");
            }
            other => panic!("{other:?}"),
        }
    }
    // A DEEPER prefix is refused here, naming the part of the URL that is
    // wrong — rather than reaching `Store::from_url` and producing a message
    // about a prefix the operator never typed.
    let err = evidence_url(&location("s3://kafka-backups/prod")).unwrap_err();
    assert!(err.contains("Global Constraint 6"), "{err}");
    assert!(err.contains("prod"), "{err}");
}

#[test]
fn evidence_url_refuses_a_credential_on_the_command_line_without_echoing_it() {
    use logweir::catalog::cli::evidence_url;
    let err = evidence_url(&location("s3://AKIAEXAMPLE:sUp3rS3cret@kafka-backups")).unwrap_err();
    assert!(err.contains("userinfo"), "{err}");
    assert!(
        !err.contains("sUp3rS3cret") && !err.contains("AKIAEXAMPLE"),
        "the refusal must not echo the credential it is refusing: {err}"
    );
}

#[test]
fn evidence_url_refuses_what_it_cannot_honour() {
    use logweir::catalog::cli::evidence_url;
    for (url, needle) in [
        ("", "is empty"),
        ("kafka-backups", "is neither"),
        ("ftp://kafka-backups", "unsupported scheme"),
        ("s3://", "names no bucket"),
        ("s3://b?versionId=1", "no query string"),
        ("s3://b#frag", "no fragment"),
        ("az://acct", "account and a container"),
        ("file://host/path", "three slashes"),
    ] {
        let err = evidence_url(&location(url)).unwrap_err();
        assert!(err.contains(needle), "{url:?} -> {err}");
    }
}

#[test]
fn evidence_url_accepts_every_scheme_object_store_is_built_with() {
    use logweir::catalog::cli::evidence_url;
    use logweir_core::engine::StorageUrl as U;
    assert!(matches!(
        evidence_url(&location("gs://kafka-backups")).unwrap(),
        U::Gcs { .. }
    ));
    assert!(matches!(
        evidence_url(&location("az://acct/container")).unwrap(),
        U::Azure { .. }
    ));
    match evidence_url(&location("file:///var/archive")).unwrap() {
        U::Filesystem { path } => assert_eq!(path, Path::new("/var/archive")),
        other => panic!("{other:?}"),
    }
    match evidence_url(&location("/var/archive")).unwrap() {
        U::Filesystem { path } => assert_eq!(path, Path::new("/var/archive")),
        other => panic!("{other:?}"),
    }
}

#[test]
fn transport_security_is_never_derived_from_the_url() {
    // **D-SEAMS S5**, defects SEC-ENVHTTP and UI-HTTPDOWNGRADE. Neither the
    // scheme nor the endpoint's own `http://` may turn plaintext on: only the
    // explicit flag does.
    use logweir::catalog::cli::evidence_url;
    use logweir_core::engine::StorageUrl as U;
    let mut l = location("s3://kafka-backups");
    l.endpoint = Some("http://minio.internal:9000".into());
    match evidence_url(&l).unwrap() {
        U::S3 { allow_http, .. } => assert!(
            !allow_http,
            "an http:// endpoint must NOT enable plaintext transport on its own"
        ),
        other => panic!("{other:?}"),
    }
    l.allow_http = true;
    match evidence_url(&l).unwrap() {
        U::S3 { allow_http, .. } => assert!(allow_http, "the explicit flag is what enables it"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_catalog_commands_exit_with_the_existing_contract_and_no_new_variant() {
    use logweir::exit::ExitCode;
    // A bad `--url` is a bad COMMAND LINE — exit 1 — and is answered before
    // any store is dialled.
    assert_eq!(
        logweir::catalog::cli::run_list(&ListArgs {
            location: location("s3://kafka-backups/prod"),
            since: None,
            max: 10,
            days: 1,
        }),
        ExitCode::Operational
    );
    assert_eq!(
        logweir::catalog::cli::run_sync(&SyncArgs {
            location: location("nonsense"),
            signing_key: PathBuf::from("/nonexistent/key.pem"),
            public_keys: vec![],
            since: None,
            max: 10,
        }),
        ExitCode::Operational
    );
    // A signing key that cannot be loaded is exit 4: Global Constraint 11's
    // "signing or lock-proof failed, nothing uploaded", and it is resolved
    // BEFORE the store is dialled so a missing key costs a message and not a
    // half-written catalog.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("public.pem"), "not a key").unwrap();
    assert_eq!(
        logweir::catalog::cli::run_sync(&SyncArgs {
            location: location("file:///tmp"),
            signing_key: dir.path().join("missing.pem"),
            public_keys: vec![],
            since: None,
            max: 10,
        }),
        ExitCode::SigningOrLock
    );
    // An unreadable public key is a bad command line, not a signing failure.
    assert_eq!(
        logweir::catalog::cli::run_sync(&SyncArgs {
            location: location("file:///tmp"),
            signing_key: dir.path().join("missing.pem"),
            public_keys: vec![dir.path().join("public.pem")],
            since: None,
            max: 10,
        }),
        ExitCode::Operational
    );
}

// ---------------------------------------------------------------------------
// The published schema
// ---------------------------------------------------------------------------

#[test]
fn the_checked_in_catalog_point_schema_is_the_one_the_type_generates() {
    // The drift arm, IN PROCESS, so the gate holds with no subprocess — the
    // shape `crates/logweir-api/tests/contract.rs` uses for the OpenAPI
    // document. `just schema` is the only sanctioned way to change the file.
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../schemas/logweir-catalog-point-1.0.0.json");
    let checked_in = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()));
    assert_eq!(
        checked_in,
        logweir::catalog::schema::catalog_point_schema(),
        "schemas/logweir-catalog-point-1.0.0.json no longer describes CatalogPoint. Run \
         `just schema` and commit the diff."
    );
    // The major is pinned by a PATTERN as well as by the reader, so a
    // schema-only validator — the one route that does not go through
    // `read_record` — refuses a 9.9.9 document against a file called 1.0.0.
    assert!(
        checked_in.contains(r"^1\\.[0-9]+\\.[0-9]+$"),
        "the schema must pin format_version's major"
    );
}

// ---------------------------------------------------------------------------
// Review fix round (2026-09-16)
// ---------------------------------------------------------------------------

#[test]
fn a_store_failure_on_the_first_point_is_operational_and_never_signing() {
    // **Review finding F1.** Global Constraint 11's exit 4 means "signing or
    // lock-proof failed — and NOTHING was uploaded". A denied PUT under
    // `logweir/catalog/*`, or a 503 on the first point of a walk, is neither:
    // the record was signed before any put was attempted, and part of the
    // point may already be in the bucket. Reporting 4 sends an operator to
    // rotate signing material over a bucket policy.
    //
    // THE INJECTION IS THE REVIEWER'S OWN WORST SUB-CASE, one object further
    // along: the record and its sidecar land and the INDEX ENTRY is refused. A
    // regular FILE is planted where the day shard's directory would go, so the
    // first two create-only puts succeed and the third fails with a store error
    // that is not `AlreadyExists`. The bucket really does end up holding two of
    // this point's three objects — exactly the state exit 4's "nothing was
    // uploaded" would have denied.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("archive");
    // `LocalFileSystem` canonicalises its root, so the directory has to exist
    // before the handle is built.
    std::fs::create_dir_all(&root).unwrap();
    let store =
        Store::from_url(&logweir_core::engine::StorageUrl::Filesystem { path: root.clone() })
            .unwrap();

    let (signer, key, _) = signer_in(dir.path());
    let r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    let (_, receipt_bytes) = seed_receipt(&store, &r, &key);
    let id = point_id(&receipt_bytes);
    let log = logweir::catalog::record::log_key(r.started_at, &id);
    let shard_dir = root.join(&log).parent().unwrap().to_path_buf();
    std::fs::create_dir_all(shard_dir.parent().unwrap()).unwrap();
    std::fs::write(&shard_dir, b"not a directory").unwrap();

    let err = logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &[key.verifying_key()],
        ts("2026-09-16T00:00:00Z"),
        "s3://kafka-backups/prod",
    )
    .expect_err("a refused catalog put must fail the sync");

    assert_eq!(
        logweir::exit::ExitCode::from(&err),
        logweir::exit::ExitCode::Operational,
        "a STORE failure is exit 1; exit 4 asserts `nothing was uploaded`, which is false \
         here and points an operator at the wrong thing entirely: {err:?}"
    );
    let msg = format!("{err:?}");
    assert!(
        msg.contains("says nothing about the signing key"),
        "the message must say what did NOT fail: {msg}"
    );
    assert!(
        msg.contains("--since"),
        "and how to resume, because nothing under `logweir/` is ever rewritten: {msg}"
    );
    // The state exit 4 would have denied: two of the three objects ARE in the
    // bucket, and the record verifies.
    assert!(
        store.get(&record_key(&id)).is_ok(),
        "the record landed before the index entry was refused, so `nothing was uploaded` \
         is false and exit 4 would have said it"
    );
    assert!(store.get(&record_sidecar_key(&id)).is_ok());
    assert!(
        store.get(&log).is_err(),
        "the index entry is the object that was refused"
    );
}

#[test]
fn a_put_error_knows_whether_anything_was_uploaded() {
    // The predicate the exit-code decision is made on, in isolation. The
    // mutant it kills is `nothing_was_uploaded()` returning `true` for
    // `Store`, which puts the old defect straight back.
    use logweir::catalog::writer::PutError;
    assert!(PutError::Signing("x".into()).nothing_was_uploaded());
    assert!(PutError::SelfContradicting("x".into()).nothing_was_uploaded());
    assert!(
        !PutError::Store {
            key: "logweir/catalog/v1/points/lwp1-x/record.sig".into(),
            detail: "403".into(),
        }
        .nothing_was_uploaded(),
        "record.json may already have landed when record.sig is refused, so this variant \
         can never claim nothing was uploaded"
    );
}

#[test]
fn the_published_schema_carries_the_algorithm_vocabulary_as_an_enum() {
    // **Review finding F2.** The checked-in schema is the one artifact that
    // exists to publish this vocabulary, and it documented `p256` — the
    // spelling this record deliberately rejects — with no `enum` or `pattern`
    // for anything mechanical to catch the drift. A consumer generating its
    // type from it would have matched on a value no record carries.
    let schema: serde_json::Value =
        serde_json::from_str(&logweir::catalog::schema::catalog_point_schema()).unwrap();
    let algorithm = &schema["definitions"]["RecordSigning"]["properties"]["algorithm"];
    let values: Vec<&str> = algorithm["enum"]
        .as_array()
        .expect("`algorithm` publishes an enum, not a free string")
        .iter()
        .map(|v| v.as_str().expect("enum values are strings"))
        .collect();
    assert_eq!(values, vec!["ecdsa-p256-sha256", "ed25519"]);
    assert_eq!(algorithm["type"], "string");
    assert!(
        !values.contains(&"p256"),
        "`p256` is decision D3 §5.2's illustrative spelling and is not a value this \
         product writes: {values:?}"
    );

    // THE VOCABULARY IS DERIVED FROM THE WRITER, not typed twice. A mutant
    // that changed `algorithm_name` without the schema — or the other way —
    // fails here rather than shipping a schema no record satisfies.
    for key in [
        SigningKey::generate_p256().verifying_key(),
        SigningKey::generate_ed25519().verifying_key(),
    ] {
        let written = logweir::catalog::algorithm_name(&key);
        assert!(
            values.contains(&written),
            "the writer emits {written:?}, which the published schema's enum does not \
             allow: {values:?}"
        );
    }
}

/// Write one index entry under a day shard, bypassing the writer, so the
/// listing rows below are about bytes a foreign or future producer could have
/// left there.
fn seed_index_entry(store: &Store, day: &str, ms: i64, body: &[u8]) -> String {
    let key = format!(
        "logweir/catalog/v1/log/{day}/{ms:013}-lwp1-{}.json",
        "0".repeat(32)
    );
    store.put_create_only(&key, body).unwrap();
    key
}

#[test]
fn one_bad_index_entry_is_skipped_and_counted_never_fatal_to_the_listing() {
    // **Review finding F3.** `docs/formats/catalog-point.md` promises
    // "refusal is per entry, not per catalog". That was implemented for
    // `record.json` and NOT for the day-sharded index, where one unreadable
    // object aborted the whole listing with exit 1 — exactly the wholesale
    // failure the promise rules out.
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    let mut r = receipt("nightly-20260915", "01J9X2QK7C4V0R8YB3ZP6MTS5A");
    r.started_at = ts("2026-09-15T03:00:00Z");
    r.finished_at = ts("2026-09-15T03:04:00Z");
    seed_receipt(&store, &r, &key);
    logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &[key.verifying_key()],
        ts("2026-09-16T00:00:00Z"),
        "s3://kafka-backups/prod",
    )
    .unwrap();

    // Four objects a v1 reader cannot use, in the SAME shard as the good one.
    seed_index_entry(
        &store,
        "2026/09/15",
        1_757_980_800_001,
        br#"{"format_version":"2.0.0","whatever":{"a":[1]}}"#,
    );
    seed_index_entry(&store, "2026/09/15", 1_757_980_800_002, b"not json at all");
    seed_index_entry(
        &store,
        "2026/09/15",
        1_757_980_800_003,
        br#"{"format_version":"1.0.0"}"#,
    );
    // **Review finding F6**: a well-formed entry whose `record_key` is not the
    // one its own `point_id` implies. The log prefix is create-only but not
    // append-restricted, so anyone who can write a NEW key there could publish
    // a row attributing an arbitrary record to a chosen identity.
    let forged = serde_json::json!({
        "format_version": "1.0.0",
        "point_id": format!("lwp1-{}", "a".repeat(32)),
        "backup_id": "nightly-20260915",
        "run_id": "01J9X2QK7C4V0R8YB3ZP6MTS5A",
        "recovery_point_at_ms": 1_757_980_800_004_i64,
        "covered": {"from_ms": 1, "to_ms": 2},
        "record_key": format!("logweir/catalog/v1/points/lwp1-{}/record.json", "b".repeat(32)),
        "receipt_key": "logweir/backups/x/y.receipt.json",
        "receipt_sha256": format!("sha256:{}", "c".repeat(64)),
    });
    seed_index_entry(
        &store,
        "2026/09/15",
        1_757_980_800_004,
        serde_json::to_string(&forged).unwrap().as_bytes(),
    );

    let today = chrono::NaiveDate::from_ymd_opt(2026, 9, 16).unwrap();
    let report = logweir::catalog::cli::list_with(&list_args(50, None), &store, today).unwrap();

    assert_eq!(
        report.rows.len(),
        1,
        "the ONE good row still lists: {:?}",
        report.rows
    );
    assert_eq!(report.unsupported_format, 1, "the major-2 entry (rule 1)");
    assert_eq!(
        report.unreadable, 2,
        "the non-JSON and the wrong-shape entry"
    );
    assert_eq!(report.inconsistent, 1, "the forged record_key (F6)");

    // …and the reader's own verdicts, so the counts above are not the only
    // thing standing between a forged row and a printed one.
    match reader::read_log_entry(serde_json::to_string(&forged).unwrap().as_bytes()) {
        reader::LogEntryVerdict::Inconsistent(m) => {
            assert!(m.contains("is not the key `point_id`"), "{m}")
        }
        other => panic!("a forged record_key must be Inconsistent, got {other:?}"),
    }
    match reader::read_log_entry(br#"{"format_version":"2.0.0"}"#) {
        reader::LogEntryVerdict::UnsupportedFormat { format_version } => {
            assert_eq!(format_version, "2.0.0")
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_malformed_point_id_in_an_index_entry_is_refused() {
    // The other half of F6: `record_key` and `point_id` can also be made to
    // agree on a value that is not an identity at all.
    let entry = serde_json::json!({
        "format_version": "1.0.0",
        "point_id": "lwp1-NOTHEX",
        "backup_id": "b",
        "run_id": "r",
        "recovery_point_at_ms": 1,
        "covered": {"from_ms": 1, "to_ms": 2},
        "record_key": "logweir/catalog/v1/points/lwp1-NOTHEX/record.json",
        "receipt_key": "logweir/backups/x/y.receipt.json",
        "receipt_sha256": "sha256:0",
    });
    match reader::read_log_entry(serde_json::to_string(&entry).unwrap().as_bytes()) {
        reader::LogEntryVerdict::Inconsistent(m) => {
            assert!(m.contains("32 lowercase hex"), "{m}")
        }
        other => panic!("{other:?}"),
    }
    // UPPERCASE hex is refused too: `point_id` emits lowercase, and two
    // spellings of one identity is what the id's own doc comment argues
    // against.
    assert!(!reader::is_point_id(&format!("lwp1-{}", "A".repeat(32))));
    assert!(reader::is_point_id(&format!("lwp1-{}", "a".repeat(32))));
    assert!(!reader::is_point_id(&format!("lwp2-{}", "a".repeat(32))));
}

#[test]
fn a_listing_walks_days_backwards_and_stops_once_the_page_is_full() {
    // **Review finding F5.** The first version paged forward over the whole
    // log prefix keeping a ring of the newest keys — correct, and O(n²/page)
    // object-metadata reads, which also made the day shard buy nothing. This
    // walks days newest-first and stops the moment `--max` is held, so the
    // shard structure is what bounds the work.
    //
    // Measured as OBSERVABLE work: with three days seeded and `--max 1`, the
    // listing must report having searched exactly ONE day.
    let dir = tempfile::tempdir().unwrap();
    let (signer, key, _) = signer_in(dir.path());
    let store = Store::in_memory("logweir/");
    for (day, run) in [("15", "5A"), ("16", "5B"), ("17", "5C")] {
        let mut r = receipt(
            &format!("nightly-202609{day}"),
            &format!("01J9X2QK7C4V0R8YB3ZP6MTS{run}"),
        );
        r.started_at = ts(&format!("2026-09-{day}T03:00:00Z"));
        r.finished_at = ts(&format!("2026-09-{day}T03:04:00Z"));
        seed_receipt(&store, &r, &key);
    }
    logweir::catalog::cli::sync_with(
        &sync_args(100, None),
        &store,
        &signer,
        &[key.verifying_key()],
        ts("2026-09-18T00:00:00Z"),
        "s3://kafka-backups/prod",
    )
    .unwrap();

    let today = chrono::NaiveDate::from_ymd_opt(2026, 9, 17).unwrap();
    let one = logweir::catalog::cli::list_with(&list_args(1, None), &store, today).unwrap();
    assert_eq!(one.rows.len(), 1);
    assert_eq!(
        one.days_searched, 1,
        "a full page must stop the backward walk: searching further is work an operator \
         pays for and never sees"
    );
    assert_eq!(one.oldest_day_searched, "2026-09-17");

    // All three need three shards, and the walk reports that honestly.
    let all = logweir::catalog::cli::list_with(&list_args(10, None), &store, today).unwrap();
    assert_eq!(all.rows.len(), 3);
    assert_eq!(
        all.days_searched, 400,
        "nothing filled the page, so the window ran out"
    );
    assert!(!all.truncated);

    // `--since` stops the walk at its own day rather than walking the whole
    // lookback for rows it would drop anyway.
    let since = all.rows[1].clone();
    let after = logweir::catalog::cli::list_with(
        &list_args(
            10,
            Some(&logweir::catalog::record::log_key(
                chrono::DateTime::from_timestamp_millis(since.recovery_point_at_ms).unwrap(),
                &since.point_id,
            )),
        ),
        &store,
        today,
    )
    .unwrap();
    assert_eq!(
        after
            .rows
            .iter()
            .map(|e| e.point_id.clone())
            .collect::<Vec<_>>(),
        vec![all.rows[0].point_id.clone()],
        "only rows NEWER than the cursor"
    );
    assert_eq!(
        after.days_searched, 2,
        "the backward walk stops at the cursor's own day: {after:?}"
    );
}
