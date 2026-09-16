#![cfg(feature = "e2e")]
//! The recovery catalog against a REAL S3-compatible server (PLAT-15.1).
//!
//! # Gating
//!
//! The inner `cfg` attribute above is this repository's gating convention for
//! a test that needs a live backend (`crates/logweir-kafka/tests/live.rs`,
//! `crates/logweir-store/tests/minio_options.rs`). Under the default feature
//! set this file compiles to nothing, so `cargo test -p logweir` skips it
//! cleanly when the compose MinIO is absent, and
//! `crates/logweir/tests/no_network_in_unit_tests.rs` reads the same attribute
//! to decide that a dialling constructor here is expected.
//!
//! **MinIO ONLY.** Nothing here needs a broker: the source cluster is a
//! `ClusterReader` double and the archive is `Store::in_memory`, exactly as in
//! `crates/logweir/tests/catalog.rs`. Bring up the one service:
//!
//! ```text
//! docker compose -f e2e/compose/docker-compose.yml up -d --wait minio
//! docker compose -f e2e/compose/docker-compose.yml --profile setup run --rm minio-setup
//! cargo test --locked -p logweir --features e2e --test catalog_minio
//! ```
//!
//! `LOGWEIR_TEST_S3_ENDPOINT` overrides the endpoint (default
//! `http://localhost:9000`, the compose stack's published MinIO port) and
//! `LOGWEIR_TEST_S3_BUCKET` the bucket (default `kafka-backups`, created by the
//! `minio-setup` one-shot) — the same two names
//! `crates/logweir-store/tests/minio_options.rs` uses, so nobody has to
//! discover a third.
//!
//! # What this adds over the in-memory suite
//!
//! `Store::in_memory` is a `BTreeMap`: it cannot show that the layout survives
//! a real `PutMode::Create`, a real prefix listing whose ordering the crate
//! does not contract, or a real `list_with_offset` push-down. Each row below
//! is a claim that needed a server to be worth making.
//!
//! **Nothing is cleaned up, and that is deliberate**: Logweir holds no delete
//! capability against object storage (Global Constraint 6 / guard G-RET), so a
//! test that tidied after itself would need one. Every key is namespaced by a
//! per-run `backup_id`, and `just e2e-down`'s `-v` removes the volume.

mod backup_seam;

use logweir::catalog::cli::{ListArgs, Location, PointOutcome, SyncArgs};
use logweir::catalog::reader::{self, CrossCheck, RecordVerdict};
use logweir::catalog::record::*;
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::engine::StorageUrl;
use logweir_engine_oso::storage::{Store, StoreError, StoreOptions};
use std::time::Duration;

const ROOT_USER: &str = "minioadmin";
const ROOT_PASSWORD: &str = "minioadmin";

fn endpoint() -> String {
    std::env::var("LOGWEIR_TEST_S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into())
}

fn bucket() -> String {
    std::env::var("LOGWEIR_TEST_S3_BUCKET").unwrap_or_else(|_| "kafka-backups".into())
}

/// The evidence root on the compose MinIO. `allow_http: true` with an explicit
/// `http://` endpoint is the ONE shape that reaches a plaintext local server,
/// and it is stated here rather than derived from anything (D-SEAMS S5).
fn evidence_location() -> StorageUrl {
    StorageUrl::S3 {
        bucket: bucket(),
        prefix: logweir_engine_oso::storage::LOGWEIR_ROOT.to_string(),
        region: Some("us-east-1".into()),
        endpoint: Some(endpoint()),
        // MinIO behind a custom endpoint is path-style.
        path_style: true,
        allow_http: true,
    }
}

/// The writable evidence handle, with EXPLICIT credentials rather than the
/// ambient chain: a test that inherited `AWS_*` from the shell would report on
/// a bucket nobody meant to touch.
fn evidence_store() -> Store {
    Store::from_url_with(
        &evidence_location(),
        &StoreOptions::static_keys(ROOT_USER, ROOT_PASSWORD, None)
            .with_request_timeout(Duration::from_secs(10))
            .with_max_retries(1),
    )
    .expect("the compose MinIO evidence handle builds")
}

/// A `backup_id` nothing else in this bucket has used. Every evidence key is
/// create-only and the compose volume outlives one `cargo test`, so a fixed id
/// would fail at a put on the second run instead of at an assertion.
fn unique_id(tag: &str) -> String {
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_nanos();
    format!("catalog-{tag}-{ns}")
}

fn run_id(tag: &str) -> String {
    // A ULID-shaped 26-character stem, unique per call, so the receipt key is
    // the shape `phase_run::receipt_keys` builds.
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_nanos();
    // 1 tag character + 25 zero-padded digits = the 26-character stem a ULID
    // has. `u128` nanoseconds since the epoch need 19 digits today and 25 until
    // long after this code is gone, so the padding never truncates.
    format!("{tag}{ns:0>25}")
}

#[test]
fn a_backup_writes_its_five_evidence_objects_into_a_real_bucket() {
    let evidence = evidence_store();
    let f = backup_seam::Fixture::new();
    let backup_id = unique_id("five");
    let outcome = f
        .execute_as(&evidence, &backup_id, &run_id("A"))
        .expect("the backup succeeds against a real bucket");

    let catalog_key = outcome
        .catalog_key
        .as_deref()
        .expect("a successful backup writes its catalog point");

    // All five, read back off the server — never inferred from the outcome.
    let receipt_bytes = evidence.get(&outcome.receipt_key).expect("receipt").0;
    let sidecar_bytes = evidence
        .get(&outcome.sidecar_key)
        .expect("receipt sidecar")
        .0;
    let record_bytes = evidence.get(catalog_key).expect("catalog record").0;
    let point = match reader::read_record(&record_bytes) {
        RecordVerdict::Point(p) => p,
        other => panic!("{other:?}"),
    };
    let record_sidecar = evidence
        .get(&record_sidecar_key(&point.point_id))
        .expect("catalog record sidecar")
        .0;
    let log_bytes = evidence.get(&point.log_key()).expect("catalog log entry").0;

    // The identity is the digest of the bytes the SERVER now holds.
    assert_eq!(point.point_id, point_id(&receipt_bytes));

    // Rule 3, end to end: the record's copied facts are the receipt's.
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).unwrap();
    assert_eq!(
        reader::cross_check(&point, &receipt, &receipt_bytes),
        CrossCheck::Agrees
    );

    // Both sidecars verify over the stored bytes, each under its own media
    // type — a record that verified as a receipt would be substitutable for
    // one.
    let public = f.public_key();
    let receipt_sc: logweir_evidence::Sidecar = serde_json::from_slice(&sidecar_bytes).unwrap();
    logweir_evidence::verify::verify_detached(
        &public,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &receipt_bytes,
        &receipt_sc,
    )
    .expect("the receipt sidecar verifies");
    let record_sc: logweir_evidence::Sidecar = serde_json::from_slice(&record_sidecar).unwrap();
    assert_eq!(
        record_sc.payload_type,
        logweir::catalog::PAYLOAD_TYPE_CATALOG_POINT
    );
    logweir_evidence::verify::verify_detached(
        &public,
        logweir::catalog::PAYLOAD_TYPE_CATALOG_POINT,
        &record_bytes,
        &record_sc,
    )
    .expect("the record sidecar verifies");

    let entry: CatalogLogEntry = serde_json::from_slice(&log_bytes).unwrap();
    assert_eq!(entry.point_id, point.point_id);
    assert_eq!(entry.record_key, catalog_key);
}

#[test]
fn the_real_backend_enforces_create_only_on_every_catalog_key() {
    // `Store::in_memory` cannot show this: `PutMode::Create` against MinIO is
    // a conditional request the SERVER evaluates. D3 §5.2 rule 4 — nothing
    // under `logweir/` is ever rewritten — is only as strong as that.
    let evidence = evidence_store();
    let f = backup_seam::Fixture::new();
    let backup_id = unique_id("create-only");
    let outcome = f
        .execute_as(&evidence, &backup_id, &run_id("B"))
        .expect("the backup succeeds");
    let catalog_key = outcome.catalog_key.as_deref().unwrap();
    let before = evidence.get(catalog_key).unwrap().0;
    let point = match reader::read_record(&before) {
        RecordVerdict::Point(p) => p,
        other => panic!("{other:?}"),
    };

    for key in [
        catalog_key.to_string(),
        record_sidecar_key(&point.point_id),
        point.log_key(),
    ] {
        match evidence.put_create_only(&key, b"{\"format_version\":\"1.0.0\"}") {
            Err(StoreError::AlreadyExists(k)) => assert!(k.contains(&key) || key.contains(&k)),
            other => panic!("a rewrite of {key} must be refused by the server, got {other:?}"),
        }
    }
    assert_eq!(
        evidence.get(catalog_key).unwrap().0,
        before,
        "the refused puts left the record byte-identical"
    );
}

#[test]
fn list_page_walks_a_real_prefix_in_bounded_resumable_pages() {
    // The property `object_store` does not contract: a real backend's listing
    // order. `list_page` sorts its own selection, so a page is the smallest
    // `max` keys whatever the server streamed, and the cursor resumes without
    // a gap or a repeat.
    let evidence = evidence_store();
    let f = backup_seam::Fixture::new();
    let tag = unique_id("page");
    let mut expected = Vec::new();
    for i in 0..3 {
        let outcome = f
            .execute_as(&evidence, &format!("{tag}-{i}"), &run_id("C"))
            .expect("the backup succeeds");
        expected.push(outcome.receipt_key);
    }
    expected.sort();

    // The PREFIX is the receipts root, not `…/{tag}`: object_store evaluates a
    // list prefix on a PATH SEGMENT basis (`foo/bar` is a prefix of `foo/bar/x`
    // but not of `foo/bar_baz/x`), so a partial segment matches nothing. The
    // per-run tag is applied as a filter afterwards, which is also what makes
    // this row safe against whatever else the shared compose bucket holds.
    //
    // The walk STARTS at the tag rather than at the beginning of the prefix,
    // because the compose bucket accumulates across runs: `start_after` is a
    // lexicographic offset and the tag carries this run's nanosecond, so
    // everything an earlier run wrote sorts before it and is skipped. That is
    // also the first half of the resumability claim — a cursor really is a
    // resume point on a real backend and not only in the `InMemory` map.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = Some(format!("{RECEIPTS_PREFIX}{tag}"));
    let mut rounds = 0;
    loop {
        let (page, next) = evidence
            .list_page(RECEIPTS_PREFIX, cursor.as_deref(), 2)
            .unwrap();
        assert!(page.len() <= 2, "the page bound is not honoured: {page:?}");
        let mut sorted = page.clone();
        sorted.sort();
        assert_eq!(
            page, sorted,
            "a page arrives sorted whatever the server did"
        );
        seen.extend(page);
        rounds += 1;
        assert!(
            rounds < 50,
            "the real-backend walk did not terminate; it saw {} key(s)",
            seen.len()
        );
        match next {
            Some(n) => cursor = Some(n),
            None => break,
        }
    }
    seen.retain(|k| k.ends_with(".receipt.json") && k.contains(&tag));
    assert_eq!(
        seen, expected,
        "the paged walk reconstructed the prefix exactly"
    );
    assert!(
        rounds > 1,
        "three sets over a bound of 2 must take more than one page"
    );
}

#[test]
fn sync_backfills_a_real_bucket_and_a_second_run_writes_nothing() {
    // The backfill D3 §5.2 promises ("the scanner backfills") and §5.5 step 2's
    // idempotent repeated import, over a real bucket and a real listing.
    let evidence = evidence_store();
    let f = backup_seam::Fixture::new();
    let backup_id = unique_id("sync");
    let outcome = f
        .execute_as(&evidence, &backup_id, &run_id("D"))
        .expect("the backup succeeds");
    let receipt_bytes = evidence.get(&outcome.receipt_key).unwrap().0;
    let id = point_id(&receipt_bytes);

    let dir = tempfile::tempdir().unwrap();
    let key = logweir_evidence::keys::SigningKey::generate_p256();
    let key_path = dir.path().join("sync-signer.pem");
    std::fs::write(&key_path, key.to_pkcs8_pem().unwrap()).unwrap();
    let signer = logweir::backup::phase_run::load_signer(&key_path).unwrap();
    let trust = vec![f.public_key()];
    let args = SyncArgs {
        location: Location {
            url: format!("s3://{}", bucket()),
            region: Some("us-east-1".into()),
            endpoint: Some(endpoint()),
            path_style: true,
            allow_http: true,
        },
        signing_key: key_path.clone(),
        public_keys: Vec::new(),
        since: Some(format!("{RECEIPTS_PREFIX}{backup_id}")),
        max: 10,
    };

    // The runner already wrote this point's record, so the first sync finds it
    // present rather than writing a second copy.
    let report = logweir::catalog::cli::sync_with(
        &args,
        &evidence,
        &signer,
        &trust,
        chrono::Utc::now(),
        &location_id(&evidence_location()),
    )
    .unwrap();
    assert_eq!(report.written, 0, "{:?}", report.points);
    assert!(
        report
            .points
            .iter()
            .any(|(_, p, o)| p == &id && *o == PointOutcome::AlreadyPresent),
        "{:?}",
        report.points
    );

    // Now a receipt with NO record: the case the backfill exists for. It is
    // seeded by copying the runner's own receipt under a second run id, so the
    // bytes really are a signed receipt and the point id really is derived
    // from them.
    let second = run_id("E");
    let mut parsed: BackupReceipt = serde_json::from_slice(&receipt_bytes).unwrap();
    parsed.run_id = second.clone();
    let second_bytes = logweir_core::det_json::to_deterministic_json(&parsed).unwrap();
    let keys = logweir::backup::phase_run::receipt_keys(&backup_id, &second);
    let sidecar = logweir_evidence::sign::sign_detached(
        &key,
        logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
        &second_bytes,
    )
    .unwrap();
    evidence
        .put_create_only(&keys.receipt_key, &second_bytes)
        .unwrap();
    evidence
        .put_create_only(&keys.sidecar_key, &serde_json::to_vec(&sidecar).unwrap())
        .unwrap();

    let report = logweir::catalog::cli::sync_with(
        &SyncArgs {
            // The second receipt must verify under the key that signed it, so
            // the sync's trust set is the one that covers both.
            public_keys: vec![key_path],
            ..args
        },
        &evidence,
        &signer,
        &[f.public_key(), key.verifying_key()],
        chrono::Utc::now(),
        &location_id(&evidence_location()),
    )
    .unwrap();
    assert_eq!(report.written, 1, "{:?}", report.points);
    let second_id = point_id(&second_bytes);
    assert_ne!(
        second_id, id,
        "two receipts under one backup_id are two points (defect RECEIPT-DUP)"
    );
    assert!(evidence.get(&record_key(&second_id)).is_ok());
}

#[test]
fn list_reads_the_newest_points_out_of_a_real_bucket() {
    let evidence = evidence_store();
    let f = backup_seam::Fixture::new();
    let backup_id = unique_id("list"); // engine-token-ok: a fixture prefix for the listing test, not an engine subcommand
    let outcome = f
        .execute_as(&evidence, &backup_id, &run_id("F"))
        .expect("the backup succeeds");
    let id = point_id(&evidence.get(&outcome.receipt_key).unwrap().0);

    // The DAY SHARD WALK against a real backend (review finding F5): the fixture
    // engine reports a capture on 2026-09-15, so a window anchored on that day
    // reaches the point in one shard listing rather than by walking the whole
    // log prefix.
    let report = logweir::catalog::cli::list_with(
        &ListArgs {
            location: Location {
                url: format!("s3://{}", bucket()),
                region: Some("us-east-1".into()),
                endpoint: Some(endpoint()),
                path_style: true,
                allow_http: true,
            },
            since: None,
            max: 200,
            days: 1,
        },
        &evidence,
        chrono::NaiveDate::from_ymd_opt(2026, 9, 15).unwrap(),
    )
    .expect("the index lists");
    assert_eq!(report.days_searched, 1);
    assert_eq!(report.oldest_day_searched, "2026-09-15");
    assert!(
        report.rows.iter().any(|e| e.point_id == id),
        "the point this run wrote is in the listing"
    );
    // Per-entry skipping is REPORTED even against a bucket other runs have
    // written to: a short page must never read as "these are all the points".
    assert_eq!(report.inconsistent, 0, "{report:?}");
    // Newest first, over whatever else the bucket holds.
    let mut descending = report.rows.clone();
    descending.sort_by(|a, b| b.recovery_point_at_ms.cmp(&a.recovery_point_at_ms));
    assert_eq!(
        report
            .rows
            .iter()
            .map(|e| e.recovery_point_at_ms)
            .collect::<Vec<_>>(),
        descending
            .iter()
            .map(|e| e.recovery_point_at_ms)
            .collect::<Vec<_>>(),
        "a listing is newest first"
    );
    // Every row names a record that is really there — the index is a pointer,
    // and a pointer nothing follows is worth nothing.
    for e in report.rows.iter().filter(|e| e.point_id == id) {
        assert!(evidence.get(&e.record_key).is_ok(), "{}", e.record_key);
    }
}
