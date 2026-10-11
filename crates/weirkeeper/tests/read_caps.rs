//! **FX-31 — the shared controller reads evidence under a cap.**
//!
//! `weirkeeper` is one process for every namespace. Before FX-31 it read a
//! receipt, a scorecard, a sidecar and a manifest WHOLE, so one tenant's
//! multi-gigabyte object at an evidence key could OOM-kill the controller that
//! serves every namespace, and each restart read it again. These rows hold the
//! controller's five read paths to their caps:
//!
//! * `verify_evidence` — the document under the cap of what it is (FX-33:
//!   `caps::CONTROLLER_RECEIPT` for a backup receipt, `caps::CONTROLLER_DOCUMENT`,
//!   1 MiB, for everything else), the sidecar under `caps::SIDECAR` (64 KiB);
//! * `observe_archive` — the receipt under the receipt cap, the sidecar's
//!   presence by `HEAD`;
//! * `observe_scorecard` — the scorecard under the document cap;
//! * `read_signing_time` — the document under its own kind's cap;
//! * the retention report — each manifest under `caps::CONTROLLER_MANIFEST`.
//!
//! Over the cap: `NotAttempted` (or "not observed", or `skipped`) NAMING the
//! cap — never a crash and never a pass — and FINAL, so the object is not read
//! again on the schedule. A normal document still verifies.
//!
//! The last row measures the peak resident set of all five paths in a CHILD
//! process over a 512 MiB object (`getrusage`, as
//! `crates/logweir-engine-oso/tests/offset_report_memory.rs` does), with
//! controls that show the meter sees a whole read.
//!
//! No socket: the in-memory backend, the misreporting double and a scratch
//! filesystem tree (STANDING RULE 18's allow-list names this file).

use std::path::{Path, PathBuf};

use chrono::{TimeZone as _, Utc};
use logweir_core::engine::StorageUrl;
use logweir_core::ids::sha256_prefixed;
use logweir_store::{caps, Store};
use weirkeeper::controllers::backup::{observe_archive, EvidenceKeys};
use weirkeeper::controllers::restore::{observe_scorecard, split_refused};
use weirkeeper::crds::backup_schedule::Retention;
use weirkeeper::crds::trust_roster::{KeyEntry, TrustRosterSpec};
use weirkeeper::trust::ResolvedTrust;
use weirkeeper::verification::{
    not_attempted_class, read_signing_time, verify_evidence, NotAttemptedClass, SigningTime,
    SigningTimeNeed, VerificationVerdict, CONTROLLER_READ_CAP_PHRASE,
};

const PAYLOAD_KEY: &str = "logweir/drills/r1.json";
const SIDECAR_KEY: &str = "logweir/drills/r1.sig";
/// `VerifyingKey::key_id()` over the fixture's public key.
const FIXTURE_KEY_ID: &str = "917cf9a299872cbf8b2715999ce457464705bb8f48df0a07e9b1e19bb9f383fd";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/weirkeeper sits two levels under the workspace root")
        .to_path_buf()
}

fn fixture(rel: &str) -> Vec<u8> {
    std::fs::read(repo_root().join(rel)).expect("the signed fixture is readable")
}

/// A roster carrying the fixture's PUBLIC key, resolved the way a cluster with
/// no `TrustPolicy` resolves it.
fn trust() -> ResolvedTrust {
    let pem = String::from_utf8(fixture("e2e/fixtures/signed/public.pem")).unwrap();
    weirkeeper::trust::synthesize_legacy(&TrustRosterSpec {
        approver_keys: Vec::new(),
        signing_keys: vec![KeyEntry {
            key_id: FIXTURE_KEY_ID.to_string(),
            spki_pem: pem,
            subject: Some("logweir-runner@example.invalid".to_string()),
            not_after: None,
        }],
        allowed_cluster_ids: Vec::new(),
    })
}

fn put(s: &Store, key: &str, bytes: &[u8]) {
    s.put_create_only(key, bytes)
        .expect("the fixture object is written");
}

/// What every controller refusal says after the cap's byte count, for a cap.
fn names(detail: &str, cap: u64) -> bool {
    detail.contains(&format!("{cap}{CONTROLLER_READ_CAP_PHRASE}"))
}

fn verify(store: &Store, digest: &str) -> weirkeeper::verification::VerificationResult {
    verify_evidence(
        Some(store),
        &trust(),
        PAYLOAD_KEY,
        digest,
        SIDECAR_KEY,
        logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT,
    )
}

// ===========================================================================
// verify_evidence
// ===========================================================================

/// **An oversized receipt is `NotAttempted` naming the cap, FINAL; a normal
/// one verifies.**
///
/// KILLS: "a cap far too large" (the 1 MiB + 1 receipt is read and is
/// `Invalid` on its digest instead); "the controller reads uncapped".
#[test]
fn an_oversized_receipt_is_not_attempted_naming_the_cap_while_a_normal_one_verifies() {
    // CONTROL: the signed fixture verifies through the same capped read.
    let receipt = fixture("e2e/fixtures/signed/backup-receipt.json");
    let sidecar = fixture("e2e/fixtures/signed/backup-receipt.sig");
    let normal = Store::in_memory("logweir/");
    put(&normal, PAYLOAD_KEY, &receipt);
    put(&normal, SIDECAR_KEY, &sidecar);
    let r = verify(&normal, &sha256_prefixed(&receipt));
    assert_eq!(
        r.result,
        VerificationVerdict::Valid,
        "the control verifies: {r:?}"
    );

    // ONE BYTE over the cap — a RECEIPT's own (FX-33), which is what
    // `verify` reads this document as.
    let big = vec![b' '; usize::try_from(caps::CONTROLLER_RECEIPT).unwrap() + 1];
    let over = Store::in_memory("logweir/");
    put(&over, PAYLOAD_KEY, &big);
    put(&over, SIDECAR_KEY, &sidecar);
    let r = verify(&over, &sha256_prefixed(&big));
    assert_eq!(r.result, VerificationVerdict::NotAttempted, "{r:?}");
    let detail = r.detail.clone().expect("a refusal carries its sentence");
    assert!(
        names(&detail, caps::CONTROLLER_RECEIPT) && detail.contains(PAYLOAD_KEY),
        "the detail names the key and the receipt cap: {detail}"
    );
    assert_eq!(
        not_attempted_class(&detail),
        NotAttemptedClass::Final,
        "the object will not shrink: never read again on the schedule"
    );
    let (scheduled, retry) =
        weirkeeper::evidence_fetch::scheduled(r, 1, Utc.timestamp_opt(1_790_000_000, 0).unwrap());
    assert_eq!(retry, None, "a final verdict gets no retry: {scheduled:?}");
}

/// A multi-gigabyte receipt costs no body byte: it is refused on the size the
/// store reports.
///
/// KILLS: "no head check" in the store (the meter moves).
#[test]
fn a_receipt_reported_at_gigabytes_is_refused_without_reading_its_body() {
    let (store, meter) = Store::in_memory_misreporting_size("logweir/", 5 << 30);
    put(&store, PAYLOAD_KEY, b"{}");
    put(&store, SIDECAR_KEY, b"{}");
    let r = verify(&store, &sha256_prefixed(b"{}"));
    assert_eq!(r.result, VerificationVerdict::NotAttempted, "{r:?}");
    let detail = r.detail.expect("a refusal carries its sentence");
    assert!(names(&detail, caps::CONTROLLER_RECEIPT), "{detail}");
    assert!(
        detail.contains("the store reports 5368709120 bytes"),
        "{detail}"
    );
    assert_eq!(meter.streamed(), 0, "not one body byte was taken");
}

/// The sidecar has its own, smaller cap.
#[test]
fn an_oversized_sidecar_is_not_attempted_naming_its_cap() {
    let receipt = fixture("e2e/fixtures/signed/backup-receipt.json");
    let store = Store::in_memory("logweir/");
    put(&store, PAYLOAD_KEY, &receipt);
    put(
        &store,
        SIDECAR_KEY,
        &vec![b' '; usize::try_from(caps::SIDECAR).unwrap() + 1],
    );
    let r = verify(&store, &sha256_prefixed(&receipt));
    assert_eq!(r.result, VerificationVerdict::NotAttempted, "{r:?}");
    let detail = r.detail.expect("a refusal carries its sentence");
    assert!(
        names(&detail, caps::SIDECAR) && detail.contains(SIDECAR_KEY),
        "the detail names the sidecar and the 64 KiB cap: {detail}"
    );
}

// ===========================================================================
// observe_archive
// ===========================================================================

/// **The sidecar's presence is a `HEAD`, and an oversized receipt is PRESENT
/// and unread.** Both objects are reported at five gibibytes.
///
/// KILLS: "existence via a full read" (a capped GET of the sidecar is refused,
/// so the sidecar reads as ABSENT and an exit-4 run would be called orphaned);
/// "an oversized receipt is absent" (the same, for the payload).
#[test]
fn observe_archive_sees_oversized_objects_present_and_reads_neither() {
    let (store, meter) = Store::in_memory_misreporting_size("logweir/", 5 << 30);
    put(
        &store,
        PAYLOAD_KEY,
        br#"{"covered":{"from_ms":1,"to_ms":2}}"#,
    );
    put(&store, SIDECAR_KEY, b"{}");
    let keys = EvidenceKeys {
        receipt: Some(PAYLOAD_KEY.to_string()),
        sidecar: Some(SIDECAR_KEY.to_string()),
        receipt_sha256: None,
    };
    let o = observe_archive(&store, &keys).expect("two keys: an observation");
    assert!(o.presence.payload, "an oversized receipt is THERE");
    assert!(o.presence.sidecar, "an oversized sidecar is THERE, by HEAD");
    assert_eq!(
        o.receipt_sha256, None,
        "and nothing of the receipt was read"
    );
    assert_eq!(o.covered, None);
    assert_eq!(o.records, None);
    assert_eq!(o.capture, None);
    assert_eq!(meter.streamed(), 0, "neither body was read");

    // CONTROL: a normal receipt is read and observed.
    let normal = Store::in_memory("logweir/");
    let receipt = fixture("e2e/fixtures/signed/backup-receipt.json");
    put(&normal, PAYLOAD_KEY, &receipt);
    put(&normal, SIDECAR_KEY, b"{}");
    let o = observe_archive(&normal, &keys).expect("an observation");
    assert!(o.presence.payload && o.presence.sidecar);
    assert_eq!(o.receipt_sha256, Some(sha256_prefixed(&receipt)));
    assert!(o.covered.is_some(), "the fixture's window is observed");
}

// ===========================================================================
// observe_scorecard
// ===========================================================================

/// **An oversized scorecard is an observation carrying only the refusal**, and
/// `split_refused` makes it NOT OBSERVED with the sentence beside it.
///
/// KILLS: "an oversized scorecard is unread like any other" (the reason would
/// be "read no such document", transient, re-read on the schedule).
#[test]
fn an_oversized_scorecard_is_not_observed_and_its_refusal_names_the_cap() {
    let (store, meter) = Store::in_memory_misreporting_size("logweir/", 5 << 30);
    put(&store, PAYLOAD_KEY, br#"{"outcome":"pass"}"#);
    let o = observe_scorecard(&store, PAYLOAD_KEY).expect("a refusal is an observation");
    let (observed, refused) = split_refused(Some(o));
    assert_eq!(observed, None, "no fact is taken from a refused scorecard");
    let refused = refused.expect("the refusal is returned beside it");
    assert!(names(&refused, caps::CONTROLLER_DOCUMENT), "{refused}");
    assert_eq!(not_attempted_class(&refused), NotAttemptedClass::Final);
    assert_eq!(meter.streamed(), 0);

    // CONTROL: the signed fixture is observed, and split_refused passes it on.
    let normal = Store::in_memory("logweir/");
    put(
        &normal,
        PAYLOAD_KEY,
        &fixture("e2e/fixtures/signed/scorecard.json"),
    );
    let (observed, refused) = split_refused(observe_scorecard(&normal, PAYLOAD_KEY));
    assert_eq!(refused, None);
    assert!(
        observed.and_then(|o| o.outcome).is_some(),
        "the fixture's outcome is observed"
    );
}

// ===========================================================================
// read_signing_time
// ===========================================================================

#[test]
fn the_signing_time_re_read_names_the_cap() {
    let (store, meter) = Store::in_memory_misreporting_size("logweir/", 5 << 30);
    put(&store, PAYLOAD_KEY, b"{}");
    let need = SigningTimeNeed {
        payload_key: PAYLOAD_KEY.to_string(),
        payload_sha256: sha256_prefixed(b"{}"),
        payload_type: logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT.to_string(),
    };
    match read_signing_time(Some(&store), &need) {
        // Settled, not retried (review F8). The cap is the receipt's own.
        SigningTime::OverCap(detail) => {
            assert!(names(&detail, caps::CONTROLLER_RECEIPT), "{detail}")
        }
        other => panic!("an oversized document yields no signing time, got {other:?}"),
    }
    assert_eq!(meter.streamed(), 0);
}

// ===========================================================================
// The retention report
// ===========================================================================

/// A manifest over the controller's manifest cap is SKIPPED with the sentence
/// naming the cap — neither kept nor removable — and the rest is reported.
#[test]
fn the_retention_report_skips_an_oversized_manifest_naming_the_cap() {
    let (store, meter) = Store::in_memory_misreporting_size("logweir/", 5 << 30);
    put(
        &store,
        "logweir/archive/b1/manifest.json",
        br#"{"topics":[{"partitions":[{"segments":[{"start_timestamp":1,"end_timestamp":2}]}]}]}"#,
    );
    let report = weirkeeper::retention::evaluate(
        &store,
        "s3://bucket/logweir/archive/",
        "logweir/archive/",
        &Retention {
            keep_last: Some(1),
            keep_days: None,
        },
        Utc.timestamp_opt(1_790_000_000, 0).unwrap(),
    )
    .expect("the listing answers");
    assert_eq!(report.skipped.len(), 1, "{report:?}");
    assert!(
        report.skipped[0]
            .reason
            .contains(&format!("{}-byte read cap", caps::CONTROLLER_MANIFEST)),
        "the reason names the cap: {}",
        report.skipped[0].reason
    );
    assert_eq!(meter.streamed(), 0, "the manifest's body was not read");
}

// ===========================================================================
// Peak resident memory, measured in a child process
// ===========================================================================

/// How big the planted objects are.
const PLANTED: u64 = 512 << 20;

/// The most resident memory the five capped read paths may add, together, to
/// a baseline child running the same paths over small objects.
const BOUND: u64 = 16 << 20;

const TEST_NAME: &str = "the_controller_read_paths_hold_bounded_memory_over_a_512_mib_object";
const CHILD_ENV: &str = "FX31_MEM_CHILD";
const ROOT_ENV: &str = "FX31_MEM_ROOT";
const PEAK_LINE: &str = "FX31_PEAK_RSS=";
/// What a capped child prints about each path's answer; the parent relays it.
const CHILD_LINE: &str = "[fx31-child]";

/// This process's own peak resident set, in bytes (`ru_maxrss` is kilobytes on
/// Linux and bytes on macOS). Read by the CHILD at its end, so each child
/// reports only its own peak and no ordering between children matters.
fn self_peak_rss() -> u64 {
    let usage = nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_SELF)
        .expect("getrusage(RUSAGE_SELF)");
    let max = u64::try_from(usage.max_rss()).unwrap_or(0);
    if cfg!(target_os = "macos") {
        max
    } else {
        max * 1024
    }
}

/// A filesystem evidence tree: each of the five objects at `size` bytes,
/// SPARSE (no disk is spent on the zeros), plus the manifest under the
/// retention prefix.
fn plant(root: &Path, size: u64) {
    for key in [
        PAYLOAD_KEY,
        SIDECAR_KEY,
        "logweir/drills/r2.scorecard.json",
        "logweir/archive/b1/manifest.json",
    ] {
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(size).unwrap();
    }
}

/// The control's object: [`RANDOM_BYTES`] of pseudo-random bytes, which no
/// page compressor shrinks.
const RANDOM_KEY: &str = "logweir/drills/random.bin";
const RANDOM_BYTES: u64 = 128 << 20;

/// `len` pseudo-random bytes (xorshift64*) at `path`.
fn write_random(path: &Path, len: u64) {
    use std::io::Write as _;
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut w = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut left = len;
    while left > 0 {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        let word = x.wrapping_mul(0x2545_F491_4F6C_DD1D).to_le_bytes();
        let n = usize::try_from(left.min(8)).unwrap();
        w.write_all(&word[..n]).unwrap();
        left -= n as u64;
    }
    w.flush().unwrap();
}

/// Where [`plant_tiny_values`] writes its manifest.
const TINY_VALUES_KEY: &str = "logweir/archive/tiny/manifest.json";

/// A 16 MiB manifest-shaped body of tiny values — `{"topics":[],"pad":[0,0,…]}`
/// — written under `root`; returns its size.
fn plant_tiny_values(root: &Path) -> u64 {
    use std::io::Write as _;
    let path = root.join(TINY_VALUES_KEY);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut w = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
    w.write_all(br#"{"topics":[],"pad":[0"#).unwrap();
    for _ in 0..(8 << 20) {
        w.write_all(b",0").unwrap();
    }
    w.write_all(b"]}").unwrap();
    w.flush().unwrap();
    drop(w);
    std::fs::metadata(&path).unwrap().len()
}

/// Set to the compose MinIO's S3 endpoint (for example
/// `http://localhost:39000` on slot 3) to run the LIVE row: every child then
/// reads the bucket [`LIVE_BUCKET`] through a read-only S3 handle instead of a
/// filesystem tree, with the credential from the three named `AWS_*`
/// variables. The objects are seeded OUT of this process (`claude/fx-31.result.md`
/// §5 uses `curl --aws-sigv4` over a sparse file), at the keys [`plant`] uses.
const LIVE_ENDPOINT_ENV: &str = "FX31_LIVE_S3_ENDPOINT";
const LIVE_BUCKET: &str = "kafka-backups";

fn handle(root: &Path) -> Store {
    if let Ok(endpoint) = std::env::var(LIVE_ENDPOINT_ENV) {
        return Store::read_only_with(
            &StorageUrl::S3 {
                bucket: LIVE_BUCKET.to_string(),
                prefix: "logweir/".to_string(),
                region: Some("us-east-1".to_string()),
                endpoint: Some(endpoint),
                path_style: true,
                allow_http: true,
            },
            &logweir_store::StoreOptions::static_from_env()
                .with_request_timeout(std::time::Duration::from_secs(60)),
        )
        .expect("a read-only S3 handle over the compose MinIO builds");
    }
    Store::read_only_from_url(&StorageUrl::Filesystem {
        path: root.to_path_buf(),
    })
    .expect("a filesystem handle over an existing directory builds")
}

/// The child's work: every controller read path once over the tree, or (the
/// control) one read of the receipt under a cap far too large.
fn run_child(mode: &str, root: &Path) {
    let store = handle(root);
    match mode {
        "baseline" | "capped" => {
            let r = verify(&store, &sha256_prefixed(b"x"));
            let keys = EvidenceKeys {
                receipt: Some(PAYLOAD_KEY.to_string()),
                sidecar: Some(SIDECAR_KEY.to_string()),
                receipt_sha256: None,
            };
            let o = observe_archive(&store, &keys).expect("an observation");
            assert!(o.presence.payload && o.presence.sidecar, "{o:?}");
            let (_, refused) = split_refused(observe_scorecard(
                &store,
                "logweir/drills/r2.scorecard.json",
            ));
            let signing_time = read_signing_time(
                Some(&store),
                &SigningTimeNeed {
                    payload_key: PAYLOAD_KEY.to_string(),
                    payload_sha256: sha256_prefixed(b"x"),
                    payload_type: logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT.to_string(),
                },
            );
            let report = weirkeeper::retention::evaluate(
                &store,
                "file:///archive",
                "logweir/archive/",
                &Retention {
                    keep_last: Some(1),
                    keep_days: None,
                },
                Utc.timestamp_opt(1_790_000_000, 0).unwrap(),
            )
            .expect("the listing answers");
            assert_eq!(report.skipped.len(), 1, "{report:?}");
            if mode == "capped" {
                // What each path SAID, for the record a live run keeps.
                println!(
                    "{CHILD_LINE} verify_evidence: {:?} {:?}",
                    r.result, r.detail
                );
                println!(
                    "{CHILD_LINE} observe_archive: presence {:?}, receipt_sha256 {:?}",
                    o.presence, o.receipt_sha256
                );
                println!("{CHILD_LINE} observe_scorecard: read_refused {refused:?}");
                println!("{CHILD_LINE} read_signing_time: {signing_time:?}");
                println!(
                    "{CHILD_LINE} retention skipped: {:?}",
                    report.skipped[0].reason
                );
                assert_eq!(r.result, VerificationVerdict::NotAttempted, "{r:?}");
                // FX-33: the receipt under its own cap, the scorecard under
                // the document cap.
                assert!(
                    r.detail
                        .as_deref()
                        .is_some_and(|d| names(d, caps::CONTROLLER_RECEIPT)),
                    "{r:?}"
                );
                assert!(refused.is_some_and(|d| names(&d, caps::CONTROLLER_DOCUMENT)));
                assert!(
                    matches!(&signing_time, SigningTime::OverCap(d) if names(d, caps::CONTROLLER_RECEIPT))
                );
                assert!(report.skipped[0]
                    .reason
                    .contains(&format!("{}-byte read cap", caps::CONTROLLER_MANIFEST)));
            }
        }
        "uncapped" => {
            let (bytes, _) = store
                .get_capped(RANDOM_KEY, u64::MAX)
                .expect("a cap far too large reads the whole object");
            assert_eq!(bytes.len() as u64, RANDOM_BYTES);
        }
        "uncapped-live" => {
            let (bytes, _) = store
                .get_capped(PAYLOAD_KEY, u64::MAX)
                .expect("a cap far too large reads the whole object");
            assert!(bytes.len() as u64 > caps::CONTROLLER_DOCUMENT);
        }
        // A manifest of tiny values WITHIN the controller's cap: the
        // streaming fold holds the bytes and nothing per value.
        "stream" => {
            let r = store.manifest_facts(TINY_VALUES_KEY, caps::CONTROLLER_MANIFEST);
            assert!(r.is_err(), "the body declares no segment: {r:?}");
        }
        // The control: the same bytes as a `serde_json::Value`, which is what
        // `manifest_facts` built before FX-31.
        "value" => {
            let (bytes, _) = store
                .get_capped(TINY_VALUES_KEY, caps::CONTROLLER_MANIFEST)
                .expect("within the cap");
            let v: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
            assert!(v["pad"].is_array());
        }
        "conc-manifests" | "conc-manifests-free" => {
            let free = weirkeeper::read_budget::ReadBudget::new(u64::MAX);
            let budget = if mode.ends_with("-free") {
                &free
            } else {
                weirkeeper::read_budget::ReadBudget::controller()
            };
            let start = std::sync::Barrier::new(CONC_MANIFESTS);
            std::thread::scope(|scope| {
                for i in 0..CONC_MANIFESTS {
                    let (store, start) = (&store, &start);
                    scope.spawn(move || {
                        start.wait();
                        let report = weirkeeper::retention::evaluate_within(
                            budget,
                            store,
                            "file:///archive",
                            &format!("logweir/conc/p{i}/"),
                            &Retention {
                                keep_last: Some(1),
                                keep_days: None,
                            },
                            Utc.timestamp_opt(1_790_000_000, 0).unwrap(),
                        )
                        .expect("the listing answers");
                        assert_eq!(report.skipped, Vec::new(), "every manifest folds");
                    });
                }
            });
        }
        "conc-documents" | "conc-documents-free" => {
            let free = weirkeeper::read_budget::ReadBudget::new(u64::MAX);
            let budget = if mode.ends_with("-free") {
                &free
            } else {
                weirkeeper::read_budget::ReadBudget::controller()
            };
            let start = std::sync::Barrier::new(CONC_DOCUMENTS);
            std::thread::scope(|scope| {
                for i in 0..CONC_DOCUMENTS {
                    let (store, start) = (&store, &start);
                    scope.spawn(move || {
                        start.wait();
                        let o = weirkeeper::controllers::restore::observe_scorecard_within(
                            budget,
                            store,
                            &format!("logweir/conc/s{i}.scorecard.json"),
                        );
                        assert!(o.is_some(), "a JSON object is observed");
                    });
                }
            });
        }
        other => panic!("unknown {CHILD_ENV} mode {other}"),
    }
}

/// Runs this test again in a child, in `mode`, over `root`, and returns the
/// child's own peak resident set.
fn child_peak(mode: &str, root: &Path) -> u64 {
    child_peak_over(mode, root, None)
}

/// [`child_peak`], over the compose MinIO at `live` when it is `Some`, and
/// over the filesystem tree at `root` (never an endpoint the parent happens to
/// carry) when it is `None`.
fn child_peak_over(mode: &str, root: &Path, live: Option<&str>) -> u64 {
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, mode)
        .env(ROOT_ENV, root)
        // THE METER MEASURES LIVE MEMORY, NOT AN ALLOCATOR'S CACHE (review F2).
        // macOS's libmalloc keeps freed large blocks resident in its large
        // cache: measured here, SIXTEEN parses of a 1 MB document in ONE
        // thread, one after the other, peaked at 602 MB, and at 39 MB with the
        // cache off. glibc's dynamic mmap threshold does the same in kind. The
        // controller's bound is on what is live at once, so each child turns
        // both off; each variable is ignored by the other platform's allocator.
        .env("MallocLargeCache", "0")
        .env("MALLOC_MMAP_THRESHOLD_", "131072");
    match live {
        Some(endpoint) => cmd.env(LIVE_ENDPOINT_ENV, endpoint),
        None => cmd.env_remove(LIVE_ENDPOINT_ENV),
    };
    let out = cmd.output().expect("the child test process starts");
    let stdout = String::from_utf8_lossy(&out.stdout);
    for line in stdout.lines().filter(|l| l.contains(CHILD_LINE)) {
        eprintln!("{mode}: {}", &line[line.find(CHILD_LINE).unwrap_or(0)..]);
    }
    assert!(
        out.status.success(),
        "the {mode} child failed: {}\n{stdout}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
        .lines()
        .find_map(|l| l.find(PEAK_LINE).map(|at| &l[at + PEAK_LINE.len()..]))
        .and_then(|v| {
            v.trim()
                .split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse()
                .ok()
        })
        .unwrap_or_else(|| panic!("the {mode} child printed no {PEAK_LINE} line:\n{stdout}"))
}

/// **Peak RSS is bounded.** All five controller read paths over 512 MiB
/// objects add at most [`BOUND`] to a child running them over 1 KiB objects;
/// the control — the same `Store` reading the receipt under a cap far too
/// large — adds the object, which shows the meter sees a whole read.
///
/// The second half measures the PARSE: a 16 MiB manifest of tiny values,
/// within the controller's manifest cap, through `manifest_facts`' streaming
/// fold, against the same bytes parsed as a `serde_json::Value` — what
/// `manifest_facts` did before FX-31.
///
/// KILLS: "a cap far too large" in any of the five paths (that child holds
/// 512 MiB); "`manifest_facts` parses into a `Value`" (the stream child holds
/// the control's many-times-the-bytes). "No head check" is NOT this row's: a
/// filesystem stream would still stop at the running cap (1 MiB), under the
/// bound; the misreporting rows above, whose meter counts body bytes, kill
/// it.
#[test]
fn the_controller_read_paths_hold_bounded_memory_over_a_512_mib_object() {
    if let Ok(mode) = std::env::var(CHILD_ENV) {
        let root = PathBuf::from(std::env::var(ROOT_ENV).expect(ROOT_ENV));
        run_child(&mode, &root);
        println!("{PEAK_LINE}{}", self_peak_rss());
        return;
    }
    let scratch = std::env::temp_dir().join(format!(
        "logweir-fx31-rss-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let small = scratch.join("small");
    let big = scratch.join("big");
    plant(&small, 1024);
    plant(&big, PLANTED);

    let baseline = child_peak("baseline", &small);
    let capped = child_peak("capped", &big);
    let added = capped.saturating_sub(baseline);
    eprintln!(
        "[fx31-mem] baseline child {baseline} B; five capped paths over {PLANTED} B objects \
         {capped} B (adds {added} B, bound {BOUND} B)"
    );
    // THE CONTROL READS INCOMPRESSIBLE BYTES (review F9): the sparse objects
    // read as zeros, which macOS compresses under memory pressure, so a whole
    // read of one measured anywhere from 171 MB to 536 MB.
    write_random(&big.join(RANDOM_KEY), RANDOM_BYTES);
    let uncapped = child_peak("uncapped", &big);
    let whole = uncapped.saturating_sub(baseline);
    eprintln!(
        "[fx31-mem] control, one read under a cap far too large: {uncapped} B (adds {whole} B)"
    );

    // THE PARSE, NOT ONLY THE READ: a manifest of tiny values within the
    // controller's manifest cap.
    let tiny = scratch.join("tiny");
    let raw = plant_tiny_values(&tiny);
    let streamed = child_peak("stream", &tiny).saturating_sub(baseline);
    let as_value = child_peak("value", &tiny).saturating_sub(baseline);
    eprintln!(
        "[fx31-mem] a {raw} B manifest of tiny values: the streaming fold adds {streamed} B; \
         the same bytes as a serde_json::Value add {as_value} B ({}x the bytes)",
        as_value / raw.max(1)
    );
    let _ = std::fs::remove_dir_all(&scratch);

    assert!(
        streamed < raw + BOUND,
        "the streaming fold over a {raw}-byte manifest added {streamed} bytes; it may hold the \
         bytes it read and nothing per value (bound {raw} + {BOUND})"
    );
    assert!(
        as_value > 4 * raw,
        "the Value control added only {as_value} bytes over {raw} bytes of JSON; the meter cannot \
         show what the streaming fold saves"
    );
    assert!(
        added < BOUND,
        "the controller's read paths over {PLANTED}-byte objects added {added} bytes of resident \
         memory at their peak; each must be refused at its cap, never read (bound {BOUND})"
    );
    // The control's 128 MiB of random bytes are resident whatever the host's
    // memory pressure; four times the bound is what it must clear.
    assert!(
        whole > 4 * BOUND,
        "the control added only {whole} bytes; the meter cannot tell a capped read from a whole \
         one at this size"
    );
}

/// **LIVE (FX-31), against the compose MinIO: the controller's five read paths
/// over 768 MiB objects in a real S3 bucket.** `#[ignore]`d: it needs the
/// stack and objects seeded out of process. Run it as
/// `claude/fx-31.result.md` §5 does:
///
/// ```text
/// FX31_LIVE_S3_ENDPOINT=http://localhost:39000 AWS_ACCESS_KEY_ID=… \
///   AWS_SECRET_ACCESS_KEY=… cargo test -p weirkeeper --test read_caps -- \
///   --ignored --nocapture against_minio
/// ```
///
/// The store reports each object's size in the GET's own `Content-Length`,
/// so every capped path is refused on it with no body read, and the control
/// child reading the receipt under a cap far too large holds the object.
#[test]
#[ignore = "live: needs the compose MinIO and seeded objects (FX31_LIVE_S3_ENDPOINT)"]
fn the_controller_read_paths_hold_bounded_memory_against_minio() {
    let size: u64 = std::env::var("FX31_LIVE_OBJECT_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(768 << 20);
    assert!(
        std::env::var(LIVE_ENDPOINT_ENV).is_ok(),
        "set {LIVE_ENDPOINT_ENV} to the compose MinIO and seed the objects first"
    );
    // The filesystem root is unused by an S3 child; the baseline still reads
    // small objects, from a scratch tree.
    let scratch = std::env::temp_dir().join(format!(
        "logweir-fx31-live-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let small = scratch.join("small");
    plant(&small, 1024);
    let endpoint = std::env::var(LIVE_ENDPOINT_ENV).expect("checked above");
    // The baseline reads the SMALL tree, with no endpoint.
    let baseline = child_peak("baseline", &small);
    let capped = child_peak_over("capped", &small, Some(&endpoint));
    let uncapped = child_peak_over("uncapped-live", &small, Some(&endpoint));
    let _ = std::fs::remove_dir_all(&scratch);
    let added = capped.saturating_sub(baseline);
    let whole = uncapped.saturating_sub(baseline);
    eprintln!(
        "[fx31-live] S3 objects of {size} B: baseline child {baseline} B; five capped paths \
         {capped} B (adds {added} B, bound {BOUND} B); control under a cap far too large \
         {uncapped} B (adds {whole} B)"
    );
    assert!(
        added < BOUND,
        "the capped paths added {added} bytes over S3 (bound {BOUND})"
    );
    assert!(
        whole > 4 * BOUND,
        "the control added only {whole} bytes over S3 (objects of {size} bytes)"
    );
}

// ===========================================================================
// Concurrent reads share ONE budget (FX-31 review F2)
// ===========================================================================

/// How many retention evaluations, and how many scorecard reads, run at once.
const CONC_MANIFESTS: usize = 8;
const CONC_DOCUMENTS: usize = 16;

/// A tree for the concurrency row: [`CONC_MANIFESTS`] archive prefixes, each
/// holding ONE manifest (a hard link to one file of `manifest_bytes`), and
/// [`CONC_DOCUMENTS`] scorecards (hard links to one document of
/// `document_bytes`).
///
/// The manifest is a VALID one whose segment keys are pseudo-random text, so
/// its fold takes long enough that the reads overlap, and the buffered bytes
/// do not compress. The scorecard is `{"pad":[0,0,…]}`: valid, and the shape
/// whose `serde_json::Value` is largest per byte.
fn plant_concurrency(root: &Path, manifest_bytes: usize, document_bytes: usize) {
    use std::io::Write as _;
    let pool = root.join("pool");
    std::fs::create_dir_all(&pool).unwrap();
    let manifest = pool.join("manifest.json");
    {
        let mut w = std::io::BufWriter::new(std::fs::File::create(&manifest).unwrap());
        w.write_all(br#"{"topics":[{"partitions":[{"segments":["#)
            .unwrap();
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut written = 0usize;
        let mut first = true;
        while first || written < manifest_bytes {
            let mut key = [0u8; 96];
            for b in &mut key {
                x ^= x >> 12;
                x ^= x << 25;
                x ^= x >> 27;
                *b =
                    alphabet[usize::try_from(x.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 58).unwrap()];
            }
            let entry = format!(
                r#"{}{{"key":"{}","start_timestamp":1,"end_timestamp":2}}"#,
                if first { "" } else { "," },
                std::str::from_utf8(&key).unwrap()
            );
            w.write_all(entry.as_bytes()).unwrap();
            written += entry.len();
            first = false;
        }
        w.write_all(b"]}]}]}").unwrap();
    }
    let document = pool.join("scorecard.json");
    {
        let mut w = std::io::BufWriter::new(std::fs::File::create(&document).unwrap());
        w.write_all(br#"{"pad":[0"#).unwrap();
        for _ in 0..document_bytes.saturating_sub(12) / 2 {
            w.write_all(b",0").unwrap();
        }
        w.write_all(b"]}").unwrap();
    }
    for i in 0..CONC_MANIFESTS {
        let at = root.join(format!("logweir/conc/p{i}/b1/manifest.json"));
        std::fs::create_dir_all(at.parent().unwrap()).unwrap();
        std::fs::hard_link(&manifest, &at).unwrap();
    }
    for i in 0..CONC_DOCUMENTS {
        let at = root.join(format!("logweir/conc/s{i}.scorecard.json"));
        std::fs::create_dir_all(at.parent().unwrap()).unwrap();
        std::fs::hard_link(&document, &at).unwrap();
    }
}

/// **The caps bound each read; the budget bounds them TOGETHER** (FX-31 review
/// F2).
///
/// In child processes:
/// - [`CONC_MANIFESTS`] retention evaluations, each over a 60 MiB manifest
///   (under the 64 MiB cap), start together;
/// - [`CONC_DOCUMENTS`] scorecard observations, each over a document just
///   under the 1 MiB cap that parses into about 37 MiB, start together.
///
/// Under the controller's budget, peak RSS stays within the budget plus a
/// fixed slack, whatever the number of readers. The control runs the same
/// readers under a budget that admits everything (`ReadBudget::new(u64::MAX)`)
/// and must add more than twice the budget, which shows the meter sees
/// concurrent reads.
///
/// KILLS: "no reservation" in `retention::evaluate` or `observe_scorecard`
/// (the budgeted child holds the control's memory); "a budget far too large".
#[test]
fn concurrent_reads_share_one_budget() {
    use weirkeeper::read_budget::CONTROLLER_READ_BUDGET_BYTES as BUDGET;
    if std::env::var(CHILD_ENV).is_ok() {
        // The child's work is the other row's dispatch (`TEST_NAME`).
        return;
    }
    let scratch = std::env::temp_dir().join(format!(
        "logweir-fx31-conc-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let small = scratch.join("small");
    let big = scratch.join("big");
    plant_concurrency(&small, 1, 16);
    plant_concurrency(&big, 60 << 20, 1_040_000);
    let slack: u64 = 32 << 20;

    let m_base = child_peak("conc-manifests", &small);
    let m_budget = child_peak("conc-manifests", &big).saturating_sub(m_base);
    let m_free = child_peak("conc-manifests-free", &big).saturating_sub(m_base);
    let d_base = child_peak("conc-documents", &small);
    let d_budget = child_peak("conc-documents", &big).saturating_sub(d_base);
    let d_free = child_peak("conc-documents-free", &big).saturating_sub(d_base);
    let _ = std::fs::remove_dir_all(&scratch);
    eprintln!(
        "[fx31-conc] {CONC_MANIFESTS} evaluations of 60 MiB manifests: under the budget add \
         {m_budget} B, with no budget {m_free} B; {CONC_DOCUMENTS} scorecards of 1,040,000 B: \
         under the budget {d_budget} B, with no budget {d_free} B (budget {BUDGET} B, slack \
         {slack} B)"
    );
    assert!(
        m_budget < BUDGET + slack,
        "{CONC_MANIFESTS} concurrent retention evaluations added {m_budget} bytes; the budget \
         holds them to {BUDGET} plus {slack}"
    );
    assert!(
        d_budget < BUDGET + slack,
        "{CONC_DOCUMENTS} concurrent scorecard reads added {d_budget} bytes; the budget holds \
         them to {BUDGET} plus {slack}"
    );
    assert!(
        m_free > 2 * BUDGET,
        "the control's evaluations added only {m_free} bytes; the meter cannot see concurrent \
         reads at this size"
    );
    assert!(
        d_free > 2 * BUDGET,
        "the control's scorecard reads added only {d_free} bytes; the meter cannot see \
         concurrent reads at this size"
    );
}

/// **Every controller read path reserves from the ONE budget before it reads**
/// (FX-31 review F2). With the whole controller budget held, each of the five
/// paths waits; released, each one finishes.
///
/// KILLS: "no reservation" in any one of `verify_evidence`, `observe_archive`,
/// `observe_scorecard`, `read_signing_time` and `retention::evaluate` (that
/// path finishes while the budget is held).
#[test]
fn every_controller_read_path_waits_for_the_budget() {
    use std::sync::mpsc;
    use std::time::Duration;
    use weirkeeper::read_budget::ReadBudget;
    let receipt = fixture("e2e/fixtures/signed/backup-receipt.json");
    type ReadPath = Box<dyn FnOnce(&Store) + Send>;
    let paths: Vec<(&str, ReadPath)> = vec![
        (
            "verify_evidence",
            Box::new(|s| {
                let _ = verify(s, "sha256:x");
            }),
        ),
        (
            "observe_archive",
            Box::new(|s| {
                let _ = observe_archive(
                    s,
                    &EvidenceKeys {
                        receipt: Some(PAYLOAD_KEY.to_string()),
                        sidecar: Some(SIDECAR_KEY.to_string()),
                        receipt_sha256: None,
                    },
                );
            }),
        ),
        (
            "observe_scorecard",
            Box::new(|s| {
                let _ = observe_scorecard(s, PAYLOAD_KEY);
            }),
        ),
        (
            "read_signing_time",
            Box::new(|s| {
                let _ = read_signing_time(
                    Some(s),
                    &SigningTimeNeed {
                        payload_key: PAYLOAD_KEY.to_string(),
                        payload_sha256: "sha256:x".to_string(),
                        payload_type: logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT.to_string(),
                    },
                );
            }),
        ),
        (
            "retention::evaluate",
            Box::new(|s| {
                let _ = weirkeeper::retention::evaluate(
                    s,
                    "s3://bucket/logweir/archive/",
                    "logweir/archive/",
                    &Retention {
                        keep_last: Some(1),
                        keep_days: None,
                    },
                    Utc.timestamp_opt(1_790_000_000, 0).unwrap(),
                );
            }),
        ),
    ];
    for (name, path) in paths {
        let store = Store::in_memory("logweir/");
        put(&store, PAYLOAD_KEY, &receipt);
        put(&store, SIDECAR_KEY, b"{}");
        put(
            &store,
            "logweir/archive/b1/manifest.json",
            br#"{"topics":[{"partitions":[{"segments":[{"start_timestamp":1,"end_timestamp":2}]}]}]}"#,
        );
        let budget = ReadBudget::controller();
        let held = budget.reserve(budget.total());
        let (done, finished) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            path(&store);
            let _ = done.send(());
        });
        assert!(
            finished.recv_timeout(Duration::from_millis(300)).is_err(),
            "{name} read while the controller's whole budget was held: it reserves nothing"
        );
        drop(held);
        assert!(
            finished.recv_timeout(Duration::from_secs(20)).is_ok(),
            "{name} did not finish once the budget was released"
        );
        worker.join().expect("the reader thread ends");
    }
}

/// **The budget fits the controller it runs in** (FX-31 review F2): at most a
/// quarter of the chart's controller memory limit, room for one manifest
/// read, and a document read costs its cap plus the measured 37× parse.
///
/// KILLS: "a budget far too large"; "a document read that reserves only its
/// bytes".
#[test]
fn the_read_budget_fits_the_charts_controller_limit() {
    use weirkeeper::read_budget::{
        CONTROLLER_READ_BUDGET_BYTES, DOCUMENT_READ_COST_BYTES, MANIFEST_READ_COST_BYTES,
    };
    let values: serde_yaml::Value = serde_yaml::from_str(
        &std::fs::read_to_string(repo_root().join("charts/logweir/values.yaml"))
            .expect("the chart's values read"),
    )
    .expect("the chart's values parse");
    let limit = values["controller"]["resources"]["limits"]["memory"]
        .as_str()
        .expect("controller.resources.limits.memory is set");
    let mib: u64 = limit
        .strip_suffix("Mi")
        .and_then(|n| n.parse().ok())
        .expect("the limit is written in Mi");
    let limit_bytes = mib << 20;
    assert!(
        4 * CONTROLLER_READ_BUDGET_BYTES <= limit_bytes,
        "the read budget ({CONTROLLER_READ_BUDGET_BYTES} B) is more than a quarter of the \
         controller's {limit} limit"
    );
    // Constant relations, held at compile time.
    const _: () = assert!(MANIFEST_READ_COST_BYTES <= CONTROLLER_READ_BUDGET_BYTES);
    const _: () = assert!(MANIFEST_READ_COST_BYTES >= caps::CONTROLLER_MANIFEST);
    // A document read reserves its cap and its parse (the measured 37×).
    const _: () = assert!(DOCUMENT_READ_COST_BYTES >= 38 * caps::CONTROLLER_DOCUMENT);
}
