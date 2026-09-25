//! The `RecoveryCatalog` reconciler and the bounded Kubernetes view — D3 §5.3,
//! §5.4, PLAT-15.1's controller half.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST, A `mock_client` TEST OR A
//! SOURCE-READING TEST. Nothing dials a socket, nothing waits on a Job and
//! nothing runs `kubectl`. The double PANICS on a request it was not given a
//! route for, which is what makes a ZERO COUNT — no `DELETE` anywhere, no
//! `pods/log` read for a pod this Job does not own, no `POST` on a refused path
//! — mean "the reconciler did not ask" rather than "the table forgot a route".
//!
//! READ `the_two_axes_are_never_merged` AND
//! `a_403_on_part_of_the_archive_is_unreadable_and_never_missing` FIRST. They
//! are the two properties this whole module exists for: a point is offered for
//! restore only when it is readable AND verifies under trusted key material,
//! and "this credential could not tell" is never reported as "the archive does
//! not hold it".
//!
//! **The live proof is owed to W14.** D2's `catalogSync` plan kind is not in
//! the runner yet (see `the_catalog_sync_kind_is_not_yet_in_the_closed_vocabulary`),
//! so every sync here is exercised against the check framework's own fakes. The
//! docker-desktop scenario is D3 §15's and belongs to W14.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone as _, Utc};
use serde_json::{json, Value};

use logweir_core::check_contract::{
    frames, CheckPlanKind, FrameExpectations, Stream, CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT,
};
use weirkeeper::catalog_view as view;
use weirkeeper::catalog_view::{
    Availability, EntryLocation, RunnerCounts, RunnerEntry, RunnerSigner, SignatureCounts,
    SignatureVerdict, SyncTrigger, TrustKey, TrustKeyState, TrustView, Verification, ViewLimits,
    WindowStates,
};
use weirkeeper::check;
use weirkeeper::conditions::apply_merge_patch;
use weirkeeper::controllers::recovery_catalog as ctrl;
use weirkeeper::crds::recovery_catalog::{RecoveryCatalog, SignerSummary};
use weirkeeper::job::RunnerImage;
use weirkeeper::testing::{mock_client_recording_bodies, Recorder, Route, SeenBody};

// ===========================================================================
// Fixtures
// ===========================================================================

/// This task's namespace (STANDING RULE 13).
const NS: &str = "logweir-d3w8";
const NAME: &str = "primary";
const UID: &str = "c47a1f00-0000-4000-8000-0000000000c1";
const DEST: &str = "archive";
const DEST_UID: &str = "d0d0d0d0-0000-4000-8000-0000000000d1";
const JOB_UID: &str = "1b1b1b1b-0000-4000-8000-0000000000b7";

/// The key this installation's roster lists.
const TRUSTED_KEY: &str = "aa11bb22cc33dd44ee55ff6600778899aabbccddeeff00112233445566778899";
/// A key it does not — a fresh installation's, or another installation's.
const STRANGER_KEY: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 16, 12, 0, 0)
        .single()
        .expect("a real instant")
}

fn spki(marker: u8) -> String {
    format!(
        "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE{marker:02x}\n\
         -----END PUBLIC KEY-----\n"
    )
}

fn roster_body() -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustRoster",
        "metadata": {"name": "default", "uid": "r0", "generation": 2, "resourceVersion": "9"},
        "spec": {
            "approverKeys": [],
            "signingKeys": [{"keyId": TRUSTED_KEY, "spkiPem": spki(0xA1), "subject": "runner"}],
            "allowedClusterIds": []
        }
    })
    .to_string()
}

fn empty_roster_body() -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustRoster",
        "metadata": {"name": "default", "uid": "r0", "generation": 1, "resourceVersion": "9"},
        "spec": {"approverKeys": [], "signingKeys": [], "allowedClusterIds": []}
    })
    .to_string()
}

fn destination_body() -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {
            "name": DEST, "namespace": NS, "uid": DEST_UID,
            "generation": 1, "resourceVersion": "77"
        },
        "spec": {
            "storage": {
                "provider": "S3", "bucket": "lw-archive", "prefix": "team-a",
                "region": "us-east-1", "endpoint": "http://minio.storage.svc:9000",
                "addressing": "PathStyle"
            },
            "transport": {"security": "InsecureHTTP"},
            "access": {
                "archiveWrite": {"mode": "SecretKeys", "secret": {
                    "name": "lw-writer", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
                }},
                "archiveRead": {"mode": "SecretKeys", "secret": {
                    "name": "lw-reader", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
                }}
            }
        },
        "status": {
            "observedGeneration": 1, "reason": "Valid",
            "conditions": [{"type": "Valid", "status": "True", "reason": "Valid",
                            "observedGeneration": 1}]
        }
    })
    .to_string()
}

fn catalog_value(spec_extra: Value, status: Value) -> Value {
    let mut spec = json!({
        "destinationRef": {"name": DEST},
        "sync": {
            "intervalSeconds": 3600, "mode": "Index", "maxObjectsPerRun": 100000,
            "deepCheck": "ManifestDigest", "viewLimit": 2000
        }
    });
    if let Some(extra) = spec_extra.as_object() {
        for (k, v) in extra {
            spec[k] = v.clone();
        }
    }
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
        // EVERY OBJECT FROM A WATCH CARRIES A `resourceVersion`, and the
        // `/status` write uses it as its compare-and-set precondition (seam S7),
        // so a fixture without one is not a fixture of anything this reconciler
        // ever sees.
        "metadata": {
            "name": NAME, "namespace": NS, "uid": UID,
            "generation": 1, "resourceVersion": "4242"
        },
        "spec": spec,
        "status": status
    })
}

fn catalog(spec_extra: Value, status: Value) -> RecoveryCatalog {
    serde_json::from_value(catalog_value(spec_extra, status))
        .expect("the fixture is a RecoveryCatalog")
}

/// The stem of the PREVIOUS slot's sync — a Job that has aged out while this
/// pass's trigger names a new one.
fn previous_stem() -> String {
    let slot = view::periodic_slot(now(), 3600).expect("an hourly catalog has slots");
    view::sync_stem(UID, &SyncTrigger::Periodic(slot - 1).token())
}

/// The Job name this catalog's periodic sync computes at [`now`].
fn periodic_stem() -> String {
    let slot = view::periodic_slot(now(), 3600).expect("an hourly catalog has slots");
    view::sync_stem(UID, &SyncTrigger::Periodic(slot).token())
}

fn job_body(name: &str, finished: bool, plan_sha: &str, owner_uid: &str) -> String {
    let status = if finished {
        json!({
            "startTime": "2026-09-16T11:50:00Z",
            "completionTime": "2026-09-16T11:55:00Z",
            "conditions": [{"type": "Complete", "status": "True"}]
        })
    } else {
        json!({"startTime": "2026-09-16T11:59:00Z", "active": 1})
    };
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": name, "namespace": NS, "uid": JOB_UID, "resourceVersion": "555",
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
                "name": NAME, "uid": owner_uid, "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {"template": {"spec": {"containers": [{
            "name": "runner",
            "env": [{"name": check::job::PLAN_SHA256_ENV, "value": plan_sha}]
        }]}}},
        "status": status
    })
    .to_string()
}

fn pod_list_body(owner_uid: &str) -> String {
    json!({
        "apiVersion": "v1", "kind": "PodList", "metadata": {},
        "items": [{
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": "sync-pod-abcde", "namespace": NS, "uid": "p1",
                "ownerReferences": [{
                    "apiVersion": "batch/v1", "kind": "Job", "name": "job",
                    "uid": owner_uid, "controller": true, "blockOwnerDeletion": true
                }]
            },
            "status": {"phase": "Succeeded", "containerStatuses": [{
                "name": "runner", "ready": false, "restartCount": 0, "image": "i",
                "imageID": "i",
                "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
            }]}
        }]
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Result-body builders — the `catalogSync` wire format this controller reads
// ---------------------------------------------------------------------------

fn entry_value(point: &str, at_ms: i64, availability: &str, signature: &str, key: &str) -> Value {
    json!({
        "pointId": point,
        "backupId": "sched-1-20260916-030000",
        "runId": "run-1",
        "recoveryPointAtMs": at_ms,
        "coveredFromMs": at_ms - 3_600_000,
        "coveredToMs": at_ms,
        "locations": [{"locationId": "s3://lw-archive/team-a"}],
        "receiptKey": format!("logweir/backups/sched-1/{point}.receipt.json"),
        "receiptSha256": format!("sha256:{}", "0".repeat(64)),
        "manifestKey": "logweir/backups/sched-1/manifest.json",
        "manifestSha256": format!("sha256:{}", "1".repeat(64)),
        "availability": availability,
        "signature": signature,
        "signerKeyId": key
    })
}

fn ok_entry(point: &str, at_ms: i64) -> Value {
    entry_value(point, at_ms, "Available", "verified", TRUSTED_KEY)
}

/// One `catalog-page=` header plus its `catalog-entry=` lines, with the digest
/// computed the way the controller recomputes it.
fn page_block(index: u32, of: u32, entries: &[Value]) -> String {
    let bodies: Vec<String> = entries
        .iter()
        .map(|e| serde_json::to_string(e).expect("an entry serialises"))
        .collect();
    let refs: Vec<&str> = bodies.iter().map(String::as_str).collect();
    let mut out = format!(
        "{}{index}/{of} count={} sha256={}\n",
        view::PAGE_LINE_PREFIX,
        entries.len(),
        view::page_digest(&refs)
    );
    for body in &bodies {
        out.push_str(view::ENTRY_LINE_PREFIX);
        out.push_str(body);
        out.push('\n');
    }
    out
}

fn counts_value(total: i64, available: i64) -> Value {
    json!({
        "total": total, "available": available, "missing": 0, "unreadable": 0,
        "deleted": 0, "conflict": 0, "unsupportedFormat": 0, "partial": 0,
        "signature": {"verified": available, "invalid": 0, "noEvidence": 0, "notAttempted": 0},
        "byDay": [{"day": "2026-09-16", "points": available}]
    })
}

fn body_for(pages: &[Vec<Value>], counts: Value, signers: Value, complete: bool) -> String {
    let mut out = format!(
        "{}{}\n",
        view::FORMAT_LINE_PREFIX,
        view::BODY_FORMAT_VERSION
    );
    let of = u32::try_from(pages.len()).expect("a small page count");
    for (i, entries) in pages.iter().enumerate() {
        out.push_str(&page_block(
            u32::try_from(i + 1).expect("small"),
            of,
            entries,
        ));
    }
    out.push_str(&format!("{}{counts}\n", view::COUNTS_LINE_PREFIX));
    out.push_str(&format!(
        "{}{}\n",
        view::CURSOR_LINE_PREFIX,
        json!({"indexShard": "2026/09/16", "complete": complete})
    ));
    out.push_str(&format!("{}{signers}\n", view::SIGNERS_LINE_PREFIX));
    out
}

/// The default happy body: one page, two available points, one trusted signer.
fn happy_body() -> String {
    body_for(
        &[vec![
            ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1_758_000_000_000),
            ok_entry("lwp1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1_757_900_000_000),
        ]],
        counts_value(2, 2),
        json!([{"keyId": TRUSTED_KEY, "points": 2, "principalHint": "runner"}]),
        true,
    )
}

/// D2's frames around a result body, as the runner would print them.
fn framed(plan_sha: &str, subject_uid: &str, body: &str) -> String {
    let payload = body.as_bytes().to_vec();
    let parts = frames::write_parts(Stream::Details, &payload).expect("parts fit the frame bound");
    let mut streams: BTreeMap<Stream, (Vec<u8>, usize)> = BTreeMap::new();
    streams.insert(Stream::Details, (payload, parts.len()));
    let end = frames::end_frame(plan_sha, subject_uid, &streams, None);
    let mut out = parts.join("\n");
    out.push('\n');
    out.push_str(&frames::write_end(&end).expect("an end frame"));
    out.push('\n');
    out
}

// ---------------------------------------------------------------------------
// The route table
// ---------------------------------------------------------------------------

struct Fixture {
    client: kube::Client,
    recorder: Recorder,
    bodies: std::sync::Arc<std::sync::Mutex<Vec<SeenBody>>>,
}

impl Fixture {
    fn seen(&self) -> Vec<(String, String)> {
        self.recorder
            .lock()
            .expect("the recorder")
            .iter()
            .map(|r| (r.method.clone(), r.uri.clone()))
            .collect()
    }

    fn patched_status(&self) -> Value {
        let bodies = self.bodies.lock().expect("the body recorder");
        let patch = bodies
            .iter()
            .find(|b| b.method == "PATCH" && b.uri.contains("/recoverycatalogs/"))
            .unwrap_or_else(|| panic!("no /status PATCH was sent; requests: {:?}", self.seen()));
        serde_json::from_str(&patch.body).expect("the patch body is JSON")
    }

    fn status_patches(&self) -> Vec<Value> {
        self.bodies
            .lock()
            .expect("the body recorder")
            .iter()
            .filter(|b| b.method == "PATCH" && b.uri.contains("/recoverycatalogs/"))
            .map(|b| serde_json::from_str(&b.body).expect("JSON"))
            .collect()
    }

    fn posted(&self, fragment: &str) -> Vec<Value> {
        self.bodies
            .lock()
            .expect("the body recorder")
            .iter()
            .filter(|b| b.method == "POST" && b.uri.contains(fragment))
            .map(|b| serde_json::from_str(&b.body).expect("JSON"))
            .collect()
    }
}

fn fixture(routes: Vec<Route>) -> Fixture {
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    Fixture {
        client,
        recorder,
        bodies,
    }
}

/// An empty `TrustPolicy` list: no policy governs the namespace, so the
/// catalog resolves the synthesised `legacy-roster-v1` from the roster route.
fn no_trust_policies() -> String {
    json!({"apiVersion": "logweir.dev/v1alpha1", "kind": "TrustPolicyList", "metadata": {"resourceVersion": "1"}, "items": []}).to_string()
}

fn route(method: &'static str, path_suffix: &'static str, body: String) -> Route {
    Route {
        method,
        path_suffix,
        status: 200,
        body,
    }
}

async fn run(fixture: &Fixture, catalog: &RecoveryCatalog) -> ctrl::Outcome {
    run_at(fixture, catalog, now()).await
}

async fn run_at(fixture: &Fixture, catalog: &RecoveryCatalog, at: DateTime<Utc>) -> ctrl::Outcome {
    let policy = check::policy::Policy::defaults();
    let image = RunnerImage::default();
    ctrl::reconcile_catalog(
        catalog,
        &ctrl::SyncContext {
            client: &fixture.client,
            policy: &policy,
            runner_image: &image,
            now: at,
            trust_policies: None,
            peers: Some(&[]),
        },
    )
    .await
    .expect("the reconcile reaches a verdict")
}

/// The table for a pass that HARVESTS a finished sync Job.
fn harvest_routes(plan_sha: &str, log: String, job_owner_uid: &str) -> Vec<Route> {
    let stem: &'static str = Box::leak(periodic_stem().into_boxed_str());
    let job_path: &'static str = Box::leak(format!("/jobs/{stem}").into_boxed_str());
    vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route(
            "GET",
            job_path,
            job_body(stem, true, plan_sha, job_owner_uid),
        ),
        route("GET", "/pods", pod_list_body(JOB_UID)),
        route("GET", "/log", log),
        route("POST", "/configmaps", empty_config_map()),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]
}

/// A minimal but WELL-FORMED answer: `kube` deserialises every response into
/// the typed object, so a `{}` body fails as a transport error and would hide
/// whatever the reconciler actually did.
fn empty_config_map() -> String {
    json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {}}).to_string()
}

/// What the API server answers a `POST /jobs` with: the object it created,
/// carrying the UID the plan `ConfigMap`'s ownerReference needs (finding F1).
fn empty_job() -> String {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": periodic_stem(), "namespace": NS, "uid": JOB_UID,
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
                "name": NAME, "uid": UID, "controller": true,
                "blockOwnerDeletion": true
            }]
        }
    })
    .to_string()
}

fn patched_catalog() -> String {
    catalog_value(json!({}), json!({})).to_string()
}

/// A status that says "a sync Job is tracked and has not been harvested yet".
fn tracked_status(stem: &str) -> Value {
    json!({
        "observedGeneration": 1,
        "lastSyncJob": {"name": stem},
        "conditions": [{"type": "Synced", "status": "Unknown", "reason": "SyncInProgress"}]
    })
}

// ===========================================================================
// 1. The seam D2 has not landed yet
// ===========================================================================

/// **THE ALARM FIRED AND THE SEAM IS CLOSED.**
///
/// This was `the_catalog_sync_kind_is_not_yet_in_the_closed_vocabulary`: while
/// `CheckPlanKind` was a closed FIVE-member vocabulary it asserted that
/// `catalogSync` was NOT in it, so the day D2 landed the kind the suite would
/// fail and say what to do about it. D2 has landed it, so the assertion is
/// INVERTED rather than deleted — a guard that only ever fires once is a guard
/// that stops guarding, and what matters from here on is that the controller's
/// two constants are the enum's own strings and not a second spelling of them.
#[test]
fn the_catalog_sync_kind_is_the_closed_vocabularys_own() {
    assert!(
        CheckPlanKind::ALL
            .iter()
            .any(|k| k.as_str() == view::PLAN_KIND),
        "`{}` is not a CheckPlanKind; the controller is naming a kind the one runner does not \
         dispatch and every sync Job would exit 3 with CheckContractMismatch",
        view::PLAN_KIND
    );
    assert_eq!(view::PLAN_KIND, CheckPlanKind::CatalogSync.as_str());
    assert_eq!(
        view::JOB_DISCRIMINATOR,
        CheckPlanKind::CatalogSync.job_discriminator()
    );
}

/// The Job-name discriminator collides with none of the other five: two kinds
/// sharing one would give two checks of one subject the same Job name.
#[test]
fn the_job_discriminator_collides_with_no_landed_kind() {
    for kind in CheckPlanKind::ALL {
        if kind == CheckPlanKind::CatalogSync {
            continue;
        }
        assert_ne!(
            kind.job_discriminator(),
            view::JOB_DISCRIMINATOR,
            "`{}` already uses the discriminator `{}`",
            kind.as_str(),
            view::JOB_DISCRIMINATOR
        );
    }
}

/// The plan document carries D2's own field names, so the runner's `CheckPlan`
/// deserialises it.
#[test]
fn the_plan_document_carries_d2s_field_names() {
    let request = sync_request();
    let bytes = view::plan_document(UID, 900, Some("sha256:deadbeef"), &request)
        .expect("the plan serialises");
    let doc: Value = serde_json::from_slice(&bytes).expect("JSON");
    assert_eq!(doc["contract"], CHECK_PLAN_CONTRACT);
    assert_eq!(doc["contractVersion"], CHECK_CONTRACT_VERSION);
    assert_eq!(doc["subjectUid"], UID);
    assert_eq!(doc["timeoutSeconds"], 900);
    assert_eq!(doc["policyDigest"], "sha256:deadbeef");
    assert!(
        doc["request"][view::PLAN_KIND].is_object(),
        "the request is externally tagged by the plan kind: {doc}"
    );
    // The credential never travels in the plan: only the MODE does.
    let rendered = doc.to_string();
    assert!(
        !rendered.contains("AWS_SECRET_ACCESS_KEY") && !rendered.contains("secret-access-key"),
        "a plan document carries no credential value or key name: {rendered}"
    );
}

fn sync_request() -> view::CatalogSyncRequest {
    view::CatalogSyncRequest {
        destination: logweir_core::check_contract::DestinationPlan {
            name: DEST.to_string(),
            uid: DEST_UID.to_string(),
            location: logweir_core::destination::DestinationLocation {
                provider: logweir_core::destination::StorageProvider::S3,
                bucket: "lw-archive".to_string(),
                prefix: "team-a".to_string(),
                region: Some("us-east-1".to_string()),
                endpoint: Some("http://minio.storage.svc:9000".to_string()),
                addressing: logweir_core::destination::Addressing::PathStyle,
                transport: logweir_core::destination::TransportSecurity::InsecureHttp,
            },
            location_digest: format!("sha256:{}", "2".repeat(64)),
            ca_file: None,
            credentials: logweir_core::check_contract::CredentialMode::Static,
        },
        mode: weirkeeper::crds::recovery_catalog::SyncMode::Index.into(),
        deep_check: weirkeeper::crds::recovery_catalog::DeepCheck::ManifestDigest.into(),
        max_objects_per_run: 100_000,
        view_limit: 2000,
        index_shard: None,
        rescan_start_after: None,
        trust_bundle_file: Some("/check/trust/trust-bundle.pem".to_string()),
    }
}

// ===========================================================================
// 2. The two axes
// ===========================================================================

/// **THE PROPERTY THIS MODULE EXISTS FOR.** Availability and verification are
/// separate axes and a point is offered only when BOTH allow it. Every one of
/// the 49 combinations is stated, so a future "healthy" boolean over the two
/// cannot quietly widen the set.
#[test]
fn the_two_axes_are_never_merged() {
    let mut offered = Vec::new();
    for a in Availability::ALL {
        for v in Verification::ALL {
            if view::selectable(*a, *v) {
                offered.push((a.as_str(), v.as_str()));
            }
        }
    }
    assert_eq!(
        offered,
        vec![
            ("Available", "Verified"),
            ("Available", "VerifiedHistorical"),
        ],
        "D3 §5.4: ordinary restore selection requires `Available` AND (`Verified` | \
         `VerifiedHistorical`). Everything else is listed with its exact state and a remedy \
         sentence; nothing unverified is presented as verified evidence."
    );
    assert_eq!(Availability::ALL.len() * Verification::ALL.len(), 49);
}

/// A signature verdict that did not verify is `Invalid` whatever the trust
/// source says, and trust is asked about SECOND.
#[test]
fn trust_cannot_rescue_a_signature_that_did_not_verify() {
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
    assert_eq!(
        view::classify_verification(
            SignatureVerdict::Invalid,
            Some(TRUSTED_KEY),
            Some(now()),
            &trust,
            now()
        ),
        Verification::Invalid
    );
}

/// A verified signature under a key this installation does not list is
/// `UntrustedSigner` — never upgraded by proximity (`docs/keys.md`).
#[test]
fn a_verified_signature_under_an_unlisted_key_is_untrusted_and_never_verified() {
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
    assert_eq!(
        view::classify_verification(
            SignatureVerdict::Verified,
            Some(STRANGER_KEY),
            Some(now()),
            &trust,
            now()
        ),
        Verification::UntrustedSigner,
        "a fresh installation reading somebody else's archive must say so"
    );
    assert!(!view::selectable(
        Availability::Available,
        Verification::UntrustedSigner
    ));
}

/// With no trust material at all, nothing is `Invalid` and nothing is
/// `UntrustedSigner`: an installation that holds no key has not disproved
/// anything.
#[test]
fn no_trust_material_is_not_attempted_and_not_a_refutation() {
    let trust = TrustView::default();
    assert_eq!(
        view::classify_verification(
            SignatureVerdict::Verified,
            Some(TRUSTED_KEY),
            Some(now()),
            &trust,
            now()
        ),
        Verification::NotAttempted
    );
}

/// **The `TrustPolicy` seam, tested before it exists.** A retired key verifies
/// what it signed while it was valid, and nothing newer.
#[test]
fn a_retired_key_verifies_evidence_it_signed_while_valid() {
    let expiry = now() - chrono::Duration::days(10);
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, Some(expiry));
    let before = expiry - chrono::Duration::days(1);
    let after = expiry + chrono::Duration::days(1);
    assert_eq!(
        view::classify_verification(
            SignatureVerdict::Verified,
            Some(TRUSTED_KEY),
            Some(before),
            &trust,
            now()
        ),
        Verification::VerifiedHistorical,
        "D3 §7.4: a rotation must not make every archive the old key signed unverifiable"
    );
    assert_eq!(
        view::classify_verification(
            SignatureVerdict::Verified,
            Some(TRUSTED_KEY),
            Some(after),
            &trust,
            now()
        ),
        Verification::Invalid,
        "signed after the key stopped being accepted: there is nothing to place it inside the \
         validity window"
    );
    // And a revoked key is neither.
    let revoked = trust_with(TRUSTED_KEY, TrustKeyState::Revoked, None);
    assert_eq!(
        view::classify_verification(
            SignatureVerdict::Verified,
            Some(TRUSTED_KEY),
            Some(before),
            &revoked,
            now()
        ),
        Verification::Revoked
    );
}

fn trust_with(key_id: &str, state: TrustKeyState, not_after: Option<DateTime<Utc>>) -> TrustView {
    TrustView {
        keys: vec![TrustKey {
            key_id: key_id.to_string(),
            spki_pem: spki(0xA1),
            subject: Some("runner".to_string()),
            lifecycle: None,
            not_after,
            state,
        }],
        source: view::TRUST_SOURCE_ROSTER.to_string(),
        unresolved: None,
    }
}

/// The roster is projected as the trust source in use, signing keys only.
#[test]
fn the_roster_projects_its_signing_keys_and_not_its_approver_keys() {
    let spec: weirkeeper::crds::trust_roster::TrustRosterSpec = serde_json::from_value(json!({
        "approverKeys": [{"keyId": STRANGER_KEY, "spkiPem": spki(0xB2)}],
        "signingKeys": [{"keyId": TRUSTED_KEY, "spkiPem": spki(0xA1)}],
        "allowedClusterIds": []
    }))
    .expect("a roster spec");
    let trust = TrustView::from_roster(&spec);
    assert_eq!(trust.keys.len(), 1);
    assert!(trust.key(TRUSTED_KEY).is_some());
    assert!(
        trust.key(STRANGER_KEY).is_none(),
        "D3 §7.3: a key that may AUTHORISE a restore is not thereby a key that may ATTEST to one"
    );
}

// ===========================================================================
// 3. The result-body grammar
// ===========================================================================

/// A body fragment with the grammar version line the parser requires.
fn versioned(body: &str) -> String {
    format!(
        "{}{}\n{body}",
        view::FORMAT_LINE_PREFIX,
        view::BODY_FORMAT_VERSION
    )
}

#[test]
fn a_page_whose_digest_does_not_match_is_refused_and_no_page_is_written_from_it() {
    let entries = vec![ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)];
    let good = page_block(1, 1, &entries);
    let tampered = good.replace(
        "\"availability\":\"Available\"",
        "\"availability\":\"Deleted\"",
    );
    assert_ne!(good, tampered, "the fixture really was changed");
    match view::parse_body(&versioned(&tampered), 5000) {
        Err(view::BodyError::PageDigestMismatch { index }) => assert_eq!(index, 1),
        other => panic!("a tampered page must be refused, got {other:?}"),
    }
    // The control: the untouched page parses.
    assert_eq!(
        view::parse_body(&versioned(&good), 5000)
            .expect("the untouched page parses")
            .pages[0]
            .entries
            .len(),
        1
    );
}

#[test]
fn a_malformed_entry_is_skipped_and_counted_and_never_fatal() {
    let good = ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5);
    let good_body = serde_json::to_string(&good).expect("json");
    let broken = r#"{"pointId":"lwp1-zzzz","availability":"NotAThing"}"#;
    let lines = vec![good_body.as_str(), broken];
    let mut body = format!(
        "{}1/1 count=2 sha256={}\n",
        view::PAGE_LINE_PREFIX,
        view::page_digest(&lines)
    );
    for line in &lines {
        body.push_str(view::ENTRY_LINE_PREFIX);
        body.push_str(line);
        body.push('\n');
    }
    let parsed =
        view::parse_body(&versioned(&body), 5000).expect("one broken entry is not a broken sync");
    assert_eq!(parsed.pages[0].entries.len(), 1);
    assert_eq!(parsed.skipped_entries, 1);
}

#[test]
fn page_headers_must_be_one_to_n_in_order_each_exactly_once() {
    let e = vec![ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)];
    let body = format!("{}{}", page_block(2, 2, &e), page_block(1, 2, &e));
    assert_eq!(
        view::parse_body(&versioned(&body), 5000),
        Err(view::BodyError::PageSequence)
    );
}

#[test]
fn an_entry_line_before_any_page_header_is_refused() {
    let body = format!(
        "{}{}\n",
        view::ENTRY_LINE_PREFIX,
        ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)
    );
    assert_eq!(
        view::parse_body(&versioned(&body), 5000),
        Err(view::BodyError::EntryBeforePage)
    );
}

#[test]
fn every_body_error_is_d2s_one_closed_code() {
    for e in [
        view::BodyError::MalformedPageHeader,
        view::BodyError::EntryBeforePage,
        view::BodyError::PageDigestMismatch { index: 1 },
        view::BodyError::PageSequence,
        view::BodyError::TooManyEntries { allowed: 5000 },
        view::BodyError::TooLarge { got: 9 },
        view::BodyError::MissingFormat,
        view::BodyError::UnsupportedBodyFormat {
            got: "2".to_string(),
        },
        view::BodyError::RepeatedSummary(view::COUNTS_LINE_PREFIX),
        view::BodyError::MalformedSummary(view::SIGNERS_LINE_PREFIX),
        view::BodyError::NotUtf8,
    ] {
        assert_eq!(
            e.code(),
            logweir_core::check_contract::CheckCode::ResultUnreadable,
            "D-SEAMS S1: failures use D2's closed error-code vocabulary and never a new string"
        );
        assert!(!e.to_string().is_empty());
    }
}

/// Lines the body does not own are ignored, exactly as D2's own frame decoder
/// ignores them — a runner's stderr shares the stream.
#[test]
fn unrelated_lines_are_ignored() {
    let e = vec![ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)];
    let body = format!("noise\n{}{{\"level\":\"warn\"}}\n", page_block(1, 1, &e));
    assert_eq!(
        view::parse_body(&versioned(&body), 5000)
            .expect("noise does not break a body")
            .pages[0]
            .entries
            .len(),
        1
    );
}

// ===========================================================================
// 4. Duplicate identity — D3 §5.1, defect RECEIPT-DUP
// ===========================================================================

/// **Defect RECEIPT-DUP's answer, asserted.** Two receipts under ONE
/// `backupId` have different content-derived point ids and are TWO points; the
/// SAME receipt found in two buckets is ONE point in two places.
#[test]
fn two_receipts_under_one_backup_id_are_two_points_and_one_copied_archive_is_one() {
    let a = entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 20, "s3://one/p");
    let mut b = entry("lwp1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 10, "s3://one/p");
    b.run_id = "run-2".to_string();
    assert_eq!(a.backup_id, b.backup_id, "one archive set");
    let merged = view::merge_entries(vec![a.clone(), b.clone()]);
    assert_eq!(merged.len(), 2, "two receipts, two recovery points");

    let copy = entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 20, "s3://two/p");
    let merged = view::merge_entries(vec![a, copy]);
    assert_eq!(merged.len(), 1, "one receipt, one point");
    assert_eq!(
        location_ids(&merged[0]),
        vec!["s3://one/p", "s3://two/p"],
        "an archive copied to a second bucket is ONE point with TWO locations (D3 §5.1)"
    );
    assert_eq!(merged[0].availability, Availability::Available);
}

/// Two records of one identity that disagree about a RECEIPT-DERIVED fact are
/// `Conflict` — D3 §5.2 rule 3's `RecordMismatch`. Neither is preferred.
#[test]
fn a_record_mismatch_is_a_conflict_and_an_informational_difference_is_not() {
    let a = entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 20, "s3://one/p");

    let mut mismatch = a.clone();
    mismatch.manifest_sha256 = Some(format!("sha256:{}", "9".repeat(64)));
    let merged = view::merge_entries(vec![a.clone(), mismatch]);
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged[0].availability,
        Availability::Conflict,
        "D3 §5.4: two records disagreeing for one identity is `Conflict`, not a silent pick"
    );
    assert!(!view::selectable(
        merged[0].availability,
        Verification::Verified
    ));

    // The control: an INFORMATIONAL difference is one point in two places.
    let mut informational = a.clone();
    informational.locations = vec![at("s3://two/p", None)];
    informational.recorded_at = Some(now());
    informational.remedy = Some("ignored".to_string());
    let merged = view::merge_entries(vec![a, informational]);
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged[0].availability,
        Availability::Available,
        "D3 §5.2 rule 3: everything but the receipt-derived facts is informational"
    );
}

/// **Review finding F9 — availability merges BEST-of.** A point present in one
/// bucket and absent from another is still fully recoverable from the first;
/// hiding it because a second copy went missing is the opposite of what a
/// second copy is for. Asserted in BOTH arrival orders, because a merge whose
/// answer depends on which observation arrived first is not a merge.
#[test]
fn availability_merges_best_of_across_locations_in_either_order() {
    let good = entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 20, "s3://one/p");
    let mut lost = good.clone();
    lost.availability = Availability::Missing;
    lost.locations = vec![at("s3://two/p", None)];

    for (label, order) in [
        ("good first", vec![good.clone(), lost.clone()]),
        ("lost first", vec![lost.clone(), good.clone()]),
    ] {
        let merged = view::merge_entries(order);
        assert_eq!(merged.len(), 1, "{label}: one receipt, one point");
        assert_eq!(
            merged[0].availability,
            Availability::Available,
            "{label}: D3 §5.1's copied archive is ONE point in TWO places, and it is still \
             recoverable from the place that has it"
        );
        assert_eq!(location_ids(&merged[0]), vec!["s3://one/p", "s3://two/p"]);
        // Each place keeps its OWN verdict, so nobody has to guess.
        let by_id: Vec<(&str, Availability)> = merged[0]
            .locations
            .iter()
            .map(|l| {
                (
                    l.location_id.as_str(),
                    l.availability_or(merged[0].availability),
                )
            })
            .collect();
        assert_eq!(
            by_id,
            vec![
                ("s3://one/p", Availability::Available),
                ("s3://two/p", Availability::Missing),
            ],
            "{label}"
        );
        // And the degraded one is NAMED, so a best-of merge does not silently
        // drop the fact that a copy needs repairing.
        let remedy = merged[0].remedy.clone().unwrap_or_default();
        assert!(
            remedy.contains("s3://two/p") && remedy.contains("Missing"),
            "{label}: the remedy names the broken copy: {remedy:?}"
        );
        assert!(
            !remedy.contains("AWS_") && !remedy.contains("minio.storage.svc"),
            "{label}: a location id is a bucket and a prefix, never an endpoint or a credential"
        );
    }
}

/// **Review finding F5 — the SIGNATURE half merges worst-of**, with the key id
/// following the worse verdict, in both arrival orders.
///
/// The reviewer's mutant (`worse_signature` returns `false`) survived all 54
/// rows before this one existed: bytes that verify in bucket A and fail in
/// bucket B would have been published `selectable: true` whenever the good
/// observation happened to arrive first.
#[test]
fn the_signature_merges_worst_of_with_its_key_id_in_either_order() {
    let mut good = entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 20, "s3://one/p");
    good.signature = SignatureVerdict::Verified;
    good.signer_key_id = Some(TRUSTED_KEY.to_string());
    let mut corrupt = good.clone();
    corrupt.locations = vec![at("s3://two/p", None)];
    corrupt.signature = SignatureVerdict::Invalid;
    corrupt.signer_key_id = Some(STRANGER_KEY.to_string());

    for (label, order) in [
        ("good first", vec![good.clone(), corrupt.clone()]),
        ("corrupt first", vec![corrupt.clone(), good.clone()]),
    ] {
        let merged = view::merge_entries(order);
        assert_eq!(merged.len(), 1, "{label}");
        assert_eq!(
            merged[0].signature,
            SignatureVerdict::Invalid,
            "{label}: bytes that fail verification in one place are evidence about the POINT, \
             not about the place"
        );
        assert_eq!(
            merged[0].signer_key_id.as_deref(),
            Some(STRANGER_KEY),
            "{label}: the key id follows the verdict it belongs to"
        );
        let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
        let view_row = view::view_entry(merged[0].clone(), &trust, now());
        assert_eq!(view_row.verification, Verification::Invalid, "{label}");
        assert!(!view_row.selectable, "{label}: and it is never offered");
    }
}

/// One PLACE reported twice keeps the worse of the two: two attempts at one
/// bucket are two attempts at one thing, and "it worked once" is not a property
/// of the bucket.
#[test]
fn one_location_observed_twice_keeps_its_worse_verdict() {
    let good = entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 20, "s3://one/p");
    let mut flaky = good.clone();
    flaky.availability = Availability::Unreadable;
    let merged = view::merge_entries(vec![good, flaky]);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].locations.len(), 1);
    assert_eq!(
        merged[0].locations[0].availability,
        Some(Availability::Unreadable)
    );
    assert_eq!(merged[0].availability, Availability::Unreadable);
}

/// The view order is total and reproducible: newest recovery point first, point
/// id as the tie-break.
#[test]
fn the_view_is_newest_first_with_a_total_order() {
    let a = entry("lwp1-cccccccccccccccccccccccccccccccc", 10, "s3://one/p");
    let b = entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 10, "s3://one/p");
    let c = entry("lwp1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 99, "s3://one/p");
    let merged = view::merge_entries(vec![a, b, c]);
    let ids: Vec<&str> = merged.iter().map(|e| e.point_id.as_str()).collect();
    assert_eq!(
        ids,
        vec![
            "lwp1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "lwp1-cccccccccccccccccccccccccccccccc",
        ]
    );
}

fn entry(point: &str, at_ms: i64, location: &str) -> RunnerEntry {
    let mut e: RunnerEntry =
        serde_json::from_value(ok_entry(point, at_ms)).expect("an entry parses");
    e.locations = vec![at(location, None)];
    e
}

/// One location, with its own verdict or inheriting the entry's.
fn at(location_id: &str, availability: Option<Availability>) -> EntryLocation {
    EntryLocation {
        location_id: location_id.to_string(),
        availability,
    }
}

/// The location ids of a merged entry, in order.
fn location_ids(e: &RunnerEntry) -> Vec<&str> {
    e.locations.iter().map(|l| l.location_id.as_str()).collect()
}

// ===========================================================================
// 5. Bounds — the large catalog
// ===========================================================================

/// **The "large catalog" row.** 5 001 points page into two `ConfigMap`s, the
/// newest `viewLimit` are materialised, `truncated` is true and the counts stay
/// exact.
#[test]
fn a_catalog_larger_than_the_view_limit_pages_and_says_so() {
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
    let entries: Vec<RunnerEntry> = (0..5001)
        .map(|i| {
            entry(
                &format!("lwp1-{i:032x}"),
                1_000_000 + i64::from(i),
                "s3://b/p",
            )
        })
        .collect();
    let limits = ViewLimits {
        view_limit: view::MAX_VIEW_ENTRIES,
        page_max_bytes: view::PAGE_MAX_BYTES,
        max_pages: view::MAX_PAGES,
    };
    let built = view::materialise(entries, 5001, &trust, &limits, now());
    assert_eq!(built.entries, 5000, "the newest viewLimit points");
    assert!(
        built.pages.len() <= view::MAX_PAGES,
        "the CRD's `status.pages` maxItems is {}; got {} pages",
        view::MAX_PAGES,
        built.pages.len()
    );
    // D3 §5.3 ESTIMATES ~350 B an entry and therefore two pages. The real
    // entry is larger — two `sha256:` digests are 71 characters each and a key
    // id is 64 — so the bound that matters is the one asserted here: every page
    // is inside the byte budget and the page count is inside the CRD's
    // `maxItems`. The estimate is recorded as an estimate rather than made true
    // by trimming the binding out of the view.
    let per_entry = built.pages[0].body.len() / built.pages[0].entries.len();
    assert!(
        (350..=1200).contains(&per_entry),
        "one view entry is {per_entry} bytes; D3 §5.3 estimates ~350 and the page arithmetic \
         above assumes under 1200"
    );
    assert!(built.truncated, "5001 > 5000");
    assert_eq!(built.dropped_for_space, 0);
    for page in &built.pages {
        assert!(
            page.body.len() <= view::PAGE_MAX_BYTES,
            "a page over the byte budget is rejected at CREATE with a message about etcd"
        );
    }
    // The newest point is first on page 0 and the oldest is last on the last.
    assert_eq!(built.pages[0].entries[0].recovery_point_at_ms, 1_005_000);
    let last = built.pages.last().expect("a page");
    assert_eq!(
        last.entries.last().expect("an entry").recovery_point_at_ms,
        1_000_001,
        "the OLDEST of the window, not of the archive"
    );
}

/// The `viewLimit` is clamped to what this build supports, whatever the spec
/// says.
#[test]
fn the_view_limit_is_clamped_to_five_thousand() {
    let sync = weirkeeper::crds::recovery_catalog::SyncSettings {
        interval_seconds: 3600,
        mode: weirkeeper::crds::recovery_catalog::SyncMode::Index,
        max_objects_per_run: 100_000,
        deep_check: weirkeeper::crds::recovery_catalog::DeepCheck::ManifestDigest,
        view_limit: 99_999,
    };
    assert_eq!(
        ViewLimits::from_settings(&sync).view_limit,
        view::MAX_VIEW_ENTRIES
    );
}

/// The fence pointer EXCLUDES pages and never proves one holds anything, and it
/// is tight on the axis the view is ordered by.
#[test]
fn the_fence_pointer_bounds_both_axes() {
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
    let entries: Vec<RunnerEntry> = (0..40)
        .map(|i| entry(&format!("lwp1-{i:032x}"), 1_000 + i64::from(i), "s3://b/p"))
        .collect();
    let limits = ViewLimits {
        view_limit: 100,
        page_max_bytes: 2000,
        max_pages: view::MAX_PAGES,
    };
    let built = view::materialise(entries, 40, &trust, &limits, now());
    assert!(built.pages.len() > 1, "the small budget forces paging");
    let names: Vec<String> = (0..built.pages.len())
        .map(|i| view::page_config_map_name("stem", i))
        .collect();
    let document = view::index_document("stem", &limits, &built, &names);
    assert_eq!(document.pages.len(), built.pages.len());
    for (i, fence) in document.pages.iter().enumerate() {
        assert_eq!(fence.index, i as i64);
        assert_eq!(fence.config_map_name, names[i]);
        assert!(fence.min_point_id <= fence.max_point_id);
        assert!(
            fence.newest_ms >= fence.oldest_ms,
            "the view is ordered newest first"
        );
        let page = &built.pages[i];
        for e in &page.entries {
            assert!(
                e.point_id >= fence.min_point_id && e.point_id <= fence.max_point_id,
                "no false negatives: every id in the page is inside the fence"
            );
        }
    }
    // Consecutive pages do not overlap on the ordered axis.
    for w in document.pages.windows(2) {
        assert!(w[0].oldest_ms >= w[1].newest_ms);
    }
}

// ===========================================================================
// 6. The ten counters
// ===========================================================================

/// `untrustedSigner` is computed from the SIGNER SUMMARY, which covers the whole
/// walk, and `unverified` folds `notAttempted` and `noEvidence`. The states the
/// ten fields cannot carry are returned rather than dropped.
#[test]
fn the_counters_are_a_projection_and_the_residue_is_named() {
    let counts = RunnerCounts {
        total: 100,
        available: 80,
        missing: 5,
        unreadable: 4,
        deleted: 3,
        conflict: 2,
        unsupported_format: 1,
        partial: 5,
        signature: SignatureCounts {
            verified: 90,
            invalid: 4,
            no_evidence: 3,
            not_attempted: 3,
        },
        by_day: vec![],
    };
    let signers = vec![
        SignerSummary {
            key_id: TRUSTED_KEY.to_string(),
            principal_hint: None,
            points: Some(70),
            trusted: Some(true),
        },
        SignerSummary {
            key_id: STRANGER_KEY.to_string(),
            principal_hint: None,
            points: Some(20),
            trusted: Some(false),
        },
    ];
    let tally = view::tally(&counts, &signers, WindowStates::default());
    assert_eq!(tally.counts.total, Some(100));
    assert_eq!(tally.counts.available, Some(80));
    assert_eq!(
        tally.counts.untrusted_signer,
        Some(20),
        "summed over signers the trust source does not list — the whole walk, not the window"
    );
    assert_eq!(
        tally.counts.unverified,
        Some(6),
        "notAttempted + noEvidence"
    );
    assert_eq!(tally.counts.invalid, Some(4));
    assert_eq!(
        tally.unrepresented,
        vec![("Partial", 5, view::SCOPE_WALK)],
        "the v1alpha1 status has no `counts.partial`; it is NAMED rather than dropped (NOTE FOR \
         W13)"
    );
}

/// The signer summary puts UNTRUSTED keys first and is bounded, so the one row
/// PLAT-15.2 asks an administrator to act on cannot be the one that is dropped.
#[test]
fn the_signer_summary_lists_the_unknown_key_first_and_is_bounded() {
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
    let mut signers = vec![RunnerSigner {
        key_id: TRUSTED_KEY.to_string(),
        principal_hint: Some("runner".to_string()),
        points: 9999,
    }];
    for i in 0..20 {
        signers.push(RunnerSigner {
            key_id: format!("{i:064x}"),
            principal_hint: None,
            points: 1,
        });
    }
    let all = view::signer_summaries(&signers, &trust);
    assert_eq!(
        all.len(),
        21,
        "`signer_summaries` returns the FULL list; `bounded` is what cuts it (finding F10)"
    );
    let rows = view::bounded(all);
    assert_eq!(rows.len(), view::MAX_SIGNERS);
    assert_eq!(rows[0].trusted, Some(false));
    assert!(
        rows.iter().all(|r| r.trusted.is_some()),
        "`trusted` is never absent: an absent value reads as `not known yet` on the surface \
         whose whole job is to say whether an unknown key signed these points"
    );
    assert!(
        !rows.iter().any(|r| r.key_id == TRUSTED_KEY),
        "the trusted key with 9999 points is dropped before any untrusted one"
    );
}

#[test]
fn the_histogram_is_newest_day_first_deduplicated_and_bounded() {
    let mut by_day: Vec<view::DayCount> = (0..500)
        .map(|i| view::DayCount {
            day: format!("2026-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1),
            points: i64::from(i),
        })
        .collect();
    by_day.push(view::DayCount {
        day: "not-a-day".to_string(),
        points: 1,
    });
    let counts = RunnerCounts {
        by_day,
        ..RunnerCounts::default()
    };
    let out = view::histogram(&counts);
    assert!(out.len() <= view::MAX_HISTOGRAM_DAYS);
    assert!(
        !out.iter().any(|b| b.day == "not-a-day"),
        "a malformed day is dropped, never rendered"
    );
    for w in out.windows(2) {
        assert!(w[0].day > w[1].day, "newest first");
    }
}

// ===========================================================================
// 7. Names, tokens, TTL and staleness
// ===========================================================================

/// The name is a pure function of the catalog UID and the trigger, so a
/// duplicate reconcile gets 409 rather than running a second walk — and no
/// catalog can be NAMED in a way that makes its sync unschedulable.
#[test]
fn every_name_is_derived_from_the_uid_and_is_bounded() {
    let long = "a".repeat(240);
    let stem = view::sync_stem(UID, &SyncTrigger::Requested(long.clone()).token());
    assert!(
        stem.len() <= 63,
        "a Job name becomes the `batch.kubernetes.io/job-name` LABEL value, capped at 63: {stem}"
    );
    assert!(view::page_config_map_name(&stem, 7).len() <= 253);
    assert!(view::index_config_map_name(&stem).len() <= 253);
    assert!(view::trust_config_map_name(UID, 9_999_999).len() <= 253);
    assert_eq!(
        stem,
        view::sync_stem(UID, &SyncTrigger::Requested(long).token()),
        "deterministic: two reconciles of one trigger compute one name"
    );
    assert_ne!(
        stem,
        view::sync_stem(UID, &SyncTrigger::Requested("other".to_string()).token()),
        "a genuinely new request is distinguishable from a retry"
    );
    assert!(stem.starts_with(&format!("{}{}", view::NAME_PREFIX, "")));
}

/// `intervalSeconds: 0` is manual only: no slots, and never stale by the clock.
#[test]
fn a_manual_only_catalog_has_no_slots_and_is_never_stale() {
    assert_eq!(view::periodic_slot(now(), 0), None);
    assert!(!view::is_stale(
        now() + chrono::Duration::days(400),
        Some(now()),
        0
    ));
}

/// `Stale` after two intervals, and not before — D3's test matrix.
#[test]
fn the_view_is_stale_after_two_intervals() {
    let synced = now();
    assert!(!view::is_stale(
        synced + chrono::Duration::seconds(7200),
        Some(synced),
        3600
    ));
    assert!(view::is_stale(
        synced + chrono::Duration::seconds(7201),
        Some(synced),
        3600
    ));
    assert!(
        !view::is_stale(synced + chrono::Duration::days(9), None, 3600),
        "a catalog that never synced is `NeverSynced`, not `Stale`"
    );
}

/// The TTL is `max(3 × interval, 86400)` and it is the ONLY thing that removes
/// a page.
/// **Review finding F3.** The TTL bounds how many view generations coexist, and
/// the old day-long floor did not: one Job lives per slot, so the number alive
/// is `ttl / interval`, which at an hourly cadence was 24 and at D3 §5.3's own
/// 300 s floor was 288.
#[test]
fn the_ttl_bounds_how_many_generations_coexist() {
    assert_eq!(view::TTL_FLOOR_SECONDS, 3_600, "an hour, not a day");
    assert_eq!(view::ttl_seconds(3600), 10_800, "three intervals");
    assert_eq!(view::ttl_seconds(86_400), 259_200);
    assert_eq!(
        view::ttl_seconds(300),
        3_600,
        "the floor wins under 20 minutes"
    );
    assert_eq!(view::ttl_seconds(0), view::TTL_FLOOR_SECONDS);
    assert_eq!(
        view::view_expires_at(now(), 3600),
        now() + chrono::Duration::seconds(10_800)
    );

    // The property the number exists for, stated as arithmetic.
    assert_eq!(view::live_generations(3600), 3, "exactly three at an hour");
    assert_eq!(
        view::live_generations(300),
        12,
        "twelve at the CEL floor — at most 12 x (8 pages + index + plan) = 120 ConfigMaps"
    );
    assert_eq!(
        view::live_generations(0),
        1,
        "a manual-only catalog has one"
    );
    for interval in [300, 600, 900, 1800, 3600, 21_600, 86_400] {
        assert!(
            view::live_generations(interval) <= 12,
            "{interval}s keeps {} generations alive",
            view::live_generations(interval)
        );
    }
}

/// **Review finding F3, the other half.** The CRD refuses a cadence below D3
/// §5.3's own floor, which the `schemars` range alone admitted.
#[test]
fn the_crd_refuses_a_cadence_below_the_floor() {
    let rule = weirkeeper::crds::recovery_catalog::J3_INTERVAL_FLOOR_RULE;
    assert!(
        rule.contains("self.sync.intervalSeconds >= 300")
            && rule.contains("self.sync.intervalSeconds == 0"),
        "the rule keeps `0` (manual only) AND imposes D3 §5.3's 300 s floor: {rule}"
    );
    assert_eq!(
        weirkeeper::crds::recovery_catalog::SPEC_RULES.len(),
        3,
        "J1 (syncRequest-only), J2 (destinationRef xor legacyArchive), J3 (the cadence floor)"
    );
    let crd = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config/crd/recoverycatalogs.yaml"),
    )
    .expect("the generated CRD is on disk");
    assert!(
        crd.contains("self.sync.intervalSeconds >= 300"),
        "`just crds` has not been re-run: the rule is in the type and not in the shipped CRD"
    );
}

// ===========================================================================
// 8. The objects the controller writes
// ===========================================================================

/// Pages are IMMUTABLE, owned by the sync Job, and do NOT block its deletion —
/// which is what makes Job TTL the garbage collector.
#[test]
fn a_page_is_immutable_and_owned_by_the_job_without_blocking_its_deletion() {
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
    let built = view::materialise(
        vec![entry(
            "lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            1,
            "s3://b/p",
        )],
        1,
        &trust,
        &ViewLimits {
            view_limit: 10,
            page_max_bytes: view::PAGE_MAX_BYTES,
            max_pages: 8,
        },
        now(),
    );
    let owner = weirkeeper::job::RunnerOwner {
        api_version: "batch/v1".to_string(),
        kind: "Job".to_string(),
        name: "sync".to_string(),
        uid: JOB_UID.to_string(),
    };
    let cm = view::page_config_map("p0", NS, &owner, UID, &built.pages[0], 0);
    assert_eq!(cm.immutable, Some(true));
    let refs = cm.metadata.owner_references.expect("owned");
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].uid, JOB_UID);
    assert_eq!(refs[0].kind, "Job");
    assert_eq!(refs[0].controller, Some(true));
    assert_eq!(
        refs[0].block_owner_deletion,
        Some(false),
        "D3 §5.3: `blockOwnerDeletion: false`. `true` asks for `update` on the owner's \
         finalizers, which this ClusterRole grants on nothing — and blocking the deletion of a \
         Job whose TTL fired is the opposite of the design."
    );
    assert!(cm
        .metadata
        .annotations
        .expect("annotated")
        .contains_key(view::PAGE_DIGEST_ANNOTATION));
}

/// **A page name taken by a foreign object is NEVER adopted**, whatever it
/// contains — adopting it would publish somebody else's bytes as this catalog's
/// view.
#[test]
fn a_foreign_owned_page_is_never_adopted() {
    let mine = k8s_openapi::api::core::v1::ConfigMap {
        metadata: kube::api::ObjectMeta {
            name: Some("p0".to_string()),
            annotations: Some(BTreeMap::from([(
                view::PAGE_DIGEST_ANNOTATION.to_string(),
                "sha256:abc".to_string(),
            )])),
            owner_references: Some(vec![
                k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                    uid: JOB_UID.to_string(),
                    controller: Some(true),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        },
        immutable: Some(true),
        ..Default::default()
    };
    assert!(view::accepts_existing_page(&mine, JOB_UID, "sha256:abc").is_ok());

    // (a) another owner
    let mut foreign = mine.clone();
    foreign.metadata.owner_references = Some(vec![
        k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
            uid: "somebody-else".to_string(),
            controller: Some(true),
            ..Default::default()
        },
    ]);
    let err = view::accepts_existing_page(&foreign, JOB_UID, "sha256:abc")
        .expect_err("a foreign owner is refused");
    assert!(err.to_string().contains("never adopted across owners"));

    // (b) not immutable
    let mut mutable = mine.clone();
    mutable.immutable = None;
    assert!(view::accepts_existing_page(&mutable, JOB_UID, "sha256:abc").is_err());

    // (c) a different digest — two views want one name
    assert!(view::accepts_existing_page(&mine, JOB_UID, "sha256:different").is_err());

    // (d) no owner reference at all
    let mut orphan = mine;
    orphan.metadata.owner_references = None;
    assert!(view::accepts_existing_page(&orphan, JOB_UID, "sha256:abc").is_err());
}

/// The trust `ConfigMap` carries PUBLIC material only, is immutable, and is
/// owned by the CATALOG so it is reused across syncs.
#[test]
fn the_trust_bundle_is_public_material_owned_by_the_catalog() {
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
    let owner = weirkeeper::job::RunnerOwner {
        api_version: "logweir.dev/v1alpha1".to_string(),
        kind: "RecoveryCatalog".to_string(),
        name: NAME.to_string(),
        uid: UID.to_string(),
    };
    let cm = view::trust_config_map("t", NS, &owner, &trust);
    assert_eq!(cm.immutable, Some(true));
    assert_eq!(
        cm.metadata.owner_references.expect("owned")[0].kind,
        "RecoveryCatalog"
    );
    let data = cm.data.expect("data");
    assert!(data[view::TRUST_BUNDLE_KEY].contains("BEGIN PUBLIC KEY"));
    assert!(
        !data[view::TRUST_BUNDLE_KEY].contains("PRIVATE"),
        "never a private key in a ConfigMap"
    );
    assert_eq!(data[view::TRUST_KEY_IDS_KEY], TRUSTED_KEY);
}

/// The sync Job carries its TTL AT CREATION — unlike every other check Job in
/// this tree, and for a stated reason.
#[test]
fn the_sync_job_carries_its_ttl_at_creation_and_mounts_no_secret() {
    let job = view::build_sync_job(&sync_job_spec());
    let spec = job.spec.expect("a Job spec");
    assert_eq!(spec.ttl_seconds_after_finished, Some(10_800));
    assert_eq!(spec.backoff_limit, Some(0));
    let pod = spec.template.spec.expect("a pod spec");
    assert_eq!(pod.automount_service_account_token, Some(false));
    assert_eq!(pod.service_account_name.as_deref(), Some("logweir-runner"));
    let container = &pod.containers[0];
    assert_eq!(container.name, "runner");
    assert_eq!(
        container.args.as_ref().expect("argv"),
        &check::job::runner_argv(),
        "D-SEAMS S1: the SAME argv every other check runs"
    );
    // The trust bundle is a ConfigMap volume; nothing is a Secret volume.
    assert!(
        pod.volumes
            .expect("volumes")
            .iter()
            .all(|v| v.secret.is_none()),
        "a catalog sync mounts no Secret volume; its credential arrives by secretKeyRef"
    );
    let labels = job.metadata.labels.expect("labels");
    assert_eq!(
        labels[check::job::LABEL_CHECK_KIND],
        view::PLAN_KIND,
        "the Job is discoverable as the check kind it runs"
    );
    assert_eq!(labels[check::job::LABEL_CHECK_OWNER_UID], UID);
}

fn sync_job_spec() -> view::SyncJobSpec {
    view::SyncJobSpec {
        name: periodic_stem(),
        namespace: NS.to_string(),
        owner: weirkeeper::job::RunnerOwner {
            api_version: "logweir.dev/v1alpha1".to_string(),
            kind: "RecoveryCatalog".to_string(),
            name: NAME.to_string(),
            uid: UID.to_string(),
        },
        plan_config_map: "plan".to_string(),
        plan_sha256: format!("sha256:{}", "3".repeat(64)),
        subject_uid: UID.to_string(),
        timeout_seconds: 900,
        ttl_seconds: view::ttl_seconds(3600),
        service_account_name: "logweir-runner".to_string(),
        trust_config_map: Some("trust".to_string()),
        env_literal: vec![("AWS_ALLOW_HTTP".to_string(), "false".to_string())],
        env_from_secret: vec![],
        image: None,
        image_pull_policy: None,
    }
}

// ===========================================================================
// 9. The reconciler, over a route table
// ===========================================================================

/// The first pass on a fresh catalog: the destination is resolved, the plan and
/// the trust bundle are created, ONE Job is created, and the status says a sync
/// is running. **No DELETE is sent, on anything.**
#[tokio::test]
async fn a_first_pass_creates_one_sync_job_and_deletes_nothing() {
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", "/backupdestinations/archive", destination_body()),
        route("POST", "/configmaps", empty_config_map()),
        route("POST", "/jobs", empty_job()),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let outcome = run(&f, &catalog(json!({}), json!({}))).await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Started);
    // `Ready` IS ABOUT THE VIEW (finding F6). This catalog has never synced, so
    // it is `Unknown/NeverSynced` — not `SyncInProgress`, which belongs on
    // `Synced` and is asserted there.
    assert_eq!(outcome.ready, "Unknown");
    assert_eq!(outcome.ready_reason, ctrl::REASON_NEVER_SYNCED);
    assert_eq!(outcome.synced_reason, ctrl::REASON_SYNC_IN_PROGRESS);
    assert_eq!(outcome.job_name.as_deref(), Some(periodic_stem().as_str()));

    let jobs = f.posted("/jobs");
    assert_eq!(jobs.len(), 1, "exactly one sync Job per trigger");
    assert_eq!(jobs[0]["metadata"]["name"], periodic_stem());
    assert_eq!(
        jobs[0]["spec"]["ttlSecondsAfterFinished"], 10800,
        "the TTL is the only garbage collector this design has, and it bounds how many \
         generations coexist (finding F3)"
    );

    // **Review finding F1.** The plan `ConfigMap` is created AFTER the Job and
    // is owned BY the Job, so the Job's TTL collects it with the pages. Owned
    // by the catalog it was an orphan per slot that nothing ever removed.
    let plans = f.posted("/configmaps");
    let plan = plans
        .iter()
        .find(|cm| {
            cm["metadata"]["name"]
                .as_str()
                .is_some_and(|n| n.ends_with(check::plan::PLAN_SUFFIX))
        })
        .expect("the plan ConfigMap was written");
    let owner = &plan["metadata"]["ownerReferences"][0];
    assert_eq!(
        owner["kind"], "Job",
        "the plan is owned by the SYNC JOB and not by the RecoveryCatalog: a catalog is \
         long-lived and its plan name changes every slot, so a catalog-owned plan is an orphan \
         per sync that no `delete` verb exists to remove"
    );
    assert_eq!(owner["uid"], JOB_UID);
    assert_eq!(owner["controller"], true);
    assert_eq!(
        owner["blockOwnerDeletion"], false,
        "`true` asks for `update` on jobs/finalizers under \
         OwnerReferencesPermissionEnforcement, which this ClusterRole grants on nothing"
    );

    // AND THE ORDER IS OBSERVABLE. The Job is POSTed before its plan, because
    // an ownerReference needs a UID the API server has not minted yet.
    let order: Vec<String> = f
        .seen()
        .into_iter()
        .filter(|(m, _)| m == "POST")
        .map(|(_, u)| u.split('?').next().unwrap_or(&u).to_string())
        .collect();
    let job_at = order
        .iter()
        .position(|u| u.ends_with("/jobs"))
        .expect("a Job was posted");
    let plan_at = order
        .iter()
        .rposition(|u| u.ends_with("/configmaps"))
        .expect("a ConfigMap was posted");
    assert!(
        job_at < plan_at,
        "the Job is created first so its UID can own the plan: {order:?}"
    );

    assert_no_delete(&f);
    // The status names the Job and carries the S7 precondition.
    let patch = f.patched_status();
    assert_eq!(patch["status"]["lastSyncJob"]["name"], periodic_stem());
    assert_eq!(patch["metadata"]["resourceVersion"], "4242");
}

/// **D-SEAMS S5.** The Job carries the destination's COMPLETE, EXPLICIT
/// environment and its credential by `secretKeyRef`; no credential VALUE
/// appears in the Job at all.
#[tokio::test]
async fn the_sync_job_carries_the_destinations_explicit_environment_and_no_credential_value() {
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", "/backupdestinations/archive", destination_body()),
        route("POST", "/configmaps", empty_config_map()),
        route("POST", "/jobs", empty_job()),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    run(&f, &catalog(json!({}), json!({}))).await;
    let job = f.posted("/jobs").remove(0);
    let env = job["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array()
        .expect("env")
        .clone();
    let names: Vec<&str> = env
        .iter()
        .map(|e| e["name"].as_str().expect("a name"))
        .collect();
    for required in [
        "AWS_ALLOW_HTTP",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
        "AWS_METADATA_ENDPOINT",
        "AWS_REGION",
        "LOGWEIR_STORE_CONTRACT_VERSION",
        "LOGWEIR_ARCHIVE_CREDENTIALS",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        check::job::PLAN_SHA256_ENV,
        check::job::SUBJECT_UID_ENV,
    ] {
        assert!(
            names.contains(&required),
            "{required} is absent from {names:?}"
        );
    }
    for credential in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] {
        let e = env
            .iter()
            .find(|e| e["name"] == credential)
            .expect("the variable");
        assert!(
            e.get("value").is_none() && e["valueFrom"]["secretKeyRef"]["name"] == "lw-reader",
            "a credential reaches the runner by secretKeyRef and never as a literal: {e}"
        );
    }
    assert!(
        !names.contains(&"AWS_ENDPOINT_URL"),
        "absent by construction (D2 §3.5)"
    );
    // The read-only grant, not the write one.
    assert_eq!(
        env.iter()
            .find(|e| e["name"] == "AWS_ACCESS_KEY_ID")
            .expect("id")["valueFrom"]["secretKeyRef"]["name"],
        "lw-reader"
    );
}

/// **The happy path end to end.** A finished Job's relay becomes pages, a fence
/// pointer, counts, a histogram and a signer summary — and still no DELETE.
#[tokio::test]
async fn a_finished_sync_publishes_a_bounded_view() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let log = framed(&plan_sha, UID, &happy_body());
    let f = fixture(harvest_routes(&plan_sha, log, UID));
    let outcome = run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;

    assert_eq!(outcome.phase, ctrl::CatalogPhase::Published);
    assert_eq!(outcome.ready, "True");
    assert_eq!(outcome.ready_reason, ctrl::REASON_VIEW_READY);
    assert_eq!(outcome.synced_reason, ctrl::REASON_SUCCEEDED);
    assert_eq!(outcome.pages, 1);
    assert_eq!(outcome.entries, 2);
    assert!(!outcome.truncated);
    assert_no_delete(&f);

    let posted = f.posted("/configmaps");
    assert_eq!(posted.len(), 2, "one page and one fence pointer");
    let page = &posted[0];
    assert_eq!(page["immutable"], true);
    assert_eq!(page["metadata"]["ownerReferences"][0]["uid"], JOB_UID);
    assert_eq!(page["metadata"]["ownerReferences"][0]["kind"], "Job");
    assert_eq!(
        page["metadata"]["ownerReferences"][0]["blockOwnerDeletion"],
        false
    );
    let lines: Vec<&str> = page["data"][view::PAGE_DATA_KEY]
        .as_str()
        .expect("entries")
        .lines()
        .collect();
    assert_eq!(lines.len(), 2);
    let first: Value = serde_json::from_str(lines[0]).expect("an entry");
    assert_eq!(first["verification"], "Verified");
    assert_eq!(first["selectable"], true);

    let status = f.patched_status()["status"].clone();
    assert_eq!(status["counts"]["total"], 2);
    assert_eq!(status["counts"]["available"], 2);
    assert_eq!(status["counts"]["untrustedSigner"], 0);
    assert_eq!(status["truncated"], false);
    assert_eq!(status["pages"][0]["count"], 2);
    assert_eq!(status["pages"][0]["index"], 0);
    assert!(status["indexConfigMap"].is_string());
    assert_eq!(status["histogram"][0]["day"], "2026-09-16");
    assert_eq!(status["signers"][0]["trusted"], true);
    assert_eq!(status["syncedAt"], "2026-09-16T11:55:00Z");
    assert_eq!(
        status["viewExpiresAt"], "2026-09-16T14:55:00Z",
        "finish + max(3 × interval, one hour) — three generations at an hourly cadence"
    );
    assert_eq!(status["cursor"]["indexShard"], "2026/09/16");
    assert_eq!(status["cursor"]["complete"], true);
    assert_condition(&status, ctrl::CONDITION_TRUST_AVAILABLE, "True");
    assert_condition(&status, ctrl::CONDITION_STALE, "False");
}

/// **The partial-access row.** A 403 on part of the archive yields `Unreadable`
/// entries and `Synced=False/PartialScan`, and NEVER `Missing`.
#[tokio::test]
async fn a_403_on_part_of_the_archive_is_unreadable_and_never_missing() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let mut counts = counts_value(3, 2);
    counts["unreadable"] = json!(1);
    counts["available"] = json!(2);
    let body = body_for(
        &[vec![
            ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 30),
            entry_value(
                "lwp1-cccccccccccccccccccccccccccccccc",
                20,
                "Unreadable",
                "notAttempted",
                TRUSTED_KEY,
            ),
        ]],
        counts,
        json!([{"keyId": TRUSTED_KEY, "points": 2}]),
        true,
    );
    let f = fixture(harvest_routes(
        &plan_sha,
        framed(&plan_sha, UID, &body),
        UID,
    ));
    let outcome = run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;

    assert_eq!(outcome.synced_reason, ctrl::REASON_PARTIAL_SCAN);
    let status = f.patched_status()["status"].clone();
    assert_eq!(status["counts"]["unreadable"], 1);
    assert_eq!(
        status["counts"]["missing"], 0,
        "a permission failure is NOT an absence; reporting it as one is how an operator comes to \
         believe an outage deleted their backups"
    );
    assert_condition(&status, ctrl::CONDITION_SYNCED, "False");
    let page = f.posted("/configmaps").remove(0);
    let entries: Vec<Value> = page["data"][view::PAGE_DATA_KEY]
        .as_str()
        .expect("entries")
        .lines()
        .map(|l| serde_json::from_str(l).expect("json"))
        .collect();
    let unreadable = entries
        .iter()
        .find(|e| e["availability"] == "Unreadable")
        .expect("the unreadable row is listed, not hidden");
    assert_eq!(unreadable["selectable"], false);
    assert_eq!(unreadable["verification"], "NotAttempted");
}

/// **The unsupported-major row.** A record this build does not understand is
/// ONE entry's state; the sync still completes.
#[tokio::test]
async fn a_record_from_a_future_major_is_one_unsupported_entry_and_not_a_failed_sync() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let mut future = entry_value(
        "lwp1-dddddddddddddddddddddddddddddddd",
        10,
        "UnsupportedFormat",
        "notAttempted",
        TRUSTED_KEY,
    );
    future["formatVersion"] = json!("2.0.0");
    future["remedy"] = json!("this record was written by a newer Logweir; upgrade to read it");
    let mut counts = counts_value(2, 1);
    counts["unsupportedFormat"] = json!(1);
    counts["available"] = json!(1);
    let body = body_for(
        &[vec![
            ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 30),
            future,
        ]],
        counts,
        json!([{"keyId": TRUSTED_KEY, "points": 1}]),
        true,
    );
    let f = fixture(harvest_routes(
        &plan_sha,
        framed(&plan_sha, UID, &body),
        UID,
    ));
    let outcome = run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;
    assert_eq!(
        outcome.phase,
        ctrl::CatalogPhase::Published,
        "D3 §5.2 rule 1: a higher major makes the ENTRY unsupported, never the sync fatal"
    );
    let status = f.patched_status()["status"].clone();
    assert_eq!(status["counts"]["unsupportedFormat"], 1);
    assert_condition(&status, ctrl::CONDITION_SYNCED, "True");
}

/// **The untrusted-signer row.** A point signed by a key this installation does
/// not list is listed with its exact state, is not selectable, and the signer
/// summary names the key id an administrator has to act on.
#[tokio::test]
async fn an_unknown_signer_is_untrusted_and_never_offered() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let body = body_for(
        &[vec![entry_value(
            "lwp1-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            30,
            "Available",
            "verified",
            STRANGER_KEY,
        )]],
        counts_value(1, 1),
        json!([{"keyId": STRANGER_KEY, "points": 1, "principalHint": "another installation"}]),
        true,
    );
    let f = fixture(harvest_routes(
        &plan_sha,
        framed(&plan_sha, UID, &body),
        UID,
    ));
    run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;

    let page = f.posted("/configmaps").remove(0);
    let entry: Value = serde_json::from_str(
        page["data"][view::PAGE_DATA_KEY]
            .as_str()
            .expect("entries")
            .lines()
            .next()
            .expect("one entry"),
    )
    .expect("json");
    assert_eq!(entry["availability"], "Available");
    assert_eq!(entry["verification"], "UntrustedSigner");
    assert_eq!(entry["selectable"], false);
    assert_eq!(entry["signerKeyId"], STRANGER_KEY);

    let status = f.patched_status()["status"].clone();
    assert_eq!(status["counts"]["untrustedSigner"], 1);
    assert_eq!(status["signers"][0]["keyId"], STRANGER_KEY);
    assert_eq!(status["signers"][0]["trusted"], false);
}

/// With NO trust material the whole view is `NotAttempted`, `TrustAvailable` is
/// `False`, and nothing is presented as verified evidence.
#[tokio::test]
async fn with_no_trust_material_nothing_is_offered_and_the_condition_says_why() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let mut routes = harvest_routes(&plan_sha, framed(&plan_sha, UID, &happy_body()), UID);
    routes[0] = route("GET", "/trustrosters/default", empty_roster_body());
    let f = fixture(routes);
    run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;

    let page = f.posted("/configmaps").remove(0);
    for line in page["data"][view::PAGE_DATA_KEY]
        .as_str()
        .expect("entries")
        .lines()
    {
        let e: Value = serde_json::from_str(line).expect("json");
        assert_eq!(e["verification"], "NotAttempted");
        assert_eq!(e["selectable"], false);
    }
    let status = f.patched_status()["status"].clone();
    assert_condition(&status, ctrl::CONDITION_TRUST_AVAILABLE, "False");
    // And NO trust ConfigMap was ever written: an empty bundle would make the
    // runner configure a trust store with nothing in it.
    assert_eq!(
        f.posted("/configmaps").len(),
        2,
        "one page and one fence pointer, and no trust bundle"
    );
}

/// **The impostor pod.** A pod carrying the Job's LABEL but owned by something
/// else is never read, and the sync reports `ResultUnreadable` rather than the
/// impostor's output. D-SEAMS **S6**, defect `SEC-PODLOG`.
#[tokio::test]
async fn an_impostor_pod_is_never_read() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let mut routes = harvest_routes(&plan_sha, framed(&plan_sha, UID, &happy_body()), UID);
    // The listing answers with a pod whose controller owner is NOT this Job.
    let pods = routes
        .iter()
        .position(|r| r.path_suffix == "/pods")
        .expect("the harvest table lists pods");
    routes[pods] = route("GET", "/pods", pod_list_body("some-other-job-uid"));
    // The log route is REMOVED: if the reconciler reads it, the double panics.
    routes.retain(|r| r.path_suffix != "/log");
    let f = fixture(routes);
    let outcome = run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;

    assert_eq!(outcome.phase, ctrl::CatalogPhase::Failed);
    assert_eq!(outcome.synced_reason, "ResultUnreadable");
    // `contains("/log")` would match `/apis/logweir.dev/…`; the PATH is what
    // matters, so the query string is stripped first.
    let read_a_log = |uri: &str| uri.split('?').next().unwrap_or(uri).ends_with("/log");
    assert!(
        f.seen()
            .iter()
            .any(|(m, u)| m == "GET" && u.split('?').next().unwrap_or(u).ends_with("/pods")),
        "the test is not vacuous: the reconciler DID list pods"
    );
    assert!(
        !f.seen().iter().any(|(m, u)| m == "GET" && read_a_log(u)),
        "a `pods/log` read for a pod this Job does not own is a read of somebody else's output \
         (defect SEC-PODLOG): {:?}",
        f.seen()
    );
    assert!(
        f.posted("/configmaps").is_empty(),
        "no page is written from a relay that was never read"
    );
}

/// A Job carrying this sync's NAME but controlled by something else is refused
/// and nothing is read from it — a name is not an identity.
#[tokio::test]
async fn a_job_owned_by_something_else_is_refused_and_never_read() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let stem: &'static str = Box::leak(periodic_stem().into_boxed_str());
    let job_path: &'static str = Box::leak(format!("/jobs/{stem}").into_boxed_str());
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route(
            "GET",
            job_path,
            job_body(stem, true, &plan_sha, "not-this-catalog"),
        ),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let outcome = run(&f, &catalog(json!({}), tracked_status(stem))).await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Refused);
    assert_eq!(outcome.ready_reason, ctrl::REASON_JOB_NAME_CONFLICT);
    assert!(
        !f.seen().iter().any(|(_, u)| u.contains("/pods")),
        "nothing is listed for a Job this catalog does not control"
    );
}

/// **A page whose digest does not verify writes nothing**, and the previous
/// view is retained.
#[tokio::test]
async fn a_tampered_relay_writes_no_page_and_keeps_the_previous_view() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let body = happy_body().replace(
        "\"availability\":\"Available\"",
        "\"availability\":\"Missing\"",
    );
    let f = fixture(harvest_routes(
        &plan_sha,
        framed(&plan_sha, UID, &body),
        UID,
    ));
    let mut status = tracked_status(&periodic_stem());
    status["pages"] = json!([{"configMapName": "old-p0", "index": 0, "count": 9}]);
    let outcome = run(&f, &catalog(json!({}), status)).await;

    assert_eq!(outcome.phase, ctrl::CatalogPhase::Failed);
    assert_eq!(outcome.synced_reason, "ResultUnreadable");
    assert!(f.posted("/configmaps").is_empty());
    let patch = f.patched_status();
    assert!(
        patch["status"].get("pages").is_none(),
        "a merge patch that omits `pages` leaves the previous view, which is still true until \
         its Job's TTL fires: {patch}"
    );
    assert_eq!(outcome.ready_reason, ctrl::REASON_VIEW_READY);
}

/// **The stale-index rows.** `Stale` after two intervals; `ViewExpired` once the
/// Job — and with it the pages — has been garbage-collected.
#[tokio::test]
async fn a_view_whose_job_is_gone_reports_view_expired_and_stops_listing_its_pages() {
    let stem: &'static str = Box::leak(periodic_stem().into_boxed_str());
    let job_path: &'static str = Box::leak(format!("/jobs/{stem}").into_boxed_str());
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        Route {
            method: "GET",
            path_suffix: job_path,
            status: 404,
            body: json!({"kind": "Status", "code": 404, "reason": "NotFound",
                         "message": "jobs not found", "status": "Failure"})
            .to_string(),
        },
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let status = json!({
        "observedGeneration": 1,
        "syncedAt": "2026-09-14T00:00:00Z",
        "viewExpiresAt": "2026-09-15T00:00:00Z",
        "observedSyncRequest": "t1",
        "truncated": false,
        "pages": [{"configMapName": "old-p0", "index": 0, "count": 4}],
        "indexConfigMap": "old-index",
        "lastSyncJob": {"name": stem, "finishedAt": "2026-09-14T00:00:00Z"},
        "conditions": [{"type": "Synced", "status": "True", "reason": "Succeeded"}]
    });
    // `syncRequest` equals what was observed and the slot has moved on, so the
    // trigger is the periodic one — whose stem IS the tracked name, so this pass
    // is idle and only reports.
    let outcome = run(&f, &catalog(json!({"syncRequest": "t1"}), status)).await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Idle);
    assert_eq!(outcome.ready_reason, ctrl::REASON_VIEW_EXPIRED);
    let patch = f.patched_status()["status"].clone();
    // `.get(…) == Some(&Null)` and NOT `["pages"].is_null()`: indexing a
    // missing key also yields `Null`, so the weaker form passes for a patch
    // that simply forgot to clear the pages — which is the mutant this row
    // exists to kill.
    assert_eq!(
        patch.get("pages"),
        Some(&Value::Null),
        "`null` DELETES the key: a status.pages[] naming a ConfigMap the API server no longer \
         has is a link to a 404, and a reader cannot tell it from a page it has not fetched: \
         {patch}"
    );
    assert_eq!(patch.get("indexConfigMap"), Some(&Value::Null));
    assert_eq!(patch.get("truncated"), Some(&Value::Null));
    assert_condition(&patch, ctrl::CONDITION_STALE, "True");
    assert_condition(&patch, ctrl::CONDITION_READY, "False");
    assert_no_delete(&f);
}

/// **`syncRequest` is idempotent.** A request the controller has already acted
/// on starts nothing; a genuinely new one does.
#[tokio::test]
async fn a_repeated_sync_request_starts_nothing_and_a_new_one_starts_one_job() {
    // (a) already observed, and the periodic slot's Job is the tracked one.
    let stem: &'static str = Box::leak(periodic_stem().into_boxed_str());
    let job_path: &'static str = Box::leak(format!("/jobs/{stem}").into_boxed_str());
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", job_path, job_body(stem, true, "sha256:x", UID)),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let status = json!({
        "observedGeneration": 1,
        "observedSyncRequest": "token-1",
        "syncedAt": "2026-09-16T11:55:00Z",
        "viewExpiresAt": "2026-09-17T11:55:00Z",
        "pages": [{"configMapName": "p0", "index": 0, "count": 1}],
        "lastSyncJob": {"name": stem, "finishedAt": "2026-09-16T11:55:00Z"},
        "conditions": [{"type": "Synced", "status": "True", "reason": "Succeeded"}]
    });
    let outcome = run(
        &f,
        &catalog(json!({"syncRequest": "token-1"}), status.clone()),
    )
    .await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Idle);
    assert!(
        f.posted("/jobs").is_empty(),
        "a retried request is not a second walk"
    );

    // (b) a new token: exactly one Job, named after that token and not the slot.
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", job_path, job_body(stem, true, "sha256:x", UID)),
        route("GET", "/backupdestinations/archive", destination_body()),
        route("POST", "/configmaps", empty_config_map()),
        route("POST", "/jobs", empty_job()),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let outcome = run(&f, &catalog(json!({"syncRequest": "token-2"}), status)).await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Started);
    let jobs = f.posted("/jobs");
    assert_eq!(jobs.len(), 1);
    let expected = view::sync_stem(UID, &SyncTrigger::Requested("token-2".to_string()).token());
    assert_eq!(jobs[0]["metadata"]["name"], expected);
    assert_eq!(
        f.patched_status()["status"]["observedSyncRequest"],
        "token-2",
        "the token is recorded when the controller ACTS on it, so a crash mid-sync does not \
         start a second walk for the same request"
    );
}

/// A sync that is already running is watched, not duplicated.
#[tokio::test]
async fn a_running_sync_is_watched_and_never_duplicated() {
    let stem: &'static str = Box::leak(periodic_stem().into_boxed_str());
    let job_path: &'static str = Box::leak(format!("/jobs/{stem}").into_boxed_str());
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", job_path, job_body(stem, false, "sha256:x", UID)),
        route(
            "GET",
            "/pods",
            json!({"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}).to_string(),
        ),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let outcome = run(
        &f,
        &catalog(json!({"syncRequest": "brand-new"}), tracked_status(stem)),
    )
    .await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Running);
    assert!(
        f.posted("/jobs").is_empty(),
        "a syncRequest arriving mid-sync waits for the running one to finish"
    );
    assert_no_delete(&f);
}

/// A destination that is not usable creates NOTHING — no ConfigMap, no Job.
#[tokio::test]
async fn an_unusable_destination_creates_nothing() {
    let mut body: Value = serde_json::from_str(&destination_body()).expect("json");
    body["status"]["conditions"][0]["status"] = json!("False");
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", "/backupdestinations/archive", body.to_string()),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let outcome = run(&f, &catalog(json!({}), json!({}))).await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Refused);
    assert_eq!(outcome.ready_reason, ctrl::REASON_DESTINATION_UNUSABLE);
    assert!(f.posted("/configmaps").is_empty());
    assert!(f.posted("/jobs").is_empty());
}

/// `spec.legacyArchive` is an ABSENT CAPABILITY, named — never a fake stub, and
/// never a Job it cannot address.
#[tokio::test]
async fn a_legacy_archive_catalog_is_refused_by_name_and_creates_no_job() {
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let mut value = catalog_value(json!({}), json!({}));
    value["spec"]
        .as_object_mut()
        .expect("a spec")
        .remove("destinationRef");
    value["spec"]["legacyArchive"] = json!({"url": "s3://old-archive"});
    let catalog: RecoveryCatalog = serde_json::from_value(value).expect("a catalog");
    let outcome = run(&f, &catalog).await;
    assert_eq!(
        outcome.ready_reason,
        ctrl::REASON_LEGACY_ARCHIVE_UNSUPPORTED
    );
    assert!(f.posted("/jobs").is_empty());
}

/// A foreign page name refuses the whole publish rather than adopting it.
#[tokio::test]
async fn a_page_name_taken_by_a_foreign_object_refuses_the_publish() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let stem = periodic_stem();
    let job_path: &'static str = Box::leak(format!("/jobs/{stem}").into_boxed_str());
    let page_path: &'static str =
        Box::leak(format!("/configmaps/{}", view::page_config_map_name(&stem, 0)).into_boxed_str());
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", job_path, job_body(&stem, true, &plan_sha, UID)),
        route("GET", "/pods", pod_list_body(JOB_UID)),
        route("GET", "/log", framed(&plan_sha, UID, &happy_body())),
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 409,
            body: json!({"kind": "Status", "code": 409, "reason": "AlreadyExists",
                         "status": "Failure", "message": "exists"})
            .to_string(),
        },
        route(
            "GET",
            page_path,
            json!({
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {
                    "name": view::page_config_map_name(&stem, 0), "namespace": NS,
                    "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job",
                                         "name": "someone-elses", "uid": "another-job",
                                         "controller": true}]
                },
                "immutable": true,
                "data": {view::PAGE_DATA_KEY: "{}"}
            })
            .to_string(),
        ),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let outcome = run(&f, &catalog(json!({}), tracked_status(&stem))).await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Refused);
    assert_eq!(outcome.ready_reason, ctrl::REASON_PAGE_CONFLICT);
    assert_no_delete(&f);
}

/// **Seam S7 in both halves, on every status write this reconciler makes.**
#[tokio::test]
async fn every_status_write_is_a_conditional_merge_patch() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let f = fixture(harvest_routes(
        &plan_sha,
        framed(&plan_sha, UID, &happy_body()),
        UID,
    ));
    run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;
    let patches = f.status_patches();
    assert!(!patches.is_empty());
    for patch in &patches {
        assert_eq!(
            patch["metadata"]["resourceVersion"], "4242",
            "the body carries metadata.resourceVersion as the API server's update precondition"
        );
        assert_eq!(patch["metadata"]["name"], NAME);
    }
    // And NO `PUT`: `replace_status` is authorised as `update`, which this role
    // grants on nothing.
    assert!(
        !f.seen().iter().any(|(m, _)| m == "PUT"),
        "no reconciler in this crate calls Api::replace_status: {:?}",
        f.seen()
    );
}

/// A status that would not change sends NO PATCH AT ALL — erratum E11(d), the
/// reconcile loop that a status write of its own wakes.
#[tokio::test]
async fn an_unchanged_status_sends_no_patch() {
    let stem: &'static str = Box::leak(periodic_stem().into_boxed_str());
    let job_path: &'static str = Box::leak(format!("/jobs/{stem}").into_boxed_str());
    // Two passes: the first writes, the second is handed back what it wrote.
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", job_path, job_body(stem, true, "sha256:x", UID)),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let status = json!({
        "observedGeneration": 1,
        "observedSyncRequest": "t",
        "syncedAt": "2026-09-16T11:55:00Z",
        "viewExpiresAt": "2026-09-17T11:55:00Z",
        "pages": [{"configMapName": "p0", "index": 0, "count": 1}],
        "lastSyncJob": {"name": stem, "finishedAt": "2026-09-16T11:55:00Z"},
        "conditions": []
    });
    let catalog_object = catalog(json!({"syncRequest": "t"}), status);
    run(&f, &catalog_object).await;
    let first = f.status_patches();
    assert_eq!(first.len(), 1, "the first pass writes the four conditions");

    // Feed the written status back and run again: nothing changes.
    let mut merged = serde_json::to_value(catalog_object.status.as_ref().expect("a status"))
        .expect("serialises");
    apply_merge_patch(&mut merged, &first[0]["status"]);
    let mut value = catalog_value(json!({"syncRequest": "t"}), merged);
    value["metadata"]["resourceVersion"] = json!("4243");
    let settled: RecoveryCatalog = serde_json::from_value(value).expect("a catalog");
    let f2 = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", job_path, job_body(stem, true, "sha256:x", UID)),
    ]);
    run(&f2, &settled).await;
    assert!(
        f2.status_patches().is_empty(),
        "a patch that would change nothing is not sent: this reconciler's own status write is \
         what wakes it, and a patch that moved only the clock spins the loop"
    );
}

// ===========================================================================
// 9b. The fix round's own rows
// ===========================================================================

/// **Review finding F7.** The body declares its grammar version, and a body
/// that does not — or declares one this build cannot read — is refused by name
/// rather than parsed on hope.
#[test]
fn the_body_must_declare_a_grammar_version_this_build_reads() {
    let e = vec![ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)];
    let page = page_block(1, 1, &e);
    assert_eq!(
        view::parse_body(&page, 5000),
        Err(view::BodyError::MissingFormat),
        "the record layout is versioned; the relay grammar was not, so a newer runner could \
         only extend it by hoping this parser ignored what it did not know"
    );
    let future = format!("{}2\n{page}", view::FORMAT_LINE_PREFIX);
    assert_eq!(
        view::parse_body(&future, 5000),
        Err(view::BodyError::UnsupportedBodyFormat {
            got: "2".to_string()
        })
    );
    // The control.
    assert!(view::parse_body(&versioned(&page), 5000).is_ok());
}

/// **Review finding F7.** A summary line that arrived twice is an ERROR, not
/// last-wins: two `catalog-counts=` lines mean the runner disagreed with itself
/// about the whole walk.
#[test]
fn a_repeated_summary_line_is_refused_and_never_silently_overwritten() {
    let e = vec![ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)];
    for (prefix, first, second) in [
        (
            view::COUNTS_LINE_PREFIX,
            counts_value(1, 1).to_string(),
            counts_value(9, 9).to_string(),
        ),
        (
            view::CURSOR_LINE_PREFIX,
            json!({"complete": true}).to_string(),
            json!({"complete": false}).to_string(),
        ),
        (
            view::SIGNERS_LINE_PREFIX,
            json!([{"keyId": TRUSTED_KEY, "points": 1}]).to_string(),
            json!([{"keyId": STRANGER_KEY, "points": 9}]).to_string(),
        ),
    ] {
        let body = versioned(&format!(
            "{}{prefix}{first}\n{prefix}{second}\n",
            page_block(1, 1, &e)
        ));
        assert_eq!(
            view::parse_body(&body, 5000),
            Err(view::BodyError::RepeatedSummary(prefix)),
            "`{prefix}` twice must be refused: a summary that can be overwritten is a summary \
             nobody can attribute to an observation"
        );
    }
}

/// **Review finding F7.** The body is bounded by BYTES and by this catalog's own
/// `viewLimit` — never by an unreachable line count.
#[test]
fn the_body_is_bounded_by_bytes_and_by_the_view_limit() {
    let entries: Vec<Value> = (0..5)
        .map(|i| ok_entry(&format!("lwp1-{i:032x}"), i64::from(i)))
        .collect();
    let body = versioned(&page_block(1, 1, &entries));
    assert_eq!(
        view::parse_body(&body, 3),
        Err(view::BodyError::TooManyEntries { allowed: 3 }),
        "the runner is told to relay the newest `viewLimit` points and nothing more"
    );
    assert!(view::parse_body(&body, 5).is_ok());

    let huge = format!("{}{}", versioned(""), "x".repeat(view::MAX_BODY_BYTES));
    match view::parse_body(&huge, 5000) {
        Err(view::BodyError::TooLarge { got }) => assert!(got > view::MAX_BODY_BYTES),
        other => panic!("a body over the byte budget must be refused, got {other:?}"),
    }
    // The relay's own budget is ~5.9 MB of raw `details` after the ~1.35x
    // base64 part-frame expansion of `DECODER_BUDGET_BYTES`; this cap must sit
    // inside it, and it is derived rather than asserted so a change to either
    // constant is visible here.
    let relay_raw_budget = (check::relay::DECODER_BUDGET_BYTES * 3) / 4;
    assert!(
        view::MAX_BODY_BYTES < relay_raw_budget,
        "{} is not inside the relay's ~{relay_raw_budget} bytes of raw details",
        view::MAX_BODY_BYTES
    );
}

/// **Review finding F7 / F12.** `catalog-signers` is capped, and so is one
/// point's `locations[]`.
#[test]
fn the_signer_list_and_an_entrys_locations_are_both_capped() {
    let e = vec![ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)];
    let many: Vec<Value> = (0..view::MAX_BODY_SIGNERS + 1)
        .map(|i| json!({"keyId": format!("{i:064x}"), "points": 1}))
        .collect();
    let body = versioned(&format!(
        "{}{}{}\n",
        page_block(1, 1, &e),
        view::SIGNERS_LINE_PREFIX,
        json!(many)
    ));
    assert_eq!(
        view::parse_body(&body, 5000),
        Err(view::BodyError::MalformedSummary(view::SIGNERS_LINE_PREFIX))
    );

    // An entry naming more places than the grammar allows is SKIPPED and
    // counted, exactly like any other malformed entry — `locations[]` is the
    // only unbounded field an entry has.
    let mut wide = ok_entry("lwp1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 2);
    wide["locations"] = json!((0..view::MAX_ENTRY_LOCATIONS + 1)
        .map(|i| json!({"locationId": format!("s3://b{i}/p")}))
        .collect::<Vec<_>>());
    let body = versioned(&page_block(1, 1, &[wide]));
    let parsed = view::parse_body(&body, 5000).expect("skipped, never fatal");
    assert_eq!(parsed.pages[0].entries.len(), 0);
    assert_eq!(parsed.skipped_entries, 1);
}

/// **Review finding F12.** One entry whose rendered line cannot fit a page is
/// refused rather than placed into a `ConfigMap` the API server rejects at
/// CREATE — which would surface as a requeue loop and not as a verdict.
#[test]
fn an_entry_too_large_for_any_page_is_refused_and_counted() {
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
    let mut fat = entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 20, "s3://one/p");
    fat.remedy = Some("y".repeat(4096));
    let small = entry("lwp1-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 10, "s3://one/p");
    let limits = ViewLimits {
        view_limit: 10,
        page_max_bytes: 1024,
        max_pages: 8,
    };
    let built = view::materialise(vec![fat, small], 2, &trust, &limits, now());
    assert_eq!(built.dropped_oversized, 1);
    assert_eq!(built.entries, 1);
    assert!(
        built.truncated,
        "the view is not the whole archive, and says so"
    );
    for page in &built.pages {
        assert!(page.body.len() <= limits.page_max_bytes);
    }
}

/// **Review finding F11.** A day reported in two shards is FOLDED, never
/// dropped: `counts.total` would still include those points.
#[test]
fn a_histogram_day_reported_twice_is_summed_and_not_dropped() {
    let counts = RunnerCounts {
        by_day: vec![
            view::DayCount {
                day: "2026-09-16".to_string(),
                points: 7,
            },
            view::DayCount {
                day: "2026-09-15".to_string(),
                points: 1,
            },
            view::DayCount {
                day: "2026-09-16".to_string(),
                points: 5,
            },
        ],
        ..RunnerCounts::default()
    };
    let out = view::histogram(&counts);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].day, "2026-09-16");
    assert_eq!(
        out[0].points, 12,
        "7 + 5, never 7: silently losing points is the one option this module's philosophy \
         argues against"
    );
    assert_eq!(out[1].points, 1);
}

/// **Review finding F4.** `trusted` answers "does this installation ACCEPT
/// evidence from this key", not "is it listed" — and `Revoked` and
/// `VerifiedHistorical`, which have no counter, are named instead of lost.
#[test]
fn a_revoked_key_is_not_trusted_and_its_points_are_named() {
    let revoked = trust_with(TRUSTED_KEY, TrustKeyState::Revoked, None);
    assert!(
        !view::accepts(&revoked, TRUSTED_KEY),
        "a key whose private half is in someone else's hands is LISTED and not ACCEPTED"
    );
    assert!(view::accepts(
        &trust_with(TRUSTED_KEY, TrustKeyState::Retired, None),
        TRUSTED_KEY
    ));
    assert!(view::accepts(
        &trust_with(TRUSTED_KEY, TrustKeyState::Active, None),
        TRUSTED_KEY
    ));

    let signers = vec![RunnerSigner {
        key_id: TRUSTED_KEY.to_string(),
        principal_hint: None,
        points: 4,
    }];
    let rows = view::signer_summaries(&signers, &revoked);
    assert_eq!(
        rows[0].trusted,
        Some(false),
        "an operator reading `status` alone must not see a compromised signer reported as fine"
    );
    let counts = RunnerCounts {
        total: 4,
        available: 4,
        ..RunnerCounts::default()
    };
    let tally = view::tally(
        &counts,
        &rows,
        WindowStates {
            revoked: 4,
            verified_historical: 1,
        },
    );
    assert_eq!(
        tally.counts.untrusted_signer,
        Some(4),
        "revoked points land in an adverse counter and not in none at all"
    );
    assert_eq!(
        tally.unrepresented,
        vec![
            ("Revoked", 4, view::SCOPE_VIEW),
            ("VerifiedHistorical", 1, view::SCOPE_VIEW),
        ],
        "neither has a counter, so both are NAMED in the Synced message"
    );
}

/// **Review finding F10, through the reconciler.** `counts.untrustedSigner`
/// covers the whole walk, so it must be summed over the full signer list and
/// not over the sixteen rows `status.signers` can display.
#[tokio::test]
async fn the_status_counts_every_untrusted_signer_and_displays_sixteen() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let signers: Vec<Value> = (0..20)
        .map(|i| json!({"keyId": format!("{i:064x}"), "points": 1}))
        .collect();
    let body = body_for(
        &[vec![ok_entry("lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 30)]],
        counts_value(20, 20),
        json!(signers),
        true,
    );
    let f = fixture(harvest_routes(
        &plan_sha,
        framed(&plan_sha, UID, &body),
        UID,
    ));
    run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;
    let status = f.patched_status()["status"].clone();
    assert_eq!(
        status["counts"]["untrustedSigner"], 20,
        "twenty untrusted signers signed these points; summing over the sixteen the status can \
         DISPLAY would under-report an archive written by more installations than that"
    );
    assert_eq!(
        status["signers"].as_array().expect("signers").len(),
        view::MAX_SIGNERS,
        "and the displayed list is still bounded"
    );
}

/// **Review finding F10.** `counts.untrustedSigner` is summed over the FULL
/// signer list, before the sixteen-row display bound.
#[test]
fn the_untrusted_total_is_summed_before_the_display_bound() {
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
    let signers: Vec<RunnerSigner> = (0..20)
        .map(|i| RunnerSigner {
            key_id: format!("{i:064x}"),
            principal_hint: None,
            points: 1,
        })
        .collect();
    let all = view::signer_summaries(&signers, &trust);
    let tally = view::tally(&RunnerCounts::default(), &all, WindowStates::default());
    assert_eq!(
        tally.counts.untrusted_signer,
        Some(20),
        "twenty untrusted signers, not the sixteen the status can display"
    );
    assert_eq!(view::bounded(all).len(), view::MAX_SIGNERS);
}

/// **Review finding F2, the refusal path.** A catalog whose destination went
/// invalid AFTER its sync Job aged out must stop naming garbage-collected page
/// ConfigMaps — and it used to name them forever.
#[tokio::test]
async fn an_expired_view_is_cleared_on_the_refusal_path_too() {
    // The tracked Job is the PREVIOUS slot's and has aged out, so this pass
    // computes a NEW trigger and reaches `start()` — where the destination
    // refuses it. That is the reviewer's reproduction exactly.
    let stem: &'static str = Box::leak(previous_stem().into_boxed_str());
    let job_path: &'static str = Box::leak(format!("/jobs/{stem}").into_boxed_str());
    let mut dest: Value = serde_json::from_str(&destination_body()).expect("json");
    dest["status"]["conditions"][0]["status"] = json!("False");
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        Route {
            method: "GET",
            path_suffix: job_path,
            status: 404,
            body: json!({"kind": "Status", "code": 404, "reason": "NotFound",
                         "message": "jobs not found", "status": "Failure"})
            .to_string(),
        },
        route("GET", "/backupdestinations/archive", dest.to_string()),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let status = json!({
        "observedGeneration": 1,
        "syncedAt": "2026-09-16T09:00:00Z",
        "viewExpiresAt": "2026-09-16T23:00:00Z",
        "truncated": false,
        "pages": [{"configMapName": "gone-p0", "index": 0, "count": 4}],
        "indexConfigMap": "gone-index",
        "lastSyncJob": {"name": stem, "finishedAt": "2026-09-16T09:00:00Z"},
        "conditions": [{"type": "Synced", "status": "True", "reason": "Succeeded"}]
    });
    let outcome = run(&f, &catalog(json!({}), status)).await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Refused);
    assert_eq!(outcome.ready_reason, ctrl::REASON_DESTINATION_UNUSABLE);
    let patch = f.patched_status()["status"].clone();
    assert_eq!(
        patch.get("pages"),
        Some(&Value::Null),
        "the pages are gone with their Job even though `viewExpiresAt` has not passed, and the \
         published contract tells W11/W12 to read these names: leaving them is leading them \
         into a 404 with no signal at all. Got {patch}"
    );
    assert_eq!(patch.get("indexConfigMap"), Some(&Value::Null));
    assert_eq!(patch.get("truncated"), Some(&Value::Null));
}

/// **Review finding F2, the sync-failure path.**
#[tokio::test]
async fn an_expired_view_is_cleared_when_a_sync_result_does_not_read() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let stem = periodic_stem();
    let job_path: &'static str = Box::leak(format!("/jobs/{stem}").into_boxed_str());
    // The tracked Job is present but its RESULT does not read; the PREVIOUS
    // view's Job (a different, older one) is what aged out — modelled by a
    // status whose recorded expiry has passed.
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", job_path, job_body(&stem, true, &plan_sha, UID)),
        route("GET", "/pods", pod_list_body(JOB_UID)),
        route(
            "GET",
            "/log",
            framed(&plan_sha, UID, "not a catalog body at all"),
        ),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let status = json!({
        "observedGeneration": 1,
        "syncedAt": "2026-09-15T09:00:00Z",
        "viewExpiresAt": "2026-09-15T12:00:00Z",
        "truncated": true,
        "pages": [{"configMapName": "gone-p0", "index": 0, "count": 4}],
        "indexConfigMap": "gone-index",
        "lastSyncJob": {"name": stem},
        "conditions": [{"type": "Synced", "status": "True", "reason": "Succeeded"}]
    });
    let outcome = run(&f, &catalog(json!({}), status)).await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Failed);
    assert_eq!(outcome.ready_reason, ctrl::REASON_VIEW_EXPIRED);
    let patch = f.patched_status()["status"].clone();
    assert_eq!(patch.get("pages"), Some(&Value::Null), "{patch}");
    assert_eq!(patch.get("indexConfigMap"), Some(&Value::Null));
    assert!(
        patch["lastSyncJob"]["finishedAt"].is_string(),
        "the failed sync is still recorded: the view is gone, the attempt is not"
    );
}

/// **Review finding F6.** A catalog with a LIVE view that starts its next sync
/// keeps `Ready=True`; `SyncInProgress` lives on `Synced`.
#[tokio::test]
async fn a_catalog_with_live_pages_that_starts_a_sync_keeps_ready_true() {
    let stem: &'static str = Box::leak(periodic_stem().into_boxed_str());
    let job_path: &'static str = Box::leak(format!("/jobs/{stem}").into_boxed_str());
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        // The PREVIOUS generation's Job, still present and already harvested.
        route("GET", job_path, job_body(stem, true, "sha256:x", UID)),
        route("GET", "/backupdestinations/archive", destination_body()),
        route("POST", "/configmaps", empty_config_map()),
        route("POST", "/jobs", empty_job()),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    let status = json!({
        "observedGeneration": 1,
        "observedSyncRequest": "t1",
        "syncedAt": "2026-09-16T11:55:00Z",
        "viewExpiresAt": "2026-09-16T14:55:00Z",
        "truncated": false,
        "pages": [{"configMapName": "p0", "index": 0, "count": 2}],
        "indexConfigMap": "idx",
        "lastSyncJob": {"name": stem, "finishedAt": "2026-09-16T11:55:00Z"},
        "conditions": [{"type": "Synced", "status": "True", "reason": "Succeeded"}]
    });
    let outcome = run(&f, &catalog(json!({"syncRequest": "t2"}), status)).await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Started);
    assert_eq!(
        outcome.ready, "True",
        "D3 §5.3 defines `Ready` as `a usable view exists right now`, and one does throughout \
         the sync. Hardcoding `Unknown` made it flap True -> Unknown -> True every hour."
    );
    assert_eq!(outcome.ready_reason, ctrl::REASON_VIEW_READY);
    assert_eq!(outcome.synced_reason, ctrl::REASON_SYNC_IN_PROGRESS);
    let patch = f.patched_status()["status"].clone();
    assert_condition(&patch, ctrl::CONDITION_READY, "True");
    assert_condition(&patch, ctrl::CONDITION_SYNCED, "Unknown");
    assert!(
        patch.get("pages").is_none(),
        "a live view is neither cleared nor rewritten by starting a sync: {patch}"
    );
}

/// **Review finding F8.** Pages and the fence pointer carry the CATALOG's UID,
/// so `kubectl get cm -l logweir.dev/catalog-uid=<uid>` finds every object of
/// one catalog — which is the triage the label exists for.
#[test]
fn every_object_is_labelled_with_the_catalogs_uid_and_not_the_jobs() {
    let trust = trust_with(TRUSTED_KEY, TrustKeyState::Active, None);
    let built = view::materialise(
        vec![entry(
            "lwp1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            1,
            "s3://b/p",
        )],
        1,
        &trust,
        &ViewLimits {
            view_limit: 10,
            page_max_bytes: view::PAGE_MAX_BYTES,
            max_pages: 8,
        },
        now(),
    );
    let job_owner = weirkeeper::job::RunnerOwner {
        api_version: "batch/v1".to_string(),
        kind: "Job".to_string(),
        name: "sync".to_string(),
        uid: JOB_UID.to_string(),
    };
    let catalog_owner = weirkeeper::job::RunnerOwner {
        api_version: "logweir.dev/v1alpha1".to_string(),
        kind: "RecoveryCatalog".to_string(),
        name: NAME.to_string(),
        uid: UID.to_string(),
    };
    let page = view::page_config_map("p0", NS, &job_owner, UID, &built.pages[0], 0);
    let document = view::index_document(
        "g",
        &ViewLimits::from_settings(&catalog(json!({}), json!({})).spec.sync),
        &built,
        &["p0".to_string()],
    );
    let index = view::index_config_map("idx", NS, &job_owner, UID, &document).expect("serialises");
    let trust_cm = view::trust_config_map("t", NS, &catalog_owner, &trust);
    for (what, cm) in [("page", &page), ("index", &index), ("trust", &trust_cm)] {
        let labels = cm.metadata.labels.as_ref().expect("labelled");
        assert_eq!(
            labels[view::LABEL_CATALOG_UID],
            UID,
            "the {what} ConfigMap must carry the CATALOG's uid; keyed on the Job it changed \
             every slot and matched nothing but that slot"
        );
    }
    // And ownership is still the Job's for the two the TTL collects.
    for cm in [&page, &index] {
        assert_eq!(
            cm.metadata.owner_references.as_ref().expect("owned")[0].uid,
            JOB_UID
        );
    }
}

// ===========================================================================
// 10. Guards over the source itself
// ===========================================================================

/// **No `delete` verb, anywhere in this task's source.** The `ClusterRole`
/// grants it on nothing and the whole garbage-collection design depends on that
/// staying true.
#[test]
fn this_tasks_source_calls_no_delete_and_names_no_delete_verb() {
    for file in ["src/catalog_view.rs", "src/controllers/recovery_catalog.rs"] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(file);
        let src = std::fs::read_to_string(&path).expect("a readable source file");
        for needle in [".delete(", ".delete_opt(", "DeleteParams"] {
            assert!(
                !src.contains(needle),
                "{file} contains `{needle}`. D3 §5.3 and §9: this decision adds NO delete \
                 permission anywhere — the view is collected by the sync Job's TTL and by \
                 ownerReference garbage collection, which is the entire reason the pages are \
                 owned by the Job."
            );
        }
    }
}

/// Every condition reason this reconciler writes is a valid
/// `metav1.Condition.reason`.
#[test]
fn every_condition_reason_is_a_valid_metav1_reason() {
    for reason in ctrl::CONDITION_REASONS {
        assert!(
            !reason.is_empty()
                && reason
                    .chars()
                    .next()
                    .expect("non-empty")
                    .is_ascii_alphabetic()
                && reason
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ',' | ':')),
            "`{reason}` is not `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`, which the API server \
             validates a condition reason against"
        );
    }
    // The four types, and no duplicates.
    let mut types = ctrl::CONDITION_TYPES.to_vec();
    types.sort_unstable();
    types.dedup();
    assert_eq!(types.len(), 4);
}

// ===========================================================================
// Helpers
// ===========================================================================

fn assert_no_delete(f: &Fixture) {
    let seen = f.seen();
    assert!(
        !seen.iter().any(|(m, _)| m == "DELETE"),
        "this controller deletes nothing: {seen:?}"
    );
}

fn assert_condition(status: &Value, r#type: &str, want: &str) {
    let conditions = status["conditions"]
        .as_array()
        .unwrap_or_else(|| panic!("no conditions in {status}"));
    let got = conditions
        .iter()
        .find(|c| c["type"] == r#type)
        .unwrap_or_else(|| panic!("no `{type}` condition in {status}"));
    assert_eq!(
        got["status"], want,
        "`{}` is {} and not {want}: {got}",
        r#type, got["status"]
    );
}

/// The frame expectations are read off the JOB, so the relay is verified
/// against the plan that actually ran.
#[test]
fn the_frame_expectations_come_from_the_job_that_ran() {
    let job: k8s_openapi::api::batch::v1::Job =
        serde_json::from_str(&job_body("j", true, "sha256:pinned", UID)).expect("a Job");
    assert_eq!(
        ctrl::frame_expectations(&job, UID),
        FrameExpectations {
            plan_sha256: "sha256:pinned".to_string(),
            subject_uid: UID.to_string(),
        }
    );
}

// ===========================================================================
// 12. The D2 seam: the runner's bytes, read by this parser
// ===========================================================================
//
// `weirkeeper` and `logweir` share no dependency edge — this crate links
// `logweir-core` and `logweir-verify`, never the runner — and adding one so a
// test could call the emitter would be a dependency decision, not a test. What
// the two crates DO share is a file: `crates/logweir/tests/check_cli.rs`'s
// `the_catalog_sync_body_is_pinned_for_the_controllers_parser` asserts the
// emitter produces `PINNED_SYNC_BODY` byte for byte, so that literal is a
// pinned statement of the runner's behaviour. Reading it here closes the loop —
// the runner test pins runner <-> literal, and this one pins literal <-> parser
// — and it is the pattern D2 W9 used against the same file for the expected
// row set.

/// The runner's pinned body, read out of `crates/logweir/tests/check_cli.rs`.
fn runner_pinned_body() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the repository root")
        .join("crates/logweir/tests/check_cli.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("could not read {}: {e}", path.display()));
    const OPEN: &str = "const PINNED_SYNC_BODY: &str = r#\"";
    let at = source.find(OPEN).unwrap_or_else(|| {
        panic!(
            "`PINNED_SYNC_BODY` is gone from {}; the runner no longer pins the body this parser \
             is the oracle for",
            path.display()
        )
    });
    let rest = &source[at + OPEN.len()..];
    let end = rest.find("\"#;").expect("the raw literal closes");
    let body = rest[..end].to_string();
    assert!(
        body.lines().count() >= 6 && body.starts_with("catalog-format="),
        "the extracted literal is not a body; this guard would pass vacuously: {body:?}"
    );
    body
}

/// **THE CROSS-CRATE GUARD.** The bytes D2's runner emits are bytes this
/// parser reads, and it reads the two axes out of them as the states the
/// controller then classifies.
#[test]
fn the_runners_pinned_body_is_one_this_parser_reads() {
    let body = runner_pinned_body();
    let parsed = view::parse_body(&body, 2000).unwrap_or_else(|e| {
        panic!("the runner's own pinned body does not parse here: {e}\n{body}")
    });

    assert_eq!(parsed.pages.len(), 1);
    assert_eq!(
        parsed.skipped_entries, 0,
        "no entry line was skipped: {body}"
    );
    let entries = parsed.entries();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry.availability, Availability::Available);
    assert_eq!(
        entry.signature,
        SignatureVerdict::NotAttempted,
        "the runner reports a verdict and never a trust decision"
    );
    assert!(entry.point_id.starts_with("lwp1-"));
    assert!(entry.receipt_sha256.starts_with("sha256:"));
    assert_eq!(
        entry.receipt_sha256.len(),
        "sha256:".len() + 64,
        "THE BINDING SURVIVED THE RUNNER'S REDACTOR. `redact`'s long-run rule eats any 40-plus \
         character hex run, so a digest that came through as `[redacted]` would make the body \
         parse and mean nothing: {entry:?}"
    );
    assert_eq!(entry.locations.len(), 1);
    assert_eq!(
        entry.locations[0].availability,
        Some(Availability::Available),
        "a location carries its OWN verdict"
    );
    assert!(
        entry.recorded_at.is_some(),
        "the record instant round-trips into `Time`"
    );
    assert!(entry.remedy.is_some());

    let counts = parsed.counts.expect("a counts line");
    assert_eq!(counts.total, 1);
    assert_eq!(counts.available, 1);
    assert_eq!(counts.signature.not_attempted, 1);
    assert_eq!(counts.by_day.len(), 1);

    assert_eq!(parsed.signers.len(), 1);
    assert_eq!(parsed.signers[0].points, 1);
    assert!(
        view::MAX_BODY_SIGNERS >= parsed.signers.len(),
        "the signer list is inside the cap"
    );

    let cursor = parsed.cursor.expect("a cursor line");
    assert!(cursor.complete);
    assert!(cursor.index_shard.is_some());

    // And the whole pipeline the controller runs over it produces a page.
    let trust = TrustView::default();
    let view = view::materialise(
        entries,
        counts.total,
        &trust,
        &ViewLimits::from_settings(&catalog(json!({}), json!({})).spec.sync),
        now(),
    );
    assert_eq!(view.entries, 1);
    assert_eq!(view.pages.len(), 1);
    let published = &view.pages[0].entries[0];
    assert_eq!(
        published.verification,
        Verification::NotAttempted,
        "with no trust material an installation has not disproved anything"
    );
    assert!(!published.selectable);
}

/// The same body, seen at a SECOND location: one entry, both locations, and
/// availability merged BEST-of.
#[test]
fn the_runners_entry_merges_with_a_second_location_into_one_row() {
    let body = runner_pinned_body();
    let parsed = view::parse_body(&body, 2000).expect("the pinned body parses");
    let here = parsed.entries().remove(0);
    let mut copy = here.clone();
    copy.locations = vec![EntryLocation {
        location_id: "s3://lw-archive-dr/kafka-backups".to_string(),
        availability: None,
    }];
    copy.availability = Availability::Missing;

    let merged = view::merge_entries(vec![here.clone(), copy]);
    assert_eq!(merged.len(), 1, "one receipt in two buckets is ONE point");
    assert_eq!(
        merged[0].availability,
        Availability::Available,
        "availability merges BEST-of: hiding a recoverable point because a second copy went \
         missing is the opposite of what a second copy is for"
    );
    let ids: Vec<&str> = merged[0]
        .locations
        .iter()
        .map(|l| l.location_id.as_str())
        .collect();
    assert_eq!(
        ids,
        vec![
            "s3://lw-archive-dr/kafka-backups",
            here.locations[0].location_id.as_str()
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>(),
        "both places are named: {:?}",
        merged[0].locations
    );
    assert!(
        merged[0]
            .remedy
            .as_deref()
            .is_some_and(|r| r.contains("s3://lw-archive-dr/kafka-backups")),
        "the degraded copy is NAMED, so a best-of answer never costs the knowledge of which \
         bucket to repair: {:?}",
        merged[0].remedy
    );
}

/// **THE OTHER HALF OF THE SEAM.** The plan this controller renders is one the
/// runner's own `CheckPlan::parse_and_verify` accepts, at the timeout this
/// controller actually uses.
///
/// It is a REAL guard and not a formality: `SYNC_TIMEOUT_SECONDS` is 900 and
/// the contract's ceiling was a flat 600 for every kind, so every sync Job on
/// every cadence would have been refused at startup step 4 with
/// `CheckContractMismatch` — before a credential was read, with no frame
/// printed, and reported as `ResultUnreadable` with no way to say why.
#[test]
fn the_controllers_sync_plan_is_one_the_runner_accepts() {
    let request = sync_request();
    let bytes = view::plan_document(
        UID,
        u32::try_from(ctrl::SYNC_TIMEOUT_SECONDS).expect("the sync timeout fits a u32"),
        Some("sha256:deadbeef"),
        &request,
    )
    .expect("the plan serialises");
    let digest = logweir_core::ids::sha256_prefixed(&bytes);
    let plan = logweir_core::check_contract::CheckPlan::parse_and_verify(&bytes, &digest, UID)
        .unwrap_or_else(|e| {
            panic!(
                "the runner refuses the plan this controller writes: {e}\n{}",
                String::from_utf8_lossy(&bytes)
            )
        });
    assert_eq!(plan.kind(), CheckPlanKind::CatalogSync);
    let logweir_core::check_contract::CheckRequest::CatalogSync(got) = plan.request else {
        panic!("the plan carries a `catalogSync` request");
    };
    assert_eq!(*got, request, "every field round-trips");
}

/// The CRD's two enums and the plan's two enums spell the same strings.
///
/// The controller keeps its own `SyncMode`/`DeepCheck` because they carry
/// `JsonSchema` and the published CRD is generated from them; the `From` impls
/// are the whole translation, and a rename on either side that changed a wire
/// spelling would make an operator's `spec.sync.mode: Full` arrive at the
/// runner as something else.
#[test]
fn the_crd_and_the_plan_spell_the_two_enums_identically() {
    use logweir_core::check_contract::{CatalogDeepCheck, CatalogSyncMode};
    use weirkeeper::crds::recovery_catalog::{DeepCheck, SyncMode};
    for mode in [SyncMode::Index, SyncMode::Full] {
        let plan: CatalogSyncMode = mode.into();
        assert_eq!(
            serde_json::to_value(mode).expect("the CRD enum serialises"),
            serde_json::to_value(plan).expect("the plan enum serialises"),
            "`{mode:?}` is spelled two ways"
        );
    }
    for deep in [
        DeepCheck::None,
        DeepCheck::ManifestDigest,
        DeepCheck::SegmentSample,
    ] {
        let plan: CatalogDeepCheck = deep.into();
        assert_eq!(
            serde_json::to_value(deep).expect("the CRD enum serialises"),
            serde_json::to_value(plan).expect("the plan enum serialises"),
            "`{deep:?}` is spelled two ways"
        );
    }
}

// ===========================================================================
// CATALOG-TRUST-ROSTER-ONLY — the catalog judges points by the trust bound to
// its namespace (`trust::resolve`), not by the `TrustRoster` alone
// ===========================================================================

/// A REAL Ed25519 public key (SubjectPublicKeyInfo PEM) — public material, the
/// same one `trust_policy_controller.rs` and `rehearsal_controller.rs` use —
/// so the policy's key is USABLE: it parses and hashes to its declared id.
const POLICY_KEY_PEM: &str =
    "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEApFpEU8uY5S8Lv43HL4DcXKKyM8WHurCPZIxvq8ZBfpY=\n-----END PUBLIC KEY-----\n";
/// `sha256(POLICY_KEY_PEM's SPKI DER)`, lowercase hex.
const POLICY_KEY: &str = "f27c7f51aad0700db76887b306d413a039156b44ee147c1d82c5e4dc339558f6";
const POLICY_NAME: &str = "team-a-trust";

/// 2026-03-01T00:00:00Z — signed well inside every fixture key's life.
const EARLY_MS: i64 = 1_772_323_200_000;
/// 2026-08-01T00:00:00Z — after the fixtures' 2026-06-01 retirement.
const LATE_MS: i64 = 1_785_542_400_000;

/// One `EvidenceSigning` key, `lifecycle` merged over an Active base.
fn policy_key(lifecycle: Value) -> Value {
    let mut key = json!({
        "keyId": POLICY_KEY,
        "spkiPem": POLICY_KEY_PEM,
        "algorithm": "ed25519",
        "usages": ["EvidenceSigning"],
        "principal": {"id": "install:team-a-runner"},
        "notBefore": "2025-01-01T00:00:00Z",
        "notAfter": "2027-06-01T00:00:00Z",
        "state": "Active"
    });
    for (k, v) in lifecycle.as_object().expect("an object") {
        key[k] = v.clone();
    }
    key
}

fn policy_value(name: &str, namespaces: &[&str], keys: Vec<Value>) -> Value {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "TrustPolicy",
        "metadata": {"name": name, "uid": format!("uid-{name}"), "generation": 1, "resourceVersion": "3"},
        "spec": {"namespaces": namespaces, "keys": keys}
    })
}

fn policy_list(items: Vec<Value>) -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "TrustPolicyList",
        "metadata": {"resourceVersion": "4"}, "items": items
    })
    .to_string()
}

/// The harvest table with `policies` answering the `TrustPolicy` LIST. The
/// roster route stays and still lists [`TRUSTED_KEY`]: a namespace a policy
/// governs must not consult it.
fn harvest_with_policies(body: &str, policies: String) -> Fixture {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let mut routes = harvest_routes(&plan_sha, framed(&plan_sha, UID, body), UID);
    let at = routes
        .iter()
        .position(|r| r.path_suffix == "/trustpolicies")
        .expect("the harvest table lists trust policies");
    routes[at] = route("GET", "/trustpolicies", policies);
    fixture(routes)
}

/// Two points signed by [`POLICY_KEY`] (early, late) and one by the roster's
/// [`TRUSTED_KEY`].
fn policy_signed_body() -> String {
    body_for(
        &[vec![
            entry_value(
                "lwp1-cccccccccccccccccccccccccccccccc",
                LATE_MS,
                "Available",
                "verified",
                POLICY_KEY,
            ),
            entry_value(
                "lwp1-dddddddddddddddddddddddddddddddd",
                EARLY_MS,
                "Available",
                "verified",
                POLICY_KEY,
            ),
            entry_value(
                "lwp1-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                EARLY_MS - 1,
                "Available",
                "verified",
                TRUSTED_KEY,
            ),
        ]],
        counts_value(3, 3),
        json!([
            {"keyId": POLICY_KEY, "points": 2, "principalHint": "team-a runner"},
            {"keyId": TRUSTED_KEY, "points": 1, "principalHint": "runner"}
        ]),
        true,
    )
}

/// `pointId → (verification, selectable)` from the one page this sync wrote.
fn page_verdicts(f: &Fixture) -> BTreeMap<String, (String, bool)> {
    let page = f
        .posted("/configmaps")
        .into_iter()
        .find(|cm| cm["data"][view::PAGE_DATA_KEY].is_string())
        .expect("a page was written");
    page["data"][view::PAGE_DATA_KEY]
        .as_str()
        .expect("entries")
        .lines()
        .map(|line| {
            let e: Value = serde_json::from_str(line).expect("an entry");
            (
                e["pointId"].as_str().expect("id").to_string(),
                (
                    e["verification"]
                        .as_str()
                        .expect("verification")
                        .to_string(),
                    e["selectable"].as_bool().expect("selectable"),
                ),
            )
        })
        .collect()
}

fn signer_trusted(status: &Value, key: &str) -> Option<bool> {
    status["signers"]
        .as_array()?
        .iter()
        .find(|s| s["keyId"] == key)
        .and_then(|s| s["trusted"].as_bool())
}

fn verdict(v: &str, selectable: bool) -> (String, bool) {
    (v.to_string(), selectable)
}

/// **The lab-refresh-8 row, as a unit.** A point signed under a `TrustPolicy`
/// key is `Verified` and selectable, the policy names itself on
/// `TrustAvailable`, and the roster the policy displaced is NOT consulted — its
/// key is an untrusted signer here.
///
/// KILLS: `trust_view` reading only the `TrustRoster` (the defect) — the
/// policy-signed points become `UntrustedSigner` and the roster-signed one
/// `Verified`.
#[tokio::test]
async fn a_point_signed_under_a_trust_policy_key_is_verified() {
    let f = harvest_with_policies(
        &policy_signed_body(),
        policy_list(vec![policy_value(
            POLICY_NAME,
            &[NS],
            vec![policy_key(json!({}))],
        )]),
    );
    run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;

    let verdicts = page_verdicts(&f);
    assert_eq!(
        verdicts["lwp1-cccccccccccccccccccccccccccccccc"],
        verdict("Verified", true)
    );
    assert_eq!(
        verdicts["lwp1-dddddddddddddddddddddddddddddddd"],
        verdict("Verified", true)
    );
    assert_eq!(
        verdicts["lwp1-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"],
        verdict("UntrustedSigner", false),
        "a governed namespace is judged by its policy and not by the roster"
    );
    let status = f.patched_status()["status"].clone();
    assert_eq!(signer_trusted(&status, POLICY_KEY), Some(true));
    assert_eq!(signer_trusted(&status, TRUSTED_KEY), Some(false));
    assert_condition(&status, ctrl::CONDITION_TRUST_AVAILABLE, "True");
    let trust = status["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == ctrl::CONDITION_TRUST_AVAILABLE)
        .expect("TrustAvailable")
        .clone();
    assert!(
        trust["message"]
            .as_str()
            .is_some_and(|m| m.contains(&format!("TrustPolicy/{POLICY_NAME}"))),
        "{trust}"
    );
}

/// A key the policy REVOKED for `KeyCompromise` turns every point it signed
/// `Revoked` — early or late — and its signer row untrusted.
///
/// KILLS: projecting a compromise revocation as anything but `Revoked` (the
/// points would stay `Verified`/`VerifiedHistorical` and selectable).
#[tokio::test]
async fn a_trust_policy_revoked_key_gives_revoked() {
    let revoked = policy_key(json!({
        "state": "Revoked",
        "revokedAt": "2026-09-01T00:00:00Z",
        "revocationReason": "KeyCompromise",
        "revocationEffectiveFrom": "2026-09-01T00:00:00Z"
    }));
    let f = harvest_with_policies(
        &policy_signed_body(),
        policy_list(vec![policy_value(POLICY_NAME, &[NS], vec![revoked])]),
    );
    run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;

    let verdicts = page_verdicts(&f);
    assert_eq!(
        verdicts["lwp1-cccccccccccccccccccccccccccccccc"],
        verdict("Revoked", false)
    );
    assert_eq!(
        verdicts["lwp1-dddddddddddddddddddddddddddddddd"],
        verdict("Revoked", false)
    );
    let status = f.patched_status()["status"].clone();
    assert_eq!(signer_trusted(&status, POLICY_KEY), Some(false));
}

/// A key the policy RETIRED still verifies what it signed before `retiredAt`
/// — `VerifiedHistorical`, selectable — and nothing it claims to have signed
/// after it.
///
/// KILLS: projecting a retired key as `Active` (both points `Verified`), and
/// bounding it by `notAfter` instead of `accepted_through()` (the late point
/// `VerifiedHistorical`).
#[tokio::test]
async fn a_trust_policy_retired_key_gives_verified_historical() {
    let retired = policy_key(json!({"state": "Retired", "retiredAt": "2026-06-01T00:00:00Z"}));
    let f = harvest_with_policies(
        &policy_signed_body(),
        policy_list(vec![policy_value(POLICY_NAME, &[NS], vec![retired])]),
    );
    run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;

    let verdicts = page_verdicts(&f);
    assert_eq!(
        verdicts["lwp1-dddddddddddddddddddddddddddddddd"],
        verdict("VerifiedHistorical", true),
        "signed before retiredAt"
    );
    assert_eq!(
        verdicts["lwp1-cccccccccccccccccccccccccccccccc"],
        verdict("Invalid", false),
        "claims a signing time after retiredAt"
    );
    let status = f.patched_status()["status"].clone();
    assert_eq!(
        signer_trusted(&status, POLICY_KEY),
        Some(true),
        "a retired key is still accepted for what it signed while valid"
    );
}

/// A namespace two policies claim resolves to NO trust: every point is
/// `NotAttempted`, and `TrustAvailable=False/TrustPolicyConflict` names both —
/// not "no key material", in a cluster that has two policies.
#[tokio::test]
async fn a_contested_namespace_verifies_nothing_and_names_the_conflict() {
    let f = harvest_with_policies(
        &policy_signed_body(),
        policy_list(vec![
            policy_value("policy-one", &[NS], vec![policy_key(json!({}))]),
            policy_value("policy-two", &[NS], vec![policy_key(json!({}))]),
        ]),
    );
    run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;

    for (point, (verification, selectable)) in page_verdicts(&f) {
        assert_eq!(verification, "NotAttempted", "{point}");
        assert!(!selectable, "{point}");
    }
    let status = f.patched_status()["status"].clone();
    let trust = status["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .find(|c| c["type"] == ctrl::CONDITION_TRUST_AVAILABLE)
        .expect("TrustAvailable")
        .clone();
    assert_eq!(trust["status"], "False");
    assert_eq!(trust["reason"], ctrl::REASON_TRUST_POLICY_CONFLICT);
    let message = trust["message"].as_str().expect("a message");
    assert!(
        message.contains("policy-one") && message.contains("policy-two"),
        "{message}"
    );
}

/// A policy for ANOTHER namespace does not govern this one: the catalog falls
/// back to the synthesised `legacy-roster-v1`, and the roster-signed point is
/// `Verified` exactly as it always was.
#[tokio::test]
async fn a_policy_for_another_namespace_leaves_the_roster_in_charge() {
    let f = harvest_with_policies(
        &policy_signed_body(),
        policy_list(vec![policy_value(
            POLICY_NAME,
            &["team-b"],
            vec![policy_key(json!({}))],
        )]),
    );
    run(&f, &catalog(json!({}), tracked_status(&periodic_stem()))).await;

    let verdicts = page_verdicts(&f);
    assert_eq!(
        verdicts["lwp1-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"],
        verdict("Verified", true)
    );
    assert_eq!(
        verdicts["lwp1-cccccccccccccccccccccccccccccccc"],
        verdict("UntrustedSigner", false)
    );
}

/// The trust BUNDLE the sync Job mounts is the governing policy's
/// `EvidenceSigning` keys — so the Job can verify a policy-signed receipt at
/// all. Approval-only keys are not in it.
#[tokio::test]
async fn the_sync_job_mounts_the_policys_signing_keys() {
    let mut approver = policy_key(json!({"usages": ["GovernedApproval"]}));
    approver["keyId"] = json!(STRANGER_KEY);
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route(
            "GET",
            "/trustpolicies",
            policy_list(vec![policy_value(
                POLICY_NAME,
                &[NS],
                vec![policy_key(json!({})), approver],
            )]),
        ),
        route("GET", "/backupdestinations/archive", destination_body()),
        route("POST", "/configmaps", empty_config_map()),
        route("POST", "/jobs", empty_job()),
        route(
            "PATCH",
            "/recoverycatalogs/primary/status",
            patched_catalog(),
        ),
    ]);
    run(&f, &catalog(json!({}), json!({}))).await;
    let bundle = f
        .posted("/configmaps")
        .into_iter()
        .find(|cm| cm["data"][view::TRUST_KEY_IDS_KEY].is_string())
        .expect("a trust bundle was written");
    assert_eq!(bundle["data"][view::TRUST_KEY_IDS_KEY], POLICY_KEY);
    assert_eq!(
        bundle["data"][view::TRUST_BUNDLE_KEY],
        POLICY_KEY_PEM.trim_end()
    );
}

/// **Roster-only behaviour is unchanged.** For a roster with no `TrustPolicy`
/// in the cluster, the projection of the synthesised `legacy-roster-v1` is the
/// projection the roster always had: the same keys in the same order, the same
/// public material (unparseable entries included, as before), the same
/// `notAfter`, `Active`, and therefore the same bundle bytes, the same bundle
/// name and the same verdict for every point.
///
/// KILLS: dropping unusable keys from the LEGACY projection (the roster's
/// fixture PEMs do not parse, so the view would empty and every point turn
/// `NotAttempted`); reading the synthesis's `9999-12-31` fallback as a real
/// `notAfter`.
#[test]
fn a_roster_only_namespace_projects_exactly_as_the_roster_did() {
    let expiry = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .single()
        .expect("instant");
    let spec: weirkeeper::crds::trust_roster::TrustRosterSpec = serde_json::from_value(json!({
        "approverKeys": [{"keyId": STRANGER_KEY, "spkiPem": spki(0xB2)}],
        "signingKeys": [
            {"keyId": TRUSTED_KEY, "spkiPem": spki(0xA1), "subject": "runner"},
            {"keyId": POLICY_KEY, "spkiPem": POLICY_KEY_PEM, "notAfter": expiry}
        ],
        "allowedClusterIds": []
    }))
    .expect("a roster spec");
    let before = TrustView::from_roster(&spec);
    let resolution = weirkeeper::trust::resolve_in(NS, &[], Some(&spec));
    let after = TrustView::from_resolution(&resolution);

    let shape = |v: &TrustView| -> Vec<(String, String, Option<DateTime<Utc>>, TrustKeyState)> {
        v.keys
            .iter()
            .map(|k| (k.key_id.clone(), k.spki_pem.clone(), k.not_after, k.state))
            .collect()
    };
    assert_eq!(shape(&after), shape(&before));
    assert_eq!(after.source, before.source);
    assert_eq!(after.unresolved, None);
    let owner = weirkeeper::job::RunnerOwner {
        api_version: "logweir.dev/v1alpha1".to_string(),
        kind: "RecoveryCatalog".to_string(),
        name: NAME.to_string(),
        uid: UID.to_string(),
    };
    assert_eq!(
        view::trust_config_map("n", NS, &owner, &after).data,
        view::trust_config_map("n", NS, &owner, &before).data,
        "the same bundle bytes"
    );
    for key in [TRUSTED_KEY, POLICY_KEY, STRANGER_KEY] {
        for at in [EARLY_MS, LATE_MS] {
            let signed = Utc.timestamp_millis_opt(at).single();
            assert_eq!(
                view::classify_verification(
                    SignatureVerdict::Verified,
                    Some(key),
                    signed,
                    &after,
                    now()
                ),
                view::classify_verification(
                    SignatureVerdict::Verified,
                    Some(key),
                    signed,
                    &before,
                    now()
                ),
                "{key} at {at}"
            );
        }
    }
    // And an ABSENT roster is what it was: no keys, the roster's name.
    let none = TrustView::from_resolution(&weirkeeper::trust::resolve_in(NS, &[], None));
    assert!(none.is_empty());
    assert_eq!(none.source, view::TRUST_SOURCE_ROSTER);
}

/// A supersession is a RETIREMENT at `revocationEffectiveFrom` (D3 §7.4, and
/// what `logweir_core::trust::decide` answers for the same key) — not a
/// compromise: what was signed before it stays `VerifiedHistorical`.
#[test]
fn a_superseded_revocation_is_a_retirement_at_its_effective_instant() {
    let policy: weirkeeper::crds::trust_policy::TrustPolicy = serde_json::from_value(policy_value(
        POLICY_NAME,
        &[NS],
        vec![policy_key(json!({
            "state": "Revoked",
            "revokedAt": "2026-09-01T00:00:00Z",
            "revocationReason": "Superseded",
            "revocationEffectiveFrom": "2026-06-01T00:00:00Z"
        }))],
    ))
    .expect("a policy");
    let trust = TrustView::from_resolution(&weirkeeper::trust::resolve_in(NS, &[policy], None));
    let at = |ms: i64| Utc.timestamp_millis_opt(ms).single();
    assert_eq!(
        view::classify_verification(
            SignatureVerdict::Verified,
            Some(POLICY_KEY),
            at(EARLY_MS),
            &trust,
            now()
        ),
        Verification::VerifiedHistorical
    );
    assert_eq!(
        view::classify_verification(
            SignatureVerdict::Verified,
            Some(POLICY_KEY),
            at(LATE_MS),
            &trust,
            now()
        ),
        Verification::Invalid
    );
}

/// **The re-trust trigger.** A `TrustPolicy` event must reach the catalogs in
/// the namespaces it could govern — a new key, a retirement or a revocation
/// changes `TrustAvailable` and what the next sync mounts and concludes — not
/// wait for the idle requeue. A SOURCE SCAN, for the reason
/// `approval_controller.rs`'s twin gives: `controller()` returns a future that
/// needs a live watch, and the property is structural.
///
/// KILLS: dropping the `.watches()` arm; mapping with the policy's CURRENT
/// scope only (a narrowing edit then wakes nothing in the namespace it stopped
/// governing); a second copy of the mapping rule.
#[test]
fn a_trust_policy_event_enqueues_the_catalogs_it_could_govern() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/controllers/recovery_catalog.rs"),
    )
    .expect("the reconciler's own source");
    let start = src
        .find("pub async fn controller(")
        .expect("the controller constructor");
    let body = &src[start..];
    assert!(
        body.contains(".watches(policy_api, watcher::Config::default()"),
        "a TrustPolicy event has to reach this controller. Body:\n{body}"
    );
    assert!(
        body.contains("policy_targets(&objects, &scopes, &policy)"),
        "…mapped over this controller's OWN store. Body:\n{body}"
    );
    assert!(
        !body.contains("reflector::reflector("),
        "no standalone reflector: a trigger watch inherits Controller's backoff, a hand-built \
         reflector stream does not"
    );
    let mapper = src
        .find("fn policy_targets(")
        .map(|i| &src[i..])
        .expect("the mapper");
    assert!(mapper.contains("scopes.observe(policy)"));
    assert!(mapper.contains("crate::verification::targets_in_scope("));
    let resolver = src
        .find("async fn trust_view(")
        .map(|i| &src[i..])
        .expect("trust_view");
    assert!(
        resolver[..resolver.find("\n}\n").unwrap_or(resolver.len())]
            .contains("crate::trust::resolve(client, namespace)"),
        "the catalog resolves the namespace's trust through the one resolver"
    );
}

/// The `TrustAvailable` reason vocabulary includes the conflict, so the
/// `metav1.Condition.reason` rules that walk [`ctrl::CONDITION_REASONS`] cover
/// it.
#[test]
fn the_conflict_reason_is_in_the_closed_vocabulary() {
    assert!(ctrl::CONDITION_REASONS.contains(&ctrl::REASON_TRUST_POLICY_CONFLICT));
    assert_eq!(
        ctrl::REASON_TRUST_POLICY_CONFLICT,
        weirkeeper::trust::REASON_TRUST_POLICY_CONFLICT
    );
}

/// **The view agrees with `logweir_core::trust::decide` — EXHAUSTIVELY over a
/// grid of key lifecycles × claimed signing times** (review MEDIUM-1). `decide`
/// is the judge the restore preflight (M-2) and the runner apply to the same
/// key; a view that is more permissive offers a green point the restore will
/// refuse. The grid covers every window row `decide` has: before `notBefore`,
/// inside the window, at and after `notAfter`/`retiredAt`/the revocation's
/// effective instant, AT `now`, just after `now` (F4, a claim in the future),
/// a key whose window has not opened yet (F3, a staged successor), and a point
/// with no claimed instant at all.
///
/// The expected value is derived from `decide`'s verdict independently of the
/// code under test: `Valid/Current` ↔ `Verified`, `Valid/Historical` ↔
/// `VerifiedHistorical`, `Untrusted/Revoked` ↔ `Revoked`, any other refusal ↔
/// `Invalid`.
///
/// KILLS: classifying a policy key without `decide` (the previous projection,
/// which read `Verified` for a future claim and for a staged key, and
/// `VerifiedHistorical` for a future claim under a future `retiredAt`);
/// projecting `Retired` as `Active`; ignoring `notBefore`.
#[test]
fn the_policy_projection_agrees_with_the_core_trust_decision() {
    use logweir_core::trust::{
        ClaimAbsence, EvidenceClaim, IndependentObservation, KeyUsage, TrustBasis, TrustResult,
        UntrustReason,
    };
    let now = now();
    let at = |s: &str| {
        DateTime::parse_from_rfc3339(s)
            .expect("an instant")
            .with_timezone(&Utc)
    };
    let lifecycles = [
        ("active", json!({})),
        ("expires-now", json!({"notAfter": now.to_rfc3339()})),
        ("expired", json!({"notAfter": "2026-06-01T00:00:00Z"})),
        ("staged-past", json!({"notBefore": "2026-04-01T00:00:00Z"})),
        (
            "staged-future",
            json!({"notBefore": "2026-11-01T00:00:00Z"}),
        ),
        (
            "retired-past",
            json!({"state": "Retired", "retiredAt": "2026-06-01T00:00:00Z"}),
        ),
        (
            "retired-future",
            json!({"state": "Retired", "retiredAt": "2026-12-01T00:00:00Z"}),
        ),
        (
            "revoked-compromise",
            json!({"state": "Revoked", "revokedAt": "2026-09-01T00:00:00Z",
                   "revocationReason": "KeyCompromise", "revocationEffectiveFrom": "2026-06-01T00:00:00Z"}),
        ),
        (
            "revoked-superseded",
            json!({"state": "Revoked", "revokedAt": "2026-09-01T00:00:00Z",
                   "revocationReason": "Superseded", "revocationEffectiveFrom": "2026-06-01T00:00:00Z"}),
        ),
        (
            "revoked-unspecified-future",
            json!({"state": "Revoked", "revokedAt": "2026-09-01T00:00:00Z",
                   "revocationEffectiveFrom": "2026-12-01T00:00:00Z"}),
        ),
    ];
    let claims: Vec<Option<DateTime<Utc>>> = vec![
        Some(at("2024-06-01T00:00:00Z")),
        Some(at("2026-03-01T00:00:00Z")),
        Some(at("2026-06-01T00:00:00Z")),
        Some(at("2026-08-01T00:00:00Z")),
        Some(now - chrono::Duration::seconds(1)),
        Some(now),
        Some(now + chrono::Duration::seconds(1)),
        Some(at("2026-11-15T00:00:00Z")),
        Some(at("2026-12-01T00:00:00Z")),
        None,
    ];
    let mut compared = 0;
    let mut refused_by_the_new_rows = 0;
    for (label, lifecycle) in lifecycles {
        let policy: weirkeeper::crds::trust_policy::TrustPolicy = serde_json::from_value(
            policy_value(POLICY_NAME, &[NS], vec![policy_key(lifecycle)]),
        )
        .expect("a policy");
        let weirkeeper::trust::Resolution::Trust(resolved) =
            weirkeeper::trust::resolve_in(NS, &[policy], None)
        else {
            panic!("{label}: the policy governs the namespace")
        };
        let projected = TrustView::from_resolved(&resolved);
        for signed in &claims {
            let claim = signed.map_or(
                EvidenceClaim::absent(ClaimAbsence::FieldAbsent),
                EvidenceClaim::at,
            );
            let decided = resolved.decide_for(
                POLICY_KEY,
                KeyUsage::EvidenceSigning,
                &claim,
                &IndependentObservation::none(),
                now,
            );
            let want = match (decided.result, decided.basis, decided.reason) {
                (TrustResult::Valid, TrustBasis::Current, _) => Verification::Verified,
                (TrustResult::Valid, TrustBasis::Historical, _) => Verification::VerifiedHistorical,
                (TrustResult::Untrusted, _, Some(UntrustReason::Revoked)) => Verification::Revoked,
                (TrustResult::Untrusted, _, Some(UntrustReason::SignedOutsideValidity)) => {
                    Verification::Invalid
                }
                other => {
                    panic!("{label} at {signed:?}: a verdict this table does not map: {other:?}")
                }
            };
            let got = view::classify_verification(
                SignatureVerdict::Verified,
                Some(POLICY_KEY),
                *signed,
                &projected,
                now,
            );
            assert_eq!(got, want, "{label}, claim {signed:?}");
            compared += 1;
            if want == Verification::Invalid
                && (signed.is_some_and(|t| t > now) || label == "staged-future")
            {
                refused_by_the_new_rows += 1;
            }
        }
    }
    assert_eq!(compared, 100, "the whole grid was compared");
    assert!(
        refused_by_the_new_rows >= 10,
        "the grid exercises the future-claim and unopened-key rows: {refused_by_the_new_rows}"
    );
    // The reviewer's three probes, by name.
    let probe = |lifecycle: Value, claim: &str| {
        let policy: weirkeeper::crds::trust_policy::TrustPolicy = serde_json::from_value(
            policy_value(POLICY_NAME, &[NS], vec![policy_key(lifecycle)]),
        )
        .expect("a policy");
        let trust = TrustView::from_resolution(&weirkeeper::trust::resolve_in(NS, &[policy], None));
        view::classify_verification(
            SignatureVerdict::Verified,
            Some(POLICY_KEY),
            Some(at(claim)),
            &trust,
            now,
        )
    };
    assert_eq!(
        probe(json!({}), "2026-12-01T00:00:00Z"),
        Verification::Invalid
    );
    assert_eq!(
        probe(
            json!({"notBefore": "2026-11-01T00:00:00Z"}),
            "2026-12-01T00:00:00Z"
        ),
        Verification::Invalid
    );
    assert_eq!(
        probe(
            json!({"state": "Retired", "retiredAt": "2026-12-01T00:00:00Z"}),
            "2026-11-15T00:00:00Z"
        ),
        Verification::Invalid
    );
}

/// **A synced store costs no LIST** (review LOW-5). Handed the process-wide
/// `TrustPolicy` snapshot, the pass resolves the namespace from it: the route
/// table has NO `/trustpolicies` route, so a pass that listed anyway would
/// panic the double — and the policy-signed point is still `Verified`.
///
/// KILLS: `trust_view` ignoring the snapshot (it LISTs, and the double
/// refuses the unrecorded route).
#[tokio::test]
async fn a_synced_policy_store_resolves_the_catalog_without_a_list() {
    let plan_sha = format!("sha256:{}", "4".repeat(64));
    let routes: Vec<Route> = harvest_routes(
        &plan_sha,
        framed(&plan_sha, UID, &policy_signed_body()),
        UID,
    )
    .into_iter()
    .filter(|r| r.path_suffix != "/trustpolicies")
    .collect();
    let f = fixture(routes);
    let policies: Vec<weirkeeper::crds::trust_policy::TrustPolicy> = vec![serde_json::from_value(
        policy_value(POLICY_NAME, &[NS], vec![policy_key(json!({}))]),
    )
    .expect("a policy")];
    let policy = check::policy::Policy::defaults();
    let image = RunnerImage::default();
    ctrl::reconcile_catalog(
        &catalog(json!({}), tracked_status(&periodic_stem())),
        &ctrl::SyncContext {
            client: &f.client,
            policy: &policy,
            runner_image: &image,
            now: now(),
            trust_policies: Some(&policies),
            peers: Some(&[]),
        },
    )
    .await
    .expect("the reconcile reaches a verdict");
    assert!(
        !f.seen().iter().any(|(_, u)| u.contains("/trustpolicies")),
        "no TrustPolicy LIST: {:?}",
        f.seen()
    );
    assert_eq!(
        page_verdicts(&f)["lwp1-cccccccccccccccccccccccccccccccc"],
        verdict("Verified", true)
    );
}
