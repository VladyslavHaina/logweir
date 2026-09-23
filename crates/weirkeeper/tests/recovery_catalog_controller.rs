//! `CATALOG-RESYNC-NOT-HARVESTED` — the rows that make `spec.syncRequest` and
//! `spec.sync.intervalSeconds` mean what `docs/kubernetes.md` §7d says they
//! mean, and that make `Synced` usable as a completion signal.
//!
//! # What D3 W14 measured live, and what each row here holds
//!
//! On docker-desktop (`claude/artifacts/d3-live/20260918t0316z/`) a second
//! `spec.syncRequest` ran a sync Job to `Complete` — 19 of 19 of them — that
//! the controller never harvested: after 480 s the object still read
//! `Synced=Unknown/PodNotStarted` with the PREVIOUS view published, and an
//! `intervalSeconds: 300` catalog behaved the same past its interval. A
//! catalog's view could only be refreshed by deleting and re-creating the
//! object. Two causes, one row each, and a mutant for each:
//!
//! 1. **`status.lastSyncJob` was MERGED, not replaced.** The `/status` write is
//!    an RFC 7386 merge patch and a merge patch recurses into an object, so
//!    `{"lastSyncJob": {"name": <new>}}` over a harvested record kept the
//!    PREVIOUS Job's `finishedAt`. `reconcile_catalog` decides "this Job has
//!    been read" from exactly that field, so every sync after the first was
//!    born already looking harvested.
//!    Rows: `a_second_sync_request_is_harvested_and_publishes_a_new_view`,
//!    `the_started_record_names_every_field_of_last_sync_job`.
//! 2. **A completed sync did not serve its own interval slot.** A request that
//!    finished at 11:55 was followed one reconcile later by the 12:00 slot's
//!    Job, whose `report_running` pass wrote `Synced=Unknown/PodNotStarted`
//!    over the `Synced=True/Succeeded` the publish had just written — the
//!    write that flipped the condition, and why the live harness had to watch
//!    `status.syncedAt` instead.
//!    Rows: `a_completed_sync_serves_its_interval_slot_and_synced_stays_true`,
//!    `the_next_slot_starts_one_sync_and_it_is_harvested`,
//!    `a_failed_sync_spends_its_slot_and_is_not_re_created_every_pass`.
//!
//! Both writes of a `lastSyncJob` record — `start`'s and the harvest's — are
//! held to naming every field of the struct, by
//! `the_started_record_names_every_field_of_last_sync_job` and
//! `the_harvested_record_names_every_field_of_last_sync_job`. The harvest one
//! is reachable only on the upgrade path, where a harvest is not preceded by
//! this build's `start`.
//!
//! EVERY TEST HERE IS A `mock_client` TEST. Nothing dials a socket and nothing
//! waits on a Job; the double PANICS on a request it holds no route for, which
//! is what makes "no second Job was created" and "the pod log was never read"
//! mean *the reconciler did not ask*. The live proof of the fixed behaviour is
//! owed to the next lab run and is not claimed here.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone as _, Utc};
use serde_json::{json, Value};

use logweir_core::check_contract::{frames, Stream};
use weirkeeper::catalog_view as view;
use weirkeeper::catalog_view::SyncTrigger;
use weirkeeper::check;
use weirkeeper::conditions::apply_merge_patch;
use weirkeeper::controllers::recovery_catalog as ctrl;
use weirkeeper::crds::recovery_catalog::{LastSyncJob, RecoveryCatalog};
use weirkeeper::job::RunnerImage;
use weirkeeper::testing::{mock_client_recording_bodies, Recorder, Route, SeenBody};

// ===========================================================================
// Fixtures
// ===========================================================================

/// This task's namespace (STANDING RULE 13).
const NS: &str = "logweir-catalog-resync";
const NAME: &str = "primary";
const UID: &str = "c47a1f00-0000-4000-8000-0000000000c1";
const DEST: &str = "archive";
const DEST_UID: &str = "d0d0d0d0-0000-4000-8000-0000000000d1";
/// The first request's Job.
const JOB_UID_1: &str = "1b1b1b1b-0000-4000-8000-0000000000b1";
/// The second request's — a DIFFERENT Job, which is the whole point.
const JOB_UID_2: &str = "1b1b1b1b-0000-4000-8000-0000000000b2";

const TRUSTED_KEY: &str = "aa11bb22cc33dd44ee55ff6600778899aabbccddeeff00112233445566778899";

/// An hour, which is this fixture's `intervalSeconds`.
const INTERVAL: i32 = 3600;

/// 12:00:00 — the first instant of its slot, so "the slot has not rolled over"
/// and "it has" are both an exact hour away and neither is a rounding accident.
fn now() -> DateTime<Utc> {
    at(2026, 9, 16, 12, 0, 0)
}

fn at(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, s)
        .single()
        .expect("a real instant")
}

fn stamp(when: DateTime<Utc>) -> String {
    when.format("%Y-%m-%dT%H:%M:%SZ").to_string()
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
            "intervalSeconds": INTERVAL, "mode": "Index", "maxObjectsPerRun": 100000,
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

/// The Job name one request token produces.
fn request_stem(token: &str) -> String {
    view::sync_stem(UID, &SyncTrigger::Requested(token.to_string()).token())
}

/// The Job name the interval produces for the slot `when` falls in.
fn slot_stem(when: DateTime<Utc>) -> String {
    let slot = view::periodic_slot(when, INTERVAL).expect("an hourly catalog has slots");
    view::sync_stem(UID, &SyncTrigger::Periodic(slot).token())
}

/// A route path that outlives the fixture, which `Route::path_suffix` requires.
fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn job_path(stem: &str) -> &'static str {
    leak(format!("/jobs/{stem}"))
}

/// A sync Job, finished or running, owned by this catalog.
fn job_body(
    name: &str,
    uid: &str,
    plan_sha: &str,
    finished: Option<(DateTime<Utc>, DateTime<Utc>)>,
) -> String {
    let status = match finished {
        Some((started, completed)) => json!({
            "startTime": stamp(started),
            "completionTime": stamp(completed),
            "conditions": [{"type": "Complete", "status": "True"}]
        }),
        None => json!({"startTime": stamp(now()), "active": 1}),
    };
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": name, "namespace": NS, "uid": uid, "resourceVersion": "555",
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
                "name": NAME, "uid": UID, "controller": true, "blockOwnerDeletion": true
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
// The `catalogSync` result body the controller reads
// ---------------------------------------------------------------------------

fn entry_value(point: &str, at_ms: i64) -> Value {
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
        "availability": "Available",
        "signature": "verified",
        "signerKeyId": TRUSTED_KEY
    })
}

fn page_block(entries: &[Value]) -> String {
    let bodies: Vec<String> = entries
        .iter()
        .map(|e| serde_json::to_string(e).expect("an entry serialises"))
        .collect();
    let refs: Vec<&str> = bodies.iter().map(String::as_str).collect();
    let mut out = format!(
        "{}1/1 count={} sha256={}\n",
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

/// A body of `count` available, verified points — one page, walk complete.
fn body_with(count: i64) -> String {
    let entries: Vec<Value> = (0..count)
        .map(|i| {
            entry_value(
                &format!("lwp1-{}{}", "a".repeat(31), i),
                1_758_000_000_000 - i * 1_000,
            )
        })
        .collect();
    let counts = json!({
        "total": count, "available": count, "missing": 0, "unreadable": 0,
        "deleted": 0, "conflict": 0, "unsupportedFormat": 0, "partial": 0,
        "signature": {"verified": count, "invalid": 0, "noEvidence": 0, "notAttempted": 0},
        "byDay": [{"day": "2026-09-16", "points": count}]
    });
    let mut out = format!(
        "{}{}\n",
        view::FORMAT_LINE_PREFIX,
        view::BODY_FORMAT_VERSION
    );
    out.push_str(&page_block(&entries));
    out.push_str(&format!("{}{counts}\n", view::COUNTS_LINE_PREFIX));
    out.push_str(&format!(
        "{}{}\n",
        view::CURSOR_LINE_PREFIX,
        json!({"indexShard": "2026/09/16", "complete": true})
    ));
    out.push_str(&format!(
        "{}{}\n",
        view::SIGNERS_LINE_PREFIX,
        json!([{"keyId": TRUSTED_KEY, "points": count, "principalHint": "runner"}])
    ));
    out
}

/// D2's frames around a result body, as the runner would print them.
fn framed(plan_sha: &str, body: &str) -> String {
    let payload = body.as_bytes().to_vec();
    let parts = frames::write_parts(Stream::Details, &payload).expect("parts fit the frame bound");
    let mut streams: BTreeMap<Stream, (Vec<u8>, usize)> = BTreeMap::new();
    streams.insert(Stream::Details, (payload, parts.len()));
    let end = frames::end_frame(plan_sha, UID, &streams, None);
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

    fn status_patches(&self) -> Vec<Value> {
        self.bodies
            .lock()
            .expect("the body recorder")
            .iter()
            .filter(|b| b.method == "PATCH" && b.uri.contains("/recoverycatalogs/"))
            .map(|b| serde_json::from_str(&b.body).expect("JSON"))
            .collect()
    }

    /// The `status` of the one `/status` PATCH this pass sent.
    fn patched_status(&self) -> Value {
        let patches = self.status_patches();
        assert_eq!(
            patches.len(),
            1,
            "exactly one status write per pass; requests: {:?}",
            self.seen()
        );
        patches[0]["status"].clone()
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

    /// Whether this pass read a pod's log — the act of harvesting.
    ///
    /// The path is matched without its query string and against `/pods/…/log`
    /// and not against `"/log"`, which is a substring of every
    /// `/apis/logweir.dev/…` path there is.
    fn read_a_pod_log(&self) -> bool {
        self.seen().iter().any(|(_, u)| {
            let path = u.split('?').next().unwrap_or(u);
            path.contains("/pods/") && path.ends_with("/log")
        })
    }

    /// Whether this pass listed pods — the act of finding a Job's result.
    fn listed_pods(&self) -> bool {
        self.seen()
            .iter()
            .any(|(m, u)| m == "GET" && u.split('?').next().unwrap_or(u).ends_with("/pods"))
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

async fn run_at(
    fixture: &Fixture,
    catalog: &RecoveryCatalog,
    when: DateTime<Utc>,
) -> ctrl::Outcome {
    let policy = check::policy::Policy::defaults();
    let image = RunnerImage::default();
    ctrl::reconcile_catalog(
        catalog,
        &ctrl::SyncContext {
            client: &fixture.client,
            policy: &policy,
            runner_image: &image,
            now: when,
        },
    )
    .await
    .expect("the reconcile reaches a verdict")
}

fn empty_config_map() -> String {
    json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {}}).to_string()
}

/// What the API server answers a `POST /jobs` with: the object it created,
/// carrying the UID the plan `ConfigMap`'s ownerReference needs.
fn created_job(name: &str, uid: &str) -> String {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": name, "namespace": NS, "uid": uid,
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
                "name": NAME, "uid": UID, "controller": true, "blockOwnerDeletion": true
            }]
        }
    })
    .to_string()
}

fn patched_catalog() -> String {
    catalog_value(json!({}), json!({})).to_string()
}

fn status_route() -> Route {
    route(
        "PATCH",
        "/recoverycatalogs/primary/status",
        patched_catalog(),
    )
}

/// The status of a catalog that HAS published a view from the sync named by
/// `stem`, which finished at `finished`.
fn published_status(stem: &str, token: &str, finished: DateTime<Utc>) -> Value {
    json!({
        "observedGeneration": 1,
        "observedSyncRequest": token,
        "syncedAt": stamp(finished),
        "viewExpiresAt": stamp(finished + chrono::Duration::seconds(3 * i64::from(INTERVAL))),
        "truncated": false,
        "counts": {"total": 2, "available": 2},
        "pages": [{"configMapName": "old-p0", "index": 0, "count": 2,
                   "sha256": format!("sha256:{}", "e".repeat(64))}],
        "indexConfigMap": "old-index",
        "lastSyncJob": {
            "name": stem,
            "startedAt": stamp(finished - chrono::Duration::seconds(300)),
            "finishedAt": stamp(finished),
            "exitCode": 0
        },
        "conditions": [
            {"type": "Ready", "status": "True", "reason": "ViewReady",
             "message": "the bounded view is published and has not aged out",
             "observedGeneration": 1, "lastTransitionTime": stamp(finished)},
            {"type": "Synced", "status": "True", "reason": "Succeeded",
             "message": "the walk completed", "observedGeneration": 1,
             "lastTransitionTime": stamp(finished)},
            {"type": "Stale", "status": "False", "reason": "ViewFresh",
             "message": "fresh", "observedGeneration": 1,
             "lastTransitionTime": stamp(finished)},
            {"type": "TrustAvailable", "status": "True", "reason": "TrustMaterialPresent",
             "message": "1 signing key(s)", "observedGeneration": 1,
             "lastTransitionTime": stamp(finished)}
        ]
    })
}

/// Feed one pass's status write back onto the object, exactly as the API
/// server's RFC 7386 merge would — which is the whole mechanism this defect
/// lived in, so the chain is simulated and never hand-written.
fn merged(previous: &RecoveryCatalog, patch: &Value) -> Value {
    let mut status = previous
        .status
        .as_ref()
        .map_or(Value::Null, |s| serde_json::to_value(s).expect("status"));
    apply_merge_patch(&mut status, patch);
    status
}

fn condition<'a>(status: &'a Value, r#type: &str) -> &'a Value {
    status["conditions"]
        .as_array()
        .unwrap_or_else(|| panic!("no conditions in {status}"))
        .iter()
        .find(|c| c["type"] == r#type)
        .unwrap_or_else(|| panic!("no `{type}` condition in {status}"))
}

fn assert_no_delete(f: &Fixture) {
    let seen = f.seen();
    assert!(
        !seen.iter().any(|(m, _)| m == "DELETE"),
        "this controller deletes nothing: {seen:?}"
    );
}

/// The routes a HARVEST pass needs: the Job, its pod, the pod's log, the pages
/// it writes and the status write. `ran` is the Job's start and completion.
fn harvest_routes(
    stem: &'static str,
    job_uid: &str,
    plan_sha: &str,
    ran: (DateTime<Utc>, DateTime<Utc>),
    log: String,
) -> Vec<Route> {
    vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route(
            "GET",
            job_path(stem),
            job_body(stem, job_uid, plan_sha, Some(ran)),
        ),
        route("GET", "/pods", pod_list_body(job_uid)),
        route("GET", "/log", log),
        route("POST", "/configmaps", empty_config_map()),
        status_route(),
    ]
}

// ===========================================================================
// 1. The second `syncRequest` — the defect's first half
// ===========================================================================

/// **`CATALOG-RESYNC-NOT-HARVESTED`, the harvest half.** A catalog that has
/// already published one view, asked for a second sync, RUNS it and READS it:
/// the new Job is harvested, a new `syncedAt` is published and `Synced` is
/// `True` again.
///
/// Live, this was two reconciles that never happened. The start pass recorded
/// `lastSyncJob: {name: <new>}` as a merge patch over the first sync's
/// harvested record, so `finishedAt` from the FIRST Job stayed on the record of
/// the SECOND; `reconcile_catalog` reads that one field to decide a Job has
/// been read, so the second Job — `Complete`, its result sitting in its pod's
/// log — was never looked at. The chain here is the real one: pass 1's status
/// write is applied to the object with `apply_merge_patch` and pass 2 runs on
/// the result.
#[tokio::test]
async fn a_second_sync_request_is_harvested_and_publishes_a_new_view() {
    let first = leak(request_stem("token-1"));
    let second = leak(request_stem("token-2"));
    let published_at = at(2026, 9, 16, 11, 55, 0);

    // ---- pass 1: the second request starts its own Job ------------------
    let f1 = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route(
            "GET",
            job_path(first),
            job_body(
                first,
                JOB_UID_1,
                "sha256:first",
                Some((published_at - chrono::Duration::seconds(300), published_at)),
            ),
        ),
        route("GET", "/backupdestinations/archive", destination_body()),
        route("POST", "/configmaps", empty_config_map()),
        route("POST", "/jobs", created_job(second, JOB_UID_2)),
        status_route(),
    ]);
    let before = catalog(
        json!({"syncRequest": "token-2"}),
        published_status(first, "token-1", published_at),
    );
    let started = run_at(&f1, &before, now()).await;
    assert_eq!(started.phase, ctrl::CatalogPhase::Started);
    let jobs = f1.posted("/jobs");
    assert_eq!(jobs.len(), 1, "one request is one walk");
    assert_eq!(jobs[0]["metadata"]["name"], second);
    assert!(
        !f1.read_a_pod_log(),
        "the FIRST request's Job is finished and already harvested; a pass that read its log \
         again would republish a view the archive has already moved past: {:?}",
        f1.seen()
    );

    let patch = f1.patched_status();
    assert_eq!(patch["observedSyncRequest"], "token-2");
    assert_eq!(patch["lastSyncJob"]["name"], second);
    for field in ["finishedAt", "startedAt", "exitCode", "refusalReason"] {
        assert_eq!(
            patch["lastSyncJob"][field],
            Value::Null,
            "`lastSyncJob` is REPLACED and not merged into: a merge patch recurses, so leaving \
             `{field}` out keeps the PREVIOUS Job's value on the record of the new one, and \
             `finishedAt` is the one fact that says 'this Job has been read'. Got {patch}"
        );
    }

    // ---- pass 2: that Job completes and IS harvested ---------------------
    let mut next = catalog_value(json!({"syncRequest": "token-2"}), merged(&before, &patch));
    next["metadata"]["resourceVersion"] = json!("4243");
    let running: RecoveryCatalog = serde_json::from_value(next).expect("a catalog");
    assert!(
        running
            .status
            .as_ref()
            .and_then(|s| s.last_sync_job.as_ref())
            .and_then(|j| j.finished_at)
            .is_none(),
        "after the merge the new Job's record carries NO finishedAt — the state the harvest \
         arm keys on"
    );

    let plan_sha = format!("sha256:{}", "7".repeat(64));
    let f2 = fixture(harvest_routes(
        second,
        JOB_UID_2,
        &plan_sha,
        (now() - chrono::Duration::seconds(240), now()),
        framed(&plan_sha, &body_with(3)),
    ));
    let outcome = run_at(&f2, &running, now() + chrono::Duration::seconds(30)).await;
    assert_eq!(
        outcome.phase,
        ctrl::CatalogPhase::Published,
        "the second request's completed Job is harvested; live it never was, and an operator \
         could only refresh a view by deleting and re-creating the catalog"
    );
    assert_eq!(outcome.entries, 3);
    assert_eq!(outcome.ready, "True");

    let published = f2.patched_status();
    assert_eq!(
        published["syncedAt"],
        stamp(now()),
        "`syncedAt` is the NEW Job's completion time, not the first sync's {}: {published}",
        stamp(published_at)
    );
    assert_eq!(condition(&published, "Synced")["status"], "True");
    assert_eq!(condition(&published, "Synced")["reason"], "Succeeded");
    assert_eq!(published["lastSyncJob"]["name"], second);
    assert_eq!(published["lastSyncJob"]["finishedAt"], stamp(now()));
    assert_no_delete(&f2);
}

/// **The meta-guard on the fix.** `start` replaces `status.lastSyncJob` by
/// naming every one of its fields, and a field added to `LastSyncJob` without a
/// line there would resurrect `CATALOG-RESYNC-NOT-HARVESTED` silently — the new
/// field would carry the PREVIOUS Job's value forward. This reads the struct's
/// own serialisation, so the failure arrives with the field that caused it.
#[tokio::test]
async fn the_started_record_names_every_field_of_last_sync_job() {
    let stem = leak(request_stem("token-1"));
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", "/backupdestinations/archive", destination_body()),
        route("POST", "/configmaps", empty_config_map()),
        route("POST", "/jobs", created_job(stem, JOB_UID_1)),
        status_route(),
    ]);
    let outcome = run_at(
        &f,
        &catalog(json!({"syncRequest": "token-1"}), json!({})),
        now(),
    )
    .await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Started);

    assert_names_every_last_sync_job_field("start", &f.patched_status()["lastSyncJob"]);
}

/// **The same guard on the HARVEST write — review finding L1.** `start` is not
/// the only place a `lastSyncJob` record is written, and it is not the one that
/// matters on the path this branch added: a harvest is normally preceded by
/// this build's `start`, which has already cleared the record, but a catalog
/// stuck by the defect is harvested with NO such `start` in front of it. A
/// field the harvest closure does not name therefore survives from whatever the
/// stuck record held — a `refusalReason` from the one sync that did refuse,
/// republished beside `exitCode: 0` and `Synced=True/Succeeded`, describing two
/// different Jobs as one record.
#[tokio::test]
async fn the_harvested_record_names_every_field_of_last_sync_job() {
    let stem = leak(request_stem("token-1"));
    let plan_sha = format!("sha256:{}", "3".repeat(64));
    let f = fixture(harvest_routes(
        stem,
        JOB_UID_1,
        &plan_sha,
        (now() - chrono::Duration::seconds(240), now()),
        framed(&plan_sha, &body_with(1)),
    ));
    let outcome = run_at(
        &f,
        &catalog(
            json!({"syncRequest": "token-1"}),
            json!({
                "observedGeneration": 1,
                "observedSyncRequest": "token-1",
                "lastSyncJob": {"name": stem},
                "conditions": []
            }),
        ),
        now() + chrono::Duration::seconds(30),
    )
    .await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Published);
    assert_names_every_last_sync_job_field(
        "harvest (published)",
        &f.patched_status()["lastSyncJob"],
    );

    // ---- and the harvest whose Job supplies NEITHER ----------------------
    //
    // A SUCCESSFUL harvest writes `startedAt` and `exitCode` from the Job and
    // its pod, so the seeded nulls for those two are invisible on that path
    // and a mutant that drops them survives the case above. They are not
    // decoration: a Job with no `startTime`, whose pod is gone before the
    // controller reads it, supplies neither — and on the upgrade path the
    // stuck record's OWN `startedAt` and `exitCode`, from a different Job,
    // are what would survive in their place. This is that Job.
    let bare = leak(format!("{stem}-bare"));
    let f2 = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route(
            "GET",
            job_path(bare),
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {
                    "name": bare, "namespace": NS, "uid": JOB_UID_2,
                    "resourceVersion": "556",
                    "ownerReferences": [{
                        "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
                        "name": NAME, "uid": UID, "controller": true,
                        "blockOwnerDeletion": true
                    }]
                },
                "spec": {"template": {"spec": {"containers": [{
                    "name": "runner",
                    "env": [{"name": check::job::PLAN_SHA256_ENV, "value": "sha256:x"}]
                }]}}},
                // No `startTime`, no `completionTime` — finished by condition
                // alone, which is all `job_finished` reads.
                "status": {"conditions": [{"type": "Complete", "status": "True"}]}
            })
            .to_string(),
        ),
        // The pod is gone, so there is no relay and no exit code.
        route(
            "GET",
            "/pods",
            json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": []}).to_string(),
        ),
        status_route(),
    ]);
    let outcome = run_at(
        &f2,
        &catalog(
            json!({"syncRequest": "token-1"}),
            json!({
                "observedGeneration": 1,
                "observedSyncRequest": "token-1",
                "lastSyncJob": {"name": bare},
                "conditions": []
            }),
        ),
        now(),
    )
    .await;
    assert_eq!(
        outcome.phase,
        ctrl::CatalogPhase::Failed,
        "a finished Job with no pod has no relay, which is ResultUnreadable and not a guess"
    );
    let written = f2.patched_status()["lastSyncJob"].clone();
    assert_names_every_last_sync_job_field("harvest (no pod)", &written);
    assert_eq!(
        written["startedAt"],
        Value::Null,
        "the Job supplies no startTime, so the record must say so rather than keep another \
         Job's: {written}"
    );
    assert_eq!(written["exitCode"], Value::Null, "{written}");
    assert!(
        written["refusalReason"].is_string(),
        "the refusal that IS this Job's is still recorded: {written}"
    );
}

/// Every field `LastSyncJob` serialises must appear in `written`, by the
/// struct's own schema and not by a list typed here — a field added to the
/// struct without a line in the writer fails at the writer that forgot it.
fn assert_names_every_last_sync_job_field(writer: &str, written: &Value) {
    let every_field = serde_json::to_value(LastSyncJob {
        name: Some("j".to_string()),
        started_at: Some(now()),
        finished_at: Some(now()),
        exit_code: Some(0),
        refusal_reason: Some("Refused".to_string()),
    })
    .expect("a LastSyncJob serialises");
    for field in every_field.as_object().expect("an object").keys() {
        assert!(
            written.get(field).is_some(),
            "`{writer}` writes `lastSyncJob` as a MERGE patch, so a field it does not name \
             keeps the previous Job's value. `{field}` is on LastSyncJob and not in the patch: \
             {written}"
        );
    }
}

// ===========================================================================
// 2. The interval — the defect's second half, and the write that flipped
//    `Synced`
// ===========================================================================

/// **`CATALOG-RESYNC-NOT-HARVESTED`, the `Synced` half.** A sync that finished
/// inside an interval slot SERVES that slot: nothing new starts, and the
/// `Synced=True/Succeeded` the publish wrote stays.
///
/// THE WRITE THAT FLIPPED IT. `spec.syncRequest`'s Job is named after the
/// token, the interval's Job after the slot, and the pass that compared the
/// tracked name with the slot's name therefore matched nothing: one reconcile
/// after a requested sync published, the current slot's own Job was created,
/// and the NEXT pass — `report_running`, with no pod yet — patched
/// `Synced=Unknown/PodNotStarted` over it (`check::classify`'s default reason
/// for a Job that has not started). Live, that new Job was then never harvested
/// (the half above), so the condition stayed `Unknown/PodNotStarted` for 480 s
/// with a perfectly good view published, and the harness had to watch
/// `status.syncedAt` instead.
///
/// The route table holds NO `POST /jobs`: a pass that started one panics in the
/// double, which is this row's strongest assertion.
#[tokio::test]
async fn a_completed_sync_serves_its_interval_slot_and_synced_stays_true() {
    let stem = leak(request_stem("token-1"));
    // 12:02 — inside the 12:00 slot, two minutes after the request's Job
    // completed. This is exactly the live shape.
    let published_at = at(2026, 9, 16, 12, 2, 0);
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route(
            "GET",
            job_path(stem),
            job_body(
                stem,
                JOB_UID_1,
                "sha256:first",
                Some((published_at - chrono::Duration::seconds(120), published_at)),
            ),
        ),
        status_route(),
    ]);
    let outcome = run_at(
        &f,
        &catalog(
            json!({"syncRequest": "token-1"}),
            published_status(stem, "token-1", published_at),
        ),
        at(2026, 9, 16, 12, 3, 0),
    )
    .await;

    assert_eq!(
        outcome.phase,
        ctrl::CatalogPhase::Idle,
        "a walk that finished inside this slot IS the walk this slot asked for; \
         `intervalSeconds` is a cadence and not an alarm clock"
    );
    assert!(f.posted("/jobs").is_empty());
    assert_eq!(outcome.synced_reason, "Succeeded");
    assert_eq!(outcome.ready_reason, ctrl::REASON_VIEW_READY);
    let patches = f.status_patches();
    assert!(
        !patches.is_empty(),
        "this pass does write a status, so the assertions below are about something"
    );
    for patch in patches {
        let synced = condition(&patch["status"], "Synced");
        assert_eq!(
            synced["status"], "True",
            "no write after a publish may take `Synced` off `True` until a new request or a \
             new slot fires; live this became Unknown/PodNotStarted and the condition stopped \
             being usable as a completion signal: {synced}"
        );
        assert!(
            patch["status"].get("syncedAt").is_none(),
            "an idle pass republishes nothing: {patch}"
        );
    }
    assert!(
        !f.read_a_pod_log(),
        "nothing is re-harvested: {:?}",
        f.seen()
    );
    assert_no_delete(&f);
}

/// **A sync that ran and FAILED spends its slot too — review finding M1.**
///
/// `slot_already_served` reads `lastSyncJob.finishedAt` and NOT `syncedAt`, and
/// the difference is only visible here: a failed harvest records the Job but
/// never writes `syncedAt`, so a slot guard sourced from `syncedAt` would leave
/// a failed sync's slot unserved and re-create the Job on **every** reconcile
/// until the slot rolled over — a 60-second requeue against a destination that
/// just refused, with each Job's `ttlSecondsAfterFinished` piling the previous
/// ones up. The failure belongs on `Synced`, for an operator to act on, and not
/// in a retry loop this controller never bounds.
///
/// The catalog here has **no `syncedAt` at all** — its first sync is the one
/// that failed — so a guard reading `syncedAt` finds nothing to be served by.
/// The route table holds no `POST /jobs`: a pass that started one is refused by
/// name in the double.
#[tokio::test]
async fn a_failed_sync_spends_its_slot_and_is_not_re_created_every_pass() {
    let stem = leak(request_stem("token-1"));
    let failed_at = at(2026, 9, 16, 12, 2, 0);
    // A refusal, recorded: exit 3 with its `refusal-reason=`, no view, no
    // `syncedAt`, and `Synced=False` carrying the code.
    let status = json!({
        "observedGeneration": 1,
        "observedSyncRequest": "token-1",
        "lastSyncJob": {
            "name": stem,
            "startedAt": stamp(failed_at - chrono::Duration::seconds(60)),
            "finishedAt": stamp(failed_at),
            "exitCode": 3,
            "refusalReason": "DestinationUnreachable"
        },
        "conditions": [
            {"type": "Ready", "status": "Unknown", "reason": "NeverSynced",
             "message": "no sync has published a view yet; the durable catalog in object \
    storage is unaffected",
             "observedGeneration": 1, "lastTransitionTime": stamp(failed_at)},
            {"type": "Synced", "status": "False", "reason": "DestinationUnreachable",
             "message": "the destination refused the walk", "observedGeneration": 1,
             "lastTransitionTime": stamp(failed_at)}
        ]
    });

    let job = job_body(
        stem,
        JOB_UID_1,
        "sha256:first",
        Some((failed_at - chrono::Duration::seconds(60), failed_at)),
    );
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route("GET", job_path(stem), job),
        status_route(),
    ]);
    let outcome = run_at(
        &f,
        &catalog(json!({"syncRequest": "token-1"}), status.clone()),
        at(2026, 9, 16, 12, 3, 0),
    )
    .await;

    assert_eq!(
        outcome.phase,
        ctrl::CatalogPhase::Idle,
        "the slot is spent by the walk that RAN, not by the one that succeeded; a guard \
         sourced from `syncedAt` re-creates this Job on every pass for the rest of the hour"
    );
    assert!(f.posted("/jobs").is_empty());
    assert!(
        !f.read_a_pod_log(),
        "the failed Job was harvested once and is not read again: {:?}",
        f.seen()
    );
    let patches = f.status_patches();
    assert!(!patches.is_empty(), "this pass does write a status");
    for patch in patches {
        let synced = condition(&patch["status"], "Synced");
        assert_eq!(
            synced["status"], "False",
            "the refusal stands until something replaces it: {synced}"
        );
        assert_eq!(synced["reason"], "DestinationUnreachable");
    }

    // And the slot still rolls over: the failure delays the next walk by the
    // cadence, it does not end it.
    let periodic = leak(slot_stem(at(2026, 9, 16, 13, 0, 0)));
    let f2 = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route(
            "GET",
            job_path(stem),
            job_body(
                stem,
                JOB_UID_1,
                "sha256:first",
                Some((failed_at - chrono::Duration::seconds(60), failed_at)),
            ),
        ),
        route("GET", "/backupdestinations/archive", destination_body()),
        route("POST", "/configmaps", empty_config_map()),
        route("POST", "/jobs", created_job(periodic, JOB_UID_2)),
        status_route(),
    ]);
    let next = run_at(
        &f2,
        &catalog(json!({"syncRequest": "token-1"}), status),
        at(2026, 9, 16, 13, 0, 0),
    )
    .await;
    assert_eq!(next.phase, ctrl::CatalogPhase::Started);
    let jobs = f2.posted("/jobs");
    assert_eq!(
        jobs.len(),
        1,
        "one slot is one walk, after a failure as well"
    );
    assert_eq!(jobs[0]["metadata"]["name"], periodic);
}

/// The other side of the same rule: when the slot DOES roll over, exactly one
/// sync starts, `Synced` says so honestly, and the Job it created is harvested
/// into a new view.
///
/// A guard that only suppressed syncs would pass the row above and ship a
/// catalog that never re-syncs at all, which is the failure mode this defect
/// already had.
#[tokio::test]
async fn the_next_slot_starts_one_sync_and_it_is_harvested() {
    let previous = leak(request_stem("token-1"));
    let published_at = at(2026, 9, 16, 12, 2, 0);
    let next_slot_at = at(2026, 9, 16, 13, 0, 0);
    let periodic = leak(slot_stem(next_slot_at));

    // ---- the slot rolls over: one Job, named after the slot ---------------
    let f1 = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route(
            "GET",
            job_path(previous),
            job_body(
                previous,
                JOB_UID_1,
                "sha256:first",
                Some((published_at - chrono::Duration::seconds(120), published_at)),
            ),
        ),
        route("GET", "/backupdestinations/archive", destination_body()),
        route("POST", "/configmaps", empty_config_map()),
        route("POST", "/jobs", created_job(periodic, JOB_UID_2)),
        status_route(),
    ]);
    let before = catalog(
        json!({"syncRequest": "token-1"}),
        published_status(previous, "token-1", published_at),
    );
    let started = run_at(&f1, &before, next_slot_at).await;
    assert_eq!(
        started.phase,
        ctrl::CatalogPhase::Started,
        "the interval still fires once the slot the last sync served has passed"
    );
    let jobs = f1.posted("/jobs");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["metadata"]["name"], periodic);
    let patch = f1.patched_status();
    assert_eq!(
        condition(&patch, "Synced")["reason"],
        ctrl::REASON_SYNC_IN_PROGRESS,
        "a new slot firing IS a new sync, and saying so is honest; what the defect did was say \
         it with no sync anyone would ever read"
    );
    assert_eq!(
        condition(&patch, "Ready")["status"],
        "True",
        "the published view is still usable while its replacement runs"
    );
    assert_eq!(patch["lastSyncJob"]["finishedAt"], Value::Null);

    // ---- it finishes and is harvested into a new view --------------------
    let mut next = catalog_value(json!({"syncRequest": "token-1"}), merged(&before, &patch));
    next["metadata"]["resourceVersion"] = json!("4243");
    let running: RecoveryCatalog = serde_json::from_value(next).expect("a catalog");
    let plan_sha = format!("sha256:{}", "9".repeat(64));
    let finished_at = at(2026, 9, 16, 13, 4, 0);
    let f2 = fixture(harvest_routes(
        periodic,
        JOB_UID_2,
        &plan_sha,
        (next_slot_at, finished_at),
        framed(&plan_sha, &body_with(2)),
    ));
    let outcome = run_at(&f2, &running, finished_at + chrono::Duration::seconds(15)).await;
    assert_eq!(
        outcome.phase,
        ctrl::CatalogPhase::Published,
        "the interval re-sync is harvested; live, `intervalSeconds: 300` behaved exactly like \
         the second request and never was"
    );
    let published = f2.patched_status();
    assert_eq!(published["syncedAt"], stamp(finished_at));
    assert_eq!(condition(&published, "Synced")["status"], "True");
    assert_eq!(outcome.entries, 2);
}

// ===========================================================================
// 3. The stale Job
// ===========================================================================

/// **A Job that has already been harvested is never harvested twice.** The
/// tracked Job is still present — its TTL has not fired — and finished, and its
/// result is already published. A second harvest would re-POST the pages, write
/// a `syncedAt` the archive did not move for, and (with the pages owned by the
/// same Job) hide a genuine re-sync behind an unchanged status.
///
/// The route table holds no `/pods` and no `/log`: reading either is the
/// reconciler asking to re-harvest, and the double refuses by name.
#[tokio::test]
async fn a_stale_finished_job_is_never_harvested_twice() {
    let stem = leak(request_stem("token-1"));
    let published_at = at(2026, 9, 16, 12, 2, 0);
    let f = fixture(vec![
        route("GET", "/trustrosters/default", roster_body()),
        route("GET", "/trustpolicies", no_trust_policies()),
        route(
            "GET",
            job_path(stem),
            job_body(
                stem,
                JOB_UID_1,
                "sha256:first",
                Some((published_at - chrono::Duration::seconds(120), published_at)),
            ),
        ),
        status_route(),
    ]);
    let outcome = run_at(
        &f,
        &catalog(
            json!({"syncRequest": "token-1"}),
            published_status(stem, "token-1", published_at),
        ),
        at(2026, 9, 16, 12, 30, 0),
    )
    .await;
    assert_eq!(outcome.phase, ctrl::CatalogPhase::Idle);
    assert!(
        !f.listed_pods() && !f.read_a_pod_log(),
        "a harvested Job is read once: {:?}",
        f.seen()
    );
    assert!(
        f.posted("/configmaps").is_empty(),
        "no page is written by a pass that harvested nothing"
    );
    let patches = f.status_patches();
    assert!(!patches.is_empty(), "this pass does write a status");
    for patch in patches {
        assert!(
            patch["status"].get("syncedAt").is_none(),
            "`syncedAt` is the archive's freshness and not this pass's clock: {patch}"
        );
        assert!(
            patch["status"].get("pages").is_none(),
            "the published pages are left exactly as they are: {patch}"
        );
    }
    assert_no_delete(&f);
}

// ===========================================================================
// 4. The objects the defect has already stuck, and the upgrade past it
// ===========================================================================

/// **Upgrade.** Every catalog this defect stuck is carrying, right now, a NEW
/// Job's name beside an OLD Job's `finishedAt` — that is what the merge wrote.
/// Replacing the record stops another one being created; it does not read the
/// ones that exist, so the harvest arm asks the honest question instead: is
/// this record the record of THIS Job's completion?
///
/// It matters most for a `intervalSeconds: 0` catalog, which has no next slot
/// to recover on and would otherwise need a brand-new `syncRequest` token — but
/// the shape is the same at every cadence, so this row drives the hourly one
/// the rest of the module uses and a pass one reconcile after the upgrade.
#[tokio::test]
async fn a_record_left_by_the_defect_is_harvested_after_the_upgrade() {
    let first_finished = at(2026, 9, 16, 12, 2, 0);
    let second_finished = at(2026, 9, 16, 12, 20, 0);
    let second = leak(request_stem("token-2"));

    // Exactly what the live object looked like: the second request's Job name,
    // the FIRST request's timestamps, the first view still published and
    // `Synced` left on a waiting code by the running pass that never got a
    // harvest after it.
    let mut status = published_status(second, "token-2", first_finished);
    status["conditions"][1] = json!({
        "type": "Synced", "status": "Unknown", "reason": "PodNotStarted",
        "message": "the check Job is running", "observedGeneration": 1,
        "lastTransitionTime": stamp(first_finished)
    });

    let plan_sha = format!("sha256:{}", "5".repeat(64));
    let f = fixture(harvest_routes(
        second,
        JOB_UID_2,
        &plan_sha,
        (at(2026, 9, 16, 12, 15, 0), second_finished),
        framed(&plan_sha, &body_with(4)),
    ));
    let outcome = run_at(
        &f,
        &catalog(json!({"syncRequest": "token-2"}), status),
        at(2026, 9, 16, 12, 25, 0),
    )
    .await;

    assert_eq!(
        outcome.phase,
        ctrl::CatalogPhase::Published,
        "a record whose `finishedAt` is not this Job's completion time has not been harvested, \
         whoever wrote it; a stuck catalog recovers on the first reconcile after the upgrade \
         and not only at its next slot — and a manual-only catalog has no next slot"
    );
    let patch = f.patched_status();
    assert_eq!(patch["syncedAt"], stamp(second_finished));
    assert_eq!(condition(&patch, "Synced")["status"], "True");
    assert_eq!(condition(&patch, "Synced")["reason"], "Succeeded");
    assert_eq!(outcome.entries, 4);
}

/// The identity test itself, over its four cases — including the one that must
/// answer `true` with nothing to compare, because answering `false` there would
/// re-harvest a failed sync on every pass forever.
#[test]
fn the_harvest_test_is_about_this_jobs_completion() {
    let completed = at(2026, 9, 16, 12, 20, 0);
    let job: k8s_openapi::api::batch::v1::Job = serde_json::from_str(&job_body(
        "j",
        JOB_UID_1,
        "sha256:x",
        Some((at(2026, 9, 16, 12, 15, 0), completed)),
    ))
    .expect("a Job");
    // A Job that finished by FAILING carries no `completionTime`.
    let failed: k8s_openapi::api::batch::v1::Job = serde_json::from_value(json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": "j", "namespace": NS, "uid": JOB_UID_1},
        "status": {"startTime": stamp(at(2026, 9, 16, 12, 15, 0)),
                   "conditions": [{"type": "Failed", "status": "True"}]}
    }))
    .expect("a Job");

    let record = |finished: Option<DateTime<Utc>>| LastSyncJob {
        name: Some("j".to_string()),
        started_at: None,
        finished_at: finished,
        exit_code: None,
        refusal_reason: None,
    };

    assert!(
        !ctrl::harvested_record(&record(None), &job),
        "no `finishedAt` is the plain 'not read yet'"
    );
    assert!(
        ctrl::harvested_record(&record(Some(completed)), &job),
        "the record of this Job's own completion IS the harvest"
    );
    assert!(
        !ctrl::harvested_record(&record(Some(at(2026, 9, 16, 12, 2, 0))), &job),
        "another Job's timestamp is not this Job's result, which is the whole defect"
    );
    assert!(
        ctrl::harvested_record(&record(Some(at(2026, 9, 16, 12, 16, 0))), &failed),
        "a Job with no completionTime is recorded with the harvesting pass's own clock; there \
         is nothing to compare, and answering `false` re-harvests a failed sync forever"
    );
}
