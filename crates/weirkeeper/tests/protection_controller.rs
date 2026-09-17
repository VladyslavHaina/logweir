//! The `ProtectionPolicy` reconciler and its pure half — PLAT-14.2, D3 §3.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST OR A `mock_client` TEST. Nothing
//! posts to a sink, nothing waits on a Job, and the double PANICS on a request
//! it was not given a route for — which is what turns "it never patched a
//! Backup", "it never read that pod's log" and "it created exactly one
//! delivery Job" into assertions rather than absences of evidence.
//!
//! READ THESE FIVE FIRST, in this order:
//!
//! 1. [`a_condition_that_stays_true_pages_exactly_once`] — the whole point of
//!    PLAT-14.2's deduplication. The mutant is a ledger that keys on the
//!    reconcile rather than on the transition, which is how an on-call
//!    rotation mutes an integration in week two.
//! 2. [`a_notification_failure_never_patches_a_backup`] — the tracker's own
//!    acceptance sentence, over a route table with no `backups/status` route
//!    at all.
//! 3. [`a_label_wearing_pod_the_delivery_job_does_not_own_is_never_read`] —
//!    seam **S6**, defect `SEC-PODLOG`.
//! 4. [`the_ttl_is_patched_only_after_the_status_landed`] — seam **S7**'s
//!    ordering, asserted on the recorded request sequence.
//! 5. [`a_sink_credential_value_never_reaches_the_status`] — the API server
//!    echoes a routing key back in the Job it returns and prints one on the
//!    pod log; not one byte of it may reach a status, a condition, a plan or
//!    a log line this controller writes.

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Value};
use weirkeeper::controllers::protection_policy::{self as pp, ProtectionContext};
use weirkeeper::crds::protection_policy::{
    AlertDelivery, AlertEntry, AlertKind, ProtectionPolicy, ProtectionPolicySpec,
};
use weirkeeper::job::RunnerImage;
use weirkeeper::protection as p;
use weirkeeper::testing::{mock_client_recording_bodies, BodyRecorder, Recorder, Route};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const NS: &str = "logweir-d3-w6";
const POLICY: &str = "orders-prod";
const POLICY_UID: &str = "11111111-2222-3333-4444-555555555555";
const SOURCE: &str = "prod-kafka";
const DESTINATION: &str = "primary";
const SCHEDULE: &str = "nightly";
const SCHEDULE_UID: &str = "9f2b1c44-0000-4000-8000-0000000000a1";
const CATALOG: &str = "primary";
const PAGE_CM: &str = "primary-catalog-p0";

const STATUS_PATH: &str = "/protectionpolicies/orders-prod/status";
const SOURCE_PATH: &str = "/kafkaclusters/prod-kafka";
const DESTINATION_PATH: &str = "/backupdestinations/primary";
const SCHEDULE_PATH: &str = "/backupschedules/nightly";
const BACKUPS_PATH: &str = "/backups";
const CATALOG_PATH: &str = "/recoverycatalogs/primary";
const RESTORES_PATH: &str = "/restores";
const PAGE_PATH: &str = "/configmaps/primary-catalog-p0";

/// A routing key the fake API server echoes back everywhere it can. No byte of
/// it may reach anything this controller writes.
const ROUTING_KEY: &str = "R0UT1NGK3Y-do-not-leak-me";

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 17, 12, 0, 0)
        .single()
        .expect("the fixture instant exists")
}

fn at(offset_hours: i64) -> String {
    p::rfc3339(now() - Duration::hours(offset_hours))
}

fn context(client: kube::Client) -> ProtectionContext {
    ProtectionContext {
        client,
        runner_image: RunnerImage::default(),
    }
}

/// The default spec: one source, one saved destination, one schedule, a
/// 26-hour objective, one PagerDuty + webhook route.
fn spec_value() -> Value {
    json!({
        "protects": {
            "sourceRef": {"name": SOURCE},
            "topics": ["orders"],
            "scheduleRefs": [{"name": SCHEDULE}],
            "destinationRef": {"name": DESTINATION}
        },
        "objectives": {
            "maxRecoveryPointAgeSeconds": 93600,
            "maxConsecutiveFailedRuns": 2,
            "requireVerifiedEvidence": true,
            "requireCatalogAvailability": true
        },
        "notifications": {
            "routes": [{
                "name": "oncall",
                "pagerDuty": {"routingKeySecretRef": {"name": "pd", "key": "routing-key"}},
                "webhook": {"urlSecretRef": {"name": "hooks", "key": "ops"}}
            }],
            "sendResolved": true,
            "renotifyAfterSeconds": 86400
        },
        "evaluationIntervalSeconds": 300
    })
}

fn spec() -> ProtectionPolicySpec {
    serde_json::from_value(spec_value()).expect("the fixture spec parses")
}

/// The same spec with a `catalogRef`, for the tests whose subject is the
/// catalog conjunct. `evaluate` consults the catalog only when the POLICY asked
/// it to (`catalogRef` set AND `requireCatalogAvailability`), which is itself
/// the rule `a_policy_that_did_not_ask_for_the_catalog_does_not_consult_it`
/// asserts.
fn spec_with_catalog() -> ProtectionPolicySpec {
    let mut value = spec_value();
    merge(
        &mut value,
        &json!({"protects": {"catalogRef": {"name": CATALOG}}}),
    );
    serde_json::from_value(value).expect("the catalog-backed fixture spec parses")
}

fn policy_with(spec: Value, status: Value) -> ProtectionPolicy {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "ProtectionPolicy",
        "metadata": {
            "name": POLICY, "namespace": NS, "uid": POLICY_UID,
            "generation": 3, "resourceVersion": "9090",
            "creationTimestamp": "2026-09-01T00:00:00Z"
        },
        "spec": spec,
        "status": status
    }))
    .expect("the fixture is a ProtectionPolicy")
}

/// A `Backup` of the fixture schedule, `hours_ago` old, succeeded and verified
/// unless the caller says otherwise.
fn backup(name: &str, hours_ago: i64, overrides: Value) -> Value {
    let mut object = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Backup",
        "metadata": {
            "name": name, "namespace": NS,
            "uid": format!("aaaaaaaa-0000-4000-8000-{name:0>12}"),
            "creationTimestamp": at(hours_ago)
        },
        "spec": {
            "sourceRef": {"name": SOURCE},
            "topics": ["orders", "payments"],
            "archive": {"url": format!("logweir-destination://{DESTINATION}")},
            "destinationRef": {"name": DESTINATION},
            "scheduleRef": {"name": SCHEDULE, "uid": SCHEDULE_UID},
            "slot": format!("2026091{}-020000", hours_ago.min(9)),
            "triggeredBy": "schedule",
            "deadlineSeconds": 3600
        },
        "status": {
            "phase": "Succeeded",
            "exitCode": 0,
            "backupId": format!("{SCHEDULE_UID}-{name}"),
            "capture": {"startedAt": at(hours_ago), "finishedAt": at(hours_ago)},
            "windowCovered": {"fromMs": 0, "toMs": 1_700_000_000_000_i64},
            "evidence": {
                "receiptSha256": receipt_digest(name),
                "verification": {"result": "Valid"}
            }
        }
    });
    merge(&mut object, &overrides);
    object
}

/// A deterministic 64-hex digest per fixture name, so each point has its own
/// `lwp1-` identity.
fn receipt_digest(name: &str) -> String {
    let seed: String = name
        .bytes()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
        .chars()
        .cycle()
        .take(64)
        .collect();
    format!("sha256:{seed}")
}

fn point_id(name: &str) -> String {
    p::point_id_from_receipt_digest(&receipt_digest(name))
        .expect("the fixture digest is well formed")
}

/// RFC 7386-shaped test helper, so a fixture override is a patch and not a
/// rewrite of the whole object.
fn merge(target: &mut Value, patch: &Value) {
    let Some(fields) = patch.as_object() else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(serde_json::Map::new());
    }
    let map = target.as_object_mut().expect("just made an object");
    for (k, v) in fields {
        merge(map.entry(k.clone()).or_insert(Value::Null), v);
    }
}

fn backup_list(items: Vec<Value>) -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "BackupList",
        "metadata": {},
        "items": items
    })
    .to_string()
}

fn schedule_object(status: Value) -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupSchedule",
        "metadata": {"name": SCHEDULE, "namespace": NS, "uid": SCHEDULE_UID},
        "spec": {
            "schedule": "0 2 * * *",
            "sourceRef": {"name": SOURCE},
            "topics": ["orders", "payments"],
            "archive": {"url": format!("logweir-destination://{DESTINATION}")},
            "destinationRef": {"name": DESTINATION},
            "suspend": false
        },
        "status": status
    })
    .to_string()
}

/// A minimal, VALID `KafkaCluster`.
///
/// The reconciler only asks "does the referenced source exist?", but
/// `Api<KafkaCluster>::get_opt` deserializes into the typed object, so the
/// answer has to be one — which is itself a property worth having: a policy
/// that pointed at an object this build cannot decode is not a policy whose
/// verdict should read `Healthy`.
fn source_object() -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "KafkaCluster",
        "metadata": {"name": SOURCE, "namespace": NS, "uid": "c0ffee00-0000-4000-8000-000000000001"},
        "spec": {
            "bootstrapServers": ["kafka-source:9092"],
            "role": "source",
            "auth": {"mode": "scramSha512", "username": "backup", "tls": false,
                     "secretRef": {"name": "kafka-source-scram", "passwordKey": "password"}}
        }
    })
    .to_string()
}

/// A minimal, VALID `BackupDestination`.
fn destination_object() -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupDestination",
        "metadata": {"name": DESTINATION, "namespace": NS, "uid": "d0d0d0d0-0000-4000-8000-000000000002"},
        "spec": {
            "storage": {
                "provider": "S3", "bucket": "lw-a", "prefix": "team-a/prod",
                "endpoint": "http://minio.storage.svc:9000", "addressing": "PathStyle"
            },
            "transport": {"security": "InsecureHTTP"},
            "access": {"archiveWrite": {"mode": "SecretKeys", "secret": {
                "name": "lw-writer", "accessKeyIdKey": "id", "secretAccessKeyKey": "key"
            }}}
        }
    })
    .to_string()
}

fn ok(path: &'static str, body: String) -> Route {
    Route {
        method: "GET",
        path_suffix: path,
        status: 200,
        body,
    }
}

fn post(path: &'static str, body: String) -> Route {
    Route {
        method: "POST",
        path_suffix: path,
        status: 201,
        body,
    }
}

/// A PATCH route. The answer is the object the API server would return, not
/// `{}`: `Api::patch_status` deserializes the response into the typed object.
fn patch(path: &'static str) -> Route {
    let body = if path.contains("/jobs/") {
        json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": "j", "namespace": NS, "uid": JOB_UID}, "spec": {}
        })
    } else {
        json!({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "ProtectionPolicy",
            "metadata": {"name": POLICY, "namespace": NS, "uid": POLICY_UID,
                         "resourceVersion": "9091"},
            "spec": spec_value()
        })
    };
    Route {
        method: "PATCH",
        path_suffix: path,
        status: 200,
        body: body.to_string(),
    }
}

/// The route table every "healthy read" test starts from: the source, the
/// destination, the schedule and a `Backup` listing.
fn read_routes(backups: Vec<Value>, schedule_status: Value) -> Vec<Route> {
    vec![
        ok(SOURCE_PATH, source_object()),
        ok(DESTINATION_PATH, destination_object()),
        ok(SCHEDULE_PATH, schedule_object(schedule_status)),
        ok(BACKUPS_PATH, backup_list(backups)),
        ok(RESTORES_PATH, restore_list(Vec::new())),
    ]
}

/// The GET route the event `ConfigMap`'s owner read needs (review F7): the
/// FIRST delivery Job for a transition, answered with the UID the ConfigMap's
/// `ownerReferences` entry is built from.
fn first_job_route(key: &str, transition: i64) -> Route {
    let name = p::delivery_job_name(POLICY, POLICY_UID, key, transition, 1);
    let path: &'static str = Box::leak(format!("/jobs/{name}").into_boxed_str());
    ok(path, running_job(&name))
}

/// A `RestoreList`, for the `RecoveryCompleted` path.
fn restore_list(items: Vec<Value>) -> String {
    json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RestoreList",
        "metadata": {}, "items": items
    })
    .to_string()
}

/// One terminal `Restore` of the fixture policy's archive set.
fn restore(name: &str, uid: &str, backup_set: &str, overrides: Value) -> Value {
    let mut object = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"name": name, "namespace": NS, "uid": uid,
                     "creationTimestamp": at(1)},
        "spec": {
            "planBytes": "plan",
            "approvalRef": {"name": "ap"},
            "sourceArchive": {"url": format!("logweir-destination://{DESTINATION}")},
            "sourceDestinationRef": {"name": DESTINATION},
            "backupSetRef": backup_set,
            "pointInTime": at(2),
            "target": {"clusterRef": {"name": "target"}, "mode": "newTopic",
                       "topicNaming": {"prefix": "restored-"}},
            "deadlineSeconds": 3600
        },
        "status": {"phase": "Succeeded", "outcome": "pass"}
    });
    merge(&mut object, &overrides);
    object
}

/// The request path with its query string removed.
///
/// A route table is written against paths, and a namespace called
/// `logweir-d3-w6` contains the substring `/log` — which is exactly the kind of
/// coincidence that makes a "this log was never read" assertion pass for the
/// wrong reason.
fn path_of(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

fn requests(recorder: &Recorder) -> Vec<(String, String)> {
    recorder
        .lock()
        .expect("the recorder mutex")
        .iter()
        .map(|r| (r.method.clone(), r.uri.clone()))
        .collect()
}

fn bodies(recorder: &BodyRecorder) -> Vec<(String, String, String)> {
    recorder
        .lock()
        .expect("the body recorder mutex")
        .iter()
        .map(|r| (r.method.clone(), r.uri.clone(), r.body.clone()))
        .collect()
}

/// The last `/status` PATCH body this pass sent, parsed.
fn last_status_patch(recorder: &BodyRecorder) -> Value {
    let body = bodies(recorder)
        .into_iter()
        .filter(|(method, uri, _)| method == "PATCH" && uri.contains("/status"))
        .next_back()
        .expect("a status patch was sent")
        .2;
    serde_json::from_str(&body).expect("the status patch is JSON")
}

// ===========================================================================
// U — the health table across all five states (tracker row: STALE POINT)
// ===========================================================================

fn inputs<'a>(
    spec: &'a ProtectionPolicySpec,
    candidates: &'a [p::PointCandidate],
    catalog: &'a p::CatalogAnswer,
    schedules: &'a [p::ScheduleFacts],
    slots: &'a [p::SlotRun],
    rehearsal: &'a p::RehearsalFacts,
) -> p::Inputs<'a> {
    p::Inputs {
        spec,
        source_exists: true,
        destination_exists: true,
        candidates,
        catalog,
        schedules,
        slots,
        rehearsal,
        now: now(),
    }
}

fn candidate(hours_ago: i64) -> p::PointCandidate {
    p::PointCandidate {
        backup_name: Some("b-1".to_string()),
        point_id: Some(point_id("b-1")),
        recovery_point_at: Some(now() - Duration::hours(hours_ago)),
        newest_record_at: Some(now() - Duration::hours(hours_ago)),
        phase: Some("Succeeded".to_string()),
        exit_code: Some(0),
        evidence: p::Evidence::Valid,
        topics: vec!["orders".to_string(), "payments".to_string()],
        covers_all_topics: false,
        destination_matches: true,
        source_matches: true,
    }
}

fn healthy_schedule() -> p::ScheduleFacts {
    p::ScheduleFacts {
        name: SCHEDULE.to_string(),
        suspended: false,
        ready: Some("True".to_string()),
        next_fire_time: Some(now() + Duration::hours(1)),
        last_missed_slot: None,
        last_fire_time: Some(now() - Duration::hours(10)),
        missed_slots: None,
        exists: true,
    }
}

#[test]
fn the_health_table_covers_all_five_states() {
    let spec = spec();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let catalog = p::CatalogAnswer::NotConsulted;

    // Healthy — a point inside the objective, nothing at risk.
    let fresh = [candidate(2)];
    let verdict = p::evaluate(&inputs(
        &spec,
        &fresh,
        &catalog,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(verdict.health, p::Health::Healthy);
    assert_eq!(verdict.freshness, p::Freshness::Fresh);

    // AtRisk — inside the objective, but the schedule is suspended.
    let suspended = [p::ScheduleFacts {
        suspended: true,
        ..healthy_schedule()
    }];
    let verdict = p::evaluate(&inputs(
        &spec,
        &fresh,
        &catalog,
        &suspended,
        &[],
        &rehearsal,
    ));
    assert_eq!(verdict.health, p::Health::AtRisk);
    assert_eq!(
        verdict.freshness,
        p::Freshness::Fresh,
        "a suspended schedule does not make an existing point stale; it makes protection risky"
    );

    // Stale — a point, past the objective (26 h).
    let old = [candidate(31)];
    let verdict = p::evaluate(&inputs(&spec, &old, &catalog, &schedules, &[], &rehearsal));
    assert_eq!(verdict.health, p::Health::Stale);
    assert_eq!(verdict.reason, p::FreshnessReason::PointOlderThanObjective);

    // Unprotected — no available point at all.
    let verdict = p::evaluate(&inputs(&spec, &[], &catalog, &schedules, &[], &rehearsal));
    assert_eq!(verdict.health, p::Health::Unprotected);
    assert_eq!(verdict.reason, p::FreshnessReason::NoAvailablePoint);

    // Unknown — the source the policy names is gone.
    let mut missing = inputs(&spec, &fresh, &catalog, &schedules, &[], &rehearsal);
    missing.source_exists = false;
    let verdict = p::evaluate(&missing);
    assert_eq!(verdict.health, p::Health::Unknown);
    assert_eq!(verdict.reason, p::FreshnessReason::SourceMissing);
}

/// The brief's rule, said as a test: `unknown` is a third answer, not a shade
/// of either other one.
///
/// MUTANT: make `Freshness::Unknown` fall through to the `Fresh` arm (or to
/// `Stale`) in `evaluate`. The first assertion fails on the enum, the second
/// on the condition status, and the third on the alert set — three
/// independent places, because "unknown rendered as healthy" is the exact
/// defect this task exists to prevent.
#[test]
fn unknown_is_neither_fresh_nor_stale_and_is_not_a_failure() {
    let spec = spec_with_catalog();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let catalog = p::CatalogAnswer::Stale(p::FreshnessReason::CatalogStale);
    let fresh = [candidate(2)];

    let verdict = p::evaluate(&inputs(
        &spec,
        &fresh,
        &catalog,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(verdict.freshness, p::Freshness::Unknown);
    assert_ne!(verdict.freshness, p::Freshness::Fresh);
    assert_ne!(verdict.freshness, p::Freshness::Stale);
    assert_eq!(verdict.health, p::Health::Unknown);
    assert_eq!(verdict.basis, p::AvailabilityBasis::CatalogStale);
    assert_eq!(
        p::Health::Unknown.condition_status(),
        "Unknown",
        "`Protected` on an evaluation that could not happen is neither True nor False: a False \
         reads as `Logweir checked and you are not protected`, which is a claim this controller \
         did not make"
    );
    assert!(
        !verdict.open_kinds.contains(&p::PolicyAlertKind::Staleness),
        "an unreadable catalog does not open a Staleness page: nothing was measured"
    );
    for health in p::Health::ALL {
        let expected = match health {
            p::Health::Healthy => "True",
            p::Health::Unknown => "Unknown",
            _ => "False",
        };
        assert_eq!(health.condition_status(), expected);
    }
}

// ===========================================================================
// U — the availability rule (tracker row: UNAVAILABLE ARCHIVE)
// ===========================================================================

#[test]
fn the_availability_rule_names_its_four_conjuncts() {
    let spec = spec();
    let catalog = p::CatalogAnswer::NotConsulted;
    assert!(is_available_with(&spec, &catalog, candidate(2)));

    // 1. the run itself
    let mut crashed = candidate(2);
    crashed.exit_code = None;
    assert!(
        !is_available_with(&spec, &catalog, crashed),
        "a terminal phase with NO exit code is a run nobody can say completed"
    );
    let mut failed = candidate(2);
    failed.phase = Some("Failed".to_string());
    assert!(!is_available_with(&spec, &catalog, failed));

    // 2. the evidence
    let mut untrusted = candidate(2);
    untrusted.evidence = p::Evidence::Untrusted;
    assert!(
        !is_available_with(&spec, &catalog, untrusted),
        "`Untrusted` verifies under a key this installation refuses; counting it would make \
         TrustPolicy decorative"
    );
    let mut historical = candidate(2);
    historical.evidence = p::Evidence::ValidHistorical;
    assert!(
        is_available_with(&spec, &catalog, historical),
        "a key that was valid when it signed and has since been retired is what rotation looks \
         like: a PASS"
    );

    // 3. the subject
    let mut elsewhere = candidate(2);
    elsewhere.destination_matches = false;
    assert!(!is_available_with(&spec, &catalog, elsewhere));
    let mut missing_topic = candidate(2);
    missing_topic.topics = vec!["payments".to_string()];
    assert!(
        !is_available_with(&spec, &catalog, missing_topic),
        "`topics ⊆ point topics`: a point that does not carry `orders` does not protect `orders`"
    );
    let mut dynamic = candidate(2);
    dynamic.topics = Vec::new();
    dynamic.covers_all_topics = true;
    assert!(
        is_available_with(&spec, &catalog, dynamic),
        "an allUserTopics run enumerates no topics on the object; a literal subset test would \
         make every dynamic installation Unprotected"
    );

    // 4. the catalog
    let degraded = p::CatalogAnswer::Fresh(vec![entry(&point_id("b-1"), "Missing", "Verified")]);
    assert!(!is_available_with(&spec, &degraded, candidate(2)));
    let present = p::CatalogAnswer::Fresh(vec![entry(&point_id("b-1"), "Available", "Verified")]);
    assert!(is_available_with(&spec, &present, candidate(2)));
    let stale = p::CatalogAnswer::Stale(p::FreshnessReason::CatalogStale);
    assert!(
        !is_available_with(&spec, &stale, candidate(2)),
        "a catalog that cannot answer makes nothing available; that is how CatalogStale becomes \
         Unknown and never Healthy"
    );
}

fn is_available_with(
    spec: &ProtectionPolicySpec,
    catalog: &p::CatalogAnswer,
    candidate: p::PointCandidate,
) -> bool {
    p::is_available(&candidate, spec, catalog)
}

fn entry(point_id: &str, availability: &str, verification: &str) -> p::CatalogEntry {
    serde_json::from_value(json!({
        "pointId": point_id,
        "backupId": "set-1",
        "recoveryPointAtMs": 1_700_000_000_000_i64,
        "availability": availability,
        "verification": verification,
        // BOTH axes, exactly as `catalog_view::view_entry` materialises it
        // (`availability.selectable() && verification.selectable()`). A helper
        // that keyed on availability alone would hand the reader a `selectable`
        // the real catalog never writes.
        "selectable": availability == "Available"
            && matches!(verification, "Verified" | "VerifiedHistorical"),
        "runId": "ignored-unknown-field",
        "coveredFromMs": 0
    }))
    .expect("a view entry parses leniently")
}

/// A newer point the catalog cannot read must not silently become the answer,
/// and it must not hide the older point that CAN be read.
#[test]
fn an_unreadable_newest_point_selects_the_older_one_and_opens_archive_unavailable() {
    let spec = spec_with_catalog();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();

    let mut newest = candidate(2);
    newest.backup_name = Some("b-new".to_string());
    newest.point_id = Some(point_id("b-new"));
    let mut older = candidate(10);
    older.backup_name = Some("b-old".to_string());
    older.point_id = Some(point_id("b-old"));
    let candidates = [newest, older];

    let catalog = p::CatalogAnswer::Fresh(vec![
        entry(&point_id("b-new"), "Unreadable", "Verified"),
        entry(&point_id("b-old"), "Available", "Verified"),
    ]);
    let verdict = p::evaluate(&inputs(
        &spec,
        &candidates,
        &catalog,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(
        verdict.point.as_ref().and_then(|p| p.backup_name.clone()),
        Some("b-old".to_string()),
        "the newest point the catalog cannot read is not the newest AVAILABLE point"
    );
    assert!(
        verdict
            .open_kinds
            .contains(&p::PolicyAlertKind::ArchiveUnavailable),
        "the operator has to learn that their newest backup is not there; a silently older \
         recovery point is the defect"
    );
    assert_eq!(verdict.basis, p::AvailabilityBasis::Catalog);
    assert_eq!(
        verdict.health,
        p::Health::Healthy,
        "ten hours is inside the 26-hour objective, so protection is met by the older point — \
         and the archive alert is how the degraded newer one is still reported"
    );
}

// ===========================================================================
// U — the ledger (tracker rows: REPEATED FAILURE DEDUPLICATION, RECOVERY
// NOTIFICATION)
// ===========================================================================

fn notifications(
    spec: &ProtectionPolicySpec,
) -> Option<&weirkeeper::crds::protection_policy::Notifications> {
    spec.notifications.as_ref()
}

#[test]
fn a_condition_that_stays_true_pages_exactly_once() {
    let spec = spec();
    let open = [p::PolicyAlertKind::Staleness];

    // Pass 1 — the alert opens at transition 1 and is due.
    let first = p::reconcile_alerts(
        &[],
        &open,
        p::Health::Stale,
        POLICY_UID,
        notifications(&spec),
        now(),
    );
    assert_eq!(first.alerts.len(), 1);
    assert_eq!(first.alerts[0].transition, Some(1));
    assert_eq!(first.due.len(), 1);

    // The controller records that a Job was created for transition 1.
    let mut delivered = first.alerts.clone();
    delivered[0].notified_transition = Some(1);
    delivered[0].delivery = Some(AlertDelivery {
        state: Some("Delivered".to_string()),
        attempts: Some(1),
        last_attempt_at: Some(now()),
        job_ref: None,
        last_error: None,
    });

    // Passes 2..=5 — the condition is still true. NOTHING is due.
    let mut ledger = delivered;
    for pass in 0..4 {
        let out = p::reconcile_alerts(
            &ledger,
            &open,
            p::Health::Stale,
            POLICY_UID,
            notifications(&spec),
            now() + Duration::minutes(5 * (pass + 1)),
        );
        assert!(
            out.due.is_empty(),
            "pass {pass}: a condition that stays true produces no further transitions — one \
             page per reconcile is how an integration gets muted"
        );
        assert_eq!(out.alerts[0].transition, Some(1));
        ledger = out.alerts;
    }

    // The re-notify window, and exactly one more transition when it elapses.
    let out = p::reconcile_alerts(
        &ledger,
        &open,
        p::Health::Stale,
        POLICY_UID,
        notifications(&spec),
        now() + Duration::seconds(86_400),
    );
    assert_eq!(out.alerts[0].transition, Some(2));
    assert_eq!(out.due.len(), 1);
}

#[test]
fn the_key_is_stable_across_open_and_resolve_and_is_its_own_family() {
    let spec = spec();
    let open = p::reconcile_alerts(
        &[],
        &[p::PolicyAlertKind::Staleness],
        p::Health::Stale,
        POLICY_UID,
        notifications(&spec),
        now(),
    );
    let key = open.alerts[0].key.clone();
    assert_eq!(key, format!("logweir-protection-{POLICY_UID}-Staleness"));

    let resolved = p::reconcile_alerts(
        &open.alerts,
        &[],
        p::Health::Healthy,
        POLICY_UID,
        notifications(&spec),
        now() + Duration::hours(1),
    );
    assert_eq!(
        resolved.alerts[0].key, key,
        "PagerDuty gets `trigger` and `resolve` under ONE dedup_key; a key that changed between \
         them leaves the incident open forever"
    );
    assert_eq!(resolved.alerts[0].state, "Resolved");
    assert_eq!(resolved.alerts[0].transition, Some(2));
    assert_eq!(resolved.due.len(), 1, "the resolve is its own transition");

    assert!(
        key.starts_with(p::DEDUP_PREFIX),
        "the prefix keeps this family apart from `logweir-drill-…`, so a protection resolve can \
         never close a drill's open page"
    );
    assert_eq!(
        p::dedup_key("   ", p::PolicyAlertKind::Staleness),
        "logweir-protection-unknown-Staleness",
        "a blank UID collapses to `unknown`, never to an empty segment — that key would be ONE \
         incident per kind across every policy in the cluster"
    );
}

#[test]
fn recovery_completed_keys_on_the_restore_uid_and_never_on_the_policy() {
    const RESTORE_A: &str = "77777777-0000-4000-8000-00000000000a";
    const RESTORE_B: &str = "88888888-0000-4000-8000-00000000000b";
    assert_ne!(
        p::recovery_completed_key(RESTORE_A),
        p::recovery_completed_key(RESTORE_B),
        "a policy's points are restored many times; keying on the policy would make the second \
         restore of the day overwrite the first one's message"
    );
    assert_eq!(
        p::recovery_completed_key(RESTORE_A),
        format!("logweir-protection-{RESTORE_A}-RecoveryCompleted")
    );
    // The wrong builder does not COMPILE: `PolicyAlertKind` has four variants
    // and `RecoveryCompleted` is not one of them.
    assert_eq!(p::PolicyAlertKind::ALL.len(), 4);
    assert_eq!(
        p::PolicyAlertKind::narrow(AlertKind::RecoveryCompleted),
        None
    );
    assert!(!p::kind_pages(AlertKind::RecoveryCompleted));
    for kind in [
        AlertKind::BackupFailure,
        AlertKind::Staleness,
        AlertKind::ArchiveUnavailable,
        AlertKind::RehearsalFailure,
    ] {
        assert!(p::kind_pages(kind));
    }
}

#[test]
fn a_retry_chain_is_one_failed_slot_and_a_running_slot_stops_the_walk() {
    let slots = vec![
        slot("20260916-020000", 2, p::SlotOutcome::Failed),
        slot("20260916-020000", 1, p::SlotOutcome::Failed),
        slot("20260916-020000", 0, p::SlotOutcome::Failed),
        slot("20260915-020000", 0, p::SlotOutcome::Failed),
        slot("20260914-020000", 0, p::SlotOutcome::Succeeded),
    ];
    assert_eq!(
        p::consecutive_failed_slots(&slots),
        2,
        "three attempts of one slot are ONE failed slot: maxConsecutiveFailedRuns counts slots, \
         not attempts"
    );

    let with_running = vec![
        slot("20260917-020000", 0, p::SlotOutcome::Running),
        slot("20260916-020000", 0, p::SlotOutcome::Failed),
        slot("20260915-020000", 0, p::SlotOutcome::Failed),
    ];
    assert_eq!(
        p::consecutive_failed_slots(&with_running),
        0,
        "a slot still running has not failed; counting past it opens an alert for a condition a \
         run in flight may be about to end"
    );
}

fn slot(name: &str, attempt: u32, outcome: p::SlotOutcome) -> p::SlotRun {
    p::SlotRun {
        slot: name.to_string(),
        attempt,
        outcome,
        at: Some(now() - Duration::hours(i64::from(attempt) + 1)),
        backup_name: format!("{name}-r{attempt}"),
        reason: None,
    }
}

#[test]
fn suppression_is_a_choice_and_not_a_failure() {
    // No notifications block at all.
    assert!(p::is_suppressed(
        None,
        AlertKind::Staleness,
        p::AlertState::Open
    ));

    // A kinds list that does not name this kind.
    let narrowed: ProtectionPolicySpec = serde_json::from_value({
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"notifications": {"kinds": ["BackupFailure"]}}),
        );
        value
    })
    .expect("the narrowed spec parses");
    assert!(p::is_suppressed(
        notifications(&narrowed),
        AlertKind::Staleness,
        p::AlertState::Open
    ));
    assert!(!p::is_suppressed(
        notifications(&narrowed),
        AlertKind::BackupFailure,
        p::AlertState::Open
    ));

    // `sendResolved: false` suppresses the resolve and not the open.
    let quiet: ProtectionPolicySpec = serde_json::from_value({
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"notifications": {"sendResolved": false}}),
        );
        value
    })
    .expect("the quiet spec parses");
    assert!(p::is_suppressed(
        notifications(&quiet),
        AlertKind::Staleness,
        p::AlertState::Resolved
    ));
    assert!(!p::is_suppressed(
        notifications(&quiet),
        AlertKind::Staleness,
        p::AlertState::Open
    ));

    // D3 W4's recorded no-op: `RecoveryCompleted` with PagerDuty-only routes.
    let pager_only: ProtectionPolicySpec = serde_json::from_value(json!({
        "protects": {"sourceRef": {"name": SOURCE}, "destinationRef": {"name": DESTINATION}},
        "objectives": {"maxRecoveryPointAgeSeconds": 93600},
        "notifications": {"routes": [{
            "name": "oncall",
            "pagerDuty": {"routingKeySecretRef": {"name": "pd", "key": "routing-key"}}
        }]}
    }))
    .expect("the pager-only spec parses");
    assert!(
        p::is_suppressed(
            notifications(&pager_only),
            AlertKind::RecoveryCompleted,
            p::AlertState::Open
        ),
        "a RecoveryCompleted with PagerDuty-only routes configures zero sinks: `logweir notify \
         deliver` exits 1 with `none:unconfigured` BY DESIGN, and burning three attempts on it \
         would turn a no-op into a red NotificationsDelivered"
    );
    assert!(!p::is_suppressed(
        notifications(&pager_only),
        AlertKind::Staleness,
        p::AlertState::Open
    ));
}

// ===========================================================================
// U — sampled-versus-complete labelling
// ===========================================================================

/// MUTANT: add a `Complete => "complete"` variant to
/// `protection::VerificationScope`, or make `event_document` write the literal.
/// This test fails on the enum's member list AND on the document, and
/// `logweir notify deliver` would refuse the document at parse time on top of
/// that — three independent refusals for the one claim this product must never
/// make.
#[test]
fn nothing_can_label_a_verification_complete() {
    assert_eq!(
        p::VerificationScope::ALL
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>(),
        vec!["sampled", "degraded", "none"]
    );
    assert_eq!(p::VerificationScope::parse("complete"), None);

    for scope in p::VerificationScope::ALL {
        let point = facts_point();
        let document = p::event_document(&p::EventFacts {
            namespace: NS,
            name: POLICY,
            uid: POLICY_UID,
            alert_key: &p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness),
            kind: AlertKind::Staleness,
            open: true,
            transition: 3,
            health: p::Health::Stale,
            summary: "the newest available recovery point is 31h 0m old, past the objective of \
                      26h 0m",
            point: Some(&point),
            consecutive_failed_runs: 2,
            missed_slots: 1,
            scope: *scope,
            generated_at: now(),
        });
        let rendered = document.to_string();
        assert!(
            !rendered.contains("\"complete\""),
            "no event document may carry a `complete` verification scope: {rendered}"
        );
        assert_eq!(
            document["verification_scope"].as_str(),
            Some(scope.as_str())
        );
    }
}

/// D3 W4 scans the summary and edits a claim out of it; nothing this
/// controller generates should ever trip that scan. The word `exhaustive` is
/// forbidden EVEN INSIDE A DENIAL, because these channels truncate.
#[test]
fn no_summary_claims_an_exhaustive_check() {
    const BANNED: [&str; 6] = [
        "exhaustive",
        "every record",
        "all records",
        "complete verification",
        "fully verified",
        "byte-for-byte comparison of the archive",
    ];
    let spec = spec();
    let point = facts_point();
    for health in p::Health::ALL {
        for reason in p::FreshnessReason::ALL {
            for with_age in [true, false] {
                let summary = p::summarize(&spec, *health, *reason, Some(&point), 2, with_age);
                let lowered = summary.to_ascii_lowercase();
                for phrase in BANNED {
                    assert!(
                        !lowered.contains(phrase),
                        "the summary reaches a PagerDuty incident title verbatim and must not \
                         claim `{phrase}`: {summary}"
                    );
                }
                assert!(!summary.is_empty());
            }
        }
    }

    // The condition sentence carries NO clock-derived number (review F4): the
    // event's does, because it is written once and read by a human, and a
    // condition `message` is part of an object that is compared on every pass.
    let event = p::summarize(
        &spec,
        p::Health::Stale,
        p::FreshnessReason::PointOlderThanObjective,
        Some(&point),
        2,
        true,
    );
    let condition = p::summarize(
        &spec,
        p::Health::Stale,
        p::FreshnessReason::PointOlderThanObjective,
        Some(&point),
        2,
        false,
    );
    // 111 600 s is 1d 7h, and the objective 93 600 s is 1d 2h.
    assert!(event.contains("1d 7h old"), "{event}");
    assert!(
        !condition.contains("1d 7h"),
        "a condition message that embeds the age changes when the minute rolls over, which is a \
         status patch on a pass where nothing happened: {condition}"
    );
    assert!(
        condition.contains("1d 2h"),
        "the objective is a spec value and never moves: {condition}"
    );
}

fn facts_point() -> p::AvailablePointFacts {
    p::AvailablePointFacts {
        point_id: Some(point_id("b-1")),
        backup_name: Some("b-1".to_string()),
        recovery_point_at: Some(now() - Duration::hours(31)),
        newest_record_at: Some(now() - Duration::hours(30)),
        age_seconds: Some(111_600),
        evidence: p::Evidence::Valid,
        topics: vec!["orders".to_string()],
        topics_truncated: false,
    }
}

// ===========================================================================
// U — the event document, held to the format page's own worked example
// ===========================================================================

/// The cross-crate contract, without the cross-crate dependency.
///
/// `weirkeeper` cannot depend on `crates/logweir` — that crate links the
/// SIGNER, and `scripts/check-one-signer.sh` computes the set of crates that
/// do, so even a `[dev-dependencies]` edge would break guard **G-SIGN** for a
/// type import. So the document is built here and held to
/// `docs/formats/protection-event.md`'s own worked example, which
/// `crates/logweir/tests/notify_deliver.rs` parses into
/// `logweir::notify::ProtectionEvent` from the SAME page. A rename on either
/// side is a red test on one of the two.
#[test]
fn the_event_document_is_the_documented_field_set() {
    let page = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/formats/protection-event.md"),
    )
    .expect("the format page is readable");
    let example = fenced_json(&page).expect("the format page carries a worked JSON example");
    let documented: Value = serde_json::from_str(&example).expect("the example is JSON");

    let point = facts_point();
    let built = p::event_document(&p::EventFacts {
        namespace: NS,
        name: POLICY,
        uid: POLICY_UID,
        alert_key: &p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness),
        kind: AlertKind::Staleness,
        open: true,
        transition: 3,
        health: p::Health::Stale,
        summary: "a summary",
        point: Some(&point),
        consecutive_failed_runs: 2,
        missed_slots: 1,
        scope: p::VerificationScope::Sampled,
        generated_at: now(),
    });

    let mut documented_keys: Vec<&String> = documented
        .as_object()
        .expect("an object")
        .keys()
        .collect::<Vec<_>>();
    let mut built_keys: Vec<&String> = built.as_object().expect("an object").keys().collect();
    documented_keys.sort();
    built_keys.sort();
    assert_eq!(
        built_keys, documented_keys,
        "the delivery Job parses with `deny_unknown_fields`, so an extra key here is a refused \
         delivery and a missing one is a refused parse"
    );
    for key in &built_keys {
        assert!(
            p::EVENT_FIELDS.contains(&key.as_str()),
            "`{key}` is emitted but is not in EVENT_FIELDS"
        );
    }
    for nested in ["policy", "alert", "last_available_point"] {
        let mut a: Vec<&String> = built[nested]
            .as_object()
            .expect("an object")
            .keys()
            .collect();
        let mut b: Vec<&String> = documented[nested]
            .as_object()
            .expect("an object")
            .keys()
            .collect();
        a.sort();
        b.sort();
        assert_eq!(a, b, "`{nested}`'s field set drifted from the format page");
    }
    assert_eq!(built["alert"]["action"].as_str(), Some("trigger"));
    assert_eq!(
        built["details_route"].as_str(),
        Some(format!("#/protection?ns={NS}&name={POLICY}").as_str()),
        "a fragment route and not an absolute URL: the controller does not know the \
         installation's hostname, and inventing one puts a dead link in an incident"
    );
    assert_eq!(
        built["event_id"].as_str(),
        Some(
            p::event_id(
                POLICY_UID,
                &p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness),
                3
            )
            .as_str()
        )
    );
}

/// A `health: Unprotected` event has NO point, and its absence is the fact.
#[test]
fn an_unprotected_event_omits_the_point_rather_than_inventing_one() {
    let document = p::event_document(&p::EventFacts {
        namespace: NS,
        name: POLICY,
        uid: POLICY_UID,
        alert_key: &p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness),
        kind: AlertKind::Staleness,
        open: true,
        transition: 1,
        health: p::Health::Unprotected,
        summary: "there is no available recovery point for this policy at all",
        point: None,
        consecutive_failed_runs: 0,
        missed_slots: 0,
        scope: p::VerificationScope::None,
        generated_at: now(),
    });
    assert!(document.get("last_available_point").is_none());

    // A point with no derivable identity is also omitted, rather than emitted
    // half-filled: the delivery Job requires all four sub-fields.
    let partial = p::AvailablePointFacts {
        point_id: None,
        ..facts_point()
    };
    let document = p::event_document(&p::EventFacts {
        namespace: NS,
        name: POLICY,
        uid: POLICY_UID,
        alert_key: "k",
        kind: AlertKind::Staleness,
        open: false,
        transition: 2,
        health: p::Health::Healthy,
        summary: "s",
        point: Some(&partial),
        consecutive_failed_runs: 0,
        missed_slots: 0,
        scope: p::VerificationScope::Sampled,
        generated_at: now(),
    });
    assert!(document.get("last_available_point").is_none());
    assert_eq!(document["alert"]["action"].as_str(), Some("resolve"));
}

#[test]
fn the_point_identity_is_the_recorded_receipt_digest() {
    let digest = format!("sha256:{}", "ab".repeat(32));
    assert_eq!(
        p::point_id_from_receipt_digest(&digest),
        Some(format!("lwp1-{}", "ab".repeat(16))),
        "D3 §5.1: `lwp1-` + the first 32 hex characters of sha256(receipt bytes) — which is \
         exactly what `Backup.status.evidence.receiptSha256` already holds"
    );
    assert_eq!(p::point_id_from_receipt_digest("sha256:short"), None);
    assert_eq!(p::point_id_from_receipt_digest(&"ab".repeat(32)), None);
    assert_eq!(
        p::point_id_from_receipt_digest(&format!("sha256:{}", "zz".repeat(32))),
        None,
        "nothing is invented from a digest that is not one"
    );
}

fn fenced_json(page: &str) -> Option<String> {
    let mut lines = page.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "```json" {
            continue;
        }
        let block: Vec<&str> = lines.by_ref().take_while(|l| l.trim() != "```").collect();
        let joined = block.join("\n");
        if joined.contains("\"format_version\"") {
            return Some(joined);
        }
    }
    None
}

// ===========================================================================
// U — the delivery verdict (tracker row: NOTIFICATION TRANSPORT FAILURE)
// ===========================================================================

#[test]
fn the_delivery_verdict_reads_d3_w4s_exit_contract() {
    assert_eq!(
        p::classify_delivery(Some(0), &["notify-result=pagerduty:ok"]).0,
        p::DeliveryState::Delivered
    );
    assert_eq!(
        p::classify_delivery(Some(1), &["notify-result=webhook:failed"]).0,
        p::DeliveryState::Failed
    );
    assert_eq!(
        p::classify_delivery(Some(1), &["notify-result=none:unconfigured"]).0,
        p::DeliveryState::Suppressed,
        "exit 1 with `none:unconfigured` is a no-op, not a transport failure: D3 W4's recorded \
         hand-off for a RecoveryCompleted with PagerDuty-only routes"
    );
    assert_eq!(
        p::classify_delivery(Some(3), &[]).0,
        p::DeliveryState::Failed,
        "exit 3 refused the document and posted nothing"
    );
    assert_eq!(p::classify_delivery(None, &[]).0, p::DeliveryState::Failed);
    assert_eq!(
        p::classify_delivery(Some(2), &[]).0,
        p::DeliveryState::Failed
    );
}

/// The sink error is SWALLOWED AND REPORTED: it becomes a bounded sentence on
/// this policy's status and nothing else moves.
#[test]
fn a_sink_error_is_reported_and_never_echoed_from_the_log() {
    let (state, message) = p::classify_delivery(
        Some(1),
        &[
            "notify-result=pagerduty:failed",
            &format!("notify-result=webhook:ok {ROUTING_KEY}"),
            &format!("some sink said: {ROUTING_KEY}"),
        ],
    );
    assert_eq!(state, p::DeliveryState::Failed);
    assert!(
        !message.contains(ROUTING_KEY),
        "a pod log is adopter-influenced input; echoing a matched line's tail into a CR status \
         is how a credential on that line becomes a permanently stored, API-served secret. \
         Got: {message}"
    );
    assert!(message.contains("pagerduty:failed"));
    assert!(
        !message.contains("webhook:ok"),
        "`webhook:ok <key>` is not one of the seven values this build knows, so it is not read \
         at all"
    );
    assert_eq!(p::known_result("pagerduty:ok"), Some("pagerduty:ok"));
    assert_eq!(p::known_result("pagerduty:maybe"), None);
    assert_eq!(p::NOTIFY_RESULTS.len(), 7);

    let long = "x".repeat(4096);
    assert!(p::cap_error(&long).chars().count() <= p::MAX_ERROR_CHARS);
}

#[test]
fn the_retry_schedule_is_three_bounded_attempts() {
    assert_eq!(p::MAX_DELIVERY_ATTEMPTS, 3);
    assert_eq!(p::DELIVERY_BACKOFF_SECONDS, [60, 300]);
    let mut delivery = AlertDelivery {
        state: Some("Failed".to_string()),
        attempts: Some(1),
        last_attempt_at: Some(now()),
        job_ref: None,
        last_error: Some("nope".to_string()),
    };
    assert_eq!(
        p::next_attempt_at(&delivery),
        Some(now() + Duration::seconds(60))
    );
    delivery.attempts = Some(2);
    assert_eq!(
        p::next_attempt_at(&delivery),
        Some(now() + Duration::seconds(300))
    );
    delivery.attempts = Some(3);
    assert_eq!(
        p::next_attempt_at(&delivery),
        None,
        "exhaustion sets NotificationsDelivered=False/DeliveryFailed AND NOTHING ELSE"
    );
}

#[test]
fn the_derived_names_are_pure_functions_and_fit_kubernetes() {
    let key = p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness);
    let a = p::delivery_job_name(POLICY, POLICY_UID, &key, 3, 1);
    let b = p::delivery_job_name(POLICY, POLICY_UID, &key, 3, 1);
    assert_eq!(
        a, b,
        "a duplicate reconcile must compute the same name and get a 409"
    );
    assert_ne!(a, p::delivery_job_name(POLICY, POLICY_UID, &key, 4, 1));
    assert_ne!(a, p::delivery_job_name(POLICY, POLICY_UID, &key, 3, 2));
    assert_ne!(
        a,
        p::delivery_job_name(POLICY, "another-uid", &key, 3, 1),
        "two policies of the same name in different namespaces must not share a Job name"
    );

    let long = "a".repeat(200);
    for name in [
        p::delivery_job_name(&long, POLICY_UID, &key, 3, 1),
        p::event_config_map_name(&long, &key, 3),
    ] {
        assert!(name.len() <= 63, "`{name}` is {} characters", name.len());
        assert!(
            name.ends_with(|c: char| c.is_ascii_alphanumeric()),
            "a Kubernetes name ends in an alphanumeric: {name}"
        );
    }
    // The digest half survives truncation; the readable half is what is cut.
    let truncated = p::delivery_job_name(&long, POLICY_UID, &key, 3, 1);
    let digest = p::sha8(&[POLICY_UID, &key, "3"]);
    assert!(
        truncated.contains(&digest),
        "trimming the digest would make two policies' Jobs collide: {truncated}"
    );
}

// ===========================================================================
// C — the reconcile
// ===========================================================================

fn drive(policy: &ProtectionPolicy, routes: Vec<Route>) -> (pp::Outcome, Recorder, BodyRecorder) {
    drive_at(policy, routes, now())
}

/// [`drive`] with the reconcile's `now` named.
///
/// **The clock is a parameter because freezing it hides the defect** (review
/// F4). `a_steady_policy_issues_no_patch_at_all` used to call `drive` twice,
/// which passed the same frozen instant both times, so it proved nothing about
/// a reconciler whose clock advances — and the reconciler wrote a live
/// `sinceLastFire` and a humanized age into the object, so every real pass was
/// a PATCH.
fn drive_at(
    policy: &ProtectionPolicy,
    routes: Vec<Route>,
    at: DateTime<Utc>,
) -> (pp::Outcome, Recorder, BodyRecorder) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime");
    runtime.block_on(async {
        let (client, recorder, bodies) = mock_client_recording_bodies(routes);
        let outcome = pp::reconcile_policy(policy, &context(client), at)
            .await
            .expect("the reconcile did not error");
        (outcome, recorder, bodies)
    })
}

/// A healthy policy: one recent verified backup, one ready schedule, no
/// catalog. It writes a status and creates NOTHING.
#[test]
fn a_healthy_policy_creates_nothing_and_publishes_healthy() {
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let routes = {
        let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], json!({}));
        routes.push(patch(STATUS_PATH));
        routes
    };
    let (outcome, recorder, bodies) = drive(&policy_with(no_catalog, json!({})), routes);

    assert_eq!(outcome.health, p::Health::Healthy);
    assert_eq!(outcome.created_jobs, 0);
    assert!(outcome.committed);
    let status = last_status_patch(&bodies);
    assert_eq!(status["status"]["health"].as_str(), Some("Healthy"));
    assert_eq!(
        status["status"]["availabilityBasis"].as_str(),
        Some("KubernetesStatus"),
        "no catalog was consulted, and the status says which claim it is making"
    );
    assert_eq!(
        status["status"]["lastAvailablePoint"]["pointId"].as_str(),
        Some(point_id("b-1").as_str())
    );
    assert_eq!(
        status["status"]["observedGeneration"].as_i64(),
        Some(3),
        "the verdict names the generation it was computed from"
    );
    assert!(
        status["status"]["alerts"].is_null(),
        "a healthy policy has no ledger entries at all; a ledger full of never-fired alerts is \
         noise in the `kubectl get -o yaml` an operator runs during an incident"
    );
    let calls = requests(&recorder);
    assert!(
        !calls.iter().any(|(method, _)| method == "POST"),
        "a healthy policy creates nothing: {calls:?}"
    );
    assert!(
        calls
            .iter()
            .any(|(m, u)| m == "GET" && u.contains("/backups")),
        "membership is read from the runs, bounded: {calls:?}"
    );
}

/// A steady object issues ZERO patches per reconcile — erratum **E11(d)**.
///
/// MUTANT: write `evaluatedAt: now` unconditionally. The route table below has
/// NO `PATCH` route, so the double panics and this test fails with the request
/// that should not have been made.
#[test]
fn a_steady_policy_issues_no_patch_at_all() {
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    // Pass 1: learn the status this pass computes.
    //
    // THE SCHEDULE HAS FIRED, which is what puts `missed.sinceLastFire` in the
    // status at all — and that field is one of the three clock-derived ones, so
    // a fixture without it cannot see the defect (review F4).
    let routes = {
        let mut routes = read_routes(
            vec![backup("b-1", 2, json!({}))],
            json!({"lastFireTime": at(10)}),
        );
        routes.push(patch(STATUS_PATH));
        routes
    };
    let (_, _, bodies) = drive(&policy_with(no_catalog.clone(), json!({})), routes);
    let written = last_status_patch(&bodies)["status"].clone();

    // Pass 2: the same object, now carrying that status, THE CLOCK ADVANCED,
    // and NO patch route — the double panics on a PATCH it was not given one
    // for.
    //
    // THE CLOCK MOVES, which is the whole point (review F4). The old form drove
    // twice at the same frozen instant and so could not see a status field that
    // is a live clock reading. A hundred seconds is inside `evaluatedAt`'s
    // half-interval window (150 s at the fixture's 300 s), so the honest
    // expectation is still ZERO writes: nothing about the cluster changed.
    let steady = policy_with(no_catalog, written);
    let routes = read_routes(
        vec![backup("b-1", 2, json!({}))],
        json!({"lastFireTime": at(10)}),
    );
    let (outcome, recorder, _) = drive_at(&steady, routes, now() + Duration::seconds(100));
    assert!(outcome.committed);
    let calls = requests(&recorder);
    assert!(
        !calls.iter().any(|(method, _)| method == "PATCH"),
        "a reconciler's own status patch is what wakes it; a pass that changed nothing but a \
         clock reading spins the loop at whatever rate the API server will serve. Calls: \
         {calls:?}"
    );
}

/// The other side of erratum **E11(d)**: past `evaluatedAt`'s half-interval
/// window the object IS written, and the only things that move are the three
/// clock-derived fields.
///
/// MUTANT: compute `ageSeconds` or `sinceLastFire` from `now` instead of from
/// the settled `evaluatedAt`. The equality below then fails on a field that is
/// not in the allowed set — which is what "a steady object costs one bounded
/// write per interval, not one per reconcile" actually means.
#[test]
fn past_the_half_interval_only_the_clock_fields_move() {
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let fired = json!({"lastFireTime": at(10)});
    let routes = {
        let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], fired.clone());
        routes.push(patch(STATUS_PATH));
        routes
    };
    let (_, _, bodies) = drive(&policy_with(no_catalog.clone(), json!({})), routes);
    let first = last_status_patch(&bodies)["status"].clone();
    assert!(
        first["missed"]["sinceLastFire"].is_i64(),
        "the fixture must actually carry a clock-derived field, or this test is vacuous: {first}"
    );

    let steady = policy_with(no_catalog, first.clone());
    let routes = {
        let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], fired);
        routes.push(patch(STATUS_PATH));
        routes
    };
    let (_, recorder, bodies) = drive_at(&steady, routes, now() + Duration::seconds(300));
    assert_eq!(
        requests(&recorder)
            .iter()
            .filter(|(m, _)| m == "PATCH")
            .count(),
        1,
        "one bounded write per interval, and not one per reconcile"
    );
    let second = last_status_patch(&bodies)["status"].clone();

    let strip = |value: &Value| {
        let mut v = value.clone();
        if let Some(map) = v.as_object_mut() {
            map.remove("evaluatedAt");
            if let Some(point) = map
                .get_mut("lastAvailablePoint")
                .and_then(Value::as_object_mut)
            {
                point.remove("ageSeconds");
            }
            if let Some(missed) = map.get_mut("missed").and_then(Value::as_object_mut) {
                missed.remove("sinceLastFire");
            }
        }
        v
    };
    assert_eq!(
        strip(&first),
        strip(&second),
        "past the half-interval the object is rewritten, but ONLY `evaluatedAt`, \
         `lastAvailablePoint.ageSeconds` and `missed.sinceLastFire` may differ — a condition \
         message or any other field moving here is a write the cluster did not ask for"
    );
    assert_ne!(first["evaluatedAt"], second["evaluatedAt"]);
}

/// The tracker's acceptance sentence, as a test.
///
/// The route table has NO route for `backups/status` or `restores/status`, and
/// the double PANICS on a request it was not given a route for — so an
/// attempted write is a failing test naming the request, not a silent pass.
#[test]
fn a_notification_failure_never_patches_a_backup() {
    let key = p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness);
    let job_name = p::delivery_job_name(POLICY, POLICY_UID, &key, 1, 3);
    let status = json!({
        "alerts": [{
            "key": key,
            "kind": "Staleness",
            "state": "Open",
            "openedAt": at(4),
            "transition": 1,
            "notifiedTransition": 1,
            "delivery": {
                "state": "Pending",
                "attempts": 3,
                "lastAttemptAt": at(1),
                "jobRef": {"name": job_name.clone()}
            }
        }]
    });
    let policy = policy_with(
        {
            let mut value = spec_value();
            merge(
                &mut value,
                &json!({"objectives": {"requireCatalogAvailability": false}}),
            );
            value
        },
        status,
    );

    let job_path: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let mut routes = read_routes(vec![backup("b-1", 40, json!({}))], json!({}));
    routes.push(ok(job_path, finished_job(&job_name, "Failed")));
    routes.push(ok("/pods", pod_list(&job_name, JOB_UID)));
    routes.push(ok(
        POD_LOG_PATH,
        "notify-result=webhook:failed\n".to_string(),
    ));
    routes.push(patch(STATUS_PATH));
    routes.push(patch(job_path));

    let (outcome, recorder, bodies) = drive(&policy, routes);
    assert_eq!(outcome.health, p::Health::Stale);
    let status = last_status_patch(&bodies);
    let alert = &status["status"]["alerts"][0];
    assert_eq!(alert["delivery"]["state"].as_str(), Some("Failed"));
    assert_eq!(alert["delivery"]["attempts"].as_i64(), Some(3));
    let delivered = condition(&status, "NotificationsDelivered");
    assert_eq!(delivered["status"].as_str(), Some("False"));
    assert_eq!(delivered["reason"].as_str(), Some("DeliveryFailed"));

    for (method, uri) in requests(&recorder) {
        assert!(
            !(uri.contains("/backups/") || uri.contains("/restores/")),
            "the protection controller touched `{method} {uri}`; a notification failure must \
             never rewrite a backup result"
        );
    }
    assert!(
        !requests(&recorder)
            .iter()
            .any(|(method, uri)| method == "POST" && path_of(uri).ends_with("/jobs")),
        "three attempts are three attempts: a fourth delivery Job is not created"
    );
}

const JOB_UID: &str = "1a2b3c4d-0000-4000-8000-0000000000b2";
const OTHER_JOB_UID: &str = "deadbeef-0000-4000-8000-0000000000c3";
const POD: &str = "delivery-pod-abcde";
const POD_LOG_PATH: &str = "/pods/delivery-pod-abcde/log";

fn finished_job(name: &str, condition: &str) -> String {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": name, "namespace": NS, "uid": JOB_UID,
            // The fake API server echoes a routing key back inside the object
            // it returns. Nothing this controller writes may carry it.
            "annotations": {"an-operator-pasted-this": ROUTING_KEY}
        },
        "spec": {},
        "status": {"conditions": [{"type": condition, "status": "True"}]}
    })
    .to_string()
}

fn pod_list(job_name: &str, owner_uid: &str) -> String {
    pod_list_exiting(job_name, owner_uid, 1)
}

fn pod_list_exiting(job_name: &str, owner_uid: &str, exit_code: i32) -> String {
    json!({
        "apiVersion": "v1", "kind": "PodList", "metadata": {},
        "items": [{
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": POD, "namespace": NS,
                "labels": {"batch.kubernetes.io/job-name": job_name},
                "ownerReferences": [{
                    "apiVersion": "batch/v1", "kind": "Job",
                    "name": job_name, "uid": owner_uid, "controller": true
                }]
            },
            "status": {"containerStatuses": [{
                "name": "runner",
                "state": {"terminated": {"exitCode": exit_code}}
            }]}
        }]
    })
    .to_string()
}

fn condition<'a>(status: &'a Value, r#type: &str) -> &'a Value {
    status["status"]["conditions"]
        .as_array()
        .expect("the status carries conditions")
        .iter()
        .find(|c| c["type"].as_str() == Some(r#type))
        .unwrap_or_else(|| panic!("no `{type}` condition"))
}

/// Seam **S6**, defect `SEC-PODLOG`. A `notify-result=` line becomes a status
/// field and then an API response; reading a stranger's is the whole defect.
///
/// MUTANT: replace `find_owned_pod` with a label-only `list(...).items.first()`.
/// The `/pods/.../log` route below is never registered for the impostor, so
/// the double panics naming the request.
#[test]
fn a_label_wearing_pod_the_delivery_job_does_not_own_is_never_read() {
    let key = p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness);
    let job_name = p::delivery_job_name(POLICY, POLICY_UID, &key, 1, 1);
    let job_path: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let policy = policy_with(
        {
            let mut value = spec_value();
            merge(
                &mut value,
                &json!({"objectives": {"requireCatalogAvailability": false}}),
            );
            value
        },
        json!({
            "alerts": [{
                "key": key, "kind": "Staleness", "state": "Open",
                "openedAt": at(4), "transition": 1, "notifiedTransition": 1,
                "delivery": {
                    "state": "Pending", "attempts": 1, "lastAttemptAt": at(1),
                    "jobRef": {"name": job_name.clone()}
                }
            }]
        }),
    );

    let mut routes = read_routes(vec![backup("b-1", 40, json!({}))], json!({}));
    routes.push(ok(job_path, finished_job(&job_name, "Failed")));
    // The pod wears the Job's name label and claims a DIFFERENT Job's UID.
    routes.push(ok("/pods", pod_list(&job_name, OTHER_JOB_UID)));
    routes.push(patch(STATUS_PATH));
    routes.push(patch(job_path));

    let (_, recorder, bodies) = drive(&policy, routes);
    let calls = requests(&recorder);
    assert!(
        !calls.iter().any(|(_, uri)| path_of(uri).ends_with("/log")),
        "an impostor pod's log is never read — not even to discard it: {calls:?}"
    );
    let status = last_status_patch(&bodies);
    assert_eq!(
        status["status"]["alerts"][0]["delivery"]["state"].as_str(),
        Some("Failed"),
        "a Job whose pod cannot be proved yields no exit code, which is a failed delivery and \
         never a claimed success"
    );
}

/// Seam **S7**'s ordering, as a property of the recorded request sequence.
///
/// MUTANT: move the `set_delivery_ttl` call above `write_status`. The index
/// comparison below fails.
#[test]
fn the_ttl_is_patched_only_after_the_status_landed() {
    let key = p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness);
    let job_name = p::delivery_job_name(POLICY, POLICY_UID, &key, 1, 1);
    let job_path: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let policy = policy_with(
        {
            let mut value = spec_value();
            merge(
                &mut value,
                &json!({"objectives": {"requireCatalogAvailability": false}}),
            );
            value
        },
        json!({
            "alerts": [{
                "key": key, "kind": "Staleness", "state": "Open",
                "openedAt": at(4), "transition": 1, "notifiedTransition": 1,
                "delivery": {
                    "state": "Pending", "attempts": 1, "lastAttemptAt": at(1),
                    "jobRef": {"name": job_name.clone()}
                }
            }]
        }),
    );
    let mut routes = read_routes(vec![backup("b-1", 40, json!({}))], json!({}));
    routes.push(ok(job_path, finished_job(&job_name, "Complete")));
    routes.push(ok("/pods", pod_list_exiting(&job_name, JOB_UID, 0)));
    routes.push(ok(
        POD_LOG_PATH,
        "notify-result=pagerduty:ok\nnotify-result=webhook:ok\n".to_string(),
    ));
    routes.push(patch(STATUS_PATH));
    routes.push(patch(job_path));

    let (outcome, recorder, body_log) = drive(&policy, routes);
    assert_eq!(outcome.ttl_patched, 1);
    let calls = requests(&recorder);
    let status_at = calls
        .iter()
        .position(|(m, u)| m == "PATCH" && u.contains("/status"))
        .expect("the status was patched");
    let ttl_at = calls
        .iter()
        .position(|(m, u)| m == "PATCH" && u.contains("/jobs/"))
        .expect("the TTL was patched");
    assert!(
        status_at < ttl_at,
        "the exit code lives on the POD and the TTL controller removes the Job and its pod \
         together: a TTL set first lets garbage collection race the read. Calls: {calls:?}"
    );
    let ttl_body = bodies(&body_log)
        .into_iter()
        .find(|(m, u, _)| m == "PATCH" && u.contains("/jobs/"))
        .expect("a TTL patch body")
        .2;
    assert!(ttl_body.contains("ttlSecondsAfterFinished"));
    let status = last_status_patch(&body_log);
    assert_eq!(
        status["status"]["alerts"][0]["delivery"]["state"].as_str(),
        Some("Delivered")
    );
}

/// Exactly one delivery Job per `(key, transition)`, and a 409 on the replay.
#[test]
fn one_delivery_job_per_transition_and_a_replay_is_a_409() {
    let policy = policy_with(
        {
            let mut value = spec_value();
            merge(
                &mut value,
                &json!({"objectives": {"requireCatalogAvailability": false}}),
            );
            value
        },
        json!({}),
    );
    let mut routes = read_routes(vec![backup("b-1", 40, json!({}))], json!({}));
    routes.push(post("/configmaps", json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm", "namespace": NS}}).to_string()));
    routes.push(post("/jobs", json!({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j", "namespace": NS}, "spec": {}}).to_string()));
    routes.push(first_job_route(
        &p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness),
        1,
    ));
    routes.push(patch(STATUS_PATH));

    let (outcome, recorder, bodies) = drive(&policy, routes);
    assert_eq!(outcome.health, p::Health::Stale);
    assert_eq!(outcome.created_jobs, 1);
    let calls = requests(&recorder);
    assert_eq!(
        calls
            .iter()
            .filter(|(m, u)| m == "POST" && path_of(u).ends_with("/jobs"))
            .count(),
        1,
        "one alert transition is one page: {calls:?}"
    );

    // The Job is created BEFORE the ConfigMap it mounts, and the ConfigMap's
    // owner UID is read back from it (review F7). The cost is a window in which
    // a scheduled pod sits `ContainerCreating` on a mount the kubelet retries;
    // the benefit is that the Job's TTL collects the immutable object, which
    // policy ownership never did.
    let job_at = calls
        .iter()
        .position(|(m, u)| m == "POST" && path_of(u).ends_with("/jobs"))
        .expect("the delivery Job was created");
    let cm_at = calls
        .iter()
        .position(|(m, u)| m == "POST" && path_of(u).ends_with("/configmaps"))
        .expect("the event ConfigMap was created");
    assert!(job_at < cm_at, "{calls:?}");

    // The replay: the SAME object, now carrying the ledger the first pass
    // wrote, against an API server that answers 409 to both creates.
    let written = last_status_patch(&bodies)["status"].clone();
    let replayed = policy_with(
        {
            let mut value = spec_value();
            merge(
                &mut value,
                &json!({"objectives": {"requireCatalogAvailability": false}}),
            );
            value
        },
        written,
    );
    let key = p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness);
    let job_name = p::delivery_job_name(POLICY, POLICY_UID, &key, 1, 1);
    let job_path: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let mut routes = read_routes(vec![backup("b-1", 40, json!({}))], json!({}));
    routes.push(ok(job_path, running_job(&job_name)));
    let (outcome, recorder, _) = drive(&replayed, routes);
    assert_eq!(
        outcome.created_jobs, 0,
        "the ledger already records a Job for this transition; a second pass must not page \
         again"
    );
    let calls = requests(&recorder);
    assert!(
        !calls.iter().any(|(m, _)| m == "POST"),
        "nothing is created on the replay: {calls:?}"
    );
}

fn running_job(name: &str) -> String {
    json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "namespace": NS, "uid": JOB_UID},
        "spec": {}, "status": {"active": 1}
    })
    .to_string()
}

/// Every credential is a `secretKeyRef` and nothing echoed back reaches a
/// status, a condition, the event document or a log line.
#[test]
fn a_sink_credential_value_never_reaches_the_status() {
    let key = p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness);
    let job_name = p::delivery_job_name(POLICY, POLICY_UID, &key, 1, 1);
    let job_path: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let policy = policy_with(
        {
            let mut value = spec_value();
            merge(
                &mut value,
                &json!({"objectives": {"requireCatalogAvailability": false}}),
            );
            value
        },
        json!({
            "alerts": [{
                "key": key, "kind": "Staleness", "state": "Open",
                "openedAt": at(4), "transition": 1, "notifiedTransition": 1,
                "delivery": {
                    "state": "Pending", "attempts": 1, "lastAttemptAt": at(1),
                    "jobRef": {"name": job_name.clone()}
                }
            }]
        }),
    );
    let mut routes = read_routes(vec![backup("b-1", 40, json!({}))], json!({}));
    // The API server echoes the routing key back in the Job object AND the
    // pod prints it on stdout.
    routes.push(ok(job_path, finished_job(&job_name, "Failed")));
    routes.push(ok("/pods", pod_list(&job_name, JOB_UID)));
    routes.push(ok(
        POD_LOG_PATH,
        format!("posting with {ROUTING_KEY}\nnotify-result=pagerduty:failed\n"),
    ));
    routes.push(patch(STATUS_PATH));
    routes.push(patch(job_path));

    let (_, _, body_log) = drive(&policy, routes);
    for (method, uri, body) in bodies(&body_log) {
        assert!(
            !body.contains(ROUTING_KEY),
            "`{method} {uri}` carried a sink credential value: {body}"
        );
    }

    // And the Job this controller builds names a Secret and a KEY, never a
    // value — which is the only shape a controller with no verb on `secrets`
    // could produce.
    let job_spec = pp::delivery_job_spec(
        "j",
        NS,
        &pp::owner_of(POLICY, POLICY_UID),
        "cm",
        spec()
            .notifications
            .as_ref()
            .and_then(|n| n.routes.as_deref()),
        &RunnerImage::default(),
    );
    assert!(
        job_spec.secret_mounts.is_empty(),
        "a delivery signs nothing, reads no archive and mounts no Secret volume"
    );
    let names: Vec<&str> = job_spec
        .env_from_secret
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(names, vec![pp::ROUTING_KEY_ENV, pp::WEBHOOK_URL_ENV]);
    for entry in &job_spec.env_from_secret {
        assert!(!entry.secret_name.is_empty() && !entry.key.is_empty());
    }
    for (name, value) in &job_spec.env_literal {
        assert!(
            !value.contains(ROUTING_KEY),
            "`{name}` carries a literal credential"
        );
    }
    let built = weirkeeper::job::build(&job_spec);
    let rendered = serde_json::to_string(&built).expect("the Job serialises");
    assert!(!rendered.contains(ROUTING_KEY));
    assert!(rendered.contains("secretKeyRef"));
    assert!(
        rendered.contains("\"automountServiceAccountToken\":false"),
        "a delivery pod makes ZERO Kubernetes API calls"
    );
}

/// D3 §3.4 point 3: `blockOwnerDeletion: false`.
#[test]
fn the_delivery_job_does_not_block_deleting_its_policy() {
    let policy = policy_with(
        {
            let mut value = spec_value();
            merge(
                &mut value,
                &json!({"objectives": {"requireCatalogAvailability": false}}),
            );
            value
        },
        json!({}),
    );
    let mut routes = read_routes(vec![backup("b-1", 40, json!({}))], json!({}));
    routes.push(post("/configmaps", json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm", "namespace": NS}}).to_string()));
    routes.push(post("/jobs", json!({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j", "namespace": NS}, "spec": {}}).to_string()));
    routes.push(first_job_route(
        &p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness),
        1,
    ));
    routes.push(patch(STATUS_PATH));
    let (_, _, body_log) = drive(&policy, routes);

    for (_, uri, body) in bodies(&body_log).into_iter().filter(|(m, u, _)| {
        m == "POST" && (path_of(u).ends_with("/jobs") || path_of(u).ends_with("/configmaps"))
    }) {
        let object: Value = serde_json::from_str(&body).expect("the created object is JSON");
        let owner = &object["metadata"]["ownerReferences"][0];
        assert_eq!(owner["controller"].as_bool(), Some(true), "{uri}");
        assert_eq!(
            owner["blockOwnerDeletion"].as_bool(),
            Some(false),
            "an operator deleting a policy during an incident must not find the delete hanging \
             on a notification Job waiting out a 120-second deadline against a sink that is \
             down: {uri}"
        );
        if path_of(&uri).ends_with("/jobs") {
            assert_eq!(owner["kind"].as_str(), Some("ProtectionPolicy"), "{uri}");
            assert_eq!(owner["uid"].as_str(), Some(POLICY_UID), "{uri}");
        } else {
            // REVIEW F7. The event ConfigMap is `immutable: true` and this role
            // holds `delete` on nothing, so a policy-owned one is never removed
            // until the policy is: one object per `(alertKey, transition)`,
            // thousands a year. Owned by the Job, the API server's TTL
            // controller collects it.
            assert_eq!(
                owner["kind"].as_str(),
                Some("Job"),
                "the event ConfigMap must be collected by a Job TTL, not accumulate for the \
                 life of the policy: {uri}"
            );
            assert_eq!(owner["uid"].as_str(), Some(JOB_UID), "{uri}");
            assert_ne!(owner["uid"].as_str(), Some(POLICY_UID), "{uri}");
        }
    }

    // The event ConfigMap is immutable and carries the event under the key the
    // Job mounts.
    let (_, _, cm_body) = bodies(&body_log)
        .into_iter()
        .find(|(m, u, _)| m == "POST" && path_of(u).ends_with("/configmaps"))
        .expect("the event ConfigMap was created");
    let map: Value = serde_json::from_str(&cm_body).expect("JSON");
    assert_eq!(map["immutable"].as_bool(), Some(true));
    let event: Value = serde_json::from_str(
        map["data"][p::EVENT_DATA_KEY]
            .as_str()
            .expect("the event.json key"),
    )
    .expect("the event document is JSON");
    assert_eq!(event["health"].as_str(), Some("Stale"));
    assert_eq!(event["alert"]["action"].as_str(), Some("trigger"));
    assert_eq!(event["verification_scope"].as_str(), Some("sampled"));
    assert_eq!(event["policy"]["uid"].as_str(), Some(POLICY_UID));
}

/// A manual run of the schedule is part of its history — D1 W3b's
/// `is_run_of_schedule`, and the reason `spec.scheduleRef.uid` is the
/// authority and the label is only an index.
///
/// MUTANT: narrow the listing by `logweir.dev/schedule-uid` and decide
/// membership from the label. The manual run below carries no label at all, so
/// the policy reads `Unprotected` and this test fails on the health.
#[test]
fn membership_counts_a_manual_run_of_the_schedule() {
    let manual = backup(
        "b-manual",
        2,
        json!({
            "spec": {
                "triggeredBy": "manual",
                "slot": Value::Null,
                "trigger": {"kind": "Manual", "attempt": 0}
            }
        }),
    );
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let mut routes = read_routes(vec![manual], json!({}));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&policy_with(no_catalog, json!({})), routes);

    assert_eq!(
        outcome.health,
        p::Health::Healthy,
        "a manual run of the schedule IS part of its history; it protects the objective exactly \
         as a scheduled one does"
    );
    let status = last_status_patch(&bodies);
    assert_eq!(
        status["status"]["lastAvailablePoint"]["backupRef"]["name"].as_str(),
        Some("b-manual")
    );
}

/// A foreign run — a different source — is not history and never protects.
#[test]
fn a_run_of_another_source_is_not_this_policys_history() {
    let foreign = backup(
        "b-other",
        1,
        json!({"spec": {"sourceRef": {"name": "staging-kafka"}}}),
    );
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let mut routes = read_routes(vec![foreign], json!({}));
    routes.push(post("/configmaps", json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm", "namespace": NS}}).to_string()));
    routes.push(post("/jobs", json!({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j", "namespace": NS}, "spec": {}}).to_string()));
    routes.push(first_job_route(
        &p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness),
        1,
    ));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, _) = drive(&policy_with(no_catalog, json!({})), routes);
    assert_eq!(outcome.health, p::Health::Unprotected);
}

/// The catalog path end to end: a fresh view, a page `ConfigMap`, and the
/// availability answer that comes out of it.
#[test]
fn a_fresh_catalog_view_answers_availability_and_an_expired_one_does_not() {
    let entries = format!(
        "{}\n",
        json!({
            "pointId": point_id("b-1"), "backupId": "s", "runId": "r",
            "recoveryPointAtMs": 1_700_000_000_000_i64,
            "coveredFromMs": 0, "coveredToMs": 1,
            "receiptKey": "k", "receiptSha256": "sha256:0",
            "availability": "Available", "verification": "Verified", "selectable": true
        })
    );
    let page = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": PAGE_CM, "namespace": NS},
        "data": {"entries.jsonl": entries}
    })
    .to_string();
    let fresh_catalog = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
        "metadata": {"name": CATALOG, "namespace": NS},
        "spec": {"destinationRef": {"name": DESTINATION}, "sync": {}},
        "status": {
            "viewExpiresAt": p::rfc3339(now() + Duration::hours(1)),
            "pages": [{"configMapName": PAGE_CM, "index": 0, "count": 1}]
        }
    })
    .to_string();
    let with_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"protects": {"catalogRef": {"name": CATALOG}}}),
        );
        value
    };

    let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], json!({}));
    routes.push(ok(CATALOG_PATH, fresh_catalog));
    routes.push(ok(PAGE_PATH, page));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&policy_with(with_catalog.clone(), json!({})), routes);
    assert_eq!(outcome.health, p::Health::Healthy);
    assert_eq!(
        last_status_patch(&bodies)["status"]["availabilityBasis"].as_str(),
        Some("Catalog")
    );

    // An EXPIRED view is not a view.
    let expired = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
        "metadata": {"name": CATALOG, "namespace": NS},
        "spec": {"destinationRef": {"name": DESTINATION}, "sync": {}},
        "status": {"viewExpiresAt": at(1)}
    })
    .to_string();
    let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], json!({}));
    routes.push(ok(CATALOG_PATH, expired));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&policy_with(with_catalog, json!({})), routes);
    assert_eq!(outcome.health, p::Health::Unknown);
    let status = last_status_patch(&bodies);
    assert_eq!(
        status["status"]["availabilityBasis"].as_str(),
        Some("CatalogStale")
    );
    assert_eq!(
        condition(&status, "Protected")["status"].as_str(),
        Some("Unknown"),
        "a catalog that cannot answer is never rendered as protected and never as a failure"
    );
}

// ===========================================================================
// Structural guards
// ===========================================================================

/// D3 §3.4's structural requirement, read off the source.
#[test]
fn the_controller_names_no_backup_or_restore_status_write() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/controllers/protection_policy.rs"),
    )
    .expect("the reconciler source is readable");
    let code: String = source
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    for forbidden in [
        "backups.patch_status",
        "restores.patch_status",
        "patch_status(&backup",
        "replace_status",
        "Api::<Backup>::patch_status",
        "Api::<Restore>::patch_status",
    ] {
        assert!(
            !code.contains(forbidden),
            "`{forbidden}` appears in the protection reconciler; notification failure must \
             never rewrite a backup result"
        );
    }
    assert_eq!(
        code.matches("Api<Backup>").count(),
        1,
        "the only `Api<Backup>` in this file is the bounded membership `list`"
    );
    assert_eq!(
        code.matches("Api<Restore>").count(),
        1,
        "the only `Api<Restore>` in this file is the bounded recovery `list`; a protection \
         verdict is a projection of runs that already happened and this controller is the \
         execution authority for none of them"
    );
    for handle in ["Api<Backup>", "Api<Restore>"] {
        let at = code.find(handle).expect("the handle is declared");
        let region = &code[at..];
        let stop = region.find("\n}").unwrap_or(region.len());
        assert!(
            !region[..stop].contains("patch"),
            "`{handle}` is reached and then patched in this file"
        );
    }
    assert!(
        !code.contains("Api<Secret>") && !code.contains("secrets"),
        "this controller holds no verb on `secrets` and names none"
    );
    assert!(
        code.contains("find_owned_pod"),
        "the pod is proved by the Job's own UID before one byte of its log is read (seam S6)"
    );
}

/// The delivery Job runs exactly D3 §3.4's subcommand, with the deadline and
/// the mount the contract names.
#[test]
fn the_delivery_job_is_the_documented_invocation() {
    let job_spec = pp::delivery_job_spec(
        "j",
        NS,
        &pp::owner_of(POLICY, POLICY_UID),
        "cm",
        spec()
            .notifications
            .as_ref()
            .and_then(|n| n.routes.as_deref()),
        &RunnerImage::default(),
    );
    assert_eq!(
        job_spec.args,
        vec!["notify", "deliver", "--event", "/event/event.json"]
    );
    assert_eq!(job_spec.deadline_seconds, pp::DELIVERY_DEADLINE_SECONDS);
    assert_eq!(job_spec.service_account_name, pp::SERVICE_ACCOUNT);
    assert_eq!(job_spec.config_map_mounts.len(), 1);
    assert_eq!(
        job_spec.config_map_mounts[0].mount_path,
        p::EVENT_MOUNT_PATH
    );
    assert!(job_spec.plan_config_map.is_none());

    let built = weirkeeper::job::build(&job_spec);
    let rendered = serde_json::to_value(&built).expect("the Job serialises");
    assert!(
        rendered["spec"]["ttlSecondsAfterFinished"].is_null(),
        "the TTL is patched on only after the status write; setting it at creation lets garbage \
         collection race the exit-code read"
    );
    assert_eq!(rendered["spec"]["backoffLimit"].as_i64(), Some(0));
    assert_eq!(
        rendered["spec"]["activeDeadlineSeconds"].as_i64(),
        Some(pp::DELIVERY_DEADLINE_SECONDS)
    );
}

/// The per-pass burst bound, and the arithmetic it makes exact.
#[test]
fn the_number_of_delivery_jobs_per_pass_is_bounded() {
    assert_eq!(p::MAX_DELIVERY_JOBS_PER_PASS, 4);
    assert_eq!(p::MAX_ALERTS, 16);
    assert_eq!(p::MAX_SCHEDULES, 16);
    assert_eq!(p::MAX_TOPICS, 64);
    assert_eq!(p::MAX_BACKUPS_SCANNED, 50);

    // The ledger never grows past the CRD's cap, whatever it is handed.
    let spec = spec();
    let noise: Vec<AlertEntry> = (0..40)
        .map(|n| AlertEntry {
            key: format!("logweir-protection-{POLICY_UID}-noise-{n}"),
            kind: AlertKind::Staleness,
            state: "Open".to_string(),
            opened_at: Some(now()),
            resolved_at: None,
            transition: Some(1),
            notified_transition: Some(1),
            delivery: None,
        })
        .collect();
    let out = p::reconcile_alerts(
        &noise,
        &p::PolicyAlertKind::ALL,
        p::Health::Stale,
        POLICY_UID,
        notifications(&spec),
        now(),
    );
    assert!(out.alerts.len() <= p::MAX_ALERTS);

    // And a point covering more than the cap is truncated, visibly.
    let many: Vec<String> = (0..200).map(|n| format!("topic-{n}")).collect();
    let mut candidate = candidate(2);
    candidate.topics = many;
    candidate.covers_all_topics = true;
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let catalog = p::CatalogAnswer::NotConsulted;
    let candidates = [candidate];
    let verdict = p::evaluate(&inputs(
        &spec,
        &candidates,
        &catalog,
        &schedules,
        &[],
        &rehearsal,
    ));
    let point = verdict.point.expect("a point");
    assert_eq!(point.topics.len(), p::MAX_TOPICS);
    assert!(point.topics_truncated);
}

// ===========================================================================
// C — RecoveryCompleted (tracker row: RECOVERY NOTIFICATION)
// ===========================================================================

const RESTORE_UID: &str = "77777777-0000-4000-8000-00000000000a";

/// A terminal `Restore` of a point this policy protects produces exactly ONE
/// informational message, keyed on the RESTORE UID, with action `trigger`.
///
/// MUTANT: key it on the policy UID (or on `(policy, kind)`, which is what
/// every other alert here does). `the second restore of the day` below then
/// collides with the first, the ledger holds one entry instead of two, and the
/// count assertion fails — which is the defect
/// `recovery_completed_keys_on_the_restore_uid_and_never_on_the_policy`
/// prevents at the type level and this one prevents at the controller level.
#[test]
fn a_terminal_restore_of_this_policys_point_is_recorded_once_and_never_pages() {
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let set = format!("{SCHEDULE_UID}-b-1");
    let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], json!({}));
    routes.retain(|r| r.path_suffix != RESTORES_PATH);
    routes.push(ok(
        RESTORES_PATH,
        restore_list(vec![
            restore("r-1", RESTORE_UID, &set, json!({})),
            // A restore of ANOTHER destination is not this policy's recovery.
            restore(
                "r-foreign",
                "99999999-0000-4000-8000-00000000000f",
                &set,
                json!({"spec": {"sourceDestinationRef": {"name": "elsewhere"}}}),
            ),
            // A restore still running is not a completion.
            restore(
                "r-running",
                "66666666-0000-4000-8000-000000000006",
                &set,
                json!({"status": {"phase": "Running"}}),
            ),
        ]),
    ));
    routes.push(post(
        "/configmaps",
        json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm", "namespace": NS}})
            .to_string(),
    ));
    routes.push(post(
        "/jobs",
        json!({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j", "namespace": NS}, "spec": {}})
            .to_string(),
    ));
    routes.push(first_job_route(&p::recovery_completed_key(RESTORE_UID), 1));
    routes.push(patch(STATUS_PATH));

    let (outcome, recorder, body_log) = drive(&policy_with(no_catalog.clone(), json!({})), routes);
    assert_eq!(outcome.health, p::Health::Healthy);
    assert_eq!(
        outcome.created_jobs, 1,
        "one completed recovery is one message; the foreign and the running restores are not \
         recoveries of this policy's points"
    );

    let status = last_status_patch(&body_log);
    let alerts = status["status"]["alerts"]
        .as_array()
        .expect("the ledger carries the recovery");
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0]["kind"].as_str(), Some("RecoveryCompleted"));
    assert_eq!(
        alerts[0]["key"].as_str(),
        Some(p::recovery_completed_key(RESTORE_UID).as_str())
    );
    assert_eq!(
        alerts[0]["state"].as_str(),
        Some("Resolved"),
        "informational, auto-resolved immediately: nothing stays open and nothing re-notifies"
    );

    let event: Value = serde_json::from_str(
        serde_json::from_str::<Value>(
            &bodies(&body_log)
                .into_iter()
                .find(|(m, u, _)| m == "POST" && path_of(u).ends_with("/configmaps"))
                .expect("the event ConfigMap was created")
                .2,
        )
        .expect("JSON")["data"][p::EVENT_DATA_KEY]
            .as_str()
            .expect("the event.json key"),
    )
    .expect("the event document is JSON");
    assert_eq!(event["alert"]["kind"].as_str(), Some("RecoveryCompleted"));
    assert_eq!(
        event["alert"]["action"].as_str(),
        Some("trigger"),
        "a completed recovery is NEWS, not the clearing of a page: a `resolve` would read on a \
         webhook as `the recovery-completed condition has cleared`"
    );

    // The replay: the ledger already knows this restore, so nothing is sent.
    let replayed = policy_with(no_catalog, last_status_patch(&body_log)["status"].clone());
    let key = p::recovery_completed_key(RESTORE_UID);
    let job_name = p::delivery_job_name(POLICY, POLICY_UID, &key, 1, 1);
    let job_path: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], json!({}));
    routes.retain(|r| r.path_suffix != RESTORES_PATH);
    routes.push(ok(
        RESTORES_PATH,
        restore_list(vec![restore("r-1", RESTORE_UID, &set, json!({}))]),
    ));
    routes.push(ok(job_path, running_job(&job_name)));
    let (outcome, recorder2, _) = drive(&replayed, routes);
    assert_eq!(outcome.created_jobs, 0);
    assert!(
        !requests(&recorder2).iter().any(|(m, _)| m == "POST"),
        "a Restore reaches a terminal state once; re-opening would deliver a second message \
         about the same recovery on every reconcile"
    );
    let _ = requests(&recorder);
}

/// A failed delivery is retried after the backoff, as a NEW attempt with a new
/// Job name, and the third failure is the last.
#[test]
fn a_failed_delivery_is_retried_three_times_and_then_stops() {
    let key = p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness);
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    // Attempt 1 failed 10 minutes ago; the 60-second backoff has elapsed.
    let policy = policy_with(
        no_catalog,
        json!({
            "alerts": [{
                "key": key, "kind": "Staleness", "state": "Open",
                "openedAt": at(4), "transition": 1, "notifiedTransition": 1,
                "delivery": {
                    "state": "Failed", "attempts": 1, "lastAttemptAt": at(1),
                    "jobRef": {"name": p::delivery_job_name(POLICY, POLICY_UID, &key, 1, 1)},
                    "lastError": "a configured sink did not accept (webhook:failed)"
                }
            }]
        }),
    );
    let mut routes = read_routes(vec![backup("b-1", 40, json!({}))], json!({}));
    routes.push(post(
        "/configmaps",
        json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm", "namespace": NS}})
            .to_string(),
    ));
    routes.push(post(
        "/jobs",
        json!({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j", "namespace": NS}, "spec": {}})
            .to_string(),
    ));
    routes.push(first_job_route(&key, 1));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, body_log) = drive(&policy, routes);
    assert_eq!(outcome.created_jobs, 1);
    let status = last_status_patch(&body_log);
    let delivery = &status["status"]["alerts"][0]["delivery"];
    assert_eq!(delivery["attempts"].as_i64(), Some(2));
    assert_eq!(
        delivery["jobRef"]["name"].as_str(),
        Some(p::delivery_job_name(POLICY, POLICY_UID, &key, 1, 2).as_str()),
        "a retry is a NEW Job: the previous one has a terminal pod whose exit code was read"
    );
    assert_eq!(
        status["status"]["alerts"][0]["transition"].as_i64(),
        Some(1),
        "a retry is not a new transition; the condition did not change"
    );
}

/// The backoff is honoured: a failure one second ago is not retried now.
#[test]
fn a_failed_delivery_waits_out_its_backoff() {
    let key = p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness);
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let policy = policy_with(
        no_catalog,
        json!({
            "alerts": [{
                "key": key, "kind": "Staleness", "state": "Open",
                "openedAt": at(4), "transition": 1, "notifiedTransition": 1,
                "delivery": {
                    "state": "Failed", "attempts": 1,
                    "lastAttemptAt": p::rfc3339(now()),
                    "jobRef": {"name": p::delivery_job_name(POLICY, POLICY_UID, &key, 1, 1)},
                    "lastError": "nope"
                }
            }]
        }),
    );
    // NO create routes at all: the double panics if anything is created.
    let mut routes = read_routes(vec![backup("b-1", 40, json!({}))], json!({}));
    routes.push(patch(STATUS_PATH));
    let (outcome, recorder, _) = drive(&policy, routes);
    assert_eq!(outcome.created_jobs, 0);
    assert_eq!(outcome.deferred, 1);
    assert_eq!(
        outcome.requeue_seconds, 15,
        "a deferred page is looked at again soon, not at the evaluation interval"
    );
    assert!(!requests(&recorder).iter().any(|(m, _)| m == "POST"));
}

/// The other half of seam **S7**: a status patch that did NOT land leaves the
/// delivery Job's pod alone.
///
/// A 409 means something wrote this status between the read and the write, so
/// this pass's conclusion is not on the server. Patching the TTL anyway lets
/// the API server's TTL controller delete the Job and its pod before any pass
/// has recorded the exit code, and the delivery is then `NoExitCode` forever.
///
/// MUTANT: drop the `commit.is_committed()` gate around the TTL patch. The
/// route table below has no PATCH route for the Job, so the double panics
/// naming the request.
#[test]
fn a_conflicted_status_patch_leaves_the_delivery_job_alone() {
    let key = p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness);
    let job_name = p::delivery_job_name(POLICY, POLICY_UID, &key, 1, 1);
    let job_path: &'static str = Box::leak(format!("/jobs/{job_name}").into_boxed_str());
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let policy = policy_with(
        no_catalog,
        json!({
            "alerts": [{
                "key": key, "kind": "Staleness", "state": "Open",
                "openedAt": at(4), "transition": 1, "notifiedTransition": 1,
                "delivery": {
                    "state": "Pending", "attempts": 1, "lastAttemptAt": at(1),
                    "jobRef": {"name": job_name.clone()}
                }
            }]
        }),
    );
    let mut routes = read_routes(vec![backup("b-1", 40, json!({}))], json!({}));
    routes.push(ok(job_path, finished_job(&job_name, "Complete")));
    routes.push(ok("/pods", pod_list_exiting(&job_name, JOB_UID, 0)));
    routes.push(ok(
        POD_LOG_PATH,
        "notify-result=pagerduty:ok\nnotify-result=webhook:ok\n".to_string(),
    ));
    // The API server says somebody else wrote this status first.
    routes.push(Route {
        method: "PATCH",
        path_suffix: STATUS_PATH,
        status: 409,
        body: json!({
            "kind": "Status", "apiVersion": "v1", "status": "Failure",
            "message": "the object has been modified", "reason": "Conflict", "code": 409
        })
        .to_string(),
    });
    // NO PATCH route for the Job at all.

    let (outcome, recorder, _) = drive(&policy, routes);
    assert!(!outcome.committed);
    assert_eq!(outcome.ttl_patched, 0);
    assert!(
        !requests(&recorder)
            .iter()
            .any(|(m, u)| m == "PATCH" && u.contains("/jobs/")),
        "a conclusion that is not on the server must not garbage-collect the pod that proves it"
    );
}

// ===========================================================================
// Fix round 1 — the review's findings, each with the row that would have
// caught it
// ===========================================================================

/// **F1.** Protection getting WORSE must never send a `resolve`.
///
/// D3 §3.3's resolve column is "`health` back to `Healthy`/`AtRisk`", not
/// "anything other than `Stale`". Read the wider way, the page that woke
/// on-call resolved itself at the instant the archive stopped existing.
///
/// MUTANT: drop the `may_resolve` gate in `reconcile_alerts`'s `(Some, false)`
/// arm, or widen `resolves_alerts` to `!matches!(health, Health::Stale)`.
/// Either makes the first two assertions fail with `state: Resolved` and a
/// transition of 2.
#[test]
fn protection_getting_worse_never_resolves_the_page() {
    let spec = spec();
    let open = p::reconcile_alerts(
        &[],
        &[p::PolicyAlertKind::Staleness],
        p::Health::Stale,
        POLICY_UID,
        notifications(&spec),
        now(),
    );
    assert_eq!(open.alerts[0].state, "Open");
    assert_eq!(open.alerts[0].transition, Some(1));

    // On-call has been woken: the ledger records a delivery for transition 1,
    // which is what makes "nothing further is due" mean "no second message".
    let mut delivered = open.alerts.clone();
    delivered[0].notified_transition = Some(1);
    delivered[0].delivery = Some(AlertDelivery {
        state: Some("Delivered".to_string()),
        attempts: Some(1),
        last_attempt_at: Some(now()),
        job_ref: None,
        last_error: None,
    });

    // Retention or GC took the last point: `Stale` → `Unprotected`.
    let worse = p::reconcile_alerts(
        &delivered,
        &[],
        p::Health::Unprotected,
        POLICY_UID,
        notifications(&spec),
        now() + Duration::hours(1),
    );
    assert_eq!(
        worse.alerts[0].state, "Open",
        "the objective is breached AND the last point is gone; a `resolve` under the shared \
         dedup key would close the incident at the instant the archive stopped existing"
    );
    assert_eq!(worse.alerts[0].transition, Some(1), "no transition at all");
    assert!(worse.due.is_empty(), "and therefore nothing to deliver");

    // The catalog view expired: `Stale` → `Unknown`. Nothing was measured.
    let unknown = p::reconcile_alerts(
        &delivered,
        &[],
        p::Health::Unknown,
        POLICY_UID,
        notifications(&spec),
        now() + Duration::hours(2),
    );
    assert_eq!(
        unknown.alerts[0].state, "Open",
        "Logweir stopped being able to look; that is not the condition clearing"
    );
    assert!(unknown.due.is_empty());

    // And the two healths that MAY resolve, still do.
    for health in [p::Health::Healthy, p::Health::AtRisk] {
        let resolved = p::reconcile_alerts(
            &delivered,
            &[],
            health,
            POLICY_UID,
            notifications(&spec),
            now() + Duration::hours(3),
        );
        assert_eq!(resolved.alerts[0].state, "Resolved", "{health}");
        assert_eq!(resolved.alerts[0].transition, Some(2), "{health}");
        assert_eq!(resolved.due.len(), 1, "{health}");
    }
    assert!(p::resolves_alerts(p::Health::Healthy));
    assert!(p::resolves_alerts(p::Health::AtRisk));
    for health in [p::Health::Stale, p::Health::Unprotected, p::Health::Unknown] {
        assert!(!p::resolves_alerts(health), "{health}");
    }
}

/// **F2.** `Unprotected` — the worst value the enum has — must page.
///
/// MUTANT: narrow the open rule back to `health == Health::Stale`. The first
/// assertion fails with an empty `open_kinds`, and D3 §15 L5's "exactly 1
/// POST" becomes 0 on the branch its own criterion 1 admits.
#[test]
fn a_policy_with_no_recoverable_point_at_all_opens_an_alert() {
    let spec = spec();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let catalog = p::CatalogAnswer::NotConsulted;

    let verdict = p::evaluate(&inputs(&spec, &[], &catalog, &schedules, &[], &rehearsal));
    assert_eq!(verdict.health, p::Health::Unprotected);
    assert_eq!(
        verdict.open_kinds,
        vec![p::PolicyAlertKind::Staleness],
        "`BackupFailure` does not cover it — that needs consecutiveFailedRuns >= threshold, and \
         a policy with no runs at all has zero"
    );

    // And it is the SAME key `Stale` uses, so a policy that loses its last
    // point while already stale does not open a second incident.
    let stale = p::evaluate(&inputs(
        &spec,
        &[candidate(31)],
        &catalog,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(stale.health, p::Health::Stale);
    assert_eq!(stale.open_kinds, verdict.open_kinds);

    // End to end: the controller creates exactly one delivery Job for it.
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let mut routes = read_routes(Vec::new(), json!({}));
    routes.push(post(
        "/configmaps",
        json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm", "namespace": NS}})
            .to_string(),
    ));
    routes.push(post(
        "/jobs",
        json!({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j", "namespace": NS}, "spec": {}})
            .to_string(),
    ));
    routes.push(first_job_route(
        &p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness),
        1,
    ));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, body_log) = drive(&policy_with(no_catalog, json!({})), routes);
    assert_eq!(outcome.health, p::Health::Unprotected);
    assert_eq!(outcome.created_jobs, 1);
    let status = last_status_patch(&body_log);
    assert_eq!(
        status["status"]["alerts"][0]["kind"].as_str(),
        Some("Staleness")
    );
    assert_eq!(
        status["status"]["alerts"][0]["state"].as_str(),
        Some("Open")
    );
}

/// **F3.** A field this pass computed as `None` must DISAPPEAR from the object.
///
/// An RFC 7386 merge patch removes a key only for an explicit `null`, and every
/// status field is `skip_serializing_if = "Option::is_none"`. Without the nulls
/// the API and the console kept serving a `lastAvailablePoint` naming a point
/// the controller had just decided was not available — with `health: Unknown`
/// beside it.
///
/// MUTANT: build the body with `json!({"status": status})` again. The merged
/// result below keeps both fields and the two `is_null` assertions fail.
#[test]
fn a_cleared_field_disappears_from_the_merged_status() {
    let stored = json!({
        "health": "Stale",
        "staleSince": at(48),
        "lastAvailablePoint": {"pointId": "lwp1-deadbeef", "ageSeconds": 172_800},
        "lastAttempt": {"phase": "Failed"},
        "schedules": [{"name": SCHEDULE}],
        "alerts": [],
        "conditions": []
    });
    let policy = policy_with(
        {
            let mut value = spec_value();
            merge(
                &mut value,
                &json!({"objectives": {"requireCatalogAvailability": false}}),
            );
            value
        },
        stored.clone(),
    );

    // No runs at all, so this pass computes NO point and NO lastAttempt — and
    // (per F2) opens a Staleness alert, which is why the create routes are
    // here.
    let mut routes = read_routes(Vec::new(), json!({}));
    routes.push(post(
        "/configmaps",
        json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm", "namespace": NS}})
            .to_string(),
    ));
    routes.push(post(
        "/jobs",
        json!({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j", "namespace": NS}, "spec": {}})
            .to_string(),
    ));
    routes.push(first_job_route(
        &p::dedup_key(POLICY_UID, p::PolicyAlertKind::Staleness),
        1,
    ));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, body_log) = drive(&policy, routes);
    assert_eq!(outcome.health, p::Health::Unprotected);

    let patch_body = last_status_patch(&body_log)["status"].clone();
    for field in ["lastAvailablePoint", "lastAttempt"] {
        assert!(
            patch_body[field].is_null(),
            "`{field}` must be an explicit null: an omitted key means `leave it alone`, and the \
             console would keep naming a recovery point this verdict just refused"
        );
    }
    // And the merge, applied exactly as the API server would, removes them.
    let mut merged = stored;
    weirkeeper::conditions::apply_merge_patch(&mut merged, &patch_body);
    assert!(merged.get("lastAvailablePoint").is_none(), "{merged}");
    assert!(merged.get("lastAttempt").is_none(), "{merged}");
    assert_eq!(merged["health"].as_str(), Some("Unprotected"));

    // The no-op skip still works: applying the same body to the result changes
    // nothing, so a steady object still sends nothing.
    let before = merged.clone();
    weirkeeper::conditions::apply_merge_patch(&mut merged, &patch_body);
    assert_eq!(before, merged);
}

/// **F5.** A missed slot from last January must not pin a policy to `AtRisk`.
///
/// `BackupSchedule.status.lastMissedSlot` is an audit trail that is never
/// cleared. Read as a live signal it made `Protected=False` — "Logweir checked
/// and you are not protected" — permanent on a schedule that has fired
/// correctly every night since.
///
/// MUTANT: put `missed.last_missed_slot.is_some()` back into `at_risk`. The
/// first assertion fails with `AtRisk`.
#[test]
fn a_missed_slot_older_than_the_last_fire_does_not_pin_at_risk() {
    let spec = spec();
    let rehearsal = p::RehearsalFacts::default();
    let catalog = p::CatalogAnswer::NotConsulted;
    let fresh = [candidate(1)];

    let long_ago = [p::ScheduleFacts {
        last_missed_slot: Some("20250101-000000".to_string()),
        last_fire_time: Some(now() - Duration::minutes(5)),
        ..healthy_schedule()
    }];
    let verdict = p::evaluate(&inputs(&spec, &fresh, &catalog, &long_ago, &[], &rehearsal));
    assert_eq!(
        verdict.health,
        p::Health::Healthy,
        "one controller restart past the miss horizon, in January, must not make the policy red \
         in December"
    );
    assert!(!verdict.missed_since_last_fire);
    assert_eq!(
        verdict.missed.last_missed_slot.as_deref(),
        Some("20250101-000000"),
        "the audit trail is still REPORTED; what it may not do is decide AtRisk"
    );

    // A slot missed SINCE the last fire is the live signal, and does.
    let recent = [p::ScheduleFacts {
        last_missed_slot: Some("20260917-020000".to_string()),
        last_fire_time: Some(now() - Duration::days(1)),
        ..healthy_schedule()
    }];
    let verdict = p::evaluate(&inputs(&spec, &fresh, &catalog, &recent, &[], &rehearsal));
    assert_eq!(verdict.health, p::Health::AtRisk);
    assert!(verdict.missed_since_last_fire);

    // D1 W2's explicit zero beats any inference.
    let declared_none = [p::ScheduleFacts {
        missed_slots: Some(0),
        ..recent[0].clone()
    }];
    assert!(!p::missed_since_last_fire(&declared_none[0]));

    // A schedule that has never fired still owes every recorded miss.
    let never_fired = p::ScheduleFacts {
        last_fire_time: None,
        ..recent[0].clone()
    };
    assert!(p::missed_since_last_fire(&never_fired));

    // An unparseable slot name proves nothing and is not read as recent.
    let nonsense = p::ScheduleFacts {
        last_missed_slot: Some("not-a-slot".to_string()),
        ..recent[0].clone()
    };
    assert!(!p::missed_since_last_fire(&nonsense));
    assert_eq!(
        p::slot_instant("20260917-020000"),
        Some(
            Utc.with_ymd_and_hms(2026, 9, 17, 2, 0, 0)
                .single()
                .expect("the slot instant exists")
        )
    );
}

/// **F6.** A point the catalog calls `UntrustedSigner` is not protection, and
/// the archive alert opens.
///
/// A `TrustPolicy` retires or revokes a signer; W8's catalog re-trust marks the
/// entries, while the `Backup` CR's own `status.evidence` still says `Valid`
/// until W10's asynchronous pass rewrites it. Selecting on the availability
/// axis alone reported `Healthy` with an empty alert set.
///
/// MUTANT: make `CatalogEntry::is_available` test `availability == "Available"`
/// again, or restore the `chosen_is_top` guard. The health assertion fails.
#[test]
fn a_point_the_catalog_does_not_trust_is_not_protection() {
    let spec = spec_with_catalog();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let candidates = [candidate(2)];

    let untrusted = p::CatalogAnswer::Fresh(vec![entry(
        &point_id("b-1"),
        "Available",
        "UntrustedSigner",
    )]);
    let verdict = p::evaluate(&inputs(
        &spec,
        &candidates,
        &untrusted,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_ne!(
        verdict.health,
        p::Health::Healthy,
        "the bytes are there and this installation will not accept the key that signed them"
    );
    assert_eq!(verdict.health, p::Health::Unprotected);
    assert!(
        verdict
            .open_kinds
            .contains(&p::PolicyAlertKind::ArchiveUnavailable),
        "D3 §3.3 opens ArchiveUnavailable for `Missing`/`Unreadable`/`Untrusted` — all three"
    );
    assert!(
        verdict.open_kinds.contains(&p::PolicyAlertKind::Staleness),
        "and F2's rule pages for the Unprotected state itself"
    );

    // `Revoked` and `Invalid` read the same way; `VerifiedHistorical` is a PASS.
    for verification in ["Revoked", "Invalid"] {
        let answer =
            p::CatalogAnswer::Fresh(vec![entry(&point_id("b-1"), "Available", verification)]);
        let verdict = p::evaluate(&inputs(
            &spec,
            &candidates,
            &answer,
            &schedules,
            &[],
            &rehearsal,
        ));
        assert_ne!(verdict.health, p::Health::Healthy, "{verification}");
    }
    let historical = p::CatalogAnswer::Fresh(vec![entry(
        &point_id("b-1"),
        "Available",
        "VerifiedHistorical",
    )]);
    let verdict = p::evaluate(&inputs(
        &spec,
        &candidates,
        &historical,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(
        verdict.health,
        p::Health::Healthy,
        "a key valid when it signed and since retired is what rotation looks like"
    );

    // The materialised `selectable`, where the view carries it, decides.
    let refused: p::CatalogEntry = serde_json::from_value(json!({
        "pointId": point_id("b-1"), "availability": "Available",
        "verification": "Verified", "selectable": false
    }))
    .expect("a view entry parses");
    assert!(!refused.is_available());
}

/// **F9.** The boundary of the one comparison PLAT-14.2 is named after.
///
/// MUTANT (the reviewer's, which SURVIVED all 36 rows): `age <= objective`
/// becomes `age <`. The first assertion then fails.
#[test]
fn the_objective_boundary_is_inclusive() {
    let spec = spec();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let catalog = p::CatalogAnswer::NotConsulted;
    let objective = i64::from(spec.objectives.max_recovery_point_age_seconds);

    let exactly = [p::PointCandidate {
        recovery_point_at: Some(now() - Duration::seconds(objective)),
        ..candidate(0)
    }];
    let verdict = p::evaluate(&inputs(
        &spec,
        &exactly,
        &catalog,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(
        verdict.point.as_ref().and_then(|p| p.age_seconds),
        Some(objective)
    );
    assert_eq!(
        verdict.freshness,
        p::Freshness::Fresh,
        "`maxRecoveryPointAgeSeconds` is the oldest age the objective ALLOWS; a point of exactly \
         that age meets it"
    );
    assert_eq!(verdict.reason, p::FreshnessReason::WithinObjective);
    assert_eq!(verdict.health, p::Health::Healthy);

    let one_more = [p::PointCandidate {
        recovery_point_at: Some(now() - Duration::seconds(objective + 1)),
        ..candidate(0)
    }];
    let verdict = p::evaluate(&inputs(
        &spec,
        &one_more,
        &catalog,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(verdict.freshness, p::Freshness::Stale);
    assert_eq!(verdict.reason, p::FreshnessReason::PointOlderThanObjective);
    assert_eq!(verdict.health, p::Health::Stale);
}

/// **F12.** A catalog axis this build cannot read is "could not answer", never
/// "your backups are gone".
///
/// `#[serde(default)]` on the two axes means a rename in W8's `ViewEntry`
/// yields `""` for every entry. Read as "not available" that puts every
/// catalog-backed policy in the cluster into `Unprotected` at once, on a schema
/// change — and (per F2) pages for all of them.
///
/// MUTANT: drop the `has_blank_axis` arm from `evaluate`'s `unresolvable`
/// chain. The health assertion reads `Unprotected` instead of `Unknown`.
#[test]
fn a_blank_catalog_axis_reads_as_unreadable_and_not_as_missing() {
    let spec = spec_with_catalog();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let candidates = [candidate(2)];

    let renamed: p::CatalogEntry = serde_json::from_value(json!({
        "pointId": point_id("b-1"),
        "availabilityState": "Available",
        "verificationState": "Verified"
    }))
    .expect("an entry from a renamed writer still parses");
    assert!(renamed.has_blank_axis());

    let answer = p::CatalogAnswer::Fresh(vec![renamed]);
    assert!(answer.blank_axis());
    let verdict = p::evaluate(&inputs(
        &spec,
        &candidates,
        &answer,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(
        verdict.health,
        p::Health::Unknown,
        "a field this build cannot find is a view that could not answer; reporting Unprotected \
         would tell a whole cluster its backups are gone because of a rename"
    );
    assert_eq!(verdict.reason, p::FreshnessReason::CatalogUnreadable);
    assert!(
        verdict.open_kinds.is_empty(),
        "and nothing was measured, so nothing pages"
    );
}

/// **F10.** The recovery event is stamped when the Restore FINISHED.
#[test]
fn a_recovery_is_stamped_when_it_finished_and_not_when_it_was_created() {
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let set = format!("{SCHEDULE_UID}-b-1");
    let finished = at(1);
    let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], json!({}));
    routes.retain(|r| r.path_suffix != RESTORES_PATH);
    routes.push(ok(
        RESTORES_PATH,
        restore_list(vec![restore(
            "r-long",
            RESTORE_UID,
            &set,
            json!({
                // Created eight hours ago, finished one hour ago.
                "metadata": {"creationTimestamp": at(8)},
                "status": {"conditions": [
                    {"type": "Complete", "status": "True", "lastTransitionTime": finished}
                ]}
            }),
        )]),
    ));
    routes.push(post(
        "/configmaps",
        json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cm", "namespace": NS}})
            .to_string(),
    ));
    routes.push(post(
        "/jobs",
        json!({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j", "namespace": NS}, "spec": {}})
            .to_string(),
    ));
    routes.push(first_job_route(&p::recovery_completed_key(RESTORE_UID), 1));
    routes.push(patch(STATUS_PATH));
    let (_, _, body_log) = drive(&policy_with(no_catalog, json!({})), routes);

    let status = last_status_patch(&body_log);
    let alert = &status["status"]["alerts"][0];
    assert_eq!(
        alert["openedAt"].as_str(),
        Some(finished.as_str()),
        "a long recovery is created hours before it completes, and D3 §3.5's incident-facing \
         surface reads the instant this ledger entry carries"
    );
    assert_eq!(alert["resolvedAt"].as_str(), Some(finished.as_str()));
}
