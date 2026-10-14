//! The `ProtectionPolicy` reconciler and its pure half.
//!
//! Every test here is a pure-function or `mock_client` test, and the double
//! PANICS on an unrouted request, which turns "it never patched a Backup" and
//! "it created nothing" into assertions rather than absences of evidence.
//!
//! Alert delivery is withdrawn: the ledger rows below assert what the ledger
//! records, and
//! [`a_policy_that_still_names_alert_sinks_says_they_are_withdrawn`] the
//! named withdrawal.

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Value};
use weirkeeper::controllers::protection_policy::{self as pp, ProtectionContext};
use weirkeeper::crds::protection_policy::{AlertKind, ProtectionPolicy, ProtectionPolicySpec};
use weirkeeper::protection as p;
use weirkeeper::testing::{mock_client_recording_bodies, BodyRecorder, Recorder, Route};

// Fixtures

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

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 17, 12, 0, 0)
        .single()
        .expect("the fixture instant exists")
}

fn rfc3339(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn at(offset_hours: i64) -> String {
    rfc3339(now() - Duration::hours(offset_hours))
}

/// The default spec: one source, one saved destination, one schedule, a
/// 26-hour objective.
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
        "evaluationIntervalSeconds": 300
    })
}

fn spec() -> ProtectionPolicySpec {
    serde_json::from_value(spec_value()).expect("the fixture spec parses")
}

/// The same spec with a `catalogRef`, for the tests whose subject is the
/// catalog conjunct. `evaluate` consults the catalog only when the POLICY asked
/// it to (`catalogRef` set AND `requireCatalogAvailability`).
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

fn catalog_digest(point_id: &str) -> String {
    let prefix = point_id
        .strip_prefix("lwp1-")
        .expect("the fixture point id has the canonical prefix");
    format!("sha256:{prefix}{}", "0".repeat(32))
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

/// A minimal, VALID `KafkaCluster`. The reconciler only asks whether the
/// referenced source exists, but `Api<KafkaCluster>::get_opt` deserializes
/// into the typed object, so a policy that pointed at an object this build
/// cannot decode does not read `Healthy`.
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

/// A PATCH route. The answer is the object the API server would return, not
/// `{}`: `Api::patch_status` deserializes the response into the typed object.
fn patch(path: &'static str) -> Route {
    let body = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "ProtectionPolicy",
        "metadata": {"name": POLICY, "namespace": NS, "uid": POLICY_UID,
                     "resourceVersion": "9091"},
        "spec": spec_value()
    });
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

// The health table across all five states

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
        backups_complete: true,
        now: now(),
    }
}

fn candidate(hours_ago: i64) -> p::PointCandidate {
    let point_id = point_id("b-1");
    p::PointCandidate {
        backup_name: Some("b-1".to_string()),
        receipt_sha256: Some(catalog_digest(&point_id)),
        point_id: Some(point_id),
        backup_id: Some("set-1".to_string()),
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

/// `unknown` is a third answer, not a shade of either other one.
///
/// MUTANT: make `Freshness::Unknown` fall through to the `Fresh` arm (or to
/// `Stale`) in `evaluate`. The first assertion fails on the enum, the second
/// on the condition status, and the third on the alert set: three independent
/// places, because "unknown rendered as healthy" is the defect this kind
/// exists to prevent.
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

// The availability rule

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
        "receiptSha256": catalog_digest(point_id),
        "backupId": "set-1",
        "recoveryPointAtMs": 1_700_000_000_000_i64,
        "availability": availability,
        "verification": verification,
        // BOTH axes, exactly as `catalog_view::view_entry` materialises it
        // (`availability.selectable() && verification.selectable()`): a helper
        // keyed on availability alone would hand the reader a `selectable` the real
        // catalog never writes.
        "selectable": availability == "Available"
            && matches!(verification, "Verified" | "VerifiedHistorical"),
        "runId": "ignored-unknown-field",
        "coveredFromMs": 0
    }))
    .expect("a view entry parses leniently")
}

/// A truncated `pointId` collision must not let a different receipt satisfy a
/// digest-bound candidate, even when the archive-set id also matches.
///
/// MUTANT: join digest-bearing candidates on `point_id` alone. The negative
/// assertion becomes available. MUTANT 2: fall back to `backup_id` after a
/// full-digest miss. The same assertion becomes available.
#[test]
fn the_catalog_join_requires_the_full_receipt_digest() {
    let candidate = candidate(2);
    let id = candidate.point_id.as_deref().expect("the point has an id");
    let exact = p::CatalogAnswer::Fresh(vec![entry(id, "Available", "Verified")]);
    assert!(p::is_available(
        &candidate,
        &spec_with_catalog_only(),
        &exact
    ));

    let mut collision = entry(id, "Available", "Verified");
    collision.receipt_sha256 = format!(
        "sha256:{}{}",
        id.strip_prefix("lwp1-").expect("canonical point id"),
        "f".repeat(32)
    );
    assert_ne!(
        collision.receipt_sha256,
        candidate.receipt_sha256.as_deref().expect("full digest")
    );
    let collided = p::CatalogAnswer::Fresh(vec![collision]);
    assert!(
        !p::is_available(&candidate, &spec_with_catalog_only(), &collided),
        "equal truncated ids and backup ids cannot substitute for the full digest"
    );
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
    newest.receipt_sha256 = newest.point_id.as_deref().map(catalog_digest);
    let mut older = candidate(10);
    older.backup_name = Some("b-old".to_string());
    older.point_id = Some(point_id("b-old"));
    older.receipt_sha256 = older.point_id.as_deref().map(catalog_digest);
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

// A point whose verification the controller could not reach

/// A succeeded point on a destination whose `evidenceRead` grant is
/// `SecretKeys`, where the controller reached NO verification verdict (it
/// says so, as `NotAttempted` with a sentence) and therefore wrote no
/// `status.capture` and no `status.evidence.receiptSha256`. The run itself
/// succeeded and the archive is fine.
fn unreadable_point(hours_ago: i64) -> p::PointCandidate {
    p::PointCandidate {
        point_id: None,
        recovery_point_at: None,
        newest_record_at: Some(now() - Duration::hours(hours_ago)),
        evidence: p::Evidence::NotAttempted,
        ..candidate(hours_ago)
    }
}

/// The catalog-backed spec with the evidence objective OFF, so a test can
/// exercise the catalog JOIN without the evidence objective deciding first.
fn spec_with_catalog_only() -> ProtectionPolicySpec {
    let mut value = spec_value();
    merge(
        &mut value,
        &json!({
            "protects": {"catalogRef": {"name": CATALOG}},
            "objectives": {"requireVerifiedEvidence": false}
        }),
    );
    serde_json::from_value(value).expect("the catalog-only fixture spec parses")
}

/// [`entry`] with the archive set id said out loud.
fn entry_in_set(
    point_id: &str,
    backup_id: &str,
    availability: &str,
    verification: &str,
) -> p::CatalogEntry {
    p::CatalogEntry {
        backup_id: backup_id.to_string(),
        ..entry(point_id, availability, verification)
    }
}

/// A succeeded point the controller could not place in time is `Unknown`, and
/// it does not page.
///
/// MUTANT: delete the `PointFactsUnread` arm from `evaluate`'s `unresolvable`
/// chain (or gate it on something that is never true). The candidate falls out
/// of the available set, `newest` is `None`, and the verdict is
/// `Unprotected`/`NoAvailablePoint` with a `Staleness` alert open and the
/// sentence "there is no available recovery point for this policy at all",
/// about an archive that holds the point. Four assertions fail, on the health,
/// the reason, the alert set and the sentence.
#[test]
fn a_point_whose_facts_are_unreadable_is_unknown_and_never_unprotected() {
    let spec = spec();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let catalog = p::CatalogAnswer::NotConsulted;
    let unreadable = [unreadable_point(2)];

    let verdict = p::evaluate(&inputs(
        &spec,
        &unreadable,
        &catalog,
        &schedules,
        &[],
        &rehearsal,
    ));

    assert_eq!(
        verdict.health,
        p::Health::Unknown,
        "D3 §3.2 gives `Unknown` to evaluation-impossible; `Unprotected` is reserved for no \
         available point at all"
    );
    assert_eq!(verdict.freshness, p::Freshness::Unknown);
    assert_eq!(verdict.reason, p::FreshnessReason::PointFactsUnread);
    assert!(
        !verdict.open_kinds.contains(&p::PolicyAlertKind::Staleness),
        "`Staleness` covers `Unprotected` by design, so reading an unreadable receipt as \
         `Unprotected` PAGES about an archive nobody has looked at"
    );
    assert!(
        !verdict.summary.contains("no available recovery point"),
        "that sentence reaches a PagerDuty incident title verbatim and is false here: {}",
        verdict.summary
    );
    assert!(
        verdict.summary.contains("could not be placed in time"),
        "the sentence must say what is missing — a read — and not name an object that is \
         present: {}",
        verdict.summary
    );

    // NEGATIVE CONTROL 1, and the reason this test cannot be satisfied by
    // making everything `Unknown`: a policy with NO candidate at all is still
    // `Unprotected`, which is the state that should page.
    let verdict = p::evaluate(&inputs(&spec, &[], &catalog, &schedules, &[], &rehearsal));
    assert_eq!(verdict.health, p::Health::Unprotected);
    assert_eq!(verdict.reason, p::FreshnessReason::NoAvailablePoint);
    assert!(verdict.open_kinds.contains(&p::PolicyAlertKind::Staleness));

    // NEGATIVE CONTROL 2: the set is points the policy COVERS. A failed run, or
    // one of another source, carries no capture time either; reading those as
    // "could not evaluate" would hide a policy that is genuinely unprotected.
    let failed = [p::PointCandidate {
        phase: Some("Failed".to_string()),
        exit_code: Some(3),
        ..unreadable_point(2)
    }];
    let verdict = p::evaluate(&inputs(
        &spec,
        &failed,
        &catalog,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(verdict.health, p::Health::Unprotected);
    assert_eq!(verdict.reason, p::FreshnessReason::NoAvailablePoint);

    let foreign = [p::PointCandidate {
        source_matches: false,
        ..unreadable_point(2)
    }];
    assert_eq!(
        p::evaluate(&inputs(
            &spec,
            &foreign,
            &catalog,
            &schedules,
            &[],
            &rehearsal
        ))
        .reason,
        p::FreshnessReason::NoAvailablePoint
    );
}

/// A captured receipt digest gives the point an identity; it does not mean a
/// verifier read the receipt. NotAttempted therefore remains the
/// `Unknown/PointFactsUnread` arm.
#[test]
fn a_not_attempted_point_with_a_digest_is_still_unread_and_unknown() {
    assert!(
        !p::Evidence::NotAttempted.was_reached(),
        "a digest is capture metadata, not a reached verification verdict"
    );
    let mut point = unreadable_point(2);
    point.point_id = Some(point_id("digest-only"));
    point.receipt_sha256 = point.point_id.as_deref().map(catalog_digest);
    let verdict = p::evaluate(&inputs(
        &spec(),
        &[point],
        &p::CatalogAnswer::NotConsulted,
        &[healthy_schedule()],
        &[],
        &p::RehearsalFacts::default(),
    ));
    assert_eq!(verdict.health, p::Health::Unknown);
    assert_eq!(verdict.reason, p::FreshnessReason::PointFactsUnread);
}

/// `PointFactsUnread` is the LAST resort and never masks a reason an operator
/// can act on, nor a point that IS placeable.
///
/// MUTANT: move the arm above `CatalogStale`/`SourceMissing`, or drop the
/// `newest.is_none()` gate. The first two assertions fail on the reason; the
/// third fails on the health: a policy with a good fresh point would report
/// `Unknown` for an older sibling nobody can read.
#[test]
fn an_unplaceable_point_never_masks_a_more_specific_reason_or_a_placeable_one() {
    let with_catalog = spec_with_catalog();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let unreadable = [unreadable_point(2)];

    let mut gone = inputs(
        &with_catalog,
        &unreadable,
        &p::CatalogAnswer::NotConsulted,
        &schedules,
        &[],
        &rehearsal,
    );
    gone.source_exists = false;
    assert_eq!(
        p::evaluate(&gone).reason,
        p::FreshnessReason::SourceMissing,
        "a missing source names an object the operator can go and look at; it wins"
    );

    let stale = p::CatalogAnswer::Stale(p::FreshnessReason::CatalogStale);
    assert_eq!(
        p::evaluate(&inputs(
            &with_catalog,
            &unreadable,
            &stale,
            &schedules,
            &[],
            &rehearsal
        ))
        .reason,
        p::FreshnessReason::CatalogStale
    );

    // A placeable point decides, and the unreadable sibling changes nothing.
    let plain = spec();
    let both = [unreadable_point(40), candidate(2)];
    let verdict = p::evaluate(&inputs(
        &plain,
        &both,
        &p::CatalogAnswer::NotConsulted,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(verdict.health, p::Health::Healthy);
    assert_eq!(verdict.reason, p::FreshnessReason::WithinObjective);
}

/// A point with a capture time and NO receipt digest — the join's own case,
/// isolated from the two clauses the other fixes close.
fn no_receipt_point(hours_ago: i64) -> p::PointCandidate {
    p::PointCandidate {
        point_id: None,
        receipt_sha256: None,
        evidence: p::Evidence::NotAttempted,
        ..candidate(hours_ago)
    }
}

/// The catalog join answers on the ARCHIVE SET id when the controller never
/// read the receipt that would have given the point an identity.
///
/// MUTANT: restore `match candidate.point_id { Some(id) => …, None => false }`
/// in `is_available`'s `Fresh` arm: the first assertion fails and the health
/// reads `Unprotected` over a catalog that lists the point as
/// `Available`/`Verified`. MUTANT 2: drop the `point_id.is_none()` guard in
/// `entries_for` so the coarser key is tried for a candidate that HAS an
/// identity the view does not list: the fourth assertion fails.
#[test]
fn the_catalog_join_falls_back_to_the_archive_set_id() {
    let spec = spec_with_catalog_only();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let points = [no_receipt_point(2)];

    let present = p::CatalogAnswer::Fresh(vec![entry_in_set(
        &point_id("b-1"),
        "set-1",
        "Available",
        "Verified",
    )]);
    assert!(
        p::is_available(&points[0], &spec, &present),
        "`backupId` is on the Backup from the pre-Job patch and on the catalog row; the fact \
         the join needs was already on both sides"
    );
    let verdict = p::evaluate(&inputs(
        &spec,
        &points,
        &present,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(verdict.health, p::Health::Healthy);
    assert_eq!(verdict.basis, p::AvailabilityBasis::Catalog);

    // NEGATIVE CONTROL 1 — a row of ANOTHER archive set does not answer for
    // this point. Without this the join would be "any entry at all".
    let elsewhere = p::CatalogAnswer::Fresh(vec![entry_in_set(
        &point_id("b-1"),
        "set-2",
        "Available",
        "Verified",
    )]);
    assert!(!p::is_available(&points[0], &spec, &elsewhere));

    // NEGATIVE CONTROL 2: an EMPTY `backupId` is not a key. `serde(default)`
    // hands `""` to a view that does not write the field, and joining on it
    // would make every such row an answer for every unread point.
    let blank = p::PointCandidate {
        backup_id: Some(String::new()),
        ..no_receipt_point(2)
    };
    let blank_row = p::CatalogAnswer::Fresh(vec![entry_in_set(
        &point_id("b-1"),
        "",
        "Available",
        "Verified",
    )]);
    assert!(!p::is_available(&blank, &spec, &blank_row));

    // NEGATIVE CONTROL 3: `point_id` is the narrower key and DECIDES where the
    // candidate has one: a point whose identity the view does not list is a
    // point the view does not know.
    let listed_elsewhere = p::PointCandidate {
        point_id: Some(point_id("b-9")),
        receipt_sha256: Some(catalog_digest(&point_id("b-9"))),
        ..no_receipt_point(2)
    };
    assert!(!p::is_available(&listed_elsewhere, &spec, &present));

    // NEGATIVE CONTROL 4: an AMBIGUOUS archive-set join reads no facts at all.
    // Two rows for one set are two points, and either one's capture time would
    // describe a recovery point that is not this one.
    let twice = p::CatalogAnswer::Fresh(vec![
        entry_in_set(&point_id("b-1"), "set-1", "Available", "Verified"),
        entry_in_set(&point_id("b-2"), "set-1", "Available", "Verified"),
    ]);
    assert!(p::catalog_entry_for(&points[0], &twice).is_none());
    assert!(
        p::catalog_entry_for(&points[0], &present).is_some(),
        "one row for the set IS readable; the `None` above is about ambiguity and not about \
         the join"
    );

    // And the alert reaches it too: `ArchiveUnavailable` must not use a
    // point-id-only lookup, or it stays SHUT over exactly these points.
    let broken = p::CatalogAnswer::Fresh(vec![entry_in_set(
        &point_id("b-1"),
        "set-1",
        "Missing",
        "Verified",
    )]);
    let verdict = p::evaluate(&inputs(
        &spec,
        &points,
        &broken,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert!(
        verdict
            .open_kinds
            .contains(&p::PolicyAlertKind::ArchiveUnavailable),
        "the bytes this policy would otherwise count are gone; that is D3 §3.3's alert"
    );
}

/// `requireVerifiedEvidence` is answered by whichever component read the
/// signature, and a verdict the controller DID reach still decides.
///
/// MUTANT: restore the unconditional `if require_verified_evidence &&
/// !candidate.evidence.is_verified() { return false }` at the top of
/// `is_available`. The first assertion fails: the policy reads `Unprotected`
/// over a catalog row that says `Verified`, for a fact the controller has
/// said it does not have. MUTANT 2: widen `evidence_objective_met` to
/// `!candidate.evidence.is_verified()` (let the catalog overrule ANY verdict):
/// the `Untrusted` assertion fails and `TrustPolicy` becomes decorative.
/// MUTANT 3: ask the objective of `entry.is_available()` instead of
/// `entry.is_verified()`: the `selectable`-disagrees assertion fails.
#[test]
fn the_catalog_answers_the_evidence_objective_only_where_the_controller_did_not() {
    let spec = spec_with_catalog();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let unread = [no_receipt_point(2)];
    let sound = p::CatalogAnswer::Fresh(vec![entry_in_set(
        &point_id("b-1"),
        "set-1",
        "Available",
        "Verified",
    )]);

    assert!(
        p::is_available(&unread[0], &spec, &sound),
        "the catalog controller IS the component this credential model gives the key to; it \
         verified this very receipt and published the answer on the row"
    );
    let verdict = p::evaluate(&inputs(&spec, &unread, &sound, &schedules, &[], &rehearsal));
    assert_eq!(verdict.health, p::Health::Healthy);
    assert!(verdict.open_kinds.is_empty());

    // NEGATIVE CONTROL 1 — a verdict the controller DID reach decides.
    // `Untrusted` is a signature this installation refuses, and a catalog row
    // may not overrule it: that is what would make `TrustPolicy` decorative.
    let refused = p::PointCandidate {
        evidence: p::Evidence::Untrusted,
        ..no_receipt_point(2)
    };
    assert!(!p::is_available(&refused, &spec, &sound));

    // NEGATIVE CONTROL 2 — the objective is asked of the VERIFICATION AXIS and
    // not of the materialised `selectable`. A writer that disagrees with
    // itself does not get to answer a question about signatures.
    let disagrees = p::CatalogAnswer::Fresh(vec![p::CatalogEntry {
        selectable: Some(true),
        ..entry_in_set(&point_id("b-1"), "set-1", "Available", "NotAttempted")
    }]);
    assert!(!p::is_available(&unread[0], &spec, &disagrees));

    // NEGATIVE CONTROL 3 — with the objective ON, a point whose bytes the
    // catalog calls `Missing` is not protection AND is not silence: the
    // archive alert is the one that says the bytes are gone.
    let broken = p::CatalogAnswer::Fresh(vec![entry_in_set(
        &point_id("b-1"),
        "set-1",
        "Missing",
        "Verified",
    )]);
    let verdict = p::evaluate(&inputs(
        &spec,
        &unread,
        &broken,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert!(!p::is_available(&unread[0], &spec, &broken));
    assert!(
        verdict
            .open_kinds
            .contains(&p::PolicyAlertKind::ArchiveUnavailable),
        "PLAT-14.2's `unavailable archive` row, on the credential posture the documentation \
         recommends"
    );
}

// The ledger

/// A condition that stays true is ONE transition, however many passes see it.
#[test]
fn a_condition_that_stays_true_is_one_transition() {
    let open = [p::PolicyAlertKind::Staleness];

    // Pass 1 — the alert opens at transition 1.
    let mut ledger = p::reconcile_alerts(&[], &open, p::Health::Stale, POLICY_UID, now());
    assert_eq!(ledger.len(), 1);
    assert_eq!(ledger[0].transition, Some(1));

    // Passes 2..=5 — the condition is still true; nothing moves.
    for pass in 0..4 {
        let out = p::reconcile_alerts(
            &ledger,
            &open,
            p::Health::Stale,
            POLICY_UID,
            now() + Duration::minutes(5 * (pass + 1)),
        );
        assert_eq!(
            out[0].transition,
            Some(1),
            "pass {pass}: a condition that stays true produces no further transitions"
        );
        ledger = out;
    }
}

#[test]
fn the_key_is_stable_across_open_and_resolve_and_is_its_own_family() {
    let open = p::reconcile_alerts(
        &[],
        &[p::PolicyAlertKind::Staleness],
        p::Health::Stale,
        POLICY_UID,
        now(),
    );
    let key = open[0].key.clone();
    assert_eq!(key, format!("logweir-protection-{POLICY_UID}-Staleness"));

    let resolved = p::reconcile_alerts(
        &open,
        &[],
        p::Health::Healthy,
        POLICY_UID,
        now() + Duration::hours(1),
    );
    assert_eq!(resolved[0].key, key, "one key across open and resolve");
    assert_eq!(resolved[0].state, "Resolved");
    assert_eq!(
        resolved[0].transition,
        Some(2),
        "the resolve is its own transition"
    );

    assert!(key.starts_with(p::DEDUP_PREFIX));
    assert_eq!(
        p::dedup_key("   ", p::PolicyAlertKind::Staleness),
        "logweir-protection-unknown-Staleness",
        "a blank UID collapses to `unknown`, never to an empty segment — that key would be ONE \
         alert per kind across every policy in the cluster"
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

// The summary never claims an exhaustive check

/// Nothing this controller generates may claim an exhaustive check: the word
/// `exhaustive` is forbidden EVEN INSIDE A DENIAL, because these channels
/// truncate.
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

    // The condition sentence carries NO clock-derived number: the event's does,
    // because it is written once and read by a human, and a condition `message`
    // is part of an object that is compared on every pass.
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

// The point identity

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

// The reconcile

fn drive(policy: &ProtectionPolicy, routes: Vec<Route>) -> (pp::Outcome, Recorder, BodyRecorder) {
    drive_at(policy, routes, now())
}

/// [`drive`] with the reconcile's `now` named. **The clock is a parameter
/// because freezing it hides a status field that is a live clock reading**:
/// two passes at one frozen instant prove nothing about a reconciler whose
/// clock advances.
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
        let outcome = pp::reconcile_policy(policy, &ProtectionContext { client }, at)
            .await
            .expect("the reconcile did not error");
        (outcome, recorder, bodies)
    })
}

/// **Alert sinks are withdrawn, by name.** A policy written before v1 that
/// still carries `spec.notifications` parses, is evaluated as before, and its
/// status says `NotificationsDelivered=False/NotificationsWithdrawn`, naming
/// the field. The route table has no Job or ConfigMap route, so the double
/// would panic on any delivery. CONTROL: the same policy without the field
/// publishes no such condition.
#[test]
fn a_policy_that_still_names_alert_sinks_says_they_are_withdrawn() {
    let mut legacy = spec_value();
    merge(
        &mut legacy,
        &json!({
            "objectives": {"requireCatalogAvailability": false},
            "notifications": {
                "routes": [{
                    "name": "oncall",
                    "pagerDuty": {"routingKeySecretRef": {"name": "pd", "key": "routing-key"}}
                }],
                "renotifyAfterSeconds": 86400
            }
        }),
    );
    let routes = || {
        let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], json!({}));
        routes.push(patch(STATUS_PATH));
        routes
    };
    let (outcome, _, bodies) = drive(&policy_with(legacy.clone(), json!({})), routes());
    assert_eq!(
        outcome.health,
        p::Health::Healthy,
        "health is evaluated as before"
    );
    let status = last_status_patch(&bodies);
    let withdrawn = condition(&status, pp::CONDITION_NOTIFICATIONS_DELIVERED);
    assert_eq!(withdrawn["status"].as_str(), Some("False"));
    assert_eq!(
        withdrawn["reason"].as_str(),
        Some(pp::REASON_NOTIFICATIONS_WITHDRAWN)
    );
    assert!(
        withdrawn["message"]
            .as_str()
            .is_some_and(|m| m.contains("spec.notifications is withdrawn")),
        "{withdrawn}"
    );

    // CONTROL: without the field there is no such condition at all.
    legacy
        .as_object_mut()
        .expect("a spec object")
        .remove("notifications");
    let (_, _, bodies) = drive(&policy_with(legacy, json!({})), routes());
    let status = last_status_patch(&bodies);
    let types: Vec<&str> = status["status"]["conditions"]
        .as_array()
        .expect("conditions")
        .iter()
        .filter_map(|c| c["type"].as_str())
        .collect();
    assert_eq!(types, vec!["Ready", "Protected"]);
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

/// A steady object issues ZERO patches per reconcile.
///
/// MUTANT: write `evaluatedAt: now` unconditionally. The route table below has
/// NO `PATCH` route, so the double panics on the request that should not have
/// been made.
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
    // THE SCHEDULE HAS FIRED, which puts `missed.sinceLastFire` in the status:
    // one of the three clock-derived fields, so the fixture can see it.
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
    // and NO patch route: the double panics on a PATCH.
    //
    // A hundred seconds is inside `evaluatedAt`'s half-interval window (150 s
    // at the fixture's 300 s), so the honest expectation is still ZERO writes:
    // nothing about the cluster changed.
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

/// **A field that becomes absent is cleared, and the policy then settles.**
///
/// The newest point loses its `windowCovered`, so `lastAvailablePoint` no
/// longer carries `newestRecordAt`. A merge patch keeps an omitted key, so
/// the key must be sent as `null`; otherwise `settle_clock_fields` compares
/// the stale stored value with the computed `None` on every pass,
/// `evaluatedAt` is `now` on every pass, and every write wakes this
/// reconciler's own watch.
#[test]
fn a_field_that_becomes_absent_is_cleared_and_the_policy_settles() {
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let fired = json!({"lastFireTime": at(10)});
    let without_window = || {
        let mut b = backup("b-1", 2, json!({}));
        b["status"]
            .as_object_mut()
            .expect("a status")
            .remove("windowCovered");
        b
    };

    // Pass 1: the point's window is known, so `newestRecordAt` is published.
    let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], fired.clone());
    routes.push(patch(STATUS_PATH));
    let (_, _, bodies) = drive(&policy_with(no_catalog.clone(), json!({})), routes);
    let first = last_status_patch(&bodies)["status"].clone();
    assert!(
        first["lastAvailablePoint"]["newestRecordAt"].is_string(),
        "the premise: the window is published: {first}"
    );

    // Pass 2: the window is gone.
    let mut routes = read_routes(vec![without_window()], fired.clone());
    routes.push(patch(STATUS_PATH));
    let (_, _, bodies) = drive_at(
        &policy_with(no_catalog.clone(), first.clone()),
        routes,
        now() + Duration::seconds(10),
    );
    let patch = last_status_patch(&bodies);
    assert!(
        patch["status"]["lastAvailablePoint"]
            .as_object()
            .expect("a point object")
            .get("newestRecordAt")
            .is_some_and(Value::is_null),
        "the nested key is sent as an explicit null, which clears it: {patch}"
    );
    let mut stored = first;
    weirkeeper::conditions::apply_merge_patch(&mut stored, &patch["status"]);
    assert!(stored["lastAvailablePoint"].get("newestRecordAt").is_none());

    // Pass 3: nothing changed. NO PATCH route: the double refuses one.
    let routes = read_routes(vec![without_window()], fired);
    let (outcome, recorder, _) = drive_at(
        &policy_with(no_catalog, stored),
        routes,
        now() + Duration::seconds(100),
    );
    assert!(outcome.committed);
    assert!(
        !requests(&recorder).iter().any(|(m, _)| m == "PATCH"),
        "a settled policy writes nothing: {:?}",
        requests(&recorder)
    );
}

/// **A point that ages out is seen by the timed requeue.**
///
/// A recovery point ages with no event on any object, so the pass that turns
/// a policy `Stale` is the one its requeue runs. Driven as kube drives it with
/// no event: pass 1 sees the newest point 120 s inside the 26-hour objective
/// (`Healthy`) and returns `Action::requeue(evaluationIntervalSeconds)`; the
/// pass that requeue runs (no watch event in between) sees it 180 s past the
/// objective and publishes `Stale`. A failed pass is requeued on
/// `ERROR_REQUEUE_SECONDS`.
///
/// Mutants that fail here: `policy_action` returning `await_change()`, or a
/// requeue longer than the evaluation interval.
#[test]
fn a_point_that_ages_out_is_seen_by_the_timed_requeue() {
    use kube::runtime::controller::Action;
    let spec = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
            .as_object_mut()
            .expect("a spec object")
            .remove("notifications");
        value
    };
    let objective = 93_600_i64;
    let started = rfc3339(now() - Duration::seconds(objective - 120));
    let point = backup(
        "b-1",
        26,
        json!({
            "metadata": {"creationTimestamp": started},
            "status": {"capture": {"startedAt": started, "finishedAt": started}}
        }),
    );
    let fired = json!({"lastFireTime": at(26)});
    let routes = || {
        let mut routes = read_routes(vec![point.clone()], fired.clone());
        routes.push(patch(STATUS_PATH));
        routes
    };

    let (first, _, bodies) = drive(&policy_with(spec.clone(), json!({})), routes());
    assert_eq!(
        first.health,
        p::Health::Healthy,
        "120 s inside the objective"
    );
    let interval = 300;
    assert_eq!(
        pp::policy_action(&first),
        Action::requeue(std::time::Duration::from_secs(interval)),
        "the pass hands kube a timed requeue of the evaluation interval"
    );
    assert!(first.requeue_seconds <= interval);
    let written = last_status_patch(&bodies)["status"].clone();

    let next =
        now() + Duration::seconds(i64::try_from(first.requeue_seconds).expect("a small number"));
    let (second, _, bodies) = drive_at(&policy_with(spec, written), routes(), next);
    assert_eq!(
        second.health,
        p::Health::Stale,
        "the pass the requeue runs sees the point past its objective"
    );
    assert_eq!(
        last_status_patch(&bodies)["status"]["health"].as_str(),
        Some("Stale"),
        "and publishes it"
    );
    assert_eq!(
        pp::policy_error_action(),
        Action::requeue(std::time::Duration::from_secs(pp::ERROR_REQUEUE_SECONDS))
    );
}

/// Past `evaluatedAt`'s half-interval window the object IS written, and the
/// only things that move are the three clock-derived fields: a steady object
/// costs one bounded write per interval, not one per reconcile.
///
/// MUTANT: compute `ageSeconds` or `sinceLastFire` from `now` instead of from
/// the settled `evaluatedAt`. The equality below then fails on a field that is
/// not in the allowed set.
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

fn condition<'a>(status: &'a Value, r#type: &str) -> &'a Value {
    status["status"]["conditions"]
        .as_array()
        .expect("the status carries conditions")
        .iter()
        .find(|c| c["type"].as_str() == Some(r#type))
        .unwrap_or_else(|| panic!("no `{type}` condition"))
}

/// A manual run of the schedule is part of its history
/// (`is_run_of_schedule`): `spec.scheduleRef.uid` is the authority and the
/// label is only an index.
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
            "receiptKey": "k", "receiptSha256": receipt_digest("b-1"),
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
            "viewExpiresAt": rfc3339(now() + Duration::hours(1)),
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

/// **A WITHDRAWN DUPLICATE'S VIEW IS STALE, NOT EMPTY.** A `RecoveryCatalog`
/// refused as `DuplicateCatalog` withdraws its pages AND its `viewExpiresAt`,
/// so a policy over it reads `CatalogStale` (availability unknown), never the
/// "no available point" a fresh, empty view would mean.
#[test]
fn a_withdrawn_duplicate_catalog_view_is_stale_not_empty() {
    let withdrawn = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
        "metadata": {"name": CATALOG, "namespace": NS},
        "spec": {"destinationRef": {"name": DESTINATION}, "sync": {}},
        "status": {
            "syncedAt": rfc3339(now() - Duration::minutes(10)),
            "counts": {"total": 1, "available": 1},
            "conditions": [{"type": "Ready", "status": "False", "reason": "DuplicateCatalog"}]
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
    routes.push(ok(CATALOG_PATH, withdrawn));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&policy_with(with_catalog, json!({})), routes);
    assert_eq!(outcome.health, p::Health::Unknown);
    assert_eq!(
        last_status_patch(&bodies)["status"]["availabilityBasis"].as_str(),
        Some("CatalogStale"),
        "a withdrawn view cannot answer; it is not an answer of 'nothing'"
    );
}

/// A `Backup` on a `SecretKeys` destination: a succeeded run, an honest
/// `NotAttempted` verdict, and therefore NO capture block and NO receipt
/// digest. `backupId` survives, because it is written in the pre-Job patch
/// before any verification is attempted.
fn evidence_unread_backup(name: &str, hours_ago: i64) -> Value {
    backup(
        name,
        hours_ago,
        json!({"status": {
            "capture": null,
            "evidence": {
                "receiptSha256": null,
                "verification": {
                    "result": "NotAttempted",
                    "detail": "the destination's evidenceRead grant is SecretKeys and this \
                               controller holds no Secret verb"
                }
            }
        }}),
    )
}

/// A one-entry catalog page and the fresh `RecoveryCatalog` that publishes it.
fn catalog_routes(entry: Value) -> Vec<Route> {
    let page = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": PAGE_CM, "namespace": NS},
        "data": {"entries.jsonl": format!("{entry}\n")}
    })
    .to_string();
    let object = json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "RecoveryCatalog",
        "metadata": {"name": CATALOG, "namespace": NS},
        "spec": {"destinationRef": {"name": DESTINATION}, "sync": {}},
        "status": {
            "viewExpiresAt": rfc3339(now() + Duration::hours(1)),
            "pages": [{"configMapName": PAGE_CM, "index": 0, "count": 1}]
        }
    })
    .to_string();
    vec![ok(CATALOG_PATH, object), ok(PAGE_PATH, page)]
}

/// The catalog row for one archive set, with the two facts the controller
/// could not read off the object.
fn catalog_entry(name: &str, hours_ago: i64, availability: &str, verification: &str) -> Value {
    json!({
        "pointId": point_id(name), "backupId": format!("{SCHEDULE_UID}-{name}"), "runId": "r",
        "recoveryPointAtMs": (now() - Duration::hours(hours_ago)).timestamp_millis(),
        "coveredFromMs": 0, "coveredToMs": 1,
        "receiptKey": "k", "receiptSha256": receipt_digest(name),
        "availability": availability, "verification": verification,
        "selectable": availability == "Available"
            && matches!(verification, "Verified" | "VerifiedHistorical")
    })
}

/// The capture time and the identity come off the catalog row for the SAME
/// point when the controller could not read the receipt that carries them.
///
/// MUTANT: delete the `with_catalog_facts` call at the end of
/// `candidate_from_backup`. The point keeps `recoveryPointAt: null` and
/// `pointId: null`, `evaluate` cannot age it, and the policy reports `Unknown`
/// instead of the `Healthy` this archive has earned: three assertions fail.
/// MUTANT 2: fill from `entry.recovery_point_at_ms` without the `> 0` guard
/// and point the page at a row that omits `recoveryPointAtMs`; the last
/// assertion fails on a capture date in 1970.
#[test]
fn an_unread_points_capture_time_and_identity_come_from_the_catalog_row() {
    // The evidence objective is OFF here, so this test is about the FACTS and
    // not about the verdict; the objective's own arm is the next test.
    let spec = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({
                "protects": {"catalogRef": {"name": CATALOG}},
                "objectives": {"requireVerifiedEvidence": false}
            }),
        );
        value
    };

    let mut routes = read_routes(vec![evidence_unread_backup("b-1", 2)], json!({}));
    routes.extend(catalog_routes(catalog_entry(
        "b-1",
        2,
        "Available",
        "Verified",
    )));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&policy_with(spec.clone(), json!({})), routes);

    assert_eq!(
        outcome.health,
        p::Health::Healthy,
        "the archive holds a two-hour-old point the catalog calls Available/Verified"
    );
    let status = last_status_patch(&bodies);
    let point = &status["status"]["lastAvailablePoint"];
    assert_eq!(
        point["recoveryPointAt"].as_str(),
        Some(rfc3339(now() - Duration::hours(2)).as_str()),
        "`recoveryPointAtMs` IS the receipt's `started_at` carried through the view: the same \
         number from the same signed document by a different route"
    );
    assert_eq!(
        point["pointId"].as_str(),
        Some(point_id("b-1").as_str()),
        "without the identity every event about this policy would have to omit the whole \
         last-available-point block"
    );
    assert_eq!(
        point["evidence"].as_str(),
        Some("NotAttempted"),
        "the verdict is NOT overwritten: the controller still did not read that receipt, and \
         the status says so"
    );

    // NEGATIVE CONTROL 1: a row whose `recoveryPointAtMs` is `0` is a field the
    // view did not write, not a capture in 1970; filling from it would publish
    // a 56-year-old recovery point and a `Stale` health that measured nothing.
    let mut row = catalog_entry("b-1", 2, "Available", "Verified");
    row["recoveryPointAtMs"] = json!(0);
    let mut routes = read_routes(vec![evidence_unread_backup("b-1", 2)], json!({}));
    routes.extend(catalog_routes(row));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&policy_with(spec.clone(), json!({})), routes);
    assert_eq!(outcome.health, p::Health::Unknown);
    assert_eq!(
        condition(&last_status_patch(&bodies), "Protected")["reason"].as_str(),
        Some("PointFactsUnread")
    );

    // NEGATIVE CONTROL 2: a view that holds no row for this archive set fills
    // nothing, and the policy says it could not place the point rather than
    // inventing a time or claiming the archive is empty.
    let mut routes = read_routes(vec![evidence_unread_backup("b-1", 2)], json!({}));
    routes.extend(catalog_routes(catalog_entry(
        "b-9",
        2,
        "Available",
        "Verified",
    )));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&policy_with(spec, json!({})), routes);
    assert_eq!(outcome.health, p::Health::Unknown);
    let status = last_status_patch(&bodies);
    assert_eq!(
        condition(&status, "Protected")["reason"].as_str(),
        Some("PointFactsUnread")
    );
    assert_eq!(
        condition(&status, "Protected")["status"].as_str(),
        Some("Unknown"),
        "a point nobody could place is never rendered as protected and never as a failure"
    );
}

/// **A `SecretKeys` destination is protected and does not page**, on the
/// shipped objectives.
///
/// A succeeded run on a destination whose `evidenceRead` grant is
/// `SecretKeys`, an honest `NotAttempted` verdict, no capture block, no
/// receipt digest, and a catalog whose row for the same point reads
/// `Available`/`Verified`: the policy must not read `Unprotected` and page
/// "there is no available recovery point for this policy at all" about an
/// archive holding a two-hour-old verified point.
///
/// `requireVerifiedEvidence` and `requireCatalogAvailability` are BOTH on here
/// (`spec_with_catalog` is the shipped fixture), so all three refusing clauses
/// are in force and the test fails if any one of them is restored.
#[test]
fn a_secret_keys_destination_is_protected_and_does_not_page() {
    let mut routes = read_routes(vec![evidence_unread_backup("b-1", 2)], json!({}));
    routes.extend(catalog_routes(catalog_entry(
        "b-1",
        2,
        "Available",
        "Verified",
    )));
    routes.push(patch(STATUS_PATH));
    let spec = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"protects": {"catalogRef": {"name": CATALOG}}}),
        );
        value
    };
    let (outcome, _, bodies) = drive(&policy_with(spec, json!({})), routes);

    assert_eq!(outcome.health, p::Health::Healthy);
    let status = last_status_patch(&bodies);
    assert_eq!(status["status"]["health"].as_str(), Some("Healthy"));
    assert_eq!(
        status["status"]["availabilityBasis"].as_str(),
        Some("Catalog")
    );
    assert_eq!(
        condition(&status, "Protected")["status"].as_str(),
        Some("True")
    );
    assert_eq!(
        status["status"]["alerts"].as_array().map_or(0, Vec::len),
        0,
        "an archive with a fresh verified point opens no alert"
    );
    assert_eq!(
        status["status"]["lastAvailablePoint"]["pointId"].as_str(),
        Some(point_id("b-1").as_str())
    );
    assert_eq!(
        status["status"]["lastAvailablePoint"]["evidence"].as_str(),
        Some("NotAttempted"),
        "the status still reports what this controller did and did not read"
    );
}

// One row per `evidenceRead` grant kind, and the flip between them

/// A `SecretKeys`-shaped run whose receipt the controller DID read and found is
/// not what it claims to be.
///
/// `VerificationVerdict::Invalid` is "the digest did not match, or no roster
/// signing key verified the sidecar", and `controllers::backup` never produces
/// it from a storage failure. `status.capture` is absent, because it is written
/// only on a `Valid` verdict.
///
/// `receiptSha256` IS present: it is written on the terminal patch whenever the
/// receipt bytes were fetched and hashed, before any verdict exists, so a
/// refused point arrives with `point_id: Some` and `recovery_point_at: None`:
/// the shape the catalog join matches on `pointId`.
fn invalid_verdict_backup(name: &str, hours_ago: i64) -> Value {
    backup(
        name,
        hours_ago,
        json!({"status": {
            "capture": null,
            "evidence": {
                "verification": {"result": "Invalid", "detail": "the receipt bytes do not hash to the recorded digest"}
            }
        }}),
    )
}

/// A verdict the controller REACHED is never answered by the catalog, however
/// good the row is.
///
/// A digest mismatch folded into `NotAttempted` (the one verdict
/// `evidence_objective_met` defers for) would let a tampered archive read
/// `Healthy`/`Protected=True` behind a catalog view harvested before the
/// tampering; the view is served for `max(3 × intervalSeconds, 3600)`, up to
/// an hour by default. This row drives the shipped objectives with a row that
/// says `Available`/`Verified` sitting right there.
///
/// MUTANT: delete `(Some("Invalid"), _) => Self::Invalid` from
/// `Evidence::from_verification`, or `Invalid` from the `wire_enum!`. The
/// verdict folds into `NotAttempted`, the catalog answers for it, and the
/// health, the condition and the alert assertions all fail.
#[test]
fn an_invalid_verdict_is_unprotected_and_pages_however_good_the_catalog_row_is() {
    assert_eq!(
        p::Evidence::from_verification(Some("Invalid"), None),
        p::Evidence::Invalid,
        "a digest that did not match is not `the controller could not look`"
    );
    assert!(!p::Evidence::Invalid.is_verified());
    assert!(
        p::Evidence::Invalid.was_reached(),
        "`was_reached` is the single test everything in the module asks; `NotAttempted` is the \
         only member it answers `false` for"
    );

    let spec = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"protects": {"catalogRef": {"name": CATALOG}}}),
        );
        value
    };
    let mut routes = read_routes(vec![invalid_verdict_backup("b-1", 2)], json!({}));
    routes.extend(catalog_routes(catalog_entry(
        "b-1",
        2,
        "Available",
        "Verified",
    )));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&policy_with(spec, json!({})), routes);

    assert_eq!(
        outcome.health,
        p::Health::Unprotected,
        "the controller read this receipt and refuses it; a catalog row does not overrule that"
    );
    let status = last_status_patch(&bodies);
    assert_eq!(
        condition(&status, "Protected")["status"].as_str(),
        Some("False"),
        "not `Unknown`: Logweir knows perfectly well that you are not protected"
    );
    let alerts = status["status"]["alerts"]
        .as_array()
        .expect("a refused archive pages");
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0]["kind"].as_str(), Some("Staleness"));
    assert_eq!(alerts[0]["state"].as_str(), Some("Open"));
    assert_eq!(
        status["status"]["lastAvailablePoint"].as_object(),
        None,
        "and nothing is published as this policy's recovery point"
    );
}

/// A `Backup` whose `Valid` sits on a basis the badge refuses is not
/// protecting evidence, and the reconciler hands
/// `Evidence::from_verification` the whole `trust` block so it can tell. The
/// unit table is `tests/trust_basis_class.rs`; this row pins the call site in
/// `controllers::protection_policy`.
///
/// MUTANT: pass `None` for the trust block in `candidate_from_backup`
/// (`controllers/protection_policy.rs`), or revert `from_verification` to the
/// plain `Valid` check. The `Unverified`, `RecordedBeforeRevocation` and
/// `None` rows go `Healthy` and fail; the `Current`, `Historical` and no-block
/// rows are the negative control that the objective still passes.
#[test]
fn a_valid_backup_protects_only_on_a_basis_the_badge_admits() {
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    for (label, trust, protects) in [
        ("Current", json!({"basis": "Current"}), true),
        ("Historical", json!({"basis": "Historical"}), true),
        ("no trust block", Value::Null, true),
        ("Unverified", json!({"basis": "Unverified"}), false),
        (
            "RecordedBeforeRevocation",
            json!({"basis": "RecordedBeforeRevocation"}),
            false,
        ),
        ("None", json!({"basis": "None"}), false),
    ] {
        let mut object = backup("b-1", 2, json!({}));
        if !trust.is_null() {
            object["status"]["evidence"]["verification"]["trust"] = trust;
        }
        let mut routes = read_routes(vec![object], json!({}));
        routes.push(patch(STATUS_PATH));
        let (outcome, _, bodies) = drive(&policy_with(no_catalog.clone(), json!({})), routes);
        let status = last_status_patch(&bodies);
        if protects {
            assert_eq!(outcome.health, p::Health::Healthy, "{label}");
            assert_eq!(
                status["status"]["lastAvailablePoint"]["pointId"].as_str(),
                Some(point_id("b-1").as_str()),
                "{label}"
            );
        } else {
            assert_ne!(
                outcome.health,
                p::Health::Healthy,
                "{label}: a Valid on this basis is not verified evidence"
            );
            assert_eq!(
                status["status"]["lastAvailablePoint"].as_object(),
                None,
                "{label}: and it is not published as the policy's recovery point"
            );
        }
    }
}

/// A verification result THIS BUILD does not know (reachable after a rollback
/// past a build that wrote a fifth verdict) is still a verdict some verifier
/// reached. It is read as a refusal (`Untrusted`), never as `NotAttempted`,
/// the one verdict a catalog row may answer for; the rehearsal join applies
/// the same rule.
///
/// MUTANT: map `(Some(_), _)` in `Evidence::from_verification` back to
/// `NotAttempted`. The row goes `Healthy` behind the `Verified` catalog entry
/// and every assertion below fails.
#[test]
fn an_unrecognised_verdict_is_a_refusal_the_catalog_cannot_overrule() {
    let evidence = p::Evidence::from_verification(Some("SomeFutureVerdict"), None);
    assert!(evidence.was_reached() && !evidence.is_verified());
    assert_eq!(
        p::Evidence::from_verification(None, None),
        p::Evidence::NotAttempted
    );
    assert_eq!(
        p::Evidence::from_verification(Some("NotAttempted"), None),
        p::Evidence::NotAttempted
    );
    // `Pending` is not a reached verdict: an evidence-fetch Job is still
    // reading the document. Read as a refusal, every freshly finished
    // destination-backed run would page `Unprotected`.
    assert_eq!(
        p::Evidence::from_verification(Some("Pending"), None),
        p::Evidence::NotAttempted
    );

    let spec = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"protects": {"catalogRef": {"name": CATALOG}}}),
        );
        value
    };
    let mut unknown = invalid_verdict_backup("b-1", 2);
    unknown["status"]["evidence"]["verification"]["result"] = json!("SomeFutureVerdict");
    let mut routes = read_routes(vec![unknown], json!({}));
    routes.extend(catalog_routes(catalog_entry(
        "b-1",
        2,
        "Available",
        "Verified",
    )));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&policy_with(spec, json!({})), routes);
    assert_eq!(
        outcome.health,
        p::Health::Unprotected,
        "a verdict this build cannot read is not the catalog's to answer"
    );
    let status = last_status_patch(&bodies);
    assert_eq!(
        condition(&status, "Protected")["status"].as_str(),
        Some("False")
    );
    assert_eq!(status["status"]["lastAvailablePoint"].as_object(), None);
}

/// `requireVerifiedEvidence` does not decide whether a REFUSED point may count
/// as protection, only whether an UNVERIFIED one may.
///
/// A point the controller fetched and found `Invalid` has no `status.capture`
/// (written only on `Valid`) but does carry `receiptSha256`, so it reaches
/// `with_catalog_facts`; handed the view's capture time with the objective
/// off, it would become the policy's newest available point: `Healthy`,
/// `Protected=True`, no alert, beside `evidence: "Invalid"`. A refused point is
/// `Unprotected` at every setting of the objective; turning off *verified
/// evidence* does not make a mismatched digest healthy.
///
/// All four postures, so the row pins the axis rather than one corner of it.
///
/// MUTANT: delete `if candidate.evidence.was_reached() { return candidate; }`
/// from `with_catalog_facts`. The fourth posture goes `Healthy` and three
/// assertions fail; the other three are unchanged, which makes this row about
/// the flag.
#[test]
fn a_refused_verdict_is_unprotected_at_every_setting_of_the_evidence_objective() {
    for require_verified in [true, false] {
        for catalog_ref in [true, false] {
            let spec = {
                let mut value = spec_value();
                merge(
                    &mut value,
                    &json!({"objectives": {"requireVerifiedEvidence": require_verified}}),
                );
                if catalog_ref {
                    merge(
                        &mut value,
                        &json!({"protects": {"catalogRef": {"name": CATALOG}}}),
                    );
                }
                value
            };
            let mut routes = read_routes(vec![invalid_verdict_backup("b-1", 2)], json!({}));
            if catalog_ref {
                routes.extend(catalog_routes(catalog_entry(
                    "b-1",
                    2,
                    "Available",
                    "Verified",
                )));
            }
            routes.push(patch(STATUS_PATH));
            let (outcome, _, bodies) = drive(&policy_with(spec, json!({})), routes);
            let posture =
                format!("requireVerifiedEvidence={require_verified} catalogRef={catalog_ref}");

            assert_eq!(outcome.health, p::Health::Unprotected, "{posture}");
            let status = last_status_patch(&bodies);
            assert_eq!(
                condition(&status, "Protected")["reason"].as_str(),
                Some("NoAvailablePoint"),
                "{posture}"
            );
            assert_eq!(
                status["status"]["lastAvailablePoint"].as_object(),
                None,
                "{posture}: a point whose digest did not match is not this policy's recovery point"
            );
            assert_eq!(
                status["status"]["alerts"]
                    .as_array()
                    .map_or(0, |alerts| alerts
                        .iter()
                        .filter(|a| a["kind"] == "Staleness" && a["state"] == "Open")
                        .count()),
                1,
                "{posture}: a refused archive pages at every setting of the objective"
            );
        }
    }
}

/// A reached-and-refused verdict is `Unprotected` whether or not the point can
/// be placed in time.
///
/// `Backup.status.capture` is written only on a `Valid` verdict, so an
/// `Untrusted` or `Invalid` point has no capture time either. Read as
/// unplaceable, "Logweir checked this archive and refuses its signature"
/// would become `Unknown`, which opens no alert, under a sentence claiming
/// the controller read no verdict.
///
/// MUTANT: drop `&& !candidate.evidence.was_reached()` from `is_unplaceable`.
/// Both arms go `Unknown`/`PointFactsUnread` with an empty alert set and all
/// four assertions per verdict fail.
#[test]
fn a_reached_and_refused_verdict_is_unprotected_even_with_no_capture_time() {
    let spec = spec();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    let catalog = p::CatalogAnswer::NotConsulted;

    for evidence in [p::Evidence::Untrusted, p::Evidence::Invalid] {
        let refused = [p::PointCandidate {
            evidence,
            ..unreadable_point(2)
        }];
        assert!(
            !p::is_unplaceable(&refused[0], &spec),
            "{evidence}: the controller answered; what is missing is not the answer"
        );
        let verdict = p::evaluate(&inputs(
            &spec,
            &refused,
            &catalog,
            &schedules,
            &[],
            &rehearsal,
        ));
        assert_eq!(verdict.health, p::Health::Unprotected, "{evidence}");
        assert_eq!(
            verdict.reason,
            p::FreshnessReason::NoAvailablePoint,
            "{evidence}"
        );
        assert!(
            verdict.open_kinds.contains(&p::PolicyAlertKind::Staleness),
            "{evidence}: an archive this installation refuses must still page"
        );
        assert!(
            !verdict.summary.contains("read no verification verdict"),
            "{evidence}: that sentence reaches a condition message and an incident title, and \
             it is false here: {}",
            verdict.summary
        );
    }

    // NEGATIVE CONTROL — the one verdict the rationale IS about keeps the
    // `Unknown` arm. Without this the fix could be "delete the arm".
    let unread = [unreadable_point(2)];
    assert!(p::is_unplaceable(&unread[0], &spec));
    assert_eq!(
        p::evaluate(&inputs(
            &spec,
            &unread,
            &catalog,
            &schedules,
            &[],
            &rehearsal
        ))
        .reason,
        p::FreshnessReason::PointFactsUnread
    );
}

/// The grant the controller CAN read: a verified receipt, and the policy is
/// protected on the controller's own verdict.
///
/// The counterpart to `a_secret_keys_destination_is_protected_and_does_not_page`:
/// the grant kind where the catalog answers nothing. The point identity is
/// derived from the receipt digest the controller recorded, NOT taken from the
/// catalog row, and the capture time is the object's own.
///
/// MUTANT: make the catalog row AUTHORITATIVE rather than a fallback (drop
/// `with_catalog_facts`'s reached-verdict return and both `is_none()`
/// conjuncts). The row here carries a capture time 40 h old against a 26 h
/// objective, so the health and `recoveryPointAt` assertions both fail.
///
/// Dropping either `is_none()` conjunct alone is unobservable, each for a
/// DIFFERENT reason: `point_id` because where it is `Some`, `entries_for`
/// matched on it, so the assignment is a no-op; the capture time because
/// `capture` is written only on a `Valid` verdict, which the reached-verdict
/// return has already excluded.
#[test]
fn a_controller_identity_destination_with_a_verified_receipt_is_protected() {
    let spec = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"protects": {"catalogRef": {"name": CATALOG}}}),
        );
        value
    };
    // The catalog's row for this very point carries a capture time 40 h old
    // against a 26 h objective, so a fill that OVERWROTE the object's own
    // 2-hour-old one would show on both the health and the published instant.
    // (In the wild the two agree; here they disagree so the "fill, never
    // overwrite" rule has a witness.)
    let row = catalog_entry("b-1", 40, "Available", "Verified");
    let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], json!({}));
    routes.extend(catalog_routes(row));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&policy_with(spec, json!({})), routes);

    assert_eq!(outcome.health, p::Health::Healthy);
    let status = last_status_patch(&bodies);
    assert_eq!(
        condition(&status, "Protected")["status"].as_str(),
        Some("True")
    );
    let point = &status["status"]["lastAvailablePoint"];
    assert_eq!(
        point["evidence"].as_str(),
        Some("Valid"),
        "the controller read this receipt itself and says so"
    );
    assert_eq!(
        point["pointId"].as_str(),
        Some(point_id("b-1").as_str()),
        "the identity is the recorded receipt digest's, not the catalog row's: facts are FILLED \
         where absent and never overwritten"
    );
    assert_eq!(
        point["recoveryPointAt"].as_str(),
        Some(at(2).as_str()),
        "and the capture time is the object's own"
    );
    assert_eq!(status["status"]["alerts"].as_array().map_or(0, Vec::len), 0);
}

/// **The flip, composed**: an open incident, a point the catalog can vouch for
/// arriving, and exactly one resolve, on the `SecretKeys` path.
///
/// The health values are NOT hand-written: each comes out of `evaluate`, so the
/// row breaks if the flip stops happening as well as if the ledger misbehaves.
///
/// MUTANT: make `resolves_alerts` include `Health::Unknown`. The last block
/// fails: Logweir would claim the condition cleared because it stopped being
/// able to look. MUTANT 2: any mutant of the catalog fill; the `Healthy` step
/// never arrives and the resolve assertions fail.
#[test]
fn a_verified_point_arriving_later_flips_the_policy_and_resolves_exactly_one_incident() {
    let spec = spec_with_catalog();
    let schedules = [healthy_schedule()];
    let rehearsal = p::RehearsalFacts::default();
    // 1. The newest point the catalog vouches for is 31 h old: past the 26 h
    //    objective. `Stale`, and `Staleness` opens.
    let old = [no_receipt_point(31)];
    let vouched = p::CatalogAnswer::Fresh(vec![entry_in_set(
        &point_id("b-1"),
        "set-1",
        "Available",
        "Verified",
    )]);
    let stale = p::evaluate(&inputs(&spec, &old, &vouched, &schedules, &[], &rehearsal));
    assert_eq!(stale.health, p::Health::Stale);
    let open = p::reconcile_alerts(&[], &stale.open_kinds, stale.health, POLICY_UID, now());
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].state, "Open");
    assert_eq!(open[0].transition, Some(1));

    let delivered = open.clone();

    // 2. A fresh backup runs. Its receipt is STILL unread — the grant has not
    //    changed — and the catalog harvests the point.
    let fresh = [no_receipt_point(1)];
    let healthy = p::evaluate(&inputs(
        &spec,
        &fresh,
        &vouched,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(healthy.health, p::Health::Healthy);
    assert!(healthy.open_kinds.is_empty());
    let resolved = p::reconcile_alerts(
        &delivered,
        &healthy.open_kinds,
        healthy.health,
        POLICY_UID,
        now() + Duration::hours(1),
    );
    assert_eq!(resolved.len(), 1, "one entry, not a second incident");
    assert_eq!(resolved[0].state, "Resolved");
    assert_eq!(resolved[0].transition, Some(2));

    // 3. A policy with no catalog to answer for it lands on
    //    `Unknown`/`PointFactsUnread` and keeps its incident open: only
    //    `Healthy`/`AtRisk` resolve, and Logweir does not claim a condition
    //    cleared because it stopped being able to look.
    let unknown = p::evaluate(&inputs(
        &spec,
        &[unreadable_point(1)],
        &p::CatalogAnswer::NotConsulted,
        &schedules,
        &[],
        &rehearsal,
    ));
    assert_eq!(unknown.health, p::Health::Unknown);
    assert_eq!(unknown.reason, p::FreshnessReason::PointFactsUnread);
    let still_open = p::reconcile_alerts(
        &delivered,
        &unknown.open_kinds,
        unknown.health,
        POLICY_UID,
        now() + Duration::hours(2),
    );
    assert_eq!(still_open[0].state, "Open");
    assert_eq!(still_open[0].transition, Some(1), "no transition");
}

// Structural guards

/// The controller writes no `Backup` or `Restore` status, read off the source.
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
}

// RecoveryCompleted

const RESTORE_UID: &str = "77777777-0000-4000-8000-00000000000a";

/// A terminal `Restore` of a point this policy protects produces exactly ONE
/// informational ledger entry, keyed on the RESTORE UID.
///
/// MUTANT: key it on the policy UID (or on `(policy, kind)`, as every other
/// alert here is). `the second restore of the day` below then collides with
/// the first and the ledger holds one entry instead of two.
/// `recovery_completed_keys_on_the_restore_uid_and_never_on_the_policy`
/// prevents the same at the type level.
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
    routes.push(patch(STATUS_PATH));

    let (outcome, recorder, body_log) = drive(&policy_with(no_catalog.clone(), json!({})), routes);
    assert_eq!(outcome.health, p::Health::Healthy);

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

    // The replay: the ledger already knows this restore, so nothing moves.
    let replayed = policy_with(no_catalog, last_status_patch(&body_log)["status"].clone());
    let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], json!({}));
    routes.retain(|r| r.path_suffix != RESTORES_PATH);
    routes.push(ok(
        RESTORES_PATH,
        restore_list(vec![restore("r-1", RESTORE_UID, &set, json!({}))]),
    ));
    let (_, recorder2, _) = drive(&replayed, routes);
    assert!(
        !requests(&recorder2).iter().any(|(m, _)| m == "POST"),
        "a Restore reaches a terminal state once"
    );
    let _ = requests(&recorder);
}

// Resolve, open, clear and staleness rules

/// Protection getting WORSE must never send a `resolve`: only `health` back to
/// `Healthy`/`AtRisk` resolves. Read wider, the page would resolve itself at
/// the instant the archive stopped existing.
///
/// MUTANT: drop the `may_resolve` gate in `reconcile_alerts`'s `(Some, false)`
/// arm, or widen `resolves_alerts` to `!matches!(health, Health::Stale)`.
/// Either makes the first two assertions fail with `state: Resolved` and a
/// transition of 2.
#[test]
fn protection_getting_worse_never_resolves_the_page() {
    let open = p::reconcile_alerts(
        &[],
        &[p::PolicyAlertKind::Staleness],
        p::Health::Stale,
        POLICY_UID,
        now(),
    );
    assert_eq!(open[0].state, "Open");
    assert_eq!(open[0].transition, Some(1));

    let delivered = open.clone();

    // Retention or GC took the last point: `Stale` → `Unprotected`.
    let worse = p::reconcile_alerts(
        &delivered,
        &[],
        p::Health::Unprotected,
        POLICY_UID,
        now() + Duration::hours(1),
    );
    assert_eq!(
        worse[0].state, "Open",
        "the objective is breached AND the last point is gone; resolving here would close the \
         alert at the instant the archive stopped existing"
    );
    assert_eq!(worse[0].transition, Some(1), "no transition at all");

    // The catalog view expired: `Stale` → `Unknown`. Nothing was measured.
    let unknown = p::reconcile_alerts(
        &delivered,
        &[],
        p::Health::Unknown,
        POLICY_UID,
        now() + Duration::hours(2),
    );
    assert_eq!(
        unknown[0].state, "Open",
        "Logweir stopped being able to look; that is not the condition clearing"
    );

    // And the two healths that MAY resolve, still do.
    for health in [p::Health::Healthy, p::Health::AtRisk] {
        let resolved = p::reconcile_alerts(
            &delivered,
            &[],
            health,
            POLICY_UID,
            now() + Duration::hours(3),
        );
        assert_eq!(resolved[0].state, "Resolved", "{health}");
        assert_eq!(resolved[0].transition, Some(2), "{health}");
    }
    assert!(p::resolves_alerts(p::Health::Healthy));
    assert!(p::resolves_alerts(p::Health::AtRisk));
    for health in [p::Health::Stale, p::Health::Unprotected, p::Health::Unknown] {
        assert!(!p::resolves_alerts(health), "{health}");
    }
}

/// `Unprotected`, the worst value the enum has, opens an alert.
///
/// MUTANT: narrow the open rule back to `health == Health::Stale`. The first
/// assertion fails with an empty `open_kinds`.
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

    // End to end: the controller opens the alert in the ledger.
    let no_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"objectives": {"requireCatalogAvailability": false}}),
        );
        value
    };
    let mut routes = read_routes(Vec::new(), json!({}));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, body_log) = drive(&policy_with(no_catalog, json!({})), routes);
    assert_eq!(outcome.health, p::Health::Unprotected);
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

/// A field this pass computed as `None` must DISAPPEAR from the object.
///
/// An RFC 7386 merge patch removes a key only for an explicit `null`, and every
/// status field is `skip_serializing_if = "Option::is_none"`. Without the nulls
/// the API and the console would keep serving a `lastAvailablePoint` naming a
/// point the controller had just decided was not available, with `health:
/// Unknown` beside it.
///
/// MUTANT: build the body with `json!({"status": status})`. The merged result
/// below keeps both fields and the two `is_null` assertions fail.
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

    // No runs at all, so this pass computes NO point and NO lastAttempt, and
    // opens a Staleness alert, which is why the create routes are here.
    let mut routes = read_routes(Vec::new(), json!({}));
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

/// A missed slot from last January must not pin a policy to `AtRisk`.
///
/// `BackupSchedule.status.lastMissedSlot` is an audit trail that is never
/// cleared. Read as a live signal it would make `Protected=False` permanent
/// on a schedule that has fired correctly every night since.
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

    // An explicit zero beats any inference.
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

/// A point the catalog calls `UntrustedSigner` is not protection, and the
/// archive alert opens.
///
/// A `TrustPolicy` retires or revokes a signer; the catalog's re-trust marks
/// the entries, while the `Backup` CR's own `status.evidence` still says
/// `Valid` until an asynchronous pass rewrites it. Selecting on the
/// availability axis alone would report `Healthy` with an empty alert set.
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

/// The boundary of the objective comparison is inclusive.
///
/// MUTANT: `age <= objective` becomes `age <`. The first assertion then fails.
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

/// A catalog axis this build cannot read is "could not answer", never "your
/// backups are gone".
///
/// `#[serde(default)]` on the two axes means a rename in the catalog's
/// `ViewEntry` yields `""` for every entry. Read as "not available" that would
/// put every catalog-backed policy in the cluster into `Unprotected` at once,
/// on a schema change, and page for all of them.
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

// ------------------------------------------------- the empty capture

/// A verified `Backup` whose `Complete` reason is `EmptyCapture`: Succeeded,
/// exit 0, `Valid`, a capture time and no `windowCovered`, exactly as the
/// backup controller leaves an empty capture.
fn empty_capture_backup(name: &str, hours_ago: i64) -> Value {
    let mut b = backup(
        name,
        hours_ago,
        json!({"status": {
            "records": 0,
            "conditions": [{"type": "Complete", "status": "True", "reason": "EmptyCapture",
                            "message": "an EMPTY CAPTURE"}]
        }}),
    );
    b["status"]
        .as_object_mut()
        .expect("a status")
        .remove("windowCovered");
    b
}

fn no_catalog_policy() -> ProtectionPolicy {
    let mut value = spec_value();
    merge(
        &mut value,
        &json!({"objectives": {"requireCatalogAvailability": false}}),
    );
    policy_with(value, json!({}))
}

/// An empty capture holds no record, so a policy without a `catalogRef` never
/// names it as its newest recovery point: beside an older real point, the
/// real point is `lastAvailablePoint`; alone, there is no available point
/// (`NoAvailablePoint`, `Unprotected`), never Protected. Control: the same
/// object as an ordinary Backup is the newest point.
#[test]
fn an_empty_capture_is_never_a_policys_recovery_point() {
    // Beside a real point 10 hours old (inside the 26-hour objective).
    let mut routes = read_routes(
        vec![
            empty_capture_backup("b-empty", 2),
            backup("b-real", 10, json!({})),
        ],
        json!({}),
    );
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&no_catalog_policy(), routes);
    let status = last_status_patch(&bodies);
    assert_eq!(
        status["status"]["lastAvailablePoint"]["pointId"].as_str(),
        Some(point_id("b-real").as_str()),
        "{status}"
    );
    assert_eq!(outcome.health, p::Health::Healthy);

    // Alone: no available point at all.
    let mut routes = read_routes(vec![empty_capture_backup("b-empty", 2)], json!({}));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&no_catalog_policy(), routes);
    let status = last_status_patch(&bodies);
    // Freshness Stale for `NoAvailablePoint`, which the health table renders
    // `Unprotected` (no point at all), and `Protected=False`.
    assert_eq!(outcome.health, p::Health::Unprotected, "{status}");
    assert!(status["status"]["lastAvailablePoint"].is_null(), "{status}");
    assert_eq!(
        condition(&status, "Protected")["reason"].as_str(),
        Some("NoAvailablePoint"),
        "{status}"
    );
    assert_ne!(
        condition(&status, "Protected")["status"].as_str(),
        Some("True")
    );

    // CONTROL: the same Backup with an ordinary `Complete` reason is the
    // policy's newest point.
    let mut ordinary = empty_capture_backup("b-empty", 2);
    ordinary["status"]["conditions"][0]["reason"] = json!("Ok");
    let mut routes = read_routes(vec![ordinary, backup("b-real", 10, json!({}))], json!({}));
    routes.push(patch(STATUS_PATH));
    let (outcome, _, bodies) = drive(&no_catalog_policy(), routes);
    assert_eq!(
        last_status_patch(&bodies)["status"]["lastAvailablePoint"]["pointId"].as_str(),
        Some(point_id("b-empty").as_str())
    );
    assert_eq!(outcome.health, p::Health::Healthy);
}

// FX-38: a cut or partly unreadable read is never a verdict

/// **(4) A `Backup` listing cut at its bound is `Unknown`, never a verdict.**
/// The API server lists by name, so past 1 000 `Backup`s the runs not read
/// may be the newest: the point, the failure streak and the last attempt are
/// all unknown. Every page here answers one fresh run and a `continue` token,
/// so the walk stops at its five pages with more to read.
/// CONTROL: the same run without the token is `Healthy`.
/// MUTANTS: `list_backups` answering complete, or `evaluate` ignoring
/// `backups_complete`, make the cut listing `Healthy`.
#[test]
fn fx38_a_backup_listing_cut_at_its_bound_is_unknown() {
    let mut spec = spec_value();
    merge(
        &mut spec,
        &json!({"objectives": {"requireCatalogAvailability": false}}),
    );
    let listing = |cut: bool| {
        let mut list: Value =
            serde_json::from_str(&backup_list(vec![backup("b-1", 2, json!({}))])).unwrap();
        if cut {
            list["metadata"]["continue"] = json!("more");
        }
        let mut routes = vec![
            ok(SOURCE_PATH, source_object()),
            ok(DESTINATION_PATH, destination_object()),
            ok(SCHEDULE_PATH, schedule_object(json!({}))),
            ok(BACKUPS_PATH, list.to_string()),
            ok(RESTORES_PATH, restore_list(Vec::new())),
        ];
        routes.push(patch(STATUS_PATH));
        routes
    };
    let (outcome, recorder, bodies) = drive(&policy_with(spec.clone(), json!({})), listing(true));
    assert_eq!(outcome.health, p::Health::Unknown);
    assert_eq!(outcome.reason, p::FreshnessReason::BackupHistoryTooLarge);
    let lists = requests(&recorder)
        .iter()
        .filter(|(m, u)| m == "GET" && u.split('?').next().is_some_and(|p| p.ends_with("/backups")))
        .count();
    assert_eq!(
        lists,
        pp::MAX_BACKUP_PAGES,
        "the bound still bounds the read"
    );
    let status = last_status_patch(&bodies);
    assert_eq!(status["status"]["health"].as_str(), Some("Unknown"));
    let protected = condition(&status, "Protected");
    assert_eq!(protected["status"].as_str(), Some("Unknown"));
    assert_eq!(protected["reason"].as_str(), Some("BackupHistoryTooLarge"));
    assert!(
        protected["message"]
            .as_str()
            .is_some_and(|m| m.contains("more than 1000 Backup objects")),
        "{protected}"
    );

    // CONTROL: the whole listing is a verdict.
    let (outcome, _, _) = drive(&policy_with(spec, json!({})), listing(false));
    assert_eq!(outcome.health, p::Health::Healthy);
}

/// **(5) A catalog line this build cannot read makes the view unreadable.**
/// The line may be the very point the verdict is about; skipping it would
/// answer "the point is gone". The page holds the fresh run's row and a
/// garbled one: `Unknown`, `CatalogUnreadable`.
/// CONTROL: the page without the garbled line answers `Healthy`.
#[test]
fn fx38_an_unreadable_catalog_line_is_an_unreadable_view() {
    let with_catalog = {
        let mut value = spec_value();
        merge(
            &mut value,
            &json!({"protects": {"catalogRef": {"name": CATALOG}}}),
        );
        value
    };
    let run = |garbled: bool| {
        let entry = catalog_entry("b-1", 2, "Available", "Verified");
        let body = if garbled {
            format!("{entry}\n{{\"pointId\": 7\n")
        } else {
            format!("{entry}\n")
        };
        let mut routes = read_routes(vec![backup("b-1", 2, json!({}))], json!({}));
        let mut catalog = catalog_routes(entry);
        catalog[1] = ok(
            PAGE_PATH,
            json!({
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": PAGE_CM, "namespace": NS},
                "data": {"entries.jsonl": body}
            })
            .to_string(),
        );
        routes.extend(catalog);
        routes.push(patch(STATUS_PATH));
        drive(&policy_with(with_catalog.clone(), json!({})), routes)
    };
    let (outcome, _, bodies) = run(true);
    assert_eq!(outcome.health, p::Health::Unknown);
    assert_eq!(outcome.reason, p::FreshnessReason::CatalogUnreadable);
    assert_eq!(
        condition(&last_status_patch(&bodies), "Protected")["status"].as_str(),
        Some("Unknown")
    );
    // CONTROL.
    assert_eq!(run(false).0.health, p::Health::Healthy);
}
