//! P10 — the manual-run pool: "Back up now" (and a manual restore) no longer
//! turns N accepted requests into N simultaneous runner pods.
//!
//! THE DEFECT, MEASURED. On the PoC install one operator's hundred accepted
//! `POST …/backups` became a hundred runner pods at once: docker-desktop hit
//! its 110-pod limit, the node went `NotReady`, MinIO answered `503 SlowDown`
//! (`poc-install.result.md`, 2026-09-24T16:08:53Z). These rows hold the
//! controller half of the fix: at most `runs.maxManualBackupsActivePerNamespace`
//! manual runs of a namespace hold a runner slot, the rest wait `Queued` with
//! NOTHING created, and they start in arrival order as slots free.
//!
//! THE REVIEW ROUND (Tier-A, H1/M1). The first round decided from the watch
//! cache alone, which lags the controller's own writes, so passes deciding in
//! the same instant — a burst, runs released together from a hold, a restart,
//! a `TrustPolicy` event — all saw the same free slot. Every "burst" row below
//! therefore decides EVERY pass over the SAME STALE snapshot (nobody's
//! admission reflected), which is the worst case the watch can produce, and
//! asserts the ceiling holds anyway.
//!
//! EVERY ROW IS PURE OR A ROUTE-TABLE ROW. The pool reads no API (its
//! snapshot is the reflector store the controller already runs), so a row
//! hands `reconcile_backup_pooled` the exact snapshot and reservation registry
//! it is about, and the double panics on any request the row did not route.

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use kube::runtime::watcher;
use serde_json::{json, Value};
use weirkeeper::backup_execution::inputs_config_map;
use weirkeeper::controllers::backup::{
    desired_execution_inputs, execution_status_patch, queued_status_patch, reconcile_backup,
    reconcile_backup_pooled, runner_job, running_status_patch, unobserved_archive,
    with_status_patch,
};
use weirkeeper::crds::backup::Backup;
use weirkeeper::crds::kafka_cluster::KafkaCluster;
use weirkeeper::crds::restore::Restore;
use weirkeeper::job::RunnerImage;
use weirkeeper::run_pool::{
    backup_gated, backup_standing, restore_gated, restore_standing, store_snapshot, Decision, Pool,
    PoolKind, Reservations, Standing, RESERVATION_TTL_SECONDS,
};
use weirkeeper::testing::{mock_client_recording_bodies, Route, SeenBody};
use weirkeeper::verification::unverified_evidence;

const NS: &str = "lw-p10";
const OTHER_NS: &str = "lw-p10-other";
const CLUSTER_UID: &str = "7a2b9c1d-0000-4000-8000-0000000000c1";
const SCHEDULE_UID: &str = "9c4d2e6f-0000-4000-8000-0000000000d1";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn utc(h: u32, m: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 24, h, m, s)
        .single()
        .expect("the instant exists")
}

fn now() -> DateTime<Utc> {
    utc(16, 30, 0)
}

/// A MANUAL `Backup` as the API creates one ("Back up now"), in `namespace`,
/// created at `created` by the API server's clock.
fn manual_in(namespace: &str, name: &str, uid: &str, created: DateTime<Utc>) -> Backup {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": uid,
            "generation": 1,
            "resourceVersion": "100",
            "creationTimestamp": created.to_rfc3339(),
        },
        "spec": {
            "sourceRef": {"name": "prod"},
            "topics": ["orders", "payments"],
            "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
            "triggeredBy": "manual",
            "trigger": {"kind": "Manual", "attempt": 0},
            "deadlineSeconds": 3600
        }
    }))
    .expect("the fixture is a Backup")
}

fn manual(name: &str, uid: &str, created: DateTime<Utc>) -> Backup {
    manual_in(NS, name, uid, created)
}

/// `n` runs created within the same second — the PoC shape — whose NAMES
/// DESCEND while their UIDs (the server's tie-break) ascend, so a queue
/// ordered by name and one ordered by arrival disagree on every pair.
fn burst(n: usize) -> Vec<Backup> {
    (0..n)
        .map(|i| {
            manual(
                &format!("logweir-manual-z{:02}", 99 - i),
                &format!("00000000-0000-4000-8000-0000000000{i:02}"),
                utc(16, 0, 0),
            )
        })
        .collect()
}

/// A scheduled run of `nightly`, the shape `backup_schedule` creates.
fn scheduled(created: DateTime<Utc>) -> Backup {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "Backup",
        "metadata": {
            "name": "logweir-backup-nightly-20260924-020000",
            "namespace": NS,
            "uid": "5c4d2e6f-0000-4000-8000-00000000005c",
            "generation": 1,
            "resourceVersion": "100",
            "creationTimestamp": created.to_rfc3339(),
        },
        "spec": {
            "sourceRef": {"name": "prod"},
            "topics": ["orders", "payments"],
            "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}},
            "triggeredBy": "schedule",
            "trigger": {"kind": "Scheduled", "attempt": 0},
            "scheduleRef": {"name": "nightly", "uid": SCHEDULE_UID},
            "slot": "20260924-020000",
            "deadlineSeconds": 3600
        }
    }))
    .expect("the fixture is a Backup")
}

fn with_status(b: &Backup, status: Value) -> Backup {
    with_status_patch(b, &json!({ "status": status }))
}

fn running(b: &Backup) -> Backup {
    with_status(
        b,
        json!({"phase": "Running", "jobRef": {"name": b.metadata.name}}),
    )
}

fn succeeded(b: &Backup) -> Backup {
    with_status(b, json!({"phase": "Succeeded", "exitCode": 0}))
}

/// What a destination hold writes on a `Backup` (D2 §3.6): `phase: Pending`
/// and `Admitted=False` naming the destination refusal.
fn held_on_destination(b: &Backup) -> Backup {
    with_status(
        b,
        json!({
            "phase": "Pending",
            "conditions": [{"type": "Admitted", "status": "False",
                            "reason": "DestinationNotValid",
                            "lastTransitionTime": "2026-09-24T16:00:01Z"}]
        }),
    )
}

/// The durable admission record alone — what a run carries between the gate's
/// write and its freeze.
fn admitted_record(b: &Backup) -> Backup {
    with_status(
        b,
        json!({"conditions": [{"type": "Admitted", "status": "True", "reason": "Admitted",
                               "lastTransitionTime": "2026-09-24T16:00:01Z"}]}),
    )
}

fn arcs(v: &[Backup]) -> Vec<Arc<Backup>> {
    v.iter().cloned().map(Arc::new).collect()
}

fn cluster() -> KafkaCluster {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "KafkaCluster",
        "metadata": {"name": "prod", "namespace": NS, "uid": CLUSTER_UID},
        "spec": {
            "bootstrapServers": ["broker-0.prod:9093"],
            "auth": {"mode": "scramSha512", "username": "logweir", "secretRef": {"name": "prod-sasl"}, "tls": true},
            "role": "source"
        },
        "status": {"reachable": true, "clusterId": "MkU3OEVBNTcwNTJENDM2Qk"}
    }))
    .expect("the fixture is a KafkaCluster")
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn not_found(kind: &str, name: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure","message":"{kind} \"{name}\" not found","reason":"NotFound","code":404}}"#
    )
}

/// Everything a CREATE pass of `b` may ask for, write routes included, so a
/// zero-`POST` count is a statement about the reconciler and not about the
/// table.
fn create_routes(b: &Backup) -> Vec<Route> {
    let name = b.metadata.name.clone().expect("named");
    let frozen = desired_execution_inputs(b, &cluster()).expect("the fixture resolves");
    let plan = inputs_config_map(b, &frozen).expect("the plan renders");
    let job = runner_job(b, &cluster(), &frozen, &RunnerImage::default()).expect("the Job renders");
    let object = serde_json::to_string(b).expect("a Backup serialises");
    vec![
        Route {
            method: "GET",
            path_suffix: leak(format!("/jobs/{name}")),
            status: 404,
            body: not_found("jobs.batch", &name),
        },
        Route {
            method: "GET",
            path_suffix: "/kafkaclusters/prod",
            status: 200,
            body: serde_json::to_string(&cluster()).expect("serialises"),
        },
        Route {
            method: "GET",
            path_suffix: "/backupschedules/nightly",
            status: 200,
            body: json!({
                "apiVersion": "logweir.dev/v1alpha1", "kind": "BackupSchedule",
                "metadata": {"name": "nightly", "namespace": NS, "uid": SCHEDULE_UID, "generation": 1},
                "spec": {"schedule": "0 2 * * *", "sourceRef": {"name": "prod"},
                         "topics": ["orders", "payments"],
                         "archive": {"url": "s3://kafka-backups/logweir", "secretRef": {"name": "logweir-s3"}}}
            })
            .to_string(),
        },
        Route {
            method: "GET",
            path_suffix: leak(format!("/configmaps/{name}-plan")),
            status: 200,
            body: serde_json::to_string(&plan).expect("serialises"),
        },
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 201,
            body: serde_json::to_string(&plan).expect("serialises"),
        },
        Route {
            method: "POST",
            path_suffix: "/jobs",
            status: 201,
            body: serde_json::to_string(&job).expect("serialises"),
        },
        Route {
            method: "PATCH",
            path_suffix: leak(format!("/backups/{name}/status")),
            status: 200,
            body: object,
        },
    ]
}

fn path(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

fn posts(seen: &[SeenBody], suffix: &str) -> usize {
    seen.iter()
        .filter(|r| r.method == "POST" && path(&r.uri).ends_with(suffix))
        .count()
}

fn patches(seen: &[SeenBody]) -> Vec<Value> {
    seen.iter()
        .filter(|r| r.method == "PATCH")
        .map(|r| serde_json::from_str::<Value>(&r.body).expect("a patch is JSON")["status"].clone())
        .collect()
}

fn condition<'a>(status: &'a Value, r#type: &str) -> Option<&'a Value> {
    status["conditions"]
        .as_array()?
        .iter()
        .find(|c| c["type"] == r#type)
}

/// One pooled pass of `b` over `peers`, with the ceiling `limit` and the
/// reservation registry `reservations`.
async fn pooled_pass(
    b: &Backup,
    peers: Option<Vec<Backup>>,
    limit: Option<u32>,
    reservations: &Reservations,
) -> Vec<SeenBody> {
    let (client, _seen, bodies) = mock_client_recording_bodies(create_routes(b));
    let snapshot: Option<Vec<Arc<Backup>>> = peers.map(|p| arcs(&p));
    let source = move || snapshot.clone();
    let pool = Pool {
        peers: &source,
        limit,
        reservations,
    };
    reconcile_backup_pooled(
        b,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        now(),
        &RunnerImage::default(),
        None,
        &pool,
    )
    .await
    .expect("the pass reconciles");
    let seen = bodies.lock().expect("readable").clone();
    seen
}

/// Decide `candidate` over `snapshot` with registry `r`, ceiling `limit`.
fn decide(r: &Reservations, candidate: &Backup, snapshot: &[Backup], limit: u32) -> Decision {
    let snapshot = arcs(snapshot);
    r.decide(
        PoolKind::Backup,
        candidate,
        &move || Some(snapshot.clone()),
        limit,
        backup_standing,
        now(),
    )
}

// ---------------------------------------------------------------------------
// The rule, pure
// ---------------------------------------------------------------------------

/// **A burst admits exactly the ceiling, in arrival order, whatever order its
/// passes run in and however stale the snapshot is** (review M1, probe P1).
///
/// Seven runs created within one second, every pass deciding over the SAME
/// snapshot in which nobody's admission is reflected, ceiling four, in three
/// pass orders. Each order admits exactly four, and they are the four that
/// ARRIVED first (creation second, then UID) — the burst's names descend, so
/// a queue ordered by name would pick the other end.
///
/// KILLS: deciding without the reservation (all seven admitted in ascending
/// order); ordering by name (the wrong four).
#[test]
fn a_burst_admits_exactly_the_ceiling_in_arrival_order_whatever_the_pass_order() {
    let runs = burst(7);
    let arrival_first_four: Vec<String> = runs[..4]
        .iter()
        .map(|b| b.metadata.name.clone().unwrap())
        .collect();
    for order in [
        vec![0, 1, 2, 3, 4, 5, 6],
        vec![6, 5, 4, 3, 2, 1, 0],
        vec![3, 6, 0, 5, 1, 4, 2],
    ] {
        let r = Reservations::new();
        let mut admitted: Vec<String> = order
            .iter()
            .filter(|&&i| decide(&r, &runs[i], &runs, 4).is_admitted())
            .map(|&i| runs[i].metadata.name.clone().unwrap())
            .collect();
        admitted.sort();
        let mut want = arrival_first_four.clone();
        want.sort();
        assert_eq!(admitted, want, "pass order {order:?}");
        assert_eq!(r.len(), 4, "exactly the admitted runs are reserved");
    }
}

/// **Arrivals one at a time into growing snapshots never exceed the ceiling**
/// — probe P1's exact shape: six same-second arrivals in descending name
/// order, each pass seeing only the arrivals so far and none of the earlier
/// admissions reflected, ceiling two. The first round admitted all six.
#[test]
fn arrivals_into_growing_snapshots_never_exceed_the_ceiling() {
    let runs = burst(6);
    let r = Reservations::new();
    let admitted = (0..runs.len())
        .filter(|&i| decide(&r, &runs[i], &runs[..=i], 2).is_admitted())
        .count();
    assert_eq!(admitted, 2);
}

/// **An older run released from a hold does not take a slot a younger run was
/// just admitted to** — probe P3: ceiling one; the younger run is admitted
/// (reserved, its admission not reflected: the snapshot still shows it
/// waiting); the older one rejoins from `Pending` and is QUEUED.
#[test]
fn an_older_run_rejoining_from_a_hold_counts_the_younger_admission() {
    let older = manual(
        "logweir-manual-old",
        "10000000-0000-4000-8000-000000000001",
        utc(15, 0, 0),
    );
    let younger = manual(
        "logweir-manual-new",
        "20000000-0000-4000-8000-000000000002",
        utc(16, 0, 0),
    );
    let r = Reservations::new();
    // While the older one is held, the younger one is admitted.
    let snapshot = vec![held_on_destination(&older), younger.clone()];
    assert!(decide(&r, &younger, &snapshot, 1).is_admitted());
    // The hold clears; the snapshot has not caught up with anything.
    assert_eq!(
        decide(&r, &older, &snapshot, 1),
        Decision::Queued {
            active: 1,
            ahead: 0,
            limit: 1
        }
    );
}

/// **Runs released together from a destination hold are admitted up to the
/// ceiling and no further** (review H1, the `Backup` variant), pure half: three
/// runs held `Pending`, every pass over the same stale snapshot, ceiling two.
#[test]
fn runs_released_together_from_a_hold_are_admitted_up_to_the_ceiling() {
    let runs: Vec<Backup> = burst(3).iter().map(held_on_destination).collect();
    for order in [[0, 1, 2], [2, 1, 0], [1, 2, 0]] {
        let r = Reservations::new();
        let admitted = order
            .iter()
            .filter(|&&i| decide(&r, &runs[i], &runs, 2).is_admitted())
            .count();
        assert_eq!(admitted, 2, "order {order:?}");
    }
}

/// **After a restart an admitted run is counted from its OWN record** (review
/// H1). A fresh process has no reservations; a run whose admission record
/// (`Admitted=True`) was written before the crash — but whose Job was not — is
/// `Occupying` by its status, so the restarted controller does not admit a
/// run into its slot.
///
/// KILLS: dropping `Admitted=True` from `backup_standing` (the restarted
/// controller admits a fifth run).
#[test]
fn after_a_restart_an_admitted_run_is_counted_from_its_own_record() {
    let runs = burst(5);
    let mut snapshot: Vec<Backup> = runs[..4].iter().map(admitted_record).collect();
    snapshot.push(runs[4].clone());
    let fresh = Reservations::new();
    assert_eq!(
        decide(&fresh, &runs[4], &snapshot, 4),
        Decision::Queued {
            active: 4,
            ahead: 0,
            limit: 4
        }
    );
}

/// **A reservation ends when the watch catches up, when its run finishes or
/// is deleted, or when it is too old — and only in its own kind and
/// namespace.**
///
/// KILLS: a reservation that is never released (a slot leaks); one that is
/// released by another namespace's snapshot, where its run is simply absent.
#[test]
fn a_reservation_ends_when_the_watch_catches_up_or_it_expires() {
    let runs = burst(2);
    let other = manual_in(
        OTHER_NS,
        "logweir-manual-else",
        "e0000000-0000-4000-8000-000000000001",
        utc(16, 0, 0),
    );
    let r = Reservations::new();
    assert!(decide(&r, &runs[0], &runs, 4).is_admitted());
    assert!(r
        .decide(
            PoolKind::Backup,
            &other,
            &|| Some(arcs(std::slice::from_ref(&other))),
            4,
            backup_standing,
            now()
        )
        .is_admitted());
    assert_eq!(r.len(), 2);
    // ANOTHER NAMESPACE'S SNAPSHOT DOES NOT RELEASE THIS ONE: `runs[0]` is not
    // in it, and that is "not in this store", not "deleted".
    let _ = r.decide(
        PoolKind::Backup,
        &other,
        &|| Some(arcs(std::slice::from_ref(&other))),
        4,
        backup_standing,
        now(),
    );
    assert_eq!(
        r.len(),
        2,
        "a snapshot of another namespace releases nothing there"
    );
    // THE WATCH CATCHES UP: `runs[0]`'s admission record is visible.
    let caught_up = vec![admitted_record(&runs[0]), runs[1].clone()];
    let _ = decide(&r, &runs[1], &caught_up, 4);
    assert!(
        r.len() == 2,
        "runs[1] is now reserved and runs[0]'s reservation is released"
    );
    let finished = vec![succeeded(&runs[0]), succeeded(&runs[1])];
    let probe = manual(
        "logweir-manual-probe",
        "f0000000-0000-4000-8000-000000000009",
        utc(16, 1, 0),
    );
    let mut with_probe = finished.clone();
    with_probe.push(probe.clone());
    assert!(decide(&r, &probe, &with_probe, 4).is_admitted());
    assert_eq!(
        r.len(),
        2,
        "finished runs release theirs; the probe and `other` remain"
    );
    // TOO OLD: a pass a TTL later releases everything it can see.
    let later = now() + chrono::Duration::seconds(RESERVATION_TTL_SECONDS + 1);
    let _ = r.decide(
        PoolKind::Backup,
        &probe,
        &|| Some(arcs(&with_probe)),
        4,
        backup_standing,
        later,
    );
    assert_eq!(
        r.len(),
        1,
        "only the fresh re-reservation of the probe remains"
    );
}

/// A manual `Restore` held on its approval (`phase: Pending`) in this
/// namespace — the shape three console restores are in before their
/// approvals verify.
fn held_restore(name: &str, uid: &str) -> Restore {
    serde_json::from_value(json!({
        "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
        "metadata": {"name": name, "namespace": NS, "uid": uid,
                     "creationTimestamp": utc(16, 0, 0).to_rfc3339()},
        "spec": {
            "planBytes": "x", "approvalRef": {"name": "a1"},
            "sourceArchive": {"url": "s3://kafka-backups/logweir"},
            "backupSetRef": "set", "pointInTime": "2026-09-07T14:05:00Z",
            "target": {"clusterRef": {"name": "scratch"}, "mode": "scratch", "topicNaming": {"prefix": "drill-"}},
            "deadlineSeconds": 1800
        },
        "status": {"phase": "Pending"}
    }))
    .expect("the fixture is a Restore")
}

/// **The ONE registry is scoped by kind: a `Backup` pass never releases a
/// `Restore` reservation** (re-check B1, mutant K1).
///
/// Production shares `Reservations::global()` between the `Backup` and
/// `Restore` reconcilers. A `Backup` pass prunes against a snapshot of
/// `Backup`s, in which no `Restore` appears — so without the kind check it
/// would release every restore reservation in the namespace as "gone", and
/// H1's restore burst would be back: three restores released from their
/// approval hold together would make three Jobs at ceiling two.
///
/// KILLS: dropping `r.kind != kind` from the prune scope.
#[test]
fn a_backup_pass_keeps_a_restore_reservation_in_the_shared_registry() {
    let r = Reservations::new();
    let restores: Vec<Restore> = (1..=3)
        .map(|i| held_restore(&format!("rst-{i}"), &format!("u-rst-{i}")))
        .collect();
    let snapshot = || Some(restores.iter().cloned().map(Arc::new).collect::<Vec<_>>());
    let decide_restore = |i: usize| {
        r.decide(
            PoolKind::Restore,
            &restores[i],
            &snapshot,
            2,
            restore_standing,
            now(),
        )
    };
    assert!(decide_restore(0).is_admitted());
    // A BACKUP PASS IN THE SAME NAMESPACE, over a snapshot of Backups.
    let backup = manual(
        "logweir-manual-b",
        "b0000000-0000-4000-8000-00000000000b",
        utc(16, 0, 0),
    );
    assert!(r
        .decide(
            PoolKind::Backup,
            &backup,
            &|| Some(arcs(std::slice::from_ref(&backup))),
            4,
            backup_standing,
            now()
        )
        .is_admitted());
    assert!(decide_restore(1).is_admitted());
    assert_eq!(
        decide_restore(2),
        Decision::Queued {
            active: 2,
            ahead: 0,
            limit: 2
        },
        "the third restore is queued at ceiling two: the Backup pass released nothing of theirs"
    );
}

/// **A reserved run that is later QUEUED gives its reservation back** (re-check
/// B2, mutant K3) — so the older run it now waits behind is admitted on its
/// next pass instead of both stalling until the TTL.
///
/// Ceiling one. X is admitted while the older run O is held (X's admission
/// write then fails, so its status still says waiting). O rejoins and waits
/// behind X's reservation; X's retry now queues behind O — and must release,
/// or O's next pass would still count X and the two would block each other.
///
/// KILLS: deleting the release in the queued branch.
#[test]
fn a_reserved_run_that_is_queued_releases_its_reservation() {
    let r = Reservations::new();
    let older = manual(
        "logweir-manual-o",
        "a0000000-0000-4000-8000-00000000000a",
        utc(15, 0, 0),
    );
    let x = manual(
        "logweir-manual-x",
        "b0000000-0000-4000-8000-00000000000b",
        utc(16, 0, 0),
    );
    assert!(decide(&r, &x, &[held_on_destination(&older), x.clone()], 1).is_admitted());
    let both = [older.clone(), x.clone()];
    assert!(
        !decide(&r, &older, &both, 1).is_admitted(),
        "O waits behind X's reservation"
    );
    assert!(!decide(&r, &x, &both, 1).is_admitted(), "X queues behind O");
    assert!(
        decide(&r, &older, &both, 1).is_admitted(),
        "the older run is admitted once X has queued and released"
    );
    assert_eq!(r.len(), 1, "only O is reserved");
}

/// **A run that is frozen but whose phase has not been written yet still
/// holds its slot.** `status.execution` is written BEFORE the Job is created,
/// so a pass that stops between the two leaves exactly this object — and it
/// must count, or the next candidate takes a slot that is not free.
#[test]
fn a_frozen_run_holds_its_slot_before_its_phase_says_so() {
    let candidate = manual(
        "logweir-manual-cand",
        "c0000000-0000-4000-8000-000000000001",
        utc(16, 0, 0),
    );
    let mut peers: Vec<Backup> = (0..3)
        .map(|i| {
            running(&manual(
                &format!("logweir-manual-run{i}"),
                &format!("r0000000-0000-4000-8000-00000000000{i}"),
                utc(15, 0, 0),
            ))
        })
        .collect();
    let frozen = manual(
        "logweir-manual-frozen",
        "f0000000-0000-4000-8000-000000000001",
        utc(16, 5, 0),
    );
    let inputs = desired_execution_inputs(&frozen, &cluster()).expect("resolves");
    peers.push(with_status_patch(&frozen, &execution_status_patch(&inputs)));
    peers.push(candidate.clone());
    assert_eq!(
        decide(&Reservations::new(), &candidate, &peers, 4),
        Decision::Queued {
            active: 4,
            ahead: 0,
            limit: 4
        }
    );
}

/// **Where every status leaves a run**, for both kinds, in one table.
///
/// KILLS: counting a `Pending` hold as a slot; NOT counting a phase this build
/// does not know; counting a scheduled run, a rehearsal's restore or a
/// finished run — `Refused` included (review L5).
#[test]
fn what_holds_a_slot_what_waits_and_what_is_outside() {
    let b = manual(
        "logweir-manual-t",
        "t0000000-0000-4000-8000-000000000001",
        utc(16, 0, 0),
    );
    let rows: Vec<(&str, Backup, Standing)> = vec![
        ("no status", b.clone(), Standing::Waiting),
        (
            "queued",
            with_status(&b, json!({"phase": "Queued"})),
            Standing::Waiting,
        ),
        (
            "destination hold",
            held_on_destination(&b),
            Standing::Outside,
        ),
        ("admission record", admitted_record(&b), Standing::Occupying),
        ("running", running(&b), Standing::Occupying),
        (
            "resolving",
            with_status(&b, json!({"phase": "Resolving"})),
            Standing::Occupying,
        ),
        (
            "a phase nobody knows yet",
            with_status(&b, json!({"phase": "Draining"})),
            Standing::Occupying,
        ),
        (
            "job recorded, no phase",
            with_status(&b, json!({"jobRef": {"name": "x"}})),
            Standing::Occupying,
        ),
        ("succeeded", succeeded(&b), Standing::Finished),
        (
            "failed",
            with_status(&b, json!({"phase": "Failed"})),
            Standing::Finished,
        ),
        (
            "refused",
            with_status(&b, json!({"phase": "Refused"})),
            Standing::Finished,
        ),
        ("scheduled", scheduled(utc(2, 0, 0)), Standing::Outside),
        (
            "scheduled, running",
            running(&scheduled(utc(2, 0, 0))),
            Standing::Outside,
        ),
    ];
    for (label, object, want) in rows {
        assert_eq!(backup_standing(&object), want, "Backup: {label}");
    }

    let restore = |authorization: Option<Value>, status: Option<Value>| -> Restore {
        let mut v = json!({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
            "metadata": {"name": "r1", "namespace": NS, "uid": "u-r1"},
            "spec": {
                "planBytes": "x", "approvalRef": {"name": "a1"},
                "sourceArchive": {"url": "s3://kafka-backups/logweir"},
                "backupSetRef": "set", "pointInTime": "2026-09-07T14:05:00Z",
                "target": {"clusterRef": {"name": "scratch"}, "mode": "scratch", "topicNaming": {"prefix": "drill-"}},
                "deadlineSeconds": 1800
            }
        });
        if let Some(a) = authorization {
            v["spec"]["authorization"] = a;
        }
        if let Some(s) = status {
            v["status"] = s;
        }
        serde_json::from_value(v).expect("the fixture is a Restore")
    };
    let standing = Some(
        json!({"kind": "Standing", "approvalRef": {"name": "standing-1"}, "rehearsalScheduleRef": {"name": "weekly"}}),
    );
    let admitted = json!({"phase": "Pending", "conditions": [{"type": "Admitted", "status": "True", "reason": "Admitted"}]});
    let restore_rows: Vec<(&str, Restore, Standing, bool)> = vec![
        ("fresh", restore(None, None), Standing::Waiting, true),
        (
            "queued",
            restore(None, Some(json!({"phase": "Queued"}))),
            Standing::Waiting,
            true,
        ),
        (
            "approval hold",
            restore(None, Some(json!({"phase": "Pending"}))),
            Standing::Outside,
            true,
        ),
        (
            "admission record",
            restore(None, Some(admitted)),
            Standing::Occupying,
            false,
        ),
        (
            "running",
            restore(
                None,
                Some(json!({"phase": "Running", "jobRef": {"name": "r1"}})),
            ),
            Standing::Occupying,
            false,
        ),
        (
            "unknown phase",
            restore(None, Some(json!({"phase": "Draining"}))),
            Standing::Occupying,
            false,
        ),
        (
            "succeeded",
            restore(None, Some(json!({"phase": "Succeeded"}))),
            Standing::Finished,
            true,
        ),
        (
            "refused",
            restore(None, Some(json!({"phase": "Refused"}))),
            Standing::Finished,
            true,
        ),
        (
            "a rehearsal's restore",
            restore(standing.clone(), None),
            Standing::Outside,
            false,
        ),
        (
            "a rehearsal's restore, queued-looking",
            restore(standing, Some(json!({"phase": "Queued"}))),
            Standing::Outside,
            false,
        ),
    ];
    for (label, object, want, gated) in restore_rows {
        assert_eq!(restore_standing(&object), want, "Restore: {label}");
        assert_eq!(restore_gated(&object), gated, "Restore gate: {label}");
    }
}

/// **Which passes reach the gate at all.**
///
/// KILLS: gating a scheduled run (it would queue behind a browser click and
/// miss its slot); re-queueing a FROZEN or ADMITTED run (it holds its slot and
/// must go on to its Job); gating a `Resolving` run whose discovery Job
/// already exists.
#[test]
fn only_an_unfrozen_manual_run_that_holds_no_slot_is_gated() {
    let b = manual(
        "logweir-manual-g",
        "g0000000-0000-4000-8000-000000000001",
        utc(16, 0, 0),
    );
    let inputs = desired_execution_inputs(&b, &cluster()).expect("resolves");
    let frozen = with_status_patch(&b, &execution_status_patch(&inputs));
    let frozen_looking_queued = with_status_patch(&frozen, &json!({"status": {"phase": "Queued"}}));
    assert!(backup_gated(&b), "a fresh manual run is gated");
    assert!(
        backup_gated(&with_status(&b, json!({"phase": "Queued"}))),
        "a queued one too"
    );
    assert!(
        backup_gated(&held_on_destination(&b)),
        "and one released from a hold"
    );
    assert!(
        !backup_gated(&admitted_record(&b)),
        "an admitted one is not re-gated"
    );
    assert!(
        !backup_gated(&scheduled(utc(2, 0, 0))),
        "a scheduled run never is"
    );
    assert!(!backup_gated(&frozen), "a frozen run never is");
    assert!(
        !backup_gated(&frozen_looking_queued),
        "whatever its phase says"
    );
    assert!(
        !backup_gated(&with_status(&b, json!({"phase": "Resolving"}))),
        "nor a resolving one"
    );
}

/// **Other namespaces, scheduled runs and finished runs do not spend the
/// pool.**
#[test]
fn other_namespaces_scheduled_and_finished_runs_do_not_count() {
    let candidate = manual(
        "logweir-manual-cand",
        "c1000000-0000-4000-8000-000000000001",
        utc(16, 0, 0),
    );
    let mut peers = vec![candidate.clone()];
    for i in 0..4 {
        peers.push(running(&manual_in(
            OTHER_NS,
            &format!("logweir-manual-else{i}"),
            &format!("e0000000-0000-4000-8000-00000000000{i}"),
            utc(15, 0, 0),
        )));
        peers.push(succeeded(&manual(
            &format!("logweir-manual-done{i}"),
            &format!("d0000000-0000-4000-8000-00000000000{i}"),
            utc(14, 0, 0),
        )));
    }
    let mut nightly = running(&scheduled(utc(2, 0, 0)));
    nightly.metadata.uid = Some("5c-running".into());
    peers.push(nightly);
    assert_eq!(
        decide(&Reservations::new(), &candidate, &peers, 4),
        Decision::Admit {
            active: 0,
            ahead: 0,
            limit: 4
        }
    );
}

/// **The startup guard is real: a store that has not finished its first list
/// is not a snapshot** (review L4, mutant R1).
///
/// kube-runtime swaps a whole list into the store at `InitDone`; before it,
/// the store is empty (or, on a relist, the previous list), and an empty store
/// looks like a namespace with no runs. So `store_snapshot` answers `None`
/// until `InitDone`, and `decide` answers `Unsynced` for `None`.
///
/// KILLS: `store_snapshot` returning the store's state before it is ready.
#[test]
fn a_store_that_has_not_synced_is_not_a_snapshot() {
    let (reader, mut writer) = kube::runtime::reflector::store::<Backup>();
    let runs = burst(2);
    assert!(
        store_snapshot(&reader).is_none(),
        "a new store is not synced"
    );
    writer.apply_watcher_event(&watcher::Event::Init);
    writer.apply_watcher_event(&watcher::Event::InitApply(runs[0].clone()));
    assert!(
        store_snapshot(&reader).is_none(),
        "half a list is not a snapshot: InitDone has not arrived"
    );
    assert_eq!(
        Reservations::new().decide(
            PoolKind::Backup,
            &runs[1],
            &|| store_snapshot(&reader),
            4,
            backup_standing,
            now()
        ),
        Decision::Unsynced
    );
    writer.apply_watcher_event(&watcher::Event::InitApply(runs[1].clone()));
    writer.apply_watcher_event(&watcher::Event::InitDone);
    assert_eq!(
        store_snapshot(&reader).map(|s| s.len()),
        Some(2),
        "after InitDone the whole list is the snapshot"
    );
}

// ---------------------------------------------------------------------------
// The rule, through the reconciler
// ---------------------------------------------------------------------------

/// **N+1 manual Backups make N Jobs and one queued run** (N = 4, the default
/// `runs.maxManualBackupsActivePerNamespace`, read from the installation
/// policy because the row passes no ceiling of its own).
///
/// Five runs created in one second, EVERY pass over the same snapshot (the
/// burst the PoC measured, before any status was written), sharing one
/// reservation registry as the process does. Four passes write the admission
/// record, freeze and create a Job; the fifth writes `phase: Queued`,
/// `status.queue.limit: 4` and `Admitted=False/ConcurrencyLimited` and creates
/// NOTHING — no plan `ConfigMap`, no `status.execution`, no Job, so no
/// execution claim either.
#[tokio::test]
async fn n_plus_one_manual_backups_make_n_jobs_and_one_queued_run() {
    let runs = burst(5);
    let reservations = Reservations::new();
    let mut jobs = 0;
    let mut plans = 0;
    let mut queued = Vec::new();
    for b in &runs {
        let seen = pooled_pass(b, Some(runs.clone()), None, &reservations).await;
        jobs += posts(&seen, "/jobs");
        plans += posts(&seen, "/configmaps");
        if patches(&seen).iter().any(|s| s["phase"] == "Queued") {
            queued.push((b.metadata.name.clone().unwrap(), seen));
        } else {
            let first = seen.iter().find(|r| r.method != "GET").expect("a write");
            assert_eq!(first.method, "PATCH", "the admission record comes first");
            let record = &patches(&seen)[0];
            assert_eq!(
                condition(record, "Admitted").map(|c| (c["status"].clone(), c["reason"].clone())),
                Some((json!("True"), json!("Admitted"))),
                "{record}"
            );
        }
    }
    assert_eq!(jobs, 4, "exactly N runner Jobs are created");
    assert_eq!(plans, 4, "exactly N plans are frozen");
    assert_eq!(queued.len(), 1, "exactly one run is queued");
    let (name, seen) = &queued[0];
    assert_eq!(
        name,
        &runs[4].metadata.name.clone().unwrap(),
        "the last to ARRIVE waits"
    );
    assert_eq!(posts(seen, ""), 0, "a queued pass creates nothing at all");
    let statuses = patches(seen);
    assert_eq!(statuses.len(), 1, "one status write: {statuses:?}");
    let status = &statuses[0];
    assert_eq!(status["phase"], "Queued");
    assert_eq!(status["queue"], json!({"limit": 4}));
    assert!(
        status.get("execution").is_none(),
        "no frozen inputs on a queued run"
    );
    let admitted = condition(status, "Admitted").expect("the Admitted condition");
    assert_eq!(admitted["status"], "False");
    assert_eq!(admitted["reason"], "ConcurrencyLimited");
    assert!(
        admitted["message"]
            .as_str()
            .unwrap()
            .contains("runs.maxManualBackupsActivePerNamespace"),
        "the message names the ceiling's field: {admitted}"
    );
}

/// **H1, the `Backup` variant: three runs released together from a
/// destination hold make exactly TWO Jobs at ceiling two.** Each run's stored
/// status is the hold (`phase: Pending`, `Admitted=False/DestinationNotValid`);
/// the destination step now passes (it is `admit_destination`'s rows'
/// business, and these runs carry the inline archive so it answers
/// `NotRequested`), and all three passes decide over the same snapshot in
/// which all three are still `Pending`. The first round admitted all three.
#[tokio::test]
async fn three_backups_released_from_a_destination_hold_together_make_two_jobs() {
    let runs: Vec<Backup> = burst(3).iter().map(held_on_destination).collect();
    let reservations = Reservations::new();
    let mut jobs = 0;
    let mut queued = 0;
    for b in &runs {
        let seen = pooled_pass(b, Some(runs.clone()), Some(2), &reservations).await;
        jobs += posts(&seen, "/jobs");
        queued += usize::from(patches(&seen).iter().any(|s| s["phase"] == "Queued"));
    }
    assert_eq!((jobs, queued), (2, 1));
}

/// **The queued run starts when a slot frees** — and the admission record is
/// ONE write before anything is created: `status.queue: null` and
/// `Admitted=True`, then the freeze, then the Job, every write preconditioned
/// on where the one before it left the object (the double enforces seam S7).
#[tokio::test]
async fn a_queued_run_starts_when_a_slot_frees() {
    let runs = burst(5);
    let queued = with_status_patch(&runs[4], &queued_status_patch(&runs[4], 4, now()));
    let mut peers: Vec<Backup> = runs[..4].iter().map(running).collect();
    let reservations = Reservations::new();
    // STILL FULL: the fifth waits.
    let full = pooled_pass(
        &queued,
        Some([peers.clone(), vec![queued.clone()]].concat()),
        Some(4),
        &reservations,
    )
    .await;
    assert_eq!(posts(&full, ""), 0, "a full pool creates nothing");
    assert!(
        patches(&full).is_empty(),
        "a run that stays queued is not written again: {:?}",
        patches(&full)
    );
    // ONE FINISHES.
    peers[0] = succeeded(&runs[0]);
    let seen = pooled_pass(
        &queued,
        Some([peers, vec![queued.clone()]].concat()),
        Some(4),
        &reservations,
    )
    .await;
    assert_eq!(posts(&seen, "/jobs"), 1, "the queued run's Job is created");
    let order: Vec<(String, String)> = seen
        .iter()
        .filter(|r| r.method != "GET")
        .map(|r| {
            (
                r.method.clone(),
                path(&r.uri).rsplit('/').next().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        order.first(),
        Some(&("PATCH".to_string(), "status".to_string())),
        "the admission record comes before anything is created: {order:?}"
    );
    let record = &patches(&seen)[0];
    assert_eq!(
        record["queue"],
        Value::Null,
        "the queue block is cleared: {record}"
    );
    let admitted = condition(record, "Admitted").expect("Admitted");
    assert_eq!(
        (admitted["status"].as_str(), admitted["reason"].as_str()),
        (Some("True"), Some("Admitted"))
    );
    let last = patches(&seen).last().cloned().unwrap();
    assert_eq!(last["phase"], "Running", "the pass ends running: {last}");
}

/// **A scheduled run is unaffected by a full pool.**
#[tokio::test]
async fn a_scheduled_run_is_not_queued_by_a_full_manual_pool() {
    let runs = burst(4);
    let nightly = scheduled(utc(16, 1, 0));
    let mut peers: Vec<Backup> = runs.iter().map(running).collect();
    peers.push(nightly.clone());
    let reservations = Reservations::new();
    let seen = pooled_pass(&nightly, Some(peers), Some(4), &reservations).await;
    assert_eq!(
        posts(&seen, "/jobs"),
        1,
        "the scheduled run's Job is created"
    );
    assert!(
        patches(&seen)
            .iter()
            .all(|s| s["phase"] != "Queued" && s.get("queue").is_none()),
        "nothing about a queue is written: {:?}",
        patches(&seen)
    );
    assert!(reservations.is_empty(), "a scheduled run reserves nothing");
}

/// **A frozen manual run whose Job is gone is re-created, never re-queued** —
/// the frozen-inputs contract (D1 §3.3).
#[tokio::test]
async fn a_frozen_manual_run_whose_job_is_gone_is_re_created_not_queued() {
    let runs = burst(5);
    let b = &runs[4];
    let inputs = desired_execution_inputs(b, &cluster()).expect("resolves");
    let recorded = with_status_patch(b, &execution_status_patch(&inputs));
    let frozen = with_status_patch(&recorded, &running_status_patch(&recorded, "x", now()));
    let mut peers: Vec<Backup> = runs[..4].iter().map(running).collect();
    peers.push(frozen.clone());
    let seen = pooled_pass(&frozen, Some(peers), Some(4), &Reservations::new()).await;
    assert_eq!(
        posts(&seen, "/jobs"),
        1,
        "the Job is re-created from the frozen inputs"
    );
    assert_eq!(posts(&seen, "/configmaps"), 0, "and nothing is re-frozen");
    assert!(patches(&seen).iter().all(|s| s["phase"] != "Queued"));
}

/// **Until the watch has synced, the gate creates, writes and reserves
/// nothing.**
#[tokio::test]
async fn an_unsynced_pool_creates_and_writes_nothing() {
    let b = &burst(1)[0];
    let reservations = Reservations::new();
    let seen = pooled_pass(b, None, Some(4), &reservations).await;
    let writes: Vec<_> = seen.iter().filter(|r| r.method != "GET").collect();
    assert!(
        writes.is_empty(),
        "nothing is written or created: {writes:?}"
    );
    assert!(reservations.is_empty());
}

/// **A controller without the pool runs a `Queued` object** — the rollback
/// and mixed-version row.
#[tokio::test]
async fn a_pool_less_pass_runs_a_queued_object() {
    let b = &burst(1)[0];
    let queued = with_status_patch(b, &queued_status_patch(b, 4, now()));
    let (client, _seen, bodies) = mock_client_recording_bodies(create_routes(&queued));
    reconcile_backup(
        &queued,
        &client,
        &unobserved_archive,
        &unverified_evidence,
        now(),
    )
    .await
    .expect("reconciles");
    let seen = bodies.lock().expect("readable").clone();
    assert_eq!(posts(&seen, "/jobs"), 1, "the queued object is run");
}

/// **The running controllers reconcile through the pool**, with the
/// process's ONE reservation registry and the installation policy's ceiling.
///
/// KILLS: `reconcile` calling a pool-less entry point again; a per-reconcile
/// registry (every pass would see none of the others' admissions); a fixed
/// ceiling instead of the policy's.
#[test]
fn the_running_controllers_reconcile_through_the_pool() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for file in ["src/controllers/backup.rs", "src/controllers/restore.rs"] {
        let source = std::fs::read_to_string(root.join(file)).expect("the source reads");
        let start = source
            .find("\nasync fn reconcile(")
            .unwrap_or_else(|| panic!("{file}: the kube-runtime entry point"));
        let end = source[start + 1..]
            .find("\nfn ")
            .map_or(source.len(), |i| start + 1 + i);
        let body = &source[start..end];
        for needle in [
            "reconcile_in_context_pooled(",
            "store_snapshot(&objects)",
            "limit: None",
            "Reservations::global()",
            "Some(&pool)",
        ] {
            assert!(
                body.contains(needle),
                "{file}: `reconcile` must contain `{needle}`"
            );
        }
    }
}

/// **The ceiling is the installation policy's `runs` block, and a document
/// written before the block existed takes the defaults.**
#[test]
fn the_ceiling_is_the_policy_runs_block_and_absent_is_the_defaults() {
    use weirkeeper::check::policy;
    let older = policy::parse(br#"{"version": 1}"#).expect("an older document parses");
    assert_eq!(
        (
            older.runs.max_manual_backups_active_per_namespace,
            older.runs.max_manual_restores_active_per_namespace
        ),
        (4, 2)
    );
    let newer = policy::parse(
        br#"{"version": 1, "runs": {"maxManualBackupsActivePerNamespace": 7, "maxManualRestoresActivePerNamespace": 3}}"#,
    )
    .expect("parses");
    assert_eq!(
        (
            newer.runs.max_manual_backups_active_per_namespace,
            newer.runs.max_manual_restores_active_per_namespace
        ),
        (7, 3)
    );
    assert!(
        policy::parse(br#"{"version": 1, "runs": {"maxManualBackupsActivePerNamespace": 7}}"#)
            .is_err(),
        "a half-written runs block is refused"
    );
}
