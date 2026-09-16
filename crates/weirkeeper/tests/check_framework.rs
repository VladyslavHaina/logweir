//! The shared check framework — D2 §4.3 and §4.4.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST OR A `mock_client` TEST. Nothing
//! dials a socket and nothing waits on a Job. The double PANICS on a request it
//! was not given a route for, which is what makes "the framework did not read
//! that pod's log" an assertion rather than an absence of evidence.
//!
//! READ `a_pod_that_wears_the_label_but_is_not_owned_is_never_read` FIRST. It
//! is D-SEAMS **S6** and defect `SEC-PODLOG`: `batch.kubernetes.io/job-name` is
//! writable by anything that can create a pod, and a check's stdout becomes a
//! custom resource's status and then an API response. The label narrows the
//! list; the controller `ownerReference` UID decides.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{Event, Pod};
use logweir_core::check_contract::{
    frames, topic_tsv_sha256, CheckCode, CheckId, CheckPlanKind, FrameExpectations, Stream,
    TopicEntry, CHECK_CONTRACT_VERSION,
};
use serde_json::{json, Value};
use weirkeeper::check::{
    self, job as cjob, pod as cpod, policy, relay, waiting, CheckPhase, EventFact, Input,
    Projections, Waiting,
};
use weirkeeper::job::{RunnerOwner, SecretMount};
use weirkeeper::testing::{mock_client_recording_bodies, Route};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const NS: &str = "logweir-d2-w5";
/// The subject (a `TopicDiscovery`, once W6a lands the kind).
const SUBJECT_UID: &str = "9f2b1c44-0000-4000-8000-0000000000a1";
/// The check Job's UID, which is the only thing a pod can be proved against.
const JOB_UID: &str = "1a2b3c4d-0000-4000-8000-0000000000b2";
/// Another Job's UID — the "wrong owner" in the S6 tests.
const OTHER_JOB_UID: &str = "deadbeef-0000-4000-8000-0000000000c3";
const PLAN_SHA: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const POD: &str = "lwc-td-9ab-xyz12";
/// The `pods/log` path suffix for [`POD`]. A `&'static str`, because
/// [`Route::path_suffix`] is one.
const POD_LOG_PATH: &str = "/pods/lwc-td-9ab-xyz12/log";
/// The check Job's path suffix, spelled out rather than composed, so a change
/// to [`cjob::check_job_name`] has to be argued for here as well as asserted
/// there. `a_check_job_name_is_a_pure_function_of_the_kind_and_the_owner_uid`
/// holds the two together.
const JOB_PATH: &str = "/jobs/lwc-td-288fecc03251c1c31851";

fn utc(h: u32, m: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 16, h, m, s)
        .single()
        .expect("the fixture instant exists")
}

fn now() -> DateTime<Utc> {
    utc(12, 0, 0)
}

fn job_name() -> String {
    cjob::check_job_name(CheckPlanKind::TopicInventory, SUBJECT_UID)
}

fn expectations() -> FrameExpectations {
    FrameExpectations {
        plan_sha256: PLAN_SHA.to_string(),
        subject_uid: SUBJECT_UID.to_string(),
    }
}

fn spec() -> cjob::CheckJobSpec {
    cjob::CheckJobSpec {
        kind: CheckPlanKind::TopicInventory,
        namespace: NS.to_string(),
        owner: RunnerOwner {
            api_version: "logweir.dev/v1alpha1".to_string(),
            kind: "TopicDiscovery".to_string(),
            name: "orders-discovery".to_string(),
            uid: SUBJECT_UID.to_string(),
        },
        plan_config_map: format!("{}-plan", job_name()),
        plan_sha256: PLAN_SHA.to_string(),
        subject_uid: SUBJECT_UID.to_string(),
        timeout_seconds: 120,
        service_account_name: "logweir-runner".to_string(),
        secret_mounts: vec![SecretMount {
            volume: "signing".to_string(),
            secret_name: "logweir-signing-key".to_string(),
            mount_path: "/signing".to_string(),
            items: Vec::new(),
        }],
        config_map_mounts: Vec::new(),
        env_from_secret: Vec::new(),
        env_literal: Vec::new(),
        image: None,
        image_pull_policy: None,
    }
}

/// A Job, finished or not.
fn job_object(finished: Option<&str>, reason: Option<&str>) -> Job {
    let status = match finished {
        Some(kind) => json!({"conditions":[{
            "type": kind, "status": "True",
            "reason": reason,
            "lastProbeTime": "2026-09-16T11:59:00Z",
            "lastTransitionTime": "2026-09-16T11:59:00Z"
        }]}),
        None => json!({}),
    };
    serde_json::from_value(json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": job_name(), "namespace": NS, "uid": JOB_UID,
            "creationTimestamp": "2026-09-16T11:58:00Z",
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "TopicDiscovery",
                "name": "orders-discovery", "uid": SUBJECT_UID,
                "controller": true, "blockOwnerDeletion": true
            }]
        },
        "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}},
        "status": status
    }))
    .expect("the fixture is a Job")
}

/// A pod whose controller owner is the Job with `owner_uid`, or none at all.
fn pod_object(owner: Option<(&str, &str, bool)>, container: Value) -> Pod {
    let owners = match owner {
        Some((kind, uid, controller)) => json!([{
            "apiVersion": "batch/v1", "kind": kind, "name": job_name(),
            "uid": uid, "controller": controller, "blockOwnerDeletion": true
        }]),
        None => json!([]),
    };
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {
            "name": POD, "namespace": NS,
            "creationTimestamp": "2026-09-16T11:58:10Z",
            "ownerReferences": owners,
            "labels": {"batch.kubernetes.io/job-name": job_name()}
        },
        "spec": {"containers": []},
        "status": container
    }))
    .expect("the fixture is a Pod")
}

fn terminated(exit_code: i32) -> Value {
    json!({"phase":"Succeeded","containerStatuses":[{
        "name":"runner","image":"x","imageID":"x","ready":false,"restartCount":0,
        "state":{"terminated":{"exitCode":exit_code,"finishedAt":"2026-09-16T11:59:00Z"}}
    }]})
}

fn waiting_container(reason: &str, message: &str) -> Value {
    json!({"phase":"Pending","containerStatuses":[{
        "name":"runner","image":"x","imageID":"","ready":false,"restartCount":0,
        "state":{"waiting":{"reason":reason,"message":message}}
    }]})
}

fn pod_list(pods: Vec<Pod>) -> String {
    serde_json::to_string(&json!({
        "apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": pods
    }))
    .expect("a serialisable list")
}

/// A well-formed relay for a two-topic inventory.
fn good_log() -> String {
    let entries = vec![TopicEntry::new("orders", 6), TopicEntry::new("payments", 3)];
    let result = br#"{"contract":"logweir.dev/check-result/v1","kind":"topicInventory"}"#;
    let parts = frames::write_parts(Stream::Result, result).expect("small enough to frame");
    let mut streams = BTreeMap::new();
    streams.insert(Stream::Result, (result.to_vec(), parts.len()));
    let end = frames::end_frame(PLAN_SHA, SUBJECT_UID, &streams, Some(&entries));

    let mut out = String::new();
    // A non-frame line FIRST, because the decoder must ignore it (D2 §4.5:
    // the KafkaCluster probe prints its I14 lines on the same stdout).
    out.push_str("cluster-id=M29I2S7FQPyHBEX12Vx7XA\n");
    for e in &entries {
        out.push_str(&frames::write_topic_line(e).expect("a framable entry"));
        out.push('\n');
    }
    for p in parts {
        out.push_str(&p);
        out.push('\n');
    }
    out.push_str(&frames::write_end(&end).expect("a framable end"));
    out.push('\n');
    out
}

// ---------------------------------------------------------------------------
// The Job shape — D2 §4.3's `job.rs` row
// ---------------------------------------------------------------------------

#[test]
fn a_check_job_name_is_a_pure_function_of_the_kind_and_the_owner_uid() {
    let name = cjob::check_job_name(CheckPlanKind::TopicInventory, SUBJECT_UID);
    assert!(name.starts_with("lwc-td-"), "{name}");
    assert_eq!(
        name.len(),
        27,
        "the name is always 27 characters, whatever the owner is called: {name}"
    );
    assert!(
        name.len() <= 63,
        "a Job name has to fit the batch.kubernetes.io/job-name label"
    );
    // The owner's NAME is not in it, which is what removes the `NameTooLong`
    // path the KafkaCluster probe has to carry.
    let long_owner = RunnerOwner {
        name: "a".repeat(200),
        ..spec().owner
    };
    let mut s = spec();
    s.owner = long_owner;
    assert_eq!(
        s.job_name(),
        name,
        "a 200-character owner name changes nothing"
    );

    // Every kind gets its own discriminator, so two checks on the same subject
    // are two Jobs.
    let mut seen: Vec<String> = CheckPlanKind::ALL
        .iter()
        .map(|k| cjob::check_job_name(*k, SUBJECT_UID))
        .collect();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), CheckPlanKind::ALL.len());
    // And two subjects get two Jobs for the same kind.
    assert_ne!(
        cjob::check_job_name(CheckPlanKind::TopicInventory, OTHER_JOB_UID),
        name
    );
}

#[test]
fn the_check_job_is_the_runner_job_shape_plus_labels() {
    let built = cjob::build(&spec());
    let plain = weirkeeper::job::build(&cjob::runner_job_spec(&spec()));

    // THE DEFERRED `job.rs` SEAM. Everything except the two label maps is
    // byte-identical to what `job::build` produces, which is what makes the
    // later move of `labels`/`template_labels` into `RunnerJobSpec` a no-op.
    let mut stripped = built.clone();
    stripped.metadata.labels = None;
    if let Some(s) = stripped.spec.as_mut() {
        s.template.metadata = None;
    }
    assert_eq!(
        serde_json::to_value(&stripped).unwrap(),
        serde_json::to_value(&plain).unwrap(),
        "the check Job differs from the runner Job shape by more than its labels"
    );

    let labels = built.metadata.labels.clone().expect("labels");
    assert_eq!(
        labels.get(cjob::LABEL_MANAGED_BY).map(String::as_str),
        Some("weirkeeper")
    );
    assert_eq!(
        labels.get(cjob::LABEL_COMPONENT).map(String::as_str),
        Some("check")
    );
    assert_eq!(
        labels.get(cjob::LABEL_CHECK_KIND).map(String::as_str),
        Some("topicInventory")
    );
    assert_eq!(
        labels.get(cjob::LABEL_CHECK_OWNER_UID).map(String::as_str),
        Some(SUBJECT_UID)
    );
    let template = built
        .spec
        .as_ref()
        .and_then(|s| s.template.metadata.as_ref())
        .and_then(|m| m.labels.clone())
        .expect("template labels");
    assert_eq!(
        template, labels,
        "the Job and its pod template carry one map"
    );
}

#[test]
fn a_check_job_carries_the_contract_env_the_deadline_margin_and_no_ttl() {
    let built = cjob::build(&spec());
    let job_spec = built.spec.as_ref().expect("a spec");
    assert_eq!(
        job_spec.active_deadline_seconds,
        Some(120 + cjob::DEADLINE_MARGIN_SECONDS)
    );
    assert_eq!(
        job_spec.ttl_seconds_after_finished, None,
        "a TTL at creation time lets pod garbage collection race the relay read"
    );
    assert_eq!(job_spec.backoff_limit, Some(0));

    let container = &job_spec.template.spec.as_ref().unwrap().containers[0];
    assert_eq!(container.name, "runner");
    assert_eq!(
        container.args.as_deref(),
        Some(
            [
                "check",
                "run",
                "--plan",
                "/check/check-plan.json",
                "--check-contract-version",
                "1"
            ]
            .map(String::from)
            .as_slice()
        )
    );
    let env: BTreeMap<String, String> = container
        .env
        .as_ref()
        .unwrap()
        .iter()
        .filter_map(|e| e.value.clone().map(|v| (e.name.clone(), v)))
        .collect();
    assert_eq!(
        env.get(cjob::CONTRACT_VERSION_ENV).map(String::as_str),
        Some(CHECK_CONTRACT_VERSION.to_string().as_str())
    );
    assert_eq!(
        env.get(cjob::PLAN_SHA256_ENV).map(String::as_str),
        Some(PLAN_SHA)
    );
    assert_eq!(
        env.get(cjob::SUBJECT_UID_ENV).map(String::as_str),
        Some(SUBJECT_UID)
    );
    assert_eq!(env.get("RUST_LOG").map(String::as_str), Some("warn"));
    assert_eq!(env.get("TMPDIR").map(String::as_str), Some("/work"));

    // The plan is mounted at /check and NOT through `RunnerJobSpec::plan_config_map`.
    let mounts = container.volume_mounts.as_ref().unwrap();
    assert!(
        mounts
            .iter()
            .any(|m| m.mount_path == "/check" && m.read_only == Some(true)),
        "the check plan is mounted read-only at /check: {mounts:?}"
    );
    assert!(
        !mounts.iter().any(|m| m.mount_path == "/plan"),
        "a check has no /plan mount"
    );
    let pod_spec = job_spec.template.spec.as_ref().unwrap();
    assert_eq!(pod_spec.automount_service_account_token, Some(false));
    assert_eq!(
        pod_spec.service_account_name.as_deref(),
        Some("logweir-runner")
    );
}

// ---------------------------------------------------------------------------
// D-SEAMS S6 — pod identity
// ---------------------------------------------------------------------------

#[test]
fn only_a_controller_owner_reference_with_the_jobs_uid_counts() {
    let owned = pod_object(Some(("Job", JOB_UID, true)), terminated(0));
    assert!(cpod::is_owned_by_job(&owned, JOB_UID));

    // Each of the three conditions, failed one at a time.
    assert!(
        !cpod::is_owned_by_job(
            &pod_object(Some(("Job", OTHER_JOB_UID, true)), terminated(0)),
            JOB_UID
        ),
        "another Job's UID"
    );
    assert!(
        !cpod::is_owned_by_job(
            &pod_object(Some(("Job", JOB_UID, false)), terminated(0)),
            JOB_UID
        ),
        "a non-controller reference is an association somebody else made"
    );
    assert!(
        !cpod::is_owned_by_job(
            &pod_object(Some(("ReplicaSet", JOB_UID, true)), terminated(0)),
            JOB_UID
        ),
        "a ReplicaSet that happens to share the UID string is not this Job"
    );
    assert!(
        !cpod::is_owned_by_job(&pod_object(None, terminated(0)), JOB_UID),
        "an ownerless pod is owned by nothing"
    );
}

/// D2 §12's PLAT-09.1 row
/// `pod::ignores_label_matching_pod_without_job_controller_owner`, over the
/// double, so the ABSENCE of a `pods/log` read is the assertion.
#[tokio::test]
async fn a_pod_that_wears_the_label_but_is_not_owned_is_never_read() {
    for impostor in [
        pod_object(Some(("Job", OTHER_JOB_UID, true)), terminated(0)),
        pod_object(None, terminated(0)),
        pod_object(Some(("Job", JOB_UID, false)), terminated(0)),
    ] {
        // THE TABLE HAS NO `pods/log` ROUTE. The double panics on a request it
        // was not given a route for, so a framework that read the impostor's
        // stdout fails here by panicking rather than by returning bad data.
        let routes = vec![Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list(vec![impostor]),
        }];
        let (client, recorder, _) = mock_client_recording_bodies(routes);
        let observation = check::observe(
            &client,
            NS,
            &job_object(Some("Complete"), None),
            &[],
            &expectations(),
            now(),
        )
        .await
        .expect("the list answered");
        assert_eq!(observation.phase, CheckPhase::Failed);
        assert_eq!(
            observation.reason,
            CheckCode::ResultUnreadable,
            "with no owned pod there is no relay, and nothing is inferred from an exit code \
             that belongs to somebody else's pod"
        );
        let seen = recorder.lock().unwrap();
        assert_eq!(seen.len(), 1, "exactly one call was made: {seen:?}");
        assert!(
            !seen[0].uri.contains(POD_LOG_PATH),
            "the framework read a foreign pod's log: {seen:?}"
        );
    }
}

/// The happy path: an owned pod, a verified relay, and the TTL patched
/// afterwards.
#[tokio::test]
async fn an_owned_pod_with_a_verified_relay_succeeds_and_then_takes_a_ttl() {
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list(vec![pod_object(
                Some(("Job", JOB_UID, true)),
                terminated(0),
            )]),
        },
        Route {
            method: "GET",
            path_suffix: POD_LOG_PATH,
            status: 200,
            body: good_log(),
        },
        Route {
            method: "PATCH",
            path_suffix: JOB_PATH,
            status: 200,
            body: r#"{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"x"}}"#.to_string(),
        },
    ];
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let observation = check::observe(
        &client,
        NS,
        &job_object(Some("Complete"), None),
        &[],
        &expectations(),
        now(),
    )
    .await
    .expect("the double answered");

    assert_eq!(observation.phase, CheckPhase::Succeeded);
    assert_eq!(observation.reason, CheckCode::Succeeded);
    assert_eq!(observation.exit_code, Some(0));
    let relay = observation.relay.expect("a verified relay");
    assert_eq!(relay.topics.len(), 2);
    assert_eq!(
        relay.end.topic_lines.as_ref().map(|t| t.sha256.clone()),
        Some(topic_tsv_sha256(&relay.topics)),
        "the decoder verified the topic digest it was given"
    );
    assert!(!observation.cancel_now);

    // THE TTL IS SENT ONLY BY A CALLER THAT ASKS, AND ONLY AFTER ITS COMMIT.
    // `observe` itself sends no PATCH — asserted by the recorder below.
    {
        let seen = recorder.lock().unwrap();
        assert_eq!(
            seen.iter().filter(|r| r.method == "PATCH").count(),
            0,
            "observe must not write anything: {seen:?}"
        );
    }

    check::set_ttl(&client, NS, &job_name())
        .await
        .expect("the patch route answered");
    let bodies = bodies.lock().unwrap();
    let patch = bodies
        .iter()
        .find(|b| b.method == "PATCH")
        .expect("the TTL patch was sent");
    let body: Value = serde_json::from_str(&patch.body).expect("a merge patch body");
    assert_eq!(
        body["spec"]["ttlSecondsAfterFinished"],
        json!(cjob::TTL_SECONDS)
    );
    assert_eq!(
        body["spec"].as_object().map(serde_json::Map::len),
        Some(1),
        "the TTL patch touches one field and nothing else: {body}"
    );
}

#[tokio::test]
async fn a_running_job_reads_no_log_at_all() {
    let routes = vec![Route {
        method: "GET",
        path_suffix: "/pods",
        status: 200,
        body: pod_list(vec![pod_object(
            Some(("Job", JOB_UID, true)),
            json!({"phase":"Running"}),
        )]),
    }];
    let (client, recorder, _) = mock_client_recording_bodies(routes);
    let observation = check::observe(
        &client,
        NS,
        &job_object(None, None),
        &[],
        &expectations(),
        now(),
    )
    .await
    .expect("the list answered");
    assert_eq!(observation.phase, CheckPhase::Running);
    assert_eq!(observation.reason, CheckCode::PodNotStarted);
    assert!(observation.relay.is_none());
    let seen = recorder.lock().unwrap();
    assert!(
        seen.iter().all(|r| !r.uri.contains(POD_LOG_PATH)),
        "a running check's stdout has no end frame yet; reading it would always be \
         ResultUnreadable: {seen:?}"
    );
}

// ---------------------------------------------------------------------------
// The relay — D2 §4.3's `relay.rs` row
// ---------------------------------------------------------------------------

#[test]
fn the_log_read_names_the_runner_container_and_a_byte_limit() {
    let p = relay::log_params();
    assert_eq!(p.container.as_deref(), Some("runner"));
    assert_eq!(p.limit_bytes, Some(relay::RELAY_LIMIT_BYTES));
    assert_eq!(relay::RELAY_LIMIT_BYTES, 8 * 1024 * 1024);
    assert!(!p.follow, "a check's log is read once, not streamed");
    assert!(
        !p.previous,
        "there is no previous container: restartPolicy is Never"
    );
}

/// D2 §12's framework rows `relay::missing_end_line_is_result_unreadable`,
/// `relay::wrong_plan_sha_is_refused` and `relay::part_digest_mismatch`, plus
/// the truncation case the brief names.
#[test]
fn every_way_a_relay_can_fail_to_verify_is_result_unreadable() {
    let good = good_log();
    assert!(
        relay::decode(&good, &expectations()).is_ok(),
        "the fixture verifies"
    );

    // 1. TRUNCATED: the end frame never arrived, which is what an 8 MiB
    //    `limitBytes` cut or a killed pod looks like.
    let truncated: String = good
        .lines()
        .filter(|l| !l.starts_with("logweir-check-end="))
        .map(|l| format!("{l}\n"))
        .collect();
    let e = relay::decode(&truncated, &expectations()).expect_err("no end frame");
    assert_eq!(e.code, CheckCode::ResultUnreadable);

    // 2. TRUNCATED MID-STREAM: the end frame declares two parts and one arrived.
    let cut: String = good
        .lines()
        .filter(|l| !l.contains("logweir-check-part=result:1/"))
        .map(|l| format!("{l}\n"))
        .collect();
    if cut != good {
        assert_eq!(
            relay::decode(&cut, &expectations())
                .expect_err("a missing part")
                .code,
            CheckCode::ResultUnreadable
        );
    }

    // 3. A TOPIC LINE DROPPED: count and digest both disagree.
    let short: String = good
        .lines()
        .filter(|l| !l.starts_with("logweir-check-topic=payments"))
        .map(|l| format!("{l}\n"))
        .collect();
    assert_eq!(
        relay::decode(&short, &expectations())
            .expect_err("a dropped topic")
            .code,
        CheckCode::ResultUnreadable
    );

    // 4. THE WRONG PLAN. A relay that verifies against another plan's digest is
    //    a relay from another check's Job.
    let other = FrameExpectations {
        plan_sha256: "sha256:2222222222222222222222222222222222222222222222222222222222222222"
            .to_string(),
        subject_uid: SUBJECT_UID.to_string(),
    };
    assert_eq!(
        relay::decode(&good, &other)
            .expect_err("a wrong plan digest")
            .code,
        CheckCode::ResultUnreadable
    );

    // 5. THE WRONG SUBJECT.
    let other = FrameExpectations {
        plan_sha256: PLAN_SHA.to_string(),
        subject_uid: OTHER_JOB_UID.to_string(),
    };
    assert_eq!(
        relay::decode(&good, &other)
            .expect_err("a wrong subject uid")
            .code,
        CheckCode::ResultUnreadable
    );
}

#[test]
fn a_relay_refusal_never_carries_log_content() {
    let secret_bearing = format!(
        "{}\nAWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY\n",
        good_log()
            .lines()
            .filter(|l| !l.starts_with("logweir-check-end="))
            .collect::<Vec<_>>()
            .join("\n")
    );
    let e = relay::decode(&secret_bearing, &expectations()).expect_err("no end frame");
    assert!(
        !e.reason.contains("wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"),
        "the refusal quoted the log: {}",
        e.reason
    );
    assert!(
        !e.reason.contains("logweir-check-topic="),
        "the refusal quoted a frame: {}",
        e.reason
    );
}

/// The runner's exit-3 contract refusal, read BY KEY NAME from a bounded tail
/// (plan erratum E4) — and read BEFORE the frames, because exit 3 prints none.
#[test]
fn a_contract_refusal_is_read_by_key_name_and_beats_the_absent_frames() {
    let log = "some tracing on stderr\nrefusal-reason=CheckContractMismatch\nmore noise\n";
    assert_eq!(
        relay::refusal_reason(log),
        Some(CheckCode::CheckContractMismatch)
    );
    // The LAST occurrence within the tail wins.
    assert_eq!(
        relay::refusal_reason("refusal-reason=Timeout\nrefusal-reason=DeadlineExceeded\n"),
        Some(CheckCode::DeadlineExceeded)
    );
    // A value outside the closed vocabulary is an ABSENCE, never a fabricated
    // reason: this string becomes a metav1.Condition reason verbatim.
    assert_eq!(relay::refusal_reason("refusal-reason=SomethingNew\n"), None);
    assert_eq!(relay::refusal_reason("nothing here\n"), None);

    let observation = check::classify(&Input {
        job: &job_object(Some("Failed"), None),
        pod: Some(&pod_object(Some(("Job", JOB_UID, true)), terminated(3))),
        events: &[],
        log: Some(log),
        expect: &expectations(),
        now: now(),
    });
    assert_eq!(observation.phase, CheckPhase::Failed);
    assert_eq!(
        observation.reason,
        CheckCode::CheckContractMismatch,
        "a correct exit-3 refusal must not be reported as an unreadable result"
    );
    assert_eq!(observation.exit_code, Some(3));
}

// ---------------------------------------------------------------------------
// The waiting table — D2 §4.3
// ---------------------------------------------------------------------------

fn classify_pod(container: Value, events: &[EventFact], now: DateTime<Utc>) -> Option<Waiting> {
    let pod = pod_object(Some(("Job", JOB_UID, true)), container);
    waiting::classify(&waiting::Observed {
        job: &job_object(None, None),
        pod: Some(&pod),
        events,
        now,
    })
}

#[test]
fn every_waiting_row_of_the_table_has_its_own_code() {
    // 1. A missing Secret, naming it.
    let w = classify_pod(
        waiting_container(
            "CreateContainerConfigError",
            r#"secret "orders-sasl" not found"#,
        ),
        &[],
        now(),
    )
    .expect("classified");
    assert_eq!(w.code, CheckCode::CredentialSecretNotFound);
    assert_eq!(w.secret.as_deref(), Some("orders-sasl"));
    assert!(
        w.is_terminal(),
        "a Secret that is not there will not appear by waiting"
    );

    // 2. A missing KEY inside a Secret that exists, naming both.
    let w = classify_pod(
        waiting_container(
            "CreateContainerConfigError",
            "couldn't find key password in Secret logweir-d2-w5/orders-sasl",
        ),
        &[],
        now(),
    )
    .expect("classified");
    assert_eq!(w.code, CheckCode::CredentialSecretKeyMissing);
    assert_eq!(w.secret.as_deref(), Some("orders-sasl"));
    assert_eq!(w.key.as_deref(), Some("password"));

    // 3. A missing trust bundle.
    let w = classify_pod(
        waiting_container(
            "CreateContainerConfigError",
            r#"configmap "minio-ca" not found"#,
        ),
        &[],
        now(),
    )
    .expect("classified");
    assert_eq!(w.code, CheckCode::TrustBundleNotFound);
    assert_eq!(w.config_map.as_deref(), Some("minio-ca"));

    // 4-6. The three image rows.
    for (reason, code, terminal) in [
        ("ErrImagePull", CheckCode::RunnerImagePullFailed, false),
        ("ImagePullBackOff", CheckCode::RunnerImagePullFailed, false),
        ("ErrImageNeverPull", CheckCode::RunnerImageNotPresent, true),
        ("InvalidImageName", CheckCode::RunnerImageInvalid, true),
    ] {
        let w = classify_pod(waiting_container(reason, "x"), &[], now()).expect("classified");
        assert_eq!(w.code, code, "{reason}");
        assert_eq!(w.is_terminal(), terminal, "{reason}");
    }

    // 7. Unschedulable, only AFTER its grace period.
    let unschedulable = json!({"phase":"Pending","conditions":[{
        "type":"PodScheduled","status":"False","reason":"Unschedulable",
        "lastTransitionTime":"2026-09-16T11:59:00Z"
    }]});
    assert_eq!(
        classify_pod(unschedulable.clone(), &[], utc(12, 0, 0)).map(|w| w.code),
        Some(CheckCode::PodUnschedulable),
        "the condition transitioned 60 s ago, which is exactly the grace boundary"
    );
    assert_eq!(
        classify_pod(unschedulable, &[], utc(11, 59, 59)),
        None,
        "one second inside the grace period is not yet a finding"
    );

    // 8. A FailedMount on the signing volume, and on any other.
    let creating = json!({"phase":"Pending","containerStatuses":[{
        "name":"runner","image":"x","imageID":"","ready":false,"restartCount":0,
        "state":{"waiting":{"reason":"ContainerCreating","message":""}}
    }]});
    let mount = |volume: &str| EventFact {
        reason: "FailedMount".to_string(),
        message: format!(r#"Unable to attach or mount volumes: unmounted volume "{volume}""#),
        involved_kind: "Pod".to_string(),
        involved_name: POD.to_string(),
    };
    let w = classify_pod(creating.clone(), &[mount("signing")], utc(12, 0, 0)).expect("classified");
    assert_eq!(w.code, CheckCode::SigningKeyMissing);
    assert_eq!(w.volume.as_deref(), Some("signing"));
    let w =
        classify_pod(creating.clone(), &[mount("archive-ca")], utc(12, 0, 0)).expect("classified");
    assert_eq!(w.code, CheckCode::VolumeMountFailed);
    assert_eq!(w.volume.as_deref(), Some("archive-ca"));
    assert_eq!(
        classify_pod(creating, &[mount("signing")], utc(11, 58, 30)),
        None,
        "inside the mount grace period a slow mount is not a finding"
    );

    // 9. A disrupted pod beats everything else that is true at the same time.
    let disrupted = json!({
        "phase":"Pending",
        "conditions":[{"type":"DisruptionTarget","status":"True",
                       "lastTransitionTime":"2026-09-16T11:59:00Z"}],
        "containerStatuses":[{"name":"runner","image":"x","imageID":"","ready":false,
            "restartCount":0,"state":{"waiting":{"reason":"ErrImagePull","message":"x"}}}]
    });
    assert_eq!(
        classify_pod(disrupted, &[], now()).map(|w| w.code),
        Some(CheckCode::DisruptedMidCheck)
    );
}

#[test]
fn a_job_with_no_pod_is_explained_by_its_failed_create_event_after_the_grace_period() {
    let sa_missing = EventFact {
        reason: "FailedCreate".to_string(),
        message: r#"Error creating: pods "lwc-td-x" is forbidden: error looking up service account logweir-d2-w5/logweir-runner: serviceaccount "logweir-runner" not found"#.to_string(),
        involved_kind: "Job".to_string(),
        involved_name: job_name(),
    };
    let quota = EventFact {
        reason: "FailedCreate".to_string(),
        message: "Error creating: pods \"lwc-td-x\" is forbidden: exceeded quota: compute"
            .to_string(),
        involved_kind: "Job".to_string(),
        involved_name: job_name(),
    };
    let at = |events: &[EventFact], now: DateTime<Utc>| {
        waiting::classify(&waiting::Observed {
            job: &job_object(None, None),
            pod: None,
            events,
            now,
        })
    };
    assert_eq!(
        at(std::slice::from_ref(&sa_missing), utc(11, 58, 30)).map(|w| w.code),
        Some(CheckCode::RunnerServiceAccountMissing),
        "30 s after the Job was created is exactly the grace boundary"
    );
    assert_eq!(
        at(&[sa_missing], utc(11, 58, 29)),
        None,
        "inside the grace period the Job simply has not made its pod yet"
    );
    assert_eq!(
        at(&[quota], now()).map(|w| w.code),
        Some(CheckCode::PodCreateRejected)
    );
    assert_eq!(at(&[], now()), None, "no event, no finding");
}

#[test]
fn a_jobs_own_deadline_is_a_code_and_not_an_unreadable_relay() {
    let job = job_object(Some("Failed"), Some("DeadlineExceeded"));
    assert!(waiting::job_deadline_exceeded(&job));
    let observation = check::classify(&Input {
        job: &job,
        pod: Some(&pod_object(
            Some(("Job", JOB_UID, true)),
            json!({"phase":"Failed"}),
        )),
        events: &[],
        log: Some("no frames here at all\n"),
        expect: &expectations(),
        now: now(),
    });
    assert_eq!(observation.phase, CheckPhase::Failed);
    assert_eq!(
        observation.reason,
        CheckCode::DeadlineExceeded,
        "a pod killed by its deadline wrote no relay; reporting ResultUnreadable would hide why"
    );
}

#[test]
fn a_terminal_waiting_state_asks_for_an_immediate_cancel() {
    let observation = check::classify(&Input {
        job: &job_object(None, None),
        pod: Some(&pod_object(
            Some(("Job", JOB_UID, true)),
            waiting_container("CreateContainerConfigError", r#"secret "gone" not found"#),
        )),
        events: &[],
        log: None,
        expect: &expectations(),
        now: now(),
    });
    assert_eq!(observation.phase, CheckPhase::Failed);
    assert_eq!(observation.reason, CheckCode::CredentialSecretNotFound);
    assert!(
        observation.cancel_now,
        "a user should not watch a spinner for a Secret that does not exist"
    );

    // Unschedulable is deliberately NOT an immediate cancel: a node can join.
    let observation = check::classify(&Input {
        job: &job_object(None, None),
        pod: Some(&pod_object(
            Some(("Job", JOB_UID, true)),
            json!({"phase":"Pending","conditions":[{
                "type":"PodScheduled","status":"False","reason":"Unschedulable",
                "lastTransitionTime":"2026-09-16T11:58:00Z"}]}),
        )),
        events: &[],
        log: None,
        expect: &expectations(),
        now: now(),
    });
    assert_eq!(observation.reason, CheckCode::PodUnschedulable);
    assert!(!observation.cancel_now);
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_patches_the_deadline_only_on_an_owned_unfinished_job() {
    // A foreign Job with the same NAME is never touched: the table has no
    // PATCH route, so a framework that sent one panics here.
    let (client, _, _) = mock_client_recording_bodies(Vec::new());
    let foreign: Job = serde_json::from_value(json!({
        "apiVersion":"batch/v1","kind":"Job",
        "metadata":{"name":job_name(),"namespace":NS,"uid":OTHER_JOB_UID,
            "ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"x",
                "uid":"someone-else","controller":true}]},
        "spec":{"template":{"spec":{"containers":[],"restartPolicy":"Never"}}},
        "status":{}
    }))
    .unwrap();
    assert!(!check::is_cancellable(&foreign, SUBJECT_UID));
    assert!(!check::cancel(&client, NS, &foreign, SUBJECT_UID)
        .await
        .unwrap());

    // A FINISHED Job is not cancelled either: that would rewrite the reason it
    // finished for.
    assert!(!check::is_cancellable(
        &job_object(Some("Complete"), None),
        SUBJECT_UID
    ));
    assert!(!check::cancel(
        &client,
        NS,
        &job_object(Some("Complete"), None),
        SUBJECT_UID
    )
    .await
    .unwrap());

    // The owned, unfinished one is.
    let routes = vec![Route {
        method: "PATCH",
        path_suffix: JOB_PATH,
        status: 200,
        body: r#"{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"x"}}"#.to_string(),
    }];
    let (client, _, bodies) = mock_client_recording_bodies(routes);
    assert!(
        check::cancel(&client, NS, &job_object(None, None), SUBJECT_UID)
            .await
            .unwrap()
    );
    let bodies = bodies.lock().unwrap();
    let body: Value = serde_json::from_str(&bodies[0].body).unwrap();
    assert_eq!(body, json!({"spec":{"activeDeadlineSeconds":1}}));
}

// ---------------------------------------------------------------------------
// Attribution — D2 §4.3's "Mapping to check IDs"
// ---------------------------------------------------------------------------

#[test]
fn a_waiting_code_belongs_to_the_check_whose_projection_it_names() {
    let projections = Projections {
        connection_secret: Some("orders-sasl".to_string()),
        destination_secret: Some("minio-keys".to_string()),
        signer_secret: Some("logweir-signing-key".to_string()),
        trust_config_maps: vec!["minio-ca".to_string()],
    };
    let secret_not_found = |name: &str| Waiting {
        code: CheckCode::CredentialSecretNotFound,
        secret: Some(name.to_string()),
        key: None,
        config_map: None,
        volume: None,
        message: String::new(),
    };
    assert_eq!(
        check::attribute(&secret_not_found("orders-sasl"), &projections),
        Some(CheckId::ConnectionCredentialProjected)
    );
    assert_eq!(
        check::attribute(&secret_not_found("minio-keys"), &projections),
        Some(CheckId::DestinationCredentialProjected)
    );
    assert_eq!(
        check::attribute(&secret_not_found("logweir-signing-key"), &projections),
        Some(CheckId::SignerPrivateKeyUsable)
    );
    // THE UNMATCHED CASE IS `None`, AND IT IS A REAL ANSWER. Attributing a
    // Secret this plan did not project to the connection would send an
    // operator to rotate the wrong credential.
    assert_eq!(
        check::attribute(&secret_not_found("somebody-elses"), &projections),
        None
    );
    assert_eq!(
        check::attribute(
            &Waiting {
                code: CheckCode::RunnerImageNotPresent,
                secret: None,
                key: None,
                config_map: None,
                volume: None,
                message: String::new()
            },
            &projections
        ),
        Some(CheckId::RunnerImage)
    );
    assert_eq!(
        check::attribute(
            &Waiting {
                code: CheckCode::TrustBundleNotFound,
                secret: None,
                key: None,
                config_map: Some("minio-ca".to_string()),
                volume: None,
                message: String::new()
            },
            &projections
        ),
        Some(CheckId::DestinationResolved)
    );
}

// ---------------------------------------------------------------------------
// The policy ConfigMap — D2 §4.4
// ---------------------------------------------------------------------------

const GOOD_POLICY: &str = r#"{"version":1,
 "checks":{"maxActivePerNamespace":4,"maxActiveTotal":20,
           "maxActiveDiscoveriesPerConnection":1,"maxEvidenceFetchActivePerNamespace":4},
 "discovery":{"freshSeconds":900,"retentionSeconds":86400,"keepPerConnection":5,
              "defaultMaxTopics":20000,"hardMaxTopics":50000,
              "visibilityAttestations":[
                {"id":"att-orders-prod","namespace":"team-a","kafkaCluster":"source",
                 "clusterId":"M29I2S7FQPyHBEX12Vx7XA","principal":"User:backup",
                 "attestedBy":"platform-admin@example.invalid",
                 "attestedAt":"2026-09-15T00:00:00Z","expiresAt":"2026-12-15T00:00:00Z",
                 "statement":"reviewed ACL export 2026-09-14"}]},
 "preflight":{"defaultTimeoutSeconds":120,"retentionSeconds":3600},
 "engine":{"allowUnverifiedCustomCa":false},
 "evidence":{"controllerIdentityLocations":[
   {"endpoint":"https://minio-b.ns.svc:9000","region":"","bucket":"lw-b"}]},
 "legacyArchiveAddressing":{"endpoint":"","region":"","allowHttp":false,
                            "virtualHostedStyle":false}}"#;

#[tokio::test]
async fn an_absent_policy_is_the_documented_defaults_and_is_ready() {
    // No reference configured at all: nothing is read.
    let (client, recorder, _) = mock_client_recording_bodies(Vec::new());
    let cache = policy::PolicyCache::new();
    let load = policy::load(&client, None, &cache, now()).await.unwrap();
    assert_eq!(
        load,
        policy::PolicyLoad::Defaulted(policy::Policy::defaults())
    );
    assert_eq!(load.code(), CheckCode::PolicyLoaded);
    assert!(recorder.lock().unwrap().is_empty(), "nothing was read");

    // Configured, but the ConfigMap is not there: still the defaults.
    let routes = vec![Route {
        method: "GET",
        path_suffix: "/configmaps/weirkeeper-policy",
        status: 404,
        body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
            "message":"configmaps \"weirkeeper-policy\" not found",
            "reason":"NotFound","code":404}"#
            .to_string(),
    }];
    let (client, _, _) = mock_client_recording_bodies(routes);
    let reference = (
        "logweir-system".to_string(),
        "weirkeeper-policy".to_string(),
    );
    let load = policy::load(
        &client,
        Some(&reference),
        &policy::PolicyCache::new(),
        now(),
    )
    .await
    .unwrap();
    assert!(matches!(load, policy::PolicyLoad::Defaulted(_)));
    assert_eq!(load.code(), CheckCode::PolicyLoaded);
    let p = load.policy();
    assert_eq!(p.checks.max_active_per_namespace, 4);
    assert_eq!(p.checks.max_active_total, 20);
    assert_eq!(p.discovery.default_max_topics, 20_000);
    assert_eq!(p.discovery.hard_max_topics, 50_000);
    assert_eq!(p.discovery.keep_per_connection, 5);
    assert_eq!(p.preflight.default_timeout_seconds, 120);
    assert!(
        p.discovery.visibility_attestations.is_empty(),
        "with no policy nothing is ever attestedComplete"
    );
    assert!(p.evidence.controller_identity_locations.is_empty());
    assert!(!p.legacy_archive_addressing.allow_http, "D-SEAMS S5");
}

#[tokio::test]
async fn a_well_formed_policy_is_loaded_and_carries_its_attestation() {
    let routes = vec![Route {
        method: "GET",
        path_suffix: "/configmaps/weirkeeper-policy",
        status: 200,
        body: serde_json::to_string(&json!({
            "apiVersion":"v1","kind":"ConfigMap",
            "metadata":{"name":"weirkeeper-policy","namespace":"logweir-system"},
            "data":{"policy.json": GOOD_POLICY}
        }))
        .unwrap(),
    }];
    let (client, recorder, _) = mock_client_recording_bodies(routes);
    let reference = (
        "logweir-system".to_string(),
        "weirkeeper-policy".to_string(),
    );
    let cache = policy::PolicyCache::new();
    let load = policy::load(&client, Some(&reference), &cache, now())
        .await
        .unwrap();
    assert!(matches!(load, policy::PolicyLoad::Loaded(_)));
    assert_eq!(load.code(), CheckCode::PolicyLoaded);
    let p = load.policy();
    assert_eq!(p.discovery.visibility_attestations.len(), 1);
    assert_eq!(p.discovery.visibility_attestations[0].id, "att-orders-prod");
    assert_eq!(p.evidence.controller_identity_locations[0].bucket, "lw-b");
    assert!(p.digest().starts_with("sha256:"));

    // THE CACHE. A second load inside the TTL sends nothing.
    let again = policy::load(&client, Some(&reference), &cache, now())
        .await
        .unwrap();
    assert_eq!(again, load);
    assert_eq!(recorder.lock().unwrap().len(), 1, "the 30 s cache held");
    // And past the TTL it reads again.
    let _ = policy::load(
        &client,
        Some(&reference),
        &cache,
        now() + chrono::Duration::seconds(31),
    )
    .await
    .unwrap();
    assert_eq!(recorder.lock().unwrap().len(), 2, "the cache expired");
}

/// MALFORMED FAILS CLOSED. Every case below leaves the attestation list and the
/// identity allowlist EMPTY, because a policy nobody can read must never be the
/// reason a topic listing is reported as complete (D-SEAMS S3).
#[test]
fn a_malformed_policy_is_refused_by_name_and_fails_closed() {
    let cases: &[(&str, &str)] = &[
        ("{", "truncated JSON"),
        (
            r#"{"version":2}"#,
            "a version this build does not understand",
        ),
        (
            r#"{"version":1,"checks":{"maxActivePerNamespace":0,"maxActiveTotal":20,
                "maxActiveDiscoveriesPerConnection":1,"maxEvidenceFetchActivePerNamespace":4}}"#,
            "a limit of zero would queue every check forever",
        ),
        (
            r#"{"version":1,"discovery":{"freshSeconds":900,"retentionSeconds":1,
                "keepPerConnection":5,"defaultMaxTopics":60000,"hardMaxTopics":50000}}"#,
            "defaultMaxTopics above hardMaxTopics",
        ),
        (
            r#"{"version":1,"discovery":{"freshSeconds":900,"retentionSeconds":1,
                "keepPerConnection":5,"defaultMaxTopics":10,"hardMaxTopics":999999}}"#,
            "hardMaxTopics above the contract ceiling",
        ),
        (
            r#"{"version":1,"somethingNew":true}"#,
            "an unknown field is a policy this build would silently ignore half of",
        ),
    ];
    for (body, why) in cases {
        let mut data = BTreeMap::new();
        data.insert(policy::POLICY_KEY.to_string(), (*body).to_string());
        let load = policy::from_data(Some(&data));
        let policy::PolicyLoad::Unreadable { policy: p, reason } = &load else {
            panic!("`{why}` was accepted: {load:?}");
        };
        assert_eq!(load.code(), CheckCode::PolicyUnreadable, "{why}");
        assert!(!load.is_readable(), "{why}");
        assert!(!reason.is_empty(), "the refusal must name itself: {why}");
        assert!(
            p.discovery.visibility_attestations.is_empty()
                && p.evidence.controller_identity_locations.is_empty(),
            "`{why}` did not fail closed"
        );
        // The limits still have their defaults, so checks keep running.
        assert_eq!(p.checks.max_active_per_namespace, 4, "{why}");
    }

    // A ConfigMap that exists with the WRONG KEY is malformed, not absent: an
    // administrator meant to configure something.
    let mut data = BTreeMap::new();
    data.insert("policy.yaml".to_string(), GOOD_POLICY.to_string());
    assert_eq!(
        policy::from_data(Some(&data)).code(),
        CheckCode::PolicyUnreadable
    );
    assert_eq!(policy::from_data(None).code(), CheckCode::PolicyUnreadable);

    // And the good one is accepted, so the cases above are not vacuous.
    let mut data = BTreeMap::new();
    data.insert(policy::POLICY_KEY.to_string(), GOOD_POLICY.to_string());
    assert_eq!(
        policy::from_data(Some(&data)).code(),
        CheckCode::PolicyLoaded
    );
}

#[test]
fn the_policy_reference_comes_from_the_environment_or_the_release_namespace() {
    assert_eq!(
        policy::configured_ref(Some("logweir-system/weirkeeper-policy"), None),
        Some((
            "logweir-system".to_string(),
            "weirkeeper-policy".to_string()
        ))
    );
    // Only the namespace: the default name in it.
    assert_eq!(
        policy::configured_ref(None, Some("logweir-system")),
        Some((
            "logweir-system".to_string(),
            policy::DEFAULT_POLICY_NAME.to_string()
        ))
    );
    // Neither: no policy is configured, which is Defaulted and not an error.
    assert_eq!(policy::configured_ref(None, None), None);
    assert_eq!(policy::configured_ref(Some("  "), None), None);
    // A malformed reference names no object, so nothing is read.
    assert_eq!(policy::configured_ref(Some("no-slash"), None), None);
    assert_eq!(policy::configured_ref(Some("/name"), None), None);
    assert_eq!(policy::configured_ref(Some("ns/"), None), None);
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

#[test]
fn an_unattributable_waiting_code_makes_the_whole_set_unknown() {
    use logweir_core::check_contract::{Authority, CheckOutcome, CheckState, Gating, OverallState};
    let ready = CheckOutcome::new(
        CheckId::ConnectionResolved,
        CheckState::Ready,
        Gating::Blocking,
        Authority::Controller,
        CheckCode::Resolved,
    );
    assert_eq!(
        check::overall(std::slice::from_ref(&ready), None),
        OverallState::Ready
    );
    let blocker = Waiting {
        code: CheckCode::CredentialSecretNotFound,
        secret: Some("somebody-elses".to_string()),
        key: None,
        config_map: None,
        volume: None,
        message: String::new(),
    };
    assert_eq!(
        check::overall(&[ready], Some(&blocker)),
        OverallState::Unknown,
        "a pod that could not start makes every Job-sourced answer unknown"
    );
    // The empty set is Unknown and never Ready — the pure crate's rule,
    // restated here because a controller reaches it through this wrapper.
    assert_eq!(check::overall(&[], None), OverallState::Unknown);
}

// ---------------------------------------------------------------------------
// The EventFact converter
// ---------------------------------------------------------------------------

#[test]
fn an_event_reduces_to_the_four_fields_the_table_reads() {
    let event: Event = serde_json::from_value(json!({
        "apiVersion":"v1","kind":"Event",
        "metadata":{"name":"lwc-td-x.1","namespace":NS},
        "reason":"FailedMount",
        "message":"Unable to attach or mount volumes: unmounted volume \"signing\"",
        "involvedObject":{"kind":"Pod","name":POD,"namespace":NS}
    }))
    .unwrap();
    let fact = EventFact::from_event(&event).expect("a reason is present");
    assert_eq!(fact.reason, "FailedMount");
    assert_eq!(fact.involved_kind, "Pod");
    assert_eq!(fact.involved_name, POD);

    let no_reason: Event = serde_json::from_value(json!({
        "apiVersion":"v1","kind":"Event","metadata":{"name":"x","namespace":NS},
        "involvedObject":{}
    }))
    .unwrap();
    assert_eq!(
        EventFact::from_event(&no_reason),
        None,
        "an event with no reason could match no row"
    );
}
