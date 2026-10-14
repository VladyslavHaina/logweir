//! **PROD-11.1a: `logweir restore cleanup` deletes only what the run created**
//! (design `PROD-04.2-cutover.md` §5; C40/C41/C42's unit halves).
//!
//! Every row drives `restore_cleanup::clean` over a signed scorecard in an
//! in-memory evidence store and a recording cluster double, and asserts what
//! the deleter was ASKED, not only what the report says.

use logweir::restore_cleanup::{clean, finish, Cleaned, EvidenceStores, Inputs};
use logweir::signer::ValidatedSigner;
use logweir_core::spec::DrillSpec;
use logweir_engine_oso::storage::{caps, Store, StoreError};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicDeleter, TopicMeta};
use logweir_kafka::topic_ids::TopicIdRead;
use std::collections::BTreeMap;
use std::sync::Mutex;

const RUN: &str = "01J9X2QK7C4V0R8YB3ZP6MTS5A";
const CLUSTER: &str = "MkU3OEVBNTcwNTJENDM2Qk";
const A: &str = "Cf6zT_mcTNCoxuPmv1Ztxw";
const B: &str = "Zz6zT_mcTNCoxuPmv1Ztxw";

fn plan_text(prefix: &str, teardown: &str) -> String {
    format!(
        "source:\n  storage: {{backend: s3, bucket: b, prefix: p, region: us-east-1}}\n  \
         backup: latestCompleted\n  topics: [orders, payments, refunds, audit]\n\
         target:\n  bootstrap_servers: [\"k:9092\"]\n  mode: scratch\n  \
         topic_mapping_prefix: \"{prefix}\"\n  teardown: {teardown}\n\
         sample:\n  window_start: \"2026-10-13T00:00:00Z\"\n  \
         window_end: \"2026-10-13T01:00:00Z\"\n\
         objectives: {{}}\n\
         evidence: {{backend: s3, bucket: e, prefix: logweir/, region: us-east-1}}\n"
    )
}

/// The cluster double: a cluster id, each topic's current ID, and every name
/// the deleter was asked to delete.
struct Cluster {
    id: String,
    ids: BTreeMap<String, TopicIdRead>,
    asked: Mutex<Vec<String>>,
    refuse: Vec<String>,
}

impl Cluster {
    fn new(ids: &[(&str, TopicIdRead)]) -> Self {
        Self {
            id: CLUSTER.into(),
            ids: ids
                .iter()
                .map(|(n, r)| ((*n).to_string(), r.clone()))
                .collect(),
            asked: Mutex::new(Vec::new()),
            refuse: Vec::new(),
        }
    }
    fn asked(&self) -> Vec<String> {
        self.asked.lock().unwrap().clone()
    }
}

impl ClusterReader for Cluster {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok(self.id.clone())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(Vec::new())
    }
    fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(Vec::new())
    }
    fn topic_configs(&self, _topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        _topic: &str,
        _partition: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(Vec::new())
    }
    fn topic_ids(&self, topics: &[String]) -> Result<Vec<(String, TopicIdRead)>, KafkaError> {
        Ok(topics
            .iter()
            .filter_map(|t| self.ids.get(t).map(|r| (t.clone(), r.clone())))
            .collect())
    }
}

impl TopicDeleter for Cluster {
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.asked.lock().unwrap().extend(names.iter().cloned());
        Ok(names
            .iter()
            .map(|n| {
                if self.refuse.contains(n) {
                    (n.clone(), Err("Broker: Topic authorization failed".into()))
                } else {
                    (n.clone(), Ok(()))
                }
            })
            .collect())
    }
}

fn signer() -> ValidatedSigner {
    ValidatedSigner::load(
        std::path::Path::new("../../e2e/fixtures/signed/signing.pem"),
        logweir_evidence::PAYLOAD_TYPE_TEARDOWN,
        b"logweir restore signing readiness probe v1",
        "No evidence was uploaded",
    )
    .expect("the fixture key")
}

/// A signed clone scorecard for `plan` naming `created`, put in a fresh
/// in-memory evidence store.
fn store_with(
    plan: &str,
    created: &[(&str, Option<&str>)],
    edit: impl FnOnce(&mut serde_json::Value),
) -> Store {
    let mut doc: serde_json::Value = serde_json::from_str(include_str!(
        "../../../e2e/fixtures/invariants/target_creation_1_10_ttl_clone.json"
    ))
    .unwrap();
    doc["approval"]["plan_hash"] = logweir_core::ids::sha256_prefixed(plan.as_bytes()).into();
    doc["target"]["creation"]["topics"] = created
        .iter()
        .map(|(name, id)| match id {
            Some(id) => serde_json::json!({"name": name, "partitions": 1, "topic_id": id}),
            None => serde_json::json!({"name": name, "partitions": 1, "topic_id": null,
                                      "topic_id_reason": "noTopicId"}),
        })
        .collect();
    edit(&mut doc);
    let bytes = serde_json::to_vec_pretty(&doc).unwrap();
    let sidecar = signer()
        .sign(logweir_evidence::PAYLOAD_TYPE_SCORECARD, &bytes)
        .unwrap();
    let store = Store::in_memory("logweir");
    store
        .put_create_only(&format!("logweir/drills/{RUN}.json"), &bytes)
        .unwrap();
    store
        .put_create_only(
            &format!("logweir/drills/{RUN}.sig"),
            &serde_json::to_vec(&sidecar).unwrap(),
        )
        .unwrap();
    store
}

fn run(
    plan: &str,
    store: &Store,
    cluster: &Cluster,
) -> Result<Cleaned, logweir::drill::DrillError> {
    let spec: DrillSpec = serde_yaml::from_str(plan).unwrap();
    let signer = signer();
    let trusted = [signer.verifying_key()];
    clean(
        &Inputs {
            spec: &spec,
            plan_bytes: plan.as_bytes(),
            run_id: RUN,
            attempt: 1,
            trusted: &trusted,
        },
        cluster,
        cluster,
        store,
        &signer,
    )
}

fn refusal(r: Result<Cleaned, logweir::drill::DrillError>) -> String {
    match r {
        Err(logweir::drill::DrillError::Guard(g)) => g.0,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// **C40/C41's unit half: the cleanup deletes exactly the recorded topics
/// whose ID is unchanged.** A recreated topic is left `CloneTargetReplaced`;
/// one whose ID was never recorded is left `CloneTargetIdentityUnknown`; a
/// missing one is `alreadyAbsent`; a topic with the clone's prefix that the
/// run did not create is never named. The attestation is signed
/// `teardown_policy: ttl` at `.cleanup.json`.
///
/// KILLS (mutants on `cleanup_plan`): deleting on the name alone (the
/// replaced topic is asked), deleting a topic whose ID was never recorded,
/// deleting what the record does not name.
#[test]
fn the_cleanup_deletes_only_recorded_topics_whose_id_is_unchanged() {
    let plan = plan_text("clone-", "{ttl_seconds: 300}");
    let store = store_with(
        &plan,
        &[
            ("clone-orders", Some(A)),
            ("clone-payments", Some(A)),
            ("clone-refunds", Some(A)),
            ("clone-audit", None),
        ],
        |d| d["target"]["topic_mapping_prefix"] = "clone-".into(),
    );
    let cluster = Cluster::new(&[
        ("clone-orders", TopicIdRead::Id(A.into())),
        ("clone-payments", TopicIdRead::Id(B.into())),
        ("clone-refunds", TopicIdRead::NotFound),
        ("clone-audit", TopicIdRead::Id(A.into())),
        ("clone-stranger", TopicIdRead::Id(A.into())),
    ]);
    let cleaned = run(&plan, &store, &cluster).expect("the cleanup runs");
    assert_eq!(cluster.asked(), ["clone-orders"]);
    assert!(!cleaned.retry, "the ID rule's refusals are final");
    let r = &cleaned.report;
    assert_eq!(r.deleted, ["clone-orders"]);
    assert_eq!(r.already_absent, ["clone-refunds"]);
    let left: Vec<(&str, &str)> = r
        .left
        .iter()
        .map(|l| (l.topic.as_str(), l.reason.split(':').next().unwrap()))
        .collect();
    assert_eq!(
        left,
        [
            ("clone-payments", "CloneTargetReplaced"),
            ("clone-audit", "CloneTargetIdentityUnknown"),
        ]
    );
    let key = r.attestation_key.clone().expect("attested");
    assert_eq!(key, format!("logweir/drills/{RUN}.cleanup.json"));
    let (bytes, _) = store.get_capped(&key, caps::SIGNED_DOCUMENT).unwrap();
    let att: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(att["teardown_policy"], "ttl");
    assert_eq!(att["topics_deleted"], serde_json::json!(["clone-orders"]));
    assert_eq!(att["run_id"], RUN);
    assert!(store
        .get_capped(&format!("logweir/drills/{RUN}.cleanup.sig"), caps::SIDECAR)
        .is_ok());
    // The line the controller reads.
    let mut out = Vec::new();
    let code = finish(RUN, Ok(cleaned), &mut out);
    assert_eq!(code as u8, 0);
    let text = String::from_utf8(out).unwrap();
    let value = text
        .trim()
        .strip_prefix("clone-cleanup=")
        .expect("the line");
    let parsed = logweir_core::target_creation::CloneCleanupReport::parse(value).unwrap();
    assert_eq!(parsed.deleted, ["clone-orders"]);
}

/// **C42's unit half: a repointed cluster deletes nothing** — the cluster's
/// ID is not the scorecard's, so the cleanup refuses
/// `CloneTargetClusterChanged` before any ID is read or any topic named.
/// NEGATIVE CONTROL: the recorded cluster proceeds (the row above).
#[test]
fn a_repointed_cluster_deletes_nothing() {
    let plan = plan_text("clone-", "{ttl_seconds: 300}");
    let store = store_with(&plan, &[("clone-orders", Some(A))], |d| {
        d["target"]["topic_mapping_prefix"] = "clone-".into();
    });
    let mut cluster = Cluster::new(&[("clone-orders", TopicIdRead::Id(A.into()))]);
    cluster.id = "AnotherClusterIdAAAAAA".into();
    let why = refusal(run(&plan, &store, &cluster));
    assert!(why.starts_with("CloneTargetClusterChanged: "), "{why}");
    assert!(cluster.asked().is_empty());
}

/// The evidence as the PoC's write grant saw it (theme A's D1): the store
/// refuses every read with a 403; writes go to the store it wraps.
struct Denying {
    write: Store,
}

impl EvidenceStores for Denying {
    fn read(&self, key: &str, _max_bytes: u64) -> Result<Vec<u8>, StoreError> {
        Err(StoreError::Io(format!(
            "{key}: Error performing GET http://minio:9000/e/{key} in 2ms - Server returned \
             non-2xx status code: 403 Forbidden: <Error><Code>AccessDenied</Code></Error>"
        )))
    }
    fn writer(&self) -> &Store {
        &self.write
    }
    fn grant(&self) -> &str {
        "evidenceRead of BackupDestination team-a/poc"
    }
}

/// **D1: a scorecard the store will not hand over is an operational failure
/// that names the grant, never a refusal, never a deletion.** On the PoC the
/// cleanup read with the write grant, was refused 403, exited 1, and the
/// clone's status said only that. Now the failure names the key, the read
/// grant and the store's class, the run exits 1 (a later attempt may read),
/// and the `clone-cleanup=` line carries the sentence for the clone's status.
/// CONTROL: the same scorecard, readable, is cleaned.
///
/// KILLS: the cleanup's read error without `evidence_read::unreadable` (no
/// grant named); the report line without `error`.
#[test]
fn a_scorecard_the_store_refuses_is_a_named_operational_failure() {
    let plan = plan_text("clone-", "{ttl_seconds: 300}");
    let created = [("clone-orders", Some(A))];
    let prefix = |d: &mut serde_json::Value| d["target"]["topic_mapping_prefix"] = "clone-".into();
    let denying = Denying {
        write: store_with(&plan, &created, prefix),
    };
    let cluster = Cluster::new(&[("clone-orders", TopicIdRead::Id(A.into()))]);
    let spec: DrillSpec = serde_yaml::from_str(&plan).unwrap();
    let signer = signer();
    let trusted = [signer.verifying_key()];
    let inputs = Inputs {
        spec: &spec,
        plan_bytes: plan.as_bytes(),
        run_id: RUN,
        attempt: 1,
        trusted: &trusted,
    };
    let denied = clean(&inputs, &cluster, &cluster, &denying, &signer);
    match &denied {
        Err(logweir::drill::DrillError::Operational(why)) => {
            assert!(
                why.starts_with(&format!(
                    "EvidenceUnreadable: could not read logweir/drills/{RUN}.json with grant \
                     evidenceRead of BackupDestination team-a/poc: AccessDenied"
                )),
                "{why}"
            );
            assert!(!why.contains("minio:9000"), "no backend text: {why}");
        }
        other => panic!("expected an operational failure, got {other:?}"),
    }
    assert!(cluster.asked().is_empty(), "nothing deleted");
    let mut out = Vec::new();
    assert_eq!(
        finish(RUN, denied, &mut out) as u8,
        1,
        "retried, not refused"
    );
    let text = String::from_utf8(out).unwrap();
    let report = text
        .lines()
        .find_map(|l| l.strip_prefix("clone-cleanup="))
        .and_then(logweir_core::target_creation::CloneCleanupReport::parse)
        .expect("the line");
    let error = report.error.expect("the error is on the line");
    assert!(error.starts_with("EvidenceUnreadable: "), "{error}");
    assert!(report.refused.is_none() && report.deleted.is_empty());

    // CONTROL: readable, the same record cleans.
    let cleaned = run(&plan, &store_with(&plan, &created, prefix), &cluster).expect("cleans");
    assert_eq!(cleaned.report.deleted, ["clone-orders"]);
}

/// **Nothing is deleted without the run's own verified record**: a scorecard
/// of another plan, one signed by another key, a missing one, and a plan
/// that is not a clone are each refused, and the deleter is never asked.
#[test]
fn without_the_runs_verified_record_nothing_is_deleted() {
    let plan = plan_text("clone-", "{ttl_seconds: 300}");
    let cluster = Cluster::new(&[("clone-orders", TopicIdRead::Id(A.into()))]);
    let prefixed = |d: &mut serde_json::Value| {
        d["target"]["topic_mapping_prefix"] = "clone-".into();
    };

    // Another plan's run.
    let other = plan_text("clone-", "{ttl_seconds: 600}");
    let store = store_with(&other, &[("clone-orders", Some(A))], prefixed);
    assert!(refusal(run(&plan, &store, &cluster)).contains("another plan's run"));

    // Signed by a stranger.
    let store = store_with(&plan, &[("clone-orders", Some(A))], prefixed);
    let stranger = logweir_evidence::keys::SigningKey::generate_p256();
    let (bytes, _) = store
        .get_capped(&format!("logweir/drills/{RUN}.json"), caps::SIGNED_DOCUMENT)
        .unwrap();
    let forged = logweir_evidence::sign::sign_detached(
        &stranger,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        &bytes,
    )
    .unwrap();
    let forged_store = Store::in_memory("logweir");
    forged_store
        .put_create_only(&format!("logweir/drills/{RUN}.json"), &bytes)
        .unwrap();
    forged_store
        .put_create_only(
            &format!("logweir/drills/{RUN}.sig"),
            &serde_json::to_vec(&forged).unwrap(),
        )
        .unwrap();
    assert!(refusal(run(&plan, &forged_store, &cluster)).contains("does not verify"));

    // No record at all.
    assert!(refusal(run(&plan, &Store::in_memory("logweir"), &cluster))
        .contains("is not in the evidence store"));

    // Not a clone plan.
    let plain = plan_text("clone-", "delete");
    assert!(refusal(run(&plain, &store, &cluster)).contains("not one"));

    assert!(cluster.asked().is_empty(), "{:?}", cluster.asked());
}

/// A delete the broker refused, or an ID that cannot be read now, is left
/// for a later attempt: exit 1, and attempt 2 attests under its own key.
#[test]
fn a_failed_delete_is_retried_under_its_own_attestation() {
    let plan = plan_text("clone-", "{ttl_seconds: 300}");
    let store = store_with(
        &plan,
        &[("clone-orders", Some(A)), ("clone-payments", Some(A))],
        |d| d["target"]["topic_mapping_prefix"] = "clone-".into(),
    );
    let mut cluster = Cluster::new(&[
        ("clone-orders", TopicIdRead::Id(A.into())),
        ("clone-payments", TopicIdRead::Failed("timed out".into())),
    ]);
    cluster.refuse = vec!["clone-orders".into()];
    let cleaned = run(&plan, &store, &cluster).unwrap();
    assert!(cleaned.retry);
    assert_eq!(cleaned.report.deleted, Vec::<String>::new());
    let mut out = Vec::new();
    assert_eq!(finish(RUN, Ok(cleaned), &mut out) as u8, 1);
    // Attempt 2 writes its own key.
    let spec: DrillSpec = serde_yaml::from_str(&plan).unwrap();
    let signer = signer();
    let trusted = [signer.verifying_key()];
    cluster.refuse.clear();
    let again = clean(
        &Inputs {
            spec: &spec,
            plan_bytes: plan.as_bytes(),
            run_id: RUN,
            attempt: 2,
            trusted: &trusted,
        },
        &cluster,
        &cluster,
        &store,
        &signer,
    )
    .unwrap();
    assert_eq!(
        again.report.attestation_key.as_deref(),
        Some(format!("logweir/drills/{RUN}.cleanup-2.json").as_str())
    );
    assert_eq!(again.report.deleted, ["clone-orders"]);
}

/// A refusal prints the `clone-cleanup=` line with the reason, then the
/// standard refusal lines, last; exit 3.
#[test]
fn a_refusal_prints_its_reason_on_the_line_and_exits_3() {
    let mut out = Vec::new();
    let code = finish(
        RUN,
        Err(logweir_core::guard::GuardRefusal("CloneTargetClusterChanged: x".into()).into()),
        &mut out,
    );
    assert_eq!(code as u8, 3);
    let text = String::from_utf8(out).unwrap();
    let mut lines = text.lines();
    let first = lines.next().unwrap();
    assert!(first.starts_with("clone-cleanup="), "{text}");
    assert!(first.contains("CloneTargetClusterChanged: x"));
    assert!(
        text.trim_end().ends_with("refusal-reason=GuardRefused"),
        "{text}"
    );
}

/// **Review LOW-1: an ID that cannot be read now is retried even when every
/// delete succeeded.** One topic's ID read fails, the other is deleted: the
/// unreadable topic is not asked for, `retry` is set and the exit is 1, so
/// the clone is never `Removed` with a topic the run created left behind.
/// KILLS dropping the `unreadable` term from `retry`. CONTROL: the first row,
/// where every ID reads and the cleanup exits 0.
#[test]
fn an_unreadable_id_alone_is_retried() {
    let plan = plan_text("clone-", "{ttl_seconds: 300}");
    let store = store_with(
        &plan,
        &[("clone-orders", Some(A)), ("clone-payments", Some(A))],
        |d| d["target"]["topic_mapping_prefix"] = "clone-".into(),
    );
    let cluster = Cluster::new(&[
        ("clone-orders", TopicIdRead::Id(A.into())),
        ("clone-payments", TopicIdRead::Failed("timed out".into())),
    ]);
    let cleaned = run(&plan, &store, &cluster).unwrap();
    assert_eq!(cluster.asked(), ["clone-orders"]);
    assert_eq!(cleaned.report.deleted, ["clone-orders"]);
    assert!(cleaned.retry, "an unreadable ID must be retried");
    assert!(cleaned.report.left[0]
        .reason
        .starts_with("CloneTargetIdentityUnknown: "));
    let mut out = Vec::new();
    assert_eq!(finish(RUN, Ok(cleaned), &mut out) as u8, 1);
}
