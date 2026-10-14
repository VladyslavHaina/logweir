#![cfg(feature = "e2e")]
//! **PROD-11.1a: safe clones, live** (design
//! `docs/to-do/decisions/PROD-04.2-cutover.md` §5 and §8.1, rows C40 to C45).
//!
//! | Row | Pass predicate | Negative control |
//! |---|---|---|
//! | `c40_…` | a TTL clone (`ttl_seconds: 300`) passes and phase 9 KEEPS its topics; `logweir restore cleanup` deletes exactly `target.creation.topics`, signs `logweir/drills/<run>.cleanup.json` | a topic with the clone's prefix that the run did not create survives |
//! | `c41_…` | a clone topic deleted and created again before the cleanup is left `CloneTargetReplaced`; the other is deleted | the unchanged topic is deleted |
//! | `c42_…` (ignored; the `cluster2` profile) | a clone to a cluster outside `allowed_cluster_ids`, or to the source, is refused before anything is created (exit 3); a cleanup whose plan address now answers as ANOTHER cluster refuses `CloneTargetClusterChanged` and deletes nothing | pointed back at the recorded cluster, the cleanup deletes |
//! | `c43_…` | `target.creation` (names, IDs equal to the broker's, `lineage_headers: kept`) is in the scorecard of every run that created topics — a clone (with its lifetime) and a plain restore (without) — both readers accept it, and a cloned record carries `x-original-offset` and `x-original-timestamp` | — |
//! | `c44_…` (ignored; `LOGWEIR_E2E_OLDER_RUNNER`) | main's runner from before PROD-11.1a refuses the map form: exit 1, `drill spec does not parse`, nothing created | the same runner restores the plan with `teardown: keep` |
//! | `c45_…` | a TTL clone whose run does not pass (an objective it cannot meet) is torn down at once: its topics are gone and phase 9 attests `teardown_policy: ttl` | — |
//! | `d1_…` | theme A's D1 on the PoC's least-privilege MinIO policies: the cleanup reads the scorecard with the read grant and writes with the write grant, deletes exactly the created topics and attests them | the write grant in the read slot: exit 1, `EvidenceUnreadable` naming the grant and `AccessDenied` on the line, nothing deleted |
//!
//! # Running it
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --kafka 4.3 --profiles cluster2)"
//! just e2e-up && ./scripts/e2e-seed.sh
//! cargo build -p logweir
//! LOGWEIR_PYTHON=<python3 with cryptography> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test clone_ttl -- --include-ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! Each row writes what it observed to `clone-ttl/<row>.json` under the
//! stack's scratch directory (`harness::demo_dir()`).
mod harness;

use harness::*;
use logweir_core::target_creation::CloneCleanupReport;
use logweir_kafka::reader::{AuthConfig, ClusterReader};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CLONE_TOPICS: [&str; 2] = ["drill-orders", "drill-payments"];

/// `examples/drill.yaml` as the suite binds it, as a TTL clone of `ttl`
/// seconds.
fn clone_spec(ttl: u64) -> serde_yaml::Value {
    let mut s = spec_default();
    s["target"]["teardown"] = serde_yaml::from_str(&format!("{{ttl_seconds: {ttl}}}")).unwrap();
    s
}

/// `logweir restore run` over `s`, with the harness's approval and allowlist.
fn restore(s: &serde_yaml::Value) -> Run {
    let mut o = RunOpts::new(s);
    o.restore_run = true;
    run_with(o)
}

fn summary(r: &Run) -> String {
    format!(
        "exit {:?}\nstdout:\n{}\nstderr:\n{}",
        r.out.status.code(),
        r.out.stdout_utf8(),
        r.out.stderr_utf8()
    )
}

/// `logweir restore cleanup` over the SAME plan bytes the run was approved
/// for (`run_with` writes `serde_yaml::to_string(spec)`, as here).
fn cleanup(s: &serde_yaml::Value, run_id: &str, attempt: u32) -> (Output, CloneCleanupReport) {
    let path = demo_dir().join(format!("clone-cleanup-{}.yaml", std::process::id()));
    std::fs::write(&path, serde_yaml::to_string(s).unwrap()).unwrap();
    let out = Command::new(bin())
        .args(["restore", "cleanup", "--spec"])
        .arg(&path)
        .args(["--run-id", run_id, "--attempt", &attempt.to_string()])
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .output()
        .expect("logweir runs");
    let report = out
        .stdout_utf8()
        .lines()
        .find_map(|l| l.strip_prefix("clone-cleanup="))
        .and_then(CloneCleanupReport::parse)
        .unwrap_or_else(|| {
            panic!(
                "no clone-cleanup= line:\nstdout:\n{}\nstderr:\n{}",
                out.stdout_utf8(),
                out.stderr_utf8()
            )
        });
    (out, report)
}

fn creation(sc: &Value) -> &Value {
    let c = &sc["target"]["creation"];
    assert!(c.is_object(), "no target.creation: {sc:#}");
    c
}

fn created_names(sc: &Value) -> Vec<String> {
    let mut names: Vec<String> = creation(sc)["topics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    names
}

fn sorted(v: &[String]) -> Vec<String> {
    let mut v = v.to_vec();
    v.sort();
    v
}

/// Wait, bounded, until none of `topics` is listed.
fn wait_gone(topics: &[String]) {
    for _ in 0..60 {
        if topics.iter().all(|t| !topic_exists(t)) {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("{topics:?} still listed 15 s after the cleanup");
}

/// Delete one topic by its exact name and wait until it is gone.
fn delete_one(topic: &str) {
    let o = kafka_topics(&[
        "--bootstrap-server",
        "kafka-broker-1:9094",
        "--delete",
        "--topic",
        topic,
    ]);
    assert!(o.status.success(), "delete {topic}: {}", o.stderr_utf8());
    wait_gone(&[topic.to_string()]);
}

fn write_outcome(row: &str, value: &Value) {
    let dir = demo_dir().join("clone-ttl");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(format!("{row}.json")),
        serde_json::to_vec_pretty(value).unwrap(),
    )
    .unwrap();
}

/// **C40: a TTL clone is kept by phase 9, and its cleanup deletes exactly
/// what the run created.** NEGATIVE CONTROL: `drill-stranger`, a topic with
/// the clone's prefix that the run did not create, survives the cleanup.
#[test]
fn c40_a_ttl_clone_is_kept_and_its_cleanup_deletes_exactly_what_it_created() {
    let s = clone_spec(300);
    let r = restore(&s);
    assert_eq!(r.out.status.code(), Some(0), "{}", summary(&r));
    let sc = read_scorecard(&r);
    let run_id = r.run_id();
    let created = created_names(&sc);
    assert_eq!(created, CLONE_TOPICS.map(String::from));
    assert_eq!(creation(&sc)["teardown_after_seconds"], 300);
    for t in &created {
        assert!(topic_exists(t), "phase 9 kept {t}");
    }
    let stranger = "drill-stranger";
    create_topic(stranger, 1);
    await_created(stranger, 1);

    let (out, report) = cleanup(&s, &run_id, 1);
    assert_eq!(out.status.code(), Some(0), "{}", out.stderr_utf8());
    assert_eq!(sorted(&report.deleted), created);
    assert!(report.left.is_empty() && report.already_absent.is_empty());
    wait_gone(&created);
    assert!(
        topic_exists(stranger),
        "the cleanup deleted a topic the run did not create"
    );
    let key = report.attestation_key.clone().expect("attested");
    assert_eq!(key, format!("logweir/drills/{run_id}.cleanup.json"));
    let evidence = evidence_for_run(&run_id);
    assert!(
        evidence.iter().any(|k| k.ends_with(&key))
            && evidence
                .iter()
                .any(|k| k.ends_with(&format!("{run_id}.cleanup.sig"))),
        "{evidence:?}"
    );
    write_outcome(
        "c40",
        &json!({"run_id": run_id, "created": created, "report": serde_json::to_value(&report).unwrap(),
                "stranger_survives": topic_exists(stranger), "evidence": evidence}),
    );
    delete_all_drill_topics();
}

/// `logweir restore cleanup` as the controller's cleanup Job runs it (theme
/// A's D1): under the store contract, `read` in the archive slot (the run's
/// scorecard is read with it) and `write` in the evidence slot (the
/// attestation is written with it), the read grant named.
fn cleanup_with_grants(
    s: &serde_yaml::Value,
    run_id: &str,
    attempt: u32,
    read: &MinioUser,
    write: &MinioUser,
    grant: &str,
) -> (Output, CloneCleanupReport) {
    let path = demo_dir().join(format!("clone-cleanup-d1-{}.yaml", std::process::id()));
    std::fs::write(&path, serde_yaml::to_string(s).unwrap()).unwrap();
    let out = Command::new(bin())
        .args(["restore", "cleanup", "--spec"])
        .arg(&path)
        .args(["--run-id", run_id, "--attempt", &attempt.to_string()])
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .args([
            "--store-contract-version",
            "1",
            "--evidence-read-grant",
            grant,
        ])
        .env("LOGWEIR_STORE_CONTRACT_VERSION", "1")
        .env("LOGWEIR_ARCHIVE_CREDENTIALS", "static")
        .env("AWS_ACCESS_KEY_ID", &read.access_key_id)
        .env("AWS_SECRET_ACCESS_KEY", &read.secret_access_key)
        .env("AWS_REGION", "us-east-1")
        .env("AWS_ALLOW_HTTP", "true")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("LOGWEIR_EVIDENCE_CREDENTIALS", "static")
        .env("LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID", &write.access_key_id)
        .env(
            "LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY",
            &write.secret_access_key,
        )
        .output()
        .expect("logweir runs");
    let report = out
        .stdout_utf8()
        .lines()
        .find_map(|l| l.strip_prefix("clone-cleanup="))
        .and_then(CloneCleanupReport::parse)
        .unwrap_or_else(|| {
            panic!(
                "no clone-cleanup= line:\nstdout:\n{}\nstderr:\n{}",
                out.stdout_utf8(),
                out.stderr_utf8()
            )
        });
    (out, report)
}

/// **Theme A's D1, live: a clone's cleanup reads its scorecard with the READ
/// grant and writes its attestation with the WRITE grant, on the PoC's
/// least-privilege MinIO policies** (`harness::least_privilege_evidence`).
/// It deletes exactly the topics the run created and attests them.
///
/// NEGATIVE CONTROL, first: the write grant in the read slot too (what
/// `sha-4db6e5bb`'s cleanup Job read with). The attempt exits 1, its
/// `clone-cleanup=` line names `EvidenceUnreadable` with the key, the grant
/// and `AccessDenied`, nothing is deleted, and no attestation is written.
#[test]
fn d1_a_clone_cleanup_reads_with_the_read_grant_on_least_privilege() {
    let (writer, reader) = harness::least_privilege_evidence();
    let s = clone_spec(300);
    let r = restore(&s);
    assert_eq!(r.out.status.code(), Some(0), "{}", summary(&r));
    let sc = read_scorecard(&r);
    let run_id = r.run_id();
    let created = created_names(&sc);
    for t in &created {
        assert!(topic_exists(t), "phase 9 kept {t}");
    }

    // NEGATIVE CONTROL: the write grant reads.
    let (old, old_report) = cleanup_with_grants(
        &s,
        &run_id,
        1,
        &writer,
        &writer,
        "evidenceWrite of BackupDestination e2e/least-privilege",
    );
    assert_eq!(old.status.code(), Some(1), "{}", old.stderr_utf8());
    let error = old_report.error.clone().expect("the cause is on the line");
    assert!(
        error.starts_with(&format!(
            "EvidenceUnreadable: could not read logweir/drills/{run_id}.json with grant \
             evidenceWrite of BackupDestination e2e/least-privilege: AccessDenied"
        )),
        "{error}"
    );
    assert!(old_report.deleted.is_empty() && old_report.attestation_key.is_none());
    for t in &created {
        assert!(topic_exists(t), "the control deleted {t}");
    }

    // THE ROW: the read grant reads, the write grant writes.
    let (out, report) = cleanup_with_grants(
        &s,
        &run_id,
        2,
        &reader,
        &writer,
        "evidenceRead of BackupDestination e2e/least-privilege",
    );
    assert_eq!(out.status.code(), Some(0), "{}", out.stderr_utf8());
    assert_eq!(sorted(&report.deleted), created);
    wait_gone(&created);
    let key = report.attestation_key.clone().expect("attested");
    assert_eq!(key, format!("logweir/drills/{run_id}.cleanup-2.json"));
    let evidence = evidence_for_run(&run_id);
    assert!(evidence.iter().any(|k| k.ends_with(&key)), "{evidence:?}");
    write_outcome(
        "d1_least_privilege",
        &json!({"run_id": run_id, "created": created,
                "control_exit": old.status.code(), "control_error": error,
                "report": serde_json::to_value(&report).unwrap(), "evidence": evidence}),
    );
    delete_all_drill_topics();
}

/// **C41: a clone topic recreated before the cleanup is left
/// `CloneTargetReplaced`; the unchanged one is deleted (TI-11.1-4).**
#[test]
fn c41_a_recreated_clone_topic_is_left_and_the_others_are_deleted() {
    let s = clone_spec(300);
    let r = restore(&s);
    assert_eq!(r.out.status.code(), Some(0), "{}", summary(&r));
    let run_id = r.run_id();
    let partitions = count_partitions("drill-payments");
    delete_one("drill-payments");
    create_topic("drill-payments", partitions);
    await_created("drill-payments", partitions);

    let (out, report) = cleanup(&s, &run_id, 1);
    assert_eq!(out.status.code(), Some(0), "{}", out.stderr_utf8());
    assert_eq!(report.deleted, ["drill-orders"]);
    assert_eq!(report.left.len(), 1, "{report:?}");
    assert_eq!(report.left[0].topic, "drill-payments");
    assert!(
        report.left[0].reason.starts_with("CloneTargetReplaced: "),
        "{}",
        report.left[0].reason
    );
    wait_gone(&["drill-orders".to_string()]);
    assert!(
        topic_exists("drill-payments"),
        "the recreated topic was deleted"
    );
    write_outcome(
        "c41",
        &json!({"run_id": run_id, "report": serde_json::to_value(&report).unwrap(),
                "recreated_survives": topic_exists("drill-payments")}),
    );
    delete_all_drill_topics();
}

/// A TCP forwarder on `127.0.0.1:<ephemeral>` whose upstream can be moved:
/// the plan names its address, so the SAME approved plan bytes can be made to
/// answer as another cluster (a repointed `KafkaCluster`: same address,
/// another cluster behind it). Bounded: at most 256 connections; stopped and
/// joined on drop.
struct Forwarder {
    port: u16,
    upstream: Arc<Mutex<String>>,
    stop: Arc<AtomicBool>,
    accept: Option<std::thread::JoinHandle<()>>,
}

impl Forwarder {
    fn start(upstream: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("an ephemeral port");
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let upstream = Arc::new(Mutex::new(upstream.to_string()));
        let stop = Arc::new(AtomicBool::new(false));
        let (up, halt) = (Arc::clone(&upstream), Arc::clone(&stop));
        let accept = std::thread::spawn(move || {
            let mut served = 0;
            while !halt.load(Ordering::SeqCst) && served < 256 {
                match listener.accept() {
                    Ok((client, _)) => {
                        served += 1;
                        let target = up.lock().unwrap().clone();
                        if let Ok(server) = TcpStream::connect(&target) {
                            let _ = client.set_nonblocking(false);
                            pump(client, server);
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            port,
            upstream,
            stop,
            accept: Some(accept),
        }
    }

    fn point_at(&self, upstream: &str) {
        *self.upstream.lock().unwrap() = upstream.to_string();
    }
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.accept.take() {
            let _ = h.join();
        }
    }
}

/// Copy both directions until either side closes.
fn pump(a: TcpStream, b: TcpStream) {
    let (mut a_read, mut b_write) = (a.try_clone().unwrap(), b.try_clone().unwrap());
    let (mut b_read, mut a_write) = (b, a);
    std::thread::spawn(move || {
        let mut buf = [0u8; 16 * 1024];
        while let Ok(n) = a_read.read(&mut buf) {
            if n == 0 || b_write.write_all(&buf[..n]).is_err() {
                break;
            }
        }
        let _ = b_write.shutdown(Shutdown::Write);
    });
    std::thread::spawn(move || {
        let mut buf = [0u8; 16 * 1024];
        while let Ok(n) = b_read.read(&mut buf) {
            if n == 0 || a_write.write_all(&buf[..n]).is_err() {
                break;
            }
        }
        let _ = a_write.shutdown(Shutdown::Write);
    });
}

fn cluster2_id() -> String {
    logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        &[bootstrap_cluster2()],
        AuthConfig::Plaintext,
    )
    .expect("a cluster2 reader")
    .cluster_id()
    .expect("cluster2's id")
}

/// **C42: `AllowedClusters` is scratch mode's own check, and a repointed
/// cluster deletes nothing.** A clone to a cluster outside
/// `allowed_cluster_ids`, or to the source, is refused before anything is
/// created. A clone restored through an address that answers as `cluster2`
/// at cleanup time refuses `CloneTargetClusterChanged`. NEGATIVE CONTROL: the
/// same address pointed back at the recorded cluster, the cleanup deletes.
#[test]
#[ignore = "needs the cluster2 profile (`stack-env.sh --profiles cluster2`), which CI's e2e job does not start"]
fn c42_allowed_clusters_and_a_repointed_cluster_delete_nothing() {
    let here = cluster_id();
    let mut refusals = Vec::new();
    for (case, allowed) in [
        (
            "not-allowed",
            json!({"allowed_cluster_ids": ["AnotherClusterIdAAAAAAA"], "source_cluster_id": null}),
        ),
        (
            "the-source",
            json!({"allowed_cluster_ids": [here], "source_cluster_id": here}),
        ),
    ] {
        let path = demo_dir().join(format!("allowed-{case}.json"));
        std::fs::write(&path, serde_json::to_vec(&allowed).unwrap()).unwrap();
        let s = clone_spec(300);
        let mut o = RunOpts::new(&s);
        o.restore_run = true;
        o.allowlist = Some(&path);
        let r = run_with(o);
        assert_eq!(r.out.status.code(), Some(3), "{case}: {}", summary(&r));
        for t in CLONE_TOPICS {
            assert!(!topic_exists(t), "{case}: {t} was created");
        }
        refusals.push(json!({"case": case, "exit": r.out.status.code(),
                             "last_line": r.out.stdout_utf8().lines().last()}));
    }

    let forwarder = Forwarder::start(&bootstrap());
    let mut s = clone_spec(300);
    s["target"]["bootstrap_servers"] =
        serde_yaml::from_str(&format!("[\"localhost:{}\"]", forwarder.port)).unwrap();
    let r = restore(&s);
    assert_eq!(r.out.status.code(), Some(0), "{}", summary(&r));
    let run_id = r.run_id();
    assert_eq!(read_scorecard(&r)["target"]["cluster_id"], here.as_str());

    forwarder.point_at(&bootstrap_cluster2());
    let (out, refused) = cleanup(&s, &run_id, 1);
    assert_eq!(out.status.code(), Some(3), "{}", out.stderr_utf8());
    let why = refused.refused.clone().expect("a refusal");
    assert!(why.starts_with("CloneTargetClusterChanged: "), "{why}");
    assert!(why.contains(&cluster2_id()) && why.contains(&here), "{why}");
    for t in CLONE_TOPICS {
        assert!(
            topic_exists(t),
            "{t} was deleted through a repointed cluster"
        );
    }

    // NEGATIVE CONTROL.
    forwarder.point_at(&bootstrap());
    let (out, report) = cleanup(&s, &run_id, 2);
    assert_eq!(out.status.code(), Some(0), "{}", out.stderr_utf8());
    assert_eq!(sorted(&report.deleted), CLONE_TOPICS.map(String::from));
    assert_eq!(
        report.attestation_key.as_deref(),
        Some(format!("logweir/drills/{run_id}.cleanup-2.json").as_str())
    );
    write_outcome(
        "c42",
        &json!({"refusals": refusals, "run_id": run_id, "repointed": serde_json::to_value(&refused).unwrap(),
                "control": serde_json::to_value(&report).unwrap()}),
    );
    drop(forwarder);
    delete_all_drill_topics();
}

/// **C43: every run that created topics signs `target.creation`**: the
/// names, each topic's ID equal to the broker's, `lineage_headers: kept`, and
/// a clone's lifetime (absent on a plain restore); both readers accept the
/// documents; a cloned record carries both lineage headers.
#[test]
fn c43_every_creating_run_signs_what_it_created_and_records_keep_their_lineage() {
    let mut seen = Vec::new();
    for (case, s) in [("clone", clone_spec(300)), ("plain", spec_default())] {
        let r = restore(&s);
        assert_eq!(r.out.status.code(), Some(0), "{case}: {}", summary(&r));
        let sc = read_scorecard(&r);
        assert_format_at_least(sc["format_version"].as_str().unwrap(), "1.10.0", case);
        let c = creation(&sc);
        assert_eq!(c["lineage_headers"], "kept");
        if case == "clone" {
            assert_eq!(c["teardown_after_seconds"], 300);
        } else {
            assert!(c.get("teardown_after_seconds").is_none(), "{c}");
        }
        let names = created_names(&sc);
        assert_eq!(names, CLONE_TOPICS.map(String::from));
        let ids: std::collections::BTreeMap<String, String> = reader()
            .topic_ids(&names)
            .unwrap()
            .into_iter()
            .filter_map(|(n, read)| match read {
                logweir_kafka::topic_ids::TopicIdRead::Id(id) => Some((n, id)),
                _ => None,
            })
            .collect();
        for t in c["topics"].as_array().unwrap() {
            let name = t["name"].as_str().unwrap();
            assert_eq!(
                t["topic_id"].as_str(),
                ids.get(name).map(String::as_str),
                "{case}: {name}'s recorded ID is the broker's"
            );
        }
        assert!(logweir_verify(&r).success(), "{case}: drill verify");
        assert!(python_verify(&r).success(), "{case}: verify_scorecard.py");
        let records = reader()
            .consume_range("drill-orders", 0, 0, 1)
            .expect("a restored record");
        let headers: Vec<&str> = records[0].headers.iter().map(|(k, _)| k.as_str()).collect();
        assert!(
            headers.contains(&"x-original-offset") && headers.contains(&"x-original-timestamp"),
            "{case}: {headers:?}"
        );
        seen.push(
            json!({"case": case, "format_version": sc["format_version"], "creation": c,
                         "headers": headers}),
        );
    }
    write_outcome("c43", &json!(seen));
    delete_all_drill_topics();
}

/// **C44: main's runner from before PROD-11.1a refuses the clone form**,
/// because it types `target.teardown` as a string: exit 1, `drill spec does
/// not parse`, nothing created, nothing signed. NEGATIVE CONTROL: the same
/// runner restores the same plan with `teardown: keep`.
///
/// IGNORED by default: the older binary is built outside this tree; an unset
/// variable is a PANIC here, never a silent pass.
#[test]
#[ignore = "needs LOGWEIR_E2E_OLDER_RUNNER: a logweir built from main before PROD-11.1a"]
fn c44_an_older_runner_refuses_the_clone_form() {
    let older = std::path::PathBuf::from(
        std::env::var("LOGWEIR_E2E_OLDER_RUNNER")
            .expect("LOGWEIR_E2E_OLDER_RUNNER names the older logweir binary"),
    );
    assert!(older.is_file(), "{} is not a file", older.display());
    let s = clone_spec(300);
    let mut o = RunOpts::new(&s);
    o.restore_run = true;
    o.bin = Some(older.clone());
    let r = run_with(o);
    assert_eq!(r.out.status.code(), Some(1), "{}", summary(&r));
    let both = format!("{}{}", r.out.stdout_utf8(), r.out.stderr_utf8());
    assert!(both.contains("drill spec does not parse"), "{both}");
    assert!(!r.scorecard.exists(), "the older runner signed a scorecard");
    for t in CLONE_TOPICS {
        assert!(!topic_exists(t), "the older runner created {t}");
    }
    // NEGATIVE CONTROL.
    let keep = spec_default();
    let mut o = RunOpts::new(&keep);
    o.restore_run = true;
    o.bin = Some(older);
    let control = run_with(o);
    assert_eq!(control.out.status.code(), Some(0), "{}", summary(&control));
    write_outcome(
        "c44",
        &json!({"refused_exit": r.out.status.code(), "control_exit": control.out.status.code()}),
    );
    delete_all_drill_topics();
}

/// **C45: a TTL clone whose run did not pass is torn down at once**: an
/// objective it cannot meet (an RTO of one second) scores `fail-objective`,
/// exit 2, and phase 9 deletes what the run created, attesting
/// `teardown_policy: ttl`.
#[test]
fn c45_a_failed_ttl_clone_is_torn_down_at_once() {
    let mut s = clone_spec(300);
    s["objectives"]["rto_seconds"] = 1.into();
    s["objectives"]["rpo_seconds"] = 0.into();
    let r = restore(&s);
    assert_eq!(r.out.status.code(), Some(2), "{}", summary(&r));
    let sc = read_scorecard(&r);
    assert_eq!(sc["outcome"], "fail-objective");
    let created = created_names(&sc);
    assert_eq!(creation(&sc)["teardown_after_seconds"], 300);
    wait_gone(&created);
    let run_id = r.run_id();
    let att = mc(&[
        "cat",
        &format!("local/{EVIDENCE_BUCKET}/logweir/drills/{run_id}.teardown.json"),
    ]);
    let att: Value = serde_json::from_slice(&att.stdout)
        .unwrap_or_else(|e| panic!("the teardown attestation: {e}: {}", att.stderr_utf8()));
    assert_eq!(att["teardown_policy"], "ttl");
    let mut deleted: Vec<String> = att["topics_deleted"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t.as_str().unwrap().to_string())
        .collect();
    deleted.sort();
    assert_eq!(deleted, created);
    write_outcome(
        "c45",
        &json!({"run_id": run_id, "outcome": sc["outcome"], "created": created, "attestation": att}),
    );
}
