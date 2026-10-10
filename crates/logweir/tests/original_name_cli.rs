//! **PROD-15.1 review M3, M2 and L2: the runner BINARY's own refusals of a
//! restore under the original topic names, before anything is read or
//! dialled.** CI-run: every refusal here happens at startup, so the target
//! (`127.0.0.1:1`, nothing listens) is never contacted and no bucket is
//! opened (the plan's stores are filesystem paths nothing creates).
//!
//! - The approval SUBJECT is held to the plan at startup, BEFORE the runner
//!   reads its other inputs: the rows give a `--kafka-topic-resources` path
//!   that does not exist, so a runner whose startup check was removed answers
//!   exit 1 ("cannot read") instead of exit 3 `ApprovalSubjectMismatch`.
//! - `drill approve` refuses to sign an ordinary approval for such a plan.
//! - The owner condition fails CLOSED on what the resources file holds.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const ORIGINAL_PLAN: &str = "source:\n  storage:\n    backend: filesystem\n    path: /logweir-original-name-cli-archive\n  backup: latestCompleted\n  topics: [orders]\ntarget:\n  bootstrap_servers: [127.0.0.1:1]\n  mode: newTopic\n  topic_mapping_prefix: \"drill-\"\n  topic_naming:\n    prefix: \"\"\n    original_name: {owners: []}\n  default_replication_factor: 1\nsample:\n  window_start: \"2026-08-29T00:00:00Z\"\n  window_end: \"2026-08-30T02:00:00Z\"\n  records_per_partition: 25\n  anchor: head\n  coverage: complete\nobjectives: {}\nevidence:\n  backend: filesystem\n  path: /logweir-original-name-cli-evidence\n";

fn ordinary_plan() -> String {
    ORIGINAL_PLAN.replace(
        "  topic_naming:\n    prefix: \"\"\n    original_name: {owners: []}\n",
        "  topic_naming:\n    prefix: \"restore-\"\n",
    )
}

struct Bundle {
    _dir: tempfile::TempDir,
    spec: PathBuf,
    args: Vec<std::ffi::OsString>,
    key: PathBuf,
}

/// A spec and a v1 approval signed over its exact bytes, carrying
/// `approval_subject` when `subject` is given.
fn bundle(spec_text: &str, subject: Option<&str>) -> Bundle {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("drill.yaml");
    std::fs::write(&spec, spec_text).unwrap();
    let key = logweir_evidence::keys::SigningKey::generate_p256();
    let key_path = dir.path().join("key.pem");
    std::fs::write(&key_path, key.to_pkcs8_pem().unwrap()).unwrap();
    let approver_key = dir.path().join("approver.pub.pem");
    std::fs::write(
        &approver_key,
        key.verifying_key().to_public_key_pem().unwrap(),
    )
    .unwrap();
    let doc = logweir_core::spec::ApprovalDoc {
        approver: "original-name-cli@example.com".into(),
        ticket: "PROD-15.1-M3".into(),
        plan_hash: logweir_core::ids::sha256_prefixed(spec_text.as_bytes()),
        approved_at: chrono::Utc::now(),
        subject_kind: logweir_core::spec::SUBJECT_KIND_RESTORE.into(),
        approval_subject: subject.map(str::to_string),
    };
    let bytes = serde_json::to_vec(&doc).unwrap();
    let approval = dir.path().join("approval.json");
    std::fs::write(&approval, &bytes).unwrap();
    let sidecar = logweir_evidence::sign::sign_detached(
        &key,
        logweir::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL,
        &bytes,
    )
    .unwrap();
    std::fs::write(
        approval.with_extension("sig"),
        serde_json::to_vec(&sidecar).unwrap(),
    )
    .unwrap();
    let allowed = dir.path().join("allowed-clusters.json");
    std::fs::write(
        &allowed,
        br#"{"allowed_cluster_ids": [], "source_cluster_id": null}"#,
    )
    .unwrap();
    let args = vec![
        "--approval".into(),
        approval.into_os_string(),
        "--approver-key".into(),
        approver_key.into_os_string(),
        "--allowed-clusters".into(),
        allowed.into_os_string(),
        "--signing-key".into(),
        key_path.clone().into_os_string(),
    ];
    Bundle {
        _dir: dir,
        spec,
        args,
        key: key_path,
    }
}

fn restore_run(b: &Bundle, resources: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_logweir"))
        .args(["restore", "run", "--spec"])
        .arg(&b.spec)
        .args(&b.args)
        .arg("--kafka-topic-resources")
        .arg(resources)
        .env_remove("LOGWEIR_TARGET_PASSWORD")
        .output()
        .unwrap()
}

fn both(o: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

fn last_stdout_line(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout)
        .lines()
        .last()
        .unwrap_or_default()
        .to_string()
}

/// Review M3, the STARTUP call site: an ordinary approval for an
/// original-name plan is exit 3 `ApprovalSubjectMismatch`, before the runner
/// reads its `KafkaTopic` resources (a path that does not exist) or dials
/// anything. KILLS: removing the startup check (the unreadable file would
/// answer exit 1 first).
#[test]
fn an_ordinary_approval_is_refused_before_the_runner_reads_its_inputs() {
    let b = bundle(ORIGINAL_PLAN, None);
    let out = restore_run(&b, Path::new("/logweir-no-such-kafkatopics.yaml"));
    let text = both(&out);
    assert_eq!(out.status.code(), Some(3), "{text}");
    assert!(text.contains("ApprovalSubjectMismatch"), "{text}");
    assert!(
        last_stdout_line(&out).starts_with("refusal-reason="),
        "{text}"
    );
}

/// **An original-name restore requires complete verification, and the
/// runner says so before it dials anything.** The same plan WITHOUT
/// `sample.coverage: complete`, under a correct `originalName` approval and
/// with a target nothing listens on: exit 3, the refusal opening
/// `OriginalNameNeedsCompleteCoverage`, the last stdout line a
/// `refusal-reason=`. CONTROL: the complete plan with the same inputs is NOT
/// refused for its shape (it goes on to fail on the unreachable target or a
/// later input). KILLS: removing the startup shape check AND phase 0's (the
/// run would reach the broker and exit 1); refusing the complete plan too.
#[test]
fn a_sampled_original_name_plan_is_refused_by_name_before_anything_is_dialled() {
    let sampled = ORIGINAL_PLAN.replace("  coverage: complete\n", "");
    assert_ne!(
        sampled, ORIGINAL_PLAN,
        "the fixture asks for complete coverage"
    );
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("kafkatopics.yaml");
    std::fs::write(&empty, "").unwrap();

    let b = bundle(&sampled, Some("originalName"));
    let out = restore_run(&b, &empty);
    let text = both(&out);
    assert_eq!(out.status.code(), Some(3), "{text}");
    assert!(
        text.contains("OriginalNameNeedsCompleteCoverage: "),
        "{text}"
    );
    assert!(text.contains("Set sample.coverage: complete"), "{text}");
    assert!(
        last_stdout_line(&out).starts_with("refusal-reason="),
        "{text}"
    );

    let b = bundle(ORIGINAL_PLAN, Some("originalName"));
    let out = restore_run(&b, &empty);
    let text = both(&out);
    assert!(
        !text.contains("OriginalNameNeedsCompleteCoverage"),
        "the complete plan is not refused for its coverage: {text}"
    );
}

/// `ORIGINAL_PLAN` narrowed to one partition of `orders`, in the one spelling
/// PROD-11.1b's grammar accepts for a subset (the interval form).
fn subset_plan() -> String {
    format!(
        "{ORIGINAL_PLAN}restore:\n  point_in_time: \"../2026-08-30T01:00:00Z\"\n  partitions:\n    orders: [0]\n"
    )
}

/// **An original-name restore restores whole topics, and the runner says so
/// before it dials anything.** The plan with `restore.partitions`, under a
/// correct `originalName` approval and with a target nothing listens on:
/// exit 3, the refusal opening `OriginalNameNeedsWholeTopics`, the last
/// stdout line a `refusal-reason=`. CONTROLS: the same plan with a window
/// (no subset) is not refused for its shape, and a subset plan under a
/// PREFIX is not this rule's (each goes on to fail on the unreachable target
/// or a later input). KILLS: removing the startup shape check AND phase 0's
/// (the run would reach the broker and exit 1); refusing a stated window;
/// refusing a prefixed subset.
#[test]
fn a_subset_original_name_plan_is_refused_by_name_before_anything_is_dialled() {
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("kafkatopics.yaml");
    std::fs::write(&empty, "").unwrap();

    let b = bundle(&subset_plan(), Some("originalName"));
    let out = restore_run(&b, &empty);
    let text = both(&out);
    assert_eq!(out.status.code(), Some(3), "{text}");
    assert!(text.contains("OriginalNameNeedsWholeTopics: "), "{text}");
    assert!(text.contains("Remove restore.partitions"), "{text}");
    assert!(
        last_stdout_line(&out).starts_with("refusal-reason="),
        "{text}"
    );

    // CONTROL: whole topics from a stated window start.
    let windowed = format!(
        "{ORIGINAL_PLAN}restore:\n  point_in_time: \"2026-08-29T12:00:00Z/2026-08-30T01:00:00Z\"\n"
    );
    let b = bundle(&windowed, Some("originalName"));
    let out = restore_run(&b, &empty);
    let text = both(&out);
    assert!(
        !text.contains("OriginalNameNeedsWholeTopics"),
        "a window is not a subset: {text}"
    );
    assert!(!text.contains("does not parse"), "{text}");

    // CONTROL: the subset under a prefix, with an ordinary approval.
    let prefixed = subset_plan().replace(
        "  topic_naming:\n    prefix: \"\"\n    original_name: {owners: []}\n",
        "  topic_naming:\n    prefix: \"restore-\"\n",
    );
    assert_ne!(prefixed, subset_plan());
    let b = bundle(&prefixed, None);
    let out = restore_run(&b, &empty);
    let text = both(&out);
    assert!(
        !text.contains("OriginalNameNeedsWholeTopics"),
        "a prefixed subset is PROD-11.1b's: {text}"
    );
    assert!(!text.contains("does not parse"), "{text}");
}

/// The reverse direction at the same call site: an `originalName` approval
/// authorises nothing else. KILLS: a one-directional check.
#[test]
fn an_original_name_approval_is_refused_for_an_ordinary_plan() {
    let b = bundle(&ordinary_plan(), Some("originalName"));
    let out = restore_run(&b, Path::new("/logweir-no-such-kafkatopics.yaml"));
    let text = both(&out);
    assert_eq!(out.status.code(), Some(3), "{text}");
    assert!(text.contains("ApprovalSubjectMismatch"), "{text}");
}

/// Review M2 and L2 through the binary: the owner condition fails CLOSED on
/// what `--kafka-topic-resources` holds — a `KafkaTopic` that manages
/// `orders` under a reference too long to record (the review's probe P1: a
/// 63-character namespace and a 200-character name), a `KafkaTopic` with no
/// readable name, and a file holding no `KafkaTopic` at all — each exit 3
/// `OriginalNameOwnerUnreadable`, before anything is dialled. The explicit
/// empty `List` is a look that found none, and passes the owner condition
/// (the run then stops on the dead target, never with this token). KILLS:
/// dropping an owner the run cannot record; reading any parseable file as
/// "looked, none found".
#[test]
fn the_owner_condition_fails_closed_on_the_resources_file() {
    let b = bundle(ORIGINAL_PLAN, Some("originalName"));
    let dir = tempfile::tempdir().unwrap();
    let cases = [
        (
            "long-reference",
            format!(
                "apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\nmetadata:\n  name: {}\n  namespace: {}\n  labels:\n    strimzi.io/cluster: prod\nspec:\n  topicName: orders\n",
                "n".repeat(200),
                "k".repeat(63)
            ),
        ),
        (
            "nameless",
            "apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\nmetadata:\n  namespace: kafka\n  labels: {strimzi.io/cluster: prod}\nspec: {topicName: orders}\n".to_string(),
        ),
        (
            "kafka-cr",
            "apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\nmetadata: {name: prod}\n".to_string(),
        ),
        ("empty-file", String::new()),
    ];
    for (name, body) in cases {
        let file = dir.path().join(format!("{name}.yaml"));
        std::fs::write(&file, body).unwrap();
        let out = restore_run(&b, &file);
        let text = both(&out);
        assert_eq!(out.status.code(), Some(3), "{name}: {text}");
        assert!(
            text.contains("OriginalNameOwnerUnreadable"),
            "{name}: {text}"
        );
    }
    let empty_list = dir.path().join("empty-list.yaml");
    std::fs::write(&empty_list, "apiVersion: v1\nkind: List\nitems: []\n").unwrap();
    let out = restore_run(&b, &empty_list);
    let text = both(&out);
    assert!(!text.contains("OriginalNameOwnerUnreadable"), "{text}");
    assert!(!text.contains("ApprovalSubjectMismatch"), "{text}");
}

/// **PROD-15.1 review 2, M1 (the reviewer's probe C2, through the real
/// binary): a plan string with line breaks never STARTS A LINE of the
/// runner's own output.** The plan's `evidence.path` carries the two key
/// lines a controller reads, `target-topics-appeared=` and
/// `failure-reason=CreatedTopicsLeft`; the run fails on that store (exit 1)
/// and its error text ENDS with the plan's string. Printed raw, the last two
/// lines of the pod log would be the forged pair, naming `orders` as "created
/// by this restore … remove it yourself" when the run created nothing.
///
/// The row requires that the string IS echoed (a row that passes because
/// nothing was printed proves nothing), on ONE line, with its line breaks
/// shown as `\n`; and that no line of stdout or stderr begins with either
/// key. KILLS: `report_with` printing the error raw.
#[test]
fn a_plan_string_with_line_breaks_never_starts_a_line_of_the_runners_output() {
    // As the YAML double-quoted scalar an author writes: `\n` is a line break.
    let forged = "x\\ntarget-topics-appeared={\\\"appeared\\\":[],\\\"left\\\":[\\\"orders\\\"]}\\nfailure-reason=CreatedTopicsLeft\\n";
    let plan = ordinary_plan().replace(
        "  path: /logweir-original-name-cli-evidence\n",
        &format!("  path: \"/logweir-no-such-evidence{forged}\"\n"),
    );
    assert_ne!(plan, ordinary_plan());
    let parsed: logweir_core::spec::DrillSpec =
        serde_yaml::from_str(&plan).expect("the plan parses");
    let logweir_core::engine::StorageUrl::Filesystem { path } = &parsed.evidence else {
        panic!("a filesystem evidence store");
    };
    assert!(
        path.to_string_lossy()
            .ends_with("\ntarget-topics-appeared={\"appeared\":[],\"left\":[\"orders\"]}\nfailure-reason=CreatedTopicsLeft\n"),
        "the plan grammar takes the line breaks: {path:?}"
    );

    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("kafkatopics.yaml");
    std::fs::write(&empty, "apiVersion: v1\nkind: List\nitems: []\n").unwrap();
    let out = restore_run(&bundle(&plan, None), &empty);
    let text = both(&out);
    assert_eq!(out.status.code(), Some(1), "{text}");

    // The string was echoed, on one line, escaped.
    let stderr = String::from_utf8_lossy(&out.stderr);
    let echoed: Vec<&str> = stderr
        .lines()
        .filter(|l| l.contains("failure-reason=CreatedTopicsLeft"))
        .collect();
    assert_eq!(echoed.len(), 1, "{text}");
    assert!(
        echoed[0].starts_with("operational: ")
            && echoed[0].ends_with(
                "/logweir-no-such-evidencex\\ntarget-topics-appeared={\"appeared\":[],\"left\":[\"orders\"]}\\nfailure-reason=CreatedTopicsLeft\\n"
            ),
        "{}",
        echoed[0]
    );
    // And nothing this process printed begins with a key a controller reads
    // for a stopped creation step.
    for line in text.lines() {
        assert!(
            !line.starts_with("target-topics-appeared=") && !line.starts_with("failure-reason="),
            "a line begins with a forged key: {line}\n---\n{text}"
        );
    }
}

/// Review M3 (R09): `drill approve` refuses to sign an ordinary approval for
/// an original-name plan, and writes nothing; with the flag it signs the
/// subject. KILLS: minting the ordinary subject for such a plan.
#[test]
fn drill_approve_signs_the_original_name_subject_only_when_asked() {
    let b = bundle(ORIGINAL_PLAN, None);
    let out_path = b.spec.with_file_name("minted.json");
    let approve = |subject: bool| {
        let mut c = Command::new(env!("CARGO_BIN_EXE_logweir"));
        c.args(["drill", "approve", "--spec"])
            .arg(&b.spec)
            .arg("--key")
            .arg(&b.key)
            .args([
                "--approver",
                "ops@example.com",
                "--ticket",
                "CHG-1",
                "--out",
            ])
            .arg(&out_path);
        if subject {
            c.args(["--approval-subject", "original-name"]);
        }
        c.output().unwrap()
    };
    let refused = approve(false);
    let text = both(&refused);
    assert_eq!(refused.status.code(), Some(1), "{text}");
    assert!(text.contains("--approval-subject original-name"), "{text}");
    assert!(!out_path.exists(), "nothing is signed");
    let minted = approve(true);
    assert_eq!(minted.status.code(), Some(0), "{}", both(&minted));
    let doc: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&out_path).unwrap()).unwrap();
    assert_eq!(doc["approval_subject"], "originalName");

    // A SAMPLED original-name plan is never signed for: the runner refuses
    // it at phase 0, and the approver is told so before signing. KILLS: an
    // approval minted for a plan that can only be refused.
    let sampled = bundle(&ORIGINAL_PLAN.replace("  coverage: complete\n", ""), None);
    let sampled_out = sampled.spec.with_file_name("minted.json");
    let out = Command::new(env!("CARGO_BIN_EXE_logweir"))
        .args(["drill", "approve", "--spec"])
        .arg(&sampled.spec)
        .arg("--key")
        .arg(&sampled.key)
        .args([
            "--approver",
            "ops@example.com",
            "--ticket",
            "CHG-1",
            "--out",
        ])
        .arg(&sampled_out)
        .args(["--approval-subject", "original-name"])
        .output()
        .unwrap();
    let text = both(&out);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(text.contains("OriginalNameNeedsCompleteCoverage"), "{text}");
    assert!(text.contains("Nothing was signed"), "{text}");
    assert!(!sampled_out.exists(), "nothing is signed");

    // Nor is a PARTITION SUBSET under the original names: such a restore
    // restores whole topics. KILLS: an approval minted over a plan that
    // would create a production-named topic and fill part of it.
    let subset = bundle(&subset_plan(), None);
    let subset_out = subset.spec.with_file_name("minted.json");
    for flag in [true, false] {
        let mut c = Command::new(env!("CARGO_BIN_EXE_logweir"));
        c.args(["drill", "approve", "--spec"])
            .arg(&subset.spec)
            .arg("--key")
            .arg(&subset.key)
            .args([
                "--approver",
                "ops@example.com",
                "--ticket",
                "CHG-1",
                "--out",
            ])
            .arg(&subset_out);
        if flag {
            c.args(["--approval-subject", "original-name"]);
        }
        let out = c.output().unwrap();
        let text = both(&out);
        assert_eq!(out.status.code(), Some(1), "{flag}: {text}");
        assert!(text.contains("Nothing was signed"), "{flag}: {text}");
        if flag {
            assert!(text.contains("OriginalNameNeedsWholeTopics"), "{text}");
        }
        assert!(!subset_out.exists(), "nothing is signed");
    }
}
