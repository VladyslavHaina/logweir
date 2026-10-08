//! **A topic a row creates is read only once the cluster serves it (FX-18).**
//!
//! A successful create means the controller committed the topic, not that
//! every broker applied it. Until they have, a one-shot read answers with
//! what a creation in progress looks like: `NotLeaderForPartition` from a
//! broker that has not yet become the leader it is listed as (main CI run
//! 37753000930, `topic_identity.rs` c02, reading watermarks after recreating
//! its topic), or an empty DescribeConfigs answer from a broker that does not
//! hold the topic yet (PROD-00.3f's matrix row, `guards.rs`). "Listed in
//! metadata" was the wait every helper used, and it is not enough.
//!
//! Two rows:
//!
//! * [`every_function_that_creates_a_topic_waits_until_it_is_served`], a text
//!   scan in the default test set (no Docker): a function under `e2e/tests/`
//!   that creates a topic also calls one of [`WAITS`]. A Java CLI or a
//!   producer retries those answers itself, so the rule is about the
//!   creating function, which is where the next read is decided.
//! * `live::a_recreated_topic_is_read_the_moment_it_is_served`, against the
//!   stack: one name deleted and recreated [`live::ROUNDS`] times, each
//!   generation read at once — watermarks, then configuration — through the
//!   shipped `await_served` and `created_topic_configs`. With those two made
//!   no-ops (the FX-18 report's negative control) it fails.

use std::path::{Path, PathBuf};

/// How a function under `e2e/tests/` creates a topic.
const CREATORS: [&str; 2] = ["\"--create\"", "create_topics("];

/// What it calls before the topic is read: the harness helpers and the two
/// product seams they wrap.
const WAITS: [&str; 4] = [
    "await_created(",
    "await_created_on(",
    "await_served(",
    "created_topic_configs(",
];

/// Files that plant the creators in string literals, as negative controls.
const PLANTERS: [&str; 2] = ["created_topics.rs", "fixture_retention.rs"];

fn tests_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests")
}

/// Every `.rs` file under `dir`, recursively, sorted.
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap_or_else(|e| panic!("{}: {e}", d.display())) {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// The code part of a line: everything before a `//` comment.
fn code(line: &str) -> &str {
    line.split("//").next().unwrap_or("")
}

/// `(first line, the function's code)` for every `fn` in `src`, from its
/// signature to the brace that closes its body. Comments are dropped, so a
/// creator or a wait named only in a comment counts for nothing.
fn functions(src: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = src.lines().collect();
    let mut out = Vec::new();
    for (start, line) in lines.iter().enumerate() {
        let c = code(line);
        let t = c.trim_start();
        let is_fn = t.starts_with("fn ")
            || t.starts_with("pub fn ")
            || t.starts_with("pub(crate) fn ")
            || t.starts_with("async fn ");
        if !is_fn {
            continue;
        }
        let mut depth = 0i32;
        let mut opened = false;
        let mut body = String::new();
        for l in &lines[start..] {
            let c = code(l);
            body.push_str(c);
            body.push('\n');
            for ch in c.chars() {
                match ch {
                    '{' => {
                        depth += 1;
                        opened = true;
                    }
                    '}' => depth -= 1,
                    _ => {}
                }
            }
            // A signature that ends in `;` (a trait method) has no body.
            if !opened && c.trim_end().ends_with(';') {
                break;
            }
            if opened && depth <= 0 {
                break;
            }
        }
        out.push((start + 1, body));
    }
    out
}

/// Does this function create a topic? A validate-only CreateTopics creates
/// nothing, and `validate_create_topics(` is that call.
fn creates(body: &str) -> bool {
    if body.contains("validate_only(true)") {
        return false;
    }
    CREATORS.iter().any(|c| {
        body.match_indices(c)
            .any(|(at, _)| !body[..at].ends_with("validate_"))
    })
}

/// `file:line fn` of every function in `src` that creates a topic and waits
/// for nothing. Only the INNERMOST function counts: an outer function whose
/// text contains a nested creating `fn` is judged by that `fn`.
fn offenders(name: &str, src: &str) -> Vec<String> {
    let fns = functions(src);
    let mut out = Vec::new();
    for (i, (line, body)) in fns.iter().enumerate() {
        if !creates(body) {
            continue;
        }
        let nested = fns[i + 1..]
            .iter()
            .any(|(_, inner)| body.contains(inner.as_str()) && creates(inner));
        if nested {
            continue;
        }
        if !WAITS.iter().any(|w| body.contains(w)) {
            let sig = body.lines().next().unwrap_or("").trim();
            out.push(format!("{name}:{line} `{sig}`"));
        }
    }
    out
}

/// **FX-18, the class sweep's guard.** Every function under `e2e/tests/` that
/// creates a topic waits until the cluster serves it.
#[test]
fn every_function_that_creates_a_topic_waits_until_it_is_served() {
    let root = tests_dir();
    let files = rust_files(&root);
    let mut seen = Vec::new();
    let mut found = Vec::new();
    let mut creating = 0;
    for path in &files {
        let name = path
            .strip_prefix(&root)
            .expect("under tests/")
            .display()
            .to_string();
        if PLANTERS.iter().any(|p| name.ends_with(p)) {
            continue;
        }
        let src = std::fs::read_to_string(path).expect("readable");
        creating += functions(&src).iter().filter(|(_, b)| creates(b)).count();
        found.extend(offenders(&name, &src));
        seen.push(name);
    }
    // The walk reaches the files whose helpers carry the fix, nested ones too.
    for must in [
        "harness/mod.rs",
        "topic_identity.rs",
        "guards.rs",
        "config_coverage.rs",
    ] {
        assert!(seen.iter().any(|s| s == must), "the scan never read {must}");
    }
    assert!(
        creating >= 8,
        "the scan recognised only {creating} creating functions; the creators it looks for \
         ({CREATORS:?}) no longer match how this suite creates topics"
    );
    assert!(
        found.is_empty(),
        "these functions create a topic and read it without waiting until the cluster serves \
         it; end them with `harness::await_created(topic, partitions)` (or `await_created_on` \
         for another broker):\n{}",
        found.join("\n")
    );
}

/// The scan's own negative controls: it flags a creating function that does
/// not wait, and only that.
#[test]
fn the_scan_flags_a_create_without_a_wait_and_nothing_else() {
    let unwaited = "fn make(t: &str) {\n    kafka_topics(&[\"--create\", \"--topic\", t]);\n}\n";
    assert_eq!(
        offenders("x.rs", unwaited),
        vec!["x.rs:1 `fn make(t: &str) {`"]
    );

    let admin = "fn make(r: &R) {\n    TopicCreator::create_topics(r, &[spec]).unwrap();\n}\n";
    assert_eq!(offenders("x.rs", admin).len(), 1, "the admin call too");

    let waited = "fn make(t: &str) {\n    kafka_topics(&[\"--create\", \"--topic\", t]);\n    \
                  await_created(t, 3);\n}\n";
    assert!(offenders("x.rs", waited).is_empty());

    let validate = "fn v(r: &R) {\n    admin.create_topics(&[t], &o.validate_only(true));\n}\n\
         fn w(r: &R) {\n    r.validate_create_topics(&specs);\n}\n";
    assert!(
        offenders("x.rs", validate).is_empty(),
        "validate-only creates nothing"
    );

    let commented = "fn c() {\n    // kafka-topics \"--create\" happens elsewhere\n}\n";
    assert!(
        offenders("x.rs", commented).is_empty(),
        "a comment is no create"
    );

    let wait_in_a_comment = "fn make(t: &str) {\n    kafka_topics(&[\"--create\"]); // \
                             await_created(t, 1)\n}\n";
    assert_eq!(
        offenders("x.rs", wait_in_a_comment).len(),
        1,
        "a wait named in a comment is no wait"
    );

    let nested = "mod live {\n    fn outer() {\n        fn inner(t: &str) {\n            \
                  k(&[\"--create\"]);\n            await_created(t, 1);\n        }\n        \
                  inner(\"a\");\n    }\n}\n";
    assert!(
        offenders("x.rs", nested).is_empty(),
        "the innermost creating fn is the one judged"
    );
}

// Gated by its own `#![cfg(feature = "e2e")]`.
mod harness;

#[cfg(feature = "e2e")]
mod live {
    use super::harness::*;
    use logweir_kafka::reader::{
        ClusterReader, NewTopicSpec, TopicCreator, TopicDeleter, CREATED_TOPIC_SETTLE,
    };
    use std::time::{Duration, Instant};

    /// Generations of one name. c02's shape, delete then recreate, is the
    /// one that failed in CI: the broker is retiring the old partition while
    /// it is told to lead the new one.
    pub const ROUNDS: usize = 20;
    const PARTITIONS: i32 = 3;

    fn nonce() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_nanos()
            % 10_000_000_000
    }

    /// **FX-18, live.** Each generation of a recreated topic answers a
    /// watermark read and a configuration read the moment the shipped wait
    /// returns.
    ///
    /// NEGATIVE CONTROL (run for the FX-18 report, not in CI): with
    /// `RdKafkaReader::await_served` returning `Ok(())` at once and
    /// `created_topic_configs` reading once, this row fails on a read that
    /// raced its topic's creation.
    #[test]
    fn a_recreated_topic_is_read_the_moment_it_is_served() {
        let topic = format!("{SCRATCH_PREFIX}fx18-recreated-{}", nonce());
        let scoped = || {
            reader()
                .with_scratch_prefix(SCRATCH_PREFIX)
                .expect("`drill-` is a usable scratch namespace")
        };
        let spec = NewTopicSpec {
            name: topic.clone(),
            num_partitions: PARTITIONS,
            replication_factor: 1,
            configs: vec![("retention.ms".to_string(), "-1".to_string())],
        };
        for round in 1..=ROUNDS {
            // Create. A name whose previous generation the controller is
            // still deleting is refused for a moment; that is the
            // delete-then-create race, not the one this row is about.
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                let res = TopicCreator::create_topics(&scoped(), std::slice::from_ref(&spec))
                    .expect("the CreateTopics call answers");
                match &res[0].1 {
                    Ok(()) => break,
                    Err(e) if Instant::now() < deadline => {
                        eprintln!("[fx-18] round {round}: create {topic}: {e}; retrying");
                        std::thread::sleep(Duration::from_millis(200));
                    }
                    Err(e) => panic!("round {round}: create {topic}: {e}"),
                }
            }

            // Read at once, through the shipped seams.
            let r = scoped();
            ClusterReader::await_served(&r, &topic, PARTITIONS, CREATED_TOPIC_SETTLE)
                .unwrap_or_else(|e| panic!("round {round}: {topic} never served: {e}"));
            let ends = ClusterReader::end_offsets(&r, &topic)
                .unwrap_or_else(|e| panic!("round {round}: watermarks of {topic}: {e}"));
            assert_eq!(
                ends,
                (0..PARTITIONS).map(|p| (p, 0)).collect::<Vec<_>>(),
                "round {round}: a new generation is empty on every partition"
            );
            let configs =
                ClusterReader::created_topic_configs(&r, &topic, PARTITIONS, CREATED_TOPIC_SETTLE)
                    .unwrap_or_else(|e| panic!("round {round}: configuration of {topic}: {e}"));
            assert_eq!(
                configs.get("retention.ms").map(String::as_str),
                Some("-1"),
                "round {round}: the configuration read is this generation's"
            );

            // Delete, and wait until the metadata agrees.
            TopicDeleter::delete_topics(&r, std::slice::from_ref(&topic))
                .expect("the DeleteTopics call answers");
            let deadline = Instant::now() + Duration::from_secs(60);
            while ClusterReader::list_topics(&r)
                .map(|ts| ts.iter().any(|t| t.name == topic))
                .unwrap_or(true)
            {
                assert!(
                    Instant::now() < deadline,
                    "round {round}: {topic} still listed 60 s after its deletion"
                );
                std::thread::sleep(Duration::from_millis(200));
            }
            eprintln!("[fx-18] round {round}/{ROUNDS}: served and read");
        }
    }
}
