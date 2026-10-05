//! **Records stamped at a fixed past instant need a topic that keeps them.**
//!
//! A suite that stamps its records at a literal instant writes records that
//! have already breached the broker's default retention (7 days) when they
//! land. Today that is `T` = 2025-10-09, in `record_semantics.rs` and
//! `pitr_boundary.rs`. The broker's retention check runs every five minutes
//! and deletes such records the next time it runs, so a row that captures
//! after it finds its topic empty. Engine-matrix run 36542777892 lost
//! PROD-01.1's shapes row that way, with either engine
//! (`docs/to-do/decisions/PROD-00-engine-route.md` 4.4, A-C20-2).
//!
//! The rule held here: a test file under `e2e/tests/` that contains a fixed
//! epoch-milliseconds literal older than seven days creates its topics only
//! through `harness::create_topic_for_fixed_timestamps`, which adds
//! `retention.ms=-1`, as Logweir's own restore targets have. It is a text
//! scan in the default test set, with no Docker and no broker.
//!
//! It is narrow on purpose. It keys on a fixed past literal, which is how a
//! point-in-time fixture states its instants ("A FIXED LITERAL, NEVER A CLOCK
//! READ", `pitr_boundary.rs`). Two suites stamp relative to now and are
//! outside it:
//! * PROD-01.4's `topic_identity.rs` stamps a minute ago, and every row
//!   starts on `retention.ms=-1`; its row c10 lowers retention on purpose.
//! * `mvp_demo.rs` stamps at the live seed's newest instant.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// The one way such a suite creates a topic.
const HELPER: &str = "create_topic_for_fixed_timestamps(";

/// Every other way the suites create a topic.
const CREATORS: [&str; 5] = [
    "create_topic(",
    "create_topic_with_configs(",
    "create_topic_exact(",
    "create_topics(",
    "\"--create\"",
];

const SEVEN_DAYS_MS: i64 = 7 * 24 * 3600 * 1000;

/// This file plants the pattern it refuses, in its negative controls.
const SELF: &str = "fixture_retention.rs";

fn tests_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_millis() as i64
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

fn is_comment(line: &str) -> bool {
    line.trim_start().starts_with("//")
}

/// `(line, value)` of every integer literal of exactly 13 digits (underscores
/// allowed, an `i64`/`u64` suffix allowed) whose value is an
/// epoch-milliseconds instant from 2001 up to seven days before `now`.
fn fixed_past_instants(src: &str, now: i64) -> Vec<(usize, i64)> {
    let mut found = Vec::new();
    for (n, line) in src.lines().enumerate() {
        if is_comment(line) {
            continue;
        }
        let b = line.as_bytes();
        let mut i = 0;
        while i < b.len() {
            let starts = b[i].is_ascii_digit()
                && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_'));
            if !starts {
                i += 1;
                continue;
            }
            let start = i;
            while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'_') {
                i += 1;
            }
            let rest = &line[i..];
            let ends = i == b.len()
                || !(b[i].is_ascii_alphanumeric())
                || rest.starts_with("i64")
                || rest.starts_with("u64");
            let digits: String = line[start..i]
                .chars()
                .filter(char::is_ascii_digit)
                .collect();
            if ends && digits.len() == 13 {
                if let Ok(v) = digits.parse::<i64>() {
                    if (1_000_000_000_000..now - SEVEN_DAYS_MS).contains(&v) {
                        found.push((n + 1, v));
                    }
                }
            }
        }
    }
    found
}

/// `(line, text)` of every topic creation that does not go through the helper.
fn other_creations(src: &str) -> Vec<(usize, String)> {
    src.lines()
        .enumerate()
        .filter(|(_, l)| {
            let t = l.trim_start();
            !is_comment(l) && !t.starts_with("fn ") && !t.starts_with("pub fn ")
        })
        .filter(|(_, l)| CREATORS.iter().any(|c| l.contains(c)) && !l.contains(HELPER))
        .map(|(n, l)| (n + 1, l.trim().to_string()))
        .collect()
}

/// The rule for one source: `None` when it stamps no fixed past instant,
/// otherwise the creations that break it.
fn violations(src: &str, now: i64) -> Option<Vec<(usize, String)>> {
    if fixed_past_instants(src, now).is_empty() {
        None
    } else {
        Some(other_creations(src))
    }
}

/// The rule over a tree: `(files that stamp a fixed past instant, every
/// breaking line as "file:line: text")`.
fn scan(dir: &Path, now: i64) -> (Vec<String>, Vec<String>) {
    let mut stamped = Vec::new();
    let mut broken = Vec::new();
    for path in rust_files(dir) {
        let rel = path
            .strip_prefix(dir)
            .expect("under the scanned dir")
            .to_string_lossy()
            .to_string();
        if rel == SELF {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("a readable source");
        if let Some(lines) = violations(&src, now) {
            stamped.push(rel.clone());
            broken.extend(lines.into_iter().map(|(n, l)| format!("{rel}:{n}: {l}")));
        }
    }
    (stamped, broken)
}

#[test]
fn every_suite_stamped_at_a_fixed_past_instant_keeps_its_records() {
    let dir = tests_dir();
    let walked: Vec<String> = rust_files(&dir)
        .iter()
        .map(|p| p.strip_prefix(&dir).unwrap().to_string_lossy().to_string())
        .collect();
    for nested in ["harness/mod.rs", "record_semantics_support/kafka.rs"] {
        assert!(
            walked.iter().any(|w| w == nested),
            "the scan must walk subdirectories; it did not see {nested}: {walked:?}"
        );
    }
    let (stamped, broken) = scan(&dir, now_ms());
    for known in ["record_semantics.rs", "pitr_boundary.rs"] {
        assert!(
            stamped.iter().any(|s| s == known),
            "{known} stamps `T` = 2025-10-09, so the scan must see it: {stamped:?}"
        );
    }
    assert!(
        broken.is_empty(),
        "a suite that stamps a fixed past instant creates a topic without \
         harness::create_topic_for_fixed_timestamps, so the broker's retention check can \
         empty it before the capture (PROD-00.1 4.4):\n{}",
        broken.join("\n")
    );
}

/// The helper is what makes the rule mean anything: it must add the override.
#[test]
fn the_helper_keeps_the_records() {
    let src = std::fs::read_to_string(tests_dir().join("harness/mod.rs")).expect("the harness");
    let start = src
        .find("pub fn create_topic_for_fixed_timestamps(")
        .expect("the harness defines create_topic_for_fixed_timestamps");
    let body = &src[start..start + src[start..].find("\n}\n").expect("its end")];
    assert!(
        body.contains("(\"retention.ms\", \"-1\")"),
        "create_topic_for_fixed_timestamps must add retention.ms=-1:\n{body}"
    );
    assert!(
        body.contains("create_topic_with_configs("),
        "it creates through the configs path:\n{body}"
    );
}

/// A scratch directory under the system temp dir, removed on drop (this crate
/// has no `tempfile`, and this guard adds no package).
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("fixture-retention-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a scratch dir");
        Scratch(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Negative controls: the rule refuses the planted pattern, on disk and in
/// memory, and leaves alone what is outside the class.
#[test]
fn a_planted_suite_without_the_override_is_refused() {
    let now = now_ms();
    let stamp = "const T: i64 = 1_760_000_000_000;\n";
    let bad = format!(
        "{stamp}fn row() {{\n    create_topic_with_configs(&t, 3, &[(\"message.timestamp.type\", \"CreateTime\")]);\n}}\n"
    );
    let cli =
        format!("{stamp}fn row() {{\n    kafka_topics(&[\"--create\", \"--topic\", t]);\n}}\n");
    let good = format!(
        "{stamp}fn row() {{\n    create_topic_for_fixed_timestamps(&t, 3, &[(\"message.timestamp.type\", \"CreateTime\")]);\n}}\n"
    );
    assert_eq!(
        violations(&bad, now).map(|v| v.len()),
        Some(1),
        "the configs path without the helper is refused"
    );
    assert_eq!(violations(&bad, now).unwrap()[0].0, 3);
    assert_eq!(
        violations(&cli, now).map(|v| v.len()),
        Some(1),
        "a raw --create is refused"
    );
    assert_eq!(violations(&good, now), Some(vec![]), "the helper passes");

    // Outside the class: stamps relative to now, a recent literal, a literal
    // mentioned only in a comment.
    let relative = "fn row() {\n    let ts = now_ms() - 60_000;\n    create_topic(&t, 3);\n}\n";
    let recent = format!(
        "const T: i64 = {};\nfn row() {{\n    create_topic(&t, 3);\n}}\n",
        now - 24 * 3600 * 1000
    );
    let comment = "/// T = 1_760_000_000_000 ms\nfn row() {\n    create_topic(&t, 3);\n}\n";
    for (label, src) in [
        ("relative", relative.to_string()),
        ("recent", recent),
        ("comment", comment.to_string()),
    ] {
        assert_eq!(violations(&src, now), None, "{label} is outside the class");
    }

    // On disk, nested one level down, through the same walk the real tree gets.
    let tmp = Scratch::new();
    std::fs::create_dir_all(tmp.0.join("nested")).unwrap();
    std::fs::write(tmp.0.join("nested/planted.rs"), &bad).unwrap();
    std::fs::write(tmp.0.join("fine.rs"), &good).unwrap();
    let (stamped, broken) = scan(&tmp.0, now);
    assert_eq!(
        stamped,
        vec!["fine.rs".to_string(), "nested/planted.rs".to_string()]
    );
    assert_eq!(broken.len(), 1, "{broken:?}");
    assert!(broken[0].starts_with("nested/planted.rs:3: "), "{broken:?}");
}
