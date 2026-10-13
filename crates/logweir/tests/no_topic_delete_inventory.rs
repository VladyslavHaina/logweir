//! **The no-delete guarantee, pinned ACROSS THE WORKSPACE.**
//!
//! "No code path deletes a topic under an original name" cannot be pinned by
//! text in the runner alone: a delete added behind the creator, in
//! `logweir-kafka`, would pass every row that drives the creation step through
//! a double.
//!
//! This is an INVENTORY, in the style of `logweir-kafka/tests/inventory.rs`'s
//! source scans, over every `.rs` file under `crates/*/src`:
//!
//! 1. the broker's delete call appears EXACTLY ONCE, inside
//!    `impl TopicDeleter for RdKafkaReader`, behind the scratch prefix and the
//!    protected names;
//! 2. `impl TopicCreator for RdKafkaReader` names no delete at all;
//! 3. the trait method is called from EXACTLY THREE places in the product:
//!    the phase-0 `LogAppendTime` probe (its own probe topic), phase 9, and
//!    a TTL clone's cleanup (PROD-11.1a), over the names its ID rule kept;
//! 4. no crate but `logweir-kafka` (which defines it) and `logweir` (the
//!    runner) names `TopicDeleter`, and the runner takes its deleter from one
//!    place;
//! 5. nothing anywhere names another call that removes a topic or its
//!    records.
//!
//! A new mention of `delete_topics` in ANY file under `crates/*/src`, test
//! module or not, changes a pinned count and fails this row: whoever adds one
//! has to come here and say where it sits.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .to_path_buf()
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|e| e.expect("a directory entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Every `.rs` file under `crates/*/src`, as `(crates-relative path, text)`.
fn sources() -> Vec<(String, String)> {
    let crates = crates_dir();
    let mut members: Vec<PathBuf> = std::fs::read_dir(&crates)
        .expect("crates/ is readable")
        .map(|e| e.expect("a directory entry").path())
        .filter(|p| p.join("src").is_dir())
        .collect();
    members.sort();
    assert!(
        members.len() >= 10,
        "the inventory walks every workspace crate, found {}",
        members.len()
    );
    let mut files = Vec::new();
    for member in &members {
        walk(&member.join("src"), &mut files);
    }
    files
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(&crates)
                .expect("under crates/")
                .to_string_lossy()
                .replace('\\', "/");
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            (relative, text)
        })
        .collect()
}

/// `text` without its full-line comments (`//`, `///`, `//!`): what the
/// compiler reads, near enough for a count that errs on the strict side (a
/// trailing comment or a string that names the token still counts).
fn code(text: &str) -> String {
    text.lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The block that opens with `header` (at column 0) up to its closing brace
/// at column 0.
fn block<'a>(text: &'a str, header: &str) -> &'a str {
    let start = text
        .find(&format!("\n{header}"))
        .unwrap_or_else(|| panic!("no `{header}`"));
    let rest = &text[start + 1..];
    let end = rest
        .find("\n}\n")
        .unwrap_or_else(|| panic!("`{header}` has no closing brace at column 0"));
    &rest[..end + 2]
}

fn count(text: &str, needle: &str) -> usize {
    text.matches(needle).count()
}

/// Per file, how many times its code names `needle`; files that never do are
/// left out.
fn inventory(needle: &str) -> BTreeMap<String, usize> {
    sources()
        .into_iter()
        .map(|(path, text)| (path, count(&code(&text), needle)))
        .filter(|(_, n)| *n > 0)
        .collect()
}

fn expect(pairs: &[(&str, usize)]) -> BTreeMap<String, usize> {
    pairs
        .iter()
        .map(|(path, n)| ((*path).to_string(), *n))
        .collect()
}

const READER: &str = "logweir-kafka/src/reader.rs";
const RDKAFKA_READER: &str = "logweir-kafka/src/rdkafka_reader.rs";
const PHASE0: &str = "logweir/src/drill/phase0_admit.rs";
const PHASE9: &str = "logweir/src/drill/phase9_teardown.rs";
/// PROD-11.1a: `logweir restore cleanup`, a TTL clone's cleanup.
const RESTORE_CLEANUP: &str = "logweir/src/restore_cleanup.rs";
const ORCHESTRATOR: &str = "logweir/src/drill/mod.rs";
/// The vendored copy of the pinned engine's request-version table.
const ENGINE_VERSION_TABLE: &str = "logweir-engine-oso/src/vendored/request_versions.rs";

fn source(path: &str) -> String {
    sources()
        .into_iter()
        .find(|(p, _)| p == path)
        .map(|(_, text)| text)
        .unwrap_or_else(|| panic!("crates/{path} is gone: the inventory names it"))
}

/// **Every mention of `delete_topics` in the workspace's sources, by file.**
/// KILLS: a delete added in any crate, in any file, behind any trait.
#[test]
fn every_mention_of_a_topic_delete_is_where_this_inventory_says() {
    assert_eq!(
        inventory("delete_topics"),
        expect(&[
            // The trait method's declaration.
            (READER, 1),
            // `impl TopicDeleter`: the method, the three refusal sentences
            // (no scratch namespace, a protected name, outside the
            // namespace) and the ONE broker call; plus one call in the
            // file's own test module.
            (RDKAFKA_READER, 6),
            // The probe's delete of its own probe topic, and a test double.
            (PHASE0, 2),
            // Phase 9.
            (PHASE9, 1),
            // A TTL clone's cleanup (PROD-11.1a).
            (RESTORE_CLEANUP, 1),
        ]),
        "a topic delete is named somewhere this inventory does not list, or a listed one moved"
    );
    // The call sites themselves (`.delete_topics(`): the broker's, the two
    // product callers of the trait method, and one test call.
    assert_eq!(
        inventory(".delete_topics("),
        expect(&[
            (RDKAFKA_READER, 2),
            (PHASE0, 1),
            (PHASE9, 1),
            (RESTORE_CLEANUP, 1)
        ])
    );
    // Nothing names another way to remove a topic or its records: the
    // broker protocol's own names, librdkafka's, or a UFCS spelling.
    for needle in [
        "DeleteTopics",
        "delete_records",
        "DeleteGroups",
        "delete_groups",
        "AdminClient::delete",
    ] {
        assert_eq!(inventory(needle), BTreeMap::new(), "`{needle}` is named");
    }
    // `DeleteRecords` IS NAMED ONCE, AS DATA, and nowhere as a call: the
    // vendored request-version table (`ENGINE_REQUEST_VERSIONS`, held to the
    // engine's source by `cargo xtask check-drift`) declares a version for it.
    // Nothing in Logweir sends the request, and a second mention, or this one
    // becoming a call, fails.
    assert_eq!(
        inventory("DeleteRecords"),
        expect(&[(ENGINE_VERSION_TABLE, 1)]),
        "`DeleteRecords` is named somewhere but the vendored engine version table"
    );
    assert!(
        source(ENGINE_VERSION_TABLE).contains("\n    (\"DeleteRecords\", 1),\n"),
        "the one mention is a row of the table, not a call"
    );
}

/// **The broker's delete call is inside `impl TopicDeleter for
/// RdKafkaReader`, once, behind both refusals; and the real creator names no
/// delete.** KILLS: the creator deleting what its own request created; a
/// second broker delete; a delete that skips the scratch prefix
/// or the protected names.
#[test]
fn the_brokers_delete_call_is_the_deleters_alone_and_the_creator_names_none() {
    let reader = source(RDKAFKA_READER);
    let production = reader
        .split("\n#[cfg(test)]\nmod tests {")
        .next()
        .expect("the production half");
    // Everything outside the test module: exactly one call, and it is in
    // the deleter's impl.
    assert_eq!(count(&code(production), ".delete_topics("), 1);
    let deleter = code(block(production, "impl TopicDeleter for RdKafkaReader {"));
    assert_eq!(count(&deleter, ".delete_topics("), 1);
    let call = deleter.find(".delete_topics(").expect("the broker call");
    // It is the admin client's, over the names that passed both refusals.
    assert!(
        deleter[..call].trim_end().ends_with("self.admin"),
        "the one call is the admin client's"
    );
    assert!(deleter[call..].starts_with(".delete_topics(&allowed,"));
    for refusal in [
        "self.scratch_prefix.as_deref() else",
        "self.protected_names.contains(n)",
        "n.starts_with(prefix)",
    ] {
        let at = deleter
            .find(refusal)
            .unwrap_or_else(|| panic!("the deleter no longer checks `{refusal}`"));
        assert!(
            at < call,
            "`{refusal}` is decided before the broker is asked"
        );
    }
    // The creator: no delete, in code or in a string.
    let creator = code(block(production, "impl TopicCreator for RdKafkaReader {"));
    assert!(
        creator.contains(".create_topics(&new_topics, &options)"),
        "the creator's own broker call"
    );
    assert!(
        !creator.to_lowercase().contains("delete"),
        "impl TopicCreator for RdKafkaReader names a delete"
    );
    // And the creation call bounds the broker below the client: a
    // per-name answer wherever there can be one.
    assert!(creator.contains(".operation_timeout(Some(CREATE_OPERATION_TIMEOUT))"));
    assert!(creator.contains(".request_timeout(Some(T))"));
    // No other file of the crate asks a broker to delete anything.
    for (path, text) in sources() {
        if path.starts_with("logweir-kafka/src/") && path != RDKAFKA_READER {
            assert_eq!(count(&code(&text), ".delete_topics("), 0, "{path}");
        }
    }
}

/// **The trait method is called from exactly three places: the phase-0
/// probe, for its own probe topic, phase 9, and a TTL clone's cleanup.** Phase 9's file also holds the
/// teardown of a run that failed after the restore started, through the same
/// one call; the orchestrator reaches it only from the error returns of
/// phase 6 onwards. KILLS: a third caller (a cleanup after the creation step,
/// in the creation step, or anywhere between it and phase 6); the probe's
/// delete handed anything but the probe's name; the failure teardown called
/// before phase 6.
#[test]
fn the_deleter_is_called_from_the_probe_and_from_phase_9_and_nowhere_else() {
    let callers: Vec<String> = sources()
        .into_iter()
        .filter(|(path, _)| path.starts_with("logweir/src/"))
        .filter(|(_, text)| count(&code(text), ".delete_topics(") > 0)
        .map(|(path, _)| path)
        .collect();
    assert_eq!(
        callers,
        vec![
            PHASE0.to_string(),
            PHASE9.to_string(),
            RESTORE_CLEANUP.to_string()
        ]
    );

    // PROD-11.1a: the cleanup's one call deletes exactly what the ID rule
    // kept, after the record was verified and the cluster checked.
    let cleanup = code(&source(RESTORE_CLEANUP));
    assert_eq!(count(&cleanup, ".delete_topics("), 1);
    let call = cleanup
        .find("deleter.delete_topics(&plan.delete)")
        .expect("the cleanup deletes the ID rule's list and nothing else");
    for before in [
        "verified_scorecard(inputs, store)?",
        "check_cluster(&sc.target.cluster_id, &current_cluster)",
        "let plan = cleanup_plan(creation, &current);",
    ] {
        let at = cleanup
            .find(before)
            .unwrap_or_else(|| panic!("the cleanup no longer does `{before}`"));
        assert!(
            at < call,
            "`{before}` is decided before anything is deleted"
        );
    }

    // Phase 0: one call, the probe's own name, BEFORE the creation step in
    // the file, and the creation step itself holds no deleter.
    let phase0 = source(PHASE0);
    let production = code(
        phase0
            .split("\n#[cfg(test)]\nmod tests {")
            .next()
            .expect("the production half"),
    );
    assert_eq!(count(&production, ".delete_topics("), 1);
    let call = production
        .find("deleter.delete_topics(std::slice::from_ref(&probe))")
        .expect("the probe deletes exactly the probe topic it created");
    let step = production
        .find("pub fn create_target_topics(")
        .expect("the creation step");
    assert!(
        call < step,
        "the probe's delete is not in the creation step"
    );
    let creation = &production[step..];
    assert!(!creation.contains("TopicDeleter"));
    assert!(!creation.contains("delete_topics"));
    assert!(!creation.contains("deleter"));

    // Phase 9: one call.
    assert_eq!(count(&code(&source(PHASE9)), ".delete_topics("), 1);

    // The orchestrator hands the deleter out of ONE place, and calls no
    // delete itself; between the creation step and phase 6 nothing deletes.
    let orchestrator = source(ORCHESTRATOR);
    let production = code(
        orchestrator
            .split("\n#[cfg(test)]\nmod ")
            .next()
            .expect("the production half"),
    );
    assert_eq!(count(&production, ".delete_topics("), 0);
    assert_eq!(count(&production, ".as_deleter()"), 1);
    let from = production
        .find("phase0_admit::create_target_topics_from(")
        .expect("the creation step's call site");
    let to = from
        + production[from..]
            .find("phase6_restore::run(")
            .expect("phase 6 follows");
    assert!(
        !production[from..to].contains("delete"),
        "nothing is deleted between creation and the restore"
    );
    // The failure teardown is defined once, before the creation step,
    // and every use of it but its own definition comes after phase 6 starts.
    assert_eq!(count(&production, "phase9_teardown::after_failure("), 1);
    assert!(production.find("phase9_teardown::after_failure(").unwrap() < from);
    assert_eq!(count(&production, "remove_created("), 1);
    let uses: Vec<usize> = production
        .match_indices("torn_down")
        .map(|(i, _)| i)
        .collect();
    // Its definition and the four error returns of phases 6, 7 and 8.
    assert_eq!(uses.len(), 5, "{uses:?}");
    assert!(
        uses[1..].iter().all(|i| *i > to),
        "the failure teardown runs only once the restore has started"
    );
}

/// **No other crate names `TopicDeleter`**: the controller, the API, the
/// evidence, store, verifier, reaper and retention crates cannot delete a
/// topic because none of them holds the seam (comments included, so a
/// planned use is seen too). KILLS: a deleter handed to another crate.
#[test]
fn only_the_kafka_crate_and_the_runner_name_the_deleter() {
    let naming: Vec<String> = sources()
        .into_iter()
        .filter(|(_, text)| text.contains("TopicDeleter"))
        .map(|(path, _)| path)
        .collect();
    assert_eq!(
        naming,
        vec![
            ORCHESTRATOR.to_string(),
            PHASE0.to_string(),
            PHASE9.to_string(),
            RESTORE_CLEANUP.to_string(),
            RDKAFKA_READER.to_string(),
            READER.to_string(),
        ],
        "TopicDeleter is named outside logweir-kafka and the runner's drill module"
    );
    // The crates that link a Kafka client at all.
    for (path, text) in sources() {
        let krate = path.split('/').next().unwrap_or_default();
        if !matches!(krate, "logweir-kafka" | "logweir") {
            assert!(
                !code(&text).contains("delete_topics"),
                "{path} names a topic delete"
            );
        }
    }
}
