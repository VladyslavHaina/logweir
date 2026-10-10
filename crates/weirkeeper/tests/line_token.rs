//! **FX-34: the Job's line token.** What it is made from, where it is put,
//! how it is read back, and the places its text may be written.
//!
//! A runner's `refusal-detail=` line is honoured only when it carries the
//! token of the Job the runner belongs to (`weirkeeper::refusal`). That is
//! only worth anything if the token cannot be known by whoever wrote the
//! plan, so this file holds the three properties it rests on:
//!
//! * it comes from the operating system's random source, 160 bits of it, and
//!   from nothing a plan's author knows;
//! * it is an ARGUMENT of the runner container, the last two, and the Job is
//!   otherwise unchanged;
//! * its text leaves the type in exactly two places in the whole workspace:
//!   the Job's argument and the runner's line.
//!
//! The rows about which log lines are honoured are in
//! `tests/restore_controller.rs`, `tests/backup_controller.rs` and
//! `src/refusal.rs`; the rows about what a read logs are in
//! `tests/refusal_read.rs`, and the rows about what the pass that CREATES the
//! Job logs are in `tests/create_pass_log.rs`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use logweir_core::refusal_detail::{LineToken, LINE_TOKEN_ARG};
use weirkeeper::job::{self, RunnerJobSpec, RunnerOwner};

fn spec() -> RunnerJobSpec {
    RunnerJobSpec {
        name: "logweir-restore-incident-4471".to_string(),
        namespace: "team-a".to_string(),
        owner: RunnerOwner {
            api_version: "logweir.dev/v1alpha1".to_string(),
            kind: "Restore".to_string(),
            name: "logweir-restore-incident-4471".to_string(),
            uid: "4f1c8a5e-0000-4000-8000-0000000000a1".to_string(),
        },
        args: ["restore", "run", "--spec", "/plan/restore.yaml"]
            .map(str::to_string)
            .to_vec(),
        deadline_seconds: 1800,
        service_account_name: "logweir-runner".to_string(),
        secret_mounts: Vec::new(),
        config_map_mounts: Vec::new(),
        env_from_secret: Vec::new(),
        env_literal: vec![("RUST_LOG".to_string(), "info".to_string())],
        plan_config_map: Some("logweir-restore-incident-4471-plan".to_string()),
        image: None,
        image_pull_policy: None,
        resources: None,
    }
}

/// A token for the rows that need a fixed one, assembled so that no source
/// line holds a secret-shaped literal.
fn fixed() -> LineToken {
    LineToken::parse(&"7f".repeat(20)).expect("forty hex digits")
}

fn runner_args(built: &k8s_openapi::api::batch::v1::Job) -> Vec<String> {
    built
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.containers.iter().find(|c| c.name == job::CONTAINER_NAME))
        .and_then(|c| c.args.clone())
        .expect("the runner has an argv")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root")
}

/// A source file with its test module and its comment lines removed: what
/// the compiler builds into the product.
fn production(path: &Path) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let cut = text.find("\n#[cfg(test)]").unwrap_or(text.len());
    text[..cut]
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// **A token is 160 bits from the operating system's random source, and two
/// are never the same.**
///
/// The behaviour half: 2000 tokens, all different, each forty lower-case hex
/// digits, and every one of the forty positions takes every one of the
/// sixteen digits somewhere in the sample (a token that was partly constant,
/// or made from a counter or a clock, would leave positions fixed).
///
/// The source half, because no sample can tell a kernel generator from a
/// seeded one: `new_line_token`'s body calls the `ring` provider's
/// `secure_random.fill` on a buffer of `LINE_TOKEN_BYTES`, and names none of
/// the things a derived or seedable value would be made from.
///
/// KILLS: a constant token (the bytes never filled); a token derived from a
/// name, a UID, a hash or a time; a seedable generator.
#[test]
fn a_token_is_160_bits_from_the_operating_system_and_two_are_never_the_same() {
    assert_eq!(job::LINE_TOKEN_BYTES, 20, "160 bits");
    let tokens: Vec<String> = (0..2000)
        .map(|_| {
            job::new_line_token()
                .expect("the operating system's random source answers")
                .expose_token()
                .to_string()
        })
        .collect();
    for token in &tokens {
        assert!(
            token.len() == 40
                && token
                    .bytes()
                    .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
            "forty lower-case hex digits: {} characters",
            token.len()
        );
    }
    let distinct: BTreeSet<&String> = tokens.iter().collect();
    assert_eq!(distinct.len(), tokens.len(), "no two tokens are the same");
    for position in 0..40 {
        let seen: BTreeSet<u8> = tokens.iter().map(|t| t.as_bytes()[position]).collect();
        assert_eq!(
            seen.len(),
            16,
            "digit {position} takes all sixteen values over 2000 tokens"
        );
    }

    let source = production(&workspace_root().join("crates/weirkeeper/src/job.rs"));
    let start = source
        .find("pub fn new_line_token() -> Option<LineToken> {")
        .expect("the generator is in job.rs");
    let body = &source[start..start + source[start..].find("\n}\n").expect("its end")];
    for needed in [
        "let mut bytes = [0u8; LINE_TOKEN_BYTES];",
        "rustls::crypto::ring::default_provider()",
        ".secure_random",
        ".fill(&mut bytes)",
        "LineToken::from_random_bytes(&bytes)",
    ] {
        assert!(
            body.contains(needed),
            "`{needed}` is in the generator:\n{body}"
        );
    }
    for forbidden in [
        "SeedableRng",
        "StdRng",
        "SmallRng",
        "thread_rng",
        "seed",
        "fastrand",
        "Utc::now",
        "SystemTime",
        "Instant",
        "uid",
        "name",
        "sha256",
        "hash",
    ] {
        assert!(
            !body.contains(forbidden),
            "`{forbidden}` has no place in the generator:\n{body}"
        );
    }
    // NEGATIVE CONTROL: the reader of sources reads the function and not a
    // comment about it.
    assert!(body.lines().count() < 12 && !body.contains("///"));
}

/// **The token is the runner's last two arguments, and the Job is otherwise
/// unchanged.** It is not in the environment (the engine the runner starts
/// inherits that, and expands `${NAME}` over its configuration text), not in
/// an annotation, a label or the Job's name.
///
/// KILLS: the token passed as an environment variable; put anywhere but
/// last; a second change to the Job.
#[test]
fn the_token_is_the_runners_last_two_arguments_and_the_job_is_otherwise_unchanged() {
    let plain = job::build(&spec());
    let mut tokened = plain.clone();
    let token = fixed();
    job::add_line_token(&mut tokened, &token);

    let before = runner_args(&plain);
    let after = runner_args(&tokened);
    assert_eq!(after.len(), before.len() + 2);
    assert_eq!(
        after[..before.len()],
        before[..],
        "the argv before it is untouched"
    );
    assert_eq!(after[before.len()], LINE_TOKEN_ARG);
    assert_eq!(after[before.len() + 1], token.expose_token());
    assert_eq!(LINE_TOKEN_ARG, "--line-token");

    // Take the two arguments off again and the Job is the one `build` made.
    let mut stripped = serde_json::to_value(&tokened).expect("a Job serialises");
    let args = stripped["spec"]["template"]["spec"]["containers"][0]["args"]
        .as_array_mut()
        .expect("args");
    args.truncate(before.len());
    assert_eq!(
        stripped,
        serde_json::to_value(&plain).expect("a Job serialises")
    );
    assert!(
        !stripped.to_string().contains(token.expose_token()),
        "and nothing else of the Job holds the token"
    );
    // In particular not the environment.
    let env = serde_json::to_value(&tokened).expect("serialises")["spec"]["template"]["spec"]
        ["containers"][0]["env"]
        .to_string();
    assert!(!env.contains(token.expose_token()) && !env.to_lowercase().contains("line_token"));

    // A fresh one, through the function the reconcilers call.
    let mut fresh = plain.clone();
    assert!(job::add_fresh_line_token(&mut fresh));
    let fresh_token = job::line_token(&fresh).expect("it reads back");
    assert_eq!(runner_args(&fresh).len(), before.len() + 2);
    assert_ne!(fresh_token, token);
}

/// **The token is read back off the Job, and only from where this controller
/// writes it**: the last two arguments of the container named `runner`.
///
/// KILLS: the token read from any position (a value earlier in the argv that
/// happens to be the flag); a malformed value accepted; another container's
/// arguments read.
#[test]
fn the_token_is_read_back_off_the_job_and_only_from_where_it_is_written() {
    let token = fixed();
    let mut tokened = job::build(&spec());
    job::add_line_token(&mut tokened, &token);
    assert_eq!(job::line_token(&tokened), Some(token.clone()));
    assert_eq!(
        job::line_token(&job::build(&spec())),
        None,
        "a Job with none"
    );

    let with_args = |args: Vec<String>| {
        let mut built = job::build(&spec());
        built
            .spec
            .as_mut()
            .and_then(|s| s.template.spec.as_mut())
            .and_then(|p| p.containers.first_mut())
            .expect("the runner")
            .args = Some(args);
        built
    };
    let t = token.expose_token().to_string();
    let flag = LINE_TOKEN_ARG.to_string();
    for (label, args) in [
        (
            "the flag and no value",
            vec!["restore".to_string(), flag.clone()],
        ),
        (
            "the pair, and an argument after it",
            vec![flag.clone(), t.clone(), "--spec".to_string()],
        ),
        (
            "the value without its flag",
            vec!["restore".to_string(), t.clone()],
        ),
        (
            "a value that is not a token",
            vec![flag.clone(), "not-a-token".to_string()],
        ),
        (
            "a token in upper case",
            vec![flag.clone(), t.to_uppercase()],
        ),
        (
            "a token one digit short of 128 bits",
            vec![flag.clone(), t[..31].to_string()],
        ),
        ("the flag joined to its value", vec![format!("{flag}={t}")]),
        ("no arguments", Vec::new()),
    ] {
        assert_eq!(job::line_token(&with_args(args)), None, "{label}");
    }
    // The pair LAST is read whatever stands before it, including the flag as
    // another argument's value.
    assert_eq!(
        job::line_token(&with_args(vec![
            "--triggered-by".to_string(),
            flag.clone(),
            flag.clone(),
            t.clone(),
        ])),
        Some(token.clone())
    );
    // Another container's arguments are not the runner's.
    let mut sidecar = job::build(&spec());
    if let Some(pod) = sidecar.spec.as_mut().and_then(|s| s.template.spec.as_mut()) {
        let mut other = pod.containers[0].clone();
        other.name = "log-shipper".to_string();
        other.args = Some(vec![flag, t]);
        pod.containers.insert(0, other);
    }
    assert_eq!(job::line_token(&sidecar), None);
}

/// **The token's text leaves the type in two places in the whole workspace,
/// and both are the ones it exists for**: the Job's argument
/// (`job::add_line_token`) and the runner's line (`RefusalDetail::to_line`).
/// `LineToken` has no `Display`, and its `Debug` prints no value, so
/// `expose_token` is the only way out; this row counts every mention of that
/// name in every crate's product source, HOWEVER IT IS SPELLED: a method call
/// (`token.expose_token()`), a path (`token.map(LineToken::expose_token)`,
/// which the first version of this row did not see) or anything else. The
/// one definition is set aside.
///
/// So the token cannot reach a status, a condition, an event, an annotation
/// or a log line THROUGH THE TYPE, and a change that adds a third mention
/// meets this row.
///
/// **What this row cannot see**, and which rows do: once `job::add_line_token`
/// has run, the token is a plain `String` among the Job's arguments, and code
/// that logs or stores the JOB (a `?job` on a log line) never names the
/// accessor. `tests/create_pass_log.rs` captures every event of a create pass
/// for both kinds and holds the posted Job's token out of all of them; the
/// controller rows hold it out of every status.
///
/// KILLS: the token written into a condition, a log line or an annotation
/// through the accessor, in either spelling.
#[test]
fn the_tokens_text_leaves_the_type_in_two_places_only() {
    let root = workspace_root();
    let mut files = Vec::new();
    for member in std::fs::read_dir(root.join("crates")).expect("crates/") {
        let src = member.expect("a member").path().join("src");
        if src.is_dir() {
            rust_files(&src, &mut files);
        }
    }
    assert!(
        files.len() > 100,
        "the sweep read the workspace: {}",
        files.len()
    );
    let mut callers = Vec::new();
    let mut definitions = 0;
    for file in &files {
        let text = production(file);
        for (at, _) in text.match_indices("expose_token") {
            // The definition is not a caller. It is counted by itself below.
            if text[..at].ends_with("pub fn ") {
                definitions += 1;
                continue;
            }
            let line = text[..at].lines().count();
            callers.push(format!(
                "{}:{line}",
                file.strip_prefix(&root).expect("under the root").display()
            ));
        }
    }
    let files_with_a_caller: BTreeSet<String> = callers
        .iter()
        .map(|c| c.rsplit_once(':').expect("a line").0.to_string())
        .collect();
    assert_eq!(
        files_with_a_caller.into_iter().collect::<Vec<_>>(),
        vec![
            "crates/logweir-core/src/refusal_detail.rs".to_string(),
            "crates/weirkeeper/src/job.rs".to_string(),
        ],
        "every mention of `expose_token`, the definition aside: {callers:?}"
    );
    assert_eq!(callers.len(), 2, "one in each: {callers:?}");
    assert_eq!(definitions, 1, "and it is defined once");
    // NEGATIVE CONTROL: the count sees the spelling with no dot and no
    // parentheses, which is the one a `.expose_token()` search misses.
    let other_spelling = "let shown = token.map(LineToken::expose_token);";
    assert_eq!(other_spelling.match_indices("expose_token").count(), 1);
    assert_eq!(other_spelling.match_indices(".expose_token()").count(), 0);

    // The type gives no other way out.
    let core = production(&root.join("crates/logweir-core/src/refusal_detail.rs"));
    assert!(!core.contains("impl std::fmt::Display for LineToken"));
    assert!(core.contains("f.write_str(\"LineToken(<not shown>)\")"));
    assert!(
        core.contains("pub struct LineToken(String);"),
        "a private field"
    );
    let token = fixed();
    assert!(!format!("{token:?}").contains(token.expose_token()));

    // And the runner holds it in three files: the flag, the dispatch that
    // stores it, and the one printer that reads it. No other file of the
    // runner names it, so nothing can hand it to the engine or to a log.
    let mut runner = Vec::new();
    rust_files(&root.join("crates/logweir/src"), &mut runner);
    let naming: BTreeSet<String> = runner
        .iter()
        .filter(|f| {
            let text = production(f);
            text.contains("line_token") || text.contains("LineToken") || text.contains("LINE_TOKEN")
        })
        .map(|f| {
            f.strip_prefix(&root)
                .expect("under the root")
                .display()
                .to_string()
        })
        .collect();
    assert_eq!(
        naming.into_iter().collect::<Vec<_>>(),
        vec![
            "crates/logweir/src/cli.rs".to_string(),
            "crates/logweir/src/exit.rs".to_string(),
            "crates/logweir/src/main.rs".to_string(),
        ]
    );
}
