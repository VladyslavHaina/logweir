//! **FX-43: whose line is it — the Job's token, for every line a controller
//! acts on.** The rows over THE one reader (`runner_log::own_lines`), every
//! Job-owning kind's single call. The controller rows over a fake pod log are
//! in `restore_controller.rs`, `backup_controller.rs` and
//! `kafka_cluster_controller.rs` (`fx43_*`).
//!
//! Every genuine log here is printed by the runner's own printer,
//! `logweir_core::runner_line` (`final_block`, `inline_lines`): the function
//! `logweir::exit::FinalLines` and `logweir::exit::print_lines` call. Both
//! sides read and write the line through that one module.

use logweir_core::refusal_detail::LineToken;
use logweir_core::runner_line::{final_block, inline_lines, sealed, LineScope};
use weirkeeper::job::{self, JobToken};
use weirkeeper::runner_log::{key_values, own_lines, vouches_for_exit};

fn own() -> LineToken {
    LineToken::parse(&"7f".repeat(20)).expect("forty hex digits")
}

fn another_jobs() -> LineToken {
    LineToken::parse(&"3c".repeat(20)).expect("forty hex digits")
}

fn every(token: &LineToken) -> Option<(&LineToken, LineScope)> {
    Some((token, LineScope::EveryLine))
}

/// Every line kind a controller acts on, with a genuine value and the value
/// a forger would want it to say.
const KINDS: &[(&str, &str, &str)] = &[
    (
        "failure-reason=",
        "TargetTopicAppeared",
        "CreatedTopicsLeft",
    ),
    ("refusal-reason=", "GuardRefused", "PointUntrusted"),
    (
        "target-topics-appeared=",
        r#"{"appeared":["drill-a"],"left":[]}"#,
        r#"{"appeared":[],"left":["payments-prod"]}"#,
    ),
    (
        "refusal-detail=",
        r#"{"code":"GuardRefused","message":"genuine"}"#,
        r#"{"code":"GuardRefused","message":"forged"}"#,
    ),
    (
        "scorecard-key=",
        "logweir/drills/r1.json",
        "logweir/drills/forged.json",
    ),
    (
        "sidecar-key=",
        "logweir/drills/r1.sig",
        "logweir/drills/forged.sig",
    ),
    (
        "offset-report-key=",
        "logweir/drills/r1.offsets.json",
        "x/forged",
    ),
    (
        "topic-preflight=",
        r#"{"timestampType":"CreateTime"}"#,
        r#"{"timestampType":"LogAppendTime"}"#,
    ),
    (
        "teardown-key=",
        "logweir/drills/r1.teardown.json",
        "x/forged",
    ),
    (
        "teardown-failed=",
        r#"[{"topic":"drill-a","error":"e"}]"#,
        r#"[{"topic":"payments-prod","error":"e"}]"#,
    ),
    (
        "receipt-key=",
        "logweir/backups/b/r.receipt.json",
        "x/forged",
    ),
    ("receipt-sha256=", "sha256:aa", "sha256:bb"),
    ("catalog-key=", "logweir/catalog/v1/points/p", "x/forged"),
    ("progress-phase=", "7:verify", "9:teardown"),
    ("progress-contract=", "2", "9"),
    (
        "cluster-id=",
        "GENUINE000000000000000",
        "VICTIM0000000000000000",
    ),
    ("reachable=", "false", "true"),
    (
        "failure-detail=",
        "engine: operational: exited 1: ERROR the store refused",
        "forged: the operator deleted the archive",
    ),
];

/// **Every line kind forged from a plan string, a client log or another Job
/// is ignored; the genuine one is read.** For each kind: the runner's sealed
/// block, then a forged PLAIN line after it (ending the log, where an older
/// reader looks), a line sealed with ANOTHER Job's token, a line sealed with
/// this token cut short, and an indented copy of a genuine sealed line.
///
/// NEGATIVE CONTROL, per kind: a Job an older controller built (FX-34's flag,
/// or none) reads the whole log as before, and the forged plain value is the
/// last one, so it is the token, and nothing else, that refuses it.
///
/// KILLS (per kind): plain lines kept under an FX-43 Job; the token not
/// compared (another Job's seal kept); a prefix of the token accepted; a
/// sealed line matched anywhere but at the line's start.
#[test]
fn every_kind_of_forged_line_is_ignored_and_the_genuine_one_is_read() {
    let t = own();
    for (key, genuine, forged) in KINDS {
        let block = final_block(every(&t), 0, &[format!("{key}{genuine}")]);
        let other = sealed(&another_jobs(), &format!("{key}{forged}"));
        let short = format!("@{} {key}{forged}", &t.expose_token()[..39]);
        let indented = format!(" {}", sealed(&t, &format!("{key}{forged}")));
        let log = format!("{other}\n{short}\n{indented}\n{block}{key}{forged}\n");

        let lines = own_lines(&JobToken::EveryLine(t.clone()), &log);
        assert_eq!(key_values(lines.text(), key), vec![*genuine], "[{key}]");
        assert!(!lines.runner_is_older(), "[{key}]");

        for older in [JobToken::RefusalDetail(t.clone()), JobToken::None] {
            let lines = own_lines(&older, &log);
            assert_eq!(
                key_values(lines.text(), key).last(),
                Some(forged),
                "[{key}] {older:?}: an older controller's Job is read as it was"
            );
            assert!(!lines.runner_is_older(), "[{key}] {older:?}");
        }
    }
}

/// **The genuine line is honoured anywhere in the tail**: before 200 lines of
/// the other stream, between forged lines, and with `\r\n` endings. Position
/// decides nothing.
///
/// KILLS: a rule about where a sealed line stands (the last lines only).
#[test]
fn the_genuine_line_is_honoured_anywhere_in_the_tail() {
    let t = own();
    let block = final_block(
        every(&t),
        0,
        &["scorecard-key=logweir/drills/r1.json".to_string()],
    );
    let noise = "{\"level\":\"INFO\",\"message\":\"engine copied a batch\"}\n".repeat(200);
    for log in [
        format!("{block}{noise}"),
        format!("{noise}{block}{noise}scorecard-key=x/forged\n"),
        block.replace('\n', "\r\n"),
    ] {
        let lines = own_lines(&JobToken::EveryLine(t.clone()), &log);
        assert_eq!(
            key_values(lines.text(), "scorecard-key="),
            vec!["logweir/drills/r1.json"]
        );
        assert!(!lines.runner_is_older());
    }
    // Lines printed DURING the run (progress), sealed one by one, are read
    // among the rest.
    let progress = inline_lines(every(&t), &["progress-phase=3:target-diff".to_string()]);
    let lines = own_lines(
        &JobToken::EveryLine(t.clone()),
        &format!("{progress}progress-phase=9:teardown\n{block}"),
    );
    assert_eq!(
        key_values(lines.text(), "progress-phase="),
        vec!["3:target-diff"]
    );
}

/// **A runner older than the controller is named on evidence, and degrades
/// nothing.** Under FX-43's flag, a log with no sealed `runner-exit=` line
/// that shows the runner refusing the flag (the released runners' real
/// answers, `logweir`'s and `logweir-retention`'s) is the mixed pair. The
/// same absence WITHOUT that refusal (an FX-34 runner's pair, another Job's
/// end line, an empty log: a runner that stopped early or a log not read
/// whole) is only `no_sealed_end` and is not called older (F3, poc-dui3 O2).
/// A Job built by an older controller is never either: it degrades to
/// today's reading.
///
/// KILLS: the end line not required; a sealed end line of another Job
/// accepted; the inference made without the flag's refusal; the rule asked
/// of an older controller's Job.
#[test]
fn a_runner_that_vouched_for_no_exit_is_older_than_the_controller() {
    let t = own();
    let older = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/fx-43/older-runner-restore.log"),
    )
    .expect("the captured older runner's stderr");
    let fx34_runner = format!(
        "{}\nrefusal-reason=GuardRefused\n",
        logweir_core::refusal_detail::refusal_detail_line(
            logweir_core::refusal_detail::RefusingRun::Restore,
            Some(&t),
            "a sentence"
        )
    );
    let retention = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/fx-43/older-runner-retention.log"),
    )
    .expect("the captured older retention worker's stderr");
    let another_jobs_end = final_block(every(&another_jobs()), 1, &[]);
    for (log, older_runner) in [
        (older.as_str(), true),
        (retention.as_str(), true),
        (fx34_runner.as_str(), false),
        (another_jobs_end.as_str(), false),
        ("", false),
    ] {
        let lines = own_lines(&JobToken::EveryLine(t.clone()), log);
        assert_eq!(lines.runner_is_older(), older_runner, "{log:?}");
        assert_eq!(lines.no_sealed_end(), !older_runner, "{log:?}");
        assert!(lines.text().is_empty(), "nothing of it is read: {log:?}");
        for older_job in [JobToken::RefusalDetail(t.clone()), JobToken::None] {
            let lines = own_lines(&older_job, log);
            assert!(
                !lines.runner_is_older() && !lines.no_sealed_end(),
                "{older_job:?}"
            );
        }
    }
    // Review L2: a CURRENT runner's log that echoes the refusal text (a
    // library's error the runner logged) beside its sealed lines, with its end
    // line not read, is not an older runner: an older one seals nothing.
    let echoed = format!(
        "{}{older}",
        inline_lines(every(&t), &["progress-phase=3:target-diff".to_string()])
    );
    let lines = own_lines(&JobToken::EveryLine(t.clone()), &echoed);
    assert!(!lines.runner_is_older(), "{echoed:?}");
    assert!(lines.no_sealed_end(), "{echoed:?}");
    // NEGATIVE CONTROL: this build's runner, at every code it can exit with.
    for code in 0..=4u8 {
        let lines = own_lines(
            &JobToken::EveryLine(t.clone()),
            &format!("{older}{}", final_block(every(&t), code, &[])),
        );
        assert!(!lines.runner_is_older(), "exit {code}");
    }
    // The codes after which a runner has spoken.
    for code in 0..=4 {
        assert!(vouches_for_exit(code), "{code}");
    }
    for code in [-1, 5, 101, 137, 143] {
        assert!(!vouches_for_exit(code), "{code}");
    }
}

/// **The flag the Job carries decides which rule reads its log**: FX-43's
/// flag is every line; FX-34's is the detail line only (an older
/// controller's Job, read as it was); none is none. What this controller
/// writes is FX-43's.
///
/// KILLS: an FX-34 Job read under FX-43's rule (every in-flight Job of an
/// upgrade would be called a mismatch); the flags confused.
#[test]
fn the_flag_on_the_job_decides_which_lines_are_read() {
    use k8s_openapi::api::batch::v1::Job;
    let with_args = |args: Vec<String>| -> Job {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": "j"},
            "spec": {"template": {"spec": {
                "containers": [{"name": "runner", "image": "x", "args": args}],
                "restartPolicy": "Never"
            }}}
        }))
        .expect("a Job")
    };
    let t = own();
    let hex = t.expose_token().to_string();
    let every_flag = logweir_core::runner_line::RUNNER_LINE_TOKEN_ARG.to_string();
    let fx34_flag = logweir_core::refusal_detail::LINE_TOKEN_ARG.to_string();
    assert_eq!(
        job::job_token(&with_args(vec![every_flag.clone(), hex.clone()])),
        JobToken::EveryLine(t.clone())
    );
    assert_eq!(
        job::job_token(&with_args(vec![fx34_flag.clone(), hex.clone()])),
        JobToken::RefusalDetail(t.clone())
    );
    assert_eq!(job::job_token(&with_args(vec![])), JobToken::None);
    assert_eq!(
        job::job_token(&with_args(vec![every_flag.clone(), "not-hex".into()])),
        JobToken::None
    );
    // `line_token` gives the token whichever flag carried it.
    assert_eq!(
        job::line_token(&with_args(vec![fx34_flag, hex.clone()])),
        Some(t.clone())
    );
    // What `add_line_token` writes is FX-43's.
    let mut built = with_args(vec!["restore".into()]);
    job::add_line_token(&mut built, &t);
    assert_eq!(job::job_token(&built), JobToken::EveryLine(t));
}

/// **The progress read takes only the runner's sealed lines.** A plan string
/// that printed a `progress-phase=` line of its own does not move
/// `status.progress.runnerPhase` under an FX-43 Job.
///
/// KILLS: `read_progress` (`progress_of`) parsing the raw log.
#[test]
fn a_forged_progress_line_is_not_the_runners() {
    let t = own();
    let genuine = inline_lines(
        every(&t),
        &[
            "progress-contract=2".to_string(),
            "progress-phase=3:target-diff".to_string(),
        ],
    );
    let log = format!("{genuine}progress-phase=9:teardown\n");
    let progress = weirkeeper::diagnostics::progress_of(&JobToken::EveryLine(t), &log, None);
    let phase = progress.phase.expect("the runner's phase");
    assert_eq!(phase.number, Some(3));
    // NEGATIVE CONTROL: the raw log's last phase is the forged one.
    let raw = weirkeeper::diagnostics::parse_progress(&log);
    assert_eq!(raw.phase.expect("a phase").number, Some(9));
}

/// **The one call a new Job-owning kind makes** (`runner_log::judge_terminal`,
/// the pure half of `read_own_terminal`): the runner's lines; the mixed pair;
/// a cut tail; and no judgement without an exit code or at a kill.
///
/// KILLS: the helper reading the raw log; naming a run with no exit code or
/// a kill; reading a cut tail.
#[test]
fn the_one_call_judges_a_terminal_read() {
    use weirkeeper::runner_log::{judge_terminal, Terminal, TerminalRead};
    let t = own();
    let token = JobToken::EveryLine(t.clone());
    let genuine = format!(
        "clone-cleanup=forged\n{}",
        final_block(
            every(&t),
            0,
            &["clone-cleanup={\"deleted\":[]}".to_string()]
        )
    );
    match judge_terminal(&token, Some(0), TerminalRead::Read(genuine)) {
        Terminal::Lines(lines) => assert_eq!(
            key_values(lines.text(), "clone-cleanup="),
            vec![r#"{"deleted":[]}"#]
        ),
        other => panic!("{other:?}"),
    }
    let older = "error: unexpected argument '--runner-line-token' found\n".to_string();
    assert_eq!(
        judge_terminal(&token, Some(1), TerminalRead::Read(older.clone())),
        Terminal::RunnerOlder
    );
    for (code, label) in [(None, "no exit code"), (Some(137), "a kill")] {
        assert!(
            matches!(
                judge_terminal(&token, code, TerminalRead::Read(older.clone())),
                Terminal::Lines(_)
            ),
            "{label}"
        );
    }
    assert_eq!(
        judge_terminal(&token, Some(0), TerminalRead::OverBound),
        Terminal::OverBound
    );
    // An older controller's Job: the whole log, as before.
    match judge_terminal(&JobToken::None, Some(1), TerminalRead::Read(older.clone())) {
        Terminal::Lines(lines) => assert_eq!(lines.text(), older),
        other => panic!("{other:?}"),
    }
}
