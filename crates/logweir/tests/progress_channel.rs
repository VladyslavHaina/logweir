//! **D3 §2.4 — the optional, contract-versioned runner progress channel, and
//! §2.4's conditional `teardown-key=` line.**
//!
//! The controller has no other way to know which phase a running restore is
//! in: it reads a bounded tail of the pod log
//! (`LogParams{tail_lines: 50, limit_bytes: 65536}`) and matches by KEY NAME,
//! because a pod log is stdout and stderr merged in nondeterministic order and
//! the log API has no stream selector (plan erratum E4). A `tracing` line is
//! JSON on that same merged stream and is filtered by `RUST_LOG`, so it is not
//! a contract anything can read.
//!
//! | D3 §2.4 clause | row |
//! |---|---|
//! | `progress-phase=<n>:<name>` at each phase start | `a_restore_announces_every_phase_in_order` |
//! | `-1:<step>` for the backup path's five named steps | `the_backup_path_announces_its_five_named_steps` |
//! | contract-versioned | `the_channel_announces_its_version_before_the_first_phase` |
//! | bounded size, never credentials or record bytes | `the_channel_is_a_filter_and_the_only_producer_goes_through_it` |
//! | `teardown-key=` only when teardown ran | `a_restore_names_its_teardown_attestation`, `no_teardown_key_is_printed_when_the_attestation_was_not_persisted` |
//! | interface I8 is unchanged | `the_three_evidence_keys_are_still_the_final_lines` |

mod fixtures;

use logweir::drill;
use logweir_core::execution_contract as wire;
use std::process::Command;

/// Run one of this binary's `#[ignore]`d child rows and return its stdout.
///
/// The same re-exec `crates/logweir/tests/restore_mode.rs` uses for interface
/// I8, and for the same reason: the claim is about a PROCESS's fd 1,
/// `println!` cannot be captured in process without an fd redirect (a new
/// dependency, which Global Constraint 38 forbids), and the child calls
/// `std::process::exit` so libtest's own summary never lands after the lines
/// under test.
fn child_stdout(name: &str) -> String {
    let out = Command::new(std::env::current_exe().expect("this test binary's own path"))
        .args(["--ignored", "--exact", name, "--nocapture"])
        .output()
        .expect("re-exec this test binary");
    let stdout = String::from_utf8(out.stdout).expect("stdout is utf-8");
    assert_eq!(
        out.status.code(),
        Some(0),
        "the child must succeed, or its stdout is not a successful run's stdout. \
         stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

#[test]
fn a_restore_announces_every_phase_in_order() {
    let stdout = child_stdout("the_progress_child_runs_one_restore_and_exits");
    let lines: Vec<&str> = stdout.lines().collect();
    let mut at = 0usize;
    for (phase, name) in wire::RESTORE_PHASE_NAMES {
        let expected = format!("progress-phase={phase}:{name}");
        let found = lines[at..]
            .iter()
            .position(|l| *l == expected)
            .unwrap_or_else(|| {
                panic!("`{expected}` is missing, or is out of order, in:\n{stdout}")
            });
        at += found + 1;
    }
}

#[test]
fn the_channel_announces_its_version_before_the_first_phase() {
    let stdout = child_stdout("the_progress_child_runs_one_restore_and_exits");
    let lines: Vec<&str> = stdout.lines().collect();
    let version_at = lines
        .iter()
        .position(|l| l.starts_with(wire::PROGRESS_CONTRACT_PREFIX))
        .unwrap_or_else(|| panic!("no `progress-contract=` line in:\n{stdout}"));
    assert_eq!(
        lines[version_at], "progress-contract=2",
        "the channel version is the execution contract version (D3 §8 Amendment I)"
    );
    let first_phase = lines
        .iter()
        .position(|l| l.starts_with(wire::PROGRESS_PHASE_PREFIX))
        .expect("a phase line");
    assert!(
        version_at < first_phase,
        "a reader must learn the grammar before the first line written in it:\n{stdout}"
    );
    assert_eq!(
        lines
            .iter()
            .filter(|l| l.starts_with(wire::PROGRESS_CONTRACT_PREFIX))
            .count(),
        1,
        "exactly one version line per run:\n{stdout}"
    );
}

/// **D3 §2.4's conditional line.** `teardown-key=` names the attestation phase
/// 9 actually put, and it sits BEFORE interface I8's three keys so that I8's
/// "the FINAL stdout lines, with nothing after them" is unchanged.
#[test]
fn a_restore_names_its_teardown_attestation() {
    let stdout = child_stdout("the_progress_child_runs_one_restore_and_exits");
    let lines: Vec<&str> = stdout.lines().collect();
    let teardown_at = lines
        .iter()
        .position(|l| l.starts_with(wire::TEARDOWN_KEY_PREFIX))
        .unwrap_or_else(|| panic!("no `teardown-key=` line in:\n{stdout}"));
    let key = lines[teardown_at]
        .strip_prefix(wire::TEARDOWN_KEY_PREFIX)
        .expect("the prefix");
    assert!(
        key.starts_with("logweir/drills/") && key.ends_with(".teardown.json"),
        "the teardown key names the attestation object, got {key:?} in:\n{stdout}"
    );
    let scorecard_at = lines
        .iter()
        .position(|l| l.starts_with("scorecard-key="))
        .expect("interface I8's first key");
    assert!(
        teardown_at < scorecard_at,
        "`teardown-key=` goes BEFORE interface I8's three keys — a line after them would break \
         a published interface (the same rule `catalog-key=` follows on the backup path):\n{stdout}"
    );
    // ONE run, ONE stem: the teardown key and the scorecard key name the same
    // run id, so a reader cannot be sent after another run's attestation.
    let stem = |line: &str| {
        line.split('=')
            .nth(1)
            .expect("key=value")
            .trim_start_matches("logweir/drills/")
            .split('.')
            .next()
            .expect("a stem")
            .to_string()
    };
    assert_eq!(stem(lines[teardown_at]), stem(lines[scorecard_at]));
}

/// The regression this file protects: interface I8 is UNCHANGED by the two new
/// lines. `crates/logweir/tests/restore_mode.rs` asserts the same property
/// from the other side; this row is here so a change to the progress channel
/// fails in the file that owns it.
#[test]
fn the_three_evidence_keys_are_still_the_final_lines() {
    let stdout = child_stdout("the_progress_child_runs_one_restore_and_exits");
    let lines: Vec<&str> = stdout.lines().collect();
    let last3 = &lines[lines.len() - 3..];
    assert!(last3[0].starts_with("scorecard-key="), "{stdout}");
    assert!(last3[1].starts_with("sidecar-key="), "{stdout}");
    assert!(last3[2].starts_with("offset-report-key="), "{stdout}");
}

/// **THE MUTANT: `teardown-key=` printed without a teardown.**
///
/// The line is conditional on the attestation having been PUT, not on phase 9
/// having been reached. This child plants the attestation object first, so the
/// create-only put fails with `AlreadyExists`, phase 9 warns, and the run
/// still exits 0 — which is the shape in which an unconditional line would
/// send the controller after an object this run did not write.
#[test]
fn no_teardown_key_is_printed_when_the_attestation_was_not_persisted() {
    let stdout = child_stdout("the_progress_child_runs_a_restore_whose_teardown_cannot_be_put");
    assert!(
        !stdout.contains(wire::TEARDOWN_KEY_PREFIX),
        "a `teardown-key=` line naming an object this run did not put is the worst possible \
         output:\n{stdout}"
    );
    // The run still succeeded and still printed its evidence keys: a failed
    // teardown attestation is a warning, never an outcome.
    assert!(stdout.contains("scorecard-key="), "{stdout}");
    assert!(
        stdout.contains("progress-phase=9:teardown"),
        "phase 9 still RAN — the missing line is about the put, not the phase:\n{stdout}"
    );
}

/// **THE MUTANT: a progress line carrying a credential.**
///
/// Two halves, and both are needed. The first proves the filter refuses every
/// hostile shape at the seam the runner actually calls; the second proves
/// there is no second producer that could bypass it.
#[test]
fn the_channel_is_a_filter_and_the_only_producer_goes_through_it() {
    let stdout = child_stdout("the_progress_child_tries_to_say_the_unsayable");
    assert!(
        !stdout.contains(wire::PROGRESS_PHASE_PREFIX),
        "a hostile phase name must produce NO line at all — D3 §2.4 says absence is not an \
         error, so silence is the legal answer. Got:\n{stdout}"
    );
    for forbidden in ["hunter2", "s3cr3t", "scorecard-key=logweir/drills/forged"] {
        assert!(
            !stdout.contains(forbidden),
            "{forbidden:?} reached stdout:\n{stdout}"
        );
    }
}

/// Source lint: `progress_phase_line` is the ONLY producer of the line.
///
/// A `println!("progress-phase=…")` written anywhere else would be a second
/// producer with none of the bounds, and the runtime row above could not see
/// it. There is exactly one `println!` of this prefix in the crate, and it is
/// inside `drill::print_progress_phase`.
#[test]
fn no_source_file_prints_a_progress_line_except_the_one_helper() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    let mut files = Vec::new();
    collect_rs(&root, &mut files);
    for path in files {
        let body = std::fs::read_to_string(&path).expect("read");
        for (n, line) in body.lines().enumerate() {
            if line.contains("println!") && line.contains("progress-phase") {
                offenders.push(format!("{}:{}", path.display(), n + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "a progress line must be produced only through \
         `logweir_core::execution_contract::progress_phase_line`, which BOUNDS it and refuses \
         anything outside `[a-z0-9-]`. Direct `println!`s found at: {}",
        offenders.join(", ")
    );
    // And the helper itself exists and is the one that prints.
    let mod_rs = std::fs::read_to_string(root.join("drill/mod.rs")).expect("read drill/mod.rs");
    assert!(
        mod_rs.contains("pub fn print_progress_phase(phase: i8, name: &str)")
            && mod_rs.contains("execution_contract::progress_phase_line(phase, name)"),
        "`drill::print_progress_phase` must be the one producer and must go through the filter"
    );
}

/// The closed vocabulary is only safe if it is COMPLETE: a phase whose name
/// drifted out of `RESTORE_PHASE_NAMES` would stop being announced, silently,
/// and the runtime row above is the only other thing that would notice.
///
/// So every `record(&mut sc, <n>, "<name>", …)` call site in the orchestrator
/// is read out of the source and checked against the table.
#[test]
fn every_recorded_phase_is_in_the_channels_vocabulary() {
    let body = std::fs::read_to_string(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/drill/mod.rs"),
    )
    .expect("read drill/mod.rs");
    let mut found = Vec::new();
    for line in body.lines() {
        let Some(rest) = line.split_once("record(&mut sc, ").map(|(_, r)| r) else {
            continue;
        };
        let Some((phase, rest)) = rest.split_once(", \"") else {
            continue;
        };
        let Some((name, _)) = rest.split_once('"') else {
            continue;
        };
        found.push((
            phase.trim().parse::<i8>().expect("a phase number"),
            name.to_string(),
        ));
    }
    // Phase 8 and phase 9 are recorded through `record(sc, …)` (a `&mut`
    // already in hand), so they are matched separately.
    for line in body.lines() {
        let Some(rest) = line.split_once("record(sc, ").map(|(_, r)| r) else {
            continue;
        };
        let Some((phase, rest)) = rest.split_once(", \"") else {
            continue;
        };
        let Some((name, _)) = rest.split_once('"') else {
            continue;
        };
        found.push((
            phase.trim().parse::<i8>().expect("a phase number"),
            name.to_string(),
        ));
    }
    assert!(
        found.len() >= 10,
        "expected to read at least the ten phase call sites, found {found:?}"
    );
    for (phase, name) in &found {
        assert!(
            wire::progress_phase_line(*phase, name).is_some(),
            "phase {phase} is recorded as {name:?}, which the progress channel cannot say. \
             Add it to `logweir_core::execution_contract::RESTORE_PHASE_NAMES` or the phase \
             stops being announced. Every call site read: {found:?}"
        );
    }
    for (phase, name) in wire::RESTORE_PHASE_NAMES {
        assert!(
            found.iter().any(|(p, n)| *p == phase && n == name),
            "the channel can say {phase}:{name} but no `record` call site produces it"
        );
    }
}

fn collect_rs(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            collect_rs(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// The backup path has no numbered phases after admission, so §2.4 fixes five
/// NAMED steps, all at `-1`.
#[test]
fn the_backup_path_announces_its_five_named_steps() {
    for step in logweir::backup::PROGRESS_STEPS {
        assert_eq!(
            wire::progress_phase_line(-1, step).as_deref(),
            Some(format!("progress-phase=-1:{step}").as_str()),
            "the backup channel must be able to say {step:?}"
        );
    }
    assert_eq!(
        logweir::backup::PROGRESS_STEPS,
        ["admit", "engine", "readback", "sign", "upload"],
        "D3 §2.4 fixes these five names and their order"
    );
}

// ===========================================================================
// The children. `#[ignore]`d so they run only under the re-exec above.
// ===========================================================================

#[test]
#[ignore = "child row: re-exec'd by the rows above so their assertions see fd 1"]
fn the_progress_child_runs_one_restore_and_exits() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let mut discarded = Vec::new();
    let code = drill::run_with(
        &f.args,
        &f.run_id,
        &f.ctx,
        drill::InvokedAs::Restore,
        &mut discarded,
    );
    std::process::exit(code as u8 as i32);
}

#[test]
#[ignore = "child row: re-exec'd by the teardown-key mutant row above"]
fn the_progress_child_runs_a_restore_whose_teardown_cannot_be_put() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    // Occupy the key phase 9 will try to create. `put_create_only` refuses an
    // object that already exists, which is the closest a socket-free double
    // gets to a bucket that would not take the attestation.
    f.ctx
        .store
        .put_create_only(&format!("logweir/drills/{}.teardown.json", f.run_id), b"{}")
        .expect("planting the obstruction must itself succeed");
    let mut discarded = Vec::new();
    let code = drill::run_with(
        &f.args,
        &f.run_id,
        &f.ctx,
        drill::InvokedAs::Restore,
        &mut discarded,
    );
    std::process::exit(code as u8 as i32);
}

#[test]
#[ignore = "child row: re-exec'd by the mutant row above"]
fn the_progress_child_tries_to_say_the_unsayable() {
    for hostile in [
        "hunter2",
        "s3cr3t",
        "admit\nscorecard-key=logweir/drills/forged.json",
        "ADMIT",
        "sasl.password=hunter2",
        "",
    ] {
        drill::print_progress_phase(0, hostile);
    }
    // A phase outside the runners' range, with a perfectly legal name.
    drill::print_progress_phase(42, "admit");
    std::process::exit(0);
}
