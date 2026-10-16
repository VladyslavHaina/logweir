//! **PROD-07.2: a stop reaches the engine.** `request_stop` (the runner's
//! SIGTERM or SIGINT handler) sends SIGTERM to the engine child, SIGKILLs it
//! after `STOP_GRACE`, and no engine starts after it. Its own test binary: the
//! stop is process-global and never cleared.
use logweir_engine_oso::subprocess::{request_stop, run_engine, STOP_GRACE};
use std::time::{Duration, Instant};

fn script(dir: &std::path::Path, name: &str, body: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let p = dir.join(name);
    std::fs::write(&p, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
    p
}

/// One test, in order, because the stop cannot be undone. KILLS: an engine
/// spawned with `Command::output` (no stop reaches it: the first wait is 60
/// s), a stop that sends no SIGTERM (the child never prints `term`), no
/// SIGKILL after the grace (the second child ignores SIGTERM and sleeps 60
/// s), and an engine started after the stop.
#[test]
fn a_stop_terminates_then_kills_the_engine_and_starts_no_other() {
    let dir = std::env::temp_dir().join(format!("logweir-engine-stop-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    struct Gone(std::path::PathBuf);
    impl Drop for Gone {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let dir = Gone(dir);
    // Answers SIGTERM by saying so and exiting 0, as the engine does.
    let polite = script(
        &dir.0,
        "polite.sh",
        "trap 'echo term; exit 0' TERM\necho started\nwhile :; do sleep 1; done",
    );
    // Ignores SIGTERM.
    let deaf = script(&dir.0, "deaf.sh", "trap '' TERM\necho started\nsleep 60");

    let t = std::thread::spawn(move || {
        let started = Instant::now();
        let run = run_engine(&polite, &[], &mut |_, _| {}).expect("it ran");
        (run, started.elapsed())
    });
    let t2 = std::thread::spawn(move || {
        let started = Instant::now();
        let run = run_engine(&deaf, &[], &mut |_, _| {}).expect("it ran");
        (run, started.elapsed())
    });
    std::thread::sleep(Duration::from_millis(800));
    request_stop();
    let (run, took) = t.join().unwrap();
    assert!(
        took < Duration::from_secs(5),
        "SIGTERM reached it: {took:?}"
    );
    assert!(run.stdout.contains("term"), "{}", run.stdout);
    assert_eq!(run.exit_code, 0, "an engine may exit 0 after a stop");
    let (run, took) = t2.join().unwrap();
    assert!(
        took >= STOP_GRACE && took < STOP_GRACE + Duration::from_secs(5),
        "SIGKILL after the grace: {took:?}"
    );
    assert_eq!(run.exit_code, -1, "killed by a signal: no exit code");

    let again = script(&dir.0, "again.sh", "echo ran");
    let refused = run_engine(&again, &[], &mut |_, _| {}).expect_err("no engine after a stop");
    assert!(refused.to_string().contains("stopped"), "{refused}");
}
