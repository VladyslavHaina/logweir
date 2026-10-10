//! `testing::CapturedLog` — the log capture FX-19's rows assert "no WARN"
//! with — sees its own thread's events even when another thread hit the same
//! callsite first.
//!
//! ONE ROW, IN A BINARY OF ITS OWN, ON PURPOSE: the property is about how many
//! `tracing` dispatchers are alive, and a sibling test's capture in the same
//! process would hide the failure this row exists to show.

use weirkeeper::testing::CapturedLog;

/// One callsite, shared by both threads below.
fn emit() {
    tracing::warn!(target: "weirkeeper::captured_log_row", "the shared callsite");
}

/// **A CALLSITE FIRST HIT ON ANOTHER THREAD STILL REACHES THE CAPTURE.**
/// `tracing-core` caches a callsite's interest on its first hit, and while one
/// dispatcher is registered it asks the hitting thread's default — here a
/// thread with no subscriber, which says `never`. Measured: the catalog
/// class-sweep row's control saw no WARN once in a full-suite run, and passed
/// alone.
///
/// NEGATIVE CONTROL: with `CapturedLog::start`'s permanent second dispatcher
/// removed, this row fails (no event captured), deterministically: the capture
/// is the only dispatcher when the other thread registers the callsite.
#[test]
fn a_callsite_first_hit_on_another_thread_still_reaches_the_capture() {
    let log = CapturedLog::start();
    std::thread::spawn(emit)
        .join()
        .expect("the other thread emits and exits");
    emit();
    assert_eq!(
        log.messages_at("WARN"),
        vec!["the shared callsite".to_string()],
        "this thread's event is captured, once; the other thread's is not"
    );
}
