//! **FX-34**: what the bounded exit-3 log read ([`weirkeeper::refusal::read`])
//! returns and what it LOGS.
//!
//! A pod Kubernetes already collected is not an error and not a WARN (FX-19);
//! a refused or failed read is one WARN; and no line this controller logs
//! carries a byte of the pod's log.
//!
//! # Every row here installs a capturing subscriber
//!
//! `tracing` caches a callsite's interest the first time the callsite is hit.
//! A binary that mixes capturing rows with rows that install no subscriber can
//! cache "never" and drop the capturing rows' events (FX-37). So this file
//! holds ONLY rows that capture, each through [`capture`], and the rows about
//! statuses live in `tests/restore_controller.rs` and
//! `tests/backup_controller.rs`.

use std::io::Write;
use std::sync::{Arc, Mutex};

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use logweir_core::refusal_detail::{refusal_detail_line, RefusingRun};
use weirkeeper::refusal::{read, RefusalLog, RunnerReason};
use weirkeeper::testing::{mock_client_recording, Route};

const NS: &str = "logweir-fx34";
const POD: &str = "rst-refused-abcde";
/// A sentence no log line of the CONTROLLER may ever carry.
const SENTENCE: &str = "SeededReason: seeded-refusal-sentence-9d41 names a topic";

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// One read of `status`/`body`, with every event the read emitted, at every
/// level, as JSON lines.
async fn capture(status: u16, body: String) -> (RefusalLog, Vec<serde_json::Value>, usize) {
    let sink = Captured::default();
    let writer = sink.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let (client, seen) = mock_client_recording(vec![Route {
        method: "GET",
        path_suffix: "/log",
        status,
        body,
    }]);
    let pods: Api<Pod> = Api::namespaced(client, NS);
    let log = read(&pods, NS, POD, RefusingRun::Restore).await;
    let requests = seen.lock().unwrap().len();
    let text = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
    let events = text
        .lines()
        .map(|l| serde_json::from_str(l).expect("a JSON event"))
        // Only this crate's events: `kube` logs its own at debug.
        .filter(|e: &serde_json::Value| {
            e["target"]
                .as_str()
                .is_some_and(|t| t.starts_with("weirkeeper"))
        })
        .collect();
    (log, events, requests)
}

fn failure(code: u16, reason: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure",
  "message":"pods \"{POD}\" log: {reason}","reason":"{reason}","code":{code}}}"#
    )
}

fn levels(events: &[serde_json::Value]) -> Vec<&str> {
    events
        .iter()
        .map(|e| e["level"].as_str().unwrap_or(""))
        .collect()
}

/// The pod is gone: an answer, logged at `debug`, and nothing at `warn`.
///
/// KILLS: logging the 404 as a warning; treating it as any other failure.
#[tokio::test]
async fn a_pod_that_is_gone_is_an_answer_and_not_a_warning() {
    let (log, events, requests) = capture(404, failure(404, "NotFound")).await;
    assert_eq!(log.reason, RunnerReason::PodGone);
    assert!(log.body.is_empty());
    assert_eq!(requests, 1, "one read, no retry");
    assert_eq!(levels(&events), vec!["DEBUG"], "{events:?}");
}

/// A refused or failed read: an answer carrying the status, and ONE warning.
///
/// The CONTROL is the row above: the same read answering 404 warns nothing,
/// so the count asserted here is this failure's and not the harness's.
#[tokio::test]
async fn a_refused_or_failed_read_is_an_answer_and_one_warning() {
    for (code, reason) in [
        (403, "Forbidden"),
        (500, "InternalError"),
        (400, "BadRequest"),
    ] {
        let (log, events, requests) = capture(code, failure(code, reason)).await;
        assert_eq!(
            log.reason,
            RunnerReason::LogUnreadable { status: Some(code) },
            "HTTP {code}"
        );
        assert!(log.body.is_empty());
        assert_eq!(requests, 1, "HTTP {code}: one read, no retry");
        assert_eq!(levels(&events), vec!["WARN"], "HTTP {code}: {events:?}");
        assert_eq!(events[0]["fields"]["status"], code, "{events:?}");
        assert_eq!(events[0]["fields"]["pod"], POD);
    }
}

/// A log that was read: the reason, the body for the other tail scanners, and
/// no event at all. Whatever a read logs, it never logs the log.
#[tokio::test]
async fn a_read_logs_nothing_the_pod_wrote() {
    let detail = refusal_detail_line(RefusingRun::Restore, SENTENCE);
    let body = format!("{detail}\nrefusal-reason=GuardRefused\n");
    let (log, events, _) = capture(200, body.clone()).await;
    match &log.reason {
        // `SeededReason` is in no closed set, so it stays in the sentence.
        RunnerReason::Stated(d) => assert_eq!(d.to_string(), format!("GuardRefused: {SENTENCE}")),
        other => panic!("{other:?}"),
    }
    assert_eq!(log.body, body);
    assert!(events.is_empty(), "{events:?}");

    // A line that does not validate, a line that is not where the runner
    // prints it, and a tail over the bound: each is said at `debug`, and no
    // event quotes the line.
    let bad = format!(
        "refusal-detail={{\"code\":\"Bad Code\",\"message\":\"{SENTENCE}\"}}\n\
         refusal-reason=GuardRefused\n"
    );
    let (log, events, _) = capture(200, bad).await;
    assert_eq!(log.reason, RunnerReason::Unreadable);
    assert_eq!(levels(&events), vec!["DEBUG"]);
    assert!(!format!("{events:?}").contains("seeded-refusal-sentence"));

    let misplaced = format!("{detail}\nanother line\nrefusal-reason=GuardRefused\n");
    let (log, events, _) = capture(200, misplaced).await;
    assert_eq!(log.reason, RunnerReason::Misplaced);
    assert_eq!(levels(&events), vec!["DEBUG"]);
    assert!(!format!("{events:?}").contains("seeded-refusal-sentence"));

    let over = format!("{}\n{detail}\n", SENTENCE.repeat(12_000));
    let (log, events, _) = capture(200, over).await;
    assert_eq!(log.reason, RunnerReason::TailOverBound);
    assert!(log.body.is_empty());
    assert_eq!(levels(&events), vec!["DEBUG"]);
    assert!(!format!("{events:?}").contains("seeded-refusal-sentence"));
}
