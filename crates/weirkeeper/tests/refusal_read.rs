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
use logweir_core::refusal_detail::{refusal_detail_line, LineToken, RefusingRun};
use weirkeeper::refusal::{read, RefusalLog, RunnerReason};
use weirkeeper::testing::{mock_client_recording, Route};

const NS: &str = "logweir-fx34";
const POD: &str = "rst-refused-abcde";
/// A sentence no log line of the CONTROLLER may ever carry.
const SENTENCE: &str = "SeededReason: seeded-refusal-sentence-9d41 names a topic";

/// The line token of the Job these reads are for. Assembled at run time, so
/// no source line holds a secret-shaped literal.
fn token() -> LineToken {
    LineToken::parse(&"7f".repeat(20)).expect("forty hex digits")
}

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
    let seen = Arc::new(Mutex::new(None));
    let recorder = Arc::clone(&seen);
    let (log, events) = capture_over(move || {
        let (client, requests) = mock_client_recording(vec![Route {
            method: "GET",
            path_suffix: "/log",
            status,
            body,
        }]);
        *recorder.lock().unwrap() = Some(requests);
        client
    })
    .await;
    let requests = seen
        .lock()
        .unwrap()
        .as_ref()
        .expect("the client was built")
        .lock()
        .unwrap()
        .len();
    (log, events, requests)
}

/// One read through the client `make_client` builds, with every event the
/// read emitted, at every level, as JSON lines. The client is built AFTER the
/// subscriber is installed, so its worker task logs here too.
async fn capture_over<F>(make_client: F) -> (RefusalLog, Vec<serde_json::Value>)
where
    F: FnOnce() -> kube::Client,
{
    let sink = Captured::default();
    let writer = sink.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let client = make_client();
    let pods: Api<Pod> = Api::namespaced(client, NS);
    let log = read(&pods, NS, POD, RefusingRun::Restore, Some(&token())).await;
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
    (log, events)
}

/// How many requests a transport double was asked.
type Asked = Arc<Mutex<usize>>;

/// A client whose every request FAILS IN THE TRANSPORT: no HTTP status ever
/// exists. It is what a connection that drops before the answer looks like
/// to the reader (`kube` hands a transport error up as a non-`Api` error).
fn client_whose_connection_drops_before_the_status() -> (kube::Client, Asked) {
    let asked = Asked::default();
    let counter = Arc::clone(&asked);
    let service = tower::service_fn(move |_request: http::Request<kube::client::Body>| {
        let counter = Arc::clone(&counter);
        async move {
            *counter.lock().unwrap() += 1;
            Err::<http::Response<kube::client::Body>, std::io::Error>(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "the connection was reset before any response",
            ))
        }
    });
    (kube::Client::new(service, "default"), asked)
}

/// A client that answers the log read `200`, delivers `first` as the body's
/// first bytes, and then BREAKS THE STREAM with an error instead of ending
/// it.
fn client_whose_log_body_breaks_after(first: String) -> (kube::Client, Asked) {
    use futures::StreamExt as _;
    let asked = Asked::default();
    let counter = Arc::clone(&asked);
    let service = tower::service_fn(move |_request: http::Request<kube::client::Body>| {
        let counter = Arc::clone(&counter);
        let first = first.clone();
        async move {
            *counter.lock().unwrap() += 1;
            let delivered =
                http_body_util::BodyStream::new(kube::client::Body::from(first.into_bytes()))
                    .map(|frame| frame.map_err(|e| std::io::Error::other(e.to_string())));
            let then_breaks = futures::stream::once(async {
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "the body broke after some bytes",
                ))
            });
            let body = http_body_util::StreamBody::new(delivered.chain(then_breaks));
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(200)
                    .body(body)
                    .expect("a response"),
            )
        }
    });
    (kube::Client::new(service, "default"), asked)
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
/// no event at all. Whatever a read logs, it never logs the log, and it never
/// logs the Job's line token.
#[tokio::test]
async fn a_read_logs_nothing_the_pod_wrote_and_never_the_token() {
    let own = token();
    let leaked = |events: &[serde_json::Value]| {
        let text = format!("{events:?}");
        text.contains("seeded-refusal-sentence") || text.contains(own.expose_token())
    };
    let detail = refusal_detail_line(RefusingRun::Restore, Some(&own), SENTENCE);
    let body = format!("{detail}\nrefusal-reason=GuardRefused\n");
    let (log, events, _) = capture(200, body.clone()).await;
    match &log.reason {
        // `SeededReason` is in no closed set, so it stays in the sentence.
        RunnerReason::Stated(d) => assert_eq!(d.to_string(), format!("GuardRefused: {SENTENCE}")),
        other => panic!("{other:?}"),
    }
    assert_eq!(log.body, body);
    assert!(events.is_empty(), "{events:?}");
    assert!(
        !format!("{:?}", log.reason).contains(own.expose_token()),
        "the reason the read returns does not hold the token"
    );

    // A line with the token that does not validate, and a tail over the
    // bound: each is said at `debug`, and no event quotes the line or the
    // token.
    let bad = format!(
        "refusal-detail={{\"token\":\"{}\",\"code\":\"Bad Code\",\"message\":\"{SENTENCE}\"}}\n\
         refusal-reason=GuardRefused\n",
        own.expose_token()
    );
    let (log, events, _) = capture(200, bad).await;
    assert_eq!(log.reason, RunnerReason::Unreadable);
    assert_eq!(levels(&events), vec!["DEBUG"]);
    assert!(!leaked(&events), "{events:?}");

    let over = format!("{}\n{detail}\n", SENTENCE.repeat(12_000));
    let (log, events, _) = capture(200, over).await;
    assert_eq!(log.reason, RunnerReason::TailOverBound);
    assert!(log.body.is_empty());
    assert_eq!(levels(&events), vec!["DEBUG"]);
    assert!(!leaked(&events), "{events:?}");

    // A line WITHOUT the token is not this Job's: nothing is said about it at
    // all, at any level.
    let forged = format!(
        "{}\nrefusal-reason=GuardRefused\n",
        refusal_detail_line(RefusingRun::Restore, None, SENTENCE)
    );
    let (log, events, _) = capture(200, forged).await;
    assert_eq!(log.reason, RunnerReason::NotStated);
    assert!(events.is_empty(), "{events:?}");
}

/// **The token is in no line this controller logs**, whatever the read's
/// outcome: a pod that is gone, a read that is refused, a read that fails.
/// Each of those DOES log (the control: the event is there, with the pod's
/// name), and none carries the token.
#[tokio::test]
async fn no_log_line_of_a_read_holds_the_line_token() {
    let own = token();
    for (code, reason, level) in [
        (404, "NotFound", "DEBUG"),
        (403, "Forbidden", "WARN"),
        (500, "InternalError", "WARN"),
    ] {
        let (log, events, _) = capture(code, failure(code, reason)).await;
        assert!(log.body.is_empty());
        assert_eq!(levels(&events), vec![level], "HTTP {code}");
        assert_eq!(
            events[0]["fields"]["pod"], POD,
            "the control: the read did log"
        );
        let text = format!("{events:?} {:?}", log.reason);
        assert!(!text.contains(own.expose_token()), "HTTP {code}: {text}");
    }
}

/// What both rows below assert of a read that failed with NO HTTP status:
/// the answer, its sentence, one warning that names the pod and no status,
/// one request, and nothing of the log or the token in what was logged.
fn assert_unreadable_before_a_status(
    log: &RefusalLog,
    events: &[serde_json::Value],
    asked: &Asked,
    says: &str,
) {
    assert_eq!(
        log.reason,
        RunnerReason::LogUnreadable { status: None },
        "{events:?}"
    );
    assert!(
        log.body.is_empty(),
        "nothing of a read that failed is handed on"
    );
    assert_eq!(
        log.reason.message_suffix(),
        "; the runner's reason could not be read: the pod log read failed before an HTTP status"
    );
    assert_eq!(*asked.lock().unwrap(), 1, "one read, no retry");
    assert_eq!(levels(events), vec!["WARN"], "{events:?}");
    assert_eq!(events[0]["fields"]["pod"], POD);
    assert!(
        events[0]["fields"].get("status").is_none(),
        "there was no HTTP status to name: {events:?}"
    );
    let message = events[0]["fields"]["message"].as_str().unwrap_or_default();
    assert!(message.contains(says), "{message}");
    let text = format!("{events:?} {:?}", log.reason);
    assert!(
        !text.contains("seeded-refusal-sentence") && !text.contains(token().expose_token()),
        "no byte of the log and no token in what was logged: {text}"
    );
}

/// **A connection that drops before the status**: the read fails in the
/// transport, with no HTTP status at all. It is an answer
/// (`LogUnreadable { status: None }`), ONE warning, one request, and the
/// condition says "failed before an HTTP status".
///
/// The CONTROL is `a_pod_that_is_gone_is_an_answer_and_not_a_warning`: the
/// same read answered 404 is `PodGone` at `debug`, so what this row asserts
/// is this failure's and not every failure's.
///
/// KILLS: a transport failure reported as a pod that is gone (the review's
/// mutant R16); the failure returned as an error that fails the reconcile.
#[tokio::test]
async fn a_connection_that_drops_before_the_status_is_an_answer_and_one_warning() {
    let asked = Arc::new(Mutex::new(None));
    let keep = Arc::clone(&asked);
    let (log, events) = capture_over(move || {
        let (client, counter) = client_whose_connection_drops_before_the_status();
        *keep.lock().unwrap() = Some(counter);
        client
    })
    .await;
    let asked = asked.lock().unwrap().clone().expect("the client was built");
    assert_unreadable_before_a_status(&log, &events, &asked, "failed before an HTTP status");
}

/// **A 200 whose body breaks after some bytes**: the bytes that did arrive
/// are a COMPLETE, genuine pair carrying the Job's token, and then the stream
/// errors instead of ending. Nothing is taken from a body that did not end:
/// the answer is `LogUnreadable { status: None }`, ONE warning, one request.
///
/// The CONTROL is the first arm: the same bytes in a body that ENDS are
/// read, and the reason is shown. So what refuses them in the second arm is
/// the break, not the bytes.
///
/// KILLS: a broken stream reported as a pod that is gone (the review's mutant
/// R15); a broken stream read as a short log, which would show a reason from
/// a body nobody saw the end of.
#[tokio::test]
async fn a_log_body_that_breaks_after_some_bytes_is_an_answer_and_one_warning() {
    let detail = refusal_detail_line(RefusingRun::Restore, Some(&token()), SENTENCE);
    let pair = format!("{detail}\nrefusal-reason=GuardRefused\n");

    let (log, events, _) = capture(200, pair.clone()).await;
    assert!(
        matches!(log.reason, RunnerReason::Stated(_)),
        "the control: these bytes, in a body that ends, are a stated reason: {:?}",
        log.reason
    );
    assert!(events.is_empty(), "{events:?}");

    let asked = Arc::new(Mutex::new(None));
    let keep = Arc::clone(&asked);
    let (log, events) = capture_over(move || {
        let (client, counter) = client_whose_log_body_breaks_after(pair);
        *keep.lock().unwrap() = Some(counter);
        client
    })
    .await;
    let asked = asked.lock().unwrap().clone().expect("the client was built");
    assert_unreadable_before_a_status(&log, &events, &asked, "pod log stream broke");
}
