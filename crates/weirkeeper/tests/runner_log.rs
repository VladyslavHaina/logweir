//! **PROD-17.1 B4**: the one runner-log read ([`weirkeeper::runner_log`])
//! asks for the log's TAIL, so a key at the end of a long log is found.
//!
//! The double answers `pods/log` as the kubelet does: `tailLines` selects the
//! last lines, and `limitBytes` keeps the FIRST bytes of what was selected.

use std::sync::{Arc, Mutex};

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, LogParams};
use logweir_core::refusal_detail::{refusal_detail_line, LineToken, RefusingRun};
use weirkeeper::refusal::{self, RunnerReason};
use weirkeeper::runner_log;

const NS: &str = "logweir-b4";
const POD: &str = "rst-long-log-abcde";
const LINES: usize = 100_000;
const KEY: &str = "scorecard-key=";
const VALUE: &str = "logweir/drills/r1.json";

/// A 100,000-line log of about 10 MB whose LAST line is `last`.
fn long_log(last: &str) -> String {
    let mut log = String::with_capacity(LINES * 100);
    for i in 0..LINES - 1 {
        log.push_str(&format!(
            "{i:08} engine: copied one more batch of records to the target topic, offsets \
             committed\n"
        ));
    }
    log.push_str(last);
    log.push('\n');
    log
}

/// What the kubelet sends for `query`: the last `tailLines` lines, cut to the
/// first `limitBytes` bytes.
fn kubelet(log: &str, query: &str) -> Vec<u8> {
    let param = |name: &str| {
        query.split('&').find_map(|p| {
            p.strip_prefix(name)?
                .strip_prefix('=')?
                .parse::<usize>()
                .ok()
        })
    };
    let lines: Vec<&str> = log.split_inclusive('\n').collect();
    let start = param("tailLines").map_or(0, |n| lines.len().saturating_sub(n));
    let mut body = lines[start..].concat().into_bytes();
    if let Some(limit) = param("limitBytes") {
        body.truncate(limit);
    }
    body
}

type Asked = Arc<Mutex<Vec<String>>>;

/// A client whose `pods/log` answers as [`kubelet`] does, and the queries it
/// was asked.
fn kubelet_client(log: String) -> (kube::Client, Asked) {
    let asked = Asked::default();
    let seen = Arc::clone(&asked);
    let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
        let query = request.uri().query().unwrap_or_default().to_string();
        seen.lock().unwrap().push(query.clone());
        let body = kubelet(&log, &query);
        async move {
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .status(200)
                    .body(kube::client::Body::from(body))
                    .expect("a response"),
            )
        }
    });
    (kube::Client::new(service, "default"), asked)
}

/// **A key on the last line of a 100,000-line log (about 10 MB, over every
/// byte bound) is found** through the shared read, under each bound a key is
/// read with: the terminal read of a `Backup`, `Restore` or `KafkaCluster`
/// probe, the progress read, and FX-34's exit-3 read.
///
/// The CONTROL is the same double read for its HEAD (`limitBytes` and no
/// `tailLines`, the shape of the check relay's read before the fold): the
/// key is not in what comes back, so this double tells a head from a tail.
///
/// KILLS: a bound sent without `tailLines` (the progress and exit-3 arms get
/// the log's first bytes); a tail taken from the start of the body (the
/// terminal arm).
#[tokio::test]
async fn a_key_on_the_last_line_of_a_long_log_is_found() {
    let log = long_log(&format!("{KEY}{VALUE}"));
    assert!(log.len() > 8 * 1024 * 1024, "{} bytes", log.len());
    for (name, bound) in [
        ("terminal", runner_log::TERMINAL),
        ("progress", runner_log::PROGRESS),
    ] {
        let (client, asked) = kubelet_client(log.clone());
        let pods: Api<Pod> = Api::namespaced(client, NS);
        let text = runner_log::read_text(&pods, POD, &bound)
            .await
            .expect("the read answers");
        assert_eq!(runner_log::key_values(&text, KEY), vec![VALUE], "[{name}]");
        let asked = asked.lock().unwrap().clone();
        assert!(
            asked[0].contains(&format!("tailLines={}", bound.lines)),
            "[{name}] {asked:?}"
        );
    }

    // FX-34's exit-3 read: the runner's own pair is the log's last two lines.
    let token = LineToken::parse(&"7f".repeat(20)).expect("forty hex digits");
    let sentence = "restore.partitions.orders names partition 99";
    let pair = format!(
        "{}\n{}",
        refusal_detail_line(RefusingRun::Restore, Some(&token), sentence),
        logweir_core::guard::refusal_reason_line(sentence)
    );
    let (client, _) = kubelet_client(long_log(pair.trim_end()));
    let pods: Api<Pod> = Api::namespaced(client, NS);
    let read = refusal::read(&pods, NS, POD, RefusingRun::Restore, Some(&token)).await;
    assert!(
        matches!(read.reason, RunnerReason::Stated(_)),
        "{:?}",
        read.reason
    );

    // CONTROL: a reader that reads the head does not find the key.
    let (client, _) = kubelet_client(log);
    let pods: Api<Pod> = Api::namespaced(client, NS);
    let head = pods
        .logs(
            POD,
            &LogParams {
                limit_bytes: Some(512 * 1024),
                ..LogParams::default()
            },
        )
        .await
        .expect("the read answers");
    assert!(
        runner_log::key_values(&head, KEY).is_empty(),
        "the head of the log does not hold its last line"
    );
}

/// **The check relay and a retention Job's lines are still read whole.** A
/// 100,000-line log within [`RELAY_LIMIT_BYTES`] comes back byte for byte,
/// as it did when that read asked for no `tailLines`: the bound asks for as
/// many lines as it has bytes, and a log has no more lines than bytes.
///
/// KILLS: a relay bound whose `tailLines` is smaller than the line count of a
/// log that fits its byte bound (the frames' first lines would be cut).
///
/// [`RELAY_LIMIT_BYTES`]: weirkeeper::check::relay::RELAY_LIMIT_BYTES
#[tokio::test]
async fn a_log_within_the_relay_bound_is_read_whole() {
    let mut log: String = (0..LINES - 1).map(|i| format!("{i}\n")).collect();
    log.push_str(&format!("{KEY}{VALUE}\n"));
    let (client, asked) = kubelet_client(log.clone());
    let pods: Api<Pod> = Api::namespaced(client, NS);
    let text = runner_log::read_text(&pods, POD, &runner_log::RELAY)
        .await
        .expect("the read answers");
    assert!(
        text == log,
        "{} of the log's {} bytes came back",
        text.len(),
        log.len()
    );
    let asked = asked.lock().unwrap().clone();
    assert!(asked[0].contains("container=runner"), "{asked:?}");
}
