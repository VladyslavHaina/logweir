//! **FX-34.** Why a guard refused a run, read off its pod log and carried
//! into the terminal condition.
//!
//! # What was missing
//!
//! A `Restore` or a `Backup` whose runner exited 3 said "the runner exited 3
//! (guard-refused)" and nothing else. The runner's own sentence was only in
//! the pod log, and the pod goes with its Job's TTL (PoC batch 5, F-1). The
//! runner now prints that sentence as one `refusal-detail=` line
//! ([`logweir_core::refusal_detail`]); this module reads it.
//!
//! # A pod log is untrusted text
//!
//! The runner pod runs in the tenant's namespace. Its sentence interpolates
//! what the plan, the broker and the archive said, and in shared mode one
//! controller reads the logs of every namespace into statuses the console
//! shows. So:
//!
//! * **One line, by its key name** ([`runner_reason`]), from the same bounded
//!   tail every other key is read from
//!   ([`crate::controllers::backup::tail_lines`]). Never free text, never a
//!   position. The LAST such line is the one that counts: the runner prints
//!   its own line last, immediately before `refusal-reason=`, so a look-alike
//!   that anything printed earlier is superseded by it. If that last line
//!   does not validate nothing is shown; an earlier one is not fallen back
//!   to.
//! * **Validated and cleaned before it is stored.**
//!   [`RefusalDetail`](logweir_core::refusal_detail::RefusalDetail) cannot be
//!   built any other way: a reason code of ASCII letters and digits, a
//!   sentence reduced to printable characters, passed through the credential
//!   rules and cut to 760 bytes.
//! * **A bounded read, once** ([`read`]). [`REFUSAL_LOG_TAIL_LINES`] lines and
//!   [`REFUSAL_LOG_LIMIT_BYTES`] bytes, asked of the API server and enforced
//!   again on the stream. It is made on the one pass that writes the terminal
//!   status; a terminal object's pod is never read again (the reconcilers'
//!   STEP 2b).
//! * **A log that cannot be read is an answer, not an error.** The pod may
//!   already be collected (404), or the read may be refused or fail (403,
//!   500, a broken stream). Each becomes a [`RunnerReason`] the condition
//!   states in words; none fails the reconcile, so none is retried, and the
//!   exit code the pod reported is still recorded.
//! * **No new permission.** The read is the `pods/log` `get` the controller
//!   already holds for the evidence keys.
//!
//! # Only exit 3
//!
//! The reconcilers call [`read`] for exit 3 and for nothing else, and pass
//! the result to their patch builders as `Some`. Every other exit code takes
//! the read it always took and builds the message it always built.

use futures::io::AsyncRead;
use futures::AsyncReadExt as _;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, LogParams};
use tracing::{debug, warn};

use logweir_core::refusal_detail::{RefusalDetail, REFUSAL_DETAIL_PREFIX};

use crate::controllers::backup::tail_lines;

/// How many trailing log lines the exit-3 read asks for.
///
/// Twice [`crate::controllers::backup::KEY_SCAN_TAIL_LINES`]. The scan looks
/// at the final sixteen NON-EMPTY lines; the API counts every line, and CRI
/// stores a long line as several, so the read asks for twice as many. A
/// refused run prints its two key lines last, so nothing it owes is further
/// back than that.
pub const REFUSAL_LOG_TAIL_LINES: i64 = 32;

/// The byte bound on the exit-3 read: 512 KiB.
///
/// `limitBytes` counts from the START of the lines `tailLines` selected, so a
/// bound smaller than those lines would cut the END, where the keys are.
/// 512 KiB is [`REFUSAL_LOG_TAIL_LINES`] times the 16 KiB at which CRI splits
/// a container log line: on a default runtime the byte bound can never cut
/// the tail the line bound selected. On a runtime that stores longer lines it
/// can, and then nothing is read from the tail at all
/// ([`RunnerReason::TailOverBound`]).
pub const REFUSAL_LOG_LIMIT_BYTES: i64 = 512 * 1024;

/// How many bytes [`read`] takes off the stream before it stops, whatever the
/// server sends: the bound, plus one byte to see that the bound was passed.
/// The API documents that a server "may return slightly more" than
/// `limitBytes`; a body that reaches the bound is treated as cut either way.
const READ_CAP_BYTES: u64 = REFUSAL_LOG_LIMIT_BYTES as u64 + 1;

/// The `pods/log` parameters of the exit-3 read.
///
/// `container: Some("runner")`, as [`crate::check::relay::log_params`] names
/// it and for its reason: the exit code was read from the container of that
/// name, and `None` would read whatever container a pod with two happened to
/// default to.
#[must_use]
pub fn log_params() -> LogParams {
    LogParams {
        container: Some(crate::job::CONTAINER_NAME.to_string()),
        tail_lines: Some(REFUSAL_LOG_TAIL_LINES),
        limit_bytes: Some(REFUSAL_LOG_LIMIT_BYTES),
        ..LogParams::default()
    }
}

/// What an exit-3 run's pod log said about why, or why it said nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunnerReason {
    /// The runner's own reason code and sentence, validated and cleaned.
    Stated(RefusalDetail),
    /// The log was read and carries no `refusal-detail=` line: a runner that
    /// predates the line. The condition is exactly what it was before.
    NotStated,
    /// The last `refusal-detail=` line in the tail did not validate. Nothing
    /// from it is shown.
    Unreadable,
    /// The end of the log is longer than [`REFUSAL_LOG_LIMIT_BYTES`], so the
    /// lines that would carry the reason were not read.
    TailOverBound,
    /// The pod is gone: the log read answered 404.
    PodGone,
    /// The log read was refused or failed. `status` is the HTTP status, or
    /// `None` when the read failed without one (a transport error, a stream
    /// that broke).
    LogUnreadable {
        /// The API server's status code, when it answered.
        status: Option<u16>,
    },
}

impl RunnerReason {
    /// What the terminal condition's message says AFTER its "the runner
    /// exited 3 (guard-refused); …" text. Empty for [`Self::NotStated`], which
    /// is what keeps an older runner's condition byte-identical.
    ///
    /// A pure function of the observation, with no clock and no counter in
    /// it: a pass that recomputes the status of the same finished Job
    /// computes the same bytes, and the same bytes are not written twice.
    #[must_use]
    pub fn message_suffix(&self) -> String {
        match self {
            Self::Stated(detail) => {
                format!("; the runner's own reason, cleaned and bounded: {detail}")
            }
            Self::NotStated => String::new(),
            Self::Unreadable => format!(
                "; the runner gave no readable reason: its `{REFUSAL_DETAIL_PREFIX}` line did \
                 not validate, so nothing from it is shown"
            ),
            Self::TailOverBound => format!(
                "; the runner's reason could not be read: the last {REFUSAL_LOG_TAIL_LINES} \
                 lines of the pod log are over the {} KiB this controller reads",
                REFUSAL_LOG_LIMIT_BYTES / 1024
            ),
            Self::PodGone => {
                "; the runner's reason could not be read because the pod is gone".to_string()
            }
            Self::LogUnreadable {
                status: Some(status),
            } => format!(
                "; the runner's reason could not be read: the pod log read answered HTTP \
                 {status}"
            ),
            Self::LogUnreadable { status: None } => {
                "; the runner's reason could not be read: the pod log read failed before an \
                 HTTP status"
                    .to_string()
            }
        }
    }
}

/// The runner's reason, from a log body that WAS read whole.
///
/// The last line of the bounded tail that opens with
/// [`REFUSAL_DETAIL_PREFIX`] decides: [`RunnerReason::Stated`] when it
/// validates, [`RunnerReason::Unreadable`] when it does not, and
/// [`RunnerReason::NotStated`] when there is no such line. A line that merely
/// contains the key (inside a tracing line, after other text) is not one.
#[must_use]
pub fn runner_reason(log: &str) -> RunnerReason {
    let mut last = None;
    for line in tail_lines(log) {
        if line.starts_with(REFUSAL_DETAIL_PREFIX) {
            last = Some(line);
        }
    }
    match last {
        None => RunnerReason::NotStated,
        Some(line) => {
            RefusalDetail::from_line(line).map_or(RunnerReason::Unreadable, RunnerReason::Stated)
        }
    }
}

/// What [`read`] returns: the log body the other tail scanners read, and the
/// runner's reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefusalLog {
    /// The bounded log tail, decoded lossily. EMPTY whenever the reason is
    /// not [`RunnerReason::Stated`], [`RunnerReason::NotStated`] or
    /// [`RunnerReason::Unreadable`]: a log that was not read, or not read to
    /// its end, is not scanned for anything.
    pub body: String,
    /// The runner's reason, or why there is none.
    pub reason: RunnerReason,
}

impl RefusalLog {
    fn without_body(reason: RunnerReason) -> Self {
        Self {
            body: String::new(),
            reason,
        }
    }
}

/// The bytes a log read returned, as the [`RefusalLog`] they mean — **pure**.
///
/// A body that reaches [`REFUSAL_LOG_LIMIT_BYTES`] was cut, by the server or
/// by [`read`]'s own cap, and a cut body ends somewhere before the lines the
/// runner printed last. Nothing is taken from it: the last line it shows is
/// not the last line of the log, and "the last one counts" is the rule every
/// key is read by. Bytes that are not UTF-8 decode to `U+FFFD` and the read
/// goes on, so one stray byte elsewhere in the tail does not cost the reason.
#[must_use]
pub fn interpret(bytes: &[u8]) -> RefusalLog {
    if bytes.len() as u64 >= REFUSAL_LOG_LIMIT_BYTES as u64 {
        return RefusalLog::without_body(RunnerReason::TailOverBound);
    }
    let body = String::from_utf8_lossy(bytes).into_owned();
    let reason = runner_reason(&body);
    RefusalLog { body, reason }
}

/// At most [`READ_CAP_BYTES`] bytes off `reader`, and not one more is asked
/// of it.
///
/// THE SECOND BOUND, AND THE ONE THIS PROCESS ENFORCES ITSELF. `limitBytes` is
/// a request; this is what stops a server that ignored it, or a stream that
/// never ends, from growing a buffer in a controller that serves every
/// namespace. The rest of the stream is dropped unread.
///
/// # Errors
///
/// The reader's own error.
pub async fn read_capped<R: AsyncRead + Unpin>(reader: R) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(READ_CAP_BYTES).read_to_end(&mut bytes).await?;
    Ok(bytes)
}

/// Read a finished exit-3 run's bounded log tail and say what it means.
///
/// # Errors
///
/// Never. Every failure of the read is a [`RunnerReason`]: the caller records
/// the exit code it already has and the condition says in words why the
/// reason is missing. A reconcile that failed here would be retried, read the
/// same log again and fail again, with the object stuck in `Running` over a
/// finished Job; that is the loop this function exists not to start.
///
/// # What is logged
///
/// The pod's name and the HTTP status, never a byte of the log. A pod that is
/// gone is `debug`: Kubernetes collecting a finished pod is not news (FX-19).
/// A refused or failed read is ONE `warn`, because an operator has something
/// to fix; the pass that logs it writes the terminal status, so it is not
/// logged again.
pub async fn read(pods: &Api<Pod>, namespace: &str, pod_name: &str) -> RefusalLog {
    let stream = match pods.log_stream(pod_name, &log_params()).await {
        Ok(stream) => stream,
        Err(kube::Error::Api(response)) if response.code == 404 => {
            debug!(
                namespace,
                pod = pod_name,
                "the refused run's pod is gone, so its reason cannot be read; the exit code is \
                 recorded and the condition says so"
            );
            return RefusalLog::without_body(RunnerReason::PodGone);
        }
        Err(kube::Error::Api(response)) => {
            warn!(
                namespace,
                pod = pod_name,
                status = response.code,
                "the refused run's pod log could not be read; the exit code is recorded and the \
                 condition says the reason could not be read. It is not read again"
            );
            return RefusalLog::without_body(RunnerReason::LogUnreadable {
                status: Some(response.code),
            });
        }
        Err(error) => {
            warn!(
                namespace,
                pod = pod_name,
                %error,
                "the refused run's pod log read failed before an HTTP status; the exit code is \
                 recorded and the condition says the reason could not be read. It is not read \
                 again"
            );
            return RefusalLog::without_body(RunnerReason::LogUnreadable { status: None });
        }
    };
    let bytes = match read_capped(Box::pin(stream)).await {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(
                namespace,
                pod = pod_name,
                %error,
                "the refused run's pod log stream broke; the exit code is recorded and the \
                 condition says the reason could not be read. It is not read again"
            );
            return RefusalLog::without_body(RunnerReason::LogUnreadable { status: None });
        }
    };
    let log = interpret(&bytes);
    match &log.reason {
        RunnerReason::Unreadable => debug!(
            namespace,
            pod = pod_name,
            "the refused run's `refusal-detail=` line did not validate; nothing from it is stored"
        ),
        RunnerReason::TailOverBound => debug!(
            namespace,
            pod = pod_name,
            limit_bytes = REFUSAL_LOG_LIMIT_BYTES,
            "the end of the refused run's pod log is over the byte bound; nothing is read from it"
        ),
        _ => {}
    }
    log
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detail_line(code: &str, message: &str) -> String {
        format!(
            "{REFUSAL_DETAIL_PREFIX}{}",
            serde_json::json!({ "code": code, "message": message })
        )
    }

    #[test]
    fn the_last_detail_line_in_the_tail_decides() {
        let first = detail_line("LibraryNoise", "printed by something else, earlier");
        let runner = detail_line("GuardRefused", "the runner's own sentence");
        let log = format!("{first}\nsome line\n{runner}\nrefusal-reason=GuardRefused\n");
        match runner_reason(&log) {
            RunnerReason::Stated(d) => {
                assert_eq!(d.code(), "GuardRefused");
                assert_eq!(d.message(), "the runner's own sentence");
            }
            other => panic!("{other:?}"),
        }
        // An INVALID last line is not rescued by a valid earlier one.
        let bad = format!("{REFUSAL_DETAIL_PREFIX}not json");
        let log = format!("{first}\n{bad}\nrefusal-reason=GuardRefused\n");
        assert_eq!(runner_reason(&log), RunnerReason::Unreadable);
    }

    #[test]
    fn only_a_line_that_opens_with_the_key_is_the_line() {
        let inside = format!(
            "{{\"level\":\"INFO\",\"message\":\"{}\"}}",
            "refusal-detail={\\\"code\\\":\\\"A\\\",\\\"message\\\":\\\"m\\\"}"
        );
        let indented = format!(" {}", detail_line("A", "m"));
        for log in [
            inside,
            indented,
            "refusal-reason=GuardRefused\n".to_string(),
            String::new(),
        ] {
            assert_eq!(runner_reason(&log), RunnerReason::NotStated, "{log}");
        }
    }

    #[test]
    fn a_line_outside_the_scanned_tail_is_not_read() {
        let stale = detail_line("Stale", "scrolled out of the window");
        let filler = "a later line\n".repeat(crate::controllers::backup::KEY_SCAN_TAIL_LINES);
        assert_eq!(
            runner_reason(&format!("{stale}\n{filler}")),
            RunnerReason::NotStated
        );
    }

    #[test]
    fn a_body_at_the_byte_bound_is_not_scanned() {
        let line = detail_line("GuardRefused", "a sentence");
        let mut body = "x".repeat(REFUSAL_LOG_LIMIT_BYTES as usize - line.len() - 2);
        body.push('\n');
        body.push_str(&line);
        // One byte under the bound: read.
        assert_eq!(body.len() as i64, REFUSAL_LOG_LIMIT_BYTES - 1);
        assert!(matches!(
            interpret(body.as_bytes()).reason,
            RunnerReason::Stated(_)
        ));
        // At the bound: cut, so nothing is taken from it, the body included.
        body.push('\n');
        let cut = interpret(body.as_bytes());
        assert_eq!(cut.reason, RunnerReason::TailOverBound);
        assert!(cut.body.is_empty());
    }

    /// The reader is asked for the bound plus one byte and for nothing after
    /// it: a source eight times that size is left with seven eighths unread.
    ///
    /// KILLS: reading the stream to its end (the source is drained, and the
    /// buffer is eight times the bound).
    #[tokio::test]
    async fn the_stream_is_read_to_the_bound_and_no_further() {
        let cap = REFUSAL_LOG_LIMIT_BYTES as u64 + 1;
        let mut source = futures::io::repeat(b'x').take(8 * cap);
        let bytes = read_capped(&mut source)
            .await
            .expect("a repeat never fails");
        assert_eq!(bytes.len() as u64, cap);
        assert_eq!(source.limit(), 7 * cap, "the rest was never pulled");
        assert_eq!(
            interpret(&bytes).reason,
            RunnerReason::TailOverBound,
            "and what was read is a cut body, from which nothing is taken"
        );
        // A source under the bound is read whole.
        let mut small = futures::io::repeat(b'x').take(1000);
        assert_eq!(read_capped(&mut small).await.expect("reads").len(), 1000);
    }

    #[test]
    fn invalid_utf8_does_not_cost_the_reason_and_is_never_stored_raw() {
        // A stray byte elsewhere in the tail.
        let mut bytes = b"engine said \xFF\xFE\n".to_vec();
        bytes.extend_from_slice(detail_line("GuardRefused", "a sentence").as_bytes());
        bytes.push(b'\n');
        assert!(matches!(interpret(&bytes).reason, RunnerReason::Stated(_)));
        // Inside the sentence itself: one replacement character.
        let mut bytes =
            format!("{REFUSAL_DETAIL_PREFIX}{{\"code\":\"A\",\"message\":\"bad ").into_bytes();
        bytes.extend_from_slice(b"\xFF\xC0 bytes\"}\n");
        match interpret(&bytes).reason {
            RunnerReason::Stated(d) => assert_eq!(d.message(), "bad \u{FFFD} bytes"),
            other => panic!("{other:?}"),
        }
        // Inside the code: the line does not validate.
        let mut bytes = format!("{REFUSAL_DETAIL_PREFIX}{{\"code\":\"A").into_bytes();
        bytes.extend_from_slice(b"\xFF\",\"message\":\"m\"}\n");
        assert_eq!(interpret(&bytes).reason, RunnerReason::Unreadable);
    }

    #[test]
    fn every_suffix_is_one_bounded_line_and_only_a_stated_reason_carries_log_text() {
        let stated = RunnerReason::Stated(
            RefusalDetail::from_refusal_message("StorageRegionInvalid: a sentence").unwrap(),
        );
        assert_eq!(
            stated.message_suffix(),
            "; the runner's own reason, cleaned and bounded: StorageRegionInvalid: a sentence"
        );
        assert_eq!(RunnerReason::NotStated.message_suffix(), "");
        for reason in [
            RunnerReason::Unreadable,
            RunnerReason::TailOverBound,
            RunnerReason::PodGone,
            RunnerReason::LogUnreadable { status: Some(403) },
            RunnerReason::LogUnreadable { status: Some(500) },
            RunnerReason::LogUnreadable { status: None },
        ] {
            let suffix = reason.message_suffix();
            assert!(suffix.starts_with("; the runner"), "{suffix}");
            assert!(!suffix.contains('\n') && suffix.len() < 200, "{suffix}");
        }
        assert!(RunnerReason::PodGone
            .message_suffix()
            .ends_with("because the pod is gone"));
        assert!(RunnerReason::LogUnreadable { status: Some(403) }
            .message_suffix()
            .ends_with("HTTP 403"));
    }
}
