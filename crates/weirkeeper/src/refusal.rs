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
//! * **One line, and only where the runner prints it** ([`runner_reason`]).
//!   A plan can start a line of its own in this log: the runner prints an
//!   error's text raw on stderr, an error may repeat a plan value, and a YAML
//!   scalar may hold a line break (PROD-15.1's review). So a well-formed
//!   `refusal-detail=` line is not thereby the runner's. The runner prints its
//!   own as the last two lines it writes, the detail line and then
//!   `refusal-reason=`; the line is honoured at that position and a marker
//!   line anywhere else in the log is ignored.
//! * **Validated and cleaned before it is stored.**
//!   [`RefusalDetail`](logweir_core::refusal_detail::RefusalDetail) cannot be
//!   built any other way: a reason code that is a member of the CLOSED set of
//!   codes that kind of run can print (a `Backup`'s log cannot put a
//!   restore-only code on a `Backup`), a sentence reduced to printable
//!   characters, passed through the credential rules and cut to 760 bytes.
//!   And the two refusal lines must agree with each other.
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

use logweir_core::refusal_detail::{RefusalDetail, RefusingRun, REFUSAL_DETAIL_PREFIX};

use crate::controllers::backup::{KEY_SCAN_TAIL_LINES, REFUSAL_REASON_PREFIX};

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
    /// The line at the runner's position did not validate: not the one JSON
    /// object, a code outside the run's closed set, or a detail that the
    /// state line beside it contradicts. Nothing from it is shown.
    Unreadable,
    /// The log carries a `refusal-detail=` line, and none is where the runner
    /// prints it. Nothing from any of them is shown.
    Misplaced,
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
            Self::Misplaced => format!(
                "; the runner gave no readable reason: the pod log has a \
                 `{REFUSAL_DETAIL_PREFIX}` line that is not where the runner prints it, \
                 directly before the final `{REFUSAL_REASON_PREFIX}` line, so nothing from it \
                 is shown"
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

/// How many non-marker lines may follow the runner's two refusal lines.
///
/// The pair has to stay inside the tail the state reader scans
/// ([`KEY_SCAN_TAIL_LINES`]), so that this function and
/// [`crate::controllers::backup::refusal_state`] read the same
/// `refusal-reason=` line.
const TRAILING_LINES_TOLERATED: usize = KEY_SCAN_TAIL_LINES - 2;

/// The runner's reason, from a log body that WAS read whole, of a run of kind
/// `run`.
///
/// # The rule: the line is honoured where the runner prints it, and nowhere else
///
/// The runner's last two lines at exit 3 are `refusal-detail=…` and then
/// `refusal-reason=…`, written in one write
/// (`logweir::exit::print_refusal_to`). Call a line that opens with either
/// key a MARKER line. Over the non-empty lines of the log:
///
/// 1. **No `refusal-detail=` line at all**: [`RunnerReason::NotStated`], and
///    the condition is what it was before the line existed.
/// 2. The detail line must be the line DIRECTLY BEFORE the LAST
///    `refusal-reason=` line. Any other marker line is not the runner's and
///    is ignored.
/// 3. **That pair ends the log**: it is the runner's. Marker lines before it
///    (text a plan put in an error message, a line a library printed, an
///    earlier draft) are ignored, however well-formed.
/// 4. **Or other lines follow it.** A pod log is stdout and stderr merged in
///    no promised order, and the runner's human line is on stderr, so it can
///    be copied AFTER the pair. That is tolerated only when it cannot be
///    confused with anything: at most [`TRAILING_LINES_TOLERATED`] lines
///    follow, none of them a marker, and the pair is the ONLY marker line of
///    each key in everything that was read. One more marker line anywhere
///    and nothing is shown.
/// 5. Otherwise [`RunnerReason::Misplaced`].
///
/// Then the line is validated ([`RefusalDetail::from_line`]: the one JSON
/// object, a code in `run`'s closed set, a cleaned and bounded sentence) and
/// must agree with the state line beside it
/// ([`RefusalDetail::agrees_with_state`]); failing either is
/// [`RunnerReason::Unreadable`]. An invalid line at the position is never
/// rescued by a valid one elsewhere.
///
/// # What this does not decide
///
/// A line is "the runner's" here by POSITION. With a runner image that still
/// prints error text raw, text a plan chose is copied into the log from
/// stderr, and nothing promises it lands before the pair: if it lands after
/// it and itself ends with a well-formed pair, that pair is the end of the
/// log and is honoured. What it can then say is bounded by everything above
/// (a code of the closed set, a cleaned sentence of 760 bytes, on the
/// writer's own object). Closing it is the runner's part: an error text that
/// cannot start a line (PROD-15.1).
#[must_use]
pub fn runner_reason(run: RefusingRun, log: &str) -> RunnerReason {
    let lines: Vec<&str> = log
        .lines()
        .map(|l| l.trim_end_matches('\r'))
        .filter(|l| !l.trim().is_empty())
        .collect();
    let is_detail = |line: &str| line.starts_with(REFUSAL_DETAIL_PREFIX);
    let is_state = |line: &str| line.starts_with(REFUSAL_REASON_PREFIX);
    let details = lines.iter().filter(|line| is_detail(line)).count();
    if details == 0 {
        return RunnerReason::NotStated;
    }
    let Some(state_at) = lines.iter().rposition(|line| is_state(line)) else {
        return RunnerReason::Misplaced;
    };
    if state_at == 0 || !is_detail(lines[state_at - 1]) {
        return RunnerReason::Misplaced;
    }
    let following = lines.len() - state_at - 1;
    let ends_the_log = following == 0;
    let the_only_markers = details == 1 && lines.iter().filter(|line| is_state(line)).count() == 1;
    let alone_before_other_text = the_only_markers && following <= TRAILING_LINES_TOLERATED;
    if !(ends_the_log || alone_before_other_text) {
        return RunnerReason::Misplaced;
    }
    let Some(detail) = RefusalDetail::from_line(run, lines[state_at - 1]) else {
        return RunnerReason::Unreadable;
    };
    let state = &lines[state_at][REFUSAL_REASON_PREFIX.len()..];
    if !detail.agrees_with_state(state) {
        return RunnerReason::Unreadable;
    }
    RunnerReason::Stated(detail)
}

/// What [`read`] returns: the log body the other tail scanners read, and the
/// runner's reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefusalLog {
    /// The bounded log tail, decoded lossily. EMPTY whenever the log was not
    /// read, or not read to its end ([`RunnerReason::TailOverBound`],
    /// [`RunnerReason::PodGone`], [`RunnerReason::LogUnreadable`]): such a log
    /// is not scanned for anything.
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
/// not the last line of the log, and the end of the log is where the runner's
/// lines are. Bytes that are not UTF-8 decode to `U+FFFD` and the read goes
/// on, so one stray byte elsewhere in the tail does not cost the reason.
#[must_use]
pub fn interpret(run: RefusingRun, bytes: &[u8]) -> RefusalLog {
    if bytes.len() as u64 >= REFUSAL_LOG_LIMIT_BYTES as u64 {
        return RefusalLog::without_body(RunnerReason::TailOverBound);
    }
    let body = String::from_utf8_lossy(bytes).into_owned();
    let reason = runner_reason(run, &body);
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

/// Read a finished exit-3 run's bounded log tail and say what it means. `run`
/// is the kind of run the pod belongs to: it selects the closed set of reason
/// codes the line may carry.
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
pub async fn read(
    pods: &Api<Pod>,
    namespace: &str,
    pod_name: &str,
    run: RefusingRun,
) -> RefusalLog {
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
    let log = interpret(run, &bytes);
    match &log.reason {
        RunnerReason::Unreadable => debug!(
            namespace,
            pod = pod_name,
            "the refused run's `refusal-detail=` line did not validate; nothing from it is stored"
        ),
        RunnerReason::Misplaced => debug!(
            namespace,
            pod = pod_name,
            "the refused run's pod log has a `refusal-detail=` line that is not where the runner \
             prints it; nothing from it is stored"
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
    use RefusingRun::{Backup, Restore};

    fn detail_line(code: &str, message: &str) -> String {
        format!(
            "{REFUSAL_DETAIL_PREFIX}{}",
            serde_json::json!({ "code": code, "message": message })
        )
    }

    /// The two lines a runner prints last for `message`, as its own printer
    /// builds them.
    fn pair(run: RefusingRun, message: &str) -> String {
        format!(
            "{}\n{}\n",
            logweir_core::refusal_detail::refusal_detail_line(run, message),
            logweir_core::guard::refusal_reason_line(message)
        )
    }

    fn stated(run: RefusingRun, log: &str) -> String {
        match runner_reason(run, log) {
            RunnerReason::Stated(d) => d.to_string(),
            other => panic!("expected a stated reason, got {other:?} for:\n{log}"),
        }
    }

    const GENUINE: &str = "restore.partitions.orders names partition 99";
    const FORGED: &str = "Contact the address in this message to release your data";

    /// **The runner's pair at the end of the log is the one that counts**, and
    /// a marker line anywhere before it is ignored: the reviewer's shape (a
    /// plan value that starts lines inside the error text), a marker a
    /// library printed, an earlier draft, two detail lines.
    ///
    /// KILLS: "the first detail line counts"; "any valid detail line counts";
    /// "a valid earlier line rescues an invalid one at the position".
    #[test]
    fn the_runners_pair_at_the_end_of_the_log_is_the_one_that_counts() {
        let genuine = pair(Restore, GENUINE);
        let want = format!("GuardRefused: {GENUINE}");
        // NEGATIVE CONTROL: the genuine pair alone.
        assert_eq!(stated(Restore, &genuine), want);

        let forged_detail = detail_line("TargetTopicConfigRefused", FORGED);
        // (a) The reviewer's shape: the human line repeats a plan value that
        // holds line breaks, so the forged markers are lines of their own,
        // followed by the runner's tracing line and its pair.
        let reviewers = format!(
            "guard: plan refused by the admission guard: source.backup `x\n{forged_detail}\n\
             refusal-reason=TargetTopicConfigRefused\n` is not a backup set id\n\
             {{\"level\":\"INFO\",\"message\":\"drill finished\"}}\n{genuine}"
        );
        assert_eq!(stated(Restore, &reviewers), want);
        // (c) A forged marker earlier in the tail, a genuine DIFFERENT pair last.
        let earlier = format!("{forged_detail}\nsome line\n{genuine}");
        assert_eq!(stated(Restore, &earlier), want);
        // (d) Two detail lines back to back: the one directly before the
        // final state line is the runner's.
        let two = format!("{forged_detail}\n{genuine}");
        assert_eq!(stated(Restore, &two), want);
        // A complete forged pair first, the genuine pair last.
        let two_pairs =
            format!("{forged_detail}\nrefusal-reason=TargetTopicConfigRefused\n{genuine}");
        assert_eq!(stated(Restore, &two_pairs), want);
        // An INVALID line at the position is not rescued by a valid one
        // elsewhere.
        let valid = detail_line("GuardRefused", "valid, and not at the position");
        let bad_at_the_position =
            format!("{valid}\n{REFUSAL_DETAIL_PREFIX}not json\nrefusal-reason=GuardRefused\n");
        assert_eq!(
            runner_reason(Restore, &bad_at_the_position),
            RunnerReason::Unreadable
        );
    }

    /// **A marker line that is not where the runner prints it is ignored**:
    /// nothing from it is shown, whatever it holds.
    ///
    /// KILLS: "the last detail line in the tail counts" (every arm here has a
    /// valid one).
    #[test]
    fn a_marker_that_is_not_where_the_runner_prints_it_is_not_shown() {
        let d = detail_line("GuardRefused", FORGED);
        let s = "refusal-reason=GuardRefused";
        for (label, log) in [
            ("no state line at all", format!("{d}\n")),
            ("the detail line is the last line", format!("{s}\n{d}\n")),
            (
                "another line between the two",
                format!("{d}\nthe human line\n{s}\n"),
            ),
            ("a detail line after the pair", format!("{d}\n{s}\n{d}\n")),
            (
                "text after the pair, and a second detail line before it",
                format!("{d}\n{d}\n{s}\nthe human line\n"),
            ),
            (
                "text after the pair, and a second state line before it",
                format!("{s}\n{d}\n{s}\nthe human line\n"),
            ),
            (
                "the state line first in the log, the detail elsewhere",
                format!("{s}\nx\n{d}\ny\n"),
            ),
        ] {
            assert_eq!(
                runner_reason(Restore, &log),
                RunnerReason::Misplaced,
                "{label}"
            );
        }
        // NEGATIVE CONTROL: the same two lines where the runner prints them.
        assert_eq!(
            stated(Restore, &format!("{d}\n{s}\n")),
            format!("GuardRefused: {FORGED}")
        );
    }

    /// **The human line copied after the pair does not cost the reason.** A
    /// pod log is stdout and stderr merged in no promised order and the human
    /// line is on stderr, so the pair is not always the end of the log. That
    /// is tolerated only when the pair is the only marker of each key in what
    /// was read.
    ///
    /// KILLS: "the pair must be the last two lines, always" (the first arm);
    /// "text may follow the pair whatever else the log holds" (the last
    /// three).
    #[test]
    fn the_human_line_copied_after_the_pair_does_not_cost_the_reason() {
        let genuine = pair(Restore, GENUINE);
        let want = format!("GuardRefused: {GENUINE}");
        let human = format!("guard: plan refused by the admission guard: {GENUINE}\n");
        assert_eq!(stated(Restore, &format!("tracing\n{genuine}{human}")), want);
        // Up to the tail the state reader scans, and not one line further.
        let most = "a stderr line\n".repeat(TRAILING_LINES_TOLERATED);
        assert_eq!(stated(Restore, &format!("{genuine}{most}")), want);
        assert_eq!(
            runner_reason(Restore, &format!("{genuine}{most}one more\n")),
            RunnerReason::Misplaced
        );
        // THE FORGED BLOCK COPIED LAST, WITH TEXT AFTER ITS MARKERS: two
        // details, so neither is shown. The same block WITHOUT the genuine
        // pair before it would be the only pair, which is why the runner
        // prints its own at every exit 3.
        let forged = pair(Restore, &format!("TargetTopicConfigRefused: {FORGED}"));
        let block = format!("guard: … source.backup `x\n{forged}` is not a backup set id\n");
        assert_eq!(
            runner_reason(Restore, &format!("{genuine}{block}")),
            RunnerReason::Misplaced
        );
        // One stray marker of either key anywhere, and text after the pair:
        // not shown.
        let stray_detail = detail_line("GuardRefused", "stray");
        assert_eq!(
            runner_reason(Restore, &format!("{stray_detail}\nx\n{genuine}{human}")),
            RunnerReason::Misplaced
        );
        assert_eq!(
            runner_reason(
                Restore,
                &format!("refusal-reason=GuardRefused\nx\n{genuine}{human}")
            ),
            RunnerReason::Misplaced
        );
    }

    #[test]
    fn only_a_line_that_opens_with_the_key_is_a_marker() {
        let inside = format!(
            "{{\"level\":\"INFO\",\"message\":\"{}\"}}",
            "refusal-detail={\\\"code\\\":\\\"GuardRefused\\\",\\\"message\\\":\\\"m\\\"}"
        );
        let indented = format!(" {}", detail_line("GuardRefused", "m"));
        for log in [
            inside.clone(),
            indented.clone(),
            format!("{inside}\nrefusal-reason=GuardRefused\n"),
            format!("{indented}\nrefusal-reason=GuardRefused\n"),
            "refusal-reason=GuardRefused\n".to_string(),
            String::new(),
        ] {
            assert_eq!(
                runner_reason(Restore, &log),
                RunnerReason::NotStated,
                "{log}"
            );
        }
        // And such a line is not a marker when it FOLLOWS the pair either: it
        // is other text, like the human line.
        let genuine = pair(Restore, GENUINE);
        assert!(matches!(
            runner_reason(Restore, &format!("{genuine}{indented}\n{inside}\n")),
            RunnerReason::Stated(_)
        ));
        // Blank lines and a `\r` are not lines of their own.
        let spaced = genuine.replace('\n', "\r\n\r\n   \n");
        assert_eq!(stated(Restore, &spaced), format!("GuardRefused: {GENUINE}"));
    }

    /// **The code is a member of THAT KIND's closed set or the line is not
    /// shown.** A well-formed word is not a code, and a `Backup`'s log cannot
    /// carry a restore-only code.
    ///
    /// KILLS: the set check removed (pattern only); one set for both kinds.
    #[test]
    fn a_code_outside_the_runs_closed_set_is_not_shown() {
        let at_the_position = |code: &str| {
            format!(
                "{}\nrefusal-reason=GuardRefused\n",
                detail_line(code, "a sentence")
            )
        };
        for unknown in [
            "Succeeded",
            "A",
            "PartitionSubsetsAwaitOwnerDecision",
            "CreatedTopicsLeft",
        ] {
            for run in [Restore, Backup] {
                assert_eq!(
                    runner_reason(run, &at_the_position(unknown)),
                    RunnerReason::Unreadable,
                    "{run:?} {unknown}"
                );
            }
        }
        // Per kind.
        assert_eq!(
            stated(Restore, &at_the_position("PointUntrusted")),
            "PointUntrusted: a sentence"
        );
        assert_eq!(
            runner_reason(Backup, &at_the_position("PointUntrusted")),
            RunnerReason::Unreadable
        );
        assert_eq!(
            stated(Backup, &at_the_position("ConsumerGroupIdInvalid")),
            "ConsumerGroupIdInvalid: a sentence"
        );
        assert_eq!(
            runner_reason(Restore, &at_the_position("ConsumerGroupIdInvalid")),
            RunnerReason::Unreadable
        );
        // NEGATIVE CONTROL: every member, for its own kind, at the position.
        for run in [Restore, Backup] {
            for code in run.reason_codes() {
                assert_eq!(
                    stated(run, &at_the_position(code)),
                    format!("{code}: a sentence")
                );
            }
        }
    }

    /// **The two lines agree or the pair is not the runner's.** The runner
    /// derives both from one refusal, so the state is the detail's code or
    /// the default.
    ///
    /// KILLS: the agreement check removed.
    #[test]
    fn a_detail_beside_a_state_it_was_not_printed_with_is_not_shown() {
        let d = detail_line("PointUntrusted", "a sentence");
        for (state, agrees) in [
            ("GuardRefused", true),
            ("PointUntrusted", true),
            ("TargetTopicConfigRefused", false),
            ("Succeeded", false),
            ("", false),
        ] {
            let got = runner_reason(Restore, &format!("{d}\nrefusal-reason={state}\n"));
            assert_eq!(
                matches!(got, RunnerReason::Stated(_)),
                agrees,
                "{state:?}: {got:?}"
            );
            if !agrees {
                assert_eq!(got, RunnerReason::Unreadable, "{state:?}");
            }
        }
        // Every pair the runner's own printer builds agrees with itself.
        for message in [
            "TargetTopicConfigRefused: cleanup.policy is `compact`",
            "PointBindingMismatch. The plan is bound to another point",
            GENUINE,
            "",
        ] {
            assert!(
                matches!(
                    runner_reason(Restore, &pair(Restore, message)),
                    RunnerReason::Stated(_)
                ),
                "{message:?}"
            );
        }
    }

    #[test]
    fn a_pair_outside_the_scanned_tail_is_not_read() {
        let genuine = pair(Restore, GENUINE);
        let filler = "a later line\n".repeat(KEY_SCAN_TAIL_LINES);
        assert_eq!(
            runner_reason(Restore, &format!("{genuine}{filler}")),
            RunnerReason::Misplaced
        );
    }

    #[test]
    fn a_body_at_the_byte_bound_is_not_scanned() {
        let genuine = pair(Restore, "a sentence");
        let mut body = "x".repeat(REFUSAL_LOG_LIMIT_BYTES as usize - genuine.len() - 1);
        body.push('\n');
        body.push_str(genuine.trim_end_matches('\n'));
        // One byte under the bound: read.
        assert_eq!(body.len() as i64, REFUSAL_LOG_LIMIT_BYTES - 1);
        assert!(matches!(
            interpret(Restore, body.as_bytes()).reason,
            RunnerReason::Stated(_)
        ));
        // At the bound: cut, so nothing is taken from it, the body included.
        body.push('\n');
        let cut = interpret(Restore, body.as_bytes());
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
            interpret(Restore, &bytes).reason,
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
        bytes.extend_from_slice(pair(Restore, "a sentence").as_bytes());
        assert!(matches!(
            interpret(Restore, &bytes).reason,
            RunnerReason::Stated(_)
        ));
        // Inside the sentence itself: one replacement character.
        let mut bytes =
            format!("{REFUSAL_DETAIL_PREFIX}{{\"code\":\"GuardRefused\",\"message\":\"bad ")
                .into_bytes();
        bytes.extend_from_slice(b"\xFF\xC0 bytes\"}\nrefusal-reason=GuardRefused\n");
        match interpret(Restore, &bytes).reason {
            RunnerReason::Stated(d) => assert_eq!(d.message(), "bad \u{FFFD} bytes"),
            other => panic!("{other:?}"),
        }
        // Inside the code: the line does not validate.
        let mut bytes = format!("{REFUSAL_DETAIL_PREFIX}{{\"code\":\"GuardRefused").into_bytes();
        bytes.extend_from_slice(b"\xFF\",\"message\":\"m\"}\nrefusal-reason=GuardRefused\n");
        assert_eq!(interpret(Restore, &bytes).reason, RunnerReason::Unreadable);
    }

    #[test]
    fn every_suffix_is_one_bounded_line_and_only_a_stated_reason_carries_log_text() {
        let stated = RunnerReason::Stated(RefusalDetail::from_refusal_message(
            Restore,
            "StorageRegionInvalid: a sentence",
        ));
        assert_eq!(
            stated.message_suffix(),
            "; the runner's own reason, cleaned and bounded: StorageRegionInvalid: a sentence"
        );
        assert_eq!(RunnerReason::NotStated.message_suffix(), "");
        for reason in [
            RunnerReason::Unreadable,
            RunnerReason::Misplaced,
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
        for unreadable in [RunnerReason::Unreadable, RunnerReason::Misplaced] {
            assert!(unreadable
                .message_suffix()
                .starts_with("; the runner gave no readable reason: "));
        }
        assert!(RunnerReason::PodGone
            .message_suffix()
            .ends_with("because the pod is gone"));
        assert!(RunnerReason::LogUnreadable { status: Some(403) }
            .message_suffix()
            .ends_with("HTTP 403"));
    }
}
