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
//! * **One line, and only when it carries this Job's token**
//!   ([`runner_reason`]). Text the runner did not choose can start a line of
//!   its own in this log. The runner escapes every line break in the error
//!   text it prints itself (`logweir::exit::one_line`, PROD-15.1); the Kafka
//!   client inside it logs to the same stderr by itself, unescaped, and what
//!   it logs can repeat a plan value that holds a line break (FX-43).
//!   Both streams reach this controller as one log, so nothing about where a
//!   line stands, or how well-formed it is, tells the runner's line from text
//!   a plan's author got into the log. What tells them apart is a value the
//!   plan's author could not have had: the line token this controller made
//!   when it built the Job ([`crate::job::new_line_token`]), gave the runner
//!   as an argument, and reads back off the Job's own pod template
//!   ([`crate::job::line_token`]). A `refusal-detail=` line that does not
//!   carry it is not read at all.
//! * **Validated and cleaned before it is stored.**
//!   [`RefusalDetail`](logweir_core::refusal_detail::RefusalDetail) cannot be
//!   built any other way: a reason code that is a member of the CLOSED set of
//!   codes that kind of run can print (a `Backup`'s log cannot put a
//!   restore-only code on a `Backup`), a sentence reduced to printable
//!   characters, passed through the credential rules and cut to 760 bytes.
//!   And the two refusal lines must agree with each other.
//! * **The token is never shown.** It is in the Job's pod template and in the
//!   runner's own line and nowhere else: not in a status, a condition, an
//!   event or a log line of this controller.
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

use logweir_core::refusal_detail::{
    LineRead, LineToken, RefusalDetail, RefusingRun, REFUSAL_DETAIL_PREFIX,
};

use crate::controllers::backup::REFUSAL_REASON_PREFIX;

/// How many trailing log lines the exit-3 read asks for.
///
/// Twice [`crate::controllers::backup::KEY_SCAN_TAIL_LINES`], and that number
/// belongs to ONE of the two readers this body is handed to:
///
/// - [`crate::controllers::backup::refusal_state`], the reader of
///   `refusal-reason=`, looks at the final sixteen NON-EMPTY lines. The API
///   counts every line, and CRI stores a long line as several, so the read
///   asks for twice as many. A refused run prints its two key lines last, so
///   nothing that reader owes is further back than that.
/// - [`runner_reason`], the reader of `refusal-detail=`, has no window of its
///   own: it scans EVERY line of the body this read returned.
///
/// So the two can disagree about one log, on purpose and harmlessly: with
/// more than sixteen non-empty lines after the runner's pair, the detail line
/// is still found and shown, beside `exitReason: GuardRefusedUnknownReason`,
/// because the state line is outside the other reader's window (the unit row
/// `the_runners_line_is_shown_wherever_it_stands` puts 25 lines after the
/// pair).
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
    /// Nothing in the log is this Job's runner's reason: the Job has no line
    /// token (an older controller built it), or no line carries the Job's
    /// token (an older runner, or lines that are not the runner's). The
    /// condition is exactly what it was before the line existed.
    NotStated,
    /// A line carries the Job's token, so the runner wrote it, and it did not
    /// validate: a code outside the run's closed set, nothing printable in
    /// its sentence, or a state line after it that contradicts it. Nothing
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

/// The runner's reason, from a log body that WAS read whole, of a run of kind
/// `run` whose Job carries `token`.
///
/// # The rule: the Job's token decides whose line it is
///
/// 1. **The Job has no token** (`None`): [`RunnerReason::NotStated`]. Nothing
///    in the log is read as a reason, whatever it holds, because nothing in
///    it could be told from text the plan's author wrote.
/// 2. Otherwise the LAST line of the log that is a `refusal-detail=` line
///    carrying `token` is the runner's
///    ([`RefusalDetail::read_line`]; the comparison is constant-time). Every
///    other `refusal-detail=` line (no token, another Job's token, not the
///    JSON object) is not read at all, wherever it stands and however
///    well-formed it is: before the runner's line, after it, alone at the
///    end of the log.
/// 3. **No such line**: [`RunnerReason::NotStated`] (an older runner, which
///    prints none; or a runner that was never given the argument).
/// 4. The runner's line is then held to what a line always was: a code of
///    `run`'s closed set and a cleaned, bounded sentence, and the
///    `refusal-reason=` line the runner wrote WITH it (the next one after it
///    in the log; the runner writes the two in one write) must agree with it
///    ([`RefusalDetail::agrees_with_state`]). Failing any of that is
///    [`RunnerReason::Unreadable`].
///
/// Position decides nothing. A forged line cannot carry the token, because
/// the token was made when the Job was built and the plan is older than its
/// Job; and a line that carries it needs no particular place.
///
/// # What a token does not cover
///
/// It separates the runner's line from text written BEFORE the Job existed.
/// Anyone who can read the Job can read its token, so an input that can
/// still change after the Job is built, and that a refusal repeats raw (a
/// value a broker reports, the text of a mounted file's parse error), could
/// in principle carry it. No plan can.
#[must_use]
pub fn runner_reason(run: RefusingRun, token: Option<&LineToken>, log: &str) -> RunnerReason {
    let Some(token) = token else {
        return RunnerReason::NotStated;
    };
    let lines: Vec<&str> = log
        .lines()
        .map(|l| l.trim_end_matches('\r'))
        .filter(|l| !l.trim().is_empty())
        .collect();
    for (at, line) in lines.iter().enumerate().rev() {
        match RefusalDetail::read_line(run, token, line) {
            LineRead::NotThisJobs => {}
            LineRead::Invalid => return RunnerReason::Unreadable,
            LineRead::Valid(detail) => {
                let state = lines[at + 1..]
                    .iter()
                    .find_map(|l| l.strip_prefix(REFUSAL_REASON_PREFIX));
                return match state {
                    Some(state) if detail.agrees_with_state(state) => RunnerReason::Stated(detail),
                    _ => RunnerReason::Unreadable,
                };
            }
        }
    }
    RunnerReason::NotStated
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
pub fn interpret(run: RefusingRun, token: Option<&LineToken>, bytes: &[u8]) -> RefusalLog {
    if bytes.len() as u64 >= REFUSAL_LOG_LIMIT_BYTES as u64 {
        return RefusalLog::without_body(RunnerReason::TailOverBound);
    }
    let body = String::from_utf8_lossy(bytes).into_owned();
    let reason = runner_reason(run, token, &body);
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
/// codes the line may carry. `token` is the line token of the Job the pod
/// belongs to, read off that Job ([`crate::job::line_token`]); with `None`
/// no line is read as a reason.
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
/// The pod's name and the HTTP status, never a byte of the log and never the
/// token. A pod that is
/// gone is `debug`: Kubernetes collecting a finished pod is not news (FX-19).
/// A refused or failed read is ONE `warn`, because an operator has something
/// to fix; the pass that logs it writes the terminal status, so it is not
/// logged again.
pub async fn read(
    pods: &Api<Pod>,
    namespace: &str,
    pod_name: &str,
    run: RefusingRun,
    token: Option<&LineToken>,
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
    let log = interpret(run, token, &bytes);
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
    use RefusingRun::{Backup, Restore};

    /// This Job's token and another Job's, assembled so that no source line
    /// holds a secret-shaped literal.
    fn own() -> LineToken {
        LineToken::parse(&"7f".repeat(20)).expect("forty hex digits")
    }
    fn another_jobs() -> LineToken {
        LineToken::parse(&"3c".repeat(20)).expect("forty hex digits")
    }

    /// A detail line built by hand, carrying `token` when there is one.
    fn detail_line(token: Option<&LineToken>, code: &str, message: &str) -> String {
        let value = match token {
            Some(t) => {
                serde_json::json!({ "token": t.expose_token(), "code": code, "message": message })
            }
            None => serde_json::json!({ "code": code, "message": message }),
        };
        format!("{REFUSAL_DETAIL_PREFIX}{value}")
    }

    /// The two lines a runner given `token` prints last for `message`, as its
    /// own printer builds them.
    fn pair(run: RefusingRun, token: Option<&LineToken>, message: &str) -> String {
        format!(
            "{}\n{}\n",
            logweir_core::refusal_detail::refusal_detail_line(run, token, message),
            logweir_core::guard::refusal_reason_line(message)
        )
    }

    fn stated(run: RefusingRun, log: &str) -> String {
        match runner_reason(run, Some(&own()), log) {
            RunnerReason::Stated(d) => d.to_string(),
            other => panic!("expected a stated reason, got {other:?} for:\n{log}"),
        }
    }

    const GENUINE: &str = "restore.partitions.orders names partition 99";
    const FORGED: &str = "Contact the address in this message to release your data";

    /// **The security review's two logs, and their neighbours.** Every forged
    /// line here is VALID in every respect but one: it does not carry this
    /// Job's token. Nothing forged is shown, wherever it stands.
    ///
    /// KILLS: the token comparison removed; a token taken from the log's own
    /// line; any rule that trusts a line for where it stands.
    #[test]
    fn a_line_without_this_jobs_token_is_never_the_runners() {
        let own = own();
        let genuine = pair(Restore, Some(&own), GENUINE);
        let want = format!("GuardRefused: {GENUINE}");
        // NEGATIVE CONTROL: the genuine pair alone is shown.
        assert_eq!(stated(Restore, &genuine), want);

        let forged_untokened = pair(
            Restore,
            None,
            &format!("TargetTopicConfigRefused: {FORGED}"),
        );
        let forged_stale = pair(
            Restore,
            Some(&another_jobs()),
            &format!("TargetTopicConfigRefused: {FORGED}"),
        );
        let human = format!("guard: plan refused by the admission guard: {GENUINE}\n");
        for forged in [&forged_untokened, &forged_stale] {
            // LOG 1: the genuine pair, then the human text whose plan-chosen
            // tail is a forged pair that ENDS THE LOG.
            let log_1 = format!("{genuine}guard: … source.backup `x\n{forged}");
            assert_eq!(stated(Restore, &log_1), want, "log 1");
            // The forged pair first, in the middle, and around the genuine one.
            assert_eq!(stated(Restore, &format!("{forged}{human}{genuine}")), want);
            assert_eq!(stated(Restore, &format!("{forged}{genuine}{forged}")), want);
            // LOG 2: an older runner prints the human text and then only its
            // state line; the plan put a forged detail line directly before
            // it, alone, ending the log. Nothing is shown.
            let forged_detail = forged.lines().next().expect("the detail line");
            let log_2 = format!(
                "guard: … source.backup `x\n{forged_detail}\nrefusal-reason=GuardRefused\n"
            );
            assert_eq!(
                runner_reason(Restore, Some(&own), &log_2),
                RunnerReason::NotStated,
                "log 2"
            );
            // The forged pair alone, where a runner would print it.
            assert_eq!(
                runner_reason(Restore, Some(&own), forged),
                RunnerReason::NotStated
            );
        }
        // NEGATIVE CONTROL for "nothing forged is shown": the same forged
        // lines ARE what the reader of the OTHER Job shows, so they are
        // well-formed and it is the token that refuses them here.
        match runner_reason(Restore, Some(&another_jobs()), &forged_stale) {
            RunnerReason::Stated(d) => {
                assert_eq!(d.to_string(), format!("TargetTopicConfigRefused: {FORGED}"))
            }
            other => panic!("{other:?}"),
        }
    }

    /// **A Job with no token has no line this controller reads as a reason**,
    /// whatever its log holds: an older controller built it.
    ///
    /// KILLS: a missing token treated as "any line will do".
    #[test]
    fn a_job_with_no_token_shows_nothing_whatever_its_log_holds() {
        for log in [
            pair(Restore, None, GENUINE),
            pair(Restore, Some(&own()), GENUINE),
            String::new(),
            "refusal-reason=GuardRefused\n".to_string(),
        ] {
            assert_eq!(
                runner_reason(Restore, None, &log),
                RunnerReason::NotStated,
                "{log}"
            );
        }
        // NEGATIVE CONTROL: with the token the second log is shown.
        assert_eq!(
            stated(Restore, &pair(Restore, Some(&own()), GENUINE)),
            format!("GuardRefused: {GENUINE}")
        );
    }

    /// **The runner's line needs no particular place**: with other lines of
    /// either key before it, after it and around it, and with the human line
    /// copied after it, it is shown.
    ///
    /// KILLS: any requirement about where the line stands.
    #[test]
    fn the_runners_line_is_shown_wherever_it_stands() {
        let own = own();
        let genuine = pair(Restore, Some(&own), GENUINE);
        let want = format!("GuardRefused: {GENUINE}");
        let noise = "refusal-reason=TargetTopicConfigRefused\n";
        let untokened = detail_line(None, "PointUntrusted", FORGED);
        let human = format!("guard: plan refused by the admission guard: {GENUINE}\n");
        for log in [
            format!("{genuine}{human}"),
            format!("{genuine}{}", "a stderr line\n".repeat(25)),
            format!("{noise}{untokened}\n{genuine}{untokened}\n{human}"),
            format!("{untokened}\n{untokened}\n{genuine}"),
            genuine.replace('\n', "\r\n\r\n   \n"),
        ] {
            assert_eq!(stated(Restore, &log), want, "{log}");
        }
    }

    /// A line that only MENTIONS the key, or is indented, is not a line of
    /// that key, token or no token.
    #[test]
    fn only_a_line_that_opens_with_the_key_is_read() {
        let own = own();
        let genuine_detail = detail_line(Some(&own), "GuardRefused", "m");
        let inside = format!(
            "{{\"level\":\"INFO\",\"message\":{}}}",
            serde_json::Value::String(genuine_detail.clone())
        );
        let indented = format!(" {genuine_detail}");
        for log in [
            format!("{inside}\nrefusal-reason=GuardRefused\n"),
            format!("{indented}\nrefusal-reason=GuardRefused\n"),
            "refusal-reason=GuardRefused\n".to_string(),
            String::new(),
        ] {
            assert_eq!(
                runner_reason(Restore, Some(&own), &log),
                RunnerReason::NotStated,
                "{log}"
            );
        }
        // NEGATIVE CONTROL: the same line at the start of a line is read.
        assert_eq!(
            stated(
                Restore,
                &format!("{genuine_detail}\nrefusal-reason=GuardRefused\n")
            ),
            "GuardRefused: m"
        );
    }

    /// **The code is a member of THAT KIND's closed set or the runner's line
    /// is not shown.** These lines carry the Job's token, so the runner wrote
    /// them: the answer is "no readable reason", not silence.
    ///
    /// KILLS: the set check removed (pattern only); one set for both kinds.
    #[test]
    fn a_code_outside_the_runs_closed_set_is_not_shown() {
        let own = own();
        let with_state = |code: &str| {
            format!(
                "{}\nrefusal-reason=GuardRefused\n",
                detail_line(Some(&own), code, "a sentence")
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
                    runner_reason(run, Some(&own), &with_state(unknown)),
                    RunnerReason::Unreadable,
                    "{run:?} {unknown}"
                );
            }
        }
        // Per kind.
        assert_eq!(
            stated(Restore, &with_state("PointUntrusted")),
            "PointUntrusted: a sentence"
        );
        assert_eq!(
            runner_reason(Backup, Some(&own), &with_state("PointUntrusted")),
            RunnerReason::Unreadable
        );
        assert_eq!(
            runner_reason(Restore, Some(&own), &with_state("ConsumerGroupIdInvalid")),
            RunnerReason::Unreadable
        );
        // NEGATIVE CONTROL: every member, for its own kind.
        for run in [Restore, Backup] {
            for code in run.reason_codes() {
                match runner_reason(run, Some(&own), &with_state(code)) {
                    RunnerReason::Stated(d) => {
                        assert_eq!(d.to_string(), format!("{code}: a sentence"))
                    }
                    other => panic!("{run:?} {code}: {other:?}"),
                }
            }
        }
    }

    /// **The two lines agree or the runner's line is not shown.** The state
    /// line is the one the runner wrote WITH the detail: the next one after
    /// it.
    ///
    /// KILLS: the agreement check removed; the check made against a state
    /// line BEFORE the detail.
    #[test]
    fn a_detail_and_the_state_line_after_it_must_agree() {
        let own = own();
        let d = detail_line(Some(&own), "PointUntrusted", "a sentence");
        for (state, agrees) in [
            ("GuardRefused", true),
            ("PointUntrusted", true),
            ("TargetTopicConfigRefused", false),
            ("Succeeded", false),
            ("", false),
        ] {
            let got = runner_reason(
                Restore,
                Some(&own),
                &format!("{d}\nrefusal-reason={state}\n"),
            );
            assert_eq!(
                matches!(got, RunnerReason::Stated(_)),
                agrees,
                "{state:?}: {got:?}"
            );
            if !agrees {
                assert_eq!(got, RunnerReason::Unreadable, "{state:?}");
            }
        }
        // No state line after it at all: the runner writes both in one write.
        assert_eq!(
            runner_reason(Restore, Some(&own), &format!("{d}\n")),
            RunnerReason::Unreadable
        );
        // A state line BEFORE the detail is not the one written with it.
        assert_eq!(
            runner_reason(
                Restore,
                Some(&own),
                &format!("refusal-reason=GuardRefused\n{d}\n")
            ),
            RunnerReason::Unreadable
        );
        // An agreeing state line directly after it decides, whatever a later
        // line of that key says.
        assert_eq!(
            stated(
                Restore,
                &format!("{d}\nrefusal-reason=GuardRefused\nrefusal-reason=Succeeded\n")
            ),
            "PointUntrusted: a sentence"
        );
        // Every pair the runner's own printer builds agrees with itself.
        for message in [
            "TargetTopicConfigRefused: cleanup.policy is `compact`",
            "PointBindingMismatch. The plan is bound to another point",
            GENUINE,
            "",
        ] {
            assert!(
                matches!(
                    runner_reason(Restore, Some(&own), &pair(Restore, Some(&own), message)),
                    RunnerReason::Stated(_)
                ),
                "{message:?}"
            );
        }
    }

    /// The LAST line carrying the token decides, and an invalid one is not
    /// rescued by a valid one before it.
    #[test]
    fn the_last_line_with_the_token_decides() {
        let own = own();
        let first = pair(Restore, Some(&own), "the first sentence");
        let last = pair(Restore, Some(&own), "the last sentence");
        assert_eq!(
            stated(Restore, &format!("{first}{last}")),
            "GuardRefused: the last sentence"
        );
        let invalid = format!(
            "{}\nrefusal-reason=GuardRefused\n",
            detail_line(Some(&own), "Succeeded", "a sentence")
        );
        assert_eq!(
            runner_reason(Restore, Some(&own), &format!("{first}{invalid}")),
            RunnerReason::Unreadable
        );
    }

    #[test]
    fn a_body_at_the_byte_bound_is_not_scanned() {
        let own = own();
        let genuine = pair(Restore, Some(&own), "a sentence");
        let mut body = "x".repeat(REFUSAL_LOG_LIMIT_BYTES as usize - genuine.len() - 1);
        body.push('\n');
        body.push_str(genuine.trim_end_matches('\n'));
        // One byte under the bound: read.
        assert_eq!(body.len() as i64, REFUSAL_LOG_LIMIT_BYTES - 1);
        assert!(matches!(
            interpret(Restore, Some(&own), body.as_bytes()).reason,
            RunnerReason::Stated(_)
        ));
        // At the bound: cut, so nothing is taken from it, the body included.
        body.push('\n');
        let cut = interpret(Restore, Some(&own), body.as_bytes());
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
            interpret(Restore, Some(&own()), &bytes).reason,
            RunnerReason::TailOverBound,
            "and what was read is a cut body, from which nothing is taken"
        );
        // A source under the bound is read whole.
        let mut small = futures::io::repeat(b'x').take(1000);
        assert_eq!(read_capped(&mut small).await.expect("reads").len(), 1000);
    }

    #[test]
    fn invalid_utf8_does_not_cost_the_reason_and_is_never_stored_raw() {
        let own = own();
        // A stray byte elsewhere in the tail.
        let mut bytes = b"engine said \xFF\xFE\n".to_vec();
        bytes.extend_from_slice(pair(Restore, Some(&own), "a sentence").as_bytes());
        assert!(matches!(
            interpret(Restore, Some(&own), &bytes).reason,
            RunnerReason::Stated(_)
        ));
        // Inside the sentence itself: one replacement character.
        let mut bytes = format!(
            "{REFUSAL_DETAIL_PREFIX}{{\"token\":\"{}\",\"code\":\"GuardRefused\",\"message\":\"bad ",
            own.expose_token()
        )
        .into_bytes();
        bytes.extend_from_slice(b"\xFF\xC0 bytes\"}\nrefusal-reason=GuardRefused\n");
        match interpret(Restore, Some(&own), &bytes).reason {
            RunnerReason::Stated(d) => assert_eq!(d.message(), "bad \u{FFFD} bytes"),
            other => panic!("{other:?}"),
        }
        // Inside the TOKEN: it is no longer this Job's token, so the line is
        // not read at all.
        let mut bytes = format!(
            "{REFUSAL_DETAIL_PREFIX}{{\"token\":\"{}",
            &own.expose_token()[..38]
        )
        .into_bytes();
        bytes.extend_from_slice(
            b"\xFF\",\"code\":\"GuardRefused\",\"message\":\"m\"}\nrefusal-reason=GuardRefused\n",
        );
        assert_eq!(
            interpret(Restore, Some(&own), &bytes).reason,
            RunnerReason::NotStated
        );
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

    /// **Nothing a reason is built from, and nothing this module can say,
    /// holds the token.**
    ///
    /// KILLS: the token carried into the detail, and so into the condition.
    #[test]
    fn no_reason_and_no_suffix_holds_the_token() {
        let own = own();
        for log in [
            pair(Restore, Some(&own), GENUINE),
            // A sentence that REPEATS the token: the credential rules read 40
            // hex digits as a key and remove them.
            pair(
                Restore,
                Some(&own),
                &format!("the token is {} and more", own.expose_token()),
            ),
            format!(
                "{}\nrefusal-reason=GuardRefused\n",
                detail_line(Some(&own), "Succeeded", "a sentence")
            ),
        ] {
            let reason = runner_reason(Restore, Some(&own), &log);
            let shown = format!("{} {reason:?}", reason.message_suffix());
            assert!(!shown.contains(own.expose_token()), "{shown}");
        }
        match runner_reason(
            Restore,
            Some(&own),
            &pair(
                Restore,
                Some(&own),
                &format!("the token is {} and more", own.expose_token()),
            ),
        ) {
            RunnerReason::Stated(d) => assert_eq!(d.message(), "the token is [redacted] and more"),
            other => panic!("{other:?}"),
        }
    }
}
