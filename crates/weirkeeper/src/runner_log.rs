//! A runner pod's log, the one way every controller reads it: one bounded
//! terminal read ([`TERMINAL`], every exit code, every Job-owning kind; the
//! check relay reads the head, see [`RELAY`]), ONE answer to "whose line is
//! it" ([`own_lines`], FX-43), one key scanner, and one TTL patch for the
//! finished Job.
//!
//! # Whose line is it: the Job's token, for every line
//!
//! A pod log is one stream: the runner's own lines, the human error text,
//! the Kafka client's log, the tracing events. Text a plan's author chose can
//! reach it, and no rule about where a line stands tells the runner's line
//! from that text. A Job this controller builds carries a fresh random token
//! ([`crate::job::add_line_token`]); the runner seals every line a controller
//! acts on with it (`logweir_core::runner_line`), and [`own_lines`] keeps
//! exactly those lines. Every key reader then scans [`RunnerLines::text`].
//!
//! **How a Job-owning kind adopts it: one call.** At the terminal read,
//! `let lines = runner_log::own_lines(&job::job_token(&job), &raw_log);`, then
//! every reader of that kind scans `lines.text()`, and
//! `lines.runner_is_older()` names a runner image older than this controller
//! (`RunnerOlderThanController`) instead of reading anything. The runner side
//! is `logweir::exit::FinalLines` (the lines printed last) and
//! `logweir::exit::print_lines` (a line printed during the run).
//!
//! What a reader may take from its lines stays with that reader: a closed
//! set, a grammar, the check frames ([`crate::check::relay`]).

use futures::io::AsyncRead;
use futures::AsyncReadExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, LogParams, Patch, PatchParams};
use serde_json::json;

use crate::controllers::backup::KEY_SCAN_TAIL_LINES;

/// What one `pods/log` read asks for. `lines` (`tailLines`) asks for the log's
/// tail; every bound but [`RELAY`] sets it. `bytes` (`limitBytes`) counts from
/// the start of those lines, so a body that reaches it was cut before the
/// log's last lines; [`read`] also stops taking bytes one past it, whatever
/// the server sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bound {
    pub lines: Option<i64>,
    pub bytes: Option<i64>,
    pub runner_container: bool,
}

impl Bound {
    #[must_use]
    pub fn params(&self) -> LogParams {
        LogParams {
            container: self
                .runner_container
                .then(|| crate::job::CONTAINER_NAME.to_string()),
            tail_lines: self.lines,
            limit_bytes: self.bytes,
            ..LogParams::default()
        }
    }
}

/// THE terminal read of a finished `Backup`, `Restore` or `KafkaCluster`
/// probe, at every exit code (FX-43 folded FX-34's exit-3 read into it): the
/// last [`TERMINAL_LOG_TAIL_LINES`] lines of the runner container, at most
/// [`TERMINAL_LOG_LIMIT_BYTES`]. A body that reaches the byte bound was cut
/// before its end, where the runner's lines are, and nothing is taken from it
/// ([`TerminalRead::OverBound`]).
pub const TERMINAL: Bound = Bound {
    lines: Some(TERMINAL_LOG_TAIL_LINES),
    bytes: Some(TERMINAL_LOG_LIMIT_BYTES),
    runner_container: true,
};

/// `tailLines` on the terminal read. A runner's last block (the sealed lines,
/// its end line and the plain lines) is under forty lines; the rest leaves
/// room for the other stream's lines interleaved with it, and for a Job built
/// by an older controller, whose readers keep the last
/// [`KEY_SCAN_TAIL_LINES`] non-empty lines, blank ones between them.
pub const TERMINAL_LOG_TAIL_LINES: i64 = 256;

/// `limitBytes` on the terminal read: 4 MiB, which is
/// [`TERMINAL_LOG_TAIL_LINES`] times the 16 KiB at which CRI splits a
/// container log line, so on a default runtime the byte bound never cuts the
/// tail the line bound selected. `limitBytes` counts from the START of those
/// lines, so a smaller bound would cut the END, where the runner's lines are.
pub const TERMINAL_LOG_LIMIT_BYTES: i64 = 4 * 1024 * 1024;

const _: () = assert!(TERMINAL_LOG_TAIL_LINES as usize >= 16 * KEY_SCAN_TAIL_LINES);
const _: () = assert!(TERMINAL_LOG_LIMIT_BYTES == TERMINAL_LOG_TAIL_LINES * 16 * 1024);

/// D3 §2.4's progress read of a running Job ([`crate::diagnostics::read_progress`]).
pub const PROGRESS: Bound = Bound {
    lines: Some(crate::diagnostics::PROGRESS_TAIL_LINES),
    bytes: Some(crate::diagnostics::PROGRESS_LIMIT_BYTES),
    runner_container: false,
};

/// A check's relay and a retention Job's lines: the log's head, its first
/// [`RELAY_LIMIT_BYTES`](crate::check::relay::RELAY_LIMIT_BYTES), and no
/// `tailLines`.
///
/// The one read that is not a tail: the frames are one document, read from the
/// start and verified whole, and a frame after the end frame is refused. A tail
/// window would let text printed after the genuine end frame push it out of the
/// read, and a second self-consistent relay printed there would then verify.
pub const RELAY: Bound = Bound {
    lines: None,
    bytes: Some(crate::check::relay::RELAY_LIMIT_BYTES),
    runner_container: true,
};

/// Read `bound` of the pod's log: its last `bound.lines` lines, or from its
/// start when there are none, through [`read_capped`].
///
/// # Errors
///
/// The request's [`kube::Error`]; a body that broke after the server
/// answered as [`kube::Error::ReadEvents`].
pub async fn read(pods: &Api<Pod>, pod_name: &str, bound: &Bound) -> Result<Vec<u8>, kube::Error> {
    let stream = pods.log_stream(pod_name, &bound.params()).await?;
    read_capped(Box::pin(stream), bound)
        .await
        .map_err(kube::Error::ReadEvents)
}

/// At most `bound.bytes` plus one bytes off `reader` (the one is how a caller
/// sees the bound was reached); all of it when the bound has no bytes.
///
/// The second bound, the one this process enforces itself: `limitBytes` is a
/// request, and this stops a server that ignored it, or a stream that never
/// ends, from growing a buffer in a controller serving every namespace. The
/// rest of the stream is dropped unread.
///
/// # Errors
///
/// The reader's own error.
pub async fn read_capped<R: AsyncRead + Unpin>(
    reader: R,
    bound: &Bound,
) -> std::io::Result<Vec<u8>> {
    let cap = bound.bytes.map_or(u64::MAX, |b| {
        u64::try_from(b).unwrap_or(0).saturating_add(1)
    });
    let mut bytes = Vec::new();
    reader.take(cap).read_to_end(&mut bytes).await?;
    Ok(bytes)
}

/// What one terminal read returned ([`read_terminal`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminalRead {
    /// The tail, read to its end, decoded lossily (a stray byte elsewhere in
    /// the tail does not cost the lines after it).
    Read(String),
    /// The tail is longer than [`TERMINAL_LOG_LIMIT_BYTES`]: the body was cut
    /// before its end, so nothing is taken from it.
    OverBound,
}

/// The bytes a terminal read returned, as what they mean: **pure**.
#[must_use]
pub fn interpret_terminal(bytes: &[u8]) -> TerminalRead {
    if bytes.len() as u64 >= TERMINAL_LOG_LIMIT_BYTES as u64 {
        return TerminalRead::OverBound;
    }
    TerminalRead::Read(String::from_utf8_lossy(bytes).into_owned())
}

/// The terminal read ([`TERMINAL`]) of `pod_name`.
///
/// # Errors
///
/// [`read`]'s.
pub async fn read_terminal(pods: &Api<Pod>, pod_name: &str) -> Result<TerminalRead, kube::Error> {
    Ok(interpret_terminal(&read(pods, pod_name, &TERMINAL).await?))
}

/// The lines of a finished (or running) Job's log that its runner vouched
/// for, and whether it vouched for its exit: what [`own_lines`] returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunnerLines {
    text: String,
    token: Option<logweir_core::refusal_detail::LineToken>,
    every_line: bool,
    exit_vouched: bool,
}

impl RunnerLines {
    /// What every key reader scans: under an FX-43 token, the sealed lines
    /// with their seal removed, in log order, the `runner-exit=` line aside;
    /// otherwise the whole log, as the Job's older controller read it.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The token a `refusal-detail=` line's JSON must carry
    /// ([`crate::refusal::runner_reason`]), whichever flag gave it.
    #[must_use]
    pub fn token(&self) -> Option<&logweir_core::refusal_detail::LineToken> {
        self.token.as_ref()
    }

    /// Whether [`Self::text`] holds ONLY lines sealed with the Job's FX-43
    /// token. A reader of a line the runner prints sealed only (DUI-5's
    /// `failure-detail=`) reads nothing otherwise: an older Job's whole log
    /// is text anyone could have put there.
    #[must_use]
    pub fn sealed_only(&self) -> bool {
        self.every_line
    }

    /// **The mixed pair, named.** True when the Job carries FX-43's token and
    /// no line of the log sealed with it is the runner's `runner-exit=`: a
    /// runner that knows the flag prints one at every exit it reaches, and one
    /// that does not stops at argument parsing. Ask it only of a log read to
    /// its end, of a runner that exited 0 to 4 ([`vouches_for_exit`]).
    #[must_use]
    pub fn runner_is_older(&self) -> bool {
        self.every_line && !self.exit_vouched
    }
}

/// Whether a container exit code is one a runner reaches through its own
/// code (Global Constraint 11's 0 to 4), the only codes after which a runner
/// that knows FX-43's flag has printed its end line. A kill (137, 143) or a
/// panic (101) is not.
#[must_use]
pub fn vouches_for_exit(exit_code: i32) -> bool {
    (0..=4).contains(&exit_code)
}

/// [`own_lines`] for a log whose other lines a sealed line binds by digest
/// (the check relay: the sealed end frame declares every stream's digest and
/// the topic lines' count and digest). Under FX-43's token a plain line that
/// opens with one of `bound` is kept in place beside the sealed ones; a plain
/// line of any other kind (a forged end frame, a forged `refusal-reason=`) is
/// not. A forged bound line can only make the relay fail to verify, never
/// verify. Every other token kind reads as [`own_lines`] does.
#[must_use]
pub fn own_lines_binding(token: &crate::job::JobToken, log: &str, bound: &[&str]) -> RunnerLines {
    use crate::job::JobToken;
    use logweir_core::runner_line::{unseal, RUNNER_EXIT_PREFIX};
    let JobToken::EveryLine(t) = token else {
        return own_lines(token, log);
    };
    let mut text = String::new();
    let mut exit_vouched = false;
    for line in log.lines().map(|l| l.trim_end_matches('\r')) {
        let own = match unseal(t, line) {
            Some(own) if own.starts_with(RUNNER_EXIT_PREFIX) => {
                exit_vouched = true;
                continue;
            }
            Some(own) => own,
            None if bound.iter().any(|p| line.starts_with(p)) => line,
            None => continue,
        };
        text.push_str(own);
        text.push('\n');
    }
    RunnerLines {
        text,
        token: Some(t.clone()),
        every_line: true,
        exit_vouched,
    }
}

/// **THE answer to "whose line is it"** (FX-43), the one call every
/// Job-owning kind makes on the log it read: `token` is the Job's own
/// ([`crate::job::job_token`]).
///
/// - [`JobToken::EveryLine`](crate::job::JobToken::EveryLine): only the lines
///   sealed with THIS token (`logweir_core::runner_line::unseal`, compared in
///   constant time) are kept, wherever they stand; every other line, however
///   well-formed (a forged key line from a plan string, the Kafka client's
///   log, an older runner's plain lines, a line sealed with another Job's
///   token) is not read at all.
/// - [`JobToken::RefusalDetail`](crate::job::JobToken::RefusalDetail) and
///   [`JobToken::None`](crate::job::JobToken::None): a Job an older
///   controller built. The whole log, read as that controller read it, so an
///   older pair degrades to what it did before, never to trusting more.
#[must_use]
pub fn own_lines(token: &crate::job::JobToken, log: &str) -> RunnerLines {
    use crate::job::JobToken;
    use logweir_core::runner_line::{unseal, RUNNER_EXIT_PREFIX};
    match token {
        JobToken::EveryLine(t) => {
            let mut text = String::new();
            let mut exit_vouched = false;
            for line in log.lines().map(|l| l.trim_end_matches('\r')) {
                let Some(own) = unseal(t, line) else {
                    continue;
                };
                if own.starts_with(RUNNER_EXIT_PREFIX) {
                    exit_vouched = true;
                    continue;
                }
                text.push_str(own);
                text.push('\n');
            }
            RunnerLines {
                text,
                token: Some(t.clone()),
                every_line: true,
                exit_vouched,
            }
        }
        JobToken::RefusalDetail(t) => RunnerLines {
            text: log.to_string(),
            token: Some(t.clone()),
            every_line: false,
            exit_vouched: false,
        },
        JobToken::None => RunnerLines {
            text: log.to_string(),
            token: None,
            every_line: false,
            exit_vouched: false,
        },
    }
}

/// What a finished Job's terminal read means, under the Job's token: the ONE
/// call a Job-owning kind added after FX-43 makes ([`read_own_terminal`];
/// the pure half is [`judge_terminal`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Terminal {
    /// The lines the runner vouched for: read every key off
    /// [`RunnerLines::text`].
    Lines(RunnerLines),
    /// The Job carries FX-43's token, the runner exited 0 to 4, and it printed
    /// no sealed `runner-exit=`: the runner image is older than the
    /// controller. Name it ([`RUNNER_OLDER_SENTENCE`]) and read nothing.
    RunnerOlder,
    /// The tail is longer than [`TERMINAL_LOG_LIMIT_BYTES`]: cut before its
    /// end, so nothing is taken from it and nothing is said about the runner.
    OverBound,
}

/// What a kind's status says beside `RunnerOlderThanController` when it has no
/// sentence of its own.
pub const RUNNER_OLDER_SENTENCE: &str = "the runner printed no line sealed with this Job's \
     line token, which every runner at least as new as this controller does, so the runner \
     image is older than the controller (an older one stops at the unknown argument \
     `--runner-line-token`); nothing of its log was read. Upgrade the runner image to the \
     controller's version (the chart's `runnerImage`)";

/// [`read_own_terminal`]'s pure half: `read` judged under `token` at
/// `exit_code` (`None`: no container exit code was read, so nothing is said
/// about the runner).
#[must_use]
pub fn judge_terminal(
    token: &crate::job::JobToken,
    exit_code: Option<i32>,
    read: TerminalRead,
) -> Terminal {
    let raw = match read {
        TerminalRead::OverBound => return Terminal::OverBound,
        TerminalRead::Read(raw) => raw,
    };
    let lines = own_lines(token, &raw);
    if !raw.is_empty() && exit_code.is_some_and(vouches_for_exit) && lines.runner_is_older() {
        return Terminal::RunnerOlder;
    }
    Terminal::Lines(lines)
}

/// **The one call** for a Job-owning kind's terminal read (FX-43): THE bound
/// ([`TERMINAL`]), the Job's own token ([`crate::job::job_token`]), and the
/// mixed-pair rule, in one.
///
/// # Errors
///
/// [`read`]'s: the caller decides whether a failed read is retried.
pub async fn read_own_terminal(
    pods: &Api<Pod>,
    pod_name: &str,
    job: &Job,
    exit_code: Option<i32>,
) -> Result<Terminal, kube::Error> {
    Ok(judge_terminal(
        &crate::job::job_token(job),
        exit_code,
        read_terminal(pods, pod_name).await?,
    ))
}

/// [`read`] as UTF-8 text, failing on any other body as `Api::logs` does.
///
/// # Errors
///
/// [`read`]'s, and [`kube::Error::FromUtf8`].
pub async fn read_text(
    pods: &Api<Pod>,
    pod_name: &str,
    bound: &Bound,
) -> Result<String, kube::Error> {
    String::from_utf8(read(pods, pod_name, bound).await?).map_err(kube::Error::FromUtf8)
}

/// The final [`KEY_SCAN_TAIL_LINES`] non-empty lines, trimmed of `\r`.
///
/// A pod log is stdout and stderr merged in nondeterministic order, so every
/// key is read from this bounded tail by name ([`key_values`]), never as "the
/// last line".
#[must_use]
pub fn tail_lines(log: &str) -> Vec<&str> {
    let all: Vec<&str> = log
        .lines()
        .map(|l| l.trim_end_matches('\r'))
        .filter(|l| !l.trim().is_empty())
        .collect();
    let start = all.len().saturating_sub(KEY_SCAN_TAIL_LINES);
    all[start..].to_vec()
}

/// The key scanner: the value of every [`tail_lines`] line that starts with
/// `key`, oldest first. A reader that takes one value takes the last: a runner
/// that printed an earlier draft of a line has the final one be the true one.
#[must_use]
pub fn key_values<'a>(log: &'a str, key: &str) -> Vec<&'a str> {
    tail_lines(log)
        .into_iter()
        .filter_map(|line| line.strip_prefix(key))
        .collect()
}

/// Patch a finished runner Job's `ttlSecondsAfterFinished`, with `marker` (an
/// annotation name and value) in the same merge patch when there is one.
///
/// Only after the caller's status write returned: the TTL deletes the pod, and
/// with it the log the status was read from.
///
/// # Errors
///
/// [`kube::Error`] from the patch.
pub async fn set_ttl(
    jobs: &Api<Job>,
    job_name: &str,
    seconds: i32,
    marker: Option<(&str, &str)>,
) -> Result<(), kube::Error> {
    let patch = match marker {
        None => json!({ "spec": { "ttlSecondsAfterFinished": seconds } }),
        Some((name, value)) => json!({
            "metadata": { "annotations": { name: value } },
            "spec": { "ttlSecondsAfterFinished": seconds }
        }),
    };
    jobs.patch(job_name, &PatchParams::default(), &Patch::Merge(patch))
        .await?;
    Ok(())
}
