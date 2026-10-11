//! A runner pod's log, the one way every controller reads it: one bounded
//! read (the log's TAIL for every key reader; the check relay's head, see
//! [`RELAY`]), one key scanner, and one TTL patch for the finished Job.
//!
//! Only the plumbing is shared. What a reader may take from the log stays
//! with that reader: the line token ([`crate::refusal`]), the place rule
//! ([`crate::controllers::restore::creation_stop_state`]), the check frames
//! ([`crate::check::relay`]) and each kind's own keys.

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

/// The terminal read of a finished `Backup`, `Restore` or `KafkaCluster`
/// probe at every exit code but 3: the last
/// [`TERMINAL_LOG_TAIL_LINES`](crate::controllers::restore::TERMINAL_LOG_TAIL_LINES)
/// lines. Every key those reconcilers read is in the last
/// [`KEY_SCAN_TAIL_LINES`] non-empty lines.
pub const TERMINAL: Bound = Bound {
    lines: Some(crate::controllers::restore::TERMINAL_LOG_TAIL_LINES),
    bytes: None,
    runner_container: false,
};

/// The exit-3 read ([`crate::refusal::read`]).
pub const REFUSAL: Bound = Bound {
    lines: Some(crate::refusal::REFUSAL_LOG_TAIL_LINES),
    bytes: Some(crate::refusal::REFUSAL_LOG_LIMIT_BYTES),
    runner_container: true,
};

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
