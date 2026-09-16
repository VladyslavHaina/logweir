//! Reading a check's framed stdout — D2 §4.3's `relay.rs` row.
//!
//! # Two readers over one bounded log, and they answer different questions
//!
//! * **The frames.** The runner's stdout carries `logweir-check-topic=`,
//!   `logweir-check-part=` and one final `logweir-check-end=` line. They are
//!   decoded with [`logweir_core::check_contract::frames::Decoder`], which
//!   requires the end line and then verifies the plan digest, the subject UID,
//!   every declared part, every per-stream digest and the topic-line count AND
//!   digest. A mismatch is [`CheckCode::ResultUnreadable`], reported **with the
//!   exit code but without the log content**: a relay that does not verify is
//!   exactly the input nobody should paste into a status field.
//! * **The refusal key.** D2 §4.2's exit 3 prints `refusal-reason=<Code>` and
//!   NO frames. That is read the way every other key in this crate is —
//!   [`crate::controllers::backup::tail_lines`], the ONE bounded-tail
//!   implementation, matched BY KEY NAME (plan erratum **E4**: a pod log is
//!   stdout and stderr merged in nondeterministic order, so no reader here may
//!   take "the last line").
//!
//! # The log read is bounded twice
//!
//! `limitBytes` bounds what the API server sends ([`RELAY_LIMIT_BYTES`]); the
//! decoder's own budget bounds what it will accumulate from what arrives. Two
//! bounds because they fail differently: the first stops a runner filling the
//! controller's memory over the wire, the second stops one that stayed inside
//! the wire budget from filling it with frame CONTENT.

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, LogParams};

use logweir_core::check_contract::{
    frames::Decoder, redact, CheckCode, CheckRelay, FrameExpectations, DEFAULT_RELAY_BUDGET_BYTES,
};

use crate::controllers::backup::{tail_lines, REFUSAL_REASON_PREFIX};

/// `limitBytes` on the `pods/log` read — D2 §4.3.
///
/// EIGHT MEBIBYTES, above D2 §5.5's 6 MiB relay budget for topic lines and
/// below the kubelet's default `containerLogMaxSize` of 10 MiB. The margin
/// carries the result stream, the details stream and whatever the runner wrote
/// on stderr.
pub const RELAY_LIMIT_BYTES: i64 = 8 * 1024 * 1024;

/// The decoder's own budget: the relay budget plus 2 MiB of headroom for the
/// non-topic streams, which is [`Decoder::default`]'s.
pub const DECODER_BUDGET_BYTES: usize = DEFAULT_RELAY_BUDGET_BYTES + (2 * 1024 * 1024);

/// The `pods/log` parameters for a check.
///
/// `container: Some("runner")` and not `None`: `None` means "the only
/// container if there is one", which is true today and would silently start
/// reading a sidecar's stdout the day something adds one.
#[must_use]
pub fn log_params() -> LogParams {
    LogParams {
        container: Some(crate::job::CONTAINER_NAME.to_string()),
        limit_bytes: Some(RELAY_LIMIT_BYTES),
        ..LogParams::default()
    }
}

/// Decode a pod log into a verified relay — **pure**.
///
/// # Errors
///
/// Always [`CheckCode::ResultUnreadable`], with a reason that names the decode
/// failure's KIND and never any log content.
pub fn decode(log: &str, expect: &FrameExpectations) -> Result<CheckRelay, RelayRefusal> {
    let mut decoder = Decoder::with_budget(DECODER_BUDGET_BYTES);
    for line in log.lines() {
        // Non-frame lines are ignored by the decoder itself, which is what lets
        // the `KafkaCluster` probe's I14 lines pass through it (D2 §4.5).
        decoder
            .push_line(line.trim_end_matches('\r'))
            .map_err(|e| RelayRefusal::new(&e))?;
    }
    decoder.finish(expect).map_err(|e| RelayRefusal::new(&e))
}

/// Why a relay could not be read.
///
/// The code is always [`CheckCode::ResultUnreadable`] — D2 §4.3 gives the whole
/// class one code, because from a consumer's side "the runner's output did not
/// verify" is one fact whatever the sub-cause. The sub-cause is in `reason`,
/// which is the `FrameError`'s own `Display` and carries no log bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayRefusal {
    /// Always [`CheckCode::ResultUnreadable`].
    pub code: CheckCode,
    /// The decode failure, named. Never log content.
    pub reason: String,
}

impl RelayRefusal {
    fn new(error: &logweir_core::check_contract::FrameError) -> Self {
        Self {
            code: error.code(),
            // REDACTED AND CAPPED, even though no `FrameError` variant reachable
            // from `decode` carries runner bytes today. `FrameError::
            // ResultDocument`, whose `CheckResultError::Contract(String)`
            // quotes the runner's own `contract` field verbatim, arises from
            // `CheckRelay::result()` — which W8 and W9 must call, and which
            // `CheckResult::sanitise` exists for. This string reaches
            // `Observation.message` and from there a status condition, and
            // `check_contract`'s redaction chokepoint is only a chokepoint if
            // every path through it actually calls it.
            reason: redact(&error.to_string()),
        }
    }
}

impl std::fmt::Display for RelayRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.reason)
    }
}

/// The runner's contract refusal, read BY KEY NAME from a bounded tail.
///
/// D2 §4.2's exit 3 prints `refusal-reason=<Code>` and no frames. `None` when
/// the tail carries no such line, or when it carries one whose value is not in
/// the closed vocabulary — an unparseable code is an absence, never a
/// fabricated reason, because [`CheckCode`] is what a `metav1.Condition.reason`
/// is written from.
///
/// The LAST occurrence within the tail wins, matching
/// [`crate::controllers::backup::evidence_keys`].
#[must_use]
pub fn refusal_reason(log: &str) -> Option<CheckCode> {
    let mut found = None;
    for line in tail_lines(log) {
        if let Some(v) = line.strip_prefix(REFUSAL_REASON_PREFIX) {
            found = CheckCode::parse(v.trim()).or(found);
        }
    }
    found
}

/// Read and verify one check pod's relay.
///
/// # Errors
///
/// [`kube::Error`] from the `pods/log` read. A log that was read but does not
/// verify is `Ok(Err(_))`: the difference matters, because the first is a
/// transport failure this pass retries and the second is a terminal fact about
/// the runner's output.
pub async fn read(
    client: &kube::Client,
    namespace: &str,
    pod_name: &str,
    expect: &FrameExpectations,
) -> Result<Result<CheckRelay, RelayRefusal>, kube::Error> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let log = pods.logs(pod_name, &log_params()).await?;
    Ok(decode(&log, expect))
}
