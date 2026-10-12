//! Reading a check's framed stdout (D2 §4.3's `relay.rs` row).
//!
//! # Two readers over one bounded log
//!
//! * **The frames.** `logweir-check-topic=`, `logweir-check-part=` and one
//!   final `logweir-check-end=` line, decoded with
//!   [`logweir_core::check_contract::frames::Decoder`], which requires the end
//!   line and verifies the plan digest, the subject UID, every declared part,
//!   every per-stream digest and the topic-line count and digest. A mismatch is
//!   [`CheckCode::ResultUnreadable`], reported **with the exit code but without
//!   the log content**: a relay that does not verify is exactly the input
//!   nobody should paste into a status field.
//! * **The refusal key.** D2 §4.2's exit 3 prints `refusal-reason=<Code>` and
//!   no frames, read through [`crate::runner_log::key_values`], the one key
//!   scan over the bounded tail, by key name: a pod log is stdout and stderr
//!   merged in nondeterministic order, so no reader may take "the last line".
//!
//! # The log read is bounded twice
//!
//! `limitBytes` ([`RELAY_LIMIT_BYTES`]) stops a runner filling the controller's
//! memory over the wire; the decoder's own budget stops one that stayed inside
//! the wire budget from filling it with frame content.

use kube::api::LogParams;

use logweir_core::check_contract::{
    frames::Decoder, redact, CheckCode, CheckRelay, FrameExpectations, DEFAULT_RELAY_BUDGET_BYTES,
};

use crate::controllers::backup::REFUSAL_REASON_PREFIX;

/// `limitBytes` on the `pods/log` read (D2 §4.3).
///
/// Above D2 §5.5's 6 MiB relay budget for topic lines and below the kubelet's
/// default `containerLogMaxSize` of 10 MiB; the margin carries the result and
/// details streams and the runner's stderr.
pub const RELAY_LIMIT_BYTES: i64 = 8 * 1024 * 1024;

/// The decoder's own budget: the relay budget plus 2 MiB of headroom for the
/// non-topic streams, which is [`Decoder::default`]'s.
pub const DECODER_BUDGET_BYTES: usize = DEFAULT_RELAY_BUDGET_BYTES + (2 * 1024 * 1024);

/// The `pods/log` parameters for a check: [`crate::runner_log::RELAY`].
///
/// `container: Some("runner")`, not `None`: `None` means "the only container",
/// which would silently read a sidecar's stdout the day one is added.
#[must_use]
pub fn log_params() -> LogParams {
    crate::runner_log::RELAY.params()
}

/// Decode a pod log into a verified relay — **pure**.
///
/// # Errors
///
/// Always [`CheckCode::ResultUnreadable`], with a reason naming the decode
/// failure's kind and never any log content.
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
/// The code is always [`CheckCode::ResultUnreadable`] (D2 §4.3 gives the whole
/// class one code: for a consumer "the output did not verify" is one fact).
/// The sub-cause is in `reason`, the `FrameError`'s own `Display`, with no log
/// bytes.
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
            // Redacted and capped although no `FrameError` reachable from
            // `decode` carries runner bytes today: `CheckRelay::result()`'s
            // `CheckResultError::Contract(String)` quotes the runner's own
            // field, this string reaches a status condition, and the redaction
            // chokepoint is only one if every path calls it.
            reason: redact(&error.to_string()),
        }
    }
}

impl std::fmt::Display for RelayRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.reason)
    }
}

/// The runner's contract refusal, read by key name from a bounded tail.
///
/// D2 §4.2's exit 3 prints `refusal-reason=<Code>` and no frames. `None` when
/// the tail has no such line or its value is outside the closed vocabulary: an
/// unparseable code is an absence, never a fabricated reason, because
/// [`CheckCode`] is what a `metav1.Condition.reason` is written from. The last
/// occurrence within the tail wins, as in
/// [`crate::controllers::backup::evidence_keys`].
#[must_use]
pub fn refusal_reason(log: &str) -> Option<CheckCode> {
    crate::runner_log::key_values(log, REFUSAL_REASON_PREFIX)
        .into_iter()
        .rev()
        .find_map(|v| CheckCode::parse(v.trim()))
}

// There is deliberately no `read(client, namespace, pod_name, …)` here: a log
// read must take the `&Pod` that `pod::find_owned_pod` returned, so the
// ownership proof (seam S6) is carried by the argument type rather than by the
// caller remembering.
