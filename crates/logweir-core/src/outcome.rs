use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Spec §6.1. `drift` is deliberately NOT a v0.1 value — v0.1 collects no
/// metadata. It returns as a minor bump when SP2 ships.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename_all = "kebab-case")]
pub enum Outcome {
    Pass,
    FailObjective,
    FailIntegrity,
    PreflightFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename_all = "kebab-case")]
pub enum IntegrityLevel {
    /// sha256(key‖value‖headers‖timestamp) reconciled per record.
    ByteFingerprint,
    /// The KBAK decoder returned Err(Unsupported); records were consumed but
    /// not reconciled against archive bytes (spec §11).
    ConsumeOnly,
    NotAttempted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename_all = "kebab-case")]
pub enum IntegrityResult {
    Pass,
    /// A selection whose evidence was inconclusive — something in the sample
    /// was not examined, so the drill cannot claim it. NOT a pass.
    ///
    /// A COMPACTED TOPIC DOES NOT REACH THIS in v0.1: it is reported as `fail`,
    /// through the same path a real mismatch takes. See `docs/stability.md`.
    // Full rationale, kept out of the doc comment because schemars publishes
    // doc comments as `description` in schemas/logweir-drill-scorecard-1.0.0.json
    // and this belongs in the code, not in the wire format:
    //
    // What actually reaches `Partial` in v0.1, per `phase7_verify`'s module
    // doc: an archive returning zero or short fingerprints for a selection; a
    // (topic, partition, window) the manifest claims exists but no segment
    // matches; a pre-0.21 segment carrying no sha256; a consume-only selection
    // whose target partition gave back less than the manifest claims. Each
    // names itself in `integrity.partial_reason`.
    //
    // What does NOT reach it, despite the name this variant is usually
    // explained by: a compacted topic. Telling "compaction removed this record
    // on purpose" apart from "the restore or the archive lost it" needs a
    // specific mismatch cross-referenced against `topic_parity`'s
    // `cleanup.policy=compact` flag, and `phase7_verify::run` does not attempt
    // it. That is a declared limitation, not a defect.
    // `fixtures::verify_outcome_for_compacted_topic` pins the SHAPE of such a
    // document for the scoring and rendering layers and describes no path
    // `run` itself can take; Task 21c confirmed against a live cluster that no
    // e2e drill can drive it.
    Partial,
    Fail,
}

/// Per-run readback of an engine lever (spec §9.3 phase 5, §7.2(a)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename_all = "kebab-case")]
pub enum LeverState {
    Honoured,
    Ignored,
    /// `dry_run_check_segments` is not observable from DryRunReport; its only
    /// per-run readback is the absence of an unknown-key warning.
    UnknownNotObservable,
}

/// The engine-matrix row THIS RUN established, decided once — in
/// `logweir::drill::phase8_score::matrix_verdict_for` — immediately before the
/// document is validated and signed.
///
/// This comment used to read "copied from this engine tag's engine-matrix row;
/// never re-derived per run". That was never true of the shipped code: phase 5
/// raised the field to `Pass` the moment the `header_preflight` lever was
/// honoured, and nothing lowered it afterwards, so a drill that blocked at
/// preflight, restored nothing, failed its reconciliation or missed its RTO
/// signed `matrix_verdict: "pass"` beside its own contradicting `outcome`.
/// `docs/support-matrix.md` defines the five values; the decision function's
/// doc comment carries the table.
///
/// Every variant serialises as a FLAT kebab-case string, so the field's schema
/// is a plain enum and the auditor's ~20-line Python verifier never has to
/// handle `string | object`. The reason for a `fail` lives in
/// `EngineInfo.matrix_verdict_reason`, mirroring the
/// `integrity.result`/`partial_reason` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename_all = "kebab-case")]
pub enum MatrixVerdict {
    Pass,
    PassDegraded,
    Fail,
    FailLeverNotHonoured,
    UnsupportedLeverAbsent,
}

// ---------------------------------------------------------------------------
// ONE SPELLING PER VALUE, FOR EVERY SURFACE.
//
// One binary produced three different spellings of the same enum. `drill show`
// rendered Rust `Debug` (`Pass`, `ByteFingerprint/Pass`, `Honoured`);
// `drill run`'s stdout line went through `metrics::outcome_str` (`pass`); and
// the signed JSON and the Prometheus labels used kebab-case. `metrics.rs`'s own
// comment claimed its mapping was "shared ... so the two can never disagree" —
// and a third renderer disagreed with both.
//
// The wire spelling is the one an auditor can grep for in the signed document,
// so it is the one every surface uses. Each `wire_name` below is an exhaustive
// match — adding a variant fails to compile rather than falling through to a
// wrong string — and each is pinned against the enum's own `Serialize` impl by
// the test module at the bottom of this file, so a rename in the `#[serde]`
// attribute that is not mirrored here turns a named test red.

impl Outcome {
    /// The exact string this value carries in the signed JSON document.
    pub fn wire_name(&self) -> &'static str {
        match self {
            Outcome::Pass => "pass",
            Outcome::FailObjective => "fail-objective",
            Outcome::FailIntegrity => "fail-integrity",
            Outcome::PreflightFailed => "preflight-failed",
        }
    }
}

impl IntegrityLevel {
    /// The exact string this value carries in the signed JSON document.
    pub fn wire_name(&self) -> &'static str {
        match self {
            IntegrityLevel::ByteFingerprint => "byte-fingerprint",
            IntegrityLevel::ConsumeOnly => "consume-only",
            IntegrityLevel::NotAttempted => "not-attempted",
        }
    }
}

impl IntegrityResult {
    /// The exact string this value carries in the signed JSON document.
    pub fn wire_name(&self) -> &'static str {
        match self {
            IntegrityResult::Pass => "pass",
            IntegrityResult::Partial => "partial",
            IntegrityResult::Fail => "fail",
        }
    }
}

impl LeverState {
    /// The exact string this value carries in the signed JSON document.
    pub fn wire_name(&self) -> &'static str {
        match self {
            LeverState::Honoured => "honoured",
            LeverState::Ignored => "ignored",
            LeverState::UnknownNotObservable => "unknown-not-observable",
        }
    }
}

impl MatrixVerdict {
    /// The exact string this value carries in the signed JSON document.
    pub fn wire_name(&self) -> &'static str {
        match self {
            MatrixVerdict::Pass => "pass",
            MatrixVerdict::PassDegraded => "pass-degraded",
            MatrixVerdict::Fail => "fail",
            MatrixVerdict::FailLeverNotHonoured => "fail-lever-not-honoured",
            MatrixVerdict::UnsupportedLeverAbsent => "unsupported-lever-absent",
        }
    }
}

#[cfg(test)]
mod wire_name_tests {
    //! Every `wire_name` is checked against the value's OWN `Serialize` impl,
    //! variant by variant. A `#[serde(rename_all)]` change, a renamed variant
    //! or a hand-edited string in one of the matches above turns exactly one
    //! of these red — which is the property `metrics::outcome_str` asserted in
    //! a comment and could not enforce.
    use super::*;

    fn serialised<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).expect("these enums are plain unit variants")
    }

    macro_rules! agrees {
        ($name:ident, $($v:expr),+ $(,)?) => {
            #[test]
            fn $name() {
                $(
                    assert_eq!(
                        serialised(&$v),
                        format!("{:?}", $v.wire_name()),
                        "wire_name disagrees with the Serialize impl for {:?}",
                        $v
                    );
                )+
            }
        };
    }

    agrees!(
        outcome_wire_names_match_the_json,
        Outcome::Pass,
        Outcome::FailObjective,
        Outcome::FailIntegrity,
        Outcome::PreflightFailed,
    );
    agrees!(
        integrity_level_wire_names_match_the_json,
        IntegrityLevel::ByteFingerprint,
        IntegrityLevel::ConsumeOnly,
        IntegrityLevel::NotAttempted,
    );
    agrees!(
        integrity_result_wire_names_match_the_json,
        IntegrityResult::Pass,
        IntegrityResult::Partial,
        IntegrityResult::Fail,
    );
    agrees!(
        lever_state_wire_names_match_the_json,
        LeverState::Honoured,
        LeverState::Ignored,
        LeverState::UnknownNotObservable,
    );
    agrees!(
        matrix_verdict_wire_names_match_the_json,
        MatrixVerdict::Pass,
        MatrixVerdict::PassDegraded,
        MatrixVerdict::Fail,
        MatrixVerdict::FailLeverNotHonoured,
        MatrixVerdict::UnsupportedLeverAbsent,
    );
}
