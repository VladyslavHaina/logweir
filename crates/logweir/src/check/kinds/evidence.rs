//! `evidenceFetch` — D2 §3.9's relay of a receipt or a scorecard out of a
//! destination the controller cannot reach itself.
//!
//! # Why the controller asks a Job to read an object
//!
//! A `BackupDestination`'s credential is projected into runner pods and not
//! into the controller (D2 §3.8, §6.1). Verification needs the signed evidence
//! document; the controller cannot fetch it; so a short check Job fetches it
//! and relays the bytes as base64 part frames, whose per-stream digest the end
//! frame declares and `weirkeeper::check::relay` verifies.
//!
//! # Bounds
//!
//! `CheckPlan::validate` caps the request at three objects, a 1 MiB payload
//! and a 64 KiB sidecar. This module enforces the per-object `maxBytes`
//! again on the bytes it actually read, and reports `truncated: true` when the
//! object was longer — in which case the relayed digest is the PREFIX's digest
//! and not the object's, which is exactly why the flag exists.
//!
//! # The `key` is echoed VERBATIM, and that is deliberate
//!
//! `EvidenceObjectResult::key` is the plan's own key, returned unchanged and
//! un-redacted, because it is the CORRELATION between the request and the
//! answer: the controller asked for three objects and has to know which result
//! is which, and a redacted key would match nothing it holds. It is a
//! reference the controller wrote, not a secret the runner discovered — D2
//! §6.5's "what may appear: Secret and `ConfigMap` names and key names, which
//! are public references".
//!
//! It is the ONE field in a check's output that is not run through
//! `check_contract::redact`; every message, remedy, fact, scope and detail
//! sample is (see [`crate::check::catalogue::scope`] for why the scope is,
//! although it is a reference too). The exception is written down here rather
//! than left to be rediscovered.
//!
//! # `present: false` is never a guess
//!
//! `present` is `true` only when the object was read. A `false` requires a
//! `NotFound` from the backend; a denial is `present: false` WITH a `code`,
//! never a claim of absence. That is `EvidenceObjectResult`'s own contract and
//! the `NotFound`-versus-`Io` distinction `logweir_store::StoreError` makes.

use logweir_core::check_contract::{
    CheckCode, CheckPlanKind, CheckResult, EvidenceFetchRequest, EvidenceObjectResult,
};
use logweir_core::destination::DestinationRole;

use super::Wiring;
use crate::check::store;
use crate::check::{Deadline, Emission};

/// Run one `evidenceFetch`.
#[must_use]
pub fn run(req: &EvidenceFetchRequest, wiring: &dyn Wiring, deadline: Deadline) -> Emission {
    let mut result = CheckResult::new(CheckPlanKind::EvidenceFetch);
    let mut extra: Vec<(logweir_core::check_contract::Stream, Vec<u8>)> = Vec::new();

    // ONE handle for the whole fetch, built with the WHOLE remaining budget:
    // every object is read with the `evidenceRead` grant (`CheckPlan::validate`
    // refuses any other role), so a second handle would be a second credential
    // evaluation for the same principal.
    //
    // The request timeout is a property of the HANDLE and not of a call, so
    // there is no per-object slice to take; the loop is bounded by
    // `Deadline::has_room` instead, over at most three objects of at most
    // 1 MiB each. An earlier draft carried an unused `objects` binding that
    // implied a per-object slice this code never took (reviewer finding F6);
    // it is gone rather than half-implemented.
    let access = match wiring.objects(
        &req.destination,
        DestinationRole::EvidenceRead,
        deadline.remaining(),
    ) {
        Ok(a) => a,
        Err(f) => {
            for o in &req.objects {
                result.evidence.push(EvidenceObjectResult {
                    key: o.key.clone(),
                    stream: o.stream,
                    present: false,
                    sha256: None,
                    bytes: None,
                    code: Some(f.code),
                    truncated: false,
                });
            }
            return Emission::of(result);
        }
    };

    for o in &req.objects {
        if !deadline.has_room() {
            result.evidence.push(EvidenceObjectResult {
                key: o.key.clone(),
                stream: o.stream,
                present: false,
                sha256: None,
                bytes: None,
                code: Some(CheckCode::Timeout),
                truncated: false,
            });
            continue;
        }
        match access.get(&o.key) {
            Ok(bytes) => {
                let truncated = bytes.len() as u64 > o.max_bytes;
                let relayed: Vec<u8> = if truncated {
                    // A usize cast is safe: `max_bytes` is capped at 1 MiB by
                    // `CheckPlan::validate`, and the branch is reached only
                    // when the object is LONGER than it.
                    bytes[..o.max_bytes as usize].to_vec()
                } else {
                    bytes
                };
                result.evidence.push(EvidenceObjectResult {
                    key: o.key.clone(),
                    stream: o.stream,
                    present: true,
                    sha256: Some(logweir_core::ids::sha256_prefixed(&relayed)),
                    bytes: Some(relayed.len() as u64),
                    code: None,
                    truncated,
                });
                extra.push((o.stream, relayed));
            }
            Err(e) => {
                let code = store::classify(&e);
                result.evidence.push(EvidenceObjectResult {
                    key: o.key.clone(),
                    stream: o.stream,
                    present: false,
                    sha256: None,
                    bytes: None,
                    code: Some(code),
                    truncated: false,
                });
            }
        }
    }

    Emission {
        topics: Vec::new(),
        emit_topics: false,
        result,
        extra,
    }
}
