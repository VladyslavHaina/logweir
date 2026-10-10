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
//! and a 64 KiB sidecar. **This module never reads past an object's
//! `maxBytes` (FX-31).** It reads through `ObjectAccess::get` with that cap,
//! so an object whose size the store reports over it is refused before a
//! body byte is read, and one whose stream runs past it is cut off at the cap.
//! Either is reported `present: true, truncated: true` with NO bytes relayed
//! (no `sha256`, no `bytes`): the controller refuses a truncated object
//! whatever was relayed (`weirkeeper::evidence_fetch`'s "the cap is a refusal,
//! not a prefix"), so a prefix was never worth relaying. Before FX-31 the
//! whole object was read and then truncated, so only the pod's memory limit
//! bounded what one planted object could cost.
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

use logweir_engine_oso::storage::StoreError;

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
        match access.get(&o.key, o.max_bytes) {
            Ok(bytes) => {
                result.evidence.push(EvidenceObjectResult {
                    key: o.key.clone(),
                    stream: o.stream,
                    present: true,
                    sha256: Some(logweir_core::ids::sha256_prefixed(&bytes)),
                    bytes: Some(bytes.len() as u64),
                    code: None,
                    truncated: false,
                });
                extra.push((o.stream, bytes));
            }
            // FX-31: over the plan's `maxBytes`. The object is there and was
            // not read past the cap; nothing is relayed for it.
            Err(StoreError::TooLarge { .. }) => {
                result.evidence.push(EvidenceObjectResult {
                    key: o.key.clone(),
                    stream: o.stream,
                    present: true,
                    sha256: None,
                    bytes: None,
                    code: None,
                    truncated: true,
                });
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
