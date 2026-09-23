//! `destinationAccess`, and the per-role destination probes that
//! `operationReadiness` and `restorePreflight` reuse.
//!
//! # One probe per role, and each one answers a different question
//!
//! | role | probe | row |
//! |---|---|---|
//! | `archiveRead` | one bounded `list` under the destination's prefix | `destination.archiveListable` |
//! | `archiveWrite` | none — a write into an adopter's archive is what a RUN does | `destination.archivePrefixWritable` (execution-only) |
//! | `evidenceRead` | a `get` of a key nobody wrote, AS the `evidenceRead` principal | `destination.evidenceReadable` (advisory) |
//! | `evidenceWrite` | the optional create-only marker, AS the `evidenceWrite` principal | `destination.evidenceWritable` |
//!
//! **The `archiveWrite` row is execution-only on purpose.** D2 §4.2 permits a
//! check exactly one write, `logweir/readiness/<uid>.json`, and that key is
//! under the EVIDENCE root. Probing an archive-write grant would mean writing
//! an object into the adopter's archive prefix, which Global Constraint 6
//! forbids outright — so the honest answer is
//! `ArchivePrefixWriteVerifiedOnlyAtExecution`, and the run's own guards stay
//! authoritative (D2 §6.8).
//!
//! **An absent object is a PASSING evidence-read probe.** The `get` is of a
//! key that does not exist, so `ObjectNotFound` means the backend evaluated
//! the request and answered — which is the grant. A denial answers
//! `AccessDenied` or `InvalidCredentials` before it looks for the object.
//! Collapsing the two is the `NotFound`-versus-`Io` defect
//! `logweir_store::StoreError` exists to prevent, and it is the distinction
//! D2 §4.2 asks this probe for by name.
//!
//! # One row, one principal
//!
//! **`destination.evidenceWritable` is answered by the `evidenceWrite`
//! grant and by no other** (defect PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL).
//! When the destination separates that grant from the one the plan was
//! resolved for, the plan carries it as [`DestinationProbe::evidence_write`]
//! and the marker handle is built with it; the row then carries the fact
//! `grant=evidenceWrite` and names the Secret or ServiceAccount. When the plan
//! carries none, the two grants are one grant and the row says
//! `grant=destination`. The evidence-write handle is used for the ONE
//! create-only put and for nothing else — no read, no list, no delete — so the
//! probe needs `s3:PutObject` on `logweir/readiness/*` and nothing D2 §3.11
//! does not already grant that principal.
//!
//! **`destination.evidenceReadable` is answered by the `evidenceRead` grant
//! and by no other** (the class sweep of the same defect). A separate grant
//! arrives as [`DestinationProbe::evidence_read`]; the read handle is built
//! with it and used for the one `get` of an absent key. A grant no check pod
//! holds — `ControllerIdentity`, or none configured — is answered `unknown`
//! (`EvidenceReadNotConfigured`, advisory) with no request at all, never by
//! the destination grant. Every probed destination row carries the `grant`
//! fact: `destination` for the plan's own grant, or the role that answered.
//!
//! **`destination.archivePrefixWritable` is NEVER probed and never green**,
//! whatever the plan asks: the one key a check may write is under the evidence
//! root and proves nothing about the archive prefix (decision on
//! DESTINATIONACCESS-IGNORES-WRITEPROBE, 2026-09-23).

use logweir_core::check_contract::{
    CheckCode, CheckId, CheckOutcome, CheckPlanKind, CheckResult, DestinationAccessRequest,
    DestinationPlan, Gating, GrantRef,
};
use logweir_core::destination::DestinationRole;

use super::{execution_only, from_store_failure, ready, remedy_for, Wiring};
use crate::check::store::{self, StoreFailure, LIST_PROBE_KEYS};
use crate::check::{catalogue, Deadline, Emission};

/// The destination half of a check, as the three kinds that have one state it.
pub struct DestinationProbe<'a> {
    pub destination: &'a DestinationPlan,
    pub roles: &'a [DestinationRole],
    /// `writeProbe: CreateOnlyMarker` on the destination — the ONLY thing that
    /// makes `destination.evidenceWritable` a blocking, actually-probed row.
    pub write_probe: bool,
    /// The destination's `evidenceWrite` grant, when it is not the grant
    /// `destination.credentials` describes. The marker is written AS this
    /// principal; `None` means the two are one grant.
    pub evidence_write: Option<&'a GrantRef>,
    /// The destination's `evidenceRead` grant, when it is not the grant
    /// `destination.credentials` describes. The read probe runs AS this
    /// principal, or not at all; `None` means the two are one grant.
    pub evidence_read: Option<&'a GrantRef>,
}

/// The `grant` fact on `destination.evidenceWritable`: which principal the
/// marker was written as.
pub const GRANT_FACT: &str = "grant";
/// [`GRANT_FACT`] when the plan carried a separate `evidenceWrite` grant.
pub const GRANT_EVIDENCE_WRITE: &str = "evidenceWrite";
/// [`GRANT_FACT`] when the plan carried a separate `evidenceRead` grant.
pub const GRANT_EVIDENCE_READ: &str = "evidenceRead";
/// [`GRANT_FACT`] when the row was answered by the plan's destination grant.
pub const GRANT_DESTINATION: &str = "destination";

/// The principal clause an evidence-read row's message carries.
fn read_principal_clause(grant: Option<&GrantRef>) -> String {
    match grant {
        Some(g) => format!("as the evidence-read grant ({})", g.reference()),
        None => "as the destination's grant, which is also its evidence-read grant".to_string(),
    }
}

/// The principal clause a marker row's message carries.
fn principal_clause(grant: Option<&GrantRef>) -> String {
    match grant {
        Some(g) => format!("as the evidence-write grant ({})", g.reference()),
        None => "as the destination's grant, which is also its evidence-write grant".to_string(),
    }
}

/// The `BackupDestination` scope every destination row carries.
#[must_use]
fn scope(destination: &DestinationPlan) -> logweir_core::check_contract::CheckScope {
    catalogue::scope(
        "BackupDestination",
        &destination.name,
        Some(&destination.uid),
    )
}

/// Append the destination rows for the requested roles.
pub fn destination_checks(
    probe: &DestinationProbe<'_>,
    wiring: &dyn Wiring,
    deadline: Deadline,
    out: &mut Vec<CheckOutcome>,
) {
    let now = wiring.now();
    let dest = probe.destination;
    for role in probe.roles {
        // The budget is sliced across the roles left to probe, so three roles
        // on an unreachable endpoint cannot spend the whole check on the
        // first.
        let budget = deadline.slice(probe.roles.len() as u32);
        match role {
            DestinationRole::ArchiveRead => {
                let row = match wiring.objects(dest, *role, budget) {
                    Err(f) => from_store_failure(CheckId::DestinationArchiveListable, &f, now),
                    Ok(access) => {
                        let prefix = store::prefix_for(dest, *role);
                        match access.list_bounded(&prefix, LIST_PROBE_KEYS) {
                            Ok(_) => ready(
                                CheckId::DestinationArchiveListable,
                                CheckCode::ArchiveListable,
                                now,
                            )
                            .with_message(&format!(
                                "the archive prefix `{prefix}` on destination `{}` is listable",
                                dest.name
                            )),
                            Err(e) => {
                                let code = store::classify(&e);
                                super::catalogue_outcome_for_store(
                                    CheckId::DestinationArchiveListable,
                                    code,
                                    &format!(
                                        "listing the archive prefix `{prefix}` on destination \
                                         `{}` was refused",
                                        dest.name
                                    ),
                                    now,
                                )
                            }
                        }
                    }
                };
                out.push(
                    row.with_fact(GRANT_FACT, GRANT_DESTINATION)
                        .with_scope(scope(dest)),
                );
            }
            DestinationRole::ArchiveWrite => {
                out.push(
                    execution_only(
                        CheckId::DestinationArchivePrefixWritable,
                        CheckCode::ArchivePrefixWriteVerifiedOnlyAtExecution,
                        now,
                    )
                    // NEVER GREEN, AND IT SAYS WHY. The write probe, when the
                    // destination opts in, is under `logweir/readiness/` — not
                    // under the archive prefix — so it proves nothing here,
                    // and Global Constraint 6 forbids a check writing into the
                    // archive prefix to find out.
                    .with_message(&format!(
                        "the archive prefix `{}` is not write-probed: a check writes only its \
                         own readiness marker under `{}`, which proves nothing about the \
                         archive prefix, so the archive-write grant is verified by the run \
                         itself",
                        store::prefix_for(dest, DestinationRole::ArchiveWrite),
                        store::MARKER_PREFIX
                    ))
                    .with_scope(scope(dest)),
                );
            }
            DestinationRole::EvidenceRead => {
                // THE PRINCIPAL IS THE EVIDENCE-READ GRANT'S (class sweep of
                // PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL). A grant no pod
                // holds is answered here, with no request: reading as the
                // destination grant instead is the defect.
                let grant = probe.evidence_read;
                let grant_fact = if grant.is_some() {
                    GRANT_EVIDENCE_READ
                } else {
                    GRANT_DESTINATION
                };
                if let Some(g) = grant.filter(|g| !g.exercisable_in_pod()) {
                    out.push(
                        catalogue::outcome(
                            CheckId::DestinationEvidenceReadable,
                            logweir_core::check_contract::CheckState::Unknown,
                            CheckCode::EvidenceReadNotConfigured,
                            now,
                        )
                        .with_message(&format!(
                            "the evidence-read grant of destination `{}` is {}, which no check \
                             pod holds, so this check did not read the evidence root as any \
                             principal",
                            dest.name,
                            g.reference()
                        ))
                        .with_remedy(remedy_for(CheckCode::EvidenceReadNotConfigured))
                        .with_fact(GRANT_FACT, grant_fact)
                        .with_scope(scope(dest)),
                    );
                    continue;
                }
                let about = |f: &StoreFailure| {
                    let message = match grant {
                        Some(g) if f.message.contains(&g.reference()) => f.message.clone(),
                        _ => format!("{} {}", f.message, read_principal_clause(grant)),
                    };
                    from_store_failure(
                        CheckId::DestinationEvidenceReadable,
                        &StoreFailure::new(f.code, message),
                        now,
                    )
                };
                let row = match wiring.evidence_reader(dest, grant, budget) {
                    Err(f) => about(&f),
                    Ok(access) => {
                        let key = store::absent_probe_key(&dest.uid);
                        match access.get(&key) {
                            // A key nobody wrote: reading it is a SUCCESS.
                            Err(e) if store::classify(&e) == CheckCode::ObjectNotFound => ready(
                                CheckId::DestinationEvidenceReadable,
                                CheckCode::EvidenceReadable,
                                now,
                            )
                            .with_message(&format!(
                                "the evidence root on destination `{}` answered a read of an \
                                     absent key {}, which is the grant",
                                dest.name,
                                read_principal_clause(grant)
                            )),
                            // It should not exist; if it does, the read still
                            // proves the grant.
                            Ok(_) => ready(
                                CheckId::DestinationEvidenceReadable,
                                CheckCode::EvidenceReadable,
                                now,
                            )
                            .with_message(&format!(
                                "the evidence root on destination `{}` is readable {}",
                                dest.name,
                                read_principal_clause(grant)
                            )),
                            // THE KEY FAMILY, NOT THE KEY (reviewer finding
                            // F7). `logweir/readiness/<uid>.absent-probe` is
                            // one long run of the base64 alphabet — `/`, `-`
                            // and a UUID's hex all belong to it — so `redact`
                            // eats it whole and the operator loses the one fact
                            // the message carried. The family is fixed and
                            // public, the UID is already on `scope.uid`, and
                            // neither is credential-shaped.
                            Err(e) => super::catalogue_outcome_for_store(
                                CheckId::DestinationEvidenceReadable,
                                store::classify(&e),
                                &format!(
                                    "reading an absent probe key under `{}` on destination `{}` \
                                     {} was refused",
                                    store::MARKER_PREFIX,
                                    dest.name,
                                    read_principal_clause(grant)
                                ),
                                now,
                            ),
                        }
                    }
                };
                out.push(
                    row.with_fact(GRANT_FACT, grant_fact)
                        .with_scope(scope(dest)),
                );
            }
            DestinationRole::EvidenceWrite => {
                if !probe.write_probe {
                    out.push(
                        catalogue::outcome_gated(
                            CheckId::DestinationEvidenceWritable,
                            logweir_core::check_contract::CheckState::Unknown,
                            CheckCode::WriteNotProbed,
                            Gating::ExecutionOnly,
                            now,
                        )
                        .with_message(
                            "this destination configures no create-only write probe, so the \
                             evidence-write grant is verified when a run executes",
                        )
                        .with_remedy(remedy_for(CheckCode::WriteNotProbed))
                        .with_scope(scope(dest)),
                    );
                    continue;
                }
                // THE PRINCIPAL IS THE EVIDENCE-WRITE GRANT'S, and the wiring
                // is told so rather than left to assume it (defect
                // PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL). Passing `None`
                // here on a plan that carries a separate grant is the defect:
                // the row would say what the DESTINATION grant may do.
                let grant = probe.evidence_write;
                let grant_fact = if grant.is_some() {
                    GRANT_EVIDENCE_WRITE
                } else {
                    GRANT_DESTINATION
                };
                // Every refusal names the principal it is about, whichever of
                // the two steps refused: building the handle (a missing
                // projection, an absent identity) or the put itself.
                let about = |f: &StoreFailure| {
                    // A refusal that already names the grant (the principal
                    // selection's own) is not told its principal twice.
                    let message = match grant {
                        Some(g) if f.message.contains(&g.reference()) => f.message.clone(),
                        _ => format!("{} {}", f.message, principal_clause(grant)),
                    };
                    from_store_failure(
                        CheckId::DestinationEvidenceWritable,
                        &StoreFailure::new(f.code, message),
                        now,
                    )
                };
                let row = match wiring.evidence_writer(dest, grant, budget) {
                    Err(f) => about(&f),
                    Ok(access) => match store::put_marker(access.as_ref(), &dest.uid) {
                        Ok(outcome) => {
                            // The key FAMILY, not the key — see the
                            // evidence-read arm above for why (finding F7).
                            ready(CheckId::DestinationEvidenceWritable, outcome.code(), now)
                                .with_message(&format!(
                                    "the create-only readiness marker for this destination, \
                                     under `{}`, is write-authorised on destination `{}` {}",
                                    store::MARKER_PREFIX,
                                    dest.name,
                                    principal_clause(grant)
                                ))
                                // Reviewer question Q1: whether `PutMode::Create`
                                // was really enforced, or the backend declined it
                                // and the store fell back to HEAD-then-PUT. The
                                // grant is proved either way, so the CODE is the
                                // same; a reader that cares about the create-only
                                // guarantee reads this.
                                .with_fact(
                                    "createOnlyEnforced",
                                    if outcome.create_only_enforced() {
                                        "true"
                                    } else {
                                        "false"
                                    },
                                )
                        }
                        Err(f) => about(&f),
                    },
                };
                out.push(
                    row.with_fact(GRANT_FACT, grant_fact)
                        .with_scope(scope(dest)),
                );
            }
        }
    }
}

/// Run one `destinationAccess`.
#[must_use]
pub fn run(req: &DestinationAccessRequest, wiring: &dyn Wiring, deadline: Deadline) -> Emission {
    let now = wiring.now();
    let mut result = CheckResult::new(CheckPlanKind::DestinationAccess);
    result.checks.push(super::runner_contract(now));
    destination_checks(
        &DestinationProbe {
            destination: &req.destination,
            roles: &req.roles,
            write_probe: req.write_probe,
            evidence_write: req.evidence_write.as_ref(),
            evidence_read: req.evidence_read.as_ref(),
        },
        wiring,
        deadline,
        &mut result.checks,
    );
    Emission::of(result)
}
