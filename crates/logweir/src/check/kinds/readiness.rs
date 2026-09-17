//! `operationReadiness` — D2 §6.3's Backup catalogue, from the check Job's
//! side.
//!
//! # Which rows are here, and which are deliberately not
//!
//! Every row whose §6.3 authority contains **J**, minus `connection.
//! clusterIdentity`. The runner publishes the observed cluster id as the
//! `clusterId` FACT on `connection.authenticated`; the verdict needs
//! `KafkaCluster.status.clusterId` and `TrustRoster.allowedClusterIds`, which a
//! credential-holding Job is deliberately not given Kubernetes read for. See
//! [`crate::check::catalogue`].
//!
//! # `skipCheck` means "do not run it and do not report it"
//!
//! The closed code vocabulary has no "the requester asked me not to" spelling,
//! and inventing one would put a reason string into a `metav1.Condition` that
//! no UI has text for. A skipped row is therefore ABSENT from the relay, and
//! the controller — which chose the skip — renders it. D2 §6.4's `skipped`
//! state stays reachable for the rows the controller itself skips
//! (`approval.state` for a draft, `SubjectNotCreated`).

use std::collections::BTreeSet;

use logweir_core::check_contract::{
    CheckCode, CheckId, CheckOperation, CheckOutcome, CheckPlanKind, CheckResult, CheckState,
    ConnectionPlan, OperationReadinessRequest,
};
use logweir_kafka::inventory::{InventoryProbe, TopicPresence};

use super::access::{destination_checks, DestinationProbe};
use super::{execution_only, from_broker_failure, ready, remedy_for, runner_contract, Wiring};
use crate::check::{catalogue, Deadline, Emission};

/// Which connection rows an operation uses.
///
/// D2 §6.3: "Operation `Restore` runs the target equivalents
/// (`target.resolved`, `target.credentialProjected`, `target.authenticated`)",
/// so the id depends on what the connection IS to this operation, not on how
/// it is dialled.
#[must_use]
pub fn connection_ids(operation: CheckOperation) -> (CheckId, Option<CheckId>) {
    match operation {
        CheckOperation::Restore => (CheckId::TargetAuthenticated, None),
        CheckOperation::Backup | CheckOperation::DestinationAccess => (
            CheckId::ConnectionAuthenticated,
            Some(CheckId::ConnectionTopicsDescribable),
        ),
    }
}

/// `connection.authenticated` / `target.authenticated`: the cluster id and the
/// broker count, off ONE metadata read.
///
/// The two facts are D2 §6.3's, and they are the J half of the J+C
/// `clusterIdentity` row as well — which is why they are facts and not a
/// second check.
pub fn authenticated(
    id: CheckId,
    probe: &dyn InventoryProbe,
    scope: Option<logweir_core::check_contract::CheckScope>,
    now: chrono::DateTime<chrono::Utc>,
) -> (CheckOutcome, Option<String>) {
    let cluster_id = match probe.cluster_id() {
        Ok(c) => c,
        Err(f) => return (from_broker_failure(id, &f, now), None),
    };
    let listing = match probe.list_topics() {
        Ok(l) => l,
        Err(f) => return (from_broker_failure(id, &f, now), cluster_id),
    };
    let mut row = ready(id, CheckCode::Authenticated, now)
        .with_message("the broker answered a metadata request for this principal")
        .with_fact("brokerCount", &listing.broker_count.to_string());
    if let Some(c) = cluster_id.as_deref() {
        row = row.with_fact("clusterId", c);
    }
    if let Some(s) = scope {
        row = row.with_scope(s);
    }
    (row, cluster_id)
}

/// `connection.topicsDescribable` — targeted metadata over the operation's
/// selected topics.
///
/// The three answers are the three a broker gives (`TopicPresence`), and the
/// AUTHORIZATION answer is never reported as absence: a principal without
/// `DESCRIBE` gets `TOPIC_AUTHORIZATION_FAILED` whether or not the topic
/// exists, so `TopicNotAuthorized` is a visibility fact and `TopicNotFound`
/// is an existence fact. D2 §5.4 is the same distinction from the discovery
/// side.
#[must_use]
pub fn topics_describable(
    id: CheckId,
    probe: &dyn InventoryProbe,
    topics: &[String],
    deadline: Deadline,
    now: chrono::DateTime<chrono::Utc>,
) -> CheckOutcome {
    let mut not_found: Vec<String> = Vec::new();
    let mut not_authorized: Vec<String> = Vec::new();
    let mut unknown: Vec<String> = Vec::new();
    for name in topics {
        if !deadline.has_room() {
            unknown.push(name.clone());
            continue;
        }
        match probe.describe_topic(name) {
            Ok(TopicPresence::Present { .. }) => {}
            Ok(TopicPresence::NotFound) => not_found.push(name.clone()),
            Ok(TopicPresence::NotAuthorized) => not_authorized.push(name.clone()),
            // A targeted probe that FAILED is "I could not tell", never
            // "it is not there".
            Ok(TopicPresence::Unknown) | Err(_) => unknown.push(name.clone()),
        }
    }
    // ORDER MATTERS: a not-found topic is a plan that cannot run, a
    // not-authorized topic is a plan that may run and may silently read less,
    // and an unknown is neither. The strongest finding decides the row.
    let (code, sample) = if !not_found.is_empty() {
        (CheckCode::TopicNotFound, not_found)
    } else if !not_authorized.is_empty() {
        (CheckCode::TopicNotAuthorized, not_authorized)
    } else if !unknown.is_empty() {
        (CheckCode::TopicVisibilityUnknown, unknown)
    } else {
        return ready(id, CheckCode::TopicsDescribable, now).with_message(&format!(
            "all {} selected topic(s) are describable by this principal",
            topics.len()
        ));
    };
    catalogue::outcome(id, super::state_for(code), code, now)
        .with_message(&format!(
            "{} of {} selected topic(s) answered {code}",
            sample.len(),
            topics.len()
        ))
        .with_remedy(remedy_for(code))
        .with_detail(detail(&sample))
}

/// `{"count": N, "sample": [… ≤ 10 …]}` — D2 §6.4's bounded detail shape, with
/// every sampled name **redacted**.
///
/// TEN, and the full list goes to the `details` stream. A status field that
/// grew with the number of collisions is how a `Preflight` object stops
/// fitting in etcd.
///
/// The redaction is the same chokepoint argument
/// [`crate::check::catalogue::scope`] records: `CheckOutcome::with_detail`
/// does not redact, and a rule with two exceptions is a rule a reader has to
/// remember. `redact` fires only on credential shapes, so an ordinary topic
/// name reaches the UI unchanged.
#[must_use]
pub fn detail(names: &[String]) -> serde_json::Value {
    serde_json::json!({
        "count": names.len(),
        "sample": names
            .iter()
            .take(DETAIL_SAMPLE)
            .map(|n| logweir_core::check_contract::redact(n))
            .collect::<Vec<_>>(),
    })
}

/// How many names a bounded `detail` carries.
pub const DETAIL_SAMPLE: usize = 10;

/// Run one `operationReadiness`.
#[must_use]
pub fn run(req: &OperationReadinessRequest, wiring: &dyn Wiring, deadline: Deadline) -> Emission {
    let now = wiring.now();
    let skip: BTreeSet<CheckId> = req.skip_checks.iter().copied().collect();
    let mut checks: Vec<CheckOutcome> = Vec::new();
    let push = |c: CheckOutcome, skip: &BTreeSet<CheckId>, checks: &mut Vec<CheckOutcome>| {
        if !skip.contains(&c.id) {
            checks.push(c);
        }
    };

    push(runner_contract(now), &skip, &mut checks);

    let (auth_id, topics_id) = connection_ids(req.operation);
    let scope = connection_scope(&req.connection, req.operation);
    // The broker half gets half the budget; the destination half gets the
    // rest. Neither can spend the whole check on an unreachable endpoint.
    let broker_budget = deadline.slice(2);
    match wiring.broker(&req.connection, broker_budget) {
        Err(f) => {
            push(
                from_broker_failure(auth_id, &f, now).with_scope(scope.clone()),
                &skip,
                &mut checks,
            );
            if let Some(tid) = topics_id {
                push(
                    blocked(tid, now).with_scope(scope.clone()),
                    &skip,
                    &mut checks,
                );
            }
        }
        Ok(probe) => {
            let (row, _) = authenticated(auth_id, probe.as_ref(), Some(scope.clone()), now);
            let authenticated_ok = row.state == CheckState::Ready;
            push(row, &skip, &mut checks);
            if let Some(tid) = topics_id {
                let row = if !authenticated_ok {
                    blocked(tid, now)
                } else if req.topics.is_empty() {
                    ready(tid, CheckCode::TopicsDescribable, now)
                        .with_message("the operation selects no topic by name")
                } else {
                    topics_describable(tid, probe.as_ref(), &req.topics, deadline, now)
                };
                push(row.with_scope(scope.clone()), &skip, &mut checks);
            }
        }
    }

    if req.operation == CheckOperation::Backup {
        push(
            execution_only(
                CheckId::ConnectionTopicsReadable,
                CheckCode::ReadVerifiedOnlyAtExecution,
                now,
            )
            .with_message(
                "a check reads metadata only; whether this principal may CONSUME each selected \
                 topic is established by the run",
            )
            .with_scope(scope),
            &skip,
            &mut checks,
        );
    }

    if let Some(destination) = req.destination.as_ref() {
        let mut rows = Vec::new();
        destination_checks(
            &DestinationProbe {
                destination,
                roles: &req.roles,
                write_probe: req.write_probe,
            },
            wiring,
            deadline,
            &mut rows,
        );
        for row in rows {
            push(row, &skip, &mut checks);
        }
    }

    if let Some(path) = req.signer_path.as_deref() {
        push(signer_row(path, wiring, now), &skip, &mut checks);
    }

    let mut result = CheckResult::new(CheckPlanKind::OperationReadiness);
    result.checks = checks;
    Emission::of(result)
}

/// The `KafkaCluster` scope a connection row carries.
///
/// The plan carries no `KafkaCluster` name or UID — it carries a resolved
/// CONNECTION — so the scope names the principal, which is the identity this
/// check actually exercised and the one an operator matches against an ACL
/// export.
#[must_use]
fn connection_scope(
    plan: &ConnectionPlan,
    operation: CheckOperation,
) -> logweir_core::check_contract::CheckScope {
    let kind = match operation {
        CheckOperation::Restore => "RestoreTarget",
        _ => "KafkaCluster",
    };
    catalogue::scope(kind, &plan.principal, None)
}

/// A row that could not run because the one before it did not pass.
#[must_use]
fn blocked(id: CheckId, now: chrono::DateTime<chrono::Utc>) -> CheckOutcome {
    catalogue::outcome(
        id,
        CheckState::Unknown,
        CheckCode::BlockedByPrerequisite,
        now,
    )
    .with_message("the connection did not authenticate, so this check did not run")
    .with_remedy(remedy_for(CheckCode::BlockedByPrerequisite))
}

/// `signer.privateKeyUsable` — parse the projected key, sign a probe with it,
/// verify that signature with the derived public half, and publish the PUBLIC
/// key id as a fact.
///
/// The PRIVATE half never leaves `ValidatedSigner`, and the key id is a SHA-256
/// of the SubjectPublicKeyInfo DER — a public identifier the roster is written
/// in.
#[must_use]
fn signer_row(path: &str, wiring: &dyn Wiring, now: chrono::DateTime<chrono::Utc>) -> CheckOutcome {
    match wiring.signer_key_id(path) {
        Ok(key_id) => ready(
            CheckId::SignerPrivateKeyUsable,
            CheckCode::SignerUsable,
            now,
        )
        .with_message("the projected signing key parsed, signed a probe and verified it")
        .with_fact("signerKeyId", &key_id),
        Err(detail) => {
            // WHICH failure it was decides the remedy: a file that is not
            // there is a mount problem, a file that is there and will not
            // parse is a key problem. `ValidatedSigner::load`'s own message
            // names the path, and `with_message` redacts it.
            let code = if std::path::Path::new(path).exists() {
                CheckCode::SigningKeyInvalid
            } else {
                CheckCode::SigningKeyMissing
            };
            catalogue::outcome(
                CheckId::SignerPrivateKeyUsable,
                CheckState::NotReady,
                code,
                now,
            )
            .with_message(&detail)
            .with_remedy(remedy_for(code))
        }
    }
}
