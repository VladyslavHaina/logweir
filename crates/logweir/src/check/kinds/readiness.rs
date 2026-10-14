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
    ConnectionPlan, Gating, OperationReadinessRequest,
};
use logweir_kafka::inventory::{CheckFailure, InventoryProbe, TopicPresence};

use super::access::{credential_bound_row, destination_checks, DestinationProbe};
use super::capability;
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
        // A switchover's review dials the restore's TARGET.
        CheckOperation::Restore | CheckOperation::Switchover => {
            (CheckId::TargetAuthenticated, None)
        }
        // A SOURCE-CONNECTION CHECK NAMES NO TOPIC, so it has no topic row —
        // the same rule `kinds::source_connection` states at length, kept here
        // too because this table is what any caller reads to find out which
        // rows an operation owns, and an operation missing from it would be a
        // second place the answer could differ.
        CheckOperation::SourceConnection => (CheckId::ConnectionAuthenticated, None),
        CheckOperation::Backup | CheckOperation::DestinationAccess => (
            CheckId::ConnectionAuthenticated,
            Some(CheckId::ConnectionTopicsDescribable),
        ),
    }
}

/// What to do when a cluster's bootstrap address answers and the brokers it
/// advertises do not: the remedy [`authenticated`] gives in place
/// of `BrokerUnreachable`'s generic one, which is about the bootstrap address.
pub const ADVERTISED_UNREACHABLE_REMEDY: &str =
    "A connection test that only reads the cluster id passes here and nothing else can. Check \
     advertised.listeners for the listener this bootstrap address belongs to: every broker must \
     advertise a host and port this runner can resolve and reach. Use the bootstrap address of \
     a listener meant for this network, or fix its advertisement.";

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
    // The scope is on the failure rows too: `connection.authenticated
    // notReady/BrokerUnreachable` is the row an operator reads, and D2 §6.3
    // names "check time AND scope".
    let scoped = |row: CheckOutcome| match scope.clone() {
        Some(s) => row.with_scope(s),
        None => row,
    };
    let cluster_id = match probe.cluster_id() {
        Ok(c) => c,
        Err(f) => return (scoped(from_broker_failure(id, &f, now)), None),
    };
    let listing = match probe.list_topics() {
        Ok(l) => l,
        Err(f) => {
            let mut row = scoped(from_broker_failure(id, &f, now));
            // THE BOOTSTRAP ANSWERED AND THE ADVERTISED BROKERS DID
            // NOT. The cluster id above came off the bootstrap connection; a
            // listing that then cannot reach a broker is the cluster telling
            // this client to dial an address it cannot reach. Measured on the
            // `confluent` profile's OFFNET listener (it advertises
            // `127.0.0.1:1`): `cluster-probe` reads the id and answers
            // `reachable=true`, this row is `BrokerUnreachable`, and a backup
            // fails after a minute with only "kafka-backup backup exited 1".
            // The generic remedy for that code talks about the BOOTSTRAP
            // address, which is the one thing that works here.
            if let (Some(cluster), CheckCode::BrokerUnreachable) = (cluster_id.as_deref(), f.code) {
                row = row
                    .with_message(&format!(
                        "the bootstrap address answered and named cluster {cluster}, and the \
                         brokers that cluster advertises did not answer a metadata request: the \
                         advertised listeners are not reachable from here"
                    ))
                    .with_remedy(ADVERTISED_UNREACHABLE_REMEDY)
                    .with_fact("clusterId", cluster);
            }
            return (row, cluster_id);
        }
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
/// remember. It is [`crate::check::redact_path`] rather than the whole-string
/// form: a sample is a segment key or a topic name, and the per-segment form
/// is the STRICTER of the two (it still redacts a forty-character component
/// inside a path that the whole-string rule would read as anchored).
#[must_use]
pub fn detail(names: &[String]) -> serde_json::Value {
    serde_json::json!({
        "count": names.len(),
        "sample": names
            .iter()
            .take(DETAIL_SAMPLE)
            .map(|n| sample_value(n))
            .collect::<Vec<_>>(),
    })
}

/// How many names a bounded `detail` carries.
pub const DETAIL_SAMPLE: usize = 10;

/// How long one sampled name may be.
///
/// TWO HUNDRED AND FIFTY-SIX characters. A Kafka topic name is at most 249, so
/// no topic is ever cut; an S3 object key may be 1,024 bytes, and ten of those
/// would be a ten-kilobyte `detail` inside a record D2 §6.4 sizes at about
/// 1.2 KiB. The cap is on the VALUE and the value is then re-serialised by
/// `serde_json`, so the JSON stays parseable, unlike a cap on the rendered
/// line.
pub const DETAIL_VALUE_MAX_CHARS: usize = 256;

/// One sampled name: redacted as a key path, then truncated as a VALUE.
///
/// The ellipsis says the name was cut, so a reader does not take a prefix for
/// a whole key.
#[must_use]
fn sample_value(name: &str) -> String {
    let redacted = crate::check::redact_path(name);
    if redacted.chars().count() <= DETAIL_VALUE_MAX_CHARS {
        return redacted;
    }
    let mut out: String = redacted.chars().take(DETAIL_VALUE_MAX_CHARS - 1).collect();
    out.push('…');
    out
}

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
    // The capability rows the plan lists, and only those. The plan
    // validation has already refused an id that is not this operation's.
    let capabilities: Vec<CheckId> = req.capability_checks.clone();
    // The broker half gets half the budget; the destination half gets the
    // rest. Neither can spend the whole check on an unreachable endpoint.
    let broker_budget = deadline.slice(2);
    // `connection.topicsReadable` of a Backup (FX-45): asked over the same
    // connection as the rows above, when it authenticated.
    let mut readable: Option<CheckOutcome> = None;
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
            for id in &capabilities {
                push(
                    capability::blocked(
                        *id,
                        "the connection did not authenticate, so this check did not run",
                        now,
                    )
                    .with_scope(scope.clone()),
                    &skip,
                    &mut checks,
                );
            }
        }
        Ok(probe) => {
            let (row, _) = authenticated(auth_id, probe.as_ref(), Some(scope.clone()), now);
            let authenticated_ok = row.state == CheckState::Ready;
            push(row, &skip, &mut checks);
            let mut topics_ok = authenticated_ok;
            if let Some(tid) = topics_id {
                let row = if !authenticated_ok {
                    blocked(tid, now)
                } else if req.topics.is_empty() {
                    ready(tid, CheckCode::TopicsDescribable, now)
                        .with_message("the operation selects no topic by name")
                } else {
                    topics_describable(tid, probe.as_ref(), &req.topics, deadline, now)
                };
                topics_ok = row.state == CheckState::Ready;
                push(row.with_scope(scope.clone()), &skip, &mut checks);
            }
            for row in capability_rows(
                &capabilities,
                &req.connection,
                probe.as_ref(),
                &req.topics,
                (authenticated_ok, topics_ok),
                deadline,
                now,
            ) {
                push(row.with_scope(scope.clone()), &skip, &mut checks);
            }
            if req.operation == CheckOperation::Backup
                && authenticated_ok
                && !skip.contains(&CheckId::ConnectionTopicsReadable)
            {
                readable = Some(topics_readable(probe.as_ref(), &req.topics, deadline, now));
            }
        }
    }

    if req.operation == CheckOperation::Backup {
        push(
            readable
                .unwrap_or_else(|| read_verified_at_execution(None, now))
                .with_scope(scope),
            &skip,
            &mut checks,
        );
    }

    if let Some(destination) = req.destination.as_ref() {
        // Every grant the run presents, compared with no request —
        // a separated `evidenceWrite` Secret included, which no row probes
        // unless the destination opted in to the marker.
        if let Some(row) = credential_bound_row(&[destination], wiring) {
            push(row, &skip, &mut checks);
        }
        let mut rows = Vec::new();
        destination_checks(
            &DestinationProbe {
                destination,
                roles: &req.roles,
                write_probe: req.write_probe,
                evidence_write: req.evidence_write.as_ref(),
                evidence_read: req.evidence_read.as_ref(),
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

/// The capability rows `listed`, in the plan's order, over one
/// authenticated connection.
///
/// ONE ApiVersions observation serves every row that reads it, and it is
/// made only when such a row is listed. `passed` is whether the connection
/// authenticated and whether every selected topic is describable: a
/// capability row about a connection that did not authenticate, or about
/// topics the principal cannot even describe, has no honest answer of its own
/// and says which row to fix first.
#[must_use]
pub fn capability_rows(
    listed: &[CheckId],
    connection: &ConnectionPlan,
    probe: &dyn InventoryProbe,
    topics: &[String],
    passed: (bool, bool),
    deadline: Deadline,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<CheckOutcome> {
    let (authenticated_ok, topics_ok) = passed;
    if !authenticated_ok {
        return listed
            .iter()
            .map(|id| {
                capability::blocked(
                    *id,
                    "the connection did not authenticate, so this check did not run",
                    now,
                )
            })
            .collect();
    }
    let reads_api_versions = |id: &CheckId| {
        matches!(
            id,
            CheckId::ConnectionEngineProtocol
                | CheckId::TargetEngineProtocol
                | CheckId::ConnectionGroupTypes
        )
    };
    // ONE observation, inside what is left of the check's own budget, and not
    // started with none left: a dial begun after the deadline would overrun
    // the check and report the clock as a broker problem.
    let observed = listed.iter().any(reads_api_versions).then(|| {
        if deadline.has_room() {
            probe.api_versions(deadline.remaining())
        } else {
            Err(CheckFailure::new(
                CheckCode::ApiVersionsNotObserved,
                "the check's time budget was spent before the ApiVersions observation could \
                 start, so the endpoint was not asked which request versions it serves",
            ))
        }
    });
    let sasl = capability::engine_uses_sasl(connection);
    listed
        .iter()
        .filter_map(|id| match (id, observed.as_ref()) {
            (CheckId::ConnectionEngineProtocol | CheckId::TargetEngineProtocol, Some(observed)) => {
                Some(capability::engine_protocol(*id, observed, sasl, now))
            }
            (CheckId::ConnectionGroupTypes, Some(observed)) => {
                Some(capability::group_types(observed, now))
            }
            (CheckId::ConnectionTopicConfigsReadable, _) => Some(if topics_ok {
                capability::topic_configs_readable(probe, topics, deadline, now)
            } else {
                capability::blocked(
                    *id,
                    "a selected topic is not describable by this principal, so its \
                     configuration read was not attempted",
                    now,
                )
            }),
            // Not a capability row: the plan validation refused it already.
            _ => None,
        })
        .collect()
}

/// `connection.topicsReadable` as it was before FX-45, and as it stays
/// whenever the broker did not say: execution-only, `unknown`, and why.
fn read_verified_at_execution(
    why: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> CheckOutcome {
    let base = "a check reads metadata only; whether this principal may CONSUME each selected \
                topic is established by the run";
    execution_only(
        CheckId::ConnectionTopicsReadable,
        CheckCode::ReadVerifiedOnlyAtExecution,
        now,
    )
    .with_message(&match why {
        Some(why) => format!("{base}: {why}"),
        None => base.to_string(),
    })
}

/// The remedy of a `connection.topicsReadable` refusal: the grant, named.
pub const READ_DENIED_REMEDY: &str =
    "Grant READ on each named topic to the principal this connection authenticates as (READ \
     implies DESCRIBE), or remove the topic from the selection. `logweir backup run` refuses \
     these topics by name before its engine starts.";

/// **`connection.topicsReadable`: whether this principal may READ each
/// selected topic, as the broker reports it** (FX-45).
///
/// One DescribeTopics request with the caller's authorized operations
/// (KIP-430). The row was execution-only, so a principal without `Read`
/// passed every Preflight and failed a minute later in the engine with
/// "kafka-backup backup exited 1". Now:
///
/// * the broker REPORTS that READ is missing on a selected topic (its
///   authorized operations exclude READ, or it refuses to describe the topic
///   at all): BLOCKING `notReady`, `TopicAuthorizationFailed`, naming the
///   topics and the grant;
/// * otherwise the row stays what it was, execution-only `unknown`
///   (`ReadVerifiedOnlyAtExecution`), and its message says what the broker
///   reported: READ on every topic, or why it is not known (a broker that
///   does not compute authorized operations, a read that failed).
///
/// **A reported grant does not make the row `ready`.** Only the run consumes,
/// and no code an older controller reads says "readable"; an older controller
/// reads every code and gating value this row now emits (FX-47's rule).
#[must_use]
pub fn topics_readable(
    probe: &dyn InventoryProbe,
    topics: &[String],
    deadline: Deadline,
    now: chrono::DateTime<chrono::Utc>,
) -> CheckOutcome {
    if topics.is_empty() {
        return read_verified_at_execution(Some("the operation selects no topic by name"), now);
    }
    if !deadline.has_room() {
        return read_verified_at_execution(
            Some("the check's budget was spent before the broker could be asked"),
            now,
        );
    }
    let reads = match probe.topic_read_access(topics) {
        Ok(reads) => reads,
        Err(f) => {
            return read_verified_at_execution(
                Some(&format!(
                    "the broker could not be asked for this principal's operations on them ({})",
                    f.message
                )),
                now,
            )
        }
    };
    let denied: Vec<String> = reads
        .iter()
        .filter(|(_, r)| r.is_denied())
        .map(|(t, _)| t.clone())
        .collect();
    if !denied.is_empty() {
        let hidden = reads
            .iter()
            .filter(|(_, r)| {
                *r == logweir_kafka::access::TopicRead::Denied(
                    logweir_kafka::access::ReadDenied::NotDescribable,
                )
            })
            .count();
        let code = CheckCode::TopicAuthorizationFailed;
        return catalogue::outcome_gated(
            CheckId::ConnectionTopicsReadable,
            CheckState::NotReady,
            code,
            Gating::Blocking,
            now,
        )
        .with_message(&format!(
            "the broker reports that this principal may not READ {} of {} selected topic(s){}, \
             so a backup of them would fail in the engine: grant READ on each",
            denied.len(),
            topics.len(),
            if hidden > 0 {
                format!(" ({hidden} of them it may not even Describe)")
            } else {
                String::new()
            }
        ))
        .with_remedy(READ_DENIED_REMEDY)
        .with_detail(detail(&denied));
    }
    let unknown: Vec<&str> = reads
        .iter()
        .filter_map(|(_, r)| match r {
            logweir_kafka::access::TopicRead::Unknown(why) => Some(why.as_str()),
            _ => None,
        })
        .collect();
    match unknown.first() {
        None => read_verified_at_execution(
            Some(&format!(
                "the broker reports READ for this principal on all {} selected topic(s)",
                topics.len()
            )),
            now,
        ),
        Some(why) => read_verified_at_execution(
            Some(&format!(
                "whether it may READ {} of {} selected topic(s) is not known before the run \
                 ({why})",
                unknown.len(),
                topics.len()
            )),
            now,
        ),
    }
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
