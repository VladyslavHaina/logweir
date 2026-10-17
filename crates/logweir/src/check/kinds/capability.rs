//! **The capability rows** — does this endpoint HAVE what the
//! operation needs, said from the endpoint's own answers before the operation
//! starts.
//!
//! # What a capability row is, and what it is not
//!
//! The rows D2 §6.3 already owns ask whether THIS PRINCIPAL may do something
//! (authenticate, describe a topic, create one). These ask whether THIS
//! ENDPOINT can: a Kafka-compatible endpoint is not always Apache Kafka, and
//! a connection test passing says nothing about a restore
//! (`docs/to-do/decisions/PROD-01.2-compatibility-contract.md`). Each row
//! names the missing capability and what to do instead, in its message and
//! remedy.
//!
//! | row | asks | gating |
//! |---|---|---|
//! | `connection.engineProtocol`, `target.engineProtocol` | does the endpoint serve every request version the engine sends, which it never negotiates | blocking: without it the operation cannot run at all |
//! | `connection.topicConfigsReadable` | does DescribeConfigs answer for every selected topic | advisory: the backup runs, and records the configuration as not captured |
//! | `connection.groupTypes` | does the endpoint's group listing name each group's type | advisory: the backup runs, and records a selected consumer group as not captured |
//!
//! # Listed by the plan, or not emitted
//!
//! A runner emits a capability row only for an id the plan lists in
//! `capabilityChecks`. The id vocabulary is closed on the reading side too, so
//! volunteering a new id to an older controller would make it refuse the
//! whole result as unreadable; a controller that knows the ids asks for them.
//!
//! # NOT OBSERVED is `unknown`, never `ready`
//!
//! Every row here reads an answer. When there is none (the ApiVersions answer
//! was not in the client's log, a DescribeConfigs call timed out) the row is
//! `unknown` with the reason. No arm turns "I could not tell" into a verdict
//! in either direction.
//!
//! **And an answer is about the CLUSTER only when every broker gave one.**
//! The engine may be sent to any broker, so a row that read two
//! brokers of three has not read the cluster: it is `unknown`, and its
//! message names how many of how many answered and which did not
//! (`logweir_kafka::api_versions::full_view`). When the brokers' answers
//! differ, the row is judged on what every one of them serves, and says so.

use chrono::{DateTime, Utc};
use logweir_core::check_contract::{CheckCode, CheckId, CheckOutcome, CheckState, ConnectionPlan};
use logweir_engine_oso::vendored::request_versions::{
    capture_requests, replay_requests, EngineRequest,
};
use logweir_kafka::api_versions::{ApiVersions, LIST_GROUPS, LIST_GROUPS_WITH_TYPES};
use logweir_kafka::inventory::{CheckFailure, InventoryProbe};

use super::readiness::detail;
use super::{ready, remedy_for, state_for};
use crate::check::{catalogue, Deadline};

/// The fact every row that read the ApiVersions answer carries: how many
/// DISTINCT BROKERS answered, of how many the cluster lists. The two numbers
/// are equal on every row that has an answer at all: a partial view is
/// `unknown` and has no fact. Never a count of connections.
const BROKERS_ANSWERED: &str = "brokersAnswered";

fn brokers_answered(served: &ApiVersions) -> String {
    format!("{0} of {0}", served.brokers())
}

/// Whose answer a message reports: the endpoint's one broker, or every one of
/// its brokers. The ranges are the versions EVERY broker serves, so "all N
/// brokers serve v3-v7" is true of a mixed cluster too.
fn whose(served: &ApiVersions) -> String {
    match served.brokers() {
        1 => "this endpoint serves".to_string(),
        2 => "both brokers of this endpoint serve".to_string(),
        n => format!("all {n} brokers of this endpoint serve"),
    }
}

/// The sentence a message ends with when the brokers' answers were not all
/// the same: the row was judged on the weakest of them.
fn differing(served: &ApiVersions) -> &'static str {
    if served.differ() {
        " The brokers do not all serve the same versions: this row is judged on the versions \
         every one of them serves."
    } else {
        ""
    }
}

/// Whether the engine authenticates with SASL on this connection, and so
/// sends SaslHandshake and SaslAuthenticate before anything else.
///
/// Every `authMode` but the two that carry no SASL exchange. A mode this
/// build cannot dial never reaches a capability row: its
/// `*.authenticated` row has already failed.
#[must_use]
pub fn engine_uses_sasl(plan: &ConnectionPlan) -> bool {
    !matches!(
        plan.auth_mode.as_str(),
        crate::check::kafka::AUTH_MODE_PLAINTEXT | crate::check::kafka::AUTH_MODE_MTLS
    )
}

/// [`engine_uses_sasl`] for a spec's `auth:` block, which is what the
/// standalone commands hold.
#[must_use]
pub fn spec_uses_sasl(auth: &logweir_core::spec::AuthSpec) -> bool {
    !matches!(
        auth,
        logweir_core::spec::AuthSpec::Plaintext | logweir_core::spec::AuthSpec::Mtls { .. }
    )
}

/// How long a standalone command (`backup run`, `drill run`, `doctor`) gives
/// the endpoint's ApiVersions observation before its operation starts
/// (FX-45): the observation's own ceiling. It ends as soon as every broker
/// answered, so a healthy cluster costs one quiet period.
pub const STANDALONE_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// **FX-45: a standalone command refuses on a capability the endpoint was
/// SEEN not to have.** `Some(sentence)` for a `notReady` row, naming the row,
/// what is missing and the fallback; `None` for `ready`, and for `unknown`:
/// not observed is never a refusal, so a command run where the observation
/// cannot be made works as it did before.
#[must_use]
pub fn refusal_of(row: &CheckOutcome) -> Option<String> {
    (row.state == CheckState::NotReady).then(|| {
        let remedy = if row.remedy.is_empty() {
            String::new()
        } else {
            format!(" {}", row.remedy)
        };
        format!(
            "{} is notReady ({}): {}{remedy}",
            row.id, row.code, row.message
        )
    })
}

/// The requests the engine sends for the operation `id` is about, and the
/// word for that operation in a message.
fn engine_requests(id: CheckId, sasl: bool) -> (Vec<EngineRequest>, &'static str) {
    match id {
        CheckId::TargetEngineProtocol => (replay_requests(sasl), "restore into"),
        _ => (capture_requests(sasl), "back up from"),
    }
}

fn spelled(r: &EngineRequest) -> String {
    format!("{} v{}", r.api, r.version)
}

/// A row whose ApiVersions answer was not observed.
fn not_observed(id: CheckId, f: &CheckFailure, now: DateTime<Utc>) -> CheckOutcome {
    catalogue::outcome(
        id,
        CheckState::Unknown,
        CheckCode::ApiVersionsNotObserved,
        now,
    )
    .with_message(&f.message)
    .with_remedy(remedy_for(CheckCode::ApiVersionsNotObserved))
}

/// `connection.engineProtocol` / `target.engineProtocol`: every request the
/// engine sends for this operation, at the version it sends it, against what
/// the endpoint said it serves.
///
/// **The engine's versions are the vendored table's**
/// (`logweir_engine_oso::vendored::request_versions`, held to the pinned
/// engine source by the xtask drift gate), never a list written here.
#[must_use]
pub fn engine_protocol(
    id: CheckId,
    observed: &Result<ApiVersions, CheckFailure>,
    sasl: bool,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let served = match observed {
        Ok(v) => v,
        Err(f) => return not_observed(id, f, now),
    };
    let (requests, operation) = engine_requests(id, sasl);
    let sent: Vec<String> = requests.iter().map(spelled).collect();
    let refused: Vec<&EngineRequest> = requests
        .iter()
        .filter(|r| !served.serves(r.key, r.version))
        .collect();
    if refused.is_empty() {
        let pairs: Vec<String> = requests
            .iter()
            .map(|r| format!("{} (served {})", spelled(r), served.describe(r.key)))
            .collect();
        return ready(id, CheckCode::EngineProtocolSupported, now)
            .with_message(&format!(
                "{} every request version the engine sends to {operation} it: {}.{}",
                whose(served),
                pairs.join(", "),
                differing(served)
            ))
            .with_fact("engineRequests", &sent.join(", "))
            .with_fact(BROKERS_ANSWERED, &brokers_answered(served));
    }
    let named: Vec<String> = refused
        .iter()
        .map(|r| {
            format!(
                "{} and {} {} {}",
                spelled(r),
                whose(served),
                r.api,
                served.describe(r.key)
            )
        })
        .collect();
    let code = CheckCode::EngineProtocolUnsupported;
    catalogue::outcome(id, state_for(code), code, now)
        .with_message(&format!(
            "the engine sends each request at one fixed version and never negotiates: it sends \
             {}, so the engine cannot {operation} this endpoint.{}",
            named.join("; "),
            differing(served)
        ))
        .with_remedy(remedy_for(code))
        .with_fact("engineRequests", &sent.join(", "))
        .with_fact(BROKERS_ANSWERED, &brokers_answered(served))
        .with_detail(detail(
            &refused.iter().map(|r| spelled(r)).collect::<Vec<_>>(),
        ))
}

/// `connection.groupTypes`: whether the endpoint's group listing names each
/// group's type (ListGroups v5). Below it every group reads as type Unknown,
/// and a backup records a selected consumer group as excluded
/// (`GroupTypeNotCaptured`), never as captured.
#[must_use]
pub fn group_types(
    observed: &Result<ApiVersions, CheckFailure>,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let id = CheckId::ConnectionGroupTypes;
    let served = match observed {
        Ok(v) => v,
        Err(f) => return not_observed(id, f, now),
    };
    let range = served.describe(LIST_GROUPS);
    if served.serves(LIST_GROUPS, LIST_GROUPS_WITH_TYPES) {
        return ready(id, CheckCode::GroupTypesListed, now)
            .with_message(&format!(
                "{} ListGroups {range}, so its group listing names each group's type.{}",
                whose(served),
                differing(served)
            ))
            .with_fact(BROKERS_ANSWERED, &brokers_answered(served));
    }
    let code = CheckCode::GroupTypesNotListed;
    catalogue::outcome(id, state_for(code), code, now)
        .with_message(&format!(
            "{} ListGroups {range}, and a group's type is named only from \
             v{LIST_GROUPS_WITH_TYPES}: it cannot say whether a group is a classic consumer \
             group, so a backup records every selected consumer group as excluded \
             (GroupTypeNotCaptured) and archives no position for it.{}",
            whose(served),
            differing(served)
        ))
        .with_remedy(remedy_for(code))
        .with_fact(BROKERS_ANSWERED, &brokers_answered(served))
}

/// `connection.topicConfigsReadable`: DescribeConfigs on each selected topic,
/// as this principal.
///
/// A refused read is the finding (`TopicConfigsNotReadable`): the backup still
/// runs and its receipt says `captureDenied` for that topic. A read that did
/// not answer for another reason is `unknown`, never "readable" and never
/// "refused".
#[must_use]
pub fn topic_configs_readable(
    probe: &dyn InventoryProbe,
    topics: &[String],
    deadline: Deadline,
    now: DateTime<Utc>,
) -> CheckOutcome {
    let id = CheckId::ConnectionTopicConfigsReadable;
    if topics.is_empty() {
        // NOTHING WAS READ, so this is not an answer. A Backup Preflight always
        // names a topic; a hand-built plan that lists this row and names none
        // is told so.
        return catalogue::outcome(
            id,
            CheckState::Unknown,
            CheckCode::BlockedByPrerequisite,
            now,
        )
        .with_message(
            "the plan names no topic, so no topic's configuration was read and this row says \
             nothing about any topic",
        )
        .with_remedy(
            "Name the topics the backup selects in the check's plan; a Backup Preflight always \
             does.",
        );
    }
    let mut refused: Vec<String> = Vec::new();
    let mut unanswered: Vec<String> = Vec::new();
    let mut unanswered_code = CheckCode::MetadataTimeout;
    for name in topics {
        if !deadline.has_room() {
            unanswered.push(name.clone());
            continue;
        }
        match probe.topic_configs(name) {
            // `Ok` always holds entries: a refused read is never an empty map
            // (`InventoryProbe::topic_configs`).
            Ok(_) => {}
            Err(f)
                if matches!(
                    f.code,
                    CheckCode::TopicAuthorizationFailed
                        | CheckCode::TopicNotAuthorized
                        | CheckCode::ClusterAuthorizationFailed
                ) =>
            {
                refused.push(name.clone());
            }
            Err(f) => {
                if super::is_unknown_code(f.code) {
                    unanswered_code = f.code;
                }
                unanswered.push(name.clone());
            }
        }
    }
    if !refused.is_empty() {
        let code = CheckCode::TopicConfigsNotReadable;
        return catalogue::outcome(id, state_for(code), code, now)
            .with_message(&format!(
                "{} of {} selected topic(s) refused DescribeConfigs for this principal, so a \
                 backup records their configuration as not captured (captureDenied)",
                refused.len(),
                topics.len()
            ))
            .with_remedy(remedy_for(code))
            .with_detail(detail(&refused));
    }
    if !unanswered.is_empty() {
        return catalogue::outcome(id, CheckState::Unknown, unanswered_code, now)
            .with_message(&format!(
                "{} of {} selected topic(s) did not answer DescribeConfigs within the check's \
                 budget, so whether their configuration can be read is not known",
                unanswered.len(),
                topics.len()
            ))
            .with_remedy(remedy_for(unanswered_code))
            .with_detail(detail(&unanswered));
    }
    ready(id, CheckCode::TopicConfigsReadable, now).with_message(&format!(
        "DescribeConfigs answered for all {} selected topic(s)",
        topics.len()
    ))
}

/// A capability row that could not run because the row before it did not
/// pass: the connection did not authenticate, or a selected topic is not
/// describable.
#[must_use]
pub fn blocked(id: CheckId, why: &str, now: DateTime<Utc>) -> CheckOutcome {
    catalogue::outcome(
        id,
        CheckState::Unknown,
        CheckCode::BlockedByPrerequisite,
        now,
    )
    .with_message(why)
    .with_remedy(remedy_for(CheckCode::BlockedByPrerequisite))
}

/// The connection's Schema Registry, dialled with its credential: `Ok` with a
/// client that answered `GET /subjects`, or the row's state, code and message.
fn registry_answers(
    connection: &ConnectionPlan,
) -> Result<
    (
        crate::backup::schema_registry::RegistryClient,
        &logweir_core::check_contract::RegistryPlan,
    ),
    (CheckState, CheckCode, String),
> {
    use logweir_core::schema_registry::Rest as _;
    let Some(plan) = connection.schema_registry.as_ref() else {
        return Err((
            CheckState::Unknown,
            CheckCode::RegistryUnreachable,
            "this connection names no Schema Registry".to_string(),
        ));
    };
    let side = if plan
        .password_env
        .as_deref()
        .is_some_and(|v| v.starts_with("LOGWEIR_TARGET_"))
    {
        crate::tls_ca::Side::Target
    } else {
        crate::tls_ca::Side::Source
    };
    let password = crate::credential_binding::registry_password(side).map_err(|e| {
        (
            CheckState::NotReady,
            CheckCode::CredentialBindingMismatch,
            e.to_string(),
        )
    })?;
    let spec = logweir_core::spec::SchemaRegistrySpec {
        url: plan.url.clone(),
        username: plan.username.clone(),
    };
    let mut client = crate::backup::schema_registry::RegistryClient::new(
        &spec,
        password.as_deref(),
        connection.ca_file.as_deref(),
    )
    .map_err(|e| (CheckState::Unknown, CheckCode::RegistryUnreachable, e))?;
    match client.call("GET", "/subjects", None) {
        Ok((200, _)) => Ok((client, plan)),
        Ok((status @ (401 | 403), _)) => Err((
            CheckState::NotReady,
            CheckCode::RegistryUnauthorized,
            format!(
                "the Schema Registry at {} refused this connection's registry credential \
                 (GET /subjects answered {status})",
                plan.url
            ),
        )),
        Ok((status, _)) => Err((
            CheckState::Unknown,
            CheckCode::RegistryUnreachable,
            format!(
                "the Schema Registry at {} answered GET /subjects with {status}",
                plan.url
            ),
        )),
        Err(e) => Err((
            CheckState::Unknown,
            CheckCode::RegistryUnreachable,
            format!("the Schema Registry at {} did not answer: {e}", plan.url),
        )),
    }
}

/// `registry.reachable` (PROD-03.1): the connection's Schema Registry answers
/// with its credential. Advisory: a backup whose registry does not answer
/// still runs, and its receipt says the registry was not captured.
#[must_use]
pub fn registry_reachable(connection: &ConnectionPlan, now: DateTime<Utc>) -> CheckOutcome {
    let id = CheckId::RegistryReachable;
    match registry_answers(connection) {
        // DUI-62.2: a registry with no user is asked with no credential.
        Ok((_, plan)) => ready(id, CheckCode::RegistryAnswered, now).with_message(&format!(
            "the Schema Registry at {} answered {}",
            plan.url,
            if plan.username.is_some() {
                "with this connection's registry credential"
            } else {
                "without a credential (this connection names no registry user)"
            }
        )),
        Err((state, code, message)) => catalogue::outcome(id, state, code, now)
            .with_message(&message)
            .with_remedy(remedy_for(code)),
    }
}

/// `target.schemas` (PROD-03.2): the target registry answers, the bound
/// point's snapshot verifies, and the subjects holding the IDs its receipt
/// lists for the selected topics, with their references, can be imported
/// keeping their IDs or are already there. Read only: nothing is written. The
/// run recomputes the set from every record it restores.
#[must_use]
pub fn target_schemas(
    target: &ConnectionPlan,
    topics: &[String],
    receipt: Option<&logweir_core::backup_receipt::BackupReceipt>,
    snapshot: Option<&Result<logweir_core::schema_registry::Snapshot, String>>,
    now: DateTime<Utc>,
) -> CheckOutcome {
    use logweir_core::schema_registry as sr;
    let id = CheckId::TargetSchemas;
    let refuse = |state: CheckState, code: CheckCode, message: String| {
        catalogue::outcome(id, state, code, now)
            .with_message(&message)
            .with_remedy(remedy_for(code))
    };
    let (mut client, plan) = match registry_answers(target) {
        Ok(ok) => ok,
        Err((state, code, message)) => return refuse(state, code, message),
    };
    let Some(snapshot) = snapshot else {
        return ready(id, CheckCode::SchemasImportable, now).with_message(&format!(
            "the target registry at {} answers; the recovery point carries no captured \
             registry snapshot, so the run imports nothing and its scorecard says so (noSnapshot)",
            plan.url
        ));
    };
    let snapshot = match snapshot {
        Ok(s) => s,
        Err(e) => {
            return refuse(
                CheckState::NotReady,
                CheckCode::SchemaSnapshotInvalid,
                e.clone(),
            )
        }
    };
    let needed: std::collections::BTreeSet<u32> = receipt
        .and_then(|r| r.schema_dependency.as_ref())
        .map(|block| {
            topics
                .iter()
                .filter_map(|t| block.get(t))
                .flat_map(|e| logweir_core::schema_dependency::dependent_ids(e).0)
                .collect::<std::collections::BTreeSet<u32>>()
        })
        .unwrap_or_default()
        .intersection(&snapshot.ids())
        .copied()
        .collect();
    let entries = sr::closure(snapshot, &needed);
    // DUI-58: the run reads every subject's mode before it writes and refuses
    // a read-only one; the preview asks the same question, with the same rule.
    let classified = sr::classify(&mut client, &entries).and_then(|classes| {
        let mut subjects: Vec<&str> = entries
            .iter()
            .zip(&classes)
            .filter(|(_, c)| **c == sr::Class::Import)
            .map(|(v, _)| v.subject.as_str())
            .collect();
        subjects.sort_unstable();
        subjects.dedup();
        sr::writable_modes(&mut client, &subjects).map(|_| classes)
    });
    match classified {
        Ok(classes) => {
            let import = classes.iter().filter(|c| **c == sr::Class::Import).count();
            ready(id, CheckCode::SchemasImportable, now).with_message(&format!(
                "the target registry at {} answers; the IDs the point's receipt lists for the \
                 selected topics need {} subject-version(s): {import} will be imported with \
                 their source IDs, {} are already there, none conflict. The run recomputes the \
                 set from every record it restores",
                plan.url,
                entries.len(),
                entries.len() - import
            ))
        }
        Err(e @ sr::ImportRefusal::Conflict { .. }) => refuse(
            CheckState::NotReady,
            CheckCode::SchemaConflict,
            e.to_string(),
        ),
        Err(e @ sr::ImportRefusal::ReadOnly { .. }) => refuse(
            CheckState::NotReady,
            CheckCode::SchemaRegistryReadOnly,
            e.to_string(),
        ),
        Err(e) => refuse(
            CheckState::Unknown,
            CheckCode::RegistryUnreachable,
            e.to_string(),
        ),
    }
}

/// A capability row when the Kafka connection did not authenticate: blocked,
/// except `registry.reachable`, which asks the registry and is answered
/// anyway.
#[must_use]
pub fn blocked_unless_registry(
    id: CheckId,
    connection: &ConnectionPlan,
    why: &str,
    now: DateTime<Utc>,
) -> CheckOutcome {
    if id == CheckId::RegistryReachable {
        registry_reachable(connection, now)
    } else {
        blocked(id, why, now)
    }
}
