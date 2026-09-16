//! Authorization: may this actor do this, in this namespace.
//!
//! NAMESPACE FIRST, BEFORE ANY LOOKUP. [`authorize`] checks the namespace grant
//! before the action and before any Kubernetes call, so an ungranted namespace
//! answers `namespace_forbidden` whether or not it exists — the answer never
//! depends on cluster state the actor may not see.
//!
//! NAMESPACES ARE EXPLICIT. [`Authorizer::namespaces`] returns configured
//! grants; nothing here or anywhere in this crate lists core `Namespace`
//! objects.
//!
//! CAPABILITIES ARE DERIVED, NOT DECLARED TWICE. `GET /api/v1/session` reports
//! a capability as `true` only when [`Action::implemented`] is true AND the
//! authorizer allows it, so a domain whose route is absent can never be
//! advertised, and a role that denies an action hides it.

use schemars::JsonSchema;
use serde::Serialize;

use crate::auth::Actor;
use crate::problem::{ApiError, ProblemCode};

/// Every product action a route performs, plus the not-yet-implemented
/// domains the session advertises as unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// List/get `KafkaCluster` projections.
    ReadConnections,
    /// Create a `KafkaCluster` that references an existing Secret by name.
    CreateConnection,
    /// Test a saved connection (PLAT-07.2). Not implemented.
    TestConnection,
    /// Write a credential Secret (PLAT-07.1). Not implemented.
    WriteCredential,
    /// Start topic discovery (PLAT-09.1). Not implemented.
    DiscoverTopics,
    /// Start a preflight (PLAT-03). Not implemented.
    RunPreflight,
    /// Manage saved destinations (PLAT-08). Not implemented.
    ManageDestinations,
    /// List/get `BackupSchedule` projections.
    ReadSchedules,
    /// Create a `BackupSchedule`.
    CreateSchedule,
    /// Change `BackupSchedule.spec.suspend` under a resourceVersion
    /// precondition.
    SetScheduleSuspension,
    /// List/get `Backup` projections.
    ReadBackups,
    /// Create a manual `Backup` (PLAT-06.1/06.2). Not implemented.
    CreateManualBackup,
    /// List/get `Restore` projections.
    ReadRestores,
    /// Create a `Restore` with opaque plan bytes.
    CreateRestore,
    /// List/get `Approval` metadata.
    ReadApprovals,
    /// Read raw approval and sidecar bytes through the explicit packet route.
    ReadApprovalPacket,
    /// Submit an approval (PLAT-19.2). Not implemented.
    SubmitApproval,
    /// Read the normalized operation status.
    ReadOperations,
    /// Stream operation events (server-sent events). Not implemented.
    StreamOperationEvents,
}

impl Action {
    /// Whether this build serves a route for the action. Everything `false`
    /// here has NO route: no stub, no 501.
    #[must_use]
    pub const fn implemented(self) -> bool {
        !matches!(
            self,
            Action::TestConnection
                | Action::WriteCredential
                | Action::DiscoverTopics
                | Action::RunPreflight
                | Action::ManageDestinations
                | Action::CreateManualBackup
                | Action::SubmitApproval
                | Action::StreamOperationEvents
        )
    }
}

/// Decides grants and actions for an actor.
pub trait Authorizer: Send + Sync + 'static {
    /// The namespaces explicitly granted to `actor`, in a stable order.
    fn namespaces(&self, actor: &Actor) -> Vec<String>;

    /// Whether `actor` may perform `action` in the granted `namespace`.
    fn allows(&self, actor: &Actor, namespace: &str, action: Action) -> bool;
}

/// The local administrator may perform every implemented action in every
/// configured namespace, and nothing anywhere else.
#[derive(Clone, Debug)]
pub struct LocalAdminAuthorizer {
    namespaces: Vec<String>,
}

impl LocalAdminAuthorizer {
    /// An authorizer over the configured namespace list.
    #[must_use]
    pub fn new(namespaces: Vec<String>) -> Self {
        Self { namespaces }
    }
}

impl Authorizer for LocalAdminAuthorizer {
    fn namespaces(&self, _actor: &Actor) -> Vec<String> {
        self.namespaces.clone()
    }

    fn allows(&self, _actor: &Actor, namespace: &str, action: Action) -> bool {
        action.implemented() && self.namespaces.iter().any(|n| n == namespace)
    }
}

/// Namespace grant first, then the action.
///
/// # Errors
///
/// `namespace_forbidden` when the namespace is not granted (checked first and
/// independently of whether it exists), `forbidden` when the action is not.
pub fn authorize(
    authorizer: &dyn Authorizer,
    actor: &Actor,
    namespace: &str,
    action: Action,
) -> Result<(), ApiError> {
    if !authorizer.namespaces(actor).iter().any(|n| n == namespace) {
        return Err(ApiError::new(
            ProblemCode::NamespaceForbidden,
            "The namespace is not granted to this actor.",
        ));
    }
    if !action.implemented() || !authorizer.allows(actor, namespace, action) {
        return Err(ApiError::new(
            ProblemCode::Forbidden,
            "The actor is not permitted this action in this namespace.",
        ));
    }
    Ok(())
}

/// The capability flags of one namespace grant.
///
/// Every flag is `implemented && allowed`. The eight domains that have no
/// route in this build are always `false`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    /// `GET .../connections[/{name}]`.
    pub connections_read: bool,
    /// `POST .../connections`.
    pub connection_create: bool,
    /// Connection tests (PLAT-07.2): no route.
    pub connection_test: bool,
    /// Write-only credential input (PLAT-07.1): no route.
    pub credential_write: bool,
    /// Topic discovery (PLAT-09.1): no route.
    pub topic_discovery: bool,
    /// Preflight checks (PLAT-03): no route.
    pub preflight: bool,
    /// Saved destinations (PLAT-08): no route.
    pub destinations: bool,
    /// `GET .../schedules[/{name}]`.
    pub schedules_read: bool,
    /// `POST .../schedules`.
    pub schedule_create: bool,
    /// `POST .../schedules/{name}:set-suspension`.
    pub schedule_set_suspension: bool,
    /// `GET .../backups[/{name}]`.
    pub backups_read: bool,
    /// Manual backup creation (PLAT-06.1/06.2): no route.
    pub manual_backup_create: bool,
    /// `GET .../restores[/{name}]`.
    pub restores_read: bool,
    /// `POST .../restores`.
    pub restore_create: bool,
    /// `GET .../approvals[/{name}]`.
    pub approvals_read: bool,
    /// `GET .../approvals/{name}/packet`.
    pub approval_packet_read: bool,
    /// Approval submission (PLAT-19.2): no route.
    pub approval_submit: bool,
    /// `GET .../operations/{kind}/{name}`.
    pub operations_read: bool,
    /// Operation event streams: no route.
    pub operation_events: bool,
}

impl Capabilities {
    /// The flags for `actor` in `namespace`.
    #[must_use]
    pub fn for_namespace(authorizer: &dyn Authorizer, actor: &Actor, namespace: &str) -> Self {
        let can =
            |action: Action| action.implemented() && authorizer.allows(actor, namespace, action);
        Self {
            connections_read: can(Action::ReadConnections),
            connection_create: can(Action::CreateConnection),
            connection_test: can(Action::TestConnection),
            credential_write: can(Action::WriteCredential),
            topic_discovery: can(Action::DiscoverTopics),
            preflight: can(Action::RunPreflight),
            destinations: can(Action::ManageDestinations),
            schedules_read: can(Action::ReadSchedules),
            schedule_create: can(Action::CreateSchedule),
            schedule_set_suspension: can(Action::SetScheduleSuspension),
            backups_read: can(Action::ReadBackups),
            manual_backup_create: can(Action::CreateManualBackup),
            restores_read: can(Action::ReadRestores),
            restore_create: can(Action::CreateRestore),
            approvals_read: can(Action::ReadApprovals),
            approval_packet_read: can(Action::ReadApprovalPacket),
            approval_submit: can(Action::SubmitApproval),
            operations_read: can(Action::ReadOperations),
            operation_events: can(Action::StreamOperationEvents),
        }
    }

    /// The union of several grants' flags.
    #[must_use]
    pub fn union<'a>(all: impl IntoIterator<Item = &'a Capabilities>) -> Self {
        let mut out = Capabilities::none();
        for c in all {
            out.connections_read |= c.connections_read;
            out.connection_create |= c.connection_create;
            out.connection_test |= c.connection_test;
            out.credential_write |= c.credential_write;
            out.topic_discovery |= c.topic_discovery;
            out.preflight |= c.preflight;
            out.destinations |= c.destinations;
            out.schedules_read |= c.schedules_read;
            out.schedule_create |= c.schedule_create;
            out.schedule_set_suspension |= c.schedule_set_suspension;
            out.backups_read |= c.backups_read;
            out.manual_backup_create |= c.manual_backup_create;
            out.restores_read |= c.restores_read;
            out.restore_create |= c.restore_create;
            out.approvals_read |= c.approvals_read;
            out.approval_packet_read |= c.approval_packet_read;
            out.approval_submit |= c.approval_submit;
            out.operations_read |= c.operations_read;
            out.operation_events |= c.operation_events;
        }
        out
    }

    /// Every flag `false`.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            connections_read: false,
            connection_create: false,
            connection_test: false,
            credential_write: false,
            topic_discovery: false,
            preflight: false,
            destinations: false,
            schedules_read: false,
            schedule_create: false,
            schedule_set_suspension: false,
            backups_read: false,
            manual_backup_create: false,
            restores_read: false,
            restore_create: false,
            approvals_read: false,
            approval_packet_read: false,
            approval_submit: false,
            operations_read: false,
            operation_events: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor() -> Actor {
        Actor {
            issuer: "i".into(),
            subject: "s".into(),
            display_name: "d".into(),
        }
    }

    #[test]
    fn the_namespace_is_checked_before_the_action() {
        let z = LocalAdminAuthorizer::new(vec!["team-a".into()]);
        let err = authorize(&z, &actor(), "team-b", Action::CreateManualBackup).unwrap_err();
        assert_eq!(err.code, ProblemCode::NamespaceForbidden);
        let err = authorize(&z, &actor(), "team-a", Action::CreateManualBackup).unwrap_err();
        assert_eq!(err.code, ProblemCode::Forbidden);
        assert!(authorize(&z, &actor(), "team-a", Action::CreateSchedule).is_ok());
    }

    #[test]
    fn unimplemented_domains_are_never_advertised() {
        let z = LocalAdminAuthorizer::new(vec!["team-a".into()]);
        let c = Capabilities::for_namespace(&z, &actor(), "team-a");
        assert!(c.schedule_create && c.restore_create && c.connection_create);
        assert!(!c.manual_backup_create);
        assert!(!c.topic_discovery && !c.preflight && !c.destinations && !c.credential_write);
        assert!(!c.approval_submit && !c.operation_events && !c.connection_test);
        assert_eq!(
            Capabilities::for_namespace(&z, &actor(), "team-b"),
            Capabilities::none()
        );
    }
}
