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
//!
//! ROLES ARE A TABLE, NOT A TREE. [`Role::allows`] is one `match` per action,
//! written out so that reading it is reading the policy. There is no
//! inheritance and no "administrator implies everything": Administrator is
//! absent from [`Action::SubmitApproval`] on purpose, because D0 says an
//! administrator may submit a governed approval only when separately bound as
//! an Approver, and a `_ => true` arm for administrators is exactly the bug
//! that would erase that.
//!
//! BINDINGS ARE EXACT STRINGS, RE-READ PER REQUEST. [`RoleBindings`] maps exact
//! group claims and exact `issuer#subject` strings to `(role, namespace)`
//! pairs. No regex, no wildcard, no email-domain inference and no default
//! namespace. [`SharedAuthorizer`] holds them behind an `RwLock`, so replacing
//! the table takes effect on the next request rather than on the next restart,
//! and the session cookie carries only the raw claims — never the derived
//! roles — so a removed binding cannot be replayed from a cookie.
//!
//! ENUMERATION RESISTANCE IS A MODE, NOT A GUESS. In shared mode an ungranted
//! namespace answers exactly what a nonexistent object answers — 404
//! `not_found` — so a caller cannot map which namespaces exist. In localAdmin
//! mode the more informative 403 `namespace_forbidden` is kept: there is one
//! actor, it is the administrator, and there is nothing to enumerate.
//! [`Authorizer::hides_unbound_namespaces`] is the switch, and the audit record
//! keeps the real reason either way.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::RwLock;

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
    /// Write a credential Secret from a request body (PLAT-07.1/PLAT-08.1
    /// write-only entry). The value is created and never read back.
    WriteCredential,
    /// List/get `TopicDiscovery` projections and their stored topic pages.
    ReadTopicDiscoveries,
    /// Start topic discovery (PLAT-09.1).
    DiscoverTopics,
    /// Ask an unfinished discovery of one's own to stop.
    CancelTopicDiscovery,
    /// List/get `Preflight` projections and their detail pages.
    ReadPreflights,
    /// Start a preflight (PLAT-03).
    RunPreflight,
    /// Ask an unfinished preflight of one's own to stop.
    CancelPreflight,
    /// List/get `BackupDestination` projections (PLAT-08).
    ReadDestinations,
    /// Create a destination, rotate its access, test it, adopt a legacy
    /// location (PLAT-08).
    ManageDestinations,
    /// List/get `BackupSchedule` projections.
    ReadSchedules,
    /// Create a `BackupSchedule`.
    CreateSchedule,
    /// Change `BackupSchedule.spec.suspend` under a resourceVersion
    /// precondition.
    SetScheduleSuspension,
    /// Replace a `BackupSchedule`'s FUTURE policy under an
    /// `expectedGeneration` precondition (D1 §5.1/§5.6). `spec.sourceRef` is
    /// not part of it and cannot be reached through it.
    EditSchedulePolicy,
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
    /// The stable name this action carries in the audit record.
    ///
    /// It is the PRODUCT action, `<domain>.<verb>`, not the HTTP method and not
    /// the Kubernetes verb: an audit reader should be able to answer "who
    /// created a restore in team-a last week" without knowing either.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Action::ReadConnections => "connection.read",
            Action::CreateConnection => "connection.create",
            Action::TestConnection => "connection.test",
            Action::WriteCredential => "credential.write",
            Action::ReadTopicDiscoveries => "topicDiscovery.read",
            Action::DiscoverTopics => "topicDiscovery.start",
            Action::CancelTopicDiscovery => "topicDiscovery.cancel",
            Action::ReadPreflights => "preflight.read",
            Action::RunPreflight => "preflight.start",
            Action::CancelPreflight => "preflight.cancel",
            Action::ReadDestinations => "destination.read",
            Action::ManageDestinations => "destination.manage",
            Action::ReadSchedules => "schedule.read",
            Action::CreateSchedule => "schedule.create",
            Action::SetScheduleSuspension => "schedule.setSuspension",
            Action::EditSchedulePolicy => "schedule.editPolicy",
            Action::ReadBackups => "backup.read",
            Action::CreateManualBackup => "backup.create",
            Action::ReadRestores => "restore.read",
            Action::CreateRestore => "restore.create",
            Action::ReadApprovals => "approval.read",
            Action::ReadApprovalPacket => "approval.readPacket",
            Action::SubmitApproval => "approval.submit",
            Action::ReadOperations => "operation.read",
            Action::StreamOperationEvents => "operation.stream",
        }
    }

    /// Whether this build serves a route for the action. Everything `false`
    /// here has NO route: no stub, no 501.
    #[must_use]
    pub const fn implemented(self) -> bool {
        !matches!(
            self,
            Action::TestConnection | Action::SubmitApproval | Action::StreamOperationEvents
        )
    }
}

/// Decides grants and actions for an actor.
pub trait Authorizer: Send + Sync + 'static {
    /// The namespaces explicitly granted to `actor`, in a stable order.
    fn namespaces(&self, actor: &Actor) -> Vec<String>;

    /// Whether `actor` may perform `action` in the granted `namespace`.
    fn allows(&self, actor: &Actor, namespace: &str, action: Action) -> bool;

    /// The roles `actor` holds in `namespace`, sorted, for the audit record.
    fn roles(&self, _actor: &Actor, _namespace: &str) -> Vec<Role> {
        Vec::new()
    }

    /// The administrator-declared revision of the binding table that decided
    /// this request. It goes in the audit record so a decision can be tied to
    /// the configuration that made it.
    fn binding_revision(&self) -> String {
        String::new()
    }

    /// Whether an ungranted namespace must be indistinguishable from a
    /// nonexistent one. See the module documentation.
    fn hides_unbound_namespaces(&self) -> bool {
        false
    }

    /// The exact group strings this authorizer's bindings could ever match, or
    /// `None` when the mode has no bindings.
    ///
    /// A SESSION CARRIES ONLY THESE. A provider that emits a thousand group
    /// claims emits a thousand strings that cannot grant anything, and putting
    /// them in a cookie costs the browser's 4 KB ceiling and tells anyone who
    /// steals the cookie the whole directory membership of its owner. Keeping
    /// only the bindable ones is smaller AND less to leak.
    ///
    /// THE TRADE, STATED: a binding ADDED for a group an actor already holds
    /// takes effect on that actor's next sign-in rather than its next request.
    /// Removing a binding still takes effect immediately, because the roles are
    /// still derived per request from the current table — and revocation is the
    /// direction that matters.
    fn bindable_groups(&self) -> Option<BTreeSet<String>> {
        None
    }
}

// ======================================================================
// Roles
// ======================================================================

/// The four product roles. They are LOGWEIR roles: they are not Kubernetes
/// roles, they are not granted by cluster RBAC, and holding one says nothing
/// about what the console ServiceAccount may do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Role {
    /// Reads, never mutates.
    Viewer,
    /// Creates and runs operations. Never submits a governed approval and
    /// never changes trust or policy.
    Operator,
    /// Submits governed approvals in approver-bound namespaces. Cannot create
    /// an execution.
    Approver,
    /// Administers bound namespaces. Is STILL the requester when it operates,
    /// and is not an approver unless separately bound as one.
    Administrator,
}

impl Role {
    /// Every role, in declaration order.
    pub const ALL: [Role; 4] = [
        Role::Viewer,
        Role::Operator,
        Role::Approver,
        Role::Administrator,
    ];

    /// The exact string an administrator writes in the configuration.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Approver => "approver",
            Role::Administrator => "administrator",
        }
    }

    /// Parse the configured spelling.
    #[must_use]
    pub fn parse(value: &str) -> Option<Role> {
        Role::ALL.into_iter().find(|r| r.as_str() == value)
    }

    /// THE DECISION TABLE. One arm per action per role, written out.
    ///
    /// Read it as the matrix in D0 §"Application role and namespace matrix":
    /// Viewer never mutates; Operator never submits a governed approval and
    /// never changes trust or policy; Approver cannot create an execution and
    /// sees only what a governed approval needs; Administrator administers its
    /// bound namespaces but is NOT an approver.
    #[must_use]
    pub const fn allows(self, action: Action) -> bool {
        match self {
            Role::Viewer => matches!(
                action,
                Action::ReadConnections
                    | Action::ReadDestinations
                    | Action::ReadTopicDiscoveries
                    | Action::ReadPreflights
                    | Action::ReadSchedules
                    | Action::ReadBackups
                    | Action::ReadRestores
                    | Action::ReadApprovals
                    | Action::ReadOperations
                    | Action::StreamOperationEvents
            ),
            Role::Operator => matches!(
                action,
                Action::ReadConnections
                    | Action::CreateConnection
                    | Action::TestConnection
                    | Action::WriteCredential
                    | Action::ReadDestinations
                    | Action::ManageDestinations
                    | Action::ReadTopicDiscoveries
                    | Action::DiscoverTopics
                    | Action::CancelTopicDiscovery
                    | Action::ReadPreflights
                    | Action::RunPreflight
                    | Action::CancelPreflight
                    | Action::ReadSchedules
                    | Action::CreateSchedule
                    | Action::SetScheduleSuspension
                    | Action::EditSchedulePolicy
                    | Action::ReadBackups
                    | Action::CreateManualBackup
                    | Action::ReadRestores
                    | Action::CreateRestore
                    | Action::ReadApprovals
                    | Action::ReadApprovalPacket
                    | Action::ReadOperations
                    | Action::StreamOperationEvents
            ),
            // The approver sees the governed approval subject and its plan,
            // and nothing that would let it prepare one: no connections, no
            // schedules, no discovery, no creates.
            Role::Approver => matches!(
                action,
                // READ ONLY WHEN NEEDED FOR THE PACKET. The role table opens
                // `preflight.read`; `routes::preflights` then refuses any
                // preflight that is not a Restore readiness result to an actor
                // whose only binding here is Approver, so "what the approval
                // packet needs" is enforced on the OBJECT and not merely
                // promised in a comment.
                Action::ReadPreflights
                    | Action::ReadBackups
                    | Action::ReadRestores
                    | Action::ReadApprovals
                    | Action::ReadApprovalPacket
                    | Action::ReadOperations
                    | Action::StreamOperationEvents
                    | Action::SubmitApproval
            ),
            // Everything an operator may do, plus the reads an approver has —
            // and DELIBERATELY NOT `SubmitApproval`. An administrator who must
            // approve is bound as an Approver as well, and the
            // separation-of-duties check then still compares principals.
            Role::Administrator => matches!(
                action,
                Action::ReadConnections
                    | Action::CreateConnection
                    | Action::TestConnection
                    | Action::WriteCredential
                    | Action::ReadDestinations
                    | Action::ManageDestinations
                    | Action::ReadTopicDiscoveries
                    | Action::DiscoverTopics
                    | Action::CancelTopicDiscovery
                    | Action::ReadPreflights
                    | Action::RunPreflight
                    | Action::CancelPreflight
                    | Action::ReadSchedules
                    | Action::CreateSchedule
                    | Action::SetScheduleSuspension
                    | Action::EditSchedulePolicy
                    | Action::ReadBackups
                    | Action::CreateManualBackup
                    | Action::ReadRestores
                    | Action::CreateRestore
                    | Action::ReadApprovals
                    | Action::ReadApprovalPacket
                    | Action::ReadOperations
                    | Action::StreamOperationEvents
            ),
        }
    }
}

/// One administrator-written binding: a role in one exact namespace for exact
/// group strings and exact subjects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleBinding {
    /// The role granted.
    pub role: Role,
    /// The exact namespace. Never a pattern, never a default.
    pub namespace: String,
    /// Exact group claim strings.
    pub groups: Vec<String>,
    /// Exact `issuer#subject` actor IDs.
    pub subjects: Vec<String>,
}

impl RoleBinding {
    /// Whether this binding matches an actor. Exact string comparison only.
    #[must_use]
    pub fn matches(&self, actor: &Actor) -> bool {
        let actor_id = actor.id();
        self.subjects.iter().any(|s| s == &actor_id)
            || self
                .groups
                .iter()
                .any(|g| actor.groups.iter().any(|claim| claim == g))
    }
}

/// The whole binding table, with the revision an administrator declared.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoleBindings {
    /// The administrator's revision string, recorded in every audit line.
    pub revision: String,
    /// The bindings, in configuration order.
    pub bindings: Vec<RoleBinding>,
}

impl RoleBindings {
    /// Every `(namespace, roles)` pair for an actor, unioned across bindings.
    #[must_use]
    pub fn roles_by_namespace(&self, actor: &Actor) -> BTreeMap<String, BTreeSet<Role>> {
        let mut out: BTreeMap<String, BTreeSet<Role>> = BTreeMap::new();
        for binding in &self.bindings {
            if binding.matches(actor) {
                out.entry(binding.namespace.clone())
                    .or_default()
                    .insert(binding.role);
            }
        }
        out
    }
}

/// The shared-mode authorizer: exact bindings, re-read per request.
pub struct SharedAuthorizer {
    bindings: RwLock<std::sync::Arc<RoleBindings>>,
}

impl SharedAuthorizer {
    /// An authorizer over a binding table.
    #[must_use]
    pub fn new(bindings: RoleBindings) -> Self {
        Self {
            bindings: RwLock::new(std::sync::Arc::new(bindings)),
        }
    }

    /// Replace the binding table. The next request uses the new one; no
    /// session is invalidated, because sessions carry claims and never roles.
    pub fn replace(&self, bindings: RoleBindings) {
        *self
            .bindings
            .write()
            .expect("the binding lock is never poisoned") = std::sync::Arc::new(bindings);
    }

    /// The current table.
    #[must_use]
    pub fn bindings(&self) -> std::sync::Arc<RoleBindings> {
        std::sync::Arc::clone(
            &self
                .bindings
                .read()
                .expect("the binding lock is never poisoned"),
        )
    }
}

impl Authorizer for SharedAuthorizer {
    fn namespaces(&self, actor: &Actor) -> Vec<String> {
        self.bindings()
            .roles_by_namespace(actor)
            .into_keys()
            .collect()
    }

    fn allows(&self, actor: &Actor, namespace: &str, action: Action) -> bool {
        if !action.implemented() {
            return false;
        }
        self.bindings()
            .roles_by_namespace(actor)
            .get(namespace)
            .is_some_and(|roles| roles.iter().any(|role| role.allows(action)))
    }

    fn roles(&self, actor: &Actor, namespace: &str) -> Vec<Role> {
        self.bindings()
            .roles_by_namespace(actor)
            .remove(namespace)
            .map(|roles| roles.into_iter().collect())
            .unwrap_or_default()
    }

    fn binding_revision(&self) -> String {
        self.bindings().revision.clone()
    }

    fn hides_unbound_namespaces(&self) -> bool {
        true
    }

    fn bindable_groups(&self) -> Option<BTreeSet<String>> {
        Some(
            self.bindings()
                .bindings
                .iter()
                .flat_map(|binding| binding.groups.iter().cloned())
                .collect(),
        )
    }
}

// ======================================================================
// Separation of duties
// ======================================================================

/// An identity, as authorization compares them: issuer and subject, never a
/// display name, an email or a key id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    /// The issuer.
    pub issuer: String,
    /// The subject.
    pub subject: String,
}

impl Principal {
    /// The principal of an actor.
    #[must_use]
    pub fn of(actor: &Actor) -> Self {
        Self {
            issuer: actor.issuer.clone(),
            subject: actor.subject.clone(),
        }
    }

    /// The stable id, `<issuer>#<subject>`.
    #[must_use]
    pub fn id(&self) -> String {
        format!("{}#{}", self.issuer, self.subject)
    }
}

/// Whether an approver is independent of a requester.
///
/// IT COMPARES `(issuer, subject)`. Comparing display names, email claims or
/// key ids is not separation of duties: one person holds several keys and may
/// change their display name, and D0 says so in as many words.
#[must_use]
pub fn independent_principals(requester: &Principal, approver: &Principal) -> bool {
    requester != approver
}

/// The governed-approval decision, ahead of the PLAT-19.2 route.
///
/// This is the decision the route WILL call. It exists now, tested, because
/// the tracker's acceptance is "an operator cannot assume approver rights" and
/// "admin is not a self-approval bypass" — properties of the decision, not of
/// the transport. The route is absent and `approvalSubmit` is advertised
/// `false`; when PLAT-19.2 adds it, it calls this.
///
/// # Errors
///
/// `forbidden` when the actor holds no Approver binding in the namespace, or
/// when the approver is the requester.
pub fn authorize_governed_approval(
    authorizer: &dyn Authorizer,
    approver: &Actor,
    namespace: &str,
    requester: &Principal,
) -> Result<(), ApiError> {
    if !authorizer
        .roles(approver, namespace)
        .contains(&Role::Approver)
    {
        return Err(ApiError::new(
            ProblemCode::Forbidden,
            "Submitting a governed approval requires an Approver binding in this namespace.",
        ));
    }
    if !independent_principals(requester, &Principal::of(approver)) {
        return Err(ApiError::new(
            ProblemCode::Forbidden,
            "A governed approval requires an approver who is not the requester.",
        ));
    }
    Ok(())
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
    decide(authorizer, actor, namespace, action)
        .map_err(|denial| denial.response(authorizer.hides_unbound_namespaces()))
}

/// Why a request was refused — the REAL reason, which is not always the reason
/// the response is allowed to state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Denial {
    /// The namespace is not bound to this actor.
    NamespaceNotGranted,
    /// The namespace is bound, but not for this action.
    ActionNotPermitted,
}

impl Denial {
    /// The stable code the AUDIT record carries. It always names the real
    /// reason, even when the response withholds it for enumeration resistance:
    /// hiding a namespace from a caller must not also hide an authorization
    /// problem from the operator reading the log.
    #[must_use]
    pub const fn audit_code(self) -> &'static str {
        match self {
            Denial::NamespaceNotGranted => ProblemCode::NamespaceForbidden.as_str(),
            Denial::ActionNotPermitted => ProblemCode::Forbidden.as_str(),
        }
    }

    /// The response. With `hide_unbound`, an ungranted namespace answers
    /// exactly what a nonexistent object answers.
    #[must_use]
    pub fn response(self, hide_unbound: bool) -> ApiError {
        match self {
            Denial::NamespaceNotGranted if hide_unbound => ApiError::not_found(),
            Denial::NamespaceNotGranted => ApiError::new(
                ProblemCode::NamespaceForbidden,
                "The namespace is not granted to this actor.",
            ),
            Denial::ActionNotPermitted => ApiError::new(
                ProblemCode::Forbidden,
                "The actor is not permitted this action in this namespace.",
            ),
        }
    }
}

/// The decision itself: namespace grant first, then the action.
///
/// # Errors
///
/// The [`Denial`] that applies.
pub fn decide(
    authorizer: &dyn Authorizer,
    actor: &Actor,
    namespace: &str,
    action: Action,
) -> Result<(), Denial> {
    if !authorizer.namespaces(actor).iter().any(|n| n == namespace) {
        return Err(Denial::NamespaceNotGranted);
    }
    if !action.implemented() || !authorizer.allows(actor, namespace, action) {
        return Err(Denial::ActionNotPermitted);
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
    /// Write-only credential input: `POST .../destinations` and
    /// `:update-access` accept a credential value that is created once and
    /// never read back.
    pub credential_write: bool,
    /// The topic-discovery domain (PLAT-09.1) is served AND readable here.
    ///
    /// ONE FLAG PER DOMAIN, AND IT IS THE READ FLOOR. D2 §8 names exactly
    /// three flags — `destinations`, `topicDiscovery`, `preflight` — so a
    /// domain gets one bit, not a bit per verb, and the bit answers "is this
    /// domain served and visible to me". Whether the actor may also START or
    /// CANCEL is the role table above, which `/session` publishes per grant in
    /// `roles`; a console reads `roles.includes("operator")` for the buttons
    /// and this flag for the page. Splitting these into read/start pairs is a
    /// contract change that must land together with `ui/contract.js`'s
    /// `CAPABILITY_FLAGS`.
    pub topic_discovery: bool,
    /// The preflight domain (PLAT-03) is served AND readable here.
    pub preflight: bool,
    /// The saved-destination domain (PLAT-08) is served AND readable here.
    pub destinations: bool,
    /// `GET .../schedules[/{name}]`.
    pub schedules_read: bool,
    /// `POST .../schedules` — AND `PUT .../schedules/{name}`.
    ///
    /// ONE FLAG FOR TWO ROUTES, BECAUSE IT IS ONE AUTHORITY. D1 §5.1 makes a
    /// schedule's future policy editable, and this crate's decision (recorded
    /// in `docs/api.md`) is that an operator who may WRITE a policy into a
    /// bound namespace may write a new one over it: the edit reaches exactly
    /// the fields `POST` already sets, never `spec.sourceRef`, and never a
    /// created run. `Action::EditSchedulePolicy` is a separate action so the
    /// AUDIT record says `schedule.editPolicy` rather than `schedule.create`,
    /// but it has the same role allowance — pinned by
    /// `tests/role_matrix.rs::editing_a_policy_is_exactly_the_authority_to_create_one`
    /// — so a console reads this one flag for both buttons. D2 §8's rule
    /// stands: `ui/contract.js`'s `CAPABILITY_FLAGS` is a frozen 19-entry list,
    /// and splitting a flag is a contract change that lands on both sides at
    /// once.
    pub schedule_create: bool,
    /// `POST .../schedules/{name}:set-suspension`.
    pub schedule_set_suspension: bool,
    /// `GET .../backups[/{name}]`.
    pub backups_read: bool,
    /// `POST .../backups` — the canonical manual `Backup` (D1 §8.2).
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
            topic_discovery: can(Action::ReadTopicDiscoveries),
            preflight: can(Action::ReadPreflights),
            destinations: can(Action::ReadDestinations),
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
        Actor::new("i", "s", "d")
    }

    #[test]
    fn the_namespace_is_checked_before_the_action() {
        let z = LocalAdminAuthorizer::new(vec!["team-a".into()]);
        let err = authorize(&z, &actor(), "team-b", Action::SubmitApproval).unwrap_err();
        assert_eq!(err.code, ProblemCode::NamespaceForbidden);
        let err = authorize(&z, &actor(), "team-a", Action::SubmitApproval).unwrap_err();
        assert_eq!(err.code, ProblemCode::Forbidden);
        assert!(authorize(&z, &actor(), "team-a", Action::CreateSchedule).is_ok());
        assert!(authorize(&z, &actor(), "team-a", Action::EditSchedulePolicy).is_ok());
    }

    #[test]
    fn unimplemented_domains_are_never_advertised() {
        let z = LocalAdminAuthorizer::new(vec!["team-a".into()]);
        let c = Capabilities::for_namespace(&z, &actor(), "team-a");
        assert!(c.schedule_create && c.restore_create && c.connection_create);
        assert!(c.topic_discovery && c.preflight && c.destinations && c.credential_write);
        // D1 W6: `POST .../backups` has a route, so the flag is no longer a
        // permanent `false`.
        assert!(c.manual_backup_create);
        assert!(!c.approval_submit && !c.operation_events && !c.connection_test);
        assert_eq!(
            Capabilities::for_namespace(&z, &actor(), "team-b"),
            Capabilities::none()
        );
    }
}
