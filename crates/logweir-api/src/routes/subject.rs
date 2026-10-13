//! **The object an approval authorises**, as the approval routes read it: a
//! `Restore` or (PROD-04.2) a `Switchover`.
//!
//! ONE SET OF GATES SERVES BOTH KINDS. The create's refusals
//! (`restores::refuse_before_create`), the console's signing of a request
//! (`restores::authorize_submission`), an approver's submission
//! (`restores::submitted_files`, `restores::submit_for`) and the second
//! person's click in the console (`console_approval::request_for`,
//! `console_approval::approve_for`) take a [`Subject`], so a `Switchover` is
//! held to exactly the rules a `Restore` is, by the same code. What differs
//! by kind lives here and nowhere else: the CRD kind and its wire spelling,
//! the words a sentence names it by, and its approval scope
//! ([`Subject::scope`]).

use kube::ResourceExt as _;
use logweir_core::approval_policy::{ExpectedSubject, RestoreAuthorization};
use logweir_core::approval_scope::{
    self, ApprovalScope, ScopeIncomplete, ScopeOf, SwitchoverScope,
};
use logweir_core::original_name::ApprovalSubject;
use weirkeeper::crds::approval::SubjectKind;
use weirkeeper::crds::restore::Restore;
use weirkeeper::crds::switchover::Switchover;

use super::get_object;
use crate::app::AppState;
use crate::auth::Actor;
use crate::problem::ApiError;

/// Which kind a route is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// `.../restores/{name}/...`.
    Restore,
    /// `.../switchovers/{name}/...`.
    Switchover,
}

/// The facts of one authorised object every approval gate reads.
#[derive(Clone, Debug)]
pub struct Subject {
    /// Its kind.
    pub kind: Kind,
    /// The route's namespace.
    pub namespace: String,
    /// Its name.
    pub name: String,
    /// Its UID, now.
    pub uid: String,
    /// Its plan, exactly as stored.
    pub plan_bytes: String,
    /// `spec.approvalRef.name`.
    pub approval_name: String,
    /// The approval subject it declares (a `Switchover` always `Ordinary`).
    pub approval_subject: ApprovalSubject,
}

/// A request's scope, complete, by kind.
#[derive(Clone, Debug)]
pub enum ShownScope {
    /// A restore's, with its plan's topic count, counted a second way.
    Restore {
        /// The scope.
        scope: Box<ApprovalScope>,
        /// `source.topics.len()` of an independent parse.
        topics_in_plan: usize,
    },
    /// A switchover's, with its plan's position count, counted a second way.
    Switchover {
        /// The scope.
        scope: Box<SwitchoverScope>,
        /// `positions()` of the plan's own grammar parse.
        positions_in_plan: usize,
    },
}

impl ShownScope {
    /// Notes the audit record states about the scope an approval was given
    /// on: what was shown and what the plan holds, counted two ways.
    pub fn audit(&self, actor: &Actor) {
        match self {
            Self::Restore {
                scope,
                topics_in_plan,
            } => {
                actor
                    .audit
                    .note("scopeTopicsShown", &scope.topics.len().to_string());
                actor
                    .audit
                    .note("scopeTopicsInPlan", &topics_in_plan.to_string());
            }
            Self::Switchover {
                scope,
                positions_in_plan,
            } => {
                actor
                    .audit
                    .note("scopeGroupsShown", &scope.plan.groups.len().to_string());
                actor
                    .audit
                    .note("scopePositionsShown", &scope.plan.positions.to_string());
                actor
                    .audit
                    .note("scopePositionsInPlan", &positions_in_plan.to_string());
            }
        }
    }
}

impl Subject {
    /// A `Restore`, read by a route of namespace `ns`.
    #[must_use]
    pub fn of_restore(ns: &str, restore: &Restore) -> Self {
        Self {
            kind: Kind::Restore,
            namespace: ns.to_string(),
            name: restore.name_any(),
            uid: restore.uid().unwrap_or_default(),
            plan_bytes: restore.spec.plan_bytes.clone(),
            approval_name: restore.spec.approval_ref_name().to_string(),
            approval_subject: super::restores::restore_approval_subject(restore),
        }
    }

    /// A `Switchover`, read by a route of namespace `ns`.
    #[must_use]
    pub fn of_switchover(ns: &str, switchover: &Switchover) -> Self {
        Self {
            kind: Kind::Switchover,
            namespace: ns.to_string(),
            name: switchover.name_any(),
            uid: switchover.uid().unwrap_or_default(),
            plan_bytes: switchover.spec.plan_bytes.clone(),
            approval_name: switchover.spec.approval_ref.name.clone(),
            approval_subject: ApprovalSubject::Ordinary,
        }
    }

    /// Read the object `kind`/`name` names (the name check and the audit
    /// attribution are [`get_object`]'s).
    ///
    /// # Errors
    ///
    /// `not_found` or the adapter's failure.
    pub async fn fetch(
        state: &AppState,
        actor: &Actor,
        ns: &str,
        kind: Kind,
        name: &str,
    ) -> Result<Self, ApiError> {
        Ok(match kind {
            Kind::Restore => {
                Self::of_restore(ns, &get_object::<Restore>(state, actor, ns, name).await?)
            }
            Kind::Switchover => {
                Self::of_switchover(ns, &get_object::<Switchover>(state, actor, ns, name).await?)
            }
        })
    }

    /// The CRD's `subjectRef.kind`.
    #[must_use]
    pub const fn crd_kind(&self) -> SubjectKind {
        match self.kind {
            Kind::Restore => SubjectKind::Restore,
            Kind::Switchover => SubjectKind::Switchover,
        }
    }

    /// The kind as the authorization document's `subject.kind` spells it.
    #[must_use]
    pub const fn wire(&self) -> &'static str {
        self.crd_kind().as_str()
    }

    /// The kind as a sentence names an object of it (`Restore`).
    #[must_use]
    pub const fn title(&self) -> &'static str {
        self.wire()
    }

    /// The kind as a sentence names the operation (`restore`).
    #[must_use]
    pub const fn noun(&self) -> &'static str {
        match self.kind {
            Kind::Restore => "restore",
            Kind::Switchover => "switchover",
        }
    }

    /// `sha256:<hex>` of the plan bytes, recomputed.
    #[must_use]
    pub fn plan_hash(&self) -> String {
        logweir_core::ids::sha256_prefixed(self.plan_bytes.as_bytes())
    }

    /// What an authorization must bind to authorise this object.
    #[must_use]
    pub fn expected(&self) -> ExpectedSubject {
        ExpectedSubject {
            kind: self.wire().to_string(),
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            uid: self.uid.clone(),
            plan_hash: self.plan_hash(),
        }
    }

    /// The scope-sentence wording of this kind.
    #[must_use]
    pub const fn scope_of(&self) -> ScopeOf {
        match self.kind {
            Kind::Restore => ScopeOf::Restore,
            Kind::Switchover => ScopeOf::Switchover,
        }
    }

    /// **WHAT A SECOND PERSON IS SHOWN IS WHAT THEY APPROVE, ALL OF IT, OR
    /// NOTHING IS APPROVABLE**: the scope of a VERIFIED request over this
    /// object's plan bytes, which the scope function holds to the request's
    /// `planHash` before it reads one byte of them. Beside it, the plan's
    /// size counted a second and independent way; a scope whose list is not
    /// the plan's own length is incomplete.
    ///
    /// # Errors
    ///
    /// Why the request cannot be shown in full.
    pub fn scope(&self, request: &RestoreAuthorization) -> Result<ShownScope, ScopeIncomplete> {
        match self.kind {
            Kind::Restore => {
                let scope = approval_scope::approval_scope(request, self.plan_bytes.as_bytes())?;
                let in_plan =
                    serde_yaml::from_str::<logweir_core::spec::DrillSpec>(&self.plan_bytes)
                        .map_err(|_| ScopeIncomplete::PlanUnreadable)?
                        .source
                        .topics
                        .len();
                if scope.topics.len() != in_plan {
                    return Err(ScopeIncomplete::TooManyTopics { count: in_plan });
                }
                Ok(ShownScope::Restore {
                    scope: Box::new(scope),
                    topics_in_plan: in_plan,
                })
            }
            Kind::Switchover => {
                let scope = approval_scope::switchover_scope(request, self.plan_bytes.as_bytes())?;
                let in_plan =
                    logweir_core::switchover::SwitchoverPlan::parse(self.plan_bytes.as_bytes())
                        .map_err(|_| ScopeIncomplete::PlanUnreadable)?
                        .positions();
                let listed: usize = scope
                    .plan
                    .groups
                    .iter()
                    .flat_map(|g| g.topics.iter())
                    .map(|t| t.positions.len())
                    .sum();
                if listed != in_plan || scope.plan.positions != in_plan {
                    return Err(ScopeIncomplete::TooManyPositions { count: in_plan });
                }
                Ok(ShownScope::Switchover {
                    scope: Box::new(scope),
                    positions_in_plan: in_plan,
                })
            }
        }
    }
}

/// The plan a CREATE is about, before any object exists: what the two-person
/// gate checks can be shown in full.
#[derive(Clone, Copy, Debug)]
pub enum GatedPlan<'a> {
    /// A restore plan's bytes.
    Restore(&'a str),
    /// A switchover plan's bytes.
    Switchover(&'a str),
}

impl GatedPlan<'_> {
    /// The plan part of the scope a second person would be shown.
    ///
    /// # Errors
    ///
    /// Why it could not be shown in full.
    pub fn scope(self) -> Result<(), ScopeIncomplete> {
        match self {
            Self::Restore(bytes) => approval_scope::plan_scope(bytes.as_bytes()).map(|_| ()),
            Self::Switchover(bytes) => {
                approval_scope::switchover_plan_scope(bytes.as_bytes()).map(|_| ())
            }
        }
    }

    /// The sentence wording of its kind.
    #[must_use]
    pub const fn scope_of(self) -> ScopeOf {
        match self {
            Self::Restore(_) => ScopeOf::Restore,
            Self::Switchover(_) => ScopeOf::Switchover,
        }
    }
}
