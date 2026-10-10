//! PROD-16.2 — **two-person approval in the console**: the request as the
//! second person is shown it, and that person's click.
//!
//! A namespace bound to a policy whose `approverSignature` is `Console`
//! (`two-person`) takes its approval from a second person who signs in to the
//! shared console and clicks Approve. No personal key, no copy, sign and
//! paste. The console signs the approval with the key that signed the
//! request, so everything here is about ONE question: which bytes the console
//! may sign an approval over, and for whom.
//!
//! # The order of the click, and why
//!
//! 0. Transport (`crate::http::unsafe_request_guard`, the session
//!    authenticator): `Origin` exactly the configured public origin,
//!    `Content-Type: application/json`, a live session, its synchronizer CSRF
//!    token. The route exists for POST only; the read route beside it signs
//!    nothing.
//! 1. **Role.** `approval.submit`, which only the Approver role grants. An
//!    Administrator is not an Approver unless separately bound as one.
//! 2. **Mode.** A shared console, and a namespace whose CURRENT policy takes
//!    its approval from the console.
//! 3. **The console verifies its OWN earlier signature on the stored
//!    request** (`crate::approval::verified_request`), and until that passes
//!    reads nothing else of the stored object — which is why
//!    [`load_request`] hands it the two byte strings and nothing more.
//! 4. **The verified bytes are held to the Restore the click names**: its
//!    namespace, name, current UID, the hash of its current plan bytes, the
//!    current policy's name, digest and mode, and the window
//!    (`logweir_core::approval_policy::check_request_binding`). A signature
//!    copied from another request names another one of those.
//! 5. **What was shown is what is approved**: the body's
//!    `confirmationSha256` is the sha256 of the verified bytes.
//! 6. **Requester is not approver**
//!    (`logweir_core::approval_policy::console_separation`), over the
//!    requester INSIDE the verified bytes and the principal of this session.
//! 7. The approval is the verified document plus `approver`, `approvedAt`
//!    and the 2.2.0 version (`crate::approval::approved_document`), checked
//!    once more by the readers' own rule before it is signed.
//! 8. Create-only: a replay by the same approver answers the stored object; a
//!    second approver is a conflict and replaces nothing.
//!
//! The controller re-derives every one of these from the signed bytes before
//! any Job exists, and the runner again; nothing here is trusted by them.

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::{StatusCode, Uri};
use kube::ResourceExt as _;
use logweir_core::approval_policy::{
    self as policy, ApprovalPolicy, ApproverSignature, AuthorizationRefusal, ExpectedSubject,
    Requester, RestoreAuthorization,
};
use weirkeeper::crds::approval::{Approval, ApprovalSpec, SubjectKind, SubjectRef};
use weirkeeper::crds::restore::Restore;

use super::{authorize, get_object, json, ApiPath};
use crate::app::AppState;
use crate::approval::{self, VerifiedRequest};
use crate::auth::Actor;
use crate::authz::Action;
use crate::contract::{
    ApprovalRequestResponse, ApprovalRequestState, ApprovalRequestView, ApprovalResponse,
    ApprovalSubjectView, ApproveOfferView, ApproveRefusal, ConsoleApprovalRequest,
};
use crate::http::{read_json, RequestId, MAX_JSON_BODY};
use crate::kube::KubeFailure;
use crate::problem::{ApiError, FieldError, ProblemCode};
use crate::projection;

/// The read route identifier.
pub const ROUTE_REQUEST: &str = "GET /api/v1/namespaces/{ns}/restores/{name}/approval-request";
/// The click's route identifier.
pub const ROUTE_APPROVE: &str = "POST /api/v1/namespaces/{ns}/restores/{name}/console-approval";

/// What both routes need about the namespace before they read a request: the
/// policy a `two-person` namespace is bound to, or the refusal.
///
/// # Errors
///
/// `policy_mismatch` (409) outside a policy whose approval the console signs.
async fn console_policy(
    state: &AppState,
    actor: &Actor,
    ns: &str,
) -> Result<ApprovalPolicy, ApiError> {
    let effective = approval::effective_policies(state.approval(), state.kube())
        .await
        .map_err(KubeFailure::into_api_error)?
        .resolve(ns);
    actor.audit.note("approvalPolicy", effective.name());
    actor.audit.note("approvalMode", effective.mode().as_str());
    if let Some(digest) = effective.digest() {
        actor
            .audit
            .set_policy_digest(&format!("{}@{digest}", effective.name()));
    }
    match effective.bound() {
        Some(bound) if bound.approver_signature == ApproverSignature::Console => Ok(bound.clone()),
        _ => {
            actor.audit.set_failure("not_a_console_approval_policy");
            Err(ApiError::new(
                ProblemCode::PolicyMismatch,
                format!(
                    "Namespace {ns} is under approval policy {} ({}), which does not take its \
                     approval from the console; there is no request to approve in the console \
                     here.",
                    effective.name(),
                    logweir_core::approval_policy::OperatorMode::of(&effective)
                ),
            ))
        }
    }
}

/// What a stored request turned out to be.
enum Loaded {
    /// This console's own request for this Restore, this plan and this
    /// policy, inside its window.
    Pending(VerifiedRequest),
    /// The same, with its window closed.
    Expired(VerifiedRequest),
    /// Not this console's confirmation of this Restore under this policy. The
    /// sentence is for the page; the reason is for the audit record.
    NotConfirmed {
        /// The stable audit code.
        code: &'static str,
        /// What an operator reads.
        sentence: String,
    },
}

/// What a Restore expects an authorization to bind, from the object itself.
fn expected_subject(ns: &str, restore: &Restore) -> ExpectedSubject {
    ExpectedSubject {
        namespace: ns.to_string(),
        name: restore.name_any(),
        uid: restore.uid().unwrap_or_default(),
        plan_hash: logweir_core::ids::sha256_prefixed(restore.spec.plan_bytes.as_bytes()),
    }
}

/// **Steps 3 and 4.** Read the stored request for `restore`, verify this
/// console's own signature on it, and hold the verified bytes to the Restore,
/// its plan and the namespace's current policy.
///
/// THE STORED OBJECT IS REDUCED TO TWO STRINGS BEFORE ANYTHING IS DECIDED:
/// `spec.approvalBytes` and `spec.sidecarBytes`. Its annotations, labels,
/// `spec.planHash` and `spec.subjectRef` are whatever the last writer with
/// `create approvals` in the namespace put there, and nothing below can read
/// them.
///
/// # Errors
///
/// The adapter's failure, or `internal_error` for an unreadable key.
async fn load_request(
    state: &AppState,
    ns: &str,
    restore: &Restore,
    bound: &ApprovalPolicy,
) -> Result<Loaded, ApiError> {
    let key = match state.approval().confirmation_key() {
        Ok(Some(key)) => key,
        Ok(None) => {
            return Ok(Loaded::NotConfirmed {
                code: "confirmation_key_absent",
                sentence: "This console's confirmation key is not there yet, so it can neither \
                           confirm a request nor approve one."
                    .to_string(),
            })
        }
        Err(reason) => return Err(ApiError::new(ProblemCode::InternalError, reason)),
    };
    let confirmation_name = approval::confirmation_name(restore.spec.approval_ref_name());
    let (approval_bytes, sidecar_bytes) =
        match state.kube().get::<Approval>(ns, &confirmation_name).await {
            Ok(object) => (object.spec.approval_bytes, object.spec.sidecar_bytes),
            Err(KubeFailure::NotFound) => {
                return Ok(Loaded::NotConfirmed {
                    code: "confirmation_absent",
                    sentence: format!(
                        "Restore {} has no request {confirmation_name}: only a Restore submitted \
                         through the console carries one, and only that can be approved here.",
                        restore.name_any()
                    ),
                })
            }
            Err(other) => return Err(other.into_api_error()),
        };
    // ---- 3. THE CONSOLE'S OWN SIGNATURE, FIRST ----------------------------
    let verified = match approval::verified_request(key, &approval_bytes, &sidecar_bytes) {
        Ok(verified) => verified,
        Err(reason) => {
            tracing::warn!(
                namespace = %ns,
                confirmation = %confirmation_name,
                reason = %reason,
                "a stored approval request is NOT this console's confirmation and will not be \
                 approved"
            );
            return Ok(Loaded::NotConfirmed {
                code: "confirmation_not_verified",
                sentence: format!(
                    "The stored request {confirmation_name} does not carry this console's \
                     signature over its bytes. It was not confirmed here, nothing of it is \
                     shown, and it is never approved. Submit the Restore again through the \
                     console."
                ),
            });
        }
    };
    // ---- 4. THE VERIFIED BYTES, AGAINST THIS RESTORE AND THIS POLICY ------
    let doc = &verified.document;
    let binding =
        policy::check_request_binding(doc, &expected_subject(ns, restore), bound, state.now())
            .and_then(|()| {
                // The subject the request was signed for is the one this Restore
                // declares (PROD-15.1); the controller holds both to the plan.
                let declared = super::restores::restore_approval_subject(restore);
                if doc.approval_subject.as_deref() == declared.wire() {
                    Ok(())
                } else {
                    Err(AuthorizationRefusal::DocumentInvalid(
                "the request was signed for another approval subject than this Restore declares"
                    .to_string(),
            ))
                }
            });
    match binding {
        Ok(()) => Ok(Loaded::Pending(verified)),
        Err(AuthorizationRefusal::Expired(_)) => Ok(Loaded::Expired(verified)),
        Err(refusal) => {
            tracing::warn!(
                namespace = %ns,
                confirmation = %confirmation_name,
                reason = refusal.reason(),
                "a stored approval request carries this console's signature and does not bind \
                 this Restore, its plan and the current policy; it will not be approved"
            );
            Ok(Loaded::NotConfirmed {
                code: "confirmation_not_bound",
                sentence: format!(
                    "The stored request {confirmation_name} was not signed for this Restore's \
                     UID and plan under the namespace's current policy {} ({}). It is never \
                     approved. Submit the Restore again.",
                    bound.name,
                    refusal.reason()
                ),
            })
        }
    }
}

/// The approval already stored under the Restore's `approvalRef`, when this
/// console signed it: who approved and when, from its verified bytes.
async fn stored_approval(
    state: &AppState,
    ns: &str,
    restore: &Restore,
) -> Result<Option<(Approval, Option<RestoreAuthorization>)>, ApiError> {
    let name = restore.spec.approval_ref_name().to_string();
    match state.kube().get::<Approval>(ns, &name).await {
        Ok(object) => {
            let verified = match state.approval().confirmation_key() {
                Ok(Some(key)) => approval::verified_request(
                    key,
                    &object.spec.approval_bytes,
                    &object.spec.sidecar_bytes,
                )
                .ok()
                .map(|v| v.document),
                _ => None,
            };
            Ok(Some((object, verified)))
        }
        Err(KubeFailure::NotFound) => Ok(None),
        Err(other) => Err(other.into_api_error()),
    }
}

/// The principal of a session, as a console approval names it.
fn approver_of(actor: &Actor) -> policy::Approver {
    policy::Approver {
        issuer: actor.issuer.clone(),
        subject: actor.subject.clone(),
    }
}

/// Whether this session is offered the Approve button for a pending request,
/// by the rules the click is held to.
fn offer(state: &AppState, actor: &Actor, ns: &str, requester: &Requester) -> ApproveOfferView {
    let refuse = |refusal: ApproveRefusal, sentence: String| ApproveOfferView {
        offered: false,
        refusal: Some(refusal),
        sentence,
    };
    if state.shared().is_none() {
        return refuse(
            ApproveRefusal::LocalAdmin,
            "This is the in-cluster administrator console, whose one identity cannot be two \
             people. A second person approves in the shared console."
                .to_string(),
        );
    }
    if !state.authorizer().allows(actor, ns, Action::SubmitApproval) {
        return refuse(
            ApproveRefusal::NotApprover,
            "Your sign-in holds no approver role in this namespace. An administrator is not an \
             approver unless separately bound as one."
                .to_string(),
        );
    }
    match policy::console_separation(requester, &approver_of(actor)) {
        Ok(()) => ApproveOfferView {
            offered: true,
            refusal: None,
            sentence: "You are signed in as an approver and you are not the requester: \
                       approving records your identity and the time in the signed approval. No \
                       key is needed."
                .to_string(),
        },
        Err(refusal) => {
            let same = policy::fold_issuer(&requester.issuer) == policy::fold_issuer(&actor.issuer)
                && policy::fold_subject(&requester.subject) == policy::fold_subject(&actor.subject);
            if same {
                refuse(
                    ApproveRefusal::Requester,
                    "You requested this restore. A two-person approval needs a second person, \
                     and no role changes that."
                        .to_string(),
                )
            } else {
                refuse(
                    ApproveRefusal::NotSecondPerson,
                    capitalised(&refusal.to_string()),
                )
            }
        }
    }
}

/// A refusal sentence, as a sentence.
fn capitalised(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => format!("{}{}.", first.to_uppercase(), chars.as_str()),
        None => String::new(),
    }
}

/// The original topic names a restore under the original names restores, from
/// the Restore's own plan: at most [`projection::MAX_LIST_ENTRIES`], and the
/// whole count. `None` for a plan that does not parse (the controller refuses
/// it before any Job).
fn original_topics(restore: &Restore) -> Option<(Vec<String>, usize)> {
    let plan =
        serde_yaml::from_str::<logweir_core::spec::DrillSpec>(&restore.spec.plan_bytes).ok()?;
    let count = plan.source.topics.len();
    Some((
        plan.source
            .topics
            .into_iter()
            .take(projection::MAX_LIST_ENTRIES)
            .collect(),
        count,
    ))
}

/// `GET .../restores/{name}/approval-request` — the request, as the second
/// person is shown it before the click.
///
/// It verifies the console's own signature exactly as the click does, and
/// shows the request's fields only from the verified bytes. It signs and
/// stores nothing.
///
/// # Errors
///
/// `policy_mismatch` outside a two-person namespace, `not_found`.
pub async fn request(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    authorize(&state, &actor, &ns, Action::ReadApprovals)?;
    crate::http::parse_query(uri.query(), &[])?;
    let restore = get_object::<Restore>(&state, &actor, &ns, &name).await?;
    let bound = console_policy(&state, &actor, &ns).await?;
    let approval_name = restore.spec.approval_ref_name().to_string();
    let mut view = ApprovalRequestView {
        namespace: ns.clone(),
        restore: restore.name_any(),
        restore_uid: restore.uid().unwrap_or_default(),
        confirmation_name: approval::confirmation_name(&approval_name),
        approval_name,
        policy: bound.name.clone(),
        policy_digest: bound.digest(),
        state: ApprovalRequestState::NotConfirmed,
        state_sentence: String::new(),
        requester: None,
        plan_hash: None,
        approval_subject: None,
        original_topics: None,
        original_topics_count: None,
        ticket: None,
        issued_at: None,
        expires_at: None,
        confirmation_sha256: None,
        approver: None,
        approved_at: None,
        approve: ApproveOfferView {
            offered: false,
            refusal: Some(ApproveRefusal::NotPending),
            sentence: "There is no pending request to approve.".to_string(),
        },
    };
    let loaded = load_request(&state, &ns, &restore, &bound).await?;
    let (verified, expired) = match loaded {
        Loaded::NotConfirmed { code, sentence } => {
            actor.audit.note("approvalRequest", code);
            view.state_sentence = sentence;
            return Ok(json(
                StatusCode::OK,
                &ApprovalRequestResponse {
                    request_id,
                    item: view,
                },
            ));
        }
        Loaded::Pending(verified) => (verified, false),
        Loaded::Expired(verified) => (verified, true),
    };
    let doc = &verified.document;
    view.requester = Some(doc.requester.principal_id());
    view.plan_hash = Some(doc.plan_hash.clone());
    let subject =
        logweir_core::original_name::ApprovalSubject::from_wire(doc.approval_subject.as_deref());
    view.approval_subject = Some(match subject {
        Ok(logweir_core::original_name::ApprovalSubject::OriginalName) => {
            ApprovalSubjectView::OriginalName
        }
        Ok(logweir_core::original_name::ApprovalSubject::Ordinary) => ApprovalSubjectView::Ordinary,
        Err(_) => ApprovalSubjectView::Unknown,
    });
    if view.approval_subject == Some(ApprovalSubjectView::OriginalName) {
        if let Some((topics, count)) = original_topics(&restore) {
            view.original_topics = Some(topics);
            view.original_topics_count = Some(count);
        }
    }
    view.ticket.clone_from(&doc.ticket);
    view.issued_at = Some(doc.issued_at);
    view.expires_at = Some(doc.expires_at);
    view.confirmation_sha256 = Some(verified.sha256.clone());
    if let Some((_, approved)) = stored_approval(&state, &ns, &restore).await? {
        view.state = ApprovalRequestState::Approved;
        view.state_sentence = format!(
            "Approval {} exists for this Restore; the controller's verdict on it is on the \
             Approval.",
            view.approval_name
        );
        if let Some(approved) = approved {
            view.approver = approved
                .approver
                .as_ref()
                .map(policy::Approver::principal_id);
            view.approved_at = approved.approved_at;
        }
        view.approve.sentence = "This request has already been approved.".to_string();
    } else if expired {
        view.state = ApprovalRequestState::Expired;
        view.state_sentence = format!(
            "The request expired at {}; an expired request authorises nothing. Submit the \
             Restore again.",
            doc.expires_at.to_rfc3339()
        );
        view.approve.sentence =
            "The request has expired and can no longer be approved.".to_string();
    } else {
        view.state = ApprovalRequestState::Pending;
        view.state_sentence = format!(
            "The console confirmed this request for {} and it waits for a second person's \
             approval until {}.",
            doc.requester.principal_id(),
            doc.expires_at.to_rfc3339()
        );
        view.approve = offer(&state, &actor, &ns, &doc.requester);
    }
    Ok(json(
        StatusCode::OK,
        &ApprovalRequestResponse {
            request_id,
            item: view,
        },
    ))
}

/// A refusal of the click, attributed in the audit record under its own
/// stable reason.
fn refused(actor: &Actor, code: &'static str, problem: ProblemCode, detail: String) -> ApiError {
    actor.audit.set_failure(code);
    ApiError::new(problem, detail)
}

/// `POST .../restores/{name}/console-approval` — **the second person's
/// click** (see the module documentation for the order and its reasons).
///
/// # Errors
///
/// `forbidden` (not an approver; the requester; not a second person),
/// `policy_mismatch` (the administrator console; a namespace that does not
/// take its approval from the console; a stored request that is not this
/// console's, or not this Restore's), `not_found`, `state_conflict` (expired;
/// changed since it was shown; already approved by someone else),
/// `validation_failed`.
#[allow(clippy::too_many_lines)]
pub async fn approve(
    State(state): State<AppState>,
    RequestId(request_id): RequestId,
    actor: Actor,
    ApiPath((ns, name)): ApiPath<(String, String)>,
    uri: Uri,
    body: Body,
) -> Result<Response, ApiError> {
    // ---- 1. the Approver role ---------------------------------------------
    authorize(&state, &actor, &ns, Action::SubmitApproval)?;
    crate::http::parse_query(uri.query(), &[])?;
    let request: ConsoleApprovalRequest = read_json(body, MAX_JSON_BODY).await?;
    let well_formed = request
        .confirmation_sha256
        .strip_prefix("sha256:")
        .is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        });
    if !well_formed {
        return Err(ApiError::validation(vec![FieldError::new(
            "confirmationSha256",
            "invalid_format",
            "must be sha256: followed by 64 lowercase hexadecimal characters, as the request's \
             view showed it",
        )]));
    }
    actor.audit.note("approverPrincipal", &actor.id());
    // ---- 2. the console's mode, and the namespace's ------------------------
    if state.shared().is_none() || policy::fold_issuer(&actor.issuer) == policy::LOCAL_ADMIN_ISSUER
    {
        return Err(refused(
            &actor,
            "local_admin_cannot_approve",
            ProblemCode::PolicyMismatch,
            "This is the in-cluster administrator console, whose one identity cannot be two \
             people. A second person approves in the shared console. Nothing was approved."
                .to_string(),
        ));
    }
    let restore = get_object::<Restore>(&state, &actor, &ns, &name).await?;
    let bound = console_policy(&state, &actor, &ns).await?;
    // ---- 3 / 4. the console's own signature, then the binding --------------
    let verified = match load_request(&state, &ns, &restore, &bound).await? {
        Loaded::Pending(verified) => verified,
        Loaded::Expired(verified) => {
            actor
                .audit
                .note("requester", &verified.document.requester.principal_id());
            return Err(refused(
                &actor,
                "request_expired",
                ProblemCode::StateConflict,
                format!(
                    "The request expired at {}; an expired request authorises nothing. Submit \
                     the Restore again.",
                    verified.document.expires_at.to_rfc3339()
                ),
            ));
        }
        Loaded::NotConfirmed { code, sentence } => {
            let problem = if code == "confirmation_absent" {
                ProblemCode::NotFound
            } else {
                ProblemCode::PolicyMismatch
            };
            return Err(refused(&actor, code, problem, sentence));
        }
    };
    // FROM HERE ON, EVERY FACT ABOUT THE REQUEST IS `doc`: the bytes this
    // console verified its own signature on.
    let doc = &verified.document;
    let requester = doc.requester.principal_id();
    actor.audit.note("requester", &requester);
    actor.audit.set_plan_hash(&doc.plan_hash);
    actor.audit.note("expiresAt", &doc.expires_at.to_rfc3339());
    // ---- 5. what was shown is what is approved -----------------------------
    if request.confirmation_sha256 != verified.sha256 {
        return Err(refused(
            &actor,
            "request_changed",
            ProblemCode::StateConflict,
            "The request stored for this Restore is not the one you were shown (its signed \
             bytes hash to another value). Open the request again and review it before \
             approving. Nothing was approved."
                .to_string(),
        ));
    }
    // ---- 6. requester is not approver --------------------------------------
    let approver = approver_of(&actor);
    if let Err(refusal) = policy::console_separation(&doc.requester, &approver) {
        actor.audit.note("separation", "refused");
        return Err(refused(
            &actor,
            "self_approval_forbidden",
            ProblemCode::Forbidden,
            format!(
                "Policy {} needs a second person: {refusal}. Nothing was approved.",
                bound.name
            ),
        ));
    }
    actor.audit.note("separation", "distinct");
    // ---- 7. the approval: the verified request plus who and when -----------
    let now = state.now();
    let approved = approval::approved_document(doc, approver, now);
    let expected = expected_subject(&ns, &restore);
    // THE READERS' OWN RULE, BEFORE ANYTHING IS SIGNED. The controller and the
    // runner will refuse a document this refuses; the console never signs one.
    if let Err(refusal) = policy::check_restore_authorization(&approved, &expected, &bound, now) {
        return Err(match refusal {
            // The console's clock is behind the one that stamped the request
            // (another replica), or the window closed in this instant.
            AuthorizationRefusal::WindowInvalid(_) | AuthorizationRefusal::Expired(_) => refused(
                &actor,
                "approval_outside_window",
                ProblemCode::StateConflict,
                format!("{refusal}. Try again; nothing was approved."),
            ),
            other => refused(
                &actor,
                "approval_not_valid",
                ProblemCode::InternalError,
                format!("The approval this console built would not verify ({other}); it was not signed."),
            ),
        });
    }
    actor.audit.note("approvedAt", &now.to_rfc3339());
    let key = match state.approval().confirmation_key() {
        Ok(Some(key)) => key,
        Ok(None) => {
            return Err(ApiError::new(
                ProblemCode::InternalError,
                "This console holds no confirmation key.",
            ))
        }
        Err(reason) => return Err(ApiError::new(ProblemCode::InternalError, reason)),
    };
    let bytes = approved.to_bytes();
    let sidecar = key
        .sign(&bytes)
        .map_err(|reason| ApiError::new(ProblemCode::InternalError, reason))?;
    let approval_name = restore.spec.approval_ref_name().to_string();
    let object = Approval {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(approval_name.clone()),
            namespace: Some(ns.clone()),
            // FOR A HUMAN READING THE OBJECT, NEVER FOR A DECISION: every
            // reader takes these from the signed bytes.
            annotations: Some(
                [
                    (
                        "logweir.dev/approval-mode".to_string(),
                        bound.mode.as_str().to_string(),
                    ),
                    (
                        "logweir.dev/approval-policy".to_string(),
                        bound.name.clone(),
                    ),
                    ("logweir.dev/requester".to_string(), requester.clone()),
                    ("logweir.dev/approver".to_string(), actor.id()),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        },
        spec: ApprovalSpec {
            subject_ref: SubjectRef {
                kind: SubjectKind::Restore,
                name: restore.name_any(),
            },
            plan_hash: approved.plan_hash.clone(),
            approval_bytes: String::from_utf8(bytes).map_err(|_| {
                ApiError::new(
                    ProblemCode::InternalError,
                    "The authorization document is not UTF-8.",
                )
            })?,
            sidecar_bytes: serde_json::to_string(&sidecar).map_err(|_| {
                ApiError::new(
                    ProblemCode::InternalError,
                    "The sidecar could not be rendered.",
                )
            })?,
        },
        status: None,
    };
    // ---- 8. create-only ----------------------------------------------------
    let (stored, replayed) = match state.kube().create(&ns, &object).await {
        Ok(created) => (created, false),
        Err(KubeFailure::AlreadyExists) => {
            let existing = state
                .kube()
                .get::<Approval>(&ns, &approval_name)
                .await
                .map_err(KubeFailure::into_api_error)?;
            // A REPLAY IS THE SAME APPROVER'S APPROVAL OF THE SAME REQUEST,
            // read from bytes this console signed — never from the object's
            // annotations. Anything else under the name is somebody else's,
            // and is never replaced.
            let same = approval::verified_request(
                key,
                &existing.spec.approval_bytes,
                &existing.spec.sidecar_bytes,
            )
            .ok()
            .is_some_and(|stored| {
                // The stored approval is this one in everything but the
                // instant of the click: the same request, the same approver.
                RestoreAuthorization {
                    approved_at: approved.approved_at,
                    ..stored.document
                } == approved
            });
            if !same {
                return Err(refused(
                    &actor,
                    "already_approved",
                    ProblemCode::StateConflict,
                    format!(
                        "An Approval named {approval_name} already exists for this Restore and \
                         is not your approval of this request; it is immutable and never \
                         replaced."
                    ),
                ));
            }
            (existing, true)
        }
        Err(other) => return Err(other.into_api_error()),
    };
    actor
        .audit
        .note("approval", &format!("{ns}/{}", stored.name_any()));
    Ok(json(
        if replayed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        &ApprovalResponse {
            request_id,
            replayed: Some(replayed),
            item: projection::approval(&stored),
        },
    ))
}
