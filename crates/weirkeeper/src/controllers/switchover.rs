//! The `Switchover` reconciler (PROD-04.2; design §3.4 step 1).
//!
//! # Admission, before anything is created
//!
//! 1. The plan parses and is bounded, and its `restore` block is
//!    `spec.restoreRef` (`SwitchoverPlanInvalid`).
//! 2. The `Restore` exists with that UID, is `Succeeded`, and its evidence
//!    verdict is `Valid` (`RestoreNotSucceeded`; a restore still running is a
//!    hold). It is not scratch (`RestoreIsScratch`: a clone is deleted at its
//!    TTL).
//! 3. No other non-terminal `Switchover` names it (`SwitchoverInProgress`).
//! 4. The `Approval` is `Verified=True` for subject kind `Switchover` and this
//!    object's UID (check 8 compares the kind the approval binds); a pending
//!    one is a hold, an approval for anything else `SubjectKindMismatch`.
//!
//! Then one immutable bundle ConfigMap (the two plans, the approval, the
//! approver's key, the evidence keyring, and for an authorization v2 the
//! policy snapshot and the console key), the admission marker
//! (`Admitted=True`, [`admitted_status`]) and one Job, `logweir switchover
//! run`, with the restore's target credential projection and the evidence
//! destination's two grants: `evidenceRead` to read the restore's evidence,
//! `evidenceWrite` to write the record (theme A's D1,
//! [`crate::destination::EvidenceAccessEnv`]). It reads no archive. The
//! controller never dials Kafka.
//!
//! A bundle that already exists is used only when it is exactly the one this
//! admission would write ([`compatible_bundle`]); anything else is refused
//! `ApprovalBundleConflict` and no Job is created (the Restore twin's rule).
//!
//! # One admission per Switchover
//!
//! `Admitted=True` is written once, before the Job, and its time is the
//! admission instant the approval controller records the Approval consumed at.
//! A Job named after this Switchover before its admission is not adopted
//! ([`JOB_NOT_ADMITTED`]); after it, a Job under its name is the run only when
//! it is this Switchover's own ([`own_job`]), and any other ends it
//! `JobNameConflict`. A recorded Job that is gone ends the run ([`JOB_LOST`]):
//! it is never admitted again ([`job_step`]).
//!
//! # The run's result
//!
//! Read off the finished pod's log tail: one `switchover-group=` line per
//! group into `status.groups`, the counts, the record key, the outcome, and
//! the `refusal-reason=` or `failure-reason=` word.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Resource, ResourceExt as _};
use logweir_core::switchover::{self as so, SwitchoverPlan};
use serde_json::json;
use tracing::{info, warn};

use super::Context;
use crate::crds::approval::{Approval, SubjectKind};
use crate::crds::kafka_cluster::KafkaCluster;
use crate::crds::restore::{Restore, TargetMode};
use crate::crds::switchover::{
    Switchover, SwitchoverCounts, SwitchoverEvidence, SwitchoverGroupStatus, SwitchoverStatus,
};
use crate::job::{self, ConfigMapMount, EnvFromSecret, RunnerJobSpec, RunnerOwner, SecretMount};

/// The phases this reconciler writes.
pub const PHASE_PENDING: &str = "Pending";
pub const PHASE_RUNNING: &str = "Running";
/// The terminal phases.
pub const TERMINAL_PHASES: [&str; 4] = [
    so::SUCCEEDED,
    so::PARTIALLY_APPLIED,
    so::RUN_FAILED,
    so::RUN_REFUSED,
];

/// The hold reasons.
pub const HOLD_APPROVAL_NOT_VERIFIED: &str = "ApprovalNotVerified";
pub const HOLD_RESTORE_RUNNING: &str = "RestoreNotFinished";
/// The refusal an approval for another subject gets.
pub const SUBJECT_KIND_MISMATCH: &str = "SubjectKindMismatch";

/// A Job named after this Switchover exists, and no admission of it created
/// that Job: it is not adopted, and the Switchover waits for it to go.
pub const JOB_NOT_ADMITTED: &str = "SwitchoverJobNotAdmitted";
/// The Job this Switchover's admission recorded is gone before it finished:
/// the run ends, and is never admitted again.
pub const JOB_LOST: &str = "SwitchoverJobLost";

/// How long a hold waits before it looks again.
pub const HOLD_REQUEUE_SECS: u64 = 15;

/// The bundle ConfigMap's members, mounted at [`job::APPROVAL_MOUNT_PATH`].
pub const PLAN_FILE: &str = "switchover-plan.yaml";
pub const RESTORE_PLAN_FILE: &str = "restore-plan.yaml";
pub const APPROVAL_FILE: &str = "approval.json";
pub const APPROVAL_SIG_FILE: &str = "approval.sig";
pub const APPROVER_KEY_FILE: &str = "approver.pub.pem";
pub const EVIDENCE_KEYS_FILE: &str = "evidence-keys.json";
pub const POLICY_FILE: &str = "approval-policy.json";
pub const CONFIRMATION_KEY_FILE: &str = "confirmation.pub.pem";

/// The bundle ConfigMap of one `Switchover`.
#[must_use]
pub fn bundle_name(switchover: &str) -> String {
    format!("{switchover}-switchover")
}

/// What admission decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// Create the bundle and the Job.
    Admit,
    /// Wait: `(reason, message)`.
    Hold(&'static str, String),
    /// Terminal: `(reason, message)`.
    Refuse(&'static str, String),
}

/// Whether a status is terminal.
#[must_use]
pub fn is_terminal(status: Option<&SwitchoverStatus>) -> bool {
    status
        .and_then(|s| s.phase.as_deref())
        .is_some_and(|p| TERMINAL_PHASES.contains(&p))
}

/// The admission marker, when this Switchover carries one: `Admitted=True`,
/// written by this controller (`Switchover.status` is its own) on the pass that
/// admitted the run, before its Job.
#[must_use]
pub fn admission_marker(status: Option<&SwitchoverStatus>) -> Option<&crate::crds::Condition> {
    crate::conditions::current_condition(
        status.and_then(|s| s.conditions.as_ref()),
        crate::conditions::CONDITION_ADMITTED,
    )
    .filter(|c| c.status == "True")
}

/// What a non-terminal Switchover's next step is, from its own status and
/// whether a Job named after it exists. Pure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStep {
    /// The run's Job: observe it.
    Observe,
    /// A Job no admission of this Switchover created: not adopted, wait.
    NotAdmitted,
    /// The run's recorded Job is gone: the run ends, never re-admitted.
    Lost,
    /// No Job and none recorded: admission (or, admitted, its Job created).
    Admit,
}

/// [`JobStep`] for a non-terminal Switchover.
///
/// * A Job exists. It is this run's when the admission marker is there (this
///   controller wrote it before creating the Job), or when `status.jobRef` is
///   (an older build's run, admitted before the marker existed). Otherwise no
///   admission created it, and it is never adopted: adopting it is how a Job
///   anyone made would read as this Switchover's run.
/// * No Job. A recorded Job (`status.jobRef`, written only once this
///   controller created or observed it) is gone: the run is lost, and is not
///   admitted again on an approval that may have expired since. With none
///   recorded, admission runs; an admitted Switchover whose Job was not yet
///   created (the pass stopped between the marker and the Job) completes that
///   one admission, at the marker's time.
#[must_use]
pub fn job_step(status: Option<&SwitchoverStatus>, job_exists: bool) -> JobStep {
    let recorded = status.and_then(|s| s.job_ref.as_ref()).is_some();
    match (job_exists, admission_marker(status).is_some() || recorded) {
        (true, true) => JobStep::Observe,
        (true, false) => JobStep::NotAdmitted,
        (false, _) if recorded => JobStep::Lost,
        (false, _) => JobStep::Admit,
    }
}

/// Whether a Job named after `sw` is the one this controller created for it:
/// its one owner reference is this Switchover UID as controller, as
/// [`job_spec`] writes it (review L10, the Restore twin's
/// `restore::compatible_restore_job`). A Job anyone made under that name is
/// never observed as the run. An owner reference can be written by whoever
/// may create Jobs, so this matches the twin and no more.
#[must_use]
pub fn own_job(job: &Job, sw: &Switchover) -> bool {
    let owners = job.metadata.owner_references.as_deref().unwrap_or_default();
    job.metadata.name.as_deref() == Some(sw.name_any().as_str())
        && owners.len() == 1
        && owners[0] == owner_reference(sw)
}

/// The admitting patch: `Admitted=True` (its time kept once written, by
/// [`crate::conditions::merge_condition`]) and phase `Running`, written after
/// the bundle and before the Job.
#[must_use]
pub fn admitted_status(sw: &Switchover, now: chrono::DateTime<Utc>) -> SwitchoverStatus {
    let stored = sw.status.as_ref().and_then(|s| s.conditions.clone());
    let marker = crate::conditions::merge_condition(
        crate::conditions::current_condition(
            stored.as_ref(),
            crate::conditions::CONDITION_ADMITTED,
        ),
        crate::crds::Condition {
            r#type: crate::conditions::CONDITION_ADMITTED.to_string(),
            status: "True".to_string(),
            observed_generation: sw.meta().generation,
            last_transition_time: Some(now),
            reason: Some(crate::conditions::REASON_ADMITTED.to_string()),
            message: Some(format!(
                "admitted under Approval {}; this is its one admission",
                sw.spec.approval_ref.name
            )),
        },
    );
    let mut conditions: Vec<crate::crds::Condition> = stored
        .unwrap_or_default()
        .into_iter()
        .filter(|c| c.r#type != crate::conditions::CONDITION_ADMITTED)
        .collect();
    conditions.push(marker);
    SwitchoverStatus {
        phase: Some(PHASE_RUNNING.into()),
        reason: Some(crate::conditions::REASON_ADMITTED.into()),
        message: Some(format!(
            "admitted under Approval {}; the Job follows",
            sw.spec.approval_ref.name
        )),
        conditions: Some(conditions),
        ..SwitchoverStatus::default()
    }
}

/// **Admission** (steps 1 to 4), pure. `others` are the namespace's other
/// `Switchover`s.
#[must_use]
pub fn admit(
    sw: &Switchover,
    restore: Option<&Restore>,
    others: &[Switchover],
    approval: Option<&Approval>,
) -> Admission {
    // 1. The plan and its restore block.
    let plan = match SwitchoverPlan::parse(sw.spec.plan_bytes.as_bytes()) {
        Ok(p) => p,
        Err(e) => return Admission::Refuse(so::PLAN_INVALID, e),
    };
    if plan.restore.name != sw.spec.restore_ref.name || plan.restore.uid != sw.spec.restore_ref.uid
    {
        return Admission::Refuse(
            so::PLAN_INVALID,
            format!(
                "the plan names Restore {} UID {}, and spec.restoreRef names {} UID {}",
                plan.restore.name,
                plan.restore.uid,
                sw.spec.restore_ref.name,
                sw.spec.restore_ref.uid
            ),
        );
    }
    // 2. The restore.
    let Some(restore) =
        restore.filter(|r| r.uid().as_deref() == Some(sw.spec.restore_ref.uid.as_str()))
    else {
        return Admission::Refuse(
            so::RESTORE_NOT_SUCCEEDED,
            format!(
                "Restore {} UID {} does not exist in this namespace",
                sw.spec.restore_ref.name, sw.spec.restore_ref.uid
            ),
        );
    };
    // PROD-19.1: the switchover, its restore and its approval are in one
    // workspace, one the install lists.
    if let Err(refusal) = crate::workspace::installed()
        .of(sw)
        .map(|_| ())
        .and_then(|()| crate::workspace::same(sw, restore))
    {
        return Admission::Refuse(refusal.reason(), refusal.to_string());
    }
    if restore.spec.target.mode == TargetMode::Scratch {
        return Admission::Refuse(
            so::RESTORE_IS_SCRATCH,
            "the restore is a scratch drill: its topics are torn down, and a clone is deleted at \
             its TTL, so no group is moved onto them"
                .to_string(),
        );
    }
    let status = restore.status.as_ref();
    let phase = status.and_then(|s| s.phase.as_deref()).unwrap_or("");
    if !matches!(phase, "Succeeded" | "Failed" | "Refused") {
        return Admission::Hold(
            HOLD_RESTORE_RUNNING,
            format!("Restore {} is {phase:?}, not finished", restore.name_any()),
        );
    }
    let verdict = status
        .and_then(|s| s.evidence.as_ref())
        .and_then(|e| e.verification.as_ref())
        .and_then(|v| v.result.as_deref());
    if phase != "Succeeded" || verdict != Some("Valid") {
        return Admission::Refuse(
            so::RESTORE_NOT_SUCCEEDED,
            format!(
                "Restore {} is {phase} and its evidence verdict is {}; a switchover applies only \
                 a succeeded restore whose signed scorecard verified",
                restore.name_any(),
                verdict.unwrap_or("not recorded")
            ),
        );
    }
    // 3. One switchover at a time per restore: the older one wins.
    let mine = (
        sw.metadata.creation_timestamp.as_ref().map(|t| t.0),
        sw.name_any(),
    );
    let blocker = others.iter().find(|o| {
        o.uid() != sw.uid()
            && o.spec.restore_ref.name == sw.spec.restore_ref.name
            && !is_terminal(o.status.as_ref())
            && (o.status.as_ref().and_then(|s| s.job_ref.as_ref()).is_some()
                || (
                    o.metadata.creation_timestamp.as_ref().map(|t| t.0),
                    o.name_any(),
                ) < mine)
    });
    if let Some(o) = blocker {
        return Admission::Refuse(
            so::SWITCHOVER_IN_PROGRESS,
            format!(
                "Switchover {} is still applying positions from Restore {}; one runs at a time",
                o.name_any(),
                sw.spec.restore_ref.name
            ),
        );
    }
    // 4. The approval, for THIS object and this kind.
    let Some(approval) = approval else {
        return Admission::Hold(
            HOLD_APPROVAL_NOT_VERIFIED,
            format!("Approval {} does not exist yet", sw.spec.approval_ref.name),
        );
    };
    if let Err(refusal) = crate::workspace::same(sw, approval) {
        return Admission::Refuse(refusal.reason(), refusal.to_string());
    }
    if approval.spec.subject_ref.kind != SubjectKind::Switchover {
        return Admission::Refuse(
            SUBJECT_KIND_MISMATCH,
            format!(
                "Approval {} authorises a {}, and this is a Switchover; an approval binds one kind \
                 of subject",
                approval.name_any(),
                approval.spec.subject_ref.kind
            ),
        );
    }
    let st = approval.status.as_ref();
    if st.and_then(|s| s.verified) != Some(true) {
        return Admission::Hold(
            HOLD_APPROVAL_NOT_VERIFIED,
            format!("Approval {} is not Verified=True", approval.name_any()),
        );
    }
    let bound = st.and_then(|s| s.verified_subject_ref.as_ref());
    let ours = bound.is_some_and(|b| {
        b.kind == SubjectKind::Switchover
            && b.name == sw.name_any()
            && Some(b.uid.as_str()) == sw.uid().as_deref()
    });
    if !ours {
        return Admission::Refuse(
            SUBJECT_KIND_MISMATCH,
            format!(
                "Approval {} was verified for another object, not Switchover {} UID {}",
                approval.name_any(),
                sw.name_any(),
                sw.uid().unwrap_or_default()
            ),
        );
    }
    if approval.spec.plan_hash != logweir_core::ids::sha256_prefixed(sw.spec.plan_bytes.as_bytes())
    {
        return Admission::Refuse(
            "PlanHashMismatch",
            "the Approval names another plan hash than spec.planBytes".to_string(),
        );
    }
    Admission::Admit
}

/// The owner reference every object this reconciler creates carries.
fn owner_reference(sw: &Switchover) -> OwnerReference {
    OwnerReference {
        api_version: Switchover::api_version(&()).to_string(),
        kind: Switchover::kind(&()).to_string(),
        name: sw.name_any(),
        uid: sw.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// The bundle members, by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bundle {
    pub approver_key_pem: String,
    pub evidence_keys_json: String,
    /// The bound policy's snapshot and the console key, for an authorization
    /// v2.
    pub v2: Option<(String, String)>,
}

/// The immutable bundle ConfigMap, pure.
#[must_use]
pub fn bundle_config_map(
    sw: &Switchover,
    restore: &Restore,
    approval: &Approval,
    bundle: &Bundle,
) -> ConfigMap {
    let mut data = BTreeMap::new();
    data.insert(PLAN_FILE.to_string(), sw.spec.plan_bytes.clone());
    data.insert(
        RESTORE_PLAN_FILE.to_string(),
        restore.spec.plan_bytes.clone(),
    );
    data.insert(
        APPROVAL_FILE.to_string(),
        approval.spec.approval_bytes.clone(),
    );
    data.insert(
        APPROVAL_SIG_FILE.to_string(),
        approval.spec.sidecar_bytes.clone(),
    );
    data.insert(
        APPROVER_KEY_FILE.to_string(),
        bundle.approver_key_pem.clone(),
    );
    data.insert(
        EVIDENCE_KEYS_FILE.to_string(),
        bundle.evidence_keys_json.clone(),
    );
    if let Some((policy, confirmation)) = &bundle.v2 {
        data.insert(POLICY_FILE.to_string(), policy.clone());
        data.insert(CONFIRMATION_KEY_FILE.to_string(), confirmation.clone());
    }
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(bundle_name(&sw.name_any())),
            namespace: sw.namespace(),
            owner_references: Some(vec![owner_reference(sw)]),
            ..ObjectMeta::default()
        },
        immutable: Some(true),
        data: Some(data),
        ..ConfigMap::default()
    }
}

/// Whether an existing `<sw>-switchover` ConfigMap is exactly the bundle this
/// admission would write (M1): its one owner is this Switchover UID as its
/// controller, it is immutable, and its members are byte-identical. Anything
/// else could put another plan, approval or key in front of the Job, which
/// mounts the bundle by name. The Restore twin is
/// `restore::compatible_approval_bundle`.
#[must_use]
pub fn compatible_bundle(existing: &ConfigMap, desired: &ConfigMap) -> bool {
    let owners = existing
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default();
    let wanted = desired
        .metadata
        .owner_references
        .as_deref()
        .and_then(<[OwnerReference]>::first);
    existing.metadata.name == desired.metadata.name
        && owners.len() == 1
        && wanted.is_some_and(|w| owners[0] == *w)
        && existing.immutable == Some(true)
        && existing.data == desired.data
        && existing.binary_data == desired.binary_data
}

/// The members whose bytes differ between an existing bundle and the desired
/// one, by name, sorted: the refusal names keys, never bytes.
#[must_use]
pub fn differing_bundle_members(existing: &ConfigMap, desired: &ConfigMap) -> Vec<String> {
    let empty = BTreeMap::new();
    let a = existing.data.as_ref().unwrap_or(&empty);
    let b = desired.data.as_ref().unwrap_or(&empty);
    let mut keys: Vec<String> = a.keys().chain(b.keys()).cloned().collect();
    keys.sort();
    keys.dedup();
    keys.into_iter().filter(|k| a.get(k) != b.get(k)).collect()
}

/// Create the bundle, or accept the one already there only when it is
/// [`compatible_bundle`]. `Ok(Some(message))` is the refusal.
///
/// # Errors
///
/// A Kubernetes API failure.
pub async fn ensure_bundle(
    maps: &Api<ConfigMap>,
    desired: &ConfigMap,
) -> Result<Option<String>, kube::Error> {
    let name = desired.metadata.name.clone().unwrap_or_default();
    match maps.create(&PostParams::default(), desired).await {
        Ok(_) => Ok(None),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            let Some(existing) = maps.get_opt(&name).await? else {
                return Ok(Some(format!(
                    "the bundle ConfigMap {name} answered 409 to create and was gone on the                      following read"
                )));
            };
            if compatible_bundle(&existing, desired) {
                return Ok(None);
            }
            let differing = differing_bundle_members(&existing, desired);
            let members = if differing.is_empty() {
                String::new()
            } else {
                format!("; the differing member(s) are {}", differing.join(", "))
            };
            Ok(Some(format!(
                "the ConfigMap {name} already exists but its owner, immutable bit or members                  differ from this admission's bundle; the Job mounts it by name, so it cannot                  stand in for the verified approval{members}"
            )))
        }
        Err(e) => Err(e),
    }
}

/// The restore's target access, as its Job had it, and the evidence's read
/// and write grants.
#[derive(Debug, Clone, Default)]
pub struct Access {
    pub secret_mounts: Vec<SecretMount>,
    pub config_map_mounts: Vec<ConfigMapMount>,
    pub env_from_secret: Vec<EnvFromSecret>,
    pub env_literal: Vec<(String, String)>,
    pub service_account_name: String,
    /// The store contract handshake, for a destination-backed restore.
    pub store_contract: bool,
    /// The grant the evidence is read with, as a failure names it
    /// (`--evidence-read-grant`).
    pub evidence_read_grant: Option<String>,
    /// The restore's own immutable plan `ConfigMap`, mounted at `/plan`: it
    /// holds the CA bundles the restore froze (`evidence-ca.pem`), which the
    /// store environment's `LOGWEIR_*_CA_FILE` name.
    pub plan_config_map: Option<String>,
}

/// What an inline restore's Job reads and writes its evidence with: the
/// plan's own archive credential, ambient in the pod.
pub const INLINE_READ_GRANT: &str =
    "the inline archive credential (spec.sourceArchive.secretRef), which reads and writes the \
     evidence";

/// The restore's target credential projection, by the resolver `Restore`'s
/// Job uses, so a switchover reaches exactly the target the restore reached;
/// and, for a destination-backed restore, `evidence`: the evidence
/// destination's read grant in the archive slot and its write grant in the
/// evidence slot (D1). An inline restore's Job reads and writes with the
/// plan's own archive credential, as its run did.
///
/// # Errors
///
/// `(reason, message)` when the connection does not resolve.
pub fn restore_access(
    restore: &Restore,
    plan: &logweir_core::spec::RestoreSpec,
    cluster: &KafkaCluster,
    evidence: Option<&crate::destination::EvidenceAccessEnv>,
) -> Result<Access, (&'static str, String)> {
    use super::backup;
    let namespace = restore.namespace().unwrap_or_default();
    let connection =
        crate::connection::resolve(cluster, crate::connection::ConnectionUse::RestoreTarget)
            .map_err(|e| ("ConnectionNotResolved", e.to_string()))?;
    connection
        .check_job_namespace(&namespace)
        .map_err(|e| ("ConnectionNotResolved", e.to_string()))?;
    connection
        .check_restore_target(&plan.target.bootstrap_servers, &plan.target.auth)
        .map_err(|e| ("ConnectionNotResolved", e.to_string()))?;
    let projection = connection.project();
    let mut access = Access {
        secret_mounts: vec![SecretMount {
            volume: backup::SIGNING_VOLUME.to_string(),
            secret_name: backup::SIGNING_KEY_SECRET.to_string(),
            mount_path: backup::SIGNING_MOUNT_PATH.to_string(),
            items: vec![(
                backup::SIGNING_KEY_SECRET_KEY.to_string(),
                backup::SIGNING_KEY_FILE.to_string(),
            )],
        }],
        config_map_mounts: projection.config_map_mounts,
        env_from_secret: projection.env_from_secret,
        env_literal: vec![("RUST_LOG".to_string(), "info".to_string())],
        service_account_name: connection.execution.service_account_name.clone(),
        store_contract: evidence.is_some(),
        evidence_read_grant: Some(
            evidence
                .map_or(INLINE_READ_GRANT, |e| e.read_grant.as_str())
                .to_string(),
        ),
        plan_config_map: Some(super::restore::plan_config_map_name(&restore.name_any())),
    };
    access.secret_mounts.extend(projection.secret_mounts);
    access.secret_mounts.sort_by(|a, b| a.volume.cmp(&b.volume));
    match evidence {
        None => {
            if let Some(secret) = restore.spec.source_archive.secret_ref.as_ref() {
                for (env, key) in [
                    (backup::ARCHIVE_ACCESS_KEY_ENV, backup::ARCHIVE_ACCESS_KEY),
                    (backup::ARCHIVE_SECRET_KEY_ENV, backup::ARCHIVE_SECRET_KEY),
                ] {
                    access.env_from_secret.push(EnvFromSecret {
                        name: env.to_string(),
                        secret_name: secret.name.clone(),
                        optional: false,
                        key: key.to_string(),
                    });
                }
                let binding = super::restore::inline_restore_binding_env(
                    &secret.name,
                    plan,
                    &backup::archive_addressing_env(),
                );
                access
                    .env_from_secret
                    .extend(binding.from_secret.iter().cloned());
                access.env_literal.extend(binding.literals.iter().cloned());
            }
            access.env_literal.extend(backup::archive_addressing_env());
        }
        Some(evidence) => {
            for env in [&evidence.read, &evidence.write] {
                access
                    .env_from_secret
                    .extend(env.from_secret.iter().cloned());
                access.env_literal.extend(env.literals.iter().cloned());
            }
            if let Some(sa) = evidence.service_account_name() {
                access.service_account_name = sa.to_string();
            }
        }
    }
    access.env_literal.extend(projection.env_literal);
    Ok(access)
}

/// The runner's argv.
#[must_use]
pub fn runner_argv(
    sw: &Switchover,
    scorecard_key: &str,
    v2: bool,
    contract: bool,
    read_grant: Option<&str>,
) -> Vec<String> {
    let at = |f: &str| format!("{}/{f}", job::APPROVAL_MOUNT_PATH);
    let mut argv = vec![
        "switchover".to_string(),
        "run".to_string(),
        "--plan".to_string(),
        at(PLAN_FILE),
        "--restore-plan".to_string(),
        at(RESTORE_PLAN_FILE),
        "--approval".to_string(),
        at(APPROVAL_FILE),
        "--approver-key".to_string(),
        at(APPROVER_KEY_FILE),
        "--evidence-keys".to_string(),
        at(EVIDENCE_KEYS_FILE),
        "--signing-key".to_string(),
        format!(
            "{}/{}",
            super::backup::SIGNING_MOUNT_PATH,
            super::backup::SIGNING_KEY_FILE
        ),
        "--scorecard-key".to_string(),
        scorecard_key.to_string(),
        "--namespace".to_string(),
        sw.namespace().unwrap_or_default(),
        "--workspace".to_string(),
        crate::workspace::label_of(sw)
            .unwrap_or_default()
            .to_string(),
        "--name".to_string(),
        sw.name_any(),
        "--uid".to_string(),
        sw.uid().unwrap_or_default(),
    ];
    if v2 {
        argv.extend([
            "--approval-policy".to_string(),
            at(POLICY_FILE),
            "--confirmation-key".to_string(),
            at(CONFIRMATION_KEY_FILE),
        ]);
    }
    if contract {
        argv.extend([
            crate::destination::STORE_CONTRACT_VERSION_ARG.to_string(),
            crate::destination::STORE_CONTRACT_VERSION.to_string(),
        ]);
    }
    if let Some(grant) = read_grant {
        argv.extend([READ_GRANT_ARG.to_string(), grant.to_string()]);
    }
    argv
}

/// The runner flag the evidence's read grant is named with
/// (`logweir::evidence_read::READ_GRANT_ARG`, which this crate does not
/// link).
pub const READ_GRANT_ARG: &str = "--evidence-read-grant";

/// The Job, pure.
#[must_use]
pub fn job_spec(sw: &Switchover, access: Access, scorecard_key: &str, v2: bool) -> RunnerJobSpec {
    let mut config_map_mounts = vec![ConfigMapMount {
        volume: job::APPROVAL_VOLUME.to_string(),
        config_map_name: bundle_name(&sw.name_any()),
        mount_path: job::APPROVAL_MOUNT_PATH.to_string(),
        items: {
            let mut items: Vec<(String, String)> = [
                PLAN_FILE,
                RESTORE_PLAN_FILE,
                APPROVAL_FILE,
                APPROVAL_SIG_FILE,
                APPROVER_KEY_FILE,
                EVIDENCE_KEYS_FILE,
            ]
            .iter()
            .map(|f| (f.to_string(), f.to_string()))
            .collect();
            if v2 {
                for f in [POLICY_FILE, CONFIRMATION_KEY_FILE] {
                    items.push((f.to_string(), f.to_string()));
                }
            }
            items
        },
    }];
    config_map_mounts.extend(access.config_map_mounts);
    RunnerJobSpec {
        name: sw.name_any(),
        namespace: sw.namespace().unwrap_or_default(),
        owner: RunnerOwner {
            api_version: Switchover::api_version(&()).to_string(),
            kind: Switchover::kind(&()).to_string(),
            name: sw.name_any(),
            uid: sw.uid().unwrap_or_default(),
            workspace: crate::workspace::label_of(sw).map(str::to_string),
        },
        args: runner_argv(
            sw,
            scorecard_key,
            v2,
            access.store_contract,
            access.evidence_read_grant.as_deref(),
        ),
        deadline_seconds: sw.spec.deadline_seconds,
        service_account_name: access.service_account_name,
        secret_mounts: access.secret_mounts,
        config_map_mounts,
        env_from_secret: access.env_from_secret,
        env_literal: access.env_literal,
        plan_config_map: access.plan_config_map,
        image: None,
        image_pull_policy: None,
        resources: None,
    }
}

/// The status a finished run's exit code and log tail give, pure.
#[must_use]
pub fn status_from_run(exit_code: i32, log: &str, job_name: &str) -> SwitchoverStatus {
    let lines: Vec<&str> = log.lines().map(|l| l.trim_end_matches('\r')).collect();
    let last = |key: &str| {
        lines
            .iter()
            .rev()
            .find_map(|l| l.strip_prefix(key))
            .map(str::to_string)
    };
    let group_lines: Vec<&str> = lines
        .iter()
        .filter_map(|l| l.strip_prefix(crate_line::GROUP))
        .collect();
    let parsed: Vec<serde_json::Value> = group_lines
        .iter()
        .filter_map(|j| serde_json::from_str::<serde_json::Value>(j).ok())
        .collect();
    // FX-38: a group line not shown is said, so the list never reads as
    // every group.
    let unreadable = group_lines.len() - parsed.len();
    let past_bound = parsed
        .len()
        .saturating_sub(crate::crds::switchover::MAX_STATUS_GROUPS);
    let groups: Vec<SwitchoverGroupStatus> = parsed
        .into_iter()
        .take(crate::crds::switchover::MAX_STATUS_GROUPS)
        .map(|v| SwitchoverGroupStatus {
            source: v["source"].as_str().unwrap_or_default().to_string(),
            target: v["target"].as_str().unwrap_or_default().to_string(),
            outcome: v["outcome"].as_str().unwrap_or_default().to_string(),
            reason: v["reason"].as_str().map(str::to_string),
            created: v["created"].as_bool().unwrap_or(false),
            partitions: v["partitions"]
                .as_array()
                .map_or(0, |a| u32::try_from(a.len()).unwrap_or(u32::MAX)),
        })
        .collect();
    let mut counts = SwitchoverCounts::default();
    for g in &groups {
        match g.outcome.as_str() {
            so::APPLIED => counts.applied += 1,
            so::ALREADY_APPLIED => counts.already_applied += 1,
            so::REFUSED => counts.refused += 1,
            so::FAILED => counts.failed += 1,
            _ => counts.untouched += 1,
        }
    }
    let outcome = last(crate_line::OUTCOME);
    let reason_word = last("refusal-reason=").or_else(|| last("failure-reason="));
    let phase = match (exit_code, outcome.as_deref()) {
        (0, _) => so::SUCCEEDED,
        (2, Some(so::PARTIALLY_APPLIED)) => so::PARTIALLY_APPLIED,
        (2, Some(so::RUN_FAILED)) => so::RUN_FAILED,
        (2, _) => so::RUN_REFUSED,
        (3, _) => so::RUN_REFUSED,
        _ => so::RUN_FAILED,
    };
    // The runner's own sentence for a whole-run refusal or failure (D1:
    // `EvidenceUnreadable` names the key and the grant), cleaned and
    // bounded as every relayed message is.
    let mut message = match last(crate_line::DETAIL) {
        Some(detail) => format!(
            "the switchover Job exited {exit_code}: {phase}: {}",
            logweir_core::refusal_detail::clean_message(&detail)
        ),
        None => format!("the switchover Job exited {exit_code}: {phase}"),
    };
    if unreadable + past_bound > 0 {
        message.push_str(&format!(
            "; {} group line(s) are not listed here ({unreadable} unreadable, {past_bound} past \
             the {}-group bound), so the groups and counts are partial: the signed switchover \
             record names every group",
            unreadable + past_bound,
            crate::crds::switchover::MAX_STATUS_GROUPS
        ));
    }
    SwitchoverStatus {
        phase: Some(phase.to_string()),
        exit_code: Some(exit_code),
        exit_reason: reason_word.clone(),
        reason: Some(reason_word.unwrap_or_else(|| phase.to_string())),
        message: Some(message),
        job_ref: Some(job_name.to_string()),
        groups,
        counts: Some(counts),
        evidence: last(crate_line::RECORD).map(|k| SwitchoverEvidence {
            record_key: Some(k),
        }),
        conditions: None,
    }
}

/// The status of a run whose log tail was past the bound: the phase its exit
/// code gives, and no group and no count, because none was read (FX-38): zero
/// counts would read as "nothing was applied".
#[must_use]
pub fn status_from_unread_log(exit_code: i32, job_name: &str) -> SwitchoverStatus {
    let mut status = status_from_run(exit_code, "", job_name);
    status.counts = None;
    status.message = Some(format!(
        "{}; its log tail was past the {}-byte bound and was not read, so which groups were \
         applied is not known here: the signed switchover record names them",
        status.message.unwrap_or_default(),
        crate::runner_log::TERMINAL_LOG_LIMIT_BYTES
    ));
    status
}

/// The runner's line keys (`logweir::switchover`'s, which this crate does
/// not link).
mod crate_line {
    pub const GROUP: &str = "switchover-group=";
    pub const OUTCOME: &str = "switchover-outcome=";
    pub const RECORD: &str = "switchover-record-key=";
    pub const DETAIL: &str = "switchover-detail=";
}

/// A reconcile's failure: a Kubernetes API error, retried.
pub type SwitchoverError = kube::Error;

async fn patch_status(
    api: &Api<Switchover>,
    name: &str,
    status: &SwitchoverStatus,
) -> Result<(), kube::Error> {
    api.patch_status(
        name,
        &PatchParams::default(),
        &Patch::Merge(json!({ "status": status })),
    )
    .await
    .map(|_| ())
}

fn hold_or_refuse(phase: &str, reason: &str, message: String) -> SwitchoverStatus {
    SwitchoverStatus {
        phase: Some(phase.to_string()),
        reason: Some(reason.to_string()),
        message: Some(message),
        ..SwitchoverStatus::default()
    }
}

/// One reconcile.
///
/// # Errors
///
/// A Kubernetes API failure.
/// [`reconcile`] behind the workspace gate: what the watch runs.
async fn gated(
    sw: Arc<Switchover>,
    ctx: Arc<Context>,
    policies: crate::approval_policy::PolicySource,
) -> Result<Action, SwitchoverError> {
    // PROD-19.1: the object's workspace, and every object and Secret it
    // names, before anything else; a refusal is terminal and creates nothing.
    if let Some(action) = crate::workspace::gate(&ctx.client, sw.as_ref()).await? {
        return Ok(action);
    }
    reconcile(sw, ctx, policies).await
}

pub async fn reconcile(
    sw: Arc<Switchover>,
    ctx: Arc<Context>,
    policies: crate::approval_policy::PolicySource,
) -> Result<Action, SwitchoverError> {
    let client = &ctx.client;
    let name = sw.name_any();
    let Some(namespace) = sw.namespace() else {
        return Ok(Action::await_change());
    };
    let api: Api<Switchover> = Api::namespaced(client.clone(), &namespace);
    if is_terminal(sw.status.as_ref()) {
        return Ok(Action::await_change());
    }
    let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);
    let job = jobs.get_opt(&name).await?;
    match job_step(sw.status.as_ref(), job.is_some()) {
        JobStep::Observe | JobStep::Admit => {}
        JobStep::NotAdmitted => {
            warn!(switchover = %name, "a Job named after this Switchover exists that no admission created; not adopted");
            patch_status(
                &api,
                &name,
                &hold_or_refuse(
                    PHASE_PENDING,
                    JOB_NOT_ADMITTED,
                    format!(
                        "a Job named {name} exists, and this Switchover was never admitted: this                          controller did not create it and does not adopt it, so it is not this                          run; the Switchover waits until that Job is gone"
                    ),
                ),
            )
            .await?;
            return Ok(Action::requeue(std::time::Duration::from_secs(
                HOLD_REQUEUE_SECS,
            )));
        }
        JobStep::Lost => {
            warn!(switchover = %name, "the recorded switchover Job is gone; the run ends");
            let status = SwitchoverStatus {
                phase: Some(so::RUN_FAILED.into()),
                reason: Some(JOB_LOST.into()),
                message: Some(format!(
                    "the Job {name} this Switchover's admission created is gone before it                      finished; the run is not admitted again (its approval was used by that                      admission): which groups moved is not known here, and a new Switchover                      with a new approval applies the positions"
                )),
                job_ref: sw.status.as_ref().and_then(|s| s.job_ref.clone()),
                ..SwitchoverStatus::default()
            };
            patch_status(&api, &name, &status).await?;
            return Ok(Action::await_change());
        }
    }
    // The run's Job: observe it, when it is this Switchover's own.
    if let Some(job) = job {
        if !own_job(&job, &sw) {
            warn!(switchover = %name, "a Job named after this Switchover is not controlled by its UID; not adopted");
            patch_status(
                &api,
                &name,
                &hold_or_refuse(
                    so::RUN_REFUSED,
                    crate::conditions::TERMINAL_STATE_JOB_NAME_CONFLICT,
                    format!(
                        "the Job {name} is not controlled by this Switchover UID, so it is not \
                         this run and was not adopted"
                    ),
                ),
            )
            .await?;
            return Ok(Action::await_change());
        }
        let st = job.status.clone().unwrap_or_default();
        if st.succeeded.unwrap_or(0) == 0 && st.failed.unwrap_or(0) == 0 {
            let mut status = sw.status.clone().unwrap_or_default();
            if status.phase.as_deref() != Some(PHASE_RUNNING) || status.job_ref.is_none() {
                status.phase = Some(PHASE_RUNNING.into());
                status.job_ref = Some(name.clone());
                patch_status(&api, &name, &status).await?;
            }
            return Ok(Action::requeue(std::time::Duration::from_secs(
                HOLD_REQUEUE_SECS,
            )));
        }
        let found = crate::check::pod::find_owned_pod_by_selectors(
            client,
            &namespace,
            &name,
            job.uid().as_deref(),
            &super::backup::pod_selectors(&name),
        )
        .await?;
        let terminated = found
            .pod
            .as_ref()
            .and_then(|p| super::backup::terminated_exit_code(p).map(|c| (p, c)));
        let status = match terminated {
            Some((pod, code)) => {
                let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
                // FX-43: the run's lines are read only through its Job's
                // token, in one call; a runner older than the controller is
                // named and nothing of its log is read. A failed read is a
                // reconcile error (retried), never an empty log.
                match crate::runner_log::read_own_terminal(&pods, &pod.name_any(), &job, Some(code))
                    .await?
                {
                    crate::runner_log::Terminal::Lines(lines) => {
                        status_from_run(code, lines.text(), &name)
                    }
                    crate::runner_log::Terminal::OverBound => status_from_unread_log(code, &name),
                    crate::runner_log::Terminal::RunnerOlder => SwitchoverStatus {
                        phase: Some(so::RUN_FAILED.into()),
                        exit_code: Some(code),
                        exit_reason: Some(
                            logweir_core::runner_line::RUNNER_OLDER_THAN_CONTROLLER.into(),
                        ),
                        reason: Some(
                            logweir_core::runner_line::RUNNER_OLDER_THAN_CONTROLLER.into(),
                        ),
                        message: Some(format!(
                            "{}: {}",
                            logweir_core::runner_line::RUNNER_OLDER_THAN_CONTROLLER,
                            crate::runner_log::RUNNER_OLDER_SENTENCE
                        )),
                        job_ref: Some(name.clone()),
                        ..SwitchoverStatus::default()
                    },
                }
            }
            None => SwitchoverStatus {
                phase: Some(so::RUN_FAILED.into()),
                reason: Some(crate::conditions::TERMINAL_STATE_NO_EXIT_CODE.into()),
                message: Some("the Job finished and no exit code could be read".into()),
                job_ref: Some(name.clone()),
                ..SwitchoverStatus::default()
            },
        };
        info!(switchover = %name, phase = ?status.phase, "switchover finished");
        patch_status(&api, &name, &status).await?;
        return Ok(Action::await_change());
    }

    // Admission.
    let restores: Api<Restore> = Api::namespaced(client.clone(), &namespace);
    let restore = restores.get_opt(&sw.spec.restore_ref.name).await?;
    let others = api.list(&Default::default()).await?.items;
    let approvals: Api<Approval> = Api::namespaced(client.clone(), &namespace);
    let approval = approvals.get_opt(&sw.spec.approval_ref.name).await?;
    match admit(&sw, restore.as_ref(), &others, approval.as_ref()) {
        Admission::Hold(reason, message) => {
            patch_status(&api, &name, &hold_or_refuse(PHASE_PENDING, reason, message)).await?;
            return Ok(Action::requeue(std::time::Duration::from_secs(
                HOLD_REQUEUE_SECS,
            )));
        }
        Admission::Refuse(reason, message) => {
            warn!(switchover = %name, reason, "switchover refused: {message}");
            patch_status(
                &api,
                &name,
                &hold_or_refuse(so::RUN_REFUSED, reason, message),
            )
            .await?;
            return Ok(Action::await_change());
        }
        Admission::Admit => {}
    }
    let (Some(restore), Some(approval)) = (restore, approval) else {
        return Ok(Action::await_change());
    };
    let refuse =
        |reason: &'static str, message: String| hold_or_refuse(so::RUN_REFUSED, reason, message);
    // Materialization: the restore's plan, target and evidence access.
    let plan = match super::restore::parse_plan(&restore) {
        Ok(p) => p,
        Err(e) => {
            patch_status(
                &api,
                &name,
                &refuse(so::RESTORE_NOT_SUCCEEDED, e.to_string()),
            )
            .await?;
            return Ok(Action::await_change());
        }
    };
    let clusters: Api<KafkaCluster> = Api::namespaced(client.clone(), &namespace);
    let Some(cluster) = clusters
        .get_opt(&restore.spec.target.cluster_ref.name)
        .await?
    else {
        patch_status(
            &api,
            &name,
            &hold_or_refuse(
                PHASE_PENDING,
                "ClusterNotReachable",
                "the restore's KafkaCluster does not exist".into(),
            ),
        )
        .await?;
        return Ok(Action::requeue(std::time::Duration::from_secs(
            HOLD_REQUEUE_SECS,
        )));
    };
    let destinations = match super::restore::admit_restore_destinations(
        &restore,
        Some(&plan),
        client,
        &namespace,
        Utc::now(),
    )
    .await
    {
        Ok(super::restore::RestoreDestinationAdmission::NotRequested) => None,
        Ok(super::restore::RestoreDestinationAdmission::Resolved(pair)) => Some(pair),
        Ok(super::restore::RestoreDestinationAdmission::Holding { reason, message }) => {
            patch_status(&api, &name, &hold_or_refuse(PHASE_PENDING, reason, message)).await?;
            return Ok(Action::requeue(std::time::Duration::from_secs(
                HOLD_REQUEUE_SECS,
            )));
        }
        Err(e) => {
            patch_status(
                &api,
                &name,
                &refuse(so::RESTORE_NOT_SUCCEEDED, e.to_string()),
            )
            .await?;
            return Ok(Action::await_change());
        }
    };
    // D1: the evidence destination's read grant reads the restore's
    // evidence, its write grant writes the record.
    let evidence = match &destinations {
        None => None,
        Some(_) => {
            match super::restore::restore_clone::evidence_access(
                &restore,
                client,
                &namespace,
                Utc::now(),
            )
            .await?
            {
                Ok(evidence) => evidence,
                Err(refusal) => {
                    patch_status(
                        &api,
                        &name,
                        &refuse(refusal.reason(), refusal.message.clone()),
                    )
                    .await?;
                    return Ok(Action::await_change());
                }
            }
        }
    };
    let access = match restore_access(&restore, &plan, &cluster, evidence.as_ref()) {
        Ok(a) => a,
        Err((reason, message)) => {
            patch_status(&api, &name, &refuse(reason, message)).await?;
            return Ok(Action::await_change());
        }
    };
    let Some(scorecard_key) = restore
        .status
        .as_ref()
        .and_then(|s| s.evidence.as_ref())
        .and_then(|e| e.scorecard_key.clone())
    else {
        patch_status(
            &api,
            &name,
            &refuse(
                so::NO_POSITIONS_MAPPING,
                "the restore records no scorecard key".into(),
            ),
        )
        .await?;
        return Ok(Action::await_change());
    };
    // The trust: the approver's key, the evidence keyring, and for a v2
    // authorization the bound policy's snapshot and the console key.
    let trust = match crate::trust::resolve(client, &namespace).await? {
        crate::trust::Resolution::Trust(t) => *t,
        _ => {
            patch_status(
                &api,
                &name,
                &hold_or_refuse(
                    PHASE_PENDING,
                    crate::trust::REASON_TRUST_POLICY_CONFLICT,
                    "the namespace resolves to no single trust".into(),
                ),
            )
            .await?;
            return Ok(Action::requeue(std::time::Duration::from_secs(
                HOLD_REQUEUE_SECS,
            )));
        }
    };
    let st = approval.status.clone().unwrap_or_default();
    let Some(approver) = st.matched_key_id.as_deref().and_then(|k| trust.key(k)) else {
        patch_status(
            &api,
            &name,
            &refuse(
                HOLD_APPROVAL_NOT_VERIFIED,
                "the approval's key is not in the namespace's trust".into(),
            ),
        )
        .await?;
        return Ok(Action::await_change());
    };
    let v2 = match st.authorization.as_ref() {
        None => None,
        Some(a) => {
            // The switchover's workspace, which admission held to be listed
            // and the approval's.
            let effective = policies
                .effective()
                .resolve(crate::workspace::label_of(sw.as_ref()).unwrap_or_default());
            let (Some(bound), Some(console)) =
                (effective.bound(), trust.key(&a.confirmation_key_id))
            else {
                patch_status(
                    &api,
                    &name,
                    &refuse(
                        "PolicyMismatch",
                        "the approval's policy or console key is not the namespace's".into(),
                    ),
                )
                .await?;
                return Ok(Action::await_change());
            };
            Some((
                String::from_utf8_lossy(&bound.snapshot_bytes()).into_owned(),
                console.spki_pem.clone(),
            ))
        }
    };
    let evidence_keys_json = match super::restore::evidence_keyring_bytes(&trust) {
        Ok(k) => k,
        Err(e) => {
            patch_status(
                &api,
                &name,
                &refuse(so::NO_POSITIONS_MAPPING, e.to_string()),
            )
            .await?;
            return Ok(Action::await_change());
        }
    };
    let bundle = Bundle {
        approver_key_pem: approver.spki_pem.clone(),
        evidence_keys_json,
        v2,
    };
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
    // M1: a bundle already there is used only when it is this admission's own.
    if let Some(message) =
        ensure_bundle(&maps, &bundle_config_map(&sw, &restore, &approval, &bundle)).await?
    {
        warn!(switchover = %name, "switchover bundle conflict: {message}");
        patch_status(
            &api,
            &name,
            &refuse(
                crate::conditions::TERMINAL_STATE_APPROVAL_BUNDLE_CONFLICT,
                message,
            ),
        )
        .await?;
        return Ok(Action::await_change());
    }
    // L6: the admission marker, before the Job. Its time is the instant the
    // approval controller records the Approval consumed at.
    patch_status(&api, &name, &admitted_status(&sw, Utc::now())).await?;
    let mut spec = job_spec(&sw, access, &scorecard_key, bundle.v2.is_some());
    spec.image = ctx.runner_image.image.clone();
    spec.image_pull_policy = ctx.runner_image.image_pull_policy.clone();
    match jobs
        // FX-43: a fresh line token; only the lines sealed with it are read.
        .create(&PostParams::default(), &job::tokened(job::build(&spec)))
        .await
    {
        Ok(_) => {}
        // L10: a Job created under this name since the read at the top of this
        // pass is used only when it is this Switchover's own (the pass that
        // stopped after creating it); a stranger's is refused, never recorded.
        Err(kube::Error::Api(e)) if e.code == 409 => match jobs.get_opt(&name).await? {
            Some(raced) if own_job(&raced, &sw) => {}
            Some(_) => {
                warn!(switchover = %name, "a concurrent Job under this name is not this Switchover's");
                patch_status(
                    &api,
                    &name,
                    &refuse(
                        crate::conditions::TERMINAL_STATE_JOB_NAME_CONFLICT,
                        format!(
                            "the Job {name} won a concurrent create but is not controlled by \
                             this Switchover UID; it was not adopted"
                        ),
                    ),
                )
                .await?;
                return Ok(Action::await_change());
            }
            // Deleted between the 409 and the read: the next pass creates it.
            None => {
                return Ok(Action::requeue(std::time::Duration::from_secs(
                    HOLD_REQUEUE_SECS,
                )))
            }
        },
        Err(e) => return Err(e),
    }
    let status = SwitchoverStatus {
        phase: Some(PHASE_RUNNING.into()),
        reason: Some("JobCreated".into()),
        job_ref: Some(name.clone()),
        ..SwitchoverStatus::default()
    };
    patch_status(&api, &name, &status).await?;
    info!(switchover = %name, restore = %restore.name_any(), "switchover Job created");
    Ok(Action::requeue(std::time::Duration::from_secs(
        HOLD_REQUEUE_SECS,
    )))
}

fn error_policy(_: Arc<Switchover>, err: &SwitchoverError, _: Arc<Context>) -> Action {
    warn!(error = %err, "switchover reconcile failed; retrying");
    Action::requeue(std::time::Duration::from_secs(HOLD_REQUEUE_SECS))
}

/// The `Switchover` controller, one watch per watched namespace.
pub async fn controller(
    client: kube::Client,
    runner_image: job::RunnerImage,
    policies: crate::approval_policy::PolicySource,
) {
    let api: Api<Switchover> = Api::namespaced(client.clone(), crate::workspace::namespace());
    let jobs: Api<Job> = Api::namespaced(client.clone(), crate::workspace::namespace());
    let ctx = Arc::new(Context {
        client,
        archive: None,
        runner_image,
    });
    Controller::new(api, watcher::Config::default())
        .owns(jobs, watcher::Config::default())
        .run(
            move |object, context| gated(object, context, policies.clone()),
            error_policy,
            ctx,
        )
        .for_each(|_| std::future::ready(()))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_core::switchover::{Expected, PlanGroup, PlanTopic, RestoreRef};

    fn sha(c: char) -> String {
        format!("sha256:{}", c.to_string().repeat(64))
    }

    fn plan_bytes(restore: &str, uid: &str) -> String {
        let plan = SwitchoverPlan {
            format_version: so::PLAN_FORMAT_VERSION.into(),
            restore: RestoreRef {
                name: restore.into(),
                uid: uid.into(),
                plan_hash: sha('a'),
                scorecard_sha256: sha('b'),
                mapping_sha256: sha('c'),
            },
            rollback_of: None,
            groups: vec![PlanGroup {
                source: "app".into(),
                target: "app".into(),
                topics: [(
                    "r-orders".to_string(),
                    PlanTopic {
                        offsets: [(0, 40)].into(),
                        approximate: vec![],
                        expected: Expected::none(),
                    },
                )]
                .into(),
            }],
        };
        serde_yaml::to_string(&plan).unwrap()
    }

    fn switchover(name: &str, uid: &str, plan: &str) -> Switchover {
        serde_json::from_value(json!({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "Switchover",
            "metadata": {"labels": {"logweir.dev/workspace": "dev"}, "name": name, "namespace": "ns", "uid": uid,
                         "creationTimestamp": "2026-10-13T08:00:00Z"},
            "spec": {"planBytes": plan, "approvalRef": {"name": "ok"},
                     "restoreRef": {"name": "incident", "uid": "r-uid"}, "deadlineSeconds": 600}
        }))
        .unwrap()
    }

    fn restore(mode: &str, phase: &str, verdict: &str) -> Restore {
        serde_json::from_value(json!({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "Restore",
            "metadata": {"labels": {"logweir.dev/workspace": "dev"}, "name": "incident", "namespace": "ns", "uid": "r-uid"},
            "spec": {"planBytes": "x", "approvalRef": {"name": "a"},
                     "sourceArchive": {"url": "s3://b/p"}, "backupSetRef": "b1",
                     "pointInTime": "2026-10-13T08:00:00Z",
                     "target": {"clusterRef": {"name": "c"}, "mode": mode,
                                "topicNaming": {"prefix": "r-"}},
                     "deadlineSeconds": 600},
            "status": {"phase": phase,
                       "evidence": {"scorecardKey": "logweir/drills/R.json",
                                    "verification": {"result": verdict}}}
        }))
        .unwrap()
    }

    fn approval(kind: &str, verified: bool, subject_uid: &str, plan: &str) -> Approval {
        serde_json::from_value(json!({
            "apiVersion": "logweir.dev/v1alpha1", "kind": "Approval",
            "metadata": {"labels": {"logweir.dev/workspace": "dev"}, "name": "ok", "namespace": "ns"},
            "spec": {"subjectRef": {"kind": kind, "name": "sw"},
                     "planHash": logweir_core::ids::sha256_prefixed(plan.as_bytes()),
                     "approvalBytes": "{}", "sidecarBytes": "{}"},
            "status": {"verified": verified,
                       "verifiedSubjectRef": {"apiVersion": "logweir.dev/v1alpha1", "kind": kind,
                                              "name": "sw", "namespace": "ns", "uid": subject_uid}}
        }))
        .unwrap()
    }

    fn reason(a: &Admission) -> &str {
        match a {
            Admission::Admit => "Admit",
            Admission::Hold(r, _) | Admission::Refuse(r, _) => r,
        }
    }

    /// Step 1 to 4's rows. Mutants killed: an approval for the Restore
    /// accepted (C17's controller half), a scratch restore accepted, a
    /// second switchover admitted beside a running one, a plan naming
    /// another restore accepted.
    #[test]
    fn admission_refuses_by_name() {
        crate::testing::install_test_workspaces();
        let plan = plan_bytes("incident", "r-uid");
        let sw = switchover("sw", "s-uid", &plan);
        let ok_restore = restore("newTopic", "Succeeded", "Valid");
        let ok_approval = approval("Switchover", true, "s-uid", &plan);
        assert_eq!(
            admit(&sw, Some(&ok_restore), &[], Some(&ok_approval)),
            Admission::Admit
        );
        // PROD-19.1: an approval of another workspace, a restore of another
        // workspace, and an unlabelled switchover are each refused by reason;
        // relabelling the Approval does not carry `dev`'s approval to `prd`.
        let relabel = |labels: &mut Option<std::collections::BTreeMap<String, String>>,
                       to: Option<&str>| {
            *labels = to.map(|w| [(crate::workspace::LABEL.to_string(), w.to_string())].into());
        };
        let mut prd_approval = ok_approval.clone();
        relabel(&mut prd_approval.metadata.labels, Some("prd"));
        assert_eq!(
            reason(&admit(&sw, Some(&ok_restore), &[], Some(&prd_approval))),
            crate::workspace::REASON_MISMATCH
        );
        let mut prd_restore = ok_restore.clone();
        relabel(&mut prd_restore.metadata.labels, Some("prd"));
        assert_eq!(
            reason(&admit(&sw, Some(&prd_restore), &[], Some(&ok_approval))),
            crate::workspace::REASON_MISMATCH
        );
        let mut unlabelled = sw.clone();
        relabel(&mut unlabelled.metadata.labels, None);
        assert_eq!(
            reason(&admit(
                &unlabelled,
                Some(&ok_restore),
                &[],
                Some(&ok_approval)
            )),
            crate::workspace::REASON_UNKNOWN
        );
        let other_plan = switchover("sw", "s-uid", &plan_bytes("other", "r-uid"));
        assert_eq!(
            reason(&admit(
                &other_plan,
                Some(&ok_restore),
                &[],
                Some(&ok_approval)
            )),
            so::PLAN_INVALID
        );
        assert_eq!(
            reason(&admit(&sw, None, &[], Some(&ok_approval))),
            so::RESTORE_NOT_SUCCEEDED
        );
        assert_eq!(
            reason(&admit(
                &sw,
                Some(&restore("scratch", "Succeeded", "Valid")),
                &[],
                Some(&ok_approval)
            )),
            so::RESTORE_IS_SCRATCH
        );
        assert_eq!(
            reason(&admit(
                &sw,
                Some(&restore("newTopic", "Failed", "Valid")),
                &[],
                Some(&ok_approval)
            )),
            so::RESTORE_NOT_SUCCEEDED
        );
        assert_eq!(
            reason(&admit(
                &sw,
                Some(&restore("newTopic", "Succeeded", "Invalid")),
                &[],
                Some(&ok_approval)
            )),
            so::RESTORE_NOT_SUCCEEDED
        );
        assert_eq!(
            reason(&admit(
                &sw,
                Some(&restore("newTopic", "Running", "")),
                &[],
                Some(&ok_approval)
            )),
            HOLD_RESTORE_RUNNING
        );
        let mut running = switchover("first", "f-uid", &plan);
        running.status = Some(SwitchoverStatus {
            phase: Some(PHASE_RUNNING.into()),
            job_ref: Some("first".into()),
            ..SwitchoverStatus::default()
        });
        assert_eq!(
            reason(&admit(
                &sw,
                Some(&ok_restore),
                &[running.clone()],
                Some(&ok_approval)
            )),
            so::SWITCHOVER_IN_PROGRESS
        );
        running.status.as_mut().unwrap().phase = Some(so::SUCCEEDED.into());
        assert_eq!(
            admit(&sw, Some(&ok_restore), &[running], Some(&ok_approval)),
            Admission::Admit,
            "a finished switchover does not block the next"
        );
        assert_eq!(
            reason(&admit(
                &sw,
                Some(&ok_restore),
                &[],
                Some(&approval("Restore", true, "r-uid", &plan))
            )),
            SUBJECT_KIND_MISMATCH
        );
        assert_eq!(
            reason(&admit(
                &sw,
                Some(&ok_restore),
                &[],
                Some(&approval("Switchover", true, "other", &plan))
            )),
            SUBJECT_KIND_MISMATCH
        );
        assert_eq!(
            reason(&admit(
                &sw,
                Some(&ok_restore),
                &[],
                Some(&approval("Switchover", false, "s-uid", &plan))
            )),
            HOLD_APPROVAL_NOT_VERIFIED
        );
        assert_eq!(
            reason(&admit(&sw, Some(&ok_restore), &[], None)),
            HOLD_APPROVAL_NOT_VERIFIED
        );
    }

    #[test]
    fn the_status_is_lifted_from_the_runners_lines() {
        let log = "switchover-run-id=X\n\
             {\"level\":\"INFO\"}\n\
             switchover-group={\"source\":\"a\",\"target\":\"a\",\"outcome\":\"applied\",\"reason\":null,\"created\":true,\"committed\":true,\"partitions\":[{},{}]}\n\
             switchover-group={\"source\":\"b\",\"target\":\"b\",\"outcome\":\"refused\",\"reason\":\"GroupActive\",\"created\":false,\"committed\":false,\"partitions\":[{}]}\n\
             switchover-record-key=logweir/switchovers/X.record.json\n\
             switchover-outcome=PartiallyApplied\n\
             switchover-counts=applied=1 alreadyApplied=0 refused=1 failed=0 untouched=0\n";
        let st = status_from_run(2, log, "sw");
        assert_eq!(st.phase.as_deref(), Some(so::PARTIALLY_APPLIED));
        assert_eq!(st.groups.len(), 2);
        assert_eq!(st.groups[0].partitions, 2);
        assert!(st.groups[0].created);
        assert_eq!(st.groups[1].reason.as_deref(), Some("GroupActive"));
        let c = st.counts.unwrap();
        assert_eq!((c.applied, c.refused), (1, 1));
        assert_eq!(
            st.evidence.and_then(|e| e.record_key).as_deref(),
            Some("logweir/switchovers/X.record.json")
        );
        let refused = status_from_run(
            3,
            "switchover-outcome=Refused\nrefusal-reason=TargetGenerationChanged\n",
            "sw",
        );
        assert_eq!(refused.phase.as_deref(), Some(so::RUN_REFUSED));
        assert_eq!(
            refused.exit_reason.as_deref(),
            Some("TargetGenerationChanged")
        );
        assert_eq!(
            status_from_run(0, "", "sw").phase.as_deref(),
            Some(so::SUCCEEDED)
        );
    }

    /// FX-38: a group line the controller cannot read, and a log it did not
    /// read at all, never read as every group. CONTROL: the lines above,
    /// whole, say nothing of the kind.
    #[test]
    fn a_partial_or_unread_log_says_its_groups_are_partial() {
        let log = "switchover-group={\"source\":\"a\",\"target\":\"a\",\"outcome\":\"applied\",\"partitions\":[{}]}\n\
             switchover-group={\"source\":\"b\"\n\
             switchover-outcome=PartiallyApplied\n";
        let st = status_from_run(2, log, "sw");
        assert_eq!(st.groups.len(), 1);
        let message = st.message.unwrap_or_default();
        assert!(
            message.contains("1 group line(s) are not listed here (1 unreadable, 0 past"),
            "{message}"
        );
        let unread = status_from_unread_log(0, "sw");
        assert_eq!(unread.phase.as_deref(), Some(so::SUCCEEDED));
        assert!(unread.counts.is_none(), "no zero counts for a log not read");
        assert!(unread.message.unwrap_or_default().contains("was not read"));
        // CONTROL.
        let whole = status_from_run(
            2,
            &log.replace("switchover-group={\"source\":\"b\"\n", ""),
            "sw",
        );
        assert!(!whole.message.unwrap_or_default().contains("not listed"));
        assert!(whole.counts.is_some());
    }

    /// **D1: a store that refused the evidence is `Failed`, named, and the
    /// status says which grant could not read what.** The runner's exit 1,
    /// `failure-reason=EvidenceUnreadable` and its `switchover-detail=`
    /// sentence reach `reason`, `exitReason` and `message`. CONTROL: without
    /// a detail line the message is the exit and the phase, as before.
    ///
    /// KILLS: the detail line not read (the console shows only "exited 1").
    #[test]
    fn an_unreadable_evidence_is_failed_and_names_the_grant() {
        let log = "switchover-run-id=X\n\
             switchover-group={\"source\":\"a\",\"target\":\"a\",\"outcome\":\"untouched\",\"reason\":null,\"created\":false,\"committed\":false,\"partitions\":[]}\n\
             switchover-record-key=logweir/switchovers/X.record.json\n\
             switchover-outcome=Failed\n\
             switchover-counts=applied=0 alreadyApplied=0 refused=0 failed=0 untouched=1\n\
             switchover-detail=EvidenceUnreadable: could not read logweir/drills/R.json with grant evidenceRead of BackupDestination ns/d: AccessDenied; the store refused the read, so the evidence was not judged\n\
             failure-reason=EvidenceUnreadable\n";
        let st = status_from_run(1, log, "sw");
        assert_eq!(st.phase.as_deref(), Some(so::RUN_FAILED));
        assert_eq!(st.reason.as_deref(), Some("EvidenceUnreadable"));
        assert_eq!(st.exit_reason.as_deref(), Some("EvidenceUnreadable"));
        let message = st.message.unwrap_or_default();
        assert!(
            message.starts_with(
                "the switchover Job exited 1: Failed: EvidenceUnreadable: could not read \
                 logweir/drills/R.json with grant evidenceRead of BackupDestination ns/d: \
                 AccessDenied"
            ),
            "{message}"
        );
        assert_eq!(
            status_from_run(1, "failure-reason=Operational\n", "sw")
                .message
                .as_deref(),
            Some("the switchover Job exited 1: Failed")
        );
    }

    #[test]
    fn the_job_mounts_the_bundle_and_runs_the_switchover() {
        let plan = plan_bytes("incident", "r-uid");
        let sw = switchover("sw", "s-uid", &plan);
        let spec = job_spec(&sw, Access::default(), "logweir/drills/R.json", true);
        assert_eq!(&spec.args[..2], ["switchover", "run"]);
        assert!(spec.args.contains(&"--approval-policy".to_string()));
        assert_eq!(spec.config_map_mounts[0].config_map_name, "sw-switchover");
        assert_eq!(spec.owner.kind, "Switchover");
        assert!(spec.plan_config_map.is_none());
    }
}
