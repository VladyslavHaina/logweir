//! Run identity: one module that decides what a `Backup` IS, from the object
//! alone.
//!
//! # Why this is one function and not a rule per call site
//!
//! Three things have to agree about a run: its `metadata.name`, its execution
//! id (the archive's `backup_id`) and which `BackupSchedule` it belongs to.
//! Before retries and catch-up there was one derivation and it lived where it
//! was used. With four trigger kinds there are four, and a second
//! implementation anywhere is how a retry comes to be named `-r1` in the
//! scheduler and adopted as attempt 0 by the controller that reconciles it.
//!
//! [`run_identity`] is that one derivation. It reads the object and nothing
//! else: **no clock, no status field, no annotation** (D1 §3.1 rule 7). The
//! scheduler mints names from observed deterministic objects; this function
//! only ever CHECKS what an object claims against what its own fields imply.
//!
//! # What it refuses, and why each refusal is terminal
//!
//! A scheduled-kind `Backup` whose name is not the name its
//! `(scheduleRef, slot, attempt)` implies is `ScheduledIdentityMismatch` — it
//! is never quietly re-read as a manual run, because a manual run executes
//! under its own UID and would write a second archive of a window a scheduled
//! run already owns. `Backup.spec` is CEL-immutable, so none of these can be
//! fixed in place and all of them are terminal.

use crate::crds::backup::{Backup, ScheduleRef, Trigger, TriggerKind};
use crate::slot::{
    backup_id_for_attempt, scheduled_backup_name_for_attempt, MAX_RETRIES, SLOT_NAME_LEN,
};
use kube::ResourceExt;

/// The label carrying the schedule's name. A HINT, never authority.
pub const SCHEDULE_LABEL: &str = "logweir.dev/schedule";
/// The label carrying the schedule's UID.
pub const SCHEDULE_UID_LABEL: &str = "logweir.dev/schedule-uid";
/// The label carrying the slot.
pub const SLOT_LABEL: &str = "logweir.dev/slot";
/// The label carrying the trigger kind, lowercase-hyphenated.
pub const TRIGGER_LABEL: &str = "logweir.dev/trigger";
/// The label carrying the attempt number.
pub const ATTEMPT_LABEL: &str = "logweir.dev/attempt";

/// The annotation the PLAT-05.2 history migration writes onto a `Backup` whose
/// controller ownerReference it removed.
///
/// WITHOUT IT, DETACHING HISTORY WOULD ORPHAN IT. Membership used to be read
/// from the controller ownerReference alone; the migration removes that
/// reference so deleting a schedule stops deleting its history, and this
/// annotation is what still ties the run to the UID it was created under.
pub const RETAINED_FROM_OWNER_ANNOTATION: &str = "logweir.dev/history-retained-from-owner";

/// `spec.triggeredBy` for a scheduled run.
pub const TRIGGERED_BY_SCHEDULE: &str = "schedule";
/// `spec.triggeredBy` for a manual run.
pub const TRIGGERED_BY_MANUAL: &str = "manual";

/// What a run is, derived from the object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunIdentity {
    /// Which kind of run.
    pub kind: TriggerKind,
    /// `0` for everything but a retry.
    pub attempt: u32,
    /// The archive's `backup_id`.
    pub execution_id: String,
    /// The schedule this run belongs to, for the scheduled kinds.
    pub schedule: Option<ScheduleIdentity>,
    /// The slot, for the scheduled kinds.
    pub slot: Option<String>,
    /// The zone the slot was computed in. Informational.
    pub time_zone: Option<String>,
}

/// The schedule half of a scheduled run's identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleIdentity {
    /// The `BackupSchedule` name.
    pub name: String,
    /// Its UID — from `spec.scheduleRef.uid`, or from the legacy controller
    /// ownerReference.
    pub uid: String,
    /// Whether the UID came from the ownerReference rather than the spec.
    pub from_owner_reference: bool,
}

/// Why a `Backup` has no identity. Every one of these is terminal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityError {
    /// The object's own fields do not compose the identity it claims: a name
    /// that is not `name(schedule, slot, attempt)`, a retry with `attempt` 0,
    /// a `Scheduled` with an attempt, a `Manual` carrying a slot, a
    /// `retryOf` that does not name the previous attempt, or a scheduled kind
    /// with no UID anywhere.
    ScheduledIdentityMismatch {
        /// What is wrong, in one sentence naming the fields.
        detail: String,
    },
    /// A scheduled kind whose `scheduleRef` names nothing this cluster has.
    /// Raised by the CALLER after it looks the schedule up; carried here so
    /// the vocabulary lives in one enum.
    ScheduleNotFound {
        /// The name that resolved to nothing.
        name: String,
    },
    /// The copied policy digest does not equal the one recomputed from this
    /// object's own fields.
    RunPolicyDigestMismatch {
        /// What `spec.scheduleRef.runPolicySha256` says.
        got: String,
        /// What the object's fields produce.
        want: String,
    },
    /// The composed name does not fit `metadata.name`'s 63 characters.
    NameTooLong {
        /// The length it would have had.
        got: usize,
    },
}

impl IdentityError {
    /// The terminal state this refusal is recorded as.
    #[must_use]
    pub fn terminal_state(&self) -> &'static str {
        match self {
            Self::ScheduledIdentityMismatch { .. } => {
                crate::conditions::TERMINAL_STATE_SCHEDULED_IDENTITY_MISMATCH
            }
            Self::ScheduleNotFound { .. } => crate::conditions::TERMINAL_STATE_SCHEDULE_NOT_FOUND,
            Self::RunPolicyDigestMismatch { .. } => {
                crate::conditions::TERMINAL_STATE_RUN_POLICY_DIGEST_MISMATCH
            }
            Self::NameTooLong { .. } => crate::conditions::TERMINAL_STATE_NAME_TOO_LONG,
        }
    }
}

impl std::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ScheduledIdentityMismatch { detail } => write!(f, "{detail}"),
            Self::ScheduleNotFound { name } => write!(
                f,
                "spec.scheduleRef names `{name}`, which does not exist in this namespace; \
                 deleting a schedule stops future work"
            ),
            Self::RunPolicyDigestMismatch { got, want } => write!(
                f,
                "spec.scheduleRef.runPolicySha256 is {got} but this Backup's own policy fields \
                 digest to {want}"
            ),
            Self::NameTooLong { got } => write!(
                f,
                "the composed run name is {got} characters; Kubernetes object names are at most \
                 {} ",
                crate::slot::NAME_LIMIT
            ),
        }
    }
}

fn mismatch(detail: impl Into<String>) -> IdentityError {
    IdentityError::ScheduledIdentityMismatch {
        detail: detail.into(),
    }
}

/// The trigger this object declares, or the one D1 §3.1 rule 4 reads from a
/// `Backup` created before `spec.trigger` existed.
///
/// RULE 4 IS THE WHOLE UPGRADE STORY. Every stored `Backup` has no `trigger`,
/// and reading `triggeredBy: schedule` as `Scheduled`/0 and anything else as
/// `Manual` is exactly what the controller that created them did. No object is
/// converted and no write happens on upgrade.
#[must_use]
pub fn declared_trigger(backup: &Backup) -> (TriggerKind, u32, Option<&LocalRefLike>) {
    match backup.spec.trigger.as_ref() {
        Some(Trigger {
            kind,
            attempt,
            retry_of,
            ..
        }) => (
            *kind,
            u32::try_from(*attempt).unwrap_or(u32::MAX),
            retry_of.as_ref(),
        ),
        None if backup.spec.triggered_by == TRIGGERED_BY_SCHEDULE => {
            (TriggerKind::Scheduled, 0, None)
        }
        None => (TriggerKind::Manual, 0, None),
    }
}

/// The reference type `spec.trigger.retryOf` carries.
pub type LocalRefLike = crate::crds::LocalRef;

/// Derive this `Backup`'s identity from the object alone.
///
/// # Errors
///
/// [`IdentityError::ScheduledIdentityMismatch`] when the object's fields do
/// not compose the identity it claims, and [`IdentityError::NameTooLong`] when
/// the composed name does not fit. `ScheduleNotFound` and
/// `RunPolicyDigestMismatch` are the caller's to raise — the first needs a
/// cluster read and the second needs [`crate::policy::run_policy_sha256`],
/// which [`check_run_policy_digest`] performs.
pub fn run_identity(backup: &Backup) -> Result<RunIdentity, IdentityError> {
    let name = backup.name_any();
    let uid = backup.uid().unwrap_or_default();
    let (kind, attempt, retry_of) = declared_trigger(backup);
    let slot = backup.spec.slot.clone();
    let time_zone = backup
        .spec
        .trigger
        .as_ref()
        .and_then(|t| t.time_zone.clone());

    // ---- rule 3, FIRST, because it is checked before rule 2 -------------
    match kind {
        TriggerKind::Manual => {
            if let Some(slot) = slot.as_deref() {
                return Err(mismatch(format!(
                    "spec.trigger.kind is Manual but spec.slot is `{slot}`; a slot names one \
                     BackupSchedule run, and a manual Backup runs under its own UID"
                )));
            }
            if attempt > 0 {
                return Err(mismatch(format!(
                    "spec.trigger.kind is Manual with attempt {attempt}; a manual run has no \
                     attempt chain to be the {attempt}th of"
                )));
            }
            if uid.is_empty() {
                return Err(mismatch(
                    "a manual Backup executes under its own metadata.uid, and this object \
                     carries none",
                ));
            }
            return Ok(RunIdentity {
                kind,
                attempt: 0,
                execution_id: uid,
                schedule: None,
                slot: None,
                time_zone,
            });
        }
        TriggerKind::Scheduled | TriggerKind::CatchUp => {
            if attempt != 0 {
                return Err(mismatch(format!(
                    "spec.trigger.kind is {kind:?} with attempt {attempt}; a slot's first run is \
                     always attempt 0 and a later attempt is kind Retry"
                )));
            }
        }
        TriggerKind::Retry => {
            if attempt == 0 {
                return Err(mismatch(
                    "spec.trigger.kind is Retry with attempt 0; a retry is attempt 1 or higher, \
                     and attempt 0 is the run being retried",
                ));
            }
            if attempt > MAX_RETRIES {
                return Err(mismatch(format!(
                    "spec.trigger.attempt is {attempt}; at most {MAX_RETRIES} retries are \
                     accepted"
                )));
            }
        }
    }

    // ---- the schedule reference ----------------------------------------
    let reference: &ScheduleRef = backup
        .spec
        .schedule_ref
        .as_ref()
        .filter(|r| !r.name.is_empty())
        .ok_or_else(|| {
            mismatch(
                "a scheduled Backup needs spec.scheduleRef; only the BackupSchedule controller \
                 creates scheduled runs, and a Backup created by hand is manual",
            )
        })?;
    let slot = slot.ok_or_else(|| {
        mismatch(
            "a scheduled Backup needs spec.slot, which is half of both its name and its \
             execution id",
        )
    })?;
    if slot.len() != SLOT_NAME_LEN || !valid_slot(&slot) {
        return Err(mismatch(format!(
            "spec.slot `{slot}` is not a UTC slot in yyyymmdd-hhmmss form"
        )));
    }

    // ---- rule 3, the retry chain ----------------------------------------
    if kind == TriggerKind::Retry {
        let previous = scheduled_backup_name_for_attempt(&reference.name, &slot, attempt - 1)
            .map_err(|_| IdentityError::NameTooLong {
                got: composed_len(&reference.name, &slot, attempt - 1),
            })?;
        match retry_of {
            Some(r) if r.name == previous => {}
            Some(r) => {
                return Err(mismatch(format!(
                    "spec.trigger.retryOf names `{}` but attempt {attempt} of this slot retries \
                     `{previous}`",
                    r.name
                )))
            }
            None => {
                return Err(mismatch(format!(
                    "spec.trigger.kind is Retry with no retryOf; attempt {attempt} of this slot \
                     retries `{previous}`"
                )))
            }
        }
    }

    // ---- rule 1, the name ------------------------------------------------
    let expected =
        scheduled_backup_name_for_attempt(&reference.name, &slot, attempt).map_err(|_| {
            IdentityError::NameTooLong {
                got: composed_len(&reference.name, &slot, attempt),
            }
        })?;
    if name != expected {
        return Err(mismatch(format!(
            "metadata.name is `{name}` but spec.scheduleRef, spec.slot and the attempt compose \
             `{expected}`; a scheduled run's name is a pure function of its trigger and is \
             never re-read as a manual run"
        )));
    }

    // ---- rule 3's last clause: a UID from somewhere ---------------------
    let (uid, from_owner_reference) = match reference.uid.as_deref().filter(|u| !u.is_empty()) {
        Some(uid) => (uid.to_string(), false),
        None => match legacy_owner_uid(backup) {
            Some(uid) => (uid, true),
            None => {
                return Err(mismatch(
                    "a scheduled Backup needs spec.scheduleRef.uid, or the complete \
                     BackupSchedule controller ownerReference an earlier controller wrote; with \
                     neither, two same-named schedules' runs would share an archive prefix",
                ))
            }
        },
    };

    Ok(RunIdentity {
        kind,
        attempt,
        execution_id: backup_id_for_attempt(&uid, &slot, attempt),
        schedule: Some(ScheduleIdentity {
            name: reference.name.clone(),
            uid,
            from_owner_reference,
        }),
        slot: Some(slot),
        time_zone,
    })
}

/// Rule 5: the copied policy digest must equal the one this object's own
/// fields produce.
///
/// AN INTEGRITY CHECK AGAINST BUGS, NOT A SECURITY BOUNDARY (D1 §8.7).
/// `Backup.spec` is CEL-immutable and the digest is recomputed from the same
/// object, so a mismatch means the control plane copied a policy and then
/// wrote different fields — a defect, and terminal because nothing can fix it
/// in place.
///
/// # Errors
///
/// [`IdentityError::RunPolicyDigestMismatch`], naming both values.
pub fn check_run_policy_digest(backup: &Backup) -> Result<(), IdentityError> {
    let Some(got) = backup
        .spec
        .schedule_ref
        .as_ref()
        .and_then(|r| r.run_policy_sha256.as_deref())
    else {
        return Ok(());
    };
    let want = crate::policy::run_policy_sha256(&backup.spec);
    if got == want {
        Ok(())
    } else {
        Err(IdentityError::RunPolicyDigestMismatch {
            got: got.to_string(),
            want,
        })
    }
}

/// Whether `backup` is a run of the `BackupSchedule` named `name` with UID
/// `uid`.
///
/// # Three ways to be a member, and why all three are needed
///
/// 1. `spec.scheduleRef.name` and `.uid` both match — every run created since
///    the reference grew a UID.
/// 2. The complete `BackupSchedule` controller ownerReference matches — every
///    run created before it did.
/// 3. The PLAT-05.2 migration annotation carries the UID and
///    `spec.scheduleRef.name` matches — a legacy run whose ownerReference was
///    REMOVED so that deleting the schedule would stop deleting its history.
///
/// Dropping (3) would make the history-detaching migration orphan exactly the
/// history it exists to keep.
#[must_use]
pub fn is_run_of_schedule(backup: &Backup, name: &str, uid: &str) -> bool {
    let reference = backup.spec.schedule_ref.as_ref();
    let named = reference.is_some_and(|r| r.name == name);
    if named && reference.and_then(|r| r.uid.as_deref()) == Some(uid) {
        return true;
    }
    if legacy_owner_uid(backup).as_deref() == Some(uid)
        && backup
            .owner_references()
            .iter()
            .any(|o| o.controller == Some(true) && o.name == name)
    {
        return true;
    }
    named
        && backup
            .annotations()
            .get(RETAINED_FROM_OWNER_ANNOTATION)
            .map(String::as_str)
            == Some(uid)
}

/// The UID of the complete `BackupSchedule` controller ownerReference, if any.
fn legacy_owner_uid(backup: &Backup) -> Option<String> {
    backup
        .owner_references()
        .iter()
        .find(|o| {
            o.controller == Some(true)
                && o.kind == "BackupSchedule"
                && o.api_version.starts_with(crate::crds::GROUP)
                && !o.uid.is_empty()
        })
        .map(|o| o.uid.clone())
}

/// The length `scheduled_backup_name_for_attempt` would have produced.
fn composed_len(schedule: &str, slot: &str, attempt: u32) -> usize {
    let suffix = if attempt == 0 {
        0
    } else {
        crate::slot::RETRY_SUFFIX_LEN
    };
    crate::slot::SCHEDULED_BACKUP_PREFIX.len() + schedule.len() + 1 + slot.len() + suffix
}

/// `yyyymmdd-hhmmss`, digits and one hyphen.
fn valid_slot(slot: &str) -> bool {
    let bytes = slot.as_bytes();
    bytes.len() == SLOT_NAME_LEN
        && bytes[8] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(i, b)| i == 8 || b.is_ascii_digit())
}
