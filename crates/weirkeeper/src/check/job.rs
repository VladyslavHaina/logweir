//! The check Job (D2 §4.3's `job.rs` row).
//!
//! One shape for every check kind, built on [`crate::job::build`] so a check
//! pod and an execution pod cannot drift apart: same container name, same
//! `restartPolicy: Never` + `backoffLimit: 0`, same
//! `automountServiceAccountToken: false`, same `podFailurePolicy`, same
//! security context, and (the ordering the framework depends on) **no
//! `ttlSecondsAfterFinished` at creation time**.

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::Job;
use logweir_core::check_contract::CheckPlanKind;

use crate::job::{self, ConfigMapMount, EnvFromSecret, RunnerJobSpec, RunnerOwner, SecretMount};

/// The prefix of every check Job's name.
pub const CHECK_JOB_PREFIX: &str = "lwc-";

/// How many hex characters of `sha256(owner uid)` a check Job's name carries.
///
/// The name is `lwc-` + a two-character discriminator + `-` + this, so always
/// 27 characters whatever the owner is called: no object can be named so that
/// its check is unschedulable (unlike the `KafkaCluster` probe's
/// [`crate::controllers::kafka_cluster::name_limit_for_cluster`]).
///
/// 80 bits: a namespace would need about 2^40 live check subjects for an even
/// chance of a collision, both wanting the same check kind at once, and the
/// plan `ConfigMap`'s owner-UID check (D2 §4.3's `plan.rs` 409 rule) turns a
/// collision into a refusal rather than a wrong answer.
pub const OWNER_UID_HEX_CHARS: usize = 20;

/// `app.kubernetes.io/managed-by` on every check Job and its pod template.
pub const LABEL_MANAGED_BY: &str = "app.kubernetes.io/managed-by";
/// The value of [`LABEL_MANAGED_BY`].
pub const MANAGED_BY: &str = "weirkeeper";
pub const LABEL_COMPONENT: &str = "app.kubernetes.io/component";
/// The value of [`LABEL_COMPONENT`], also the selector D2 §4.3's `limits.rs`
/// counts active check Jobs with.
pub const COMPONENT_CHECK: &str = "check";
/// Which plan kind this Job runs.
pub const LABEL_CHECK_KIND: &str = "logweir.dev/check-kind";
/// The UID of the custom resource this check belongs to.
///
/// **A label, never an identity**: it is for `kubectl get jobs -l` and the
/// `limits.rs` count. A pod is matched to a Job by
/// [`super::pod::find_owned_pod`]'s `ownerReferences` check (seam **S6**),
/// because a label is writable by anything that can create a pod.
pub const LABEL_CHECK_OWNER_UID: &str = "logweir.dev/check-owner-uid";

/// The UID of the `KafkaCluster` this check dials, when it dials one.
///
/// Present only so [`super::limits`] can enforce D2 §4.4's
/// `maxActiveDiscoveriesPerConnection` without reading every check's plan
/// `ConfigMap`. Absent for a kind with no connection (`destinationAccess`,
/// `evidenceFetch`), and absent counts as "no connection", never as a wildcard.
pub const LABEL_CHECK_CONNECTION_UID: &str = "logweir.dev/check-connection-uid";

/// Where the check plan `ConfigMap` is mounted.
///
/// `/check`, not [`crate::job::PLAN_MOUNT_PATH`] (D2 §4.2's argv is
/// `--plan /check/check-plan.json`): an execution pod's `/plan` carries a
/// `backup.yaml` the engine reads, a check pod's `/check` a contract document
/// whose digest is pinned in the environment, and reusing `/plan` would make
/// `RunnerJobSpec::plan_config_map` mean two things.
pub const CHECK_MOUNT_PATH: &str = "/check";
/// The volume name for [`CHECK_MOUNT_PATH`].
pub const CHECK_VOLUME: &str = "check-plan";
/// The plan document's key inside the `ConfigMap`, and its file name.
pub const CHECK_PLAN_KEY: &str = "check-plan.json";

/// `LOGWEIR_CHECK_CONTRACT_VERSION` (D2 §4.2's env).
pub const CONTRACT_VERSION_ENV: &str = "LOGWEIR_CHECK_CONTRACT_VERSION";
pub const PLAN_SHA256_ENV: &str = "LOGWEIR_CHECK_PLAN_SHA256";
pub const SUBJECT_UID_ENV: &str = "LOGWEIR_CHECK_SUBJECT_UID";

/// Seconds added to a check's own `timeoutSeconds` to get the Job's
/// `activeDeadlineSeconds` (D2 §4.3).
///
/// The margin covers image pull, scheduling and container start, which the
/// runner cannot bound, so a Job deadline that fires means the pod never got
/// going: a different fact, and code, from a check that ran out of time.
pub const DEADLINE_MARGIN_SECONDS: i64 = 90;

/// `ttlSecondsAfterFinished` for a finished check Job (D2 §4.3).
///
/// **Patched on only after the status commit**: the relay lives on the pod, and
/// the TTL controller deletes a Job and its pods together, so a TTL set at
/// creation would let garbage collection race the log read.
/// [`crate::runner_log::set_ttl`] is the one place it is written.
pub const TTL_SECONDS: i32 = 600;

/// The Job name for one check: `lwc-<k>-<first 20 hex of sha256(owner uid)>`.
///
/// A pure function of the kind and the UID (see [`OWNER_UID_HEX_CHARS`] for
/// why the owner's name is absent), so a duplicate reconcile gets **409
/// `AlreadyExists`** instead of creating a second check Job, as [`crate::slot`]
/// does for scheduled backups.
#[must_use]
pub fn check_job_name(kind: CheckPlanKind, owner_uid: &str) -> String {
    let digest = logweir_core::ids::sha256_hex(owner_uid.as_bytes());
    format!(
        "{CHECK_JOB_PREFIX}{}-{}",
        kind.job_discriminator(),
        &digest[..OWNER_UID_HEX_CHARS]
    )
}

/// The evidence-fetch Job name for one attempt:
/// `lwc-ev-<first 20 hex of sha256(owner uid + ":" + attempt)>` (D2 §3.9 step 2).
///
/// The attempt in the name is the idempotence argument: an unfinished fetch is
/// retried with a new Job (D2 §3.9 step 5), and a controller restarted between
/// creating attempt `n`'s Job and recording it recomputes this name and finds
/// that Job. Two attempts never share a Job; one attempt never has two. Same
/// length and alphabet as [`check_job_name`].
#[must_use]
pub fn evidence_fetch_job_name(owner_uid: &str, attempt: u32) -> String {
    check_job_name(
        CheckPlanKind::EvidenceFetch,
        &format!("{owner_uid}:{attempt}"),
    )
}

/// The labels on a check Job and on its pod template (the same map on both).
///
/// With no `connection_uid` the key is absent rather than empty, because an
/// empty label value is a value a selector can match.
#[must_use]
pub fn labels(
    kind: CheckPlanKind,
    owner_uid: &str,
    connection_uid: Option<&str>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::from([
        (LABEL_MANAGED_BY.to_string(), MANAGED_BY.to_string()),
        (LABEL_COMPONENT.to_string(), COMPONENT_CHECK.to_string()),
        (LABEL_CHECK_KIND.to_string(), kind.as_str().to_string()),
        (LABEL_CHECK_OWNER_UID.to_string(), owner_uid.to_string()),
    ]);
    if let Some(uid) = connection_uid.filter(|u| !u.trim().is_empty()) {
        out.insert(LABEL_CHECK_CONNECTION_UID.to_string(), uid.to_string());
    }
    out
}

/// Everything one check Job needs.
///
/// The projections ([`SecretMount`], [`ConfigMapMount`], [`EnvFromSecret`]) are
/// the caller's: this module never decides which Secret a check may name, only
/// that they are the same kinds of projection an execution pod gets, which is
/// what makes a check's `credentialProjected` answer mean something about the
/// run it checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckJobSpec {
    pub kind: CheckPlanKind,
    /// The Job's namespace (the subject's).
    pub namespace: String,
    pub owner: RunnerOwner,
    /// The `KafkaCluster` UID this check dials, for
    /// [`LABEL_CHECK_CONNECTION_UID`]. `None` for a kind that dials none.
    pub connection_uid: Option<String>,
    /// The plan `ConfigMap`'s name, mounted at [`CHECK_MOUNT_PATH`].
    pub plan_config_map: String,
    /// `sha256:<hex>` of the plan document, pinned into the environment so the
    /// runner refuses a plan that is not the one this controller rendered.
    pub plan_sha256: String,
    /// The subject's UID, likewise pinned.
    pub subject_uid: String,
    /// The plan's own budget. The Job's deadline is this plus
    /// [`DEADLINE_MARGIN_SECONDS`].
    pub timeout_seconds: i64,
    /// The ServiceAccount, from the execution context or the destination grant.
    pub service_account_name: String,
    /// Secrets projected as volumes (signer, trust material).
    pub secret_mounts: Vec<SecretMount>,
    /// Public `ConfigMap`s projected as volumes, other than the plan.
    pub config_map_mounts: Vec<ConfigMapMount>,
    /// Credentials taken from Secret keys.
    pub env_from_secret: Vec<EnvFromSecret>,
    /// Extra plain environment variables. The four D2 §4.2 names are added by
    /// [`runner_job_spec`] and need not appear here.
    pub env_literal: Vec<(String, String)>,
    /// The runner image this process was handed, or `None` for the pin.
    pub image: Option<String>,
    /// The pull policy this process was handed, or `None` for the compiled-in.
    pub image_pull_policy: Option<String>,
    /// The evidence-fetch attempt this Job serves: `Some` only for
    /// [`CheckPlanKind::EvidenceFetch`], whose name is
    /// [`evidence_fetch_job_name`].
    pub attempt: Option<u32>,
}

impl CheckJobSpec {
    #[must_use]
    pub fn job_name(&self) -> String {
        match self.attempt {
            Some(attempt) => evidence_fetch_job_name(&self.owner.uid, attempt),
            None => check_job_name(self.kind, &self.owner.uid),
        }
    }

    #[must_use]
    pub fn labels(&self) -> BTreeMap<String, String> {
        labels(self.kind, &self.owner.uid, self.connection_uid.as_deref())
    }

    /// [`super::plan::plan_config_map_name`].
    #[must_use]
    pub fn plan_name(&self) -> String {
        super::plan::plan_config_map_name(&self.job_name())
    }
}

/// D2 §4.2's argv, verbatim.
///
/// A function, not a literal at the call site, so the runner's contract and the
/// controller's spelling of it are one thing.
#[must_use]
pub fn runner_argv() -> Vec<String> {
    vec![
        "check".to_string(),
        "run".to_string(),
        "--plan".to_string(),
        format!("{CHECK_MOUNT_PATH}/{CHECK_PLAN_KEY}"),
        "--check-contract-version".to_string(),
        logweir_core::check_contract::CHECK_CONTRACT_VERSION.to_string(),
    ]
}

/// The [`RunnerJobSpec`] for a check.
#[must_use]
pub fn runner_job_spec(spec: &CheckJobSpec) -> RunnerJobSpec {
    let mut config_map_mounts = spec.config_map_mounts.clone();
    // The plan as an ordinary `ConfigMap` mount at `/check`, not through
    // `RunnerJobSpec::plan_config_map` (`/plan`): see `CHECK_MOUNT_PATH`.
    config_map_mounts.push(ConfigMapMount {
        volume: CHECK_VOLUME.to_string(),
        config_map_name: spec.plan_config_map.clone(),
        mount_path: CHECK_MOUNT_PATH.to_string(),
        items: Vec::new(),
    });

    let mut env_literal = vec![
        (
            CONTRACT_VERSION_ENV.to_string(),
            logweir_core::check_contract::CHECK_CONTRACT_VERSION.to_string(),
        ),
        (PLAN_SHA256_ENV.to_string(), spec.plan_sha256.clone()),
        (SUBJECT_UID_ENV.to_string(), spec.subject_uid.clone()),
        // D2 §4.2: stderr carries JSON tracing at `warn`. A check's stdout is
        // the machine contract, and anything chattier competes with the frames
        // for the log's byte budget.
        ("RUST_LOG".to_string(), "warn".to_string()),
    ];
    env_literal.extend(spec.env_literal.iter().cloned());

    RunnerJobSpec {
        name: spec.job_name(),
        namespace: spec.namespace.clone(),
        owner: spec.owner.clone(),
        args: runner_argv(),
        deadline_seconds: spec.timeout_seconds + DEADLINE_MARGIN_SECONDS,
        service_account_name: spec.service_account_name.clone(),
        secret_mounts: spec.secret_mounts.clone(),
        config_map_mounts,
        env_from_secret: spec.env_from_secret.clone(),
        env_literal,
        // Not the `/plan` mount: see `CHECK_MOUNT_PATH`.
        plan_config_map: None,
        image: spec.image.clone(),
        image_pull_policy: spec.image_pull_policy.clone(),
        // No check kind's spec states resources for its Job.
        resources: None,
    }
}

/// The check Job, labelled.
///
/// The labels are written onto the built `Job` here rather than through
/// [`crate::job::RunnerJobSpec`], which has no label fields: `job::build`
/// leaves `metadata.labels` and `spec.template.metadata` unset, so there is
/// nothing to merge with or overwrite.
/// `the_check_job_is_the_runner_job_shape_plus_labels` holds the result to
/// `job::build`'s shape.
#[must_use]
pub fn build(spec: &CheckJobSpec) -> Job {
    let mut job = job::build(&runner_job_spec(spec));
    let labels = spec.labels();
    job.metadata.labels = Some(labels.clone());
    if let Some(job_spec) = job.spec.as_mut() {
        let mut meta = job_spec.template.metadata.take().unwrap_or_default();
        // The pod template's labels and the Job's are the same map: the
        // `limits.rs` count selects Jobs and an operator's `kubectl` selects
        // pods, and two spellings would make one of them wrong.
        meta.labels = Some(labels);
        job_spec.template.metadata = Some(meta);
    }
    job
}
