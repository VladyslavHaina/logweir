//! The check Job — D2 §4.3's `job.rs` row.
//!
//! One shape for every check kind, built on [`crate::job::build`] so that a
//! check pod and an execution pod cannot drift apart: same container name,
//! same `restartPolicy: Never` + `backoffLimit: 0`, same
//! `automountServiceAccountToken: false`, same `podFailurePolicy`, same
//! security context, and — the ordering the whole framework depends on — **no
//! `ttlSecondsAfterFinished` at creation time**.

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::Job;
use logweir_core::check_contract::CheckPlanKind;

use crate::job::{self, ConfigMapMount, EnvFromSecret, RunnerJobSpec, RunnerOwner, SecretMount};

/// The prefix of every check Job's name.
pub const CHECK_JOB_PREFIX: &str = "lwc-";

/// How many hex characters of `sha256(owner uid)` a check Job's name carries.
///
/// TWENTY. The name is `lwc-` + a two-character discriminator + `-` + this, so
/// 4 + 2 + 1 + 20 = **27 characters, always**, whatever the owning object is
/// called. That is what removes the `NameTooLong` path the `KafkaCluster`
/// probe has to carry ([`crate::controllers::kafka_cluster::name_limit_for_cluster`]):
/// a check Job's name is independent of its owner's name, so no object can be
/// named in a way that makes its check unschedulable.
///
/// Twenty hex characters is 80 bits. A namespace would need about 2^40 live
/// check subjects for an even chance of a collision, and the two objects would
/// also have to want the same check KIND at the same time; the plan
/// `ConfigMap`'s owner-UID check (D2 §4.3's `plan.rs` 409 rule) is the backstop
/// that turns a collision into a refusal rather than into a wrong answer.
pub const OWNER_UID_HEX_CHARS: usize = 20;

/// `app.kubernetes.io/managed-by` on every check Job and its pod template.
pub const LABEL_MANAGED_BY: &str = "app.kubernetes.io/managed-by";
/// The value of [`LABEL_MANAGED_BY`].
pub const MANAGED_BY: &str = "weirkeeper";
/// `app.kubernetes.io/component`.
pub const LABEL_COMPONENT: &str = "app.kubernetes.io/component";
/// The value of [`LABEL_COMPONENT`] — also the selector D2 §4.3's `limits.rs`
/// counts active check Jobs with.
pub const COMPONENT_CHECK: &str = "check";
/// Which plan kind this Job runs.
pub const LABEL_CHECK_KIND: &str = "logweir.dev/check-kind";
/// The UID of the custom resource this check belongs to.
///
/// **A LABEL, AND NEVER AN IDENTITY.** It is here so an operator can find a
/// check's Job with `kubectl get jobs -l`, and for the `limits.rs` count. It is
/// NOT how a pod is matched to a Job: that is
/// [`super::pod::find_owned_pod`]'s `ownerReferences` check (D-SEAMS **S6**),
/// because a label is writable by anything that can create a pod.
pub const LABEL_CHECK_OWNER_UID: &str = "logweir.dev/check-owner-uid";

/// The UID of the `KafkaCluster` this check dials, when it dials one.
///
/// Present ONLY so [`super::limits`] can enforce D2 §4.4's
/// `maxActiveDiscoveriesPerConnection` — one active discovery per connection —
/// without reading every check's plan `ConfigMap` to find out which cluster it
/// names. Absent for a kind that has no connection (`destinationAccess`,
/// `evidenceFetch`), and absent is counted as "no connection", never as a
/// wildcard.
pub const LABEL_CHECK_CONNECTION_UID: &str = "logweir.dev/check-connection-uid";

/// Where the check plan `ConfigMap` is mounted.
///
/// `/check`, NOT [`crate::job::PLAN_MOUNT_PATH`]. D2 §4.2's argv is
/// `--plan /check/check-plan.json`, and the two mounts are deliberately
/// different directories: an execution pod's `/plan` carries a `backup.yaml`
/// the engine reads, a check pod's `/check` carries a contract document with a
/// digest pinned in the environment. A check Job that reused `/plan` would make
/// `RunnerJobSpec::plan_config_map` mean two things.
pub const CHECK_MOUNT_PATH: &str = "/check";
/// The volume name for [`CHECK_MOUNT_PATH`].
pub const CHECK_VOLUME: &str = "check-plan";
/// The plan document's key inside the `ConfigMap`, and its file name.
pub const CHECK_PLAN_KEY: &str = "check-plan.json";

/// `LOGWEIR_CHECK_CONTRACT_VERSION` — D2 §4.2's env.
pub const CONTRACT_VERSION_ENV: &str = "LOGWEIR_CHECK_CONTRACT_VERSION";
/// `LOGWEIR_CHECK_PLAN_SHA256`.
pub const PLAN_SHA256_ENV: &str = "LOGWEIR_CHECK_PLAN_SHA256";
/// `LOGWEIR_CHECK_SUBJECT_UID`.
pub const SUBJECT_UID_ENV: &str = "LOGWEIR_CHECK_SUBJECT_UID";

/// Seconds added to a check's own `timeoutSeconds` to get the Job's
/// `activeDeadlineSeconds` — D2 §4.3.
///
/// NINETY. The runner's own budget is the plan's `timeoutSeconds`; the margin
/// is image pull, scheduling and container start, none of which the runner can
/// bound. A deadline that fires therefore means the POD never got going, which
/// is a different fact from a check that ran out of time — and it is reported
/// as a different code.
pub const DEADLINE_MARGIN_SECONDS: i64 = 90;

/// `ttlSecondsAfterFinished` for a finished check Job — D2 §4.3.
///
/// TEN MINUTES, and **patched on only after the status commit**. The relay
/// lives on the pod, and the TTL controller deletes a Job and its pods
/// together: a TTL set at creation time would let garbage collection race the
/// log read. [`super::ttl_patch`] is the one place it is written.
pub const TTL_SECONDS: i32 = 600;

/// The Job name for one check: `lwc-<k>-<first 20 hex of sha256(owner uid)>`.
///
/// A PURE FUNCTION OF THE KIND AND THE UID, and of nothing else — see
/// [`OWNER_UID_HEX_CHARS`] for why the owner's NAME is deliberately absent.
/// A duplicate reconcile therefore computes the same name and gets
/// **409 `AlreadyExists`** from the API server instead of creating a second
/// check Job, which is the same mechanism [`crate::slot`] uses for scheduled
/// backups.
#[must_use]
pub fn check_job_name(kind: CheckPlanKind, owner_uid: &str) -> String {
    let digest = logweir_core::ids::sha256_hex(owner_uid.as_bytes());
    format!(
        "{CHECK_JOB_PREFIX}{}-{}",
        kind.job_discriminator(),
        &digest[..OWNER_UID_HEX_CHARS]
    )
}

/// The labels on a check Job and on its pod template — the same map, on both.
///
/// `connection_uid` is `None` for a kind that dials no cluster; the key is then
/// ABSENT rather than empty, because an empty label value is a value a
/// selector can match.
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
/// the CALLER's: W8 renders a connection's credential, W9 a destination's, and
/// this module never decides which Secret a check may name. What it does decide
/// is that they are the SAME kinds of projection an execution pod gets, which
/// is what makes a check's `credentialProjected` answer mean something about
/// the run it is a check for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckJobSpec {
    /// Which plan kind runs.
    pub kind: CheckPlanKind,
    /// The Job's namespace — the subject's.
    pub namespace: String,
    /// The owning custom resource.
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
}

impl CheckJobSpec {
    /// This check's Job name.
    #[must_use]
    pub fn job_name(&self) -> String {
        check_job_name(self.kind, &self.owner.uid)
    }

    /// This check's labels.
    #[must_use]
    pub fn labels(&self) -> BTreeMap<String, String> {
        labels(self.kind, &self.owner.uid, self.connection_uid.as_deref())
    }

    /// This check's plan `ConfigMap` name — [`super::plan::plan_config_map_name`].
    #[must_use]
    pub fn plan_name(&self) -> String {
        super::plan::plan_config_map_name(&self.job_name())
    }
}

/// D2 §4.2's argv, verbatim.
///
/// A FUNCTION AND NOT A LITERAL AT THE CALL SITE, so the runner's contract and
/// the controller's spelling of it are one thing.
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
    // THE PLAN, AS AN ORDINARY ConfigMap MOUNT AT `/check`. It deliberately
    // does not go through `RunnerJobSpec::plan_config_map`, which mounts at
    // `/plan` — see `CHECK_MOUNT_PATH`.
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
        // the machine contract, so anything chattier competes with the frames
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
        // NOT the `/plan` mount. See `CHECK_MOUNT_PATH`.
        plan_config_map: None,
        image: spec.image.clone(),
        image_pull_policy: spec.image_pull_policy.clone(),
    }
}

/// The check Job, labelled.
///
/// # THE ONE PLACE THE DEFERRED `job.rs` SEAM IS APPLIED
///
/// D2 §13.2 gives W5 a last commit that adds `labels` and `template_labels`
/// fields to [`crate::job::RunnerJobSpec`], scheduled AFTER PLAT-07.1 merges
/// because `job.rs` is that task's file for the duration of its rebase. Until
/// then the labels are written onto the built `Job` here, which produces the
/// identical object: `job::build` leaves `metadata.labels` and
/// `spec.template.metadata` unset, so there is nothing to merge with and
/// nothing to overwrite.
///
/// **What moves when the seam closes.** The two `labels` assignments below
/// become `spec.labels` / `spec.template_labels` on the `RunnerJobSpec`
/// [`runner_job_spec`] already returns, and this function becomes
/// `job::build(&runner_job_spec(spec))`. Nothing else changes, and
/// `the_check_job_is_the_runner_job_shape_plus_labels` is the test that holds
/// both forms to the same object.
#[must_use]
pub fn build(spec: &CheckJobSpec) -> Job {
    let mut job = job::build(&runner_job_spec(spec));
    let labels = spec.labels();
    job.metadata.labels = Some(labels.clone());
    if let Some(job_spec) = job.spec.as_mut() {
        let mut meta = job_spec.template.metadata.take().unwrap_or_default();
        // THE POD TEMPLATE'S LABELS, and the Job's, are the same map. The
        // `limits.rs` count selects Jobs and the `kubectl` an operator types
        // selects pods; two spellings would make one of them wrong.
        meta.labels = Some(labels);
        job_spec.template.metadata = Some(meta);
    }
    job
}
