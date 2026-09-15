//! The runner image, and nothing else yet.
//!
//! Task 17 grows `job::build` around [`RUNNER_IMAGE`] — the `Job` with
//! `restartPolicy: Never`, `backoffLimit: 0`, `activeDeadlineSeconds` from
//! `spec.deadlineSeconds` and a `podFailurePolicy` of `FailJob` on exit codes
//! `[2, 3, 4]`. None of that is here. What is here is the one string that must
//! exist in exactly one place before anything builds a Job at all.

/// The image the runner Jobs use — **interface I15**.
///
/// THE ONLY PLACE UNDER `crates/` THE RUNNER IMAGE IS NAMED. Task 17's
/// `job::build` reads this constant, and
/// `tests/crd_shape.rs::the_runner_image_is_named_once` asserts the registry
/// path appears exactly once in this file and nowhere else under `crates/`.
/// The property is defended from the task that first states it, rather than
/// from the task that finally pins the digest: a second occurrence is how a
/// digest bump comes to update one call site and miss another.
///
/// THE NAMESPACE IS THE LITERAL `docker.io/vladyslavhaina/…`, NOT A PLACEHOLDER
/// (Global Constraint 24). A shipped `logweir.yaml` carrying
/// `docker.io/<namespace>/…` is not applyable, which would make spec §16's
/// first clause unsatisfiable. A trademark answer changes one string, here.
///
/// IT IS DOCKER HUB, AND IT IS THE OWNER'S PERSONAL NAMESPACE — measured
/// 2026-09-12: the GitHub organisation `logweir` does not exist and the Docker
/// Hub user `logweir` is unclaimed, so neither is a namespace any workflow can
/// push to. `docker.io/vladyslavhaina/…` is one the release workflow's
/// credentials reach. It is still `blocked: images not published`: the rename
/// makes a push possible and does not perform one.
///
/// A DIGEST, NOT A TAG — Global Constraint 7, pinned by Task 23. The value below
/// is the manifest-list digest `docker inspect --format
/// '{{index .RepoDigests 0}}' logweir:check` reported for the image `just image`
/// built on 2026-09-11. A tag under `imagePullPolicy: Never` on a single node
/// can be replaced by a local `docker build -t <tag>` **without touching a
/// single Kubernetes object**, which defeats the org-root anchor baked into
/// that image (`/etc/logweir/org-root.fingerprint`, stage-2 Task 16's T1); a
/// digest names bytes.
///
/// AND IT IS STILL `blocked: images not published` (Global Constraint 37). "Published"
/// means a PULL from a registry the author does not control, and this digest
/// names bytes that exist on exactly one laptop. Two things were MEASURED on
/// docker-desktop v1.34.1 and are written into `docs/kubernetes.md` §14 rather
/// than assumed:
///
///   * a pod referencing this exact string with `imagePullPolicy: Never`
///     **does start**, once `docker tag logweir:check <this repository>:v0.1.0`
///     has put the image under that repository name locally — the kubelet keys
///     on the WHOLE reference, and a matching digest alone does not make the
///     repository name resolve. The exact command is in `docs/kubernetes.md`
///     §14, which is not under `crates/` and may therefore spell the name out;
///     this file may not, because the registry path appears here EXACTLY ONCE
///     by construction (`crd_shape.rs::the_runner_image_is_named_once`);
///   * the digest a locally built image reports **changes on every build**,
///     including a fully cached no-op rebuild, because BuildKit re-generates
///     the provenance attestation each time. So this value is not reproducible
///     even on the machine that produced it, and the install file's digest rows
///     keep reading `blocked: images not published` until `release.yml` has
///     pushed to a real registry and the digest comes back from there
///     (Task 30b). No tag has been pushed, so that workflow has never run.
pub const RUNNER_IMAGE: &str =
    "docker.io/vladyslavhaina/logweir@sha256:6440a4a06d6f4a0ecbef71fa7d8ad11b5a87f3670298c585d5cd0073ae2e1229";

/// The environment variable that overrides [`RUNNER_IMAGE`] for the Jobs one
/// running controller creates — Task 33.
///
/// # Why an override exists at all
///
/// [`RUNNER_IMAGE`] is a COMPILE-TIME digest, measured on the machine that
/// built it, and plan erratum E19(a) says a local digest changes on every
/// build. On the laptop that is survivable: the operator's own build IS the
/// pinned bytes and one `docker tag` puts them under the pinned repository
/// name (E19b). **On a cluster that did not build the pins it is not.** A
/// GitHub runner builds both images minutes before the demo, at digests
/// nothing in the tree names, and the shipped repository is not pullable
/// (Global Constraint 37, `blocked: images not published`), so a controller that could
/// only ever name the compile-time pin would create Jobs no such node can
/// start. This variable is how the operator hands the controller the image
/// the node actually holds.
///
/// # It overrides the image, and NOTHING ELSE
///
/// [`IMAGE_PULL_POLICY`] is not overridable and stays `Never` — see its own
/// doc. The override's whole purpose is an image that was LOADED onto the
/// node, which is exactly the case `Never` is right for.
///
/// THE NAME IS A NAME, NOT A REFERENCE. This string carries no registry path,
/// so `tests/crd_shape.rs::the_runner_image_is_named_once` still finds the
/// runner image named exactly once under `crates/` — in [`RUNNER_IMAGE`]
/// above. The literal default is spelt out only outside `crates/`
/// (`docs/kubernetes.md` §14, `scripts/demo-steps.sh`).
pub const RUNNER_IMAGE_ENV: &str = "LOGWEIR_RUNNER_IMAGE";

/// [`RUNNER_IMAGE_ENV`]'s value as a decision: `Some(image)` or "unset".
///
/// **AN EMPTY VALUE IS UNSET, AND THAT IS PLAN ERRATUM E19(e)** — the same
/// ruling `retention::configured_archive_url` carries, for the same reason: a
/// Kubernetes `env:` entry with an empty `value:` makes `env::var` return
/// `Ok("")`, not `Err(NotPresent)`, so a controller that treated the empty
/// string as a configured value would create Jobs with `image: ""`. Whitespace
/// is trimmed and trims to the same answer.
///
/// # Why it takes the `Result` instead of reading the variable itself
///
/// So a test can hand it `Ok(String::new())` — the exact value the defect is
/// about — without mutating process-global state the rest of the binary
/// shares. The predicate is the whole of the decision; `main` supplies the
/// read, exactly once, and logs which of the two answers it got
/// (`crates/weirkeeper/tests/crd_shape.rs::main_reads_the_runner_image_override_once`).
#[must_use]
pub fn configured_runner_image(raw: Result<String, std::env::VarError>) -> Option<String> {
    let value = raw.ok()?.trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

use k8s_openapi::api::batch::v1::{
    Job, JobSpec, PodFailurePolicy, PodFailurePolicyOnExitCodesRequirement,
    PodFailurePolicyOnPodConditionsPattern, PodFailurePolicyRule,
};
use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvVar, EnvVarSource,
    KeyToPath, PodSecurityContext, PodSpec, PodTemplateSpec, SeccompProfile, SecretKeySelector,
    SecretVolumeSource, SecurityContext, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::ObjectMeta;

/// The one container in every runner Job, **always named this**.
///
/// THE CONTAINER IS SELECTED BY NAME AND NEVER BY INDEX, everywhere in this
/// crate. `pod.status.containerStatuses` is not ordered by anything a
/// controller may rely on, and a future init container or logging sidecar
/// would put an unrelated `exitCode: 0` at index 0 — which is the mutant
/// `the_container_is_selected_by_name` kills.
pub const CONTAINER_NAME: &str = "runner";

/// `spec.template.spec.restartPolicy`. **`Never`, and the value is the whole
/// point.**
///
/// `OnFailure` is not a milder choice, it is a destructive one: the job
/// controller restarts the container in place and then DELETES the pod
/// (measured live, `docs/kubernetes.md` §1), and since the exit code lives
/// only on the pod object it is not buried in `lastState` — it is gone. A
/// drill that found a real problem would leave behind no evidence of which
/// problem it was.
pub const RESTART_POLICY: &str = "Never";

/// `spec.backoffLimit`. **Zero**, so one Job yields exactly one pod.
///
/// Verified live: `kubectl --context docker-desktop get pods -l job-name=… |
/// wc -l` returned 1, the Job reached a `Failed` condition immediately, and
/// the code was readable at
/// `status.containerStatuses[].state.terminated.exitCode`.
pub const BACKOFF_LIMIT: i32 = 0;

/// The GID that makes a projected signing key readable, and the mode the
/// Secret is projected at.
///
/// WITHOUT `fsGroup` EVERY SCHEDULED RUN DIES OPENING ITS OWN SIGNING KEY.
/// Kubelet writes Secret files `root:root`, so a container running as 65532
/// reads nothing through the owner bits and the runner now exits 4 at its
/// signing-readiness gate, before any data operation. `fsGroup` chowns the
/// volume's group AND ORs group-read into the
/// mode, so it is sufficient on its own; widening the mode without it is not.
/// All four combinations were run live on docker-desktop —
/// `docs/kubernetes.md` §3, and `examples/cronjob-drill.yaml:47-101` carries
/// the same four values for the same reason.
pub const FS_GROUP: i64 = 65532;

/// The UID and GID the runner container runs as.
pub const RUN_AS: i64 = 65532;

/// `defaultMode` on a projected Secret: `0440`, stated as the octal literal.
///
/// `0440` AND NOT `0400`: with [`FS_GROUP`] set, kubelet writes the file
/// `root:65532` and ORs group-read in anyway, so `0400` lands on disk as
/// `0440` regardless. Saying `0440` means the manifest states the permission
/// that actually exists rather than one kubelet silently widens.
pub const SECRET_DEFAULT_MODE: i32 = 0o440;

/// The writable scratch volume every runner needs, and where it is mounted.
///
/// WHY `build` ALWAYS ADDS IT. The container runs with
/// `readOnlyRootFilesystem: true`, and `logweir backup run` writes its
/// document and its receipt under
/// [`crate::backup_execution::OUT_PATH`] /
/// `RECEIPT_OUT_PATH` — both under `/work`. A Job with no writable `/work`
/// fails at the first write with a read-only filesystem error, which looks
/// nothing like the contract it breaks. The volume is `build`'s because the
/// mount path is `build`'s.
pub const WORK_VOLUME: &str = "work";
/// Where [`WORK_VOLUME`] is mounted.
pub const WORK_MOUNT_PATH: &str = "/work";

/// The volume name a [`RunnerJobSpec::plan_config_map`] is projected as.
pub const PLAN_VOLUME: &str = "plan";
/// Where [`PLAN_VOLUME`] is mounted, read-only.
pub const PLAN_MOUNT_PATH: &str = "/plan";

/// The volume name the per-Restore approval bundle ConfigMap is projected as.
///
/// WHY THE NAME AND THE PATH LIVE HERE AND THE CONFIGMAP'S NAME DOES NOT. This
/// module owns the Job's SHAPE: a volume name has to be unique within the pod
/// spec [`build`] renders, and a mount path has to agree with the argv the
/// caller writes. WHICH ConfigMap is projected is the Restore controller's
/// decision; this shared renderer only enforces the mount shape.
///
/// The ConfigMap is immutable, create-only and owned by the Restore. The
/// controller compares every byte on an `AlreadyExists` response before a Job
/// may use it. The private runner signing key remains a Secret and is never a
/// member of this volume.
pub const APPROVAL_VOLUME: &str = "approval";
/// Where [`APPROVAL_VOLUME`] is mounted, read-only. The directory half of
/// every `--approval` / `--approver-key` / `--allowed-clusters` path in
/// [`crate::controllers::restore::runner_argv`].
pub const APPROVAL_MOUNT_PATH: &str = "/approval";

/// `imagePullPolicy` on the runner container. **`Never`.**
///
/// GLOBAL CONSTRAINT 17 AND GC37, TOGETHER. Zero cloud spend means every
/// demo, test and CI job runs against a locally built image, and GC37 records
/// that the images are not published, so [`RUNNER_IMAGE`] names a repository
/// nothing can pull from. `Never` makes a missing local image fail legibly as
/// `ErrImageNeverPull` instead of as an opaque pull error against a registry
/// path that resolves to nothing. **Task 23 pinned the digest and the policy
/// did not change**, which is the point: the two are separate decisions.
/// Measured with the digest in place — `ErrImageNeverPull`, with the message
/// naming the whole reference, until the image is tagged into that repository
/// name locally; then the pod starts (`docs/kubernetes.md` §14).
///
/// **TASK 33 MADE THE IMAGE OVERRIDABLE AND DELIBERATELY LEFT THIS ALONE —
/// AND TASK 37 CHANGED THAT. This is the history and the reason.** Task 33's
/// argument was that [`RUNNER_IMAGE_ENV`] exists for images LOADED onto the
/// node, which is the one case `Never` is exactly right for, so an overridable
/// policy would only ever buy a pull against a registry namespace that
/// resolves to nothing. What broke that argument is not a defect in it: **the
/// owner decided, on 2026-09-12, that `charts/logweir`'s defaults name the two
/// Logweir images by the `latest` TAG rather than by a digest.** A chart whose
/// default reference is a mutable tag under a policy that never pulls is a
/// chart that cannot work — the kubelet would refuse a tag no node holds — so
/// the policy had to become configurable alongside the image. It is configured
/// by [`RUNNER_PULL_POLICY_ENV`], which the chart renders from its own
/// `runnerImagePullPolicy` value (default `Always`, matching Kubernetes' own
/// default for a `:latest` tag).
///
/// **AN UNSET VARIABLE STILL MEANS `Never`, AND THAT IS THE POINT OF LEAVING
/// THE CONSTANT WHERE IT IS.** Nothing outside the chart sets the variable:
/// the laptop walk, `scripts/demo-steps.sh`'s kind path and every unit test
/// create Jobs for images that were BUILT or `kind load`ed onto the node, and
/// for those `Never` is still exactly right — a wrong reference fails as
/// `ErrImageNeverPull` naming the whole reference instead of as a pull error
/// naming a registry. Only an install that asked for another policy gets one.
/// `the_runner_image_override_does_not_touch_the_pull_policy` still holds:
/// the IMAGE override does not move the policy. The policy has its own
/// override now, and `a_runner_pull_policy_override_does_not_touch_the_image`
/// holds the other half.
pub const IMAGE_PULL_POLICY: &str = "Never";

/// The environment variable that overrides [`IMAGE_PULL_POLICY`] for the Jobs
/// one running controller creates — **Task 37**, the twin of
/// [`RUNNER_IMAGE_ENV`].
///
/// # Why it exists
///
/// See [`IMAGE_PULL_POLICY`]'s doc: the chart's defaults now name the two
/// Logweir images by the `latest` tag (the owner's decision of 2026-09-12), and
/// a mutable tag under a policy that never pulls is a reference no node can
/// resolve. The chart renders this variable from its `runnerImagePullPolicy`
/// value beside [`RUNNER_IMAGE_ENV`], so the two halves of one decision travel
/// together.
///
/// THE NAME IS A NAME, NOT A REFERENCE, exactly as [`RUNNER_IMAGE_ENV`] is,
/// and it carries no registry path — so
/// `tests/crd_shape.rs::the_runner_image_is_named_once` is untouched by it.
pub const RUNNER_PULL_POLICY_ENV: &str = "LOGWEIR_RUNNER_PULL_POLICY";

/// The three values Kubernetes accepts for `imagePullPolicy`, and the whole of
/// what [`configured_runner_pull_policy`] admits.
///
/// SPELT HERE AND NOWHERE ELSE. The API server refuses anything else with a
/// validation error on the Job, at CREATE time — which is to say, at every
/// `Backup` and every `Restore`, forever, from a controller that started
/// happily. Refusing the value once at startup is the difference between one
/// legible failure and an unbounded stream of illegible ones.
pub const PULL_POLICIES: [&str; 3] = ["Never", "IfNotPresent", "Always"];

/// [`RUNNER_PULL_POLICY_ENV`]'s value as a decision: `Ok(None)` for "unset"
/// (the compiled-in [`IMAGE_PULL_POLICY`]), `Ok(Some(policy))` for one of
/// [`PULL_POLICIES`], and `Err(message)` for anything else.
///
/// **AN EMPTY VALUE IS UNSET — PLAN ERRATUM E19(e)**, the same ruling
/// [`configured_runner_image`] and `retention::configured_archive_url` carry,
/// for the same reason: a Kubernetes `env:` entry with an empty `value:` makes
/// `env::var` return `Ok("")`, not `Err(NotPresent)`. Whitespace trims to the
/// same answer.
///
/// # Why a wrong value is an `Err` and not a fallback
///
/// [`configured_runner_image`] validates nothing, because only a kubelet can
/// say whether a reference resolves on a node. A pull POLICY is the opposite:
/// the set of legal values is closed, it is known here, and a Job carrying
/// `imagePullPolicy: always` is rejected by the API server's own validation at
/// CREATE. A controller that silently fell back to [`IMAGE_PULL_POLICY`] would
/// run an entire install under a policy the operator did not ask for; one that
/// passed the value through would turn every `Backup` into a rejected Job with
/// no `Backup` to show for it. `main` refuses to start and says which value it
/// refused (`crates/weirkeeper/tests/crd_shape.rs::main_reads_the_runner_pull_policy_once`).
///
/// The message names the variable and the value, because an operator reading a
/// crashed controller's last log line has nothing else to go on.
///
/// # Why it takes the `Result`
///
/// So a test can hand it `Ok(String::new())` — the exact value the E19(e)
/// defect is about — without mutating process-global state the rest of the
/// binary shares. The predicate is the whole of the decision; `main` supplies
/// the read, exactly once.
///
/// # Errors
///
/// A value that is not one of [`PULL_POLICIES`] after trimming.
pub fn configured_runner_pull_policy(
    raw: Result<String, std::env::VarError>,
) -> Result<Option<String>, String> {
    let Ok(value) = raw else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if PULL_POLICIES.contains(&value) {
        return Ok(Some(value.to_string()));
    }
    Err(format!(
        "{RUNNER_PULL_POLICY_ENV}={value:?} is not an imagePullPolicy; it must be one of {}. \
         The API server would reject every runner Job this controller created, so it refuses \
         to start instead.",
        PULL_POLICIES.join(", ")
    ))
}

/// The image AND the pull policy one running controller puts in every runner
/// Job it creates — **Task 37**, the pair Task 33's single `Option<String>`
/// grew into.
///
/// ONE STRUCT AND NOT TWO PARALLEL `Option`s. Task 33 threaded the image
/// through `Context::runner_image` and a `reconcile_*_with_runner_image`
/// sibling per reconciler; the policy travels the same path, for the same
/// reason, and a second parallel parameter beside the first is how the two
/// halves of one decision come to disagree. Both fields are `None` by default
/// and `None` means the compiled-in constant — [`RUNNER_IMAGE`] and
/// [`IMAGE_PULL_POLICY`] — so a controller handed nothing creates exactly the
/// Jobs it created before either override existed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunnerImage {
    /// [`RunnerJobSpec::image`]: the reference, or `None` for [`RUNNER_IMAGE`].
    pub image: Option<String>,
    /// [`RunnerJobSpec::image_pull_policy`]: the policy, or `None` for
    /// [`IMAGE_PULL_POLICY`].
    pub image_pull_policy: Option<String>,
}

/// The engine version the runner container declares, and the digest beside it.
///
/// MANDATORY, AND THE RUN EXITS 1 WITHOUT THEM, BEFORE THE ENGINE SPAWNS: a
/// signed receipt must name the engine build that produced the archive, so
/// `logweir backup run` refuses an empty version or digest rather than signing
/// a document that describes nothing. A Job template that omitted these two
/// would produce a pod that fails at startup with no archive and no receipt —
/// and an exit code (1) that says "operational" about a template bug.
///
/// THE VALUES MIRROR THE `Dockerfile`, AND A TEST HOLDS THE MIRROR.
/// `the_engine_env_mirrors_the_dockerfile` reads the `Dockerfile`'s
/// digest-pinned engine stage and `third_party/kafka-backup-binary.digest`
/// and asserts both against [`ENGINE_DIGEST`]; the version is asserted
/// against the vendored tarball's own name. (The upstream image reference is
/// deliberately NOT quoted here: `tests/crd_shape.rs`'s
/// `no_vendor_crd_group_is_named_anywhere` forbids a vendor registry
/// namespace anywhere under `crates/weirkeeper/src`, and reading the value
/// out of the `Dockerfile` in a test is strictly better than restating it.)
/// The `Dockerfile` sets `LOGWEIR_ENGINE_BIN` and neither of these two,
/// deliberately — the CLI path reads them off the binary that will actually
/// run (`scripts/demo.sh:128-129`) — so the Job template is where they have
/// to be stated, and the test is what keeps the statement true.
pub const ENGINE_VERSION: &str = "0.21.0";
/// See [`ENGINE_VERSION`]. The digest of the image the engine binary was
/// extracted from, byte-identical to
/// `third_party/kafka-backup-binary.digest`.
pub const ENGINE_DIGEST: &str =
    "sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317";

/// `LOGWEIR_ENGINE_VERSION`, the env name.
pub const ENGINE_VERSION_ENV: &str = "LOGWEIR_ENGINE_VERSION";
/// `LOGWEIR_ENGINE_DIGEST`, the env name.
pub const ENGINE_DIGEST_ENV: &str = "LOGWEIR_ENGINE_DIGEST";
/// `TMPDIR`, pointed at [`WORK_MOUNT_PATH`] so the engine's temporary files
/// land on the writable volume and not on the read-only root.
pub const TMPDIR_ENV: &str = "TMPDIR";

/// One Secret projected into the runner pod as a volume.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretMount {
    /// The volume name, unique within the pod.
    pub volume: String,
    /// The Secret's `metadata.name`, in the Job's own namespace.
    pub secret_name: String,
    /// Where the volume is mounted. Always `readOnly`.
    pub mount_path: String,
    /// `(secret key, path within the mount)` pairs. Empty projects every key
    /// at its own name — which is what a credential Secret with an
    /// adopter-chosen key set needs.
    pub items: Vec<(String, String)>,
}

/// One public, immutable ConfigMap projected into a runner pod.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigMapMount {
    /// The volume name, unique within the pod.
    pub volume: String,
    /// The ConfigMap's `metadata.name`, in the Job's namespace.
    pub config_map_name: String,
    /// Where the volume is mounted. Always `readOnly`.
    pub mount_path: String,
    /// `(ConfigMap key, path within the mount)` pairs.
    pub items: Vec<(String, String)>,
}

/// One environment variable taken from a Secret key.
///
/// `valueFrom.secretKeyRef` AND NEVER A LITERAL. An object-store credential
/// reaches the runner this way, so it never appears in the Job's own spec —
/// which every reader of `kubectl get job -o yaml` can see.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvFromSecret {
    /// The environment variable name.
    pub name: String,
    /// The Secret's `metadata.name`, in the Job's own namespace.
    pub secret_name: String,
    /// The key within that Secret.
    pub key: String,
}

/// The owning custom resource, as the four fields an `ownerReference` needs.
///
/// A DEDICATED TYPE RATHER THAN `OwnerReference` ITSELF, so `controller: true`
/// and `blockOwnerDeletion: true` are [`build`]'s decision and not a caller's
/// to forget. A Job with no controller owner is a Job the garbage collector
/// will not remove with its `Backup`, and a Job with `controller: false` can
/// be adopted by something else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunnerOwner {
    /// `apiVersion` of the owning object — take it from the derive's
    /// `Resource` impl, never from a literal.
    pub api_version: String,
    /// `kind` of the owning object.
    pub kind: String,
    /// `metadata.name` of the owning object.
    pub name: String,
    /// `metadata.uid` of the owning object.
    pub uid: String,
}

/// Everything [`build`] needs to produce one runner Job.
///
/// WHY THIS CARRIES `namespace`, `owner` AND `service_account_name` BEYOND THE
/// SIX FIELDS THE INTERFACE REGISTER SKETCHES. The same interface entry
/// requires `build` to emit `ownerReferences` with `controller: true` and a
/// namespaced Job, and neither is derivable from a name plus an argv. They are
/// additions to the struct, not renames: `name`, `args`, `deadline_seconds`,
/// `secret_mounts`, `env_from_secret` and `env_literal` keep the register's
/// spellings, which is what Tasks 20, 21, 22, 23, 24 and 26 name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunnerJobSpec {
    /// `metadata.name`.
    ///
    /// **NAMED AFTER THE CUSTOM RESOURCE VERBATIM** —
    /// `backup.name_any()`, never `logweir-backup-<cr name>`. A scheduled
    /// `Backup` is ALREADY called `logweir-backup-<schedule>-<slot>`
    /// (`slot::scheduled_backup_name`), so re-prefixing would pass the 63
    /// character cap on the `batch.kubernetes.io/job-name` label at any
    /// schedule name of 18 characters or more — and that label is how the pod
    /// carrying the exit code is found. Task 18's review made this ruling; the
    /// caller passes the CR's own name and `the_job_name_is_the_cr_name`
    /// asserts it.
    pub name: String,
    /// `metadata.namespace`. The Job lives with its `Backup`.
    pub namespace: String,
    /// The owning custom resource — see [`RunnerOwner`].
    pub owner: RunnerOwner,
    /// The container argv, **passed through unchanged**.
    ///
    /// NEVER REBUILT HERE. For a `Backup` this is the argv
    /// `crate::backup_execution::runner_argv` derived from the typed spec and
    /// the server-generated run identity and froze in the plan ConfigMap —
    /// never an annotation's content. A `job::build` that composed its own
    /// argv would drop the backup id override that makes a re-created Job
    /// reuse its run's backup id rather than mint a second one.
    pub args: Vec<String>,
    /// `spec.activeDeadlineSeconds`, from `Backup.spec.deadlineSeconds`.
    pub deadline_seconds: i64,
    /// The pod's ServiceAccount. Its token is **not** mounted; see
    /// [`build`].
    pub service_account_name: String,
    /// Secrets projected as volumes.
    pub secret_mounts: Vec<SecretMount>,
    /// Public immutable ConfigMaps projected as volumes, other than the plan.
    pub config_map_mounts: Vec<ConfigMapMount>,
    /// Environment variables taken from Secret keys.
    pub env_from_secret: Vec<EnvFromSecret>,
    /// Plain environment variables. [`ENGINE_VERSION_ENV`],
    /// [`ENGINE_DIGEST_ENV`] and [`TMPDIR_ENV`] are added by [`build`] and
    /// need not appear here.
    pub env_literal: Vec<(String, String)>,
    /// The ConfigMap carrying the rendered plan, mounted read-only at
    /// [`PLAN_MOUNT_PATH`].
    ///
    /// A `Option` BECAUSE THE THREE RUNNERS DIFFER. `logweir backup run` and
    /// `logweir restore run` both read `--spec <path>` out of `/plan`; Task
    /// 15c's probe reads no plan at all. The mount is therefore a property of
    /// the caller, and `None` produces a Job with no `/plan` volume rather
    /// than an empty one.
    ///
    /// **THIS TASK NAMES THE ConfigMap AND DOES NOT RENDER IT.** See
    /// `crate::controllers::backup::plan_config_map_name` for the declared
    /// late binding: no task in this plan's Files blocks renders a
    /// `backup.yaml` ConfigMap for the `Backup` path, and fixing the NAME here
    /// is what stops the renderer and the mount choosing separately.
    pub plan_config_map: Option<String>,
    /// The image this Job's one container runs, or **`None` for the shipped
    /// pin** [`RUNNER_IMAGE`] — Task 33.
    ///
    /// `None` IS THE DEFAULT AND THE DEFAULT IS THE PIN. Every
    /// `runner_job_spec` in `crate::controllers` builds this `None`, so a
    /// controller handed nothing creates exactly the Jobs it created before
    /// this field existed; the three reconcilers overwrite it, in one line
    /// each, with the value `main` read out of [`RUNNER_IMAGE_ENV`]. That is
    /// why the interface I15 assertions in `tests/backup_controller.rs` and
    /// `tests/kafka_cluster_controller.rs` needed no edit.
    ///
    /// A FIELD AND NOT A PARAMETER OF [`build`]. `build(&spec)` is called from
    /// six places in the test suite and three in this crate; a second argument
    /// would have rewritten all nine to say `None`, which is a diff that hides
    /// the one call site where the answer is not `None`. The field puts the
    /// decision where the Job's other per-run values already are.
    pub image: Option<String>,
    /// This Job's `imagePullPolicy`, or **`None` for the compiled-in**
    /// [`IMAGE_PULL_POLICY`] — Task 37.
    ///
    /// `None` IS THE DEFAULT AND THE DEFAULT IS `Never`, exactly as before this
    /// field existed. It is a sibling of [`RunnerJobSpec::image`] and not a
    /// consequence of it: an image override alone does NOT move the policy
    /// (`the_runner_image_override_does_not_touch_the_pull_policy`), because
    /// the two are separate decisions that the operator makes separately —
    /// [`RUNNER_IMAGE_ENV`] and [`RUNNER_PULL_POLICY_ENV`], rendered from the
    /// chart's `runnerImage` and `runnerImagePullPolicy`.
    pub image_pull_policy: Option<String>,
}

/// The `podFailurePolicy` every runner Job carries, **in this exact order**.
///
/// # It is inert today, and this task claims nothing more
///
/// [`BACKOFF_LIMIT`] is `0`, both rules are `FailJob`, and a single pod
/// failure already fails the Job — so this field changes no behaviour at all
/// right now. It ships anyway because it is the **declaration** that exit `1`
/// is never retried and that a disruption is a failure, so a future
/// `backoffLimit > 0` cannot quietly change either. The tests below pin a
/// declaration, not a behaviour, and say so.
///
/// # There is no `Ignore` rule on `[1]`
///
/// The obvious-looking third rule — "code 1 was operational, retry it" —
/// requires `backoffLimit > 0`, which yields SEVERAL pods, while
/// `Backup.status.exitCode` is a single value. Two pods with two codes and one
/// field is a status that is either wrong or arbitrary. Spec §3.2 M15 drops
/// the rule, and `the_failure_policy_has_no_ignore_on_one` keeps it dropped.
///
/// # Order
///
/// Rule 0 is the disruption pattern and rule 1 is the exit-code set. Order is
/// asserted rather than left to a `Vec` literal's accident because
/// `podFailurePolicy` rules are evaluated in order and the first match wins:
/// a pod that was disrupted AND happened to report a code must be classified
/// as disrupted.
#[must_use]
pub fn failure_policy() -> PodFailurePolicy {
    PodFailurePolicy {
        rules: vec![
            PodFailurePolicyRule {
                action: "FailJob".to_string(),
                on_pod_conditions: Some(vec![PodFailurePolicyOnPodConditionsPattern {
                    type_: "DisruptionTarget".to_string(),
                    status: "True".to_string(),
                }]),
                on_exit_codes: None,
            },
            PodFailurePolicyRule {
                action: "FailJob".to_string(),
                on_exit_codes: Some(PodFailurePolicyOnExitCodesRequirement {
                    container_name: Some(CONTAINER_NAME.to_string()),
                    operator: "In".to_string(),
                    values: vec![2, 3, 4],
                }),
                on_pod_conditions: None,
            },
        ],
    }
}

/// Build the runner Job for `spec`.
///
/// # The four things a reader should check first
///
/// 1. **`restartPolicy: Never` + `backoffLimit: 0`** — [`RESTART_POLICY`] and
///    [`BACKOFF_LIMIT`] carry the measured reason. Exactly one pod, and the
///    exit code survives.
/// 2. **Exactly one container, named [`CONTAINER_NAME`]** — the container the
///    exit code is read from is found by that name.
/// 3. **`automountServiceAccountToken: false`** — a runner pod makes ZERO
///    Kubernetes API calls, and the key-holder is deliberately the component
///    with no cluster credential (`design-operator.md:355-359`). A
///    ServiceAccount is still named, because Task 21's RBAC and any
///    image-pull or PSA binding attach to a name; what is refused is the
///    TOKEN.
/// 4. **No `ttlSecondsAfterFinished` at creation time** — pod garbage
///    collection must never race the exit-code read. The field is `PATCH`ed on
///    only after the status write that carries the code has returned 200; see
///    `controllers::backup::reconcile_backup` and
///    `controllers::backup::TTL_SECONDS_AFTER_FINISHED`.
///
/// Task 23 replaced [`RUNNER_IMAGE`] with a digest reference and nothing else
/// here changed, exactly as this note predicted.
///
/// Task 33 added a fifth thing to check: **the container's image is
/// [`RunnerJobSpec::image`] when that is `Some`, and [`RUNNER_IMAGE`]
/// otherwise**. The signature did not change, because the override is a
/// property of the spec and not of the builder.
///
/// Task 37 added the SIXTH, in the same shape: **`imagePullPolicy` is
/// [`RunnerJobSpec::image_pull_policy`] when that is `Some`, and
/// [`IMAGE_PULL_POLICY`] otherwise**. The two are independent — an image
/// override leaves the policy alone and a policy override leaves the image
/// alone — and nothing else in the Job moved.
#[must_use]
pub fn build(spec: &RunnerJobSpec) -> Job {
    let mut env: Vec<EnvVar> = Vec::new();
    // The two mandatory engine variables FIRST, so a caller's `env_literal`
    // cannot be read as their source. `logweir backup run` exits 1 before the
    // engine spawns if either is empty.
    env.push(EnvVar {
        name: ENGINE_VERSION_ENV.to_string(),
        value: Some(ENGINE_VERSION.to_string()),
        value_from: None,
    });
    env.push(EnvVar {
        name: ENGINE_DIGEST_ENV.to_string(),
        value: Some(ENGINE_DIGEST.to_string()),
        value_from: None,
    });
    env.push(EnvVar {
        name: TMPDIR_ENV.to_string(),
        value: Some(WORK_MOUNT_PATH.to_string()),
        value_from: None,
    });
    for (name, value) in &spec.env_literal {
        env.push(EnvVar {
            name: name.clone(),
            value: Some(value.clone()),
            value_from: None,
        });
    }
    for e in &spec.env_from_secret {
        env.push(EnvVar {
            name: e.name.clone(),
            value: None,
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: e.secret_name.clone(),
                    key: e.key.clone(),
                    optional: None,
                }),
                ..EnvVarSource::default()
            }),
        });
    }

    let mut volumes: Vec<Volume> = Vec::new();
    let mut mounts: Vec<VolumeMount> = Vec::new();
    for m in &spec.secret_mounts {
        volumes.push(Volume {
            name: m.volume.clone(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(m.secret_name.clone()),
                default_mode: Some(SECRET_DEFAULT_MODE),
                items: if m.items.is_empty() {
                    None
                } else {
                    Some(
                        m.items
                            .iter()
                            .map(|(key, path)| KeyToPath {
                                key: key.clone(),
                                path: path.clone(),
                                mode: None,
                            })
                            .collect(),
                    )
                },
                optional: None,
            }),
            ..Volume::default()
        });
        mounts.push(VolumeMount {
            name: m.volume.clone(),
            mount_path: m.mount_path.clone(),
            read_only: Some(true),
            ..VolumeMount::default()
        });
    }
    for m in &spec.config_map_mounts {
        volumes.push(Volume {
            name: m.volume.clone(),
            config_map: Some(ConfigMapVolumeSource {
                name: m.config_map_name.clone(),
                items: if m.items.is_empty() {
                    None
                } else {
                    Some(
                        m.items
                            .iter()
                            .map(|(key, path)| KeyToPath {
                                key: key.clone(),
                                path: path.clone(),
                                mode: None,
                            })
                            .collect(),
                    )
                },
                optional: Some(false),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
        mounts.push(VolumeMount {
            name: m.volume.clone(),
            mount_path: m.mount_path.clone(),
            read_only: Some(true),
            ..VolumeMount::default()
        });
    }
    if let Some(config_map) = spec.plan_config_map.as_ref() {
        volumes.push(Volume {
            name: PLAN_VOLUME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: config_map.clone(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
        mounts.push(VolumeMount {
            name: PLAN_VOLUME.to_string(),
            mount_path: PLAN_MOUNT_PATH.to_string(),
            read_only: Some(true),
            ..VolumeMount::default()
        });
    }

    // The writable volume, last, so a caller cannot shadow it with a Secret of
    // the same name without the collision being visible in the rendered spec.
    volumes.push(Volume {
        name: WORK_VOLUME.to_string(),
        empty_dir: Some(EmptyDirVolumeSource::default()),
        ..Volume::default()
    });
    mounts.push(VolumeMount {
        name: WORK_VOLUME.to_string(),
        mount_path: WORK_MOUNT_PATH.to_string(),
        ..VolumeMount::default()
    });

    let container = Container {
        name: CONTAINER_NAME.to_string(),
        // THE SHIPPED PIN UNLESS THE OPERATOR HANDED THIS PROCESS ANOTHER
        // IMAGE — Task 33, `RunnerJobSpec::image` and [`RUNNER_IMAGE_ENV`].
        image: Some(spec.image.as_deref().unwrap_or(RUNNER_IMAGE).to_string()),
        // AND THE COMPILED-IN POLICY UNLESS IT HANDED THIS PROCESS ANOTHER
        // ONE — Task 37, `RunnerJobSpec::image_pull_policy` and
        // [`RUNNER_PULL_POLICY_ENV`]. A SEPARATE decision from the image
        // above: neither override moves the other.
        image_pull_policy: Some(
            spec.image_pull_policy
                .as_deref()
                .unwrap_or(IMAGE_PULL_POLICY)
                .to_string(),
        ),
        args: Some(spec.args.clone()),
        env: Some(env),
        volume_mounts: Some(mounts),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            read_only_root_filesystem: Some(true),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                add: None,
            }),
            ..SecurityContext::default()
        }),
        ..Container::default()
    };

    Job {
        metadata: ObjectMeta {
            name: Some(spec.name.clone()),
            namespace: Some(spec.namespace.clone()),
            owner_references: Some(vec![OwnerReference {
                api_version: spec.owner.api_version.clone(),
                kind: spec.owner.kind.clone(),
                name: spec.owner.name.clone(),
                uid: spec.owner.uid.clone(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..ObjectMeta::default()
        },
        spec: Some(JobSpec {
            backoff_limit: Some(BACKOFF_LIMIT),
            active_deadline_seconds: Some(spec.deadline_seconds),
            pod_failure_policy: Some(failure_policy()),
            // NOT SET AT CREATION TIME. See `build`'s note 4.
            ttl_seconds_after_finished: None,
            template: PodTemplateSpec {
                metadata: None,
                spec: Some(PodSpec {
                    restart_policy: Some(RESTART_POLICY.to_string()),
                    service_account_name: Some(spec.service_account_name.clone()),
                    automount_service_account_token: Some(false),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        run_as_user: Some(RUN_AS),
                        run_as_group: Some(RUN_AS),
                        fs_group: Some(FS_GROUP),
                        seccomp_profile: Some(SeccompProfile {
                            type_: "RuntimeDefault".to_string(),
                            localhost_profile: None,
                        }),
                        ..PodSecurityContext::default()
                    }),
                    containers: vec![container],
                    volumes: Some(volumes),
                    ..PodSpec::default()
                }),
            },
            ..JobSpec::default()
        }),
        status: None,
    }
}
