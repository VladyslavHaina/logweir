//! The runner Job: [`build`] and the constants that shape it, the runner image
//! and its overrides, and the per-Job line token.

/// The image the runner Jobs use (interface **I15**).
///
/// The only place under `crates/` the runner image is named:
/// `tests/crd_shape.rs::the_runner_image_is_named_once` asserts the registry
/// path appears exactly once, here, so a digest bump cannot update one call
/// site and miss another. The namespace is the literal
/// `docker.io/vladyslavhaina/…`, not a placeholder (Global Constraint 24): a
/// manifest carrying `docker.io/<namespace>/…` is not applyable.
///
/// A digest, not a tag (Global Constraint 7): under `imagePullPolicy: Never` a
/// tag can be replaced by a local `docker build -t <tag>` without touching a
/// Kubernetes object, which defeats the org-root anchor baked into the image
/// (`/etc/logweir/org-root.fingerprint`); a digest names bytes. It is the
/// digest of a local build, and a locally built image's digest changes on every
/// build (BuildKit regenerates the provenance attestation), so it resolves only
/// where that image was tagged under this repository (`docs/install.md`). The
/// chart always overrides it through [`RUNNER_IMAGE_ENV`].
pub const RUNNER_IMAGE: &str =
    "docker.io/vladyslavhaina/logweir@sha256:6440a4a06d6f4a0ecbef71fa7d8ad11b5a87f3670298c585d5cd0073ae2e1229";

/// The environment variable that overrides [`RUNNER_IMAGE`] for the Jobs one
/// running controller creates.
///
/// [`RUNNER_IMAGE`] is a compile-time digest of a local build, and a cluster
/// that did not build it cannot start a Job naming it; this is how the operator
/// (or the chart's `runnerImage`) hands the controller the image its nodes can
/// run. It overrides the image only; the pull policy has its own
/// [`RUNNER_PULL_POLICY_ENV`]. The name carries no registry path, so
/// `tests/crd_shape.rs::the_runner_image_is_named_once` still finds the image
/// named once.
pub const RUNNER_IMAGE_ENV: &str = "LOGWEIR_RUNNER_IMAGE";

/// [`RUNNER_IMAGE_ENV`]'s value as a decision: `Some(image)` or unset.
///
/// An empty value is unset: a Kubernetes `env:` entry with an empty `value:`
/// makes `env::var` return `Ok("")`, and treating it as configured would create
/// Jobs with `image: ""`. Whitespace trims to the same answer. It takes the
/// `Result` so a test can pass `Ok(String::new())` without mutating process
/// state; `main` supplies the read, once
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
    KeyToPath, PodSecurityContext, PodSpec, PodTemplateSpec, ResourceRequirements, SeccompProfile,
    SecretKeySelector, SecretVolumeSource, SecurityContext, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::ObjectMeta;
use std::collections::BTreeMap;

/// The one container in every runner Job, always named this. Containers are
/// selected by name, never by index: `pod.status.containerStatuses` has no
/// order a controller may rely on, and a sidecar would put an unrelated
/// `exitCode: 0` at index 0 (`the_container_is_selected_by_name`).
pub const CONTAINER_NAME: &str = "runner";

/// `spec.template.spec.restartPolicy`: `Never`. `OnFailure` restarts the
/// container in place and then deletes the pod (measured live), and the exit
/// code lives only on the pod, so a drill that found a real problem would leave
/// no evidence of which.
pub const RESTART_POLICY: &str = "Never";

/// `spec.backoffLimit`: zero, so one Job yields exactly one pod and fails at
/// once with the code readable at
/// `status.containerStatuses[].state.terminated.exitCode` (verified live).
pub const BACKOFF_LIMIT: i32 = 0;

/// The GID that makes a projected signing key readable.
///
/// Kubelet writes Secret files `root:root`, so without `fsGroup` a container
/// running as 65532 cannot open its signing key and the runner exits 4 at its
/// signing-readiness gate. `fsGroup` chowns the volume's group and ORs
/// group-read into the mode, so it is sufficient alone; widening the mode
/// without it is not (all four combinations were run live).
pub const FS_GROUP: i64 = 65532;

/// The UID and GID the runner container runs as.
pub const RUN_AS: i64 = 65532;

/// `defaultMode` on a projected Secret: `0440`. With [`FS_GROUP`] set, kubelet
/// ORs group-read in anyway, so `0400` would land as `0440`; the manifest states
/// the permission that actually exists.
pub const SECRET_DEFAULT_MODE: i32 = 0o440;

/// The writable scratch volume every runner needs. The container runs with
/// `readOnlyRootFilesystem: true` and `logweir backup run` writes under `/work`
/// ([`crate::backup_execution::OUT_PATH`]), so [`build`] always adds it.
pub const WORK_VOLUME: &str = "work";
/// Where [`WORK_VOLUME`] is mounted.
pub const WORK_MOUNT_PATH: &str = "/work";

/// The volume name a [`RunnerJobSpec::plan_config_map`] is projected as.
pub const PLAN_VOLUME: &str = "plan";
/// Where [`PLAN_VOLUME`] is mounted, read-only.
pub const PLAN_MOUNT_PATH: &str = "/plan";

/// The volume name the per-Restore approval bundle ConfigMap is projected as.
///
/// The name and path live here because this module owns the Job's shape (a
/// volume name unique in the pod, a mount path the caller's argv agrees with);
/// which ConfigMap is projected is the Restore controller's decision. The
/// ConfigMap is immutable, create-only, owned by the Restore and compared byte
/// for byte on `AlreadyExists`. The runner signing key stays a Secret and is
/// never in this volume.
pub const APPROVAL_VOLUME: &str = "approval";
/// Where [`APPROVAL_VOLUME`] is mounted, read-only. The directory half of
/// every `--approval` / `--approver-key` / `--allowed-clusters` path in
/// [`crate::controllers::restore::runner_argv`].
pub const APPROVAL_MOUNT_PATH: &str = "/approval";

/// `imagePullPolicy` on the runner container when nothing overrides it:
/// `Never`.
///
/// For an image built or `kind load`ed onto the node (the laptop walk, the
/// demo's kind path, every unit test) `Never` is right: a wrong reference fails
/// as `ErrImageNeverPull` naming the whole reference instead of as a pull error
/// against a registry. The chart names images by the `latest` tag, which no
/// node holds under a policy that never pulls, so it sets
/// [`RUNNER_PULL_POLICY_ENV`] from `runnerImagePullPolicy` (default `Always`).
/// The image and policy overrides are independent
/// (`the_runner_image_override_does_not_touch_the_pull_policy`,
/// `a_runner_pull_policy_override_does_not_touch_the_image`).
pub const IMAGE_PULL_POLICY: &str = "Never";

/// The environment variable that overrides [`IMAGE_PULL_POLICY`] for the Jobs
/// one running controller creates, the twin of [`RUNNER_IMAGE_ENV`]. The chart
/// renders both together, since a mutable tag under a policy that never pulls
/// resolves on no node. The name carries no registry path.
pub const RUNNER_PULL_POLICY_ENV: &str = "LOGWEIR_RUNNER_PULL_POLICY";

/// The three values Kubernetes accepts for `imagePullPolicy`, and all that
/// [`configured_runner_pull_policy`] admits. The API server would refuse any
/// other at every Job create; refusing it once at startup is one legible
/// failure instead of an unbounded stream.
pub const PULL_POLICIES: [&str; 3] = ["Never", "IfNotPresent", "Always"];

/// [`RUNNER_PULL_POLICY_ENV`]'s value as a decision: `Ok(None)` for unset (the
/// compiled-in [`IMAGE_PULL_POLICY`]), `Ok(Some(policy))` for one of
/// [`PULL_POLICIES`], and `Err(message)` for anything else.
///
/// An empty value is unset, as for [`configured_runner_image`]. A wrong value
/// is an `Err`, not a fallback: the legal set is closed and known here, a
/// silent fallback would run the install under a policy nobody asked for, and
/// passing it through would make every Job a rejected create. `main` refuses to
/// start and names the variable and value
/// (`crates/weirkeeper/tests/crd_shape.rs::main_reads_the_runner_pull_policy_once`).
/// It takes the `Result` so a test can pass `Ok(String::new())`.
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

/// The image and the pull policy one running controller puts in every runner
/// Job it creates: one struct, not two parallel `Option`s, so the two halves of
/// one decision travel together. `None` means the compiled-in constant
/// ([`RUNNER_IMAGE`], [`IMAGE_PULL_POLICY`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunnerImage {
    /// [`RunnerJobSpec::image`]: the reference, or `None` for [`RUNNER_IMAGE`].
    pub image: Option<String>,
    /// [`RunnerJobSpec::image_pull_policy`]: the policy, or `None` for
    /// [`IMAGE_PULL_POLICY`].
    pub image_pull_policy: Option<String>,
}

/// The two variables a runner reads its engine identity from where its image
/// declares none. This controller never sets them, and [`build`] drops either
/// from a caller's `env_literal`.
///
/// The identity travels with the engine: the runner image declares it in
/// `/etc/logweir/engine-identity`, and the runner refuses to sign a version the
/// binary does not print (`logweir::engine_identity::verify_engine_reports`). A
/// controller stamping it would sign the wrong engine for a runner image it was
/// not built with; an image that declares none gets no identity here and
/// refuses its run (exit 1, nothing signed) rather than mislabel it.
/// `tests/backup_controller.rs::no_job_states_the_engine_identity` holds both.
pub const ENGINE_VERSION_ENV: &str = "LOGWEIR_ENGINE_VERSION";
/// See [`ENGINE_VERSION_ENV`].
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
    /// `secretKeyRef.optional: true`: the variable is unset when the Secret
    /// lacks the key, instead of the container failing to start. `false`
    /// (rendered as an absent `optional`, so older Jobs are byte-identical) for
    /// every credential; `true` only for the credential binding, whose absence
    /// the runner must see to refuse it by name.
    pub optional: bool,
}

/// The runner container's requests and limits, already validated by
/// [`crate::runner_resources::validate`]. A type of this crate rather than
/// `ResourceRequirements`, because [`RunnerJobSpec`] is `Eq` and the generated
/// type is only `PartialEq`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContainerResources {
    /// `resources.requests`, resource name → quantity.
    pub requests: BTreeMap<String, String>,
    /// `resources.limits`, resource name → quantity.
    pub limits: BTreeMap<String, String>,
}

impl ContainerResources {
    /// The container's `resources` block. An empty map renders as an ABSENT
    /// key, never as `{}`.
    #[must_use]
    pub fn requirements(&self) -> ResourceRequirements {
        let render = |map: &BTreeMap<String, String>| {
            (!map.is_empty()).then(|| {
                map.iter()
                    .map(|(name, value)| (name.clone(), Quantity(value.clone())))
                    .collect::<BTreeMap<_, _>>()
            })
        };
        ResourceRequirements {
            claims: None,
            limits: render(&self.limits),
            requests: render(&self.requests),
        }
    }
}

/// The owning custom resource, as the four fields an `ownerReference` needs. A
/// dedicated type so `controller: true` and `blockOwnerDeletion: true` are
/// [`build`]'s decision: a Job with no controller owner is not removed with its
/// `Backup`, and one with `controller: false` can be adopted by something else.
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
    /// The owning object's workspace label, which the Job carries
    /// ([`crate::workspace::LABEL`]) so caps and per-workspace views see it.
    pub workspace: Option<String>,
}

impl RunnerOwner {
    /// The owner of a Job created for `object`.
    #[must_use]
    pub fn of<K: kube::Resource<DynamicType = ()>>(object: &K, uid: &str) -> Self {
        Self {
            api_version: K::api_version(&()).into_owned(),
            kind: K::kind(&()).into_owned(),
            name: object.meta().name.clone().unwrap_or_default(),
            uid: uid.to_string(),
            workspace: crate::workspace::label_of(object).map(str::to_string),
        }
    }
}

/// Everything [`build`] needs to produce one runner Job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunnerJobSpec {
    /// `metadata.name`: the custom resource's own name, never
    /// `logweir-backup-<cr name>`. A scheduled `Backup` is already called
    /// `logweir-backup-<schedule>-<slot>`, so a prefix would pass the 63-char
    /// cap on the `batch.kubernetes.io/job-name` label (how the pod carrying the
    /// exit code is found) for schedule names of 18 characters or more
    /// (`the_job_name_is_the_cr_name`).
    pub name: String,
    /// `metadata.namespace`. The Job lives with its `Backup`.
    pub namespace: String,
    /// The owning custom resource — see [`RunnerOwner`].
    pub owner: RunnerOwner,
    /// The container argv, passed through unchanged. For a `Backup` it is the
    /// argv `crate::backup_execution::runner_argv` derived and froze in the plan
    /// ConfigMap; a `build` that composed its own would drop the backup id
    /// override that makes a re-created Job reuse its run's backup id.
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
    /// Plain environment variables. [`TMPDIR_ENV`] is added by [`build`] and
    /// need not appear here; [`ENGINE_VERSION_ENV`] and [`ENGINE_DIGEST_ENV`]
    /// are dropped by it (the controller never states the engine identity).
    pub env_literal: Vec<(String, String)>,
    /// The ConfigMap carrying the rendered plan, mounted read-only at
    /// [`PLAN_MOUNT_PATH`]. An `Option` because the probe reads no plan; `None`
    /// produces a Job with no `/plan` volume rather than an empty one.
    pub plan_config_map: Option<String>,
    /// The image this Job's one container runs, or `None` for the shipped pin
    /// [`RUNNER_IMAGE`]. Every `runner_job_spec` builds `None` and each
    /// reconciler overwrites it with the value `main` read from
    /// [`RUNNER_IMAGE_ENV`]. A field rather than a parameter of [`build`], so the
    /// per-run decision sits with the Job's other per-run values.
    pub image: Option<String>,
    /// This Job's `imagePullPolicy`, or `None` for the compiled-in
    /// [`IMAGE_PULL_POLICY`]: a sibling of [`RunnerJobSpec::image`], not a
    /// consequence of it (`the_runner_image_override_does_not_touch_the_pull_policy`).
    pub image_pull_policy: Option<String>,
    /// The runner container's requests and limits, or `None` for no `resources`
    /// key at all, so the namespace's `LimitRange` defaults apply. Only a
    /// `Restore` sets it, from `spec.runnerResources`, after
    /// [`crate::runner_resources::validate`].
    pub resources: Option<ContainerResources>,
}

/// The `podFailurePolicy` every runner Job carries, in this exact order.
///
/// Inert today: [`BACKOFF_LIMIT`] is `0`, both rules are `FailJob`, and one pod
/// failure already fails the Job. It declares that exit `1` is never retried
/// and that a disruption is a failure, so a future `backoffLimit > 0` cannot
/// quietly change either; the tests pin a declaration, not a behaviour.
///
/// No `Ignore` rule on `[1]`: retrying needs `backoffLimit > 0`, which yields
/// several pods for one `Backup.status.exitCode` (spec §3.2 M15,
/// `the_failure_policy_has_no_ignore_on_one`).
///
/// Rule 0 is the disruption pattern and rule 1 the exit-code set. Rules are
/// evaluated in order and the first match wins, so a disrupted pod that also
/// reported a code is classified as disrupted.
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
/// What to check first:
///
/// 1. **`restartPolicy: Never` + `backoffLimit: 0`** ([`RESTART_POLICY`],
///    [`BACKOFF_LIMIT`]): exactly one pod, and the exit code survives.
/// 2. **Exactly one container, named [`CONTAINER_NAME`]**, the one the exit
///    code is read from.
/// 3. **`automountServiceAccountToken: false`**: a runner pod makes no
///    Kubernetes API calls, and the key holder is deliberately the component
///    with no cluster credential. A ServiceAccount is still named, because RBAC
///    and image-pull or PSA bindings attach to a name; the token is refused.
/// 4. **No `ttlSecondsAfterFinished` at creation**: pod garbage collection must
///    never race the exit-code read. It is patched on only after the status
///    write carrying the code returned (see
///    `controllers::backup::TTL_SECONDS_AFTER_FINISHED`).
/// 5. **The image** is [`RunnerJobSpec::image`] when `Some`, else
///    [`RUNNER_IMAGE`], and **`imagePullPolicy`** is
///    [`RunnerJobSpec::image_pull_policy`] when `Some`, else
///    [`IMAGE_PULL_POLICY`]; the two are independent.
/// 6. **`resources`** is [`RunnerJobSpec::resources`] when `Some`, and absent
///    otherwise.
#[must_use]
pub fn build(spec: &RunnerJobSpec) -> Job {
    let mut env: Vec<EnvVar> = Vec::new();
    // No engine identity: the runner image states its own (see
    // [`ENGINE_VERSION_ENV`]).
    env.push(EnvVar {
        name: TMPDIR_ENV.to_string(),
        value: Some(WORK_MOUNT_PATH.to_string()),
        value_from: None,
    });
    for (name, value) in &spec.env_literal {
        if name == ENGINE_VERSION_ENV || name == ENGINE_DIGEST_ENV {
            continue;
        }
        env.push(EnvVar {
            name: name.clone(),
            value: Some(value.clone()),
            value_from: None,
        });
    }
    // The backstop: a credential projected from a Secret with no binding
    // expectation beside it would reach the runner unchecked. Every builder
    // projects the pair itself; one that forgets gets a never-satisfied
    // expectation here, and its runner refuses the credential (fail closed).
    for (credential, _, expected) in logweir_core::credential_binding::GUARDED_CREDENTIALS {
        let projected = spec.env_from_secret.iter().any(|e| e.name == credential);
        let has_expectation = spec.env_literal.iter().any(|(n, _)| n == expected);
        if projected && !has_expectation {
            env.push(EnvVar {
                name: expected.to_string(),
                value: Some(
                    logweir_core::credential_binding::UNBOUND_MISSING_EXPECTATION.to_string(),
                ),
                value_from: None,
            });
        }
    }
    for e in &spec.env_from_secret {
        env.push(EnvVar {
            name: e.name.clone(),
            value: None,
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: e.secret_name.clone(),
                    key: e.key.clone(),
                    optional: e.optional.then_some(true),
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
        // The shipped pin unless this process was handed another image.
        image: Some(spec.image.as_deref().unwrap_or(RUNNER_IMAGE).to_string()),
        // And the compiled-in policy unless it was handed another; neither
        // override moves the other.
        image_pull_policy: Some(
            spec.image_pull_policy
                .as_deref()
                .unwrap_or(IMAGE_PULL_POLICY)
                .to_string(),
        ),
        args: Some(spec.args.clone()),
        env: Some(env),
        // What the object asked for, or no key at all.
        resources: spec
            .resources
            .as_ref()
            .map(ContainerResources::requirements),
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
            labels: spec
                .owner
                .workspace
                .as_ref()
                .map(|workspace| [(crate::workspace::LABEL.to_string(), workspace.clone())].into()),
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

use logweir_core::refusal_detail::{LineToken, LINE_TOKEN_ARG};
use logweir_core::runner_line::RUNNER_LINE_TOKEN_ARG;

/// How many random bytes a line token is made of: 20, which is 160 bits and
/// 40 lower-case hex digits.
///
/// More than the 128 bits that make guessing pointless, and 40 digits for a
/// second reason: an unkeyed hex run of 40 or more is one of the shapes the
/// credential rules remove from every relayed sentence
/// (`logweir_core::check_contract::redaction_rules`). So a refusal sentence
/// that repeated the token would reach a status as `[redacted]`, although
/// nothing writes one that does.
pub const LINE_TOKEN_BYTES: usize = 20;

/// A fresh line token, from the operating system's random source.
///
/// A pod log merges stdout and stderr, and whoever wrote a plan can start a
/// line of their own in it, so the controller honours a runner's lines only
/// when they carry a value the plan's author could not have had: this one,
/// made when the Job is built and given to the runner as an argument
/// ([`add_line_token`]). A plan is older than its Job.
///
/// The bytes come from `rustls`'s `ring` provider (`ring::rand::SystemRandom`,
/// the kernel's generator, with no seed this process holds), never from a
/// name, a UID, a hash or a clock, each of which a plan's author can know or
/// guess. `None` when the random source fails; the Job is then built without a
/// token.
#[must_use]
pub fn new_line_token() -> Option<LineToken> {
    let mut bytes = [0u8; LINE_TOKEN_BYTES];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut bytes)
        .ok()?;
    LineToken::from_random_bytes(&bytes)
}

/// Give `job`'s runner `token`, as the LAST two arguments of the container
/// named [`CONTAINER_NAME`]: [`RUNNER_LINE_TOKEN_ARG`] and the token.
///
/// **FX-43's flag, not FX-34's.** Under it the runner seals EVERY line this
/// controller acts on and ends every run with a sealed `runner-exit=` line
/// (`logweir_core::runner_line`). A runner that predates the flag stops at
/// argument parsing, before any work, and the reconciler names that
/// (`RunnerOlderThanController`) instead of reading its log.
///
/// An ARGUMENT, not an environment variable: the engine the runner starts
/// inherits the runner's environment and expands `${NAME}` over its
/// configuration text, and neither reaches an argument.
///
/// Called by a reconciler on the Job it is about to create, and on nothing
/// else. It is deliberately NOT part of [`build`] or of an argv a plan
/// ConfigMap freezes: the token belongs to one Job object, a re-created Job
/// gets another, and the only place it is kept is that Job's own pod
/// template.
pub fn add_line_token(job: &mut Job, token: &LineToken) {
    let runner = job
        .spec
        .as_mut()
        .and_then(|spec| spec.template.spec.as_mut())
        .and_then(|pod| pod.containers.iter_mut().find(|c| c.name == CONTAINER_NAME));
    if let Some(runner) = runner {
        let args = runner.args.get_or_insert_with(Vec::new);
        args.push(RUNNER_LINE_TOKEN_ARG.to_string());
        args.push(token.expose_token().to_string());
    }
}

/// [`add_line_token`] with a token made now ([`new_line_token`]). `false`
/// when the random source failed and the Job was left with none: the caller
/// says so in its log, and creates the Job all the same, because a run is not
/// refused over what it would have said had IT been refused.
#[must_use]
pub fn add_fresh_line_token(job: &mut Job) -> bool {
    match new_line_token() {
        Some(token) => {
            add_line_token(job, &token);
            true
        }
        None => false,
    }
}

/// `job` with a fresh FX-43 line token ([`add_fresh_line_token`]), for the
/// reconcilers that build a Job and create it in one expression. When the
/// random source fails the Job is returned without one, and a `warn` names
/// the Job: its log is then read as a Job an older controller built.
#[must_use]
pub fn tokened(mut job: Job) -> Job {
    if !add_fresh_line_token(&mut job) {
        tracing::warn!(
            job = job.metadata.name.as_deref().unwrap_or_default(),
            "the random source gave no line token; the Job is created without one and its log \
             is read as a Job an older controller built"
        );
    }
    job
}

/// The line token `job`'s runner was given, read back off the Job's own pod
/// template, whichever flag carried it ([`job_token`]).
#[must_use]
pub fn line_token(job: &Job) -> Option<LineToken> {
    job_token(job).token().cloned()
}

/// Which lines of a Job's pod log its runner vouched for, by the flag the
/// Job's controller gave it (FX-43).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobToken {
    /// No token: a controller from before FX-34 built the Job, or the random
    /// source failed. Every reader keeps its own rules over the whole log.
    None,
    /// FX-34's [`LINE_TOKEN_ARG`]: a controller from before FX-43 built the
    /// Job. Only the `refusal-detail=` line carries the token (in its JSON);
    /// every other line is read by the rules that controller used.
    RefusalDetail(LineToken),
    /// FX-43's [`RUNNER_LINE_TOKEN_ARG`]: every line the controller acts on is
    /// sealed with the token, and nothing else in the log is read.
    EveryLine(LineToken),
}

impl JobToken {
    /// The token, whichever flag carried it.
    #[must_use]
    pub fn token(&self) -> Option<&LineToken> {
        match self {
            Self::None => None,
            Self::RefusalDetail(t) | Self::EveryLine(t) => Some(t),
        }
    }
}

/// The line token `job`'s runner was given and by which flag: the LAST two
/// arguments of the container named [`CONTAINER_NAME`], when they are
/// [`RUNNER_LINE_TOKEN_ARG`] or [`LINE_TOKEN_ARG`] and a well-formed token.
///
/// [`JobToken::None`] for a Job built without one. By POSITION IN THE ARGV,
/// which is this controller's own structure and not a log: [`add_line_token`]
/// appends, so whatever the arguments before it hold, the last two are what
/// it wrote.
#[must_use]
pub fn job_token(job: &Job) -> JobToken {
    let Some(args) = job
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .and_then(|pod| pod.containers.iter().find(|c| c.name == CONTAINER_NAME))
        .and_then(|runner| runner.args.as_ref())
    else {
        return JobToken::None;
    };
    match args.as_slice() {
        [.., flag, value] if flag == RUNNER_LINE_TOKEN_ARG => {
            LineToken::parse(value).map_or(JobToken::None, JobToken::EveryLine)
        }
        [.., flag, value] if flag == LINE_TOKEN_ARG => {
            LineToken::parse(value).map_or(JobToken::None, JobToken::RefusalDetail)
        }
        _ => JobToken::None,
    }
}
