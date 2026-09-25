//! The evidence-fetch check Job — D2 §3.8 option **C**, §3.9, §4.2.
//!
//! # Why a Job reads the evidence, and the controller still decides
//!
//! A `BackupDestination` whose `evidenceRead` grant is `SecretKeys` or
//! `WorkloadIdentity` (or `ArchiveReadGrant`, which resolves to the
//! destination's `archiveRead` grant and so to one of those two) names a
//! credential only a POD may hold: the controller has no verb on `secrets`
//! and must not get one (D2 §3.8 option B, rejected). So the controller asks a
//! short `evidenceFetch` check Job in the object's own namespace to `GET` the
//! two evidence objects with exactly that grant and relay their bytes as
//! framed stdout. The kubelet projects the credential; the controller never
//! sees it.
//!
//! **The Job is a courier, not a verifier.** Everything that decides the
//! verdict happens here, in the controller, over the relayed bytes, through
//! the SAME code the controller's own read-only handle feeds
//! (`verification::verify_fetched`): the digest is recomputed and compared
//! with the digest THE RUNNER reported (never one the relay declared), the
//! document is bound to this run, and the DSSE sidecar is checked against the
//! namespace's resolved trust. A forged relay cannot produce `Valid` without a
//! signing key the installation trusts, and cannot substitute another signed
//! document because the runner's digest pins which bytes this run wrote.
//!
//! # What this module owns, and what it leaves to the reconcilers
//!
//! The Job's lifecycle: its name, its plan `ConfigMap`, its projections, the
//! per-namespace slot, its observation and the relay's interpretation
//! ([`advance`]). The `Backup` and `Restore` reconcilers own what the verdict
//! MEANS on their status — a receipt's window and records, a scorecard's
//! outcome — because those are different documents with different facts.
//!
//! # What the Job is given, exhaustively
//!
//! * the plan `ConfigMap` at `/check` (immutable, owned by the subject,
//!   digest pinned in the environment), carrying the destination's public CA
//!   when it declares one;
//! * the destination's explicit store environment for the `evidenceRead`
//!   grant — and for `SecretKeys` exactly `AWS_ACCESS_KEY_ID`,
//!   `AWS_SECRET_ACCESS_KEY` and optionally `AWS_SESSION_TOKEN` from THAT
//!   grant's Secret, or for `WorkloadIdentity` that grant's ServiceAccount;
//! * nothing else: no signing key, no archive-write or evidence-write grant,
//!   no Kafka credential, and (from [`crate::job::build`]) no ServiceAccount
//!   token automount.

use chrono::{DateTime, Duration, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Event;
use kube::api::{Api, ListParams};
use kube::ResourceExt;
use logweir_core::check_contract::{
    CheckCode, CheckPlan, CheckPlanKind, CheckRelay, CheckRequest, CredentialMode, DestinationPlan,
    EvidenceFetchRequest, EvidenceObjectRequest, EvidenceObjectResult, FrameExpectations, Stream,
    CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT, MAX_EVIDENCE_PAYLOAD_BYTES,
    MAX_EVIDENCE_SIDECAR_BYTES,
};
use logweir_core::destination::DestinationRole;
use serde_json::{json, Value};
use tracing::{debug, info};

use crate::check::{self, job as cjob, limits, plan as cplan, policy::ChecksPolicy};
use crate::crds::ObservedJobRef;
use crate::destination::{ResolvedDestination, ResolvedGrant};
use crate::job::{RunnerImage, RunnerOwner};
use crate::verification::{
    EVIDENCE_FETCH_RELAY_PREFIX as RELAY, EVIDENCE_FETCH_UNREADABLE_PREFIX as UNREADABLE,
};

/// How many Jobs one run's evidence may take: the first attempt and the three
/// retries D2 §3.9 step 5 schedules at +1 m, +5 m and +15 m.
pub const MAX_ATTEMPTS: u32 = 4;

/// The delay before retry `n` (1-based), after attempt `n` did not finish.
pub const RETRY_DELAYS_SECONDS: [i64; 3] = [60, 300, 900];

/// The fetch's own budget. Two objects of at most 1 MiB and 64 KiB; the Job's
/// deadline is this plus [`cjob::DEADLINE_MARGIN_SECONDS`].
pub const FETCH_TIMEOUT_SECONDS: u32 = 120;

/// Where the destination's CA is mounted inside the check pod, when it has one.
#[must_use]
pub fn evidence_ca_path() -> String {
    format!("{}/{}", cjob::CHECK_MOUNT_PATH, cplan::EVIDENCE_CA_KEY)
}

/// The resolved grant's credential mode as it lands on
/// `status.evidence.observation.mode`, or `None` for a grant no Job may use.
#[must_use]
pub fn grant_mode(grant: &ResolvedGrant) -> Option<&'static str> {
    match grant {
        ResolvedGrant::SecretKeys { .. } => Some("SecretKeys"),
        ResolvedGrant::WorkloadIdentity { .. } => Some("WorkloadIdentity"),
        ResolvedGrant::ControllerIdentity | ResolvedGrant::NotConfigured => None,
    }
}

/// When attempt `attempt + 1` may start, or `None` when `attempt` was the last.
#[must_use]
pub fn retry_after(attempt: u32, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if attempt >= MAX_ATTEMPTS {
        return None;
    }
    let index = usize::try_from(attempt.saturating_sub(1)).unwrap_or(usize::MAX);
    RETRY_DELAYS_SECONDS
        .get(index)
        .map(|secs| now + Duration::seconds(*secs))
}

/// The two objects one fetch relays.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    /// The signed document's key, verbatim as the runner printed it.
    pub payload_key: String,
    /// Its detached DSSE sidecar's key.
    pub sidecar_key: String,
}

/// The plan document for one fetch — **pure**.
///
/// # Errors
///
/// A sentence naming what could not be rendered: a grant no Job may hold, or a
/// plan the contract's own `validate` refuses (a programming error).
pub fn plan_documents(
    subject_uid: &str,
    destination: &ResolvedDestination,
    request: &Request,
    policy_digest: Option<&str>,
) -> Result<(CheckPlan, cplan::PlanDocuments), String> {
    let credentials = match &destination.grant {
        ResolvedGrant::SecretKeys { .. } => CredentialMode::Static,
        ResolvedGrant::WorkloadIdentity { .. } => CredentialMode::WorkloadIdentity,
        other => {
            return Err(format!(
                "BackupDestination {}/{} reads evidence with {other:?}, which no evidence-fetch \
                 Job may use",
                destination.namespace, destination.name
            ))
        }
    };
    let ca_path = evidence_ca_path();
    let plan = CheckPlan {
        contract: CHECK_PLAN_CONTRACT.to_string(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: subject_uid.to_string(),
        timeout_seconds: FETCH_TIMEOUT_SECONDS,
        policy_digest: policy_digest.map(str::to_string),
        request: CheckRequest::EvidenceFetch(EvidenceFetchRequest {
            destination: DestinationPlan {
                name: destination.name.clone(),
                uid: destination.uid.clone(),
                location: destination.location.clone(),
                location_digest: destination.location_digest.clone(),
                // THE CA AS BYTES IN THE PLAN, BY PATH IN THE POD: the bytes
                // this resolution read are frozen into the immutable plan
                // `ConfigMap`, never re-read by the pod from the live one.
                ca_file: destination.ca_pem.as_ref().map(|_| ca_path.clone()),
                credentials,
            },
            // EXACTLY TWO OBJECTS, AT THE CONTRACT'S CAPS, WITH THE ONE ROLE
            // THE CONTRACT ALLOWS. `CheckPlan::validate` refuses any other
            // role on the runner's side too.
            objects: vec![
                EvidenceObjectRequest {
                    role: DestinationRole::EvidenceRead,
                    key: request.payload_key.clone(),
                    max_bytes: MAX_EVIDENCE_PAYLOAD_BYTES,
                    stream: Stream::EvidencePayload,
                },
                EvidenceObjectRequest {
                    role: DestinationRole::EvidenceRead,
                    key: request.sidecar_key.clone(),
                    max_bytes: MAX_EVIDENCE_SIDECAR_BYTES,
                    stream: Stream::EvidenceSidecar,
                },
            ],
        }),
    };
    plan.validate().map_err(|e| e.to_string())?;
    let check_plan = serde_json::to_vec(&plan).map_err(|e| e.to_string())?;
    let documents = cplan::PlanDocuments {
        check_plan,
        evidence_ca: destination.ca_pem.clone(),
        ..cplan::PlanDocuments::default()
    };
    Ok((plan, documents))
}

/// The environment variable names an evidence-fetch pod may carry beyond the
/// destination's own store variables — the four the check contract adds and
/// the ones [`crate::job::build`] always adds. Anything else is a leak, and
/// `tests/backup_controller.rs` asserts the rendered Job against this list.
pub const CONTRACT_ENV: [&str; 4] = [
    cjob::CONTRACT_VERSION_ENV,
    cjob::PLAN_SHA256_ENV,
    cjob::SUBJECT_UID_ENV,
    "RUST_LOG",
];

/// The check Job for one attempt — **pure**.
///
/// # THE CREDENTIAL IS THE `evidenceRead` GRANT, AND NOTHING ELSE
///
/// `destination` is a resolution FOR `DestinationRole::EvidenceRead`, so
/// [`ResolvedDestination::job_env`] projects that grant's Secret keys (or runs
/// as that grant's ServiceAccount) and no other. No signer volume, no
/// connection, no second destination. `LOGWEIR_ARCHIVE_CA_FILE` is dropped:
/// it names the execution pod's `/plan` mount, which a check pod does not
/// have — the plan's `caFile` is what the runner reads.
///
/// # Errors
///
/// [`ResolvedDestination::check_job_namespace`]'s refusal, as a sentence: a
/// bare Secret name resolves in the POD's namespace, so a Job anywhere else
/// would project whichever Secret happens to carry that name there.
pub fn job_spec(
    namespace: &str,
    owner: &RunnerOwner,
    attempt: u32,
    destination: &ResolvedDestination,
    documents: &cplan::PlanDocuments,
    image: &RunnerImage,
) -> Result<cjob::CheckJobSpec, String> {
    destination
        .check_job_namespace(namespace)
        .map_err(|refusal| refusal.message.clone())?;
    let env = destination.job_env();
    let mut env_literal: Vec<(String, String)> = env
        .literals
        .into_iter()
        .filter(|(name, _)| name != crate::destination::ARCHIVE_CA_FILE_ENV)
        .collect();
    env_literal.sort();
    let mut env_from_secret = env.from_secret;
    env_from_secret.sort_by(|a, b| a.name.cmp(&b.name));
    let name = cjob::evidence_fetch_job_name(&owner.uid, attempt);
    Ok(cjob::CheckJobSpec {
        kind: CheckPlanKind::EvidenceFetch,
        namespace: namespace.to_string(),
        owner: owner.clone(),
        connection_uid: None,
        plan_config_map: cplan::plan_config_map_name(&name),
        plan_sha256: documents.check_plan_sha256(),
        subject_uid: owner.uid.clone(),
        timeout_seconds: i64::from(FETCH_TIMEOUT_SECONDS),
        service_account_name: env
            .service_account_name
            .unwrap_or_else(|| crate::controllers::backup::RUNNER_SERVICE_ACCOUNT.to_string()),
        secret_mounts: Vec::new(),
        config_map_mounts: Vec::new(),
        env_from_secret,
        env_literal,
        image: image.image.clone(),
        image_pull_policy: image.image_pull_policy.clone(),
        attempt: Some(attempt),
    })
}

/// Whether `job` is controlled by exactly the subject `owner` — kind,
/// `apiVersion` and UID, with `controller: true`.
///
/// A NAME IS NOT AN IDENTITY. The name is a pure function of the subject's
/// UID and the attempt, so anybody who can create Jobs in the namespace can
/// create one with it first; that Job's pod log would then be read as this
/// run's evidence. It is never observed and never adopted.
#[must_use]
pub fn is_owned(job: &Job, owner: &RunnerOwner) -> bool {
    job.metadata
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| {
            o.uid == owner.uid
                && o.kind == owner.kind
                && o.api_version == owner.api_version
                && o.controller == Some(true)
        })
}

/// The plan digest the Job was CREATED with, read off its own pod template.
///
/// The relay must carry the digest of the plan the pod mounted, and the Job is
/// immutable in that field and owned by this subject ([`is_owned`] ran first),
/// so this is that digest — not a re-render of a destination that may have
/// changed since.
#[must_use]
pub fn mounted_plan_digest(job: &Job) -> Option<String> {
    job.spec
        .as_ref()?
        .template
        .spec
        .as_ref()?
        .containers
        .iter()
        .find(|c| c.name == crate::job::CONTAINER_NAME)?
        .env
        .as_ref()?
        .iter()
        .find(|e| e.name == cjob::PLAN_SHA256_ENV)?
        .value
        .clone()
}

/// What the relay said about the two objects — `status.evidence.observation.presence`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Presence {
    /// Both objects were read whole.
    Complete,
    /// The signed document was read and the store answered `NotFound` for its
    /// sidecar — the asymmetric case the exit-4 orphan check is about.
    PayloadWithoutSidecar,
    /// The store answered `NotFound` for the signed document itself.
    Absent,
    /// Anything else: a denial, a truncation, a missing answer.
    Unknown,
}

impl Presence {
    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "Complete",
            Self::PayloadWithoutSidecar => "PayloadWithoutSidecar",
            Self::Absent => "Absent",
            Self::Unknown => "Unknown",
        }
    }
}

/// The two relayed objects, or the reason they cannot be verified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Relayed {
    /// Both objects, whole.
    Both {
        /// The signed document's bytes.
        payload: Vec<u8>,
        /// The DSSE sidecar's bytes.
        sidecar: Vec<u8>,
    },
    /// The relay answered, and what it answered is not a document to verify.
    /// `NotAttempted` with this detail — never `Invalid`, because nothing was
    /// read that could make a claim.
    Unread {
        /// Why. Codes and keys only.
        detail: String,
    },
}

/// One object's answer, as [`read_relay`] classifies it.
enum Answer<'a> {
    Bytes(&'a [u8]),
    NotFound,
    Unreadable(String),
}

fn answer<'a>(
    relay: &'a CheckRelay,
    results: &[EvidenceObjectResult],
    key: &str,
    stream: Stream,
    cap: u64,
) -> Answer<'a> {
    let Some(entry) = results.iter().find(|e| e.key == key && e.stream == stream) else {
        return Answer::Unreadable(format!("{RELAY} carried no answer for {key}"));
    };
    if entry.present {
        // THE CAP IS A REFUSAL, NOT A PREFIX. A truncated object's relayed
        // digest is its prefix's, so verifying it would report a bad document
        // for what is only a big one.
        if entry.truncated {
            return Answer::Unreadable(format!(
                "{key} is larger than the {cap}-byte cap an evidence fetch relays; nothing was \
                 verified"
            ));
        }
        let Some(bytes) = relay.stream(stream) else {
            return Answer::Unreadable(format!(
                "{RELAY} reported {key} present and relayed no bytes for it"
            ));
        };
        // The end frame's per-stream digest is already verified by the
        // decoder; the entry's own digest must name the same bytes, or the
        // relay contradicts itself.
        if entry.sha256.as_deref() != Some(logweir_core::ids::sha256_prefixed(bytes).as_str()) {
            return Answer::Unreadable(format!(
                "{RELAY}'s digest for {key} does not describe the bytes it \
                 relayed"
            ));
        }
        // THE CAP IS THE CONTROLLER'S, NOT ONLY THE RUNNER'S (review MEDIUM-1).
        // `truncated` is the runner's own report, and a pod that relays more
        // than it was asked for without saying so is exactly the pod whose
        // report is not to be trusted. So the controller measures what it
        // received against the contract's per-object cap, and against the
        // length the relay declared for it.
        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if len > cap {
            return Answer::Unreadable(format!(
                "{key} is larger than the {cap}-byte cap an evidence fetch relays ({len} bytes \
                 relayed); nothing was verified"
            ));
        }
        if entry.bytes != Some(len) {
            return Answer::Unreadable(format!(
                "{RELAY} declared {} bytes for {key} and relayed {len}; \
                 nothing was verified",
                entry
                    .bytes
                    .map_or_else(|| "no length".to_string(), |b| b.to_string())
            ));
        }
        return Answer::Bytes(bytes);
    }
    match entry.code {
        Some(CheckCode::ObjectNotFound) => Answer::NotFound,
        Some(code) => Answer::Unreadable(format!(
            "{UNREADABLE}{key} with the evidenceRead grant ({code}); nothing was verified"
        )),
        None => Answer::Unreadable(format!(
            "{RELAY} reported {key} absent without a code; nothing was verified"
        )),
    }
}

/// The relay, read against what was asked — **pure**.
///
/// `present: false` is `Absent` only with the store's own `NotFound`
/// (`ObjectNotFound`); a denial is `Unknown`, never a claim of absence (the
/// contract's own rule for `EvidenceObjectResult::present`).
#[must_use]
pub fn read_relay(relay: &CheckRelay, request: &Request) -> (Presence, Relayed) {
    let results = match relay.result() {
        Some(Ok(doc)) => doc.evidence,
        Some(Err(e)) => {
            return (
                Presence::Unknown,
                Relayed::Unread {
                    detail: format!(
                        "{RELAY}'s result document did not verify ({}); nothing \
                         was verified",
                        e.code()
                    ),
                },
            )
        }
        None => {
            return (
                Presence::Unknown,
                Relayed::Unread {
                    detail: format!("{RELAY} carried no result document; nothing was verified"),
                },
            )
        }
    };
    let payload = answer(
        relay,
        &results,
        &request.payload_key,
        Stream::EvidencePayload,
        MAX_EVIDENCE_PAYLOAD_BYTES,
    );
    let sidecar = answer(
        relay,
        &results,
        &request.sidecar_key,
        Stream::EvidenceSidecar,
        MAX_EVIDENCE_SIDECAR_BYTES,
    );
    match (payload, sidecar) {
        (Answer::Bytes(p), Answer::Bytes(s)) => (
            Presence::Complete,
            Relayed::Both {
                payload: p.to_vec(),
                sidecar: s.to_vec(),
            },
        ),
        (Answer::Bytes(_), Answer::NotFound) => (
            Presence::PayloadWithoutSidecar,
            Relayed::Unread {
                detail: format!(
                    "the evidence object {} is not in the archive; nothing was verified",
                    request.sidecar_key
                ),
            },
        ),
        (Answer::NotFound, _) => (
            Presence::Absent,
            Relayed::Unread {
                detail: format!(
                    "the evidence object {} is not in the archive; nothing was verified",
                    request.payload_key
                ),
            },
        ),
        (Answer::Unreadable(detail), _) | (_, Answer::Unreadable(detail)) => {
            (Presence::Unknown, Relayed::Unread { detail })
        }
    }
}

/// What one pass of [`advance`] found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// No slot under `checks.maxEvidenceFetchActivePerNamespace`; no Job was
    /// created. `Pending`, and the next pass tries again.
    Queued {
        /// The stable sentence the `Pending` verdict carries.
        detail: String,
    },
    /// The Job exists (created this pass or earlier) and has not finished.
    Running {
        /// Its name and UID.
        job_ref: ObservedJobRef,
        /// The stable sentence the `Pending` verdict carries.
        detail: String,
    },
    /// The Job finished and relayed a verified result.
    Relayed {
        /// The Job, for the TTL patch after the status commit.
        job_name: String,
        /// Its UID.
        job_uid: Option<String>,
        /// What the relay said about the two objects.
        presence: Presence,
        /// The bytes, or why there are none.
        relayed: Relayed,
    },
    /// This attempt ended without a verified relay: the pod never ran, the
    /// runner refused, the relay did not decode, the deadline fired.
    /// `NotAttempted` naming the cause, and retried while attempts remain.
    Failed {
        /// The Job, for the TTL patch after the status commit.
        job_name: String,
        /// Its UID.
        job_uid: Option<String>,
        /// The cause. Codes and redacted messages only, never log content.
        detail: String,
    },
    /// Nothing can be fetched for this run, and retrying would not change it:
    /// a Job this subject does not control holds the name, the plan
    /// `ConfigMap` conflicts, or no Job can be rendered. `NotAttempted`,
    /// final.
    Refused {
        /// Why.
        detail: String,
    },
}

/// Everything [`advance`] reads.
pub struct Inputs<'a> {
    /// The API client.
    pub client: &'a kube::Client,
    /// The subject's namespace — the Job's.
    pub namespace: &'a str,
    /// The subject (`Backup` or `Restore`) that owns the Job and its plan.
    pub owner: &'a RunnerOwner,
    /// Which attempt, from 1.
    pub attempt: u32,
    /// What to fetch.
    pub request: &'a Request,
    /// The destination resolved for `evidenceRead`, when it still resolves to
    /// a grant a Job may hold. Needed only to CREATE a Job; an existing one
    /// carries its own frozen plan.
    pub destination: Option<&'a ResolvedDestination>,
    /// Why there is no `destination`, when there is none.
    pub unresolved: Option<&'a str>,
    /// The installation's check limits.
    pub checks: &'a ChecksPolicy,
    /// The installation policy digest, recorded in the plan.
    pub policy_digest: Option<&'a str>,
    /// The runner image.
    pub image: &'a RunnerImage,
    /// This pass's instant.
    pub now: DateTime<Utc>,
}

/// The events for one Job and its pod, by `involvedObject.uid` — never by
/// name, so an unrelated workload's `FailedCreate` cannot cancel this Job.
async fn events_for(
    client: &kube::Client,
    namespace: &str,
    uids: &[String],
) -> Result<Vec<check::EventFact>, kube::Error> {
    let api: Api<Event> = Api::namespaced(client.clone(), namespace);
    let mut out = Vec::new();
    for uid in uids.iter().filter(|u| !u.is_empty()) {
        let list = api
            .list(&ListParams::default().fields(&format!("involvedObject.uid={uid}")))
            .await?;
        out.extend(list.items.iter().filter_map(check::EventFact::from_event));
    }
    Ok(out)
}

fn job_ref(job: &Job) -> ObservedJobRef {
    ObservedJobRef {
        name: Some(job.name_any()),
        uid: job.uid(),
    }
}

fn running_detail(namespace: &str, job_name: &str, attempt: u32) -> String {
    format!(
        "evidence-fetch Job {namespace}/{job_name} (attempt {attempt} of {MAX_ATTEMPTS}) is \
         reading this run's evidence with the destination's evidenceRead grant; the controller \
         verifies what it relays"
    )
}

fn foreign_detail(namespace: &str, job_name: &str, owner: &RunnerOwner) -> String {
    format!(
        "Job {namespace}/{job_name} already exists and is not controlled by {} {namespace}/{} \
         with UID {}; it was neither observed nor adopted, so this run's evidence was not \
         fetched. Run the printed logweir drill verify command instead",
        owner.kind, owner.name, owner.uid
    )
}

/// One pass over one attempt's evidence-fetch Job.
///
/// # Idempotent across restarts, by construction
///
/// The Job's name is [`cjob::evidence_fetch_job_name`] of the subject's UID
/// and the attempt, so a pass that finds no status record of a Job it already
/// created recomputes the same name and FINDS it (`GET` first, and a `409` on
/// create is resolved the same way). One attempt, one Job — never two.
///
/// # SEC-PODLOG
///
/// The pod log is read only through [`check::observe`], which reads only a
/// pod whose controller owner is THIS Job's UID; a pod wearing the Job's label
/// that another principal created is ignored.
///
/// # Errors
///
/// [`kube::Error`] for an API failure. Every answer about the fetch itself is
/// a [`Step`].
pub async fn advance(inputs: &Inputs<'_>) -> Result<Step, kube::Error> {
    let namespace = inputs.namespace;
    let name = cjob::evidence_fetch_job_name(&inputs.owner.uid, inputs.attempt);
    let jobs: Api<Job> = Api::namespaced(inputs.client.clone(), namespace);

    if let Some(job) = jobs.get_opt(&name).await? {
        return observe(inputs, &job).await;
    }

    let Some(destination) = inputs.destination else {
        return Ok(Step::Refused {
            detail: inputs.unresolved.map_or_else(
                || {
                    "the destination no longer resolves to an evidenceRead grant a Job may hold; \
                    nothing was fetched"
                        .to_string()
                },
                str::to_string,
            ),
        });
    };

    // THE SLOT, BEFORE ANYTHING IS WRITTEN. A separate per-namespace pool, so
    // verification cannot be starved by interactive checks (D2 §4.3).
    let counts = limits::count(&limits::check_jobs(inputs.client).await?, namespace, None);
    if let limits::Admission::Queued(code) =
        limits::admit(&counts, inputs.checks, CheckPlanKind::EvidenceFetch)
    {
        return Ok(Step::Queued {
            detail: format!(
                "waiting for an evidence-fetch slot ({code}): this namespace already runs \
                 checks.maxEvidenceFetchActivePerNamespace = {} evidence-fetch Jobs; attempt {} \
                 starts when one finishes",
                inputs.checks.max_evidence_fetch_active_per_namespace, inputs.attempt
            ),
        });
    }

    let (_plan, documents) = match plan_documents(
        &inputs.owner.uid,
        destination,
        inputs.request,
        inputs.policy_digest,
    ) {
        Ok(rendered) => rendered,
        Err(detail) => return Ok(Step::Refused { detail }),
    };
    let spec = match job_spec(
        namespace,
        inputs.owner,
        inputs.attempt,
        destination,
        &documents,
        inputs.image,
    ) {
        Ok(spec) => spec,
        Err(detail) => return Ok(Step::Refused { detail }),
    };

    // THE PLAN BEFORE THE JOB: a Job whose `/check` mount does not exist yet
    // is a pod stuck in `ContainerCreating`.
    let config_map = match cplan::build(&name, namespace, inputs.owner, &documents) {
        Ok(cm) => cm,
        Err(e) => {
            return Ok(Step::Refused {
                detail: format!("{}: {e}", e.code()),
            })
        }
    };
    match cplan::ensure(
        inputs.client,
        namespace,
        &config_map,
        &inputs.owner.uid,
        &documents.check_plan_sha256(),
    )
    .await
    {
        Ok(_) => {}
        Err(cplan::EnsureError::Api(e)) => return Err(e),
        Err(cplan::EnsureError::Plan(e)) => {
            return Ok(Step::Refused {
                detail: format!("{}: {e}", e.code()),
            })
        }
    }

    let job = match check::create_job(inputs.client, &spec).await {
        Ok(job) => job,
        // A 409 IS A CONCURRENT PASS THAT GOT THERE FIRST — or a squatter.
        // The name alone says nothing; ownership decides.
        Err(kube::Error::Api(e)) if e.code == 409 => match jobs.get_opt(&name).await? {
            Some(job) if is_owned(&job, inputs.owner) => job,
            Some(_) => {
                return Ok(Step::Refused {
                    detail: foreign_detail(namespace, &name, inputs.owner),
                })
            }
            None => return Err(kube::Error::Api(e)),
        },
        Err(e) => return Err(e),
    };
    info!(
        namespace = %namespace,
        job = %name,
        owner = %inputs.owner.name,
        attempt = inputs.attempt,
        "evidence-fetch Job created"
    );
    Ok(Step::Running {
        job_ref: job_ref(&job),
        detail: running_detail(namespace, &name, inputs.attempt),
    })
}

async fn observe(inputs: &Inputs<'_>, job: &Job) -> Result<Step, kube::Error> {
    let namespace = inputs.namespace;
    let name = job.name_any();
    if !is_owned(job, inputs.owner) {
        return Ok(Step::Refused {
            detail: foreign_detail(namespace, &name, inputs.owner),
        });
    }
    let pod = check::pod::find_owned_pod(inputs.client, namespace, job).await?;
    let events = events_for(
        inputs.client,
        namespace,
        &[
            job.uid().unwrap_or_default(),
            pod.as_ref().and_then(ResourceExt::uid).unwrap_or_default(),
        ],
    )
    .await?;
    let expect = FrameExpectations {
        plan_sha256: mounted_plan_digest(job).unwrap_or_default(),
        subject_uid: inputs.owner.uid.clone(),
    };
    let observation =
        check::observe(inputs.client, namespace, job, &events, &expect, inputs.now).await?;
    if observation.cancel_now {
        check::cancel(inputs.client, namespace, job, &inputs.owner.uid).await?;
    }
    match observation.phase {
        check::CheckPhase::Running => Ok(Step::Running {
            job_ref: job_ref(job),
            detail: running_detail(namespace, &name, inputs.attempt),
        }),
        check::CheckPhase::Failed => {
            debug!(
                namespace = %namespace,
                job = %name,
                reason = %observation.reason,
                "the evidence-fetch Job ended without a verified relay"
            );
            Ok(Step::Failed {
                job_name: name.clone(),
                job_uid: job.uid(),
                detail: format!(
                    "evidence-fetch Job {namespace}/{name} (attempt {} of {MAX_ATTEMPTS}) ended \
                     without a verified relay: {}: {}",
                    inputs.attempt,
                    observation.reason,
                    logweir_core::check_contract::redact(&observation.message)
                ),
            })
        }
        check::CheckPhase::Succeeded => {
            let Some(relay) = observation.relay.as_ref() else {
                // `classify` never returns `Succeeded` without a relay; said
                // here so a future change cannot turn this into a verdict.
                return Ok(Step::Failed {
                    job_name: name.clone(),
                    job_uid: job.uid(),
                    detail: format!(
                        "evidence-fetch Job {namespace}/{name} reported success with no relay"
                    ),
                });
            };
            let (presence, relayed) = read_relay(relay, inputs.request);
            Ok(Step::Relayed {
                job_name: name,
                job_uid: job.uid(),
                presence,
                relayed,
            })
        }
    }
}

/// `status.evidence.observation` as a merge PATCH value: every key written,
/// the absent ones as explicit `null`, so a later attempt never inherits an
/// earlier attempt's Job, presence or retry instant.
#[must_use]
pub fn observation_patch(
    mode: Option<&str>,
    job_ref: Option<&ObservedJobRef>,
    attempt: u32,
    presence: Option<Presence>,
    retry_after: Option<DateTime<Utc>>,
) -> Value {
    json!({
        "mode": mode,
        "jobRef": job_ref.map(|r| json!({ "name": r.name, "uid": r.uid })),
        "attempt": attempt,
        "presence": presence.map(Presence::as_str),
        "retryAfter": retry_after
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
    })
}

/// Which attempt a TERMINAL object owes, from its stored status — or `None`
/// when no fetch is owed.
///
/// * `Pending` with an observation: the recorded attempt continues.
/// * `NotAttempted` with an observation carrying `retryAfter`: the NEXT
///   attempt, once `retryAfter` has passed; nothing before.
/// * anything else: the verdict is reached, or the attempts are spent, or this
///   run's evidence was never the Job's to read.
#[must_use]
pub fn owed_attempt(
    result: Option<&str>,
    observation: Option<&crate::crds::EvidenceObservation>,
    now: DateTime<Utc>,
) -> Option<u32> {
    let observation = observation?;
    let recorded = observation
        .attempt
        .and_then(|a| u32::try_from(a).ok())
        .filter(|a| *a >= 1)
        .unwrap_or(1);
    match result {
        Some("Pending") => Some(recorded),
        Some("NotAttempted") => {
            let at = observation.retry_after?;
            (now >= at && recorded < MAX_ATTEMPTS).then_some(recorded + 1)
        }
        _ => None,
    }
}

/// The ONE status patch an evidence-fetch pass writes — **pure**.
///
/// `status.evidence.verification` (as a merge PATCH, explicit nulls for what
/// this verdict does not hold), `status.evidence.observation`, any `evidence`
/// facts (`evidence_facts`, e.g. a scorecard's digests), any top-level facts
/// (`facts`: a receipt's window, records, capture; a scorecard's outcome), and
/// the WHOLE condition list with `Verified` merged in — a merge PATCH replaces
/// arrays, and `conditions` is the object's own stored list, so nothing
/// another pass wrote is dropped.
///
/// The badge is computed over the status that WILL exist: the stored one with
/// the facts and this verdict applied, because the `Restore` rule reads the
/// `outcome` this very patch may be the first to write.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn verdict_patch(
    current: Option<&Value>,
    conditions: &[crate::crds::Condition],
    generation: Option<i64>,
    result: &crate::verification::VerificationResult,
    observation: Value,
    evidence_facts: serde_json::Map<String, Value>,
    facts: serde_json::Map<String, Value>,
    badge: fn(&Value) -> crate::verification::Badge,
    now: DateTime<Utc>,
) -> (Value, crate::verification::Badge) {
    let block = result.to_status_value(crate::verification::stored_verification(current));
    let mut projected = current.cloned().unwrap_or_else(|| json!({}));
    for (key, value) in &facts {
        projected[key.as_str()] = value.clone();
    }
    if !projected.get("evidence").is_some_and(Value::is_object) {
        projected["evidence"] = json!({});
    }
    for (key, value) in &evidence_facts {
        projected["evidence"][key.as_str()] = value.clone();
    }
    projected["evidence"]["verification"] = block.clone();
    let badge = badge(&projected);
    let verified = crate::verification::verified_condition(
        &badge,
        conditions
            .iter()
            .find(|c| c.r#type == crate::conditions::CONDITION_VERIFIED),
        generation,
        now,
    );
    let mut patch = crate::verification::second_patch(
        conditions,
        verified,
        crate::verification::verification_patch_value(block),
    );
    if let Some(status) = patch.get_mut("status").and_then(Value::as_object_mut) {
        if let Some(evidence) = status.get_mut("evidence").and_then(Value::as_object_mut) {
            evidence.insert("observation".to_string(), observation);
            for (key, value) in evidence_facts {
                evidence.insert(key, value);
            }
        }
        for (key, value) in facts {
            status.insert(key, value);
        }
    }
    (patch, badge)
}

/// A failed attempt's `NotAttempted` detail, naming what happens next.
#[must_use]
pub fn failed_detail(detail: &str, attempt: u32, retry: Option<DateTime<Utc>>) -> String {
    match retry {
        Some(t) => format!(
            "{detail}; attempt {} starts at {}",
            attempt + 1,
            t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
        None => {
            format!("{detail}; no attempts remain — run the printed logweir drill verify command")
        }
    }
}

/// Seconds until a TERMINAL object's scheduled retry may start, when one is
/// scheduled and has not been reached — so a reconciler that otherwise waits
/// for a change (the `Restore`'s `AwaitChange`) comes back for it.
#[must_use]
pub fn retry_due_in(
    result: Option<&str>,
    observation: Option<&crate::crds::EvidenceObservation>,
    now: DateTime<Utc>,
) -> Option<u64> {
    let observation = observation?;
    if result != Some("NotAttempted") {
        return None;
    }
    let at = observation.retry_after?;
    let secs = (at - now).num_seconds().max(1);
    u64::try_from(secs).ok()
}

/// How long after a verdict a terminal pass still checks that the recorded
/// Job got its TTL.
///
/// An HOUR, and bounded on purpose (review LOW-4). The TTL is patched only
/// after the verdict commits; a `set_ttl` that fails makes that pass return an
/// error, and the controller's error policy requeues it within seconds, so a
/// transient failure is repaired on the next pass inside this window. A
/// controller that dies between the two writes and stays down longer than
/// this leaves the Job to ownerReference garbage collection when its `Backup`
/// or `Restore` goes — at most [`MAX_ATTEMPTS`] small finished Jobs per run.
/// A window rather than "forever" because a terminal `Backup` is reconciled
/// every 15 seconds for its whole life, and one `GET` per pass per verified
/// run, forever, is a cost with no finding behind it.
pub const TTL_REPAIR_WINDOW_SECONDS: i64 = 3600;

/// Patch the TTL onto the recorded evidence-fetch Job when the verdict has
/// been committed and the Job never got one — review LOW-4.
///
/// Sends nothing unless ALL hold: the verdict is not `Pending` (the Job's
/// relay may still be needed), it was reached within
/// [`TTL_REPAIR_WINDOW_SECONDS`], the recorded Job exists with the recorded
/// UID, is controlled by `owner`, has finished and carries no TTL. A Job this
/// subject does not control is never patched.
///
/// # Errors
///
/// [`kube::Error`] from the `GET` or the `PATCH`.
pub async fn repair_ttl(
    client: &kube::Client,
    namespace: &str,
    owner: &RunnerOwner,
    result: Option<&str>,
    verified_at: Option<DateTime<Utc>>,
    observation: Option<&crate::crds::EvidenceObservation>,
    now: DateTime<Utc>,
) -> Result<bool, kube::Error> {
    if matches!(result, None | Some("Pending")) {
        return Ok(false);
    }
    let Some(at) = verified_at else {
        return Ok(false);
    };
    if (now - at).num_seconds() > TTL_REPAIR_WINDOW_SECONDS {
        return Ok(false);
    }
    let Some(recorded) = observation.and_then(|o| o.job_ref.as_ref()) else {
        return Ok(false);
    };
    let Some(name) = recorded.name.as_deref() else {
        return Ok(false);
    };
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let Some(job) = jobs.get_opt(name).await? else {
        return Ok(false);
    };
    let same_job = recorded.uid.is_none() || job.uid() == recorded.uid;
    let has_ttl = job
        .spec
        .as_ref()
        .and_then(|s| s.ttl_seconds_after_finished)
        .is_some();
    if !same_job
        || !is_owned(&job, owner)
        || !crate::controllers::backup::job_finished(&job)
        || has_ttl
    {
        return Ok(false);
    }
    check::set_ttl(client, namespace, name).await?;
    info!(
        namespace = %namespace,
        job = %name,
        "the evidence verdict was committed and its Job carried no TTL; the TTL is repaired"
    );
    Ok(true)
}

// ---------------------------------------------------------------------------
// The controller's OWN read, retried — PoC defect P12
// ---------------------------------------------------------------------------
//
// A `Backup` or `Restore` whose evidence the CONTROLLER reads — an inline-
// archive run through the one global handle (D2 §3.10), or a destination whose
// `evidenceRead` is `ControllerIdentity` (D2 §3.8 option E) — used to get one
// read, on the pass that made it terminal. When that read failed, the
// `NotAttempted` it wrote was final: the PoC upgrade round's three pre-upgrade
// inline-archive points read `NotAttempted` because the controller had no
// credential (the SDK fell back to IMDS), stayed so after `logweir-evidence-ro`
// was created and the controller restarted twice, and were therefore never
// recovery points. A destination-backed run whose evidence a JOB reads was
// already retried at +1 m, +5 m and +15 m (D2 §3.9 step 5, above).
//
// The controller's own read now takes the SAME schedule, recorded in the SAME
// `status.evidence.observation` fields — `mode`, `attempt`, `retryAfter` — so
// every reader that already understands a scheduled fetch (the rehearsal
// schedule's `verdict_owed`, `retry_due_in`) understands this one. Two things
// differ, and both are about what an input change looks like for a read the
// controller makes itself:
//
// * only a TRANSIENT failure is scheduled (`verification::NotAttemptedClass`):
//   a definite `NotFound` is a fact about the archive and is never read again,
//   and a configuration refusal waits for the configuration to change;
// * the controller's credential reaches it through its own environment
//   (`logweir-evidence-ro`, projected `optional: true`), so a created or
//   rotated credential arrives with a new controller PROCESS. Each process
//   therefore reads every eligible unverified run ONCE more after its
//   schedule is spent ([`ReadMemory`]) — which is also how a `NotAttempted`
//   written before this rule existed (no observation at all) is read again
//   after the upgrade.
//
// FAIL-CLOSED THROUGHOUT: a retry runs the terminal pass's own reader and
// verifier and writes whatever they answer. Nothing here can write `Valid`;
// only `verify_oracle`/`verify_evidence` over fetched bytes can.

/// `status.evidence.observation.mode` for a read through the controller's own
/// inline-archive handle (`LOGWEIR_ARCHIVE_URL`, D2 §3.10).
pub const MODE_ARCHIVE_HANDLE: &str = "ArchiveHandle";

/// `status.evidence.observation.mode` for a read through a destination's
/// `ControllerIdentity` handle (`evidence_store::StoreCache`, D2 §3.8 E).
pub const MODE_CONTROLLER_IDENTITY: &str = "ControllerIdentity";

/// Whether a stored observation records a read the CONTROLLER made, rather
/// than an evidence-fetch Job's — the two schedules must never be confused:
/// [`owed_attempt`] would otherwise start a Job for a run whose grant no pod
/// holds.
#[must_use]
pub fn is_controller_read(mode: Option<&str>) -> bool {
    matches!(mode, Some(MODE_ARCHIVE_HANDLE | MODE_CONTROLLER_IDENTITY))
}

/// The most distinct objects one controller process remembers having read —
/// [`ReadMemory`]'s bound.
pub const READ_MEMORY_CAPACITY: usize = 100_000;

/// Which terminal objects THIS controller process has already read (by UID).
///
/// # Why process memory, when the retry backoff is on the object
///
/// The schedule (`attempt`, `retryAfter`) is on the object, so a restart can
/// neither shorten nor forget it. What a restart DOES change is the
/// controller's own credential: `logweir-evidence-ro` is projected into the
/// controller's environment, which a running process never re-reads. A run
/// whose schedule was spent under the old credential is therefore worth
/// exactly one more read under the new one, and "has this process read it
/// yet" is the exact question — a new process has read nothing, and the
/// question costs no field on the object and no clock comparison. (A stored
/// `verifiedAt` cannot answer it: an unchanged verdict keeps its old
/// `verifiedAt` by design — erratum E11(d) — so a comparison with the process
/// start would re-read such a run on every pass.)
///
/// # Bounded
///
/// Remembering is what bounds the re-read to ONE per object per process, so
/// the memory is never emptied: at [`READ_MEMORY_CAPACITY`] distinct objects
/// it stops admitting new ones, and an object it cannot remember is treated
/// as already read (no re-read), never as new. A crash-looping controller
/// re-reads each eligible run once per start, which is the cost of one
/// terminal pass and no more.
#[derive(Debug, Default)]
pub struct ReadMemory {
    read: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl ReadMemory {
    /// An empty memory — what a new process holds, and what a row passes.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The ONE memory the running controller's `Backup` and `Restore`
    /// reconcilers share (UIDs do not collide across kinds).
    pub fn global() -> &'static Self {
        static MEMORY: std::sync::OnceLock<ReadMemory> = std::sync::OnceLock::new();
        MEMORY.get_or_init(ReadMemory::new)
    }

    /// Whether this process has not read `uid` yet — and can still remember
    /// it once it has. Does not record anything: [`Self::mark`] does, after
    /// the read's verdict is written.
    #[must_use]
    pub fn unread(&self, uid: &str) -> bool {
        let read = self
            .read
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !read.contains(uid) && read.len() < READ_MEMORY_CAPACITY
    }

    /// Record that this process has read `uid`.
    pub fn mark(&self, uid: &str) {
        let mut read = self
            .read
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if read.len() < READ_MEMORY_CAPACITY {
            read.insert(uid.to_string());
        }
    }
}

/// One controller read's verdict, as it is written: a TRANSIENT `NotAttempted`
/// gains the schedule — the next attempt's instant in its sentence and in
/// `retryAfter` — and every other verdict is returned unchanged with no retry.
///
/// Returns the verdict to write and the `retryAfter` to record.
#[must_use]
pub fn scheduled(
    result: crate::verification::VerificationResult,
    attempt: u32,
    now: DateTime<Utc>,
) -> (
    crate::verification::VerificationResult,
    Option<DateTime<Utc>>,
) {
    use crate::verification::{not_attempted_class, NotAttemptedClass, VerificationVerdict};
    let transient = result.result == VerificationVerdict::NotAttempted
        && result
            .detail
            .as_deref()
            .is_some_and(|d| not_attempted_class(d) == NotAttemptedClass::Transient);
    if !transient {
        return (result, None);
    }
    let retry = retry_after(attempt, now);
    let detail =
        controller_read_detail(result.detail.as_deref().unwrap_or_default(), attempt, retry);
    (
        crate::verification::VerificationResult {
            detail: Some(detail),
            ..result
        },
        retry,
    )
}

/// A RELAYED attempt's verdict, with the schedule when it failed transiently —
/// PoC P12's class sweep of the Job path. A relay the store answered with a
/// denial or a timeout, or one whose own framing did not hold (D2 §3.9 step
/// 5's "relay unreadable"), used to be final after one Job; it now takes the
/// same +1 m, +5 m, +15 m schedule a Job that did not finish takes. A relay
/// that says the document is not there, or that it is over the relay's cap,
/// is final as before.
#[must_use]
pub fn relayed_schedule(
    result: crate::verification::VerificationResult,
    attempt: u32,
    now: DateTime<Utc>,
) -> (
    crate::verification::VerificationResult,
    Option<DateTime<Utc>>,
) {
    use crate::verification::{not_attempted_class, NotAttemptedClass, VerificationVerdict};
    let transient = result.result == VerificationVerdict::NotAttempted
        && result
            .detail
            .as_deref()
            .is_some_and(|d| not_attempted_class(d) == NotAttemptedClass::Transient);
    if !transient {
        return (result, None);
    }
    let retry = retry_after(attempt, now);
    let detail = failed_detail(result.detail.as_deref().unwrap_or_default(), attempt, retry);
    (
        crate::verification::VerificationResult {
            detail: Some(detail),
            ..result
        },
        retry,
    )
}

/// A failed controller read's sentence, naming what happens next — the twin of
/// [`failed_detail`], whose spent sentence would be wrong here: a controller
/// read IS tried again, by the next controller process.
#[must_use]
pub fn controller_read_detail(detail: &str, attempt: u32, retry: Option<DateTime<Utc>>) -> String {
    match retry {
        Some(t) => format!(
            "{detail}; attempt {} starts at {}",
            attempt + 1,
            t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
        None => format!(
            "{detail}; attempt {attempt} of {MAX_ATTEMPTS} was the last this controller process \
             makes. The controller reads it again when it next starts, which is when a created \
             or rotated logweir-evidence-ro reaches it; or run the printed logweir drill verify \
             command"
        ),
    }
}

/// Which controller-read attempt a TERMINAL object owes now, from its stored
/// verdict — `None` when none is owed.
///
/// `inline` is whether the object is an inline-archive run (no destination
/// reference), and the caller has already checked what makes a read possible
/// at all: both evidence keys named, the runner's digest recorded (a
/// `Backup`), the archive handle configured and its bucket the run's own.
///
/// * a `NotAttempted` whose observation is a controller read's, with a
///   `retryAfter` that has passed and attempts left: the NEXT attempt; before
///   `retryAfter`, nothing — THE RATE BOUND: between two attempts a terminal
///   object's reconciles read nothing and write nothing;
/// * a controller read's `NotAttempted` with no `retryAfter` (the schedule is
///   spent, or the failure was not transient), or an inline-archive run's
///   `NotAttempted` with no observation at all (written before this rule, or a
///   configuration refusal): attempt 1 of a new schedule, ONCE per controller
///   process ([`ReadMemory`]) — never for a definite absence;
/// * a destination-backed `NotAttempted` with no observation: the same, but
///   only when its sentence is a transient READ failure — a destination that
///   names no `evidenceRead`, or one the policy does not allowlist, is not a
///   read and is not re-read;
/// * an observation that is an evidence-fetch Job's: nothing here
///   ([`owed_attempt`] owns it);
/// * anything that is not `NotAttempted`: nothing — a reached verdict is final.
#[must_use]
pub fn owed_controller_read(
    result: Option<&str>,
    detail: Option<&str>,
    observation: Option<&crate::crds::EvidenceObservation>,
    inline: bool,
    now: DateTime<Utc>,
    memory: Option<(&ReadMemory, &str)>,
) -> Option<u32> {
    use crate::verification::{not_attempted_class, NotAttemptedClass};
    if result != Some("NotAttempted") {
        return None;
    }
    let class = not_attempted_class(detail.unwrap_or_default());
    if class == NotAttemptedClass::Absent {
        return None;
    }
    let reread = || {
        memory
            .is_some_and(|(memory, uid)| memory.unread(uid))
            .then_some(1)
    };
    match observation {
        Some(o) if is_controller_read(o.mode.as_deref()) => match o.retry_after {
            Some(at) => {
                let recorded = o
                    .attempt
                    .and_then(|a| u32::try_from(a).ok())
                    .filter(|a| *a >= 1)
                    .unwrap_or(1);
                (now >= at && recorded < MAX_ATTEMPTS).then_some(recorded + 1)
            }
            None => reread(),
        },
        Some(_) => None,
        None if inline || class == NotAttemptedClass::Transient => reread(),
        None => None,
    }
}
