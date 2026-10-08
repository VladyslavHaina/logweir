//! Persistent installation identity bootstrap for the Helm chart.
//!
//! This code deliberately lives in the signer-capable `logweir` runner
//! binary. The chart runs it in a short-lived hook Job whose Role can only get
//! and patch the retained identity objects. The long-lived controller and UI
//! receive neither this code nor Secret-read permission.
//!
//! # PROD-16.1: three more things, all create-once
//!
//! * **The console's ConsoleConfirmation key** ([`ConsoleKeyArgs`]): the same
//!   lifecycle as the installation identity — a retained Secret filled exactly
//!   once (an Ed25519 key generated here, or the key a hand-made Secret of
//!   that name already holds, which is ADOPTED), its public half published in
//!   a retained ConfigMap, never regenerated, and a published public half
//!   without its private key is KEY LOSS, which stops the hook.
//! * **The installation `TrustPolicy`** ([`InstallationTrustArgs`]): ONLY in
//!   the run that GENERATES the installation identity — a fresh install; an
//!   upgraded install's identity already existed, and an adopted one may
//!   have — and ONLY for a console whose key this same install GENERATED
//!   (the fix round of 2026-10-08: no console, or an adopted console key,
//!   creates nothing), one cluster-scoped `default: true` policy trusting
//!   exactly the installation signer (`EvidenceSigning`) and that console
//!   key (`ConsoleConfirmation`), so a first restore reaches a verified
//!   result with no key handled by a person. Never when the cluster's trust
//!   is already administered (ANY TrustPolicy, or `TrustRoster/default`);
//!   never modified afterwards — its keys are append-only and its lifecycle
//!   is the trust administrator's. A retried hook that finds the identity
//!   already generated does NOT finish this step: an install whose first
//!   run was interrupted starts strict, the fail-safe side.
//! * **No standing grant, on every exit path this binary controls**
//!   ([`run_guarded`]): the install-only ClusterRoleBinding that lets this
//!   account create that policy is deleted by name after the steps, after a
//!   failed step, after a store that could not be opened, after a panic, and
//!   (`main.rs`) after a usage error — a chart newer than this image.
//! * **The fresh-install marker** ([`APPROVAL_DEFAULT_ANNOTATION`]): in that
//!   same run, after the policy exists, a [`MarkerClaim`] on the public
//!   identity ConfigMap naming the policy by name and UID and both key ids.
//!   The console and the controller honour it only beside that exact,
//!   hook-made trust entry (`logweir_core::approval_policy::verify_marker`),
//!   so an annotation patched into an older install changes nothing — the
//!   PROD-16.1 security review's requirement that no single object edit can
//!   do what two gates did before.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use chrono::{DateTime, Utc};
use logweir_core::approval_policy::{
    MarkerClaim, APPROVAL_DEFAULT_ANNOTATION, APPROVAL_DEFAULT_CONFIRM, CREATED_BY_ANNOTATION,
    CREATED_BY_IDENTITY_BOOTSTRAP,
};
use logweir_evidence::keys::{KeyAlg, SigningKey};
use serde_json::{json, Value};

use crate::exit::ExitCode;

const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Where the hook reads its ServiceAccount token and the cluster CA when NOT
/// in a pod: a directory holding `token` and `ca.crt`, with
/// `KUBERNETES_SERVICE_HOST`/`KUBERNETES_SERVICE_PORT_HTTPS` naming the API
/// server. The chart never sets it; it exists so the same binary can run the
/// same steps from a workstation under a narrowly bound account's token
/// (PROD-16.1's host proof, a break-glass re-run). It grants nothing: the
/// token's own RBAC is the whole authority.
const SERVICE_ACCOUNT_DIR_ENV: &str = "LOGWEIR_SERVICE_ACCOUNT_DIR";
const IDENTITY_STATE_ANNOTATION: &str = "logweir.dev/identity-state";
const ESTABLISHED: &str = "established";
const PUBLIC_KEY_ID: &str = "key-id";
const PUBLIC_KEY_PEM: &str = "signing.pub.pem";
const PUBLIC_ALGORITHM: &str = "algorithm";
const TRUST_REFERENCE: &str = "trust-reference";
const DEFAULT_TRUST_REFERENCE: &str = "logweir.dev/v1alpha1/TrustRoster/default#spec.signingKeys";

/// PROD-16.1: on the public identity ConfigMap, what the fresh install's
/// trust step did — a record for the operator. Nothing reads it to decide
/// anything: the step runs only in the identity-generating run.
const INSTALLATION_TRUST_ANNOTATION: &str = "logweir.dev/installation-trust";

/// What a stray marker on a placeholder is overwritten with when this run
/// makes no claim: not a [`MarkerClaim`], so never honoured.
const NO_CLAIM: &str = "none";

/// How many times the fresh install's trust step is tried within its one run
/// before the hook fails: it cannot be finished by a later run.
const TRUST_ATTEMPTS: u32 = 3;

/// The pause between two attempts (none in the rows).
const TRUST_RETRY_DELAY: Duration = if cfg!(test) {
    Duration::ZERO
} else {
    Duration::from_secs(2)
};

/// The console key's public record: the same shape as the identity's, with
/// its own PEM key and, in place of the roster reference, the one usage the
/// key may be trusted for.
const CONSOLE_PUBLIC_KEY_PEM: &str = "confirmation.pub.pem";
const CONSOLE_TRUST_USAGE: &str = "trust-usage";
const CONSOLE_CONFIRMATION_USAGE: &str = "ConsoleConfirmation";

/// The `notAfter` of the keys the installation `TrustPolicy` declares: the
/// installation identity has no expiry (a roster entry without `notAfter` is
/// read the same way), and an administrator may only ever bring it FORWARD
/// (CEL rule G2) when rotating.
const INSTALLATION_TRUST_NOT_AFTER: &str = "9999-12-31T23:59:59Z";

/// How far before its Secret's creation a key's `notBefore` opens: the clock
/// of this hook and of the controller that judges a signing time are two
/// clocks.
const NOT_BEFORE_SKEW_SECONDS: i64 = 300;

#[derive(Debug)]
pub struct BootstrapArgs {
    pub namespace: String,
    pub secret_name: String,
    pub secret_key: String,
    pub public_configmap_name: String,
    pub external_secret: Option<(String, String)>,
    /// PROD-16.1: mark a GENERATED identity as a fresh install whose unbound
    /// namespaces start in `confirm`.
    pub mark_fresh_install_confirm: bool,
    /// PROD-16.1: the managed console confirmation key, when the chart
    /// manages one.
    pub console: Option<ConsoleKeyArgs>,
    /// PROD-16.1: the installation `TrustPolicy`, when the chart asks for one.
    pub installation_trust: Option<InstallationTrustArgs>,
    /// PROD-16.1 security review: the install-only ClusterRoleBinding that
    /// grants the trust step, deleted at the end of every run.
    pub revoke_trust_binding: Option<String>,
}

/// The retained objects of the console's `ConsoleConfirmation` key.
#[derive(Debug, Clone)]
pub struct ConsoleKeyArgs {
    pub secret_name: String,
    pub secret_key: String,
    pub public_configmap_name: String,
}

/// The installation `TrustPolicy` a fresh install creates.
#[derive(Debug, Clone)]
pub struct InstallationTrustArgs {
    pub policy_name: String,
    pub allowed_target_cluster_ids: Vec<String>,
}

#[derive(Debug)]
pub struct DistributeArgs {
    pub source_namespace: String,
    pub target_namespace: String,
    pub secret_name: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Generated,
    Adopted,
    Existing,
    ConcurrentWinner(u16),
}

impl Origin {
    fn as_str(self) -> &'static str {
        match self {
            Self::Generated => "generated",
            Self::Adopted => "adopted",
            Self::Existing => "existing",
            Self::ConcurrentWinner(_) => "concurrent-existing",
        }
    }

    fn contention_status(self) -> Option<u16> {
        match self {
            Self::ConcurrentWinner(status) => Some(status),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct Outcome {
    key_id: String,
    origin: Origin,
}

#[derive(Debug, Clone)]
struct PrivateRecord {
    resource_version: String,
    state: Option<String>,
    pem: Option<String>,
    has_any_data: bool,
    has_annotations: bool,
    /// `metadata.creationTimestamp`: the earliest instant the key it holds
    /// can have existed.
    created_at: Option<String>,
}

#[derive(Debug, Clone)]
struct PublicRecord {
    resource_version: String,
    state: Option<String>,
    key_id: Option<String>,
    spki_pem: Option<String>,
    algorithm: Option<String>,
    /// The identity's `trust-reference`, or the console key's `trust-usage`
    /// ([`PublicLayout::reference_key`]).
    trust_reference: Option<String>,
    data_keys: BTreeSet<String>,
    has_any_data: bool,
    has_annotations: bool,
    /// PROD-16.1: every annotation.
    annotations: BTreeMap<String, String>,
}

impl PublicRecord {
    fn annotation(&self, key: &str) -> Option<&str> {
        self.annotations.get(key).map(String::as_str)
    }
}

/// Where a public record keeps its PEM and its reference. The installation
/// identity's layout is the one PLAT-02.1 shipped, byte for byte; the console
/// key's (PROD-16.1) differs only in the two key names and the fixed value.
#[derive(Debug, Clone, Copy)]
struct PublicLayout {
    pem_key: &'static str,
    reference_key: &'static str,
    reference_value: &'static str,
}

const SIGNING_LAYOUT: PublicLayout = PublicLayout {
    pem_key: PUBLIC_KEY_PEM,
    reference_key: TRUST_REFERENCE,
    reference_value: DEFAULT_TRUST_REFERENCE,
};

const CONSOLE_LAYOUT: PublicLayout = PublicLayout {
    pem_key: CONSOLE_PUBLIC_KEY_PEM,
    reference_key: CONSOLE_TRUST_USAGE,
    reference_value: CONSOLE_CONFIRMATION_USAGE,
};

impl PublicRecord {
    fn carries_identity(&self) -> bool {
        self.state.as_deref() == Some(ESTABLISHED)
            || self.key_id.is_some()
            || self.spki_pem.is_some()
            || self.algorithm.is_some()
            || self.trust_reference.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicMaterial {
    key_id: String,
    spki_pem: String,
    algorithm: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitializeResult {
    Written,
    Contended(u16),
}

/// The answer to a create: written (with the UID the API server assigned),
/// or the name was already taken.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CreateResult {
    Created { uid: String },
    AlreadyExists,
}

/// One annotation a patch writes, beside `logweir.dev/identity-state`.
type Marker<'a> = (&'a str, &'a str);

trait IdentityStore {
    fn managed_private(&mut self) -> Result<PrivateRecord, String>;
    fn public_record(&mut self) -> Result<PublicRecord, String>;
    fn external_private(&mut self, name: &str, key: &str) -> Result<String, String>;
    fn initialize_private(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        key: &str,
        pem: &str,
        markers: &[Marker<'_>],
    ) -> Result<InitializeResult, String>;
    fn initialize_public(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        material: &PublicMaterial,
        markers: &[Marker<'_>],
    ) -> Result<InitializeResult, String>;
    /// PROD-16.1: annotations on the public identity ConfigMap, in one
    /// patch, under a `resourceVersion` precondition.
    fn annotate_public(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        markers: &[Marker<'_>],
    ) -> Result<InitializeResult, String>;
    /// PROD-16.1: the console key's retained Secret.
    fn console_private(&mut self, console: &ConsoleKeyArgs) -> Result<PrivateRecord, String>;
    /// PROD-16.1: the console key's public ConfigMap.
    fn console_public(&mut self, console: &ConsoleKeyArgs) -> Result<PublicRecord, String>;
    fn initialize_console_private(
        &mut self,
        console: &ConsoleKeyArgs,
        expected_resource_version: &str,
        has_annotations: bool,
        pem: &str,
    ) -> Result<InitializeResult, String>;
    fn initialize_console_public(
        &mut self,
        console: &ConsoleKeyArgs,
        expected_resource_version: &str,
        has_annotations: bool,
        material: &PublicMaterial,
    ) -> Result<InitializeResult, String>;
    /// PROD-16.1: every `TrustPolicy` in the cluster (a read).
    fn trust_policies(&mut self) -> Result<Vec<Value>, String>;
    /// PROD-16.1: whether `TrustRoster/default` exists (a read).
    fn trust_roster_exists(&mut self) -> Result<bool, String>;
    /// PROD-16.1: create the installation `TrustPolicy`.
    fn create_trust_policy(&mut self, policy: &Value) -> Result<CreateResult, String>;
    /// PROD-16.1 security review: delete one ClusterRoleBinding by name.
    fn delete_cluster_role_binding(&mut self, name: &str) -> Result<Revocation, String>;
}

/// What deleting the install's trust grant found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Revocation {
    /// It was bound, and now it is not.
    Revoked,
    /// It was not there (404) — the normal answer on every later run.
    Absent,
    /// This account does not hold it (403) — the grant was never rendered.
    NotHeld,
}

impl Revocation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Revoked => "revoked",
            Self::Absent => "absent",
            Self::NotHeld => "not-held",
        }
    }
}

/// What one hook run returns: its result, and what became of the install's
/// trust grant when this run tried to delete it (`None`: not attempted).
#[derive(Debug)]
struct Finished {
    result: Result<Report, String>,
    revocation: Option<Result<Revocation, String>>,
}

/// What one bootstrap run established.
#[derive(Debug)]
struct Report {
    identity: Outcome,
    console: Option<Outcome>,
    trust: Option<String>,
    /// The install's trust grant, after this run, as the test rows read it
    /// through [`bootstrap_all`]; `run` prints [`Finished::revocation`].
    #[cfg(test)]
    revocation: Option<String>,
}

pub fn run(args: &BootstrapArgs) -> ExitCode {
    let now = Utc::now();
    let finished = run_guarded(
        args,
        || open_and_run(|| KubernetesStore::in_cluster(args), args, now),
        revoke_in_cluster,
    );
    let revocation = args
        .revoke_trust_binding
        .as_deref()
        .zip(finished.revocation.as_ref())
        .map(|(name, outcome)| describe_revocation(name, outcome));
    match finished.result {
        Ok(report) => {
            print_outcome("identity-ready", &report.identity);
            if let Some(console) = &report.console {
                print_outcome("console-confirmation-ready", console);
            }
            if let Some(revocation) = &revocation {
                println!("installation-trust-grant {revocation}");
            }
            if let Some(trust) = &report.trust {
                println!("installation-trust {trust}");
                if let Some(notice) = trust_notice(trust, &report) {
                    println!("installation-trust notice: {notice}");
                }
            }
            ExitCode::Ok
        }
        Err(error) => {
            // Every error is constructed from object names, paths and HTTP
            // status codes. Private key values are never interpolated.
            eprintln!("identity bootstrap failed: {error}");
            if let Some(revocation) = &revocation {
                eprintln!("installation-trust-grant {revocation}");
            }
            ExitCode::Operational
        }
    }
}

/// What to tell the operator when the fresh install's trust step created
/// nothing — by PUBLIC key id only. `None` when there is nothing to say.
fn trust_notice(trust: &str, report: &Report) -> Option<String> {
    let reason = trust.strip_prefix("skipped:")?;
    let console = report.console.as_ref().map_or(String::new(), |c| {
        format!(" and the console key {} (ConsoleConfirmation)", c.key_id)
    });
    Some(
        if reason.starts_with("administered") || reason == "roster" {
            // Not a failure: the cluster's trust is administered already, and
            // this hook never adds to it.
            format!(
                "this cluster's trust is already administered ({reason}), so no TrustPolicy was \
             created and every namespace starts strict; to confirm in the console, add the \
             installation signer {} (EvidenceSigning){console} to the policy that governs your \
             namespaces",
                report.identity.key_id
            )
        } else if reason.starts_with("console-key-") {
            format!(
            "the console key was not generated by this install ({reason}), so nothing was trusted \
             automatically and every namespace starts strict; compare its fingerprint out of \
             band before trusting it (docs/keys.md)"
        )
        } else {
            format!("no TrustPolicy was created ({reason}); every namespace starts strict")
        },
    )
}

/// NO STANDING GRANT, ON EVERY EXIT PATH THIS BINARY CONTROLS (the PROD-16.1
/// fix round of 2026-10-08, review H1). Helm 3.12–3.18 keeps the install's
/// `<release>-identity-trust` grant when this Job fails, so the hook revokes
/// it itself whatever happened: after the steps (`bootstrap_and_revoke`),
/// after a failed step, after a store that could not be opened, after a
/// panic, and after a delete through the store that failed — the last three
/// through `revoke_fallback`, a fresh connection. What it cannot cover is a
/// process that never runs this code: an image that cannot be pulled, a pod
/// never admitted, a SIGKILL. The chart therefore refuses to render the
/// grant under a Helm that would keep it, and docs/install.md gives the
/// recovery command.
fn run_guarded(
    args: &BootstrapArgs,
    work: impl FnOnce() -> Finished,
    revoke_fallback: impl FnOnce(&str) -> Result<Revocation, String>,
) -> Finished {
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
    let mut finished = attempt.unwrap_or_else(|panic| Finished {
        result: Err(format!(
            "the hook stopped unexpectedly: {}",
            panic_text(panic.as_ref())
        )),
        revocation: None,
    });
    if let Some(name) = args.revoke_trust_binding.as_deref() {
        if !matches!(finished.revocation, Some(Ok(_))) {
            if let Some(Err(error)) = &finished.revocation {
                eprintln!("installation-trust-grant {name}: could not be deleted: {error}; retrying on a fresh connection");
            }
            finished.revocation = Some(revoke_fallback(name));
        }
    }
    finished
}

/// Open the store, then run the steps and the revocation through it; a store
/// that cannot be opened is a result with no revocation attempted, which
/// [`run_guarded`] then makes on a fresh connection.
fn open_and_run<S: IdentityStore>(
    open: impl FnOnce() -> Result<S, String>,
    args: &BootstrapArgs,
    now: DateTime<Utc>,
) -> Finished {
    match open() {
        Ok(mut store) => bootstrap_and_revoke(&mut store, args, now),
        Err(error) => Finished {
            result: Err(error),
            revocation: None,
        },
    }
}

/// A panic payload's text, when it has one.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic with no message".to_string())
}

/// `<binding>:<outcome>` as the hook prints it.
fn describe_revocation(name: &str, outcome: &Result<Revocation, String>) -> String {
    match outcome {
        Ok(outcome) => format!("{name}:{}", outcome.as_str()),
        Err(error) => {
            eprintln!("installation-trust-grant {name}: could not be deleted: {error}");
            format!("{name}:delete-failed")
        }
    }
}

/// THE USAGE-ERROR PATH (`main.rs`): clap refused this invocation — a chart
/// newer than this image passing a flag it does not know is the likely case
/// — so none of the steps ran, and nothing else would delete the install's
/// trust grant. When the arguments name one, it is deleted here, on a fresh
/// in-cluster connection, and the outcome is printed. Exit status is clap's.
pub fn revoke_after_usage_error(argv: &[String]) {
    if let Some(name) = revocation_target(argv) {
        let outcome = revoke_in_cluster(&name);
        eprintln!(
            "installation-trust-grant {} (after a usage error)",
            describe_revocation(&name, &outcome)
        );
    }
}

/// The binding `--revoke-trust-binding` names in an `identity bootstrap`
/// invocation clap could not parse; `None` for any other command, and for a
/// name that is not a DNS-1123 name.
fn revocation_target(argv: &[String]) -> Option<String> {
    let words: Vec<&str> = argv.iter().skip(1).map(String::as_str).collect();
    if words.first() != Some(&"identity") || words.get(1) != Some(&"bootstrap") {
        return None;
    }
    let mut rest = words.iter().skip(2);
    while let Some(word) = rest.next() {
        let value = if *word == "--revoke-trust-binding" {
            rest.next().map(|v| (*v).to_string())
        } else {
            word.strip_prefix("--revoke-trust-binding=")
                .map(str::to_string)
        };
        if let Some(value) = value {
            return validate_name("ClusterRoleBinding", &value)
                .ok()
                .map(|()| value);
        }
    }
    None
}

/// The whole hook over one store, as the test rows drive it: the
/// installation identity, then (PROD-16.1) the console key, then — ONLY when
/// this run generated the identity — the fresh install's trust and marker,
/// then the revocation.
#[cfg(test)]
fn bootstrap_all(
    store: &mut impl IdentityStore,
    args: &BootstrapArgs,
    now: DateTime<Utc>,
) -> Result<Report, String> {
    let finished = bootstrap_and_revoke(store, args, now);
    let revocation = args
        .revoke_trust_binding
        .as_deref()
        .zip(finished.revocation.as_ref())
        .map(|(name, outcome)| describe_revocation(name, outcome));
    finished.result.map(|mut report| {
        report.revocation = revocation;
        report
    })
}

/// The steps, then the revocation through the same store whatever the steps
/// did (PROD-16.1 security review: NO STANDING GRANT; Helm's hook cleanup
/// deletes it too, on Helm ≥ 3.19 even when this Job fails).
fn bootstrap_and_revoke(
    store: &mut impl IdentityStore,
    args: &BootstrapArgs,
    now: DateTime<Utc>,
) -> Finished {
    let result = bootstrap_steps(store, args, now);
    let revocation = args
        .revoke_trust_binding
        .as_deref()
        .map(|name| store.delete_cluster_role_binding(name));
    Finished { result, revocation }
}

fn bootstrap_steps(
    store: &mut impl IdentityStore,
    args: &BootstrapArgs,
    now: DateTime<Utc>,
) -> Result<Report, String> {
    let identity = bootstrap(store, args)?;
    let console = match &args.console {
        Some(console) => Some(bootstrap_console(store, console)?),
        None => None,
    };
    let trust = if identity.origin == Origin::Generated {
        fresh_install_trust(store, args, console.as_ref(), now)?
    } else {
        None
    };
    Ok(Report {
        identity,
        console: console.map(|(outcome, _, _)| outcome),
        trust,
        #[cfg(test)]
        revocation: None,
    })
}

pub fn run_distribution(args: &DistributeArgs) -> ExitCode {
    match KubernetesStore::for_distribution(args).and_then(|mut store| distribute(&mut store, args))
    {
        Ok(outcome) => {
            print_outcome("identity-distributed", &outcome);
            ExitCode::Ok
        }
        Err(error) => {
            eprintln!("identity distribution failed: {error}");
            ExitCode::Operational
        }
    }
}

fn print_outcome(prefix: &str, outcome: &Outcome) {
    print!(
        "{prefix} key-id={} source={}",
        outcome.key_id,
        outcome.origin.as_str()
    );
    if let Some(status) = outcome.origin.contention_status() {
        print!(" contention-http={status}");
    }
    println!();
}

trait DistributionStore {
    fn source_private(&mut self) -> Result<PrivateRecord, String>;
    fn target_private(&mut self) -> Result<PrivateRecord, String>;
    fn initialize_target(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        key: &str,
        pem: &str,
    ) -> Result<InitializeResult, String>;
}

fn distribute(
    store: &mut impl DistributionStore,
    args: &DistributeArgs,
) -> Result<Outcome, String> {
    let source = store.source_private()?;
    let source_pem = source.pem.as_deref().ok_or_else(|| {
        format!(
            "source signing Secret {}/{} is not established; let the primary identity bootstrap finish before distribution",
            args.source_namespace, args.secret_name
        )
    })?;
    let source_key = parse_private(source_pem, "source installation signing Secret")?;
    let source_material = public_material(&source_key)?;
    let target = store.target_private()?;

    if let Some(target_pem) = target.pem.as_deref() {
        let target_key = parse_private(target_pem, "target runner signing Secret")?;
        let target_material = public_material(&target_key)?;
        if target_material.key_id != source_material.key_id {
            return Err(format!(
                "target namespace {} already contains a different signer {}; restore the installation signer {} there—distribution never overwrites an established identity",
                args.target_namespace, target_material.key_id, source_material.key_id
            ));
        }
        return Ok(Outcome {
            key_id: source_material.key_id,
            origin: Origin::Existing,
        });
    }
    if target.has_any_data || target.state.as_deref() == Some(ESTABLISHED) {
        return Err(format!(
            "target namespace {} has an incomplete retained signing Secret; restore the installation identity there before retrying",
            args.target_namespace
        ));
    }

    match store.initialize_target(
        &target.resource_version,
        target.has_annotations,
        &args.secret_key,
        source_pem,
    )? {
        InitializeResult::Written => Ok(Outcome {
            key_id: source_material.key_id,
            origin: Origin::Adopted,
        }),
        InitializeResult::Contended(status) => {
            let winner = store
                .target_private()
                .map_err(|error| contention_error(status, error))?;
            if winner.state.as_deref() != Some(ESTABLISHED) {
                return Err(contention_error(
                    status,
                    format!(
                        "target signing Secret in namespace {} did not contain an established concurrent winner",
                        args.target_namespace
                    ),
                ));
            }
            let winner_pem = winner.pem.as_deref().ok_or_else(|| {
                contention_error(
                    status,
                    format!(
                        "target signing Secret in namespace {} still contains no key",
                        args.target_namespace
                    ),
                )
            })?;
            let winner_key = parse_private(winner_pem, "concurrent target runner signer")
                .map_err(|error| contention_error(status, error))?;
            let winner_material = public_material(&winner_key)?;
            if winner_material.key_id != source_material.key_id {
                return Err(contention_error(
                    status,
                    format!(
                        "target namespace {} concurrently established a different signer {}; refusing to replace it with installation identity {}",
                        args.target_namespace, winner_material.key_id, source_material.key_id
                    ),
                ));
            }
            Ok(Outcome {
                key_id: source_material.key_id,
                origin: Origin::ConcurrentWinner(status),
            })
        }
    }
}

fn bootstrap(store: &mut impl IdentityStore, args: &BootstrapArgs) -> Result<Outcome, String> {
    let private = store.managed_private()?;
    let public = store.public_record()?;

    if let Some(existing_pem) = private.pem.as_deref() {
        let key = parse_private(existing_pem, "managed signing Secret")?;
        let material = public_material(&key)?;

        if let Some((external_name, external_key)) = &args.external_secret {
            let external_pem = store.external_private(external_name, external_key)?;
            let external = parse_private(&external_pem, "configured external signing Secret")?;
            if public_material(&external)?.key_id != material.key_id {
                return Err(format!(
                    "configured external Secret {external_name}/{external_key} does not match the established installation identity {}; explicit adoption never rotates an existing signer",
                    material.key_id
                ));
            }
        }

        ensure_public(store, public, &material)?;
        return Ok(Outcome {
            key_id: material.key_id,
            origin: Origin::Existing,
        });
    }

    if private.has_any_data {
        return Err(format!(
            "the managed signing Secret contains data but not the required key {}; refusing to overwrite an existing Secret",
            args.secret_key
        ));
    }

    if private.state.as_deref() == Some(ESTABLISHED) || public.carries_identity() {
        return Err(
            "the public trust record says an installation identity is established, but the retained private key is absent; restore logweir-signing-key from backup or perform an explicit trust rotation—bootstrap will not silently replace it"
                .to_string(),
        );
    }

    let external_mode = args.external_secret.is_some();
    let candidate = if let Some((name, key)) = &args.external_secret {
        let pem = store.external_private(name, key)?;
        let parsed = parse_private(&pem, "configured external signing Secret")?;
        (parsed, pem)
    } else {
        let key = SigningKey::generate_p256();
        let pem = key
            .to_pkcs8_pem()
            .map_err(|e| format!("could not encode a generated installation key: {e}"))?;
        (key, pem)
    };
    let candidate_material = public_material(&candidate.0)?;

    let (winner, origin) = match store.initialize_private(
        &private.resource_version,
        private.has_annotations,
        &args.secret_key,
        &candidate.1,
        &[],
    )? {
        InitializeResult::Written => (
            candidate_material,
            if external_mode {
                Origin::Adopted
            } else {
                Origin::Generated
            },
        ),
        InitializeResult::Contended(status) => {
            let current = store
                .managed_private()
                .map_err(|error| contention_error(status, error))?;
            if current.state.as_deref() != Some(ESTABLISHED) {
                return Err(contention_error(
                    status,
                    "the signing Secret did not contain an established concurrent winner",
                ));
            }
            let current_pem = current.pem.as_deref().ok_or_else(|| {
                contention_error(status, "the signing Secret still contains no private key")
            })?;
            let current_key = parse_private(current_pem, "concurrently initialized signing Secret")
                .map_err(|error| contention_error(status, error))?;
            let current_material = public_material(&current_key)?;
            if external_mode && current_material.key_id != candidate_material.key_id {
                return Err(contention_error(
                    status,
                    format!(
                        "another bootstrap established identity {} while external identity {} was being adopted; refusing to rotate either identity",
                        current_material.key_id, candidate_material.key_id
                    ),
                ));
            }
            (current_material, Origin::ConcurrentWinner(status))
        }
    };

    // Reload because a separate bootstrap may have published between the
    // first reads and the successful/contended Secret initialization.
    let current_public = store.public_record()?;
    ensure_public(store, current_public, &winner)?;
    Ok(Outcome {
        key_id: winner.key_id,
        origin,
    })
}

fn ensure_public(
    store: &mut impl IdentityStore,
    record: PublicRecord,
    material: &PublicMaterial,
) -> Result<(), String> {
    if record.carries_identity() {
        return validate_public(&record, material, &SIGNING_LAYOUT);
    }
    if record.has_any_data {
        return Err(
            "the public trust ConfigMap contains unrelated data; refusing to overwrite a nonempty retained object"
                .to_string(),
        );
    }
    match store.initialize_public(
        &record.resource_version,
        record.has_annotations,
        material,
        &[],
    )? {
        InitializeResult::Written => Ok(()),
        InitializeResult::Contended(status) => {
            let winner = store
                .public_record()
                .map_err(|error| contention_error(status, error))?;
            validate_public(&winner, material, &SIGNING_LAYOUT)
                .map_err(|error| contention_error(status, error))
        }
    }
}

fn validate_public(
    record: &PublicRecord,
    material: &PublicMaterial,
    layout: &PublicLayout,
) -> Result<(), String> {
    let expected_keys = BTreeSet::from([
        PUBLIC_KEY_ID.to_string(),
        layout.pem_key.to_string(),
        PUBLIC_ALGORITHM.to_string(),
        layout.reference_key.to_string(),
    ]);
    let matches = record.data_keys == expected_keys
        && record.state.as_deref() == Some(ESTABLISHED)
        && record.key_id.as_deref() == Some(material.key_id.as_str())
        && record.spki_pem.as_deref() == Some(material.spki_pem.as_str())
        && record.algorithm.as_deref() == Some(material.algorithm.as_str())
        && record.trust_reference.as_deref() == Some(layout.reference_value);
    if matches {
        Ok(())
    } else {
        Err(format!(
            "public trust ConfigMap does not exactly match established identity {}; refusing to overwrite verification material needed by existing archives",
            material.key_id
        ))
    }
}

// ---------------------------------------------------------------------------
// PROD-16.1: the console's ConsoleConfirmation key
// ---------------------------------------------------------------------------

/// Generate, adopt or validate the console key — the installation identity's
/// lifecycle, applied to a second Secret.
///
/// * A Secret that already holds a key at the configured data key is that key
///   — ADOPTED when nothing marked it established (a hand-made Secret, as
///   every PLAT-19.2 install created), EXISTING when this hook did. Never
///   replaced.
/// * A Secret with other data, or a published public half without a private
///   key, is refused: the first could be someone else's object, and the
///   second is KEY LOSS — every confirmation the lost key signed still
///   verifies against the published half, and a silently regenerated key
///   would be a new, untrusted signer.
/// * Otherwise an Ed25519 key is generated and written in one
///   `resourceVersion`-preconditioned patch; a concurrent winner is accepted
///   only if it is itself an established, parseable key.
fn bootstrap_console(
    store: &mut impl IdentityStore,
    console: &ConsoleKeyArgs,
) -> Result<(Outcome, PublicMaterial, PrivateRecord), String> {
    let private = store.console_private(console)?;
    let public = store.console_public(console)?;

    if let Some(existing_pem) = private.pem.as_deref() {
        let key = parse_private(existing_pem, "console confirmation Secret")?;
        let material = public_material(&key)?;
        ensure_console_public(store, console, public, &material)?;
        let origin = if private.state.as_deref() == Some(ESTABLISHED) {
            Origin::Existing
        } else {
            Origin::Adopted
        };
        return Ok((
            Outcome {
                key_id: material.key_id.clone(),
                origin,
            },
            material,
            private,
        ));
    }
    if private.has_any_data {
        return Err(format!(
            "the console confirmation Secret {} contains data but not the required key {}; refusing to overwrite an existing Secret",
            console.secret_name, console.secret_key
        ));
    }
    if private.state.as_deref() == Some(ESTABLISHED) || public.carries_identity() {
        return Err(format!(
            "the public record {} says a console confirmation key is established, but the retained private key in {} is absent; restore {} from backup or perform an explicit rotation—bootstrap will not silently replace it",
            console.public_configmap_name, console.secret_name, console.secret_name
        ));
    }

    let candidate = SigningKey::generate_ed25519();
    let candidate_pem = candidate
        .to_pkcs8_pem()
        .map_err(|e| format!("could not encode a generated console confirmation key: {e}"))?;
    let candidate_material = public_material(&candidate)?;
    let (winner, origin) = match store.initialize_console_private(
        console,
        &private.resource_version,
        private.has_annotations,
        &candidate_pem,
    )? {
        InitializeResult::Written => (candidate_material, Origin::Generated),
        InitializeResult::Contended(status) => {
            let current = store
                .console_private(console)
                .map_err(|error| contention_error(status, error))?;
            if current.state.as_deref() != Some(ESTABLISHED) {
                return Err(contention_error(
                    status,
                    "the console confirmation Secret did not contain an established concurrent winner",
                ));
            }
            let current_pem = current.pem.as_deref().ok_or_else(|| {
                contention_error(
                    status,
                    "the console confirmation Secret still contains no private key",
                )
            })?;
            let current_key = parse_private(
                current_pem,
                "concurrently initialized console confirmation Secret",
            )
            .map_err(|error| contention_error(status, error))?;
            (
                public_material(&current_key)?,
                Origin::ConcurrentWinner(status),
            )
        }
    };
    let current_private = store.console_private(console)?;
    let current_public = store.console_public(console)?;
    ensure_console_public(store, console, current_public, &winner)?;
    Ok((
        Outcome {
            key_id: winner.key_id.clone(),
            origin,
        },
        winner,
        current_private,
    ))
}

fn ensure_console_public(
    store: &mut impl IdentityStore,
    console: &ConsoleKeyArgs,
    record: PublicRecord,
    material: &PublicMaterial,
) -> Result<(), String> {
    if record.carries_identity() {
        return validate_public(&record, material, &CONSOLE_LAYOUT);
    }
    if record.has_any_data {
        return Err(format!(
            "the console public ConfigMap {} contains unrelated data; refusing to overwrite a nonempty retained object",
            console.public_configmap_name
        ));
    }
    match store.initialize_console_public(
        console,
        &record.resource_version,
        record.has_annotations,
        material,
    )? {
        InitializeResult::Written => Ok(()),
        InitializeResult::Contended(status) => {
            let winner = store
                .console_public(console)
                .map_err(|error| contention_error(status, error))?;
            validate_public(&winner, material, &CONSOLE_LAYOUT)
                .map_err(|error| contention_error(status, error))
        }
    }
}

// ---------------------------------------------------------------------------
// PROD-16.1: the installation TrustPolicy
// ---------------------------------------------------------------------------

/// The fresh install's `TrustPolicy` and marker — called ONLY in the run that
/// generated the installation identity.
///
/// Creates nothing when the chart did not ask for it, or when the cluster
/// already has trust of its own: a policy of the installation's name (left
/// over, or an administrator's — never adopted or edited), another
/// `default: true` policy (a second default would contest every namespace,
/// `TrustPolicyConflict`), or `TrustRoster/default` (a default policy outranks
/// it). Otherwise it creates the policy, `default: true`, one key per usage
/// (CEL rule G8): the installation signer, and — when the chart asked for the
/// confirm default and a console key is managed — the console key, with the
/// policy annotated [`APPROVAL_DEFAULT_ANNOTATION`]`: confirm`. Then, in ONE
/// patch on the public identity ConfigMap, it records the outcome and, beside
/// a created policy with the console key, the [`MarkerClaim`] naming the
/// policy's name and UID and both key ids.
///
/// The trust calls are retried within this run: a later run cannot finish
/// them, because a later run finds the identity already generated.
fn fresh_install_trust(
    store: &mut impl IdentityStore,
    args: &BootstrapArgs,
    console: Option<&(Outcome, PublicMaterial, PrivateRecord)>,
    now: DateTime<Utc>,
) -> Result<Option<String>, String> {
    let Some(trust) = &args.installation_trust else {
        return Ok(None);
    };
    // THE FIX ROUND OF 2026-10-08 (review M2 and L1): the automatic trust
    // exists for the console's one-click confirmation and for nothing else.
    // No console key, no confirm marker requested, or a console key this
    // install did NOT generate (a hand-made Secret of that name, adopted; a
    // concurrent writer's) — nothing is created and nothing is marked: the
    // install starts strict, and the operator trusts keys as before.
    let (console_material, console_private, console_args) = match (console, &args.console) {
        (Some((outcome, material, private)), Some(console_args))
            if args.mark_fresh_install_confirm =>
        {
            if outcome.origin != Origin::Generated {
                let recorded = format!("skipped:console-key-{}", outcome.origin.as_str());
                record_fresh_install(store, &recorded, None)?;
                return Ok(Some(recorded));
            }
            (material, private, console_args)
        }
        _ => {
            let recorded = "skipped:no-console".to_string();
            record_fresh_install(store, &recorded, None)?;
            return Ok(Some(recorded));
        }
    };
    let private = store.managed_private()?;
    let public = store.public_record()?;
    let signing = PublicMaterial {
        key_id: public.key_id.clone().unwrap_or_default(),
        spki_pem: public.spki_pem.clone().unwrap_or_default(),
        algorithm: public.algorithm.clone().unwrap_or_default(),
    };
    validate_public(&public, &signing, &SIGNING_LAYOUT)?;
    let keys = [
        (
            signing.clone(),
            "EvidenceSigning",
            format!("install:{}/{}", args.namespace, args.secret_name),
            "Logweir installation signer",
            not_before(private.created_at.as_deref(), now),
        ),
        (
            console_material.clone(),
            CONSOLE_CONFIRMATION_USAGE,
            format!("console:{}/{}", args.namespace, console_args.secret_name),
            "Logweir console confirmation",
            not_before(console_private.created_at.as_deref(), now),
        ),
    ];
    let policy = installation_trust_policy(
        &trust.policy_name,
        &args.namespace,
        &keys,
        &trust.allowed_target_cluster_ids,
    );
    guard_installation_policy(&policy, &signing.key_id, &console_material.key_id)?;

    let mut attempt = 0;
    let outcome = loop {
        attempt += 1;
        match create_installation_trust(store, &trust.policy_name, &policy) {
            Ok(outcome) => break outcome,
            Err(error) if attempt < TRUST_ATTEMPTS => {
                eprintln!(
                    "installation-trust attempt {attempt}/{TRUST_ATTEMPTS} failed: {error}; retrying"
                );
                std::thread::sleep(TRUST_RETRY_DELAY);
            }
            Err(error) => {
                return Err(format!(
                    "{error}; the installation identity is established, and a later run will NOT \
                     create its TrustPolicy or mark it: this install starts strict (see \
                     docs/install.md, the fresh-install trust)"
                ))
            }
        }
    };
    let (recorded, claim) = match &outcome {
        TrustOutcome::Created { uid } => (
            format!("created:{}", trust.policy_name),
            Some(MarkerClaim {
                policy_name: trust.policy_name.clone(),
                policy_uid: uid.clone(),
                signing_key_id: signing.key_id.clone(),
                console_key_id: console_material.key_id.clone(),
            }),
        ),
        TrustOutcome::Skipped(reason) => (format!("skipped:{reason}"), None),
    };
    record_fresh_install(store, &recorded, claim.as_ref())?;
    Ok(Some(recorded))
}

/// BELT AND BRACES (PROD-16.1 security review): the one TrustPolicy this hook
/// may create declares EXACTLY the installation signer with `EvidenceSigning`
/// and the console key with `ConsoleConfirmation` — never another key, never
/// another usage, never a namespace list, never the signer alone (the fix
/// round: no automatic trust without the console) — carries the hook's
/// provenance and confirm annotations, and is the default policy. Checked on
/// the bytes about to be POSTed.
fn guard_installation_policy(
    policy: &Value,
    signing_key_id: &str,
    console_key_id: &str,
) -> Result<(), String> {
    let wanted = [
        (signing_key_id, "EvidenceSigning"),
        (console_key_id, CONSOLE_CONFIRMATION_USAGE),
    ];
    let keys = policy
        .pointer("/spec/keys")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let declared: Vec<(String, Vec<String>)> = keys
        .iter()
        .map(|k| {
            (
                k.get("keyId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                k.get("usages")
                    .and_then(Value::as_array)
                    .map(|u| {
                        u.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default(),
            )
        })
        .collect();
    let expected: Vec<(String, Vec<String>)> = wanted
        .iter()
        .map(|(id, usage)| ((*id).to_string(), vec![(*usage).to_string()]))
        .collect();
    let annotation = |key: &str| {
        policy
            .pointer("/metadata/annotations")
            .and_then(|a| a.get(key))
            .and_then(Value::as_str)
    };
    let shape_ok = policy.pointer("/spec/default").and_then(Value::as_bool) == Some(true)
        && policy.pointer("/spec/namespaces").is_none()
        && annotation(CREATED_BY_ANNOTATION) == Some(CREATED_BY_IDENTITY_BOOTSTRAP)
        && annotation(APPROVAL_DEFAULT_ANNOTATION) == Some(APPROVAL_DEFAULT_CONFIRM);
    if declared == expected && shape_ok {
        Ok(())
    } else {
        Err(
            "refusing to create an installation TrustPolicy that is not exactly the installation \
             signer (EvidenceSigning) and the console key (ConsoleConfirmation), default: true"
                .to_string(),
        )
    }
}

/// What the fresh install's trust step did.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TrustOutcome {
    Created { uid: String },
    Skipped(String),
}

/// One attempt: read what trust the cluster has, then create the policy or
/// say why not. ANY existing TrustPolicy — a default one, a namespaced one,
/// one of this name — or `TrustRoster/default` means the cluster's trust is
/// administered, and the hook adds nothing beside it (the fix round, review
/// L2: a default policy created next to namespaced ones would turn every
/// other unbound namespace into confirm).
fn create_installation_trust(
    store: &mut impl IdentityStore,
    name: &str,
    policy: &Value,
) -> Result<TrustOutcome, String> {
    let policies = store.trust_policies()?;
    if let Some(existing) = policies.first() {
        return Ok(TrustOutcome::Skipped(format!(
            "administered:{}",
            existing
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or("<unnamed>")
        )));
    }
    if store.trust_roster_exists()? {
        return Ok(TrustOutcome::Skipped("roster".into()));
    }
    match store.create_trust_policy(policy)? {
        CreateResult::Created { uid } => Ok(TrustOutcome::Created { uid }),
        // Taken between the list and the create: never adopted.
        CreateResult::AlreadyExists => Ok(TrustOutcome::Skipped(format!("name-taken:{name}"))),
    }
}

/// Record the trust step's outcome and, beside a created policy that trusts
/// the console key, the marker — ONE `resourceVersion`-preconditioned patch,
/// re-read and retried once on contention.
fn record_fresh_install(
    store: &mut impl IdentityStore,
    outcome: &str,
    claim: Option<&MarkerClaim>,
) -> Result<(), String> {
    let claim_value = claim.map(MarkerClaim::to_annotation);
    for _ in 0..2 {
        let public = store.public_record()?;
        let mut markers: Vec<Marker<'_>> = vec![(INSTALLATION_TRUST_ANNOTATION, outcome)];
        match claim_value.as_deref() {
            Some(value) => markers.push((APPROVAL_DEFAULT_ANNOTATION, value)),
            // A marker planted on the placeholder before this run is
            // overwritten with a value that is not a claim: never honoured.
            None if public.annotation(APPROVAL_DEFAULT_ANNOTATION).is_some() => {
                markers.push((APPROVAL_DEFAULT_ANNOTATION, NO_CLAIM));
            }
            None => {}
        }
        match store.annotate_public(&public.resource_version, public.has_annotations, &markers)? {
            InitializeResult::Written => return Ok(()),
            InitializeResult::Contended(_) => continue,
        }
    }
    Err(
        "the public trust ConfigMap kept changing while the fresh-install outcome was recorded"
            .to_string(),
    )
}

/// A key's `notBefore`: five minutes before its Secret was created (the
/// earliest the key can have existed), or before `now` when the creation time
/// is unknown. Earlier than the key's birth is harmless — nothing was signed
/// before it existed — and later would turn its first signatures
/// `SignedOutsideValidity`.
fn not_before(created_at: Option<&str>, now: DateTime<Utc>) -> String {
    let base = created_at
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map_or(now, |t| t.with_timezone(&Utc))
        .min(now);
    (base - chrono::Duration::seconds(NOT_BEFORE_SKEW_SECONDS))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

/// The CRD's algorithm spelling for a public record's.
fn trust_algorithm(algorithm: &str) -> &'static str {
    match algorithm {
        "ed25519" => "ed25519",
        _ => "p256",
    }
}

/// The installation `TrustPolicy`, as the bytes this hook POSTs. Public
/// material only. `confirm`: the policy records the fresh install's confirm
/// default ([`APPROVAL_DEFAULT_ANNOTATION`]), which the marker's readers
/// require beside the claim.
fn installation_trust_policy(
    name: &str,
    namespace: &str,
    keys: &[(PublicMaterial, &str, String, &str, String)],
    allowed_target_cluster_ids: &[String],
) -> Value {
    let keys: Vec<Value> = keys
        .iter()
        .map(|(material, usage, principal, display, not_before)| {
            json!({
                "keyId": material.key_id,
                "algorithm": trust_algorithm(&material.algorithm),
                "usages": [usage],
                "state": "Active",
                "notBefore": not_before,
                "notAfter": INSTALLATION_TRUST_NOT_AFTER,
                "principal": {"id": principal, "display": display},
                "spkiPem": material.spki_pem,
            })
        })
        .collect();
    let mut spec = json!({"default": true, "keys": keys});
    if !allowed_target_cluster_ids.is_empty() {
        spec["allowedTargetClusterIds"] = json!(allowed_target_cluster_ids);
    }
    let annotations = json!({
        (CREATED_BY_ANNOTATION): CREATED_BY_IDENTITY_BOOTSTRAP,
        (APPROVAL_DEFAULT_ANNOTATION): APPROVAL_DEFAULT_CONFIRM,
        "logweir.dev/installation-namespace": namespace,
    });
    json!({
        "apiVersion": "logweir.dev/v1alpha1",
        "kind": "TrustPolicy",
        "metadata": {
            "name": name,
            "labels": {
                "app.kubernetes.io/part-of": "logweir",
                "app.kubernetes.io/component": "installation-trust",
            },
            "annotations": annotations,
        },
        "spec": spec,
    })
}

fn contention_error(status: u16, detail: impl std::fmt::Display) -> String {
    format!(
        "conditional Kubernetes patch returned HTTP {status}, and no compatible established winner was found: {detail}"
    )
}

fn parse_private(pem: &str, source: &str) -> Result<SigningKey, String> {
    SigningKey::from_pkcs8_pem(pem)
        .map_err(|_| format!("{source} is not a P-256 or Ed25519 PKCS#8 PEM private key"))
}

fn public_material(key: &SigningKey) -> Result<PublicMaterial, String> {
    let verifying = key.verifying_key();
    Ok(PublicMaterial {
        key_id: verifying.key_id(),
        spki_pem: verifying
            .to_public_key_pem()
            .map_err(|e| format!("could not encode public verification material: {e}"))?,
        algorithm: match key.alg() {
            KeyAlg::EcdsaP256Sha256 => "ecdsa-p256-sha256",
            KeyAlg::Ed25519 => "ed25519",
        }
        .to_string(),
    })
}

/// The one ureq agent this module builds, for the Kubernetes API: connect, read
/// and write are bounded, and redirects are not followed. `tests/notify.rs`
/// sanctions exactly this builder as the second reviewed timeout policy beside
/// `notify_agent_with`. The unit tests pass no TLS and keep the same bounds.
fn kubernetes_agent(tls: Option<Arc<rustls::ClientConfig>>) -> ureq::Agent {
    let builder = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(20))
        .timeout_write(Duration::from_secs(20))
        .redirects(0);
    match tls {
        Some(tls) => builder.tls_config(tls),
        None => builder,
    }
    .build()
}

struct KubernetesStore {
    agent: ureq::Agent,
    base_url: String,
    token: String,
    namespace: String,
    source_namespace: Option<String>,
    secret_name: String,
    secret_key: String,
    public_configmap_name: String,
}

impl KubernetesStore {
    fn in_cluster(args: &BootstrapArgs) -> Result<Self, String> {
        validate_name("namespace", &args.namespace)?;
        validate_name("Secret", &args.secret_name)?;
        validate_name("ConfigMap", &args.public_configmap_name)?;
        validate_data_key("Secret data key", &args.secret_key)?;
        if let Some((name, key)) = &args.external_secret {
            validate_name("external Secret", name)?;
            validate_data_key("external Secret data key", key)?;
        }
        let (agent, base_url, token) = Self::connect()?;
        Ok(Self {
            agent,
            base_url,
            token,
            namespace: args.namespace.clone(),
            source_namespace: None,
            secret_name: args.secret_name.clone(),
            secret_key: args.secret_key.clone(),
            public_configmap_name: args.public_configmap_name.clone(),
        })
    }

    /// The in-cluster API connection alone — the ServiceAccount token and the
    /// cluster CA — with nothing about the identity objects, so the
    /// revocation can still run when the arguments were what failed.
    fn connect() -> Result<(ureq::Agent, String, String), String> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST").map_err(|_| {
            "KUBERNETES_SERVICE_HOST is not set; run this command in the chart bootstrap Job"
                .to_string()
        })?;
        let port =
            std::env::var("KUBERNETES_SERVICE_PORT_HTTPS").unwrap_or_else(|_| "443".to_string());
        let host = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host
        };
        let account_dir = std::env::var(SERVICE_ACCOUNT_DIR_ENV)
            .ok()
            .filter(|d| !d.trim().is_empty())
            .unwrap_or_else(|| SERVICE_ACCOUNT_DIR.to_string());
        let token_path = format!("{account_dir}/token");
        let ca_path = format!("{account_dir}/ca.crt");
        let token = std::fs::read_to_string(&token_path)
            .map_err(|e| format!("could not read ServiceAccount token at {token_path}: {e}"))?;
        let ca = std::fs::read(&ca_path)
            .map_err(|e| format!("could not read Kubernetes CA at {ca_path}: {e}"))?;
        let certs = certificates_from_pem(&ca)
            .map_err(|e| format!("could not parse Kubernetes CA at {ca_path}: {e}"))?;
        if certs.is_empty() {
            return Err(format!(
                "Kubernetes CA at {ca_path} contains no certificate"
            ));
        }
        let mut roots = rustls::RootCertStore::empty();
        for cert in certs {
            roots
                .add(cert)
                .map_err(|e| format!("could not trust Kubernetes CA at {ca_path}: {e}"))?;
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("could not configure Kubernetes TLS: {e}"))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let agent = kubernetes_agent(Some(Arc::new(tls)));
        Ok((
            agent,
            format!("https://{host}:{port}"),
            token.trim().to_string(),
        ))
    }

    fn for_distribution(args: &DistributeArgs) -> Result<Self, String> {
        validate_name("source namespace", &args.source_namespace)?;
        validate_name("target namespace", &args.target_namespace)?;
        validate_name("Secret", &args.secret_name)?;
        validate_data_key("Secret data key", &args.secret_key)?;
        let bootstrap_args = BootstrapArgs {
            namespace: args.target_namespace.clone(),
            secret_name: args.secret_name.clone(),
            secret_key: args.secret_key.clone(),
            public_configmap_name: "unused-by-distribution".to_string(),
            external_secret: None,
            mark_fresh_install_confirm: false,
            console: None,
            installation_trust: None,
            revoke_trust_binding: None,
        };
        let mut store = Self::in_cluster(&bootstrap_args)?;
        store.source_namespace = Some(args.source_namespace.clone());
        Ok(store)
    }

    fn object(&self, kind: &str, name: &str) -> Result<Value, String> {
        self.object_in(&self.namespace, kind, name)
    }

    fn object_in(&self, namespace: &str, kind: &str, name: &str) -> Result<Value, String> {
        let url = self.url(namespace, kind, name);
        let response = self
            .agent
            .get(&url)
            .set("Authorization", &format!("Bearer {}", self.token))
            .call()
            .map_err(|e| api_error("get", kind, name, e))?;
        response
            .into_json()
            .map_err(|e| format!("Kubernetes returned invalid JSON for {kind} {name}: {e}"))
    }

    fn patch(&self, kind: &str, name: &str, patch: &Value) -> Result<InitializeResult, String> {
        self.patch_in(&self.namespace, kind, name, patch)
    }

    fn patch_in(
        &self,
        namespace: &str,
        kind: &str,
        name: &str,
        patch: &Value,
    ) -> Result<InitializeResult, String> {
        let url = self.url(namespace, kind, name);
        let result = self
            .agent
            .patch(&url)
            .set("Authorization", &format!("Bearer {}", self.token))
            .set("Content-Type", "application/json-patch+json")
            .send_json(patch.clone());
        match result {
            Ok(_) => Ok(InitializeResult::Written),
            Err(ureq::Error::Status(status @ (409 | 422), _)) => {
                Ok(InitializeResult::Contended(status))
            }
            Err(error) => Err(api_error("patch", kind, name, error)),
        }
    }

    fn url(&self, namespace: &str, kind: &str, name: &str) -> String {
        format!(
            "{}/api/v1/namespaces/{}/{}/{}",
            self.base_url, namespace, kind, name
        )
    }

    /// PROD-16.1: a cluster-scoped `logweir.dev/v1alpha1` collection or
    /// object. `plural` and `name` are constants or validated names.
    fn logweir_url(&self, plural: &str, name: Option<&str>) -> String {
        match name {
            Some(name) => format!(
                "{}/apis/logweir.dev/v1alpha1/{plural}/{name}",
                self.base_url
            ),
            None => format!("{}/apis/logweir.dev/v1alpha1/{plural}", self.base_url),
        }
    }

    fn get_json(&self, url: &str, kind: &str, name: &str) -> Result<Option<Value>, String> {
        match self
            .agent
            .get(url)
            .set("Authorization", &format!("Bearer {}", self.token))
            .call()
        {
            Ok(response) => response
                .into_json()
                .map(Some)
                .map_err(|e| format!("Kubernetes returned invalid JSON for {kind} {name}: {e}")),
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(error) => Err(api_error("get", kind, name, error)),
        }
    }
}

fn certificates_from_pem(
    pem: &[u8],
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let text = std::str::from_utf8(pem).map_err(|_| "CA bundle is not UTF-8 PEM text")?;
    let mut rest = text;
    let mut certificates = Vec::new();
    while let Some(start) = rest.find(BEGIN) {
        rest = &rest[start + BEGIN.len()..];
        let end = rest
            .find(END)
            .ok_or_else(|| "certificate has no END marker".to_string())?;
        let encoded: String = rest[..end]
            .chars()
            .filter(|c| !c.is_ascii_whitespace())
            .collect();
        let der = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| "certificate body is not valid base64".to_string())?;
        certificates.push(rustls::pki_types::CertificateDer::from(der));
        rest = &rest[end + END.len()..];
    }
    Ok(certificates)
}

impl IdentityStore for KubernetesStore {
    fn managed_private(&mut self) -> Result<PrivateRecord, String> {
        let name = self.secret_name.clone();
        let object = self.object("secrets", &name)?;
        private_record(&object, &name, &self.secret_key)
    }

    fn public_record(&mut self) -> Result<PublicRecord, String> {
        let name = self.public_configmap_name.clone();
        let object = self.object("configmaps", &name)?;
        public_record(&object, &name)
    }

    fn external_private(&mut self, name: &str, key: &str) -> Result<String, String> {
        let object = self.object("secrets", name).map_err(|e| {
            format!("configured external signing Secret {name}/{key} is unavailable: {e}")
        })?;
        decode_secret_key(&object, key).map_err(|e| {
            format!("configured external signing Secret {name}/{key} is unavailable: {e}")
        })
    }

    fn initialize_private(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        key: &str,
        pem: &str,
        markers: &[Marker<'_>],
    ) -> Result<InitializeResult, String> {
        let patch = private_initialization_patch(
            expected_resource_version,
            has_annotations,
            key,
            pem,
            markers,
        );
        self.patch("secrets", &self.secret_name, &patch)
    }

    fn initialize_public(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        material: &PublicMaterial,
        markers: &[Marker<'_>],
    ) -> Result<InitializeResult, String> {
        let patch = public_initialization_patch(
            expected_resource_version,
            has_annotations,
            material,
            &SIGNING_LAYOUT,
            markers,
        );
        self.patch("configmaps", &self.public_configmap_name, &patch)
    }

    fn annotate_public(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        markers: &[Marker<'_>],
    ) -> Result<InitializeResult, String> {
        let mut patch = vec![
            json!({"op": "test", "path": "/metadata/resourceVersion", "value": expected_resource_version}),
        ];
        patch.extend(annotation_ops(has_annotations, markers));
        self.patch(
            "configmaps",
            &self.public_configmap_name,
            &Value::Array(patch),
        )
    }

    fn console_private(&mut self, console: &ConsoleKeyArgs) -> Result<PrivateRecord, String> {
        let object = self.object("secrets", &console.secret_name)?;
        private_record(&object, &console.secret_name, &console.secret_key)
    }

    fn console_public(&mut self, console: &ConsoleKeyArgs) -> Result<PublicRecord, String> {
        let object = self.object("configmaps", &console.public_configmap_name)?;
        public_record_in(&object, &console.public_configmap_name, &CONSOLE_LAYOUT)
    }

    fn initialize_console_private(
        &mut self,
        console: &ConsoleKeyArgs,
        expected_resource_version: &str,
        has_annotations: bool,
        pem: &str,
    ) -> Result<InitializeResult, String> {
        let patch = private_initialization_patch(
            expected_resource_version,
            has_annotations,
            &console.secret_key,
            pem,
            &[],
        );
        self.patch("secrets", &console.secret_name, &patch)
    }

    fn initialize_console_public(
        &mut self,
        console: &ConsoleKeyArgs,
        expected_resource_version: &str,
        has_annotations: bool,
        material: &PublicMaterial,
    ) -> Result<InitializeResult, String> {
        let patch = public_initialization_patch(
            expected_resource_version,
            has_annotations,
            material,
            &CONSOLE_LAYOUT,
            &[],
        );
        self.patch("configmaps", &console.public_configmap_name, &patch)
    }

    fn trust_policies(&mut self) -> Result<Vec<Value>, String> {
        let url = self.logweir_url("trustpolicies", None);
        let list = self
            .get_json(&url, "trustpolicies", "<list>")?
            .ok_or_else(|| {
                "the TrustPolicy kind is not served (HTTP 404); install the CRDs before the chart"
                    .to_string()
            })?;
        Ok(list
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    fn trust_roster_exists(&mut self) -> Result<bool, String> {
        let url = self.logweir_url("trustrosters", Some("default"));
        Ok(self.get_json(&url, "trustrosters", "default")?.is_some())
    }

    fn create_trust_policy(&mut self, policy: &Value) -> Result<CreateResult, String> {
        let name = policy
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>")
            .to_string();
        validate_name("TrustPolicy", &name)?;
        let url = self.logweir_url("trustpolicies", None);
        match self
            .agent
            .post(&url)
            .set("Authorization", &format!("Bearer {}", self.token))
            .send_json(policy.clone())
        {
            Ok(response) => {
                let created: Value = response.into_json().map_err(|e| {
                    format!("Kubernetes returned invalid JSON creating TrustPolicy {name}: {e}")
                })?;
                let uid = created
                    .pointer("/metadata/uid")
                    .and_then(Value::as_str)
                    .filter(|uid| !uid.is_empty())
                    .ok_or_else(|| {
                        format!(
                            "Kubernetes created TrustPolicy {name} and returned no metadata.uid"
                        )
                    })?;
                Ok(CreateResult::Created {
                    uid: uid.to_string(),
                })
            }
            Err(ureq::Error::Status(409, _)) => Ok(CreateResult::AlreadyExists),
            Err(error) => Err(api_error("create", "trustpolicies", &name, error)),
        }
    }

    fn delete_cluster_role_binding(&mut self, name: &str) -> Result<Revocation, String> {
        delete_binding(&self.agent, &self.base_url, &self.token, name)
    }
}

/// DELETE one ClusterRoleBinding by name: 404 is absent, 403 is not held.
fn delete_binding(
    agent: &ureq::Agent,
    base_url: &str,
    token: &str,
    name: &str,
) -> Result<Revocation, String> {
    validate_name("ClusterRoleBinding", name)?;
    let url = format!("{base_url}/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{name}");
    match agent
        .delete(&url)
        .set("Authorization", &format!("Bearer {token}"))
        .call()
    {
        Ok(_) => Ok(Revocation::Revoked),
        Err(ureq::Error::Status(404, _)) => Ok(Revocation::Absent),
        Err(ureq::Error::Status(403, _)) => Ok(Revocation::NotHeld),
        Err(error) => Err(api_error("delete", "clusterrolebindings", name, error)),
    }
}

/// The revocation on a fresh in-cluster connection: the fallback of
/// [`run_guarded`] and of the usage-error path.
fn revoke_in_cluster(name: &str) -> Result<Revocation, String> {
    let (agent, base_url, token) = KubernetesStore::connect()?;
    delete_binding(&agent, &base_url, &token, name)
}

impl DistributionStore for KubernetesStore {
    fn source_private(&mut self) -> Result<PrivateRecord, String> {
        let namespace = self.source_namespace.as_deref().ok_or_else(|| {
            "identity distribution source namespace is not configured".to_string()
        })?;
        let object = self
            .object_in(namespace, "secrets", &self.secret_name)
            .map_err(|error| {
                format!(
                    "source signing Secret {}/{} is unavailable: {error}",
                    namespace, self.secret_name
                )
            })?;
        private_record(&object, &self.secret_name, &self.secret_key)
    }

    fn target_private(&mut self) -> Result<PrivateRecord, String> {
        self.managed_private().map_err(|error| {
            format!(
                "target signing Secret {}/{} is unavailable: {error}; ensure the authorized namespace exists and Helm can create its prerequisites",
                self.namespace, self.secret_name
            )
        })
    }

    fn initialize_target(
        &mut self,
        expected_resource_version: &str,
        has_annotations: bool,
        key: &str,
        pem: &str,
    ) -> Result<InitializeResult, String> {
        let patch =
            private_initialization_patch(expected_resource_version, has_annotations, key, pem, &[]);
        self.patch("secrets", &self.secret_name, &patch)
    }
}

/// The JSON-patch operations that add `annotations`: one `add` of the whole
/// map when the object has none, else one `add` per key (an existing
/// annotation's value is replaced, which is what `add` does to a present
/// member).
fn annotation_ops(has_annotations: bool, annotations: &[Marker<'_>]) -> Vec<Value> {
    if has_annotations {
        annotations
            .iter()
            .map(|(key, value)| {
                json!({
                    "op": "add",
                    "path": format!("/metadata/annotations/{}", json_pointer_escape(key)),
                    "value": value
                })
            })
            .collect()
    } else {
        let map: serde_json::Map<String, Value> = annotations
            .iter()
            .map(|(key, value)| ((*key).to_string(), Value::String((*value).to_string())))
            .collect();
        vec![json!({"op": "add", "path": "/metadata/annotations", "value": map})]
    }
}

/// `identity-state: established` first (its position is pinned by a test of
/// the PLAT-02.1 shape), then any PROD-16.1 markers.
fn established_with<'a>(markers: &[Marker<'a>]) -> Vec<Marker<'a>> {
    std::iter::once((IDENTITY_STATE_ANNOTATION, ESTABLISHED))
        .chain(markers.iter().copied())
        .collect()
}

fn private_initialization_patch(
    expected_resource_version: &str,
    has_annotations: bool,
    key: &str,
    pem: &str,
    markers: &[Marker<'_>],
) -> Value {
    let encoded = base64::engine::general_purpose::STANDARD.encode(pem.as_bytes());
    let mut ops = vec![
        json!({"op": "test", "path": "/metadata/resourceVersion", "value": expected_resource_version}),
        json!({"op": "add", "path": "/data", "value": {(key): encoded}}),
    ];
    ops.extend(annotation_ops(has_annotations, &established_with(markers)));
    Value::Array(ops)
}

fn public_initialization_patch(
    expected_resource_version: &str,
    has_annotations: bool,
    material: &PublicMaterial,
    layout: &PublicLayout,
    markers: &[Marker<'_>],
) -> Value {
    let mut ops = vec![
        json!({"op": "test", "path": "/metadata/resourceVersion", "value": expected_resource_version}),
        json!({"op": "add", "path": "/data", "value": {
            (PUBLIC_KEY_ID): material.key_id,
            (layout.pem_key): material.spki_pem,
            (PUBLIC_ALGORITHM): material.algorithm,
            (layout.reference_key): layout.reference_value
        }}),
    ];
    ops.extend(annotation_ops(has_annotations, &established_with(markers)));
    Value::Array(ops)
}

fn all_annotations(object: &Value) -> BTreeMap<String, String> {
    object
        .pointer("/metadata/annotations")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn private_record(object: &Value, name: &str, key: &str) -> Result<PrivateRecord, String> {
    let has_any_data = object
        .get("data")
        .and_then(Value::as_object)
        .is_some_and(|data| !data.is_empty());
    Ok(PrivateRecord {
        resource_version: resource_version(object, "Secret", name)?,
        state: annotation(object, IDENTITY_STATE_ANNOTATION),
        pem: match object.pointer(&format!("/data/{}", json_pointer_escape(key))) {
            Some(value) => Some(decode_base64_string(
                value,
                &format!("Secret {name}/{key}"),
            )?),
            None => None,
        },
        has_any_data,
        has_annotations: object
            .pointer("/metadata/annotations")
            .and_then(Value::as_object)
            .is_some(),
        created_at: object
            .pointer("/metadata/creationTimestamp")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

fn public_record(object: &Value, name: &str) -> Result<PublicRecord, String> {
    public_record_in(object, name, &SIGNING_LAYOUT)
}

fn public_record_in(
    object: &Value,
    name: &str,
    layout: &PublicLayout,
) -> Result<PublicRecord, String> {
    let data = object.get("data").and_then(Value::as_object);
    let get = |key: &str| {
        data.and_then(|map| map.get(key))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    Ok(PublicRecord {
        resource_version: resource_version(object, "ConfigMap", name)?,
        state: annotation(object, IDENTITY_STATE_ANNOTATION),
        key_id: get(PUBLIC_KEY_ID),
        spki_pem: get(layout.pem_key),
        algorithm: get(PUBLIC_ALGORITHM),
        trust_reference: get(layout.reference_key),
        data_keys: data
            .map(|values| values.keys().cloned().collect())
            .unwrap_or_default(),
        has_any_data: data.is_some_and(|values| !values.is_empty()),
        has_annotations: object
            .pointer("/metadata/annotations")
            .and_then(Value::as_object)
            .is_some(),
        annotations: all_annotations(object),
    })
}

fn decode_secret_key(object: &Value, key: &str) -> Result<String, String> {
    let value = object
        .pointer(&format!("/data/{}", json_pointer_escape(key)))
        .ok_or_else(|| "required data key is absent".to_string())?;
    decode_base64_string(value, "Secret data")
}

fn decode_base64_string(value: &Value, label: &str) -> Result<String, String> {
    let encoded = value
        .as_str()
        .ok_or_else(|| format!("{label} is not a base64 string"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| format!("{label} is not valid base64"))?;
    String::from_utf8(bytes).map_err(|_| format!("{label} is not UTF-8 PEM text"))
}

fn resource_version(object: &Value, kind: &str, name: &str) -> Result<String, String> {
    object
        .pointer("/metadata/resourceVersion")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("Kubernetes {kind} {name} has no metadata.resourceVersion"))
}

fn annotation(object: &Value, key: &str) -> Option<String> {
    object
        .pointer(&format!(
            "/metadata/annotations/{}",
            json_pointer_escape(key)
        ))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn json_pointer_escape(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn validate_name(label: &str, value: &str) -> Result<(), String> {
    let max_length = if label.contains("namespace") { 63 } else { 253 };
    let valid_label = |part: &str| {
        !part.is_empty()
            && part.len() <= 63
            && part
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            && part
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && part
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
    };
    let valid = !value.is_empty() && value.len() <= max_length && value.split('.').all(valid_label);
    if valid {
        Ok(())
    } else {
        Err(format!("{label} name is not a Kubernetes DNS name"))
    }
}

fn validate_data_key(label: &str, value: &str) -> Result<(), String> {
    let valid = !value.is_empty()
        && value.len() <= 253
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(format!("{label} is not a valid Secret data key"))
    }
}

fn api_error(operation: &str, kind: &str, name: &str, error: ureq::Error) -> String {
    match error {
        ureq::Error::Status(code, response) => {
            // Drain a bounded amount so pooled connections remain healthy,
            // but never include a Kubernetes body: a Secret GET response may
            // contain private key data.
            let mut sink = [0_u8; 1024];
            let _ = response.into_reader().read(&mut sink);
            format!("Kubernetes {operation} {kind} {name} returned HTTP {code}")
        }
        ureq::Error::Transport(error) => {
            format!("Kubernetes {operation} {kind} {name} failed: {error}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_evidence::{sign, verify, PAYLOAD_TYPE_BACKUP_RECEIPT};
    use std::io::Write as _;
    use std::net::TcpListener;
    use std::thread;

    struct MemoryStore {
        private: PrivateRecord,
        public: PublicRecord,
        external: Result<String, String>,
        private_patch_error: Option<String>,
        concurrent_private: Option<String>,
        empty_private_contention: bool,
        public_conflict: bool,
        // PROD-16.1
        console_private: PrivateRecord,
        console_public: PublicRecord,
        policies: Vec<Value>,
        roster: bool,
        created_policies: Vec<Value>,
        create_conflict_with: Option<Value>,
        /// Trust calls that fail before one succeeds.
        trust_failures: u32,
        next_uid: u32,
        /// The install-only trust grant: `Some(true)` bound, `Some(false)`
        /// deleted, `None` never rendered (an upgrade).
        grant: Option<bool>,
        /// The order of the calls that matter: create, then revoke.
        calls: Vec<&'static str>,
        /// The fix round: a step that panics, and a delete that errors.
        panic_in_console: bool,
        revoke_error: bool,
    }

    fn empty_private(created_at: &str) -> PrivateRecord {
        PrivateRecord {
            resource_version: "1".into(),
            state: Some("uninitialized".into()),
            pem: None,
            has_any_data: false,
            has_annotations: true,
            created_at: Some(created_at.into()),
        }
    }

    fn empty_public() -> PublicRecord {
        PublicRecord {
            resource_version: "1".into(),
            state: Some("uninitialized".into()),
            key_id: None,
            spki_pem: None,
            algorithm: None,
            trust_reference: None,
            data_keys: BTreeSet::new(),
            has_any_data: false,
            has_annotations: true,
            annotations: BTreeMap::from([(
                IDENTITY_STATE_ANNOTATION.to_string(),
                "uninitialized".to_string(),
            )]),
        }
    }

    fn establish(record: &mut PublicRecord, material: &PublicMaterial, layout: &PublicLayout) {
        record.state = Some(ESTABLISHED.into());
        record
            .annotations
            .insert(IDENTITY_STATE_ANNOTATION.into(), ESTABLISHED.into());
        record.key_id = Some(material.key_id.clone());
        record.spki_pem = Some(material.spki_pem.clone());
        record.algorithm = Some(material.algorithm.clone());
        record.trust_reference = Some(layout.reference_value.into());
        record.data_keys = BTreeSet::from([
            PUBLIC_KEY_ID.to_string(),
            layout.pem_key.to_string(),
            PUBLIC_ALGORITHM.to_string(),
            layout.reference_key.to_string(),
        ]);
        record.has_any_data = true;
        record.resource_version = "2".into();
    }

    fn write_private(record: &mut PrivateRecord, pem: &str) {
        record.pem = Some(pem.to_string());
        record.has_any_data = true;
        record.state = Some(ESTABLISHED.into());
        record.resource_version = "2".into();
    }

    impl MemoryStore {
        fn fresh() -> Self {
            Self {
                private: empty_private("2026-10-07T10:00:00Z"),
                public: empty_public(),
                external: Err("HTTP 404".into()),
                private_patch_error: None,
                concurrent_private: None,
                empty_private_contention: false,
                public_conflict: false,
                console_private: empty_private("2026-10-07T10:00:01Z"),
                console_public: empty_public(),
                policies: Vec::new(),
                roster: false,
                created_policies: Vec::new(),
                create_conflict_with: None,
                trust_failures: 0,
                next_uid: 0,
                grant: Some(true),
                calls: Vec::new(),
                panic_in_console: false,
                revoke_error: false,
            }
        }

        fn establish_public(&mut self, material: &PublicMaterial) {
            establish(&mut self.public, material, &SIGNING_LAYOUT);
        }
    }

    impl IdentityStore for MemoryStore {
        fn managed_private(&mut self) -> Result<PrivateRecord, String> {
            Ok(self.private.clone())
        }

        fn public_record(&mut self) -> Result<PublicRecord, String> {
            Ok(self.public.clone())
        }

        fn external_private(&mut self, _name: &str, _key: &str) -> Result<String, String> {
            self.external.clone()
        }

        fn initialize_private(
            &mut self,
            expected_resource_version: &str,
            _has_annotations: bool,
            _key: &str,
            pem: &str,
            markers: &[Marker<'_>],
        ) -> Result<InitializeResult, String> {
            assert!(markers.is_empty(), "no generation patch carries a marker");
            if expected_resource_version != self.private.resource_version {
                return Ok(InitializeResult::Contended(409));
            }
            if let Some(error) = self.private_patch_error.take() {
                return Err(error);
            }
            if self.empty_private_contention {
                return Ok(InitializeResult::Contended(422));
            }
            if let Some(winner) = self.concurrent_private.take() {
                write_private(&mut self.private, &winner);
                return Ok(InitializeResult::Contended(422));
            }
            write_private(&mut self.private, pem);
            Ok(InitializeResult::Written)
        }

        fn initialize_public(
            &mut self,
            expected_resource_version: &str,
            _has_annotations: bool,
            material: &PublicMaterial,
            markers: &[Marker<'_>],
        ) -> Result<InitializeResult, String> {
            if expected_resource_version != self.public.resource_version {
                return Ok(InitializeResult::Contended(409));
            }
            self.establish_public(material);
            for (k, v) in markers {
                self.public.annotations.insert((*k).into(), (*v).into());
            }
            if self.public_conflict {
                Ok(InitializeResult::Contended(422))
            } else {
                Ok(InitializeResult::Written)
            }
        }

        fn annotate_public(
            &mut self,
            expected_resource_version: &str,
            _has_annotations: bool,
            markers: &[Marker<'_>],
        ) -> Result<InitializeResult, String> {
            if expected_resource_version != self.public.resource_version {
                return Ok(InitializeResult::Contended(409));
            }
            for (key, value) in markers {
                self.public
                    .annotations
                    .insert((*key).into(), (*value).into());
            }
            self.public.resource_version = "3".into();
            Ok(InitializeResult::Written)
        }

        fn console_private(&mut self, _: &ConsoleKeyArgs) -> Result<PrivateRecord, String> {
            assert!(!self.panic_in_console, "a step that panics");
            Ok(self.console_private.clone())
        }

        fn console_public(&mut self, _: &ConsoleKeyArgs) -> Result<PublicRecord, String> {
            Ok(self.console_public.clone())
        }

        fn initialize_console_private(
            &mut self,
            _: &ConsoleKeyArgs,
            expected_resource_version: &str,
            _has_annotations: bool,
            pem: &str,
        ) -> Result<InitializeResult, String> {
            if expected_resource_version != self.console_private.resource_version {
                return Ok(InitializeResult::Contended(409));
            }
            write_private(&mut self.console_private, pem);
            Ok(InitializeResult::Written)
        }

        fn initialize_console_public(
            &mut self,
            _: &ConsoleKeyArgs,
            expected_resource_version: &str,
            _has_annotations: bool,
            material: &PublicMaterial,
        ) -> Result<InitializeResult, String> {
            if expected_resource_version != self.console_public.resource_version {
                return Ok(InitializeResult::Contended(409));
            }
            establish(&mut self.console_public, material, &CONSOLE_LAYOUT);
            Ok(InitializeResult::Written)
        }

        fn trust_policies(&mut self) -> Result<Vec<Value>, String> {
            if self.trust_failures > 0 {
                self.trust_failures -= 1;
                return Err("Kubernetes get trustpolicies <list> returned HTTP 503".into());
            }
            Ok(self.policies.clone())
        }

        fn trust_roster_exists(&mut self) -> Result<bool, String> {
            Ok(self.roster)
        }

        fn create_trust_policy(&mut self, policy: &Value) -> Result<CreateResult, String> {
            if let Some(winner) = self.create_conflict_with.take() {
                self.policies.push(winner);
                return Ok(CreateResult::AlreadyExists);
            }
            assert_eq!(
                self.grant,
                Some(true),
                "a TrustPolicy is only ever created while the install's grant is bound"
            );
            self.calls.push("create-trust-policy");
            self.next_uid += 1;
            let uid = format!("00000000-0000-4000-8000-{:012}", self.next_uid);
            let mut stored = policy.clone();
            stored["metadata"]["uid"] = json!(uid);
            self.created_policies.push(stored.clone());
            self.policies.push(stored);
            Ok(CreateResult::Created { uid })
        }

        fn delete_cluster_role_binding(&mut self, name: &str) -> Result<Revocation, String> {
            assert_eq!(name, "logweir-identity-trust");
            self.calls.push("revoke-grant");
            if self.revoke_error {
                return Err("Kubernetes delete clusterrolebindings returned HTTP 503".into());
            }
            Ok(match self.grant {
                Some(true) => {
                    self.grant = Some(false);
                    Revocation::Revoked
                }
                Some(false) => Revocation::Absent,
                None => Revocation::NotHeld,
            })
        }
    }

    fn args(external: bool) -> BootstrapArgs {
        BootstrapArgs {
            namespace: "logweir-system".into(),
            secret_name: "logweir-signing-key".into(),
            secret_key: "signing.pem".into(),
            public_configmap_name: "logweir-signing-trust".into(),
            external_secret: external.then(|| ("company-signer".into(), "identity.pem".into())),
            mark_fresh_install_confirm: false,
            console: None,
            installation_trust: None,
            revoke_trust_binding: None,
        }
    }

    /// The chart's PROD-16.1 arguments: the marker, the managed console key
    /// and the installation TrustPolicy.
    fn chart_args() -> BootstrapArgs {
        BootstrapArgs {
            mark_fresh_install_confirm: true,
            console: Some(ConsoleKeyArgs {
                secret_name: "logweir-console-confirmation".into(),
                secret_key: "confirmation.key".into(),
                public_configmap_name: "logweir-console-trust".into(),
            }),
            installation_trust: Some(InstallationTrustArgs {
                policy_name: "logweir-installation".into(),
                allowed_target_cluster_ids: vec!["tQmDMMCERvy6yIB-vuOZCQ".into()],
            }),
            revoke_trust_binding: Some("logweir-identity-trust".into()),
            ..args(false)
        }
    }

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn pem(key: &SigningKey) -> String {
        key.to_pkcs8_pem().unwrap()
    }

    struct MemoryDistributionStore {
        source: PrivateRecord,
        target: PrivateRecord,
        concurrent_target: Option<String>,
    }

    impl DistributionStore for MemoryDistributionStore {
        fn source_private(&mut self) -> Result<PrivateRecord, String> {
            Ok(self.source.clone())
        }

        fn target_private(&mut self) -> Result<PrivateRecord, String> {
            Ok(self.target.clone())
        }

        fn initialize_target(
            &mut self,
            expected_resource_version: &str,
            _has_annotations: bool,
            _key: &str,
            pem: &str,
        ) -> Result<InitializeResult, String> {
            if let Some(winner) = self.concurrent_target.take() {
                self.target.pem = Some(winner);
                self.target.has_any_data = true;
                self.target.state = Some(ESTABLISHED.into());
                self.target.resource_version = "2".into();
                return Ok(InitializeResult::Contended(422));
            }
            if expected_resource_version != self.target.resource_version {
                return Ok(InitializeResult::Contended(409));
            }
            self.target.pem = Some(pem.to_string());
            self.target.has_any_data = true;
            self.target.state = Some(ESTABLISHED.into());
            self.target.resource_version = "2".into();
            Ok(InitializeResult::Written)
        }
    }

    fn distribution_store(source_key: &SigningKey) -> MemoryDistributionStore {
        MemoryDistributionStore {
            source: PrivateRecord {
                resource_version: "8".into(),
                state: Some(ESTABLISHED.into()),
                pem: Some(pem(source_key)),
                has_any_data: true,
                has_annotations: true,
                created_at: None,
            },
            target: PrivateRecord {
                resource_version: "1".into(),
                state: None,
                pem: None,
                has_any_data: false,
                has_annotations: false,
                created_at: None,
            },
            concurrent_target: None,
        }
    }

    fn distribution_args() -> DistributeArgs {
        DistributeArgs {
            source_namespace: "logweir-system".into(),
            target_namespace: "recoveries".into(),
            secret_name: "logweir-signing-key".into(),
            secret_key: "signing.pem".into(),
        }
    }

    fn store_for_http(base_url: String) -> KubernetesStore {
        KubernetesStore {
            agent: kubernetes_agent(None),
            base_url,
            token: "test-token".into(),
            namespace: "logweir-system".into(),
            source_namespace: None,
            secret_name: "logweir-signing-key".into(),
            secret_key: "signing.pem".into(),
            public_configmap_name: "logweir-signing-trust".into(),
        }
    }

    fn one_response(status: u16, reason: &'static str) -> (String, thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            let mut expected = None;
            loop {
                let read = stream.read(&mut chunk).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if expected.is_none() {
                    if let Some(split) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..split]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|value| value.parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        expected = Some(split + 4 + length);
                    }
                }
                if expected.is_some_and(|length| request.len() >= length) {
                    break;
                }
            }
            write!(
                stream,
                "HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            request
        });
        (format!("http://{address}"), handle)
    }

    #[test]
    fn fresh_install_generates_persistent_private_and_public_material() {
        let mut store = MemoryStore::fresh();
        let outcome = bootstrap(&mut store, &args(false)).unwrap();
        assert_eq!(outcome.origin, Origin::Generated);
        assert!(store.private.pem.is_some());
        assert_eq!(store.private.state.as_deref(), Some(ESTABLISHED));
        assert_eq!(
            store.public.key_id.as_deref(),
            Some(outcome.key_id.as_str())
        );
        assert_eq!(
            store.public.trust_reference.as_deref(),
            Some(DEFAULT_TRUST_REFERENCE)
        );
        assert!(!store
            .public
            .spki_pem
            .as_deref()
            .unwrap()
            .contains("PRIVATE"));
    }

    #[test]
    fn explicit_external_key_is_adopted_and_then_idempotent_on_upgrade() {
        let key = SigningKey::generate_ed25519();
        let key_pem = pem(&key);
        let expected = key.key_id();
        let mut store = MemoryStore::fresh();
        store.external = Ok(key_pem.clone());

        let first = bootstrap(&mut store, &args(true)).unwrap();
        assert_eq!(first.origin, Origin::Adopted);
        assert_eq!(first.key_id, expected);
        let stored = store.private.pem.clone();

        let second = bootstrap(&mut store, &args(true)).unwrap();
        assert_eq!(second.origin, Origin::Existing);
        assert_eq!(
            store.private.pem, stored,
            "upgrade must leave private bytes unchanged"
        );
    }

    #[test]
    fn concurrent_bootstrap_accepts_the_atomic_winner_and_publishes_only_it() {
        let winner = SigningKey::generate_p256();
        let winner_id = winner.key_id();
        let mut store = MemoryStore::fresh();
        store.concurrent_private = Some(pem(&winner));

        let outcome = bootstrap(&mut store, &args(false)).unwrap();
        assert_eq!(outcome.origin, Origin::ConcurrentWinner(422));
        assert_eq!(outcome.key_id, winner_id);
        assert_eq!(store.public.key_id.as_deref(), Some(winner_id.as_str()));
    }

    #[test]
    fn external_contention_accepts_only_the_configured_candidate() {
        let candidate = SigningKey::generate_ed25519();
        let different = SigningKey::generate_ed25519();
        let mut store = MemoryStore::fresh();
        store.external = Ok(pem(&candidate));
        store.concurrent_private = Some(pem(&different));

        let error = bootstrap(&mut store, &args(true)).unwrap_err();
        assert!(error.contains("HTTP 422"), "{error}");
        assert!(error.contains("while external identity"), "{error}");
        assert!(store.public.data_keys.is_empty());
    }

    #[test]
    fn malformed_contended_winner_retains_http_422() {
        let mut store = MemoryStore::fresh();
        store.concurrent_private = Some("not a PKCS#8 key".into());

        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("HTTP 422"), "{error}");
        assert!(error.contains("not a P-256 or Ed25519"), "{error}");
        assert!(store.public.data_keys.is_empty());
    }

    #[test]
    fn contended_patch_without_a_real_established_winner_retains_http_422() {
        let mut store = MemoryStore::fresh();
        store.empty_private_contention = true;

        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("HTTP 422"), "{error}");
        assert!(
            error.contains("no compatible established winner"),
            "{error}"
        );
        assert!(store.private.pem.is_none());
        assert!(store.public.data_keys.is_empty());
    }

    #[test]
    fn denied_secret_patch_fails_without_publication_or_key_disclosure() {
        let mut store = MemoryStore::fresh();
        store.private_patch_error =
            Some("Kubernetes patch secrets logweir-signing-key returned HTTP 403".into());
        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("HTTP 403"), "{error}");
        assert!(store.private.pem.is_none());
        assert!(store.public.key_id.is_none());
        assert!(!error.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn configured_external_secret_unavailability_fails_without_fallback_generation() {
        let mut store = MemoryStore::fresh();
        store.external = Err(
            "configured external signing Secret company-signer/identity.pem is unavailable: Kubernetes get secrets company-signer returned HTTP 404".into(),
        );
        let error = bootstrap(&mut store, &args(true)).unwrap_err();
        assert!(error.contains("unavailable"), "{error}");
        assert!(
            store.private.pem.is_none(),
            "external mode must never fall back to generation"
        );
    }

    #[test]
    fn missing_private_with_established_public_trust_is_lost_key_not_rotation() {
        let old = SigningKey::generate_p256();
        let material = public_material(&old).unwrap();
        let mut store = MemoryStore::fresh();
        store.establish_public(&material);
        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(
            error.contains("restore logweir-signing-key from backup"),
            "{error}"
        );
        assert!(store.private.pem.is_none());
        assert_eq!(
            store.public.key_id.as_deref(),
            Some(material.key_id.as_str())
        );
    }

    #[test]
    fn an_existing_secret_with_the_wrong_data_key_is_never_overwritten() {
        let mut store = MemoryStore::fresh();
        store.private.has_any_data = true;
        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(
            error.contains("contains data but not the required key"),
            "{error}"
        );
        assert!(store.private.pem.is_none());
        assert!(store.public.key_id.is_none());
    }

    #[test]
    fn a_nonempty_public_configmap_is_never_treated_as_a_placeholder() {
        let key = SigningKey::generate_p256();
        let material = public_material(&key).unwrap();
        let mut store = MemoryStore::fresh();
        store.private.pem = Some(pem(&key));
        store.private.has_any_data = true;
        store.public.has_any_data = true;

        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("unrelated data"), "{error}");
        assert!(store.public.key_id.is_none());

        store.public.has_any_data = false;
        store.public.trust_reference = Some(DEFAULT_TRUST_REFERENCE.into());
        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("does not exactly match"), "{error}");
        assert_eq!(material.key_id, key.key_id());
    }

    #[test]
    fn established_public_record_rejects_every_extra_data_key() {
        let key = SigningKey::generate_p256();
        let material = public_material(&key).unwrap();
        let mut store = MemoryStore::fresh();
        store.private.pem = Some(pem(&key));
        store.private.has_any_data = true;
        store.establish_public(&material);
        store.public.data_keys.insert("unexpected".into());

        let error = bootstrap(&mut store, &args(false)).unwrap_err();
        assert!(error.contains("does not exactly match"), "{error}");
    }

    #[test]
    fn upgrade_retains_identity_and_old_archive_verification() {
        let mut store = MemoryStore::fresh();
        let first = bootstrap(&mut store, &args(false)).unwrap();
        let private_before = store.private.pem.clone().unwrap();
        let public_before = store.public.spki_pem.clone().unwrap();
        let signer = SigningKey::from_pkcs8_pem(&private_before).unwrap();
        let archive = br#"{"archive":"created-before-upgrade"}"#;
        let signature = sign::sign_detached(&signer, PAYLOAD_TYPE_BACKUP_RECEIPT, archive).unwrap();

        let second = bootstrap(&mut store, &args(false)).unwrap();
        assert_eq!(second.origin, Origin::Existing);
        assert_eq!(second.key_id, first.key_id);
        assert_eq!(store.private.pem.as_deref(), Some(private_before.as_str()));
        assert_eq!(
            store.public.spki_pem.as_deref(),
            Some(public_before.as_str())
        );
        let verifier = logweir_evidence::keys::VerifyingKey::from_pem_str(&public_before).unwrap();
        verify::verify_detached(&verifier, PAYLOAD_TYPE_BACKUP_RECEIPT, archive, &signature)
            .expect("an archive signed before upgrade remains verifiable");
    }

    #[test]
    fn external_adoption_cannot_replace_an_established_identity() {
        let established = SigningKey::generate_p256();
        let replacement = SigningKey::generate_p256();
        let material = public_material(&established).unwrap();
        let mut store = MemoryStore::fresh();
        store.private.pem = Some(pem(&established));
        store.private.state = Some(ESTABLISHED.into());
        store.establish_public(&material);
        store.external = Ok(pem(&replacement));

        let error = bootstrap(&mut store, &args(true)).unwrap_err();
        assert!(
            error.contains("never rotates an existing signer"),
            "{error}"
        );
        assert_eq!(
            store.private.pem.as_deref(),
            Some(pem(&established).as_str())
        );
    }

    #[test]
    fn kubernetes_ca_parser_accepts_a_chain_and_rejects_truncation() {
        let pem = b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n\
                    -----BEGIN CERTIFICATE-----\nBAUG\n-----END CERTIFICATE-----\n";
        let certs = certificates_from_pem(pem).unwrap();
        assert_eq!(2, certs.len());
        assert_eq!(certs[0].as_ref(), &[1, 2, 3]);
        assert_eq!(certs[1].as_ref(), &[4, 5, 6]);
        assert!(certificates_from_pem(b"-----BEGIN CERTIFICATE-----\nAQID").is_err());
    }

    #[test]
    fn kubernetes_names_are_validated_before_building_api_urls() {
        assert!(validate_name("namespace", "logweir-system").is_ok());
        assert!(validate_name("Secret", "company.signer-1").is_ok());
        for invalid in [
            "",
            "UPPER",
            "-leading",
            "trailing-",
            "two..dots",
            "slash/name",
        ] {
            assert!(
                validate_name("Secret", invalid).is_err(),
                "accepted {invalid}"
            );
        }
        assert!(validate_name("namespace", &"a".repeat(64)).is_err());
    }

    #[test]
    fn distribution_copies_the_same_identity_and_refuses_namespace_fragmentation() {
        let installation = SigningKey::generate_p256();
        let expected_pem = pem(&installation);
        let expected_id = installation.key_id();
        let mut store = distribution_store(&installation);

        let outcome = distribute(&mut store, &distribution_args()).unwrap();
        assert_eq!(outcome.origin, Origin::Adopted);
        assert_eq!(outcome.key_id, expected_id);
        assert_eq!(store.target.pem.as_deref(), Some(expected_pem.as_str()));

        store.target.pem = Some(pem(&SigningKey::generate_p256()));
        let error = distribute(&mut store, &distribution_args()).unwrap_err();
        assert!(error.contains("different signer"), "{error}");
        assert!(error.contains("never overwrites"), "{error}");
    }

    #[test]
    fn distribution_accepts_only_the_same_concurrent_winner() {
        let installation = SigningKey::generate_ed25519();
        let expected_id = installation.key_id();
        let mut same = distribution_store(&installation);
        same.concurrent_target = Some(pem(&installation));
        let outcome = distribute(&mut same, &distribution_args()).unwrap();
        assert_eq!(outcome.origin, Origin::ConcurrentWinner(422));
        assert_eq!(outcome.key_id, expected_id);

        let mut different = distribution_store(&installation);
        different.concurrent_target = Some(pem(&SigningKey::generate_ed25519()));
        let error = distribute(&mut different, &distribution_args()).unwrap_err();
        assert!(error.contains("concurrently established a different signer"));
        assert!(error.contains("HTTP 422"), "{error}");
    }

    #[test]
    fn json_patch_handles_missing_annotations_and_http_statuses_faithfully() {
        let (base_url, request_handle) = one_response(409, "Conflict");
        let mut store = store_for_http(base_url);
        let result = store
            .initialize_private("17", false, "signing.pem", "private-pem", &[])
            .unwrap();
        assert_eq!(result, InitializeResult::Contended(409));

        let request = request_handle.join().unwrap();
        let split = request.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let headers = String::from_utf8_lossy(&request[..split]);
        assert!(
            headers.starts_with(
                "PATCH /api/v1/namespaces/logweir-system/secrets/logweir-signing-key HTTP/1.1"
            ),
            "{headers}"
        );
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("content-type: application/json-patch+json"),
            "{headers}"
        );
        let body: Value = serde_json::from_slice(&request[split + 4..]).unwrap();
        assert_eq!(
            body[0],
            json!({"op":"test","path":"/metadata/resourceVersion","value":"17"})
        );
        assert_eq!(body[2]["path"], "/metadata/annotations");
        assert_eq!(body[2]["value"][IDENTITY_STATE_ANNOTATION], ESTABLISHED);

        let present = private_initialization_patch("18", true, "signing.pem", "private-pem", &[]);
        assert_eq!(
            present[2]["path"],
            "/metadata/annotations/logweir.dev~1identity-state"
        );

        let (base_url, request_handle) = one_response(422, "Unprocessable Entity");
        let mut store = store_for_http(base_url);
        let result = store
            .initialize_private("19", false, "signing.pem", "private-pem", &[])
            .unwrap();
        request_handle.join().unwrap();
        assert_eq!(result, InitializeResult::Contended(422));
    }

    // ------------------------------------------------------------------
    // PROD-16.1: the console key, and the fresh install's trust and marker
    // ------------------------------------------------------------------

    use logweir_core::approval_policy::{
        verify_marker, InstallationMarker, MarkerRefusal, TrustKeyFacts, TrustPolicyFacts,
    };

    /// What a reader extracts from a stored policy — the same extraction the
    /// console and the controller make.
    fn facts(policy: &Value) -> TrustPolicyFacts {
        let annotation = |key: &str| {
            policy
                .pointer(&format!(
                    "/metadata/annotations/{}",
                    json_pointer_escape(key)
                ))
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        TrustPolicyFacts {
            uid: policy["metadata"]["uid"]
                .as_str()
                .unwrap_or_default()
                .into(),
            default: policy["spec"]["default"].as_bool().unwrap_or(false),
            created_by: annotation(CREATED_BY_ANNOTATION),
            approval_default: annotation(APPROVAL_DEFAULT_ANNOTATION),
            keys: policy["spec"]["keys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|k| TrustKeyFacts {
                    key_id: k["keyId"].as_str().unwrap().into(),
                    usages: k["usages"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|u| u.as_str().unwrap().to_string())
                        .collect(),
                    principal_id: k["principal"]["id"].as_str().unwrap().into(),
                })
                .collect(),
        }
    }

    /// The readers' verdict over this store, exactly as they compute it.
    fn read_marker(store: &MemoryStore) -> Result<InstallationMarker, MarkerRefusal> {
        let annotation = store.public.annotation(APPROVAL_DEFAULT_ANNOTATION);
        let named = annotation
            .and_then(MarkerClaim::parse)
            .and_then(|c| {
                store
                    .policies
                    .iter()
                    .find(|p| p["metadata"]["name"] == c.policy_name.as_str())
            })
            .map(facts);
        verify_marker(
            annotation,
            store.public.key_id.as_deref(),
            "logweir-system",
            named.as_ref(),
        )
    }

    #[test]
    fn a_fresh_install_creates_its_trust_and_a_bound_marker_the_readers_honour() {
        let mut store = MemoryStore::fresh();
        let report = bootstrap_all(&mut store, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
        assert_eq!(report.identity.origin, Origin::Generated);
        assert_eq!(
            report.trust.as_deref(),
            Some("created:logweir-installation")
        );
        assert_eq!(store.created_policies.len(), 1);
        let policy = &store.created_policies[0];
        assert_eq!(policy["kind"], "TrustPolicy");
        assert_eq!(policy["metadata"]["name"], "logweir-installation");
        assert_eq!(
            policy["metadata"]["annotations"][CREATED_BY_ANNOTATION],
            CREATED_BY_IDENTITY_BOOTSTRAP
        );
        assert_eq!(
            policy["metadata"]["annotations"][APPROVAL_DEFAULT_ANNOTATION],
            APPROVAL_DEFAULT_CONFIRM
        );
        assert_eq!(policy["spec"]["default"], true);
        assert_eq!(
            policy["spec"]["allowedTargetClusterIds"],
            json!(["tQmDMMCERvy6yIB-vuOZCQ"])
        );
        let keys = policy["spec"]["keys"].as_array().unwrap();
        assert_eq!(keys.len(), 2);
        let signing = &keys[0];
        assert_eq!(signing["keyId"], report.identity.key_id.as_str());
        assert_eq!(signing["usages"], json!(["EvidenceSigning"]));
        assert_eq!(signing["algorithm"], "p256");
        assert_eq!(
            signing["principal"]["id"],
            "install:logweir-system/logweir-signing-key"
        );
        // The window opens five minutes before the Secret was created.
        assert_eq!(signing["notBefore"], "2026-10-07T09:55:00Z");
        assert_eq!(signing["notAfter"], INSTALLATION_TRUST_NOT_AFTER);
        let console = &keys[1];
        let console_id = report.console.as_ref().unwrap().key_id.clone();
        assert_eq!(console["keyId"], console_id.as_str());
        assert_eq!(console["usages"], json!(["ConsoleConfirmation"]));
        assert_eq!(console["algorithm"], "ed25519");
        assert_eq!(
            console["principal"]["id"],
            "console:logweir-system/logweir-console-confirmation"
        );
        assert_eq!(console["notBefore"], "2026-10-07T09:55:01Z");
        for key in keys {
            assert!(!key["spkiPem"].as_str().unwrap().contains("PRIVATE"));
            assert_eq!(key["state"], "Active");
        }
        // THE MARKER names the policy by name AND UID, and both keys.
        let claim = MarkerClaim::parse(
            store
                .public
                .annotation(APPROVAL_DEFAULT_ANNOTATION)
                .unwrap(),
        )
        .expect("a claim");
        assert_eq!(claim.policy_name, "logweir-installation");
        assert_eq!(
            claim.policy_uid,
            policy["metadata"]["uid"].as_str().unwrap()
        );
        assert_eq!(claim.signing_key_id, report.identity.key_id);
        assert_eq!(claim.console_key_id, console_id);
        assert_eq!(
            store.public.annotation(INSTALLATION_TRUST_ANNOTATION),
            Some("created:logweir-installation")
        );
        // AND THE READERS HONOUR WHAT THE HOOK WROTE.
        assert_eq!(
            read_marker(&store),
            Ok(InstallationMarker::FreshInstallConfirm)
        );
    }

    /// THE FIX ROUND OF 2026-10-08 (review M2): an install that never uses
    /// confirm gets nothing new. Without the console key, or without the
    /// confirm marker requested, the hook creates NO TrustPolicy — not even a
    /// signer-only one — and marks nothing; the grant is still revoked.
    #[test]
    fn a_fresh_install_without_a_console_creates_no_trust_and_marks_nothing() {
        for (what, args) in [
            (
                "no confirm marker requested",
                BootstrapArgs {
                    mark_fresh_install_confirm: false,
                    ..chart_args()
                },
            ),
            (
                "no console key",
                BootstrapArgs {
                    console: None,
                    ..chart_args()
                },
            ),
        ] {
            let mut store = MemoryStore::fresh();
            let report = bootstrap_all(&mut store, &args, at("2026-10-07T10:01:00Z")).unwrap();
            assert_eq!(report.identity.origin, Origin::Generated, "{what}");
            assert_eq!(
                report.trust.as_deref(),
                Some("skipped:no-console"),
                "{what}"
            );
            assert!(
                store.created_policies.is_empty(),
                "{what}: no signer-only policy"
            );
            assert_eq!(store.calls, vec!["revoke-grant"], "{what}");
            assert_eq!(store.grant, Some(false), "{what}");
            assert_eq!(
                store.public.annotation(APPROVAL_DEFAULT_ANNOTATION),
                None,
                "{what}"
            );
            assert_eq!(
                store.public.annotation(INSTALLATION_TRUST_ANNOTATION),
                Some("skipped:no-console"),
                "{what}"
            );
            assert_eq!(
                read_marker(&store),
                Ok(InstallationMarker::Unmarked),
                "{what}"
            );
        }
    }

    /// THE FIX ROUND (review L1): a console key this install did NOT
    /// generate — a hand-made Secret of the managed name planted before the
    /// first install — is adopted and published, and trusted NOWHERE
    /// automatically: no policy, no marker.
    #[test]
    fn an_adopted_console_key_is_never_trusted_automatically() {
        let planted = SigningKey::generate_ed25519();
        let mut store = MemoryStore::fresh();
        store.console_private.pem = Some(pem(&planted));
        store.console_private.has_any_data = true;
        store.console_private.state = None;
        let report = bootstrap_all(&mut store, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
        assert_eq!(report.identity.origin, Origin::Generated);
        let console = report.console.unwrap();
        assert_eq!(console.origin, Origin::Adopted);
        assert_eq!(console.key_id, planted.key_id());
        assert_eq!(report.trust.as_deref(), Some("skipped:console-key-adopted"));
        assert!(store.created_policies.is_empty());
        assert_eq!(store.calls, vec!["revoke-grant"]);
        assert_eq!(read_marker(&store), Ok(InstallationMarker::Unmarked));
    }

    /// THE UPGRADE NEVER WEAKENS APPROVAL (the PROD-16.1 security review).
    /// An install whose identity already existed — established by an earlier
    /// chart, hand-provisioned before the managed identity (rehearsal R1), or
    /// adopted — gets no TrustPolicy, its console key is trusted nowhere, and
    /// nothing is marked. Then the ATTACK: a marker patched into the
    /// ConfigMap, bare or as a forged claim, is ignored.
    #[test]
    fn an_upgraded_install_gets_no_trust_and_a_patched_marker_changes_nothing() {
        let old = SigningKey::generate_p256();
        let mut established = MemoryStore::fresh();
        write_private(&mut established.private, &pem(&old));
        established.establish_public(&public_material(&old).unwrap());
        let mut hand = MemoryStore::fresh();
        hand.private.pem = Some(pem(&old));
        hand.private.has_any_data = true;
        let mut adopted = MemoryStore::fresh();
        adopted.external = Ok(pem(&SigningKey::generate_ed25519()));
        let external = BootstrapArgs {
            external_secret: Some(("company-signer".into(), "identity.pem".into())),
            ..chart_args()
        };
        for (store, args) in [
            (&mut established, chart_args()),
            (&mut hand, chart_args()),
            (&mut adopted, external),
        ] {
            let report = bootstrap_all(store, &args, at("2026-10-07T10:01:00Z")).unwrap();
            assert_ne!(report.identity.origin, Origin::Generated);
            assert_eq!(report.trust, None, "no trust step on an upgrade");
            assert!(store.created_policies.is_empty());
            assert_eq!(store.public.annotation(APPROVAL_DEFAULT_ANNOTATION), None);
            // The console key is still generated (new in this chart; an
            // explicit binding or the documented opt-in needs it).
            let console = report.console.unwrap();
            assert_eq!(console.origin, Origin::Generated);

            // THE ATTACK, bare.
            store.public.annotations.insert(
                APPROVAL_DEFAULT_ANNOTATION.into(),
                APPROVAL_DEFAULT_CONFIRM.into(),
            );
            assert_eq!(read_marker(store), Err(MarkerRefusal::NotAClaim));
            // And as a claim naming a policy that does not exist.
            let forged = MarkerClaim {
                policy_name: "logweir-installation".into(),
                policy_uid: "guessed".into(),
                signing_key_id: store.public.key_id.clone().unwrap(),
                console_key_id: console.key_id.clone(),
            };
            store
                .public
                .annotations
                .insert(APPROVAL_DEFAULT_ANNOTATION.into(), forged.to_annotation());
            assert_eq!(read_marker(store), Err(MarkerRefusal::PolicyMissing));
        }
    }

    #[test]
    fn an_interrupted_fresh_run_is_not_finished_by_a_later_run() {
        // The first run generated and published the identity, then died
        // before the trust step. The retry finds the identity established
        // and does NOT create the trust or mark anything: strict.
        let key = SigningKey::generate_p256();
        let mut store = MemoryStore::fresh();
        write_private(&mut store.private, &pem(&key));
        store.establish_public(&public_material(&key).unwrap());
        let report = bootstrap_all(&mut store, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
        assert_eq!(report.identity.origin, Origin::Existing);
        assert_eq!(report.trust, None);
        assert!(store.created_policies.is_empty());
        assert_eq!(read_marker(&store), Ok(InstallationMarker::Unmarked));
    }

    #[test]
    fn the_trust_step_is_retried_within_its_run_and_then_fails_the_hook() {
        let mut flaky = MemoryStore::fresh();
        flaky.trust_failures = TRUST_ATTEMPTS - 1;
        let report = bootstrap_all(&mut flaky, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
        assert_eq!(
            report.trust.as_deref(),
            Some("created:logweir-installation")
        );

        let mut down = MemoryStore::fresh();
        down.trust_failures = TRUST_ATTEMPTS;
        let error =
            bootstrap_all(&mut down, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap_err();
        assert!(error.contains("starts strict"), "{error}");
        assert_eq!(down.public.annotation(APPROVAL_DEFAULT_ANNOTATION), None);
    }

    #[test]
    fn existing_cluster_trust_is_never_contested_and_nothing_is_marked() {
        let mut with_default = MemoryStore::fresh();
        with_default.policies.push(json!({
            "metadata": {"name": "org-default", "uid": "u1"},
            "spec": {"default": true, "keys": []}
        }));
        let mut with_roster = MemoryStore::fresh();
        with_roster.roster = true;
        let mut name_taken = MemoryStore::fresh();
        name_taken.policies.push(json!({
            "metadata": {"name": "logweir-installation", "uid": "u2"},
            "spec": {"default": false, "keys": []}
        }));
        let mut raced = MemoryStore::fresh();
        raced.create_conflict_with = Some(json!({
            "metadata": {"name": "logweir-installation", "uid": "u3"},
            "spec": {"default": true, "keys": []}
        }));
        // THE FIX ROUND (review L2): namespaced trust only — the PoC's shape —
        // is administered trust too; a default policy beside it would turn
        // every other unbound namespace into confirm.
        let mut namespaced = MemoryStore::fresh();
        namespaced.policies.push(json!({
            "metadata": {"name": "team-a", "uid": "u4"},
            "spec": {"default": false, "namespaces": ["team-a"], "keys": []}
        }));
        for (store, expected) in [
            (&mut with_default, "skipped:administered:org-default"),
            (&mut with_roster, "skipped:roster"),
            (&mut name_taken, "skipped:administered:logweir-installation"),
            (&mut raced, "skipped:name-taken:logweir-installation"),
            (&mut namespaced, "skipped:administered:team-a"),
        ] {
            let report = bootstrap_all(store, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
            assert_eq!(report.trust.as_deref(), Some(expected));
            assert!(store.created_policies.is_empty());
            assert_eq!(store.public.annotation(APPROVAL_DEFAULT_ANNOTATION), None);
            assert_eq!(read_marker(store), Ok(InstallationMarker::Unmarked));
        }
        // The policy of that name is never edited.
        assert_eq!(name_taken.policies[0]["spec"]["default"], false);
    }

    #[test]
    fn a_marker_planted_on_the_placeholder_is_overwritten_when_this_run_makes_no_claim() {
        let mut store = MemoryStore::fresh();
        store.public.annotations.insert(
            APPROVAL_DEFAULT_ANNOTATION.into(),
            "confirm;policy=logweir-installation;uid=x;signing=y;console=z".into(),
        );
        store.roster = true;
        bootstrap_all(&mut store, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
        assert_eq!(
            store.public.annotation(APPROVAL_DEFAULT_ANNOTATION),
            Some(NO_CLAIM)
        );
        assert_eq!(read_marker(&store), Err(MarkerRefusal::NotAClaim));
    }

    #[test]
    fn the_console_key_is_generated_once_and_never_regenerated_on_upgrade() {
        let mut store = MemoryStore::fresh();
        let first = bootstrap_all(&mut store, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
        let console = first.console.unwrap();
        assert_eq!(console.origin, Origin::Generated);
        let private_before = store.console_private.pem.clone().unwrap();
        let key = SigningKey::from_pkcs8_pem(&private_before).unwrap();
        assert_eq!(key.alg(), KeyAlg::Ed25519);
        assert_eq!(key.key_id(), console.key_id);
        assert_eq!(
            store.console_public.data_keys,
            BTreeSet::from([
                PUBLIC_KEY_ID.to_string(),
                CONSOLE_PUBLIC_KEY_PEM.to_string(),
                PUBLIC_ALGORITHM.to_string(),
                CONSOLE_TRUST_USAGE.to_string(),
            ])
        );
        assert_eq!(
            store.console_public.trust_reference.as_deref(),
            Some(CONSOLE_CONFIRMATION_USAGE)
        );
        assert!(!store
            .console_public
            .spki_pem
            .clone()
            .unwrap()
            .contains("PRIVATE"));

        // THE UPGRADE: the same objects, a second run.
        let second = bootstrap_all(&mut store, &chart_args(), at("2026-10-08T10:01:00Z")).unwrap();
        let console_again = second.console.unwrap();
        assert_eq!(console_again.origin, Origin::Existing);
        assert_eq!(console_again.key_id, console.key_id);
        assert_eq!(
            store.console_private.pem.as_deref(),
            Some(private_before.as_str()),
            "an upgrade must leave the console key's bytes unchanged"
        );
        assert_eq!(second.trust, None, "and creates no second trust");
        assert_eq!(store.created_policies.len(), 1);
    }

    #[test]
    fn a_hand_made_console_key_is_adopted_and_published() {
        // Every PLAT-19.2 install created `logweir-console-confirmation` by
        // hand (`openssl genpkey -algorithm ed25519`). It is that key.
        let hand = SigningKey::generate_ed25519();
        let mut store = MemoryStore::fresh();
        store.console_private.pem = Some(pem(&hand));
        store.console_private.has_any_data = true;
        store.console_private.state = None;
        let report = bootstrap_all(&mut store, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
        let console = report.console.unwrap();
        assert_eq!(console.origin, Origin::Adopted);
        assert_eq!(console.key_id, hand.key_id());
        assert_eq!(
            store.console_private.pem.as_deref(),
            Some(pem(&hand).as_str())
        );
        assert_eq!(
            store.console_public.key_id.as_deref(),
            Some(hand.key_id().as_str())
        );
    }

    #[test]
    fn a_lost_console_key_or_a_foreign_secret_stops_the_hook() {
        let lost = SigningKey::generate_ed25519();
        let mut store = MemoryStore::fresh();
        establish(
            &mut store.console_public,
            &public_material(&lost).unwrap(),
            &CONSOLE_LAYOUT,
        );
        let error =
            bootstrap_all(&mut store, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap_err();
        assert!(
            error.contains("restore logweir-console-confirmation from backup"),
            "{error}"
        );
        assert!(
            store.console_private.pem.is_none(),
            "never silently regenerated"
        );

        let mut foreign = MemoryStore::fresh();
        foreign.console_private.has_any_data = true;
        let error =
            bootstrap_all(&mut foreign, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap_err();
        assert!(
            error.contains("contains data but not the required key"),
            "{error}"
        );
    }

    #[test]
    fn no_identity_patch_carries_a_marker() {
        let patch = private_initialization_patch("7", false, "signing.pem", "pem", &[]);
        assert_eq!(patch.as_array().unwrap().len(), 3);
        assert_eq!(patch[2]["path"], "/metadata/annotations");
        assert_eq!(
            patch[2]["value"],
            json!({(IDENTITY_STATE_ANNOTATION): ESTABLISHED})
        );
    }

    #[test]
    fn not_before_opens_before_the_secret_and_never_after_now() {
        let now = at("2026-10-07T10:00:00Z");
        assert_eq!(
            not_before(Some("2026-10-07T09:00:00Z"), now),
            "2026-10-07T08:55:00Z"
        );
        assert_eq!(not_before(None, now), "2026-10-07T09:55:00Z");
        assert_eq!(not_before(Some("garbage"), now), "2026-10-07T09:55:00Z");
        assert_eq!(
            not_before(Some("2026-10-07T11:00:00Z"), now),
            "2026-10-07T09:55:00Z"
        );
    }

    #[test]
    fn the_trust_calls_use_the_cluster_scoped_paths_and_read_their_statuses() {
        let (base_url, handle) = one_response(409, "Conflict");
        let mut store = store_for_http(base_url);
        let policy = installation_trust_policy("logweir-installation", "ns", &[], &[]);
        assert_eq!(
            store.create_trust_policy(&policy).unwrap(),
            CreateResult::AlreadyExists
        );
        let request = String::from_utf8_lossy(&handle.join().unwrap()).to_string();
        assert!(
            request.starts_with("POST /apis/logweir.dev/v1alpha1/trustpolicies HTTP/1.1"),
            "{request}"
        );
        assert!(!request.contains("PRIVATE"), "{request}");

        // A 201 with no body names no UID: refused, never a marker with none.
        let (base_url, handle) = one_response(201, "Created");
        let mut store = store_for_http(base_url);
        let error = store.create_trust_policy(&policy).unwrap_err();
        handle.join().unwrap();
        assert!(
            error.contains("TrustPolicy logweir-installation"),
            "{error}"
        );

        let (base_url, handle) = one_response(404, "Not Found");
        let mut store = store_for_http(base_url);
        assert!(!store.trust_roster_exists().unwrap());
        let request = String::from_utf8_lossy(&handle.join().unwrap()).to_string();
        assert!(
            request.starts_with("GET /apis/logweir.dev/v1alpha1/trustrosters/default HTTP/1.1"),
            "{request}"
        );

        let (base_url, handle) = one_response(403, "Forbidden");
        let mut store = store_for_http(base_url);
        let error = store.trust_policies().unwrap_err();
        handle.join().unwrap();
        assert!(error.contains("HTTP 403"), "{error}");
    }

    // ------------------------------------------------------------------
    // PROD-16.1 security review: no standing trust grant
    // ------------------------------------------------------------------

    #[test]
    fn the_fresh_install_revokes_its_trust_grant_right_after_creating_the_policy() {
        let mut store = MemoryStore::fresh();
        let report = bootstrap_all(&mut store, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
        assert_eq!(
            report.trust.as_deref(),
            Some("created:logweir-installation")
        );
        assert_eq!(store.calls, vec!["create-trust-policy", "revoke-grant"]);
        assert_eq!(store.grant, Some(false), "no standing grant after the run");
        assert_eq!(
            report.revocation.as_deref(),
            Some("logweir-identity-trust:revoked")
        );
    }

    #[test]
    fn every_later_run_finds_no_grant_and_creates_nothing() {
        // An upgrade: the grant was never rendered (403: not held).
        let old = SigningKey::generate_p256();
        let mut upgraded = MemoryStore::fresh();
        write_private(&mut upgraded.private, &pem(&old));
        upgraded.establish_public(&public_material(&old).unwrap());
        upgraded.grant = None;
        let report =
            bootstrap_all(&mut upgraded, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
        assert_eq!(
            upgraded.calls,
            vec!["revoke-grant"],
            "no create on an upgrade"
        );
        assert_eq!(
            report.revocation.as_deref(),
            Some("logweir-identity-trust:not-held")
        );
        // A grant left bound by an install whose Helm client died before
        // its cleanup: the next run revokes it, and still creates nothing.
        let mut lingering = MemoryStore::fresh();
        write_private(&mut lingering.private, &pem(&old));
        lingering.establish_public(&public_material(&old).unwrap());
        let report =
            bootstrap_all(&mut lingering, &chart_args(), at("2026-10-07T10:01:00Z")).unwrap();
        assert_eq!(lingering.calls, vec!["revoke-grant"]);
        assert_eq!(lingering.grant, Some(false));
        assert_eq!(
            report.revocation.as_deref(),
            Some("logweir-identity-trust:revoked")
        );
    }

    #[test]
    fn a_failed_trust_step_still_revokes_the_grant() {
        let mut down = MemoryStore::fresh();
        down.trust_failures = TRUST_ATTEMPTS;
        assert!(bootstrap_all(&mut down, &chart_args(), at("2026-10-07T10:01:00Z")).is_err());
        assert_eq!(down.grant, Some(false), "revoked even when the hook fails");
    }

    /// BELT AND BRACES: the one policy body this hook may POST.
    #[test]
    fn the_hook_creates_only_the_signer_and_the_console_key_as_the_default() {
        let signing = public_material(&SigningKey::generate_p256()).unwrap();
        let console = public_material(&SigningKey::generate_ed25519()).unwrap();
        let key = |m: &PublicMaterial, usage: &'static str, principal: &str| {
            (
                m.clone(),
                usage,
                principal.to_string(),
                "d",
                "2026-10-07T09:55:00Z".to_string(),
            )
        };
        let exact = installation_trust_policy(
            "logweir-installation",
            "ns",
            &[
                key(&signing, "EvidenceSigning", "install:ns/s"),
                key(&console, CONSOLE_CONFIRMATION_USAGE, "console:ns/c"),
            ],
            &[],
        );
        assert_eq!(
            guard_installation_policy(&exact, &signing.key_id, &console.key_id),
            Ok(())
        );
        // NEGATIVE CONTROLS: another key, another usage, a namespace list,
        // not the default, the signer alone (the fix round: no automatic
        // trust without the console), no provenance, no confirm annotation.
        let other = public_material(&SigningKey::generate_p256()).unwrap();
        let mut extra = exact.clone();
        extra["spec"]["keys"]
            .as_array_mut()
            .unwrap()
            .push(json!({"keyId": other.key_id, "usages": ["GovernedApproval"]}));
        let mut governed = exact.clone();
        governed["spec"]["keys"][1]["usages"] = json!(["GovernedApproval"]);
        let mut scoped = exact.clone();
        scoped["spec"]["namespaces"] = json!(["prod"]);
        let mut not_default = exact.clone();
        not_default["spec"]["default"] = json!(false);
        let signer_only = installation_trust_policy(
            "logweir-installation",
            "ns",
            &[key(&signing, "EvidenceSigning", "install:ns/s")],
            &[],
        );
        let mut no_provenance = exact.clone();
        no_provenance["metadata"]["annotations"]
            .as_object_mut()
            .unwrap()
            .remove(CREATED_BY_ANNOTATION);
        let mut no_confirm = exact.clone();
        no_confirm["metadata"]["annotations"]
            .as_object_mut()
            .unwrap()
            .remove(APPROVAL_DEFAULT_ANNOTATION);
        for (what, body) in [
            ("an extra key", &extra),
            ("GovernedApproval", &governed),
            ("a namespace list", &scoped),
            ("not the default", &not_default),
            ("the signer alone", &signer_only),
            ("no provenance", &no_provenance),
            ("no confirm annotation", &no_confirm),
        ] {
            assert!(
                guard_installation_policy(body, &signing.key_id, &console.key_id).is_err(),
                "{what} must be refused"
            );
        }
    }

    #[test]
    fn the_revocation_deletes_the_one_binding_by_name() {
        let (base_url, handle) = one_response(200, "OK");
        let mut store = store_for_http(base_url);
        assert_eq!(
            store
                .delete_cluster_role_binding("logweir-identity-trust")
                .unwrap(),
            Revocation::Revoked
        );
        let request = String::from_utf8_lossy(&handle.join().unwrap()).to_string();
        assert!(
            request.starts_with(
                "DELETE /apis/rbac.authorization.k8s.io/v1/clusterrolebindings/logweir-identity-trust HTTP/1.1"
            ),
            "{request}"
        );
        for (status, reason, expected) in [
            (404, "Not Found", Revocation::Absent),
            (403, "Forbidden", Revocation::NotHeld),
        ] {
            let (base_url, handle) = one_response(status, reason);
            let mut store = store_for_http(base_url);
            assert_eq!(
                store
                    .delete_cluster_role_binding("logweir-identity-trust")
                    .unwrap(),
                expected
            );
            handle.join().unwrap();
        }
    }

    // ------------------------------------------------------------------
    // PROD-16.1 fix round (2026-10-08, review H1): the revocation on every
    // exit path this binary controls
    // ------------------------------------------------------------------

    /// What the fallback revocation was asked to delete, by name.
    fn fallback(
        calls: &std::cell::RefCell<Vec<String>>,
    ) -> impl FnOnce(&str) -> Result<Revocation, String> + '_ {
        move |name: &str| {
            calls.borrow_mut().push(name.to_string());
            Ok(Revocation::Revoked)
        }
    }

    #[test]
    fn every_exit_path_the_hook_controls_revokes_the_grant() {
        let now = at("2026-10-07T10:01:00Z");
        let lost = SigningKey::generate_ed25519();

        // 1. Success: the store revokes; no second connection.
        let calls = std::cell::RefCell::new(Vec::new());
        let mut store = MemoryStore::fresh();
        let args = chart_args();
        let done = run_guarded(
            &args,
            || bootstrap_and_revoke(&mut store, &args, now),
            fallback(&calls),
        );
        assert!(done.result.is_ok());
        assert!(matches!(done.revocation, Some(Ok(Revocation::Revoked))));
        assert!(calls.borrow().is_empty());
        assert_eq!(store.grant, Some(false));

        // 2. A step fails (console key loss): the store still revokes.
        let calls = std::cell::RefCell::new(Vec::new());
        let mut store = MemoryStore::fresh();
        establish(
            &mut store.console_public,
            &public_material(&lost).unwrap(),
            &CONSOLE_LAYOUT,
        );
        let done = run_guarded(
            &args,
            || bootstrap_and_revoke(&mut store, &args, now),
            fallback(&calls),
        );
        assert!(done
            .result
            .unwrap_err()
            .contains("restore logweir-console-confirmation"));
        assert!(matches!(done.revocation, Some(Ok(Revocation::Revoked))));
        assert!(calls.borrow().is_empty());
        assert_eq!(store.grant, Some(false), "revoked although the hook failed");

        // 3. The store cannot be opened (no token, a bad argument): the
        //    fallback connection deletes the binding by name.
        let calls = std::cell::RefCell::new(Vec::new());
        let done = run_guarded(
            &args,
            || {
                open_and_run(
                    || Err::<MemoryStore, _>("could not read ServiceAccount token".to_string()),
                    &args,
                    now,
                )
            },
            fallback(&calls),
        );
        assert!(done.result.unwrap_err().contains("ServiceAccount token"));
        assert!(matches!(done.revocation, Some(Ok(Revocation::Revoked))));
        assert_eq!(*calls.borrow(), vec!["logweir-identity-trust".to_string()]);

        // 4. A step panics: caught, reported, and the fallback revokes.
        let calls = std::cell::RefCell::new(Vec::new());
        let mut store = MemoryStore::fresh();
        store.panic_in_console = true;
        let done = run_guarded(
            &args,
            || bootstrap_and_revoke(&mut store, &args, now),
            fallback(&calls),
        );
        assert!(done
            .result
            .unwrap_err()
            .contains("stopped unexpectedly: a step that panics"));
        assert_eq!(*calls.borrow(), vec!["logweir-identity-trust".to_string()]);

        // 5. The store's delete errors: retried on a fresh connection.
        let calls = std::cell::RefCell::new(Vec::new());
        let mut store = MemoryStore::fresh();
        store.revoke_error = true;
        let done = run_guarded(
            &args,
            || bootstrap_and_revoke(&mut store, &args, now),
            fallback(&calls),
        );
        assert!(done.result.is_ok());
        assert_eq!(*calls.borrow(), vec!["logweir-identity-trust".to_string()]);
        assert!(matches!(done.revocation, Some(Ok(Revocation::Revoked))));

        // NEGATIVE CONTROL: no binding named, nothing revoked on any path.
        let calls = std::cell::RefCell::new(Vec::new());
        let plain = self::args(false);
        let done = run_guarded(
            &plain,
            || {
                open_and_run(
                    || Err::<MemoryStore, _>("no token".to_string()),
                    &plain,
                    now,
                )
            },
            fallback(&calls),
        );
        assert!(done.result.is_err());
        assert!(done.revocation.is_none());
        assert!(calls.borrow().is_empty());
    }

    #[test]
    fn a_usage_error_still_finds_the_binding_to_revoke() {
        let argv = |words: &[&str]| -> Vec<String> {
            std::iter::once("logweir")
                .chain(words.iter().copied())
                .map(str::to_string)
                .collect()
        };
        assert_eq!(
            revocation_target(&argv(&[
                "identity",
                "bootstrap",
                "--namespace",
                "logweir-system",
                "--a-flag-from-a-newer-chart",
                "--revoke-trust-binding",
                "logweir-identity-trust",
            ])),
            Some("logweir-identity-trust".to_string())
        );
        assert_eq!(
            revocation_target(&argv(&[
                "identity",
                "bootstrap",
                "--revoke-trust-binding=logweir-identity-trust",
            ])),
            Some("logweir-identity-trust".to_string())
        );
        // NEGATIVE CONTROLS: another command, no binding named, a value that
        // is not a name, a dangling flag.
        for words in [
            &["identity", "distribute", "--revoke-trust-binding", "x"][..],
            &["restore", "run", "--revoke-trust-binding", "x"][..],
            &["identity", "bootstrap", "--namespace", "ns"][..],
            &[
                "identity",
                "bootstrap",
                "--revoke-trust-binding",
                "../../secrets",
            ][..],
            &["identity", "bootstrap", "--revoke-trust-binding"][..],
        ] {
            assert_eq!(revocation_target(&argv(words)), None, "{words:?}");
        }
    }

    /// The automatic trust is reachable only with the console key and the
    /// confirm marker (review M2): clap refuses the trust flag alone, so a
    /// chart that renders it without a console fails loudly, before any
    /// step.
    #[test]
    fn the_trust_flag_needs_the_console_key_and_the_confirm_marker() {
        use clap::Parser as _;
        let base = [
            "logweir",
            "identity",
            "bootstrap",
            "--namespace",
            "logweir-system",
            "--installation-trust-policy",
            "logweir-installation",
        ];
        let console = [
            "--console-secret-name",
            "logweir-console-confirmation",
            "--console-public-configmap-name",
            "logweir-console-trust",
        ];
        let parse = |extra: &[&str]| {
            crate::cli::Cli::try_parse_from(base.iter().chain(extra.iter()).copied()).is_ok()
        };
        assert!(!parse(&[]), "the trust flag alone");
        assert!(!parse(&console), "without the confirm marker");
        assert!(
            !parse(&["--mark-fresh-install-confirm"]),
            "without the console key"
        );
        let mut all = console.to_vec();
        all.push("--mark-fresh-install-confirm");
        assert!(parse(&all));
    }
}
