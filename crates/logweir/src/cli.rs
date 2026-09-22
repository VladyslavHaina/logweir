use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "logweir",
    version,
    about = "Signed restore drills for Apache Kafka®"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Provision or adopt the persistent Kubernetes installation signer.
    #[command(subcommand)]
    Identity(IdentityCmd),
    // Task 22 fix round 1, FIX 5. This is a `//` comment, not a `///` one, on
    // purpose: clap turns doc comments into help TEXT, so build rationale in a
    // `///` would be printed to users by `logweir drill --help`. The `///`
    // lines below are the description clap renders; before they existed the
    // `drill` row of `logweir --help` was blank — the first thing a new user
    // sees of the subcommand the whole tool exists for.
    /// Run a restore drill, or show and verify the scorecard one produced.
    ///
    /// `drill run` is the tag-0 name for `restore run`; it takes the same
    /// flags, parses the same spec, calls the same function and prints one
    /// deprecation line on stderr.
    #[command(subcommand)]
    Drill(DrillCmd),
    // Chain L, Task 9b, interfaces I20/I8. `restore` is the NAME: a restore
    // into a new topic at a point in time on a real cluster is what tag 1's
    // flagship does, and `drill` is that same command with
    // `target.mode: scratch`. The `///` line below is what clap renders.
    /// Restore a sampled window from an archive into a target cluster, verify
    /// it per record, and emit a signed scorecard.
    #[command(subcommand)]
    Restore(RestoreCmd),
    // Chain L, Task 4. The `///` line below is what clap renders in
    // `logweir --help`; the build rationale stays in `//` comments so it is
    // not printed to users (the same rule the `Drill` arm above records).
    /// Back up a source cluster into an archive, from a Logweir spec.
    #[command(subcommand)]
    Backup(BackupCmd),
    /// Print a JSON Schema. Tag 1 accepts `scorecard` and `backup-receipt`.
    Schema { which: String },
    // PLAT-14.2 / D3 §3.4. DELIBERATELY A SUBCOMMAND GROUP WITH ONE LEAF: the
    // D2 check runner adds a sibling `notify check` to this same enum, and a
    // flat `NotifyDeliver` would have to be renamed to make room for it.
    /// Deliver one protection event to the configured notification sinks.
    #[command(subcommand)]
    Notify(NotifyCmd),
    // PLAT-15.1 / decision D3 §5. A SUBCOMMAND GROUP under `catalog`, for the
    // reason `Backup` records for its own: `catalog verify`, `catalog show`
    // and the disaster-import half (PLAT-15.2) have somewhere to land without
    // changing either of these two argv surfaces.
    //
    // These are the OPERATOR's commands and not a runner surface. D-SEAMS S1
    // fixes one check runner, and a controller that wants a catalog
    // synchronised runs `logweir check run` with D2's `catalogSync` plan kind
    // in a Job — see `crates/logweir/src/catalog/cli.rs`'s module header for
    // the four differences that keep these two from being a second execution
    // path.
    /// Read and backfill the durable recovery catalog in an archive's
    /// evidence root.
    #[command(subcommand)]
    Catalog(CatalogCmd),
    // PLAT-19.1 / decision D3 §7.1 and §7.5. A SUBCOMMAND GROUP, for the
    // reason `Catalog` and `Notify` record for theirs: `trust show` and the
    // PLAT-19.2 confirmation-key commands have somewhere to land without
    // renaming either of these two argv surfaces.
    //
    // NEITHER LEAF DIALS KUBERNETES, and neither takes a key. Both read one
    // object on stdin or from a file and write one document on stdout — see
    // `crates/logweir/src/trust.rs`'s module header for why `kubectl` is the
    // credential rather than this binary.
    /// Back up and migrate the cluster's explicit trust policy.
    #[command(subcommand)]
    Trust(TrustCmd),
    // Decision D2 §4.2 / D-SEAMS S1. A SUBCOMMAND GROUP for the same
    // structural reason `Notify` and `Catalog` are one: D2 §4.5 phase 2 turns
    // the `KafkaCluster` probe into a sixth plan kind, and PLAT-09.2 invokes
    // the same `check run` from a Backup-owned Job. A flat `CheckRun` would
    // have to be renamed to make room for a sibling.
    //
    // This is the RUNNER's surface and not an operator's. The controller
    // writes the argv (`weirkeeper::check::job::runner_argv`) and pins the
    // plan digest in the pod environment; a human running it by hand needs the
    // same three variables, which is why the leaf documents them.
    /// Run one check plan — discovery, readiness, restore preflight,
    /// destination access or evidence fetch.
    #[command(subcommand)]
    Check(CheckCmd),
    // Chain L, Task 15c, interface I14. A PURPOSE-BUILT LIVENESS PROBE, and
    // not `doctor` with fewer flags: `doctor` takes two more MANDATORY paths,
    // cannot be told which auth to use, refuses unless the cluster is in a
    // restore-target allowlist AND holds a healthy marker topic — which no
    // `role: source` cluster does — and prints the observed id only inside
    // prose. `crates/logweir/src/probe.rs`'s module header states all four with
    // their call sites. The `///` lines below are what clap renders.
    /// Dial a Kafka cluster, read its cluster id, and print exactly two stdout
    /// lines — `cluster-id=<id>` then `reachable=true|false` — exiting 0 when a
    /// broker answered and 1 when none did.
    ///
    /// This is a LIVENESS PROBE and nothing more. It reads no cluster
    /// allowlist, opens no approver public key, and asserts no marker topic:
    /// those are drill-time guards belonging to phase 0, checked against the
    /// plan a restore is about to run, not against a cluster's reachability.
    /// `--marker-topic` is accepted so a controller can pass a cluster's own
    /// field through unconditionally, and it changes neither the output nor the
    /// exit code.
    ///
    /// With `--auth-mode scramSha512` the SASL password is read from the
    /// environment variable `LOGWEIR_SOURCE_PASSWORD` and from nowhere else:
    /// there is deliberately NO flag for it, because a secret on an argv is
    /// visible in every process listing on the host and in the Job spec a
    /// controller creates.
    ClusterProbe {
        /// The broker bootstrap addresses, `host:port`, comma-separated. ONE
        /// value rather than a repeated flag: a controller joins the cluster
        /// object's own `bootstrapServers` with commas, so the argv element it
        /// writes is a string it can compare against what it read.
        #[arg(long)]
        bootstrap: String,
        /// `plaintext` or `scramSha512` — byte-identical to the spec's and the
        /// `KafkaCluster` CRD's own spellings, so a value read off the object
        /// can be copied straight onto this argv. Anything else is reported as
        /// an unreachable probe naming the mode, never as a usage error: the
        /// two contract lines exist for every command line that parsed.
        #[arg(long, default_value = crate::probe::AUTH_MODE_PLAINTEXT)]
        auth_mode: String,
        /// The SASL principal. Required in practice at
        /// `--auth-mode scramSha512`; ignored at `plaintext`.
        #[arg(long)]
        username: Option<String>,
        /// Whether the transport is TLS, INDEPENDENT of the mode: SASL/SCRAM
        /// over PLAINTEXT and SASL/SCRAM over SSL are two `security.protocol`
        /// values for one mechanism, which is why the cluster object carries
        /// `auth.tls` beside `auth.mode` (Global Constraint 29).
        #[arg(long)]
        tls: bool,
        /// Accepted and NEVER asserted — see this subcommand's description.
        #[arg(long)]
        marker_topic: Option<String>,
    },
    /// Check credentials, engine VERSION and glibc floor, target reachability,
    /// marker topic and approver key — before a drill is attempted.
    //
    // "engine version", not "engine digest": `doctor` compares the engine's
    // `--version` string against the pinned 0.21.0 and computes NO digest.
    // `third_party/kafka-backup-binary.digest` is read only to interpolate
    // into a failure message. The module's own doc comment said "engine
    // version pinned" and was right; this help text was the wrong one, and it
    // is the line an operator reads before deciding what `doctor` proved.
    Doctor {
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        allowed_clusters: PathBuf,
        #[arg(long)]
        approver_key: PathBuf,
        /// Fix round 2, M5: treat any skipped check (e.g. `storage` with no
        /// live bucket to list against) as a failure to verify, exiting 1
        /// instead of 0. Off by default — a skip alone must not fail a run
        /// that genuinely had no way to look (addendum A2/A4).
        #[arg(long)]
        strict: bool,
    },
}

/// PLAT-14.2's delivery leaf. Kept SMALL on purpose — D3 §14 gives this enum a
/// second arm later, and the enum is the seam the two tasks share.
#[derive(Subcommand)]
pub enum NotifyCmd {
    /// POST one protection event to every configured sink, and report each one
    /// on stdout.
    ///
    /// Sinks come from the ENVIRONMENT, never from a flag, because a routing
    /// key or a signed webhook URL on an argv is visible in every process
    /// listing on the host and lands in the Job spec anyone with pod read can
    /// see: `PAGERDUTY_ROUTING_KEY`, `NOTIFY_WEBHOOK_URL`,
    /// `NOTIFY_SLACK_WEBHOOK_URL`, plus `PAGERDUTY_ENDPOINT` for a non-US
    /// PagerDuty service region. A variable that is present and blank is not a
    /// configured sink.
    ///
    /// Prints `notify-result=<sink>:<ok|failed>` for each configured sink as
    /// its final stdout lines. Exits 0 when every configured sink accepted
    /// (including when none was configured), 1 when one did not, and 3 when
    /// the event document itself was refused — in which case nothing was
    /// posted and no `notify-result=` line is printed.
    Deliver {
        /// The protection event document
        /// (`application/vnd.logweir.protection-event+json;version=1.0.0`).
        /// In the shipped Job this is the controller's immutable ConfigMap
        /// projected at `/event/event.json`.
        #[arg(long)]
        event: PathBuf,
    },
}

/// PLAT-19.1's two operator commands (decision D3 §7.1, §7.5).
#[derive(Subcommand)]
pub enum TrustCmd {
    /// Write a `TrustPolicy`'s PUBLIC material as a reviewable, re-appliable
    /// document — the documented backup of a cluster's trust.
    ///
    /// RBAC grants `delete` on `trustpolicies` to no Logweir role, but a
    /// cluster-admin is outside the threat boundary (`docs/stability.md` O0),
    /// so the object needs a backup that is not the cluster. This is it:
    ///
    ///   kubectl --context <ctx> get trustpolicy org-default -o json \
    ///     | logweir trust export --policy org-default --stdin > trustpolicy.yaml
    ///
    /// The output carries `apiVersion`, `kind`, `metadata.name` and `spec`,
    /// rebuilt field by field from the fields this build knows — never a
    /// filtered copy — so nothing a future schema adds can be forwarded by
    /// accident. `status`, `managedFields`, `resourceVersion` and `uid` are
    /// absent, so the file re-applies cleanly onto any cluster. An input
    /// carrying a private-key PEM is REFUSED and nothing is written.
    Export {
        /// The policy name the input object must carry. A mismatch is refused
        /// rather than renamed.
        #[arg(long)]
        policy: String,
        /// Read the object from standard input. Exactly one of `--stdin` and
        /// `--from` is required.
        #[arg(long, conflicts_with = "from", required_unless_present = "from")]
        stdin: bool,
        /// Read the object from this file instead of standard input.
        #[arg(long)]
        from: Option<PathBuf>,
    },
    /// Translate `TrustRoster/default` into a reviewable `TrustPolicy`
    /// document. **It applies nothing and deletes nothing.**
    ///
    ///   kubectl --context <ctx> get trustroster default -o json \
    ///     | logweir trust migrate-roster --stdin --name org-default --default \
    ///     > trustpolicy.yaml
    ///
    /// `approverKeys` become `GovernedApproval`, `signingKeys` become
    /// `EvidenceSigning`, a key on both lists becomes ONE entry with both
    /// usages, `allowedClusterIds` becomes `allowedTargetClusterIds`,
    /// `notAfter` is carried verbatim and every key is `Active`. No
    /// `ConsoleConfirmation` key is ever synthesised (D3 §7.3), and no
    /// retirement or revocation is invented. The roster is left in place so a
    /// rollback still reads it.
    ///
    /// IDEMPOTENT: no clock is read and no name is generated, so two runs over
    /// the same roster produce byte-identical output.
    MigrateRoster {
        /// `metadata.name` of the policy to emit.
        #[arg(long)]
        name: String,
        /// Read the roster from standard input. Exactly one of `--stdin` and
        /// `--from` is required.
        #[arg(long, conflicts_with = "from", required_unless_present = "from")]
        stdin: bool,
        /// Read the roster from this file instead of standard input.
        #[arg(long)]
        from: Option<PathBuf>,
        /// Set `spec.default: true` — the policy every namespace no other
        /// policy names falls back to. At most one policy cluster-wide may set
        /// it; a second one makes every fallback namespace a conflict that
        /// resolves to NOTHING.
        #[arg(long)]
        default: bool,
        /// A namespace this policy governs by exact name. REPEATABLE, one flag
        /// per namespace.
        //
        // NO `value_delimiter`, for the reason `--approver-key-ids` records:
        // one accepted shape means a comma-joined value is one namespace name,
        // matches nothing, and is refused loudly instead of reinterpreted.
        #[arg(long)]
        namespace: Vec<String>,
    },
}

/// D2 §4.2's runner leaf. Kept a GROUP on purpose — see the `Check` arm above.
#[derive(Subcommand)]
pub enum CheckCmd {
    /// Execute the mounted check plan and relay its result as frames on
    /// stdout.
    ///
    /// The plan document is READ ONCE and its SHA-256 is compared against
    /// `$LOGWEIR_CHECK_PLAN_SHA256` before it is parsed, and its `subjectUid`
    /// against `$LOGWEIR_CHECK_SUBJECT_UID`, before any client is built — so a
    /// plan ConfigMap swapped under a running Job is refused rather than
    /// executed against the wrong object. `$LOGWEIR_CHECK_CONTRACT_VERSION`
    /// must equal `--check-contract-version`.
    ///
    /// Stdout carries the frames and nothing else: `logweir-check-topic=`
    /// lines, `logweir-check-part=` lines and one final `logweir-check-end=`
    /// line. Stderr carries JSON tracing at `warn`.
    ///
    /// Exits 0 whenever an end line was printed, WHATEVER the per-check states
    /// — a check that found a problem is a result, not a failure. Exits 1 on
    /// an operational failure before a result existed, with no end line. Exits
    /// 3 when the plan is refused, printing `refusal-reason=` and no frame.
    /// 2 and 4 are never returned.
    Run {
        /// The check plan document. In the shipped Job this is the
        /// controller's immutable ConfigMap projected at
        /// `/check/check-plan.json`.
        #[arg(long)]
        plan: PathBuf,
        /// The check contract version this invocation asks for. A value other
        /// than the one this build implements is a refusal, not a
        /// negotiation: a runner that silently accepted a newer contract would
        /// do less than the controller believes it did.
        #[arg(long)]
        check_contract_version: u32,
    },
}

#[derive(Subcommand)]
pub enum IdentityCmd {
    /// Initialize retained private/public identity objects, or validate the
    /// identity already stored there. Intended for the chart's short-lived
    /// bootstrap Job, not for a long-lived controller.
    Bootstrap {
        /// Namespace containing the retained identity objects.
        #[arg(long)]
        namespace: String,
        /// Retained Secret written exactly once by bootstrap.
        #[arg(long, default_value = "logweir-signing-key")]
        secret_name: String,
        /// Key in the retained Secret.
        #[arg(long, default_value = "signing.pem")]
        secret_key: String,
        /// Retained ConfigMap carrying public verification material.
        #[arg(long, default_value = "logweir-signing-trust")]
        public_configmap_name: String,
        /// Explicit source Secret to adopt instead of generating a key.
        #[arg(long, requires = "external_secret_key")]
        external_secret_name: Option<String>,
        /// Key within --external-secret-name.
        #[arg(long, requires = "external_secret_name")]
        external_secret_key: Option<String>,
    },
    /// Copy the already established installation signer into one explicitly
    /// authorized runner namespace. Refuses missing, incomplete, or different
    /// target identities; intended only for the chart's short-lived hook Job.
    Distribute {
        /// Namespace containing the primary installation signing Secret.
        #[arg(long)]
        source_namespace: String,
        /// Authorized runner namespace receiving the same signing identity.
        #[arg(long)]
        target_namespace: String,
        /// Fixed retained Secret name in both namespaces.
        #[arg(long, default_value = "logweir-signing-key")]
        secret_name: String,
        /// Key in the retained Secret.
        #[arg(long, default_value = "signing.pem")]
        secret_key: String,
    },
}

/// `#[allow(clippy::large_enum_variant)]`, and the reason is clap.
///
/// `Run(RestoreRunArgs)` carries every flag `logweir restore run` takes —
/// twelve of them since execution contract v2 added the three bundle members —
/// while `Approve` carries a handful. Boxing the large variant is the lint's
/// usual remedy and is not available: `#[command(flatten)]` needs the `Args`
/// value by type, and a `Box` around it stops clap deriving the flattened
/// surface at all. Two subcommands of a CLI enum are constructed once per
/// process, so the size difference costs nothing measurable.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
pub enum DrillCmd {
    /// Mint the DSSE-signed approval `drill run --approval` requires. Runs no
    /// drill and touches no cluster.
    //
    // The unowned-path audit: `drill run --approval` is MANDATORY, and the
    // only producer in the tree was a cargo EXAMPLE
    // (`logweir-evidence/examples/sign_approval.rs`), which ships in neither
    // the container image nor the release tarballs. On the Kubernetes path
    // this product targets, an operator could not mint `approval.sig` from
    // Logweir's own artifacts at all — and `plan_hash` binds the approval to
    // the exact spec bytes, so they would have to re-mint on every spec edit.
    // See `crates/logweir/src/approve.rs` for why this is a subcommand rather
    // than a second `[[bin]]`.
    Approve {
        /// The drill spec this approval authorises. `plan_hash` is the sha256
        /// of its EXACT bytes, so re-run this after any edit — including a
        /// moved `sample.window_*`.
        ///
        /// Required for a per-run approval; **not used with `--standing`**,
        /// which binds a SCOPE covering every slot of one schedule rather than
        /// one plan's hash.
        #[arg(long)]
        spec: Option<PathBuf>,
        /// The approver's PRIVATE key (PKCS#8 PEM, P-256 or Ed25519).
        /// `drill run --approver-key` takes the matching PUBLIC key.
        #[arg(long)]
        key: PathBuf,
        /// Who approved: a person, a rota address, a change-management
        /// identity. Copied into the signed scorecard verbatim.
        ///
        /// Not used with `--standing`: version 1.0.0 of the standing document
        /// carries no such field, so a value would not be signed.
        //
        // REQUIRED, AND ONLY `--standing` RELAXES IT. Fix round 1 made both
        // `default_value = ""` so the standing arm would not have to supply
        // them, which silently allowed a PER-RUN approval to be minted by
        // nobody under no ticket — signed bytes that `phase1_approval::verify`
        // copies verbatim into the scorecard's `approval.approver`. The
        // accountability record is the product's output; it does not get to be
        // blank because a sibling subcommand found the flag inconvenient.
        //
        // `Option<String>` is the shape clap-derive needs for a CONDITIONALLY
        // required argument: a bare `String` makes the derive infer
        // `required(true)` (which then conflicts with `required_unless_present`)
        // and, with `required(false)`, fail at RUNTIME when extracting the
        // value. `main` collapses the absent case, which only `--standing`
        // reaches.
        #[arg(long, required_unless_present = "standing")]
        approver: Option<String>,
        /// The change ticket this drill is authorised under. Not used with
        /// `--standing`, for the same reason as `--approver`.
        #[arg(long, required_unless_present = "standing")]
        ticket: Option<String>,
        /// Where the approval JSON is written. Its DSSE sidecar lands beside
        /// it with the extension replaced by `.sig`, which is the only place
        /// `drill run` looks for it.
        #[arg(long, default_value = "approval.json")]
        out: PathBuf,
        /// Which kind of subject this approval authorises. Written INSIDE the
        /// signed bytes, so it cannot be changed without invalidating the
        /// signature.
        ///
        /// RUNNER: an approval with no `subject_kind` at all — every approval
        /// minted before this flag existed — is treated as `Restore` and
        /// verifies unchanged. CONTROLLER: the same approval is refused by
        /// the `Approval` reconciler's check 8 when the referent it names is
        /// not a `Restore`, because there the field is compared against the
        /// referent's actual kind and an absent field matches nothing.
        //
        // `conflicts_with = "standing"` and NOT a silent override. Under
        // `--standing` the document's `subjectRef.kind` is always
        // `RehearsalSchedule`, so an operator who wrote `--subject-kind
        // Backup` had their explicit instruction discarded while this flag's
        // own help promised it was "written INSIDE the signed bytes". clap
        // does not count a default as present for a conflict, so the ordinary
        // `--standing` invocation is unaffected.
        #[arg(
            long,
            value_enum,
            default_value_t = SubjectKindArg::Restore,
            conflicts_with = "standing"
        )]
        subject_kind: SubjectKindArg,
        /// Mint a **standing rehearsal authorization** (D3 §4.3(e)) instead of
        /// a per-run approval: one signature covering every slot of one
        /// `RehearsalSchedule`, which is what
        /// `logweir restore run --standing-authorization` verifies and what
        /// the `Approval` controller requires for a `RehearsalSchedule`
        /// referent.
        ///
        /// Requires `--scope`, `--schedule-namespace`, `--schedule-name` and
        /// `--schedule-uid`; refuses `--spec`, `--approver` and `--ticket`,
        /// none of which this document binds.
        #[arg(long)]
        standing: bool,
        /// With `--standing`: the `RehearsalSchedule`'s namespace.
        #[arg(long, requires = "standing", required_if_eq("standing", "true"))]
        schedule_namespace: Option<String>,
        /// With `--standing`: the `RehearsalSchedule`'s name.
        #[arg(long, requires = "standing", required_if_eq("standing", "true"))]
        schedule_name: Option<String>,
        /// With `--standing`: the `RehearsalSchedule`'s `metadata.uid`.
        ///
        /// **THE BINDING.** The runner compares it against the UID the
        /// controller stamps on the Job, so a document signed for a schedule
        /// that was deleted and recreated under the same name authorises
        /// nothing.
        #[arg(long, requires = "standing", required_if_eq("standing", "true"))]
        schedule_uid: Option<String>,
        /// With `--standing`: a JSON file holding D3 §4.3's `RehearsalScope`
        /// in camelCase — `templateDigest`, `targetClusterId`, `topicPrefix`,
        /// `topics`, `maxPartitions`, `recordsPerPartition`,
        /// `deadlineSeconds`, `modes`.
        ///
        /// A FILE, not a dozen flags: it is what the signature covers, so an
        /// operator should be able to diff it and keep it in version control.
        #[arg(long, requires = "standing", required_if_eq("standing", "true"))]
        scope: Option<PathBuf>,
        /// With `--standing`: `expiresAt - issuedAt` in days. D3 §4.3 caps it
        /// at 90 and this command refuses more, so the limit is learned at
        /// minting time and not from a Job that will not start.
        #[arg(long, requires = "standing", default_value_t = 30)]
        valid_days: i64,
        /// With `--standing`: override `issuedAt` (RFC 3339). An operator has
        /// no reason to set it; it exists so a test can mint the same bytes
        /// twice.
        #[arg(long, requires = "standing")]
        issued_at: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// Countersign a GOVERNED restore request (PLAT-19.2): add a
    /// `GovernedApproval` signature to the console-confirmed authorization
    /// document v2 an approver downloaded from the console. Prints the
    /// requester, subject, plan hash, policy and window it is about to sign,
    /// signs the EXACT document bytes, and writes a sidecar carrying the
    /// console's signature and this one. Runs no drill and touches no cluster.
    Countersign {
        /// The authorization document v2, verbatim, as the console's
        /// confirmation packet carries it (`approvalBytes`).
        #[arg(long)]
        document: PathBuf,
        /// The console's DSSE sidecar over that document (`sidecarBytes`).
        #[arg(long)]
        confirmation: PathBuf,
        /// The approver's PRIVATE key (PKCS#8 PEM). Its public half must be on
        /// the namespace's `TrustPolicy` with usage `GovernedApproval` and a
        /// `principal.id` of the approver's own `<issuer>#<subject>`.
        #[arg(long)]
        key: PathBuf,
        /// Where the countersigned sidecar is written.
        #[arg(long)]
        out: PathBuf,
    },
    /// Run the drill: restore a sampled window into the scratch cluster,
    /// reconcile it per record, and emit a signed scorecard.
    ///
    /// With `target.auth.mode: scramSha512` in the spec, the SASL password is
    /// read from the environment variable `LOGWEIR_TARGET_PASSWORD` and from
    /// nowhere else: there is deliberately NO flag for it, at any command. A
    /// secret on an argv is visible in `/proc/<pid>/cmdline`, in a shell
    /// history and in every process listing on the host, and it would land in
    /// the Job spec a controller creates. An UNSET variable under that mode
    /// exits 1 (nothing was refused — project the Secret and re-run); a value
    /// that cannot be substituted into the engine's pre-parse config text
    /// exits 3 with `refusal-reason=CredentialNotRenderable`.
    Run(RestoreRunArgs),
    /// Render a scorecard as a fixed-width table (or `--format json`, the
    /// exact stored bytes). Reads a file; runs no drill and touches no cluster.
    Show {
        /// Path to the scorecard JSON, as written by `drill run --out`.
        scorecard: PathBuf,
        #[arg(long, default_value = "table")]
        format: String,
    },
    /// Check a signed Logweir document's DSSE signature against a public
    /// key. Exit 0 only if the signature covers the bytes of `--scorecard`
    /// exactly as stored.
    Verify {
        // The flag stays `--scorecard` even though `--payload-type` now lets
        // it name three other documents. Renaming it would break every
        // existing invocation, every doc, `scripts/check-verifier-parity.sh`
        // and `docs/verify-a-scorecard.md` in exchange for a better word; a
        // `--document` alias is a tag-2 conversation, not a tag-1 rename.
        #[arg(long)]
        scorecard: PathBuf,
        #[arg(long)]
        signature: PathBuf,
        #[arg(long)]
        public_key: PathBuf,
        /// Which of the four documents Logweir signs this is —
        /// `scorecard`, `backup-receipt`, `receipt` (the post-put storage
        /// readback) or `teardown`.
        //
        // DEFAULT `scorecard`, so every invocation that existed before Task 5
        // is byte-for-byte unchanged: the three-argument form still verifies
        // a scorecard and still runs its invariant arms. The value is
        // resolved to a media type by `crate::verify::resolve_payload_type`,
        // and an unrecognised one is an ERROR (exit 1) rather than a
        // passthrough of anything containing a slash — a typo would otherwise
        // surface as "unexpected payloadType" and read like a bad artifact
        // instead of a bad command line. `docs/verify_scorecard.py`'s
        // `--payload-type` is the same flag with the same short names, so an
        // auditor runs the two readers with one command line.
        #[arg(long, default_value = "scorecard")]
        payload_type: String,
    },
}

#[derive(Subcommand)]
pub enum RestoreCmd {
    /// Restore the window the spec names into the target cluster, reconcile it
    /// per record, and emit a signed scorecard.
    ///
    /// With `target.mode: newTopic` the restore lands in brand-new topics
    /// named `restore-<YYYYmmddTHHMMSSZ>-<topic>` (or under
    /// `target.topicNaming.prefix`), and is refused before anything runs if any
    /// of those topics already exists — appending into a half-populated topic
    /// produces a restore that reconciles against records it did not write.
    /// Nothing is torn down in that mode.
    ///
    /// With `target.mode: scratch` — the default, and what `logweir drill run`
    /// has always done — the marker topic must exist, the target cluster must
    /// be in `--allowed-clusters`, and phase 9 deletes the topics this run
    /// created.
    ///
    /// With `target.auth.mode: scramSha512` in the spec, the SASL password is
    /// read from the environment variable `LOGWEIR_TARGET_PASSWORD` and from
    /// nowhere else — see `drill run` for why there is no flag for it.
    Run(RestoreRunArgs),
}

/// The flags `logweir restore run` and `logweir drill run` share.
///
/// **ONE clap struct, flattened into both subcommands**, which is what makes
/// "the alias takes the same flags" a property of the type rather than a
/// promise in a doc comment: there is no second list to forget to update, and
/// `crates/logweir/tests/restore_mode.rs::
/// cli_drill_run_is_an_alias_for_restore_run` parses one command line under
/// each name and compares the two values with `PartialEq`.
///
/// `logweir drill run` therefore also accepts `--offset-report-out`, which it
/// did not have before. That is a SUPERSET: every invocation that worked
/// before still works, and the alternative — two field lists differing by one
/// flag — is the drift this struct exists to prevent.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct RestoreRunArgs {
    /// Controller-to-runner compatibility handshake for newly created Jobs.
    /// Omit only for an intentional legacy Job or standalone invocation.
    #[arg(long)]
    pub execution_contract_version: Option<String>,
    /// **D2 §3.5's store-contract handshake.** Written by the controller for a
    /// destination-backed Job and omitted for every legacy or standalone
    /// invocation. Separate from `--execution-contract-version` because the
    /// two answer different questions — that one is which plan grammar this
    /// is, this one is where the stores come from — and coupling them would
    /// have tied a destination rollout to a plan-grammar rollout.
    #[arg(long)]
    pub store_contract_version: Option<String>,
    #[arg(long)]
    pub spec: PathBuf,
    /// The per-run `Approval` document. MANDATORY for every ordinary Restore
    /// — approval is unconditional (spec §9.3 phase 1).
    ///
    /// **Omitted only for a standing-authorized rehearsal** (PLAT-14.3b): the
    /// signed standing document given by `--standing-authorization` REPLACES
    /// it, and the run's execution contract must say so
    /// (`LOGWEIR_EXECUTION_AUTHORIZATION_KIND=standing`). Omitting it under
    /// any other shape is exit 3 by name, and giving it under a standing
    /// contract is exit 3 too: a rehearsal bundle has no approval slot.
    #[arg(long)]
    pub approval: Option<PathBuf>,
    /// MANDATORY: the approver's public key, which SHOULD differ from the
    /// signing key. Equal keys are labelled self_attested, never refused.
    #[arg(long)]
    pub approver_key: PathBuf,
    /// Pin which approver key ids this run accepts. REPEATABLE — one flag per
    /// id (`--approver-key-ids a --approver-key-ids b`), which is the shape
    /// the operator emits, one flag per unexpired `TrustRoster` approver key.
    ///
    /// OMITTING IT CHANGES NOTHING: with no ids given the run behaves exactly
    /// as it did before this flag existed. With ids given, an approval whose
    /// approver key id is outside the set is refused with exit 3 BEFORE phase
    /// 0 dials anything.
    //
    // NO `value_delimiter`, DELIBERATELY, and this comment is the pin. A
    // comma-separated form would make `--approver-key-ids "a,b"` silently mean
    // two ids while the operator never emits that shape
    // (`restore_controller.rs::only_unexpired_roster_key_ids_reach_the_argv`:
    // "repeated flags, never one comma-joined value"). One accepted shape
    // means a comma-joined value is ONE id, matches nothing, and refuses
    // loudly with the offending string in the message instead of being
    // reinterpreted. `crates/logweir/tests/cli_exit_codes.rs::
    // a_comma_joined_approver_key_id_is_one_id_not_two` asserts it.
    #[arg(long)]
    pub approver_key_ids: Vec<String>,
    #[arg(long)]
    pub allowed_clusters: PathBuf,
    #[arg(long)]
    pub signing_key: PathBuf,
    #[arg(long)]
    pub triggered_by: Option<String>,
    /// Where the scorecard is written locally, in addition to the bucket.
    /// The DSSE sidecar lands beside it at `<out>` with the extension
    /// replaced by `.sig`. Default: `./logweir-<run_id>.json`.
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Prometheus textfile-collector output (spec §13). Task 21a.
    #[arg(long)]
    pub metrics_file: Option<PathBuf>,
    /// Where the ENGINE writes its offset-mapping report. Pod-local; the run
    /// uploads it beside the scorecard at
    /// `logweir/drills/<run_id>.offsets.json` and records that key and its
    /// sha256 in the signed document. Default: `offsets.json` in this run's
    /// workdir, beside the restore checkpoint.
    ///
    /// The report is WRITTEN and never applied: tag 1 commits no consumer-group
    /// offset on any cluster.
    #[arg(long)]
    pub offset_report_out: Option<PathBuf>,
    /// **Execution contract v2** (decision D3 §4.3(e)): the SIGNED standing
    /// rehearsal authorization document, projected into the bundle. Its DSSE
    /// sidecar is read from the same path with the extension replaced by
    /// `.sig`, exactly as `--approval`'s is.
    ///
    /// Omit it for every ordinary Restore — omitting it changes nothing. When
    /// it IS given, `--authorization-keys` is required too, its bytes must
    /// match `LOGWEIR_EXECUTION_AUTHORIZATION_SHA256`, the signature must
    /// verify under a pinned key allowed to authorise, and the rendered plan
    /// must fall inside the scope the signed document carries — or the run is
    /// refused with exit 3 before any client is constructed.
    #[arg(long)]
    pub standing_authorization: Option<PathBuf>,
    /// **Execution contract v2**: the trusted public keys the standing
    /// authorization's signature is checked against (D3 §4.3(e)).
    #[arg(long)]
    pub authorization_keys: Option<PathBuf>,
    /// **Bundle contract v2**: the frozen approval-policy snapshot
    /// (PLAT-19.2). Given together with `--confirmation-key`, its digest must
    /// match `LOGWEIR_EXECUTION_POLICY_SNAPSHOT_SHA256`, and `--approval` is
    /// then verified as an authorization document v2 under the snapshot's
    /// mode (the console's signature always; a distinct approver's too under
    /// `Governed`) before any client is constructed — exit 3 otherwise.
    #[arg(long)]
    pub policy_snapshot: Option<PathBuf>,
    /// **Bundle contract v2**: the console's `ConsoleConfirmation` public key —
    /// the second of the two public keys a v2 bundle carries (PLAT-19.2). Its
    /// digest must match `LOGWEIR_EXECUTION_CONFIRMATION_KEY_SHA256`, and the
    /// console's signature over the authorization document is verified with
    /// it.
    #[arg(long)]
    pub confirmation_key: Option<PathBuf>,
    /// **Execution contract v2** (D3 §5.5 step 6): the evidence-signing
    /// keyring a plan bound to a recovery point (`source.point`) verifies the
    /// point's receipt signature against, before any client is constructed. A
    /// point-bound plan without it is refused with exit 3 `PointUntrusted`.
    #[arg(long)]
    pub evidence_keys: Option<PathBuf>,
}

impl From<RestoreRunArgs> for crate::drill::RunArgs {
    /// The one mapping from the parsed command line to the run's arguments, so
    /// neither CLI name can build a different `RunArgs` from the same flags.
    fn from(a: RestoreRunArgs) -> Self {
        crate::drill::RunArgs {
            execution_contract_version: a.execution_contract_version,
            store_contract_version: a.store_contract_version,
            spec: a.spec,
            approval: a.approval,
            approver_key: a.approver_key,
            approver_key_ids: a.approver_key_ids,
            allowed_clusters: a.allowed_clusters,
            signing_key: a.signing_key,
            triggered_by: a.triggered_by,
            out: a.out,
            metrics_file: a.metrics_file,
            offset_report_out: a.offset_report_out,
            standing_authorization: a.standing_authorization,
            authorization_keys: a.authorization_keys,
            policy_snapshot: a.policy_snapshot,
            confirmation_key: a.confirmation_key,
            evidence_keys: a.evidence_keys,
        }
    }
}

#[derive(Subcommand)]
pub enum BackupCmd {
    /// Take a backup of the named source topics with the pinned engine, behind
    /// phase −1's admission guard, and read the resulting archive back.
    ///
    /// With `source.auth.mode: scramSha512` in the spec, the SASL password is
    /// read from the environment variable `LOGWEIR_SOURCE_PASSWORD` and from
    /// nowhere else — see `drill run` for why there is no flag, and for the
    /// two exit codes an unset and an unrenderable value produce.
    //
    // GC18(c)'s four rails all bind this command; `crates/logweir/src/backup/mod.rs`
    // says where each one is enforced. It is a SUBCOMMAND under `backup` rather
    // than a bare `logweir backup` so Task 18's scheduler and a future
    // `backup verify` have somewhere to land without changing this one's argv.
    Run {
        /// **D2 §3.5's store-contract handshake.** Written by the controller
        /// for a destination-backed Job and omitted for every legacy or
        /// standalone invocation. An older `logweir` does not know this flag
        /// and exits on the parse error, which is the point: a new controller
        /// can never drive an old runner into building its stores out of
        /// whatever `AWS_*` happens to be in the pod.
        #[arg(long)]
        store_contract_version: Option<String>,
        #[arg(long)]
        spec: PathBuf,
        /// The restore-TARGET allowlist. Supplied as a SEPARATE file argument,
        /// never read from the spec, so an edited spec cannot widen its own
        /// allowlist. GC18(c) rail 4 refuses a SOURCE cluster that appears in
        /// it: a cluster cannot be both the source of an archive and a
        /// permitted scratch target.
        #[arg(long)]
        allowed_clusters: PathBuf,
        /// The key the backup receipt is signed with (interface I6). Loaded,
        /// exercised and self-verified at startup before any Kafka, storage or
        /// engine client is constructed, then retained in memory for receipt
        /// signing so a mid-run file rotation cannot change this execution's
        /// identity.
        #[arg(long)]
        signing_key: PathBuf,
        #[arg(long)]
        triggered_by: Option<String>,
        /// Where the backup receipt is written locally. Its DSSE sidecar
        /// lands beside it with the extension replaced by `.sig`. The receipt
        /// is ALSO put in the evidence bucket under
        /// `logweir/backups/<backup_id>/<run_id>.receipt.{json,sig}`, whose
        /// two keys are the command's final two stdout lines.
        #[arg(long)]
        out: Option<PathBuf>,
        /// The receipt's own path. Takes precedence over `--out`; naming two
        /// DIFFERENT paths is refused, because this command writes exactly
        /// one document.
        #[arg(long)]
        receipt_out: Option<PathBuf>,
        /// Replaces the spec's `backup_id` for this run (interface I10). Task
        /// 18's `BackupSchedule` reconciler passes `<schedule>-<slot>`.
        #[arg(long)]
        backup_id_override: Option<String>,
    },
}

/// `--subject-kind`'s two values, spelled as they are spelled on the wire.
///
/// `#[value(name = ...)]` on both variants, and that is load-bearing: clap's
/// default `ValueEnum` rendering is kebab-case (`restore`, `backup`), and the
/// string this flag produces goes INTO THE SIGNED BYTES where
/// `weirkeeper::controllers::approval`'s check 8 compares it against the
/// referent's Kubernetes `kind` — `Restore` and `Backup`, capitalised.
/// A lower-cased value would mint approvals that verify on the runner and are
/// refused by every controller, with no error naming the case.
/// `crates/logweir/tests/cli_approve.rs::
/// the_subject_kind_values_are_the_wire_spellings` pins both strings.
///
/// This is a SECOND declaration of the same two names, not a shared type: the
/// `logweir` binary must not take a dependency on `weirkeeper` (the dependency
/// runs the other way, and the CLI ships without a Kubernetes client at all).
/// The coupling is therefore pinned by a test over the strings, which is the
/// only thing the two sides actually share.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum SubjectKindArg {
    #[value(name = "Restore")]
    Restore,
    #[value(name = "Backup")]
    Backup,
    // The standing form. Accepted so an operator who reaches for it is told
    // what to do rather than told nothing: it REQUIRES `--standing`, because a
    // `RehearsalSchedule` referent whose bytes are signed under
    // `PAYLOAD_TYPE_APPROVAL` is refused by the `Approval` controller, and
    // `main` says exactly that when the two are not given together.
    //
    // A LINE COMMENT AND NOT A DOC COMMENT, DELIBERATELY. clap renders a
    // value-enum compactly as `[possible values: …]` only while NO variant
    // carries help text; one doc comment flips the whole list to the long
    // per-variant form and changes every `--help` an adopter has scripted
    // against. `cli_approve.rs::the_subject_kind_values_are_the_wire_spellings`
    // pins the compact line.
    #[value(name = "RehearsalSchedule")]
    RehearsalSchedule,
}

impl SubjectKindArg {
    /// The wire spelling, as it is written into `approval.json`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Restore => "Restore",
            Self::Backup => "Backup",
            Self::RehearsalSchedule => "RehearsalSchedule",
        }
    }
}

// ---------------------------------------------------------------------------
// PLAT-15.1 — the recovery catalog's two operator subcommands
// ---------------------------------------------------------------------------

/// The flags `catalog sync` and `catalog list` share: WHERE the archive is.
///
/// **One `Args` struct flattened into both**, for the reason
/// [`RestoreRunArgs`] records: "the two subcommands take the same location
/// flags" is then a property of the type rather than a promise in a doc
/// comment, and there is no second list to forget to update.
#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct CatalogLocationArgs {
    /// The archive's object-store location: `s3://<bucket>`, `gs://<bucket>`,
    /// `az://<account>/<container>`, `file:///absolute/path`, or an absolute
    /// path. The evidence root `logweir/` is imposed, not read off the URL —
    /// a deeper key prefix is refused rather than silently used (Global
    /// Constraint 6).
    ///
    /// A URL carrying userinfo (`s3://key:secret@bucket`) is REFUSED: a
    /// credential on an argv is visible in every process listing on the host.
    /// Credentials come from the environment the object-store client already
    /// reads.
    #[arg(long)]
    pub url: String,
    #[arg(long)]
    pub region: Option<String>,
    /// A custom S3-compatible endpoint. It does NOT enable plaintext
    /// transport: pass `--allow-http` for that, deliberately and separately.
    #[arg(long)]
    pub endpoint: Option<String>,
    #[arg(long)]
    pub path_style: bool,
    /// Permit plaintext HTTP to the endpoint. **Never derived** from the
    /// endpoint's scheme, from the addressing style or from any environment
    /// value (D-SEAMS S5, defects SEC-ENVHTTP and UI-HTTPDOWNGRADE): a global
    /// setting must never override what the operator asked for.
    #[arg(long)]
    pub allow_http: bool,
}

impl From<&CatalogLocationArgs> for crate::catalog::cli::Location {
    fn from(a: &CatalogLocationArgs) -> Self {
        crate::catalog::cli::Location {
            url: a.url.clone(),
            region: a.region.clone(),
            endpoint: a.endpoint.clone(),
            path_style: a.path_style,
            allow_http: a.allow_http,
        }
    }
}

#[derive(Subcommand)]
pub enum CatalogCmd {
    /// Walk the evidence root's backup receipts and write a signed catalog
    /// point record for every one that does not have one yet.
    ///
    /// Writes ONLY under `logweir/catalog/v1/`, create-only: nothing is ever
    /// rewritten and nothing is ever deleted. Re-running it is idempotent —
    /// point identity is derived from the receipt's own bytes, so a second run
    /// over the same archive produces the same ids and reports them as already
    /// present.
    ///
    /// A receipt whose DSSE signature verifies under none of the
    /// `--public-key` values gets NO record: the receipt's signature is the
    /// verification root, and a record derived from bytes nobody could
    /// authenticate would assert facts this command never established.
    Sync {
        #[command(flatten)]
        location: CatalogLocationArgs,
        /// The key the point records are signed with. Loaded, exercised and
        /// self-verified before the store is dialled.
        #[arg(long)]
        signing_key: std::path::PathBuf,
        /// A public key a backup receipt may verify under. REPEATABLE, one
        /// flag per key, and at least one is required — there is deliberately
        /// no "trust whatever is in the bucket" mode. A public key found
        /// beside an archive is a CLAIM and is never trusted merely by
        /// proximity (`docs/keys.md`).
        //
        // NO `value_delimiter`, for the reason `--approver-key-ids` records:
        // one accepted shape means a comma-joined value is one path, matches
        // nothing, and refuses loudly instead of being reinterpreted.
        #[arg(long)]
        public_key: Vec<std::path::PathBuf>,
        /// Resume the receipt walk strictly AFTER this object key — the value
        /// a previous run printed as `catalog-next=`.
        #[arg(long)]
        since: Option<String>,
        /// How many objects to examine in this run.
        #[arg(long, default_value_t = crate::catalog::cli::DEFAULT_MAX)]
        max: usize,
    },
    /// Print the newest recovery points in an archive's catalog, newest
    /// first. Reads only; writes nothing and signs nothing.
    List {
        #[command(flatten)]
        location: CatalogLocationArgs,
        /// Only index entries whose key is strictly greater than this one.
        #[arg(long)]
        since: Option<String>,
        /// How many rows to print.
        #[arg(long, default_value_t = 50)]
        max: usize,
        /// How many DAY SHARDS back from today to look.
        ///
        /// The listing walks days newest-first and stops the moment `--max`
        /// rows are held, so on an archive with a recent backup this costs one
        /// or two small listings whatever it is set to. It only matters when
        /// the newest point is old — and the output always prints
        /// `catalog-searched-days=` and `catalog-oldest-day-searched=`, so an
        /// empty page says which window it is empty for rather than implying
        /// the catalog is empty.
        //
        // 400 is decision D3 §5.3's own histogram bound (`maxItems: 400`), so
        // the CLI's default window and the Kubernetes view's are one number
        // rather than two.
        #[arg(long, default_value_t = 400)]
        days: u32,
    },
}
