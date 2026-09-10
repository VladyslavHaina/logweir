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
        #[arg(long)]
        spec: PathBuf,
        /// The approver's PRIVATE key (PKCS#8 PEM, P-256 or Ed25519).
        /// `drill run --approver-key` takes the matching PUBLIC key.
        #[arg(long)]
        key: PathBuf,
        /// Who approved: a person, a rota address, a change-management
        /// identity. Copied into the signed scorecard verbatim.
        #[arg(long)]
        approver: String,
        /// The change ticket this drill is authorised under.
        #[arg(long)]
        ticket: String,
        /// Where the approval JSON is written. Its DSSE sidecar lands beside
        /// it with the extension replaced by `.sig`, which is the only place
        /// `drill run` looks for it.
        #[arg(long, default_value = "approval.json")]
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
    #[arg(long)]
    pub spec: PathBuf,
    /// MANDATORY in v0.1: approval is unconditional (spec §9.3 phase 1).
    #[arg(long)]
    pub approval: PathBuf,
    /// MANDATORY: the approver's public key, which SHOULD differ from the
    /// signing key. Equal keys are labelled self_attested, never refused.
    #[arg(long)]
    pub approver_key: PathBuf,
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
}

impl From<RestoreRunArgs> for crate::drill::RunArgs {
    /// The one mapping from the parsed command line to the run's arguments, so
    /// neither CLI name can build a different `RunArgs` from the same flags.
    fn from(a: RestoreRunArgs) -> Self {
        crate::drill::RunArgs {
            spec: a.spec,
            approval: a.approval,
            approver_key: a.approver_key,
            allowed_clusters: a.allowed_clusters,
            signing_key: a.signing_key,
            triggered_by: a.triggered_by,
            out: a.out,
            metrics_file: a.metrics_file,
            offset_report_out: a.offset_report_out,
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
        #[arg(long)]
        spec: PathBuf,
        /// The restore-TARGET allowlist. Supplied as a SEPARATE file argument,
        /// never read from the spec, so an edited spec cannot widen its own
        /// allowlist. GC18(c) rail 4 refuses a SOURCE cluster that appears in
        /// it: a cluster cannot be both the source of an archive and a
        /// permitted scratch target.
        #[arg(long)]
        allowed_clusters: PathBuf,
        /// The key the backup receipt is signed with (interface I6). Opened
        /// once, after the archive has been read back, to sign the receipt.
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
