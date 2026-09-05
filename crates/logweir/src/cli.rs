use clap::{Parser, Subcommand};
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
    #[command(subcommand)]
    Drill(DrillCmd),
    /// Print a JSON Schema. v0.1 accepts only `scorecard`.
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
    Run {
        #[arg(long)]
        spec: PathBuf,
        /// MANDATORY in v0.1: approval is unconditional (spec §9.3 phase 1).
        #[arg(long)]
        approval: PathBuf,
        /// MANDATORY: the approver's public key, which SHOULD differ from the
        /// signing key. Equal keys are labelled self_attested, never refused.
        #[arg(long)]
        approver_key: PathBuf,
        #[arg(long)]
        allowed_clusters: PathBuf,
        #[arg(long)]
        signing_key: PathBuf,
        #[arg(long)]
        triggered_by: Option<String>,
        /// Where the scorecard is written locally, in addition to the bucket.
        /// The DSSE sidecar lands beside it at `<out>` with the extension
        /// replaced by `.sig`. Default: `./logweir-<run_id>.json`.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Prometheus textfile-collector output (spec §13). Task 21a.
        #[arg(long)]
        metrics_file: Option<PathBuf>,
    },
    /// Render a scorecard as a fixed-width table (or `--format json`, the
    /// exact stored bytes). Reads a file; runs no drill and touches no cluster.
    Show {
        /// Path to the scorecard JSON, as written by `drill run --out`.
        scorecard: PathBuf,
        #[arg(long, default_value = "table")]
        format: String,
    },
    /// Check a scorecard's DSSE signature against a public key. Exit 0 only
    /// if the signature covers the bytes of `--scorecard` exactly as stored.
    Verify {
        #[arg(long)]
        scorecard: PathBuf,
        #[arg(long)]
        signature: PathBuf,
        #[arg(long)]
        public_key: PathBuf,
    },
}
