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
    #[command(subcommand)]
    Drill(DrillCmd),
    /// Print a JSON Schema. v0.1 accepts only `scorecard`.
    Schema { which: String },
    /// Check credentials, engine digest and glibc floor, target reachability,
    /// marker topic and approver key — before a drill is attempted.
    Doctor {
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        allowed_clusters: PathBuf,
        #[arg(long)]
        approver_key: PathBuf,
    },
}

#[derive(Subcommand)]
pub enum DrillCmd {
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
    Show {
        scorecard: PathBuf,
        #[arg(long, default_value = "table")]
        format: String,
    },
    Verify {
        #[arg(long)]
        scorecard: PathBuf,
        #[arg(long)]
        signature: PathBuf,
        #[arg(long)]
        public_key: PathBuf,
    },
}
