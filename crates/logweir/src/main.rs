use clap::Parser;
use logweir::drill::{phase0_admit, DrillError};
use logweir::{cli, exit, schema, verify};
use logweir_core::spec::{AllowedClusters, DrillSpec};
use logweir_kafka::{rdkafka_reader::RdKafkaReader, reader::AuthConfig};
use std::path::Path;

fn main() -> std::process::ExitCode {
    // `cli::Cli::parse()` (via clap's `Parser::parse`) calls `Error::exit()`
    // on any usage error, which hardcodes exit code 2. Global Constraint 11
    // reserves 2 for "a drill result that is not a pass — a scorecard IS
    // written and signed", and this is a published interface a CronJob and a
    // CI job branch on. A typo'd flag or a chart bump that drops a flag must
    // never be reported as "the drill ran and did not pass": no drill ran
    // and no scorecard exists. So usage errors are handled explicitly here
    // and mapped to `ExitCode::Operational` (1) instead, while `--help` and
    // `--version` (which clap also routes through `Err`, printing to stdout)
    // keep exiting 0.
    let args = match cli::Cli::try_parse() {
        Ok(a) => a,
        Err(e) => {
            let _ = e.print();
            return if e.use_stderr() {
                exit::ExitCode::Operational.into()
            } else {
                std::process::ExitCode::SUCCESS
            };
        }
    };
    let code = match args.command {
        cli::Command::Schema { which } => schema::run(&which),
        cli::Command::Drill(cli::DrillCmd::Verify {
            scorecard,
            signature,
            public_key,
        }) => verify::run(&scorecard, &signature, &public_key),
        // TEMPORARY (Task 14): phase 0 only, so a refused plan exits 3 as Global
        // Constraint 11 requires and `tests/guard_cli.rs` can prove it. Task 21a
        // REPLACES this whole arm with `logweir::drill::run(RunArgs { .. })`.
        cli::Command::Drill(cli::DrillCmd::Run {
            spec,
            allowed_clusters,
            ..
        }) => run_phase0_only(&spec, &allowed_clusters),
        _ => {
            eprintln!("not yet implemented in this task");
            exit::ExitCode::Operational
        }
    };
    code.into()
}

/// Task 14 only. Reads the spec and the allowed-clusters file, runs the phase-0
/// admission guard, and maps its refusal to exit 3. Phases 1-9 land in Task 21a.
fn run_phase0_only(spec_path: &Path, allowed_path: &Path) -> exit::ExitCode {
    let spec_text = match std::fs::read_to_string(spec_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("cannot read --spec {}: {e}", spec_path.display());
            return exit::ExitCode::Operational;
        }
    };
    let spec: DrillSpec = match serde_yaml::from_str(&spec_text) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("cannot parse --spec {}: {e}", spec_path.display());
            return exit::ExitCode::Operational;
        }
    };
    let allowed_text = match std::fs::read_to_string(allowed_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "cannot read --allowed-clusters {}: {e}",
                allowed_path.display()
            );
            return exit::ExitCode::Operational;
        }
    };
    let allowed: AllowedClusters = match serde_json::from_str(&allowed_text) {
        Ok(a) => a,
        Err(e) => {
            eprintln!(
                "cannot parse --allowed-clusters {}: {e}",
                allowed_path.display()
            );
            return exit::ExitCode::Operational;
        }
    };
    let reader = match RdKafkaReader::connect(&spec.target.bootstrap_servers, AuthConfig::Plaintext)
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("cannot construct the target reader: {e}");
            return exit::ExitCode::Operational;
        }
    };
    match phase0_admit::run(&spec, &spec_text, &allowed, &reader) {
        Ok(_) => {
            eprintln!("phase 0 admitted the plan; phases 1-9 land in Task 21a");
            exit::ExitCode::Operational
        }
        // A genuine guard refusal: the plan is refused, before anything ran.
        Err(DrillError::Guard(e)) => {
            eprintln!("refused: {e}");
            exit::ExitCode::GuardRefused
        }
        // Everything else (a broker that could not be read, an unreachable
        // cluster) is operational, not a refusal: the plan may be fine and
        // the correct action is to retry, not to page someone to edit it.
        Err(e) => {
            eprintln!("could not evaluate phase 0: {e}");
            exit::ExitCode::Operational
        }
    }
}
