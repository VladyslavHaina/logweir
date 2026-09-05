use clap::Parser;
use logweir::drill::RunArgs;
use logweir::{cli, exit, schema, verify};

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
        cli::Command::Doctor {
            spec,
            allowed_clusters,
            approver_key,
            strict,
        } => logweir::doctor::run(&logweir::doctor::DoctorArgs {
            spec,
            allowed_clusters,
            approver_key,
            strict,
        }),
        cli::Command::Drill(cli::DrillCmd::Verify {
            scorecard,
            signature,
            public_key,
        }) => verify::run(&scorecard, &signature, &public_key),
        cli::Command::Drill(cli::DrillCmd::Run {
            spec,
            approval,
            approver_key,
            allowed_clusters,
            signing_key,
            triggered_by,
            out,
            metrics_file,
        }) => logweir::drill::run(RunArgs {
            spec,
            approval,
            approver_key,
            allowed_clusters,
            signing_key,
            triggered_by,
            out,
            metrics_file,
        }),
        _ => {
            eprintln!("not yet implemented in this task");
            exit::ExitCode::Operational
        }
    };
    code.into()
}
