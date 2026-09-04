use clap::Parser;
use logweir::{cli, exit, schema, verify};

fn main() -> std::process::ExitCode {
    let args = cli::Cli::parse();
    let code = match args.command {
        cli::Command::Schema { which } => schema::run(&which),
        cli::Command::Drill(cli::DrillCmd::Verify {
            scorecard,
            signature,
            public_key,
        }) => verify::run(&scorecard, &signature, &public_key),
        _ => {
            eprintln!("not yet implemented in this task");
            exit::ExitCode::Operational
        }
    };
    code.into()
}
