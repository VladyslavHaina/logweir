use clap::Parser;
use logweir::drill::InvokedAs;
use logweir::{cli, exit, schema, show, verify};

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
        cli::Command::Backup(cli::BackupCmd::Run {
            spec,
            allowed_clusters,
            signing_key,
            triggered_by,
            out,
            receipt_out,
            backup_id_override,
        }) => logweir::backup::run(&logweir::backup::BackupRunArgs {
            spec,
            allowed_clusters,
            signing_key,
            triggered_by,
            out,
            receipt_out,
            backup_id_override,
        }),
        // Task 15c, interface I14. Dispatched here for the structural reason
        // the comment at the end of this match records: the arm list is
        // exhaustive with no catch-all, so a subcommand added to `cli.rs` and
        // not wired here is a COMPILE error rather than a runtime stub. That is
        // also why `crates/logweir/src/main.rs` is edited by this task although
        // the brief's Files block names only `cli.rs` — the two cannot move
        // apart.
        cli::Command::ClusterProbe {
            bootstrap,
            auth_mode,
            username,
            tls,
            marker_topic,
        } => logweir::probe::run(&logweir::probe::ProbeArgs {
            bootstrap,
            auth_mode,
            username,
            tls,
            marker_topic,
        }),
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
        cli::Command::Drill(cli::DrillCmd::Approve {
            spec,
            key,
            approver,
            ticket,
            out,
            subject_kind,
        }) => logweir::approve::run(&logweir::approve::ApproveArgs {
            spec,
            key,
            approver,
            ticket,
            out,
            // The parser owns the vocabulary; `approve` owns the bytes.
            subject_kind: subject_kind.as_str().to_string(),
        }),
        cli::Command::Drill(cli::DrillCmd::Verify {
            scorecard,
            signature,
            public_key,
            payload_type,
        }) => verify::run(&scorecard, &signature, &public_key, &payload_type),
        // THE ONE FUNCTION, TWO NAMES (interface I20). Both arms build the
        // same `RunArgs` through the same `From` impl and call
        // `drill::run_named`; the only difference between them is the
        // `InvokedAs` value, which decides whether the single deprecation line
        // is printed on stderr. There is deliberately no second code path for
        // the alias — an alias that could behave differently is not an alias.
        cli::Command::Restore(cli::RestoreCmd::Run(a)) => {
            logweir::drill::run_named(a.into(), InvokedAs::Restore)
        }
        cli::Command::Drill(cli::DrillCmd::Run(a)) => {
            logweir::drill::run_named(a.into(), InvokedAs::DrillAlias)
        }
        // Task 22 (carried obligation 1). `show::run` has been implemented and
        // golden-tested since `d403abc`, but this match ended in a `_ =>` arm
        // that printed "not yet implemented in this task" — so the ONLY way to
        // reach the renderer was to link the library from a test. The shipped
        // binary silently had no `drill show`.
        //
        // There is deliberately NO catch-all arm now. With every variant named,
        // adding a subcommand to `cli.rs` without dispatching it is a compile
        // error (`non-exhaustive patterns`), not a runtime stub that a reader
        // discovers from a message. That is the structural half of the fix; the
        // wiring is the other half, and `crates/logweir/tests/cli_show.rs`
        // asserts the compiled binary, not the library, renders a table.
        cli::Command::Drill(cli::DrillCmd::Show { scorecard, format }) => {
            show::run(&scorecard, &format)
        }
    };
    code.into()
}
