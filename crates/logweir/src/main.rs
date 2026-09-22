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
        cli::Command::Identity(cli::IdentityCmd::Bootstrap {
            namespace,
            secret_name,
            secret_key,
            public_configmap_name,
            external_secret_name,
            external_secret_key,
        }) => logweir::identity::run(&logweir::identity::BootstrapArgs {
            namespace,
            secret_name,
            secret_key,
            public_configmap_name,
            external_secret: external_secret_name.zip(external_secret_key),
        }),
        cli::Command::Identity(cli::IdentityCmd::Distribute {
            source_namespace,
            target_namespace,
            secret_name,
            secret_key,
        }) => logweir::identity::run_distribution(&logweir::identity::DistributeArgs {
            source_namespace,
            target_namespace,
            secret_name,
            secret_key,
        }),
        cli::Command::Schema { which } => schema::run(&which),
        // PLAT-14.2 / D3 §3.4. `main.rs` is edited alongside `cli.rs` for the
        // structural reason the comment at the end of this match records: the
        // arm list is exhaustive with no catch-all, so a subcommand added to
        // `cli.rs` and not wired here is a COMPILE error rather than a runtime
        // stub.
        cli::Command::Notify(cli::NotifyCmd::Deliver { event }) => {
            logweir::notify::run(&logweir::notify::DeliverArgs { event })
        }
        // PLAT-15.1 / decision D3 §5. Dispatched here for the structural
        // reason the comment at the end of this match records: the arm list is
        // exhaustive with no catch-all, so a `catalog` subcommand added to
        // `cli.rs` and not wired here is a COMPILE error rather than a runtime
        // stub.
        cli::Command::Catalog(cli::CatalogCmd::Sync {
            location,
            signing_key,
            public_key,
            since,
            max,
        }) => logweir::catalog::cli::run_sync(&logweir::catalog::cli::SyncArgs {
            location: (&location).into(),
            signing_key,
            public_keys: public_key,
            since,
            max,
        }),
        cli::Command::Catalog(cli::CatalogCmd::List {
            location,
            since,
            max,
            days,
        }) => logweir::catalog::cli::run_list(&logweir::catalog::cli::ListArgs {
            location: (&location).into(),
            since,
            max,
            days,
        }),
        // PLAT-19.1 / decision D3 §7.1 and §7.5. Dispatched here for the
        // structural reason the comment at the end of this match records: the
        // arm list is exhaustive with no catch-all, so a `trust` subcommand
        // added to `cli.rs` and not wired here is a COMPILE error rather than
        // a runtime stub. `stdin` is a flag rather than a value, and clap's
        // `required_unless_present`/`conflicts_with` pair makes exactly one of
        // it and `--from` present — so `from` deciding the input here cannot
        // silently ignore a `--stdin` the operator also passed.
        cli::Command::Trust(cli::TrustCmd::Export {
            policy,
            stdin: _,
            from,
        }) => logweir::trust::run_export(&logweir::trust::ExportArgs {
            policy,
            input: from.map_or(logweir::trust::Input::Stdin, logweir::trust::Input::File),
        }),
        cli::Command::Trust(cli::TrustCmd::MigrateRoster {
            name,
            stdin: _,
            from,
            default,
            namespace,
        }) => logweir::trust::run_migrate(&logweir::trust::MigrateArgs {
            name,
            default,
            namespaces: namespace,
            input: from.map_or(logweir::trust::Input::Stdin, logweir::trust::Input::File),
        }),
        // Decision D2 §4.2 / D-SEAMS S1. Dispatched here for the structural
        // reason the comment at the end of this match records: the arm list is
        // exhaustive with no catch-all, so a `check` subcommand added to
        // `cli.rs` and not wired here is a COMPILE error rather than a runtime
        // stub.
        cli::Command::Check(cli::CheckCmd::Run {
            plan,
            check_contract_version,
        }) => logweir::check::run(&logweir::check::CheckRunArgs {
            plan,
            check_contract_version,
        }),
        cli::Command::Backup(cli::BackupCmd::Run {
            store_contract_version,
            spec,
            allowed_clusters,
            signing_key,
            triggered_by,
            out,
            receipt_out,
            backup_id_override,
        }) => logweir::backup::run(&logweir::backup::BackupRunArgs {
            store_contract_version,
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
            standing,
            schedule_namespace,
            schedule_name,
            schedule_uid,
            scope,
            valid_days,
            issued_at,
        }) => {
            // `--subject-kind RehearsalSchedule` without `--standing` would
            // mint bytes under `PAYLOAD_TYPE_APPROVAL` that the `Approval`
            // controller refuses for that referent. Told here, once, rather
            // than discovered from a rejected object.
            if subject_kind == cli::SubjectKindArg::RehearsalSchedule && !standing {
                eprintln!(
                    "--subject-kind RehearsalSchedule requires --standing: a RehearsalSchedule \
                     is authorised by a STANDING rehearsal authorization, signed under its own \
                     payload type, and the Approval controller refuses an ordinary approval for \
                     that referent"
                );
                return logweir::exit::ExitCode::Operational.into();
            }
            let standing = standing.then(|| logweir::approve::StandingArgs {
                schedule_namespace: schedule_namespace.unwrap_or_default(),
                schedule_name: schedule_name.unwrap_or_default(),
                schedule_uid: schedule_uid.unwrap_or_default(),
                scope: scope.unwrap_or_default(),
                valid_days,
                issued_at,
            });
            logweir::approve::run(&logweir::approve::ApproveArgs {
                spec,
                key,
                approver,
                ticket,
                out,
                // The parser owns the vocabulary; `approve` owns the bytes.
                subject_kind: subject_kind.as_str().to_string(),
                standing,
            })
        }
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
