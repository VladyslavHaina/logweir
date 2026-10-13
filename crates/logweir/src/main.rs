#![forbid(unsafe_code)]
use clap::Parser;
use logweir::drill::InvokedAs;
use logweir::{cli, exit, schema, show, verify};

fn main() -> std::process::ExitCode {
    // clap's `Parser::parse` exits 2 on a usage error, and Global Constraint
    // 11 reserves 2 for "a drill ran and did not pass; a signed scorecard
    // exists". A typo'd flag runs no drill, so usage errors map to
    // `ExitCode::Operational` (1) here, while `--help` and `--version` (also an
    // `Err` from clap) keep exiting 0.
    let args = match cli::Cli::try_parse() {
        Ok(a) => a,
        Err(e) => {
            let _ = e.print();
            return if e.use_stderr() {
                // An `identity bootstrap` that clap refused ran none of its
                // steps, so nothing else would delete the install's trust
                // grant; the arguments still name it.
                let argv: Vec<String> = std::env::args_os()
                    .map(|a| a.to_string_lossy().into_owned())
                    .collect();
                logweir::identity::revoke_after_usage_error(&argv);
                exit::ExitCode::Operational.into()
            } else {
                std::process::ExitCode::SUCCESS
            };
        }
    };
    let code = match args.command {
        cli::Command::Identity(cmd) => match logweir::identity::Invocation::from(cmd) {
            logweir::identity::Invocation::Bootstrap(args) => logweir::identity::run(&args),
            logweir::identity::Invocation::Distribute(args) => {
                logweir::identity::run_distribution(&args)
            }
        },
        cli::Command::Schema { which } => schema::run(&which),
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
        // clap's `required_unless_present`/`conflicts_with` pair makes exactly
        // one of `--stdin` and `--from` present, so `from` deciding the input
        // cannot silently ignore a `--stdin` the operator also passed.
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
        cli::Command::Check(cli::CheckCmd::Run {
            plan,
            check_contract_version,
            runner_line_token,
        }) => {
            // FX-43: set once, before anything runs.
            logweir::exit::set_line_tokens(None, runner_line_token);
            logweir::check::run(&logweir::check::CheckRunArgs {
                plan,
                check_contract_version,
            })
        }
        cli::Command::Backup(cli::BackupCmd::Run {
            store_contract_version,
            spec,
            allowed_clusters,
            signing_key,
            triggered_by,
            out,
            receipt_out,
            backup_id_override,
            kafka_topic_resources,
            strimzi_cluster,
            consumer_groups,
            line_token,
            runner_line_token,
        }) => {
            // The Job's line token, held for the lines that carry it. Set
            // here, once, before anything runs.
            logweir::exit::set_line_tokens(line_token, runner_line_token);
            logweir::backup::run(&logweir::backup::BackupRunArgs {
                store_contract_version,
                spec,
                allowed_clusters,
                signing_key,
                triggered_by,
                out,
                receipt_out,
                backup_id_override,
                kafka_topic_resources,
                strimzi_cluster,
                consumer_groups,
            })
        }
        cli::Command::ClusterProbe {
            bootstrap,
            auth_mode,
            username,
            tls,
            marker_topic,
            runner_line_token,
        } => {
            // FX-43: the probe's two contract lines are sealed with the Job's
            // token, and its end line is printed. Set once, before anything.
            logweir::exit::set_line_tokens(None, runner_line_token);
            logweir::probe::run(&logweir::probe::ProbeArgs {
                bootstrap,
                auth_mode,
                username,
                tls,
                marker_topic,
            })
        }
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
            approval_subject,
        }) => logweir::approve::run(&logweir::approve::ApproveArgs {
            spec,
            key,
            approver,
            ticket,
            out,
            // The parser owns the vocabulary; `approve` owns the bytes.
            subject_kind: subject_kind.as_str().to_string(),
            approval_subject: approval_subject.map(|s| s.wire().to_string()),
        }),
        cli::Command::Drill(cli::DrillCmd::Countersign {
            document,
            confirmation,
            key,
            out,
        }) => logweir::approve::run_countersign(&logweir::approve::CountersignArgs {
            document,
            confirmation,
            key,
            out,
        }),
        cli::Command::Drill(cli::DrillCmd::Verify {
            scorecard,
            signature,
            public_key,
            payload_type,
            consumer_positions,
            positions_mapping,
        }) => verify::run_with_mapping(
            &scorecard,
            &signature,
            &public_key,
            &payload_type,
            consumer_positions.as_deref(),
            positions_mapping.as_deref(),
        ),
        // THE ONE FUNCTION, TWO NAMES (interface I20). Both arms build the
        // same `RunArgs` through the same `From` impl and call
        // `drill::run_named`; the only difference between them is the
        // `InvokedAs` value, which decides whether the single deprecation line
        // is printed on stderr. There is deliberately no second code path for
        // the alias — an alias that could behave differently is not an alias.
        cli::Command::Restore(cli::RestoreCmd::Run(a)) => {
            // See the `backup run` arm.
            logweir::exit::set_line_tokens(a.line_token.clone(), a.runner_line_token.clone());
            logweir::drill::run_named(a.into(), InvokedAs::Restore)
        }
        cli::Command::Drill(cli::DrillCmd::Run(a)) => {
            logweir::exit::set_line_tokens(a.line_token.clone(), a.runner_line_token.clone());
            logweir::drill::run_named(a.into(), InvokedAs::DrillAlias)
        }
        // PROD-11.1a: a TTL clone's cleanup Job.
        cli::Command::Restore(cli::RestoreCmd::Cleanup(a)) => {
            logweir::restore_cleanup::run(a.into())
        }
        // PROD-04.2: the Switchover's plan builders and its Job.
        cli::Command::Switchover(cli::SwitchoverCmd::Plan {
            scorecard,
            positions_mapping,
            restore_name,
            restore_uid,
            groups,
            out,
        }) => match logweir::switchover::plan(&logweir::switchover::PlanArgs {
            scorecard,
            mapping: positions_mapping,
            restore_name,
            restore_uid,
            groups,
            out,
        }) {
            Ok(summary) => {
                print!("{summary}");
                logweir::exit::ExitCode::Ok
            }
            Err(e) => {
                eprintln!("{e}");
                logweir::exit::ExitCode::Operational
            }
        },
        cli::Command::Switchover(cli::SwitchoverCmd::RollbackPlan { record, out }) => {
            match logweir::switchover::rollback_plan(&logweir::switchover::RollbackPlanArgs {
                record,
                out,
            }) {
                Ok(summary) => {
                    print!("{summary}");
                    logweir::exit::ExitCode::Ok
                }
                Err(e) => {
                    eprintln!("{e}");
                    logweir::exit::ExitCode::Operational
                }
            }
        }
        cli::Command::Switchover(cli::SwitchoverCmd::Run {
            plan,
            restore_plan,
            approval,
            approver_key,
            evidence_keys,
            signing_key,
            scorecard_key,
            namespace,
            name,
            uid,
            store_contract_version,
            approval_policy,
            confirmation_key,
        }) => logweir::switchover::run(&logweir::switchover::RunArgs {
            plan,
            restore_plan,
            approval,
            approver_key,
            evidence_keys,
            signing_key,
            scorecard_key,
            namespace,
            name,
            uid,
            store_contract_version,
            approval_policy,
            confirmation_key,
        }),
        // NO catch-all arm: with every variant named, a subcommand added to
        // `cli.rs` and not dispatched here is a compile error
        // (`non-exhaustive patterns`), not a runtime stub.
        // `crates/logweir/tests/cli_show.rs` asserts the compiled binary
        // renders a table.
        cli::Command::Drill(cli::DrillCmd::Show { scorecard, format }) => {
            show::run(&scorecard, &format)
        }
    };
    code.into()
}
