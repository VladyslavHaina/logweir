use crate::exit::ExitCode;

/// The one sentence every non-shipping arm below ends with, so `logweir
/// schema <anything>` tells an operator what tag 1 DOES ship rather than only
/// what it does not. Capitalised because every use is sentence-initial.
const ACCEPTED: &str = "It ships four schemas: `scorecard`, `backup-receipt`, `positions-mapping` \
                        and `switchover-record`.";

/// Global Constraint 13, as REVISED: `logweir schema` accepts `scorecard` and
/// `backup-receipt`, and since PROD-04.2 `positions-mapping` and
/// `switchover-record`. `snapshot`, `diff` and `plan` are accepted and exit
/// `1` naming the sub-project that introduces them.
pub fn run(which: &str) -> ExitCode {
    match which {
        "scorecard" => {
            print!("{}", logweir_core::schema::scorecard_schema());
            ExitCode::Ok
        }
        // The SECOND schema tag 1 ships: the backup's evidence is a new
        // document, not new scorecard fields (spec §7, GC12 as amended).
        // Byte-identical to the CURRENT receipt schema file,
        // `schemas/logweir-backup-receipt-<v>.json` with
        // `v` = `FORMAT_VERSION_WITH_GENERATIONS` (1.6.0; 1.5.0 back to 1.0.0
        // are frozen beside it). The CI drift arm regenerates that file, and
        // `crates/logweir/tests/cli_verify_gc12.rs::
        // schema_backup_receipt_is_byte_identical_to_the_checked_in_file`
        // compares THIS stdout against it.
        "backup-receipt" => {
            print!("{}", logweir_core::schema::backup_receipt_schema());
            ExitCode::Ok
        }
        "snapshot" | "diff" => {
            eprintln!(
                "`logweir schema {which}` is introduced by SP2 (metadata snapshot and diff). \
                 {ACCEPTED}"
            );
            ExitCode::Operational
        }
        "plan" => {
            eprintln!(
                "`logweir schema plan` is introduced by SP3 (restore plan / apply). {ACCEPTED}"
            );
            ExitCode::Operational
        }
        // PROD-04.2: the Switchover's two documents, byte-identical to
        // `schemas/logweir-{positions-mapping,switchover-record}-1.0.0.json`.
        "positions-mapping" => {
            print!(
                "{}",
                logweir_core::schema::positions_mapping_document_schema()
            );
            ExitCode::Ok
        }
        "switchover-record" => {
            print!("{}", logweir_core::schema::switchover_record_schema());
            ExitCode::Ok
        }
        other => {
            eprintln!("unknown schema `{other}`. {ACCEPTED}");
            ExitCode::Operational
        }
    }
}
