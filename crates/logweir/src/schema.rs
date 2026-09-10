use crate::exit::ExitCode;

/// The one sentence every non-shipping arm below ends with, so `logweir
/// schema <anything>` tells an operator what tag 1 DOES ship rather than only
/// what it does not.
///
/// It replaced three copies of "v0.1 ships exactly one schema: `logweir
/// schema scorecard`." / "v0.1 accepts: scorecard", which were three places
/// one fact had to be edited — and the fact changed the moment Global
/// Constraint 13 was revised to accept `backup-receipt`. Capitalised because
/// every use of it is sentence-initial, after a full stop.
const ACCEPTED: &str = "Tag 1 ships two schemas: `scorecard` and `backup-receipt`.";

/// Global Constraint 13, as REVISED: `logweir schema` accepts `scorecard` and
/// `backup-receipt` in tag 1. `snapshot`, `diff` and `plan` are accepted and
/// exit `1` naming the sub-project that introduces them; `switchover-record`
/// exits `1` naming **tag 2**.
///
/// # Why `switchover-record` has its own arm (critique A F31)
///
/// It used to fall through the catch-all, whose message named no tag: an
/// operator who had read tag 2's design and typed the obvious command was
/// told `unknown schema \`switchover-record\``, which reads as "there is no
/// such thing" rather than "not yet". Every other deferred schema in this
/// function names the thing that introduces it, and this one is the only
/// deferred schema an adopter has any reason to try, because it is the only
/// one the ROADMAP names by document type.
pub fn run(which: &str) -> ExitCode {
    match which {
        "scorecard" => {
            print!("{}", logweir_core::schema::scorecard_schema());
            ExitCode::Ok
        }
        // Task 5. The SECOND schema tag 1 ships, and the reason GC13 was
        // revised: the backup's evidence is a new document, not new scorecard
        // fields (spec §7, GC12 as amended). Byte-identical to
        // `schemas/logweir-backup-receipt-1.0.0.json` — the CI drift arm
        // regenerates that file and `diff -u`s it, and
        // `crates/logweir/tests/cli_verify_gc12.rs::
        // schema_backup_receipt_is_byte_identical_to_the_checked_in_file`
        // compares THIS stdout against it, so the printed schema and the
        // published file cannot come apart.
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
        "switchover-record" => {
            eprintln!(
                "`logweir schema switchover-record` is introduced by tag 2 (Switchover). \
                 {ACCEPTED}"
            );
            ExitCode::Operational
        }
        other => {
            eprintln!("unknown schema `{other}`. {ACCEPTED}");
            ExitCode::Operational
        }
    }
}
