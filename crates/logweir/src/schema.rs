use crate::exit::ExitCode;

/// Global Constraint 13: `logweir schema` accepts exactly one argument in
/// v0.1, `scorecard`. `snapshot`, `diff` and `plan` are accepted and exit `1`
/// naming the sub-project that introduces them.
pub fn run(which: &str) -> ExitCode {
    match which {
        "scorecard" => {
            print!("{}", logweir_core::schema::scorecard_schema());
            ExitCode::Ok
        }
        "snapshot" | "diff" => {
            eprintln!(
                "`logweir schema {which}` is introduced by SP2 (metadata snapshot and diff). \
                 v0.1 ships exactly one schema: `logweir schema scorecard`."
            );
            ExitCode::Operational
        }
        "plan" => {
            eprintln!(
                "`logweir schema plan` is introduced by SP3 (restore plan / apply). \
                 v0.1 ships exactly one schema: `logweir schema scorecard`."
            );
            ExitCode::Operational
        }
        other => {
            eprintln!("unknown schema `{other}`. v0.1 accepts: scorecard");
            ExitCode::Operational
        }
    }
}
