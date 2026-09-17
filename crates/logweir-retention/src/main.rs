#![forbid(unsafe_code)]
//! The `logweir-retention` process.
//!
//! **Everything that decides anything is in the library** ([`logweir_retention`]),
//! behind two seams the tests drive: [`logweir_retention::admit`], which is pure
//! and holds every exit-3 refusal, and [`logweir_retention::execute`], which
//! takes the reaper's four injected ports. What is left here is the part that
//! cannot be tested without a bucket: read the real environment, open the real
//! handles, print, exit.
//!
//! See the library's module header for the argv, the environment, the two
//! credentials and the exit contract.

use std::process::ExitCode;

use chrono::Utc;

use logweir_core::destination::DestinationLocation;
use logweir_reaper::{
    record, record_bytes, record_key, ArchiveReaper, Credentials, RecordContext, SinkError,
    ThreadSleeper, TombstoneSink,
};
use logweir_retention::{
    admit, execute, record_line, result_line, Admitted, Keys, ProcessEnv, Refusal, EXIT_REFUSED,
};
use logweir_store::{Store, StoreError, StoreOptions};

const HELP: &str = "logweir-retention — the Logweir retention worker. It executes one \
administrator-approved deletion plan against one archive prefix and writes the attributable \
record. It is a SEPARATE binary from `logweir` on purpose: deletion linkage must not be reachable \
from the everyday command line. Usage: logweir-retention run --plan <path> \
--retention-contract-version 1 [--dry-run].";

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let flags: Vec<&str> = argv.iter().map(String::as_str).collect();
    match flags.as_slice() {
        ["--version"] => {
            println!("logweir-retention {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        ["--help"] | [] => {
            println!("{HELP}");
            ExitCode::SUCCESS
        }
        _ => code(run(&flags)),
    }
}

fn code(exit: i32) -> ExitCode {
    ExitCode::from(u8::try_from(exit).unwrap_or(1))
}

fn run(flags: &[&str]) -> i32 {
    let started_at = Utc::now();
    let admitted = match admit(flags, &ProcessEnv, |path| std::fs::read(path)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("logweir-retention: {e}");
            return e.exit_code();
        }
    };

    // THE DELETER. Built from the destination's own frozen addressing and the
    // explicitly named credential — never `from_env()`, so no ambient
    // `AWS_ENDPOINT_URL` can relocate a deletion (D-SEAMS S5).
    let reaper = match ArchiveReaper::new(
        &admitted.archive_url(),
        admitted.archive_keys.as_ref().map(as_credentials).as_ref(),
        admitted.allow_http(),
    ) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("logweir-retention: {}", Refusal::Port(e.to_string()));
            return EXIT_REFUSED;
        }
    };

    // A DRY RUN OPENS NO SINK AND DELETES NOTHING. It does hold the archive
    // handle, because the preview enumerates the set bound to report the real
    // object count (review `d3w9` M1) — listing is a separate IAM verb from
    // deleting, and `execute`'s `dry_run` arm never reaches `delete_exact`,
    // which `a_dry_run_issues_no_delete` asserts over a deleter that panics.
    if admitted.dry_run {
        let report = execute(
            &admitted,
            &reaper,
            &ThreadSleeper,
            &logweir_reaper::NoTombstones,
            &reaper,
        );
        for line in &report.lines {
            println!("{line}");
        }
        println!("{}", result_line(&report, true));
        return report.exit_code;
    }

    // THE RECORD SINK, BEFORE THE FIRST DELETE. `admit` already refused a run
    // with no `evidenceWrite` credential; this is the handle itself.
    //
    // **What this does and does not guarantee** (review `d3w9` L4).
    // `Store::from_url_with` configures a client and performs no round trip, so
    // opening it proves the credential is PRESENT and not that `logweir/` is
    // writable. The guarantee that holds is the narrower, true one: the first
    // point whose intent tombstone cannot be written is not deleted, and
    // neither is any later one — `a_point_whose_intent_cannot_be_written_is_not_deleted`.
    let sink = match EvidenceSink::open(&admitted) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("logweir-retention: {}", Refusal::Port(e.to_string()));
            return EXIT_REFUSED;
        }
    };

    let report = execute(&admitted, &reaper, &ThreadSleeper, &sink, &reaper);
    for line in &report.lines {
        println!("{line}");
    }

    let doc = record(
        &admitted.plan,
        &report.outcome,
        &RecordContext {
            run_id: admitted.attribution.run_id.clone(),
            policy_generation: admitted.policy_generation,
            approver: admitted.approver.clone(),
            plan_sha256: admitted.attribution.plan_sha256.clone(),
            started_at,
            finished_at: Utc::now(),
            exit_code: report.exit_code,
        },
    );
    match record_bytes(&doc) {
        Ok((bytes, digest)) => {
            let key = record_key(
                &admitted.attribution.policy_uid,
                &admitted.attribution.run_id,
            );
            match sink.put_create_only(&key, &bytes) {
                Ok(()) => println!("{}", record_line(&key, &digest)),
                Err(e) => eprintln!(
                    "logweir-retention: the enforcement record at `{key}` was not written: {e}. \
                     The per-point tombstones under the same prefix are the surviving trail."
                ),
            }
        }
        Err(e) => eprintln!("logweir-retention: the enforcement record did not serialise: {e}"),
    }

    println!("{}", result_line(&report, false));
    report.exit_code
}

fn as_credentials(keys: &Keys) -> Credentials {
    Credentials {
        access_key_id: keys.access_key_id.clone(),
        secret_access_key: keys.secret_access_key.clone(),
        session_token: keys.session_token.clone(),
    }
}

/// The create-only sink under `logweir/`, with the `evidenceWrite` grant.
///
/// `logweir_store::Store` and not a second object-store handle: it is the one
/// crate that implements the one write Global Constraint 6 allows, its
/// `put_create_only` asserts the `logweir/` root in code, and it **cannot
/// delete** — the whole reason the deleting is in `logweir-reaper`.
struct EvidenceSink {
    store: Store,
}

impl EvidenceSink {
    fn open(admitted: &Admitted) -> Result<Self, StoreError> {
        // THE EVIDENCE GRANT, BY NAMED VARIABLE, and `admit` has already
        // refused the run if it is absent. Falling back to the archive
        // credential would be exactly the aggregation the two-credential design
        // exists to prevent: the delete-capable principal must not be able to
        // write the record that attributes its own deletes.
        let keys = admitted
            .evidence_keys
            .as_ref()
            .ok_or_else(|| StoreError::Backend(Refusal::NoRecordCredential.to_string()))?;
        let opts = StoreOptions::static_keys(
            keys.access_key_id.clone(),
            keys.secret_access_key.clone(),
            keys.session_token.clone(),
        );
        Ok(Self {
            store: Store::from_url_with(&admitted.evidence_url(), &opts)?,
        })
    }
}

impl TombstoneSink for EvidenceSink {
    fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<(), SinkError> {
        self.store
            .put_create_only(key, bytes)
            .map(|_| ())
            .map_err(|e| SinkError(e.to_string()))
    }
}

/// `DestinationLocation` is named here so the doc link in the library's header
/// resolves in this binary too.
#[allow(dead_code)]
type _Location = DestinationLocation;
