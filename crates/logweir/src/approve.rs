//! `logweir drill approve` — mint the DSSE-signed approval that
//! `logweir drill run --approval` requires.
//!
//! WHY THIS IS A SUBCOMMAND AND NOT AN EXAMPLE. Approval is MANDATORY in v0.1
//! (`drill run --approval` has no default and no opt-out), and until this
//! existed the only producer in the repository was
//! `crates/logweir-evidence/examples/sign_approval.rs` — a cargo EXAMPLE.
//! `crates/logweir/Cargo.toml` holds the workspace's only `[[bin]]`, so that
//! example shipped in neither the container image nor the cargo-dist tarballs.
//! An operator deploying `examples/cronjob-drill.yaml` could not mint
//! `approval.sig` from Logweir's own artifacts at all: they had to clone the
//! repository and install Rust 1.89 — and repeat that on EVERY spec edit,
//! because `plan_hash` binds the approval to the exact bytes of the spec and a
//! drill spec carries a time-varying `sample.window_*`. Kubernetes is the
//! primary deployment target; a product that cannot be operated from its own
//! artifacts on that path is not shipped.
//!
//! WHY A SUBCOMMAND AND NOT A SECOND `[[bin]]`. A second binary has to be
//! added to the Dockerfile's `COPY` and picked up by cargo-dist separately —
//! two places it can silently go missing, which is the failure this fixes. One
//! binary cannot lose a subcommand. It costs one row in
//! `logweir drill --help`, and it is the row that makes the other three
//! usable. Global Constraint 13 constrains `logweir schema`'s ARGUMENT to
//! `scorecard`; it says nothing about subcommands, and nothing here emits or
//! changes a scorecard field (Global Constraint 12 — the tag is cut).
//!
//! `openssl dgst` cannot substitute for this: a DSSE signature covers
//! `PAE(payloadType, payload)`, never the bare payload bytes.
use crate::drill::phase1_approval::PAYLOAD_TYPE_APPROVAL;
use crate::exit::ExitCode;
use logweir_core::ids::sha256_prefixed;
use logweir_core::spec::ApprovalDoc;
use logweir_evidence::keys::SigningKey;
use logweir_evidence::sign::sign_detached;
use std::path::{Path, PathBuf};

pub struct ApproveArgs {
    pub spec: PathBuf,
    pub key: PathBuf,
    pub approver: String,
    pub ticket: String,
    pub out: PathBuf,
    /// `--subject-kind`, as the wire string that goes into the document —
    /// `Restore` or `Backup`. Held as a `String` rather than as
    /// `cli::SubjectKindArg` so this module stays independent of the argument
    /// parser: `mint` writes the bytes, and `cli.rs` decides what a command
    /// line may spell.
    pub subject_kind: String,
}

/// Everything here is `ExitCode::Operational` (1) on failure: this command
/// runs no drill, touches no cluster and reaches no guard, so codes 2, 3 and 4
/// — each of which makes a statement about a drill or an artifact — cannot
/// honestly apply.
pub fn run(args: &ApproveArgs) -> ExitCode {
    match mint(args) {
        Ok(summary) => {
            print!("{summary}");
            ExitCode::Ok
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::Operational
        }
    }
}

/// The signable half, separated so tests assert on the produced BYTES rather
/// than on a process exit code.
pub fn mint(args: &ApproveArgs) -> Result<String, String> {
    // `read_to_string`, not `read`, so this hashes exactly what
    // `phase1_approval::verify` hashes: it reads the spec with
    // `fs::read_to_string` and computes `sha256_prefixed(spec_text.as_bytes())`.
    // A spec that is not valid UTF-8 is refused HERE rather than approved into
    // a hash the drill can never reproduce.
    let spec_text =
        std::fs::read_to_string(&args.spec).map_err(|e| format!("{}: {e}", args.spec.display()))?;
    let plan_hash = sha256_prefixed(spec_text.as_bytes());

    let key = SigningKey::from_pem_file(&args.key).map_err(|e| {
        // `logweir_evidence::Error` carries no key material by construction,
        // and neither does this line: it names the PATH, never the contents.
        format!("{}: {e}", args.key.display())
    })?;

    let doc = ApprovalDoc {
        approver: args.approver.clone(),
        ticket: args.ticket.clone(),
        plan_hash: plan_hash.clone(),
        // The approver's clock at the moment they approved. `drill run` does
        // NOT measure RTO from this — it measures from the moment phase 1
        // validated the signature — so a skewed approver clock cannot inflate
        // or deflate a measured objective.
        approved_at: chrono::Utc::now(),
        // INSIDE THE SIGNED BYTES. `payload` below is what `sign_detached`
        // covers, and this struct is what `payload` is serialised from, so
        // the kind is bound by the signature. Writing it into the SIDECAR
        // instead would leave check 8 comparing a field anyone who can write
        // the approval bundle could rewrite without touching the signature.
        subject_kind: args.subject_kind.clone(),
    };
    let mut payload =
        serde_json::to_vec_pretty(&doc).map_err(|e| format!("serialising the approval: {e}"))?;
    payload.push(b'\n');

    let sidecar = sign_detached(&key, PAYLOAD_TYPE_APPROVAL, &payload)
        .map_err(|e| format!("signing the approval: {e}"))?;

    // The sidecar path is DERIVED, never taken as an argument:
    // `phase1_approval::verify` looks for it at `approval_json.with_extension
    // ("sig")` and nowhere else, so letting a caller name it separately is a
    // way to produce an approval `drill run` cannot find.
    let sig_path = sig_path_for(&args.out);
    overwrite(&args.out, &payload)?;
    let sig_bytes =
        serde_json::to_vec_pretty(&sidecar).map_err(|e| format!("serialising the sidecar: {e}"))?;
    overwrite(&sig_path, &sig_bytes)?;

    Ok(format!(
        "approved {spec}\n  plan_hash  {plan_hash}\n  approver   {approver}\n  \
         ticket     {ticket}\n  subject    {subject_kind}\n  key_id     {key_id}\n  \
         wrote      {out}\n  wrote      {sig}\n\
         \nThis approval binds the EXACT bytes of {spec}. Edit the spec — including its\n\
         sample window — and `logweir drill run` refuses with exit 3 until you re-run\n\
         this command.\n",
        spec = args.spec.display(),
        approver = args.approver,
        ticket = args.ticket,
        subject_kind = args.subject_kind,
        key_id = key.key_id(),
        out = args.out.display(),
        sig = sig_path.display(),
    ))
}

/// `with_extension` on a path with no extension APPENDS one, and on
/// `approval.json` REPLACES `.json` — which is what `phase1_approval::verify`
/// does, so this must do the identical thing rather than something merely
/// similar.
fn sig_path_for(out: &Path) -> PathBuf {
    out.with_extension("sig")
}

/// Named for what it does. `fs::write` TRUNCATES, and that is deliberate here:
/// re-approving after a spec edit is the documented workflow (`plan_hash` binds
/// the exact bytes), so refusing an existing `approval.json` would make the
/// common case an error. Nothing here is create-only — that discipline belongs
/// to the evidence bucket (`Store::put_create_only`), where an overwrite would
/// destroy a signed artifact.
fn overwrite(p: &Path, bytes: &[u8]) -> Result<(), String> {
    std::fs::write(p, bytes).map_err(|e| format!("{}: {e}", p.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sidecar_lands_where_phase_1_looks_for_it() {
        // Pinned against the expression in `phase1_approval::verify` itself.
        assert_eq!(
            sig_path_for(Path::new("/etc/logweir/approval.json")),
            Path::new("/etc/logweir/approval.json").with_extension("sig")
        );
        assert_eq!(
            sig_path_for(Path::new("approval")),
            PathBuf::from("approval.sig")
        );
    }
}
