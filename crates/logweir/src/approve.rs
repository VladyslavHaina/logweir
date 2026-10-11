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
    /// `--spec`. The drill spec the approval binds by hash.
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
    /// `--approval-subject` (PROD-15.1), as the wire value that goes into the
    /// document — `originalName` — or `None` for an ordinary approval, whose
    /// bytes carry no such key.
    pub approval_subject: Option<String>,
}

/// Everything here is `ExitCode::Operational` (1) on failure: this command
/// runs no drill, touches no cluster and reaches no guard, so codes 2, 3 and 4
/// — each of which makes a statement about a drill or an artifact — cannot
/// honestly apply.
///
/// # The one clock read of `drill approve`
///
/// Global Constraint 1: the clock is read HERE, at the entry point, and handed
/// down. [`mint`] stamps it as `approved_at` and reads no clock of its own,
/// so a test pins `now` and gets the same bytes on any date.
pub fn run(args: &ApproveArgs) -> ExitCode {
    let now = chrono::Utc::now();
    match mint(args, now) {
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
///
/// `now` is the caller's clock, signed as `approved_at` ([`run`] passes the
/// wall clock). It is an argument because it is not decoration: it is the
/// approval's claimed signing time, which the trust lifecycle judges against
/// the signing key's window (`logweir_core::trust::claimed_signing_time`).
pub fn mint(args: &ApproveArgs, now: chrono::DateTime<chrono::Utc>) -> Result<String, String> {
    let spec_path = &args.spec;
    // `read_to_string`, not `read`, so this hashes exactly what
    // `phase1_approval::verify` hashes: it reads the spec with
    // `fs::read_to_string` and computes `sha256_prefixed(spec_text.as_bytes())`.
    // A spec that is not valid UTF-8 is refused HERE rather than approved into
    // a hash the drill can never reproduce.
    // **A BLANK APPROVER IS NOT AN APPROVER.** clap requires both flags on this
    // path, so this is the second line — and it is the line that holds if a
    // future caller builds `ApproveArgs` itself. `approver` and `ticket` are
    // signed and copied verbatim into the scorecard's accountability record by
    // `phase1_approval::verify`; a drill approved by nobody under no ticket is
    // exactly what that record exists to prevent.
    for (value, flag) in [(&args.approver, "--approver"), (&args.ticket, "--ticket")] {
        if value.trim().is_empty() {
            return Err(format!(
                "{flag} is required and must not be blank: it is signed into the approval and \
                 copied verbatim into the scorecard's accountability record"
            ));
        }
    }
    let spec_text =
        std::fs::read_to_string(spec_path).map_err(|e| format!("{}: {e}", spec_path.display()))?;
    // PROD-11.1b (review L4): never sign a partition subset this release's
    // runner would refuse, because a runner that predates it might not.
    if args.subject_kind == "Restore" {
        refuse_unparseable_subset_plan(&spec_text)
            .map_err(|why| format!("{}: {why}", spec_path.display()))?;
    }
    let plan_hash = sha256_prefixed(spec_text.as_bytes());
    let approval_subject = approval_subject_for(args, &spec_text, spec_path)?;

    let key = SigningKey::from_pem_file(&args.key).map_err(|e| {
        // `logweir_evidence::Error` carries no key material by construction,
        // and neither does this line: it names the PATH, never the contents.
        format!("{}: {e}", args.key.display())
    })?;

    let doc = ApprovalDoc {
        approver: args.approver.clone(),
        ticket: args.ticket.clone(),
        plan_hash: plan_hash.clone(),
        // The approver's clock at the moment they approved: `now`, read by
        // `run` and never here. `drill run` does NOT measure RTO from this —
        // it measures from the moment phase 1 validated the signature — so a
        // skewed approver clock cannot inflate or deflate a measured objective.
        approved_at: now,
        // INSIDE THE SIGNED BYTES. `payload` below is what `sign_detached`
        // covers, and this struct is what `payload` is serialised from, so
        // the kind is bound by the signature. Writing it into the SIDECAR
        // instead would leave check 8 comparing a field anyone who can write
        // the approval bundle could rewrite without touching the signature.
        subject_kind: args.subject_kind.clone(),
        // PROD-15.1: INSIDE THE SIGNED BYTES too, and only when the approver
        // asked for the separate subject — an ordinary approval carries no key.
        approval_subject: approval_subject.wire().map(str::to_string),
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
         ticket     {ticket}\n  subject    {subject_kind}\n  approves   {approval_subject}\n  \
         key_id     {key_id}\n  \
         wrote      {out}\n  wrote      {sig}\n\
         \nThis approval binds the EXACT bytes of {spec}. Edit the spec — including its\n\
         sample window — and `logweir drill run` refuses with exit 3 until you re-run\n\
         this command.\n",
        spec = spec_path.display(),
        approver = args.approver,
        ticket = args.ticket,
        subject_kind = args.subject_kind,
        approval_subject = match approval_subject {
            logweir_core::original_name::ApprovalSubject::OriginalName =>
                "a restore under the ORIGINAL topic names (approval subject originalName)",
            logweir_core::original_name::ApprovalSubject::Ordinary => "an ordinary restore",
        },
        key_id = key.key_id(),
        out = args.out.display(),
        sig = sig_path.display(),
    ))
}

/// **PROD-15.1.** The approval subject this per-run approval carries, refused
/// at MINTING time when it is not the plan's: an original-name plan needs
/// `--approval-subject original-name`, and that subject approves nothing
/// else — nor a plan whose original-name shape the runner refuses (a sampled
/// one included: such a restore requires `sample.coverage: complete`). A plan this build cannot read as a restore plan is approved as it
/// always was, ordinary — and refused if the original-name subject is asked
/// for, because nothing could confirm the plan is one.
fn approval_subject_for(
    args: &ApproveArgs,
    spec_text: &str,
    spec_path: &Path,
) -> Result<logweir_core::original_name::ApprovalSubject, String> {
    use logweir_core::original_name::ApprovalSubject;
    let requested = ApprovalSubject::from_wire(args.approval_subject.as_deref())?;
    let plan = (args.subject_kind == logweir_core::spec::SUBJECT_KIND_RESTORE)
        .then(|| serde_yaml::from_str::<logweir_core::spec::DrillSpec>(spec_text).ok())
        .flatten();
    let needed = plan.as_ref().map(ApprovalSubject::of_plan);
    // The original-name subject is never signed over a plan the runner
    // refuses for its shape — in scratch mode, beside a prefix, or without
    // the complete verification such a restore requires. The approver is
    // told now, in the runner's own words, not after the approval is used.
    if requested == ApprovalSubject::OriginalName {
        if let Some(refusal) = plan
            .as_ref()
            .and_then(logweir_core::original_name::refuse_shape)
        {
            return Err(format!(
                "{} is not a restore under the original topic names the runner accepts: \
                 {refusal}. Nothing was signed.",
                spec_path.display()
            ));
        }
    }
    match (needed, requested) {
        (Some(needed), requested) if needed == requested => Ok(requested),
        (Some(ApprovalSubject::OriginalName), _) => Err(format!(
            "{} restores under the ORIGINAL topic names (target.topic_naming.original_name), \
             which only its own approval authorises: re-run with --approval-subject \
             original-name. Nothing was signed.",
            spec_path.display()
        )),
        (Some(ApprovalSubject::Ordinary), _) => Err(format!(
            "--approval-subject original-name approves a restore under the original topic \
             names, and {} is not one (it carries no target.topic_naming.original_name). \
             Nothing was signed.",
            spec_path.display()
        )),
        (None, ApprovalSubject::Ordinary) => Ok(requested),
        (None, ApprovalSubject::OriginalName) => Err(format!(
            "--approval-subject original-name needs a Restore plan this build can read, to \
             confirm it restores under the original topic names; {} is not one. Nothing was \
             signed.",
            spec_path.display()
        )),
    }
}

/// The name `logweir drill approve` refuses an unparseable partition-subset
/// plan under ([`refuse_unparseable_subset_plan`]).
pub const SUBSET_PLAN_UNPARSEABLE: &str = "SubsetPlanUnparseable";

/// **PROD-11.1b (review L4).** A `Restore` plan that states
/// `restore.partitions` must PARSE under this release's grammar before an
/// approval is minted over it, or the approval is refused, named
/// [`SUBSET_PLAN_UNPARSEABLE`], and nothing is signed.
///
/// Why here and not only at run time: this release's runner and preview
/// refuse a subset written beside a plain instant (`PlanUnparseable`), but an
/// approval binds bytes, not a parse, and a runner that predates partition
/// subsets ignores the unknown key and restores EVERY partition of the plan
/// the approver narrowed. The grammar keeps a subset beside an interval no
/// such runner parses; this keeps an approval from ever carrying one that is
/// not.
///
/// Only a plan that names the key is judged: every other plan is minted
/// exactly as before, including bytes that are not YAML at all, which no
/// runner parses either.
///
/// # Errors
/// The refusal sentence, starting with [`SUBSET_PLAN_UNPARSEABLE`].
pub fn refuse_unparseable_subset_plan(spec_text: &str) -> Result<(), String> {
    let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(spec_text) else {
        return Ok(());
    };
    let names_a_subset = doc
        .get("restore")
        .and_then(|r| r.get("partitions"))
        .is_some_and(|p| !p.is_null());
    if !names_a_subset {
        return Ok(());
    }
    serde_yaml::from_str::<logweir_core::spec::DrillSpec>(spec_text)
        .map(|_| ())
        .map_err(|e| {
            format!(
                "{SUBSET_PLAN_UNPARSEABLE}: the plan states restore.partitions but does not parse \
                 under this release's plan grammar ({e}). No approval was minted: a runner that \
                 predates partition subsets could run an approval of these bytes and restore \
                 every partition. Write the point in time as \"<start>/<end>\" or \"../<end>\"."
            )
        })
}

/// `logweir drill countersign` — the governed approver's half of PLAT-19.2.
pub struct CountersignArgs {
    /// The authorization document v2 bytes, verbatim.
    pub document: PathBuf,
    /// The console's sidecar over those bytes.
    pub confirmation: PathBuf,
    /// The approver's private key.
    pub key: PathBuf,
    /// Where the countersigned sidecar goes.
    pub out: PathBuf,
}

/// [`countersign`], as a process exit code. The one clock read of
/// `drill countersign` (Global Constraint 1): the wall clock, read here and
/// handed to [`countersign`] as `now`.
pub fn run_countersign(args: &CountersignArgs) -> ExitCode {
    match countersign(args, chrono::Utc::now()) {
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

/// Countersign a console-confirmed, GOVERNED authorization document v2.
///
/// # What it refuses before signing
///
/// Everything the `Approval` controller would refuse about the DOCUMENT and
/// the SIDECAR without a cluster: bytes that are not a v2 document, an
/// `Ordinary` document (it needs no approver and a countersignature would
/// change nothing), a document already past its `expiresAt`, a sidecar under
/// another payload type or carrying no confirmation, and a key that already
/// signed it. It CANNOT check that the console's signature is by a key the
/// namespace trusts, or that this key's principal differs from the requester —
/// the controller does both — so the summary prints the requester for the
/// approver to read before anything is written.
///
/// # The bytes are never re-serialised
///
/// The signature covers the file's EXACT bytes, which are the bytes the
/// console signed and the bytes the `Approval` will carry. Parsing is only for
/// the summary and the refusals above.
///
/// # `now` is an argument (Global Constraint 1)
///
/// The instant the `expiresAt` refusal judges: [`run_countersign`] passes the
/// wall clock, and a test passes a fixed instant. Nothing here reads a clock.
///
/// # Errors
///
/// A message naming the file or the refusal.
pub fn countersign(
    args: &CountersignArgs,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<String, String> {
    use logweir_core::approval_policy::{
        ApprovalMode, RestoreAuthorization, PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
    };

    let bytes =
        std::fs::read(&args.document).map_err(|e| format!("{}: {e}", args.document.display()))?;
    let doc = RestoreAuthorization::from_bytes(&bytes)
        .map_err(|e| format!("{}: {e}", args.document.display()))?;
    if doc.authorization_mode != ApprovalMode::Governed {
        return Err(format!(
            "{} is an {} authorization: the console's confirmation is its whole authorization, \
             and there is nothing for an approver to countersign",
            args.document.display(),
            doc.authorization_mode
        ));
    }
    // The caller's `now`, never a clock read here.
    if doc.expires_at <= now {
        return Err(format!(
            "the request expired at {} (it is now {}); an expired request authorises nothing \
             and the operator must submit it again",
            doc.expires_at.to_rfc3339(),
            now.to_rfc3339()
        ));
    }
    let sidecar_bytes = std::fs::read(&args.confirmation)
        .map_err(|e| format!("{}: {e}", args.confirmation.display()))?;
    let mut sidecar: logweir_evidence::Sidecar = serde_json::from_slice(&sidecar_bytes)
        .map_err(|e| format!("{} is not a DSSE sidecar: {e}", args.confirmation.display()))?;
    if sidecar.payload_type != PAYLOAD_TYPE_RESTORE_AUTHORIZATION {
        return Err(format!(
            "{} is a sidecar for {:?}, not for an authorization document v2",
            args.confirmation.display(),
            sidecar.payload_type
        ));
    }
    if sidecar.signatures.is_empty() {
        return Err(format!(
            "{} carries no console confirmation; a governed approval countersigns the console's \
             attestation of the requester and never replaces it",
            args.confirmation.display()
        ));
    }
    let key =
        SigningKey::from_pem_file(&args.key).map_err(|e| format!("{}: {e}", args.key.display()))?;
    let key_id = key.key_id();
    if sidecar.signatures.iter().any(|s| s.keyid == key_id) {
        return Err(format!(
            "key {key_id} has already signed this document; a governed approval is a SECOND, \
             independent signature"
        ));
    }
    let mine = sign_detached(&key, PAYLOAD_TYPE_RESTORE_AUTHORIZATION, &bytes)
        .map_err(|e| format!("signing the authorization: {e}"))?;
    sidecar.signatures.extend(mine.signatures);
    let out = serde_json::to_vec(&sidecar).map_err(|e| format!("serialising the sidecar: {e}"))?;
    overwrite(&args.out, &out)?;

    Ok(format!(
        "countersigned a governed restore request\n  requester  {requester}\n  \
         restore    {ns}/{name} (uid {uid})\n  plan_hash  {plan}\n  policy     {policy} \
         ({digest})\n  expires    {expires}\n  ticket     {ticket}\n  key_id     {key_id}\n  \
         wrote      {out}\n\nSubmit {out} as the approval's sidecar. The controller admits it \
         only if this key is a\nGovernedApproval key on the namespace's TrustPolicy whose \
         principal is NOT the requester.\n",
        requester = doc.requester.principal_id(),
        ns = doc.subject.namespace,
        name = doc.subject.name,
        uid = doc.subject.uid,
        plan = doc.plan_hash,
        policy = doc.policy.name,
        digest = doc.policy.digest,
        expires = doc.expires_at.to_rfc3339(),
        ticket = doc.ticket.as_deref().unwrap_or("-"),
        out = args.out.display(),
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
