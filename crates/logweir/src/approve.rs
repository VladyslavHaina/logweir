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
    /// `--spec`. The drill spec a PER-RUN approval binds by hash. Absent only
    /// under [`ApproveArgs::standing`], which binds a SCOPE and no plan at
    /// all, and required by name otherwise.
    pub spec: Option<PathBuf>,
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
    /// `--standing`. Mint a **standing rehearsal authorization** (D3 §4.3(e))
    /// instead of a per-run approval.
    ///
    /// # Why the product needs a second payload type here
    ///
    /// The `Approval` controller REQUIRES
    /// `PAYLOAD_TYPE_STANDING_AUTHORIZATION` for a `RehearsalSchedule`
    /// referent, and the runner refuses a standing document that is not signed
    /// under it. Before this flag nothing in the product could produce those
    /// bytes — `logweir drill approve` signed only `PAYLOAD_TYPE_APPROVAL`, and the
    /// only producers in the repository were Rust test fixtures. A feature an
    /// operator cannot mint the authorisation for is a feature nobody can use,
    /// which is why this lands with PLAT-14.3b rather than after it.
    ///
    /// # It is the same signer, a second payload type
    ///
    /// `sign_detached` is called once more with a different `payload_type`;
    /// no new primitive, no new crate, and `scripts/check-one-signer.sh`'s
    /// picture of which crates reach the signing half is unchanged.
    pub standing: Option<StandingArgs>,
}

/// The standing half of [`ApproveArgs`] — present exactly when `--standing` is.
pub struct StandingArgs {
    /// `--schedule-namespace`, `--schedule-name`, `--schedule-uid`: the
    /// `subjectRef` INSIDE the signed bytes.
    ///
    /// **The UID is the field that matters.** It is what the runner compares
    /// against `LOGWEIR_EXECUTION_REHEARSAL_SCHEDULE_UID`, so a document
    /// signed for a schedule that was deleted and recreated authorises
    /// nothing — which is the property that makes "approve this rehearsal"
    /// mean a specific rehearsal rather than a name.
    pub schedule_namespace: String,
    pub schedule_name: String,
    pub schedule_uid: String,
    /// `--scope`: a JSON file holding D3 §4.3's [`RehearsalScope`], camelCase.
    /// Read as a file rather than as a dozen flags because it is what the
    /// signature covers and an operator should be able to diff it, review it
    /// and keep it in version control.
    pub scope: PathBuf,
    /// `--valid-days`. `expiresAt - issuedAt`, capped at
    /// [`MAX_STANDING_AUTHORIZATION_DAYS`] by D3 §4.3 — refused HERE so an
    /// operator learns it at minting time rather than from a Job that will not
    /// start.
    pub valid_days: i64,
    /// `--issued-at`, optional. The approver's clock by default. Accepted so a
    /// test can mint the SAME bytes twice; an operator has no reason to set
    /// it, and a future `issuedAt` is refused by the runner's notBefore check
    /// like any other.
    pub issued_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Everything here is `ExitCode::Operational` (1) on failure: this command
/// runs no drill, touches no cluster and reaches no guard, so codes 2, 3 and 4
/// — each of which makes a statement about a drill or an artifact — cannot
/// honestly apply.
pub fn run(args: &ApproveArgs) -> ExitCode {
    let minted = if args.standing.is_some() {
        mint_standing(args)
    } else {
        mint(args)
    };
    match minted {
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
    let spec_path = args.spec.as_ref().ok_or_else(|| {
        "--spec is required: a per-run approval binds the sha256 of a plan's exact bytes. \
         (A standing rehearsal authorization binds a SCOPE instead — see --standing.)"
            .to_string()
    })?;
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
        spec = spec_path.display(),
        approver = args.approver,
        ticket = args.ticket,
        subject_kind = args.subject_kind,
        key_id = key.key_id(),
        out = args.out.display(),
        sig = sig_path.display(),
    ))
}

/// Mint the **signed standing rehearsal authorization** D3 §4.3(e) defines —
/// the document `logweir restore run --standing-authorization` verifies and
/// the `Approval` controller requires for a `RehearsalSchedule` referent.
///
/// # It refuses here what the cluster would refuse later
///
/// Everything this checks before signing is something the runner or the
/// controller checks after: the window and its ninety-day cap, the subject
/// UID, the scope's mode, and the scope fields whose absence is a refusal
/// rather than a default. An operator who has to discover those from a Job
/// that will not start has been handed a signed document that authorises
/// nothing, and re-signing is the one step that needs a key they may not have
/// twice. The last thing before writing anything is
/// `admit_standing_authorization` over the minted document — literally the
/// predicate the runner applies — so a document this command emits is one the
/// runner admits.
///
/// # Nothing about a PLAN is bound, and that is the point
///
/// A per-run approval binds `sha256(plan bytes)`. A standing authorization
/// binds a SCOPE, so one signature covers every slot of one schedule and the
/// controller and runner each prove `plan ∈ scope` per run. `--spec` is
/// therefore refused here.
pub fn mint_standing(args: &ApproveArgs) -> Result<String, String> {
    use logweir_core::execution_contract as wire;

    let standing = args
        .standing
        .as_ref()
        .ok_or_else(|| "mint_standing called without --standing".to_string())?;
    if args.spec.is_some() {
        return Err(
            "--spec is not used with --standing: a standing rehearsal authorization binds a \
             SCOPE and covers every slot of one schedule, so there is no single plan to hash. \
             Pass --scope instead."
                .to_string(),
        );
    }
    if !args.approver.trim().is_empty() || !args.ticket.trim().is_empty() {
        return Err(
            "--approver and --ticket are not used with --standing: version 1.0.0 of the standing \
             rehearsal authorization carries neither field, so a value given here would NOT be \
             signed and an operator would believe their ticket was bound when it was not. \
             PLAT-19.2 adds `requester` and `ticket` to the document."
                .to_string(),
        );
    }
    if standing.schedule_uid.trim().is_empty() {
        return Err(
            "--schedule-uid is required and must not be blank: it is what binds this document to \
             ONE RehearsalSchedule object, and the runner compares it against the UID the \
             controller stamps on the Job"
                .to_string(),
        );
    }
    if standing.schedule_name.trim().is_empty() || standing.schedule_namespace.trim().is_empty() {
        return Err("--schedule-name and --schedule-namespace must not be blank".to_string());
    }
    if standing.valid_days < 1 || standing.valid_days > wire::MAX_STANDING_AUTHORIZATION_DAYS {
        return Err(format!(
            "--valid-days is {} and D3 §4.3 caps a standing rehearsal authorization at {} days \
             (minimum 1); a document outside that window is refused by the controller at every \
             slot and by the runner before phase 0",
            standing.valid_days,
            wire::MAX_STANDING_AUTHORIZATION_DAYS
        ));
    }

    let scope_text = std::fs::read_to_string(&standing.scope)
        .map_err(|e| format!("{}: {e}", standing.scope.display()))?;
    let scope: logweir_core::rehearsal_scope::RehearsalScope = serde_json::from_str(&scope_text)
        .map_err(|e| {
            format!(
                "{} is not a RehearsalScope: {e}. It is D3 §4.3's scope in camelCase: \
                 templateDigest, targetClusterId, topicPrefix, topics, maxPartitions, \
                 recordsPerPartition, deadlineSeconds, modes.",
                standing.scope.display()
            )
        })?;
    // Each of these is a refusal at the runner (`plan_within_scope` treats an
    // absent bound as a MISMATCH, the fail-closed direction), so each is a
    // refusal here where it costs one edit instead of one re-signing.
    if !scope.is_scratch_only() {
        return Err(format!(
            "the scope's `modes` is {:?} and this build authorises `{}` and nothing else; a \
             scope naming a mode the product does not implement is one no reader may act on",
            scope.modes,
            logweir_core::rehearsal_scope::MODE_SCRATCH
        ));
    }
    for (blank, what) in [
        (scope.template_digest.trim().is_empty(), "templateDigest"),
        (scope.target_cluster_id.trim().is_empty(), "targetClusterId"),
        (scope.topic_prefix.trim().is_empty(), "topicPrefix"),
    ] {
        if blank {
            return Err(format!("the scope's `{what}` is blank"));
        }
    }
    if scope.topics.is_empty() {
        return Err(
            "the scope names no `topics`, so it authorises the restore of nothing".to_string(),
        );
    }
    for (zero, what) in [
        (scope.max_partitions == 0, "maxPartitions"),
        (scope.records_per_partition == 0, "recordsPerPartition"),
        (scope.deadline_seconds == 0, "deadlineSeconds"),
    ] {
        if zero {
            return Err(format!(
                "the scope's `{what}` is 0; it is a BOUND, and a bound of zero admits no \
                 rehearsal at all"
            ));
        }
    }

    // **NOT `approval.json`.** The standing document has its own name
    // everywhere else in the product — in the bundle, in the Job's mount, in
    // the runner's flag — precisely because a standing document in the per-run
    // approval slot makes a correctly signed rehearsal look like a substituted
    // approval. Writing one to that filename locally is how an operator comes
    // to paste it into the wrong field.
    if args.out.file_name().and_then(|n| n.to_str()) == Some("approval.json") {
        return Err(
            "--out names `approval.json`, which is the PER-RUN approval's filename. A standing \
             rehearsal authorization is a different document signed under a different payload \
             type; write it to `standing-authorization.json` (its sidecar lands beside it at \
             `.sig`, which is the path the runner derives)."
                .to_string(),
        );
    }

    let key = SigningKey::from_pem_file(&args.key)
        // The PATH, never the contents.
        .map_err(|e| format!("{}: {e}", args.key.display()))?;

    let issued_at = standing.issued_at.unwrap_or_else(chrono::Utc::now);
    let doc = wire::StandingAuthorization {
        format_version: wire::STANDING_AUTHORIZATION_FORMAT_VERSION.to_string(),
        kind: wire::STANDING_AUTHORIZATION_KIND.to_string(),
        subject_ref: wire::AuthorizationSubject {
            api_version: wire::SUBJECT_API_VERSION.to_string(),
            kind: wire::REHEARSAL_SCHEDULE_KIND.to_string(),
            namespace: standing.schedule_namespace.clone(),
            name: standing.schedule_name.clone(),
            uid: standing.schedule_uid.clone(),
        },
        scope,
        issued_at,
        expires_at: issued_at + chrono::Duration::days(standing.valid_days),
    };

    // **AN ALREADY-EXPIRED DOCUMENT IS NOT MINTED.** `admit_standing_authorization`
    // below is handed `issued_at` as its clock, which answers "is this document
    // self-consistent" and not "is it valid NOW" — so with `--issued-at` far
    // enough in the past it would happily sign something every reader refuses.
    // The header promises this command refuses what the cluster would refuse;
    // this is the line that keeps that true for the one flag that can move the
    // window out from under it.
    let now = chrono::Utc::now();
    if doc.expires_at <= now {
        return Err(format!(
            "the authorization would expire at {} and it is now {}: --issued-at {} plus \
             --valid-days {} is already in the past, and signing it would spend a key on a \
             document every reader refuses",
            doc.expires_at.to_rfc3339(),
            now.to_rfc3339(),
            issued_at.to_rfc3339(),
            standing.valid_days
        ));
    }

    // **THE RUNNER'S OWN PREDICATE, BEFORE ANYTHING IS WRITTEN.** Not a
    // paraphrase of it — the same function, from the same crate both halves
    // read — so a document this command emits cannot be one the runner refuses
    // for a reason the minting side forgot to model.
    wire::admit_standing_authorization(&doc, Some(&standing.schedule_uid), issued_at)
        .map_err(|refusal| format!("the minted authorization would be refused: {refusal}"))?;

    let mut payload = serde_json::to_vec_pretty(&doc)
        .map_err(|e| format!("serialising the authorization: {e}"))?;
    payload.push(b'\n');

    // THE SECOND PAYLOAD TYPE, AND THE WHOLE REASON THIS FUNCTION EXISTS. A
    // standing document signed under `PAYLOAD_TYPE_APPROVAL` is refused by
    // `verify_detached` as a payload-type mismatch — the variant whose doc
    // comment calls it evidence of substitution — and vice versa, so an
    // approval can never be replayed as a standing authorization.
    let sidecar = sign_detached(&key, wire::PAYLOAD_TYPE_STANDING_AUTHORIZATION, &payload)
        .map_err(|e| format!("signing the authorization: {e}"))?;

    // DERIVED, exactly as the per-run sidecar is, because the runner derives
    // `--standing-authorization`'s sidecar the same way.
    let sig_path = sig_path_for(&args.out);
    overwrite(&args.out, &payload)?;
    let sig_bytes =
        serde_json::to_vec_pretty(&sidecar).map_err(|e| format!("serialising the sidecar: {e}"))?;
    overwrite(&sig_path, &sig_bytes)?;

    Ok(format!(
        "signed a standing rehearsal authorization\n  schedule   {ns}/{name}\n  \
         uid        {uid}\n  scope      {scope}\n  issued     {issued}\n  \
         expires    {expires}  ({days} day(s))\n  key_id     {key_id}\n  \
         wrote      {out}\n  wrote      {sig}\n\
         \nPut these two files on the Approval as spec.approvalBytes and spec.sidecarBytes,\n\
         with spec.subjectRef.kind RehearsalSchedule and spec.planHash set to the schedule's\n\
         templateDigest. The signing key's PUBLIC half must be on this namespace's trust with\n\
         usage GovernedApproval. The standing format accepts GovernedApproval only, under\n\
         every approval policy: ConsoleConfirmation authorises no rehearsal, and\n\
         EvidenceSigning never authorises.\n",
        ns = standing.schedule_namespace,
        name = standing.schedule_name,
        uid = standing.schedule_uid,
        scope = standing.scope.display(),
        issued = issued_at.to_rfc3339(),
        expires = doc.expires_at.to_rfc3339(),
        days = standing.valid_days,
        key_id = key.key_id(),
        out = args.out.display(),
        sig = sig_path.display(),
    ))
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

/// [`countersign`], as a process exit code.
pub fn run_countersign(args: &CountersignArgs) -> ExitCode {
    match countersign(args) {
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
/// # Errors
///
/// A message naming the file or the refusal.
pub fn countersign(args: &CountersignArgs) -> Result<String, String> {
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
    let now = chrono::Utc::now();
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
