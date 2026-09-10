//! Renders the `backup.yaml` handed to the engine's `backup` subcommand — the
//! `--from-cluster` half of the drill, unblocked by Global Constraint 18 after
//! global ruling GR4 Part B deferred it.
//!
//! # The four rails of GC18(c), and where each one lives
//!
//! 1. **A mandatory named-topic allowlist with no wildcard** — `render` calls
//!    `logweir_core::guard::reject_glob_metacharacters` over `plan.topics` and
//!    refuses on the first entry carrying one of `* ? [ ] { }`. This is guard
//!    **G-GLOB**, and it is here rather than in the escaper because *no
//!    wildcard is not enforced by not writing one*: upstream's
//!    `TopicSelection.include` "supports glob patterns"
//!    [U/kafka-backup/crates/kafka-backup-core/src/config.rs:334-343], so a
//!    topic legitimately NAMED `orders*` widens one entry into a set, and the
//!    rail would otherwise be satisfied only by nobody having typed one.
//!    `render_restore::render` carries the identical call over both sides of
//!    its `topic_mapping`.
//! 2. **A read-only assertion** — this document names no
//!    `reset_consumer_offsets`, no `auto_consumer_groups`, no `create_topics`
//!    and no consumer-group key of any kind. `BackupPlan` has no field that
//!    could produce one, so the property is structural; the test
//!    `backup_document_names_no_write_key` pins it against the next editor.
//! 3. **`purge_topics` and `dry_run` refused in the rendered document** —
//!    `render_and_digest` re-scans its own output with
//!    `logweir_core::guard::scan_forbidden_keys`, which fails closed. A hit is
//!    unreachable from `render`'s emission set; the check exists because
//!    Global Constraint 4 states the rail as "never emitted, at any value",
//!    and global ruling GR7 forbids an injection hook, so the backstop is
//!    proved by feeding a CONSTRUCTED document to the predicate as input.
//! 4. **The source `cluster_id` recorded and re-asserted `!= target`** — NOT
//!    here. It lands in Task 4, where the run happens; see `BackupPlan`'s own
//!    doc comment for why no field on the plan could carry it.
//!
//! # Why this document is rendered rather than checked in
//!
//! The only `backup` config in the tree is the harness's checked-in
//! `e2e/compose/config/backup-drill.yaml`, invoked by `scripts/e2e-seed.sh` to
//! MANUFACTURE the archive the drill then restores from. Nothing in Logweir
//! ever read it, and a seeded fixture is not a code path: an adopter backing
//! up their own cluster needs a document Logweir produced from a plan the
//! guards accepted, digested so the bytes that ran can be re-derived.
use crate::render_restore::render_storage_block;
use crate::yaml::yaml_scalar;
use logweir_core::engine::{AuthRender, BackupPlan};

/// Why a renderer has an error type at all: two of GC18(c)'s rails are
/// REFUSALS, and a refusal that is not in the return type is a refusal
/// somebody deletes.
///
/// Deliberately just these two variants. Both renderers share it —
/// `render_restore::render` returns the same type for its G-GLOB arm — so a
/// third variant is a change to every call site in the workspace, and neither
/// rail needs one.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RenderError {
    #[error("topic include entry `{0}` contains a glob metacharacter (one of * ? [ ] {{ }}); the engine's TopicSelection treats include entries as glob patterns, so a single name would silently widen to a set")]
    GlobMetacharacter(String),
    #[error("the rendered backup document names forbidden key `{0}`, which Global Constraint 4 forbids at any value")]
    ForbiddenKey(String),
}

/// The rendered backup document and the SHA-256 of the EXACT bytes a caller
/// will write to disk, plus GC18(c)'s third rail.
///
/// The digest is over the rendered `String`, never over a re-serialisation of
/// `plan`, for the same reason `render_restore::render_and_digest` says: the
/// bytes are what the engine loads, and a second rendering is free to drift.
pub fn render_and_digest(plan: &BackupPlan) -> Result<(String, String), RenderError> {
    let doc = render(plan)?;
    scan_rendered_document(&doc)?;
    let digest = logweir_core::ids::sha256_prefixed(doc.as_bytes());
    Ok((doc, digest))
}

/// GC18(c) rail 3 as a function of a document, so the backstop can be proved
/// against a CONSTRUCTED input (global ruling GR7: there is no injection hook,
/// and no Logweir code path may exist that is capable of emitting one of the
/// three keys).
///
/// # The fail-closed case, and why it reports `ForbiddenKey`
///
/// `scan_forbidden_keys` returns `Err` — not an empty list — over text it
/// could not parse, because a scanner that answers "I found nothing" about
/// bytes it never read is the defect the whole guard exists to prevent. That
/// leaves this function with an unparseable document and a two-variant error
/// type, and the honest mapping is the refusing one: an unscannable rendered
/// document has NOT been cleared of the forbidden keys, so it is refused
/// exactly as a hit is, with the guard's own reason carried in the payload
/// where the key name would be. Widening `RenderError` for it was rejected —
/// the variant would be unreachable from `render`'s emission set (this
/// renderer's output is always valid YAML) while costing every match site in
/// the workspace an arm.
pub fn scan_rendered_document(doc: &str) -> Result<(), RenderError> {
    match logweir_core::guard::scan_forbidden_keys(doc) {
        Ok(found) => match found.first() {
            Some(path) => Err(RenderError::ForbiddenKey(path.clone())),
            None => Ok(()),
        },
        Err(refusal) => Err(RenderError::ForbiddenKey(format!(
            "<not scanned: {refusal}>"
        ))),
    }
}

pub fn render(plan: &BackupPlan) -> Result<String, RenderError> {
    // GC18(c) rail 1 / **G-GLOB**. FIRST, before a single byte is pushed: a
    // renderer that refuses halfway has already decided what the document
    // would have said, and the temptation next time is to keep the prefix.
    logweir_core::guard::reject_glob_metacharacters(&plan.topics)
        .map_err(RenderError::GlobMetacharacter)?;

    let mut s = String::new();
    s.push_str("# Rendered by logweir. Do not edit; regenerate with `logweir backup run`.\n");
    s.push_str("mode: backup\n");
    s.push_str(&format!("backup_id: {}\n\n", yaml_scalar(&plan.backup_id)));

    s.push_str("source:\n  bootstrap_servers:\n");
    for b in &plan.source_bootstrap {
        s.push_str(&format!("    - {}\n", yaml_scalar(b)));
    }
    // A NAMED allowlist. `exclude` is deliberately absent: an exclude list is
    // only meaningful against a wider include, and the rail above is that
    // there is no wider include.
    s.push_str("  topics:\n    include:\n");
    for t in &plan.topics {
        s.push_str(&format!("      - {}\n", yaml_scalar(t)));
    }
    // The SASL block. `Plaintext` emits NOTHING AT ALL rather than a
    // `security_protocol: PLAINTEXT` line, because the engine's own default is
    // plaintext and an explicit key here would be a fourth thing to keep in
    // step with upstream's spelling for no behavioural gain. The
    // `ScramSha512` arm — the username, the mechanism and the TLS switch, and
    // never the password — is Task 6's (interface **I1**); it is `todo!()`
    // rather than a silently-plaintext fallthrough, because a plan that ASKED
    // for SCRAM and got an unauthenticated document would be a downgrade
    // performed on the operator's behalf.
    match &plan.source_auth {
        AuthRender::Plaintext => {}
        AuthRender::ScramSha512 { .. } => {
            todo!("the SASL block is Task 6 (I1); this task renders only the Plaintext arm")
        }
    }
    s.push('\n');

    s.push_str("storage:\n");
    // The same shared per-variant function both other renderers use, so the
    // three documents can never disagree about the archive. It already ends
    // with a blank line.
    s.push_str(&render_storage_block(&plan.storage));

    s.push_str("backup:\n");
    s.push_str(&format!(
        "  compression: {}\n",
        yaml_scalar(&plan.compression)
    ));
    // FALSE, pinned: `continuous: true` makes `backup` a long-running tail of
    // the source cluster with no terminal exit code, and every phase in this
    // product is timed between an exec and an exit.
    s.push_str("  continuous: false\n");
    s.push_str(&format!(
        "  segment_max_records: {}\n",
        plan.segment_max_records
    ));
    s.push_str(&format!(
        "  segment_max_bytes: {}\n",
        plan.segment_max_bytes
    ));
    s.push_str(&format!(
        "  max_concurrent_partitions: {}\n",
        plan.max_concurrent_partitions
    ));
    // RENDERED EXPLICITLY, IN BOTH MODES (spec §6.1 M5/N1). Upstream defaults
    // `include_offset_headers` true and `strip_offset_headers` false
    // [U/kafka-backup/config/example-backup.yaml,
    // U/kafka-backup/config/example-restore.yaml]; these two lines write the
    // defaults out so the invariant is an assertion in this document and in
    // this golden rather than an upstream default we are trusting.
    //
    // They are the SAME invariant seen from the two ends. `x-original-offset`
    // and `x-original-timestamp` are stamped on every archived record by the
    // backup, and `crates/logweir/src/drill/phase7_verify.rs:243-265`
    // reconciles the restored records BY that header. So an archive written
    // with `include_offset_headers: false`, or restored with
    // `strip_offset_headers: true`, carries nothing phase 7 can key on and a
    // windowed restore stops being verifiable at all — while still exiting 0.
    // Global Constraint 4's sibling failure mode: a signed scorecard measured
    // around nothing.
    s.push_str("  include_offset_headers: true\n");
    s.push_str("  strip_offset_headers: false\n");
    // GC18(c) rail 2, the read-only assertion, is the ABSENCE of everything
    // below and is asserted by `backup_document_names_no_write_key`:
    // `reset_consumer_offsets`, `auto_consumer_groups`, `create_topics`,
    // `consumer_group_strategy` and `topic_mapping` are all restore-side keys
    // and a `backup` config has no business naming any of them — a backup that
    // could create a topic or commit an offset on the SOURCE cluster is the
    // one thing a backup must never be able to do (Global Constraints 19, 20).
    // Also deliberately not rendered, at any value: purge_topics, dry_run,
    // header_preflight_external (Global Constraint 4).
    Ok(s)
}
