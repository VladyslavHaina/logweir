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
use crate::yaml::{assert_no_unnamed_dollar_brace, reject_dollar_brace, yaml_scalar_checked};
use logweir_core::engine::BackupPlan;

/// Why a renderer has an error type at all: two of GC18(c)'s rails are
/// REFUSALS, and a refusal that is not in the return type is a refusal
/// somebody deletes.
///
/// All THREE renderers share it — `render_restore::render` and
/// `render_validation::render` return the same type — so a new variant is a
/// change to every call site in the workspace and is added only when a rail
/// needs one. Task 2 shipped the first two; Task 3 adds the three below,
/// because each is a distinct refusal an operator has to act on differently:
/// a `${` in an INPUT VALUE names the field to fix, a `${…}` in the FINISHED
/// DOCUMENT names a sequence no input could explain, and an unsupported auth
/// mode names a capability this build does not have.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RenderError {
    #[error("topic include entry `{0}` contains a glob metacharacter (one of * ? [ ] {{ }}); the engine's TopicSelection treats include entries as glob patterns, so a single name would silently widen to a set")]
    GlobMetacharacter(String),
    #[error("the rendered backup document names forbidden key `{0}`, which Global Constraint 4 forbids at any value")]
    ForbiddenKey(String),
    /// **G-EXP**, per-value leg. The payload is the offending INPUT VALUE, so
    /// a caller can name the field; the message deliberately does not
    /// interpolate it, because the same wording has to be right for a topic,
    /// a bootstrap server, a storage prefix and a checkpoint path.
    #[error("input value contains \"${{\", which the engine expands textually BEFORE parsing (U:kafka-backup-cli/src/commands/config.rs:1-34) — the rendered bytes are a template, not the executed document, so the plan hash would not cover what runs")]
    DollarBrace(String),
    /// **G-EXP**, post-render leg. The payload is the whole `${…}` sequence as
    /// found, read to its closing `}` or to the end of its physical line.
    #[error("the rendered document contains the unnamed placeholder `{0}`; the engine substitutes every ${{NAME}} textually before the document is parsed and replaces an UNSET name with the empty string behind a warning, so the only sequences permitted in a document logweir hashed are the two named password placeholders in `crate::yaml`")]
    UnnamedPlaceholder(String),
    /// An auth mode this build cannot render. A TYPED REFUSAL and never a
    /// `todo!()`: a panic would be an unrecoverable abort where Global
    /// Constraint 11 requires a refusal, and a silent fallthrough to the
    /// `Plaintext` arm would be an authentication DOWNGRADE performed on the
    /// operator's behalf.
    ///
    /// **Task 6 made it unreachable from `render`, and it stays.** Both of
    /// `AuthRender`'s arms render, so nothing in `crate::yaml::
    /// render_security_block` constructs this today. It is the rail for the
    /// THIRD mode — `logweir_kafka::reader::AuthConfig::Token` (OAUTHBEARER /
    /// MSK IAM, SP4) has no `AuthRender` twin yet, and the task that adds one
    /// must decide, in the type system, whether the engine can render it. A
    /// variant kept for that is cheaper than the `todo!()` its absence
    /// invites; `crates/logweir-engine-oso/tests/render_scram.rs::
    /// the_unsupported_auth_mode_rail_is_unreachable_from_both_arms` pins
    /// which of the two claims is true today.
    #[error("auth mode `{0}` is not supported by this build; tag 1 renders PLAINTEXT and SASL/SCRAM-SHA-512 and no other. A plan that asked for it is REFUSED rather than rendered unauthenticated — an unauthenticated document from a plan that named a SASL mode would be a downgrade performed on the operator's behalf")]
    UnsupportedAuthMode(String),
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
    // **G-EXP**, post-render leg, and BEFORE the digest — never after. The
    // whole point of the guard is that the hashed bytes are the executed
    // bytes, so a sweep that ran after `sha256_prefixed` would have handed the
    // caller a digest over a template and only then refused.
    assert_no_unnamed_dollar_brace(&doc)?;
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
    // **G-EXP** before **G-GLOB**, and the order is load-bearing: `${`
    // contains two glob metacharacters, so without this line `orders${X}` is
    // refused as a PATTERN and the operator is sent to fix the wrong thing.
    // See `crate::yaml::reject_dollar_brace`.
    reject_dollar_brace(&plan.topics)?;
    // GC18(c) rail 1 / **G-GLOB**. FIRST, before a single byte is pushed: a
    // renderer that refuses halfway has already decided what the document
    // would have said, and the temptation next time is to keep the prefix.
    logweir_core::guard::reject_glob_metacharacters(&plan.topics)
        .map_err(RenderError::GlobMetacharacter)?;

    let mut s = String::new();
    s.push_str("# Rendered by logweir. Do not edit; regenerate with `logweir backup run`.\n");
    s.push_str("mode: backup\n");
    s.push_str(&format!(
        "backup_id: {}\n\n",
        yaml_scalar_checked(&plan.backup_id)?
    ));

    s.push_str("source:\n  bootstrap_servers:\n");
    for b in &plan.source_bootstrap {
        s.push_str(&format!("    - {}\n", yaml_scalar_checked(b)?));
    }
    // A NAMED allowlist. `exclude` is deliberately absent: an exclude list is
    // only meaningful against a wider include, and the rail above is that
    // there is no wider include.
    s.push_str("  topics:\n    include:\n");
    for t in &plan.topics {
        s.push_str(&format!("      - {}\n", yaml_scalar_checked(t)?));
    }
    // THE SASL BLOCK (Task 6, interface **I1**). `Plaintext` emits NOTHING AT
    // ALL, which is why the two backup goldens that predate SCRAM are
    // byte-identical after this task; the `ScramSha512` arm emits the engine's
    // `security:` map — the protocol, the ONE-hyphen mechanism, the username
    // FROM THE PLAN BYTES (**G-ID**) and the password PLACEHOLDER, never the
    // password. `crate::yaml::render_security_block` is the single
    // implementation all three renderers share, and its doc comment carries
    // the measured evidence for the nesting and for the unquoted placeholder.
    //
    // Task 2 shipped a `todo!()` here and Task 3 replaced it with
    // `RenderError::UnsupportedAuthMode` — a typed refusal, because Task 4
    // wired backup RUNS before this block existed. That variant stays (see
    // its own doc comment): it is the rail for a mode `AuthRender` does not
    // yet have, not a placeholder for this one.
    s.push_str(&crate::yaml::render_security_block(
        &plan.source_auth,
        crate::yaml::PLACEHOLDER_SOURCE_PASSWORD,
    )?);
    s.push('\n');

    s.push_str("storage:\n");
    // The same shared per-variant function both other renderers use, so the
    // three documents can never disagree about the archive. It already ends
    // with a blank line.
    s.push_str(&render_storage_block(&plan.storage)?);

    s.push_str("backup:\n");
    s.push_str(&format!(
        "  compression: {}\n",
        yaml_scalar_checked(&plan.compression)?
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
    // RENDERED EXPLICITLY (spec §6.1 M5/N1). Upstream defaults
    // `include_offset_headers` true
    // [U/kafka-backup/config/example-backup.yaml]; this line writes the
    // default out so the invariant is an assertion in this document and in
    // this golden rather than an upstream default we are trusting.
    //
    // `x-original-offset` and `x-original-timestamp` are stamped on every
    // archived record by the backup, and
    // `crates/logweir/src/drill/phase7_verify.rs:243-265` reconciles the
    // restored records BY that header. So an archive written with
    // `include_offset_headers: false` carries nothing phase 7 can key on and a
    // windowed restore stops being verifiable at all — while still exiting 0.
    // Global Constraint 4's sibling failure mode: a signed scorecard measured
    // around nothing.
    //
    // **THE OTHER END OF THAT INVARIANT IS A RESTORE KEY, AND IS NOT RENDERED
    // HERE** (Task 4 review, F-1). `strip_offset_headers` is a field of
    // `RestoreOptions` [U:crates/kafka-backup-core/src/config.rs:793-801] and
    // of nothing else; `BackupOptions` (`:404-541`) has exactly one
    // offset-header field, `include_offset_headers` (`:455-458`). The engine
    // at the GC8 floor (`kafka-backup` 0.21.0, the digest in
    // `third_party/kafka-backup-binary.digest`) therefore reads
    // `backup.strip_offset_headers` as an UNKNOWN key and drops it with
    // *"Ignoring unknown config key"*
    // [U:crates/kafka-backup-cli/src/commands/config.rs:46] — and
    // `OsoCliEngine`'s `assert_no_dropped_logweir_key` correctly aborts on any
    // key of ours the engine ignored (spec §7.2(a)), so rendering it made
    // `logweir backup run` exit 1 AFTER a complete archive had been written.
    // `render_restore` still renders it, at `restore.strip_offset_headers`,
    // where the engine accepts it — see `render_restore.rs:131-143`. The
    // absence here is asserted by
    // `the_backup_document_names_no_restore_side_offset_key` and by both
    // backup goldens, and is closed end-to-end against the real engine by
    // `e2e/tests/backup_argv.rs::the_real_engine_accepts_the_rendered_backup_document`.
    s.push_str("  include_offset_headers: true\n");
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
