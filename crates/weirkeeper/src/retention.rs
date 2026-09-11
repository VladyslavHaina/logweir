//! Retention **reporting** — guard **G-RET**.
//!
//! # The whole design in one sentence
//!
//! Nothing here deletes anything, and the reason it cannot is a **type**: the
//! only entry point takes a [`Store`] the caller built with
//! [`Store::read_only_from_url`], whose `read_only` flag makes every put
//! method refuse before it checks anything else, so the reporting path holds
//! no writable archive handle to misuse.
//!
//! # Why reporting, and not a reaper
//!
//! Deleting from an archive is the one operation whose first defect is
//! unrecoverable. The draft's reaper was a delete **outside** `logweir/` by a
//! component that cannot construct a handle there at all:
//! `Store::from_url`'s `LOGWEIR_ROOT` guard refuses to build a *writable*
//! handle over the archive prefix, since an archive is never under `logweir/`
//! (`crates/logweir-store/src/lib.rs`, `from_url`'s guard; the rationale is
//! spelled out at `crates/logweir-engine-oso/src/engine.rs:55-63`). Global
//! Constraint 6 is "Logweir writes only under its own `logweir/` prefix…
//! `PutMode::Create` everywhere", and it **stands unamended**: reporting needs
//! no amendment, costs nothing, and removes the entire class of bug where the
//! first deletion defect destroys an archive.
//!
//! **The adopter's own bucket lifecycle policy does the deleting.** This
//! module renders the exact commands an operator would run — the archive
//! scheme's own CLI (`aws s3 rm`, `gsutil rm`, `az storage blob delete-batch`
//! or `rm -rf`) and the `mc` spelling of the same removal — as strings, into
//! `BackupSchedule.status.retentionReport`, and runs none of them. Retention
//! never touches a Kafka topic, in any tag.
//!
//! # Three mechanisms, because "the caller builds it read-only" is a convention
//!
//! 1. **The handle cannot put.** `Store`'s `read_only` flag is checked FIRST
//!    in `put_create_only`, before the `LOGWEIR_ROOT` assertion, so a
//!    read-only handle built over the archive prefix — which is exactly what
//!    `read_only_from_url` exists to allow — can never reach a codepath that
//!    writes. `tests/retention.rs::the_handle_refuses_to_put` asserts on the
//!    `StoreError::ReadOnly` variant.
//! 2. **The source cannot name a writable constructor.**
//!    `scripts/check-no-archive-write.sh` greps this crate's `src/` — comment
//!    and doc-comment lines stripped first — for `Store::from_url`,
//!    `put_create_only`, `PutMode`, `.put(`, `.put_opts(` and a
//!    **receiver-anchored** `.delete(`. Never the bare word delete: that is a
//!    Kubernetes verb the controller legitimately holds on Jobs and
//!    ConfigMaps, and an ordinary English word in exactly the doc comments
//!    this design requires. The gate is wired into `just lint` beside
//!    `check-one-signer.sh` and has **no path exemption list**.
//! 3. **`Store` is blocking, so every call from a reconciler is inside
//!    `spawn_blocking`** — interface **I13**, below.
//!
//! # Interface I13 — `Store` is blocking, and the rule that follows from it
//!
//! Every [`Store`] method drives its own current-thread runtime
//! (`crates/logweir-store/src/lib.rs`'s `new_rt`, and the `rt.block_on` calls
//! in `get`, `list_keys` and `put_create_only`), so a call from inside a
//! `kube` reconcile panics with *Cannot start a runtime from within a
//! runtime*: `kube`'s `Controller` drives reconcilers **on** a tokio runtime,
//! and `Runtime::block_on` from a thread already driving one panics.
//!
//! **Every call from a reconciler therefore goes through
//! `tokio::task::spawn_blocking(move || …).await`, and the handle is
//! constructed once at controller start and shared as `Arc<Store>` rather
//! than rebuilt per reconcile.** [`evaluate`] is a plain synchronous function
//! for exactly that reason — it is the body of a `spawn_blocking` closure, not
//! an `async fn` — and
//! `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking` reads
//! the source of this file and of every reconciler and fails if a store call
//! appears in an `async fn` body outside such a closure. Without it the code
//! compiles cleanly and dies at the first retention reconcile.

use chrono::{DateTime, TimeZone as _, Utc};
use logweir_core::engine::{EngineError, StorageUrl};
use logweir_store::{Store, StoreError};

use crate::crds::backup_schedule::Retention;

/// The environment variable naming the controller's archive location.
///
/// WHY AN ENVIRONMENT VARIABLE AND NOT `BackupSchedule.spec.archive.url`.
/// Interface **I13** requires ONE handle, built once at controller start; a
/// handle built from a per-object spec field would be a handle per reconcile,
/// which is the mutant the second half of
/// `no_store_call_is_made_outside_spawn_blocking` kills. `spec.archive.url` is
/// still what the rendered commands name — it reaches [`evaluate`] as
/// `archive_url`, a string — so an adopter reading the report sees their own
/// archive location and not the controller's connection.
///
/// A controller with this unset holds **no** archive handle at all and writes
/// no `retentionReport`; that is the shape every gate in this task runs in.
pub const ARCHIVE_URL_ENV: &str = "LOGWEIR_ARCHIVE_URL";

/// The `note` when no retention rule is configured.
///
/// The exact sentence, asserted by
/// `tests/retention.rs::no_retention_rule_removes_nothing`. A schedule with no
/// `spec.retention` block, and one whose block sets neither field, are the
/// same fact and get the same sentence.
pub const NO_RULE_NOTE: &str = "no retention rule is configured; nothing would be removed";

/// Why a backup set appears in [`RetentionReport::sets_that_would_be_removed`].
///
/// A CLOSED SET, and [`RemovalReason::name`] is a `match` with no wildcard, so
/// a new variant fails to compile until someone names it — the same
/// arrangement `controllers::approval::ApprovalRefusal::reason` uses, and for
/// the same reason: the machine-readable half of a status field must be a set
/// the compiler enumerates rather than a string a branch invented.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemovalReason {
    /// The set's newest record is older than `keep_days` days before the
    /// evaluation instant. **Reported in preference to
    /// [`RemovalReason::BeyondKeepLast`] when both rules select the set**,
    /// because age is the reason an operator acts on.
    OlderThanKeepDays {
        /// The configured `keepDays`.
        days: u32,
    },
    /// The set is at 1-based rank `rank` in newest-first order, and `rank`
    /// exceeds `keep_last`.
    BeyondKeepLast {
        /// The set's 1-based rank in newest-first order.
        rank: u32,
    },
}

impl RemovalReason {
    /// The variant name, for the status block's machine-readable `reason`.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::OlderThanKeepDays { .. } => "OlderThanKeepDays",
            Self::BeyondKeepLast { .. } => "BeyondKeepLast",
        }
    }
}

/// One manifest key the evaluation could not read, and why.
/// **Task 19 review, finding F-5.**
///
/// WHY A SKIP AND NOT AN ERROR. `evaluate` used to return `Err` on the first
/// manifest that did not parse, so an archive of fifty good backup sets plus
/// one stray `x/manifest.json` — a sibling JSON object the `/manifest.json`
/// filter picked up, which is the exact case `StoreError::NotAManifest`'s own
/// doc comment names — produced NO retention report at all. One unreadable
/// object is a fact about that object; it is not a reason to stop reporting on
/// the other fifty.
///
/// SKIPPING CANNOT INVENT A REMOVAL, which is why it is the safe direction.
/// A skipped set is not ranked and not aged, so it appears in neither
/// `sets_kept` nor `sets_that_would_be_removed`; and because ranks are
/// assigned over the sets that DID parse, dropping one can only ever move a
/// surviving set to a lower (safer) rank. A report over a partly unreadable
/// archive therefore under-reports removals and never over-reports them — the
/// defensible direction for a field whose siblings are `aws s3 rm` commands.
/// The skip is recorded here, and warned once per key, so it is never silent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkippedManifest {
    /// The manifest key, exactly as the archive listed it.
    pub key: String,
    /// Why it was skipped — the `StoreError`'s own message, so the
    /// `NotAManifest` / `Backend` distinction Task 13 landed survives into the
    /// status block a reader sees.
    pub reason: String,
}

/// One backup set a retention rule selects. **It is still in the archive.**
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovableSet {
    /// The backup set's id — the manifest key's parent directory, derived by
    /// `logweir_store::backup_id_from_manifest_key`, the one function
    /// `Store::list_manifests` and `Store::manifest_facts` also call.
    pub backup_id: String,
    /// The set's newest record instant, read from the manifest BODY.
    pub newest_record_at: DateTime<Utc>,
    /// Which rule selected it, and with what parameter.
    pub reason: RemovalReason,
}

/// What one retention evaluation found. **Nothing here was deleted.**
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionReport {
    /// The instant the evaluation was performed at, passed in rather than
    /// read: a reconciler reads its clock once, in the `kube::runtime`
    /// wrapper, and everything downstream takes the instant as a value.
    pub evaluated_at: DateTime<Utc>,
    /// The configured `keepLast`, as it was applied.
    pub keep_last: Option<u32>,
    /// The configured `keepDays`, as it was applied.
    pub keep_days: Option<u32>,
    /// The `backup_id`s the policy keeps, **newest first**.
    pub sets_kept: Vec<String>,
    /// The sets the policy WOULD remove, **newest first**.
    pub sets_that_would_be_removed: Vec<RemovableSet>,
    /// The exact commands an operator would run, in the ARCHIVE SCHEME'S OWN
    /// CLI — see [`cli_rm`]. Rendered, never executed. One per entry of
    /// [`RetentionReport::sets_that_would_be_removed`], in the same order.
    ///
    /// THE FIELD NAME IS `awsCli` ON THE STATUS AND IS NOT RENAMED HERE. It
    /// is CRD API — `crds::backup_schedule::RetentionReport`, and from there
    /// `config/crd/backupschedules.yaml` — and a `file://` archive's remedy
    /// living under a key spelled `awsCli` is a naming wart, not a wrong
    /// command. Renaming it is an API change with its own generated-CRD
    /// regeneration, out of this task's two-file scope, and is carried to the
    /// controller in the task report rather than taken here.
    pub aws_cli: Vec<String>,
    /// The `mc` spelling of the same commands, same order. Rendered, never
    /// executed.
    pub mc_cli: Vec<String>,
    /// The manifest keys the evaluation could not read, and why — see
    /// [`SkippedManifest`]. Empty on a wholly readable archive; NEVER an
    /// error, and never silent.
    pub skipped: Vec<SkippedManifest>,
    /// Why the evaluation removed nothing, when the reason is not "there is
    /// nothing to remove" — see [`NO_RULE_NOTE`].
    pub note: Option<String>,
}

impl RetentionReport {
    /// The typed status block for `BackupSchedule.status.retentionReport`.
    ///
    /// FIELD FOR FIELD, with one flattening: [`RemovalReason`] becomes a
    /// variant name plus `days`/`rank`, because a data-carrying enum renders
    /// as `oneOf` with an inner `type` and a Kubernetes structural schema
    /// forbids that — see
    /// [`crate::crds::backup_schedule::RemovableSetReport`].
    ///
    /// EMPTY VECTORS BECOME `Some([])`, NEVER `None`. An absent
    /// `setsThatWouldBeRemoved` and an empty one are different claims: the
    /// first says "no evaluation happened", the second says "the evaluation
    /// found nothing to remove", and a UI rendering the report has to be able
    /// to tell them apart. The whole block is `Option` on the status, and
    /// THAT is where "no evaluation happened" is said.
    #[must_use]
    pub fn to_status(&self) -> crate::crds::backup_schedule::RetentionReport {
        use crate::crds::backup_schedule::{
            RemovableSetReport, RetentionReport as Status, SkippedManifestReport,
        };
        Status {
            evaluated_at: Some(self.evaluated_at),
            keep_last: self.keep_last.map(i64::from),
            keep_days: self.keep_days.map(i64::from),
            sets_kept: Some(self.sets_kept.clone()),
            sets_that_would_be_removed: Some(
                self.sets_that_would_be_removed
                    .iter()
                    .map(|s| RemovableSetReport {
                        backup_id: s.backup_id.clone(),
                        newest_record_at: s.newest_record_at,
                        reason: s.reason.name().to_string(),
                        days: match s.reason {
                            RemovalReason::OlderThanKeepDays { days } => Some(i64::from(days)),
                            RemovalReason::BeyondKeepLast { .. } => None,
                        },
                        rank: match s.reason {
                            RemovalReason::BeyondKeepLast { rank } => Some(i64::from(rank)),
                            RemovalReason::OlderThanKeepDays { .. } => None,
                        },
                    })
                    .collect(),
            ),
            aws_cli: Some(self.aws_cli.clone()),
            mc_cli: Some(self.mc_cli.clone()),
            skipped: Some(
                self.skipped
                    .iter()
                    .map(|s| SkippedManifestReport {
                        key: s.key.clone(),
                        reason: s.reason.clone(),
                    })
                    .collect(),
            ),
            note: self.note.clone(),
        }
    }
}

/// Evaluate retention against an archive. **The ONLY entry point.**
///
/// It takes a read-only handle **by type, not by convention**: `store` must
/// have been built with [`Store::read_only_from_url`], and the module header
/// lists the three mechanisms that make that a property rather than a hope.
///
/// `archive_url` is the adopter's own archive location (`s3://bucket/prefix`,
/// from `BackupSchedule.spec.archive.url`) and is what the rendered commands
/// name; the handle may be over any backend, which is what lets this guard's
/// tests observe the zero-writes property on a plain filesystem tree.
/// `prefix` is what to list under, in the handle's own key space.
///
/// # The evaluation
///
/// List the archive's manifest keys through the read-only handle, call
/// `Store::manifest_facts` for each, **sort by `newest_record_ms`
/// descending** — read from the manifest body, never from the key string —
/// then mark everything beyond `keep_last` as
/// [`RemovalReason::BeyondKeepLast`] and everything older than `keep_days` as
/// [`RemovalReason::OlderThanKeepDays`]. A set is removable if **either** rule
/// selects it, and reports `OlderThanKeepDays` when both do, because that is
/// the reason an operator acts on.
///
/// A set with `keep_last: None` and `keep_days: None` is never removable and
/// [`RetentionReport::note`] is [`NO_RULE_NOTE`].
///
/// # Blocking
///
/// This function blocks: every `Store` method drives its own current-thread
/// runtime. Interface **I13**: a reconciler calls it inside
/// `tokio::task::spawn_blocking`, never directly.
///
/// # A manifest that cannot be read is skipped, not fatal
///
/// **Task 19 review, finding F-5.** One unreadable object under the prefix — a
/// sibling JSON body the `/manifest.json` filter picked up, a truncated read —
/// lands in [`RetentionReport::skipped`] with the `StoreError`'s own message
/// and is warned once; the other sets are still evaluated and still reported.
/// See [`SkippedManifest`] for why that is the safe direction: a skipped set is
/// neither kept nor listed as removable, and ranks are assigned over the sets
/// that parsed, so a partly unreadable archive under-reports removals and can
/// never over-report them.
///
/// # Errors
///
/// [`StoreError`] from the LIST — a report over an archive nobody could
/// enumerate would be a claim about an unknown number of sets, so that one
/// still fails hard. An individual manifest read never fails this function;
/// the `NotAManifest`-versus-`Backend` distinction Task 13 landed travels into
/// [`SkippedManifest::reason`] instead.
pub fn evaluate(
    store: &Store,
    archive_url: &str,
    prefix: &str,
    retention: &Retention,
    now: DateTime<Utc>,
) -> Result<RetentionReport, StoreError> {
    let keep_last = as_rule("keepLast", retention.keep_last);
    let keep_days = as_rule("keepDays", retention.keep_days);

    // `list_manifest_keys` IS `Store::list_manifests`' own implementation —
    // that method is `list_manifest_keys(loc.prefix())` plus the `backup_id`
    // derivation — and it is the right half here because this function is
    // handed a prefix STRING, not a `StorageUrl`. The `backup_id` comes back
    // from `manifest_facts` below, derived by the one shared function.
    //
    // THE LIST FAILURE KEEPS ITS VARIANT (Task 19 review, finding F-6). A
    // `map_err(|e| StoreError::Io(e.to_string()))` collapsed
    // `EngineError::Unsupported` — "this backend cannot do that at all" —
    // into `Io`, "storage said no", which is the `NotFound`-versus-`Io`
    // defect `StoreError`'s own doc comments argue about, one enum along.
    // `EngineError` has exactly two variants, so the mapping is total and
    // needs no wildcard.
    let keys = store.list_manifest_keys(prefix).map_err(|e| match e {
        EngineError::Unsupported(m) => StoreError::Backend(m),
        EngineError::Operational(m) => StoreError::Io(m),
    })?;

    // A MALFORMED SIBLING IS SKIPPED, NOT FATAL (Task 19 review, finding
    // F-5). `?` here used to lose the whole report to one stray
    // `x/manifest.json` — see [`SkippedManifest`] for why skipping is both
    // honest and the safe direction. The list itself still fails hard above:
    // a report over an archive nobody could enumerate would be a claim about
    // an unknown number of sets.
    let mut sets: Vec<(String, DateTime<Utc>)> = Vec::with_capacity(keys.len());
    let mut skipped: Vec<SkippedManifest> = Vec::new();
    for key in &keys {
        match store.manifest_facts(key) {
            Ok(facts) => sets.push((facts.backup_id, from_ms(facts.newest_record_ms))),
            Err(e) => {
                tracing::warn!(
                    key = %key,
                    error = %e,
                    "this manifest key could not be read; it is SKIPPED and recorded in \
                     status.retentionReport.skipped, and the rest of the archive is still \
                     reported. A skipped set is neither kept nor listed as removable."
                );
                skipped.push(SkippedManifest {
                    key: key.clone(),
                    reason: e.to_string(),
                });
            }
        }
    }

    // NEWEST FIRST, BY THE WINDOW AND NEVER BY THE KEY. `backup_id` is the tie
    // break only, so the order is total and a re-run of the same archive
    // produces the same report.
    sets.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let cutoff = keep_days.map(|d| now - chrono::Duration::days(i64::from(d)));

    let mut sets_kept = Vec::new();
    let mut removable = Vec::new();
    for (i, (backup_id, newest_record_at)) in sets.into_iter().enumerate() {
        // 1-based, and saturating rather than `as`: an archive with more than
        // u32::MAX backup sets is not a case, but a silent wrap is a wrong
        // rank in a report an operator acts on.
        let rank = u32::try_from(i + 1).unwrap_or(u32::MAX);
        let too_old = cutoff.is_some_and(|c| newest_record_at < c);
        let beyond = keep_last.is_some_and(|k| rank > k);
        // THE UNION, AND `OlderThanKeepDays` WINS. Both rules can select one
        // set; age is the reason an operator acts on.
        let reason = if too_old {
            keep_days.map(|days| RemovalReason::OlderThanKeepDays { days })
        } else if beyond {
            Some(RemovalReason::BeyondKeepLast { rank })
        } else {
            None
        };
        match reason {
            Some(reason) => removable.push(RemovableSet {
                backup_id,
                newest_record_at,
                reason,
            }),
            None => sets_kept.push(backup_id),
        }
    }

    let aws_cli = removable
        .iter()
        .map(|s| cli_rm(archive_url, &s.backup_id))
        .collect();
    let mc_cli = removable
        .iter()
        .map(|s| mc_rm(archive_url, &s.backup_id))
        .collect();
    let note = if keep_last.is_none() && keep_days.is_none() {
        Some(NO_RULE_NOTE.to_string())
    } else {
        None
    };

    Ok(RetentionReport {
        evaluated_at: now,
        keep_last,
        keep_days,
        sets_kept,
        sets_that_would_be_removed: removable,
        aws_cli,
        mc_cli,
        skipped,
        note,
    })
}

/// `value` as exactly one POSIX shell word. **Task 19 review, finding F-1.**
///
/// WHY THIS EXISTS AT ALL. The two renderers below are documented — in this
/// module, in the CRD's own field description and in `docs/kubernetes.md` — as
/// "the exact commands an operator would run", so copy-and-paste IS the
/// intended workflow. Everything they interpolate comes from outside Logweir:
/// `backup_id` is the parent directory of a key the ARCHIVE reported, and the
/// bucket and prefix come from `spec.archive.url`. S3 object keys and
/// filesystem directory names may legally contain a space, a `$`, a backtick,
/// a single quote, a newline — and a `;`. Rendered raw,
/// `backup_id = "a;rm -rf ~"` produced TWO shell commands, the second of them
/// destructive, in a status field an operator is invited to paste into a
/// shell. That is not a G-RET break — Logweir still deletes nothing, and the
/// archive is untouched — but the ADVICE Logweir prints has to be safe for a
/// hostile or merely unusual key.
///
/// THE QUOTING IS UNCONDITIONAL, AND SINGLE-QUOTE. Inside `'…'` a POSIX shell
/// expands nothing at all — no `$`, no backtick, no `~`, no glob — and a
/// newline is a literal newline rather than a command separator. The one
/// character that cannot appear inside single quotes is the single quote
/// itself, so an embedded `'` closes the string, escapes a literal quote and
/// reopens it: `'` → `'\''`. There is deliberately no "does this value need
/// quoting?" branch, because that branch is where the holes grow.
#[must_use]
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

/// The command an operator runs against the archive's OWN CLI. **Rendered,
/// never executed.**
///
/// # It is not called `aws_rm` any more, because it was not one
///
/// THE DEFECT. This renderer interpolated the archive URL into
/// `aws s3 rm '<url>/<id>/' --recursive` for EVERY scheme, so a `file://`
/// archive's report carried `aws s3 rm 'file:///srv/archive/backup-001/'
/// --recursive`. `aws s3 rm` takes an `S3Uri`; a `file://` URI is not one, so
/// that entry was not a command an operator could run — in a field this
/// module, the CRD's own field description and `docs/kubernetes.md` all
/// document as "the exact commands an operator would run", with copy-and-paste
/// the intended workflow (Task 19 review). A report on a `file://` archive
/// carried one runnable remedy (the `mc` one) and one that was not a command.
/// `gs://` and `az://` were wrong the same way and had never been looked at.
///
/// # One spelling per scheme, and why each is the one it is
///
/// * `s3://` — `aws s3 rm '<url>/<id>/' --recursive`. **Byte-identical** to
///   what this function always rendered: the `aws` CLI takes the `s3://` URL
///   itself as the target.
/// * `gs://` — `gsutil -m rm -r '<url>/<id>/'`. Same shape, Google's CLI:
///   the `gs://` URL is the target, `-r` recurses and `-m` parallelises what
///   is usually thousands of segment objects.
/// * `az://` — `az storage blob delete-batch --account-name '<account>'
///   --source '<container>' --pattern '<prefix>/<id>/*'`. Azure's CLI takes no
///   `az://` URL: the account and the container are separate arguments, which
///   is the same fact that roots the archive handle at the container — see
///   [`bucket_and_prefix`] — and the key prefix is a glob, never a path
///   argument. **The pattern carries no leading slash**: a blob name does not
///   begin with one, and `'/backup-001/*'` would match nothing.
/// * `file://` — `rm -rf '<path>/<id>/'`. There is no cloud CLI for a
///   filesystem archive, and a backup set is a directory. `mc` is a real
///   answer for the `mc` slot (it operates on a local path directly — see
///   [`mc_rm`]), but this slot is the scheme's OWN tool, and for a filesystem
///   that is the shell.
///
/// THE ENTRY IS NEVER OMITTED for a scheme this function does not recognise:
/// `aws_cli` is documented as one entry per removable set, in the same order,
/// so the correspondence with
/// [`RetentionReport::sets_that_would_be_removed`] is POSITIONAL and a skipped
/// entry would misalign every later row. An unrecognised scheme — which
/// [`storage_url_for`] refuses, so no report is ever rendered from one — falls
/// to the `aws` arm, unchanged, and is a legible command an operator will
/// notice rather than a panic in a reconcile.
///
/// AND `rm -rf` IS THE MOST DESTRUCTIVE OF THE FOUR SPELLINGS, which is
/// exactly why the path is ONE shell word: [`shell_quote`] is unconditional,
/// so a backup set directory named `a;rm -rf ~` is a path and not a second
/// command. Nothing here executes anything — guard **G-RET**,
/// `scripts/check-no-archive-write.sh`, and
/// `tests/retention.rs::the_report_path_spawns_no_process`.
#[must_use]
pub fn cli_rm(archive_url: &str, backup_id: &str) -> String {
    let scheme = archive_url
        .split_once("://")
        .map_or("", |(scheme, _)| scheme);
    match scheme {
        // The scheme whose "CLI" is the shell, and whose target is a path
        // rather than a URL — `bucket_and_prefix` returns that path as the
        // root, so this is the same one parser the listing uses.
        "file" => {
            let (root, prefix) = bucket_and_prefix(archive_url);
            let mut target = root;
            if !prefix.is_empty() {
                target.push('/');
                target.push_str(&prefix);
            }
            target.push('/');
            target.push_str(backup_id);
            target.push('/');
            format!("rm -rf {}", shell_quote(&target))
        }
        "gs" => format!(
            "gsutil -m rm -r {}",
            shell_quote(&backup_set_url(archive_url, backup_id))
        ),
        "az" => {
            let (account, container, prefix) = az_parts(archive_url);
            let pattern = if prefix.is_empty() {
                format!("{backup_id}/*")
            } else {
                format!("{prefix}/{backup_id}/*")
            };
            format!(
                "az storage blob delete-batch --account-name {} --source {} --pattern {}",
                shell_quote(account),
                shell_quote(container),
                shell_quote(&pattern)
            )
        }
        _ => format!(
            "aws s3 rm {} --recursive",
            shell_quote(&backup_set_url(archive_url, backup_id))
        ),
    }
}

/// `<archive_url>/<backup_id>/`, with exactly one slash between them whatever
/// the spec's trailing slash looked like.
///
/// The two schemes whose CLI takes the URL ITSELF as the target — `s3` and
/// `gs` — render this; Azure's takes the container and a glob instead, and a
/// filesystem archive has a path and not a URL.
fn backup_set_url(archive_url: &str, backup_id: &str) -> String {
    format!("{}/{backup_id}/", archive_url.trim_end_matches('/'))
}

/// The command an operator runs against `mc`. **Rendered, never executed.**
///
/// `mc rm --recursive --force 'local/<bucket>/<prefix>/<id>/'`. `local` is
/// `mc`'s own alias for a configured endpoint and is the adopter's to change;
/// the bucket and prefix are split out of `archive_url` by
/// [`bucket_and_prefix`], and the whole target is ONE shell word — see
/// [`shell_quote`].
///
/// AN AZURE ALIAS IS THE ACCOUNT ENDPOINT, so for `az://` the first element
/// after the alias is the CONTAINER, not the account — which is why
/// [`bucket_and_prefix`]'s `.0` is the container. See its own note.
///
/// A FILESYSTEM ARCHIVE HAS NO ALIAS TO NAME. `local` is an `mc` ALIAS — a
/// configured endpoint — and a `file://` archive has no endpoint: its root is
/// an absolute path, which `mc` operates on directly. Since
/// [`bucket_and_prefix`] returns that absolute path as the root (and only for
/// `file://`, which is the one scheme whose root is a path), prefixing it
/// would render `local//srv/archive/…` — a double slash under an alias that
/// names nothing.
#[must_use]
pub fn mc_rm(archive_url: &str, backup_id: &str) -> String {
    let (root, prefix) = bucket_and_prefix(archive_url);
    let mut target = if root.starts_with('/') {
        root
    } else {
        format!("local/{root}")
    };
    if !prefix.is_empty() {
        target.push('/');
        target.push_str(&prefix);
    }
    target.push('/');
    target.push_str(backup_id);
    target.push('/');
    format!("mc rm --recursive --force {}", shell_quote(&target))
}

/// The `(root, prefix)` an object-store URL names: the location the archive
/// handle is ROOTED at, and the key prefix left over for the listing.
///
/// `s3://kafka-backups/mvp-demo` → `("kafka-backups", "mvp-demo")`. A URL with
/// no scheme separator is taken whole as the first path segment, so a
/// malformed value produces a legible command an operator will notice rather
/// than a panic in a reconcile.
///
/// # `file://` IS NOT A BUCKET URL — plan erratum E13(d)
///
/// THE DEFECT. This function used to be scheme-BLIND: strip `…://`, trim the
/// slashes, split at the first `/`. For `file:///a/b/c` that returns
/// `("a", "b/c")` — a "bucket" named after the first segment of an absolute
/// path. But [`storage_url_for`] builds that same URL into
/// `StorageUrl::Filesystem { path: "/a/b/c" }`, and
/// `Store::read_only_from_url` hands it to
/// `LocalFileSystem::new_with_prefix("/a/b/c")`: the handle is rooted at the
/// WHOLE path, and `StorageUrl::prefix()` returns `""` for `Filesystem`
/// because upstream's variant carries no prefix field at all. The reconciler
/// (`controllers::backup_schedule`) then lists under this function's `.1`, so
/// a `file://` archive listed `<root>/b/c` under a store already rooted at
/// `/a/b/c` and found nothing. The report was EMPTY — and silently so: the
/// reconcile succeeds, the `retentionReport` block is written, and every list
/// in it is `[]`, which in a status cannot be told apart from an archive with
/// no backups in it. Task 16b's reviewer hit exactly this on a live cluster
/// and had to plant the manifests at the doubled path to get a report.
///
/// THE INVARIANT, STATED ONCE. `archive_url` is parsed twice on this path —
/// here, and by [`storage_url_for`] to build the controller's one handle — so
/// the two parsers MUST agree: this function's `.1` is the prefix left over
/// once the handle's own root is taken out, i.e. `storage_url_for(u)?.prefix()`.
/// `tests/retention.rs`'s `the_listing_prefix_agrees_with_the_root_the_handle_is_built_from`
/// asserts precisely that, per scheme, rather than pinning literals.
///
/// # `az://` IS ROOTED AT THE CONTAINER, NOT THE ACCOUNT — the same defect
///
/// THE SECOND HALF OF E13(d), closed in the fix round the first half's review
/// ruled. `Store::build_backend`'s Azure arm is
/// `MicrosoftAzureBuilder::from_env().with_account(a).with_container_name(c)`,
/// so the handle is rooted at the CONTAINER — two segments in — exactly as
/// the filesystem handle was rooted at the whole path. The scheme-blind split
/// returned `("acct", "container/pfx")`, and the reconciler then listed
/// `container/pfx` INSIDE the container: `Ok([])`, a silently empty
/// `retentionReport` on one of Global Constraint 9's four supported backends.
/// The two-segment form `az://acct/container` was broken the same way, so
/// there was no "simple az URL" that worked.
///
/// `.0` IS THE CONTAINER AND NOT `acct/container`, because `.0` has exactly
/// one consumer — [`mc_rm`] — and an `mc` alias for Azure IS the account
/// endpoint, so the first path element after the alias is the container:
/// `local/<container>/<prefix>/<id>/`. `("acct/container", "pfx")` would
/// satisfy the listing invariant too and would still render a target `mc`
/// reads as container `acct`. This is the `s3`/`gs` precedent exactly: `.0`
/// is the bucket, and the account/endpoint/region identity comes from
/// `object_store`'s environment chain, never from the rendered target.
///
/// `az://acct` — an account with no container — is NOT special-cased:
/// [`storage_url_for`] REFUSES it, so no handle is ever built over it and no
/// report is ever rendered from it. It falls through to the generic split as
/// `("acct", "")`, exactly as the refused two-slash `file://` form does.
///
/// WHAT IS DELIBERATELY UNCHANGED. `s3://` and `gs://` already satisfied the
/// invariant — their backends are rooted at the bucket, which is the first
/// segment — and are byte-identical across this change.
///
/// The two-slash form `file://relative/x` falls through to the generic split,
/// unchanged: [`storage_url_for`] REFUSES it (Task 19 review, F-7), so no
/// handle is ever built over it and no report is ever rendered from it.
#[must_use]
pub fn bucket_and_prefix(archive_url: &str) -> (String, String) {
    // The one scheme whose root is a PATH and not a named bucket. Checked
    // before the generic split, and only for the three-slash form the
    // `storage_url_for` `"file"` arm accepts.
    if let Some(path) = archive_url.strip_prefix("file://") {
        let path = path.trim_end_matches('/');
        if path.starts_with('/') {
            return (path.to_string(), String::new());
        }
    }
    // The one scheme whose root is TWO segments in: the handle is rooted at
    // the CONTAINER, so the account is not part of the listing prefix and is
    // not the `mc` target's first element either. A URL that names no
    // container is the form `storage_url_for` refuses, and falls through.
    if archive_url.starts_with("az://") {
        let (_account, container, prefix) = az_parts(archive_url);
        if !container.is_empty() {
            return (container.to_string(), prefix.to_string());
        }
    }
    let rest = archive_url
        .split_once("://")
        .map_or(archive_url, |(_, r)| r)
        .trim_matches('/');
    match rest.split_once('/') {
        Some((bucket, prefix)) => (bucket.to_string(), prefix.trim_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    }
}

/// `az://<account>/<container>[/<prefix…>]`, split into its three parts.
///
/// ONE PARSER, TWO CALLERS — [`bucket_and_prefix`] and [`cli_rm`]. E13(d) is a
/// defect class rather than two bugs: two parsers of one string that must
/// agree, and did not. This split is written to be byte-identical to
/// [`storage_url_for`]'s own `"az"` arm — trim the trailing slashes, take the
/// account at the FIRST `/`, then the container at the first `/` of what is
/// left — so the invariant `bucket_and_prefix(u).1 == storage_url_for(u)?
/// .prefix()` holds for every `az://` URL by construction and not by
/// enumeration.
///
/// THE SPLIT IS TOTAL, AND AN ABSENT CONTAINER IS EMPTY rather than an error:
/// `storage_url_for` refuses that form, so no report is ever rendered from it,
/// but a `-> String` renderer has no error to return and a panic in a
/// reconcile is not an answer. Callers decide what an empty container means —
/// [`bucket_and_prefix`] falls through to the generic split, and [`cli_rm`]
/// renders a visibly incomplete `az` command rather than a command for some
/// other cloud.
fn az_parts(archive_url: &str) -> (&str, &str, &str) {
    let rest = archive_url
        .split_once("://")
        .map_or(archive_url, |(_, r)| r)
        .trim_end_matches('/');
    let (account, tail) = match rest.split_once('/') {
        Some((a, t)) => (a, t.trim_matches('/')),
        None => (rest, ""),
    };
    let (container, prefix) = match tail.split_once('/') {
        Some((c, p)) => (c, p.trim_matches('/')),
        None => (tail, ""),
    };
    (account, container, prefix)
}

/// The [`StorageUrl`] for the controller's ONE read-only archive handle.
///
/// WHAT IT SUPPORTS, AND WHY THAT LIST. Exactly the four backends Global
/// Constraint 9 fixes `object_store`'s feature set at — `s3`, `gs`, `az` and a
/// local path — because `Store::build_backend` has one arm per `StorageUrl`
/// variant and there is nothing else to build. Anything else is an error
/// naming the scheme, never a silent fallback to a filesystem handle rooted
/// wherever the process happens to be.
///
/// REGION, ENDPOINT AND CREDENTIALS COME FROM THE ENVIRONMENT, not from this
/// URL: `AmazonS3Builder::from_env()` applies `object_store`'s own credential
/// and endpoint chain, which is what the rest of this workspace already does
/// (`crates/logweir-store/src/lib.rs`'s header states the chain and its
/// limits). `path_style` and `allow_http` are read from the two `AWS_*`
/// variables `object_store` itself names, so an adopter pointing the
/// controller at MinIO configures it the same way they configure the runner,
/// and this task invents no Logweir-specific knob.
///
/// # Errors
///
/// A string naming the scheme, for any scheme with no `StorageUrl` variant.
pub fn storage_url_for(archive_url: &str) -> Result<StorageUrl, String> {
    let (scheme, rest) = archive_url
        .split_once("://")
        .ok_or_else(|| format!("`{archive_url}` is not an object-store URL: it has no `://`"))?;
    let rest = rest.trim_end_matches('/');
    let (first, tail) = match rest.split_once('/') {
        Some((a, b)) => (a, b.trim_matches('/')),
        None => (rest, ""),
    };
    match scheme {
        "s3" => Ok(StorageUrl::S3 {
            bucket: first.to_string(),
            prefix: tail.to_string(),
            region: None,
            endpoint: None,
            path_style: !env_flag("AWS_VIRTUAL_HOSTED_STYLE_REQUEST"),
            allow_http: env_flag("AWS_ALLOW_HTTP"),
        }),
        "gs" => Ok(StorageUrl::Gcs {
            bucket: first.to_string(),
            prefix: tail.to_string(),
        }),
        "az" => {
            if tail.is_empty() {
                return Err(format!(
                    "`{archive_url}` names an Azure account but no container: the form is \
                     `az://<account>/<container>[/<prefix>]`"
                ));
            }
            let (container, prefix) = match tail.split_once('/') {
                Some((c, p)) => (c, p.trim_matches('/')),
                None => (tail, ""),
            };
            Ok(StorageUrl::Azure {
                account_name: first.to_string(),
                container_name: container.to_string(),
                prefix: prefix.to_string(),
            })
        }
        // A TWO-SLASH `file://` URL IS REFUSED, NOT REINTERPRETED (Task 19
        // review, finding F-7). `file:///tmp/x` names `/tmp/x`; but
        // `file://relative/x` used to be rewritten to `/relative/x`, silently
        // turning an authority-shaped value into an absolute root. The
        // controller's ONE archive handle is not the place to guess: an error
        // naming the form is how an adopter finds out, and the alternative is
        // a handle rooted somewhere nobody asked for.
        "file" if !rest.starts_with('/') => Err(format!(
            "`{archive_url}` has only two slashes, so `{rest}` is a URL authority and not a \
             path: the form is `file:///absolute/path`. Reading it as `/{rest}` would root \
             the controller's archive handle somewhere nobody named"
        )),
        "file" => Ok(StorageUrl::Filesystem {
            path: std::path::PathBuf::from(rest),
        }),
        other => Err(format!(
            "`{other}://` is not a storage backend Logweir can read: Global Constraint 9 fixes \
             object_store's feature set at aws/azure/gcp/http, so the schemes are `s3://`, \
             `gs://`, `az://` and `file://`"
        )),
    }
}

/// `true` when `name` is set to something that means yes.
fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| {
        let v = v.trim().to_ascii_lowercase();
        v == "1" || v == "true" || v == "yes"
    })
}

/// A configured retention bound, as a `u32`, or `None`.
///
/// A NEGATIVE BOUND IS NOT A RULE, AND IS NOT CLAMPED EITHER. `keepLast: -1`
/// clamped to `0` would report every set as removable, which is the worst
/// possible reading of a typo in a field whose report names delete commands;
/// clamped to `u32::MAX` it would report none while claiming a rule was
/// applied. It is dropped, `keep_last`/`keep_days` in the report therefore
/// read `None` — "as it was applied" — and if that leaves no rule at all the
/// report carries [`NO_RULE_NOTE`], which is the honest answer: there is no
/// usable rule.
fn as_rule(field: &str, value: Option<i64>) -> Option<u32> {
    match value {
        None => None,
        Some(v) => match u32::try_from(v) {
            Ok(v) => Some(v),
            Err(_) => {
                tracing::warn!(
                    field,
                    value = v,
                    "retention bound is not a non-negative 32-bit count; the rule is NOT applied \
                     and the report says so"
                );
                None
            }
        },
    }
}

/// Epoch milliseconds as an instant. A value `chrono` cannot represent lands
/// on the epoch rather than panicking in a reconcile; a manifest that
/// declares one is already `NotAManifest`-adjacent, and a report is not the
/// place to abort a controller.
fn from_ms(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(|| Utc.timestamp_nanos(0))
}
