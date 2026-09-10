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
//! module renders the exact commands an operator would run — `aws s3 rm` and
//! `mc rm` — as strings, into `BackupSchedule.status.retentionReport`, and
//! runs none of them. Retention never touches a Kafka topic, in any tag.
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
use logweir_core::engine::StorageUrl;
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
    /// The exact commands an operator would run. Rendered, never executed.
    /// One per entry of [`RetentionReport::sets_that_would_be_removed`], in
    /// the same order.
    pub aws_cli: Vec<String>,
    /// The `mc` spelling of the same commands, same order. Rendered, never
    /// executed.
    pub mc_cli: Vec<String>,
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
        use crate::crds::backup_schedule::{RemovableSetReport, RetentionReport as Status};
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
/// # Errors
///
/// [`StoreError`] from the list or from any manifest read. A manifest that is
/// not a manifest answers `StoreError::NotAManifest` and a manifest-shaped
/// body bounding no window answers `StoreError::Backend` — two different
/// facts, two different errors (Task 13 review carry).
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
    let keys = store
        .list_manifest_keys(prefix)
        .map_err(|e| StoreError::Io(e.to_string()))?;

    let mut sets: Vec<(String, DateTime<Utc>)> = Vec::with_capacity(keys.len());
    for key in &keys {
        let facts = store.manifest_facts(key)?;
        sets.push((facts.backup_id, from_ms(facts.newest_record_ms)));
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
        .map(|s| aws_rm(archive_url, &s.backup_id))
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
        note,
    })
}

/// The command an operator runs against `aws s3`. **Rendered, never executed.**
///
/// `aws s3 rm <url>/<id>/ --recursive`, with exactly one slash between the
/// archive URL and the id whatever the spec's trailing slash looked like.
#[must_use]
pub fn aws_rm(archive_url: &str, backup_id: &str) -> String {
    format!(
        "aws s3 rm {}/{backup_id}/ --recursive",
        archive_url.trim_end_matches('/')
    )
}

/// The command an operator runs against `mc`. **Rendered, never executed.**
///
/// `mc rm --recursive --force local/<bucket>/<prefix>/<id>/`. `local` is
/// `mc`'s own alias for a configured endpoint and is the adopter's to change;
/// the bucket and prefix are split out of `archive_url` by [`bucket_and_prefix`].
#[must_use]
pub fn mc_rm(archive_url: &str, backup_id: &str) -> String {
    let (bucket, prefix) = bucket_and_prefix(archive_url);
    let mut target = format!("local/{bucket}");
    if !prefix.is_empty() {
        target.push('/');
        target.push_str(&prefix);
    }
    format!("mc rm --recursive --force {target}/{backup_id}/")
}

/// The `(bucket, prefix)` an object-store URL names.
///
/// `s3://kafka-backups/mvp-demo` → `("kafka-backups", "mvp-demo")`. A URL with
/// no scheme separator is taken whole as the first path segment, so a
/// malformed value produces a legible command an operator will notice rather
/// than a panic in a reconcile.
#[must_use]
pub fn bucket_and_prefix(archive_url: &str) -> (String, String) {
    let rest = archive_url
        .split_once("://")
        .map_or(archive_url, |(_, r)| r)
        .trim_matches('/');
    match rest.split_once('/') {
        Some((bucket, prefix)) => (bucket.to_string(), prefix.trim_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    }
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
        "file" => Ok(StorageUrl::Filesystem {
            path: std::path::PathBuf::from(format!("/{}", rest.trim_start_matches('/'))),
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
