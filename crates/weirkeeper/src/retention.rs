//! The controller's archive location: [`ARCHIVE_URL_ENV`], and the one parser
//! of an archive URL, [`storage_url_for`].
//!
//! # The per-schedule retention report is withdrawn (PROD-17.1 B1)
//!
//! This module used to evaluate `BackupSchedule.spec.retention` against the
//! archive and render removal commands into `status.retentionReport`. It
//! deleted nothing, and v1's deletion path is a `RetentionPolicy`, so the
//! report is cut: a schedule that still sets `spec.retention` keeps backing up
//! and carries the `RetentionReportWithdrawn` condition instead.
//!
//! What is left is shared by the backup, restore and preflight paths. `main`
//! reads [`configured_archive_url`] once and builds the controller's ONE
//! archive handle with `Store::read_only_from_url`, which cannot put (guard
//! **G-RET**; `scripts/check-no-archive-write.sh` keeps the writable
//! constructor out of this crate). `Store` is blocking, so every call from a
//! reconciler goes through `tokio::task::spawn_blocking` (interface **I13**,
//! `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking`).

use logweir_core::engine::StorageUrl;

/// The environment variable naming the controller's archive location.
///
/// A controller with this unset holds **no** archive handle at all.
pub const ARCHIVE_URL_ENV: &str = "LOGWEIR_ARCHIVE_URL";

/// The archive URL this process is configured with, from the RAW environment
/// read — or `None` for "no archive configured".
///
/// **AN EMPTY VALUE IS UNSET, AND THAT IS PLAN ERRATUM E19(e).** The shipped
/// `config/manager/deployment.yaml` carries `LOGWEIR_ARCHIVE_URL: ""` — the
/// default install, with verification display switched off — and
/// `env::var` returns `Ok("")` for a Kubernetes `env:` entry with an empty
/// `value:`, not `Err(NotPresent)`. Sending that string on to
/// [`storage_url_for`] made the controller log an **ERROR** naming an
/// unreadable archive URL on every clean start of the default install. An
/// empty archive URL is a DOCUMENTED SWITCH, not a misconfiguration.
///
/// # Why it takes the `Result` instead of reading the variable itself
///
/// So a test can hand it `Ok(String::new())` — the exact value the defect is
/// about — without mutating process-global state that every other row in the
/// binary shares. The predicate is the whole of the decision; `main` supplies
/// the read. It lives here rather than in `main.rs` because a `fn` behind
/// `fn main` is reachable from no test at all, which is how the wrong ERROR
/// line survived: nothing printed it until `RUST_LOG: info` was pinned on the
/// same Deployment by the same task.
#[must_use]
pub fn configured_archive_url(raw: Result<String, std::env::VarError>) -> Option<String> {
    let value = raw.ok()?.trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// The `(root, prefix)` an object-store URL names: the location the archive
/// handle is ROOTED at, and the key prefix left over for a listing.
///
/// `s3://kafka-backups/mvp-demo` → `("kafka-backups", "mvp-demo")`. A URL with
/// no scheme separator is taken whole as the first path segment.
///
/// THE INVARIANT (plan erratum E13(d)). `.1` is the prefix left over once the
/// handle's own root is taken out, i.e. `storage_url_for(u)?.prefix()`:
/// `file://` is rooted at the WHOLE path, and `az://` at the CONTAINER, two
/// segments in. `tests/retention.rs`'s
/// `the_listing_prefix_agrees_with_the_root_the_handle_is_built_from` asserts
/// it per scheme.
///
/// `az://acct` and the two-slash `file://relative/x` fall through to the
/// generic split: [`storage_url_for`] refuses both, so no handle is ever built
/// over them.
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
/// ONE PARSER for [`bucket_and_prefix`] and [`storage_url_for`], so
/// `bucket_and_prefix(u).1 == storage_url_for(u)?.prefix()` holds for every
/// `az://` URL by construction (errata E13(d), E18(a)). An absent container is
/// empty rather than an error; callers decide what that means.
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
/// URL — and **all four addressing values are read HERE rather than left to
/// `AmazonS3Builder::from_env()`** (Task 24). The builder's own chain would
/// serve the controller's `Store`; it would not serve the OTHER consumer of
/// this value, which is the one that broke.
///
/// # The second consumer, and why `endpoint: None` was a real defect
///
/// `controllers::backup::plan_backup_spec` calls this function to fill the
/// `storage` block of the `backup.yaml` document the RUNNER mounts and the
/// pinned engine parses. The engine reads that document's own `endpoint` and
/// `region` keys; it has never read `AWS_ENDPOINT_URL`. So a controller
/// configured for MinIO rendered `endpoint: null` into the plan and the
/// engine dialled Amazon — **measured** on the first Phase B run, where the
/// `Backup` came back `exitCode: 1` with the archive never written.
/// `path_style` and `allow_http` were already read from the environment here
/// for exactly this reason; `AWS_ENDPOINT_URL` and `AWS_REGION` were the two
/// that were missing.
///
/// The variable names are `object_store`'s own, so an adopter pointing the
/// controller at MinIO or Ceph configures it the way they already configure
/// every other S3 client, and this invents no Logweir-specific knob. An unset
/// variable stays `None`, which is the shape every gate in this plan runs in
/// and the shape a real AWS deployment wants.
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
            region: env_value("AWS_REGION"),
            endpoint: env_value("AWS_ENDPOINT_URL"),
            path_style: !env_flag("AWS_VIRTUAL_HOSTED_STYLE_REQUEST"),
            allow_http: env_flag("AWS_ALLOW_HTTP"),
        }),
        "gs" => Ok(StorageUrl::Gcs {
            bucket: first.to_string(),
            prefix: tail.to_string(),
        }),
        // ONE PARSER, THREE CALLERS — and this arm was the third textual copy
        // (Task 24a review carry). `az_parts` is the split; this arm used to
        // re-derive `(container, prefix)` from `tail` with the same two lines,
        // which is exactly the "two parsers of one string that must agree"
        // defect class erratum **E13(d)** and **E18(a)** are about. The
        // invariant row `bucket_and_prefix(u).1 == storage_url_for(u)?.prefix()`
        // now holds by CONSTRUCTION rather than by both copies being edited
        // together.
        "az" => {
            let (account, container, prefix) = az_parts(archive_url);
            if container.is_empty() {
                return Err(format!(
                    "`{archive_url}` names an Azure account but no container: the form is \
                     `az://<account>/<container>[/<prefix>]`"
                ));
            }
            Ok(StorageUrl::Azure {
                account_name: account.to_string(),
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

/// `name`'s value when it is set to something non-empty.
///
/// AN EMPTY VARIABLE IS UNSET. `AWS_ENDPOINT_URL=""` is what a Deployment
/// writes when somebody deletes a value but leaves the key, and an empty
/// endpoint string reaches `object_store` as a URL that parses to nothing.
fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// `true` when `name` is set to something that means yes.
fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| {
        let v = v.trim().to_ascii_lowercase();
        v == "1" || v == "true" || v == "yes"
    })
}
