//! The controller's archive location: [`ARCHIVE_URL_ENV`], and the one parser
//! of an archive URL, [`storage_url_for`].
//!
//! The per-schedule retention report is withdrawn: v1's deletion path is a
//! `RetentionPolicy`, and a schedule that still sets `spec.retention` keeps
//! backing up and carries the `RetentionReportWithdrawn` condition.
//!
//! `main` reads [`configured_archive_url`] once and builds the controller's one
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

/// The archive URL this process is configured with, from the raw environment
/// read, or `None` for "no archive configured".
///
/// An empty value is unset: `env::var` returns `Ok("")` for a Kubernetes `env:`
/// entry with an empty `value:`, and the chart may render
/// `LOGWEIR_ARCHIVE_URL: ""` as a documented switch, which must not log an
/// error on every clean start. It takes the `Result` so a test can pass
/// `Ok(String::new())` without mutating process state; `main` supplies the
/// read.
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
/// `.1` is the prefix left once the handle's own root is taken out, i.e.
/// `storage_url_for(u)?.prefix()`: `file://` is rooted at the whole path, and
/// `az://` at the container, two segments in
/// (`tests/retention.rs::the_listing_prefix_agrees_with_the_root_the_handle_is_built_from`).
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
/// One parser for [`bucket_and_prefix`] and [`storage_url_for`], so
/// `bucket_and_prefix(u).1 == storage_url_for(u)?.prefix()` holds for every
/// `az://` URL by construction. An absent container is empty rather than an
/// error; callers decide what that means.
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
/// Exactly the four backends Global Constraint 9 fixes `object_store`'s feature
/// set at (`s3`, `gs`, `az` and a local path), one per `StorageUrl` variant.
/// Anything else is an error naming the scheme, never a silent fallback to a
/// filesystem handle rooted wherever the process happens to be.
///
/// Region, endpoint and the addressing flags come from the environment, read
/// here rather than left to `AmazonS3Builder::from_env()`, because this value
/// also fills the `storage` block of the `backup.yaml` the runner mounts, and
/// the engine reads that document's own `endpoint` and `region`, never
/// `AWS_ENDPOINT_URL`: with `endpoint: None` a controller configured for MinIO
/// would send the engine to Amazon. The variable names are `object_store`'s
/// own, so no Logweir-specific knob is invented; an unset one stays `None`.
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
        // One parser: `az_parts` is the split, so
        // `bucket_and_prefix(u).1 == storage_url_for(u)?.prefix()` holds by
        // construction.
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
        // A two-slash `file://` URL is refused, not reinterpreted: reading
        // `file://relative/x` as `/relative/x` would root the controller's one
        // archive handle somewhere nobody named.
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
/// An empty variable is unset: `AWS_ENDPOINT_URL=""` is what a Deployment
/// writes when a value is deleted but the key kept, and an empty endpoint
/// reaches `object_store` as a URL that parses to nothing.
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
