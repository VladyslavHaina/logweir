//! Renders the `restore.yaml` that serves BOTH phase 5 and phase 6 UNCHANGED.
//! That is deliberate: `validate-restore` force-sets `restore.dry_run = true`
//! on the config it loads [VERIFIED U/kafka-backup/crates/kafka-backup-cli/src/
//! commands/validate_restore.rs:11-19 — `if let Some(ref mut restore) =
//! config.restore { restore.dry_run = true; }`], which is what binds the
//! approved plan to the executed one. We therefore never render `dry_run`
//! ourselves: an ordinary YAML-deserialised `dry_run: true`
//! [VERIFIED config.rs:776-778] reaching phase 6 would produce a no-op restore,
//! exit 0, and a signed scorecard whose RTO was measured around nothing.
use logweir_core::engine::{RestorePlan, StorageUrl};

pub fn render(plan: &RestorePlan) -> String {
    let mut s = String::new();
    s.push_str("# Rendered by logweir. Do not edit; regenerate with `logweir drill run`.\n");
    s.push_str("mode: restore\n");
    s.push_str(&format!("backup_id: \"{}\"\n\n", plan.set.backup_id));

    s.push_str("target:\n  bootstrap_servers:\n");
    for b in &plan.target_bootstrap {
        s.push_str(&format!("    - {b}\n"));
    }
    s.push_str("  topics:\n    include:\n");
    for src in plan.topic_mapping.keys() {
        s.push_str(&format!("      - {src}\n"));
    }
    s.push('\n');

    s.push_str("storage:\n");
    s.push_str(&render_storage_block(&plan.storage));

    s.push_str("restore:\n");
    // config.rs:768-770 — "Topic mapping (original -> target)". There is NO
    // prefix or rename-rule option; the only `prefix` in the whole config is
    // storage.prefix (config.rs:381), a bucket key prefix. So we render one
    // explicit entry per selected topic.
    s.push_str("  topic_mapping:\n");
    for (src, dst) in &plan.topic_mapping {
        s.push_str(&format!("    {src}: {dst}\n"));
    }
    // config.rs:860-863 — create_topics DEFAULTS TO FALSE ("safe default -
    // won't create topics unexpectedly"). A scratch target contains none of
    // the prefixed topics, so without this the restore has nothing to write to.
    s.push_str("  create_topics: true\n");
    // config.rs:~866 — rendered explicitly so the chosen value appears in this
    // golden and in the scorecard. The engine already coerces the absent case
    // to -1 (restore/engine.rs:1448), so only a POSITIVE value changes
    // behaviour, and a single-broker scratch cluster can only satisfy 1.
    s.push_str(&format!(
        "  default_replication_factor: {}\n",
        plan.default_replication_factor
    ));
    // config.rs:930-940 — "full: always scan, even when offset recovery is not
    // requested (findings are warnings in that case)". Requested FOR THE
    // REPORT; Logweir applies its own blocking policy in phase 5.
    s.push_str("  header_preflight: full\n");
    // config.rs:948-954 — HEAD every segment in the window instead of only the
    // oldest per partition. THIS is the lever that yields blocking `errors`
    // (restore/engine.rs:567-568).
    s.push_str("  dry_run_check_segments: true\n");
    // config.rs:840-846 — Option<PathBuf> plus a u64. Pod-local and never
    // uploaded: a crashed restore is NOT resumable in v0.1 (spec §11).
    s.push_str(&format!(
        "  checkpoint_state: {}\n",
        plan.checkpoint_state.display()
    ));
    s.push_str(&format!(
        "  checkpoint_interval_secs: {}\n",
        plan.checkpoint_interval_secs
    ));
    // config.rs:751-758 — "Time window start (epoch milliseconds) for PITR",
    // `pub time_window_start: Option<i64>`. An RFC3339 STRING is a serde TYPE
    // error, not an unknown key, so `Config::from_yaml_with_warnings` fails
    // outright: validate-restore and restore would both abort on config load,
    // no drill could ever complete, and Task 12's stderr unknown-key readback
    // cannot catch a type error.
    s.push_str(&format!(
        "  time_window_start: {}\n",
        plan.time_window.0.timestamp_millis()
    ));
    s.push_str(&format!(
        "  time_window_end: {}\n",
        plan.time_window.1.timestamp_millis()
    ));
    // Deliberately NOT rendered, at any value: purge_topics, dry_run,
    // header_preflight_external. Also not rendered: reset_consumer_offsets and
    // auto_consumer_groups — spec §2's non-goals forbid Logweir from setting
    // either, and they are what `offset_recovery_requested()` keys off
    // (restore/preflight.rs:196-198).
    s
}

/// One arm per upstream `StorageBackendConfig` variant, field-for-field.
/// Emitting the S3 shape for every backend would make `filesystem` and `azure`
/// fail the engine's config load with a serde "missing field" error before
/// either subcommand runs. Shared by `render_restore::render` and
/// `render_validation::render` so the two documents can never disagree about
/// the archive: one arm set, one golden per backend.
pub(crate) fn render_storage_block(storage: &StorageUrl) -> String {
    match storage {
        StorageUrl::S3 {
            bucket,
            prefix,
            region,
            endpoint,
            path_style,
            allow_http,
        } => {
            let mut b = String::new();
            b.push_str(&format!(
                "  backend: s3\n  bucket: {bucket}\n  prefix: {prefix}\n"
            ));
            if let Some(r) = region {
                b.push_str(&format!("  region: {r}\n"));
            }
            if let Some(e) = endpoint {
                b.push_str(&format!("  endpoint: {e}\n"));
            }
            b.push_str(&format!(
                "  path_style: {path_style}\n  allow_http: {allow_http}\n\n"
            ));
            b
        }
        StorageUrl::Azure {
            account_name,
            container_name,
            prefix,
        } => format!(
            "  backend: azure\n  account_name: {account_name}\n  container_name: {container_name}\n  prefix: {prefix}\n\n"
        ),
        StorageUrl::Gcs { bucket, prefix } => {
            format!("  backend: gcs\n  bucket: {bucket}\n  prefix: {prefix}\n\n")
        }
        StorageUrl::Filesystem { path } => {
            format!("  backend: filesystem\n  path: {}\n\n", path.display())
        }
    }
}
