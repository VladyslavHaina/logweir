//! Renders the `restore.yaml` that serves BOTH phase 5 and phase 6 UNCHANGED.
//! That is deliberate: `validate-restore` force-sets `restore.dry_run = true`
//! on the config it loads [VERIFIED U/kafka-backup/crates/kafka-backup-cli/src/
//! commands/validate_restore.rs:11-19 — `if let Some(ref mut restore) =
//! config.restore { restore.dry_run = true; }`], which is what binds the
//! approved plan to the executed one. We therefore never render `dry_run`
//! ourselves: an ordinary YAML-deserialised `dry_run: true`
//! [VERIFIED config.rs:776-778] reaching phase 6 would produce a no-op restore,
//! exit 0, and a signed scorecard whose RTO was measured around nothing.
use crate::render_backup::RenderError;
use crate::yaml::yaml_scalar;
use logweir_core::engine::{RestorePlan, StorageUrl};

/// The rendered restore document and the SHA-256 of the EXACT bytes that
/// `OsoCliEngine::write` hands to `std::fs::write`. T0-14: the document is
/// rendered at phase 5 and again at phase 6, and `fs::write` truncates, so
/// the digest — never a re-serialisation of `plan` — is the only thing that
/// can prove the two are the same document.
pub fn render_and_digest(plan: &RestorePlan) -> Result<(String, String), RenderError> {
    let doc = render(plan)?;
    let digest = logweir_core::ids::sha256_prefixed(doc.as_bytes());
    Ok((doc, digest))
}

pub fn render(plan: &RestorePlan) -> Result<String, RenderError> {
    // GC18(c) rail 1 / **G-GLOB**, restore side. BOTH SIDES of the mapping
    // are checked, because both reach an include-style position: the keys are
    // rendered verbatim into `target.topics.include` below, and the values are
    // rendered as `restore.topic_mapping` targets — which the engine also
    // treats as topic selectors, so a mapped target named `drill-*orders` is
    // as much a pattern as a source named `orders*`. Checking only the keys
    // would leave the half an operator is MORE likely to template.
    //
    // Two calls, not one over a concatenation, so the refusal names which
    // entry it found — an operator fixing `a]b` needs to know whether it is
    // the topic or the prefix that produced it.
    let sources: Vec<String> = plan.topic_mapping.keys().cloned().collect();
    logweir_core::guard::reject_glob_metacharacters(&sources)
        .map_err(RenderError::GlobMetacharacter)?;
    let targets: Vec<String> = plan.topic_mapping.values().cloned().collect();
    logweir_core::guard::reject_glob_metacharacters(&targets)
        .map_err(RenderError::GlobMetacharacter)?;

    let mut s = String::new();
    s.push_str("# Rendered by logweir. Do not edit; regenerate with `logweir drill run`.\n");
    s.push_str("mode: restore\n");
    s.push_str(&format!(
        "backup_id: {}\n\n",
        yaml_scalar(&plan.set.backup_id)
    ));

    s.push_str("target:\n  bootstrap_servers:\n");
    for b in &plan.target_bootstrap {
        s.push_str(&format!("    - {}\n", yaml_scalar(b)));
    }
    s.push_str("  topics:\n    include:\n");
    for src in plan.topic_mapping.keys() {
        s.push_str(&format!("      - {}\n", yaml_scalar(src)));
    }
    s.push('\n');

    s.push_str("storage:\n");
    s.push_str(&render_storage_block(&plan.storage));

    s.push_str("restore:\n");
    // config.rs:768-770 — "Topic mapping (original -> target)". There is NO
    // prefix or rename-rule option; the only `prefix` in the whole config is
    // storage.prefix (config.rs:381), a bucket key prefix. So we render one
    // explicit entry per selected topic. Both sides go through `yaml_scalar`:
    // an operator-influenced topic name is exactly the kind of value GC4
    // cares about ("never emitted, at any value") — see that function's doc.
    s.push_str(&render_topic_mapping_block(&plan.topic_mapping));
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
    // The path is a rendered string like any other and goes through
    // `yaml_scalar` for the same reason.
    s.push_str(&format!(
        "  checkpoint_state: {}\n",
        yaml_scalar(&plan.checkpoint_state.display().to_string())
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
    // cannot catch a type error. These are integers, not strings — no
    // `yaml_scalar` needed; a Rust `i64` cannot contain a YAML metacharacter.
    s.push_str(&format!(
        "  time_window_start: {}\n",
        plan.time_window.0.timestamp_millis()
    ));
    s.push_str(&format!(
        "  time_window_end: {}\n",
        plan.time_window.1.timestamp_millis()
    ));
    // RENDERED EXPLICITLY, IN BOTH MODES, and never `true` (spec §6.1 M5/N1).
    // `include_offset_headers` defaults true on the BACKUP side and stamps
    // `x-original-offset`/`x-original-timestamp` on every archived record;
    // `strip_offset_headers` defaults false on both
    // [U/kafka-backup/config/example-backup.yaml,
    // U/kafka-backup/config/example-restore.yaml]. Rendering `true` here would
    // delete `x-original-offset` on the way INTO the scratch topic — the only
    // key phase 7 reconciles on (`crates/logweir/src/drill/phase7_verify.rs:
    // 243-265` reads it and falls back to the target offset), so a windowed
    // restore would become unverifiable: every sampled record would look like
    // a mismatch, or (worse) accidentally match by position on a partition
    // that happened to restore from 0 with nothing dropped. It is written out
    // rather than inherited so the invariant is an ASSERTION in this document
    // and this golden, not an upstream default we are trusting — the same
    // argument Global Constraint 20 makes for `consumer_group_strategy`.
    s.push_str("  strip_offset_headers: false\n");
    // Deliberately NOT rendered, at any value: purge_topics, dry_run,
    // header_preflight_external. Also not rendered: reset_consumer_offsets and
    // auto_consumer_groups — spec §2's non-goals forbid Logweir from setting
    // either, and they are what `offset_recovery_requested()` keys off
    // (restore/preflight.rs:196-198).
    Ok(s)
}

/// The `restore.topic_mapping` block exactly as it appears in the rendered
/// `restore.yaml`. Factored out of `render` so
/// `scorecard.target.topic_mapping_sha256` — whose own field doc promises
/// "sha256 over the rendered restore.yaml topic_mapping block, so an auditor
/// can re-derive exactly what was written" — hashes the BYTES the engine was
/// actually handed. A second, independently-formatted rendering of the same
/// map would be free to drift from this one, and an auditor re-deriving the
/// hash would then get a different answer than the drill published.
pub fn render_topic_mapping_block(mapping: &std::collections::BTreeMap<String, String>) -> String {
    let mut s = String::from("  topic_mapping:\n");
    for (src, dst) in mapping {
        s.push_str(&format!("    {}: {}\n", yaml_scalar(src), yaml_scalar(dst)));
    }
    s
}

/// One arm per upstream `StorageBackendConfig` variant, field-for-field.
/// Emitting the S3 shape for every backend would make `filesystem` and `azure`
/// fail the engine's config load with a serde "missing field" error before
/// either subcommand runs. Shared by `render_restore::render` and
/// `render_validation::render` so the two documents can never disagree about
/// the archive: one arm set, one golden per backend. Every free-text field
/// goes through `yaml_scalar`.
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
                "  backend: s3\n  bucket: {}\n  prefix: {}\n",
                yaml_scalar(bucket),
                yaml_scalar(prefix)
            ));
            if let Some(r) = region {
                b.push_str(&format!("  region: {}\n", yaml_scalar(r)));
            }
            if let Some(e) = endpoint {
                b.push_str(&format!("  endpoint: {}\n", yaml_scalar(e)));
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
            "  backend: azure\n  account_name: {}\n  container_name: {}\n  prefix: {}\n\n",
            yaml_scalar(account_name),
            yaml_scalar(container_name),
            yaml_scalar(prefix)
        ),
        StorageUrl::Gcs { bucket, prefix } => format!(
            "  backend: gcs\n  bucket: {}\n  prefix: {}\n\n",
            yaml_scalar(bucket),
            yaml_scalar(prefix)
        ),
        StorageUrl::Filesystem { path } => format!(
            "  backend: filesystem\n  path: {}\n\n",
            yaml_scalar(&path.display().to_string())
        ),
    }
}
