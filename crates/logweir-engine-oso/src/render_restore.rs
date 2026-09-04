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
    s.push_str("  topic_mapping:\n");
    for (src, dst) in &plan.topic_mapping {
        s.push_str(&format!("    {}: {}\n", yaml_scalar(src), yaml_scalar(dst)));
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

/// Escapes an arbitrary string for safe embedding as a YAML double-quoted
/// scalar, and is used at EVERY interpolation site in both renderers
/// (`backup_id`, bootstrap servers, both sides of `topic_mapping`, every
/// storage field, `checkpoint_state`, `run_id` and `triggered_by`).
///
/// Global Constraint 4 says the three forbidden keys are never emitted "at
/// any value" — a value that, written raw, would ITSELF contain a physical
/// line whose pre-colon token is one of those keys is still an emission.
/// Concretely: `render_validation::render(&plan, "r", Some("x\ndry_run:
/// true"))` written without this function would produce a physical line
/// `dry_run: true` inside `restore.yaml`/`validation.yaml`, because a raw `\n`
/// in an interpolated `&str` is a real newline byte once it lands in the
/// rendered `String`. A `"` would truncate a naively hand-quoted scalar early
/// (see the old `format!("backup_id: \"{}\"\n\n", …)`, which quoted but never
/// escaped); a `\` would be reinterpreted as the start of a YAML escape by
/// whatever eventually reads the file. This function closes all three: it
/// ALWAYS emits a double-quoted scalar (never a bare/plain one) and escapes
/// backslash, double-quote, and every C0 control character (`\n`, `\r`, `\t`
/// by their short names; everything else below 0x20 as `\xHH`).
///
/// Deliberately NOT an attempt at full YAML plain-scalar-safety detection
/// (leading indicators, ambiguous ": ", reserved words, …) with quoting only
/// where "needed": a scalar that is unconditionally quoted is unconditionally
/// safe, and the resulting document is easier for the human reviewer this
/// task exists for to audit than one where quoting is conditional on the
/// input's shape.
///
/// What this function does NOT and CANNOT defend against: upstream's
/// `expand_env_vars` [U/kafka-backup/crates/kafka-backup-cli/src/commands/
/// config.rs:6-35] scans the RAW file text for a literal `${VAR}` and
/// substitutes it BEFORE the file is parsed as YAML at all — a textual
/// preprocessing pass with no awareness of quoting. A value containing a
/// literal `${...}` is expanded by the engine's own preprocessing regardless
/// of how this function escapes it, and the substituted text (drawn from
/// whichever environment variable is named, in the engine's OWN process
/// environment) lands in the file un-requoted. That hazard sits a layer
/// below YAML syntax entirely, so no YAML-scalar escaper — this one or any
/// other — can close it; it is a distinct problem from the one this function
/// solves.
pub(crate) fn yaml_scalar(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
