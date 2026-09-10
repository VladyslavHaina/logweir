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
use crate::yaml::{assert_no_unnamed_dollar_brace, reject_dollar_brace, yaml_scalar_checked};
use logweir_core::engine::{RestorePlan, StorageUrl};

/// The rendered restore document and the SHA-256 of the EXACT bytes that
/// `OsoCliEngine::write` hands to `std::fs::write`. T0-14: the document is
/// rendered at phase 5 and again at phase 6, and `fs::write` truncates, so
/// the digest — never a re-serialisation of `plan` — is the only thing that
/// can prove the two are the same document.
pub fn render_and_digest(plan: &RestorePlan) -> Result<(String, String), RenderError> {
    let doc = render(plan)?;
    // **G-EXP**, post-render leg, BEFORE the digest — see
    // `render_backup::render_and_digest` for why the order is the property.
    assert_no_unnamed_dollar_brace(&doc)?;
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
    let targets: Vec<String> = plan.topic_mapping.values().cloned().collect();
    // **G-EXP** ahead of **G-GLOB** on both sides — `${` is two glob
    // metacharacters, and the reason an operator is given decides which fix
    // they attempt (`crate::yaml::reject_dollar_brace`).
    reject_dollar_brace(&sources)?;
    reject_dollar_brace(&targets)?;
    logweir_core::guard::reject_glob_metacharacters(&sources)
        .map_err(RenderError::GlobMetacharacter)?;
    logweir_core::guard::reject_glob_metacharacters(&targets)
        .map_err(RenderError::GlobMetacharacter)?;

    let mut s = String::new();
    s.push_str("# Rendered by logweir. Do not edit; regenerate with `logweir drill run`.\n");
    s.push_str("mode: restore\n");
    s.push_str(&format!(
        "backup_id: {}\n\n",
        yaml_scalar_checked(&plan.set.backup_id)?
    ));

    s.push_str("target:\n  bootstrap_servers:\n");
    for b in &plan.target_bootstrap {
        s.push_str(&format!("    - {}\n", yaml_scalar_checked(b)?));
    }
    s.push_str("  topics:\n    include:\n");
    for src in plan.topic_mapping.keys() {
        s.push_str(&format!("      - {}\n", yaml_scalar_checked(src)?));
    }
    // THE SASL BLOCK (Task 6, interface **I1**), and the reason the whole task
    // exists on this side: before it, the rendered `restore.yaml` emitted a
    // bare `target: bootstrap_servers:` with no security keys at all, so even a
    // correctly authenticated Logweir handed the engine a config that could not
    // authenticate. `Plaintext` emits nothing, which is what keeps the four
    // `restore_yaml*` goldens byte-identical.
    //
    // `plan.target_auth` is the ONLY source this block has — **G-ID**. The
    // username is a field of the PLAN, which `plan_hash` covers; it is never
    // read from a `KafkaCluster` object at render time, because that object is
    // mutable after approval and on SASL/SCRAM the principal IS the
    // authorisation.
    s.push_str(&crate::yaml::render_security_block(
        &plan.target_auth,
        crate::yaml::PLACEHOLDER_TARGET_PASSWORD,
    )?);
    s.push('\n');

    s.push_str("storage:\n");
    s.push_str(&render_storage_block(&plan.storage)?);

    s.push_str("restore:\n");
    // config.rs:768-770 — "Topic mapping (original -> target)". There is NO
    // prefix or rename-rule option; the only `prefix` in the whole config is
    // storage.prefix (config.rs:381), a bucket key prefix. So we render one
    // explicit entry per selected topic. Both sides go through `yaml_scalar`:
    // an operator-influenced topic name is exactly the kind of value GC4
    // cares about ("never emitted, at any value") — see that function's doc.
    s.push_str(&render_topic_mapping_block(&plan.topic_mapping)?);
    // **Guard G-TS.** `false`, rendered EXPLICITLY rather than left to the
    // engine's own default, so the chosen value is in this golden and in the
    // approved bytes.
    //
    // The engine's default is already false (config.rs:860-863, "safe default
    // - won't create topics unexpectedly") and Logweir used to render `true`,
    // because a scratch target contains none of the prefixed topics and the
    // restore would otherwise have nothing to write to. The problem is HOW the
    // engine creates them: `TopicToCreate { name, num_partitions,
    // replication_factor }`
    // [U:crates/kafka-backup-core/src/restore/engine.rs:1447-1455] carries no
    // configuration at all, so every target topic lands on cluster defaults —
    // typically `retention.ms = 604800000`, under which an older restore point
    // writes segments already past the deletion threshold and they are removed
    // on the next retention check, possibly AFTER phase 7 signed a `pass`; and
    // under a broker on `message.timestamp.type = LogAppendTime` every
    // restored timestamp is overwritten with the restore's wall clock, voiding
    // the timestamp work the whole product rests on.
    //
    // So Logweir creates the target topics itself, with the two settings that
    // decide whether the restore survives:
    // `logweir_kafka::reader::TARGET_TOPIC_CONFIGS`, applied through
    // `TopicCreator` by `logweir::drill::phase0_admit::create_target_topics`.
    // The engine must therefore create nothing, and this line is what says so.
    s.push_str("  create_topics: false\n");
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
        yaml_scalar_checked(&plan.checkpoint_state.display().to_string())?
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
    // ---------------------------------------------------------------------
    // **THE OFFSET SIDE, RENDERED EXPLICITLY** (Global Constraint 20, spec
    // §6.1 N6/N9). All four lines, unconditionally, in both modes.
    //
    // THE REASON IS THAT AN INHERITED DEFAULT IS NOT AN ASSERTION. Global
    // Constraint 20 says "tag 1 makes no consumer-group offset commit
    // anywhere", and until this task the three keys that decide it were left
    // at the engine's own defaults — the note that stood here said
    // `reset_consumer_offsets` and `auto_consumer_groups` were "deliberately
    // not rendered", which meant the invariant was a belief about
    // [U:crates/kafka-backup-core/src/config.rs:1023, :1029] rather than a
    // line in this document, in the approved bytes and in this golden. An
    // upstream bump that flipped either default would have flipped Logweir's
    // behaviour with nothing to show it.
    //
    // Each key is a `RestoreOptions` field, so all four sit under `restore:`
    // at this indent [VERIFIED against the pinned engine 0.21.0:
    // `consumer_group_strategy` config.rs:773-774, `reset_consumer_offsets`
    // :853-854, `offset_report` :857-858, `auto_consumer_groups` :899-900;
    // upstream's own round-trip test at :1400-1421 parses `restore:
    // consumer_group_strategy: skip` with `warnings.is_empty()`]. Indentation
    // is part of the contract — plan erratum E3 is the SASL block that landed
    // one indent too shallow and ran UNAUTHENTICATED behind four "Ignoring
    // unknown config key" lines.
    //
    // `skip` is `OffsetStrategy::Skip`'s wire spelling: the enum is
    // `#[serde(rename_all = "kebab-case")]` [config.rs:719-726], and `Skip` is
    // one word, so kebab-case leaves it `skip`.
    s.push_str("  consumer_group_strategy: skip\n");
    // `false` and `false`, and never `true` in tag 1: these two are what
    // `offset_recovery_requested()` keys off (restore/preflight.rs:196-198),
    // and `auto_consumer_groups: true` additionally enables
    // `reset_consumer_offsets` behind the operator's back [config.rs:897].
    // Upstream also REFUSES `reset_consumer_offsets: true` with an empty
    // `consumer_groups` [config.rs:1247-1253], so `false` is the only value
    // this document can carry that does not either commit offsets or fail
    // config validation.
    s.push_str("  reset_consumer_offsets: false\n");
    s.push_str("  auto_consumer_groups: false\n");
    // The report itself: written, never applied (Global Constraint 35). It is
    // a path like `checkpoint_state` above and goes through `yaml_scalar` for
    // the same reason.
    s.push_str(&format!(
        "  offset_report: {}\n",
        yaml_scalar_checked(&plan.offset_report.display().to_string())?
    ));
    // Deliberately NOT rendered, at any value: purge_topics, dry_run,
    // header_preflight_external (Global Constraint 4). `reset_consumer_offsets`
    // and `auto_consumer_groups` USED to be on that list; they are above now,
    // pinned at `false`, which is the same invariant stated instead of assumed.
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
pub fn render_topic_mapping_block(
    mapping: &std::collections::BTreeMap<String, String>,
) -> Result<String, RenderError> {
    let mut s = String::from("  topic_mapping:\n");
    for (src, dst) in mapping {
        s.push_str(&format!(
            "    {}: {}\n",
            yaml_scalar_checked(src)?,
            yaml_scalar_checked(dst)?
        ));
    }
    Ok(s)
}

/// One arm per upstream `StorageBackendConfig` variant, field-for-field.
/// Emitting the S3 shape for every backend would make `filesystem` and `azure`
/// fail the engine's config load with a serde "missing field" error before
/// either subcommand runs. Shared by `render_restore::render` and
/// `render_validation::render` so the two documents can never disagree about
/// the archive: one arm set, one golden per backend. Every free-text field
/// goes through `yaml_scalar`.
pub(crate) fn render_storage_block(storage: &StorageUrl) -> Result<String, RenderError> {
    Ok(match storage {
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
                yaml_scalar_checked(bucket)?,
                yaml_scalar_checked(prefix)?
            ));
            if let Some(r) = region {
                b.push_str(&format!("  region: {}\n", yaml_scalar_checked(r)?));
            }
            if let Some(e) = endpoint {
                b.push_str(&format!("  endpoint: {}\n", yaml_scalar_checked(e)?));
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
            yaml_scalar_checked(account_name)?,
            yaml_scalar_checked(container_name)?,
            yaml_scalar_checked(prefix)?
        ),
        StorageUrl::Gcs { bucket, prefix } => format!(
            "  backend: gcs\n  bucket: {}\n  prefix: {}\n\n",
            yaml_scalar_checked(bucket)?,
            yaml_scalar_checked(prefix)?
        ),
        StorageUrl::Filesystem { path } => format!(
            "  backend: filesystem\n  path: {}\n\n",
            yaml_scalar_checked(&path.display().to_string())?
        ),
    })
}
