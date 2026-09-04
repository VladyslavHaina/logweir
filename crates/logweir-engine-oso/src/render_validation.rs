//! Renders `validation.yaml` — a DIFFERENT schema from restore.yaml.
//! [VERIFIED U/kafka-backup/crates/kafka-backup-core/src/validation/config.rs:10-38]
//! `ValidationConfig` REQUIRES `backup_id`, `storage` and `target: KafkaConfig`;
//! `checks`, `evidence`, `notifications`, `pitr_timestamp` and `triggered_by`
//! are `#[serde(default)]`.
//!
//! WARNING, and the reason this document is golden-tested per engine tag:
//! `validation run` parses with a bare serde_yaml::from_str::<ValidationConfig>
//! and produces NO unknown-key warning list, so a key we get wrong here is
//! dropped SILENTLY (spec §7.2(b)). The stdout+stderr readback of Task 12 does not
//! cover this document.
use crate::render_restore::{render_storage_block, yaml_scalar};
use logweir_core::engine::RestorePlan;

pub fn render(plan: &RestorePlan, run_id: &str, triggered_by: Option<&str>) -> String {
    let mut s = String::new();
    s.push_str("# Rendered by logweir. Do not edit.\n");
    s.push_str(&format!(
        "backup_id: {}\n\n",
        yaml_scalar(&plan.set.backup_id)
    ));

    // Same per-variant rendering as render_restore.rs, and for the same reason:
    // `StorageBackendConfig` is internally tagged with incompatible required
    // fields per variant, so one shape for all four backends fails the config
    // load with a serde error the unknown-key readback cannot see.
    s.push_str("storage:\n");
    s.push_str(&render_storage_block(&plan.storage));

    s.push_str("target:\n  bootstrap_servers:\n");
    for b in &plan.target_bootstrap {
        s.push_str(&format!("    - {}\n", yaml_scalar(b)));
    }
    s.push('\n');

    s.push_str(
        "checks:\n  message_count:\n    enabled: true\n  offset_range:\n    enabled: true\n",
    );
    s.push_str("  consumer_group_offsets:\n    enabled: false\n\n");

    // config.rs:174-188 EvidenceConfig { formats, signing, storage }, and
    // :225-233 EvidenceStorageConfig { prefix, retention_days }. Logweir sets
    // `storage.prefix` EXPLICITLY rather than by default, because listing this
    // per-run prefix is the ONLY specified way to retrieve the report: the run
    // mints its report id internally and never prints it machine-readably
    // (spec §6 C4). `run_id` is escaped as part of the WHOLE composed value —
    // not interpolated raw into an otherwise-unescaped line — so a `run_id`
    // containing a newline or colon cannot split this into two lines either.
    let evidence_prefix = format!("logweir/{run_id}/engine-validation");
    s.push_str("evidence:\n  formats:\n    - json\n  storage:\n");
    s.push_str(&format!("    prefix: {}\n", yaml_scalar(&evidence_prefix)));
    s.push_str("    retention_days: 2555\n");

    if let Some(t) = triggered_by {
        s.push_str(&format!("\ntriggered_by: {}\n", yaml_scalar(t)));
    }
    s
}
