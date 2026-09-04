use logweir_core::guard::{check_topic_mapping_coverage, scan_forbidden_keys, GuardRefusal};
use logweir_core::spec::{AllowedClusters, DrillSpec};
use logweir_kafka::reader::ClusterReader;
use std::collections::BTreeMap;

pub struct Admitted {
    pub target_cluster_id: String,
    pub topic_mapping: BTreeMap<String, String>,
}

/// Every check here is OBSERVED. The `spec_text` argument is the raw file
/// bytes, not the parsed struct, so a forbidden key survives round-tripping.
pub fn run(
    spec: &DrillSpec,
    spec_text: &str,
    allowed: &AllowedClusters,
    reader: &dyn ClusterReader,
) -> Result<Admitted, GuardRefusal> {
    let bad = scan_forbidden_keys(spec_text);
    if !bad.is_empty() {
        return Err(GuardRefusal(format!(
            "forbidden key(s) present in the drill spec, at any value: {}. \
             purge_topics is irreversible, absent from the engine's dry run, has no \
             confirmation gate and truncates EVERY partition of each target topic \
             regardless of partition or time-window filters; dry_run would make the \
             restore a no-op and the measured RTO meaningless; header_preflight_external \
             would silently disable the header scan the drill depends on.",
            bad.join(", ")
        )));
    }

    // The two PURELY LOCAL checks run first, before any network round trip. A
    // local refusal should not need a reachable broker, and putting them first
    // is what lets `guard_cli.rs` distinguish "refused by the mapping guard"
    // from "refused because the broker was down".
    let topic_mapping: BTreeMap<String, String> = spec
        .source
        .topics
        .iter()
        .map(|t| {
            (
                t.clone(),
                format!("{}{t}", spec.target.topic_mapping_prefix),
            )
        })
        .collect();
    check_topic_mapping_coverage(&spec.source.topics, &topic_mapping)?;

    let target_cluster_id = reader
        .cluster_id()
        .map_err(|e| GuardRefusal(format!("cannot read the target cluster id: {e}")))?;
    if !allowed.allowed_cluster_ids.contains(&target_cluster_id) {
        return Err(GuardRefusal(format!(
            "target cluster id {target_cluster_id} is not in allowedClusterIds"
        )));
    }
    if allowed.source_cluster_id.as_deref() == Some(target_cluster_id.as_str()) {
        return Err(GuardRefusal(format!(
            "target cluster id {target_cluster_id} equals the source cluster id"
        )));
    }

    let topics = reader
        .list_topics()
        .map_err(|e| GuardRefusal(format!("cannot list target topics: {e}")))?;
    if !topics.iter().any(|t| t.name == spec.target.marker_topic) {
        return Err(GuardRefusal(format!(
            "marker topic `{}` does not exist on cluster {target_cluster_id}. \
             Create it on the SCRATCH cluster only — its existence is the v0.1 \
             segregation proof.",
            spec.target.marker_topic
        )));
    }

    Ok(Admitted {
        target_cluster_id,
        topic_mapping,
    })
}
