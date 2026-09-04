use crate::drill::DrillError;
use logweir_kafka::reader::ClusterReader;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct TopicState {
    pub partitions: i32,
    pub end_offsets: Vec<(i32, i64)>,
    pub configs: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct TargetState {
    pub cluster_id: String,
    pub topics: BTreeMap<String, TopicState>,
}

/// Reads the target's ACTUAL state — the input phase 3 diffs against. Only the
/// topics named in `of_interest` are described, so a large cluster costs a
/// bounded number of DescribeConfigs calls.
///
/// A topic named in `of_interest` that the target does not have AT ALL is
/// SKIPPED, not an error: phase 3 turns its absence into a `would_create`
/// entry, which is the normal case on a scratch cluster.
///
/// A topic the target DOES have, but whose metadata carried an error (leader
/// election, an authorization gap, a topic mid-delete — see
/// `TopicMeta::error`'s own doc comment), is a DIFFERENT fact: it might hold
/// any number of records and its config might or might not differ, and we
/// simply do not know. Neither skipping it (which phase 3 would read as
/// "absent — safe to create fresh") nor inserting a `TopicState` built from
/// its `partitions: 0` and empty reads (which phase 3 would read as "present
/// but empty") is honest — both would let an auditor draw a conclusion this
/// phase never actually established. This function refuses the WHOLE read
/// instead, so the distinction lives in the return type itself: `Err` here
/// can never be mistaken for a legitimate `Ok` that records absence or
/// emptiness, which a log line alone would not guarantee.
pub fn run(reader: &dyn ClusterReader, of_interest: &[String]) -> Result<TargetState, DrillError> {
    let cluster_id = reader.cluster_id()?;
    let existing = reader.list_topics()?;
    let mut topics = BTreeMap::new();
    for name in of_interest {
        let Some(meta) = existing.iter().find(|t| &t.name == name) else {
            continue;
        };
        if let Some(err) = &meta.error {
            return Err(DrillError::Operational(format!(
                "target topic `{name}` exists but its metadata could not be read cleanly \
                 ({err}); its before-state is UNKNOWN, and recording it as absent (safe to \
                 create) or empty (zero records) would misstate what the restore is about to \
                 do to it"
            )));
        }
        topics.insert(
            name.clone(),
            TopicState {
                partitions: meta.partitions,
                end_offsets: reader.end_offsets(name)?,
                configs: reader.topic_configs(name)?,
            },
        );
    }
    Ok(TargetState { cluster_id, topics })
}
