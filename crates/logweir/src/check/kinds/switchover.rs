//! `switchoverReadiness` — the review before a `Switchover` (PROD-04.2
//! §3.8).
//!
//! # What it answers, and with whose code
//!
//! Three rows, each the switchover's own step run read-only, so the review
//! and the run cannot disagree about what they check:
//!
//! * **`switchover.evidence`** — the restore's signed scorecard verifies
//!   under the namespace's evidence keys and trust, its plan hash is the
//!   restore's, its verification is complete, and the positions mapping it
//!   binds verifies by digest and by its arms
//!   ([`crate::switchover::restore_evidence`], step 3 of the run).
//! * **`switchover.target`** — the target answers the cluster id the restore
//!   wrote to, and every restored topic a requested group maps onto still
//!   carries the ID recorded at creation
//!   ([`crate::switchover::target_is_the_restores`], step 5).
//! * **`switchover.groups`** — each requested target group's type, state and
//!   members admit a commit ([`so::admit_group`], §3.5), and its committed
//!   positions on the mapped partitions are none or already the mapped ones.
//!   Every group, topic and partition goes to the details stream, one line per
//!   group and restored topic, with the mapped offset, its class and the
//!   committed position read (`GET …/preflights/{id}/details`).
//!
//! # What it never does
//!
//! It commits nothing, creates no group and writes no object: the target
//! handle's commit is never called, and the evidence handle is the
//! `evidenceRead` grant's, which cannot write. A `ready` verdict authorises
//! nothing; the `Switchover` Job repeats every check under its approval.

use std::collections::{BTreeMap, BTreeSet};

use logweir_core::check_contract::{
    CheckCode, CheckId, CheckPlanKind, CheckResult, CheckState, Stream, SwitchoverReadinessRequest,
};
use logweir_core::destination::DestinationRole;
use logweir_core::positions_mapping::{MappingDocument, APPROXIMATE, EXACT};
use logweir_core::switchover as so;

use super::{from_broker_failure, from_store_failure, ready, runner_contract, Wiring};
use crate::check::{catalogue, Deadline, Emission};
use crate::switchover::target::{Part, ReadFailure, SwitchoverTarget};

/// The remedy of a refused evidence row.
pub const EVIDENCE_REMEDY: &str = "Restore again with `coverage: complete` from a point that \
     captured these consumer groups, or fix the namespace's evidence trust; the message names the \
     switchover's own reason.";
/// The remedy of a target that is not the restore's.
pub const TARGET_REMEDY: &str = "The target is not the cluster, or a topic is not the generation, \
     the restore wrote; restore again onto the current target before switching over.";
/// The remedy of a group that cannot be applied now.
pub const GROUPS_REMEDY: &str = "Stop every member of the named group (or choose another target \
     group), give the target principal DESCRIBE and READ on the group, and review again; the \
     details list every group and position.";

/// One requested group: the source the mapping names and the target group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selected {
    pub source: String,
    pub target: String,
}

/// The groups this check reads: the request's, or every group the mapping
/// maps at least one position of (an uncaptured group has nothing to move).
#[must_use]
pub fn selected(req: &SwitchoverReadinessRequest, mapping: &MappingDocument) -> Vec<Selected> {
    if req.groups.is_empty() {
        return mapping
            .groups
            .iter()
            .filter(|(_, g)| g.entries.iter().any(|e| e.mapped()))
            .map(|(id, _)| Selected {
                source: id.clone(),
                target: id.clone(),
            })
            .collect();
    }
    req.groups
        .iter()
        .map(|g| Selected {
            source: g.source.clone(),
            target: g.target.clone().unwrap_or_else(|| g.source.clone()),
        })
        .collect()
}

/// One group's reading, as the details line and the row read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupReading {
    pub group: Selected,
    /// `None`: the group could be applied now; else the switchover's word and
    /// a sentence.
    pub refusal: Option<(String, String)>,
    /// `type`, `state`, `members` as classified (absent: an absent group).
    pub classified: Option<(String, String, u32)>,
    /// Whether the group is absent (a switchover would create it).
    pub absent: bool,
    /// Per restored topic, every mapping entry of this group onto it:
    /// `(partition, mapped offset, class, committed now)`.
    pub topics: BTreeMap<String, Vec<PositionReading>>,
}

/// One partition of a group's reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionReading {
    pub partition: u32,
    /// The target offset the mapping maps to; `None` for `unavailable`.
    pub offset: Option<i64>,
    pub class: String,
    /// The reason of an `unavailable` entry.
    pub reason: Option<String>,
    /// The committed position read now: `None` none, or not read.
    pub committed: Option<i64>,
}

/// **One group, read**: admission by its classification (§3.5), then its
/// committed positions on the mapped partitions, which must be none or
/// already the mapped ones (a plan's `expected: none`). Pure over the target
/// trait, so every rule is a unit row.
#[must_use]
pub fn read_group(
    target: &dyn SwitchoverTarget,
    mapping: &MappingDocument,
    group: &Selected,
) -> GroupReading {
    let mut reading = GroupReading {
        group: group.clone(),
        refusal: None,
        classified: None,
        absent: false,
        topics: BTreeMap::new(),
    };
    let Some(mapped) = mapping.groups.get(&group.source) else {
        reading.refusal = Some((
            "NotInMapping".to_string(),
            format!(
                "the restore's mapping names no group {:?}: its point did not select it",
                group.source
            ),
        ));
        return reading;
    };
    let mut parts: Vec<(Part, i64)> = Vec::new();
    for e in &mapped.entries {
        let Some(topic) = e.target_topic.clone() else {
            continue;
        };
        if e.mapped() {
            if let Some(o) = e.target_offset {
                parts.push(((topic.clone(), e.partition), o));
            }
        }
        reading
            .topics
            .entry(topic)
            .or_default()
            .push(PositionReading {
                partition: e.partition,
                offset: e.target_offset.filter(|_| e.mapped()),
                class: e.class.clone(),
                reason: e.reason.clone(),
                committed: None,
            });
    }
    if parts.is_empty() {
        reading.refusal = Some((
            "NothingMapped".to_string(),
            match mapped.reason.as_deref() {
                Some(r) => format!("the group was not captured ({r})"),
                None => "no position of the group maps onto a restored partition".to_string(),
            },
        ));
        return reading;
    }
    let classified = target.classify(&group.target);
    if let so::TargetGroup::Consumer {
        group_type,
        state,
        members,
    } = &classified
    {
        reading.classified = Some((group_type.clone(), state.clone(), *members));
    }
    reading.absent = classified == so::TargetGroup::Absent;
    let created = match so::admit_group(&classified) {
        Ok(created) => created,
        Err(word) => {
            let detail = match &classified {
                so::TargetGroup::Consumer { state, members, .. } => {
                    format!("the group is {state} with {members} member(s)")
                }
                so::TargetGroup::Unreachable(why) => format!("it could not be classified: {why}"),
                other => format!("{other:?}"),
            };
            reading.refusal = Some((word.to_string(), detail));
            return reading;
        }
    };
    let keys: Vec<Part> = parts.iter().map(|(p, _)| p.clone()).collect();
    match target.committed(&group.target, &keys, !created) {
        Err(f) => {
            let (word, why) = match f {
                ReadFailure::NotAuthorized => (
                    so::NOT_AUTHORIZED,
                    "the target principal may not read the group's offsets".to_string(),
                ),
                ReadFailure::NotVisible => (
                    so::TARGET_NOT_VISIBLE,
                    "the group may exist and this principal may not see it".to_string(),
                ),
                ReadFailure::Unstable => (
                    so::TARGET_POSITIONS_CHANGED,
                    "a transactional offset commit is pending on the group".to_string(),
                ),
                ReadFailure::Other(why) => (so::COMMIT_FAILED, why),
            };
            reading.refusal = Some((word.to_string(), why));
        }
        Ok(now) => {
            let mut differ = 0usize;
            for ((topic, partition), mapped_offset) in &parts {
                let held = now.get(&(topic.clone(), *partition)).copied().flatten();
                if let Some(entry) = reading
                    .topics
                    .get_mut(topic)
                    .and_then(|v| v.iter_mut().find(|p| p.partition == *partition))
                {
                    entry.committed = held;
                }
                if held.is_some_and(|h| h != *mapped_offset) {
                    differ += 1;
                }
            }
            if differ > 0 {
                reading.refusal = Some((
                    so::TARGET_POSITIONS_CHANGED.to_string(),
                    format!(
                        "the group holds committed positions other than the mapped ones on \
                         {differ} partition(s); a plan expecting none is refused"
                    ),
                ));
            }
        }
    }
    reading
}

/// The details line of one group and restored topic.
#[must_use]
pub fn detail_lines(readings: &[GroupReading]) -> Vec<String> {
    let mut out = Vec::new();
    for r in readings {
        let base = |topic: Option<(&String, &Vec<PositionReading>)>| {
            let mut line = serde_json::json!({
                "group": r.group.source,
                "target": r.group.target,
                "admissible": r.refusal.is_none(),
                "absent": r.absent,
            });
            if let Some((t, s, m)) = &r.classified {
                line["type"] = serde_json::json!(t);
                line["state"] = serde_json::json!(s);
                line["members"] = serde_json::json!(m);
            }
            if let Some((word, why)) = &r.refusal {
                line["reason"] = serde_json::json!(word);
                line["message"] = serde_json::json!(why);
            }
            if let Some((topic, positions)) = topic {
                line["topic"] = serde_json::json!(topic);
                line["positions"] = positions
                    .iter()
                    .map(|p| {
                        let mut v = serde_json::json!({
                            "partition": p.partition,
                            "class": p.class,
                            "committed": p.committed,
                        });
                        if let Some(o) = p.offset {
                            v["offset"] = serde_json::json!(o); // engine-token-ok: a JSON field of the details line, not an engine subcommand
                        }
                        if let Some(reason) = &p.reason {
                            v["reason"] = serde_json::json!(reason);
                        }
                        v
                    })
                    .collect();
            }
            line.to_string()
        };
        if r.topics.is_empty() {
            out.push(base(None));
        }
        for topic in &r.topics {
            out.push(base(Some(topic)));
        }
    }
    out
}

/// The `switchover.groups` row over every reading.
#[must_use]
pub fn groups_row(
    readings: &[GroupReading],
    now: chrono::DateTime<chrono::Utc>,
) -> logweir_core::check_contract::CheckOutcome {
    let count = |class: &str| {
        readings
            .iter()
            .flat_map(|r| r.topics.values().flatten())
            .filter(|p| p.class == class)
            .count()
    };
    let (exact, approximate) = (count(EXACT), count(APPROXIMATE));
    let positions: usize = readings
        .iter()
        .map(|r| r.topics.values().map(Vec::len).sum::<usize>())
        .sum();
    let refused: Vec<&GroupReading> = readings.iter().filter(|r| r.refusal.is_some()).collect();
    let summary = format!(
        "{} group(s), {positions} position(s): {exact} exact, {approximate} approximate, {} \
         unavailable",
        readings.len(),
        positions - exact - approximate
    );
    let row = if readings.is_empty() {
        catalogue::outcome(
            CheckId::SwitchoverGroups,
            CheckState::NotReady,
            CheckCode::GroupNotAdmissible,
            now,
        )
        .with_message("no group of the restore's mapping maps a position: there is nothing to move")
        .with_remedy(GROUPS_REMEDY)
    } else if let Some(first) = refused.first() {
        let (word, why) = first.refusal.clone().unwrap_or_default();
        catalogue::outcome(
            CheckId::SwitchoverGroups,
            CheckState::NotReady,
            CheckCode::GroupNotAdmissible,
            now,
        )
        .with_message(&format!(
            "{word}: {} ({why}); {} of {} group(s) cannot be applied now. {summary}",
            first.group.target,
            refused.len(),
            readings.len()
        ))
        .with_remedy(GROUPS_REMEDY)
    } else {
        ready(CheckId::SwitchoverGroups, CheckCode::GroupsAdmissible, now)
            .with_message(&format!("{summary}; every group can be applied now"))
    };
    row.with_fact("groups", &readings.len().to_string())
        .with_fact("positions", &positions.to_string())
        .with_fact("exact", &exact.to_string())
        .with_fact("approximate", &approximate.to_string())
        .with_fact("notAdmissible", &refused.len().to_string())
}

/// A row that could not be asked because an earlier one failed.
fn blocked(
    id: CheckId,
    why: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> logweir_core::check_contract::CheckOutcome {
    catalogue::outcome(
        id,
        CheckState::Unknown,
        CheckCode::BlockedByPrerequisite,
        now,
    )
    .with_message(why)
    .with_remedy(super::remedy_for(CheckCode::BlockedByPrerequisite))
}

/// Run one `switchoverReadiness`.
#[must_use]
pub fn run(req: &SwitchoverReadinessRequest, wiring: &dyn Wiring, deadline: Deadline) -> Emission {
    let now = wiring.now();
    let mut checks = vec![runner_contract(now)];
    let scope = catalogue::scope("Restore", &req.restore_name, Some(&req.restore_uid));
    let mut result = CheckResult::new(CheckPlanKind::SwitchoverReadiness);

    // 1. The restore's evidence, as the switchover's step 3 reads it.
    let evidence = match wiring.objects(
        &req.evidence,
        DestinationRole::EvidenceRead,
        deadline.remaining(),
    ) {
        Err(f) => {
            checks.push(from_store_failure(CheckId::SwitchoverEvidence, &f, now));
            None
        }
        Ok(handle) => {
            let read = |key: &str, cap: u64| {
                handle.get(key, cap).map_err(|e| match e {
                    logweir_engine_oso::storage::StoreError::NotFound(_) => {
                        format!("{key} is not in the evidence store")
                    }
                    other => format!("{key} could not be read: {other}"),
                })
            };
            match crate::switchover::restore_evidence(
                &read,
                &req.keyring,
                &req.scorecard_key,
                &req.restore_plan_sha256,
                None,
            ) {
                Ok(ev) => {
                    checks.push(
                        ready(
                            CheckId::SwitchoverEvidence,
                            CheckCode::RestoreEvidenceVerified,
                            now,
                        )
                        .with_message(&format!(
                            "the restore's scorecard verifies, its verification is complete \
                                 and the positions mapping it binds verifies ({} group(s))",
                            ev.mapping.groups.len()
                        ))
                        .with_fact("scorecardSha256", &ev.scorecard_sha256)
                        .with_fact("mappingSha256", &ev.mapping_sha256)
                        .with_scope(scope.clone()),
                    );
                    Some(ev)
                }
                Err(why) => {
                    checks.push(
                        catalogue::outcome(
                            CheckId::SwitchoverEvidence,
                            CheckState::NotReady,
                            CheckCode::RestoreEvidenceRefused,
                            now,
                        )
                        .with_message(&why)
                        .with_remedy(EVIDENCE_REMEDY)
                        .with_scope(scope.clone()),
                    );
                    None
                }
            }
        }
    };
    let Some(ev) = evidence else {
        let why = "the restore's evidence did not verify, so there is no mapping to read";
        checks.push(blocked(CheckId::SwitchoverTarget, why, now));
        checks.push(blocked(CheckId::SwitchoverGroups, why, now));
        result.checks = checks;
        return Emission::of(result);
    };

    // 2. The target, as the switchover's step 5 reads it.
    let target = match wiring.switchover_target(&req.target, deadline.remaining()) {
        Err(f) => {
            checks.push(from_broker_failure(CheckId::SwitchoverTarget, &f, now));
            checks.push(blocked(
                CheckId::SwitchoverGroups,
                "the target could not be dialled",
                now,
            ));
            result.checks = checks;
            return Emission::of(result);
        }
        Ok(t) => t,
    };
    let groups = selected(req, &ev.mapping);
    let topics: Vec<String> = groups
        .iter()
        .filter_map(|g| ev.mapping.groups.get(&g.source))
        .flat_map(|g| g.entries.iter().filter_map(|e| e.target_topic.clone()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let created_ids = crate::switchover::created_topic_ids(&ev.scorecard_bytes);
    match crate::switchover::target_is_the_restores(
        target.as_ref(),
        &ev.scorecard.target.cluster_id,
        &created_ids,
        &topics,
    ) {
        Ok((cluster, checked)) => checks.push(
            ready(
                CheckId::SwitchoverTarget,
                CheckCode::TargetIsTheRestores,
                now,
            )
            .with_message(&format!(
                "the target answers the restore's cluster id and {} restored topic(s) carry \
                     the IDs recorded at creation",
                checked.len()
            ))
            .with_fact("clusterId", &cluster),
        ),
        Err((why, _, _)) => {
            checks.push(
                catalogue::outcome(
                    CheckId::SwitchoverTarget,
                    CheckState::NotReady,
                    CheckCode::TargetNotTheRestores,
                    now,
                )
                .with_message(&why)
                .with_remedy(TARGET_REMEDY),
            );
            checks.push(blocked(
                CheckId::SwitchoverGroups,
                "the target is not the restore's, so no group is read",
                now,
            ));
            result.checks = checks;
            return Emission::of(result);
        }
    }

    // 3. Every requested group.
    let readings: Vec<GroupReading> = groups
        .iter()
        .map(|g| read_group(target.as_ref(), &ev.mapping, g))
        .collect();
    checks.push(groups_row(&readings, now));
    result.checks = checks;
    let mut emission = Emission::of(result);
    emission.extra.push((
        Stream::Details,
        super::restore::details_stream(&detail_lines(&readings)),
    ));
    emission
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switchover::target::CommitFailure;
    use logweir_core::check_contract::{
        ConnectionPlan, CredentialMode, DestinationPlan, SwitchoverReadinessGroup,
    };
    use logweir_core::positions_mapping::{GroupMapping, MappingEntry, UNAVAILABLE};
    use std::cell::RefCell;

    fn entry(topic: &str, p: u32, class: &str, offset: Option<i64>) -> MappingEntry {
        MappingEntry {
            topic: topic.into(),
            partition: p,
            position: offset.map(|o| o + 5),
            target_topic: Some(format!("r-{topic}")),
            class: class.into(),
            target_offset: offset,
            skipped: (class == APPROXIMATE).then_some(2),
            reason: match class {
                APPROXIMATE => Some(logweir_core::positions_mapping::SELECTION_EXCLUDED.into()),
                UNAVAILABLE => Some("ArchiveGap".into()),
                _ => None,
            },
        }
    }

    fn group(entries: Vec<MappingEntry>, reason: Option<&str>) -> GroupMapping {
        GroupMapping {
            group_type: Some("classic".into()),
            active_at_capture: false,
            reason: reason.map(str::to_string),
            entries,
            no_committed_position: 0,
        }
    }

    /// `app` maps two partitions (one exact, one approximate) and has one
    /// unavailable; `gone` was not captured; `dark` maps nothing.
    fn mapping() -> MappingDocument {
        MappingDocument {
            format_version: "1.0.0".into(),
            run_id: "run-1".into(),
            point_id: format!("lwp1-{}", "a".repeat(32)),
            positions_sha256: format!("sha256:{}", "b".repeat(64)),
            rule: logweir_core::positions_mapping::RULE.into(),
            groups: [
                (
                    "app".to_string(),
                    group(
                        vec![
                            entry("orders", 0, EXACT, Some(40)),
                            entry("orders", 1, APPROXIMATE, Some(12)),
                            entry("orders", 2, UNAVAILABLE, None),
                        ],
                        None,
                    ),
                ),
                (
                    "gone".to_string(),
                    group(Vec::new(), Some("NotCaptured: GroupNotFound")),
                ),
                (
                    "dark".to_string(),
                    group(vec![entry("orders", 0, UNAVAILABLE, None)], None),
                ),
            ]
            .into(),
        }
    }

    /// A target whose groups and positions are tables. `commit` panics: the
    /// review never commits.
    #[derive(Default)]
    struct FakeTarget {
        groups: BTreeMap<String, so::TargetGroup>,
        committed: BTreeMap<String, Result<BTreeMap<Part, Option<i64>>, ReadFailure>>,
        listed: RefCell<Vec<(String, bool)>>,
    }

    impl SwitchoverTarget for FakeTarget {
        fn cluster_id(&self) -> Result<String, String> {
            Ok("cluster-t".into())
        }
        fn topic_ids(
            &self,
            topics: &[String],
        ) -> Result<BTreeMap<String, Result<String, String>>, String> {
            Ok(topics
                .iter()
                .map(|t| (t.clone(), Ok(format!("id-{t}"))))
                .collect())
        }
        fn marks(&self, _: &[String]) -> BTreeMap<Part, (i64, i64)> {
            BTreeMap::new()
        }
        fn classify(&self, group: &str) -> so::TargetGroup {
            self.groups
                .get(group)
                .cloned()
                .unwrap_or(so::TargetGroup::Absent)
        }
        fn committed(
            &self,
            group: &str,
            partitions: &[Part],
            listed: bool,
        ) -> Result<BTreeMap<Part, Option<i64>>, ReadFailure> {
            self.listed.borrow_mut().push((group.to_string(), listed));
            self.committed
                .get(group)
                .cloned()
                .unwrap_or_else(|| Ok(partitions.iter().map(|p| (p.clone(), None)).collect()))
        }
        fn commit(&self, _: &str, _: &[(Part, i64)]) -> Result<(), CommitFailure> {
            panic!("a readiness check never commits");
        }
    }

    fn consumer(state: &str, members: u32) -> so::TargetGroup {
        so::TargetGroup::Consumer {
            group_type: "classic".into(),
            state: state.into(),
            members,
        }
    }

    fn sel(source: &str, target: &str) -> Selected {
        Selected {
            source: source.into(),
            target: target.into(),
        }
    }

    fn word(r: &GroupReading) -> Option<&str> {
        r.refusal.as_ref().map(|(w, _)| w.as_str())
    }

    /// **The groups read are the request's, or every group the mapping maps
    /// a position of**; a requested target defaults to the source id.
    /// KILLS: a default selection that includes an uncaptured group (the row
    /// would never be ready), or one that drops a mapped group.
    #[test]
    fn the_selection_is_the_requests_or_every_mapped_group() {
        let m = mapping();
        let mut req = request();
        assert_eq!(selected(&req, &m), vec![sel("app", "app")]);
        req.groups = vec![
            SwitchoverReadinessGroup {
                source: "app".into(),
                target: Some("app-v2".into()),
            },
            SwitchoverReadinessGroup {
                source: "gone".into(),
                target: None,
            },
        ];
        assert_eq!(
            selected(&req, &m),
            vec![sel("app", "app-v2"), sel("gone", "gone")]
        );
    }

    /// **§3.5 per group, then its committed positions**: an absent or
    /// stopped group is admissible (an absent one read as not listed); a
    /// group with members, of another type, unproven or invisible is not, by
    /// the switchover's own word; positions already the mapped ones are
    /// admissible (already applied), any other committed position is
    /// `TargetPositionsChanged`; a group the mapping does not name or maps
    /// nothing of is refused by name.
    /// KILLS: a review that admits a group with members, ignores a committed
    /// position that differs, or reads an absent group's offsets as listed.
    #[test]
    fn each_group_is_admitted_by_the_switchovers_own_rules() {
        let m = mapping();
        let mut t = FakeTarget::default();
        t.groups.insert("stopped".into(), consumer("Empty", 0));
        t.groups.insert("busy".into(), consumer("Stable", 2));
        t.groups
            .insert("streams".into(), so::TargetGroup::OtherType);
        t.groups.insert("old".into(), so::TargetGroup::TypeUnproven);
        t.groups
            .insert("hidden".into(), so::TargetGroup::NotVisible);
        for g in ["done", "moved", "locked"] {
            t.groups.insert(g.into(), consumer("Empty", 0));
        }
        let part = |p: u32| ("r-orders".to_string(), p);
        t.committed.insert(
            "done".into(),
            Ok([(part(0), Some(40)), (part(1), Some(12))].into()),
        );
        t.committed.insert(
            "moved".into(),
            Ok([(part(0), Some(41)), (part(1), None)].into()),
        );
        t.committed
            .insert("locked".into(), Err(ReadFailure::NotAuthorized));

        let absent = read_group(&t, &m, &sel("app", "fresh"));
        assert_eq!(word(&absent), None);
        assert!(absent.absent);
        let stopped = read_group(&t, &m, &sel("app", "stopped"));
        assert_eq!(word(&stopped), None);
        assert_eq!(
            stopped.classified,
            Some(("classic".into(), "Empty".into(), 0))
        );
        assert_eq!(
            *t.listed.borrow(),
            vec![("fresh".to_string(), false), ("stopped".to_string(), true)]
        );
        assert_eq!(
            word(&read_group(&t, &m, &sel("app", "busy"))),
            Some(so::GROUP_ACTIVE)
        );
        assert_eq!(
            word(&read_group(&t, &m, &sel("app", "streams"))),
            Some(so::TARGET_GROUP_TYPE_UNSUPPORTED)
        );
        assert_eq!(
            word(&read_group(&t, &m, &sel("app", "old"))),
            Some(so::TARGET_GROUP_TYPE_UNPROVEN)
        );
        assert_eq!(
            word(&read_group(&t, &m, &sel("app", "hidden"))),
            Some(so::TARGET_NOT_VISIBLE)
        );
        let done = read_group(&t, &m, &sel("app", "done"));
        assert_eq!(word(&done), None, "already the mapped positions");
        assert_eq!(done.topics["r-orders"][0].committed, Some(40));
        assert_eq!(
            word(&read_group(&t, &m, &sel("app", "moved"))),
            Some(so::TARGET_POSITIONS_CHANGED)
        );
        assert_eq!(
            word(&read_group(&t, &m, &sel("app", "locked"))),
            Some(so::NOT_AUTHORIZED)
        );
        assert_eq!(
            word(&read_group(&t, &m, &sel("nobody", "nobody"))),
            Some("NotInMapping")
        );
        assert_eq!(
            word(&read_group(&t, &m, &sel("gone", "gone"))),
            Some("NothingMapped")
        );
        assert_eq!(
            word(&read_group(&t, &m, &sel("dark", "dark"))),
            Some("NothingMapped")
        );
    }

    /// **The row and the details**: ready names the counts by class; one
    /// refused group makes it `notReady` naming that group and its word; the
    /// details list every group and partition — mapped offset, class,
    /// committed position, and an unavailable entry's reason.
    /// KILLS: a details stream that is a sample, or a row ready over a
    /// refused group.
    #[test]
    fn the_row_counts_by_class_and_the_details_list_every_position() {
        let m = mapping();
        let mut t = FakeTarget::default();
        t.groups.insert("busy".into(), consumer("Stable", 3));
        let now = chrono::Utc::now();
        let ok = vec![read_group(&t, &m, &sel("app", "app"))];
        let row = groups_row(&ok, now);
        assert_eq!(row.state, CheckState::Ready);
        assert_eq!(row.code, CheckCode::GroupsAdmissible);
        assert!(
            row.message
                .contains("1 group(s), 3 position(s): 1 exact, 1 approximate, 1 unavailable"),
            "{}",
            row.message
        );
        assert_eq!(row.facts.get("exact").map(String::as_str), Some("1"));
        let lines = detail_lines(&ok);
        assert_eq!(lines.len(), 1);
        let line: serde_json::Value = serde_json::from_str(&lines[0]).expect("json");
        assert_eq!(line["topic"], "r-orders");
        assert_eq!(line["admissible"], true);
        assert_eq!(
            line["positions"],
            serde_json::json!([
                {"partition": 0, "class": "exact", "committed": null, "offset": 40},
                {"partition": 1, "class": "approximate", "committed": null, "offset": 12, "reason": logweir_core::positions_mapping::SELECTION_EXCLUDED},
                {"partition": 2, "class": "unavailable", "committed": null, "reason": "ArchiveGap"}
            ])
        );
        let mixed = vec![
            read_group(&t, &m, &sel("app", "app")),
            read_group(&t, &m, &sel("app", "busy")),
        ];
        let row = groups_row(&mixed, now);
        assert_eq!(row.state, CheckState::NotReady);
        assert_eq!(row.code, CheckCode::GroupNotAdmissible);
        assert!(
            row.message.starts_with("GroupActive: busy") && row.message.contains("1 of 2"),
            "{}",
            row.message
        );
        let busy: serde_json::Value = serde_json::from_str(&detail_lines(&mixed)[1]).expect("json");
        assert_eq!(busy["reason"], "GroupActive");
        assert_eq!(busy["members"], 3);
        assert_eq!(groups_row(&[], now).state, CheckState::NotReady);
    }

    // ---- the whole kind over a wiring -------------------------------------

    fn request() -> SwitchoverReadinessRequest {
        SwitchoverReadinessRequest {
            restore_name: "incident".into(),
            restore_uid: "r-uid".into(),
            restore_plan_sha256: format!("sha256:{}", "c".repeat(64)),
            scorecard_key: "logweir/drills/run-1.json".into(),
            target: ConnectionPlan {
                bootstrap_servers: vec!["kafka:9092".into()],
                auth_mode: "plaintext".into(),
                username: None,
                password_env: None,
                tls: None,
                ca_file: None,
                client_cert_file: None,
                client_key_file: None,
                principal: "User:ANONYMOUS".into(),
            },
            evidence: DestinationPlan {
                name: "evidence".into(),
                uid: "d-uid".into(),
                location: logweir_core::destination::DestinationLocation {
                    provider: logweir_core::destination::StorageProvider::S3,
                    bucket: "evidence".into(),
                    prefix: "team-a".into(),
                    region: None,
                    endpoint: None,
                    addressing: logweir_core::destination::Addressing::PathStyle,
                    transport: logweir_core::destination::TransportSecurity::Tls,
                },
                location_digest: format!("sha256:{}", "d".repeat(64)),
                ca_file: None,
                credentials: CredentialMode::Ambient,
                grant_bindings: Vec::new(),
            },
            keyring: logweir_core::execution_contract::EvidenceKeyring {
                format_version: "1.0.0".into(),
                keys: Vec::new(),
            },
            groups: Vec::new(),
        }
    }
}
