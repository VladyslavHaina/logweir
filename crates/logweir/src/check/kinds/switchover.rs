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
//! * **`switchover.groups`** — each requested group goes through the run's
//!   OWN decision, [`decide_group`] (steps 1 to 4 of `switchover run`), over
//!   the plan group `logweir switchover plan` would write: §3.5's admission,
//!   every mapped offset inside the target's log start and high watermark
//!   ([`so::check_bounds`]), the priors and the compare-and-set
//!   ([`so::compare`]). Admissible exactly when the run would commit or find
//!   the group already applied; otherwise the run's own reason. Every group,
//!   topic and partition goes to the details stream, one line per group and
//!   restored topic, with the mapped offset, its class and the committed
//!   position read (`GET …/preflights/{id}/details`).
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
use crate::switchover::apply::{decide_group, Verdict};
use crate::switchover::target::{Part, SwitchoverTarget};

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
    /// Whether every planned partition already holds its mapped offset (the
    /// run would find it `alreadyApplied` and commit nothing).
    pub already_applied: bool,
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

/// **One group, read — by the Job's own decision.** The group is planned
/// exactly as `logweir switchover plan` plans it ([`so::plan_from_mapping`]:
/// every mapped entry, `expected: none`), and that plan group goes through
/// [`decide_group`], steps 1 to 4 of `switchover run`: §3.5's admission,
/// every planned offset inside the target's `marks` ([`so::check_bounds`]),
/// the priors and the compare-and-set ([`so::compare`]). Admissible exactly
/// when the run would commit or find it already applied; otherwise the run's
/// own reason. Nothing is committed. A group the mapping does not name, or
/// maps nothing of, is refused before that by name.
#[must_use]
pub fn read_group(
    target: &dyn SwitchoverTarget,
    mapping: &MappingDocument,
    restore: &so::RestoreRef,
    marks: &BTreeMap<Part, (i64, i64)>,
    group: &Selected,
) -> GroupReading {
    let mut reading = GroupReading {
        group: group.clone(),
        refusal: None,
        classified: None,
        absent: false,
        already_applied: false,
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
    for e in &mapped.entries {
        let Some(topic) = e.target_topic.clone() else {
            continue;
        };
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
    if !mapped.entries.iter().any(|e| e.mapped()) {
        reading.refusal = Some((
            "NothingMapped".to_string(),
            match mapped.reason.as_deref() {
                Some(r) => format!("the group was not captured ({r})"),
                None => "no position of the group maps onto a restored partition".to_string(),
            },
        ));
        return reading;
    }
    let choice = so::GroupChoice {
        source: group.source.clone(),
        target: group.target.clone(),
    };
    let plan = match so::plan_from_mapping(mapping, restore.clone(), &[choice]) {
        Ok((plan, _)) => plan,
        Err(why) => {
            reading.refusal = Some((so::PLAN_INVALID.to_string(), why));
            return reading;
        }
    };
    let Some(plan_group) = plan.groups.first() else {
        reading.refusal = Some((
            "NothingMapped".to_string(),
            "no position of the group maps onto a restored partition".to_string(),
        ));
        return reading;
    };
    let decided = decide_group(target, plan_group, marks);
    if let so::TargetGroup::Consumer {
        group_type,
        state,
        members,
    } = &decided.classified
    {
        reading.classified = Some((group_type.clone(), state.clone(), *members));
    }
    reading.absent = decided.classified == so::TargetGroup::Absent;
    for p in &decided.parts {
        if let Some(entry) = reading
            .topics
            .get_mut(&p.topic)
            .and_then(|v| v.iter_mut().find(|e| e.partition == p.partition))
        {
            entry.committed = p.current;
        }
    }
    match decided.verdict {
        Verdict::Commit { .. } => {}
        Verdict::AlreadyApplied => reading.already_applied = true,
        Verdict::Stop(record) => {
            let reason = record.reason.unwrap_or_else(|| record.outcome.clone());
            reading.refusal = Some(match reason.split_once(": ") {
                Some((word, why)) => (word.to_string(), why.to_string()),
                None => (reason, decided.words),
            });
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
                "alreadyApplied": r.already_applied,
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

    // 1. The restore's evidence, as the switchover's step 3 reads it, with
    // a store's refusal kept apart from the verdict (D1).
    let grant = format!("evidenceRead of BackupDestination {}", req.evidence.name);
    let evidence = match wiring
        .objects(
            &req.evidence,
            DestinationRole::EvidenceRead,
            deadline.remaining(),
        )
        .map(|handle| crate::evidence_read::NotingHandle::new(handle, &grant))
    {
        Err(f) => {
            checks.push(from_store_failure(CheckId::SwitchoverEvidence, &f, now));
            None
        }
        Ok(handle) => {
            let read = |key: &str, max_bytes: u64| handle.get(key, max_bytes);
            match crate::switchover::restore_evidence(
                &read,
                &req.keyring,
                &req.scorecard_key,
                &req.restore_plan_sha256,
                None,
                now,
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
                // D1: a read the store refused is the store's answer, in its
                // own code, never `RestoreEvidenceRefused`.
                Err(why) => {
                    checks.push(match handle.unreadable() {
                        Some(failure) => from_store_failure(
                            CheckId::SwitchoverEvidence,
                            &crate::check::store::StoreFailure::new(failure.code, failure.sentence),
                            now,
                        )
                        .with_scope(scope.clone()),
                        None => catalogue::outcome(
                            CheckId::SwitchoverEvidence,
                            CheckState::NotReady,
                            CheckCode::RestoreEvidenceRefused,
                            now,
                        )
                        .with_message(&why)
                        .with_remedy(EVIDENCE_REMEDY)
                        .with_scope(scope.clone()),
                    });
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
    let created_ids = crate::switchover::created_topic_ids(&ev.scorecard);
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

    // 3. Every requested group, by the run's own decision over the plan
    //    `logweir switchover plan` would write, against the marks read once
    //    for every restored topic (as the run reads them before any group).
    let restore = so::RestoreRef {
        name: req.restore_name.clone(),
        uid: req.restore_uid.clone(),
        plan_hash: req.restore_plan_sha256.clone(),
        scorecard_sha256: ev.scorecard_sha256.clone(),
        mapping_sha256: ev.mapping_sha256.clone(),
    };
    let marks = target.marks(&topics);
    let readings: Vec<GroupReading> = groups
        .iter()
        .map(|g| read_group(target.as_ref(), &ev.mapping, &restore, &marks, g))
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
    use crate::switchover::target::{CommitFailure, ReadFailure};
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

    fn restore_ref() -> so::RestoreRef {
        let sha = |c: char| format!("sha256:{}", c.to_string().repeat(64));
        so::RestoreRef {
            name: "incident".into(),
            uid: "r-uid".into(),
            plan_hash: sha('a'),
            scorecard_sha256: sha('b'),
            mapping_sha256: sha('c'),
        }
    }

    /// `r-orders` partitions 0 and 1 at `[0, 100]`.
    fn wide_marks() -> BTreeMap<Part, (i64, i64)> {
        [
            (("r-orders".to_string(), 0), (0, 100)),
            (("r-orders".to_string(), 1), (0, 100)),
        ]
        .into()
    }

    fn rg(t: &FakeTarget, m: &MappingDocument, g: &Selected) -> GroupReading {
        read_group(t, m, &restore_ref(), &wide_marks(), g)
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

        let absent = rg(&t, &m, &sel("app", "fresh"));
        assert_eq!(word(&absent), None);
        assert!(absent.absent);
        let stopped = rg(&t, &m, &sel("app", "stopped"));
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
            word(&rg(&t, &m, &sel("app", "busy"))),
            Some(so::GROUP_ACTIVE)
        );
        assert_eq!(
            word(&rg(&t, &m, &sel("app", "streams"))),
            Some(so::TARGET_GROUP_TYPE_UNSUPPORTED)
        );
        assert_eq!(
            word(&rg(&t, &m, &sel("app", "old"))),
            Some(so::TARGET_GROUP_TYPE_UNPROVEN)
        );
        assert_eq!(
            word(&rg(&t, &m, &sel("app", "hidden"))),
            Some(so::TARGET_NOT_VISIBLE)
        );
        let done = rg(&t, &m, &sel("app", "done"));
        assert_eq!(word(&done), None, "already the mapped positions");
        assert_eq!(done.topics["r-orders"][0].committed, Some(40));
        assert_eq!(
            word(&rg(&t, &m, &sel("app", "moved"))),
            Some(so::TARGET_POSITIONS_CHANGED)
        );
        assert_eq!(
            word(&rg(&t, &m, &sel("app", "locked"))),
            Some(so::NOT_AUTHORIZED)
        );
        assert_eq!(
            word(&rg(&t, &m, &sel("nobody", "nobody"))),
            Some("NotInMapping")
        );
        assert_eq!(
            word(&rg(&t, &m, &sel("gone", "gone"))),
            Some("NothingMapped")
        );
        assert_eq!(
            word(&rg(&t, &m, &sel("dark", "dark"))),
            Some("NothingMapped")
        );
    }

    /// **Review M1: the review decides as the run decides.** The marks
    /// (`so::check_bounds`): a mapped offset below the log start (retention
    /// moved it, design §2.3) is `BelowTargetLogStart`, one above the high
    /// watermark `AboveTargetEnd`, a partition with no marks
    /// `TargetMarksUnreadable`; the compare-and-set (`so::compare`): one
    /// partition at its mapped offset and another without a position is
    /// `TargetPositionsChanged`, never admissible. Controls: the same groups
    /// with the marks around the offsets, and with no position anywhere, are
    /// admissible; every partition already at its offset is admissible and
    /// already applied. And each refusal is the run's own: `apply_group` over
    /// the same plan group and marks refuses with the same word.
    /// KILLS: a review without the bounds, or with a looser compare than the
    /// run's (the shapes the review named in M1).
    #[test]
    #[allow(clippy::type_complexity)]
    fn the_review_decides_as_the_run_decides() {
        let m = mapping();
        let mut t = FakeTarget::default();
        for g in ["app", "mixed", "done"] {
            t.groups.insert(g.into(), consumer("Empty", 0));
        }
        let part = |p: u32| ("r-orders".to_string(), p);
        t.committed.insert(
            "mixed".into(),
            Ok([(part(0), Some(40)), (part(1), None)].into()),
        );
        t.committed.insert(
            "done".into(),
            Ok([(part(0), Some(40)), (part(1), Some(12))].into()),
        );
        let marks = |p0: (i64, i64), p1: Option<(i64, i64)>| {
            let mut out: BTreeMap<Part, (i64, i64)> = [(part(0), p0)].into();
            if let Some(p1) = p1 {
                out.insert(part(1), p1);
            }
            out
        };
        let at = |g: &str, marks: &BTreeMap<Part, (i64, i64)>| {
            read_group(&t, &m, &restore_ref(), marks, &sel("app", g))
        };
        let cases: [(&str, &str, BTreeMap<Part, (i64, i64)>, Option<&str>); 6] = [
            (
                "retention moved the log start",
                "app",
                marks((41, 100), Some((0, 100))),
                Some(so::BELOW_TARGET_LOG_START),
            ),
            (
                "past the end",
                "app",
                marks((0, 100), Some((0, 11))),
                Some(so::ABOVE_TARGET_END),
            ),
            (
                "marks not read",
                "app",
                marks((0, 100), None),
                Some(so::TARGET_MARKS_UNREADABLE),
            ),
            (
                "mixed: applied and absent",
                "mixed",
                wide_marks(),
                Some(so::TARGET_POSITIONS_CHANGED),
            ),
            (
                "control: inside the marks",
                "app",
                marks((40, 40), Some((12, 12))),
                None,
            ),
            ("control: already applied", "done", wide_marks(), None),
        ];
        for (name, group, marks, want) in cases {
            let r = at(group, &marks);
            assert_eq!(word(&r), want, "{name}: {:?}", r.refusal);
            if want.is_none() {
                assert_eq!(r.already_applied, group == "done", "{name}");
                continue;
            }
            // The run's own answer over the same plan group and marks.
            let (plan, _) = so::plan_from_mapping(
                &m,
                restore_ref(),
                &[so::GroupChoice {
                    source: "app".into(),
                    target: group.into(),
                }],
            )
            .expect("a plan");
            let run = crate::switchover::apply::apply_group(&t, &plan.groups[0], &marks);
            assert_eq!(run.outcome, so::REFUSED, "{name}");
            assert!(
                run.reason
                    .as_deref()
                    .is_some_and(|w| w.starts_with(want.unwrap_or_default())),
                "{name}: the run says {:?}",
                run.reason
            );
        }
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
        let ok = vec![rg(&t, &m, &sel("app", "app"))];
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
                {"partition": 0, "class": "exact", "committed": null, "offset": 40}, // engine-token-ok: a JSON field of the review's details line, not an engine subcommand
                {"partition": 1, "class": "approximate", "committed": null, "offset": 12, "reason": logweir_core::positions_mapping::SELECTION_EXCLUDED}, // engine-token-ok: a JSON field of the review's details line, not an engine subcommand
                {"partition": 2, "class": "unavailable", "committed": null, "reason": "ArchiveGap"}
            ])
        );
        let mixed = vec![
            rg(&t, &m, &sel("app", "app")),
            rg(&t, &m, &sel("app", "busy")),
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

    // ---- D1: a store's refusal is never the evidence's verdict ----------

    // The evidence double lives outside `check/` (`evidence_read::test_support`):
    // `check_cli.rs` holds this tree to ONE module naming the one write.
    use crate::evidence_read::test_support::Answering;

    /// The wiring of a review whose evidence handle answers every read with
    /// `answer`; the target is never reached.
    struct EvidenceOnly(fn(&str) -> logweir_engine_oso::storage::StoreError);

    impl Wiring for EvidenceOnly {
        fn broker(
            &self,
            _plan: &logweir_core::check_contract::ConnectionPlan,
            _budget: std::time::Duration,
        ) -> Result<
            Box<dyn logweir_kafka::inventory::InventoryProbe>,
            logweir_kafka::inventory::CheckFailure,
        > {
            unreachable!("the review reads no inventory")
        }
        fn objects(
            &self,
            _plan: &DestinationPlan,
            role: DestinationRole,
            _budget: std::time::Duration,
        ) -> Result<Box<dyn crate::check::store::ObjectAccess>, crate::check::store::StoreFailure>
        {
            assert_eq!(role, DestinationRole::EvidenceRead, "the read grant");
            Ok(Box::new(Answering(self.0)))
        }
        fn evidence_writer(
            &self,
            _plan: &DestinationPlan,
            _grant: Option<&logweir_core::check_contract::GrantRef>,
            _budget: std::time::Duration,
        ) -> Result<Box<dyn crate::check::store::ObjectAccess>, crate::check::store::StoreFailure>
        {
            unreachable!("the review writes nothing")
        }
        fn evidence_reader(
            &self,
            _plan: &DestinationPlan,
            _grant: Option<&logweir_core::check_contract::GrantRef>,
            _budget: std::time::Duration,
        ) -> Result<Box<dyn crate::check::store::ObjectAccess>, crate::check::store::StoreFailure>
        {
            unreachable!("the review reads through `objects`")
        }
        fn signer_key_id(&self, _path: &str) -> Result<String, String> {
            unreachable!("the review signs nothing")
        }
        fn read_bytes(&self, path: &str) -> std::io::Result<Vec<u8>> {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                path.to_string(),
            ))
        }
        fn now(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::DateTime::parse_from_rfc3339("2026-10-13T12:00:00Z")
                .expect("an instant")
                .with_timezone(&chrono::Utc)
        }
    }

    fn evidence_row(
        answer: fn(&str) -> logweir_engine_oso::storage::StoreError,
    ) -> logweir_core::check_contract::CheckOutcome {
        run(&request(), &EvidenceOnly(answer), Deadline::new(60))
            .result
            .checks
            .into_iter()
            .find(|c| c.id == CheckId::SwitchoverEvidence)
            .expect("the evidence row")
    }

    /// **A 403 reading the restore's scorecard is the store's answer, in its
    /// own code, naming the key and the read grant; it is never
    /// `RestoreEvidenceRefused`** (the PoC's switchover named the same 403
    /// `MappingDigestMismatch`). CONTROL: an ABSENT scorecard is still the
    /// evidence's refusal.
    ///
    /// KILLS: the `Err(why)` arm ignoring the handle's note (the 403 is
    /// `RestoreEvidenceRefused`); `NotingHandle::get` not noting.
    #[test]
    fn a_denied_evidence_read_is_the_stores_answer_not_a_refusal() {
        let denied = evidence_row(|key| {
            logweir_engine_oso::storage::StoreError::Io(format!(
                "{key}: Error performing GET http://minio:9000/e/{key} in 2ms - Server returned \
                 non-2xx status code: 403 Forbidden: <Error><Code>AccessDenied</Code></Error>"
            ))
        });
        assert_eq!(denied.code, CheckCode::AccessDenied, "{denied:?}");
        assert!(
            denied.message.starts_with(
                "EvidenceUnreadable: could not read logweir/drills/run-1.json with grant \
                 evidenceRead of BackupDestination evidence: AccessDenied"
            ),
            "{}",
            denied.message
        );
        let absent =
            evidence_row(|key| logweir_engine_oso::storage::StoreError::NotFound(key.to_string()));
        assert_eq!(absent.code, CheckCode::RestoreEvidenceRefused);
        assert_eq!(absent.state, CheckState::NotReady);
    }
}
