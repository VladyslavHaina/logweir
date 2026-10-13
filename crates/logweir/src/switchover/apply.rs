//! **One plan group, applied** (design §3.4 step 3 and §3.5): classify the
//! target group, hold its planned offsets to the target's marks, read its
//! committed positions (the priors, before any commit), compare-and-set,
//! commit from a non-member, read back. Each group independently, so one
//! group's refusal never stops another's.

use super::target::{CommitFailure, Part, ReadFailure, SwitchoverTarget};
use logweir_core::switchover::{
    self as so, AppliedState, Decision, GroupRecord, PartitionRecord, PlanGroup, PlannedPartition,
    TargetGroup, UnknownWord,
};
use std::collections::BTreeMap;

/// How the classification reads in the record.
fn classification_words(t: &TargetGroup) -> String {
    match t {
        TargetGroup::Absent => "absent".to_string(),
        TargetGroup::Consumer {
            group_type,
            state,
            members,
        } => format!("{group_type} {state}, {members} member(s)"),
        TargetGroup::OtherType => "another group type (share, streams or a non-consumer \
                                   protocol)"
            .to_string(),
        TargetGroup::TypeUnproven => "type unproven (the broker cannot type groups)".to_string(),
        TargetGroup::NotVisible => "not visible to this principal".to_string(),
        TargetGroup::Unreachable(why) => format!("not classified: {why}"),
    }
}

/// The plan group's partitions, in plan order.
#[must_use]
pub fn planned(g: &PlanGroup) -> Vec<PlannedPartition> {
    let mut out = Vec::new();
    for (topic, t) in &g.topics {
        for (p, o) in &t.offsets {
            out.push(PlannedPartition {
                topic: topic.clone(),
                partition: *p,
                planned: *o,
                expected: t.expected.at(*p),
                current: None,
            });
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn record(
    g: &PlanGroup,
    classification: String,
    outcome: &str,
    reason: Option<String>,
    parts: &[PlannedPartition],
    priors: Option<&BTreeMap<Part, Option<i64>>>,
    readback: Option<&BTreeMap<Part, Option<i64>>>,
    applied_unknown: bool,
) -> GroupRecord {
    GroupRecord {
        source: g.source.clone(),
        target: g.target.clone(),
        classification,
        outcome: outcome.to_string(),
        reason,
        created: false,
        committed: false,
        partitions: parts
            .iter()
            .map(|p| {
                let key = (p.topic.clone(), p.partition);
                let back = readback.and_then(|r| r.get(&key).copied().flatten());
                PartitionRecord {
                    topic: p.topic.clone(),
                    partition: p.partition,
                    prior: priors.and_then(|r| r.get(&key).copied().flatten()),
                    planned: p.planned,
                    applied: match readback {
                        Some(_) => AppliedState::Known(back == Some(p.planned)),
                        None if applied_unknown => AppliedState::Unknown(UnknownWord::Unknown),
                        None => AppliedState::Known(false),
                    },
                    readback: back,
                }
            })
            .collect(),
    }
}

/// The reason words for a group whose partitions moved: the word, then each
/// partition with where it is and how many records past what the plan
/// expected (C15: "naming the 10").
fn changed_reason(changed: &[(String, u32, Option<i64>)], parts: &[PlannedPartition]) -> String {
    let what: Vec<String> = changed
        .iter()
        .map(|(t, p, current)| {
            let expected = parts
                .iter()
                .find(|x| &x.topic == t && x.partition == *p)
                .and_then(|x| x.expected);
            match (current, expected) {
                (Some(c), Some(e)) => format!(
                    "{t}:{p} is at {c}, {} record(s) from the expected {e}",
                    c - e
                ),
                (Some(c), None) => format!("{t}:{p} is at {c}, expected no committed position"),
                (None, Some(e)) => format!("{t}:{p} has no committed position, expected {e}"),
                (None, None) => format!("{t}:{p}"),
            }
        })
        .collect();
    format!("{}: {}", so::TARGET_POSITIONS_CHANGED, what.join("; "))
}

/// What steps 1 to 4 decided for one group, before any commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Every check held: commit (`creates`: the commit creates the group).
    Commit { creates: bool },
    /// Every planned partition already holds its planned offset: no commit.
    AlreadyApplied,
    /// Refused or failed before any commit; the record says why.
    Stop(Box<GroupRecord>),
}

/// **Steps 1 to 4 of one group, read-only**: the classification and §3.5's
/// admission, every planned offset inside the target's marks, the priors
/// (RequireStable) and the compare-and-set. `switchover run` commits on
/// [`Verdict::Commit`]; the `switchoverReadiness` review reports the same
/// verdict over the plan `logweir switchover plan` would write, and commits
/// nothing — ONE decision, so the review and the run cannot disagree.
#[derive(Debug, Clone)]
pub struct Decided {
    /// The classification.
    pub classified: TargetGroup,
    /// The classification, in the record's words.
    pub words: String,
    /// The planned partitions, each with its prior once read.
    pub parts: Vec<PlannedPartition>,
    /// The priors, once read.
    pub priors: Option<BTreeMap<Part, Option<i64>>>,
    pub verdict: Verdict,
}

/// See [`Decided`]. `marks` are the target partitions' `(log start, high
/// watermark)` read before any commit of the run.
#[must_use]
pub fn decide_group(
    target: &dyn SwitchoverTarget,
    g: &PlanGroup,
    marks: &BTreeMap<Part, (i64, i64)>,
) -> Decided {
    let mut parts = planned(g);
    let keys: Vec<Part> = parts
        .iter()
        .map(|p| (p.topic.clone(), p.partition))
        .collect();
    let classified = target.classify(&g.target);
    let words = classification_words(&classified);
    let stop =
        |record: GroupRecord, parts: Vec<PlannedPartition>, classified: TargetGroup| Decided {
            classified,
            words: words.clone(),
            parts,
            priors: None,
            verdict: Verdict::Stop(Box::new(record)),
        };
    let refused = |reason: String, parts: &[PlannedPartition]| {
        record(
            g,
            words.clone(),
            so::REFUSED,
            Some(reason),
            parts,
            None,
            None,
            false,
        )
    };
    // 1. The table: a live group, another type, an unproven type or a hidden
    //    group is refused before any position is read.
    let creates = match so::admit_group(&classified) {
        Ok(creates) => creates,
        Err(reason) if reason == so::COMMIT_FAILED => {
            let r = record(
                g,
                words.clone(),
                so::FAILED,
                Some(format!("{reason}: {words}")),
                &parts,
                None,
                None,
                false,
            );
            return stop(r, parts, classified);
        }
        Err(reason) => {
            let r = refused(reason.to_string(), &parts);
            return stop(r, parts, classified);
        }
    };
    // 2. Every planned offset inside the target partition now.
    if let Err(why) = so::check_bounds(&parts, marks) {
        let r = refused(why, &parts);
        return stop(r, parts, classified);
    }
    // 3. The priors, read BEFORE any commit (RequireStable).
    let priors = match target.committed(&g.target, &keys, !creates) {
        Ok(p) => p,
        Err(e) => {
            let reason = match e {
                ReadFailure::NotAuthorized => so::NOT_AUTHORIZED.to_string(),
                ReadFailure::NotVisible => so::TARGET_NOT_VISIBLE.to_string(),
                ReadFailure::Unstable => format!(
                    "{}: a transactional offset commit is pending on the group",
                    so::TARGET_POSITIONS_CHANGED
                ),
                ReadFailure::Other(why) => {
                    let r = record(
                        g,
                        words.clone(),
                        so::FAILED,
                        Some(format!(
                            "{}: the positions could not be read: {why}",
                            so::COMMIT_FAILED
                        )),
                        &parts,
                        None,
                        None,
                        false,
                    );
                    return stop(r, parts, classified);
                }
            };
            let r = refused(reason, &parts);
            return stop(r, parts, classified);
        }
    };
    for p in &mut parts {
        p.current = priors
            .get(&(p.topic.clone(), p.partition))
            .copied()
            .flatten();
    }
    // 4. The compare-and-set.
    let verdict = match so::compare(&parts) {
        Decision::AlreadyApplied => Verdict::AlreadyApplied,
        Decision::Changed(changed) => {
            let mut r = refused(changed_reason(&changed, &parts), &parts);
            for (rp, p) in r.partitions.iter_mut().zip(&parts) {
                rp.prior = p.current;
            }
            Verdict::Stop(Box::new(r))
        }
        Decision::Apply => Verdict::Commit { creates },
    };
    Decided {
        classified,
        words,
        parts,
        priors: Some(priors),
        verdict,
    }
}

/// **One plan group, applied.** `marks` are the target partitions' `(log
/// start, high watermark)` read before any commit of the run. Steps 1 to 4
/// are [`decide_group`]'s; 5 and 6 commit and read back.
pub fn apply_group(
    target: &dyn SwitchoverTarget,
    g: &PlanGroup,
    marks: &BTreeMap<Part, (i64, i64)>,
) -> GroupRecord {
    let Decided {
        words,
        parts,
        priors,
        verdict,
        ..
    } = decide_group(target, g, marks);
    let priors = priors.unwrap_or_default();
    let creates = match verdict {
        Verdict::Stop(r) => return *r,
        Verdict::AlreadyApplied => {
            return record(
                g,
                words,
                so::ALREADY_APPLIED,
                None,
                &parts,
                Some(&priors),
                Some(&priors),
                false,
            )
        }
        Verdict::Commit { creates } => creates,
    };
    let keys: Vec<Part> = parts
        .iter()
        .map(|p| (p.topic.clone(), p.partition))
        .collect();
    // 5. The commit, from a non-member: the broker refuses it for the whole
    //    request while the group has any member.
    let positions: Vec<(Part, i64)> = parts
        .iter()
        .map(|p| ((p.topic.clone(), p.partition), p.planned))
        .collect();
    let committed = target.commit(&g.target, &positions);
    // 6. Read back, whatever the commit answered.
    let readback = target.committed(&g.target, &keys, true).ok();
    let mut r = match committed {
        Ok(()) => {
            let all = readback.as_ref().is_some_and(|b| {
                parts.iter().all(|p| {
                    b.get(&(p.topic.clone(), p.partition)).copied().flatten() == Some(p.planned)
                })
            });
            let (outcome, reason) = if all {
                (so::APPLIED, None)
            } else {
                (
                    so::FAILED,
                    Some(format!(
                        "{}: the commit was accepted and the readback does not show every \
                         planned position",
                        so::COMMIT_FAILED
                    )),
                )
            };
            let mut r = record(
                g,
                words.clone(),
                outcome,
                reason,
                &parts,
                Some(&priors),
                readback.as_ref(),
                readback.is_none(),
            );
            r.created = creates && outcome == so::APPLIED;
            r
        }
        Err(failure) => {
            let (outcome, reason, unknown) = match failure {
                CommitFailure::GroupActive => (
                    so::REFUSED,
                    format!(
                        "{}: a member joined after the check; the broker refused the commit",
                        so::GROUP_ACTIVE
                    ),
                    false,
                ),
                CommitFailure::NotAuthorized => (so::REFUSED, so::NOT_AUTHORIZED.into(), false),
                CommitFailure::NotAConsumerGroup => {
                    (so::REFUSED, so::TARGET_GROUP_TYPE_UNSUPPORTED.into(), false)
                }
                CommitFailure::NotVisible => (so::REFUSED, so::TARGET_NOT_VISIBLE.into(), false),
                CommitFailure::Failed {
                    may_have_applied,
                    message,
                } => (
                    so::FAILED,
                    format!("{}: {message}", so::COMMIT_FAILED),
                    may_have_applied,
                ),
            };
            record(
                g,
                words.clone(),
                outcome,
                Some(reason),
                &parts,
                Some(&priors),
                readback.as_ref(),
                unknown,
            )
        }
    };
    r.committed = true;
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_core::switchover::{Expected, PlanTopic};
    use std::cell::RefCell;

    /// One group of the fake: its classification and committed positions.
    pub(crate) type FakeGroup = (TargetGroup, BTreeMap<Part, i64>);

    /// A target cluster in memory: groups with members and committed
    /// positions, and a count of commits sent.
    #[derive(Default)]
    pub(crate) struct Fake {
        pub groups: RefCell<BTreeMap<String, FakeGroup>>,
        pub commits: RefCell<u32>,
        /// A member that joins between the check and the commit.
        pub joins_at_commit: RefCell<Option<String>>,
    }

    impl SwitchoverTarget for Fake {
        fn cluster_id(&self) -> Result<String, String> {
            Ok("C".into())
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
        fn classify(&self, group: &str) -> TargetGroup {
            self.groups
                .borrow()
                .get(group)
                .map_or(TargetGroup::Absent, |(t, _)| t.clone())
        }
        fn committed(
            &self,
            group: &str,
            partitions: &[Part],
            _listed: bool,
        ) -> Result<BTreeMap<Part, Option<i64>>, ReadFailure> {
            let groups = self.groups.borrow();
            Ok(partitions
                .iter()
                .map(|p| {
                    (
                        p.clone(),
                        groups.get(group).and_then(|(_, c)| c.get(p).copied()),
                    )
                })
                .collect())
        }
        fn commit(&self, group: &str, positions: &[(Part, i64)]) -> Result<(), CommitFailure> {
            *self.commits.borrow_mut() += 1;
            if self.joins_at_commit.borrow().as_deref() == Some(group) {
                return Err(CommitFailure::GroupActive);
            }
            let mut groups = self.groups.borrow_mut();
            let entry = groups.entry(group.to_string()).or_insert_with(|| {
                (
                    TargetGroup::Consumer {
                        group_type: "classic".into(),
                        state: "Empty".into(),
                        members: 0,
                    },
                    BTreeMap::new(),
                )
            });
            for (p, o) in positions {
                entry.1.insert(p.clone(), *o);
            }
            Ok(())
        }
    }

    fn empty() -> TargetGroup {
        TargetGroup::Consumer {
            group_type: "classic".into(),
            state: "Empty".into(),
            members: 0,
        }
    }

    fn group(name: &str, offsets: &[(u32, i64)]) -> PlanGroup {
        PlanGroup {
            source: name.into(),
            target: name.into(),
            topics: [(
                "r-orders".to_string(),
                PlanTopic {
                    offsets: offsets.iter().copied().collect(),
                    approximate: vec![],
                    expected: Expected::none(),
                },
            )]
            .into(),
        }
    }

    fn marks() -> BTreeMap<Part, (i64, i64)> {
        (0..3)
            .map(|p| (("r-orders".to_string(), p), (0, 100)))
            .collect()
    }

    /// C4's unit half: two groups applied and one live group refused; priors
    /// are recorded before any commit; the same plan again is alreadyApplied
    /// for the two and sends no commit for them. Mutants killed: a compare
    /// skipped (the second run commits twice), a prior read skipped (no prior
    /// in the record), a live group not refused.
    #[test]
    fn a_plan_applies_independently_and_a_rerun_is_already_applied() {
        let fake = Fake::default();
        let mut live: BTreeMap<Part, i64> = BTreeMap::new();
        live.insert(("r-orders".into(), 0), 7);
        fake.groups
            .borrow_mut()
            .insert("a".into(), (empty(), BTreeMap::new()));
        fake.groups.borrow_mut().insert(
            "b".into(),
            (
                TargetGroup::Consumer {
                    group_type: "classic".into(),
                    state: "Stable".into(),
                    members: 1,
                },
                live,
            ),
        );
        let plan = [
            group("a", &[(0, 40), (1, 50)]),
            group("b", &[(0, 40)]),
            group("c", &[(2, 5)]),
        ];
        let first: Vec<GroupRecord> = plan
            .iter()
            .map(|g| apply_group(&fake, g, &marks()))
            .collect();
        let outcomes: Vec<&str> = first.iter().map(|r| r.outcome.as_str()).collect();
        assert_eq!(outcomes, [so::APPLIED, so::REFUSED, so::APPLIED]);
        assert_eq!(first[1].reason.as_deref(), Some(so::GROUP_ACTIVE));
        assert!(!first[1].committed && first[0].committed);
        assert!(first[2].created, "an absent group is created (AP-04.2-6)");
        assert!(!first[0].created);
        assert_eq!(*fake.commits.borrow(), 2);
        // The member stops; the same plan in a new run.
        fake.groups.borrow_mut().get_mut("b").unwrap().0 = empty();
        fake.groups.borrow_mut().get_mut("b").unwrap().1.clear();
        let second: Vec<GroupRecord> = plan
            .iter()
            .map(|g| apply_group(&fake, g, &marks()))
            .collect();
        let outcomes: Vec<&str> = second.iter().map(|r| r.outcome.as_str()).collect();
        assert_eq!(
            outcomes,
            [so::ALREADY_APPLIED, so::APPLIED, so::ALREADY_APPLIED]
        );
        assert_eq!(
            *fake.commits.borrow(),
            3,
            "only the third group was committed again"
        );
        assert!(!second[0].committed);
    }

    /// The prior is read before the commit and recorded (AP-04.2-3), and a
    /// group whose position moved since the plan is refused, untouched.
    #[test]
    fn priors_are_recorded_and_a_moved_group_is_not_overwritten() {
        let fake = Fake::default();
        let mut prior = BTreeMap::new();
        prior.insert(("r-orders".to_string(), 0), 30);
        fake.groups
            .borrow_mut()
            .insert("a".into(), (empty(), prior));
        let mut rollback = group("a", &[(0, 20)]);
        rollback.topics.get_mut("r-orders").unwrap().expected =
            Expected::Positions([(0, 30)].into());
        let r = apply_group(&fake, &rollback, &marks());
        assert_eq!(r.outcome, so::APPLIED);
        assert_eq!(r.partitions[0].prior, Some(30));
        assert_eq!(r.partitions[0].readback, Some(20));
        // A forward plan over a group that holds a position: changed.
        let r = apply_group(&fake, &group("a", &[(0, 40)]), &marks());
        assert_eq!(r.outcome, so::REFUSED);
        assert!(r
            .reason
            .as_deref()
            .unwrap()
            .starts_with("TargetPositionsChanged: r-orders:0 is at 20"));
        assert_eq!(r.partitions[0].prior, Some(20));
        // A rollback after the consumer read 10 records names the 10 (C15).
        let r = apply_group(
            &fake,
            &{
                let mut g = group("a", &[(0, 30)]);
                g.topics.get_mut("r-orders").unwrap().expected =
                    Expected::Positions([(0, 10)].into());
                g
            },
            &marks(),
        );
        assert!(r
            .reason
            .as_deref()
            .unwrap()
            .contains("10 record(s) from the expected 10"));
    }

    #[test]
    fn a_member_that_joins_before_the_commit_is_refused_by_the_broker() {
        let fake = Fake::default();
        fake.groups
            .borrow_mut()
            .insert("a".into(), (empty(), BTreeMap::new()));
        *fake.joins_at_commit.borrow_mut() = Some("a".into());
        let r = apply_group(&fake, &group("a", &[(0, 40)]), &marks());
        assert_eq!(r.outcome, so::REFUSED);
        assert!(r.reason.as_deref().unwrap().starts_with(so::GROUP_ACTIVE));
        assert_eq!(r.partitions[0].readback, None, "read back: unchanged");
        assert_eq!(r.partitions[0].applied, AppliedState::Known(false));
    }

    #[test]
    fn hidden_other_and_unproven_groups_are_refused_and_never_created() {
        for (t, reason) in [
            (TargetGroup::NotVisible, so::TARGET_NOT_VISIBLE),
            (TargetGroup::OtherType, so::TARGET_GROUP_TYPE_UNSUPPORTED),
            (TargetGroup::TypeUnproven, so::TARGET_GROUP_TYPE_UNPROVEN),
        ] {
            let fake = Fake::default();
            fake.groups
                .borrow_mut()
                .insert("a".into(), (t, BTreeMap::new()));
            let r = apply_group(&fake, &group("a", &[(0, 40)]), &marks());
            assert_eq!(
                (r.outcome.as_str(), r.reason.as_deref()),
                (so::REFUSED, Some(reason))
            );
            assert!(!r.created && !r.committed);
            assert_eq!(*fake.commits.borrow(), 0);
        }
    }

    #[test]
    fn an_offset_outside_the_target_partition_is_refused() {
        let fake = Fake::default();
        let r = apply_group(&fake, &group("a", &[(0, 101)]), &marks());
        assert!(r
            .reason
            .as_deref()
            .unwrap()
            .starts_with(so::ABOVE_TARGET_END));
        assert_eq!(*fake.commits.borrow(), 0);
    }
}
