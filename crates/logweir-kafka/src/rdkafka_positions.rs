//! **PROD-04.0a's broker glue: a consumer bound to ONE group id that can
//! only read and commit that group's positions** (PROD-04.0 §4.3). The
//! decisions live in [`crate::positions`]; this module only moves values
//! between them and rust-rdkafka 0.36.2's safe consumer API. No `unsafe`.
//!
//! # The handle cannot subscribe
//!
//! A `BaseConsumer` built with the TARGET group's `group.id` would join that
//! group the moment anything called `subscribe` or `assign` on it, and a
//! join is exactly the "live member" that makes the broker refuse an
//! administrative commit, or worse, a member that takes partitions from the
//! application. So [`GroupHandle`] keeps its consumer private and exposes
//! only [`GroupHandle::committed_positions`] and
//! [`GroupHandle::commit_positions`]; it is crate-private, and
//! `RdKafkaReader` opens a fresh one per call and drops it before returning.
//! Nothing outside this module can reach the consumer.
//!
//! # What each configuration key is for
//!
//! See [`handle_config`]; `tests::the_handle_never_joins_and_reads_stable_positions`
//! pins every key.
use crate::positions::{
    commit_error, commit_request, fetch_error, fetch_request, partition_answer, valid_bound,
    valid_group, CommitError, CommittedPosition, GroupListing, GroupPositions, PositionsError,
    TopicPartition, COMMIT_LEADER_EPOCH,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError as RdError, RDKafkaErrorCode};
use rdkafka::{Offset, TopicPartitionList};
use std::time::Duration;

/// The `client.id` a positions handle announces, so a broker's request log
/// tells its requests from the drill's (`logweir-drill`).
pub(crate) const POSITIONS_CLIENT_ID: &str = "logweir-positions";

/// The handle's configuration: `base` (the connection, as
/// `RdKafkaReader::client_config` builds it) plus the keys that make it a
/// non-member of `group` reading stable positions.
///
/// | key | value | why |
/// |---|---|---|
/// | `group.id` | the group | OffsetFetch and OffsetCommit name it |
/// | `group.protocol` | `classic` | pinned: librdkafka says its default "will change to `consumer`", and a commit's non-member shape (generation −1, empty member id, K3) and the bounds below are the classic protocol's |
/// | `enable.auto.commit` | `false` | the handle commits only what it is asked to |
/// | `enable.auto.offset.store` | `false` | nothing is ever stored implicitly |
/// | `isolation.level` | `read_committed` | pinned: it is what makes librdkafka send RequireStable (`rdkafka.c:3655-3656`); without it a pending transactional commit reads as the pre-transaction position (§3.4, T8) |
/// | `session.timeout.ms` | the bound | how long a commit waits for a coordinator before failing `_WAIT_COORD` (`rdkafka_cgrp.c:3738-3746`); the handle never joins, so it bounds nothing else |
/// | `socket.timeout.ms` | the bound | each request's own timeout, so a coordinator that never answers fails the commit too |
pub(crate) fn handle_config(base: &ClientConfig, group: &str, bound: Duration) -> ClientConfig {
    let ms = bound.as_millis().to_string();
    let mut cfg = base.clone();
    cfg.set("client.id", POSITIONS_CLIENT_ID)
        .set("group.id", group)
        .set("group.protocol", "classic")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
        .set("session.timeout.ms", ms.as_str())
        .set("socket.timeout.ms", ms.as_str());
    cfg
}

/// The raw librdkafka value of an `Offset`: the exact inverse of
/// `Offset::from_raw` (`rdkafka-0.36.2/src/topic_partition_list.rs:52-63`),
/// including the values `Offset::to_raw` refuses (`OffsetTail(0)`), so a
/// broker's odd committed value reaches [`partition_answer`] as it was.
pub(crate) fn raw_offset(offset: Offset) -> i64 {
    match offset {
        Offset::Beginning => -2,
        Offset::End => -1,
        Offset::Stored => -1000,
        Offset::Invalid => -1001,
        Offset::OffsetTail(n) => -2000 - n,
        Offset::Offset(n) => n,
    }
}

/// Whether an error the handle's queue delivered is the broker refusing this
/// principal on the group: GROUP_AUTHORIZATION_FAILED, which librdkafka posts
/// for a refused coordinator lookup (`rdkafka_cgrp.c:797-807`) and rust-rdkafka
/// delivers as `MessageConsumption` (or `…Fatal`). NOTHING ELSE counts: a
/// transport, authentication or TLS error on the same queue says the broker was
/// not reached or not trusted, and calling that a refusal would send an
/// operator to fix ACLs for a network fault.
pub(crate) fn is_group_refusal(e: &RdError) -> bool {
    matches!(
        e,
        RdError::MessageConsumption(RDKafkaErrorCode::GroupAuthorizationFailed)
            | RdError::MessageConsumptionFatal(RDKafkaErrorCode::GroupAuthorizationFailed)
    )
}

/// A non-member consumer of ONE group, which can only read and commit that
/// group's positions. See the module documentation.
pub(crate) struct GroupHandle {
    consumer: BaseConsumer,
    group: String,
    bound: Duration,
}

impl GroupHandle {
    /// Builds the handle. Local only: librdkafka dials nothing until a call
    /// needs the coordinator.
    pub(crate) fn open(base: &ClientConfig, group: &str, bound: Duration) -> Result<Self, String> {
        valid_group(group)?;
        let bound = valid_bound(bound)?;
        let consumer: BaseConsumer = handle_config(base, group, bound)
            .create()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            consumer,
            group: group.to_string(),
            bound,
        })
    }

    /// Whether the handle's own queue holds a GROUP_AUTHORIZATION_FAILED
    /// refusal. librdkafka posts a refused coordinator lookup there ONCE per
    /// handle (`rdkafka_cgrp.c:797-807`) and keeps retrying, so a fetch or a
    /// commit for a group this principal may not Describe waits out its bound
    /// with the refusal sitting in the queue. The handle never subscribes or
    /// assigns, so the queue carries nothing else a caller could lose.
    fn group_refusal_seen(&self) -> bool {
        let mut seen = false;
        let mut empty = 0;
        // Bounded: a queue cannot hold more than a handful of events here.
        // One `None` is not "empty": rust-rdkafka's zero-timeout poll also
        // answers `None` after it consumes a non-error event
        // (`base_consumer.rs:142-170`), so a refusal queued behind one would
        // be missed. Two in a row are.
        for _ in 0..64 {
            match self.consumer.poll(Duration::ZERO) {
                None => {
                    empty += 1;
                    if empty == 2 {
                        break;
                    }
                }
                Some(Err(e)) if is_group_refusal(&e) => {
                    empty = 0;
                    seen = true;
                }
                Some(_) => empty = 0,
            }
        }
        seen
    }

    /// ONE RequireStable OffsetFetch for exactly `partitions`, bounded by the
    /// handle's bound. See [`crate::positions`] for the contract.
    pub(crate) fn committed_positions(
        &self,
        partitions: &[TopicPartition],
        listing: GroupListing,
    ) -> Result<GroupPositions, PositionsError> {
        fetch_request(partitions)?;
        let mut tpl = TopicPartitionList::with_capacity(partitions.len());
        for tp in partitions {
            tpl.add_partition(&tp.topic, tp.partition);
        }
        let answered = match self.consumer.committed_offsets(tpl, self.bound) {
            Ok(answered) => answered,
            Err(e) => {
                let Some(c) = e.rdkafka_error_code() else {
                    return Err(PositionsError::Client(e.to_string()));
                };
                let refused = c == RDKafkaErrorCode::OperationTimedOut && self.group_refusal_seen();
                return Err(fetch_error(
                    &self.group,
                    listing,
                    c as i32,
                    &c.to_string(),
                    refused,
                    self.bound,
                ));
            }
        };
        let mut out = Vec::with_capacity(partitions.len());
        for tp in partitions {
            let elem = answered
                .find_partition(&tp.topic, tp.partition)
                .ok_or_else(|| {
                    PositionsError::Client(format!(
                        "{}: the fetch returned no entry for requested {tp}",
                        self.group
                    ))
                })?;
            let code = elem.error().err().and_then(|e| e.rdkafka_error_code());
            let error = code.map(|c| (c as i32, c.to_string()));
            // `metadata()` panics on bytes that are not UTF-8
            // (`topic_partition_list.rs:141-145`); an Apache Kafka broker never
            // returns such bytes, and any other broker's are withheld, not read.
            let metadata = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                elem.metadata().to_string()
            }))
            .ok();
            out.push((
                tp.clone(),
                partition_answer(
                    raw_offset(elem.offset()),
                    error.as_ref().map(|(c, n)| (*c, n.as_str())),
                    metadata,
                ),
            ));
        }
        Ok(GroupPositions {
            group: self.group.clone(),
            partitions: out,
        })
    }

    /// ONE synchronous non-member OffsetCommit for every position, which the
    /// broker refuses for the whole request while the group has members.
    /// See [`crate::positions::commit_request`] and
    /// [`crate::positions::commit_error`].
    ///
    /// librdkafka's synchronous commit waits without a timeout of its own
    /// (`rd_kafka_commit`, `rdkafka_offset.c:386-414`); the handle bounds it
    /// through `session.timeout.ms` (no coordinator) and `socket.timeout.ms`
    /// (no answer, retried at most twice), so it returns within a few bounds.
    pub(crate) fn commit_positions(
        &self,
        positions: &[(TopicPartition, CommittedPosition)],
    ) -> Result<(), CommitError> {
        let entries = commit_request(positions)?;
        let mut tpl = TopicPartitionList::with_capacity(entries.len());
        for e in &entries {
            // This route cannot set a leader epoch (C3): librdkafka sends the
            // element's default, −1. Refuse an entry that asks for anything
            // else rather than send −1 in its name.
            if e.leader_epoch != COMMIT_LEADER_EPOCH {
                return Err(CommitError::InvalidRequest(format!(
                    "{}: the safe consumer API can only commit leader epoch {COMMIT_LEADER_EPOCH}, \
                     not {}",
                    e.tp, e.leader_epoch
                )));
            }
            let mut elem = tpl.add_partition(&e.tp.topic, e.tp.partition);
            elem.set_offset(Offset::Offset(e.offset))
                .map_err(|err| CommitError::InvalidRequest(format!("{}: {err}", e.tp)))?;
            elem.set_metadata(&e.metadata);
        }
        match self.consumer.commit(&tpl, CommitMode::Sync) {
            Ok(()) => Ok(()),
            Err(e) => {
                let Some(c) = e.rdkafka_error_code() else {
                    return Err(CommitError::Client(e.to_string()));
                };
                let refused =
                    c == RDKafkaErrorCode::WaitingForCoordinator && self.group_refusal_seen();
                Err(commit_error(
                    &self.group,
                    c as i32,
                    &c.to_string(),
                    refused,
                    self.bound,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::positions::{code, DEFAULT_POSITION_BOUND};

    fn base() -> ClientConfig {
        crate::rdkafka_reader::RdKafkaReader::client_config(
            &["127.0.0.1:1".to_string()],
            &crate::reader::AuthConfig::Plaintext,
        )
        .expect("a plaintext config builds")
    }

    /// **Every key that makes the handle a non-member reading STABLE
    /// positions is pinned.** No socket: `ClientConfig` is a map until
    /// `create()`. Each assertion is a mutant's failure: `read_uncommitted`
    /// would read a pending transaction's pre-transaction position (§3.4),
    /// `consumer` would change the commit's non-member shape and make
    /// librdkafka refuse `session.timeout.ms`, and auto-commit would write
    /// positions nobody asked for.
    #[test]
    fn the_handle_never_joins_and_reads_stable_positions() {
        let cfg = handle_config(&base(), "orders-app", Duration::from_secs(7));
        assert_eq!(cfg.get("group.id"), Some("orders-app"));
        assert_eq!(cfg.get("group.protocol"), Some("classic"));
        assert_eq!(cfg.get("enable.auto.commit"), Some("false"));
        assert_eq!(cfg.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(cfg.get("isolation.level"), Some("read_committed"));
        assert_eq!(cfg.get("session.timeout.ms"), Some("7000"));
        assert_eq!(cfg.get("socket.timeout.ms"), Some("7000"));
        assert_eq!(cfg.get("client.id"), Some(POSITIONS_CLIENT_ID));
        // The connection is the reader's own, unchanged.
        assert_eq!(cfg.get("bootstrap.servers"), Some("127.0.0.1:1"));
        assert_eq!(cfg.get("allow.auto.create.topics"), Some("false"));
        // And librdkafka accepts the whole set (local only: nothing dials).
        GroupHandle::open(&base(), "orders-app", DEFAULT_POSITION_BOUND)
            .expect("librdkafka accepts the handle's configuration");
    }

    /// A blank group id or a bound outside the accepted range never builds a
    /// handle: an empty `group.id` is no group at all, and a bound below
    /// 2 s makes librdkafka warn on every handle.
    #[test]
    fn a_blank_group_or_a_bound_out_of_range_builds_no_handle() {
        assert!(GroupHandle::open(&base(), "", DEFAULT_POSITION_BOUND).is_err());
        assert!(GroupHandle::open(&base(), "  ", DEFAULT_POSITION_BOUND).is_err());
        assert!(GroupHandle::open(&base(), "g", Duration::from_millis(1999)).is_err());
        assert!(GroupHandle::open(&base(), "g", Duration::from_secs(301)).is_err());
        assert!(GroupHandle::open(&base(), "g", Duration::from_secs(2)).is_ok());
    }

    /// `raw_offset` inverts `Offset::from_raw` exactly, so the value the broker
    /// left reaches `partition_answer` unchanged, and "no committed offset" is
    /// librdkafka's −1001, not 0.
    #[test]
    fn raw_offset_inverts_rdkafkas_offset_mapping() {
        for raw in [
            -2005,
            -2001,
            -2000,
            -1001,
            -1000,
            -2,
            -1,
            0,
            1,
            42,
            i64::MAX,
        ] {
            assert_eq!(raw_offset(Offset::from_raw(raw)), raw, "raw {raw}");
        }
        assert_eq!(
            raw_offset(Offset::Invalid),
            crate::positions::NO_COMMITTED_OFFSET_RAW
        );
    }

    /// The rust-rdkafka codes the glue compares against are the integers the
    /// pure mapping decides on: a renumbered or renamed variant fails here,
    /// not in a live row.
    #[test]
    fn the_rdkafka_codes_are_the_integers_the_mapping_reads() {
        for (c, n) in [
            (RDKafkaErrorCode::OperationTimedOut, code::TIMED_OUT),
            (RDKafkaErrorCode::WaitingForCoordinator, code::WAIT_COORD),
            (RDKafkaErrorCode::NoOffset, code::NO_OFFSET),
            (RDKafkaErrorCode::UnknownMemberId, code::UNKNOWN_MEMBER_ID),
            (RDKafkaErrorCode::GroupIdNotFound, code::GROUP_ID_NOT_FOUND),
            (
                RDKafkaErrorCode::GroupAuthorizationFailed,
                code::GROUP_AUTHORIZATION_FAILED,
            ),
            (
                RDKafkaErrorCode::TopicAuthorizationFailed,
                code::TOPIC_AUTHORIZATION_FAILED,
            ),
            (
                RDKafkaErrorCode::UnstableOffsetCommit,
                code::UNSTABLE_OFFSET_COMMIT,
            ),
            (
                RDKafkaErrorCode::IllegalGeneration,
                code::ILLEGAL_GENERATION,
            ),
            (
                RDKafkaErrorCode::RebalanceInProgress,
                code::REBALANCE_IN_PROGRESS,
            ),
            (
                RDKafkaErrorCode::CoordinatorLoadInProgress,
                code::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                RDKafkaErrorCode::CoordinatorNotAvailable,
                code::COORDINATOR_NOT_AVAILABLE,
            ),
            (RDKafkaErrorCode::NotCoordinator, code::NOT_COORDINATOR),
            (RDKafkaErrorCode::FencedInstanceId, code::FENCED_INSTANCE_ID),
            (RDKafkaErrorCode::StaleMemberEpoch, code::STALE_MEMBER_EPOCH),
        ] {
            assert_eq!(c as i32, n, "{c:?}");
        }
    }

    /// **L3: only GROUP_AUTHORIZATION_FAILED is the group refusal.** Mutant:
    /// count any queued error (a transport, authentication or TLS failure) as
    /// the refusal, which would label a network fault `NotAuthorized`.
    #[test]
    fn only_a_group_authorization_failure_is_the_refusal() {
        use RDKafkaErrorCode as C;
        assert!(is_group_refusal(&RdError::MessageConsumption(
            C::GroupAuthorizationFailed
        )));
        assert!(is_group_refusal(&RdError::MessageConsumptionFatal(
            C::GroupAuthorizationFailed
        )));
        for not in [
            RdError::MessageConsumption(C::BrokerTransportFailure),
            RdError::MessageConsumption(C::Authentication),
            RdError::MessageConsumption(C::AllBrokersDown),
            RdError::MessageConsumption(C::SaslAuthenticationFailed),
            RdError::MessageConsumption(C::TopicAuthorizationFailed),
            RdError::MessageConsumptionFatal(C::ClusterAuthorizationFailed),
            RdError::MetadataFetch(C::GroupAuthorizationFailed),
            RdError::PartitionEOF(0),
        ] {
            assert!(!is_group_refusal(&not), "{not:?}");
        }
    }

    /// The bound the bound rows use: librdkafka's minimum this crate accepts.
    const UNIT_BOUND: Duration = crate::positions::MIN_POSITION_BOUND;
    /// What an unanswered call may take beyond its bound. Measured on the
    /// development host (2026-10-08, five runs, other builds running): a fetch
    /// returned 2000.9–2003.3 ms after a 2 s bound and a commit 2001.8–2007.1 ms
    /// (`artifacts/prod-04-0a/fix-bound-measure.log`). The commit can also wait
    /// up to one more second for librdkafka's timeout scan, which runs once a
    /// second (`rdkafka_cgrp.c:5809-5830`). So the margins are 1.5 s over the
    /// fetch's bound and 2.5 s over the commit's: room for a loaded CI runner,
    /// and far below what a dropped bound costs (no fetch deadline at all, or
    /// librdkafka's 45 s `session.timeout.ms`).
    const FETCH_MARGIN: Duration = Duration::from_millis(1500);
    const COMMIT_MARGIN: Duration = Duration::from_millis(2500);

    /// **M1: an unanswered fetch returns within its bound.** No broker: the
    /// bootstrap is the dead loopback port, so no coordinator ever answers, the
    /// handle's queue fills with connection errors, and the fetch can only time
    /// out. Mutants: the fetch waiting a longer or no deadline (the row's upper
    /// bound fails); a fetch that does not wait (the lower bound fails); and a
    /// drain that counts the queued transport errors as the group refusal
    /// (`NotVisibleToPrincipal` instead of `NotVisibleOrUnreachable`).
    #[test]
    fn an_unanswered_fetch_returns_within_its_bound() {
        let h = GroupHandle::open(&base(), "cpos-unit-g", UNIT_BOUND).expect("local");
        let started = std::time::Instant::now();
        let got = h.committed_positions(&[TopicPartition::new("t", 0)], GroupListing::NotListed);
        let took = started.elapsed();
        eprintln!("unanswered fetch, bound {UNIT_BOUND:?}: took {took:?}");
        assert_eq!(
            got,
            Err(PositionsError::NotVisibleOrUnreachable {
                group: "cpos-unit-g".into(),
                bound: UNIT_BOUND
            }),
            "no coordinator and no refusal: unreachable, never NotVisibleToPrincipal"
        );
        assert!(
            took >= UNIT_BOUND - Duration::from_millis(100),
            "the fetch waited its bound ({took:?})"
        );
        assert!(
            took < UNIT_BOUND + FETCH_MARGIN,
            "the fetch returned within its bound plus {FETCH_MARGIN:?} ({took:?})"
        );
    }

    /// **M1, the commit's twin: an unanswered commit returns within its
    /// bound** (the coordinator wait is `session.timeout.ms`, set to the bound;
    /// librdkafka's default is 45 s). Nothing is sent, so nothing applied.
    #[test]
    fn an_unanswered_commit_returns_within_its_bound() {
        let h = GroupHandle::open(&base(), "cpos-unit-g", UNIT_BOUND).expect("local");
        let p = CommittedPosition {
            offset: 3,
            leader_epoch: None,
            metadata: None,
        };
        let started = std::time::Instant::now();
        let got = h.commit_positions(&[(TopicPartition::new("t", 0), p)]);
        let took = started.elapsed();
        eprintln!("unanswered commit, bound {UNIT_BOUND:?}: took {took:?}");
        assert_eq!(
            got,
            Err(CommitError::NotVisibleOrUnreachable {
                group: "cpos-unit-g".into(),
                bound: UNIT_BOUND
            }),
            "no coordinator and no refusal: unreachable, and nothing sent"
        );
        assert!(
            took >= UNIT_BOUND - Duration::from_millis(100),
            "the commit waited for a coordinator ({took:?})"
        );
        assert!(
            took < UNIT_BOUND + COMMIT_MARGIN,
            "the commit returned within its bound plus {COMMIT_MARGIN:?} ({took:?})"
        );
    }

    /// Bad input is refused before a handle exists or anything is sent.
    #[test]
    fn a_commit_that_cannot_mean_what_it_says_is_refused_before_sending() {
        let h = GroupHandle::open(&base(), "g", DEFAULT_POSITION_BOUND).expect("local");
        let p = |o| CommittedPosition {
            offset: o,
            leader_epoch: None,
            metadata: None,
        };
        let refused = h
            .commit_positions(&[(TopicPartition::new("t", 0), p(-1))])
            .expect_err("a negative offset is not a position");
        assert!(
            matches!(refused, CommitError::InvalidRequest(_)),
            "{refused:?}"
        );
        let refused = h
            .committed_positions(&[], GroupListing::Listed)
            .expect_err("an empty fetch is refused");
        assert!(
            matches!(refused, PositionsError::InvalidRequest(_)),
            "{refused:?}"
        );
    }
}
