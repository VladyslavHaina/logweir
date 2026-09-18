//! `sourceConnection` — dial ONE connection and say whether it answers.
//!
//! # Why this is its own kind and not `operationReadiness` with the fields left out
//!
//! D2 §4.2's `operationReadiness` request cannot express this question. Its
//! Backup catalogue asks about a destination, a signer and a named topic set,
//! and the console control that wants this one — "Test connection" on the
//! `KafkaCluster` detail page — has none of those to offer: asking an operator
//! to name a topic and choose a destination in order to find out whether a
//! broker answers would be a different control wearing the same label. Making
//! the fields optional on that request would have been smaller by line count
//! and worse by contract: `deny_unknown_fields` over a struct that carries a
//! connection and NOTHING ELSE is what makes "this check reads no destination,
//! loads no signing key and names no topic" a property the runner enforces in
//! step 4, before it has opened a socket, rather than a promise in a comment.
//!
//! # Which rows this emits, and the one it deliberately does not
//!
//! Two: [`CheckId::RunnerContract`] and [`CheckId::ConnectionAuthenticated`].
//! Those are the only D2 §6.3 rows with authority **J** whose question this
//! request can actually ask. The controller answers `connection.resolved`,
//! `connection.clusterIdentity`, `configuration.policy` and the three pod rows
//! from its own side, exactly as it does for a Backup.
//!
//! `connection.topicsDescribable` IS NOT EMITTED, and that is the deliberate
//! half. Its whole vocabulary is about NAMED topics — `TopicNotFound` is an
//! existence fact about one name, `TopicNotAuthorized` a visibility fact about
//! one name, and `remedy_for(TopicVisibilityUnknown)` reads "Grant DESCRIBE to
//! get a definite answer" about a topic the requester chose. A request that
//! names no topic has asked none of that, so the row has no honest answer:
//! `ready` would be a green verdict about the empty set, and `unknown` on a
//! BLOCKING row would pin every connection test at `unknown` for ever. The
//! question "what can this principal see" belongs to `topicInventory`, which
//! the same page's "Discover topics" control already runs, and the console
//! says so beside this one.

use logweir_core::check_contract::{
    CheckCode, CheckId, CheckPlanKind, CheckResult, CheckScope, ConnectionPlan,
    SourceConnectionRequest,
};

use super::{from_broker_failure, runner_contract, Wiring};
use crate::check::{catalogue, Deadline, Emission};

/// The scope every row of this kind carries.
///
/// The plan carries a resolved CONNECTION and no `KafkaCluster` name or UID, so
/// the scope names the principal — the identity this check actually exercised,
/// and the one an operator matches against an ACL export. Identical reasoning
/// to [`super::readiness`]'s, and identical spelling, so the two kinds' rows
/// read the same in a status.
#[must_use]
pub fn scope(plan: &ConnectionPlan) -> CheckScope {
    catalogue::scope("KafkaCluster", &plan.principal, None)
}

/// Run one `sourceConnection`.
///
/// THE WHOLE BUDGET GOES TO THE ONE DIAL. `readiness::run` halves its deadline
/// because it has a broker half and a destination half; this kind has only the
/// broker, so halving it would make a connection test time out at half the
/// budget its own `timeoutSeconds` promised.
#[must_use]
pub fn run(req: &SourceConnectionRequest, wiring: &dyn Wiring, deadline: Deadline) -> Emission {
    let now = wiring.now();
    let scope = scope(&req.connection);
    let mut checks = vec![runner_contract(now)];

    // `skipChecks` IS NOT HONOURED HERE, AND THERE IS NOTHING TO HONOUR IT
    // WITH: the request carries no skip list, because a connectivity check
    // with its one connection row skipped is an empty check, and a control
    // that could ask for one would be a control that can report `ready`
    // without dialling.
    match wiring.broker(&req.connection, deadline.remaining()) {
        Err(f) => checks
            .push(from_broker_failure(CheckId::ConnectionAuthenticated, &f, now).with_scope(scope)),
        Ok(probe) => {
            let (row, _) = super::readiness::authenticated(
                CheckId::ConnectionAuthenticated,
                probe.as_ref(),
                Some(scope),
                now,
            );
            checks.push(row);
        }
    }

    debug_assert!(
        checks
            .iter()
            .all(|c| c.code != CheckCode::TopicsDescribable),
        "a sourceConnection check names no topic and must publish no topic verdict"
    );

    let mut result = CheckResult::new(CheckPlanKind::SourceConnection);
    result.checks = checks;
    Emission::of(result)
}
