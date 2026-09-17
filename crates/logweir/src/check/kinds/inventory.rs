//! `topicInventory` — D2 §5.2's runner half.
//!
//! # The result carries SIGNALS, never a verdict
//!
//! `logweir_kafka::inventory::assemble` fills the `InventoryResult` with
//! counts, the truncation reason and the two authorization signals D2 §5.4
//! names. It does NOT decide `unknown | limited | attestedComplete`, and
//! neither does this module: the attestation that can turn a listing into
//! `attestedComplete` lives in the installation policy `ConfigMap`, which only
//! the controller reads (D-SEAMS **S3**). A runner that guessed would be
//! claiming completeness on evidence it does not hold, and "a plausible but
//! wrong `complete`" is the worst outcome D2 §5.4 lists.
//!
//! # A failed inventory is a RESULT, not exit 1
//!
//! An unreachable broker produces a result document with no `inventory` block
//! and one `connection.authenticated` row carrying the classified code, and an
//! end line — so the controller reads an actionable reason instead of
//! `ResultUnreadable`. `weirkeeper::check::classify` says so in as many words:
//! "nothing is inferred from the exit code, which covers every operational
//! failure alike". Exit 1 is reserved for a runner that could not print at
//! all.
//!
//! The end frame's `topicLines` block is therefore ABSENT on that path and
//! present with `count: 0` for a cluster that showed no topic: "the inventory
//! did not run" and "the cluster is empty" are different findings, and
//! PLAT-09.1's acceptance requires a user to be able to tell them apart.

use logweir_core::check_contract::{CheckId, CheckPlanKind, CheckResult, TopicInventoryRequest};
use logweir_kafka::inventory::{self, InventoryRequest, ProbeTimeouts};

use super::{from_broker_failure, Wiring};
use crate::check::{Deadline, Emission};

/// Run one `topicInventory`.
#[must_use]
pub fn run(req: &TopicInventoryRequest, wiring: &dyn Wiring, deadline: Deadline) -> Emission {
    let now = wiring.now();
    let mut result = CheckResult::new(CheckPlanKind::TopicInventory);
    let budget = deadline.remaining();

    let probe = match wiring.broker(&req.connection, budget) {
        Ok(p) => p,
        Err(f) => {
            result.checks.push(from_broker_failure(
                CheckId::ConnectionAuthenticated,
                &f,
                now,
            ));
            return Emission::of(result);
        }
    };

    let request = InventoryRequest {
        include_internal: req.include_internal,
        expected_topics: req.expected_topics.clone(),
        max_topics: req.max_topics,
        relay_budget_bytes: req.relay_budget_bytes,
    };
    // 40 % of the budget for the targeted expected-topic probe (D2 §5.2 step
    // 5), computed by the crate that owns the rule.
    let targeted = ProbeTimeouts::targeted_budget(budget);
    match inventory::collect(probe.as_ref(), &request, targeted) {
        Ok(inv) => {
            result.inventory = Some(inv.result);
            Emission {
                topics: inv.entries,
                emit_topics: true,
                result,
                extra: Vec::new(),
            }
        }
        Err(f) => {
            result.checks.push(from_broker_failure(
                CheckId::ConnectionAuthenticated,
                &f,
                now,
            ));
            Emission::of(result)
        }
    }
}
