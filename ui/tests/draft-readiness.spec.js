// draft-readiness.spec.js -- DRAFT-PREFLIGHT-NEVER-READY (PLAT-19.2).
//
// A readiness check run against a DRAFT restore plan answers `approval.state`
// with `skipped` + `SubjectNotCreated`: no Restore exists yet, so no approver
// has been asked. The controller's aggregate (`logweir_core::check_contract::
// aggregate`) deliberately makes a skipped blocking row `unknown` overall, and
// that stays. The wizard used to refuse every verdict that was not `ready`, so
// after ANY readiness check the Create button was refused.
//
// The rule these rows pin: a draft may be submitted when every blocking check
// is `ready` except `approval.state` in PRECISELY that shape; any other
// non-ready, unknown or skipped blocking row still refuses, by id and code.
// Every accepting row sits beside the refusing row that would pass if the rule
// were widened.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  initialState,
  isDraftApprovalRow,
  readinessRefusal,
  recoveryPoints,
  renderPlanStep,
} from "../pages/restore-wizard.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

function wizardState() {
  const backups = fixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  return initialState("logweir-t27", fixture("wizard-clusters.json"), backups, {
    uid: point.metadata.uid,
    backup: point.metadata.name,
  });
}

const PREPARED = { bytes: "b", hash: "sha256:aaa", restoreName: "rst-x", approvalName: "apr-x" };

function row(id, state, code, gating) {
  return {
    id: id,
    category: id.split(".")[0],
    state: state,
    gating: gating || "blocking",
    authority: "controller",
    code: code,
    message: id + " is " + state,
  };
}

/** The controller's draft answer: approval.state skipped/SubjectNotCreated. */
const DRAFT_APPROVAL = () => row("approval.state", "skipped", "SubjectNotCreated");

/** The ready rows a real draft preflight carries beside the approval row. */
function readyRows() {
  return [
    row("target.resolved", "ready", "TargetAllowed"),
    row("target.mappedTopics", "ready", "MappedTopicsAbsent"),
    row("archive.segments", "ready", "SegmentsPresent"),
  ];
}

/** A terminal, applicable result for THIS plan, in the product API's shape.
 *  `state` is what the controller's aggregate says about `checks`. */
function verdict(state, checks) {
  return {
    boundHash: PREPARED.hash,
    preflight: {
      id: "pf-000000000000000000000001",
      operation: "restore",
      state: state,
      terminal: true,
      applicable: true,
      stale: false,
      staleReasons: [],
      staleBasis: ["plan", "referents"],
      binding: { planHash: PREPARED.hash },
      checks: checks,
      warnings: [],
      executionOnly: [],
      detailsAvailable: false,
      conditions: [],
    },
  };
}

function refusedButton(state) {
  const step6 = renderPlanStep(PREPARED, state);
  return {
    blocked: step6.includes("id=\"readiness-blocked\""),
    disabled: step6.includes("id=\"create-restore\" class=\"primary\" disabled"),
  };
}

test("a_draft_shaped_verdict_submits", () => {
  // THE DEFECT'S OWN SHAPE: every blocking check ready, approval.state skipped
  // with SubjectNotCreated, aggregate `unknown` because of that row alone.
  const state = wizardState();
  state.readiness = verdict("unknown", readyRows().concat([DRAFT_APPROVAL()]));
  assert.equal(readinessRefusal(state, PREPARED), null,
    "the only non-ready row is the one creating the Restore answers");
  const shown = refusedButton(state);
  assert.equal(shown.blocked, false, "no refusal sentence is rendered");
  assert.equal(shown.disabled, false, "and the Create button is enabled");

  // An ADVISORY row that is not ready does not change that: warnings never
  // gated the submit and still do not.
  state.readiness = verdict("unknown", readyRows().concat([
    DRAFT_APPROVAL(),
    row("approval.keyValidity", "notReady", "ApproverKeyExpiresBeforeDeadline", "advisory"),
  ]));
  assert.equal(readinessRefusal(state, PREPARED), null);
});

test("the_draft_verdict_plus_any_other_blocking_non_ready_row_refuses_by_id_and_code", () => {
  const others = [
    // a collision: the aggregate is notReady
    ["notReady", row("target.mappedTopics", "notReady", "MappedTopicExists")],
    // a check that could not decide: the aggregate stays unknown, exactly like
    // the draft row's, which is why the exception is keyed on the row and not
    // on the aggregate
    ["unknown", row("target.resolved", "unknown", "BrokerUnreachable")],
    // a second skipped blocking row, with another code
    ["unknown", row("archive.segments", "skipped", "SubjectNotCreated")],
  ];
  for (const [aggregate, other] of others) {
    const state = wizardState();
    const rows = readyRows().filter((r) => r.id !== other.id).concat([DRAFT_APPROVAL(), other]);
    state.readiness = verdict(aggregate, rows);
    const refused = readinessRefusal(state, PREPARED);
    assert.ok(typeof refused === "string", other.id + " " + other.state + " refuses");
    assert.ok(refused.includes(other.id + " (" + other.code + ")"),
      "naming the row by id and code: " + refused);
    assert.ok(!refused.includes("approval.state"),
      "and not blaming the draft row, which is not the reason: " + refused);
    const shown = refusedButton(state);
    assert.ok(shown.blocked && shown.disabled, "the button is disabled and says why");
  }
});

test("approval_state_not_ready_refuses_because_it_is_not_the_draft_shape", () => {
  // An Approval that exists and does not verify: `notReady`, not `skipped`.
  const state = wizardState();
  state.readiness = verdict("notReady", readyRows().concat([
    row("approval.state", "notReady", "ApprovalNotVerified"),
  ]));
  const refused = readinessRefusal(state, PREPARED);
  assert.ok(typeof refused === "string", "refused");
  assert.ok(refused.includes("approval.state (ApprovalNotVerified)"), refused);

  // `skipped` with ANOTHER code is not the draft shape either, even under an
  // `unknown` aggregate.
  state.readiness = verdict("unknown", readyRows().concat([
    row("approval.state", "skipped", "ApprovalNotVerified"),
  ]));
  const skippedOther = readinessRefusal(state, PREPARED);
  assert.ok(typeof skippedOther === "string" &&
    skippedOther.includes("approval.state (ApprovalNotVerified)"), String(skippedOther));

  // And `unknown` + SubjectNotCreated is not it: the state is part of the shape.
  state.readiness = verdict("unknown", readyRows().concat([
    row("approval.state", "unknown", "SubjectNotCreated"),
  ]));
  assert.ok(typeof readinessRefusal(state, PREPARED) === "string");
});

test("the_draft_row_alone_is_not_everything_passed", () => {
  // The aggregate's own rule -- an empty blocking set is `unknown`, never
  // `ready` -- applied to the exception: with no OTHER blocking check that
  // came back ready, nothing was checked, and the submit is refused with the
  // draft row named, because it is then the only thing to name.
  const state = wizardState();
  state.readiness = verdict("unknown", [DRAFT_APPROVAL()]);
  const refused = readinessRefusal(state, PREPARED);
  assert.ok(typeof refused === "string", "refused");
  assert.ok(refused.includes("approval.state (SubjectNotCreated)"), refused);

  // And a verdict whose aggregate is not `unknown` is never rescued by the
  // exception, whatever its rows say.
  state.readiness = verdict("notReady", readyRows().concat([DRAFT_APPROVAL()]));
  assert.ok(typeof readinessRefusal(state, PREPARED) === "string");
});

test("the_exception_is_exactly_one_shape", () => {
  assert.equal(isDraftApprovalRow(DRAFT_APPROVAL()), true);
  assert.equal(isDraftApprovalRow(row("approval.state", "notReady", "SubjectNotCreated")), false);
  assert.equal(isDraftApprovalRow(row("approval.state", "skipped", "ApprovalNotVerified")), false);
  assert.equal(isDraftApprovalRow(row("archive.segments", "skipped", "SubjectNotCreated")), false);
  assert.equal(isDraftApprovalRow(null), false);
});
