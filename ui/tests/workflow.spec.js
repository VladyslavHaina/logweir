// workflow.spec.js -- the transitions are named, and an illegal one is an
// error with a name rather than a silent no-op.
//
// WHAT THIS FILE IS FOR. PLAT-18.1's acceptance asks for "state transition
// errors". Before it, the mutation record's states existed and its moves did
// not: settling an attempt that was not in flight was an `if` beside a call
// site, and a page that got it wrong published a state nobody could have
// refused. Every assertion below is about a MOVE -- that it happened, that it
// was the move the contract names, or that it was refused by name.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  MUTATION_MACHINE,
  MUTATION_PHASES,
  TRANSITION_ERROR,
  WIZARD_EVENTS,
  WIZARD_MACHINE,
  WIZARD_STEPS,
  defineMachine,
  isTransitionError,
  replayWizard,
  startMachine,
  transitionError,
} from "../workflow.js";
import { createMutation } from "../lifecycle.js";
import { initialState, stepStates } from "../pages/restore-wizard.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

// ============================================================== the kit

test("a_machine_whose_transition_names_a_state_it_does_not_declare_is_refused_at_load", () => {
  assert.throws(
    () => defineMachine({
      name: "broken", initial: "a", states: { a: { go: "b" } },
    }),
    (error) => {
      assert.ok(error instanceof TypeError);
      assert.match(error.message, /broken: a --go--> b names a state this machine does not declare/);
      return true;
    },
    "the table is checked where it is written, not on the one path that sends the event",
  );
  assert.throws(
    () => defineMachine({ name: "broken", initial: "z", states: { a: {} } }),
    /the initial state z is not declared/,
  );
});

test("an_event_a_state_does_not_accept_is_a_transition_error_naming_all_three_things", () => {
  const run = startMachine(MUTATION_MACHINE);
  assert.equal(run.state, "idle");
  assert.throws(
    () => run.send("succeed"),
    (error) => {
      assert.equal(error.name, TRANSITION_ERROR);
      assert.ok(isTransitionError(error));
      assert.equal(error.from, "idle");
      assert.equal(error.event, "succeed");
      assert.deepEqual(error.allowed, ["clear", "start"]);
      assert.equal(error.kind, "transition");
      assert.match(error.message, /is not a move this workflow makes from "idle"/);
      return true;
    },
  );
  assert.equal(run.state, "idle", "and a refused move moved nothing");
  assert.deepEqual(run.transitions, [], "nor did it leave a record of having happened");
});

test("a_final_state_says_it_is_final_rather_than_listing_nothing", () => {
  const run = startMachine(WIZARD_MACHINE, { initial: "submitted" });
  assert.throws(
    () => run.send("back"),
    /this state is final/,
  );
  const built = transitionError("m", "s", "e", []);
  assert.match(built.message, /this state is final/);
});

// ======================================================= the mutation record

test("the_mutation_record_takes_the_named_moves_the_contract_declares", async () => {
  const record = createMutation();
  assert.equal(record.machineState, "idle");
  assert.equal(record.state.phase, "idle");
  await record.run(() => Promise.resolve({ outcome: "created" }));
  assert.deepEqual(record.transitions, ["start", "succeed"]);
  assert.equal(record.machineState, "succeeded");
  record.clear();
  assert.deepEqual(record.transitions, ["start", "succeed", "clear"]);
  assert.equal(record.state.phase, "idle");
});

test("a_second_submit_while_one_is_in_flight_is_the_absent_start_transition", async () => {
  const record = createMutation();
  let calls = 0;
  const first = record.run(() => { calls += 1; return new Promise(() => {}); });
  const second = record.run(() => { calls += 1; return Promise.resolve(); });
  assert.equal(second, null, "the second click is declined, and its executor never runs");
  assert.equal(calls, 1);
  assert.equal(record.machineState, "pending");
  assert.throws(
    () => record.send("start"),
    (error) => {
      assert.ok(isTransitionError(error));
      assert.equal(error.from, "pending");
      assert.deepEqual(error.allowed, ["abandon", "fail", "succeed", "timeout"]);
      return true;
    },
    "declining is what `run` does for a caller; the MOVE itself is still illegal, and a caller " +
      "that sends it anyway is told so by name",
  );
  assert.ok(first instanceof Promise);
});

test("a_timeout_is_its_own_state_and_a_late_answer_may_still_settle_it", async () => {
  let fire = null;
  const record = createMutation({
    timeoutMs: 10,
    setTimer: (fn) => { fire = fn; return 1; },
    clearTimer: () => {},
  });
  let settle = null;
  const attempt = record.run(() => new Promise((resolve) => { settle = resolve; }));
  fire();
  const timedOut = await attempt;
  assert.equal(record.machineState, "unanswered");
  assert.equal(timedOut.phase, "failed", "the published shape is unchanged");
  assert.equal(timedOut.timedOut, true);
  assert.equal(timedOut.kind, "unknown");
  assert.deepEqual(record.transitions, ["start", "timeout"]);
  settle({ outcome: "created" });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(record.machineState, "succeeded", "the answer that was never cancelled arrived");
  assert.deepEqual(record.transitions, ["start", "timeout", "succeed"]);
});

test("a_pending_record_is_never_cleared_and_never_throws_for_being_asked", async () => {
  const record = createMutation();
  record.run(() => new Promise(() => {}));
  record.clear();
  assert.equal(record.machineState, "pending", "its answer is still on its way");
  assert.deepEqual(record.transitions, ["start"], "and no clear was recorded");
});

test("an_abandoned_attempt_returns_the_record_to_idle_by_its_own_name", async () => {
  const record = createMutation();
  await record.run(() => Promise.resolve({ outcome: "abandoned" }));
  assert.deepEqual(record.transitions, ["start", "abandon"]);
  assert.equal(record.state.phase, "idle");
});

test("every_machine_state_publishes_a_phase_the_pages_already_render", () => {
  for (const state of Object.keys(MUTATION_MACHINE.states)) {
    const shape = MUTATION_PHASES[state];
    assert.ok(shape !== undefined, state + " publishes a phase");
    assert.ok(
      ["idle", "pending", "succeeded", "failed"].indexOf(shape.phase) !== -1,
      state + " publishes one of the four phases every form already reads",
    );
  }
  assert.equal(MUTATION_PHASES.unanswered.phase, "failed");
  assert.equal(MUTATION_PHASES.unanswered.timedOut, true);
});

// ========================================================== the restore wizard

test("the_wizard_s_six_steps_are_the_machine_s_six_states_in_one_order", () => {
  assert.equal(WIZARD_STEPS.length, 6);
  assert.equal(WIZARD_EVENTS.length, 6);
  let at = WIZARD_MACHINE.initial;
  for (let i = 0; i < WIZARD_STEPS.length; i += 1) {
    assert.equal(at, WIZARD_STEPS[i], "step " + String(i + 1));
    at = WIZARD_MACHINE.states[at][WIZARD_EVENTS[i]];
  }
  assert.equal(at, "submitted");
});

test("the_wizard_s_own_derivation_walks_the_machine_over_four_real_states", () => {
  // FOUR STATES THE PAGE ACTUALLY PRODUCES, each landing on a different step,
  // with the step NAMED here rather than recomputed from `replayWizard`'s own
  // rule -- which is what an earlier version of this arm did, and why it
  // asserted almost nothing.
  const backups = fixture("wizard-backups.json");
  const clusters = fixture("wizard-clusters.json");
  const uid = backups.items[0].metadata.uid;
  const build = (selection, edit) => {
    const state = initialState("team-a", clusters, backups, selection);
    if (typeof edit === "function") {
      edit(state);
    }
    return state;
  };
  const cases = [
    ["no recovery point chosen", build({}), "archive", []],
    ["a point outside the window it discloses",
      build({ uid: uid }, (s) => { s.fields.pointInTime = "1999-01-01T00:00:00Z"; }),
      "pointInTime", ["describeArchive", "choosePoint"]],
    ["a target with no topic prefix",
      build({ uid: uid }, (s) => { s.fields.target.topicPrefix = ""; }),
      "target", ["describeArchive", "choosePoint", "setPointInTime"]],
    ["every input whole", build({ uid: uid, backup: backups.items[0].metadata.name }),
      "submitted", WIZARD_EVENTS.slice()],
  ];
  for (const [said, state, current, events] of cases) {
    const walked = replayWizard(stepStates(state));
    assert.equal(walked.current, current, said + ": the walk stopped on the wrong step");
    assert.deepEqual(walked.events, events, said + ": the wrong moves were taken");
  }
});

/** Six step entries, as the wizard's own derivation shapes them. */
function derived(statuses, currentAt) {
  return statuses.map((status, i) => ({
    id: "step-" + String(i + 1), number: i + 1, title: WIZARD_STEPS[i],
    status: status, current: i === currentAt,
  }));
}

test("a_derivation_whose_current_step_is_not_the_step_it_blocked_on_cannot_be_walked", () => {
  assert.throws(
    () => replayWizard(derived(["done", "todo", "todo", "todo", "todo", "todo"], 4)),
    (error) => {
      assert.ok(isTransitionError(error));
      assert.equal(error.from, "recoveryPoint", "the walk stopped where the derivation blocked");
      assert.equal(error.event, "resume:preflight", "and the step it pointed at is named");
      return true;
    },
    "the stepper cannot point a viewer at a step the same derivation says is not reachable",
  );
  assert.throws(
    () => replayWizard(derived(["todo", "todo", "todo", "todo", "todo", "todo"], -1)),
    /resume:nowhere/,
    "and it must point somewhere",
  );
});

test("a_whole_derivation_walks_all_six_moves_and_lands_on_submitted", () => {
  const walked = replayWizard(derived(["done", "done", "done", "done", "done", "ready"], 5));
  assert.deepEqual(walked.events, WIZARD_EVENTS.slice());
  assert.equal(walked.state, "submitted");
  assert.equal(walked.current, "submitted");
  assert.throws(
    () => replayWizard(derived(["done", "done", "done", "done", "done", "ready"], 2)),
    /resume:pointInTime/,
    "a derivation with nothing left must say so by standing on the last step",
  );
});

test("a_later_step_may_be_finished_while_an_earlier_one_is_not", () => {
  // NOT A SKIP, AND THE REAL PAGE DOES THIS. All six sections are on screen at
  // once and each reports its own completeness: a wizard with no recovery
  // point chosen still has a reachable target cluster, so step 5 reads `done`
  // while step 1 does not. An earlier rule here forbade that and was wrong
  // about the page; driving the arm below over the real derivation is what
  // found it.
  const walked = replayWizard(derived(["todo", "todo", "attention", "todo", "done", "todo"], 0));
  assert.equal(walked.current, "archive");
  assert.deepEqual(walked.events, []);
});

test("a_derivation_that_is_not_six_steps_is_refused", () => {
  assert.throws(() => replayWizard([]), /the restore wizard has 6 steps; the derivation gave 0/);
  assert.throws(() => replayWizard(null), /the derivation gave 0/);
});

test("a_timed_out_attempt_whose_route_left_returns_to_idle_instead_of_throwing", async () => {
  // REPRODUCED FROM THE REVIEW. A submit that passes the timeout and THEN
  // finds its route gone settles as abandoned: `submitRestore` returns `null`
  // and the wizard turns that into `{outcome: "abandoned"}`. `unanswered` had
  // no `abandon`, so that late answer raised a TransitionError from inside a
  // promise nobody awaits -- an unhandled rejection in the browser -- and left
  // the record `unanswered` for good.
  let fire = null;
  const record = createMutation({
    timeoutMs: 10, setTimer: (fn) => { fire = fn; return 1; }, clearTimer: () => {},
  });
  let settle = null;
  const attempt = record.run(() => new Promise((resolve) => { settle = resolve; }));
  fire();
  await attempt;
  assert.equal(record.machineState, "unanswered");
  settle({ outcome: "abandoned" });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(record.machineState, "idle", "the record went back where the operator left it");
  assert.equal(record.state.phase, "idle");
  assert.deepEqual(record.transitions, ["start", "timeout", "abandon"]);
});

test("a_settle_that_cannot_move_the_record_leaves_it_alone_and_does_not_throw", () => {
  // The exported `send` still throws for a caller that can catch it -- the
  // arms above assert that -- but `run()`'s settle handlers publish from a
  // promise chain nobody awaits, so an illegal move there must not become an
  // unhandled rejection.
  const record = createMutation();
  assert.throws(() => record.send("succeed"), (error) => isTransitionError(error));
  assert.equal(record.machineState, "idle");
});
