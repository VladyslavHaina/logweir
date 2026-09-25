// workflow.js -- the state machines, with named transitions and transition
// errors.
//
// WHY NAMES. Before PLAT-18.1 the mutation record's states existed but its
// transitions did not: a settle was `publish({phase: "succeeded", ...})` at
// one call site, a timeout was the same call with different literals at
// another, and "may this record settle now?" was a boolean expression spelled
// out beside each of them. Nothing named the move, so nothing could refuse an
// illegal one and no test could assert that the moves happened in the order
// the contract says they do.
//
// A MACHINE HERE IS THE SMALLEST THING THAT FIXES THAT: a table of states, the
// events each one accepts, and the state each event leads to. Sending an event
// a state does not accept is a [`TransitionError`] naming the state, the event
// and the events that WOULD have been accepted -- never a silent no-op, which
// is the failure mode PLAT-18.1's acceptance calls out by name.
//
// WHAT A MACHINE HERE IS NOT. It holds no data, runs no side effect, starts no
// timer and knows nothing about a request. The mutation record in
// `lifecycle.js` owns all of that and asks this module one question --
// "is this move legal, and where does it land?" -- for every move it makes.
// That separation is what lets the whole transition table be read in one
// screen and driven from `node --test` with no clock and no network.
//
// THIS MODULE ISSUES NO REQUEST AND TOUCHES NO DOM.

/** @typedef {{from: string, event: string, to: string}} Transition */

/** The name every transition error carries, so a caller can branch on it
 *  without matching prose. */
export const TRANSITION_ERROR = "TransitionError";

/** An Error for an event a state does not accept.
 *
 *  It carries `from`, `event` and `allowed` as data, and `kind: "transition"`
 *  so `lifecycle.js`'s `failureKind` reports it as itself. */
export function transitionError(machine, from, event, allowed) {
  const error = new Error(
    machine + ": " + JSON.stringify(event) + " is not a move this workflow makes from " +
      JSON.stringify(from) + ". From here it accepts: " +
      (allowed.length === 0 ? "nothing -- this state is final" : allowed.join(", ")) + ".",
  );
  error.name = TRANSITION_ERROR;
  error.kind = "transition";
  error.machine = machine;
  error.from = from;
  error.event = event;
  error.allowed = allowed;
  return error;
}

/** True for an Error [`transitionError`] made. */
export function isTransitionError(error) {
  return error !== null && typeof error === "object" && error.name === TRANSITION_ERROR;
}

/** Declares a machine: `{name, initial, states: {state: {event: state}}}`.
 *
 *  Frozen, and checked at declaration time: an event whose target is not a
 *  declared state is a TypeError HERE, at module load, rather than a surprise
 *  on the one path that sends it. */
export function defineMachine(definition) {
  const d = definition || {};
  const states = d.states || {};
  for (const from of Object.keys(states)) {
    for (const event of Object.keys(states[from])) {
      const to = states[from][event];
      if (!Object.prototype.hasOwnProperty.call(states, to)) {
        throw new TypeError(
          d.name + ": " + from + " --" + event + "--> " + to + " names a state this machine " +
            "does not declare",
        );
      }
    }
  }
  if (!Object.prototype.hasOwnProperty.call(states, d.initial)) {
    throw new TypeError(d.name + ": the initial state " + String(d.initial) + " is not declared");
  }
  return Object.freeze({
    name: d.name,
    initial: d.initial,
    states: Object.freeze(states),
    /** The events `state` accepts, sorted, so a message reads the same twice. */
    accepts(state) {
      const on = states[state];
      return on === undefined ? [] : Object.keys(on).sort();
    },
  });
}

/** One running instance of a declared machine.
 *
 *  `send(event)` returns the state it moved to and THROWS on an event the
 *  current state does not accept. `can(event)` answers the same question
 *  without moving, for the two callers that have a documented reason to
 *  decline rather than to fail -- a second submit while one is in flight, and
 *  a clear of a record whose answer is still on its way. */
export function startMachine(machine, options) {
  const opts = options || {};
  let state = typeof opts.initial === "string" ? opts.initial : machine.initial;
  if (!Object.prototype.hasOwnProperty.call(machine.states, state)) {
    throw new TypeError(machine.name + ": " + String(state) + " is not a declared state");
  }
  /** @type {Transition[]} */
  const history = [];
  return {
    get state() {
      return state;
    },
    get transitions() {
      return history.slice();
    },
    /** The names of the transitions taken, in order. */
    get events() {
      return history.map((t) => t.event);
    },
    can(event) {
      const on = machine.states[state];
      return on !== undefined && Object.prototype.hasOwnProperty.call(on, event);
    },
    accepts() {
      return machine.accepts(state);
    },
    send(event) {
      const on = machine.states[state];
      if (on === undefined || !Object.prototype.hasOwnProperty.call(on, event)) {
        throw transitionError(machine.name, state, event, machine.accepts(state));
      }
      const to = on[event];
      history.push({ from: state, event: event, to: to });
      state = to;
      return to;
    },
  };
}

// ===========================================================================
// the mutation machine -- every write this page makes
// ===========================================================================
//
// FIVE STATES. `idle` before anything is sent and after a settled record is
// cleared; `pending` from before the executor's first await until an answer;
// `succeeded` and `failed` for the two answers; and `unanswered` for the one
// outcome that is neither -- a request that was NOT cancelled, whose answer
// has not arrived, and which may or may not have created the object. The
// record publishes `unanswered` as `phase: "failed", kind: "unknown",
// timedOut: true`, which is the shape every page already renders; the machine
// keeps it a separate PLACE because it is the only state a late answer may
// still settle.
//
// `start` IS ABSENT FROM `pending` ON PURPOSE. That absence is the
// duplicate-submission guard: `run()` asks `can("start")` and declines, and a
// caller that sends it anyway gets a transition error naming the state it is
// in.
//
// `unanswered` ACCEPTS EVERY ANSWER `pending` DOES, AND THAT INCLUDES
// `abandon`. A submit that passes the timeout and then finds its route gone
// settles as abandoned -- the wizard's `submitRestore` returns `null` for
// exactly that, and the record must go back to `idle` where the operator left
// it. Without this arm that late answer raised a TransitionError from inside a
// promise nobody was awaiting, and the record stayed `unanswered` for good.
export const MUTATION_MACHINE = defineMachine({
  name: "mutation",
  initial: "idle",
  states: {
    idle: { start: "pending", clear: "idle" },
    pending: { succeed: "succeeded", fail: "failed", timeout: "unanswered", abandon: "idle" },
    unanswered: {
      succeed: "succeeded", fail: "failed", abandon: "idle", start: "pending", clear: "idle",
    },
    succeeded: { start: "pending", clear: "idle" },
    failed: { start: "pending", clear: "idle" },
  },
});

/** The record phase each machine state publishes, and the `timedOut` flag that
 *  goes with it. The pages read `phase`; this table is the whole of what the
 *  machine adds to the shape they already know. */
export const MUTATION_PHASES = Object.freeze({
  idle: Object.freeze({ phase: "idle", timedOut: false }),
  pending: Object.freeze({ phase: "pending", timedOut: false }),
  unanswered: Object.freeze({ phase: "failed", timedOut: true }),
  succeeded: Object.freeze({ phase: "succeeded", timedOut: false }),
  failed: Object.freeze({ phase: "failed", timedOut: false }),
});

// ===========================================================================
// the restore wizard -- six steps, in one order
// ===========================================================================
//
// THE MACHINE IS THE ORDER THE SECTIONS ALREADY HAVE, WRITTEN DOWN. All six
// sections are on the page at once, so this is not a router: it is the
// statement that a recovery point cannot be chosen before an archive is known,
// that a point in time is meaningless without a recovery point, and that the
// plan is reviewable only once the five input steps are whole.
//
// `replayWizard` takes the wizard's OWN derived step states -- the array
// `pages/restore-wizard.js`'s `stepStates` returns -- and walks this machine
// through them, stopping at the first step that is not finished. THE ONE
// INVARIANT IT HOLDS is that the step the walk stops on is the step the
// derivation calls `current`: the stepper may not point a viewer at a place
// the same derivation says is not reachable yet.
//
// IT IS NOT "NO LATER STEP MAY BE FINISHED", and an earlier draft of this
// function said that and was wrong about the page. All six sections are on
// screen at once and each reports its OWN completeness, so a wizard with no
// recovery point chosen still has a reachable target cluster and step 5 reads
// `done` while step 1 does not. Driving this over the real `stepStates` output
// is what found that; the rule below is the one the page actually keeps.
//
// THIS MACHINE IS A TEST-TIME INVARIANT AND THE PAGE DOES NOT ENFORCE IT.
// `stepStates` and `validateRestore` legitimately disagree today -- the
// stepper wants a reachable target and the submit does not -- so wiring the
// walk into the render path would CHANGE BEHAVIOUR, which PLAT-18.1 must not.
// PLAT-18.2 owns the stepper and is where the two are reconciled and this is
// enforced. `ui/README.md` says the same, so no reader assumes otherwise.
export const WIZARD_STEPS = Object.freeze([
  "archive", "recoveryPoint", "pointInTime", "target", "preflight", "plan",
]);

/** The named move OUT of each step, in order. */
export const WIZARD_EVENTS = Object.freeze([
  "describeArchive", "choosePoint", "setPointInTime", "setTarget", "confirmTarget", "submit",
]);

export const WIZARD_MACHINE = defineMachine({
  name: "restore-wizard",
  initial: "archive",
  states: {
    archive: { describeArchive: "recoveryPoint" },
    recoveryPoint: { choosePoint: "pointInTime", back: "archive" },
    pointInTime: { setPointInTime: "target", back: "recoveryPoint" },
    target: { setTarget: "preflight", back: "pointInTime" },
    preflight: { confirmTarget: "plan", back: "target" },
    plan: { submit: "submitted", back: "preflight" },
    submitted: {},
  },
});

/** Walks [`WIZARD_MACHINE`] through the step states the wizard derived.
 *
 *  Every finished step is a named move forward; the first unfinished one is
 *  where the walk stops, and that step must be the one the derivation calls
 *  `current`. A derivation with nothing left must call the LAST step current,
 *  because that is where a viewer with every input whole is standing.
 *
 *  Returns `{state, events, current}`. Throws a transition error when the
 *  derivation points somewhere the walk cannot reach, and a TypeError when it
 *  is not six steps. */
export function replayWizard(steps) {
  const list = Array.isArray(steps) ? steps : [];
  if (list.length !== WIZARD_STEPS.length) {
    throw new TypeError(
      "the restore wizard has " + String(WIZARD_STEPS.length) + " steps; the derivation gave " +
        String(list.length),
    );
  }
  const run = startMachine(WIZARD_MACHINE);
  let blocked = -1;
  for (let i = 0; i < list.length; i += 1) {
    const step = list[i] || {};
    // `unchecked` is step 5 with no readiness check held (MCP-27): not DONE --
    // nothing checked it -- and not a block either, because the check is
    // advisory and the plan may be created without one, with a warning.
    const done = step.status === "done" || step.status === "ready" ||
      step.status === "unchecked";
    if (!done) {
      blocked = i;
      break;
    }
    // A `done` step after a step that was not done is a skip, and `send`
    // refuses it: the machine is still standing on the blocked step and has no
    // event that reaches this one.
    run.send(WIZARD_EVENTS[i]);
  }
  const at = blocked === -1 ? WIZARD_STEPS.length : blocked;
  const current = at === WIZARD_STEPS.length ? "submitted" : WIZARD_STEPS[at];
  // WHERE THE DERIVATION SAYS THE VIEWER IS. One entry carries `current`, and
  // it must be the step the walk stopped on -- or, for a derivation with
  // nothing left to do, the last one.
  const claimed = list.findIndex((step) => step.current === true);
  const expected = blocked === -1 ? WIZARD_STEPS.length - 1 : blocked;
  if (claimed !== expected) {
    throw transitionError(
      WIZARD_MACHINE.name,
      run.state,
      "resume:" + (claimed === -1 ? "nowhere" : WIZARD_STEPS[claimed]),
      run.accepts(),
    );
  }
  return { state: run.state, events: run.events, current: current };
}
