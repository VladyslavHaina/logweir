// replication-factor.spec.js -- FX-5: the restore wizard's replication factor
// comes from what the page can read, is an input, is refused by the readiness
// check's own code when the target's brokers cannot hold it, and is shown on
// the review step with where it came from.
//
// THE DEFECT. `initialState` wrote `replicationFactor: 1` and step 4 printed it
// read-only, so every console restore created replication-factor-1 topics, on
// any cluster, and nothing on screen said that was a choice.
//
// WHAT THE PAGE CAN READ, AND WHAT IT CANNOT. The source's factor is recorded
// only in the archive manifest, which only the restore Job reads: no record
// this console reads carries it (`sourceReplicationFactorsOf` says so, and a
// row below holds it to that). The target's broker count is
// `TopicDiscovery.status.result.brokerCount`, which the product API now
// publishes as `brokerCount`, and the wizard reads it from the target
// connection's newest successful discovery when that discovery is fresh. So
// the default is the source's factor capped at the broker count when both are
// known (reached by the rule's own rows only, in this build), the broker count
// at most 3 when only that is known, and the grammar's 1 -- said, never
// silent -- when nothing is.
//
// NEGATIVE CONTROLS. Every assertion that carries "NEGATIVE CONTROL" names the
// value that makes it fail, and `claude/artifacts/fx-5/tools/ui-mutants.py`
// runs this file against copies of `ui/` with each behaviour removed.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  BROKERS_NOT_READ,
  DEFAULT_REPLICATION_CEILING,
  FIELD_STEP,
  GRAMMAR_REPLICATION_FACTOR,
  REPLICATION_UNKNOWN_WARNING,
  SOURCE_FACTOR_NOTE,
  WIZARD_DRAFT_FIELDS,
  WIZARD_TEXT_INPUTS,
  applyWizardDraft,
  firstStepWithErrors,
  initialState,
  mountRestoreWizard,
  preparePlan,
  preparePlanOrProblem,
  readinessRefusal,
  recoveryPoints,
  refreshTargetBrokers,
  renderPlanStep,
  renderRecoveryLimits,
  renderTargetStep,
  replicationChoice,
  replicationDefault,
  replicationFactorOf,
  replicationBasisText,
  replicationProblems,
  replicationText,
  setReplicationFactor,
  sourceReplicationFactorsOf,
  stepStates,
  submitRestore,
  syncReplicationDefault,
  targetBrokerFact,
  validateRestore,
  wizardDraftValues,
} from "../pages/restore-wizard.js";
import { MAX_REPLICATION_FACTOR, renderPlanBytes } from "../plan.js";
import { keepDraft, readDraft } from "../lifecycle.js";
import { fakeView, parse as viewParse } from "./fake-view.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

/** The TARGET connection's latest discovery, the fixture the product API's own
 *  row (`crates/logweir-api/tests/topic_discoveries.rs`,
 *  `a_discovery_publishes_the_broker_count_its_result_recorded`) holds its
 *  projection to: two brokers, fresh. */
const DISCOVERY = () => fixture("console/discovery-target-latest.json");
const BROKERS = DISCOVERY().lastSuccessful.brokerCount;

/** The wizard fixtures, with the target connection re-identified as the one the
 *  discovery fixture was taken of -- the same object the console fixtures and
 *  the Chromium journey use -- so the discovery is OF this target. */
function clusters() {
  const list = fixture("wizard-clusters.json");
  const target = list.items.find((c) => c.spec.role === "target");
  const bound = DISCOVERY().lastSuccessful.connection;
  target.metadata.uid = bound.uid;
  target.metadata.name = bound.name;
  return list;
}

function wizardState(ns) {
  const backups = fixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  return initialState(ns || "team-fx5", clusters(), backups, {
    uid: point.metadata.uid,
    backup: point.metadata.name,
  });
}

/** The target's broker count, as the mount half puts it on a state. */
function withBrokers(state, count) {
  const answer = DISCOVERY();
  answer.lastSuccessful.brokerCount = count;
  const cluster = state.clusters.items.find((c) => c.metadata.uid === state.targetClusterUid);
  state.targetBrokers = targetBrokerFact(answer, cluster);
  syncReplicationDefault(state);
  return state;
}

/** What an operator reads of a rendered fragment: no tags, entities decoded. */
const visible = (html) => html.replace(/<[^>]*>/g, "").replace(/&#39;/g, "'")
  .replace(/&quot;/g, "\"").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");

/** One element of rendered markup, by id: its opening tag through the text up
 *  to the next tag of the same name closing, enough for a paragraph or span. */
function byId(html, id) {
  const at = html.indexOf("id=\"" + id + "\"");
  if (at === -1) {
    return null;
  }
  const open = html.lastIndexOf("<", at);
  const name = /^<(\w+)/.exec(html.slice(open))[1];
  const close = html.indexOf("</" + name + ">", at);
  return html.slice(open, close === -1 ? at + 200 : close + name.length + 3);
}

// --------------------------------------------------------------- the rule

test("fx5_the_default_rule_source_capped_by_brokers_brokers_capped_at_three_else_the_grammar", () => {
  // THE SOURCE, when it is known: the LARGEST selected topic's factor.
  assert.deepEqual(replicationDefault([3, 2], null),
    { value: 3, basis: "source", source: 3, brokers: null },
    "NEGATIVE CONTROL: the smallest (2), or the grammar's 1, fails this: one value, the largest");
  assert.deepEqual(replicationDefault([3], 5), { value: 3, basis: "source", source: 3, brokers: 5 },
    "a target with room for it takes the source's factor as it is -- never the ceiling's");
  assert.deepEqual(replicationDefault([5], 6), { value: 5, basis: "source", source: 5, brokers: 6 },
    "NEGATIVE CONTROL: a source factor above the ceiling is not capped at 3 (3 fails this)");
  // CAPPED AT THE TARGET'S BROKER COUNT.
  assert.deepEqual(replicationDefault([3], 2), { value: 2, basis: "capped", source: 3, brokers: 2 },
    "NEGATIVE CONTROL: 3 -- a factor the target's two brokers cannot place -- fails this");
  // An unrecorded topic is skipped, not read as 1, and junk is not a factor.
  assert.deepEqual(replicationDefault([null, 2, 0, 40000, 2.5, "3"], null),
    { value: 2, basis: "source", source: 2, brokers: null });
  // ONLY THE BROKER COUNT: that count, at most three.
  assert.deepEqual(replicationDefault(null, 1), { value: 1, basis: "brokers", source: null, brokers: 1 });
  assert.deepEqual(replicationDefault(null, 2), { value: 2, basis: "brokers", source: null, brokers: 2 },
    "NEGATIVE CONTROL: 1 (the old literal) or 3 (the ceiling, above two brokers) fails this");
  assert.deepEqual(replicationDefault(null, 3), { value: 3, basis: "brokers", source: null, brokers: 3 });
  assert.deepEqual(replicationDefault([], 5),
    { value: DEFAULT_REPLICATION_CEILING, basis: "ceiling", source: null, brokers: 5 },
    "NEGATIVE CONTROL: 5 -- every broker of a larger cluster -- fails this");
  assert.equal(DEFAULT_REPLICATION_CEILING, 3, "the top of the Kafka guide's \"2 or 3\"");
  // NOTHING KNOWN: the grammar's own default, and a basis that says so.
  assert.deepEqual(replicationDefault(null, null),
    { value: GRAMMAR_REPLICATION_FACTOR, basis: "grammar", source: null, brokers: null });
  for (const junk of [0, -1, Number.NaN, "3", 2.5, undefined]) {
    assert.equal(replicationDefault(null, junk).basis, "grammar",
      "a broker count of " + String(junk) + " is no count at all");
  }
});

test("fx5_the_basis_is_said_in_the_words_the_review_step_prints", () => {
  // `replicationText` is this composition over a state's own choice; the two
  // source arms are reached here through the rule itself, because no state in
  // this build carries a source factor (see the next row).
  const text = (sources, brokers) => {
    const choice = replicationDefault(sources, brokers);
    return String(choice.value) + " (" + replicationBasisText(choice) + ")";
  };
  // The brief's own two examples, verbatim.
  assert.equal(text([3], 5), "3 (the source's)");
  assert.equal(text([3], 2), "2 (capped at the target's 2 brokers; the source's is 3)");
  assert.equal(text([3], null),
    "3 (the source's; the target's broker count is not known to this console, so it is not capped)");
  assert.equal(text(null, 2),
    "2 (the target's 2 brokers; the source's replication factor is not published to this console)");
  assert.equal(text(null, 1),
    "1 (the target's 1 broker; the source's replication factor is not published to this console)");
  assert.equal(text(null, 5), "3 (at most 3 by default, of the target's 5 brokers; the source's " +
    "replication factor is not published to this console)");
  assert.equal(text(null, null), "1 (the plan grammar's default: neither the source's replication " +
    "factor nor the target's broker count is known to this console)",
    "NEGATIVE CONTROL: a bare \"1\" -- the silent default -- fails this");
  // AND OVER A STATE, the same words for the same choice.
  const state = withBrokers(wizardState(), BROKERS);
  assert.equal(replicationText(state), text(null, BROKERS));
});

test("fx5_this_build_reads_no_source_factor_and_says_why", () => {
  // THE SOURCE ARM IS NOT LIVE, and this row is what says so. The archive
  // manifest is the one record of the source's factor and only the restore Job
  // reads it; when a projection lands (the receipt, the catalog, the product
  // API), `sourceReplicationFactorsOf` is the one function that changes, and
  // this row is meant to fail and be rewritten with it.
  const state = wizardState();
  assert.equal(sourceReplicationFactorsOf(state), null,
    "NEGATIVE CONTROL: any factor read from the point fails this row by design");
  withBrokers(state, BROKERS);
  assert.equal(replicationChoice(state).source, null);
  assert.ok(!replicationText(state).includes("the source's)"), replicationText(state));
  const step4 = renderTargetStep(state);
  assert.equal(visible(byId(step4, "replication-source") || ""), SOURCE_FACTOR_NOTE,
    "step 4 says where the source's factor is and why it is not read");
});

// ---------------------------------------------------- the broker-count fact

test("fx5_the_broker_count_comes_only_from_a_fresh_discovery_of_this_target", () => {
  const state = wizardState();
  const cluster = state.clusters.items.find((c) => c.metadata.uid === state.targetClusterUid);
  const fresh = targetBrokerFact(DISCOVERY(), cluster);
  assert.equal(fresh.count, BROKERS, "NEGATIVE CONTROL: null (the count not read) fails this");
  assert.equal(fresh.discovery, "td-target-counted");
  assert.equal(fresh.observedAt, DISCOVERY().lastSuccessful.observedAt);
  assert.equal(fresh.why, "");

  const stale = DISCOVERY();
  stale.lastSuccessful.stale = true;
  stale.lastSuccessful.staleReasons = ["expired"];
  const old = targetBrokerFact(stale, cluster);
  assert.equal(old.count, null,
    "NEGATIVE CONTROL: a stale discovery's 2 used as the target's count fails this");
  assert.match(old.why, /is stale \(expired\), so its broker count is not used/);
  assert.equal(old.discoverable, true, "and running a discovery again would help");

  const none = DISCOVERY();
  none.lastSuccessful = null;
  const never = targetBrokerFact(none, cluster);
  assert.equal(never.count, null);
  assert.equal(never.why, "no topic discovery of `orders-scratch` has succeeded");

  const uncounted = DISCOVERY();
  delete uncounted.lastSuccessful.brokerCount;
  assert.match(targetBrokerFact(uncounted, cluster).why, /recorded no broker count$/);
  const zero = DISCOVERY();
  zero.lastSuccessful.brokerCount = 0;
  assert.equal(targetBrokerFact(zero, cluster).count, null, "zero brokers is no count, not a cap");

  const other = DISCOVERY();
  other.lastSuccessful.connection.uid = "a-recreated-connection";
  const elsewhere = targetBrokerFact(other, cluster);
  assert.equal(elsewhere.count, null,
    "NEGATIVE CONTROL: a discovery of another object under this name is not this target");
  assert.match(elsewhere.why, /another connection under that name/);

  const legacy = targetBrokerFact({ unread: "Topic discovery is served by the product API" },
    cluster);
  assert.equal(legacy.count, null);
  assert.equal(legacy.discoverable, false, "no discovery to run where there is no route for one");
  assert.match(legacy.why, /could not be read: Topic discovery is served by the product API$/);

  assert.equal(targetBrokerFact(DISCOVERY(), null).why, "no target connection is selected");
  assert.deepEqual(targetBrokerFact(null, cluster).count, null, "nothing read, nothing known");
});

// ------------------------------------------------- the state and the steps

test("fx5_with_nothing_read_the_factor_is_the_grammar_default_and_says_so", async () => {
  const state = wizardState();
  assert.equal(replicationFactorOf(state), GRAMMAR_REPLICATION_FACTOR);
  assert.equal(state.replicationChosen, false);
  assert.equal(replicationChoice(state).basis, "grammar");
  const step4 = renderTargetStep(state);
  assert.ok(step4.includes("<input id=\"replication-factor\" name=\"replicationFactor\" " +
    "type=\"number\" min=\"1\" max=\"32767\" step=\"1\" inputmode=\"numeric\" value=\"1\">"),
  "NEGATIVE CONTROL: a read-only cell (the old step) fails this: the factor is an input: " +
    byId(step4, "replication"));
  const unknown = byId(step4, "replication-unknown");
  assert.ok(unknown !== null && unknown.startsWith("<p class=\"complaint\""),
    "NEGATIVE CONTROL: a silent 1 -- no warning -- fails this: " + step4);
  assert.equal(visible(unknown), REPLICATION_UNKNOWN_WARNING);
  assert.equal(visible(byId(step4, "replication-brokers")),
    "The target's broker count is not known to this page: the target's topic discoveries have " +
    "not been read.");
  const step6 = renderPlanStep(await preparePlan(state), state);
  assert.equal(visible(byId(step6, "review-replication")),
    "1 (the plan grammar's default: neither the source's replication factor nor the target's " +
    "broker count is known to this console)");
});

test("fx5_a_fresh_discovery_of_the_target_sets_the_default_and_the_review_says_where_it_came_from",
  async () => {
    const state = withBrokers(wizardState(), BROKERS);
    assert.equal(replicationFactorOf(state), 2,
      "NEGATIVE CONTROL: 1 -- the count not used -- fails this");
    const prepared = await preparePlan(state);
    assert.ok(prepared.bytes.includes("\n  default_replication_factor: 2\n"),
      "the plan an approver signs asks for it: " + prepared.bytes);
    const said = "2 (the target's 2 brokers; the source's replication factor is not published " +
      "to this console)";

    const step4 = renderTargetStep(state);
    assert.equal(visible(byId(step4, "replication-basis")), "This plan asks for " + said + ".");
    assert.equal(byId(step4, "replication-unknown"), null, "no warning when a count was read");
    assert.equal(visible(byId(step4, "replication-brokers")).replace(/ at .*?\. The/, " at <t>. The"),
      "The target has 2 brokers, as topic discovery td-target-counted read them at <t>. The " +
      "readiness check in step 5 reads them again when it runs.");
    assert.ok(visible(renderRecoveryLimits(state)).includes("target replication factor" + said),
      "the limits panel says the same");

    // THE REVIEW STEP: the value, and where it came from, above Create.
    const step6 = renderPlanStep(prepared, state);
    const review = byId(step6, "review-replication");
    assert.ok(review !== null, "NEGATIVE CONTROL: no review row fails this");
    assert.equal(visible(review), said,
      "NEGATIVE CONTROL: the value alone (\"2\") fails this: the review says where it came from");
    assert.ok(step6.indexOf("id=\"review-replication\"") < step6.indexOf("id=\"create-restore\""),
      "above Create");
    assert.equal(byId(step6, "review-replication-complaint"), null, "and nothing refuses it");
  });

test("fx5_a_larger_target_is_capped_at_three_and_a_single_broker_at_one", async () => {
  const big = withBrokers(wizardState(), 5);
  assert.equal(replicationFactorOf(big), 3, "NEGATIVE CONTROL: 5 fails this");
  assert.equal(visible(byId(renderPlanStep(await preparePlan(big), big), "review-replication")),
    "3 (at most 3 by default, of the target's 5 brokers; the source's replication factor is " +
    "not published to this console)");
  const one = withBrokers(wizardState(), 1);
  assert.equal(replicationFactorOf(one), 1);
  assert.equal(replicationChoice(one).basis, "brokers",
    "NEGATIVE CONTROL: \"grammar\" -- a 1 nobody read -- fails this: this 1 is the target's");
  assert.deepEqual(replicationProblems(one), Object.create(null));
});

// ---------------------------------------------- the edit and its refusal

test("fx5_an_edited_factor_above_the_brokers_is_refused_before_create_by_the_checks_code",
  async () => {
    const state = withBrokers(wizardState(), BROKERS);
    setReplicationFactor(state, " 3 ");
    assert.equal(state.replicationChosen, true);
    assert.equal(replicationFactorOf(state), 3);
    const problem = replicationProblems(state).replicationFactor;
    assert.equal(problem,
      "`ReplicationFactorExceedsBrokers`: the target's topic discovery `td-target-counted` read 2 " +
      "brokers, and a factor of 3 needs 3. A broker refuses it, and the readiness check's " +
      "`target.topicCreate` row refuses this plan with the same code; set 2 or fewer.",
    "NEGATIVE CONTROL: no refusal (undefined) fails this");
    // REFUSED BEFORE ANYTHING IS SENT, at the step that holds the input.
    const problems = validateRestore(state);
    assert.equal(problems.replicationFactor, problem);
    assert.equal(FIELD_STEP.replicationFactor, 3);
    assert.equal(firstStepWithErrors({ replicationFactor: [problem] }), 3, "step 4 holds it");
    const steps = stepStates(state, await preparePlan(state));
    assert.equal(steps[3].status, "attention", "NEGATIVE CONTROL: \"done\" fails this");
    assert.equal(steps[5].status, "todo", "and the plan step is not ready to create");
    // Step 4 says it on the commit, step 6 says it above Create, with a way back.
    const step4 = renderTargetStep(state);
    assert.equal(visible(byId(step4, "replication-complaint")), visible(problem.replace(/`/g, "")));
    assert.ok(byId(step4, "replication-complaint").includes("<code>ReplicationFactorExceedsBrokers</code>"),
      "the code is code, not backticks");
    const step6 = renderPlanStep(await preparePlan(state), state);
    const refused = byId(step6, "review-replication-complaint");
    assert.ok(refused !== null && step6.indexOf("id=\"review-replication-complaint\"") <
      step6.indexOf("id=\"create-restore\""),
    "NEGATIVE CONTROL: no refusal on the review step, or one below Create, fails this: " + step6);
    assert.ok(step6.includes("data-go-step=\"3\">Go to step 4: Replication factor</button>"),
      "a way back to the input, its caption short enough for a 390 px screen");
    assert.equal(visible(byId(step6, "review-replication")),
      "3 (set by you; the target has 2 brokers)");
    // AND THE SUBMIT REFUSES WITH IT, sending nothing.
    let sent = 0;
    const api = { create: async () => { sent += 1; return {}; }, get: async () => ({}) };
    await assert.rejects(submitRestore(state, api), (error) => {
      assert.equal(error.kind, "invalid");
      assert.equal(error.fields.replicationFactor, problem);
      return true;
    });
    assert.equal(sent, 0, "NEGATIVE CONTROL: a create that was sent (1) fails this");

    // THE BOUNDARY: the broker count itself is a factor the target holds.
    setReplicationFactor(state, "2");
    assert.deepEqual(replicationProblems(state), Object.create(null),
      "NEGATIVE CONTROL: refusing 2 on two brokers (>= instead of >) fails this");
    assert.equal(visible(byId(renderTargetStep(state), "replication-basis")),
      "This plan asks for 2 (set by you; the target has 2 brokers).");
  });

test("fx5_a_factor_no_broker_places_is_refused_and_renders_no_plan", async () => {
  for (const typed of ["0", "-1", "32768", "2.5", "two"]) {
    const state = wizardState();
    setReplicationFactor(state, typed);
    assert.match(replicationProblems(state).replicationFactor,
      /^a replication factor is a whole number from 1 to 32767, the largest the plan carries; /,
      typed);
    const prepared = await preparePlanOrProblem(state);
    assert.equal(typeof prepared.problem, "string",
      "NEGATIVE CONTROL: a hash for `" + typed + "` fails this: no document carries it");
    assert.match(prepared.problem, /^target\.replicationFactor must be a whole number/);
  }
  // THE EMITTER'S OWN BOUND, which holds for any caller.
  const fields = fixture("plan-fields.json");
  for (const value of [1, MAX_REPLICATION_FACTOR]) {
    fields.target.replicationFactor = value;
    assert.ok(renderPlanBytes(fields).includes("  default_replication_factor: " + String(value) + "\n"));
  }
  for (const value of [0, -1, MAX_REPLICATION_FACTOR + 1]) {
    fields.target.replicationFactor = value;
    assert.throws(() => renderPlanBytes(fields), RangeError,
      "NEGATIVE CONTROL: a document carrying " + String(value) + " fails this");
  }
  // EMPTY IS THE DEFAULT AGAIN, as an emptied prefix is.
  const state = withBrokers(wizardState(), BROKERS);
  setReplicationFactor(state, "3");
  setReplicationFactor(state, "   ");
  assert.equal(state.replicationChosen, false);
  assert.equal(replicationFactorOf(state), 2, "NEGATIVE CONTROL: 3 kept, or NaN, fails this");
});

test("fx5_the_readiness_checks_own_refusal_still_holds_create", async () => {
  // THE EXISTING CHECK, unchanged: the readiness check's validate-only
  // CreateTopics answers `ReplicationFactorExceedsBrokers` for a factor the
  // target cannot place -- whatever this page knew -- and Create refuses it.
  const state = wizardState();
  const prepared = await preparePlan(state);
  const ready = (id, code) => ({ id: id, category: id.split(".")[0], state: "ready",
    gating: "blocking", authority: "checkJob", code: code });
  state.readiness = {
    boundHash: prepared.hash,
    preflight: {
      id: "pf-fx5", operation: "restore", state: "notReady", terminal: true, applicable: true,
      stale: false, staleReasons: [], staleBasis: ["plan"], binding: { planHash: prepared.hash },
      checks: [
        ready("target.authenticated", "Authenticated"),
        ready("target.mappedTopics", "MappedTopicsAbsent"),
        { id: "target.topicCreate", category: "target", state: "notReady", gating: "blocking",
          authority: "checkJob", code: "ReplicationFactorExceedsBrokers",
          message: "1 of 1 mapped topic(s) were refused by a validate-only CreateTopics" },
      ],
      warnings: [], executionOnly: [], detailsAvailable: false, conditions: [],
    },
  };
  assert.match(readinessRefusal(state, prepared),
    /: target\.topicCreate \(ReplicationFactorExceedsBrokers\)\. Nothing is sent/);
  assert.ok(renderPlanStep(prepared, state).includes(
    "id=\"create-restore\" class=\"primary\" disabled"), "Create is disabled");
});

// ------------------------------------------------------- the draft

test("fx5_a_factor_the_operator_set_survives_the_draft_and_a_default_does_not", () => {
  assert.ok(WIZARD_DRAFT_FIELDS.includes("replicationFactor"));
  assert.ok(WIZARD_TEXT_INPUTS.includes("replication-factor"),
    "a repaint waits while the factor is being typed");
  const state = withBrokers(wizardState(), BROKERS);
  assert.equal(wizardDraftValues(state).replicationFactor, "", "a default is not kept");
  setReplicationFactor(state, "1");
  assert.equal(wizardDraftValues(state).replicationFactor, "1",
    "NEGATIVE CONTROL: the number 1 fails this -- `keepDraft` drops numbers without a word");
  // THROUGH THE REAL DRAFT STORE, which keeps strings and booleans only.
  const key = "fx5-draft-probe";
  keepDraft(key, wizardDraftValues(state), WIZARD_DRAFT_FIELDS);
  const kept = readDraft(key);
  assert.equal(kept.replicationFactor, "1");
  const again = withBrokers(wizardState(), BROKERS);
  assert.equal(applyWizardDraft(again, kept), true);
  assert.equal(again.replicationChosen, true);
  assert.equal(replicationFactorOf(again), 1,
    "NEGATIVE CONTROL: 2 -- the default overriding what was set -- fails this");
  // A draft with no factor leaves the default to follow what this mount reads.
  const older = withBrokers(wizardState(), BROKERS);
  applyWizardDraft(older, Object.assign({}, kept, { replicationFactor: "" }));
  assert.equal(older.replicationChosen, false);
  assert.equal(replicationFactorOf(older), 2);
});

// ------------------------------------------------------- the mount half

function mountApi(answers, asked) {
  const backups = fixture("wizard-backups.json");
  return {
    list: async (_ns, plural) => (plural === "kafkaclusters" ? clusters() : backups),
    approvalPolicy: async () => null,
    latestDiscoveries: async (ns, name) => {
      asked.push(ns + "/" + name);
      const answer = answers[name];
      if (answer instanceof Error) {
        throw answer;
      }
      return answer === undefined ? { lastSuccessful: null, latestAttempt: null } : answer;
    },
  };
}

function pointParams() {
  const point = recoveryPoints(fixture("wizard-backups.json"))[0];
  return { uid: point.metadata.uid, backup: point.metadata.name };
}

test("fx5_the_mount_reads_the_targets_discovery_before_the_first_paint", async () => {
  const asked = [];
  const view = fakeView();
  await mountRestoreWizard(view.root, "team-fx5-mount", pointParams(), viewParse,
    mountApi({ "orders-scratch": DISCOVERY() }, asked));
  assert.deepEqual(asked, ["team-fx5-mount/orders-scratch"],
    "the TARGET's discoveries, by its name, once");
  const input = view.find("#replication-factor");
  assert.equal(input.getAttribute("value"), "2",
    "NEGATIVE CONTROL: 1 -- the first paint before the count -- fails this");
  assert.match(visible(view.html()), /This plan asks for 2 \(the target's 2 brokers;/);
});

test("fx5_another_target_is_read_again_and_a_typed_factor_is_kept", async () => {
  const asked = [];
  const view = fakeView();
  const source = fixture("wizard-clusters.json").items.find((c) => c.spec.role === "source");
  await mountRestoreWizard(view.root, "team-fx5-switch", pointParams(), viewParse,
    mountApi({ "orders-scratch": DISCOVERY() }, asked));
  // ANOTHER FIELD'S COMMIT reads every control; the factor shown is the default
  // and must stay one.
  const prefix = view.find("#topic-prefix");
  prefix.value = "fx5-";
  await prefix.dispatch("change");
  assert.match(visible(view.html()), /This plan asks for 2 \(the target's 2 brokers;/,
    "NEGATIVE CONTROL: \"set by you\" -- the default read back as a choice -- fails this");

  // ANOTHER TARGET: its own discoveries are read, and it has none.
  const select = view.find("#target-cluster");
  select.value = source.metadata.uid;
  await select.dispatch("change");
  assert.deepEqual(asked, ["team-fx5-switch/orders-scratch", "team-fx5-switch/orders-prod"]);
  assert.equal(view.find("#replication-factor").getAttribute("value"), "1",
    "NEGATIVE CONTROL: 2 -- the previous target's cap -- fails this");
  assert.match(visible(view.html()),
    /The target's broker count is not known to this page: no topic discovery of orders-prod has succeeded\. Open orders-prod and run Discover topics/);

  // A FACTOR THE OPERATOR TYPES is theirs, and moving back keeps it.
  const input = view.find("#replication-factor");
  input.value = "3";
  await input.dispatch("change");
  const back = view.find("#target-cluster");
  back.value = DISCOVERY().lastSuccessful.connection.uid;
  await back.dispatch("change");
  assert.equal(view.find("#replication-factor").getAttribute("value"), "3",
    "NEGATIVE CONTROL: 2 -- the target's default over what was typed -- fails this");
  const html = visible(view.html());
  assert.match(html, /This plan asks for 3 \(set by you; the target has 2 brokers\)\./);
  assert.match(html, /ReplicationFactorExceedsBrokers: the target's topic discovery td-target-counted read 2 brokers, and a factor of 3 needs 3\./);
});

test("fx5_a_late_answer_for_a_target_since_left_is_dropped", async () => {
  const state = wizardState("team-fx5-late");
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const api = { latestDiscoveries: async () => { await gate; return DISCOVERY(); } };
  const pending = refreshTargetBrokers(state, api);
  // The operator moves to the other connection before the answer lands.
  const source = state.clusters.items.find((c) => c.spec.role === "source");
  state.targetClusterUid = source.metadata.uid;
  release();
  assert.equal(await pending, false,
    "NEGATIVE CONTROL: true -- the first target's count capping the second's -- fails this");
  assert.equal(state.targetBrokers, BROKERS_NOT_READ);
  assert.equal(replicationFactorOf(state), 1);
});

test("fx5_legacy_mode_has_no_discovery_and_says_so_with_the_grammar_default", async () => {
  const asked = [];
  const view = fakeView();
  const refusal = new Error("Topic discovery is served by the Logweir product API and not by " +
    "the Kubernetes API this page is talking to.");
  await mountRestoreWizard(view.root, "team-fx5-legacy", pointParams(), viewParse,
    mountApi({ "orders-scratch": refusal }, asked));
  const html = visible(view.html());
  assert.equal(view.find("#replication-factor").getAttribute("value"), "1");
  assert.ok(html.includes(REPLICATION_UNKNOWN_WARNING), "the 1 is said, never silent");
  assert.match(html, /The target's broker count is not known to this page: the target's topic discoveries could not be read: Topic discovery is served by the Logweir product API/);
  assert.ok(!html.includes("run Discover topics;"),
    "no discovery to run where there is no route for one");
});

// ------------------------------------- the class: a draft value the store drops

import { catalogRecoveryPoint, selectedTopics, setCatalogTopics } from "../pages/restore-wizard.js";

/** A catalog point the wizard restores from, with no Backup behind it. */
function catalogPointState(ns) {
  const destination = fixture("console/destination.json").item;
  const pointId = "lwp1-0123456789abcdef0123456789abcdef";
  const point = catalogRecoveryPoint(
    { apiVersion: "logweir.dev/v1alpha1", kind: "RecoveryCatalog",
      metadata: { name: "archive", namespace: ns, uid: "cat-uid" },
      spec: { destinationRef: { name: destination.name } }, status: {} },
    { pointId: pointId, backupId: "set-fx5", runId: "01JB7Z00000000000000000000",
      recoveryPointAt: "2026-09-22T14:00:00Z", coveredFrom: "2026-09-22T13:00:00Z",
      coveredTo: "2026-09-22T14:00:00Z", availability: "Available", verification: "Verified",
      selectable: true, signerKeyId: "c".repeat(64),
      receiptKey: "logweir/backups/set-fx5/01JB7Z00000000000000000000.receipt.json",
      receiptSha256: "sha256:" + "a1".repeat(32), manifestKey: "set-fx5/manifest.json",
      manifestSha256: "sha256:" + "b2".repeat(32),
      locations: [{ locationId: "s3://kafka-backups/team-a/prod", availability: "Available" }] },
    destination, null);
  return initialState(ns, clusters(), { items: [] }, { catalog: "archive", point: pointId },
    destination, undefined, { point: point });
}

test("fx5_class_every_wizard_draft_value_is_one_the_draft_store_keeps", () => {
  // THE CLASS (FX-5's sweep). `keepDraft` keeps strings and booleans and DROPS
  // anything else without a word. The replication factor met it as a number;
  // the topic subset and a catalog point's typed topic list had met it as
  // arrays since they were added, so a re-mount put every frozen topic back
  // under "your unsubmitted edits ... are back". Every value the wizard hands
  // the store is held to the store's two types, over both kinds of point.
  const backupPoint = withBrokers(wizardState("team-fx5-class"), BROKERS);
  const catalogPoint = catalogPointState("team-fx5-class-cat");
  setCatalogTopics(catalogPoint, ["orders", "payments"]);
  for (const state of [backupPoint, catalogPoint]) {
    setReplicationFactor(state, "2");
    const values = wizardDraftValues(state);
    for (const field of WIZARD_DRAFT_FIELDS) {
      const kind = typeof values[field];
      assert.ok(kind === "string" || kind === "boolean",
        "NEGATIVE CONTROL: `" + field + "` is a " + (Array.isArray(values[field]) ? "array" : kind) +
          ", which keepDraft drops without a word");
    }
  }
});

test("fx5_class_the_subset_and_a_catalog_points_topics_survive_the_draft", () => {
  // THE BACKUP POINT'S SUBSET: one topic of two, through the real store.
  const state = wizardState("team-fx5-subset");
  state.fields.topics = ["orders"];
  keepDraft("fx5-subset-probe", wizardDraftValues(state), WIZARD_DRAFT_FIELDS);
  const again = wizardState("team-fx5-subset");
  assert.equal(applyWizardDraft(again, readDraft("fx5-subset-probe")), true);
  assert.deepEqual(selectedTopics(again), ["orders"],
    "NEGATIVE CONTROL: [\"orders\", \"payments\"] -- every frozen topic back -- fails this");
  // AN EMPTY KEPT SUBSET IS A REAL EDIT, and comes back as one.
  const none = wizardState("team-fx5-subset");
  none.fields.topics = [];
  keepDraft("fx5-subset-none", wizardDraftValues(none), WIZARD_DRAFT_FIELDS);
  const emptied = wizardState("team-fx5-subset");
  applyWizardDraft(emptied, readDraft("fx5-subset-none"));
  assert.deepEqual(selectedTopics(emptied), [], "the refused empty subset, not all of them");

  // A CATALOG POINT'S TYPED LIST, and the subset over it.
  const typed = catalogPointState("team-fx5-cat");
  setCatalogTopics(typed, ["orders", "payments"]);
  typed.fields.topics = ["payments"];
  keepDraft("fx5-catalog-probe", wizardDraftValues(typed), WIZARD_DRAFT_FIELDS);
  const back = catalogPointState("team-fx5-cat");
  assert.equal(applyWizardDraft(back, readDraft("fx5-catalog-probe")), true);
  assert.deepEqual(back.point.spec.topics, ["orders", "payments"],
    "NEGATIVE CONTROL: [] -- the typed list lost -- fails this");
  assert.deepEqual(selectedTopics(back), ["payments"]);
  // A draft built by hand with arrays (the suites' older rows) still applies.
  const byHand = catalogPointState("team-fx5-cat");
  applyWizardDraft(byHand, Object.assign({}, readDraft("fx5-catalog-probe"),
    { catalogTopics: ["orders"], topics: ["orders"] }));
  assert.deepEqual(selectedTopics(byHand), ["orders"]);
});
