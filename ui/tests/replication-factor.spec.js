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
// in the archive manifest; since PROD-05.1 the receipt projects it, the
// catalog point copies it and a recovery catalog's view lists it per topic
// (`PointView.topics[]`), which `sourceReplicationFactorsOf` reads -- the
// PROD-05.1 rows at the end of this file. The target's broker count is
// `TopicDiscovery.status.result.brokerCount`, which the product API now
// publishes as `brokerCount`, and the wizard reads it from the target
// connection's newest successful discovery when that discovery is fresh. So
// the default is the source's factor capped at the broker count when both are
// known, the broker count
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
  REPLICATION_DIFFERS_NOTE,
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
  replicationMayDiffer,
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
const UI_DIR = fileURLToPath(new URL("../", import.meta.url));
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
  // THE CEILING'S BOUNDARY (FX-5 review L1): the FIRST count above it is
  // capped, not only a count well above it.
  assert.deepEqual(replicationDefault(null, 4),
    { value: DEFAULT_REPLICATION_CEILING, basis: "ceiling", source: null, brokers: 4 },
    "NEGATIVE CONTROL: 4 -- the ceiling one broker late (`count > CEILING + 1`) -- fails this");
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

test("fx5_a_backup_with_no_catalog_row_reads_no_source_factor_and_says_why", () => {
  // PROD-05.1 REWROTE THIS ROW, as FX-5 said it would: the source's factor is
  // now read from a recovery catalog's view of the point. A state whose source
  // facts were never read -- or a Backup no catalog lists -- still knows none,
  // and step 4 says where it would come from AND why it is not known here.
  const state = wizardState();
  assert.equal(sourceReplicationFactorsOf(state), null,
    "NEGATIVE CONTROL: any factor read without a catalog row fails this row");
  withBrokers(state, BROKERS);
  assert.equal(replicationChoice(state).source, null);
  assert.ok(!replicationText(state).includes("the source's)"), replicationText(state));
  const step4 = renderTargetStep(state);
  assert.equal(visible(byId(step4, "replication-source") || ""),
    SOURCE_FACTOR_NOTE + " For this point: the recovery catalogs of this namespace have not " +
    "been read.", "step 4 says where the source's factor is and why it is not read");
});

// ---------------------------------------------------- the broker-count fact

test("fx5_the_broker_count_comes_from_this_targets_discovery_fresh_or_expired_alone", () => {
  const state = wizardState();
  const cluster = state.clusters.items.find((c) => c.metadata.uid === state.targetClusterUid);
  const fresh = targetBrokerFact(DISCOVERY(), cluster);
  assert.equal(fresh.count, BROKERS, "NEGATIVE CONTROL: null (the count not read) fails this");
  assert.equal(fresh.fresh, true, "a fresh count, which may refuse a factor");
  assert.equal(fresh.discovery, "td-target-counted");
  assert.equal(fresh.observedAt, DISCOVERY().lastSuccessful.observedAt);
  assert.equal(fresh.why, "");

  // PAST ITS FRESHNESS ALONE (review L5): the same connection object,
  // generation and principal, read more than `freshSeconds` ago. The count is
  // kept, NOT fresh, and running a discovery again would make it fresh.
  const expired = DISCOVERY();
  expired.lastSuccessful.stale = true;
  expired.lastSuccessful.staleReasons = ["expired"];
  const old = targetBrokerFact(expired, cluster);
  assert.equal(old.count, BROKERS,
    "NEGATIVE CONTROL: null -- the 15-minute window that made the default 1 on most installs -- " +
      "fails this");
  assert.equal(old.fresh, false,
    "NEGATIVE CONTROL: true -- an expired count treated as fresh, so it would refuse -- fails this");
  assert.equal(old.why, "");
  assert.equal(old.discoverable, true, "and running a discovery again would help");

  // ANY OTHER STALE REASON is about another connection or another identity,
  // alone or beside `expired`: no count.
  for (const reasons of [["connectionChanged"], ["expired", "connectionChanged"],
    ["principalChanged"], ["expired", "principalChanged"], ["connectionReplaced"], []]) {
    const other = DISCOVERY();
    other.lastSuccessful.stale = true;
    other.lastSuccessful.staleReasons = reasons;
    const fact = targetBrokerFact(other, cluster);
    assert.equal(fact.count, null,
      "NEGATIVE CONTROL: a count stale for " + JSON.stringify(reasons) + " used fails this");
    assert.equal(fact.fresh, false);
    assert.match(fact.why, /is stale.*, so its broker count is not used$/);
  }
  const edited = DISCOVERY();
  edited.lastSuccessful.stale = true;
  edited.lastSuccessful.staleReasons = ["expired", "connectionChanged"];
  assert.match(targetBrokerFact(edited, cluster).why,
    /is stale \(expired, connectionChanged\), so its broker count is not used$/);

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

  const elsewhere = (expiredToo) => {
    const other = DISCOVERY();
    other.lastSuccessful.connection.uid = "a-recreated-connection";
    if (expiredToo) {
      other.lastSuccessful.stale = true;
      other.lastSuccessful.staleReasons = ["expired"];
    }
    return targetBrokerFact(other, cluster);
  };
  for (const expiredToo of [false, true]) {
    assert.equal(elsewhere(expiredToo).count, null,
      "NEGATIVE CONTROL: a discovery of another object under this name is not this target" +
        (expiredToo ? ", expired or not" : ""));
    assert.match(elsewhere(expiredToo).why, /another connection under that name/);
  }

  const legacy = targetBrokerFact({ unread: "Topic discovery is served by the product API" },
    cluster);
  assert.equal(legacy.count, null);
  assert.equal(legacy.discoverable, false, "no discovery to run where there is no route for one");
  assert.match(legacy.why, /could not be read: Topic discovery is served by the product API$/);

  assert.equal(targetBrokerFact(DISCOVERY(), null).why, "no target connection is selected");
  assert.deepEqual(targetBrokerFact(null, cluster).count, null, "nothing read, nothing known");
  assert.equal(BROKERS_NOT_READ.fresh, false);
});

test("fx5_a_count_past_its_freshness_sets_the_default_with_its_age_and_refuses_nothing",
  async () => {
    // FX-5 review L5. A discovery whose only stale reason is `expired` still
    // sets the default -- the broker count rarely changes -- and the basis says
    // when it was read, on both steps. It refuses nothing: a fresh count may
    // refuse, an old one could refuse a factor a cluster grown since would hold.
    const expired = () => {
      const answer = DISCOVERY();
      answer.lastSuccessful.stale = true;
      answer.lastSuccessful.staleReasons = ["expired"];
      return answer;
    };
    const asOf = " as of " + DISCOVERY().lastSuccessful.observedAt.replace("T", " ")
      .replace(/(\.\d+)?Z$/, " UTC");
    const withAnswer = (answer) => {
      const state = wizardState();
      const cluster = state.clusters.items.find((c) => c.metadata.uid === state.targetClusterUid);
      state.targetBrokers = targetBrokerFact(answer, cluster);
      syncReplicationDefault(state);
      return state;
    };
    const state = withAnswer(expired());
    assert.equal(replicationFactorOf(state), BROKERS,
      "NEGATIVE CONTROL: 1 -- the grammar's default because the count was 16 minutes old -- " +
        "fails this");
    const said = "2 (the target's 2 brokers" + asOf + "; the source's replication factor is not " +
      "published to this console)";
    assert.equal(replicationText(state), said,
      "NEGATIVE CONTROL: the fresh wording (no \"as of\") for an old count fails this");
    const step4 = renderTargetStep(state);
    assert.equal(visible(byId(step4, "replication-basis")), "This plan asks for " + said + ".");
    assert.equal(byId(step4, "replication-unknown"), null, "a count was read: no warning");
    assert.match(visible(byId(step4, "replication-brokers")),
      /^The target had 2 brokers when topic discovery td-target-counted read them at .+\. That discovery is past its freshness, so the count sets the default and refuses no factor: open orders-scratch and run Discover topics for a fresh count, which also refuses a factor above it\. The readiness check in step 5 reads them again when it runs\.$/);
    assert.ok(step4.includes("href=\"#/clusters?ns=team-fx5&amp;name=orders-scratch\""),
      "the connection's page, where a fresh count is read: " + byId(step4, "replication-brokers"));
    const step6 = renderPlanStep(await preparePlan(state), state);
    assert.equal(visible(byId(step6, "review-replication")), said, "the review says the age too");

    // A FACTOR ABOVE AN OLD COUNT IS NOT REFUSED HERE -- the basis says what was
    // read and when -- and the readiness check stays the authority.
    setReplicationFactor(state, "3");
    assert.deepEqual(replicationProblems(state), Object.create(null),
      "NEGATIVE CONTROL: ReplicationFactorExceedsBrokers on a count past its freshness fails this");
    assert.equal(validateRestore(state).replicationFactor, undefined);
    assert.equal(replicationText(state), "3 (set by you; the target had 2 brokers" + asOf + ")");
    const typed = renderPlanStep(await preparePlan(state), state);
    assert.equal(byId(typed, "review-replication-complaint"), null);
    // CONTROL: the same 3 over a FRESH count is refused, by the check's code.
    const freshState = withAnswer(DISCOVERY());
    setReplicationFactor(freshState, "3");
    assert.match(String(replicationProblems(freshState).replicationFactor),
      /^`ReplicationFactorExceedsBrokers`/);
    assert.equal(replicationText(freshState), "3 (set by you; the target has 2 brokers)");

    // THE CEILING over an old count says its age as well.
    const big = expired();
    big.lastSuccessful.brokerCount = 5;
    assert.equal(replicationText(withAnswer(big)), "3 (at most 3 by default, of the target's 5 " +
      "brokers" + asOf + "; the source's replication factor is not published to this console)");
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
  // THE BOUNDARY, through a state and the plan (review L1): four brokers is
  // the first count the ceiling caps.
  const four = withBrokers(wizardState(), 4);
  assert.equal(replicationFactorOf(four), 3,
    "NEGATIVE CONTROL: 4 -- the ceiling one broker late -- fails this");
  assert.ok((await preparePlan(four)).bytes.includes("\n  default_replication_factor: 3\n"),
    "the plan an approver signs asks for 3, not 4");
  assert.equal(replicationChoice(four).basis, "ceiling");
  const one = withBrokers(wizardState(), 1);
  assert.equal(replicationFactorOf(one), 1);
  assert.equal(replicationChoice(one).basis, "brokers",
    "NEGATIVE CONTROL: \"grammar\" -- a 1 nobody read -- fails this: this 1 is the target's");
  assert.deepEqual(replicationProblems(one), Object.create(null));
});

test("fx5_the_factor_can_differ_from_the_sources_and_both_steps_and_the_docs_say_so", async () => {
  // FX-5 review M1. A default worked out from the TARGET's brokers knows
  // nothing of the source's factor, and the difference is the operator's
  // storage bill: a topic the source kept on one replica, restored at 3, is
  // stored three times. Where the factor is set (step 4, beside the basis) and
  // where it is reviewed (step 6, under its row, above Create), the page says
  // so whenever the source's factor is not known to equal the plan's -- every
  // basis this build reaches.
  const typedOne = withBrokers(wizardState(), BROKERS);
  setReplicationFactor(typedOne, "1");
  for (const [label, state] of [
    ["the broker count", withBrokers(wizardState(), BROKERS)],
    ["the ceiling", withBrokers(wizardState(), 5)],
    ["the grammar's 1", wizardState()],
    ["a typed factor", typedOne],
  ]) {
    const step4 = renderTargetStep(state);
    assert.equal(visible(byId(step4, "replication-differs") || ""), REPLICATION_DIFFERS_NOTE,
      "NEGATIVE CONTROL: step 4 without the sentence fails this (" + label + ")");
    assert.ok(step4.indexOf("id=\"replication-differs\"") > step4.indexOf("id=\"replication-basis\""),
      "beside the basis, after it (" + label + ")");
    const step6 = renderPlanStep(await preparePlan(state), state);
    const review = byId(step6, "review-replication-differs");
    assert.equal(visible(review || ""), REPLICATION_DIFFERS_NOTE,
      "NEGATIVE CONTROL: the review step without the sentence fails this (" + label + ")");
    assert.ok(step6.indexOf("id=\"review-replication\"") < step6.indexOf("id=\"review-replication-differs\"") &&
      step6.indexOf("id=\"review-replication-differs\"") < step6.indexOf("id=\"create-restore\""),
    "under the review row and above Create (" + label + ")");
  }
  // THE BRIEF'S EXAMPLE, in the sentence itself: replication factor 1
  // restored at 3 triples what the topic takes.
  assert.match(REPLICATION_DIFFERS_NOTE,
    /a topic the source kept at replication factor 1, restored at 3, takes three times the storage/);
  assert.match(REPLICATION_DIFFERS_NOTE, /a factor below the source's keeps fewer copies/,
    "and the other direction");
  // THE RULE: silent only when the source's factor is known and is the plan's.
  assert.equal(replicationMayDiffer(replicationDefault([3], 5)), false,
    "NEGATIVE CONTROL: true -- a difference said where there is none -- fails this");
  assert.equal(replicationMayDiffer(replicationDefault([3], 2)), true, "capped below the source's");
  assert.equal(replicationMayDiffer(replicationDefault(null, 2)), true, "the source's not known");
  assert.equal(replicationMayDiffer(replicationDefault(null, null)), true, "nothing known");
  assert.equal(replicationMayDiffer({ value: 4, basis: "chosen", source: 3, brokers: 5 }), true,
    "a typed factor that is not the source's");
  // THE SAME SENTENCE IN THE RESTORE DOCS: the wizard's own section and the
  // quickstart's restore step. Markdown wraps lines and quotes with `>`.
  const flat = (text) => text.replace(/^[ \t]*>[ \t]?/gm, "").replace(/\s+/g, " ");
  for (const doc of ["README.md", "../docs/quickstart.md"]) {
    assert.ok(flat(readFileSync(UI_DIR + doc, "utf8")).includes(REPLICATION_DIFFERS_NOTE),
      "NEGATIVE CONTROL: " + doc + " without the page's sentence fails this");
  }
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

test("fx5_with_no_readiness_check_create_stays_enabled_and_the_page_and_quickstart_say_so",
  async () => {
    // FX-5 review L4. The readiness check is advisory: with none, Create is
    // enabled. Before FX-5 every plan asked for 1, which any broker holds; now a
    // factor above the target's brokers can reach the runner, which fails the
    // approved run when it creates the topics (after phase 5). The warning
    // beside Create names the factor, and the quickstart no longer says the
    // check must pass before Create.
    const state = wizardState();
    setReplicationFactor(state, "3");
    const prepared = await preparePlan(state);
    assert.equal(readinessRefusal(state, prepared), null, "no check, no refusal: it is advisory");
    const step6 = renderPlanStep(prepared, state);
    assert.ok(step6.includes("id=\"create-restore\" class=\"primary\">"),
      "Create is enabled with no check: " + byId(step6, "create-restore"));
    const warning = visible(byId(step6, "readiness-not-run") || "");
    assert.match(warning, /or at whether the target's brokers can hold the replication factor\./,
      "NEGATIVE CONTROL: the pre-FX-5 warning, which names no factor, fails this: " + warning);
    assert.match(warning,
      /a factor the brokers cannot hold fails the approved run when it creates the topics, with nothing restored\./);
    const quickstart = readFileSync(UI_DIR + "../docs/quickstart.md", "utf8").replace(/\s+/g, " ");
    assert.ok(!quickstart.includes("The readiness check must pass before *Create the Restore* is " +
      "enabled"), "NEGATIVE CONTROL: the old step 3, which says the check gates Create, fails this");
    for (const said of ["with no check at all Create stays enabled",
      "`Failed` with `exitCode 1` / `operational`, no signed result and nothing restored",
      "names `InvalidReplicationFactor`"]) {
      assert.ok(quickstart.includes(said), "the quickstart's step 3 says: " + said);
    }
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

/** The catalog point's id: a point with no Backup behind it. */
function catalogPointId() {
  return "lwp1-0123456789abcdef0123456789abcdef";
}

/** The `RecoveryCatalog` the point is read from, over the shared destination. */
function catalogObject(ns) {
  return {
    apiVersion: "logweir.dev/v1alpha1", kind: "RecoveryCatalog",
    metadata: { name: "archive", namespace: ns, uid: "cat-uid" },
    spec: { destinationRef: { name: fixture("console/destination.json").item.name } },
    status: {},
  };
}

/** The point, as `GET .../catalogs/{name}/points` publishes it. */
function catalogPointEntry() {
  return {
    pointId: catalogPointId(), backupId: "set-fx5", runId: "01JB7Z00000000000000000000",
    recoveryPointAt: "2026-09-22T14:00:00Z", coveredFrom: "2026-09-22T13:00:00Z",
    coveredTo: "2026-09-22T14:00:00Z", availability: "Available", verification: "Verified",
    selectable: true, signerKeyId: "c".repeat(64),
    receiptKey: "logweir/backups/set-fx5/01JB7Z00000000000000000000.receipt.json",
    receiptSha256: "sha256:" + "a1".repeat(32), manifestKey: "set-fx5/manifest.json",
    manifestSha256: "sha256:" + "b2".repeat(32),
    locations: [{ locationId: "s3://kafka-backups/team-a/prod", availability: "Available" }],
  };
}

/** A catalog point the wizard restores from, with no Backup behind it. */
function catalogPointState(ns) {
  const destination = fixture("console/destination.json").item;
  const point = catalogRecoveryPoint(catalogObject(ns), catalogPointEntry(), destination, null);
  return initialState(ns, clusters(), { items: [] },
    { catalog: "archive", point: catalogPointId() }, destination, undefined, { point: point });
}

test("fx5_a_catalog_point_mount_reads_the_targets_discovery_before_the_first_paint", async () => {
  // THE CATALOG HALF (FX-5 review L2). A catalog point with no Backup behind
  // it (PLAT-15.2) is mounted by its own path, `mountCatalogPoint`, which must
  // read the TARGET's broker count before its first paint exactly as the
  // Backup mount does -- or every restore from a catalog point shows the
  // grammar's 1 and "the target's topic discoveries have not been read".
  const asked = [];
  const view = fakeView();
  const ns = "team-fx5-cat-mount";
  const api = Object.assign(mountApi({ "orders-scratch": DISCOVERY() }, asked), {
    destination: async () => ({ item: fixture("console/destination.json").item }),
    catalogReaders: {
      listCatalogs: async () => ({ items: [catalogObject(ns)] }),
      readCatalog: async () => catalogObject(ns),
      readPoints: async () => ({
        requestId: "r", items: [catalogPointEntry()], truncated: false, viewExpired: false,
        page: { limit: 200, nextCursor: null },
      }),
      ownVerdict: async () => ({ verdict: null }),
    },
  });
  await mountRestoreWizard(view.root, ns, { catalog: "archive", point: catalogPointId() },
    viewParse, api);
  assert.ok(view.find("#catalog-topics") !== null,
    "the six steps over the catalog point, not a refusal: " + visible(view.html()).slice(0, 400));
  assert.deepEqual(asked, [ns + "/orders-scratch"],
    "NEGATIVE CONTROL: [] -- the catalog mount never reading the target -- fails this");
  assert.equal(view.find("#replication-factor").getAttribute("value"), "2",
    "NEGATIVE CONTROL: 1 -- the catalog mount's first paint before the count -- fails this");
  assert.match(visible(view.html()), /This plan asks for 2 \(the target's 2 brokers;/);
});

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

// ------------------------------------------- PROD-05.1: the source's factor

import {
  MAX_SOURCE_CATALOGS,
  SOURCE_FACTS_NOT_READ,
  partitionCountsText,
  pointIdOfReceiptDigest,
  refreshSourceFacts,
  sourceFactorNote,
  sourceFactsOfEntry,
} from "../pages/restore-wizard.js";

/** The catalog row, as the product API publishes it, with the point's topics:
 *  `orders` kept on 3 replicas and owned by a Strimzi KafkaTopic, `payments`
 *  on 1. */
function catalogPointEntryWithTopics() {
  return Object.assign(catalogPointEntry(), {
    topics: [
      { name: "orders", partitions: 6, replicationFactor: 3, configCoverage: "captured",
        owner: "strimzi", applyRoute: "desiredStateExport" },
      { name: "payments", partitions: 2, replicationFactor: 1, configCoverage: "captured",
        applyRoute: "adminApi" },
    ],
  });
}

/** A catalog-point state whose row lists its topics, `orders` and `payments`
 *  typed, the source facts read the way the mount reads them. */
async function catalogStateWithTopics(ns, entry) {
  const destination = fixture("console/destination.json").item;
  const point = catalogRecoveryPoint(catalogObject(ns), entry || catalogPointEntryWithTopics(),
    destination, null);
  const state = initialState(ns, clusters(), { items: [] },
    { catalog: "archive", point: catalogPointId() }, destination, undefined, { point: point });
  setCatalogTopics(state, ["orders", "payments"]);
  assert.equal(await refreshSourceFacts(state, {}), true);
  return state;
}

test("prod051_the_point_id_of_a_backup_is_its_receipts_digest_prefix", () => {
  assert.equal(pointIdOfReceiptDigest("sha256:" + "ab".repeat(32)), "lwp1-" + "ab".repeat(16),
    "the catalog's identity (D3 section 5.1): lwp1- and the first 32 hex digits");
  for (const bad of [undefined, null, "", "sha256:ABC", "sha256:" + "ab".repeat(31), "ab".repeat(32),
    "sha256:" + "AB".repeat(32)]) {
    assert.equal(pointIdOfReceiptDigest(bad), "", String(bad));
  }
});

test("prod051_the_source_factor_from_the_catalog_capped_by_the_brokers_with_its_source_said",
  async () => {
    const state = withBrokers(await catalogStateWithTopics("team-p051"), BROKERS);
    assert.deepEqual(sourceReplicationFactorsOf(state), [3, 1],
      "one factor per SELECTED topic, from the catalog row");
    const choice = replicationChoice(state);
    assert.deepEqual([choice.value, choice.basis, choice.source], [2, "capped", 3],
      "NEGATIVE CONTROL: 2 with basis brokers -- the factor never read -- fails this");
    assert.equal(replicationText(state),
      "2 (capped at the target's 2 brokers; the source's is 3, as recovery catalog `archive` " +
      "records point `" + catalogPointId() + "`, the largest of the selected topics')");
    assert.equal(replicationFactorOf(state), 2, "the plan carries the capped default");
    // FX-5's refusal above the broker count STAYS: 3 typed over a fresh 2.
    setReplicationFactor(state, "3");
    assert.match(replicationProblems(state).replicationFactor || "",
      /^`ReplicationFactorExceedsBrokers`: the target's topic discovery/,
      "NEGATIVE CONTROL: no complaint -- the source's 3 lifting the cap -- fails this");
  });

test("prod051_a_target_with_room_takes_the_sources_factor_and_the_differs_note_goes", async () => {
  const state = withBrokers(await catalogStateWithTopics("team-p051-room"), 5);
  const choice = replicationChoice(state);
  assert.deepEqual([choice.value, choice.basis], [3, "source"],
    "NEGATIVE CONTROL: 3 by the ceiling (basis ceiling) fails this -- the source's 3, not the cap's");
  assert.equal(replicationText(state), "3 (the source's, as recovery catalog `archive` records " +
    "point `" + catalogPointId() + "`, the largest of the selected topics')");
  assert.equal(replicationMayDiffer(choice), false,
    "the plan asks for exactly the source's factor, so the storage note has nothing to warn of");
  const step4 = renderTargetStep(state);
  assert.equal(byId(step4, "replication-differs"), null);
  assert.equal(byId(step4, "replication-source"), null,
    "the not-known note is not shown when the factor IS known");
  const review = visible(renderRecoveryLimits(state));
  assert.ok(review.includes("3 (the source's, as recovery catalog `archive` records point"), review);
});

test("prod051_the_default_follows_the_selected_subset", async () => {
  const state = withBrokers(await catalogStateWithTopics("team-p051-subset"), 5);
  setCatalogTopics(state, ["payments"]);
  syncReplicationDefault(state);
  assert.deepEqual(sourceReplicationFactorsOf(state), [1]);
  assert.equal(replicationText(state),
    "1 (the source's, as recovery catalog `archive` records point `" + catalogPointId() + "`)",
    "NEGATIVE CONTROL: 3 -- the whole point's largest -- fails this: the SELECTED topics' factor");
  // A selected topic the row records no factor for: the largest of the rest, and said.
  const entry = catalogPointEntryWithTopics();
  delete entry.topics[0].replicationFactor;
  const partial = withBrokers(await catalogStateWithTopics("team-p051-partial", entry), 5);
  assert.equal(replicationText(partial), "1 (the source's, as recovery catalog `archive` records " +
    "point `" + catalogPointId() + "`; not recorded for 1 of the 2 selected topics)");
});

test("prod051_a_row_without_topics_says_why_and_falls_back_to_the_brokers", async () => {
  const state = withBrokers(await catalogStateWithTopics("team-p051-none", catalogPointEntry()),
    BROKERS);
  assert.equal(sourceReplicationFactorsOf(state), null);
  assert.equal(replicationChoice(state).basis, "brokers");
  assert.equal(sourceFactorNote(state), SOURCE_FACTOR_NOTE + " For this point: recovery catalog " +
    "`archive` publishes no topic layout for this point: its backup receipt predates format " +
    "1.3.0, the catalog was synced by an older runner, or the point is not Available.");
  const omitted = Object.assign(catalogPointEntry(), { topicsOmitted: 70 });
  const facts = sourceFactsOfEntry(omitted, "archive");
  assert.equal(facts.topics, null);
  assert.match(facts.why, /lists this point without its 70 topics/);
});

test("prod051_a_backups_point_is_found_in_the_namespaces_catalogs_by_its_receipt_digest",
  async () => {
    const digest = "sha256:" + "5c".repeat(32);
    const pointId = pointIdOfReceiptDigest(digest);
    const state = wizardState("team-p051-backup");
    state.point.status.evidence = Object.assign({}, state.point.status.evidence || {},
      { receiptSha256: digest });
    const asked = [];
    const row = Object.assign(catalogPointEntryWithTopics(), { pointId: pointId });
    const readers = (rows) => ({
      catalogReaders: {
        listCatalogs: async () => ({ items: [{ metadata: { name: "empty" } },
          { metadata: { name: "primary" } }] }),
        readPoints: async (name, query) => {
          asked.push(name + "?" + (query.cursor || ""));
          return { items: name === "primary" ? rows : [], page: { nextCursor: null } };
        },
      },
    });
    assert.equal(await refreshSourceFacts(state, readers([row])), true);
    assert.deepEqual(asked, ["empty?", "primary?"], "each catalog's view, until the point is found");
    withBrokers(state, 5);
    assert.deepEqual(sourceReplicationFactorsOf(state), [3, 1],
      "NEGATIVE CONTROL: null -- the Backup's point never looked up -- fails this");
    assert.equal(replicationText(state), "3 (the source's, as recovery catalog `primary` records " +
      "point `" + pointId + "`, the largest of the selected topics')");
    assert.equal(partitionCountsText(state), "orders 6, payments 2 (the source's, which the " +
      "restore creates each topic with, as recovery catalog `primary` records them)");
    const review = visible(renderRecoveryLimits(state));
    assert.ok(review.includes("orders 6, payments 2"), review);
    assert.ok(!review.includes("Partition counts are shown before the run only"), review);

    // A ROW THE CATALOG DOES NOT STAND BEHIND sets nothing: not selectable.
    const unsure = wizardState("team-p051-backup-unsure");
    unsure.point.status.evidence = { receiptSha256: digest };
    await refreshSourceFacts(unsure, readers([Object.assign({}, row,
      { selectable: false, verification: "UntrustedSigner" })]));
    assert.equal(sourceReplicationFactorsOf(unsure), null,
      "NEGATIVE CONTROL: [3, 1] -- an untrusted row's layout setting the default -- fails this");
    assert.match(unsure.sourceFacts.why, /as not selectable \(Available, UntrustedSigner\)/);
    // NOT LISTED: a row for another point only. The why names the point.
    const other = wizardState("team-p051-backup-other");
    other.point.status.evidence = { receiptSha256: digest };
    await refreshSourceFacts(other, readers([catalogPointEntryWithTopics()]));
    assert.equal(sourceReplicationFactorsOf(other), null);
    assert.equal(other.sourceFacts.why,
      "no recovery catalog in this namespace lists point `" + pointId + "`");
    // UNREADABLE: the why carries the read's own refusal, never a throw.
    const failing = wizardState("team-p051-backup-fail");
    failing.point.status.evidence = { receiptSha256: digest };
    await refreshSourceFacts(failing, { catalogReaders: {
      listCatalogs: async () => { throw new Error("served by the Logweir product API only"); },
    } });
    assert.match(failing.sourceFacts.why,
      /recovery catalogs could not be read: served by the Logweir product API only/);
    // NO DIGEST ON THE LIST (the product API's list projection carries none):
    // the run's own operation is read for it, once, and the point is found.
    const listed = wizardState("team-p051-backup-listed");
    const owned = [];
    const viaOperation = readers([row]);
    viaOperation.catalogReaders.ownVerdict = async (name) => {
      owned.push(name);
      return { verdict: null, receiptSha256: digest };
    };
    await refreshSourceFacts(listed, viaOperation);
    assert.deepEqual(owned, [listed.point.metadata.name]);
    assert.deepEqual(sourceReplicationFactorsOf(listed), [3, 1],
      "NEGATIVE CONTROL: null -- the operation never read for the digest -- fails this");
    // NO DIGEST ANYWHERE: nothing to look up, and said.
    const bare = wizardState("team-p051-backup-bare");
    const none = readers([row]);
    none.catalogReaders.ownVerdict = async () => ({ verdict: null, receiptSha256: null });
    await refreshSourceFacts(bare, none);
    assert.match(bare.sourceFacts.why, /records no receipt digest/);
    const unreadState = wizardState("team-p051-backup-unread");
    const refused = readers([row]);
    refused.catalogReaders.ownVerdict = async () => { throw new Error("403 operationsRead"); };
    await refreshSourceFacts(unreadState, refused);
    assert.match(unreadState.sourceFacts.why,
      /receipt digest could not be read \(403 operationsRead\)/);
    assert.ok(MAX_SOURCE_CATALOGS >= 1);
    assert.equal(SOURCE_FACTS_NOT_READ.topics, null);
  });

test("prod051_the_mount_reads_the_backups_catalog_row_before_the_first_paint", async () => {
  const digest = "sha256:" + "7e".repeat(32);
  const backups = fixture("wizard-backups.json");
  for (const b of backups.items) {
    b.status.evidence = Object.assign({}, b.status.evidence || {}, { receiptSha256: digest });
  }
  const asked = [];
  const view = fakeView();
  const api = Object.assign(mountApi({ "orders-scratch": DISCOVERY() }, asked), {
    list: async (_ns, plural) => (plural === "kafkaclusters" ? clusters() : backups),
    catalogReaders: {
      listCatalogs: async () => ({ items: [{ metadata: { name: "primary" } }] }),
      readPoints: async () => ({
        items: [Object.assign(catalogPointEntryWithTopics(), {
          pointId: pointIdOfReceiptDigest(digest) })],
        page: { nextCursor: null },
      }),
    },
  });
  await mountRestoreWizard(view.root, "team-p051-mount", pointParams(), viewParse, api);
  assert.equal(view.find("#replication-factor").getAttribute("value"), "2",
    "the source's 3, capped at the target's 2 brokers, on the FIRST paint");
  assert.match(visible(view.html()),
    /This plan asks for 2 \(capped at the target's 2 brokers; the source's is 3, as recovery catalog `primary` records point `lwp1-7e7e/,
    "NEGATIVE CONTROL: \"(the target's 2 brokers; the source's replication factor is not " +
    "published\" -- the mount never reading the catalog -- fails this");
});
