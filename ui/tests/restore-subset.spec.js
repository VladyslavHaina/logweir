// restore-subset.spec.js -- PLAT-11.2's behaviour arm: the topic subset, the
// exact mapping preview, the recovery limits, the readiness gate before a
// submit, and the fresh-target retry.
//
// EVERY ROW CARRIES ITS NEGATIVE CONTROL, in the shape the lean loop asks for
// a console change: an assertion that FAILS when the behaviour is absent. They
// are written as a second assertion over the state the behaviour is about --
// "and with the behaviour's input removed, the page does NOT say this" -- so a
// mutant that deletes the rule leaves the row red rather than vacuously green.
//
// Pure functions from a JSON object to an HTML string, as everything in this
// directory is: no DOM, no network, no clock.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  defaultPrefixFor,
  freshTargetPrefix,
  initialState,
  isKafkaTopicName,
  mappedTopicName,
  mappingProblems,
  MAX_TOPIC_NAME_CHARS,
  PARTITION_COUNT_NOT_PUBLISHED,
  readinessRefusal,
  recoveryPoints,
  renderPlanStep,
  renderRecoveryLimits,
  renderRetryBanner,
  renderTargetStep,
  renderTopicSubset,
  replicationFactorOf,
  restoreBody,
  restorePointRoute,
  restoreRetryFromOperationRoute,
  restoreRetryRoute,
  restoreRouteParams,
  RESUME_NOT_IMPLEMENTED,
  selectedTopics,
  stepStates,
  topicMapping,
  validateRestore,
  verificationPlanSentence,
  wizardDraftValues,
  applyWizardDraft,
} from "../pages/restore-wizard.js";
import { renderRetryAction, retryRoute } from "../pages/operation.js";
import { COMPLETION_GUIDANCE, TARGET_MODE_MEANING } from "../render.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

const LIMITS = fixture("restore-limits.json");

function newestPoint(list) {
  const point = recoveryPoints(list)[0];
  return { uid: point.metadata.uid, backup: point.metadata.name };
}

/** The wizard state these rows drive, built the way the page builds it. */
function wizardState(extra) {
  const backups = fixture("wizard-backups.json");
  const selection = Object.assign({}, newestPoint(backups), extra || {});
  return initialState("logweir-t27", fixture("wizard-clusters.json"), backups, selection);
}

/** A preflight result in the product API's own shape, bound to one plan. */
function preflight(planHash, over) {
  return Object.assign(
    {
      id: "pf-000000000000000000000001",
      operation: "restore",
      state: "ready",
      terminal: true,
      applicable: true,
      stale: false,
      staleReasons: [],
      staleBasis: ["plan", "referents"],
      binding: { planHash: planHash },
      checks: [],
      warnings: [],
      executionOnly: [],
      detailsAvailable: false,
      conditions: [],
    },
    over || {},
  );
}

// ------------------------------------------------- 1. the subset and its mapping

test("the_wizard_restores_a_chosen_subset_and_the_request_is_the_preview", () => {
  const state = wizardState();
  assert.deepEqual(
    selectedTopics(state),
    ["orders", "payments"],
    "every frozen topic is selected by default: a subset is a narrowing, never a new default",
  );

  // THE SUBSET. One topic ticked off; the plan, the preview and the request
  // must all be the remaining one and nothing else.
  state.fields.topics = ["payments"];
  assert.deepEqual(selectedTopics(state), ["payments"]);
  const prefix = state.fields.target.topicPrefix;
  assert.deepEqual(
    topicMapping(state),
    [{ source: "payments", target: prefix + "payments" }],
    "the mapping is the prefix and nothing else",
  );

  const subset = renderTopicSubset(state);
  assert.ok(subset.includes("<code>" + prefix + "payments</code>"),
    "the exact target name is shown BEFORE the submit: " + subset);
  assert.ok(!subset.includes("<code>" + prefix + "orders</code>"),
    "and the unticked topic is not mapped");

  // THE REQUEST IS THE PREVIEW, ROW FOR ROW. One call produces both, so this
  // asserts the identity of the data and not a coincidence of two builders.
  const body = restoreBody(state, { bytes: "b", hash: "sha256:h", restoreName: "rst-x", approvalName: "apr-x" });
  assert.deepEqual(body.topicMapping, topicMapping(state));
  assert.deepEqual(body.topicMapping, [{ source: "payments", target: prefix + "payments" }]);

  // THE NEGATIVE CONTROL: a mutant that renamed one topic after the preview.
  // The declared row and the recomputed row come apart, which is exactly what
  // the product API answers 422 `mapping_mismatch` for.
  const renamed = body.topicMapping.map((row) => ({ source: row.source, target: "somewhere-else" }));
  assert.notDeepEqual(
    renamed,
    topicMapping(state),
    "a row renamed after the preview is no longer the preview -- this is the state the API refuses",
  );
  assert.notEqual(
    renamed[0].target,
    mappedTopicName(state.fields.target.topicPrefix, renamed[0].source),
    "and it is not `prefix + source`, which is the rule the API recomputes",
  );

  // AND THE ORDER IS THE POINT'S, NOT THE CLICK ORDER: a hash an approver
  // signs must not move because two boxes were ticked the other way round.
  const clicked = wizardState();
  clicked.fields.topics = ["payments", "orders"];
  assert.deepEqual(selectedTopics(clicked), ["orders", "payments"]);
});

test("a_duplicate_mapping_is_refused_before_the_submit_and_names_both_rows", () => {
  const state = wizardState();
  // TWO SOURCE TOPICS ONTO ONE TARGET NAME. The mapping rule is a prefix and
  // prefixes are injective over distinct sources, so the only way to reach it
  // is the same topic twice -- and the page refuses it by name.
  state.fields.topics = ["orders", "orders"];
  const problems = mappingProblems(state);
  assert.ok(typeof problems.topics === "string", "the subset is refused: " + JSON.stringify(problems));
  assert.ok(
    problems.topics.includes("`orders`") &&
      problems.topics.includes(state.fields.target.topicPrefix + "orders"),
    "and the refusal names the source and the target they share: " + problems.topics,
  );
  assert.ok(
    Object.keys(validateRestore(state)).includes("topics"),
    "so `submitRestore`'s own check refuses it and nothing is sent",
  );
  const step = stepStates(state)[3];
  assert.equal(step.status, "attention", "and step 4 does not read `done` over a refused mapping");

  // THE NEGATIVE CONTROL: the same state with the duplicate removed passes
  // every one of those, so the row fails if the duplicate rule is deleted.
  state.fields.topics = ["orders"];
  assert.deepEqual(mappingProblems(state), Object.create(null));
  assert.ok(!Object.keys(validateRestore(state)).includes("topics"));
  assert.equal(stepStates(state)[3].status, "done");
});

test("an_invalid_prefix_and_an_illegal_mapped_name_are_refused_by_name", () => {
  const state = wizardState();
  for (const [bad, expect] of [
    ["", "onto itself"],
    ["has space-", "not a name a broker accepts"],
    ["two:colons-", "not a name a broker accepts"],
    ["glob*-", "not a name a broker accepts"],
  ]) {
    state.fields.target.topicPrefix = bad;
    const problems = mappingProblems(state);
    assert.ok(
      typeof problems.topicPrefix === "string" && problems.topicPrefix.includes(expect),
      "`" + bad + "` is refused by name: " + JSON.stringify(problems),
    );
  }

  // A LEGAL PREFIX THAT MAKES AN ILLEGAL NAME. 249 is the broker's own bound
  // and this is where the two halves meet: prefix + topic is what is created.
  state.fields.target.topicPrefix = "p".repeat(MAX_TOPIC_NAME_CHARS - 3) + "-";
  state.fields.topics = ["orders"];
  const long = mappingProblems(state);
  assert.ok(
    typeof long.topicPrefix === "string" && long.topicPrefix.includes(String(MAX_TOPIC_NAME_CHARS)),
    "the mapped name is refused with the bound named: " + JSON.stringify(long),
  );

  // THE IDENTITY MAP IS EXACTLY THE EMPTY PREFIX, and nothing else can be
  // one: the rule is concatenation, so `prefix + source == source` holds only
  // for the empty prefix. It is refused above, by that name, and the page's
  // target==source arm is the backstop that keeps the rule true if the mapping
  // function ever grows a second case.
  const identityState = wizardState();
  identityState.fields.target.topicPrefix = "";
  const identity = mappingProblems(identityState);
  assert.ok(
    typeof identity.topicPrefix === "string" && identity.topicPrefix.includes("onto itself"),
    "the identity map is refused by name: " + JSON.stringify(identity),
  );
  assert.equal(mappedTopicName("", "orders"), "orders", "which is why it is the identity");

  // A TOPIC THE POINT DID NOT FREEZE.
  const stranger = wizardState();
  stranger.fields.topics = ["orders", "not-in-this-point"];
  const outside = mappingProblems(stranger);
  assert.ok(
    typeof outside.topics === "string" &&
      outside.topics.includes("not-in-this-point") &&
      outside.topics.includes("PlanTopicsNotInRecoveryPoint"),
    "refused by name, with the server rail that says the same thing: " + JSON.stringify(outside),
  );

  // AN EMPTY SUBSET.
  const none = wizardState();
  none.fields.topics = [];
  assert.ok(typeof mappingProblems(none).topics === "string");

  // THE NEGATIVE CONTROL for all five: the untouched state is accepted, so a
  // rule deleted here turns this row red rather than leaving it green.
  assert.deepEqual(mappingProblems(wizardState()), Object.create(null));
  assert.equal(isKafkaTopicName("restore-20260907T140500Z-"), true);
  assert.equal(isKafkaTopicName("restore 20260907-"), false);
  assert.equal(isKafkaTopicName("x".repeat(MAX_TOPIC_NAME_CHARS)), true);
  assert.equal(isKafkaTopicName("x".repeat(MAX_TOPIC_NAME_CHARS + 1)), false);
});

// --------------------------------------------------- 2. the limits, from constants

test("the_recovery_limits_come_from_contract_constants_and_never_from_prose", () => {
  const state = wizardState();
  assert.equal(
    MAX_TOPIC_NAME_CHARS,
    LIMITS.maxTopicNameChars.value,
    "the name bound is " + LIMITS.maxTopicNameChars.owner + "'s, pinned by the fixture",
  );
  assert.equal(
    replicationFactorOf(state),
    LIMITS.defaultReplicationFactor.value,
    "the replication factor is the PLAN's own field, " + LIMITS.defaultReplicationFactor.owner,
  );
  assert.equal(
    state.fields.sample.recordsPerPartition,
    LIMITS.defaultRecordsPerPartition.value,
    LIMITS.defaultRecordsPerPartition.owner,
  );

  const html = renderRecoveryLimits(state);
  assert.ok(html.includes(">" + String(LIMITS.defaultReplicationFactor.value) + "<"),
    "the factor is displayed: " + html);
  assert.ok(html.includes(PARTITION_COUNT_NOT_PUBLISHED.slice(0, 60)),
    "the partition-count gap is NAMED rather than filled with a guess");
  assert.ok(!/\b\d+ partitions\b/.test(html), "and no partition count is invented: " + html);

  // THE SAMPLED SCOPE, NEVER LABELLED EXHAUSTIVE (D3 section 3.5).
  const scope = verificationPlanSentence(state);
  assert.ok(scope.includes(String(LIMITS.defaultRecordsPerPartition.value)));
  assert.ok(scope.includes("sampled check, not an exhaustive comparison"));
  assert.ok(!/\bexhaustively\b/.test(scope), scope);
  assert.ok(html.includes("sampled check, not an exhaustive comparison"));

  // THE CUTOVER LIMIT IS `render.js`'s CONSTANT, BYTE FOR BYTE -- not a
  // sentence this page composed and not server-authored prose.
  assert.equal(state.fields.target.mode, "newTopic");
  assert.ok(html.includes(COMPLETION_GUIDANCE.newTopic.slice(0, 60)));
  assert.ok(html.includes(TARGET_MODE_MEANING.newTopic.slice(0, 60)));
  assert.ok(html.includes("Consumers are not moved"));

  // AND RESUME IS SAID TO BE UNIMPLEMENTED, in the wizard, before the run.
  assert.ok(html.includes(RESUME_NOT_IMPLEMENTED.slice(0, 40)));
  assert.ok(RESUME_NOT_IMPLEMENTED.includes("Resume is not implemented"));
  assert.ok(renderTargetStep(state).includes(RESUME_NOT_IMPLEMENTED.slice(0, 40)),
    "step 4 carries it, which is the step an operator is on when they choose the target");

  // THE NEGATIVE CONTROL: a mode this version does not know gets NO guidance
  // rather than a sentence picked for it.
  const invented = wizardState();
  invented.fields.target.mode = "inPlace";
  const other = renderRecoveryLimits(invented);
  assert.ok(!other.includes("Consumers are not moved"),
    "no fixed sentence is shown for a mode that has none: " + other);
  assert.ok(other.includes("will not guess one"));
});

// --------------------------------------------- 3. the readiness gate before a submit

test("a_target_topic_collision_refuses_the_submit_and_names_the_check", () => {
  const state = wizardState();
  const prepared = { bytes: "b", hash: "sha256:aaa", restoreName: "rst-x", approvalName: "apr-x" };

  // THE COLLISION, as the product API publishes it: `target.mappedTopics` is
  // `notReady` with `MappedTopicExists`, so the aggregate is `notReady`.
  state.readiness = {
    boundHash: prepared.hash,
    preflight: preflight(prepared.hash, {
      state: "notReady",
      checks: [
        {
          id: "target.mappedTopics",
          category: "target",
          state: "notReady",
          gating: "blocking",
          authority: "checkJob",
          code: "MappedTopicExists",
          message: "1 mapped target topic already exists",
          remedy: "Choose a topic prefix nothing has used.",
        },
      ],
    }),
  };
  const blocked = readinessRefusal(state, prepared);
  assert.ok(typeof blocked === "string", "the submit is refused");
  assert.ok(
    blocked.includes("target.mappedTopics") && blocked.includes("MappedTopicExists"),
    "and the refusal names the check and its code: " + blocked,
  );
  const step6 = renderPlanStep(prepared, state);
  assert.ok(step6.includes("id=\"readiness-blocked\""), "the page says so");
  assert.ok(step6.includes("id=\"create-restore\" class=\"primary\" disabled"),
    "and the button is disabled: " + step6);

  // THE NEGATIVE CONTROL: the same plan with that check READY is submittable,
  // so this row fails if the gate stops reading the verdict.
  state.readiness.preflight = preflight(prepared.hash);
  assert.equal(readinessRefusal(state, prepared), null);
  assert.ok(!renderPlanStep(prepared, state).includes("id=\"readiness-blocked\""));
});

test("a_stale_preflight_and_a_target_change_both_refuse_the_submit", () => {
  const state = wizardState();
  const prepared = { bytes: "b", hash: "sha256:aaa", restoreName: "rst-x", approvalName: "apr-x" };

  // A TARGET CHANGE. Changing the target rewrites `target.bootstrap_servers`
  // in the plan, so the hash moves and the verdict on screen is about another
  // document. The page refuses until the check is run again.
  state.readiness = { boundHash: "sha256:bbb", preflight: preflight("sha256:bbb") };
  const moved = readinessRefusal(state, prepared);
  assert.ok(typeof moved === "string" && moved.includes("sha256:bbb") && moved.includes("sha256:aaa"),
    "both hashes are named: " + moved);
  assert.ok(moved.includes("Run it again"));

  // A PREFLIGHT OLDER THAN ITS BUDGET. `target.mappedTopics` expires in five
  // minutes (D2 section 6.3) and the SERVER recomputes applicability on every
  // read: this page trusts that answer rather than comparing a browser clock.
  state.readiness = {
    boundHash: prepared.hash,
    preflight: preflight(prepared.hash, {
      applicable: false,
      stale: true,
      staleReasons: [{ reason: "expired" }],
    }),
  };
  const expired = readinessRefusal(state, prepared);
  assert.ok(typeof expired === "string" && expired.includes("no longer applies"), expired);
  assert.ok(expired.includes("expired"), "with the server's own reason named: " + expired);

  // A CHECK THAT HAS NOT FINISHED is not a pass either.
  state.readiness = {
    boundHash: prepared.hash,
    preflight: preflight(prepared.hash, { state: "running", terminal: false }),
  };
  assert.ok(String(readinessRefusal(state, prepared)).includes("has not finished"));

  // THE NEGATIVE CONTROL: an applicable, ready, terminal verdict for THIS
  // plan submits. Every arm above therefore fails when its rule is removed.
  state.readiness = { boundHash: prepared.hash, preflight: preflight(prepared.hash) };
  assert.equal(readinessRefusal(state, prepared), null);

  // AND A MODE WITH NO READINESS ROUTE IS NOT GATED -- with the page saying so
  // rather than silently allowing it.
  const legacy = wizardState();
  legacy.readiness = { unavailable: true, unavailableReason: "no readiness route in this mode" };
  assert.equal(readinessRefusal(legacy, prepared), null);
  assert.ok(renderPlanStep(prepared, legacy).includes("id=\"readiness-ungated\""));

  // An UNCHECKED plan is a warning and not a refusal, and says what was not
  // looked for.
  const unchecked = wizardState();
  assert.equal(readinessRefusal(unchecked, prepared), null);
  assert.ok(renderPlanStep(prepared, unchecked).includes("id=\"readiness-not-run\""));
});

// ------------------------------------------------------ 4. the fresh-target retry

test("a_failed_restore_retries_to_a_fresh_target_and_never_reuses_its_approval", () => {
  const backups = fixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  const failedName = "rst-01jb7z0000000000000abcdef";

  // THE ROUTE. A failed Restore's operation view offers the retry; it names no
  // recovery point, because `Restore.spec` carries a backup SET id and no
  // reference to the Backup it came from.
  const fromOperation = restoreRetryFromOperationRoute("logweir-t27", failedName);
  assert.equal(fromOperation, retryRoute("logweir-t27", failedName),
    "the operation page spells the same route the wizard owns");
  assert.ok(fromOperation.includes("retryOf=" + encodeURIComponent(failedName)));
  assert.equal(restoreRouteParams(fromOperation).retryOf, failedName);
  assert.equal(restoreRouteParams(restorePointRoute("logweir-t27", point)).retryOf, "",
    "an ordinary restore retries nothing");

  // THE PREFIX IS FRESH, AND THAT IS THE WHOLE OF THE NEW IDENTITY.
  const ordinary = wizardState();
  const retry = wizardState({ retryOf: failedName });
  assert.equal(retry.retryOf, failedName);
  assert.notEqual(
    retry.fields.target.topicPrefix,
    ordinary.fields.target.topicPrefix,
    "a retry of the same point must not render the same plan: same prefix, same bytes, same " +
      "minted Restore name -- which is the collision with the old name PLAT-12.2 names",
  );
  assert.ok(retry.fields.target.topicPrefix.includes("retry-"));
  assert.equal(
    retry.fields.target.topicPrefix,
    freshTargetPrefix(retry.fields.pointInTime, failedName),
  );
  assert.equal(
    retry.fields.target.topicMappingPrefix,
    retry.fields.target.topicPrefix,
    "the scratch prefix moves with it, so switching the mode is still a one-value change",
  );
  assert.equal(defaultPrefixFor(retry), retry.fields.target.topicPrefix);

  // DETERMINISTIC: retrying the same run twice is the same retry, so a second
  // submit is the idempotent replay every other create here is.
  assert.equal(
    freshTargetPrefix(retry.fields.pointInTime, failedName),
    freshTargetPrefix(retry.fields.pointInTime, failedName),
  );
  assert.notEqual(
    freshTargetPrefix(retry.fields.pointInTime, "rst-01jb7z00000000000000zzzzz"),
    freshTargetPrefix(retry.fields.pointInTime, failedName),
    "and two different failed runs do not share a prefix",
  );

  // EVERY MAPPED NAME IS NEW.
  for (const row of topicMapping(retry)) {
    assert.ok(row.target.includes("retry-"), row.target);
    assert.ok(
      !topicMapping(ordinary).some((o) => o.target === row.target),
      "no target name of the failed run's plan is written to: " + row.target,
    );
  }

  // THE OLD APPROVAL IS NEVER SENT, and it is structural: both names are
  // minted from the plan bytes, so `restoreBody` has no field that could carry
  // another run's approval reference.
  const prepared = { bytes: "b", hash: "sha256:new", restoreName: "rst-new", approvalName: "apr-new" };
  const body = restoreBody(retry, prepared);
  assert.equal(body.spec.approvalRef.name, "apr-new");
  const serialized = JSON.stringify(body);
  assert.ok(!serialized.includes(failedName),
    "the failed run is not named anywhere in what is sent: " + serialized);
  assert.equal(body.metadata.name, "rst-new");

  // AND THE FAILED RUN IS UNTOUCHED: the wizard's create body is the only
  // write it makes, and it names a NEW object.
  assert.notEqual(body.metadata.name, failedName);
  const banner = renderRetryBanner(retry);
  assert.ok(banner.includes(failedName));
  assert.ok(banner.includes("id=\"retry-untouched\""));
  assert.ok(banner.includes("not reused"));
  assert.ok(banner.includes("PlanHashMismatch"), "the controller's backstop is named: " + banner);

  // THE DRAFT CANNOT PUT THE OLD PREFIX BACK. A retry shares the recovery
  // point -- and therefore the backup set -- with the run it retries, so a
  // draft keyed on the set alone would restore the failed run's own names.
  const kept = wizardDraftValues(ordinary);
  assert.equal(kept.retryOf, "");
  assert.equal(applyWizardDraft(wizardState({ retryOf: failedName }), kept), false,
    "an ordinary restore's draft does not apply to a retry");
  const retryDraft = wizardDraftValues(retry);
  assert.equal(retryDraft.retryOf, failedName);
  assert.equal(applyWizardDraft(wizardState(), retryDraft), false,
    "and a retry's draft does not apply to an ordinary restore");
  const same = wizardState({ retryOf: failedName });
  assert.equal(applyWizardDraft(same, retryDraft), true);
  assert.equal(same.fields.target.topicPrefix, retry.fields.target.topicPrefix);

  // THE NEGATIVE CONTROL for the affordance: it is offered for a FAILED
  // restore and for nothing else.
  const failedView = {
    kind: "restore", console: true, terminal: true, state: "failed",
    name: failedName, result: {},
  };
  const offered = renderRetryAction(failedView, "logweir-t27");
  assert.ok(offered.includes("Retry to a fresh target"));
  assert.ok(offered.includes(encodeURIComponent(failedName)));
  assert.equal(
    renderRetryAction(Object.assign({}, failedView, { state: "succeeded" }), "logweir-t27"),
    "",
    "a successful restore offers no retry",
  );
  assert.equal(
    renderRetryAction(Object.assign({}, failedView, { terminal: false, state: "running" }), "logweir-t27"),
    "",
    "and neither does one that is still running",
  );
  assert.equal(
    renderRetryAction(Object.assign({}, failedView, { kind: "backup" }), "logweir-t27"),
    "",
    "and a Backup is not a restore",
  );
  // The legacy projection says `phase` and leaves `terminal` false, and the
  // affordance must survive that: an incident without the product API is
  // exactly when a retry is needed.
  assert.ok(
    renderRetryAction(
      { kind: "restore", console: false, terminal: false, phase: "Failed", name: failedName, result: {} },
      "logweir-t27",
    ).includes("Retry to a fresh target"),
  );
});

// ------------------------------------------------------- 5. the timestamp boundary

test("a_point_outside_the_disclosed_window_is_refused_and_the_bounds_are_shown", () => {
  const state = wizardState();
  const covered = state.backups.items[0].status.windowCovered;
  assert.equal(typeof covered.fromMs, "number");
  assert.equal(typeof covered.toMs, "number");

  // INCLUSIVE AT BOTH ENDS.
  for (const at of [covered.fromMs, covered.toMs]) {
    state.fields.pointInTime = new Date(at).toISOString();
    assert.ok(
      !Object.keys(validateRestore(state)).includes("pointInTime"),
      "a point ON the bound is inside the window: " + state.fields.pointInTime,
    );
  }
  // AND STRICTLY OUTSIDE IS A REFUSAL NAMING THE WINDOW.
  for (const at of [covered.fromMs - 1, covered.toMs + 1]) {
    state.fields.pointInTime = new Date(at).toISOString();
    const problems = validateRestore(state);
    assert.ok(
      typeof problems.pointInTime === "string" &&
        problems.pointInTime.includes("outside the coverage this recovery point discloses"),
      "refused by name: " + JSON.stringify(problems),
    );
    assert.ok(problems.pointInTime.includes("both bounds are inside it"));
  }

  // THE NEGATIVE CONTROL: a point inside passes, so the row is red if the
  // window check is removed.
  state.fields.pointInTime = new Date(covered.toMs).toISOString();
  assert.ok(!Object.keys(validateRestore(state)).includes("pointInTime"));
});
