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
  draftFrom,
  effectivePrefix,
  selectTarget,
  setTopicPrefix,
  renderPointInTimeStep,
  renderPointSelector,
  freshTargetPrefix,
  frozenDestinationProblem,
  initialState,
  isKafkaTopicName,
  mappedTopicName,
  mappingProblems,
  MAX_TOPIC_NAME_CHARS,
  mountRestoreWizard,
  nextRestoreReadinessAttempt,
  PARTITION_COUNT_NOT_PUBLISHED,
  readinessRefusal,
  readinessSourceSentence,
  renderArchiveCredentialField,
  recoveryPoints,
  renderPlanStep,
  renderRecoveryLimits,
  renderRetryBanner,
  renderStoreFields,
  renderTargetStep,
  renderTopicSubset,
  requireFrozenDestination,
  replicationFactorOf,
  restoreBody,
  restoreReadinessRequest,
  restorePointRoute,
  restoreRetryFromOperationRoute,
  restoreRetryRoute,
  restoreRouteParams,
  preparePlan,
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
import { renderPlanBytes } from "../plan.js";
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

/** A point frozen to the public saved-destination fixture, including the
 * sentinel that must never be parsed as an S3 bucket. */
function savedWizardState(extra) {
  const backups = fixture("wizard-backups.json");
  const selection = Object.assign({}, newestPoint(backups), extra || {});
  const point = backups.items.find((item) => item.metadata.uid === selection.uid);
  const destination = fixture("console/destination.json").item;
  point.spec.destinationRef = { name: "primary", uid: destination.uid };
  point.spec.archive = { url: "logweir-destination://primary" };
  point.status.locationDigest = destination.locationDigest;
  return initialState(
    "logweir-t27", fixture("wizard-clusters.json"), backups, selection,
    destination,
  );
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

  // THE CONTROL IS THE IDENTITY ITSELF, not a locally built array compared
  // with the thing it was built from -- that earlier form could not fail for
  // any value and the review said so. What can fail: `restoreBody` must return
  // rows EQUAL to `topicMapping(state)` for a state the test moves underneath
  // it, so a body that cached, reordered or rebuilt them is caught.
  state.fields.topics = ["orders"];
  assert.deepEqual(
    restoreBody(state, { bytes: "b", hash: "sha256:h", restoreName: "r", approvalName: "a" })
      .topicMapping,
    topicMapping(state),
    "the declared rows follow the selection, they are not a snapshot of an earlier one",
  );
  assert.deepEqual(topicMapping(state), [{ source: "orders", target: prefix + "orders" }]);
  // The mutant that renames a row after the preview is planted in the SOURCE
  // (`restoreBody`'s `topicMapping(s)`) and killed by this row; the product
  // API's own `mapping_mismatch` rail is exercised live and in
  // `crates/logweir-api/tests/resources.rs`.
  assert.notEqual(
    mappedTopicName(prefix, "orders"),
    "somewhere-else",
    "and `prefix + source` is the rule the API recomputes",
  );
  state.fields.topics = ["payments"];

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
  // AND THE REFUSAL IS ON SCREEN ON THE KEYSTROKE, not only after a submit has
  // been refused by the server -- the window refusal in step 3 works the same
  // way. Without this the page would show a mapping it has already decided it
  // will not send, and the live journey found exactly that.
  const rendered = renderTopicSubset(state);
  assert.ok(rendered.includes("id=\"subset-complaint\""), rendered);
  assert.ok(rendered.includes("both map to the target topic"));

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
    const step4 = renderTargetStep(state);
    assert.ok(step4.includes("id=\"prefix-complaint\""),
      "and the refusal is rendered beside the field it is about, on the keystroke");
  }
  // THE NEGATIVE CONTROL for the rendering: a legal prefix carries no
  // complaint, so the paragraph is not simply always there.
  assert.ok(!renderTargetStep(wizardState()).includes("id=\"prefix-complaint\""));

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

test("the_readiness_request_uses_a_saved_destination_and_only_legacy_points_send_an_archive", () => {
  const prepared = { bytes: "plan bytes\n", hash: "sha256:" + "a".repeat(64) };
  const destinationBacked = savedWizardState();

  const saved = restoreReadinessRequest(destinationBacked, prepared);
  assert.equal(saved.restore.sourceDestination, "primary");
  assert.equal(saved.restore.evidenceDestination, "primary");
  assert.equal(saved.restore.legacySourceArchive, undefined,
    "MUTANT: a destination-backed point must never send the legacy marker");
  assert.deepEqual(saved.restore.recoveryPoint, {
    backupName: destinationBacked.point.metadata.name,
    backupUid: destinationBacked.point.metadata.uid,
  });
  const savedSentence = readinessSourceSentence(destinationBacked.point);
  assert.match(savedSentence, /saved destination `primary`/);
  assert.match(savedSentence, new RegExp(destinationBacked.point.status.locationDigest),
    "the page names the frozen digest the controller compares; it does not recompute one");
  const create = restoreBody(destinationBacked, {
    bytes: prepared.bytes, hash: prepared.hash, restoreName: "rst-x", approvalName: "apr-x",
  });
  assert.deepEqual(create.spec.sourceDestinationRef, { name: "primary" });
  assert.deepEqual(create.spec.evidenceDestinationRef, { name: "primary" });
  assert.deepEqual(create.spec.sourceArchive, { url: destinationBacked.point.spec.archive.url },
    "the saved destination owns its credential; an old inline secret does not leak through");

  const legacy = wizardState();
  assert.equal(legacy.point.spec.destinationRef, undefined, "the fixture is a legacy point");
  const inline = restoreReadinessRequest(legacy, prepared);
  assert.equal(inline.restore.sourceDestination, undefined);
  assert.equal(inline.restore.evidenceDestination, undefined);
  assert.deepEqual(inline.restore.legacySourceArchive, {
    url: "s3://kafka-backups/drill-demo",
    credentialRef: { name: "logweir-s3" },
  });
  const legacyCreate = restoreBody(legacy, {
    bytes: prepared.bytes, hash: prepared.hash, restoreName: "rst-x", approvalName: "apr-x",
  });
  assert.equal(legacyCreate.spec.sourceDestinationRef, undefined);
  assert.equal(legacyCreate.spec.evidenceDestinationRef, undefined);
  assert.match(readinessSourceSentence(legacy.point), /Legacy recovery point/);
});

test("a_saved_point_signs_the_resolved_destination_and_has_no_legacy_storage_controls", async () => {
  const state = savedWizardState();
  assert.equal(frozenDestinationProblem(state.point, state.savedDestination), null);
  assert.equal(requireFrozenDestination(state.point, state.savedDestination), state.savedDestination);
  assert.deepEqual(state.fields.source, {
    bucket: "kafka-backups", prefix: "team-a/prod", region: "us-east-1",
    endpoint: "https://minio.storage.svc:9000", pathStyle: true, allowHttp: false,
  });
  assert.deepEqual(state.fields.evidence, {
    bucket: "kafka-backups", prefix: "logweir/", region: "us-east-1",
    endpoint: "https://minio.storage.svc:9000", pathStyle: true, allowHttp: false,
  });
  assert.notEqual(state.fields.source.bucket, "primary",
    "MUTANT: the destination sentinel is not an S3 URL and its name is not a bucket");

  const plan = await preparePlan(state);
  assert.match(plan.bytes, /bucket: "kafka-backups"/);
  assert.match(plan.bytes, /prefix: "team-a\/prod"/);
  assert.match(plan.bytes, /prefix: "logweir\/"/);
  assert.doesNotMatch(plan.bytes, /bucket: "primary"/);

  const controls = renderStoreFields(state) + renderArchiveCredentialField(state);
  for (const id of ["archive-secret", "store-endpoint", "store-region", "store-pathStyle",
    "store-allow-insecure", "evidence-bucket"]) {
    assert.ok(!controls.includes("id=\"" + id + "\""),
      "saved points have no legacy override control " + id);
  }
  assert.match(controls, /saved destination/i);

  const kept = Object.assign(wizardDraftValues(state), {
    endpoint: "http://mutant.invalid", region: "wrong", pathStyle: false, allowHttp: true,
    evidenceBucket: "wrong", archiveSecret: "wrong-secret",
  });
  assert.equal(applyWizardDraft(state, kept), true);
  assert.equal(state.fields.source.endpoint, "https://minio.storage.svc:9000");
  assert.equal(state.fields.source.allowHttp, false);
  assert.equal(state.fields.evidence.bucket, "kafka-backups");
  assert.equal(state.archiveSecretName, "");

  const legacyControls = renderStoreFields(wizardState()) + renderArchiveCredentialField(wizardState());
  assert.match(legacyControls, /id="archive-secret"/,
    "legacy points retain the inline credential and storage controls");
  assert.match(legacyControls, /id="store-endpoint"/);
});

test("a_recreated_or_moved_saved_destination_is_refused_before_its_storage_can_be_signed", async () => {
  const exact = savedWizardState();
  const replacement = JSON.parse(JSON.stringify(exact.savedDestination));
  replacement.uid = "replacement-destination-uid";
  replacement.storage.bucket = "replacement-bucket";
  assert.match(frozenDestinationProblem(exact.point, replacement), /was recreated/);
  assert.throws(() => requireFrozenDestination(exact.point, replacement), /was recreated/,
    "a same-name replacement is refused by UID before the mount constructs wizard state");
  const uidMismatch = initialState(
    exact.ns, exact.clusters, exact.backups, exact.selection, replacement,
  );
  assert.equal(uidMismatch.fields.source.bucket, "");
  assert.equal(uidMismatch.fields.evidence.bucket, "");
  assert.match(validateRestore(uidMismatch).archive, /was recreated/);
  await assert.rejects(() => preparePlan(uidMismatch), /storage.bucket is required/,
    "direct construction cannot sign any plan, let alone the replacement location");

  const moved = JSON.parse(JSON.stringify(exact.savedDestination));
  moved.locationDigest = "sha256:" + "f".repeat(64);
  moved.storage.bucket = "moved-bucket";
  assert.match(frozenDestinationProblem(exact.point, moved), /moved/);
  assert.throws(() => requireFrozenDestination(exact.point, moved), /moved/,
    "a changed location under the same UID is refused before rendering");
  const digestMismatch = initialState(
    exact.ns, exact.clusters, exact.backups, exact.selection, moved,
  );
  assert.equal(digestMismatch.fields.source.bucket, "");
  assert.match(validateRestore(digestMismatch).archive, /moved/);
  await assert.rejects(() => preparePlan(digestMismatch), /storage.bucket is required/);
});

test("the_wizard_mount_resolves_the_point_destination_before_it_renders_a_plan", async () => {
  const backups = fixture("wizard-backups.json");
  const selection = newestPoint(backups);
  const point = backups.items.find((item) => item.metadata.uid === selection.uid);
  const destination = fixture("console/destination.json").item;
  point.spec.destinationRef = { name: "primary", uid: destination.uid };
  point.spec.archive = { url: "logweir-destination://primary" };
  point.status.locationDigest = destination.locationDigest;
  const calls = [];
  const api = {
    list: async (_ns, plural) => plural === "backups" ? backups : fixture("wizard-clusters.json"),
    destination: async (ns, name) => {
      calls.push({ ns: ns, name: name });
      return fixture("console/destination.json");
    },
  };
  const node = {
    children: [],
    get firstChild() { return this.children.length === 0 ? null : this.children[0]; },
    removeChild() { return this.children.shift(); },
    appendChild(child) { this.children.push(child); return child; },
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
  await mountRestoreWizard(
    node, "logweir-t27", selection, (html) => [{ html: html }], api,
    { signal: undefined, isCurrent: () => true },
  );
  assert.deepEqual(calls, [{ ns: "logweir-t27", name: "primary" }]);
  const html = node.children.map((child) => child.html || "").join("");
  assert.match(html, /kafka-backups/);
  assert.doesNotMatch(html, /id="store-endpoint"/);
  assert.doesNotMatch(html, /bucket: &quot;primary&quot;/,
    "MUTANT: the mount must not parse logweir-destination://primary as S3");
});

test("each_deliberate_readiness_click_observes_external_state_again", () => {
  const first = nextRestoreReadinessAttempt("team-a", "point-uid");
  const second = nextRestoreReadinessAttempt("team-a", "point-uid");
  assert.notEqual(first, second,
    "MUTANT: checking the same plan again must not replay the pre-collision verdict");
  assert.match(first, /^team-a\.point-uid\.[0-9a-f]{32}-\d+$/);
});

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
    preflight: preflight(prepared.hash, { state: "running", terminal: false, applicable: false }),
  };
  assert.ok(String(readinessRefusal(state, prepared)).includes("has not finished"));

  // AND A CHECK THAT IS BOTH UNFINISHED AND INVALIDATED reads as INVALIDATED.
  // The order matters: a check whose inputs have moved is about the old
  // question, and "wait for its verdict" would be telling an operator to wait
  // for an answer that will not apply when it arrives. `stale` is asked first;
  // `applicable: false` alone is not, because every freshly started check
  // reports it.
  state.readiness = {
    boundHash: prepared.hash,
    preflight: preflight(prepared.hash, {
      state: "running", terminal: false, applicable: false, stale: true,
      staleReasons: [{ reason: "referentChanged", kind: "KafkaCluster", name: "target-b" }],
    }),
  };
  const invalidated = String(readinessRefusal(state, prepared));
  assert.ok(invalidated.includes("no longer applies"), invalidated);
  assert.ok(invalidated.includes("referentChanged") && invalidated.includes("target-b"),
    invalidated);
  assert.ok(!invalidated.includes("has not finished"), invalidated);

  // THE NEGATIVE CONTROL: an applicable, ready, terminal verdict for THIS
  // plan submits. Every arm above therefore fails when its rule is removed.
  state.readiness = { boundHash: prepared.hash, preflight: preflight(prepared.hash) };
  assert.equal(readinessRefusal(state, prepared), null);

  // AN UNCHECKED PLAN IS A WARNING AND NOT A REFUSAL, and says what was not
  // looked for. This is legacy mode's arm too: it has no readiness route, so
  // its result is absent and it lands here. (An earlier draft had a second arm
  // keyed on `readiness.unavailable`, which nothing in the wizard sets -- a
  // refusal-bypass and a sentence no reader could reach. It is gone.)
  const unchecked = wizardState();
  assert.equal(readinessRefusal(unchecked, prepared), null);
  assert.ok(renderPlanStep(prepared, unchecked).includes("id=\"readiness-not-run\""));
  assert.ok(!renderPlanStep(prepared, unchecked).includes("readiness-ungated"),
    "and there is no second, unreachable arm beside it");
});

test("a_target_swap_invalidates_the_verdict_even_when_the_plan_bytes_do_not_move", () => {
  // D2 SECTION 6.6's SECOND INVALIDATION CAUSE. The API returns a result as
  // applicable only while `binding.inputsDigest` still matches its
  // recomputation from current objects, and `referentChanged:<Kind>/<name>` is
  // one of the reasons: "Choosing another target or destination, or a
  // recreated one, changes a referent UID, so the result is stale."
  //
  // THE PLAN HASH CANNOT SEE THAT. Two `KafkaCluster` objects with the same
  // bootstrap servers and the same auth render IDENTICAL plan bytes, so the
  // hash arm holds and a `ready` verdict about the OTHER cluster stayed on
  // screen with the submit enabled. The live journey sat in exactly this blind
  // spot and recorded it as a journey limitation; it was a product gap.
  const clusters = fixture("wizard-clusters.json");
  const items = clusters.items;
  const twin = JSON.parse(JSON.stringify(items[0]));
  twin.metadata = Object.assign({}, twin.metadata, {
    name: twin.metadata.name + "-twin",
    uid: twin.metadata.uid.slice(0, -1) + (twin.metadata.uid.endsWith("0") ? "1" : "0"),
  });
  const backups = fixture("wizard-backups.json");
  const state = initialState("logweir-t27", { items: items.concat([twin]) }, backups,
    newestPoint(backups));
  selectTarget(state, items[0].metadata.uid, items[0].metadata.name);
  const before = JSON.stringify(state.fields.target.bootstrapServers);

  const prepared = { bytes: "b", hash: "sha256:aaa", restoreName: "r", approvalName: "a" };
  state.readiness = { boundHash: prepared.hash, preflight: preflight(prepared.hash) };
  assert.equal(readinessRefusal(state, prepared), null, "a ready verdict for this plan submits");

  selectTarget(state, twin.metadata.uid, twin.metadata.name);
  assert.equal(
    JSON.stringify(state.fields.target.bootstrapServers),
    before,
    "the twin renders the SAME plan bytes, which is what makes the hash arm blind here",
  );

  // AND THE SUBMIT IS REFUSED, not merely warned about. D2 section 6.6 puts a
  // referent change and a plan-hash change under one rule, so the two must
  // produce the same answer: refused until the check is run again. An earlier
  // fix DELETED the verdict, which landed on the "nothing has run" arm and
  // PERMITTED the submit -- the second review named the asymmetry.
  const refused = readinessRefusal(state, prepared);
  assert.ok(typeof refused === "string", "the submit is refused after a target swap");
  assert.ok(refused.includes("referentChanged"),
    "with the server's own reason named: " + refused);
  assert.ok(refused.includes(twin.metadata.name),
    "and the cluster it is about: " + refused);
  assert.ok(refused.includes("Run it again"), refused);
  const step6 = renderPlanStep(prepared, state);
  assert.ok(step6.includes("id=\"readiness-blocked\""), "the page says so");
  assert.ok(step6.includes("id=\"create-restore\" class=\"primary\" disabled"),
    "and the button is disabled: " + step6);
  assert.ok(!step6.includes("id=\"readiness-not-run\""),
    "this is a stale verdict, not an absent one");

  // THE NEGATIVE CONTROL: re-selecting the SAME uid is not a change and leaves
  // the verdict applicable, so the rule is a change rule and not "always
  // refuse".
  state.readiness = { boundHash: prepared.hash, preflight: preflight(prepared.hash) };
  selectTarget(state, twin.metadata.uid, twin.metadata.name);
  assert.equal(readinessRefusal(state, prepared), null,
    "re-selecting the same target is not a target change");
  assert.equal(state.readiness.preflight.applicable, true);

  // AND A SWAP WITH NO VERDICT HELD CHANGES NOTHING: there is nothing to mark,
  // so the page stays on its "nothing has run" warning rather than inventing a
  // stale verdict out of an absent one.
  const fresh = initialState("logweir-t27", { items: items.concat([twin]) }, backups,
    newestPoint(backups));
  selectTarget(fresh, twin.metadata.uid, twin.metadata.name);
  assert.equal((fresh.readiness || {}).preflight, undefined);
  assert.equal(readinessRefusal(fresh, prepared), null);
});

test("the_prefix_is_one_value_in_both_modes_and_the_plan_maps_through_it", () => {
  // THE RUNNER'S GRAMMAR CARRIES TWO PREFIX KEYS AND READS A DIFFERENT ONE PER
  // MODE (`logweir_core::spec::target_topic_prefix`): `topic_naming.prefix` for
  // `newTopic`, `topic_mapping_prefix` for `scratch`. Only the first used to be
  // updated by an edit, so in `scratch` the preview, the declared mapping and
  // the API rail all used a prefix THE RUN DOES NOT USE -- "submitted
  // topics/mapping equal the preview" was false for that mode. Found by the
  // independent review.
  for (const mode of ["newTopic", "scratch"]) {
    const state = wizardState();
    state.fields.target.mode = mode;
    setTopicPrefix(state, "myprefix-");
    state.fields.topics = ["orders", "payments"];

    assert.equal(state.fields.target.topicPrefix, "myprefix-");
    assert.equal(state.fields.target.topicMappingPrefix, "myprefix-",
      mode + ": one setter writes both keys");
    assert.equal(effectivePrefix(state), "myprefix-", mode);

    // THE PREVIEW IS WHAT THE PLAN EMITS FOR THE KEY THIS MODE READS. Asserted
    // against `renderPlanBytes`'s own output rather than against the field, so
    // a future divergence between the two documents is what fails.
    const bytes = renderPlanBytes(state.fields);
    const lines = bytes.split("\n");
    // `    prefix:` occurs three times in this grammar -- the source store's,
    // the target naming block's and the evidence store's -- so the target one
    // is found by the key that introduces it, not by its own indentation.
    const at = mode === "scratch"
      ? lines.findIndex((l) => l.startsWith("  topic_mapping_prefix: "))
      : lines.findIndex((l) => l === "  topic_naming:") + 1;
    assert.ok(at > 0, mode + ": the plan carries the prefix this mode reads");
    const line = lines[at];
    const emitted = line.slice(line.indexOf(": ") + 2).replace(/^"|"$/g, "");
    assert.equal(emitted, "myprefix-", mode + ": the plan maps through the previewed prefix");
    for (const row of topicMapping(state)) {
      assert.equal(row.target, emitted + row.source, mode + ": " + row.target);
    }
    assert.deepEqual(mappingProblems(state), Object.create(null), mode);
  }

  // AND `effectivePrefix` PICKS THE MODE'S KEY WHEN THEY DISAGREE, which is the
  // only condition under which it does any work. `setTopicPrefix` keeps them
  // equal, so a state built THROUGH the setter cannot tell the two readings
  // apart -- a mutant that made `effectivePrefix` always read `topicPrefix`
  // survived the rows above for exactly that reason. This arm builds the
  // disagreement by hand, which is the state any other producer of a fields
  // object (a hand-written harness, a future prefill, the pre-fix code) could
  // still hand this page, and asserts the reading follows the runner's rule.
  const skewed = wizardState();
  skewed.fields.target.topicPrefix = "naming-";
  skewed.fields.target.topicMappingPrefix = "mapping-";
  skewed.fields.target.mode = "newTopic";
  assert.equal(effectivePrefix(skewed), "naming-", "newTopic reads topic_naming.prefix");
  assert.equal(topicMapping(skewed)[0].target, "naming-orders");
  skewed.fields.target.mode = "scratch";
  assert.equal(effectivePrefix(skewed), "mapping-",
    "scratch reads topic_mapping_prefix -- logweir_core::spec::target_topic_prefix");
  assert.equal(topicMapping(skewed)[0].target, "mapping-orders",
    "and the preview follows the key the RUN reads, not the other one");

  // EVERY WRITE PATH, because the divergence came from three call sites each
  // moving one key: a restored draft and an edit prefill as well as the input.
  const drafted = wizardState();
  drafted.fields.target.mode = "scratch";
  assert.equal(applyWizardDraft(drafted, Object.assign(wizardDraftValues(wizardState()), {
    topicPrefix: "from-a-draft-",
  })), true);
  assert.equal(drafted.fields.target.topicMappingPrefix, "from-a-draft-");
  assert.equal(effectivePrefix(drafted), "from-a-draft-");

  const prefilled = draftFrom(
    { spec: { target: { mode: "scratch", topicNaming: { prefix: "from-an-edit-" } } } },
    wizardState().fields,
  );
  assert.equal(prefilled.target.topicPrefix, "from-an-edit-");
  assert.equal(prefilled.target.topicMappingPrefix, "from-an-edit-");

  // AND THE DECLARATION IS NOT SENT IN `scratch`: the product API never parses
  // the plan, so it holds no value it could check the rows against there, and
  // it refuses a declaration for that mode by name.
  const scratch = wizardState();
  scratch.fields.target.mode = "scratch";
  setTopicPrefix(scratch, "myprefix-");
  const body = restoreBody(scratch, { bytes: "b", hash: "h", restoreName: "r", approvalName: "a" });
  assert.equal(body.topicMapping, undefined, "no declaration is sent in scratch mode");
  const newTopic = wizardState();
  assert.equal(newTopic.fields.target.mode, "newTopic");
  assert.deepEqual(
    restoreBody(newTopic, { bytes: "b", hash: "h", restoreName: "r", approvalName: "a" })
      .topicMapping,
    topicMapping(newTopic),
    "and it IS sent in newTopic, which is the mode it is defined for",
  );
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

  // AND THE SELECTOR IS REACHABLE. The retry route names no point, so the
  // wizard renders its selector first and `defaultPrefixFor` is called with no
  // instant: it must answer "" rather than throw, or the retry link replaces
  // the whole page with a render error. A live journey met exactly that.
  const noPoint = initialState("logweir-t27", fixture("wizard-clusters.json"), backups,
    { uid: "", backup: "", retryOf: failedName });
  assert.equal(noPoint.pointState, "none", "the retry route lands on the selector");
  assert.equal(freshTargetPrefix(undefined, failedName), "");
  assert.equal(freshTargetPrefix("not an instant", failedName), "");
  assert.equal(defaultPrefixFor(noPoint), "");
  assert.ok(renderPointSelector(noPoint).includes("Retry to a fresh target from this point"),
    "and the selector's rows carry the retry forward");
  assert.ok(renderPointSelector(noPoint).includes("id=\"retry-banner\""));

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
  // A TERMINAL REFUSAL IS THE SHARPER CASE, not a different one: that Restore's
  // name and its Approval name are already taken, so a retry of the same point
  // with the same prefix would mint exactly them again.
  assert.ok(
    renderRetryAction(Object.assign({}, failedView, { state: "refused" }), "logweir-t27")
      .includes("Retry to a fresh target"),
    "a terminally refused restore offers the retry too",
  );
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
  // AND THE COMPLAINT PARAGRAPH IS WHAT DISTINGUISHES THE TWO STATES, because
  // the bound itself is printed beside the input in every state: a test that
  // asserted "the window message is on the page" would hold for an accepted
  // point too. The live journey's negative control caught exactly that.
  state.fields.pointInTime = new Date(covered.toMs).toISOString();
  assert.ok(!renderPointInTimeStep(state).includes("id=\"point-in-time-complaint\""));
  state.fields.pointInTime = new Date(covered.toMs + 1).toISOString();
  assert.ok(renderPointInTimeStep(state).includes("id=\"point-in-time-complaint\""));

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
