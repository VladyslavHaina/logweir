// complete-coverage.spec.js -- PROD-08.1a: the console asks for complete
// coverage as an explicit, advanced choice with its cost stated, and says
// sampled or complete -- with a complete verification's exact counts and
// whether it covered -- wherever a restore's result is shown.
//
// THE RULE EVERY ROW HERE HOLDS: a complete verification that did NOT cover
// the restore (`covered: false`) never reads as a pass -- not on a badge, not
// on a list row's verdict, not in the detail, not in the operation view.
//
// BOTH SIDES READ ONE FIXTURE. `fixtures/console/restore-complete-uncovered.json`
// and `fixtures/console/operation-restore-complete-uncovered.json` are the
// product API's projections of `fixtures/restore-complete-uncovered.json` (the
// custom resource), written by `crates/logweir-api/tests/complete_coverage.rs`;
// this file decodes and renders the same bytes, so the names cannot drift.
// `plan-complete.golden.yaml` is read by `crates/logweir/tests/ui_lint.rs` too,
// which deserialises it into the runner's `RestoreSpec`.
//
// NEGATIVE CONTROL: each assertion names the mutant it kills.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  COMPLETE_COVERAGE_COST,
  completeCoverageSentence,
  notCovered,
  verificationScopeSentence,
} from "../render.js";
import { decodeConsoleItem, decodeD3Operation } from "../contract.js";
import {
  NOT_COVERED_CAPTION,
  coverageOf,
  renderHistoryList,
  renderRestoreDetail,
  restoreBadge,
} from "../pages/history.js";
import { renderOperation, renderCoverage, operationFacts } from "../pages/operation.js";
import {
  COVERAGE_CHOICE_LABEL,
  WIZARD_DRAFT_FIELDS,
  applyWizardDraft,
  coverageText,
  initialState,
  mountRestoreWizard,
  preparePlan,
  preparePlanOrProblem,
  recoveryPoints,
  renderCoverageChoice,
  renderPlanStep,
  restoreBody,
  setCoverage,
  verificationPlanSentence,
  wizardDraftValues,
} from "../pages/restore-wizard.js";
import { COVERAGE_COMPLETE, renderPlanBytes } from "../plan.js";
import { keepDraft, readDraft } from "../lifecycle.js";
import { fakeView, parse as viewParse } from "./fake-view.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const text = (name) => readFileSync(FIXTURES + name, "utf8");

/** What an operator reads of a rendered fragment: no tags, entities decoded. */
const visible = (html) => html.replace(/<[^>]*>/g, "").replace(/&#39;/g, "'")
  .replace(/&quot;/g, "\"").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");

const clone = (value) => JSON.parse(JSON.stringify(value));

/** The custom resource: a complete verification past its bound. */
const uncovered = () => fixture("restore-complete-uncovered.json");

/** The same object made to look like a pass in every field but `covered` --
 *  a status arm IV-6 keeps out of every accepted document, and the case a
 *  badge rule that trusted `outcome` alone would paint green. */
function forgedPass(covered) {
  const object = uncovered();
  const s = object.status;
  s.phase = "Succeeded";
  s.exitCode = 0;
  s.outcome = "pass";
  s.integrity.result = "pass";
  s.integrity.complete.covered = covered;
  if (covered) {
    delete s.integrity.complete.incompleteReason;
  }
  return object;
}

// -------------------------------------------------------- the console decode

test("prod081a_the_apis_coverage_decodes_and_projects_under_the_crds_names", async () => {
  // Through the REAL console client, decoder and projection, as the History
  // list and detail read it.
  const decoded = decodeConsoleItem("restores", fixture("console/restore-complete-uncovered.json"));
  const item = decoded.value.item || decoded.value;
  assert.deepEqual(item.coverage.requested, "complete");
  assert.equal(item.coverage.covered, false,
    "NEGATIVE CONTROL: a decoder that drops `covered` fails this");
  const operation = decodeD3Operation(fixture("console/operation-restore-complete-uncovered.json"));
  const scope = operation.value.item.verificationScope;
  assert.equal(scope.coverage, "complete");
  assert.equal(scope.complete.partitions.length, 2);

  const { apiClient, resetMode, selectMode } = await import("../client.js");
  resetMode();
  await selectMode({
    probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }),
  });
  const original = globalThis.fetch;
  globalThis.fetch = (url) => Promise.resolve({
    ok: true,
    status: 200,
    headers: { get: () => null },
    text: () => Promise.resolve(JSON.stringify(String(url).includes("/operations")
      ? fixture("console/operation-restore-complete-uncovered.json")
      : String(url).split("?")[0].endsWith("/restores")
        ? { requestId: "r", items: [fixture("console/restore-complete-uncovered.json").item],
          page: { limit: 50, nextCursor: null, snapshot: null } }
        : fixture("console/restore-complete-uncovered.json"))),
  });
  try {
    const object = await apiClient().get("team-a", "restores", item.name);
    assert.equal(object.spec.coverage, "complete",
      "NEGATIVE CONTROL: a projection that drops the request fails this");
    assert.equal(object.spec.completeMaxRecords, 300);
    assert.equal(object.status.integrity.coverage, "complete");
    assert.equal(object.status.verificationScope.complete.covered, false,
      "NEGATIVE CONTROL: `mergeOperation` dropping the complete block fails this");
    // THE CONSOLE DETAIL: the request, the signed coverage NOT covered, the
    // reason and every partition's counts.
    const html = renderRestoreDetail(object);
    assert.match(html, /<section class="complete-coverage" id="complete-coverage" data-covered="false">/);
    assert.ok(visible(html).includes("sample.complete_max_records is 300"), visible(html));
    assert.doesNotMatch(html, /badge-green/,
      "NEGATIVE CONTROL: a console detail painting a covered: false run green fails this");
    // THE CONSOLE LIST ROW.
    const list = await apiClient().list("team-a", "restores");
    const row = renderHistoryList(list, { items: [] }, "team-a");
    assert.match(row, /data-coverage="complete" data-covered="false">coverage: complete, NOT covered -- not a pass</,
      "NEGATIVE CONTROL: a list row with no coverage verdict fails this:\n" + row);
    assert.doesNotMatch(row, /badge-green/);
  } finally {
    globalThis.fetch = original;
    resetMode();
  }
});

// ------------------------------------------------------- never a pass

test("prod081a_a_covered_false_run_is_never_green_on_a_badge_even_beside_a_forged_pass", () => {
  // The fixture as signed: fail-integrity, partial.
  assert.match(restoreBadge(uncovered().status), /badge-unverified/);
  // A status that says pass in every field but `covered`.
  const forged = restoreBadge(forgedPass(false).status);
  assert.match(forged, /badge-unverified/,
    "NEGATIVE CONTROL: the badge's `notCovered` arm removed paints this green");
  assert.ok(forged.includes(NOT_COVERED_CAPTION), forged);
  // The control: the same status with `covered: true` IS green, so the rule
  // above is about `covered` and not a badge that is never green.
  assert.match(restoreBadge(forgedPass(true).status), /badge-green/);
  // And a console LIST row whose summary says verified, beside covered: false.
  const status = {
    __summary: { verifiedSuccess: true, verificationState: "valid" },
    integrity: { coverage: "complete", complete: { covered: false } },
  };
  assert.match(restoreBadge(status), /badge-unverified/,
    "NEGATIVE CONTROL: a summary badge read before the coverage fails this");
  assert.equal(notCovered({ covered: false }), true);
  assert.equal(notCovered({ covered: true }), false);
  assert.equal(notCovered(null), false);
});

test("prod081a_the_list_row_says_complete_and_not_covered_and_never_pass", () => {
  for (const object of [uncovered(), forgedPass(false)]) {
    const html = renderHistoryList({ items: [object] }, { items: [] }, "team-a");
    assert.match(html,
      /data-coverage="complete" data-covered="false">coverage: complete, NOT covered -- not a pass<\/span>/,
      "NEGATIVE CONTROL: a RESULT cell without the coverage line fails this");
    assert.doesNotMatch(html, /badge-green/,
      "NEGATIVE CONTROL: a list verdict green over covered: false fails this");
  }
  const covered = renderHistoryList({ items: [forgedPass(true)] }, { items: [] }, "team-a");
  assert.match(covered, /coverage: complete, covered</);

  // A CONSOLE LIST ROW (review RM5): no recorded verification block, only the
  // API's summary, which says verified -- beside `covered: false` and an
  // outcome that reads pass. Its RESULT cell must still carry the
  // unverified-claim caption: the row's own verdict refuses covered: false
  // even if a summary ever said otherwise.
  const consoleRow = (coveredValue) => {
    const object = forgedPass(coveredValue);
    delete object.status.evidence;
    object.status.__summary = { verifiedSuccess: true, verificationState: "valid" };
    return object;
  };
  const claimed = renderHistoryList({ items: [consoleRow(false)] }, { items: [] }, "team-a");
  assert.match(claimed, /data-scorecard-claim="true"/,
    "NEGATIVE CONTROL: `listRowVerified` without its `notCovered` clause calls this pass verified");
  // The control: the same console row with `covered: true` IS a verified
  // claim, so the caption above is about `covered`.
  const verified = renderHistoryList({ items: [consoleRow(true)] }, { items: [] }, "team-a");
  assert.doesNotMatch(verified, /data-scorecard-claim="true"/, verified);
});

test("prod081a_the_detail_shows_the_request_the_signed_coverage_and_every_partitions_counts", () => {
  const html = renderRestoreDetail(uncovered());
  const said = visible(html);
  assert.ok(said.includes("complete -- every record of every restored partition, at most 300 " +
    "archived records"), "NEGATIVE CONTROL: no `coverage (asked for)` row fails this");
  assert.match(html, /<span id="restore-coverage-signed" data-covered="false">/);
  assert.ok(said.includes("complete, NOT covered -- not a pass"));
  // The section, the reason, the counts, and the one partition not compared.
  assert.match(html, /<section class="complete-coverage" id="complete-coverage" data-covered="false">/,
    "NEGATIVE CONTROL: a detail that drops the complete block fails this");
  assert.ok(said.includes("Complete coverage was asked for and did NOT cover this restore: " +
    "sample.complete_max_records is 300: orders/1 and every later partition were not compared. " +
    "1 of 2 partitions were compared. A verification that did not cover the restore is never " +
    "a pass."), said);
  assert.match(html, /<td>orders<\/td><td>1<\/td><td><strong>NO<\/strong><\/td>/);
  assert.match(html, /<td>orders<\/td><td>0<\/td><td>yes<\/td><td>250<\/td><td>250<\/td><td>250<\/td>/);
  assert.match(html, /badge-unverified/);
  // A run that asked for complete coverage and recorded none yet says the cost
  // and what it asked for -- not a coverage it does not have.
  const pending = uncovered();
  pending.status = { phase: "Running" };
  const p = renderRestoreDetail(pending);
  assert.ok(visible(p).includes("complete asked for; not recorded yet"), visible(p));
  assert.match(p, /id="restore-coverage-cost"/);
  assert.doesNotMatch(p, /id="complete-coverage"/);
});

test("prod081a_the_operation_view_says_complete_and_not_covered_in_both_modes", () => {
  const console_ = decodeD3Operation(fixture("console/operation-restore-complete-uncovered.json"))
    .value.item;
  for (const [document, consoleMode] of [[console_, true], [uncovered(), false]]) {
    const html = renderOperation({
      ns: "team-a", kind: "restore", name: "orders-drill-complete", uid: "",
      document: document, console: consoleMode, meta: { transport: "poll", attempt: 0 },
    });
    assert.match(html, /<section class="coverage" id="operation-coverage" data-covered="false">/,
      "NEGATIVE CONTROL: an operation view with no coverage section (the completion panel is " +
        "hidden for a failed run) fails this: console=" + consoleMode);
    assert.ok(visible(html).includes("complete, NOT covered -- not a pass"));
    assert.match(html, /<strong>NO<\/strong>/);
    // THE RESULT AND THE COVERAGE SAY "NOT A PASS". The evidence section's own
    // badge is about the SIGNATURE -- a signed failure verifies, and saying so
    // is the point of publishing it (FAILED-DRILL-EVIDENCE-UNPUBLISHED) -- so
    // it is not the verdict, and the verdict sections carry no green at all.
    const result = /<section class="result">[\s\S]*?<\/section>/.exec(html)[0];
    const coverage = /<section class="coverage"[\s\S]*?<\/section><\/section>/.exec(html)[0];
    assert.ok(visible(result).includes("fail-integrity"), visible(result));
    assert.ok(!/\bpass\b/.test(visible(result).replace(/notPass/g, "")),
      "the result section never says pass: " + visible(result));
    assert.doesNotMatch(result + coverage, /badge-green/);
  }
  // A sampled run's operation view names sampled and draws no complete block.
  const sampled = renderCoverage(operationFacts(fixture("restore-valid-pass.json"), false));
  assert.ok(visible(sampled).includes("sampled (not recorded)"), visible(sampled));
  assert.doesNotMatch(sampled, /complete-coverage/);
});

// ----------------------------------------------- a sampled run never claims complete

test("prod081a_a_sampled_run_never_claims_complete", () => {
  // A scorecard before 1.4.0: nothing recorded, read as sampled, never complete.
  const plain = fixture("restore-valid-pass.json");
  const html = renderRestoreDetail(plain);
  assert.ok(visible(html).includes("sampled -- the canary and the manifest's count bound"));
  assert.ok(visible(html).includes("sampled (not recorded)"));
  assert.doesNotMatch(html, /id="complete-coverage"/,
    "NEGATIVE CONTROL: a complete block drawn for a sampled run fails this");
  const row = renderHistoryList({ items: [plain] }, { items: [] }, "team-a");
  assert.match(row, /data-coverage="not-recorded">coverage: sampled \(not recorded\)</);
  // A sampled 1.4.0 scorecard, with FX-23's unsampled topics.
  const scope = {
    level: "sampled", recordsSampled: 25, recordsSampledMatching: 25, recordsExpected: 25,
    coverage: "sampled", unsampledTopics: ["audit", "payments"],
  };
  const sentence = verificationScopeSentence(scope);
  assert.ok(sentence.includes("this is a sampled check, not an exhaustive comparison"));
  assert.ok(sentence.includes("The partition cap left audit, payments without a sampled " +
    "partition"), "NEGATIVE CONTROL: a sentence that drops the unsampled topics fails this");
  assert.equal(sentence.indexOf("complete"), -1,
    "the word `complete` never appears in a sampled run's sentence");
  // A status whose coverage says sampled but carries a complete block: the
  // block is not believed by the badge either way, and the detail draws none.
  const odd = clone(plain);
  odd.status.integrity.coverage = "sampled";
  odd.status.integrity.complete = uncovered().status.integrity.complete;
  assert.doesNotMatch(renderRestoreDetail(odd), /id="complete-coverage"/);
  assert.equal(coverageOf(odd).recorded, "sampled");
});

test("prod081a_the_complete_sentence_covers_each_case", () => {
  assert.ok(completeCoverageSentence(null).includes("could not be read here"));
  assert.ok(completeCoverageSentence(null).includes("It is not shown as a pass"));
  const covered = completeCoverageSentence({
    covered: true, replay: { expected: 10, restored: 10, matching: 10, missing: 0,
      unexpected: 0, duplicates: 0, outOfOrder: 0, mismatched: 0 },
  });
  assert.ok(covered.startsWith("Complete coverage: every record of every restored partition"));
  assert.ok(covered.includes("10 of 10 expected records matched byte for byte"));
  assert.equal(verificationScopeSentence({ level: "sampled", coverage: "complete" }),
    completeCoverageSentence(null),
    "a recorded complete coverage with no readable block is said as such, never as sampled");
});

// ------------------------------------------------------- the plan emitter

test("prod081a_the_plan_carries_complete_coverage_exactly_when_it_is_stated", () => {
  // ARM 2 of the golden pair (arm 1 is ui_lint.rs, which parses it into
  // `RestoreSpec` and asserts both values arrive).
  const fields = fixture("plan-complete-fields.json");
  assert.equal(renderPlanBytes(fields), text("plan-complete.golden.yaml"),
    "NEGATIVE CONTROL: an emitter that drops either line fails this");
  // NOT STATED, the bytes are the golden's, byte for byte: an absent field
  // keeps every plan, and its hash, exactly as it was.
  for (const absent of [undefined, null, "", "sampled"]) {
    const sample = Object.assign({}, fields.sample, { coverage: absent });
    delete sample.completeMaxRecords;
    assert.equal(renderPlanBytes(Object.assign({}, fields, { sample })), text("plan.golden.yaml"),
      "NEGATIVE CONTROL: an emitter that always writes the line fails this: " + String(absent));
  }
  for (const bad of ["Complete", "full", true]) {
    const sample = Object.assign({}, fields.sample, { coverage: bad });
    assert.throws(() => renderPlanBytes(Object.assign({}, fields, { sample })),
      /sample.coverage must be "complete", "sampled" or absent/, String(bad));
  }
  // A bound only beside complete coverage, at least 1, and a whole number.
  const sampledBound = Object.assign({}, fields.sample, { coverage: undefined });
  assert.throws(() => renderPlanBytes(Object.assign({}, fields, { sample: sampledBound })),
    /set only with sample.coverage "complete"/);
  for (const bad of [0, 2.5, "ten"]) {
    const sample = Object.assign({}, fields.sample, { completeMaxRecords: bad });
    assert.throws(() => renderPlanBytes(Object.assign({}, fields, { sample })),
      /completeMaxRecords/, String(bad));
  }
  assert.equal(COVERAGE_COMPLETE, "complete");
});

// ------------------------------------------------------- the wizard

function wizardState(ns) {
  const backups = fixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  return initialState(ns || "team-cc", fixture("wizard-clusters.json"), backups, {
    uid: point.metadata.uid,
    backup: point.metadata.name,
  });
}

function pointParams() {
  const point = recoveryPoints(fixture("wizard-backups.json"))[0];
  return { uid: point.metadata.uid, backup: point.metadata.name };
}

function mountApi() {
  const backups = fixture("wizard-backups.json");
  return {
    list: async (_ns, plural) =>
      (plural === "kafkaclusters" ? fixture("wizard-clusters.json") : backups),
    approvalPolicy: async () => null,
    latestDiscoveries: async () => ({ lastSuccessful: null, latestAttempt: null }),
  };
}

test("prod081a_the_wizard_offers_complete_coverage_as_an_advanced_choice_with_its_cost", () => {
  const state = wizardState();
  const html = renderCoverageChoice(state);
  assert.match(html, /<details class="advanced" id="coverage-advanced">/,
    "NEGATIVE CONTROL: a choice shown open by default fails this -- it is advanced");
  assert.match(html, /<input type="checkbox" id="coverage-complete" name="coverage">/,
    "NEGATIVE CONTROL: a box the page ticks by default fails this -- never the default");
  assert.match(html, /<input id="coverage-bound" name="completeMaxRecords" inputmode="numeric" value="" disabled>/);
  assert.ok(visible(html).includes(COVERAGE_CHOICE_LABEL));
  assert.ok(visible(html).includes("about a minute per GiB of one-KiB records"),
    "NEGATIVE CONTROL: a choice offered without its cost fails this");
  assert.ok(visible(html).includes("covered: false, which is never a pass"));
  assert.equal(COMPLETE_COVERAGE_COST.split("`covered: false`").length, 2);
  // The default plan sentence is the sampled one; the choice changes it.
  assert.ok(verificationPlanSentence(state).includes("sampled check"));
  // It points at the choice below it and never says no level compares every
  // record (review L5: that read as a contradiction above this very choice).
  assert.ok(verificationPlanSentence(state)
    .includes("choose complete coverage below to compare every record"),
  "NEGATIVE CONTROL: the sentence before PROD-08.1a's choice fails this");
  assert.doesNotMatch(verificationPlanSentence(state), /no level in this version/);
  setCoverage(state, true, "5000");
  assert.match(renderCoverageChoice(state), /id="coverage-advanced" open>/);
  assert.match(renderCoverageChoice(state), /id="coverage-complete" name="coverage" checked>/);
  assert.ok(verificationPlanSentence(state).includes("every record of every restored partition"));
  assert.ok(verificationPlanSentence(state).includes("at most 5000 archived records"));
  // Unticked again, both fields go: no bound survives on a sampled plan.
  setCoverage(state, false, "5000");
  assert.equal(state.fields.sample.coverage, undefined);
  assert.equal(state.fields.sample.completeMaxRecords, undefined);
});

test("prod081a_the_choice_reaches_the_plan_the_review_and_the_create_request", async () => {
  const state = wizardState();
  let prepared = await preparePlan(state);
  assert.ok(!prepared.bytes.includes("coverage"), "unticked, the plan states none");
  assert.equal(restoreBody(state, prepared).spec.coverage, undefined,
    "and the Restore declares none: the object every earlier restore was");
  const before = prepared.hash;
  setCoverage(state, true, "1000000");
  prepared = await preparePlan(state);
  assert.ok(prepared.bytes.includes("\n  coverage: \"complete\"\n  complete_max_records: 1000000\n"),
    "NEGATIVE CONTROL: a wizard that does not hand the choice to the emitter fails this");
  assert.notEqual(prepared.hash, before,
    "the choice is INSIDE the plan hash: an approval of the sampled plan does not cover it");
  const html = renderPlanStep(prepared, state);
  assert.equal(visible(/<span id="review-coverage">([^<]*)<\/span>/.exec(html)[1]),
    coverageText(state));
  assert.ok(coverageText(state).includes("complete_max_records: 1000000"));
  assert.match(html, /<p class="caveat" id="review-coverage-cost">/,
    "NEGATIVE CONTROL: a review step that does not state the cost fails this");
  // The Restore DECLARES what the plan says, from the same field.
  const body = restoreBody(state, prepared);
  assert.equal(body.spec.coverage, "complete",
    "NEGATIVE CONTROL: a body that omits the declaration (the controller then refuses the " +
      "complete plan as undeclared) fails this");
  assert.equal(body.spec.completeMaxRecords, 1000000);
  // A bound that is not a whole number is not guessed: the plan names it.
  setCoverage(state, true, "a lot");
  prepared = await preparePlanOrProblem(state);
  assert.equal(typeof prepared.problem, "string");
  assert.ok(prepared.problem.includes("completeMaxRecords"), prepared.problem);
});

test("prod081a_the_draft_keeps_the_choice_and_nothing_else_as_it", () => {
  assert.ok(WIZARD_DRAFT_FIELDS.includes("coverage") &&
    WIZARD_DRAFT_FIELDS.includes("completeMaxRecords"),
    "NEGATIVE CONTROL: a draft that drops the fields brings the plan back sampled");
  const state = wizardState();
  assert.equal(wizardDraftValues(state).coverage, "");
  setCoverage(state, true, "750");
  keepDraft("prod081a-draft-probe", wizardDraftValues(state), WIZARD_DRAFT_FIELDS);
  const kept = readDraft("prod081a-draft-probe");
  const again = wizardState();
  assert.equal(applyWizardDraft(again, kept), true);
  assert.equal(again.fields.sample.coverage, "complete");
  assert.equal(again.fields.sample.completeMaxRecords, 750);
  for (const other of [undefined, "", "Complete", true]) {
    const older = wizardState();
    setCoverage(older, true, "1");
    applyWizardDraft(older, Object.assign({}, kept, { coverage: other }));
    assert.equal(older.fields.sample.coverage, undefined,
      "NEGATIVE CONTROL: a draft that keeps an unparsed value as the choice fails this: " +
        String(other));
  }
});

test("prod081a_the_mounted_wizard_round_trips_the_advanced_choice", async () => {
  const view = fakeView();
  await mountRestoreWizard(view.root, "team-cc-mount", pointParams(), viewParse, mountApi());
  const box = view.find("#coverage-complete");
  assert.ok(box !== null, "step 4 carries the choice");
  assert.equal(box.checked, false);
  assert.ok(!visible(view.html()).includes("coverage: \"complete\""), "unticked, none");
  box.checked = true;
  await box.dispatch("change");
  let html = visible(view.html());
  assert.ok(html.includes("coverage: \"complete\""),
    "NEGATIVE CONTROL: a box `refresh` does not read fails this:\n" + html.slice(0, 400));
  assert.equal(view.find("#coverage-complete").checked, true, "the repaint keeps it ticked");
  const bound = view.find("#coverage-bound");
  bound.value = "250000";
  await bound.dispatch("change");
  html = visible(view.html());
  assert.ok(html.includes("complete_max_records: 250000"),
    "NEGATIVE CONTROL: a bound input `refresh` does not read fails this");
  assert.match(html, /coverage\s*complete \(sample\.coverage: complete, complete_max_records: 250000\)/);
  const again = view.find("#coverage-complete");
  again.checked = false;
  await again.dispatch("change");
  html = visible(view.html());
  assert.ok(!html.includes("coverage: \"complete\"") && !html.includes("complete_max_records"),
    "and unticking takes both out");
});
