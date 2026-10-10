// original-name.spec.js -- PROD-15.1: the console restores under the ORIGINAL
// topic names only as an explicit choice in `newTopic` mode, with the owner
// statement the runner needs; it renders the plan the runner reads, declares
// the choice on the request, and shows the separate approval subject,
// `originalName`, wherever an approver reads what they approve.
//
// BOTH SIDES READ ONE GOLDEN. `fixtures/plan-original-name.golden.yaml` is
// rendered here from `fixtures/plan-original-name-fields.json` and
// deserialised by `crates/logweir/tests/ui_lint.rs` into the runner's
// `RestoreSpec`.
//
// NEGATIVE CONTROL: each assertion names the mutant it kills.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  ORIGINAL_NAME_COVERAGE_SENTENCE,
  ORIGINAL_NAME_NO_OWNER_STATEMENT,
  renderCoverageChoice,
  requireCompleteCoverage,
  setCoverage,
  applyWizardDraft,
  parseTypedTopics,
  renderTypedConfirmation,
  selectedTopics,
  submitRestore,
  typedConfirmationRequired,
  typedTopicsProblem,
  approvalSubjectText,
  draftFrom,
  initialState,
  mappingProblems,
  originalNameChosen,
  preparePlan,
  preparePlanOrProblem,
  recoveryPoints,
  renderOriginalNameChoice,
  renderPlanStep,
  renderTargetStep,
  restoreBody,
  setOriginalName,
  topicMapping,
  validateRestore,
  wizardDraftValues,
} from "../pages/restore-wizard.js";
import {
  approvalSubjectOf,
  renderApprovalStatus,
  restoreApprovalSubject,
  typedTopicsOf,
} from "../pages/approvals.js";
import { renderPlanBytes } from "../plan.js";
import { decodeConsoleItem } from "../contract.js";
import { schemaFindings, wire as wireDocument } from "./console-fixture.js";

/** A console fixture's wire document, with `change` applied to it first. */
function wire(name, change) {
  const body = wireDocument(name);
  if (typeof change === "function") {
    change(body);
  }
  return body;
}

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const text = (name) => readFileSync(FIXTURES + name, "utf8");
const visible = (html) => html.replace(/<[^>]*>/g, "").replace(/&#39;/g, "'")
  .replace(/&quot;/g, "\"").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");

function wizardState() {
  const backups = fixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  const state = initialState("team-on", fixture("wizard-clusters.json"), backups, {
    uid: point.metadata.uid,
    backup: point.metadata.name,
  });
  state.fields.target.mode = "newTopic";
  return state;
}

// ------------------------------------------------------------ the plan

test("prod151_the_plan_the_console_renders_is_the_golden_the_runner_reads", () => {
  // KILLS: an emitter that renders the block under another key, or the prefix
  // as anything but the empty string (ui_lint.rs parses the same golden).
  const fields = fixture("plan-original-name-fields.json");
  assert.equal(renderPlanBytes(fields), text("plan-original-name.golden.yaml"));
  assert.match(renderPlanBytes(fields), /topic_naming:\n {4}prefix: ""\n {4}original_name:\n {6}owners: \[\]\n/);
  // Without the owner statement there is no plan: the runner would refuse it.
  const unstated = JSON.parse(JSON.stringify(fields));
  unstated.target.originalNameNoOwner = false;
  assert.throws(() => renderPlanBytes(unstated), /OriginalNameOwnerNotChecked/);
  // In scratch mode there is no plan either: the identity ban stays.
  const scratch = JSON.parse(JSON.stringify(fields));
  scratch.target.mode = "scratch";
  assert.throws(() => renderPlanBytes(scratch), /OriginalNameNotNewTopic/);
  // The plain plan carries no block (KILLS: a block rendered for every plan).
  assert.doesNotMatch(renderPlanBytes(fixture("plan-fields.json")), /original_name/);
});

// ------------------------------------------------------------ the wizard

test("prod151_the_choice_maps_every_topic_onto_itself_and_keeps_the_scratch_prefix", () => {
  const state = wizardState();
  const before = state.fields.target.topicMappingPrefix;
  setOriginalName(state, true, true);
  assert.equal(originalNameChosen(state), true);
  assert.equal(state.fields.target.topicPrefix, "");
  assert.equal(state.fields.target.topicMappingPrefix, before,
    "NEGATIVE CONTROL: a choice that emptied the scratch prefix (the probe's namespace) fails");
  for (const row of topicMapping(state)) {
    assert.equal(row.target, row.source);
  }
  // The identity map is not refused for this plan, and nothing else is either.
  assert.deepEqual(Object.keys(mappingProblems(state)), []);
  // Off again: the default prefix is back and the identity map is refused as
  // before (KILLS: an empty prefix that survives the box being unticked).
  setOriginalName(state, false, false);
  assert.equal(originalNameChosen(state), false);
  assert.ok(state.fields.target.topicPrefix.length > 0);
  state.fields.target.topicPrefix = "";
  assert.ok(typeof mappingProblems(state).topicPrefix === "string");
});

test("prod151_the_owner_statement_is_required_before_anything_is_sent", () => {
  // KILLS: a plan rendered with no place an owner was looked for.
  const state = wizardState();
  setOriginalName(state, true, false);
  assert.match(mappingProblems(state).originalName, /no declarative owner/);
  assert.ok("originalName" in validateRestore(state));
  setOriginalName(state, true, true);
  assert.equal(mappingProblems(state).originalName, undefined);
});

test("prod151_scratch_mode_never_offers_or_keeps_the_choice", () => {
  // KILLS: the box offered in scratch mode; a choice that survives a switch.
  const state = wizardState();
  state.fields.target.mode = "scratch";
  assert.equal(renderOriginalNameChoice(state), "");
  setOriginalName(state, true, true);
  assert.equal(originalNameChosen(state), false);
  state.fields.target.mode = "newTopic";
  setOriginalName(state, true, true);
  state.fields.target.mode = "scratch";
  assert.equal(originalNameChosen(state), false);
});

test("prod151_the_request_declares_the_choice_and_the_identity_mapping", async () => {
  // KILLS: a request that sends the empty prefix without the declaration
  // (the API refuses it) or the declaration on an ordinary restore.
  const state = wizardState();
  setOriginalName(state, true, true);
  const prepared = await preparePlanOrProblem(state);
  assert.equal(typeof prepared.problem, "undefined", String(prepared.problem));
  const body = restoreBody(state, prepared);
  assert.deepEqual(body.spec.target.topicNaming, { prefix: "", originalName: true });
  for (const row of body.topicMapping) {
    assert.equal(row.target, row.source);
  }
  assert.match(prepared.bytes, /original_name:\n {6}owners: \[\]/);

  const plain = wizardState();
  const plainBody = restoreBody(plain, await preparePlanOrProblem(plain));
  assert.equal(plainBody.spec.target.topicNaming.originalName, undefined);
});

test("prod151_the_target_step_and_the_review_show_the_choice_and_the_subject", async () => {
  const state = wizardState();
  const off = visible(renderTargetStep(state));
  assert.match(off, /Restore under the original topic names/);
  assert.doesNotMatch(off, /No declarative owner/,
    "the owner statement is asked only once the choice is made");
  setOriginalName(state, true, false);
  const on = renderTargetStep(state);
  assert.match(on, /id="topic-prefix"[^>]*disabled/);
  assert.match(visible(on), new RegExp(ORIGINAL_NAME_NO_OWNER_STATEMENT.slice(0, 40)));
  assert.match(visible(on), /state that no declarative owner manages these names/);
  setOriginalName(state, true, true);
  const prepared = await preparePlanOrProblem(state);
  const review = visible(renderPlanStep(prepared, state));
  // KILLS: a review that shows the ordinary subject for this plan.
  assert.match(review, /approval subject\s*originalName -- a restore under the ORIGINAL topic names/);
  assert.equal(approvalSubjectText(wizardState()), "ordinary");
});

test("prod151_a_draft_and_an_edit_bring_the_choice_back", () => {
  const state = wizardState();
  setOriginalName(state, true, true);
  const kept = wizardDraftValues(state);
  assert.equal(kept.originalName, true);
  assert.equal(kept.originalNameNoOwner, true);
  const fresh = wizardState();
  assert.equal(applyWizardDraft(fresh, kept), true);
  assert.equal(originalNameChosen(fresh), true);
  assert.equal(fresh.fields.target.topicPrefix, "");
  // An edit of an original-name Restore prefills the choice and asks for the
  // owner statement again; its scratch prefix is not emptied.
  const fields = draftFrom({ spec: { target: { mode: "newTopic",
    topicNaming: { prefix: "", originalName: true } } } }, wizardState().fields);
  assert.equal(fields.target.originalName, true);
  assert.equal(fields.target.originalNameNoOwner, false);
  assert.ok(fields.target.topicMappingPrefix.length > 0);
});

// ------------------------------------------- complete coverage is required

test("choosing_the_original_names_selects_complete_coverage_and_says_why", async () => {
  // An original-name restore REQUIRES complete verification: a sampled check
  // can pass a record another producer wrote into the restored name.
  // KILLS: a choice that leaves the plan sampled (the runner refuses it after
  // the approver signed); a box the operator can untick; a locked box with no
  // reason beside it; the rule applied to an ordinary restore.
  const state = wizardState();
  assert.equal(state.fields.sample.coverage, undefined, "sampled by default");
  const before = renderCoverageChoice(state);
  assert.doesNotMatch(before, /id="coverage-complete" name="coverage"[^>]*checked/);
  assert.doesNotMatch(before, /id="coverage-complete"[^>]*disabled/);
  assert.doesNotMatch(before, /coverage-required/);

  setOriginalName(state, true, true);
  assert.equal(state.fields.sample.coverage, "complete",
    "NEGATIVE CONTROL: a choice that does not select complete coverage fails this");
  const html = renderCoverageChoice(state);
  assert.match(html, /<details class="advanced" id="coverage-advanced" open>/);
  assert.match(html, /id="coverage-complete" name="coverage" checked disabled>/,
    "the box is ticked and locked");
  const why = /<p class="caveat" id="coverage-required">([^<]*)<\/p>/.exec(html);
  assert.ok(why !== null, "NEGATIVE CONTROL: a locked box with no reason fails this");
  assert.equal(visible(why[1]), ORIGINAL_NAME_COVERAGE_SENTENCE);
  assert.match(ORIGINAL_NAME_COVERAGE_SENTENCE, /required for a restore under the original topic names/);
  assert.match(ORIGINAL_NAME_COVERAGE_SENTENCE, /selected for you/);
  assert.match(ORIGINAL_NAME_COVERAGE_SENTENCE, /OriginalNameNeedsCompleteCoverage/);
  // The bound stays the operator's to set.
  assert.doesNotMatch(html, /id="coverage-bound"[^>]*disabled/);

  // It cannot be cleared: not by the control's writer, not by a stale form.
  setCoverage(state, false, "");
  assert.equal(state.fields.sample.coverage, "complete");
  setCoverage(state, true, "5000");
  assert.equal(state.fields.sample.completeMaxRecords, 5000);
  setCoverage(state, false, "5000");
  assert.equal(state.fields.sample.coverage, "complete");

  // The plan says so, the request declares it, and the review says why.
  setCoverage(state, true, "");
  const prepared = await preparePlanOrProblem(state);
  assert.equal(typeof prepared.problem, "undefined", String(prepared.problem));
  assert.match(prepared.bytes, /\n {2}coverage: "complete"\n/);
  assert.equal(restoreBody(state, prepared).spec.coverage, "complete");
  assert.match(visible(renderPlanStep(prepared, state)),
    /coverage\s*complete \(sample\.coverage: complete, no record bound\): every record of every restored partition, compared with the archive -- required for a restore under the original topic names/);

  // Unticking the original-name choice unlocks the box; the operator may
  // then go back to a sampled check.
  setOriginalName(state, false, false);
  const unlocked = renderCoverageChoice(state);
  assert.doesNotMatch(unlocked, /id="coverage-complete"[^>]*disabled/);
  assert.doesNotMatch(unlocked, /coverage-required/);
  setCoverage(state, false, "");
  assert.equal(state.fields.sample.coverage, undefined);

  // CONTROL: an ordinary restore's coverage is the operator's choice.
  const plain = wizardState();
  setCoverage(plain, false, "");
  assert.equal(plain.fields.sample.coverage, undefined);
  requireCompleteCoverage(plain);
  assert.equal(plain.fields.sample.coverage, undefined, "a no-op for every other plan");
});

test("a_sampled_plan_under_the_original_names_is_never_rendered_or_sent", async () => {
  // KILLS: a sampled original-name plan rendered (and so hashed, approved and
  // sent) by any path -- a hand-built state, an older draft, an edit.
  const fields = fixture("plan-original-name-fields.json");
  assert.equal(fields.sample.coverage, "complete");
  for (const coverage of [undefined, "", "sampled"]) {
    const sampled = JSON.parse(JSON.stringify(fields));
    if (coverage === undefined) {
      delete sampled.sample.coverage;
    } else {
      sampled.sample.coverage = coverage;
    }
    assert.throws(() => renderPlanBytes(sampled), /OriginalNameNeedsCompleteCoverage/,
      "NEGATIVE CONTROL: a renderer that emits a sampled original-name plan fails this");
  }
  // A state built any other way is refused by name before anything is sent.
  const state = wizardState();
  setOriginalName(state, true, true);
  delete state.fields.sample.coverage;
  assert.match(mappingProblems(state).originalName, /OriginalNameNeedsCompleteCoverage/);
  assert.ok("originalName" in validateRestore(state));
  const prepared = await preparePlanOrProblem(state);
  assert.equal(typeof prepared.bytes, "undefined", "no plan bytes for a sampled plan");

  // A draft kept before the rule (the choice, no coverage) comes back complete.
  const older = wizardState();
  setOriginalName(older, true, true);
  const kept = Object.assign({}, wizardDraftValues(older), { coverage: "" });
  const fresh = wizardState();
  assert.equal(applyWizardDraft(fresh, kept), true);
  assert.equal(originalNameChosen(fresh), true);
  assert.equal(fresh.fields.sample.coverage, "complete");
  // An edit of an original-name Restore prefills complete coverage.
  const edited = draftFrom({ spec: { target: { mode: "newTopic",
    topicNaming: { prefix: "", originalName: true } } } }, wizardState().fields);
  assert.equal(edited.sample.coverage, "complete");
  // CONTROL: an edit of an ordinary Restore leaves the sample block alone.
  const ordinary = draftFrom({ spec: { target: { mode: "newTopic",
    topicNaming: { prefix: "restore-" } } } }, wizardState().fields);
  assert.equal(ordinary.sample.coverage, undefined);
});

// ------------------------------------------------------------ approvals

test("prod151_an_approval_shows_the_subject_its_signed_bytes_carry", () => {
  // KILLS: an unreadable document shown as ordinary; a subject read from
  // anything but the signed bytes.
  const cr = (bytes) => ({ metadata: { name: "a1" }, spec: { subjectRef: { kind: "Restore",
    name: "r" }, planHash: "sha256:x", approvalBytes: bytes }, status: {} });
  assert.equal(approvalSubjectOf(cr(JSON.stringify({ approval_subject: "originalName" }))),
    "originalName");
  assert.equal(approvalSubjectOf(cr(JSON.stringify({ approvalSubject: "originalName" }))),
    "originalName");
  assert.equal(approvalSubjectOf(cr(JSON.stringify({ plan_hash: "x" }))), "ordinary");
  assert.equal(approvalSubjectOf(cr("approver: ops\n")), "unknown");
  assert.equal(approvalSubjectOf(cr(JSON.stringify({ approval_subject: "everything" }))),
    "unknown");
  assert.equal(approvalSubjectOf({ approvalSubject: "originalName" }), "originalName");
  const shown = visible(renderApprovalStatus(cr(JSON.stringify({
    approval_subject: "originalName" }))));
  assert.match(shown, /approval subject\s*originalName -- a restore under the ORIGINAL topic names/);
  assert.equal(restoreApprovalSubject({ approvalSubject: "originalName" }), "originalName");
  assert.equal(restoreApprovalSubject({ spec: { target: { topicNaming: { prefix: "",
    originalName: true } } } }), "originalName");
  assert.equal(restoreApprovalSubject({ spec: { target: { topicNaming: { prefix: "x-" } } } }),
    "ordinary");
});

test("prod151_the_console_decodes_the_new_fields", () => {
  // The fixtures are instances of the published schema (contract.spec.js);
  // this reads the two new fields through the real decoder.
  const decoded = decodeConsoleItem("restores", fixture("console/restore.json"));
  const item = decoded.value.item || decoded.value;
  assert.equal(item.approvalSubject, "ordinary");
  assert.equal(item.target.originalName, false);
});

// ------------------------------------------------------------ OD-10, typed

const ORDINARY = Object.freeze({
  name: "team-ordinary", mode: "ordinary", legacy: false,
  ordinaryConfirmationAvailable: true, ticketRequired: false,
});
const GOVERNED = Object.freeze({
  name: "prod-governed", mode: "governed", legacy: false,
  ordinaryConfirmationAvailable: false, ticketRequired: true,
});

/** An API double: records every create; every read is a 404. */
function recorder() {
  const creates = [];
  return {
    creates: creates,
    async create(ns, plural, body) {
      creates.push({ ns: ns, plural: plural, body: body });
      return Object.assign({}, body, { metadata: Object.assign({ uid: "u-1" }, body.metadata) });
    },
    async get() {
      const error = new Error("not found");
      error.status = 404;
      throw error;
    },
    async list() {
      return { items: [] };
    },
  };
}

function confirmState(policy) {
  const state = wizardState();
  setOriginalName(state, true, true);
  state.approvalPolicy = policy;
  return state;
}

test("od10_a_one_person_confirmation_asks_for_the_original_names_and_sends_them_typed", async () => {
  // KILLS: the field not offered on a one-person confirmation; a request
  // sent without the typed names; the names sent case-folded.
  const state = confirmState(ORDINARY);
  const topics = selectedTopics(state);
  assert.ok(topics.length > 0);
  assert.equal(typedConfirmationRequired(state), true);
  assert.match(renderTypedConfirmation(state), /id="original-name-typed"/);
  assert.match(visible(renderTypedConfirmation(state)), /RE-TYPING every original topic name/);

  // Nothing typed: refused before anything is sent.
  const nothing = recorder();
  await assert.rejects(() => submitRestore(state, nothing), (error) => {
    assert.match(JSON.stringify(error) + String(error.message), /re-type every original topic name/);
    return true;
  });
  assert.equal(nothing.creates.length, 0);

  // A mistyped name: refused by name.
  state.originalNameTyped = topics.slice(1).join("\n") + "\n" + topics[0].toUpperCase();
  assert.match(String(typedTopicsProblem(state)), /not typed: /);
  const mistyped = recorder();
  await assert.rejects(() => submitRestore(state, mistyped));
  assert.equal(mistyped.creates.length, 0);

  // Exactly the names, in any order, one per line: sent beside the body.
  state.originalNameTyped = topics.slice().reverse().join("\n") + "\n";
  assert.equal(typedTopicsProblem(state), null);
  const sent = recorder();
  await submitRestore(state, sent);
  assert.equal(sent.creates.length, 1);
  assert.deepEqual(sent.creates[0].body.originalNameConfirmation,
    { typedTopics: topics.slice().reverse() });
  assert.equal(sent.creates[0].body.spec.originalNameConfirmation, undefined,
    "never inside Restore.spec");
});

test("od10_a_namespace_with_a_second_person_asks_for_no_typed_names", async () => {
  // KILLS: typed names offered (or sent) where a second person approves, or
  // for an ordinary restore.
  const strict = confirmState(GOVERNED);
  assert.equal(typedConfirmationRequired(strict), false);
  assert.equal(renderTypedConfirmation(strict), "");
  const prepared = await preparePlanOrProblem(strict);
  assert.equal(restoreBody(strict, prepared).originalNameConfirmation, undefined);

  const plain = wizardState();
  plain.approvalPolicy = ORDINARY;
  assert.equal(typedConfirmationRequired(plain), false);
  assert.deepEqual(parseTypedTopics(" orders ,payments\n\n"), ["orders", "payments"]);
});

test("od10_the_approvals_page_says_a_confirmation_was_made_with_the_names_typed", () => {
  // KILLS: a one-person confirmation shown like any other approval.
  const cr = (doc) => ({ metadata: { name: "a1" }, spec: { subjectRef: { kind: "Restore",
    name: "r" }, planHash: "sha256:x", approvalBytes: JSON.stringify(doc) }, status: {} });
  const typed = cr({ approvalSubject: "originalName",
    originalNameConfirmation: { typedTopics: ["orders", "payments"] } });
  assert.deepEqual(typedTopicsOf(typed), ["orders", "payments"]);
  assert.match(visible(renderApprovalStatus(typed)),
    /confirmation\s*confirmed by one person with every original topic name re-typed: orders, payments/);
  const plain = cr({ approvalSubject: "originalName" });
  assert.equal(typedTopicsOf(plain), null);
  assert.doesNotMatch(visible(renderApprovalStatus(plain)), /re-typed/);
});

// ------------------------------------------------ a stopped creation step

test("a_stopped_creation_step_names_every_topic_it_left_on_the_restore_detail", async () => {
  // Nothing is ever deleted under an original name, so the page must say what
  // is on the cluster. KILLS: a detail that never shows the left topics; one
  // that shows them without the instruction; one that claims a deletion.
  const { creationStoppedWarning, renderRestoreDetail } = await import("../pages/history.js");
  const { LEFT_TOPIC_SENTENCE } = await import("../render.js");
  const restore = (targetTopicsAppeared) => ({
    metadata: { name: "r", namespace: "team-a" },
    spec: { planBytes: text("plan-original-name.golden.yaml"),
      pointInTime: "2026-09-07T14:05:00Z", backupSetRef: "b",
      target: { mode: "newTopic", topicNaming: { prefix: "", originalName: true } } },
    // `newTopics` is the PLAN's mapped names, as the controller derives it on
    // every terminal Restore -- the name someone else created included.
    status: Object.assign({ phase: "Failed", exitCode: 1, exitReason: "TargetTopicAppeared",
      newTopics: ["orders", "payments", "audit"] },
      targetTopicsAppeared === undefined ? {} : { targetTopicsAppeared }),
  });
  const html = renderRestoreDetail(restore({ appeared: ["payments"], left: ["orders", "audit"] }));
  const block = /<div class="caveat" id="restore-creation-stopped">[\s\S]*?<\/div>/.exec(html);
  assert.ok(block !== null, "NEGATIVE CONTROL: a detail without the block fails this");
  const words = visible(block[0]);
  assert.ok(words.includes("payments: created by someone else after this restore was admitted. " +
    "The restore wrote nothing into it."), words);
  for (const name of ["orders", "audit"]) {
    assert.ok(words.includes(name + ": created by this restore and left empty; remove it " +
      "yourself once you have checked nothing writes to it."),
    "NEGATIVE CONTROL: a left topic without its instruction fails this:\n" + words);
  }
  assert.ok(words.includes("Logweir never deletes a topic under a name it may not own"));
  assert.doesNotMatch(words, /removed|deleted it|cleaned/i);
  assert.equal(LEFT_TOPIC_SENTENCE,
    "created by this restore and left empty; remove it yourself once you have checked nothing " +
      "writes to it");
  // The block comes before every other fact of the run.
  assert.ok(html.indexOf("restore-creation-stopped") < html.indexOf("last phase completed"));
  // The "new topics" row lists what THIS restore created (what it left), never
  // the name someone else created. KILLS: the plan's mapped names shown as
  // this restore's topics after a lost race.
  const facts = visible(html.slice(html.indexOf("<h3>Topics</h3>")));
  assert.match(facts, /new topics\s*orders, audit/, facts.slice(0, 200));
  assert.doesNotMatch(facts.slice(0, facts.indexOf("old topics")), /payments/);
  // CONTROL: without a stopped creation step the row is the status's list.
  const passed = visible(renderRestoreDetail(restore(undefined)));
  assert.match(passed, /new topics\s*orders, payments, audit/);

  // A race lost before anything was created says so.
  const none = visible(creationStoppedWarning({ appeared: ["payments"], left: [] }));
  assert.ok(none.includes("This restore created no topic."), none);
  // A stop for another reason names what was left and no race.
  const other = visible(creationStoppedWarning({ appeared: [], left: ["orders"] }));
  assert.ok(other.includes("orders: created by this restore and left empty"));
  assert.ok(!other.includes("someone else"));
  // CONTROL: no block on the status, nothing rendered.
  assert.equal(creationStoppedWarning(undefined), "");
  assert.ok(!renderRestoreDetail(restore(undefined)).includes("restore-creation-stopped"));
  // A hostile name is escaped, never markup -- in EVERY list (review 2, L4:
  // the control fed only `left`, and an unescaped `appeared` sentence
  // survived). KILLS: a list rendered without `esc` (mutant R2-09).
  const hostile = "<img src=x onerror=alert(1)>";
  for (const key of ["appeared", "left", "unconfirmed"]) {
    const lists = { appeared: [], left: [], unconfirmed: [], unconfirmedSeen: true };
    lists[key] = [hostile];
    const rendered = creationStoppedWarning(lists);
    assert.ok(!rendered.includes("<img"), "NEGATIVE CONTROL: an unescaped " + key +
      " name fails this:\n" + rendered);
    assert.ok(rendered.includes("&lt;img src=x onerror=alert(1)&gt;"), key + ": the name is " +
      "shown, as text:\n" + rendered);
  }
});

test("a_stopped_creation_step_shows_what_it_cannot_account_for_and_how_many_more", async () => {
  // Review 2, M2. KILLS: an unconfirmed topic shown with the "created by this
  // restore" sentence, or not shown; "exists now" when the runner could not
  // look; a list the 100-name bound cut that says nothing; the "new topics"
  // row listing a name the restore cannot account for.
  const { creationStoppedWarning, creationStopMore, renderRestoreDetail } =
    await import("../pages/history.js");
  const { LEFT_TOPIC_SENTENCE, UNCONFIRMED_TOPIC_SENTENCE, UNCONFIRMED_UNLISTED_TOPIC_SENTENCE } =
    await import("../render.js");
  const seen = visible(creationStoppedWarning({
    appeared: [], left: ["orders"], unconfirmed: ["payments"],
    appearedCount: 0, leftCount: 1, unconfirmedCount: 1, unconfirmedSeen: true,
  }));
  assert.ok(seen.includes("orders: " + LEFT_TOPIC_SENTENCE + "."), seen);
  assert.ok(seen.includes("payments: " + UNCONFIRMED_TOPIC_SENTENCE + "."),
    "NEGATIVE CONTROL: a detail without the unconfirmed topic fails this:\n" + seen);
  assert.ok(!seen.includes("payments: created by this restore"), seen);
  assert.ok(seen.includes("Logweir never deletes a topic under a name it may not own"));
  assert.doesNotMatch(seen, /and \d+ more|first 100 names/);
  assert.equal(UNCONFIRMED_TOPIC_SENTENCE,
    "exists now; this restore asked the cluster to create it and got no definite answer, so " +
      "it may be this restore's or someone else's: check what it holds and who writes to it " +
      "before you remove it");

  // The runner could not list the cluster: "may exist", for `false` and for
  // an absent flag alike; never "exists now".
  for (const flag of [false, undefined]) {
    const lists = { appeared: [], left: [], unconfirmed: ["orders", "payments"],
      appearedCount: 0, leftCount: 0, unconfirmedCount: 2 };
    if (flag !== undefined) lists.unconfirmedSeen = flag;
    const words = visible(creationStoppedWarning(lists));
    assert.ok(words.includes("orders: " + UNCONFIRMED_UNLISTED_TOPIC_SENTENCE + "."), words);
    assert.ok(!words.includes("exists now"), words);
    assert.ok(words.includes("No CreateTopics answer says this restore created a topic."), words);
    assert.ok(!words.includes("This restore created no topic."), words);
  }

  // The bound: 100 names shown of 150, in each list.
  const names = (prefix) => Array.from({ length: 100 }, (_, i) => prefix + String(i).padStart(3, "0"));
  const cut = { appeared: ["a"], left: names("t"), unconfirmed: names("u"),
    appearedCount: 3, leftCount: 150, unconfirmedCount: 120, unconfirmedSeen: true };
  assert.equal(creationStopMore(cut, "left"), 50);
  assert.equal(creationStopMore(cut, "unconfirmed"), 20);
  assert.equal(creationStopMore(cut, "appeared"), 2);
  const words = visible(creationStoppedWarning(cut));
  assert.match(words, /t099: created by this restore[^]*?and 50 more/);
  assert.match(words, /u099: exists now[^]*?and 20 more/);
  assert.ok(words.includes("a and 2 more: created by someone else after this restore was " +
    "admitted. The restore wrote nothing into them."), words);
  assert.ok(words.includes("A list shows its first 100 names. Each name is one of this " +
    "restore's mapped target topics, and the runner's log names every one."),
  "NEGATIVE CONTROL: a cut list that says nothing fails this:\n" + words);
  // A count that is absent, smaller than the list or not a whole number cuts
  // nothing.
  for (const leftCount of [undefined, 1, -3, 2.5, "150", null]) {
    assert.equal(creationStopMore({ left: ["x", "y"], leftCount }, "left"), 0);
  }

  // The "new topics" row is what the restore LEFT, with its count, and never
  // an unconfirmed name.
  const restore = (targetTopicsAppeared) => ({
    metadata: { name: "r", namespace: "team-a" },
    spec: { planBytes: text("plan-original-name.golden.yaml"),
      pointInTime: "2026-09-07T14:05:00Z", backupSetRef: "b",
      target: { mode: "newTopic", topicNaming: { prefix: "", originalName: true } } },
    status: { phase: "Failed", exitCode: 1, exitReason: "CreatedTopicsLeft",
      newTopics: targetTopicsAppeared.left, targetTopicsAppeared },
  });
  const html = renderRestoreDetail(restore({ appeared: [], left: ["orders"],
    unconfirmed: ["payments"], appearedCount: 0, leftCount: 1, unconfirmedCount: 1,
    unconfirmedSeen: true }));
  const facts = visible(html.slice(html.indexOf("<h3>Topics</h3>")));
  assert.match(facts, /new topics\s*orders/, facts.slice(0, 200));
  assert.doesNotMatch(facts.slice(0, facts.indexOf("old topics")), /payments/);
  const cutFacts = visible(renderRestoreDetail(restore(cut)));
  assert.match(cutFacts.slice(cutFacts.indexOf("new topics")), /t099 and 50 more/);
});

test("the_console_projection_carries_what_a_stopped_creation_step_cannot_account_for", async () => {
  // BOTH SIDES READ ONE FIXTURE: `console/restore-creation-unconfirmed.json` is
  // the product API's projection of a status carrying the THIRD list
  // (`crates/logweir-api/tests/original_name.rs`). KILLS: a decoder that
  // refuses, or a projection that drops, `unconfirmed`, a count or the flag;
  // a console sentence that is not the API's.
  const { apiClient, resetMode, selectMode } = await import("../client.js");
  const { UNCONFIRMED_TOPIC_SENTENCE } = await import("../render.js");
  const answer = fixture("console/restore-creation-unconfirmed.json");
  assert.equal(answer.item.targetTopicsAppeared.unconfirmedInstruction,
    UNCONFIRMED_TOPIC_SENTENCE,
    "the console says the API's (and so the runner's) sentence, word for word");
  resetMode();
  await selectMode({
    probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }),
  });
  const operation = fixture("console/operation-restore-completed.json");
  operation.item.state = "failed";
  operation.item.result.exitCode = 1;
  operation.item.result.exitReason = "CreatedTopicsLeft";
  const original = globalThis.fetch;
  globalThis.fetch = (url) => Promise.resolve({
    ok: true,
    status: 200,
    headers: { get: () => null },
    text: () => Promise.resolve(JSON.stringify(
      String(url).includes("/operations") ? operation : answer)),
  });
  try {
    const object = await apiClient().get("team-a", "restores", answer.item.name);
    assert.deepEqual(object.status.targetTopicsAppeared, {
      appeared: [], left: ["orders"], unconfirmed: ["payments"],
      appearedCount: 0, leftCount: 1, unconfirmedCount: 1, unconfirmedSeen: true,
    }, "NEGATIVE CONTROL: a projection that drops the third list fails this");
    const { renderRestoreDetail } = await import("../pages/history.js");
    const words = visible(renderRestoreDetail(object));
    assert.ok(words.includes("payments: " + UNCONFIRMED_TOPIC_SENTENCE + "."), words);
    assert.ok(!words.includes("payments: created by this restore"), words);
  } finally {
    globalThis.fetch = original;
    resetMode();
  }
});

test("the_console_projection_carries_the_topics_a_stopped_creation_step_left", async () => {
  // BOTH SIDES READ ONE FIXTURE: `console/restore-creation-stopped.json` is the
  // product API's projection of a Restore whose status carries
  // `targetTopicsAppeared` (`crates/logweir-api/tests/original_name.rs`). Read
  // here through the real console client, decoder and projection. KILLS: a
  // decoder that refuses, or a projection that drops, the block.
  const { apiClient, resetMode, selectMode } = await import("../client.js");
  const { LEFT_TOPIC_SENTENCE } = await import("../render.js");
  const answer = fixture("console/restore-creation-stopped.json");
  assert.equal(answer.item.targetTopicsAppeared.leftInstruction, LEFT_TOPIC_SENTENCE,
    "the console says the API's (and so the runner's) sentence, word for word");
  resetMode();
  await selectMode({
    probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }),
  });
  // The detail also reads the run's operation, on its own route: a failed
  // run whose result names the closed state.
  const operation = fixture("console/operation-restore-completed.json");
  operation.item.state = "failed";
  operation.item.result.exitCode = 1;
  operation.item.result.exitReason = "TargetTopicAppeared";
  const original = globalThis.fetch;
  globalThis.fetch = (url) => Promise.resolve({
    ok: true,
    status: 200,
    headers: { get: () => null },
    text: () => Promise.resolve(JSON.stringify(
      String(url).includes("/operations") ? operation : answer)),
  });
  try {
    const object = await apiClient().get("team-a", "restores", answer.item.name);
    assert.deepEqual(object.status.targetTopicsAppeared,
      { appeared: ["payments"], left: ["orders"], unconfirmed: [],
        appearedCount: 1, leftCount: 1, unconfirmedCount: 0 },
      "NEGATIVE CONTROL: a decoder that refuses, or a projection that drops, the block " +
        "fails this");
    assert.equal(object.status.exitReason, "TargetTopicAppeared");
    const { renderRestoreDetail } = await import("../pages/history.js");
    const words = visible(renderRestoreDetail(object));
    assert.ok(words.includes("orders: " + LEFT_TOPIC_SENTENCE + "."), words);
    // CONTROL: a Restore without the block carries none.
    const plain = fixture("console/restore.json");
    assert.equal(plain.item.targetTopicsAppeared, undefined);
  } finally {
    globalThis.fetch = original;
    resetMode();
  }
});

// ===========================================================================
// FX-48: the choice reaches the product API, and the API's answer reaches the
// page -- through the REAL client
// ===========================================================================
//
// EVERY ROW ABOVE THAT SUBMITS USES AN API DOUBLE, and every row that reads
// the approval subject hands a page function an object written in the test.
// So nothing above ever ran the three places in `ui/client.js` and
// `ui/validate.js` that stand between this wizard and the product API -- and
// all three dropped the choice (found by FX-48's sweep, the same class as the
// schema dependency):
//
//   * the page's own pre-send check required a non-empty prefix, so an
//     original-name Restore was refused before the network, in both modes;
//   * `requestBody` copied `topicNaming.prefix` and nothing else, and never
//     copied `originalNameConfirmation`, so the request the API requires for
//     such a restore could not have been built;
//   * the projection copied neither `approvalSubject` nor
//     `target.originalName`, so the approval page said an original-name
//     Restore needed an `ordinary` approval.
//
// These rows go through `apiClient()` over the transport seam.

/** The shared console, signed in to the wizard state's namespace with every
 *  capability, and a transport that answers `answer(url, init)`. */
async function sharedConsole(ns, answer) {
  const { apiClient, resetMode, selectMode } = await import("../client.js");
  const session = wire("session.json");
  for (const flag of Object.keys(session.capabilities)) {
    session.capabilities[flag] = true;
  }
  for (const grant of session.namespaces) {
    grant.name = ns;
    for (const flag of Object.keys(grant.capabilities)) {
      grant.capabilities[flag] = true;
    }
  }
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: session }) });
  const seen = [];
  const original = globalThis.fetch;
  globalThis.fetch = (url, init) => {
    const request = { url: String(url), method: (init || {}).method || "GET",
      body: (init || {}).body === undefined ? null : JSON.parse(init.body) };
    seen.push(request);
    const body = answer(request);
    return Promise.resolve({
      ok: body !== null, status: body === null ? 404 : 200,
      text: () => Promise.resolve(JSON.stringify(body === null
        ? { type: "about:blank", title: "Not found", status: 404, code: "not_found",
          detail: "no such object", requestId: "r", retryable: false }
        : body)),
    });
  };
  return {
    api: apiClient(), seen: seen,
    restore: () => { globalThis.fetch = original; resetMode(); },
  };
}

/** `restore.json` as the product API answers for a Restore under the original
 *  topic names: the three members that say so, changed on the wire. */
function originalNameAnswer() {
  return wire("restore.json", (body) => {
    body.item.approvalSubject = "originalName";
    body.item.target.originalName = true;
    body.item.target.topicPrefix = "";
    body.item.target.mode = "newTopic";
  });
}

test("fx48_an_original_name_restore_leaves_the_console_as_the_route_requires", async () => {
  // THE WIZARD'S OWN BODY, through the real client: the page's check, the
  // request builder and the transport.
  const state = confirmState(ORDINARY);
  const topics = selectedTopics(state);
  state.originalNameTyped = topics.join("\n");
  const prepared = await preparePlan(state);
  const body = restoreBody(state, prepared);
  assert.deepEqual(body.spec.target.topicNaming, { prefix: "", originalName: true });

  const shared = await sharedConsole(state.ns, () => originalNameAnswer());
  try {
    await shared.api.create(state.ns, "restores", body);
    const posts = shared.seen.filter((r) => r.method === "POST");
    assert.equal(posts.length, 1, "THE DEFECT: the page refused its own body and sent nothing");
    const sent = posts[0].body;
    assert.deepEqual(schemaFindings("CreateRestoreRequest", sent), [],
      "what left the console is a request the published schema accepts");
    assert.deepEqual(sent.target.topicNaming, { prefix: "", originalName: true },
      "THE DEFECT: the declaration was dropped and only the empty prefix would have gone");
    assert.equal(sent.target.mode, "newTopic");
    assert.equal(sent.coverage, "complete", "the coverage the choice requires travels with it");
    assert.deepEqual(sent.originalNameConfirmation, { typedTopics: topics },
      "THE DEFECT: the typed names never left the page");
    assert.equal(sent.spec, undefined, "a request, not a custom resource");
  } finally {
    shared.restore();
  }
});

test("fx48_an_ordinary_restore_sends_what_it_did_and_an_empty_prefix_is_refused", async () => {
  // NEGATIVE CONTROL of the row above: no declaration, no typed names, a
  // non-empty prefix -- and the page's own check still refuses an empty one.
  const state = wizardState();
  const prepared = await preparePlan(state);
  const body = restoreBody(state, prepared);
  const shared = await sharedConsole(state.ns, () => wire("restore.json"));
  try {
    await shared.api.create(state.ns, "restores", body);
    const sent = shared.seen.filter((r) => r.method === "POST")[0].body;
    assert.deepEqual(schemaFindings("CreateRestoreRequest", sent), []);
    assert.deepEqual(Object.keys(sent.target.topicNaming), ["prefix"]);
    assert.ok(sent.target.topicNaming.prefix.length > 0);
    assert.equal(sent.originalNameConfirmation, undefined);

    // An empty prefix WITHOUT the declaration: refused here, by field.
    const blank = JSON.parse(JSON.stringify(body));
    blank.spec.planBytes = body.spec.planBytes;
    blank.spec.target.topicNaming = { prefix: "" };
    const before = shared.seen.length;
    await assert.rejects(() => shared.api.create(state.ns, "restores", blank), (error) => {
      assert.equal(error.reason, "ClientValidation");
      assert.deepEqual(error.details.causes.map((c) => c.field),
        ["spec.target.topicNaming.prefix"]);
      return true;
    });
    // And a prefix BESIDE the declaration, which the API refuses too
    // (`prefix_with_original_name`): one name for a topic, not two.
    const both = JSON.parse(JSON.stringify(body));
    both.spec.target.topicNaming = { prefix: "restore-", originalName: true };
    await assert.rejects(() => shared.api.create(state.ns, "restores", both), (error) => {
      assert.equal(error.reason, "ClientValidation");
      assert.deepEqual(error.details.causes.map((c) => [c.field, c.reason]),
        [["spec.target.topicNaming.prefix", "prefix_with_original_name"]]);
      return true;
    });
    assert.equal(shared.seen.length, before, "neither was sent");
  } finally {
    shared.restore();
  }
});

test("fx48_the_console_shows_the_approval_subject_the_api_publishes", async () => {
  const shared = await sharedConsole("team-on", (request) => {
    const path = request.url.split("?")[0];
    if (path.indexOf("/operations/") !== -1) {
      return wire("operation-restore-completed.json");
    }
    if (path.endsWith("/approvals")) {
      return { requestId: "r", page: { limit: 200 },
        items: [wire("approval.json", (body) => {
          body.item.approvalSubject = "originalName";
        }).item] };
    }
    return path.indexOf("/restores/") !== -1 ? originalNameAnswer() : null;
  });
  try {
    // THE RESTORE, as the approval page reads it.
    const restore = await shared.api.get("team-on", "restores", "orders-drill-20260911");
    assert.equal(restoreApprovalSubject(restore), "originalName",
      "THE DEFECT: the page said `ordinary` for a Restore the API called originalName");
    assert.equal(restore.approvalSubject, "originalName", "the API's own word rides with it");
    assert.equal(restore.spec.target.topicNaming.originalName, true,
      "and the declaration is where the custom resource keeps it, for both modes' readers");
    // ... so an edit of that Restore starts from the choice, not from a prefix.
    const draft = draftFrom(restore, wizardState().fields);
    assert.equal(draft.target.originalName, true);

    // AN APPROVAL'S LIST ROW carries the signed subject the API read; the
    // list has no bytes to read it from.
    const approvals = await shared.api.list("team-on", "approvals");
    assert.equal(approvals.items[0].spec.approvalBytes, undefined, "a list row has no bytes");
    assert.equal(approvalSubjectOf(approvals.items[0]), "originalName",
      "THE DEFECT: a console list row said `unknown`");
  } finally {
    shared.restore();
  }

  // NEGATIVE CONTROL: the unchanged answers are ordinary, and the projection
  // invents no declaration.
  const plain = await sharedConsole("team-on", (request) => {
    const path = request.url.split("?")[0];
    if (path.indexOf("/operations/") !== -1) {
      return wire("operation-restore-completed.json");
    }
    return path.indexOf("/restores/") !== -1 ? wire("restore.json") : null;
  });
  try {
    const restore = await plain.api.get("team-on", "restores", "orders-drill-20260911");
    assert.equal(restoreApprovalSubject(restore), "ordinary");
    assert.deepEqual(Object.keys(restore.spec.target.topicNaming), ["prefix"]);
  } finally {
    plain.restore();
  }
});
