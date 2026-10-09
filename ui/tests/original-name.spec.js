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
  ORIGINAL_NAME_NO_OWNER_STATEMENT,
  applyWizardDraft,
  approvalSubjectText,
  draftFrom,
  initialState,
  mappingProblems,
  originalNameChosen,
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
} from "../pages/approvals.js";
import { renderPlanBytes } from "../plan.js";
import { decodeConsoleItem } from "../contract.js";

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
