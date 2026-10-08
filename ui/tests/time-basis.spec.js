// time-basis.spec.js -- FX-8: the restore wizard's time-basis opt-in.
//
// WHY THE PAGE HAS IT. The archive holds each record's PRODUCER timestamp, so a
// point in time over a `LogAppendTime` topic selects by the producers' clocks.
// The runner refuses that (`PointInTimeByProducerTime`, exit 3, before any
// target topic exists) unless the approved plan states
// `restore.time_basis: producerTime`; with it, the restore runs and the signed
// scorecard lists the topic under `source.time_basis.producer_time`. Every
// console restore states a point in time, so without this box a console
// restore of a `LogAppendTime` topic could never run at all.
//
// THE ROWS. The plan carries the line exactly when the box is ticked, and is
// byte-identical to before FX-8 when it is not (the golden pair below is read by
// `crates/logweir/tests/ui_lint.rs` too, which deserialises it into the
// runner's `RestoreSpec`); the box is unticked by default and never ticked by
// the page; step 3 says what the box means and what the page cannot see; the
// review step names the time basis; the draft keeps the choice as text; the
// mounted wizard writes the box into the plan bytes and the review row.
//
// NEGATIVE CONTROL: each row names the mutant it kills. The FX-8 report records
// the mutants and the command that ran them.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  TIME_BASIS_NOTE,
  WIZARD_DRAFT_FIELDS,
  applyWizardDraft,
  initialState,
  mountRestoreWizard,
  preparePlan,
  recoveryPoints,
  renderPlanStep,
  renderPointInTimeStep,
  timeBasisText,
  wizardDraftValues,
} from "../pages/restore-wizard.js";
import { TIME_BASIS_PRODUCER_TIME, renderPlanBytes } from "../plan.js";
import { keepDraft, readDraft } from "../lifecycle.js";
import { fakeView, parse as viewParse } from "./fake-view.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const text = (name) => readFileSync(FIXTURES + name, "utf8");

/** What an operator reads of a rendered fragment: no tags, entities decoded. */
const visible = (html) => html.replace(/<[^>]*>/g, "").replace(/&#39;/g, "'")
  .replace(/&quot;/g, "\"").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");

function wizardState(ns) {
  const backups = fixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  return initialState(ns || "team-fx8", fixture("wizard-clusters.json"), backups, {
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

// ------------------------------------------------------------- the emitter

test("fx8_the_plan_carries_the_opt_in_exactly_when_it_is_stated", () => {
  // ARM 2 of the golden pair (arm 1 is ui_lint.rs, which parses it into
  // `RestoreSpec` and asserts `restore.time_basis == ProducerTime`).
  const fields = fixture("plan-time-basis-fields.json");
  assert.equal(renderPlanBytes(fields), text("plan-time-basis.golden.yaml"),
    "NEGATIVE CONTROL: an emitter that drops the line, or spells it `timeBasis:`, fails this");
  // NOT STATED, the bytes are the pre-FX-8 golden's, byte for byte: every plan
  // the console wrote before keeps its hash.
  for (const absent of [undefined, null, ""]) {
    const without = Object.assign({}, fields, { timeBasis: absent });
    assert.equal(renderPlanBytes(without), text("plan.golden.yaml"),
      "NEGATIVE CONTROL: an emitter that always writes the line fails this: " + String(absent));
  }
  // ONE VALUE. A typo is never the opt-in: it throws, like every field this
  // emitter cannot render.
  for (const bad of ["ProducerTime", "producer_time", "appendTime", true]) {
    assert.throws(() => renderPlanBytes(Object.assign({}, fields, { timeBasis: bad })),
      /timeBasis must be "producerTime" or absent/,
      "NEGATIVE CONTROL: an emitter that writes any truthy value fails this: " + String(bad));
  }
  assert.equal(TIME_BASIS_PRODUCER_TIME, "producerTime");
});

// --------------------------------------------------------------- step 3

test("fx8_step_3_offers_the_box_unticked_and_says_what_it_cannot_see", () => {
  const state = wizardState();
  const html = renderPointInTimeStep(state);
  assert.match(html, /<input type="checkbox" id="time-basis" name="timeBasis">/,
    "NEGATIVE CONTROL: a box the page ticks by default fails this -- consent nobody gave");
  const note = visible(html);
  for (const said of [
    "The archive holds each record's producer timestamp, not the broker's append time.",
    "PointInTimeByProducerTime",
    "This page cannot see each topic's timestamp type",
  ]) {
    assert.ok(note.includes(said), "step 3 says: " + said + "\n" + note);
  }
  assert.ok(html.includes("<code>restore.time_basis: producerTime</code>"),
    "the field is named as code, never as a backtick");
  assert.ok(!visible(html).includes("`"), "no backtick reaches the screen");
  assert.equal(TIME_BASIS_NOTE.split("`LogAppendTime`").length, 2);
  state.fields.timeBasis = TIME_BASIS_PRODUCER_TIME;
  assert.match(renderPointInTimeStep(state),
    /<input type="checkbox" id="time-basis" name="timeBasis" checked>/,
    "NEGATIVE CONTROL: a box that does not show the plan's own value fails this");
});

// ------------------------------------------------------------ the review

test("fx8_the_review_step_names_the_time_basis_and_the_plan_carries_it", async () => {
  const state = wizardState();
  let prepared = await preparePlan(state);
  let html = renderPlanStep(prepared, state);
  assert.match(visible(html),
    /time basis\s*not stated: a LogAppendTime topic at this point is refused before anything is created \(PointInTimeByProducerTime\)/,
    "NEGATIVE CONTROL: a review step with no time-basis row fails this");
  assert.ok(!prepared.bytes.includes("time_basis"), "unticked, the plan states none");
  const before = prepared.hash;

  state.fields.timeBasis = TIME_BASIS_PRODUCER_TIME;
  prepared = await preparePlan(state);
  html = renderPlanStep(prepared, state);
  assert.ok(prepared.bytes.includes("\n  time_basis: \"producerTime\"\n"),
    "NEGATIVE CONTROL: a wizard that does not hand the box to the emitter fails this");
  assert.notEqual(prepared.hash, before,
    "the opt-in is INSIDE the plan hash: an approval of the plan without it does not cover it");
  assert.match(visible(html),
    /time basis\s*producer time \(restore\.time_basis: producerTime\): a LogAppendTime topic is selected by its producers' clocks, and the signed scorecard lists it/);
  assert.equal(timeBasisText(state), visible(/<span id="review-time-basis">([^<]*)<\/span>/
    .exec(html)[1]));
});

// --------------------------------------------------------------- the draft

test("fx8_the_draft_keeps_the_choice_as_text_and_nothing_else_as_the_opt_in", () => {
  assert.ok(WIZARD_DRAFT_FIELDS.includes("timeBasis"),
    "NEGATIVE CONTROL: a draft that drops the field brings the plan back without the opt-in");
  const state = wizardState();
  assert.equal(wizardDraftValues(state).timeBasis, "", "unticked is the empty string");
  state.fields.timeBasis = TIME_BASIS_PRODUCER_TIME;
  assert.equal(wizardDraftValues(state).timeBasis, "producerTime");
  // THROUGH THE REAL DRAFT STORE, which keeps strings and booleans only.
  keepDraft("fx8-draft-probe", wizardDraftValues(state), WIZARD_DRAFT_FIELDS);
  const kept = readDraft("fx8-draft-probe");
  const again = wizardState();
  assert.equal(applyWizardDraft(again, kept), true);
  assert.equal(again.fields.timeBasis, "producerTime");
  // Anything else -- an older draft with none, or a value the grammar does not
  // have -- comes back unticked.
  for (const other of [undefined, "", "ProducerTime", true]) {
    const older = wizardState();
    older.fields.timeBasis = TIME_BASIS_PRODUCER_TIME;
    applyWizardDraft(older, Object.assign({}, kept, { timeBasis: other }));
    assert.equal(older.fields.timeBasis, "",
      "NEGATIVE CONTROL: a draft that keeps an unparsed value as the opt-in fails this: " +
        String(other));
  }
});

// ----------------------------------------------------------- the mount half

test("fx8_the_mounted_wizard_writes_the_box_into_the_plan_and_the_review", async () => {
  const view = fakeView();
  await mountRestoreWizard(view.root, "team-fx8-mount", pointParams(), viewParse, mountApi());
  const box = view.find("#time-basis");
  assert.ok(box !== null, "step 3 carries the box");
  assert.equal(box.checked, false);
  // The plan line is QUOTED; step 3's label names the field unquoted.
  assert.ok(!visible(view.html()).includes("time_basis: \"producerTime\""),
    "unticked, the plan states none");
  box.checked = true;
  await box.dispatch("change");
  const html = visible(view.html());
  assert.ok(html.includes("time_basis: \"producerTime\""),
    "NEGATIVE CONTROL: a box `refresh` does not read fails this:\n" + html.slice(0, 400));
  assert.match(html, /time basis\s*producer time \(restore\.time_basis: producerTime\)/);
  assert.equal(view.find("#time-basis").checked, true, "the repaint keeps it ticked");
  const again = view.find("#time-basis");
  again.checked = false;
  await again.dispatch("change");
  assert.ok(!visible(view.html()).includes("time_basis: \"producerTime\""),
    "and unticking takes it out");
});

// ------------------------------------------------------- the restore detail

test("fx8_a_restores_detail_names_the_time_basis_its_approved_plan_states", async () => {
  const { planTimeBasis } = await import("../render.js");
  const { renderRestoreDetail } = await import("../pages/history.js");
  const opted = text("plan-time-basis.golden.yaml");
  const plain = text("plan.golden.yaml");
  assert.equal(planTimeBasis(opted), "producerTime");
  assert.equal(planTimeBasis(plain), "",
    "NEGATIVE CONTROL: a reader that answers `producerTime` for every plan fails this");
  // Only inside `restore:`, and only the one value the grammar has.
  assert.equal(planTimeBasis("sample:\n  time_basis: \"producerTime\"\n"), "");
  // REVIEW L-3: every shape the runner parses as the opt-in reads as it --
  // any indentation, a flow mapping, a trailing comment -- and a commented-out
  // or nested key does not.
  for (const shape of [
    "restore:\n    point_in_time: x\n    time_basis: producerTime\n",
    "restore: {point_in_time: \"2026-01-01T00:00:00Z\", time_basis: producerTime}\nsample:\n",
    "restore: {\n  point_in_time: x,\n  time_basis: \"producerTime\"\n}\n",
    "restore:\n  time_basis: producerTime # accepted by the approver\n",
  ]) {
    assert.equal(planTimeBasis(shape), "producerTime",
      "NEGATIVE CONTROL: the 2-space-only reader fails this: " + JSON.stringify(shape));
  }
  for (const shape of [
    "restore:\n  point_in_time: x\n  # time_basis: producerTime\n",
    "restore:\n  point_in_time: x\n  other:\n    time_basis: producerTime\n",
  ]) {
    assert.equal(planTimeBasis(shape), "", JSON.stringify(shape));
  }
  assert.equal(planTimeBasis("restore:\n  time_basis: \"ProducerTime\"\n"), "");
  assert.equal(planTimeBasis("restore:\n  point_in_time: \"x\"\n  time_basis: producerTime\n"),
    "producerTime");
  const restore = (planBytes) => ({
    metadata: { name: "r", namespace: "team-fx8" },
    spec: { planBytes, pointInTime: "2026-09-07T14:05:00Z", backupSetRef: "b" },
    status: { phase: "Failed", exitCode: 3, reason: "PointInTimeByProducerTime" },
  });
  const span = (html) => visible(/<span id="restore-time-basis">([^<]*)<\/span>/.exec(html)[1]);
  assert.equal(span(renderRestoreDetail(restore(opted))),
    "producer time (restore.time_basis: producerTime): a LogAppendTime topic at this point " +
      "is selected by its producers' clocks",
    "NEGATIVE CONTROL: a detail page with no time-basis row fails this");
  const refused = renderRestoreDetail(restore(plain));
  assert.equal(span(refused),
    "no restore.time_basis: producerTime found in the plan by this page; without it a " +
      "LogAppendTime topic at a point is refused, and the signed result below is authoritative",
    "NEGATIVE CONTROL (review L-3): a page that says the plan states none fails this");
  assert.ok(visible(refused).includes("PointInTimeByProducerTime"),
    "the refusal's own reason is printed verbatim beside it");
});

// --------------------------------------------- review L-3's class sweep

test("fx8_review_l3_the_evidence_bucket_reader_is_as_tolerant_as_the_time_basis_one", async () => {
  // `planEvidenceBucket` read only the console's two-space block too; a
  // CLI-written plan with four spaces, a flow mapping or a trailing comment
  // rendered the placeholder in the fetch command. One reader serves both.
  const { planEvidenceBucket, planBlockValue } = await import("../render.js");
  for (const shape of [
    "evidence:\n    backend: s3\n    bucket: logweir-evidence\n",
    "evidence: {backend: s3, bucket: \"logweir-evidence\"}\n",
    "evidence:\n  bucket: logweir-evidence # the auditors' bucket\n",
  ]) {
    assert.equal(planEvidenceBucket(shape), "logweir-evidence",
      "NEGATIVE CONTROL: the 2-space-only reader fails this: " + JSON.stringify(shape));
  }
  // The source block's nested bucket is never the evidence bucket.
  assert.equal(planEvidenceBucket("source:\n  storage:\n    bucket: aaa\n"), "");
  assert.equal(planBlockValue("restore:\n  point_in_time: x\n", "restore", "time_basis"), null);
  assert.equal(planBlockValue(undefined, "restore", "time_basis"), null);
});
