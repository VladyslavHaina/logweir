// restore-semantics.spec.js -- FX-6: the restore review step says what a
// restore copies, above Create.
//
// WHY THE PAGE HAS TO SAY IT. PROD-01.1 measured four ways a restored topic
// differs from its source on the pinned engine: aborted and open transactions
// and their commit and abort markers come back as ordinary records; a
// `LogAppendTime` topic comes back with the producers' `CreateTime`; a repeated
// header key keeps one copy; and with out-of-order timestamps a full or
// point-in-time restore can miss records. A passing drill rules none of them
// out. It compares the restored topic with the ARCHIVE, never with the source,
// so what is changed when the archive is written is on both sides and passes,
// and a record that point-in-time selection skips is outside both sides. Some
// do fail a drill -- a full restore that drops a below-floor record fails its
// count check, and so does a sampled record whose own `x-original-offset` the
// restore replaced -- but a pass is not evidence of their absence. The decision
// record (docs/to-do/decisions/PROD-01.1-record-semantics.md, section 8.3)
// wrote the one sentence the console shows where the plan is reviewed; sections
// 8.1 and 8.2 are the long form in docs/verify-a-scorecard.md and
// docs/stability.md.
//
// THE ROWS. The sentence is on step 6, once, as the record words it, with its
// two identifiers as <code> and no backtick on screen. It follows the plan and
// comes before the approval-policy block and Create, with each of the four
// approval-policy blocks step 6 can show. It is VISIBLE, not merely present: a
// direct child of step 6's section, so no wrapper (a `hidden` element, a
// `<details>`) can fold it away, and on the step that is on screen when the
// wizard opens at step 6. (Placed under the approval block's heading, in the
// same style, it read as part of the approval in the 1440 px screenshot.)
//
// NEGATIVE CONTROL: deleting the paragraph from `renderPlanStep`, printing the
// constant through `esc` or `cell`, stripping its code spans, moving it below
// Create, under the approval block or above the plan, wrapping it in a hidden
// element or a `<details>`, showing it only when a policy was read, or
// rewording one clause each fails a row here. The FX-6 report records the
// mutants and the command that ran them.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  initialState,
  preparePlan,
  recoveryPoints,
  renderPlanStep,
  renderRestoreWizard,
  RESTORE_SEMANTICS_SENTENCE,
} from "../pages/restore-wizard.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const RECORD = fileURLToPath(
  new URL("../../docs/to-do/decisions/PROD-01.1-record-semantics.md", import.meta.url));

/** What an operator reads of this paragraph: its identifiers are code, and the
 *  words between them are the record's. */
const SHOWN =
  "Restores copy the archive as written: aborted transactions and transaction markers are " +
  "restored as ordinary records, LogAppendTime timestamps come back as producer CreateTime, " +
  "repeated header keys keep one copy, and when timestamps are out of order a full or " +
  "point-in-time restore can miss records.";

const PARAGRAPH_OPEN = "<p class=\"note\" id=\"restore-semantics\">";

/** A step-6 state over the newest recovery point of the wizard fixtures. */
function wizardState(policy) {
  const backups = fixture("wizard-backups.json");
  const point = recoveryPoints(backups)[0];
  const state = initialState("logweir-fx6", fixture("wizard-clusters.json"), backups, {
    uid: point.metadata.uid,
    backup: point.metadata.name,
  });
  state.approvalPolicy = policy;
  return state;
}

/** The four approval-policy blocks step 6 can show above Create. */
const POLICIES = Object.freeze({
  "out of band (no policy read)": null,
  "ordinary confirmation": {
    name: "team-ordinary", mode: "ordinary", legacy: false,
    ordinaryConfirmationAvailable: true, ticketRequired: false,
  },
  "ordinary, administrator console": {
    name: "team-ordinary", mode: "ordinary", legacy: false,
    ordinaryConfirmationAvailable: false, ticketRequired: false,
  },
  "governed approval": {
    name: "prod-governed", mode: "governed", legacy: false,
    ordinaryConfirmationAvailable: false, ticketRequired: true,
  },
});

/** The one paragraph carrying the sentence, by its id. */
function semanticsParagraph(html) {
  const at = html.indexOf(PARAGRAPH_OPEN);
  assert.ok(at !== -1,
    "NEGATIVE CONTROL: the review step carries the restore-semantics paragraph:\n" +
      html.slice(0, 600));
  assert.equal(html.indexOf(PARAGRAPH_OPEN, at + PARAGRAPH_OPEN.length), -1,
    "said once, not twice");
  return html.slice(at, html.indexOf("</p>", at) + "</p>".length);
}

const visible = (html) => html.replace(/<[^>]*>/g, "");

const VOID = new Set(["area", "base", "br", "col", "embed", "hr", "img", "input", "link",
  "meta", "param", "source", "track", "wbr"]);

/** The elements still open at `index` in rendered `html`, outermost first, each
 *  as its name and its whole opening tag -- the ancestors, within that markup,
 *  of whatever starts at `index`. The pages escape every value they print, so
 *  a `<` in their output always opens or closes a tag. */
function ancestorsAt(html, index) {
  const open = [];
  const tag = /<(\/?)([a-zA-Z][\w-]*)\b([^>]*)>/g;
  let m;
  while ((m = tag.exec(html)) !== null && m.index < index) {
    const name = m[2].toLowerCase();
    if (m[1] === "/") {
      const at = open.map((e) => e.name).lastIndexOf(name);
      if (at !== -1) {
        open.length = at;
      }
    } else if (!VOID.has(name) && !m[3].endsWith("/")) {
      open.push({ name: name, tag: m[0] });
    }
  }
  return open;
}

/** An ancestor that takes the sentence off the screen or out of the
 *  accessibility tree: `hidden` (attribute or class), `aria-hidden`, an inline
 *  style, or a `<details>` that folds it away until somebody opens it. */
const folds = (e) => e.name === "details" || /\s(hidden|aria-hidden|style)\b/.test(e.tag);

test("fx6_the_review_step_says_what_a_restore_copies_above_create", async () => {
  for (const [label, policy] of Object.entries(POLICIES)) {
    const state = wizardState(policy);
    const prepared = await preparePlan(state);
    assert.equal(typeof prepared.problem, "undefined", label + ": the fixture renders a plan");
    assert.ok(state.fields.topics.length > 0 && prepared.bytes.includes("  topics:\n"),
      label + ": the plan names the topics it restores");

    const step = renderPlanStep(prepared, state);
    assert.ok(step.startsWith("<section class=\"step\" id=\"step-plan\""),
      label + ": step 6 is the review step");
    const paragraph = semanticsParagraph(step);

    // THE RECORD'S WORDS, AS AN OPERATOR READS THEM.
    assert.equal(visible(paragraph), SHOWN, label + ": the sentence, verbatim");

    // CODE IS CODE (O2): the two identifiers are <code>, and nothing on screen
    // is a Markdown backtick.
    assert.ok(paragraph.includes("<code>LogAppendTime</code>"), label + ": " + paragraph);
    assert.ok(paragraph.includes("<code>CreateTime</code>"), label + ": " + paragraph);
    assert.equal(paragraph.indexOf("`"), -1,
      "NEGATIVE CONTROL: " + label + ": no literal backtick (through `esc` it prints two pairs)");

    // WHERE THE OPERATOR REVIEWS THE PLAN, BEFORE CREATE: after the plan bytes,
    // with the plan's own notes, and above the approval-policy block (its <h4>
    // is the step's first) and the one button that creates the Restore.
    const at = step.indexOf(paragraph);
    assert.ok(step.indexOf("id=\"plan-bytes\"") < at, label + ": below the plan it describes");
    assert.ok(at < step.indexOf("<h4"),
      "NEGATIVE CONTROL: " + label + ": with the plan, not under the approval block's heading");
    assert.ok(at < step.indexOf("id=\"create-restore\""),
      "NEGATIVE CONTROL: " + label + ": above Create, not after it");

    // VISIBLE, NOT MERELY PRESENT: a direct child of step 6's section. A
    // wrapper -- a `hidden` element, a `<details>` -- would fold the disclosure
    // away, and a disclosure nobody sees is not one.
    const chain = ancestorsAt(step, at);
    assert.deepEqual(chain.map((e) => e.name), ["section"],
      "NEGATIVE CONTROL: " + label + ": a direct child of step 6's section, in no wrapper: " +
        JSON.stringify(chain.map((e) => e.tag)));
    assert.deepEqual(chain.filter(folds), [], label + ": and the section itself is shown");

    // THE CHECK SEES A WRAPPER (its own control): the same step with the
    // paragraph folded into a `<details>`, or into a hidden div, fails both
    // assertions above.
    for (const [wrap, close] of [["<details><summary>More</summary>", "</details>"],
      ["<div hidden>", "</div>"]]) {
      const folded = step.replace(paragraph, wrap + paragraph + close);
      const around = ancestorsAt(folded, folded.indexOf(paragraph));
      assert.notDeepEqual(around.map((e) => e.name), ["section"], label + ": sees " + wrap);
      assert.equal(around.filter(folds).length, 1, label + ": and names it as folding: " + wrap);
    }
  }
});

test("fx6_the_sentence_is_the_decision_records_own_words_in_one_sentence", () => {
  // The record's section 8.3 blockquote, joined as Markdown joins it.
  const record = readFileSync(RECORD, "utf8");
  const heading = record.indexOf("### 8.3 The restore review screen");
  assert.ok(heading !== -1, "the record still carries section 8.3: " + RECORD);
  const quoted = [];
  for (const line of record.slice(heading).split("\n").slice(1)) {
    if (line.startsWith("> ")) {
      quoted.push(line.slice(2).trim());
    } else if (quoted.length > 0 || line.startsWith("#")) {
      break;
    }
  }
  assert.ok(quoted.length > 0, "section 8.3 quotes the sentence");
  assert.equal(RESTORE_SEMANTICS_SENTENCE, quoted.join(" "),
    "NEGATIVE CONTROL: the console's sentence is the record's, word for word");
  assert.equal((RESTORE_SEMANTICS_SENTENCE.match(/[.!?](?=\s|$)/g) || []).length, 1,
    "one sentence, as the record asks");
});

test("fx6_the_mounted_wizard_shows_it_on_the_step_on_screen", async () => {
  // THE WHOLE WIZARD renders all six pages and hides every one but the step on
  // screen (one step at a time): at step 6 the sentence is on the visible page,
  // with nothing between that page and it but step 6's own section, and it is
  // nowhere else.
  const state = wizardState(null);
  state.step = 5;
  const html = await renderRestoreWizard(state);
  const at = html.indexOf(PARAGRAPH_OPEN);
  assert.ok(at !== -1, "NEGATIVE CONTROL: the whole wizard carries the sentence");
  const chain = ancestorsAt(html, at);
  assert.equal(chain.length, 2,
    "NEGATIVE CONTROL: the page, then step 6's section, and no wrapper: " +
      JSON.stringify(chain.map((e) => e.tag)));
  assert.equal(chain[0].tag, "<div class=\"wizard-page\" data-wizard-step=\"5\">",
    "NEGATIVE CONTROL: on step 6's page, and that page is the one not hidden");
  assert.ok(chain[1].tag.startsWith("<section class=\"step\" id=\"step-plan\""), chain[1].tag);
  assert.deepEqual(chain.filter(folds), [], "nothing above it folds it away");
  assert.equal(html.split("id=\"restore-semantics\"").length, 2, "and only there");
});
