// condition-message.spec.js -- FX-34: a message the controller wrote is shown
// as TEXT, whatever it holds.
//
// WHY THIS ROW EXISTS. A refused Restore's or Backup's terminal condition now
// carries the runner's own reason, and that sentence interpolates what a plan,
// a broker and an archive said: a tenant can put text in it. weirkeeper cleans
// it before it is stored (`crates/weirkeeper/tests/restore_controller.rs`,
// over the SAME fixture this suite reads), and this suite holds the second
// lock: every page that shows a controller's message shows it inert.
//
// THREE THINGS A MESSAGE MAY HOLD, AND WHAT EACH BECOMES ON SCREEN.
//   * markup (`<script>`, `<img onerror>`): escaped text, never an element;
//   * an HTML entity (`&lt;b&gt;`): the six characters it is, never the `<`
//     it spells;
//   * a bidi override (U+202E): U+FFFD. It is not markup, so escaping does
//     nothing about it, and in a text node it reverses what follows on screen.
//
// Every row carries its NEGATIVE CONTROL.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  BIDI_REPLACEMENT,
  cell,
  checkTable,
  codeSpans,
  conditionBadge,
  esc,
  inert,
  messageText,
} from "../render.js";
import { decodeD3Operation } from "../contract.js";
import { renderOperation, renderProgress, operationFacts } from "../pages/operation.js";
import { renderConditions } from "../pages/protection.js";
import { COMPROMISE_GUARD, renderCompromiseGuard } from "../pages/keys.js";
import { renderApprovalState } from "../pages/approvals.js";
import { renderRestoreOperation } from "../pages/history.js";
import { TOPICS_RESOLVED, renderDiscoveryFailure } from "../pages/schedules.js";

const UI = fileURLToPath(new URL("../", import.meta.url));
const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const refusal = fixture("refusal-condition.json");

/** The nine explicit bidi controls. Spelled as escapes: this file carries no
 *  live one. */
const BIDI = /[\u202A-\u202E\u2066-\u2069]/;

/** What a browser shows for an escaped string: the five entities `esc`
 *  writes, undone once. */
function shown(html) {
  return html
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

/** A page's HTML holds `message` as inert text: no element made of it, its
 *  entity still six characters, no live bidi control, and the reader sees the
 *  message itself with each bidi control replaced. */
function assertInert(html, message, where) {
  assert.doesNotMatch(html, /<script|<img|onerror=alert\(2\)>/i,
    where + ": no element or handler is made of the message");
  assert.ok(html.includes("&lt;script&gt;alert(1)&lt;/script&gt;"),
    where + ": the markup is escaped text");
  assert.ok(html.includes("&amp;lt;b&amp;gt;") && !html.includes(" &lt;b&gt; "),
    where + ": the entity is the characters it is, not the tag it spells");
  assert.doesNotMatch(html, BIDI, where + ": no live bidi control reaches the page");
  assert.ok(shown(html).includes(inert(message).replace(/`/g, "")) ||
      shown(html.replace(/<\/?code>/g, "`")).includes(inert(message)),
    where + ": the reader sees the message, with each bidi control replaced: " + html);
}

test("the_fixture_holds_what_this_suite_says_it_holds", () => {
  // NEGATIVE CONTROL FOR EVERYTHING BELOW: the two messages really carry the
  // three hostile shapes, the stored one with its bidi controls already
  // replaced by the controller and the uncleaned one with them live.
  for (const text of [refusal.storedMessage, refusal.uncleanedMessage]) {
    assert.ok(text.includes("<script>alert(1)</script>") && text.includes("&lt;b&gt;") &&
      text.includes("<img src=x onerror=alert(2)>"));
  }
  assert.doesNotMatch(refusal.storedMessage, BIDI);
  assert.ok(refusal.storedMessage.includes(BIDI_REPLACEMENT + "desrever" + BIDI_REPLACEMENT));
  assert.match(refusal.uncleanedMessage, /\u202Edesrever\u202C/);
  assert.equal(inert(refusal.uncleanedMessage), refusal.storedMessage,
    "the console replaces exactly what the controller replaces");
  // AND `esc` ALONE IS NOT ENOUGH, which is why `inert` exists: it stops the
  // markup and passes the override through.
  assert.doesNotMatch(esc(refusal.uncleanedMessage), /<script/);
  assert.match(esc(refusal.uncleanedMessage), BIDI);
});

test("the_display_helpers_show_a_bidi_control_and_never_obey_it", () => {
  for (const control of ["\u202A", "\u202B", "\u202C", "\u202D", "\u202E",
    "\u2066", "\u2067", "\u2068", "\u2069"]) {
    const text = "a" + control + "b";
    assert.equal(inert(text), "a" + BIDI_REPLACEMENT + "b");
    assert.equal(cell(text), "a" + BIDI_REPLACEMENT + "b");
    assert.equal(messageText(text), "a" + BIDI_REPLACEMENT + "b");
    assert.equal(messageText("`x" + control + "y` z"),
      "<code>x" + BIDI_REPLACEMENT + "y</code> z", "inside a code span too");
    assert.equal(codeSpans(text), "a" + BIDI_REPLACEMENT + "b");
  }
  // NEGATIVE CONTROLS. Ordinary text is untouched, a non-string is still a
  // value, and `esc` -- which also writes form values and identifiers that are
  // read back and sent -- keeps every byte.
  assert.equal(inert("the runner exited 3 (guard-refused)"), "the runner exited 3 (guard-refused)");
  assert.equal(inert(null), "");
  assert.equal(cell(0), "0");
  assert.equal(cell(false), "false");
  assert.equal(cell(""), "-");
  assert.equal(esc("a\u202Eb"), "a\u202Eb");
});

test("the_operation_page_shows_a_refused_runs_reason_as_inert_text_in_both_modes", () => {
  for (const message of [refusal.storedMessage, refusal.uncleanedMessage]) {
    // CONSOLE MODE: the product API's view. `message` is the terminal
    // condition's and `progress.message` the progress block's copy of it.
    const item = decodeD3Operation(fixture("console/operation-restore-completed.json")).value.item;
    item.message = message;
    item.stateReason = "GuardRefused";
    item.progress.message = message;
    item.progress.reason = "GuardRefused";
    const html = renderOperation({
      ns: "team-a", kind: "restore", name: item.name, uid: "",
      document: item, console: true, meta: { transport: "stream", attempt: 0 },
    });
    assertInert(html, message, "console mode");
    assert.equal(html.split(esc(inert(message))).length - 1, 2,
      "the message is on the page twice: the run's own row and the progress block's");

    // LEGACY MODE: the custom resource itself.
    const cr = fixture("restore-valid-pass.json");
    cr.status.reason = "GuardRefused";
    cr.status.progress = {
      stage: "Finished", reason: "GuardRefused", message: message,
      lastTransitionTime: "2026-10-09T20:16:54Z",
    };
    const legacy = renderOperation({
      ns: "team-a", kind: "restore", name: cr.metadata.name, uid: "",
      document: cr, console: false, meta: { transport: "poll", attempt: 0 },
    });
    assertInert(legacy, message, "legacy mode");
    assertInert(renderProgress(operationFacts(cr, false)), message, "the progress block");
  }

  // NEGATIVE CONTROL: the page's own markup is still markup. The message row
  // is a real cell, and a harmless message is printed as itself.
  const plain = decodeD3Operation(fixture("console/operation-restore-completed.json")).value.item;
  const html = renderOperation({
    ns: "team-a", kind: "restore", name: plain.name, uid: "",
    document: plain, console: true, meta: { transport: "stream", attempt: 0 },
  });
  assert.match(html, /the runner exited 0 \(ok\)/);
  assert.match(html, /<section class="progress"><h3>Progress<\/h3>/);
});

test("every_page_that_shows_a_controller_message_shows_it_inert", () => {
  const message = refusal.uncleanedMessage;
  const condition = (type, status) => ({
    type: type, status: status, reason: "GuardRefused", message: message,
    lastTransitionTime: "2026-10-09T20:16:54Z",
  });
  const sinks = {
    "protection: the conditions table": renderConditions([condition("Failed", "True")]),
    "keys: the compromise guard":
      renderCompromiseGuard({ conditions: [condition(COMPROMISE_GUARD, "True")] }),
    "approvals: a refused approval": renderApprovalState(
      { state: "refused", reason: "GuardRefused", message: message },
      { name: "r1" }, "a1"),
    "approvals: an expired key": renderApprovalState(
      { state: "expired", reason: "KeyIdExpired", message: message }, { name: "r1" }, "a1"),
    "approvals: revoked after use": renderApprovalState(
      { state: "revoked-after-use", reason: "KeyCompromise", message: message, key: "k",
        consumed: { at: "2026-10-09T20:16:54Z" } },
      { name: "r1" }, "a1"),
    "history: an approval that could not be read": renderRestoreOperation(
      fixture("restore-valid-pass.json"),
      { ns: "team-a", subject: { name: "r1", approvalName: "a1", uid: "u", planHash: "h" },
        approvalError: { status: 500, reason: "InternalError", message: message } }),
    "schedules: topics not resolved": renderDiscoveryFailure(
      { status: { conditions: [condition(TOPICS_RESOLVED, "False")] } }),
    "render: a condition badge's title": conditionBadge(condition("Ready", "False"), "ready", "not ready"),
    "render: a check's message and remedy": checkTable([{
      id: "restore.plan", code: "GuardRefused", state: "notReady", gating: "blocking",
      message: message, remedy: message, scope: {}, observedAt: null, expiresAt: null,
    }]),
  };
  for (const [where, html] of Object.entries(sinks)) {
    assert.ok(html.length > 0, where + " rendered something");
    assertInert(html, message, where);
  }
  // NEGATIVE CONTROL: the helper these rows rely on can fail. A page that
  // concatenated the message would produce exactly what it refuses.
  assert.throws(() => assertInert("<p>" + message + "</p>", message, "a raw concatenation"));
  assert.throws(() => assertInert("<p>" + esc(message) + "</p>", message, "esc alone"),
    /no live bidi control/);
});

test("no_page_escapes_a_message_without_making_it_inert", () => {
  // THE CLASS, HELD AT THE SOURCE. A `.message` that reaches `esc(` directly
  // is HTML-safe and still carries whatever bidi control it came with; `cell`,
  // `messageText` and `codeSpans` apply `inert` themselves, so the one way to
  // miss it is a bare `esc(x.message)`.
  const bare = /\besc\((?![^;]*\binert\()[^;]*\.message\b/;
  const files = readdirSync(UI + "pages/").filter((f) => f.endsWith(".js"))
    .map((f) => "pages/" + f)
    .concat(readdirSync(UI).filter((f) => f.endsWith(".js")));
  const wrong = [];
  for (const file of files) {
    const lines = readFileSync(UI + file, "utf8").split("\n");
    lines.forEach((line, i) => {
      if (!line.trim().startsWith("//") && !line.trim().startsWith("*") && bare.test(line)) {
        wrong.push(file + ":" + String(i + 1) + ": " + line.trim());
      }
    });
  }
  assert.deepEqual(wrong, [], "a message escaped without `inert`:\n" + wrong.join("\n"));
  assert.ok(files.length > 15 && files.includes("pages/operation.js") &&
    files.includes("render.js"), "the sweep read the tree");
  // NEGATIVE CONTROL: the pattern finds the shapes the sweep removed, and
  // passes the shape that replaced them.
  assert.match("  \": \" + esc(f.message) + \" This is never \" +", bare);
  assert.match("  esc(String(enforced.message || \"\")) +", bare);
  assert.doesNotMatch("  \": \" + esc(inert(f.message)) + \" This is never \" +", bare);
  assert.doesNotMatch("  [\"message\", cell(f.message)],", bare);
});
