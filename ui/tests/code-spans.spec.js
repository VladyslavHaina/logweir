// code-spans.spec.js -- no page prints a literal backtick.
//
// A sentence constant printed through `esc` puts Markdown backticks on screen
// ("Use `logweir catalog list`"), and so does markup that spells code with
// backticks or a refusal that names `logweir-api` that way.
//
// THE RULE. What is code on screen is `<code>`: a sentence with backticked
// spans renders through `messageText` (a message through `codeSpans`, the DOM
// error box through `messageNodes`), and markup spells `<code>` itself.
//
// Every row carries its NEGATIVE CONTROL.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { handedPage } from "./console-fixture.js";

import {
  CATALOG_WINDOW_SENTENCE,
  codeSpans,
  errorBlock,
  errorBox,
  fieldErrorLine,
  messageNodes,
} from "../render.js";
import { noD3Route } from "../operation-watch.js";
import {
  MORE_POINTS_SENTENCE,
  renderCatalogList,
  renderCatalogStatus,
  renderPoints,
  renderTrustSnippet,
} from "../pages/catalog.js";
import { renderPolicyKeys, renderPolicySnippet, renderRosterHalf } from "../pages/keys.js";
import { renderState } from "../pages/operation.js";
import { renderDestinationForm, renderDestinationList } from "../pages/destinations.js";
import { renderLastPoint } from "../pages/protection.js";
import { renderApprovalForm, renderCountersignPanel } from "../pages/approvals.js";
import { renderConnectionCheck, renderDiscoveryPanel } from "../pages/clusters.js";
import { renderReadinessPanel, renderRunNowPanel } from "../pages/schedules.js";
import { fakeDocument } from "./fake-dom.js";
import { quotedSegments } from "./source-strings.js";

const UI = fileURLToPath(new URL("../", import.meta.url));
const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

/** What an operator reads of `html` that is not code: everything outside
 *  `<code>`, `<pre>` and `<textarea>`, tags and attributes included (a
 *  `title` is read too). */
function outsideCode(html) {
  return String(html)
    .replace(/<code\b[^>]*>[\s\S]*?<\/code>/g, "")
    .replace(/<pre\b[^>]*>[\s\S]*?<\/pre>/g, "")
    .replace(/<textarea\b[^>]*>[\s\S]*?<\/textarea>/g, "");
}

/** The shipped source files: `ui/*.js` and `ui/pages/*.js`. */
function shipped() {
  return readdirSync(UI).filter((f) => f.endsWith(".js")).map((f) => UI + f)
    .concat(readdirSync(UI + "pages").filter((f) => f.endsWith(".js"))
      .map((f) => UI + "pages/" + f));
}

// EVERY SURFACE THE SWEEP FIXED, RENDERED.

/** This page's own legacy-mode refusal, as the panels and error boxes get it. */
const REFUSAL = noD3Route("A recovery catalog's point list");

test("o2_the_catalog_pages_two_sentences_show_the_command_as_code", () => {
  const catalog = fixture("d3/catalog-truncated.json");
  catalog.status.truncated = true;
  const pages = {
    list: renderCatalogList({ items: [catalog] }, "team-a"),
    status: renderCatalogStatus(catalog),
    points: renderPoints(handedPage([], { page: { limit: 200, nextCursor: "c-2" } }),
      "team-a", "primary", "d-1"),
  };
  for (const [name, html] of Object.entries(pages)) {
    assert.ok(html.includes("<code>logweir catalog list</code>"),
      "NEGATIVE CONTROL: the " + name + " view shows the command as code");
    assert.ok(!outsideCode(html).includes("`"),
      "NEGATIVE CONTROL: no literal backtick on the " + name + " view (the round-4 screenshot " +
        "read \"listed by `logweir catalog list`\")");
  }
  assert.match(CATALOG_WINDOW_SENTENCE, /`logweir catalog list`/,
    "the sentences themselves spell code the way every message here does");
  assert.match(MORE_POINTS_SENTENCE, /`logweir catalog list`/);
});

test("o2_no_surface_the_sweep_found_prints_a_literal_backtick", () => {
  const renders = {
    "catalog trust snippet": renderTrustSnippet([{ keyId: "k-1" }]),
    "keys, not fresh": renderPolicyKeys({ spec: { keys: [] }, status: {} }, Date.now()),
    "keys, roster fallback": renderRosterHalf({ roster: { spec: {}, status: {} } }),
    "keys, policy snippet": renderPolicySnippet(),
    "operation, legacy": renderState({ state: null }),
    "operation, unknown": renderState({ state: "unknown" }),
    "destinations, no default": renderDestinationList({ items: [] }, "team-a"),
    "destination form": renderDestinationForm({}),
    "protection, no point": renderLastPoint({ status: { availabilityBasis: "CatalogStale" } },
      "team-a"),
    "approval form": renderApprovalForm({}, {}),
    "countersign panel": renderCountersignPanel({}),
    "error block": errorBlock(REFUSAL),
    "field error": fieldErrorLine("destination-prefix",
      ["`logweir` and everything under it is reserved for evidence"]),
    "connection check, unavailable": renderConnectionCheck({ unavailable: true,
      unavailableReason: REFUSAL.message }),
    "discovery, unavailable": renderDiscoveryPanel({ unavailable: true,
      unavailableReason: REFUSAL.message }),
    "readiness panel, unavailable": renderReadinessPanel({ unavailable: true,
      unavailableReason: REFUSAL.message }),
    "run now, unavailable": renderRunNowPanel({ unavailable: true,
      unavailableReason: REFUSAL.message }),
  };
  const printed = Object.entries(renders)
    .filter(([, html]) => outsideCode(html).includes("`"))
    .map(([name, html]) => name + ": " + outsideCode(html).match(/[^>]{0,60}`[^<]{0,40}/)[0]);
  assert.deepEqual(printed, [],
    "NEGATIVE CONTROL: every one of these printed its backticks before the sweep:\n" +
      printed.join("\n"));
  assert.ok(renders["error block"].includes("<code>logweir-api</code>"),
    "the refusal names the console as code");
  assert.equal(codeSpans(""), "", "no message is no message, not the absent marker");
});

test("o2_the_dom_error_box_shows_code_as_code", () => {
  const doc = fakeDocument();
  const original = globalThis.document;
  globalThis.document = doc;
  try {
    const box = errorBox(REFUSAL);
    const message = box.querySelector(".error-message");
    const code = message.querySelector("code");
    assert.ok(code !== null, "NEGATIVE CONTROL: the refusal's span is a <code> element");
    assert.equal(code.textContent, "logweir-api");
    assert.ok(!message.childNodes.filter((n) => n.nodeType === 3)
      .some((n) => n.data.includes("`")),
    "NEGATIVE CONTROL: no text node carries a backtick (before: the whole sentence did)");
    assert.equal(messageNodes("no span here"), "no span here", "plain text stays text");
    assert.equal(messageNodes("one ` alone"), "one ` alone", "an unpaired backtick is a character");
  } finally {
    globalThis.document = original;
  }
});

// THE TREE: no new instance of either shape.

test("o2_no_shipped_markup_literal_spells_code_with_a_backtick", () => {
  // A string literal that carries a tag is markup, and markup says `<code>`.
  const found = [];
  let markup = 0;
  for (const file of shipped()) {
    for (const segment of quotedSegments(readFileSync(file, "utf8"), false)) {
      if (!/<\/?[a-z][a-z0-9]*[\s>/]/i.test(segment.text)) {
        continue;
      }
      markup += 1;
      if (segment.text.includes("`")) {
        found.push(file.slice(UI.length) + ":" + String(segment.line) + ": " +
          segment.text.slice(0, 100));
      }
    }
  }
  assert.ok(markup > 500, "the scan read the markup (" + String(markup) + " literals)");
  assert.deepEqual(found, [],
    "NEGATIVE CONTROL: the approval form, the countersign panel, both trust snippets, the " +
      "prefix help and the no-point complaint spelled code with backticks:\n" + found.join("\n"));
});

test("o2_a_sentence_with_a_code_span_is_rendered_through_messageText", () => {
  // Every shipped constant whose text carries a PAIRED backticked span, and
  // every place it is used: it reaches a page through `messageText` (or
  // `codeSpans`), never through `esc` or `cell`, and never concatenated raw.
  const sources = shipped().map((file) => ({ file: file, text: readFileSync(file, "utf8") }));
  const constants = [];
  for (const { file, text } of sources) {
    const pattern = /export const ([A-Z][A-Z0-9_]*) =\s*((?:"(?:[^"\\\n]|\\.)*"\s*\+?\s*)+);/g;
    let match;
    while ((match = pattern.exec(text)) !== null) {
      const value = (match[2].match(/"(?:[^"\\\n]|\\.)*"/g) || []).join("");
      if ((value.match(/`/g) || []).length >= 2) {
        constants.push({ name: match[1], file: file.slice(UI.length) });
      }
    }
  }
  assert.ok(constants.length >= 10,
    "the scan found the sentences it is about (" + constants.map((c) => c.name).join(", ") + ")");
  const wrong = [];
  for (const { name } of constants) {
    for (const { file, text } of sources) {
      const use = new RegExp("(esc|cell)\\(\\s*" + name + "\\b|[\"'] \\+ " + name + "\\b|\\b" +
        name + " \\+ [\"']", "g");
      let hit;
      while ((hit = use.exec(text)) !== null) {
        wrong.push(file.slice(UI.length) + ": " + hit[0] + " (" + name + ")");
      }
    }
  }
  assert.deepEqual(wrong, [],
    "NEGATIVE CONTROL: before the sweep eleven sentences went through esc, the two Catalog " +
      "ones among them:\n" + wrong.join("\n"));
});

// A CODE SPAN IS TEXT, whatever it spells.

test("o2_review_a_code_span_carrying_markup_is_escaped_text_in_every_sink", () => {
  // A problem detail an API server -- or anyone who can write one -- sends,
  // with markup inside and outside a backticked span. Each sink shows it as
  // text: `&lt;img` inside `<code>`, and no element or handler made from it.
  const hostile = "`<img src=x onerror=alert(1)>` and <script>alert(2)</script> a<b";
  const error = Object.assign(new Error(hostile), { status: 422, reason: "Invalid" });
  const strings = {
    codeSpans: codeSpans(hostile),
    errorBlock: errorBlock(error),
    fieldErrorLine: fieldErrorLine("x", [hostile]),
  };
  for (const [sink, html] of Object.entries(strings)) {
    assert.ok(html.includes("<code>&lt;img src=x onerror=alert(1)&gt;</code>"),
      "NEGATIVE CONTROL: " + sink + " escapes the span's markup inside <code>: " + html);
    assert.ok(!/<img|<script/i.test(html), sink + " makes no element of it");
    assert.ok(html.includes("&lt;script&gt;") && html.includes("a&lt;b"),
      sink + " escapes the text outside the span too");
  }
  const doc = fakeDocument();
  const original = globalThis.document;
  globalThis.document = doc;
  try {
    const box = errorBox(error);
    const message = box.querySelector(".error-message");
    assert.equal(message.querySelector("code").textContent, "<img src=x onerror=alert(1)>",
      "NEGATIVE CONTROL: the DOM error box holds the span as the text of a <code> element");
    assert.equal(box.querySelectorAll("img").length + box.querySelectorAll("script").length, 0,
      "and builds no element from it");
  } finally {
    globalThis.document = original;
  }
});
