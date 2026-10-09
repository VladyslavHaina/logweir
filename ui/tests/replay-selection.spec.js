// replay-selection.spec.js -- PROD-11.1b (review M1): a restore of a
// PARTITION SUBSET says so on every console surface -- the History list row,
// the History detail and the operation view -- and its complete-coverage
// sentence says "every SELECTED partition", never "every restored partition".
//
// THE RULE EVERY ROW HERE HOLDS: a narrowed restore never reads as a restore of
// everything. An unnarrowed restore (no `selection` anywhere) renders exactly as
// before: no partial line, no "restored (signed)" row.
//
// BOTH SIDES READ ONE FIXTURE. `fixtures/console/restore-subset-pass.json` and
// `fixtures/console/operation-restore-subset-pass.json` are the product API's
// projections of `fixtures/restore-subset-pass.json` (the custom resource: a
// complete, covered pass over orders partitions 0 and 2 and the whole of
// payments, to 2026-09-07T14:05:00Z), written by
// `crates/logweir-api/tests/replay_selection.rs`; this file decodes and renders
// the same bytes, so the names cannot drift.
//
// NEGATIVE CONTROL: each assertion names the mutant it kills.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  completeCoverageSentence,
  selectionIn,
  selectionWords,
  verificationScopeSentence,
} from "../render.js";
import { decodeConsoleItem, decodeD3Operation } from "../contract.js";
import { renderHistoryList, renderRestoreDetail, selectionOf } from "../pages/history.js";
import { operationFacts, renderCoverage, renderOperation } from "../pages/operation.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

/** What an operator reads of a rendered fragment: no tags, entities decoded. */
const plainOf = (value) => JSON.parse(JSON.stringify(value));
const visible = (html) => html.replace(/<[^>]*>/g, "").replace(/&#39;/g, "'")
  .replace(/&quot;/g, "\"").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");

/** The words every surface uses for this fixture's selection. */
const PARTIAL = "partial: partitions 0, 2 of topic orders (every partition of any other " +
  "restored topic)";
const SELECTED = "every record of every SELECTED partition was compared with the archive";
const EVERY = "every record of every restored partition was compared with the archive";

const subset = () => fixture("restore-subset-pass.json");
const whole = () => fixture("restore-valid-pass.json");

// ------------------------------------------------------------ the sentences

test("prod111b_selection_words_name_the_partitions_and_never_read_as_everything", () => {
  const sel = subset().status.integrity.selection;
  assert.equal(selectionWords(sel), PARTIAL,
    "NEGATIVE CONTROL: a `selectionWords` that drops the rows fails this");
  // Rows not served (the controller's bound): the count, and where to look.
  assert.equal(selectionWords({ narrowedTopics: 3 }),
    "partial: a partition subset of 3 topic(s), listed in the signed scorecard");
  // A start only (format 1.7.0): every partition, from the stated start.
  assert.equal(selectionWords({ windowStartMs: 1788782400000, windowEndMs: 1788789900000 }),
    "partial: every partition, from 2026-09-07T12:00:00.000Z (the plan's stated window " +
      "start) to 2026-09-07T14:05:00.000Z");
  // An unreadable selection is still a selection -- never everything.
  assert.match(selectionWords({}), /^partial: a selection this page cannot read/,
    "NEGATIVE CONTROL: an empty selection read as no selection fails this");
  // THE CONTROL: no selection is no words, so an unnarrowed page is unchanged.
  assert.equal(selectionWords(null), "");
  assert.equal(selectionWords(undefined), "");
  assert.equal(selectionIn({ selection: null }), null);
  assert.equal(selectionIn({ selection: [1] }), null);
  assert.equal(selectionIn({}), null);
});

test("prod111b_the_complete_coverage_sentence_says_selected_partitions_for_a_subset", () => {
  const complete = subset().status.integrity.complete;
  const narrowed = completeCoverageSentence(complete, subset().status.integrity.selection);
  assert.ok(narrowed.includes(SELECTED), narrowed);
  assert.ok(narrowed.includes(PARTIAL), narrowed);
  assert.ok(!narrowed.includes(EVERY),
    "NEGATIVE CONTROL: the unqualified 'every restored partition' over a subset fails this: " +
      narrowed);
  // THE CONTROL: the same block with no selection reads as it always did.
  const plain = completeCoverageSentence(complete);
  assert.ok(plain.includes(EVERY), plain);
  assert.ok(!plain.includes("partial"), plain);
  // The scope sentence (the operation view's and the detail's) carries it.
  const scope = { level: "sampled", coverage: "complete", complete, selection: subset().status.integrity.selection };
  assert.ok(verificationScopeSentence(scope).includes(SELECTED),
    "NEGATIVE CONTROL: `verificationScopeSentence` not passing the selection fails this");
  const sampled = verificationScopeSentence({ level: "sampled", coverage: "sampled",
    selection: { narrowedTopics: 1, partitions: [{ topic: "orders", partitions: [0, 2] }] } });
  assert.ok(sampled.endsWith("This was a " + PARTIAL + "."),
    "NEGATIVE CONTROL: a sampled scope sentence silent about the selection fails this: " + sampled);
  assert.ok(!verificationScopeSentence({ level: "sampled", coverage: "sampled" }).includes("partial"));
});

// ------------------------------------------------- the custom resource (k8s)

test("prod111b_the_history_list_row_says_partial_and_the_unnarrowed_row_does_not", () => {
  const row = renderHistoryList({ items: [subset()] }, { items: [] }, "team-a");
  assert.match(row, /<span class="cell-sub" data-selection="partial">partial: partitions 0, 2 of topic orders/,
    "NEGATIVE CONTROL: `coverageLine` without the selection line fails this:\n" + row);
  const control = renderHistoryList({ items: [whole()] }, { items: [] }, "team-a");
  assert.doesNotMatch(control, /data-selection/, "an unnarrowed row is unchanged");
  assert.doesNotMatch(control, /partial:/);
});

test("prod111b_the_history_detail_names_the_selection_and_qualifies_complete_coverage", () => {
  const html = renderRestoreDetail(subset());
  const said = visible(html);
  assert.match(html, /<span id="restore-selection-signed" data-selection="partial">/,
    "NEGATIVE CONTROL: a detail without the 'restored (signed)' row fails this");
  assert.ok(said.includes("restored (signed)" + PARTIAL), said);
  assert.ok(said.includes(SELECTED),
    "NEGATIVE CONTROL: `renderCompleteCoverage` not given the selection fails this");
  assert.ok(!said.includes(EVERY), said);
  // THE CONTROL: an unnarrowed restore has no selection row.
  const plain = renderRestoreDetail(whole());
  assert.doesNotMatch(plain, /restore-selection-signed/);
  assert.ok(!visible(plain).includes("partial:"), visible(plain));
  assert.equal(selectionOf(whole()), null);
});

test("prod111b_the_operation_view_names_the_selection_in_both_modes", () => {
  const console_ = decodeD3Operation(fixture("console/operation-restore-subset-pass.json"))
    .value.item;
  for (const [document, consoleMode] of [[console_, true], [subset(), false]]) {
    const html = renderOperation({
      ns: "team-a", kind: "restore", name: "orders-drill-subset", uid: "",
      document: document, console: consoleMode, meta: { transport: "poll", attempt: 0 },
    });
    assert.match(html, /<section class="coverage" id="operation-coverage" data-selection="partial">/,
      "NEGATIVE CONTROL: `renderCoverage` without the selection fails this: console=" + consoleMode);
    const said = visible(html);
    assert.ok(said.includes("restored (signed)" + PARTIAL), said);
    assert.ok(said.includes(SELECTED), said);
    assert.ok(!said.includes(EVERY), said);
  }
  // THE CONTROL: an unnarrowed restore's coverage section is unchanged.
  const plain = renderCoverage(operationFacts(whole(), false));
  assert.doesNotMatch(plain, /data-selection|restored \(signed\)|partial:/);
});

// ------------------------------------------------- the product API (console)

test("prod111b_the_apis_selection_decodes_and_reaches_every_console_surface", async () => {
  const decoded = decodeConsoleItem("restores", fixture("console/restore-subset-pass.json"));
  const item = decoded.value.item || decoded.value;
  assert.deepEqual(plainOf(item.selection.partitions), [{ topic: "orders", partitions: [0, 2] }],
    "NEGATIVE CONTROL: a `RESTORE` shape without `selection` drops it here");
  assert.equal(item.selection.narrowedTopics, 1);
  assert.equal(item.selection.scope, "partial", "the API's marker");
  const operation = decodeD3Operation(fixture("console/operation-restore-subset-pass.json"));
  assert.deepEqual(plainOf(operation.value.item.verificationScope.selection.partitions),
    [{ topic: "orders", partitions: [0, 2] }],
    "NEGATIVE CONTROL: a `D3_VERIFICATION_SCOPE` shape without `selection` drops it here");
  // THE CONTROL: the unnarrowed projection carries no selection at all.
  const plainItem = decodeConsoleItem("restores", fixture("console/restore-complete-uncovered.json"));
  assert.equal((plainItem.value.item || plainItem.value).selection ?? null, null);
  assert.equal(selectionOf({ status: { integrity: { selection: null } } }), null);

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
      ? fixture("console/operation-restore-subset-pass.json")
      : String(url).split("?")[0].endsWith("/restores")
        ? { requestId: "r", items: [fixture("console/restore-subset-pass.json").item],
          page: { limit: 50, nextCursor: null, snapshot: null } }
        : fixture("console/restore-subset-pass.json"))),
  });
  try {
    const object = await apiClient().get("team-a", "restores", item.name);
    assert.deepEqual(plainOf(object.status.integrity.selection.partitions),
      [{ topic: "orders", partitions: [0, 2] }],
      "NEGATIVE CONTROL: the client's restore mapping not copying `selection` fails this");
    assert.equal(object.status.integrity.selection.scope, "partial");
    assert.deepEqual(plainOf(object.status.verificationScope.selection.partitions),
      [{ topic: "orders", partitions: [0, 2] }],
      "NEGATIVE CONTROL: `mergeOperation` dropping the scope's selection fails this");
    const html = renderRestoreDetail(object);
    assert.match(html, /id="restore-selection-signed" data-selection="partial"/);
    assert.ok(visible(html).includes(SELECTED), visible(html));
    // THE CONSOLE LIST ROW.
    const list = await apiClient().list("team-a", "restores");
    const row = renderHistoryList(list, { items: [] }, "team-a");
    assert.ok(visible(row).includes(PARTIAL),
      "NEGATIVE CONTROL: a console list row without the partial line fails this:\n" + row);
  } finally {
    globalThis.fetch = original;
    resetMode();
  }
});
