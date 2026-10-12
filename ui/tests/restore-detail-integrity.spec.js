// restore-detail-integrity.spec.js -- the Restore detail's Integrity table in
// the shared console: the level the API published is on the page, and a value
// the API does not publish is SAID not to be published, never shown as "-".
//
// For a restore whose status read `integrity.level: byte-fingerprint` and
// `integrity.result: pass`, a table of "-" cells would be wrong in two ways:
//
//   1. THE LEVEL IS PUBLISHED. The operation route answers
//      `OperationViewResponse`, whose `completion.integrityLevel` is the
//      scorecard's own word; a detail read decoded with the narrower
//      `OperationResponse` (sixteen members, no `completion`) would drop it.
//   2. A VALUE NO ROUTE PUBLISHES IS SAID NOT TO BE PUBLISHED: "-" would say
//      "the controller recorded none", which the page does not know. The
//      result, the partial reason, the target topic preflight and the old
//      topics are on the `Restore` view (`visual-pass-a.spec.js` m9_*); the
//      objectives and the measured values are on no route, and the page says
//      so once, where their table would be.
//
// EVERY ROW GOES THROUGH THE REAL CLIENT: the transport is stubbed at the
// platform boundary, the two routes answer with the checked-in documents, and
// `apiClient().get` decodes and projects them as the page's own mount does.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { CONSOLE, apiClient, mode, resetMode, selectMode } from "../client.js";
import { decodeOperation } from "../contract.js";
import {
  NOT_PUBLISHED,
  NOT_PUBLISHED_SENTENCE,
  OBJECTIVES_NOT_PUBLISHED_SENTENCE,
  notPublishedIn,
  renderRestoreDetail,
} from "../pages/history.js";
import { wire } from "./console-fixture.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const NS = "team-a";

const visible = (html) => html.replace(/<[^>]*>/g, "\n").replace(/&#39;/g, "'")
  .replace(/&quot;/g, "\"").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");

/** The `<dd>` of the fact labelled `label` inside the section headed `heading`,
 *  as visible text. */
function fact(html, heading, label) {
  const from = html.indexOf("<h3>" + heading + "</h3>");
  assert.notEqual(from, -1, heading + " is on the page");
  const section = html.slice(from, html.indexOf("</dl>", from));
  const at = section.indexOf("<dt>" + label + "</dt><dd>");
  assert.notEqual(at, -1, label + " is a row of " + heading);
  const start = at + ("<dt>" + label + "</dt><dd>").length;
  return visible(section.slice(start, section.indexOf("</dd>", start))).replace(/\s+/g, " ").trim();
}

/** The shared console, signed in, with the two routes a Restore detail reads
 *  answered by `restore` and `operation`. Returns the projected object. */
async function consoleDetail(restore, operation) {
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: wire("session.json") }) });
  assert.equal(mode(), CONSOLE);
  const original = globalThis.fetch;
  globalThis.fetch = (url) => {
    const path = String(url).split("?")[0];
    const body = path.indexOf("/api/v1/namespaces/" + NS + "/operations/restore/") === 0
      ? operation
      : (path.indexOf("/api/v1/namespaces/" + NS + "/restores/") === 0 ? restore : null);
    if (body === null) {
      return Promise.reject(new Error("the row has no answer for " + String(url)));
    }
    return Promise.resolve({
      ok: true, status: 200, text: () => Promise.resolve(JSON.stringify(body)),
    });
  };
  try {
    return await apiClient().get(NS, "restores", restore.item.name);
  } finally {
    globalThis.fetch = original;
    resetMode();
  }
}

test("fx48_a_shared_console_restore_detail_shows_the_level_the_api_published", async () => {
  const operation = wire("operation-restore-completed.json");
  assert.equal(operation.item.completion.integrityLevel, "byte-fingerprint",
    "the route publishes the scorecard's own word");
  const object = await consoleDetail(wire("restore.json"), operation);

  // THE PROJECTION: under the custom resource's own name, verbatim.
  assert.equal(object.status.integrity.level, "byte-fingerprint",
    "THE DEFECT: the detail read dropped `completion`, so no level reached the page");
  assert.equal(notPublishedIn(object, "status.integrity.level"), false,
    "what the operation route supplied is no longer named absent");

  const html = renderRestoreDetail(object);
  assert.equal(fact(html, "Integrity", "level"), "byte-fingerprint",
    "THE DEFECT: this cell read '-'");
});

test("fx48_the_objectives_the_api_does_not_publish_are_said_so_once_and_never_as_dashes",
  async () => {
    const object = await consoleDetail(wire("restore.json"),
      wire("operation-restore-completed.json"));
    const html = renderRestoreDetail(object);

    // NO ROUTE PUBLISHES THE OBJECTIVES OR THE MEASURED VALUES: one sentence
    // where their table would be, and no row of "-", which is what "the
    // controller recorded none" looks like.
    for (const path of ["status.objectives", "status.measured"]) {
      assert.ok(object.__contract.absent.indexOf(path) !== -1, path + " is named absent");
    }
    const at = html.indexOf("<h3>Objectives asked for, and what the run achieved</h3>");
    assert.notEqual(at, -1);
    assert.match(html.slice(at), /^<h3>[^<]*<\/h3><p class="note" id="restore-objectives-not-published">/);
    assert.ok(visible(html).indexOf(OBJECTIVES_NOT_PUBLISHED_SENTENCE) !== -1);
    assert.equal(html.indexOf("<dt>objectives.rtoSeconds</dt>"), -1);
    assert.equal(html.indexOf(NOT_PUBLISHED), -1,
      "every other value this view shows is published, or recorded as absent");
    assert.equal(html.indexOf("id=\"restore-not-published\""), -1);
    assert.equal(NOT_PUBLISHED, "not published by the product API");
    assert.ok(NOT_PUBLISHED_SENTENCE.indexOf("cannot say whether one is recorded") !== -1,
      "the page does not claim the controller recorded a value it cannot see");
  });

test("fx48_a_route_that_published_no_completion_leaves_the_level_named_not_invented", async () => {
  // NEGATIVE CONTROL of the first row: the same Restore, and an operation
  // answer with no `completion` block -- which is also exactly what the old
  // decode handed the page for EVERY restore. The level is not derived from
  // the scope's word, and the cell does not read "-".
  const operation = wire("operation-restore-completed.json");
  delete operation.item.completion;
  assert.equal(operation.item.verificationScope.level, "sampled");
  const object = await consoleDetail(wire("restore.json"), operation);
  assert.equal((object.status.integrity || {}).level, undefined,
    "a level is copied from the route's own word or it is absent");
  assert.ok(object.__contract.absent.indexOf("status.integrity.level") !== -1);
  const html = renderRestoreDetail(object);
  assert.equal(fact(html, "Integrity", "level"), NOT_PUBLISHED);
  assert.match(visible(html), /64 of 64 sampled records matched byte-for-byte/,
    "the scope sentence still reads the block the API did publish");

  // ... and the two readings of the route are the two decodes: the published
  // view carries the block, an answer of the first sixteen members has none.
  const published = decodeOperation(wire("operation-restore-completed.json")).value.item;
  assert.equal(published.completion.integrityLevel, "byte-fingerprint");
  assert.equal(decodeOperation(wire("operation-backup.json")).value.item.completion, null);
});

test("fx48_each_published_level_reaches_the_cell_verbatim_and_a_claim_until_verified", async () => {
  for (const [name, level] of [
    ["operation-restore-scratch.json", "consume-only"],
    ["operation-restore-no-record-check.json", "not-attempted"],
  ]) {
    const object = await consoleDetail(wire("restore.json"), wire(name));
    assert.equal(object.status.integrity.level, level, name);
    assert.ok(fact(renderRestoreDetail(object), "Integrity", "level").indexOf(level) === 0, name);
  }
  // A SCORECARD FACT IS A CLAIM UNTIL THE EVIDENCE VERIFIES, like every other
  // one on this page: an untrusted run's level carries the label.
  const untrusted = await consoleDetail(wire("restore.json"),
    wire("operation-restore-untrusted.json"));
  const cell = fact(renderRestoreDetail(untrusted), "Integrity", "level");
  assert.ok(cell.indexOf("byte-fingerprint") === 0, cell);
  assert.match(cell, /unverified scorecard claim/);
  // CONTROL: the verified run's level carries none.
  const verified = await consoleDetail(wire("restore.json"),
    wire("operation-restore-completed.json"));
  assert.equal(fact(renderRestoreDetail(verified), "Integrity", "level"), "byte-fingerprint");
});

test("fx48_a_custom_resource_keeps_its_own_cells_and_is_never_told_not_published", () => {
  // LEGACY MODE READS THE OBJECT ITSELF. What the controller recorded is on
  // the page, and what it did not record is "-" as it always was: "not
  // published" is a sentence about the product API and is never said here.
  const listed = JSON.parse(readFileSync(FIXTURES + "restore-valid-pass.json", "utf8"));
  const object = Array.isArray(listed.items) ? listed.items[0] : listed;
  const html = renderRestoreDetail(object);
  assert.equal(fact(html, "Integrity", "level"), "byte-fingerprint");
  assert.equal(fact(html, "Integrity", "result"), "pass");
  assert.equal(fact(html, "Integrity", "partial reason"), "-");
  assert.equal(fact(html, "Objectives asked for, and what the run achieved",
    "objectives.rtoSeconds"), "900");
  assert.equal(fact(html, "Target topic preflight", "timestampType"), "CreateTime");
  assert.equal(html.indexOf(NOT_PUBLISHED), -1);
  assert.equal(html.indexOf("restore-not-published"), -1);
  assert.equal(notPublishedIn(object, "status.integrity.result"), false);

  // ... and a custom resource with no integrity block at all reads "-" three
  // times, and still nothing about the product API.
  const bare = JSON.parse(JSON.stringify(object));
  delete bare.status.integrity;
  const empty = renderRestoreDetail(bare);
  for (const label of ["level", "result", "partial reason"]) {
    assert.equal(fact(empty, "Integrity", label), "-", label);
  }
  assert.equal(empty.indexOf(NOT_PUBLISHED), -1);
});

test("fx48_a_console_list_row_names_the_same_values_absent", async () => {
  // A LIST has no operation route behind it, so the level is absent there too
  // -- named, with the objectives and the measured values; what the Restore
  // view publishes is not.
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: wire("session.json") }) });
  const original = globalThis.fetch;
  globalThis.fetch = () => Promise.resolve({
    ok: true, status: 200, text: () => Promise.resolve(JSON.stringify(wire("restores-list.json"))),
  });
  try {
    const list = await apiClient().list(NS, "restores");
    assert.ok(list.items.length > 0);
    for (const path of ["status.integrity.level", "status.objectives", "status.measured",
      "status.jobRef"]) {
      assert.ok(list.items[0].__contract.absent.indexOf(path) !== -1, path);
    }
    for (const path of ["status.integrity.result", "status.integrity.partialReason",
      "status.topicPreflight", "status.oldTopics"]) {
      assert.equal(list.items[0].__contract.absent.indexOf(path), -1, path + " is published");
    }
  } finally {
    globalThis.fetch = original;
    resetMode();
  }
});
