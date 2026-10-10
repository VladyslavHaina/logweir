// schema-dependency.spec.js -- PROD-03.0: the catalog page and the restore
// wizard name the topics whose archived records carry Confluent wire-format
// framing, with the schema ids they reference, and say "registry not
// captured: applications may not read these records after restore".
//
// THE INPUT IS THE API'S OWN ANSWER. `fixtures/console/catalog-point-schema-
// dependency.json`'s `pointTopics` is what `crates/logweir-api/tests/
// d3_reads.rs::the_point_view_publishes_each_topics_schema_dependency` holds
// the product API to, for the entries the runner lists from the record
// (`crates/logweir/tests/check_cli.rs`). One fixture, three readers.
//
// AND IT REACHES EVERY ROW THE WAY IT REACHES THE PAGE (FX-48). Until then
// these rows handed `pointTopics` straight to the renderers, and every one of
// them passed while the console showed nothing: `PointTopicView` did not
// declare `schemaDependency`, so `decodeCatalogPoints` -- which stands between
// the API and both pages at run time -- dropped it (PoC batch 6, F-1). Each
// row now builds the `PointPageResponse` the route answers, holds it to the
// published schema, and reads it with `readCatalogPoints`, the pages' own
// read. `fx48_the_declaration_is_what_carries_the_note_to_the_page` is the
// control that says why that matters.
//
// EVERY ROW CARRIES ITS NEGATIVE CONTROL: the same input with the one fact the
// behaviour depends on changed, which must flip the answer. ABSENT is NOT
// ASSESSED here as everywhere: a point without the field is never said to need
// no registry.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { CONSOLE } from "../client.js";
import {
  D3_SHAPES,
  decodeCatalogPoints,
  decodeListWith,
  int,
  listOf,
  objectOf,
  shapeOf,
  str,
} from "../contract.js";
import { readCatalogPoints } from "../operation-watch.js";
import { decoded, lostPaths, schemaFindings, wire } from "./console-fixture.js";
import {
  SCHEMA_DEPENDENCY_NOT_ASSESSED,
  SCHEMA_DEPENDENCY_NOTE,
  SCHEMA_REGISTRY_NOT_CAPTURED,
  catalogRecoveryPoint,
  initialState,
  pointSchemaDependencyText,
  preparePlan,
  refreshSourceFacts,
  renderCatalogPointStep,
  renderPlanStep,
  schemaDependencyOf,
  schemaDependencyReview,
  schemaDependencyText,
  setCatalogTopics,
  sourceFactsOfEntry,
} from "../pages/restore-wizard.js";
import { renderPoints, schemaDependentTopicsOf } from "../pages/catalog.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
/** The chain file. `pointTopics` is the API's part of it: WIRE data, which a
 *  row puts inside a point page and reads -- never a renderer's argument. */
const CHAIN = wire("catalog-point-schema-dependency.json");

const visible = (html) => html.replace(/<[^>]*>/g, "").replace(/&#39;/g, "'")
  .replace(/&quot;/g, "\"").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");

function byId(html, id) {
  const at = html.indexOf("id=\"" + id + "\"");
  if (at === -1) {
    return null;
  }
  const open = html.lastIndexOf("<", at);
  const name = /^<(\w+)/.exec(html.slice(open))[1];
  const close = html.indexOf("</" + name + ">", at);
  return html.slice(open, close === -1 ? at + 400 : close + name.length + 3);
}

const POINT_ID = "lwp1-0123456789abcdef0123456789abcdef";

/** The catalog row as `GET .../catalogs/{name}/points` publishes it -- ON THE
 *  WIRE -- with the fixture's topics (or `topics`, when given). */
function wireEntry(topics) {
  return {
    pointId: POINT_ID, backupId: "set-p030", runId: "01JB7Z00000000000000000000",
    recoveryPointAt: "2026-10-09T14:00:00Z", coveredFrom: "2026-10-09T13:00:00Z",
    coveredTo: "2026-10-09T14:00:00Z", availability: "Available", verification: "Verified",
    selectable: true, signerKeyId: "c".repeat(64),
    receiptKey: "logweir/backups/set-p030/01JB7Z00000000000000000000.receipt.json",
    receiptSha256: "sha256:" + "a1".repeat(32), manifestKey: "set-p030/manifest.json",
    manifestSha256: "sha256:" + "b2".repeat(32),
    locations: [{ locationId: "s3://kafka-backups/team-a/prod", availability: "Available" }],
    topics: topics === undefined ? CHAIN.pointTopics : topics,
  };
}

/** One page of points, as the route answers it. */
function pointPage(rows) {
  return {
    requestId: "01JB7ZP030P0INTS0000000000", items: rows, page: { limit: 200 },
    truncated: false, viewExpired: false,
  };
}

/** THE PAGE, READ THE WAY BOTH PAGES READ IT: `readCatalogPoints` over its
 *  transport seam, whose decoder is `decodeCatalogPoints`. The answer is held
 *  to the published schema first, so a row cannot invent a member the API
 *  does not send. */
async function readPoints(rows) {
  const answer = pointPage(rows);
  assert.deepEqual(schemaFindings("PointPageResponse", answer), [],
    "the answer is a document the product API may send");
  return readCatalogPoints("team-a", "archive", { limit: 200 }, {}, {
    modeOf: () => CONSOLE,
    consoleSub: async () => JSON.parse(JSON.stringify(answer)),
  });
}

/** ONE POINT, AS A PAGE IS HANDED IT: the wire row through the read above. */
async function entry(topics) {
  return (await readPoints([wireEntry(topics)])).items[0];
}

function catalogObject(ns) {
  return {
    apiVersion: "logweir.dev/v1alpha1", kind: "RecoveryCatalog",
    metadata: { name: "archive", namespace: ns, uid: "cat-uid" },
    spec: { destinationRef: { name: decoded("destination.json").item.name } },
    status: {},
  };
}

/** A catalog-point wizard state over `row`, `selected` typed, the source facts
 *  read the way the mount reads them. */
async function wizardOver(row, selected) {
  const ns = "team-p030";
  const destination = decoded("destination.json").item;
  const point = catalogRecoveryPoint(catalogObject(ns), row, destination, null);
  const state = initialState(ns, fixture("wizard-clusters.json"), { items: [] },
    { catalog: "archive", point: POINT_ID }, destination, undefined, { point: point });
  setCatalogTopics(state, selected);
  assert.equal(await refreshSourceFacts(state, {}), true);
  return state;
}

test("prod030_the_fixture_is_the_chain_the_api_publishes", () => {
  // The verdicts the rows below depend on, read off the fixture: if the chain
  // drops one, these rows would prove less, so they fail here first.
  const verdicts = CHAIN.pointTopics.map((t) => [t.name, t.schemaDependency.verdict]);
  assert.deepEqual(verdicts, [["audit", "notDetected"], ["empty", "notAssessed"],
    ["orders", "schemaDependent"], ["payments", "schemaDependent"]]);
});

test("prod030_a_topics_schema_dependency_is_read_and_anything_else_is_not_assessed", async () => {
  const point = await entry();
  const orders = schemaDependencyOf(point.topics[2].schemaDependency);
  assert.deepEqual(orders, { verdict: "schemaDependent", basis: "sampled", reason: null,
    sides: ["key", "value"], schemaIds: [2, 3, 4], schemaIdsOmitted: false });
  // NEGATIVE CONTROL: no field, a verdict outside the three, a non-object --
  // each is `null`, which every reader takes as NOT ASSESSED.
  for (const bad of [undefined, null, "schemaDependent", { verdict: "registryNeeded" }, {}]) {
    assert.equal(schemaDependencyOf(bad), null, JSON.stringify(bad));
  }
  const facts = sourceFactsOfEntry(point, "archive");
  assert.deepEqual(facts.topics.map((t) => (t.schemaDependency || {}).verdict),
    ["notDetected", "notAssessed", "schemaDependent", "schemaDependent"]);
});

test("prod030_the_review_names_the_selected_schema_dependent_topics_and_their_ids",
  async () => {
    const state = await wizardOver(await entry(), ["orders", "payments", "audit"]);
    const review = schemaDependencyReview(state);
    assert.deepEqual(review.dependent.map((d) => [d.topic, d.sides, d.schemaIds]), [
      ["orders", ["key", "value"], [2, 3, 4]],
      ["payments", ["value"], CHAIN.pointTopics[3].schemaDependency.schemaIds],
    ]);
    assert.deepEqual(review.notAssessed, [], "audit is notDetected: nothing to say");
    const step6 = renderPlanStep(await preparePlan(state), state);
    const warning = byId(step6, "review-schema-dependency-warning");
    assert.ok(warning !== null, "the review warns");
    const said = visible(warning);
    assert.ok(said.includes(SCHEMA_REGISTRY_NOT_CAPTURED), said);
    assert.equal(SCHEMA_REGISTRY_NOT_CAPTURED,
      "Registry not captured: applications may not read these records after restore.");
    assert.ok(said.includes("orders (key and value: schema ids 2, 3, 4; sampled)"), said);
    assert.ok(said.includes("payments (value: schema ids 100001, 100002"), said);
    assert.ok(said.includes("100016 and more; complete)"), said);
    assert.ok(said.includes(SCHEMA_DEPENDENCY_NOTE), said);
    assert.ok(!said.includes("audit"), "a notDetected topic is not named");
    assert.equal(visible(byId(step6, "review-schema-dependency")),
      "registry not captured for 2 schema-dependent topics: orders (key and value: schema ids " +
      "2, 3, 4; sampled), payments (value: schema ids " +
      CHAIN.pointTopics[3].schemaDependency.schemaIds.join(", ") + " and more; complete)");
  });

test("prod030_no_warning_when_every_selected_topic_is_not_detected", async () => {
  // NEGATIVE CONTROL of the row above: select only `audit`, which the point
  // says carries no framing -- the warning goes, and the fact says why.
  const state = await wizardOver(await entry(), ["audit"]);
  const step6 = renderPlanStep(await preparePlan(state), state);
  assert.equal(byId(step6, "review-schema-dependency-warning"), null);
  assert.equal(byId(step6, "review-schema-dependency-not-assessed"), null);
  assert.equal(visible(byId(step6, "review-schema-dependency")),
    "no Confluent wire-format framing detected in any archived record of the selected topics");
});

test("prod030_a_sampled_not_detected_topic_says_the_sample_never_every_record", async () => {
  // `audit` judged over the bounded sample (`basis: sampled`): framing outside
  // the sample is not ruled out, so the sentence says the sample and how many
  // of the topics it covers. NEGATIVE CONTROL: the row above, `complete`.
  const sampled = CHAIN.pointTopics.map((t) => t.name !== "audit" ? t
    : Object.assign({}, t, { schemaDependency: { verdict: "notDetected", basis: "sampled" } }));
  const state = await wizardOver(await entry(sampled), ["audit"]);
  const step6 = renderPlanStep(await preparePlan(state), state);
  assert.equal(visible(byId(step6, "review-schema-dependency")),
    "no Confluent wire-format framing detected in the sampled records of the selected " +
    "topics (sampled for 1 of 1)");
  assert.ok(!visible(step6).includes("any archived record"), "the sample is never 'every record'");
  // The recovery-point step says the same of a point whose topics are all
  // `notDetected`, one of the two sampled.
  const two = [sampled[0], { name: "ledger", applyRoute: "unknown",
    schemaDependency: { verdict: "notDetected", basis: "complete" } }];
  assert.equal(pointSchemaDependencyText(await wizardOver(await entry(two), ["audit"])),
    "no Confluent wire-format framing detected in the sampled records of this point's " +
    "topics (sampled for 1 of 2)");
  const whole = [CHAIN.pointTopics[0], two[1]];
  assert.equal(pointSchemaDependencyText(await wizardOver(await entry(whole), ["audit"])),
    "no Confluent wire-format framing detected in any archived record of this point's topics");
});

test("prod030_an_old_point_reads_not_assessed_never_no_registry_needed", async () => {
  // A point whose topics carry no `schemaDependency` -- a receipt before
  // format 1.5.0, an older runner -- and a topic the backup could not judge.
  const old = await entry(
    CHAIN.pointTopics.map((t) => ({ name: t.name, applyRoute: t.applyRoute })));
  const state = await wizardOver(old, ["orders", "audit"]);
  assert.deepEqual(schemaDependencyReview(state), { dependent: [], notAssessed: ["orders", "audit"] });
  const step6 = renderPlanStep(await preparePlan(state), state);
  assert.equal(byId(step6, "review-schema-dependency-warning"), null);
  const note = visible(byId(step6, "review-schema-dependency-not-assessed"));
  assert.ok(note.startsWith(SCHEMA_DEPENDENCY_NOT_ASSESSED), note);
  assert.ok(note.endsWith("Not assessed: orders, audit."), note);
  assert.equal(schemaDependencyText(state), "not assessed for orders, audit");
  assert.ok(!visible(step6).includes("framing detected"),
    "NEGATIVE CONTROL: absent read as 'none' fails this");
  // The `notAssessed` verdict is said the same way.
  const empty = await wizardOver(await entry(), ["empty"]);
  assert.deepEqual(schemaDependencyReview(empty).notAssessed, ["empty"]);
});

test("prod030_the_recovery_point_step_names_the_points_schema_dependent_topics", async () => {
  const state = await wizardOver(await entry(), ["audit"]);
  const said = pointSchemaDependencyText(state);
  assert.ok(said.startsWith(SCHEMA_REGISTRY_NOT_CAPTURED + " Schema-dependent topics: orders"),
    said);
  assert.ok(visible(renderCatalogPointStep(state)).includes(said));
  // NEGATIVE CONTROL: a point that lists no topics says not assessed.
  const bare = await wizardOver(await entry([]), ["orders"]);
  assert.equal(pointSchemaDependencyText(bare),
    "not assessed: this point's topics are not listed by its catalog");
  // NEGATIVE CONTROL: no dependent topic, one `notDetected` and one
  // `notAssessed` -- never "no framing detected", which would read the
  // unjudged topic as none.
  const mixed = await wizardOver(await entry(CHAIN.pointTopics.slice(0, 2)), ["audit"]);
  assert.equal(pointSchemaDependencyText(mixed), "not assessed for 1 of this point's topics");
  assert.ok(!visible(renderCatalogPointStep(mixed)).includes("framing detected"));
  // ... and with the unjudged topic gone, the point says none was detected.
  const judged = await wizardOver(await entry(CHAIN.pointTopics.slice(0, 1)), ["audit"]);
  assert.equal(pointSchemaDependencyText(judged),
    "no Confluent wire-format framing detected in any archived record of this point's topics");
});

test("prod030_the_catalog_page_names_each_points_schema_dependent_topics", async () => {
  const QUIET = "lwp1-ffffffffffffffffffffffffffffffff";
  const page = await readPoints([wireEntry(),
    Object.assign(wireEntry(CHAIN.pointTopics.slice(0, 2)), { pointId: QUIET })]);
  assert.deepEqual(schemaDependentTopicsOf(page.items[0]).map((t) => t.topic),
    ["orders", "payments"]);
  const html = renderPoints(page, "team-a", "archive", "dest-a");
  const section = byId(html, "catalog-schema-dependency");
  assert.ok(section !== null, "the page has the section");
  const said = visible(section);
  assert.ok(said.includes(SCHEMA_REGISTRY_NOT_CAPTURED), said);
  assert.ok(said.includes(POINT_ID + ": orders (key and value: schema ids 2, 3, 4; sampled)"),
    said);
  assert.ok(!said.includes("lwp1-ffff"), "a point with no dependent topic is not listed");
  // NEGATIVE CONTROL: no dependent topic on the page, no section.
  const quiet = renderPoints(await readPoints([Object.assign(
    wireEntry(CHAIN.pointTopics.slice(0, 2)), { pointId: QUIET })]), "team-a", "archive", "dest-a");
  assert.equal(byId(quiet, "catalog-schema-dependency"), null);
});

// ===========================================================================
// FX-48: the declaration is what carries the note to the page
// ===========================================================================

test("fx48_the_points_schema_dependency_survives_the_decoder_whole", () => {
  // THE DECODER'S OWN ROW. What the API sends for a topic arrives on the
  // decoded point member for member: nothing ignored, nothing lost.
  const answer = pointPage([wireEntry()]);
  const read = decodeCatalogPoints(JSON.parse(JSON.stringify(answer)));
  assert.deepEqual(read.unknown, [], "the decoder ignores no member of the answer");
  assert.deepEqual(lostPaths(answer, read.value), [], "and every member survives it");
  const topics = read.value.items[0].topics;
  assert.deepEqual(topics.map((t) => [t.name, t.schemaDependency.verdict]),
    [["audit", "notDetected"], ["empty", "notAssessed"], ["orders", "schemaDependent"],
      ["payments", "schemaDependent"]]);
  assert.deepEqual(topics[2].schemaDependency.schemaIds, [2, 3, 4]);
  assert.deepEqual(topics[2].schemaDependency.sides, ["key", "value"]);
  assert.equal(topics[3].schemaDependency.schemaIdsOmitted, true);
  assert.equal(topics[1].schemaDependency.reason, "noRecords");
  // ABSENT STAYS ABSENT: a point from before receipt 1.5.0 carries no such
  // member, and the decoded topic says `null` -- NOT ASSESSED to every reader.
  const old = decodeCatalogPoints(pointPage([wireEntry(
    CHAIN.pointTopics.map((t) => ({ name: t.name, applyRoute: t.applyRoute })))]));
  assert.deepEqual(old.value.items[0].topics.map((t) => t.schemaDependency),
    [null, null, null, null]);
  assert.deepEqual(old.unknown, []);
});

test("fx48_the_declaration_is_what_carries_the_note_to_the_page", async () => {
  // THE NEGATIVE CONTROL OF THE WHOLE FILE. `before` is `PointTopicView` as
  // `ui/contract.js` declared it until FX-48 -- no `schemaDependency` -- inside
  // the point shape as it is declared today.
  const before = shapeOf("PointTopicView", { name: str, applyRoute: str },
    { partitions: int, replicationFactor: int, configCoverage: str, owner: str });
  const pointBefore = shapeOf("PointView", D3_SHAPES.PointView.required,
    Object.assign({}, D3_SHAPES.PointView.optional, { topics: listOf(objectOf(before)) }));
  const answer = pointPage([wireEntry()]);
  const dropped = decodeListWith(D3_SHAPES.PointPageResponse, pointBefore,
    JSON.parse(JSON.stringify(answer)));
  assert.deepEqual([...new Set(dropped.unknown.map((path) => path.replace(/\[\d+\]/g, "[]")))],
    ["items[].topics[].schemaDependency"],
    "the old declaration recorded the member as ignored, and nothing read the record");

  // (1) THE DEFECT, as the live install showed it: over the page the OLD
  // decoder hands on, no section and no dependent topic ...
  const blind = renderPoints(dropped.value, "team-a", "archive", "dest-a");
  assert.equal(byId(blind, "catalog-schema-dependency"), null);
  assert.deepEqual(schemaDependentTopicsOf(dropped.value.items[0]), []);
  // ... and the restore review says "not assessed" for topics the API called
  // schema-dependent.
  const wizard = await wizardOver(dropped.value.items[0], ["orders", "payments"]);
  assert.equal(schemaDependencyText(wizard), "not assessed for orders, payments");
  assert.equal(byId(renderPlanStep(await preparePlan(wizard), wizard),
    "review-schema-dependency-warning"), null);

  // (2) WHY EVERY ROW STAYED GREEN. The same bytes, handed to the renderer
  // WITHOUT the decoder -- which is what this file did until FX-48 -- carry
  // the sentence whatever the decoder declares. This is the one place a page
  // function is given the wire document, and it is here to be the control.
  const undecoded = renderPoints(answer, "team-a", "archive", "dest-a");
  assert.ok(visible(byId(undecoded, "catalog-schema-dependency"))
    .includes(SCHEMA_REGISTRY_NOT_CAPTURED),
    "a renderer-only row passes over a member the page is never given");

  // (3) THE FIX: through the page's own read, as it is declared now.
  const page = await readPoints([wireEntry()]);
  const said = visible(byId(renderPoints(page, "team-a", "archive", "dest-a"),
    "catalog-schema-dependency"));
  assert.ok(said.includes(SCHEMA_REGISTRY_NOT_CAPTURED), said);
  assert.ok(said.includes(POINT_ID + ": orders (key and value: schema ids 2, 3, 4; sampled)"),
    said);
  const fixed = await wizardOver(page.items[0], ["orders", "payments"]);
  assert.ok(schemaDependencyText(fixed).startsWith(
    "registry not captured for 2 schema-dependent topics: orders"), schemaDependencyText(fixed));
});

test("fx48_every_value_the_api_sends_is_rendered_as_text", async () => {
  // A topic name, a side and a basis are the API's strings; each reaches the
  // page escaped, on the catalog page and in the review.
  const hostile = "<img src=x onerror=alert(1)>";
  const topics = [{ name: hostile, applyRoute: "unknown",
    schemaDependency: { verdict: "schemaDependent", basis: hostile, sides: ["value"],
      schemaIds: [7] } }];
  const page = await readPoints([wireEntry(topics)]);
  const html = renderPoints(page, "team-a", "archive", "dest-a");
  assert.equal(html.indexOf("<img"), -1, "no markup from the API reaches the page as markup");
  assert.ok(html.indexOf("&lt;img src=x onerror=alert(1)&gt;") !== -1);
  const state = await wizardOver(page.items[0], [hostile]);
  const step6 = renderPlanStep(await preparePlan(state), state);
  assert.equal(step6.indexOf("<img"), -1);
  assert.ok(byId(step6, "review-schema-dependency-warning") !== null, "and the review still warns");
});

// The live row's emitter (`emit-schema-dependency.js`, run by
// `e2e/tests/schema_dependency.rs` over a live backup's catalog point) renders
// through the same functions these rows hold, so a renamed or replaced
// renderer cannot leave the live row printing something these rows never
// checked -- and it DECODES the point first (FX-48), with the decoder the
// console's own read uses, so the live row cannot be green over a member the
// console drops.
test("prod030_the_live_rows_emitter_renders_through_the_functions_held_here", () => {
  const emitter = readFileSync(fileURLToPath(new URL("./emit-schema-dependency.js",
    import.meta.url)), "utf8");
  for (const name of ["renderPoints", "pointSchemaDependencyText", "schemaDependencyText",
    "renderSchemaDependencyWarning", "sourceFactsOfEntry", "catalogRecoveryPoint",
    "decodeCatalogPoints"]) {
    assert.ok(new RegExp("\\b" + name + "\\b").test(emitter), name);
  }
  // THE DECODE IS A CALL, AND WHAT IT RETURNS IS WHAT IS RENDERED: the file's
  // own object reaches no page function.
  assert.match(emitter, /const page = decodeCatalogPoints\(/);
  assert.match(emitter, /const entry = page\.value\.items\[0\];/);
  assert.match(emitter, /renderPoints\(page\.value,/);
  assert.doesNotMatch(emitter, /renderPoints\(\{ items:/);
  const sources = [...emitter.matchAll(/from "([^"]+)"/g)].map((m) => m[1]);
  assert.deepEqual(sources.filter((src) => !src.startsWith("../")), ["node:fs"],
    "the emitter imports the file reader and the console's own modules, nothing else");
});

