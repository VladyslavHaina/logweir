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
// EVERY ROW CARRIES ITS NEGATIVE CONTROL: the same input with the one fact the
// behaviour depends on changed, which must flip the answer. ABSENT is NOT
// ASSESSED here as everywhere: a point without the field is never said to need
// no registry.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

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
const CHAIN = fixture("console/catalog-point-schema-dependency.json");

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

/** The catalog row as `GET .../catalogs/{name}/points` publishes it, with the
 *  fixture's topics (or `topics`, when given). */
function entry(topics) {
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

function catalogObject(ns) {
  return {
    apiVersion: "logweir.dev/v1alpha1", kind: "RecoveryCatalog",
    metadata: { name: "archive", namespace: ns, uid: "cat-uid" },
    spec: { destinationRef: { name: fixture("console/destination.json").item.name } },
    status: {},
  };
}

/** A catalog-point wizard state over `row`, `selected` typed, the source facts
 *  read the way the mount reads them. */
async function wizardOver(row, selected) {
  const ns = "team-p030";
  const destination = fixture("console/destination.json").item;
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

test("prod030_a_topics_schema_dependency_is_read_and_anything_else_is_not_assessed", () => {
  const orders = schemaDependencyOf(CHAIN.pointTopics[2].schemaDependency);
  assert.deepEqual(orders, { verdict: "schemaDependent", basis: "sampled", reason: null,
    sides: ["key", "value"], schemaIds: [2, 3, 4], schemaIdsOmitted: false });
  // NEGATIVE CONTROL: no field, a verdict outside the three, a non-object --
  // each is `null`, which every reader takes as NOT ASSESSED.
  for (const bad of [undefined, null, "schemaDependent", { verdict: "registryNeeded" }, {}]) {
    assert.equal(schemaDependencyOf(bad), null, JSON.stringify(bad));
  }
  const facts = sourceFactsOfEntry(entry(), "archive");
  assert.deepEqual(facts.topics.map((t) => (t.schemaDependency || {}).verdict),
    ["notDetected", "notAssessed", "schemaDependent", "schemaDependent"]);
});

test("prod030_the_review_names_the_selected_schema_dependent_topics_and_their_ids",
  async () => {
    const state = await wizardOver(entry(), ["orders", "payments", "audit"]);
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
  const state = await wizardOver(entry(), ["audit"]);
  const step6 = renderPlanStep(await preparePlan(state), state);
  assert.equal(byId(step6, "review-schema-dependency-warning"), null);
  assert.equal(byId(step6, "review-schema-dependency-not-assessed"), null);
  assert.equal(visible(byId(step6, "review-schema-dependency")),
    "no schema framing detected in the selected topics' archived records");
});

test("prod030_an_old_point_reads_not_assessed_never_no_registry_needed", async () => {
  // A point whose topics carry no `schemaDependency` -- a receipt before
  // format 1.5.0, an older runner -- and a topic the backup could not judge.
  const old = entry(CHAIN.pointTopics.map((t) => ({ name: t.name, applyRoute: t.applyRoute })));
  const state = await wizardOver(old, ["orders", "audit"]);
  assert.deepEqual(schemaDependencyReview(state), { dependent: [], notAssessed: ["orders", "audit"] });
  const step6 = renderPlanStep(await preparePlan(state), state);
  assert.equal(byId(step6, "review-schema-dependency-warning"), null);
  const note = visible(byId(step6, "review-schema-dependency-not-assessed"));
  assert.ok(note.startsWith(SCHEMA_DEPENDENCY_NOT_ASSESSED), note);
  assert.ok(note.endsWith("Not assessed: orders, audit."), note);
  assert.equal(schemaDependencyText(state), "not assessed for orders, audit");
  assert.ok(!visible(step6).includes("no schema framing detected"),
    "NEGATIVE CONTROL: absent read as 'none' fails this");
  // The `notAssessed` verdict is said the same way.
  const empty = await wizardOver(entry(), ["empty"]);
  assert.deepEqual(schemaDependencyReview(empty).notAssessed, ["empty"]);
});

test("prod030_the_recovery_point_step_names_the_points_schema_dependent_topics", async () => {
  const state = await wizardOver(entry(), ["audit"]);
  const said = pointSchemaDependencyText(state);
  assert.ok(said.startsWith(SCHEMA_REGISTRY_NOT_CAPTURED + " Schema-dependent topics: orders"),
    said);
  assert.ok(visible(renderCatalogPointStep(state)).includes(said));
  // NEGATIVE CONTROL: a point that lists no topics says not assessed.
  const bare = await wizardOver(entry([]), ["orders"]);
  assert.equal(pointSchemaDependencyText(bare),
    "not assessed: this point's topics are not listed by its catalog");
});

test("prod030_the_catalog_page_names_each_points_schema_dependent_topics", () => {
  const page = { items: [entry(), Object.assign(entry(CHAIN.pointTopics.slice(0, 2)),
    { pointId: "lwp1-ffffffffffffffffffffffffffffffff" })] };
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
  const quiet = renderPoints({ items: [page.items[1]] }, "team-a", "archive", "dest-a");
  assert.equal(byId(quiet, "catalog-schema-dependency"), null);
});
