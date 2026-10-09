// emit-schema-dependency.js -- PROD-03.0's console text, for the live row.
//
//     node ui/tests/emit-schema-dependency.js <point.json> <topic>[,<topic>...]
//
// Reads ONE catalog point exactly as the product API publishes it
// (`GET .../catalogs/{name}/points`, one `items[]` entry, its
// `topics[].schemaDependency` included) from the file named, renders the
// catalog page's points section and the restore wizard's recovery-point and
// review texts for the topics named, and writes what an operator reads -- the
// markup with its tags removed -- to stdout, one block per surface:
//
//     == catalog ==        the points section (the "Schema-dependent topics" list)
//     == recovery point == the recovery-point step's `schema registry` fact
//     == review ==         the review's fact and its warning or not-assessed note
//
// `e2e/tests/schema_dependency.rs` hands it the point a live backup's catalog
// record yields and asserts the sentence and the live schema ids are on the
// page. It reaches no network and writes no file: one read, three renders,
// one write to stdout. Not a test (no `.spec` in the name), and not shipped.

import { readFileSync } from "node:fs";

import { renderPoints } from "../pages/catalog.js";
import {
  catalogRecoveryPoint,
  pointSchemaDependencyText,
  renderSchemaDependencyWarning,
  schemaDependencyText,
  setCatalogTopics,
  sourceFactsOfEntry,
} from "../pages/restore-wizard.js";

const [file, topicList] = process.argv.slice(2);
if (typeof file !== "string" || typeof topicList !== "string") {
  throw new Error("usage: emit-schema-dependency.js <point.json> <topic>[,<topic>...]");
}
const entry = JSON.parse(readFileSync(file, "utf8"));
const visible = (html) => html.replace(/<[^>]*>/g, " ").replace(/&#39;/g, "'")
  .replace(/&quot;/g, "\"").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&")
  .replace(/\s+/g, " ").trim();

// The wizard's state over this point, the way its mount builds it: the point
// projected from the catalog row, the source facts read off the row, and the
// operator's topics typed.
const catalog = { metadata: { name: "live", namespace: "e2e" }, spec: {}, status: {} };
const point = catalogRecoveryPoint(catalog, entry, {}, null);
const state = {
  ns: "e2e",
  point: point,
  fields: { topics: [] },
  sourceFacts: sourceFactsOfEntry(entry, "live"),
};
setCatalogTopics(state, topicList.split(",").filter((t) => t.length > 0));

process.stdout.write("== catalog ==\n" + visible(renderPoints({ items: [entry] }, "e2e", "live", "")) +
  "\n== recovery point ==\n" + pointSchemaDependencyText(state) +
  "\n== review ==\n" + schemaDependencyText(state) + "\n" +
  visible(renderSchemaDependencyWarning(state)) + "\n");
