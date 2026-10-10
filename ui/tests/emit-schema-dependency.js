// emit-schema-dependency.js -- PROD-03.0's console text, for the live row.
//
//     node ui/tests/emit-schema-dependency.js <point.json> <topic>[,<topic>...]
//
// Reads ONE catalog point exactly as the product API publishes it
// (`GET .../catalogs/{name}/points`, one `items[]` entry, its
// `topics[].schemaDependency` included) from the file named, DECODES IT AS THE
// CONSOLE DOES, renders the catalog page's points section and the restore
// wizard's recovery-point and review texts for the topics named, and writes
// what an operator reads -- the markup with its tags removed -- to stdout, one
// block per surface:
//
//     == catalog ==        the points section (the "Schema-dependent topics" list)
//     == recovery point == the recovery-point step's `schema registry` fact
//     == review ==         the review's fact and its warning or not-assessed note
//
// `e2e/tests/schema_dependency.rs` hands it the point a live backup's catalog
// record yields and asserts the sentence and the live schema ids are on the
// page. It reaches no network and writes no file: one read, one decode, three
// renders, one write to stdout. Not a test (no `.spec` in the name), and not
// shipped.
//
// THE POINT GOES THROUGH `decodeCatalogPoints` BEFORE ANYTHING RENDERS IT
// (FX-48). That is the decoder `readCatalogPoints` applies between the API and
// both pages, and until it was used here this tool rendered the file's own
// object: the live row was green while the console showed nothing, because the
// decoder dropped `schemaDependency` and this tool never asked it. So the
// point is put in the page envelope the route answers, decoded, and the
// DECODED point is what every render below is given. A point that is not one
// the API may publish -- a required member missing -- is a contract failure
// here, by name, exactly as it would be in the console.

import { readFileSync } from "node:fs";

import { decodeCatalogPoints } from "../contract.js";
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
// The route's own envelope around the one point: the page facts are this
// request's, and nothing about the point is supplied here.
const page = decodeCatalogPoints({
  requestId: "emit-schema-dependency",
  items: [JSON.parse(readFileSync(file, "utf8"))],
  page: { limit: 1 },
  truncated: false,
  viewExpired: false,
});
if (page.unknown.length > 0) {
  throw new Error("the console's decoder ignored " + page.unknown.join(", ") +
    " of this point: a member the API publishes and ui/contract.js does not declare");
}
const entry = page.value.items[0];
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

process.stdout.write("== catalog ==\n" + visible(renderPoints(page.value, "e2e", "live", "")) +
  "\n== recovery point ==\n" + pointSchemaDependencyText(state) +
  "\n== review ==\n" + schemaDependencyText(state) + "\n" +
  visible(renderSchemaDependencyWarning(state)) + "\n");
