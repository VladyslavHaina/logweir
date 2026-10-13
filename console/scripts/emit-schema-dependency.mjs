// The schema-dependency text the console's Recovery catalog shows for one point, for the
// live row (e2e/tests/schema_dependency.rs).
//
//     node --experimental-strip-types console/scripts/emit-schema-dependency.mjs <point.json>
//
// Reads ONE catalog point exactly as the product API publishes it (one `items[]` entry of
// `GET .../catalogs/{name}/points`, its `topics[].schemaDependency` included) and prints
// what the catalog page's "Schema-dependent topics" block says for it, with the functions
// that page renders (src/pages/catalog/model.ts):
//
//     == catalog ==
//     Schema-dependent topics
//     <the warning sentence>
//     <pointId>: <topic (sides: schema ids ...; basis)>, ...
//
// A point with no schema-dependent topic prints no topic line, as the page shows none. It
// reaches no network and writes no file.
import { readFileSync } from "node:fs";

import { SCHEMA_REGISTRY_NOT_CAPTURED, schemaDependentTopics } from "../src/pages/catalog/model.ts";

const [file] = process.argv.slice(2);
if (!file) {
  process.stderr.write("usage: node --experimental-strip-types console/scripts/emit-schema-dependency.mjs <point.json>\n");
  process.exit(2);
}
const point = JSON.parse(readFileSync(file, "utf8"));
if (typeof point.pointId !== "string" || !Array.isArray(point.topics)) {
  process.stderr.write("emit-schema-dependency: the file is not one catalog point (pointId, topics[])\n");
  process.exit(2);
}
const topics = schemaDependentTopics(point);
process.stdout.write(["== catalog ==", "Schema-dependent topics", SCHEMA_REGISTRY_NOT_CAPTURED, ...(topics.length > 0 ? [`${point.pointId}: ${topics.join(", ")}`] : []), ""].join("\n"));
