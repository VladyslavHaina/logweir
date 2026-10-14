// The licence allowlist (docs/to-do/decisions/PROD-18.1-console.md §3.3), read from the
// committed lockfile, which records each package's declared licence. Code that ships in
// the page may carry only the first list; build and test tools may also carry the second.
// A package with no licence, or one outside its list, fails the check.
import { readFileSync } from "node:fs";

const SHIPPED = ["MIT", "ISC", "BSD-2-Clause", "BSD-3-Clause", "Apache-2.0", "0BSD", "Unlicense"];
const BUILD_ONLY = [...SHIPPED, "MPL-2.0", "Python-2.0", "BlueOak-1.0.0", "CC0-1.0", "MIT-0"];

/** "(MIT OR CC0-1.0)": allowed when one alternative is; "A AND B": when both are. */
export function allowed(expression, list) {
  const bare = expression.replace(/[()]/g, "").trim();
  if (bare.includes(" OR ")) return bare.split(" OR ").some((e) => allowed(e, list));
  if (bare.includes(" AND ")) return bare.split(" AND ").every((e) => allowed(e, list));
  return list.includes(bare);
}

const lock = JSON.parse(readFileSync(new URL("../package-lock.json", import.meta.url), "utf8"));
const refused = [];
const counts = new Map();
for (const [path, entry] of Object.entries(lock.packages)) {
  if (!path.startsWith("node_modules/") && !path.includes("/node_modules/")) continue;
  const name = path.slice(path.lastIndexOf("node_modules/") + "node_modules/".length);
  const scope = entry.dev ? "build" : "shipped";
  const licence = typeof entry.license === "string" ? entry.license : "";
  counts.set(`${scope} ${licence || "(none)"}`, (counts.get(`${scope} ${licence || "(none)"}`) ?? 0) + 1);
  if (!licence || !allowed(licence, entry.dev ? BUILD_ONLY : SHIPPED)) {
    refused.push(`${name}@${entry.version}: ${licence || "no licence declared"} (${scope})`);
  }
}
for (const [key, n] of [...counts].sort()) console.log(`${String(n).padStart(4)}  ${key}`);
if (refused.length > 0) {
  console.error(`check-licences: ${refused.length} package(s) outside the allowlist:\n  ${refused.join("\n  ")}`);
  process.exit(1);
}
console.log("check-licences: every package is on its allowlist");
