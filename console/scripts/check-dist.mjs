// What `vite build` wrote is what crates/logweir-api/src/assets.rs serves at /ui/,
// and nothing else: index.html, plus hashed scripts, styles, fonts and images directly in
// assets/, under the server's 8 MiB bound, referenced relatively, and with none of the
// in-page mock (a live build talks to the product API only). Beside them, never served,
// .vite/license.md: the bundled packages' licences, which Dockerfile.console moves to
// /usr/share/licenses/logweir/.
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

const dist = new URL("../dist/", import.meta.url).pathname;
const MAX_BYTES = 8 * 1024 * 1024;
const SAFE = /^[A-Za-z0-9._-]+$/;
// Strings only the mock carries: its base URL, its fallback answer, its demo actor.
const MOCK_MARKERS = ["logweir.invalid", "The prototype has no mock for", "Alice Martin"];
const problems = [];

const top = readdirSync(dist).sort();
if (top.join(",") !== ".vite,assets,index.html") problems.push(`dist/ holds ${top.join(", ")}; expected .vite/, assets/ and index.html`);
const vite = top.includes(".vite") ? readdirSync(join(dist, ".vite")).sort() : [];
if (vite.join(",") !== "license.md") problems.push(`dist/.vite/ holds ${vite.join(", ")}; expected license.md`);
else if (!readFileSync(join(dist, ".vite/license.md"), "utf8").includes("## react - ")) problems.push(".vite/license.md does not list the bundled packages");
const files = ["index.html", ...readdirSync(join(dist, "assets")).map((f) => `assets/${f}`)];
for (const file of files) {
  const path = join(dist, file);
  const name = file.slice(file.lastIndexOf("/") + 1);
  const stat = statSync(path);
  if (!stat.isFile()) problems.push(`${file} is not a file`);
  else if (stat.size > MAX_BYTES) problems.push(`${file} is ${stat.size} bytes; the server takes at most ${MAX_BYTES}`);
  if (file !== "index.html" && !/\.(js|css|woff2|svg)$/.test(name)) problems.push(`${file}: not a type /ui/assets/ serves`);
  if (!SAFE.test(name)) problems.push(`${file}: a name the server's safe_name refuses`);
  if (/\.(js|css|html)$/.test(name)) {
    const text = readFileSync(path, "utf8");
    for (const marker of MOCK_MARKERS) if (text.includes(marker)) problems.push(`${file} carries the mock ("${marker}")`);
    // A stylesheet loads nothing from elsewhere (the job check-ui-offline.sh did for ui/).
    if (name.endsWith(".css") && /@import|url\(\s*["']?(https?:|\/\/)/i.test(text)) problems.push(`${file} loads a stylesheet or a resource from another origin`);
  }
}
const page = readFileSync(join(dist, "index.html"), "utf8");
for (const [, ref] of page.matchAll(/(?:src|href)="([^"]*)"/g)) {
  // The one exception is the icon logweir-api compiles in beside the page.
  if (!ref.startsWith("./assets/") && ref !== "./favicon.svg") problems.push(`index.html names ${ref}; every reference is ./assets/… or ./favicon.svg`);
}
if (/<script(?![^>]*\bsrc=)/.test(page)) problems.push("index.html has an inline script, which the CSP refuses");

if (problems.length > 0) {
  console.error(`check-dist:\n  ${problems.join("\n  ")}`);
  process.exit(1);
}
console.log(`check-dist: ${files.length} files, the shape /ui/ serves, no mock`);
