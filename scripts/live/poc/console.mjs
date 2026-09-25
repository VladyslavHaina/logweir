// Playwright helpers for the PoC install's console journeys (deploy/poc/README.md steps 9-10):
// a real Chromium signs in through Traefik and Dex and drives the deployed shared console.
//
//   NODE_PATH="$(npm root -g)" node scripts/live/poc/console.mjs explore <role> '<hash route>' [outdir]
//
// Passwords come from the credentials file (LOGWEIR_POC_CREDENTIALS, default
// ~/.logweir-poc/credentials.txt) and are typed into Dex's form; they are never printed.
// The browser context ignores certificate errors because the PoC's local CA is not in the
// OS trust store; TLS itself is proven by curl --cacert against that CA (the report says so).
import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { WIZARD_STEPS, wizardAt, wizardStep, openDestinationCreate } from "../../console-steps.mjs";

export { WIZARD_STEPS, wizardAt, wizardStep, openDestinationCreate };

const require = createRequire(import.meta.url);
export const { chromium } = require("playwright");

export const HOST = process.env.CONSOLE_HOST || "logweir.localtest.me";
export const BASE = `https://${HOST}`;
const CREDS = process.env.LOGWEIR_POC_CREDENTIALS || join(homedir(), ".logweir-poc", "credentials.txt");

export function credential(role) {
  for (const line of readFileSync(CREDS, "utf8").split("\n")) {
    if (line.startsWith("#") || !line.includes("\t")) continue;
    const [user, pw] = line.split("\t");
    if (user.split("@")[0] === role) return { user, pw };
  }
  throw new Error(`no credential for ${role}`);
}

export async function newSession(browser, role) {
  const context = await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1440, height: 1000 } });
  const page = await context.newPage();
  const { user, pw } = credential(role);
  await page.goto(`${BASE}/auth/login`, { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForSelector('input[name="login"]', { timeout: 30000 });
  await page.fill('input[name="login"]', user);
  await page.fill('input[name="password"]', pw);
  await Promise.all([
    page.waitForURL((u) => u.hostname === HOST && u.pathname.startsWith("/ui"), { timeout: 30000 }),
    page.click('button[type="submit"]'),
  ]);
  await page.waitForLoadState("networkidle", { timeout: 30000 }).catch(() => {});
  return { context, page, user };
}

export async function visible(page) {
  return page.evaluate(() => {
    const txt = document.body ? document.body.innerText : "";
    const links = [...document.querySelectorAll("a[href]")].map((a) => [a.innerText.trim().slice(0, 60), a.getAttribute("href")]);
    const buttons = [...document.querySelectorAll("button, input[type=submit]")].map((b) => [(b.innerText || b.value || "").trim().slice(0, 60), b.disabled ? "disabled" : "enabled", b.getAttribute("name") || b.id || ""]);
    const inputs = [...document.querySelectorAll("input, select, textarea")].map((i) => [i.tagName, i.type || "", i.name || i.id || "", (i.labels && i.labels[0] ? i.labels[0].innerText.trim().slice(0, 50) : ""), i.type === "password" ? "" : String(i.value || "").slice(0, 60)]);
    return { url: location.href, text: txt, links, buttons, inputs };
  });
}

export async function gotoHash(page, hash) {
  await page.goto(`${BASE}/ui/${hash}`, { waitUntil: "domcontentloaded" });
  await page.waitForLoadState("networkidle", { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);
}

async function explore(role, hash, outdir) {
  const browser = await chromium.launch();
  try {
    const { page } = await newSession(browser, role);
    await gotoHash(page, hash);
    const v = await visible(page);
    mkdirSync(outdir, { recursive: true });
    const tag = hash.replace(/[^a-z0-9]+/gi, "_").slice(0, 60);
    await page.screenshot({ path: join(outdir, `explore-${role}-${tag}.png`), fullPage: true });
    writeFileSync(join(outdir, `explore-${role}-${tag}.json`), JSON.stringify(v, null, 1));
    console.log(v.url);
    console.log(v.text.slice(0, 6000));
    console.log("LINKS", JSON.stringify(v.links.filter((l) => !l[1].startsWith("#/") || l[0]).slice(0, 60)));
    console.log("BUTTONS", JSON.stringify(v.buttons));
    console.log("INPUTS", JSON.stringify(v.inputs));
  } finally {
    await browser.close();
  }
}


export async function textOf(page) {
  return page.evaluate(() => (document.body ? document.body.innerText : ""));
}

export async function waitForText(page, re, seconds, what) {
  const end = Date.now() + seconds * 1000;
  let t = "";
  while (Date.now() < end) {
    t = await textOf(page);
    if (re.test(t)) return t;
    await page.waitForTimeout(1500);
  }
  throw new Error(`timed out after ${seconds}s waiting for ${what || re}`);
}

// THE RESTORE WIZARD SHOWS ONE STEP AT A TIME (console-ux-1, MCP-29): a control on another
// step can be neither filled nor clicked, and body.innerText carries only the step on screen.
// The journeys walk to each step they drive with Next / Back (scripts/console-steps.mjs), and
// the offline guard (test_poc_harness.py) holds every wizard control in these files behind a
// walk to its own step.
//
// Opens the wizard at `hash` and REQUIRES it on the step the address names (step 1 when none).
export async function openWizard(page, hash) {
  await gotoHash(page, hash);
  return wizardAt(page, Number((hash.match(/[?&]step=([1-6])(?:&|$)/) || [])[1] || 1), 60);
}

// THE CHECK TABLE SINCE MCP ROUND 2 (R2-3, ui/render.js `checkTable`): nine fields in FOUR cells --
// the check id with its code under it, the verdict with its gating under it, the message, the two
// instants -- so a row is read from its cells and `data-field` spans. The old reading, a line of
// innerText `id \t verdict \t gating \t code`, matches no row of this layout, and every readiness
// row of the harness waited out its budget and failed on it (poc-upgrade-3, H7). Every row under
// `selector` as `{id, verdict, gating, code}` (verdict in the page's words: "ready", "not ready",
// "unknown", "skipped (never a pass)"), the container's text, and whether the check is still
// running there: its applicability line reads "checking..." until it has a result (R2-11).
//
// A CHECK THE PAGE STOPPED FOLLOWING IS NOT A VERDICT (P15, poc-fixes-5): a follow now reads a check
// until the longest time a check may take, and a page that gives up marks it `[data-check-stopped]`
// ("deadline": it did not finish; "unreadable": a read was refused), its applicability line reading
// "did not finish" or "could not be read again" (`data-applicability="unfinished"`) instead of
// "checking...". Such a check has no result, whatever rows are on screen: `stopped` names why and
// `settled` -- rows, not checking, not stopped -- is false. Every reader decides on `settled`.
export async function checkRowsIn(page, selector) {
  return page.evaluate((sel) => {
    const root = document.querySelector(sel);
    if (!root) return { rows: [], checking: false, stopped: "", settled: false, text: "" };
    const rows = [];
    for (const tr of root.querySelectorAll("tr")) {
      const gating = tr.querySelector('[data-field="gating"]');
      const tds = tr.querySelectorAll("td");
      if (!gating || tds.length < 2) continue;
      const verdict = tds[1].cloneNode(true);
      verdict.querySelectorAll("[data-field]").forEach((x) => x.remove());
      rows.push({
        id: ((tds[0].querySelector("code") || {}).textContent || "").trim(),
        verdict: verdict.textContent.trim(),
        gating: gating.textContent.trim(),
        code: ((tr.querySelector('[data-field="code"]') || {}).textContent || "").trim(),
      });
    }
    const text = root.innerText;
    const mark = root.querySelector("[data-check-stopped]");
    const stopped = mark ? mark.getAttribute("data-check-stopped") || "stopped"
      : root.querySelector('[data-applicability="unfinished"]') ? "unfinished" : "";
    const checking = !!root.querySelector('[data-applicability="checking"]') || /checking\.\.\./.test(text);
    return { rows, checking, stopped, settled: rows.length > 0 && !checking && !stopped, text };
  }, selector);
}

// THE FLOOR OF EVERY SETTLE WAIT (P15). A Preflight may run its 120 s `timeoutSeconds` and its Job
// gets 90 s more to start, so a check can settle 210 s after it is made, and the page shows it at
// its next follow read (up to 10 s later), while the page keeps following it: a reader that gives
// up sooner calls a check the page is still reading "never settled". 150 s was too short: on a
// loaded docker-desktop (poc-upgrade-5) J6's restore readiness settled 176 s after it was made and
// the journey gave up at 181 s. No reader waits less (test_poc_harness.py holds the literals).
export const SETTLE_FLOOR_SECONDS = 240;

// The check under `selector`, read every `interval` ms until it has a VERDICT (`settled`) or the page
// says it STOPPED following it (`stopped`, `[data-check-stopped]`) -- the two ways a follow ends --
// with `seconds` (never below SETTLE_FLOOR_SECONDS) as the harness's own backstop. Always an object
// `{settled, stopped, timedOut, rows, text, waitedMs}`, and ONLY a settled read carries rows: a
// stopped or timed-out one has `rows: []`, so no caller can take it for a verdict. `outcomeOf`
// names it for a row's evidence.
export async function settledRows(page, selector, seconds, interval) {
  const t0 = Date.now();
  const end = t0 + Math.max(seconds || 0, SETTLE_FLOOR_SECONDS) * 1000;
  let read = { rows: [], checking: false, stopped: "", settled: false, text: "" };
  while (Date.now() < end) {
    await page.waitForTimeout(interval || 3000);
    read = await checkRowsIn(page, selector);
    if (read.settled) return Object.assign(read, { timedOut: false, waitedMs: Date.now() - t0 });
    if (read.stopped) return Object.assign(read, { rows: [], settled: false, timedOut: false, waitedMs: Date.now() - t0 });
  }
  return Object.assign(read, { rows: [], settled: false, stopped: "", timedOut: true, waitedMs: Date.now() - t0 });
}

// What a settle wait ended on, in words, for a row's evidence.
export function outcomeOf(answer) {
  const a = answer || {};
  const s = Math.round((a.waitedMs || 0) / 1000);
  if (a.settled) return `verdict after ${s} s`;
  if (a.stopped) return `the page stopped following the check (${a.stopped}) after ${s} s -- not a verdict`;
  return `no verdict and no stop within the harness's ${s} s`;
}

const APPLIES = /applies to your current inputs/;
const NOT_APPLIES = /does not apply to your current inputs/;

// A readiness verdict WITH its applicability (the schedule form, the list panel, Test access): the
// settled rows (`settledRows`), then -- a replay is terminal on arrival and owes the follow one read
// (P8) -- up to 30 s more for "applies to your current inputs" to be read back. `applies` is true only
// on a settled read that says so; `pf` is the check id shown.
export async function checkVerdict(page, selector, seconds) {
  let read = await settledRows(page, selector, seconds, 2500);
  for (let i = 0; i < 10 && read.settled && !APPLIES.test(read.text); i++) {
    await page.waitForTimeout(3000);
    const again = await checkRowsIn(page, selector);
    read = again.settled ? Object.assign(again, { timedOut: false, waitedMs: read.waitedMs })
      : Object.assign(again, { rows: [], settled: false, timedOut: false, waitedMs: read.waitedMs });
  }
  const applies = read.settled && APPLIES.test(read.text) && !NOT_APPLIES.test(read.text);
  return Object.assign(read, { applies, pf: (read.text.match(/pf-[a-z2-7]{26}/) || [null])[0], outcome: outcomeOf(read) });
}

// Step 5's readiness table, read from the step itself until the check has a verdict or the page
// stopped following it: `settledRows`' object (`settled`, `stopped`, every row `{id, verdict,
// gating, code}` only when settled, and the step's text).
export async function readinessRows(page, seconds) {
  return settledRows(page, "#step-preflight", seconds, 4000);
}

// THE SELECTOR SHOWS 20 POINTS AT A TIME (console-ux-1, MCP-26), the rest `hidden` until "Show
// more recovery points": a row that reads EVERY point clicks it until nothing is left to show
// (bounded), and REQUIRES the count line to say every point is shown.
export async function showEveryPoint(page) {
  for (let i = 0; i < 200; i++) {
    const more = page.locator("#point-more");
    if (!(await more.count()) || !(await more.isVisible()) || (await more.isDisabled())) break;
    await more.click();
  }
  const bar = page.locator("#point-more-bar");
  const said = (await bar.count()) && (await bar.isVisible()) ? await page.locator("#point-count").innerText() : "";
  const m = said.match(/^Showing (\d+) of (\d+)/);
  if (said && !(m && m[1] === m[2])) throw new Error(`the selector still hides points: ${said}`);
  return said;
}

// A LINK IN A PAGINATED LIST (console-ux-1, MCP-26: a schedule card's points and runs are
// datagrids of 20): when the row holding `link` is on another page, the list's own filter box is
// typed into with `name`, as a person would, and the link is REQUIRED visible after.
export async function revealInGrid(page, link, name) {
  const shown = async () => (await link.count()) > 0 && (await link.first().isVisible());
  if (await shown()) return;
  // A PAGINATED DATAGRID HOLDS ONLY ITS CURRENT PAGE IN THE DOM ("1-20 of 280 recovery points"): a
  // row on a later page is not hidden, it is ABSENT, so there is no link to ask for its grid
  // (poc-upgrade-2, J6). The name goes into the grid's own filter box, as a person would type it:
  // the grid the link sits in when it is present, else each filterable grid on the page in turn.
  let grids = [];
  if (await link.count()) {
    const g = await link.first().evaluate((a) => { const d = a.closest("[data-datagrid]"); return d ? d.getAttribute("data-datagrid") : null; });
    if (g) grids = [g];
  }
  if (grids.length === 0) grids = await page.$$eval("[data-datagrid]", (gs) => gs.map((g) => g.getAttribute("data-datagrid")));
  for (const grid of grids) {
    if (!(await page.locator(`#${grid}-filter`).count())) continue;
    await page.fill(`#${grid}-filter`, name);
    await page.waitForTimeout(500);
    if (await shown()) return;
    await page.fill(`#${grid}-filter`, "");
  }
  throw new Error(`the link for ${name} is not on screen (grids ${grids.join(", ") || "none"})`);
}

// A LIST ROW BY ITS RUN'S NAME (console-ux-1, MCP-25): the NAME cell now carries "Follow this
// run" on a second line, so a row is found by its name cell's first line and never by a line of
// body text. A long list is a paginated datagrid, so its own filter box is typed into first, as a
// person would. Returns `{ name, cells: { CAPTION: text } , text }` for the one visible row, or
// null.
export async function listRow(page, gridId, name) {
  const filter = page.locator(`#${gridId}-filter`);
  if (await filter.count()) { await filter.fill(name); await page.waitForTimeout(500); }
  return page.evaluate(({ gridId, name }) => {
    const table = document.querySelector(`#${gridId}-grid`) || document.querySelector(`[data-datagrid="${gridId}"] table`);
    for (const tr of (table ? table.querySelectorAll("tbody tr") : [])) {
      if (tr.hidden) continue;
      const tds = [...tr.querySelectorAll("td")];
      if (tds.length === 0 || tds[0].innerText.split("\n")[0].trim() !== name) continue;
      const cells = Object.fromEntries(tds.map((td, i) => [td.getAttribute("data-label") || String(i), td.innerText.trim()]));
      return { name, cells, text: tds.map((td) => td.innerText.trim()).join(" | ") };
    }
    return null;
  }, { gridId, name });
}

// The restore wizard from a Backup (README step 10 / quickstart step 7), end to end: the target,
// the new-topic prefix, the readiness check (every blocking row ready but `approval.state`, which
// is skipped until the Restore exists), Create the Restore -- the operator's Ordinary confirmation
// -- and then the Restore itself, read with kubectl until it is terminal and, when it succeeded,
// until `status.completion` is written or a bound elapses. A legacy (inline-archive) point needs
// its endpoint, region, addressing and transport typed in (`o.legacy`). The steps walked:
// 1 (legacy archive fields) -> 4 (target, mode, prefix) -> 6 (ticket; the plan hash and the
// minted name) -> 5 (readiness) -> 6 (Create).
export async function restoreFromBackup(page, ns, backup, uid, opts, log) {
  const o = Object.assign({ target: "target", legacy: null, prefix: null, ticket: null, readiness: true, readinessSeconds: 240, runSeconds: 900, shots: null, follow: true }, opts || {});
  const say = (m) => log && log(m);
  await openWizard(page, `#/restore?ns=${encodeURIComponent(ns)}&backup=${encodeURIComponent(backup)}&uid=${encodeURIComponent(uid)}`);
  if (o.legacy) {
    await page.fill("#store-endpoint", o.legacy.endpoint);
    await page.fill("#store-region", o.legacy.region);
    await page.check('input[name="pathStyle"]');
    if (o.legacy.allowHttp) await page.check('input[name="allowHttp"]');
    if (o.legacy.evidenceBucket) await page.fill('input[name="evidenceBucket"]', o.legacy.evidenceBucket);
  }
  const walked = await wizardStep(page, 4);
  const options = await page.$$eval('select[name="targetCluster"] option', (os) => os.map((x) => [x.value, x.textContent]));
  const hit = options.find((x) => x[1].startsWith(o.target + " "));
  if (!hit) throw new Error(`no target option ${o.target}: ${JSON.stringify(options)}`);
  await page.selectOption('select[name="targetCluster"]', hit[0]);
  await page.selectOption("#target-mode", "newTopic");
  if (o.prefix) { await page.fill('input[name="topicPrefix"]', o.prefix); await page.locator('input[name="topicPrefix"]').blur(); }
  const prefix = await page.inputValue('input[name="topicPrefix"]');
  walked.push(...(await wizardStep(page, 6)).slice(1));
  if (o.ticket) { await page.fill("#change-ticket", o.ticket); await page.locator("#change-ticket").blur(); }
  await page.waitForTimeout(1500);
  const plan = await page.locator("#step-plan").innerText();
  const planHash = (plan.match(/plan hash\n(sha256:[0-9a-f]{64})/) || [])[1];
  const minted = (plan.match(/Restore metadata\.name\n(\S+)/) || [])[1];
  say(`wizard: target=${o.target} prefix=${prefix} planHash=${planHash} minted=${minted}`);
  let readiness = null;
  if (o.readiness) {
    walked.push(...(await wizardStep(page, 5)).slice(1));
    await page.click("#restore-readiness-start");
    readiness = await readinessRows(page, o.readinessSeconds);
    if (!readiness.settled) throw new Error(`no readiness verdict: ${outcomeOf(readiness)}`);
    const blocking = readiness.rows.filter((r) => r.gating === "blocking");
    if (blocking.length === 0) throw new Error("the readiness verdict has no blocking row");
    const notReady = blocking.filter((r) => r.verdict !== "ready" && r.id !== "approval.state");
    say(`readiness: ${blocking.length} blocking rows; not ready (besides approval.state): ${JSON.stringify(notReady)}; approval.state=${(blocking.find((r) => r.id === "approval.state") || {}).verdict}`);
    readiness.blockingNotReady = notReady;
    if (o.shots) await page.screenshot({ path: `${o.shots}-readiness.png`, fullPage: true });
  }
  walked.push(...(await wizardStep(page, 6)).slice(1));
  say(`wizard steps walked: ${walked.join(" -> ")}`);
  if (await page.isDisabled("#create-restore")) throw new Error("Create the Restore is disabled");
  await page.click("#create-restore");
  await page.waitForURL(/#\/(operations|approvals|history)/, { timeout: 60000 });
  const route = page.url();
  // The operation view names the Restore `name=`; the approvals view names it `subject=` (and
  // its `name=` is the Approval's).
  const name = decodeURIComponent((route.match(/#\/approvals/) ? route.match(/[?&]subject=([^&]+)/) : route.match(/[?&]name=([^&]+)/) || [])?.[1] || "");
  say(`created: ${route}`);
  if (!o.follow) return { name, route, planHash, readiness, walked, status: {}, operationText: "" };
  let st = {};
  const end = Date.now() + o.runSeconds * 1000;
  let terminalAt = 0;
  while (Date.now() < end && name) {
    await page.waitForTimeout(8000);
    st = (JSON.parse(execFileSync("kubectl", ["--context", "docker-desktop", "--request-timeout=30s", "-n", ns, "get", "restore", name, "-o", "json"], { timeout: 45000, maxBuffer: 256 * 1024 * 1024 }).toString()).status) || {};
    if (st.phase === "Succeeded" || st.phase === "Failed") {
      terminalAt = terminalAt || Date.now();
      if (st.completion || st.phase === "Failed" || Date.now() - terminalAt > 90000) break;
    }
  }
  // A REAL NAVIGATION to the run's operation view (the completion panel lives there): going to
  // the identical hash URL again does not re-render the page, and a stale "Pending" is the result.
  await gotoHash(page, `#/clusters?ns=${encodeURIComponent(ns)}`);
  await gotoHash(page, `#/operations?ns=${encodeURIComponent(ns)}&kind=restore&name=${encodeURIComponent(name)}`);
  await page.waitForTimeout(2500);
  const opText = await textOf(page);
  if (o.shots) await page.screenshot({ path: `${o.shots}-operation.png`, fullPage: true });
  say(`restore ${name}: phase=${st.phase} verification=${((st.evidence || {}).verification || {}).result} completion=${st.completion ? "written" : "absent"}`);
  return { name, route, planHash, readiness, walked, status: st, operationText: opText };
}


// A value from a cluster Secret, read with kubectl --context docker-desktop and handed to
// the caller (for a password field); never printed.
export function secretValue(ns, name, key) {
  const b64 = execFileSync("kubectl", ["--context", "docker-desktop", "--request-timeout=30s", "-n", ns, "get", "secret", name,
    "-o", `go-template={{index .data "${key}"}}`], { timeout: 45000, maxBuffer: 256 * 1024 * 1024 }).toString().trim();
  return Buffer.from(b64, "base64").toString("utf8");
}

async function fillGrant(page, role, grant) {
  await page.selectOption(`select[name="${role}Source"]`, grant.source);
  await page.waitForTimeout(300);
  if (grant.source === "new") {
    await page.fill(`input[name="${role}AccessKeyId"]`, grant.accessKeyId);
    await page.fill(`input[name="${role}SecretAccessKey"]`, grant.secretAccessKey);
  } else if (grant.source === "existing") {
    await page.fill(`input[name="${role}Secret"]`, grant.secret);
  }
}

// README step 10's destination: the demo MinIO, path-style, plaintext, and the three
// least-privilege users of step 7 entered once as new credentials.
// THE CREATE FORM SITS BEHIND A "Create destination" DISCLOSURE (console-ux-1, MCP-10): the
// journey opens it with a click (`openDestinationCreate`). Each credential source then shows only
// its own inputs, so `fillGrant` picks the source first.
export async function createDestination(page, ns, d, log) {
  await gotoHash(page, `#/destinations?ns=${encodeURIComponent(ns)}`);
  await waitForText(page, /Create destination/, 60, "the destinations page");
  await openDestinationCreate(page);
  await waitForText(page, /Create a destination/, 30, "the opened create form");
  const form = page.locator("form", { has: page.locator('input[name="bucket"]') }).first();
  await form.locator('input[name="name"]').fill(d.name);
  if (d.description) await form.locator('input[name="description"]').fill(d.description);
  await form.locator('input[name="bucket"]').fill(d.bucket);
  await form.locator('input[name="prefix"]').fill(d.prefix || "");
  await form.locator('input[name="region"]').fill(d.region);
  await form.locator('input[name="endpoint"]').fill(d.endpoint);
  await form.locator(`input[name="addressing"][value="${d.addressing || "pathStyle"}"]`).check();
  await form.locator(`input[name="security"][value="${d.security || "insecureHttp"}"]`).check();
  for (const role of ["archiveWrite", "archiveRead", "evidenceWrite", "evidenceRead"]) {
    if (d.grants[role]) await fillGrant(page, role, d.grants[role]);
  }
  if (d.writeProbe) await form.locator('select[name="writeProbe"]').selectOption(d.writeProbe);
  const def = form.locator('input[name="isDefault"]');
  if ((await def.isChecked()) !== !!d.isDefault) await def.click();
  await form.getByRole("button", { name: /^create$/i }).click();
  const t = await waitForText(page, new RegExp(`(${d.name}[\\s\\S]*(Valid|valid|created)|refused|error|409|422)`), 60, "the create answer");
  log && log(`destination ${d.name}: create submitted`);
  return t;
}


// Catalog -> Connect an existing archive (quickstart step 9): a RecoveryCatalog over a destination.
export async function connectCatalog(page, ns, name, destination, mode, log) {
  await gotoHash(page, `#/catalog?ns=${encodeURIComponent(ns)}`);
  await waitForText(page, /Connect an existing archive/, 60, "the catalog page");
  const form = page.locator("form", { has: page.locator('select[name="syncMode"]') }).first();
  await form.locator('input[name="name"]').fill(name);
  const control = await chooseCatalogDestination(form, destination);
  await form.locator('select[name="syncMode"]').selectOption(mode || "full");
  await form.getByRole("button", { name: /connect archive/i }).click();
  await page.waitForTimeout(3000);
  log && log(`catalog ${name} over ${destination} (${control}): connect submitted`);
  return textOf(page);
}

// THE CONNECT FORM'S DESTINATION IS A PICK-LIST of the namespace's saved destinations
// (console-ux-1, MCP-23); it is a text box only when that list could not be read. The option
// is chosen by its value, the destination's name, and REQUIRED to exist. Returns which control
// the page offered ("select" or "input"), for the row's evidence.
export async function chooseCatalogDestination(form, destination) {
  const pick = form.locator('[name="destination"]');
  if ((await pick.evaluate((n) => n.tagName)) === "SELECT") {
    const values = await pick.locator("option").evaluateAll((os) => os.map((o) => o.value));
    if (!values.includes(destination)) throw new Error(`the destination pick-list has no ${destination}: ${JSON.stringify(values)}`);
    await pick.selectOption(destination);
    return "select";
  }
  await pick.fill(destination);
  return "input";
}


// Clusters -> Create a KafkaCluster (quickstart step 4; README step 10's connections).
export async function createCluster(page, ns, c, log) {
  await gotoHash(page, `#/clusters?ns=${encodeURIComponent(ns)}`);
  await waitForText(page, /Create a KafkaCluster/, 60, "the clusters page");
  const form = page.locator("form", { has: page.locator('input[name="servers"]') }).first();
  // THE SHARED CONSOLE HAS NO NAME FIELD (P7): the product API names a
  // connection conn-<26 base32>, and the form says so. Fill it only where the
  // form still has one (legacy mode, where the typed name IS the object's).
  const nameInput = form.locator('input[name="name"]');
  if (await nameInput.count() > 0) await nameInput.fill(c.name);
  await form.locator('input[name="servers"]').fill(c.servers);
  await form.locator('input[name="role"]').fill(c.role);
  await form.locator('select[name="mode"]').selectOption(c.mode || "plaintext");
  const tls = form.locator('input[name="tls"]');
  if ((await tls.isChecked()) !== !!c.tls) await tls.click();
  await form.getByRole("button", { name: /^create$/i }).click();
  await page.waitForTimeout(2500);
  log && log(`cluster (role ${c.role}): create submitted`);
  return textOf(page);
}

const MAIN = import.meta.url === `file://${process.argv[1]}`;
if (MAIN && process.argv[2] === "explore") {
  await explore(process.argv[3], process.argv[4] || "#/schedules?ns=logweir-poc", process.argv[5] || "/tmp");
}
