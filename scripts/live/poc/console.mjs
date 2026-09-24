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

async function setChecked(page, name, on) {
  const box = page.locator(`input[name="${name}"]`);
  if ((await box.count()) && (await box.isChecked()) !== on) await box.click();
}

// The restore wizard from a Backup (README step 10 / quickstart step 7), end to end: the target,
// the new-topic prefix, the readiness check (every blocking row ready but `approval.state`, which
// is skipped until the Restore exists), Create the Restore -- the operator's Ordinary confirmation
// -- and then the Restore itself, read with kubectl until it is terminal and, when it succeeded,
// until `status.completion` is written or a bound elapses. A legacy (inline-archive) point needs
// its endpoint, region, addressing and transport typed in (`o.legacy`).
export async function restoreFromBackup(page, ns, backup, uid, opts, log) {
  const o = Object.assign({ target: "target", legacy: null, prefix: null, ticket: null, readiness: true, readinessSeconds: 180, runSeconds: 900, shots: null, follow: true }, opts || {});
  const say = (m) => log && log(m);
  await gotoHash(page, `#/restore?ns=${encodeURIComponent(ns)}&backup=${encodeURIComponent(backup)}&uid=${encodeURIComponent(uid)}`);
  await waitForText(page, /6\. Plan, hash and names/, 60, "the wizard");
  if (o.legacy) {
    await page.fill('input[name="endpoint"]', o.legacy.endpoint);
    await page.fill('input[name="region"]', o.legacy.region);
    await page.check('input[name="pathStyle"]');
    if (o.legacy.allowHttp) await page.check('input[name="allowHttp"]');
    if (o.legacy.evidenceBucket) await page.fill('input[name="evidenceBucket"]', o.legacy.evidenceBucket);
  }
  const options = await page.$$eval('select[name="targetCluster"] option', (os) => os.map((x) => [x.value, x.textContent]));
  const hit = options.find((x) => x[1].startsWith(o.target + " "));
  if (!hit) throw new Error(`no target option ${o.target}: ${JSON.stringify(options)}`);
  await page.selectOption('select[name="targetCluster"]', hit[0]);
  await page.selectOption('select[name="mode"]', "newTopic");
  if (o.prefix) { await page.fill('input[name="topicPrefix"]', o.prefix); await page.locator('input[name="topicPrefix"]').blur(); }
  if (o.ticket) { await page.fill("#change-ticket", o.ticket); await page.locator("#change-ticket").blur(); }
  await page.waitForTimeout(1500);
  const plan = await textOf(page);
  const planHash = (plan.match(/plan hash\n(sha256:[0-9a-f]{64})/) || [])[1];
  const minted = (plan.match(/Restore metadata\.name\n(\S+)/) || [])[1];
  say(`wizard: target=${o.target} prefix=${await page.inputValue('input[name="topicPrefix"]')} planHash=${planHash} minted=${minted}`);
  let readiness = null;
  if (o.readiness) {
    await page.click("#restore-readiness-start");
    const end = Date.now() + o.readinessSeconds * 1000;
    while (Date.now() < end) {
      await page.waitForTimeout(4000);
      const t = await textOf(page);
      const s5 = t.slice(t.indexOf("5. Operation readiness"), t.indexOf("6. Plan, hash and names"));
      const rows = [...s5.matchAll(/^([a-zA-Z]+\.[a-zA-Z]+)\t([^\t]+)\t(blocking|advisory|executionOnly)\t([A-Za-z]+)/gm)].map((m) => ({ id: m[1], verdict: m[2], gating: m[3], code: m[4] }));
      if (rows.length > 0 && !rows.some((r) => /pending|running/i.test(r.verdict))) { readiness = { rows, text: s5 }; break; }
    }
    if (!readiness) throw new Error("no readiness verdict in time");
    const blocking = readiness.rows.filter((r) => r.gating === "blocking");
    const notReady = blocking.filter((r) => r.verdict !== "ready" && r.id !== "approval.state");
    say(`readiness: ${blocking.length} blocking rows; not ready (besides approval.state): ${JSON.stringify(notReady)}; approval.state=${(blocking.find((r) => r.id === "approval.state") || {}).verdict}`);
    readiness.blockingNotReady = notReady;
    if (o.shots) await page.screenshot({ path: `${o.shots}-readiness.png`, fullPage: true });
  }
  if (await page.isDisabled("#create-restore")) throw new Error("Create the Restore is disabled");
  await page.click("#create-restore");
  await page.waitForURL(/#\/(operations|approvals|history)/, { timeout: 60000 });
  const route = page.url();
  // The operation view names the Restore `name=`; the approvals view names it `subject=` (and
  // its `name=` is the Approval's).
  const name = decodeURIComponent((route.match(/#\/approvals/) ? route.match(/[?&]subject=([^&]+)/) : route.match(/[?&]name=([^&]+)/) || [])?.[1] || "");
  say(`created: ${route}`);
  if (!o.follow) return { name, route, planHash, readiness, status: {}, operationText: "" };
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
  return { name, route, planHash, readiness, status: st, operationText: opText };
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
export async function createDestination(page, ns, d, log) {
  await gotoHash(page, `#/destinations?ns=${encodeURIComponent(ns)}`);
  await waitForText(page, /Create a destination/, 60, "the destinations page");
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
  await form.locator('input[name="destination"]').fill(destination);
  await form.locator('select[name="syncMode"]').selectOption(mode || "full");
  await form.getByRole("button", { name: /connect archive/i }).click();
  await page.waitForTimeout(3000);
  log && log(`catalog ${name} over ${destination}: connect submitted`);
  return textOf(page);
}


// Clusters -> Create a KafkaCluster (quickstart step 4; README step 10's connections).
export async function createCluster(page, ns, c, log) {
  await gotoHash(page, `#/clusters?ns=${encodeURIComponent(ns)}`);
  await waitForText(page, /Create a KafkaCluster/, 60, "the clusters page");
  const form = page.locator("form", { has: page.locator('input[name="servers"]') }).first();
  await form.locator('input[name="name"]').fill(c.name);
  await form.locator('input[name="servers"]').fill(c.servers);
  await form.locator('input[name="role"]').fill(c.role);
  await form.locator('select[name="mode"]').selectOption(c.mode || "plaintext");
  const tls = form.locator('input[name="tls"]');
  if ((await tls.isChecked()) !== !!c.tls) await tls.click();
  await form.getByRole("button", { name: /^create$/i }).click();
  await page.waitForTimeout(2500);
  log && log(`cluster ${c.name}: create submitted`);
  return textOf(page);
}

const MAIN = import.meta.url === `file://${process.argv[1]}`;
if (MAIN && process.argv[2] === "explore") {
  await explore(process.argv[3], process.argv[4] || "#/schedules?ns=logweir-poc", process.argv[5] || "/tmp");
}
