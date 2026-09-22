// PLAT-20.1 native console journeys: the two the audit found no live row for
// on the CURRENT console (logweir-api serving ui/, localAdmin, loopback).
//
// 1. REGISTRATION AND DISCOVERY THAT THE CONTROLLER COMPLETES. The console rows
//    that exist (scripts/d2w13-ui-e2e.mjs) were written for a lab with no
//    controller for BackupDestination/TopicDiscovery/Preflight and assert
//    "pending, never succeeded" -- on today's lab they fail at :353. Here the
//    page registers a connection and a destination and starts a discovery, and
//    each is read back as the CONTROLLER's verdict: reachable, Valid,
//    Succeeded, and the discovered inventory names the topic this run created
//    on the broker.
// 2. STALE NAMESPACE REQUEST (PLAT-13.1, "slow A response after switching to
//    B" and "stale submit callback"). The only live row is
//    scripts/plat13-ui-e2e.mjs's multi stage, which needs a deployed legacy UI
//    proxy. Here: A's list answer is held until after B has rendered, and must
//    not paint over B; a form left behind in A must write nothing anywhere;
//    and a submit after the switch must land in B only -- each read back with
//    kubectl, not only from the DOM.
//
// Every row REQUIRES what it records: the stale-render row fails unless A's
// delayed answer really arrived AFTER B was on screen (otherwise the page was
// never tested against a stale answer), and the left-form row fails unless the
// same form, when current, does issue a POST.
//
// Environment (the runner sets all of these):
//   JOURNEY_OUT        result directory (result.json, screenshots, api.log)
//   JOURNEY_PRIVATE    0700 work dir for the API config and cursor key
//   JOURNEY_OWNER      logweir.dev/test-owner value
//   JOURNEY_NS_A/_B    the two namespaces this script creates and deletes
//   JOURNEY_TOPIC      a topic the runner created on the lab source broker
//   JOURNEY_PREFIX     an archive prefix (under kafka-backups) this run owns
//   UI_E2E_API_BIN     the logweir-api binary
// NODE_PATH="$(npm root -g)" so `playwright` resolves.

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { mkdirSync, writeFileSync, rmSync, createWriteStream } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const CONTEXT = "docker-desktop";
const FIXTURE_NS = "logweir-scram-local";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
const env = (name) => {
  const value = process.env[name];
  if (!value) throw new Error(name + " is required");
  return value;
};
const OUT = env("JOURNEY_OUT");
const PRIVATE = env("JOURNEY_PRIVATE");
const OWNER = env("JOURNEY_OWNER");
const NS_A = env("JOURNEY_NS_A");
const NS_B = env("JOURNEY_NS_B");
const TOPIC = env("JOURNEY_TOPIC");
const PREFIX = env("JOURNEY_PREFIX");
const API_BIN = env("UI_E2E_API_BIN");
const SOURCE = "kafka-source." + FIXTURE_NS + ".svc.cluster.local:9096";
const MINIO = "http://minio." + FIXTURE_NS + ".svc.cluster.local:9000";
const sfx = randomBytes(3).toString("hex");

const result = { harness: "e2e/journeys/console.mjs", context: CONTEXT, namespaces: [NS_A, NS_B],
  rows: {}, details: {}, created: [], cleanup: [], startedAt: new Date().toISOString() };

function row(name, ok, detail, facts) {
  result.rows[name] = ok ? "PASS" : "FAIL";
  result.details[name] = { verdict: result.rows[name], detail: detail, facts: facts || {} };
  process.stderr.write("[" + result.rows[name] + "] " + name + " -- " + detail + "\n");
}

function assertOwnedName(ns) {
  if (!ns.startsWith("lw-plat20-") || ns === FIXTURE_NS || ns.startsWith("kube-")) {
    throw new Error("this script only touches lw-plat20-* namespaces, not " + ns);
  }
}

function kube(args, opts) {
  const o = opts || {};
  const done = spawnSync("kubectl", ["--context", CONTEXT].concat(args),
    { encoding: "utf8", input: o.input, timeout: o.timeout || 60000, maxBuffer: 8 * 1024 * 1024 });
  if (!(o.ok || [0]).includes(done.status)) {
    throw new Error("kubectl " + args.slice(0, 6).join(" ") + " exited " + done.status + ": " +
      String(done.stderr || "").slice(0, 800));
  }
  return done;
}
const kjson = (args) => JSON.parse(kube(args.concat(["-o", "json"])).stdout);
const kopt = (args) => {
  const d = kube(args.concat(["-o", "json"]), { ok: [0, 1] });
  return d.status === 0 ? JSON.parse(d.stdout) : null;
};
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
// THE API NAMES A NEW CONNECTION ITSELF (`conn-<random>`, measured live:
// the typed name is not the object's name), so a created object is found by
// the UID set difference, never by the name that was typed.
const clusters = (ns) => new Map(kjson(["-n", ns, "get", "kafkaclusters"]).items
  .map((o) => [o.metadata.uid, o]));
const added = (before, ns) => [...clusters(ns).entries()].filter(([uid]) => !before.has(uid)).map(([, o]) => o);

async function until(what, probe, seconds) {
  const deadline = Date.now() + seconds * 1000;
  let last = null;
  while (Date.now() < deadline) {
    last = await probe();
    if (last) return last;
    await pause(2000);
  }
  throw new Error("timed out after " + seconds + "s waiting for " + what);
}

function createNamespace(ns) {
  assertOwnedName(ns);
  if (kopt(["get", "namespace", ns]) !== null) throw new Error("refusing to reuse " + ns);
  kube(["create", "-f", "-"], { input: JSON.stringify({ apiVersion: "v1", kind: "Namespace",
    metadata: { name: ns, labels: { "logweir.dev/test-owner": OWNER } } }) });
  const uid = kjson(["get", "namespace", ns]).metadata.uid;
  result.created.push({ kind: "Namespace", name: ns, uid: uid });
  return uid;
}

function copySecret(name, ns) {
  const src = kjson(["-n", FIXTURE_NS, "get", "secret", name]);
  kube(["-n", ns, "create", "-f", "-"], { input: JSON.stringify({ apiVersion: "v1", kind: "Secret",
    metadata: { name: name, labels: { "logweir.dev/test-owner": OWNER } }, type: src.type, data: src.data }) });
}

function deleteOwnedNamespace(ns) {
  assertOwnedName(ns);
  const live = kopt(["get", "namespace", ns]);
  const mine = result.created.find((c) => c.kind === "Namespace" && c.name === ns);
  if (live === null) return { namespace: ns, deleted: false, why: "absent" };
  const label = (live.metadata.labels || {})["logweir.dev/test-owner"];
  if (label !== OWNER || !mine || live.metadata.uid !== mine.uid) {
    return { namespace: ns, deleted: false, why: "refused: not this run's (" + label + ")" };
  }
  kube(["delete", "namespace", ns, "--wait=true"], { timeout: 300000, ok: [0, 1] });
  return { namespace: ns, uid: live.metadata.uid, ownerLabel: label, deleted: true,
    absentAfter: kopt(["get", "namespace", ns]) === null };
}

function freePort() {
  return new Promise((ok, bad) => {
    const s = createServer();
    s.on("error", bad);
    s.listen(0, "127.0.0.1", () => { const p = s.address().port; s.close(() => ok(p)); });
  });
}

let api = null;
async function startApi(port) {
  mkdirSync(PRIVATE, { recursive: true, mode: 0o700 });
  const key = join(PRIVATE, "console-cursor.key");
  writeFileSync(key, randomBytes(32), { mode: 0o600 });
  const config = ["mode: localAdmin", "listen: \"127.0.0.1:" + port + "\"",
    "publicOrigin: \"http://127.0.0.1:" + port + "\"", "uiDirectory: " + join(REPO, "ui"),
    "localAdmin:", "  subject: admin", "  displayName: Local administrator",
    "namespaces: [" + NS_A + ", " + NS_B + "]", "kubernetes:", "  source: kubeconfig",
    "  context: " + CONTEXT, "cursorKeyFile: " + key, ""].join("\n");
  writeFileSync(join(PRIVATE, "console-api.yaml"), config);
  writeFileSync(join(OUT, "api-config.yaml"), config);
  const log = createWriteStream(join(OUT, "api.log"));
  api = spawn(API_BIN, ["--config", join(PRIVATE, "console-api.yaml")], { stdio: ["ignore", "pipe", "pipe"] });
  api.stdout.pipe(log);
  api.stderr.pipe(log);
  for (let i = 0; i < 90; i += 1) {
    try {
      if ((await fetch("http://127.0.0.1:" + port + "/healthz")).ok) return;
    } catch (notYet) { /* binding */ }
    await pause(500);
  }
  throw new Error("logweir-api never answered /healthz within 45 s");
}

const text = async (page) => (await page.evaluate(() => document.body.innerText));
async function waitText(page, needle, seconds) {
  await until("the page to show " + JSON.stringify(needle),
    async () => (await text(page)).includes(needle), seconds || 30);
}
async function shot(page, name) {
  await page.screenshot({ path: join(OUT, name + ".png"), fullPage: true });
}

async function fillCluster(page, name, servers, scram) {
  await page.waitForSelector("#cluster-form");
  await page.fill("#cluster-name", name);
  await page.fill("#cluster-servers", servers);
  await page.fill("#cluster-role", "source");
  await page.selectOption("#cluster-mode", scram ? "scramSha512" : "plaintext");
  if (scram) {
    await page.fill("#cluster-username", "scram-user");
    await page.fill("#cluster-secret", "source-scram");
  }
}

// ------------------------------------------------------------ journey 1

async function registrationAndDiscovery(page, base, port) {
  const beforeReg = clusters(NS_A);
  await page.goto(base + "#/clusters?ns=" + NS_A, { waitUntil: "load" });
  await fillCluster(page, "src-" + sfx, SOURCE, true);
  await page.click("#cluster-form button[type=submit]");
  const made = await until("the page's connection to exist", async () => {
    const n = added(beforeReg, NS_A);
    return n.length ? n : null;
  }, 60);
  const conn = made[0].metadata.name;
  const reached = await until("the controller to reach the registered connection", async () => {
    const o = kopt(["-n", NS_A, "get", "kafkacluster", conn]);
    return o && o.status && o.status.reachable === true ? o : null;
  }, 240);
  await shot(page, "01-connection-registered");
  row("console-registers-a-connection-the-controller-reaches",
    made.length === 1 && reached.metadata.uid === made[0].metadata.uid &&
      reached.spec.bootstrapServers[0] === SOURCE && reached.spec.auth.secretRef.name === "source-scram" &&
      Boolean(reached.status.clusterId),
    "the page created KafkaCluster/" + conn + " (uid " + reached.metadata.uid + "); the controller reached " +
      "it and recorded cluster id " + reached.status.clusterId,
    { uid: reached.metadata.uid, clusterId: reached.status.clusterId, spec: reached.spec });

  const dest = "dst-" + sfx;
  await page.goto(base + "#/destinations?ns=" + NS_A, { waitUntil: "load" });
  await page.waitForSelector("#destination-form");
  await page.fill("#destination-name", dest);
  await page.fill("#destination-bucket", "kafka-backups");
  await page.fill("#destination-prefix", PREFIX + "/console");
  await page.fill("#destination-region", "us-east-1");
  await page.fill("#destination-endpoint", MINIO);
  await page.check("#destination-security-http");
  await page.check("#destination-addressing-pathstyle");
  await page.selectOption("#destination-archiveWrite-source", "existing");
  await page.fill("#destination-archiveWrite-secret", "logweir-s3");
  await page.click("#destination-form button[type=submit]");
  const valid = await until("the controller to judge the registered destination", async () => {
    const o = kopt(["-n", NS_A, "get", "backupdestination", dest]);
    const c = o && ((o.status || {}).conditions || []).find((x) => x.type === "Valid");
    return c && c.status !== "Unknown" ? { o: o, c: c } : null;
  }, 240);
  await shot(page, "02-destination-registered");
  row("console-registers-a-destination-the-controller-validates", valid.c.status === "True",
    "the page created BackupDestination/" + dest + " (uid " + valid.o.metadata.uid + "); the controller " +
      "judged it Valid=" + valid.c.status + "/" + valid.c.reason,
    { uid: valid.o.metadata.uid, valid: valid.c, storage: valid.o.spec.storage });

  await page.goto(base + "#/clusters?ns=" + NS_A + "&name=" + conn, { waitUntil: "load" });
  await page.waitForSelector("#discovery-start");
  await page.click("#discovery-start");
  const done = await until("the controller to finish the page's discovery", async () => {
    const items = kjson(["-n", NS_A, "get", "topicdiscoveries"]).items;
    const d = items.find((x) => ["Succeeded", "Failed", "Cancelled"].includes((x.status || {}).phase));
    return d ? { d: d, count: items.length } : null;
  }, 360);
  let listed = [];
  let cursor = null;
  const apiStatuses = [];
  for (let i = 0; i < 20; i += 1) {
    const url = "http://127.0.0.1:" + port + "/api/v1/namespaces/" + NS_A + "/topic-discoveries/" +
      done.d.metadata.name + "/topics?limit=200" + (cursor ? "&cursor=" + encodeURIComponent(cursor) : "");
    const r = await fetch(url);
    const body = await r.json();
    apiStatuses.push(r.status);
    listed = listed.concat((body.items || []).map((t) => t.name));
    cursor = body.page && body.page.nextCursor;
    if (!cursor) break;
  }
  writeFileSync(join(OUT, "discovery.json"), JSON.stringify({ object: done.d, listed: listed }, null, 2));
  let shown = false;
  try {
    // A RELOAD, not a goto: the same hash is a same-document navigation and
    // the page would keep the view it rendered while the discovery was
    // pending (measured). The stored inventory is then searched through the
    // page's own filter form.
    await page.reload({ waitUntil: "load" });
    await page.waitForSelector("#topic-filters");
    await page.fill("#topic-q", TOPIC);
    await page.click("#topic-filters button[type=submit]");
    // innerText carries no input value, so the typed filter cannot satisfy this.
    await waitText(page, TOPIC, 60);
    shown = true;
  } catch (notShown) { shown = false; }
  await shot(page, "03-discovery-complete");
  row("console-discovery-completes-and-lists-the-run-topic",
    done.count === 1 && done.d.status.phase === "Succeeded" && apiStatuses.every((c) => c === 200) &&
      listed.includes(TOPIC) && shown,
    "one TopicDiscovery (" + done.d.metadata.name + ") started by the page ended " + done.d.status.phase +
      "; the API's inventory holds " + listed.length + " topic(s) and " +
      (listed.includes(TOPIC) ? "names" : "does NOT name") + " the run's own topic " + TOPIC +
      ", which the page " + (shown ? "shows" : "does NOT show"),
    { discovery: done.d.metadata.name, uid: done.d.metadata.uid, phase: done.d.status.phase,
      topics: listed.length, hasRunTopic: listed.includes(TOPIC), shownOnPage: shown, apiStatuses: apiStatuses });
}

// ------------------------------------------------------------ journey 2

async function staleNamespace(page, base) {
  const inA = "only-in-a-" + sfx;
  const inB = "only-in-b-" + sfx;
  for (const [ns, name] of [[NS_A, inA], [NS_B, inB]]) {
    kube(["-n", ns, "create", "-f", "-"], { input: JSON.stringify({ apiVersion: "logweir.dev/v1alpha1",
      kind: "KafkaCluster", metadata: { name: name, labels: { "logweir.dev/test-owner": OWNER } },
      spec: { bootstrapServers: ["nowhere.invalid:9092"], role: "source", auth: { mode: "plaintext", tls: false } } }) });
  }
  const posts = [];
  page.on("request", (r) => {
    if (r.method() === "POST" && r.url().includes("/connections")) posts.push(r.url());
  });

  // 2a. A's list answer is held until B has rendered.
  const timeline = { aRequested: 0, aFulfilled: 0, bRendered: 0, aStatus: 0 };
  let armed = true;
  await page.route("**/api/v1/namespaces/" + NS_A + "/connections*", async (route) => {
    if (!armed || route.request().method() !== "GET") return route.continue();
    armed = false;
    timeline.aRequested = Date.now();
    const response = await route.fetch();
    const body = await response.body();
    timeline.aStatus = response.status();
    await pause(4000);
    await route.fulfill({ status: response.status(), headers: response.headers(), body: body });
    timeline.aFulfilled = Date.now();
  });
  await page.goto(base + "#/clusters?ns=" + NS_A, { waitUntil: "load" });
  await until("A's list request to be issued", async () => timeline.aRequested > 0, 20);
  await page.evaluate((ns) => { location.hash = "#/clusters?ns=" + encodeURIComponent(ns); }, NS_B);
  await waitText(page, inB, 30);
  timeline.bRendered = Date.now();
  await until("A's held answer to be delivered", async () => timeline.aFulfilled > 0, 20);
  await pause(1500);
  const after = await text(page);
  await shot(page, "04-stale-a-after-b");
  const truth = { inA: kopt(["-n", NS_A, "get", "kafkacluster", inA]) !== null,
    inB: kopt(["-n", NS_B, "get", "kafkacluster", inB]) !== null,
    inAinB: kopt(["-n", NS_B, "get", "kafkacluster", inA]) !== null };
  row("console-slow-a-response-never-renders-over-b",
    timeline.aStatus === 200 && timeline.aFulfilled > timeline.bRendered && after.includes(inB) &&
      !after.includes(inA) && truth.inA && truth.inB && !truth.inAinB,
    "A's list answered " + timeline.aStatus + " and was delivered " +
      (timeline.aFulfilled - timeline.bRendered) + " ms AFTER B rendered; the page shows " +
      (after.includes(inB) ? "B's " + inB : "NOT B's object") + " and " +
      (after.includes(inA) ? "ALSO A's " + inA : "not A's " + inA) + "; kubectl places " + inA + " in A only and " +
      inB + " in B",
    { timeline: timeline, truth: truth });

  // 2b. A form left behind in A writes nothing; the same form, current, would.
  const left = "left-" + sfx;
  await page.unroute("**/api/v1/namespaces/" + NS_A + "/connections*");
  await page.goto(base + "#/clusters?ns=" + NS_A, { waitUntil: "load" });
  await fillCluster(page, left, "nowhere.invalid:9092", false);
  const oldForm = await page.$("#cluster-form");
  await page.evaluate((ns) => { location.hash = "#/clusters?ns=" + encodeURIComponent(ns); }, NS_B);
  await waitText(page, inB, 30);
  const before = posts.length;
  const leftBeforeA = clusters(NS_A);
  const leftBeforeB = clusters(NS_B);
  await oldForm.evaluate((f) => f.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true })));
  await pause(2500);
  const leftPosts = posts.length - before;
  const leftTruth = { inA: added(leftBeforeA, NS_A).length > 0, inB: added(leftBeforeB, NS_B).length > 0 };

  // 2c. A submit after the switch lands in B, and only in B.
  const later = "after-" + sfx;
  await fillCluster(page, later, "nowhere.invalid:9092", false);
  const beforeB = posts.length;
  const laterBeforeA = clusters(NS_A);
  const laterBeforeB = clusters(NS_B);
  await page.click("#cluster-form button[type=submit]");
  const landedAll = await until("the submitted connection to exist", async () => {
    const n = added(laterBeforeB, NS_B).concat(added(laterBeforeA, NS_A));
    return n.length ? n : null;
  }, 30);
  await pause(1500);
  const landed = landedAll[0];
  const bPosts = posts.slice(beforeB);
  const laterTruth = { inB: added(laterBeforeB, NS_B).length, inA: added(laterBeforeA, NS_A).length };
  await shot(page, "05-submit-after-switch");
  row("console-left-form-in-a-writes-nothing",
    leftPosts === 0 && !leftTruth.inA && !leftTruth.inB && bPosts.length >= 1,
    "submitting the form left behind in A issued " + leftPosts + " POST(s); a new KafkaCluster appeared in A: " +
      leftTruth.inA + ", in B: " + leftTruth.inB + ". The current form on B did POST (" + bPosts.length +
      "), so the silence is the old form's and not a page that never posts",
    { leftPosts: leftPosts, truth: leftTruth });
  row("console-submit-after-switch-lands-in-b-only",
    laterTruth.inB === 1 && laterTruth.inA === 0 && landed.metadata.namespace === NS_B && bPosts.length >= 1 &&
      bPosts.every((u) => u.includes("/namespaces/" + NS_B + "/")),
    "the submit after the switch POSTed to " + JSON.stringify(bPosts.map((u) => new URL(u).pathname)) +
      "; kubectl finds " + laterTruth.inB + " new KafkaCluster(s) in B and " + laterTruth.inA +
      " in A (" + landed.metadata.name + ", uid " + landed.metadata.uid + ")",
    { posts: bPosts.map((u) => new URL(u).pathname), truth: laterTruth, uid: landed.metadata.uid });
}

// ------------------------------------------------------------ main

async function main() {
  mkdirSync(OUT, { recursive: true });
  for (const ns of [NS_A, NS_B]) createNamespace(ns);
  for (const s of ["source-scram", "logweir-s3"]) copySecret(s, NS_A);
  kube(["-n", NS_A, "create", "-f", "-"], { input: JSON.stringify({ apiVersion: "v1", kind: "ServiceAccount",
    metadata: { name: "logweir-runner", labels: { "logweir.dev/test-owner": OWNER } },
    automountServiceAccountToken: false }) });
  const port = await freePort();
  await startApi(port);
  const base = "http://127.0.0.1:" + port + "/ui/";
  const browser = await chromium.launch();
  try {
    const context = await browser.newContext();
    for (const [name, fn] of [["registration-and-discovery", registrationAndDiscovery],
      ["stale-namespace", staleNamespace]]) {
      const page = await context.newPage();
      try {
        await fn(page, base, port);
      } catch (error) {
        result.details["journey:" + name] = { error: String(error && error.stack || error).slice(0, 3000) };
        process.stderr.write("journey " + name + " raised: " + error + "\n");
        try { await shot(page, "error-" + name); } catch (ignored) { /* page gone */ }
      } finally {
        await page.close();
      }
    }
  } finally {
    await browser.close();
  }
}

let rc = 0;
try {
  await main();
} catch (error) {
  rc = 1;
  result.failure = String(error && error.stack || error).slice(0, 3000);
  process.stderr.write("FAILED: " + result.failure + "\n");
} finally {
  if (api !== null && api.exitCode === null) api.kill("SIGTERM");
  for (const ns of [NS_A, NS_B]) {
    try { result.cleanup.push(deleteOwnedNamespace(ns)); } catch (e) { rc = 1; result.cleanup.push({ ns: ns, error: String(e) }); }
  }
  rmSync(PRIVATE, { recursive: true, force: true });
  result.finishedAt = new Date().toISOString();
  writeFileSync(join(OUT, "result.json"), JSON.stringify(result, null, 2) + "\n");
}
process.exit(rc);
