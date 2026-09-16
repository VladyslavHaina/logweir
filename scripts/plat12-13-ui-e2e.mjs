// PLAT-13.2 / PLAT-12.1 / PLAT-12.2 live UI acceptance harness.
//
// The sibling of `scripts/plat13-ui-e2e.mjs`, which owns PLAT-13.1's
// navigation-lifetime journeys. This one drives the journeys those changes are
// about: a draft that survives a refusal, one object from a double click, a
// retry after a LOST RESPONSE, the restore wizard's one guided submit and
// where it lands, the standalone approvals page, and every way a subject can
// be made to disagree with what was submitted.
//
// EVERY POSITIVE CASE IS REAL. The page is the worktree's own `ui/`, served by
// `kubectl proxy` on the loopback address; every object is created by the page
// against the real API server and read back with `kubectl`, by name and by
// UID. Two cases inject a FAULT into the transport and say so in the result:
// the double-click case DELAYS the real response (the request and its answer
// are the API server's own), and the lost-response case fetches the real
// response and then aborts it, which is what a dropped connection does to a
// create that already happened. No response body is ever fabricated.
//
// Dependencies: Node.js, kubectl, and Playwright with Chromium installed.
// Resolve Playwright without a machine-specific path, for example:
//   NODE_PATH="$(npm root -g)" node scripts/plat12-13-ui-e2e.mjs
//
// Environment (all optional):
//   UI_E2E_OWNER       the value of the logweir.dev/test-owner label this run
//                      writes and checks before it deletes anything; default
//                      ui-correct. A second worker running these journeys sets
//                      its own, so the namespace it deletes is provably its own.
//   UI_E2E_PREFIX      the namespace and object-name prefix; default
//                      lw-ui-correct-. Every namespace this harness will touch
//                      must start with it, and the assertion is made twice:
//                      before anything is created, and again before the delete.
//   UI_E2E_NAMESPACE   the namespace to create and delete; default
//                      <UI_E2E_PREFIX><utc>.
//   UI_E2E_UI_DIR      the directory to serve; default this worktree's ui/.
//   UI_E2E_PORT        the proxy port; default a free one this process picks.
//   UI_E2E_ARTIFACTS   where screenshots and the result go; default
//                      /tmp/logweir-roadmap-run/claude/artifacts/<UI_E2E_OWNER>.
//   UI_E2E_KEEP        "1" keeps the namespace (for a look around afterwards).

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = process.env.UI_E2E_KUBECTL || "kubectl";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UI_DIR = process.env.UI_E2E_UI_DIR || join(REPO, "ui");
const OWNER = process.env.UI_E2E_OWNER || "ui-correct";
const ARTIFACTS = process.env.UI_E2E_ARTIFACTS ||
  ("/tmp/logweir-roadmap-run/claude/artifacts/" + OWNER);
// The prefix is BOTH the namespace guard and the object-name tag, so every
// object this run creates is visibly one of its own in a `kubectl get -A`.
const NAMESPACE_PREFIX = process.env.UI_E2E_PREFIX || "lw-ui-correct-";
const OWNER_LABEL = "logweir.dev/test-owner=" + OWNER;
const LABELS = { "logweir.dev/test-owner": OWNER };
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z");
const namespace = process.env.UI_E2E_NAMESPACE || (NAMESPACE_PREFIX + stamp.toLowerCase());
const suffix = Math.random().toString(36).slice(2, 7);

const result = {
  harness: "scripts/plat12-13-ui-e2e.mjs",
  kubeContext: KUBE_CONTEXT,
  owner: OWNER,
  namespacePrefix: NAMESPACE_PREFIX,
  namespace: namespace,
  uiDirectory: UI_DIR,
  startedAt: new Date().toISOString(),
  journeys: [],
  faultInjection: [],
  created: [],
  screenshots: [],
  cleanup: [],
};

function check(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function errorText(error) {
  return error instanceof Error ? error.name + ": " + error.message : String(error);
}

function record(journey, detail) {
  result.journeys.push(Object.assign({ journey: journey }, detail || {}));
  process.stderr.write("== passed: " + journey + "\n");
}

function assertSafeNamespace(ns) {
  check(ns.startsWith(NAMESPACE_PREFIX), "this harness only ever touches " + NAMESPACE_PREFIX + "* namespaces, not " + ns);
  check(ns !== "default" && !ns.startsWith("kube-"), "refusing a system namespace: " + ns);
}

function kube(args, options) {
  const opts = options || {};
  const done = spawnSync(KUBECTL, ["--context", KUBE_CONTEXT].concat(args), {
    encoding: "utf8",
    input: opts.input,
    timeout: opts.timeout || 20000,
    maxBuffer: 4 * 1024 * 1024,
  });
  const expected = opts.expected || [0];
  if (!expected.includes(done.status)) {
    throw new Error(
      KUBECTL + " --context " + KUBE_CONTEXT + " " + args.join(" ") + " exited " + done.status +
        ": " + String(done.stderr || "").trim().slice(0, 1500),
    );
  }
  return done;
}

function kubeJson(args) {
  return JSON.parse(kube(args.concat(["-o", "json"])).stdout);
}

function apply(object) {
  return JSON.parse(kube(["-n", namespace, "create", "-f", "-", "-o", "json"], { input: JSON.stringify(object) }).stdout);
}

function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function freePort() {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const port = server.address().port;
      server.close(() => resolve(port));
    });
  });
}

async function shot(page, name) {
  const path = join(ARTIFACTS, "live-" + name + ".png");
  await page.screenshot({ path: path, fullPage: true });
  result.screenshots.push(path);
  return path;
}

/** Polls `read` until `want` is true of its value, or fails naming what it saw. */
async function until(label, read, want, attempts) {
  let last = null;
  for (let i = 0; i < (attempts || 30); i += 1) {
    last = await read();
    if (want(last)) {
      return last;
    }
    await pause(1000);
  }
  throw new Error(label + " never became true; last value: " + JSON.stringify(last).slice(0, 800));
}

function restoreStatus(name) {
  const out = kube(["-n", namespace, "get", "restore", name, "-o", "json"], { expected: [0, 1] });
  if (out.status !== 0) {
    return null;
  }
  return (JSON.parse(out.stdout).status) || {};
}

// --------------------------------------------------------------- fixtures

// EVERY FIXTURE BACKUP'S NAME IS LONGER THAN 63 CHARACTERS. `weirkeeper`'s
// backup reconciler refuses such a name terminally BEFORE it creates anything
// (the pod label could not carry it), so no fixture here ever produces a runner
// Job, and its status is then ours to set: phase Succeeded with a backup set
// and a covered window, which is what the restore wizard builds a plan from.
const LONG = "fixture-backup-deliberately-longer-than-sixty-three-characters-";
const backupName = NAMESPACE_PREFIX + LONG + "old-" + suffix;
const newerBackupName = NAMESPACE_PREFIX + LONG + "new-" + suffix;
const refusedBackupName = NAMESPACE_PREFIX + LONG + "bad-" + suffix;
const doomedBackupName = NAMESPACE_PREFIX + LONG + "gone-" + suffix;
const sourceCluster = NAMESPACE_PREFIX + "source-" + suffix;
const targetCluster = NAMESPACE_PREFIX + "target-" + suffix;
const archiveUrl = "s3://" + NAMESPACE_PREFIX + "fixture/" + namespace;
const scheduleName = NAMESPACE_PREFIX + "hourly-" + suffix;

/** Creates one fixture `Backup` and, when `status` is given, patches that
 *  status onto it until it sticks -- the controller may write its own terminal
 *  `NameTooLong` refusal at any moment in the next second, and a terminal
 *  status stops it looking again. Returns `{name, uid}`. */
function seedBackup(name, options) {
  const o = options || {};
  const created = apply({
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Backup",
    metadata: { name: name, namespace: namespace, labels: LABELS },
    spec: {
      archive: { url: archiveUrl },
      deadlineSeconds: 3600,
      sourceRef: { name: sourceCluster },
      topics: ["orders", "payments"],
      triggeredBy: o.slot ? "schedule" : "manual",
      scheduleRef: o.slot ? { name: scheduleName } : undefined,
      slot: o.slot,
    },
  });
  const point = { name: name, uid: created.metadata.uid };
  result.created.push(Object.assign({ kind: "Backup" }, point));
  if (!o.status) {
    return point;
  }
  for (let i = 0; i < 10; i += 1) {
    kube(["-n", namespace, "patch", "backup", name, "--subresource=status", "--type=merge",
      "-p", JSON.stringify({ status: o.status })]);
    const seen = kubeJson(["-n", namespace, "get", "backup", name]).status || {};
    if (seen.phase === o.status.phase && seen.backupId === o.status.backupId) {
      const jobs = kubeJson(["-n", namespace, "get", "jobs"]).items || [];
      check(jobs.every((j) => !j.metadata.name.startsWith(name.slice(0, 40))),
        "a fixture Backup must never produce a runner Job");
      return point;
    }
    spawnSync("sleep", ["1"]);
  }
  throw new Error("the fixture Backup " + name + " did not keep the status this run set");
}

/** A Succeeded status: a set, a covered window and a `Complete` condition. The
 *  transition time is what orders the selector, so the two points this run
 *  compares carry deliberately different ones. */
function succeededStatus(set, records, fromMs, toMs, completedAt) {
  return {
    phase: "Succeeded",
    backupId: set,
    records: records,
    exitCode: 0,
    exitReason: "ok",
    reason: "Ok",
    manifestKey: namespace + "/" + set + "/manifest.json",
    windowCovered: { fromMs: fromMs, toMs: toMs },
    conditions: [{
      type: "Complete", status: "True", reason: "Ok", message: "fixture",
      lastTransitionTime: completedAt,
    }],
  };
}

// The covered window the OLD point discloses, in the two spellings the page
// and the assertions each need.
const OLD_FROM_MS = 1760000000000;
const OLD_TO_MS = 1760000060000;
const rfc = (ms) => new Date(ms).toISOString().replace(".000Z", "Z");

const points = {};

function seedFixtures() {
  for (const [name, role] of [[sourceCluster, "source"], [targetCluster, "target"]]) {
    const created = apply({
      apiVersion: "logweir.dev/v1alpha1",
      kind: "KafkaCluster",
      metadata: { name: name, namespace: namespace, labels: LABELS },
      spec: {
        // Unreachable on purpose: a restore is never admitted against a target
        // the control plane cannot see, so no runner Job can start from here.
        bootstrapServers: ["fixture.invalid:9092"],
        auth: { mode: "plaintext", tls: false },
        role: role,
        markerTopic: "logweir.scratch",
      },
    });
    result.created.push({ kind: "KafkaCluster", name: name, uid: created.metadata.uid });
  }

  // THE POINT EVERY WIZARD JOURNEY IS BOUND TO: the OLDER of the two completed
  // runs, so "the newest one" and "the one that was chosen" are different
  // answers and a page that searched would be visible.
  points.old = seedBackup(backupName, {
    slot: "20261009-060000",
    status: succeededStatus("set-old-" + suffix, 200, OLD_FROM_MS, OLD_TO_MS,
      "2026-10-09T06:00:41Z"),
  });
  // A RUN THAT IS NOT A RECOVERY POINT, and it is the CONTROLLER that says so:
  // this one is created and then left alone, so weirkeeper writes its own
  // terminal refusal over the too-long name. Nothing about its status is this
  // harness's, which is what makes "the selector does not offer it" a
  // statement about a real non-Succeeded Backup rather than about a fixture.
  points.notAPoint = seedBackup(refusedBackupName, { slot: "20261009-080000" });
  points.notAPoint.phase = kube(["-n", namespace, "wait", "backup", refusedBackupName,
    "--for=jsonpath={.status.phase}", "--timeout=60s"], { expected: [0, 1] }).status === 0
    ? (kubeJson(["-n", namespace, "get", "backup", refusedBackupName]).status || {}).phase
    : null;
  check(points.notAPoint.phase !== null && points.notAPoint.phase !== "Succeeded",
    "the controller must have refused the too-long name, not completed it: " +
      String(points.notAPoint.phase));
  // A completed point this run DELETES later, to prove the refusal.
  points.doomed = seedBackup(doomedBackupName, {
    slot: "20261009-090000",
    status: succeededStatus("set-gone-" + suffix, 50, OLD_FROM_MS, OLD_TO_MS,
      "2026-10-09T09:00:41Z"),
  });
  result.fixtures = {
    archive: archiveUrl,
    points: points,
    oldWindow: { fromMs: OLD_FROM_MS, toMs: OLD_TO_MS, from: rfc(OLD_FROM_MS), to: rfc(OLD_TO_MS) },
  };
}

/** The NEWER completion, created mid-run: it completes AFTER every point above,
 *  so it is what "the newest Succeeded Backup" means from the moment it exists. */
function seedNewerPoint() {
  points.newer = seedBackup(newerBackupName, {
    slot: "20261009-100000",
    status: succeededStatus("set-new-" + suffix, 900, OLD_TO_MS, OLD_TO_MS + 60000,
      "2026-10-09T10:00:41Z"),
  });
  return points.newer;
}

/** The wizard's route for one point, exactly as a history row builds it. */
function pointRoute(point) {
  return "#/restore?ns=" + encodeURIComponent(namespace) +
    "&backup=" + encodeURIComponent(point.name) + "&uid=" + encodeURIComponent(point.uid);
}

// --------------------------------------------------------------- journeys

function apiPath(plural, name) {
  const base = "/apis/logweir.dev/v1alpha1/namespaces/" + namespace + "/" + plural;
  return name ? base + "/" + name : base;
}

function collectPosts(page) {
  const posts = [];
  page.on("request", (request) => {
    if (request.method() === "POST" && request.url().includes("/apis/logweir.dev/")) {
      posts.push(new URL(request.url()).pathname);
    }
  });
  return posts;
}

async function formErrorThenRetryKeepsDraft(browser, base) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  const name = NAMESPACE_PREFIX + "retry-" + suffix;
  const posts = collectPosts(page);
  try {
    await page.goto(base + "#/clusters?ns=" + namespace);
    await page.waitForSelector("#cluster-form");
    await page.fill("#cluster-name", "Not_A_Valid_Name");
    await page.fill("#cluster-servers", "broker-0.fixture.invalid:9092");
    await page.fill("#cluster-role", "target");
    await page.fill("#cluster-username", "kept-through-the-error");
    await page.click("#cluster-form button[type=submit]");
    await page.waitForSelector("#cluster-name[aria-invalid=\"true\"]");
    check(posts.length === 0, "a refused draft must not reach the API server: " + posts.join(", "));
    const kept = {
      name: await page.inputValue("#cluster-name"),
      servers: await page.inputValue("#cluster-servers"),
      role: await page.inputValue("#cluster-role"),
      username: await page.inputValue("#cluster-username"),
    };
    check(kept.name === "Not_A_Valid_Name" && kept.servers === "broker-0.fixture.invalid:9092" &&
      kept.role === "target" && kept.username === "kept-through-the-error",
      "the refusal discarded input: " + JSON.stringify(kept));
    await shot(page, "cluster-invalid-keeps-draft");

    // The one field the page refused is fixed; everything else is still typed.
    await page.fill("#cluster-name", name);
    await page.click("#cluster-form button[type=submit]");
    await page.waitForSelector(".mutation-succeeded");
    // `--show-managed-fields`: kubectl hides them by default, and the field
    // manager is how this run proves the PAGE wrote the object.
    const stored = kubeJson(["-n", namespace, "get", "kafkacluster", name, "--show-managed-fields"]);
    result.created.push({ kind: "KafkaCluster", name: name, uid: stored.metadata.uid });
    check(stored.spec.bootstrapServers[0] === "broker-0.fixture.invalid:9092", "the retained servers were sent");
    check(stored.spec.role === "target", "the retained role was sent");
    check(stored.spec.auth.username === "kept-through-the-error", "the retained username was sent");
    check((stored.metadata.managedFields || []).some((f) => f.manager === "logweir-ui"),
      "the object was written by the page, with its field manager");
    await shot(page, "cluster-retry-created");
    record("form error then retry keeps the draft", {
      postsAfterTheWholeJourney: posts.length,
      object: { kind: "KafkaCluster", name: name, uid: stored.metadata.uid },
      fieldManagers: (stored.metadata.managedFields || []).map((f) => f.manager),
    });
  } finally {
    await page.close();
  }
}

async function doubleClickCreatesOneObject(browser, base) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  const name = NAMESPACE_PREFIX + "double-" + suffix;
  const posts = collectPosts(page);
  try {
    await page.goto(base + "#/clusters?ns=" + namespace);
    await page.waitForSelector("#cluster-form");
    await page.fill("#cluster-name", name);
    await page.fill("#cluster-servers", "fixture.invalid:9092");
    // The REAL response, held back so both clicks land while it is in flight.
    await page.route("**" + apiPath("kafkaclusters") + "?fieldManager=logweir-ui", async (route) => {
      if (route.request().method() !== "POST") {
        await route.continue();
        return;
      }
      const response = await route.fetch();
      const body = await response.body();
      await pause(900);
      await route.fulfill({ status: response.status(), headers: response.headers(), body: body });
    });
    const button = page.locator("#cluster-form button[type=submit]");
    await button.click();
    await button.click({ force: true }).catch(() => {});
    await page.evaluate(() => {
      const form = document.querySelector("#cluster-form");
      form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });
    await page.waitForSelector(".mutation-succeeded", { timeout: 15000 });
    await pause(500);
    const stored = kubeJson(["-n", namespace, "get", "kafkaclusters", "-l", "", "--field-selector", "metadata.name=" + name]);
    check(stored.items.length === 1, "a double click made " + stored.items.length + " objects");
    result.created.push({ kind: "KafkaCluster", name: name, uid: stored.items[0].metadata.uid });
    check(posts.length === 1, "a double click sent " + posts.length + " POSTs: " + posts.join(", "));
    await shot(page, "cluster-double-click");
    record("double click creates exactly one object", {
      posts: posts.length,
      objects: stored.items.map((o) => ({ name: o.metadata.name, uid: o.metadata.uid })),
    });
    result.faultInjection.push({
      control: "the page's own POST response was DELAYED by 900 ms (the API server's own response, fulfilled late)",
      why: "both clicks must land while the first request is in flight",
    });
  } finally {
    await page.close();
  }
}

async function lostResponseRetryResolvesToTheSameObject(browser, base) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  const name = NAMESPACE_PREFIX + "lost-" + suffix;
  const posts = collectPosts(page);
  try {
    await page.goto(base + "#/clusters?ns=" + namespace);
    await page.waitForSelector("#cluster-form");
    await page.fill("#cluster-name", name);
    await page.fill("#cluster-servers", "fixture.invalid:9092");
    await page.fill("#cluster-secret", NAMESPACE_PREFIX + "archive");
    let dropped = false;
    await page.route("**" + apiPath("kafkaclusters") + "?fieldManager=logweir-ui", async (route) => {
      if (route.request().method() !== "POST" || dropped) {
        await route.continue();
        return;
      }
      // The request reaches the API server and the object IS created; the
      // answer never reaches the page. That is a lost response.
      const response = await route.fetch();
      check(response.status() === 201, "the dropped response was not a 201: " + response.status());
      dropped = true;
      await route.abort("failed");
    });
    await page.click("#cluster-form button[type=submit]");
    await page.waitForSelector(".mutation-unknown");
    const createdByLost = kubeJson(["-n", namespace, "get", "kafkacluster", name]);
    result.created.push({ kind: "KafkaCluster", name: name, uid: createdByLost.metadata.uid });
    const unknown = await page.locator(".mutation-unknown").innerText();
    check(/outcome is unknown|whether KafkaCluster .* was created is unknown/.test(unknown),
      "the page did not report an unknown outcome: " + unknown);
    check(await page.inputValue("#cluster-name") === name, "the draft was discarded after a lost response");
    check(await page.inputValue("#cluster-secret") === NAMESPACE_PREFIX + "archive", "the draft was discarded after a lost response");
    await shot(page, "cluster-lost-response");

    await page.click("#cluster-form button[type=submit]");
    await page.waitForSelector(".mutation-succeeded");
    const resolved = await page.locator(".mutation-succeeded").innerText();
    check(resolved.includes("already existed with exactly this content"),
      "the retry did not resolve to the existing object: " + resolved);
    check(resolved.includes(createdByLost.metadata.uid), "the resolved object's UID is not the lost request's: " + resolved);
    const after = kubeJson(["-n", namespace, "get", "kafkaclusters", "--field-selector", "metadata.name=" + name]);
    check(after.items.length === 1, "the retry created a second object");
    check(after.items[0].metadata.uid === createdByLost.metadata.uid, "the retry replaced the object");
    check(posts.length === 2, "expected the lost POST and one retry, saw " + posts.length);
    await shot(page, "cluster-lost-response-retried");
    record("lost response, retried, resolves to the same object", {
      posts: posts.length,
      uid: createdByLost.metadata.uid,
      objectsAfterRetry: after.items.length,
      pageSaid: resolved.replace(/\s+/g, " ").slice(0, 300),
    });
    result.faultInjection.push({
      control: "the page's POST response was fetched from the API server (201) and then ABORTED",
      why: "a create that happened and an answer that never arrived is exactly what a retry has to survive",
    });
  } finally {
    await page.close();
  }
}

/** PLAT-13.1's two properties, re-checked against these changes: an accepted
 *  create is never cancelled or re-rendered by the route it left, and a form
 *  whose route is gone sends nothing at all. */
async function navigationNeitherCancelsNorLeaks(browser, base) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  const name = NAMESPACE_PREFIX + "durable-" + suffix;
  const posts = collectPosts(page);
  try {
    await page.goto(base + "#/clusters?ns=" + namespace);
    await page.waitForSelector("#cluster-form");
    await page.fill("#cluster-name", name);
    await page.fill("#cluster-servers", "fixture.invalid:9092");
    await page.evaluate(() => {
      window.__uiCorrectOldForm = document.querySelector("#cluster-form");
    });
    let backendStatus = 0;
    await page.route("**" + apiPath("kafkaclusters") + "?fieldManager=logweir-ui", async (route) => {
      if (route.request().method() !== "POST") {
        await route.continue();
        return;
      }
      const response = await route.fetch();
      const body = await response.body();
      backendStatus = response.status();
      await pause(700);
      await route.fulfill({ status: response.status(), headers: response.headers(), body: body });
    });
    const sent = page.waitForRequest((request) =>
      request.method() === "POST" && request.url().includes(apiPath("kafkaclusters")));
    await page.click("#cluster-form button[type=submit]");
    await sent;
    await page.evaluate((ns) => { location.hash = "#/backups?ns=" + encodeURIComponent(ns); }, namespace);
    await page.waitForSelector("#view-slot h2");
    await pause(1500);
    check(backendStatus === 201, "the API server did not accept the create: " + backendStatus);
    const heading = await page.locator("#view-slot h2").first().innerText();
    check(heading === "Backups", "the answer to a left route re-rendered over the new one: " + heading);
    const stored = kubeJson(["-n", namespace, "get", "kafkacluster", name]);
    result.created.push({ kind: "KafkaCluster", name: name, uid: stored.metadata.uid });

    const before = posts.length;
    await page.evaluate(() => {
      window.__uiCorrectOldForm.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });
    await pause(500);
    check(posts.length === before, "a form whose route is gone issued a request: " + posts.join(", "));
    record("navigation neither cancels an accepted create nor lets a left form write", {
      backendStatus: backendStatus,
      object: { name: name, uid: stored.metadata.uid },
      postsAfterDisposal: posts.length - before,
      routeAfterNavigation: heading,
    });
  } finally {
    await page.close();
  }
}

async function restoreSubmitRoutesToAwaitingApproval(browser, base) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1200 } });
  const posts = collectPosts(page);
  try {
    await page.goto(base + pointRoute(points.old));
    await page.waitForSelector("#create-restore");
    check((await page.locator("#point-uid").innerText()).trim() === points.old.uid,
      "the wizard is bound to the point the route names");
    const planHash = (await page.locator("#plan-hash-value").innerText()).trim();
    const restoreName = "restore-" + planHash.replace("sha256:", "").slice(0, 8);
    check(await page.locator("#request-approval").count() === 0, "the second, independently navigating button is gone");
    await shot(page, "restore-plan-step");
    await page.click("#create-restore");
    await page.waitForFunction(() => location.hash.startsWith("#/approvals?subject="), null, { timeout: 20000 });
    const hash = await page.evaluate(() => location.hash);
    check(hash.includes("subject=" + restoreName), "the guided submit routed to " + hash);
    await page.waitForSelector("#approval-form");
    const stored = kubeJson(["-n", namespace, "get", "restore", restoreName, "--show-managed-fields"]);
    result.created.push({ kind: "Restore", name: restoreName, uid: stored.metadata.uid });
    check(stored.spec.approvalRef.name === "approval-" + planHash.replace("sha256:", "").slice(0, 8),
      "the Restore names the minted Approval");
    check((stored.metadata.managedFields || []).some((f) => f.manager === "logweir-ui"), "the page created it");
    const shown = {
      subject: await page.inputValue("#subject-name"),
      uid: await page.inputValue("#subject-uid"),
      hash: await page.inputValue("#plan-hash"),
      approval: await page.inputValue("#approval-name"),
    };
    check(shown.subject === restoreName && shown.uid === stored.metadata.uid &&
      shown.hash === planHash && shown.approval === stored.spec.approvalRef.name,
      "the Awaiting approval page shows another subject than the cluster holds: " + JSON.stringify(shown));
    await shot(page, "restore-awaiting-approval");

    // weirkeeper holds it, and says so on the object.
    const held = await until(
      "the Restore reports ApprovalNotVerified",
      async () => restoreStatus(restoreName),
      (status) => status && status.phase === "Pending" && status.reason === "ApprovalNotVerified",
      40,
    );

    // A refresh of the wizard re-derives the same plan and creates nothing new.
    await page.goto(base + pointRoute(points.old));
    await page.waitForSelector("#create-restore");
    check((await page.locator("#plan-hash-value").innerText()).trim() === planHash,
      "the same point rebuilt the same plan after a reload");
    await page.click("#create-restore");
    await page.waitForFunction(() => location.hash.startsWith("#/approvals?subject="), null, { timeout: 20000 });
    const restores = kubeJson(["-n", namespace, "get", "restores"]);
    check(restores.items.filter((r) => r.metadata.name === restoreName).length === 1, "a second Restore was created");
    check(restores.items.find((r) => r.metadata.name === restoreName).metadata.uid === stored.metadata.uid,
      "the resubmitted plan replaced the Restore");
    record("restore submission routes to Awaiting approval, and a resubmission creates nothing", {
      restore: { name: restoreName, uid: stored.metadata.uid, planHash: planHash },
      route: hash,
      status: { phase: held.phase, reason: held.reason },
      posts: posts.length,
      restoresInNamespace: restores.items.length,
    });
    return { restoreName: restoreName, planHash: planHash, uid: stored.metadata.uid, approvalName: stored.spec.approvalRef.name };
  } finally {
    await page.close();
  }
}

async function standaloneApprovalsPage(browser, base, subject) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  try {
    await page.goto(base + "#/approvals?ns=" + namespace);
    await page.waitForSelector("#awaiting-approval");
    check(await page.locator("#approval-form").count() === 0,
      "a standalone visit offered a form without a chosen subject");
    const link = page.locator("#awaiting-approval a", { hasText: subject.restoreName });
    check(await link.count() === 1, "the waiting Restore was not offered for selection");
    await shot(page, "approvals-standalone");
    await link.click();
    await page.waitForSelector("#approval-form");
    check((await page.inputValue("#subject-name")) === subject.restoreName, "the selection opened another subject");
    record("standalone approvals page offers selection and no assumed subject", {
      chosen: subject.restoreName,
    });
  } finally {
    await page.close();
  }
}

async function forgedAndMismatchedSubjects(browser, base, subject) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  const posts = collectPosts(page);
  try {
    // 1. The displayed subject, edited in the page the way devtools would.
    await page.goto(base + "#/approvals?ns=" + namespace + "&subject=" + subject.restoreName);
    await page.waitForSelector("#approval-form");
    await page.evaluate(() => {
      const input = document.querySelector("#subject-name");
      input.removeAttribute("readonly");
      input.value = "restore-somebody-elses-plan";
    });
    await page.fill("#approval-json", "{\"plan_hash\": \"sha256:0\"}");
    await page.fill("#approval-sig", "{\"signatures\": []}");
    await page.click("#approval-form button[type=submit]");
    await page.waitForSelector(".mutation-failed");
    const refusal = await page.locator(".mutation-failed").innerText();
    check(refusal.includes("Nothing was sent"), "the forged subject was not refused: " + refusal);
    check(posts.length === 0, "the forged subject reached the API server: " + posts.join(", "));
    check(kubeJson(["-n", namespace, "get", "approvals"]).items.length === 0, "an Approval was created from a forged subject");
    await shot(page, "approval-forged-subject-refused");

    // 2. A link that names another plan hash for the same Restore.
    await page.goto(base + "#/approvals?ns=" + namespace + "&subject=" + subject.restoreName +
      "&hash=" + encodeURIComponent("sha256:" + "0".repeat(64)) + "&name=" + subject.approvalName);
    await page.waitForSelector(".refusal-block");
    check(await page.locator("#approval-form").count() === 0, "a mismatched link still offered a form");
    const mismatch = await page.locator(".refusal-block").innerText();
    check(mismatch.includes("This link does not match"), mismatch);
    await shot(page, "approval-route-mismatch");
    check(posts.length === 0, "a mismatched route wrote something");
    record("edited subject and mismatched route are refused, and nothing is sent", {
      posts: posts.length,
      refusal: refusal.replace(/\s+/g, " ").slice(0, 240),
      routeRefusal: mismatch.replace(/\s+/g, " ").slice(0, 240),
    });
  } finally {
    await page.close();
  }
}

async function privateKeyIsRefusedAndCleared(browser, base, subject) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  const posts = collectPosts(page);
  try {
    await page.goto(base + "#/approvals?ns=" + namespace + "&subject=" + subject.restoreName);
    await page.waitForSelector("#approval-form");
    await page.fill("#approval-json", "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEH\n-----END PRIVATE KEY-----\n");
    await page.fill("#approval-sig", "{\"signatures\": []}");
    await page.click("#approval-form button[type=submit]");
    await page.waitForSelector(".mutation-failed");
    const said = await page.locator(".mutation-failed").innerText();
    check(said.includes("this page never accepts a private key"), said);
    check(posts.length === 0, "key material reached the API server");
    check(await page.inputValue("#approval-json") === "", "the key text was left in the field");
    check(await page.inputValue("#approval-sig") === "{\"signatures\": []}", "the other document was discarded");
    const bodyText = await page.locator("body").innerText();
    check(!bodyText.includes("MIGHAgEAMBMGByqGSM49"), "the key material was echoed into the page");
    await shot(page, "approval-private-key-refused");
    record("a pasted private key is refused, cleared and never sent", { posts: posts.length });
  } finally {
    await page.close();
  }
}

async function approvalRecordedThenRefusedByTheController(browser, base, subject) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  try {
    await page.goto(base + "#/approvals?ns=" + namespace + "&subject=" + subject.restoreName);
    await page.waitForSelector("#approval-form");
    // A well-formed but unsigned pair: weirkeeper must refuse it, and the page
    // must show its refusal rather than claim anything about it.
    await page.fill("#approval-json", "{\"approver\": \"ui-correct\", \"ticket\": \"UI-1\", \"plan_hash\": \"" +
      subject.planHash + "\", \"subject_kind\": \"Restore\"}");
    await page.fill("#approval-sig", "{\"payloadType\": \"application/vnd.logweir.drill-approval+json;version=1.0.0\", \"signatures\": [{\"keyid\": \"not-on-the-roster\", \"sig\": \"AA==\"}]}");
    await page.click("#approval-form button[type=submit]");
    // A recorded Approval takes the name, so the page re-mounts WITHOUT a form:
    // its state panel is the acknowledgement, and the object is the proof.
    await until(
      "the Approval exists in the cluster",
      () => kube(["-n", namespace, "get", "approval", subject.approvalName, "--ignore-not-found=true", "-o", "name"]).stdout.trim(),
      (name) => name.length > 0,
      20,
    );
    await page.waitForSelector(".approval-state", { timeout: 15000 });
    const stored = kubeJson(["-n", namespace, "get", "approval", subject.approvalName, "--show-managed-fields"]);
    check((stored.metadata.managedFields || []).some((f) => f.manager === "logweir-ui"),
      "the Approval was not written by the page");
    result.created.push({ kind: "Approval", name: subject.approvalName, uid: stored.metadata.uid });
    check(stored.spec.subjectRef.name === subject.restoreName, "the recorded subject is not the Restore");
    check(stored.spec.planHash === subject.planHash, "the recorded plan hash is not the Restore's own");
    const verdict = await until(
      "weirkeeper decides the approval",
      () => (kubeJson(["-n", namespace, "get", "approval", subject.approvalName]).status || {}),
      (status) => status && status.verified === false,
      40,
    );
    const reason = (verdict.conditions || []).find((c) => c.type === "Verified");
    check(reason !== undefined, "weirkeeper wrote no Verified condition: " + JSON.stringify(verdict));
    // A RELOAD, not a goto: the page is already on this hash, and a goto to the
    // same URL is not a navigation. This is the operator pressing refresh.
    await page.reload({ waitUntil: "load" });
    await page.waitForSelector(".approval-state");
    const state = await page.locator(".approval-state").innerText();
    check(/refused by weirkeeper|expired/.test(state), "the page did not show the refusal: " + state);
    check(state.includes(reason.reason), "the page did not name the controller's reason: " + state);
    check(await page.locator("#approval-form").count() === 0,
      "a second form was offered under a name an Approval already holds");
    await shot(page, "approval-refused-state");

    // And the durable operation view says the same thing about the same object.
    await page.goto(base + "#/history?ns=" + namespace + "&name=" + subject.restoreName);
    await page.waitForSelector("#restore-operation");
    const operation = await page.locator("#restore-operation").innerText();
    check(operation.includes(subject.planHash), "the operation view shows another plan hash");
    check(operation.includes(reason.reason), "the operation view does not carry the approval's reason: " + operation);
    await shot(page, "restore-operation-view");
    record("an unsigned approval is recorded, refused by weirkeeper, and shown as refused", {
      approval: { name: subject.approvalName, uid: stored.metadata.uid },
      controllerReason: reason.reason,
      pageState: state.replace(/\s+/g, " ").slice(0, 240),
    });
  } finally {
    await page.close();
  }
}

async function serverRefusesAForgedSubjectBinding(browser, base) {
  // A direct CR writer -- not this page -- creating an Approval that names a
  // DIFFERENT subject than the Restore that points at it. The page can no
  // longer produce this; the controller must refuse it anyway.
  const restoreName = NAMESPACE_PREFIX + "forged-" + suffix;
  const approvalName = NAMESPACE_PREFIX + "forged-approval-" + suffix;
  const restore = apply({
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Restore",
    metadata: { name: restoreName, namespace: namespace, labels: LABELS },
    spec: {
      planBytes: "name: \"forged\"\n",
      approvalRef: { name: approvalName },
      sourceArchive: { url: archiveUrl },
      backupSetRef: "set-" + suffix,
      pointInTime: "2026-09-15T12:00:00Z",
      target: { clusterRef: { name: targetCluster }, mode: "newTopic", topicNaming: { prefix: "forged-" } },
      deadlineSeconds: 3600,
    },
  });
  result.created.push({ kind: "Restore", name: restoreName, uid: restore.metadata.uid });
  const approval = apply({
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Approval",
    metadata: { name: approvalName, namespace: namespace, labels: LABELS },
    spec: {
      subjectRef: { kind: "Restore", name: NAMESPACE_PREFIX + "somebody-else-" + suffix },
      planHash: "sha256:" + "0".repeat(64),
      approvalBytes: "{\"plan_hash\": \"sha256:" + "0".repeat(64) + "\", \"subject_kind\": \"Restore\"}",
      sidecarBytes: "{\"payloadType\": \"application/vnd.logweir.drill-approval+json;version=1.0.0\", \"signatures\": []}",
    },
  });
  result.created.push({ kind: "Approval", name: approvalName, uid: approval.metadata.uid });
  const refused = await until(
    "weirkeeper refuses the forged subject binding (admit() checks the subject mismatch BEFORE " +
      "verification and calls it terminal, so a Restore held at ApprovalNotVerified here is a " +
      "controller that does not carry that ordering -- reproducible with kubectl alone, with " +
      "no page involved)",
    () => restoreStatus(restoreName),
    (status) => status && status.phase === "Failed" && status.reason === "ApprovalSubjectMismatch",
    60,
  );
  const jobs = kubeJson(["-n", namespace, "get", "jobs"]).items || [];
  check(!jobs.some((j) => j.metadata.name === restoreName), "a Job was created for a mismatched approval");

  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  try {
    await page.goto(base + "#/history?ns=" + namespace + "&name=" + restoreName);
    await page.waitForSelector("#restore-operation");
    const text = await page.locator("#view-slot").innerText();
    check(text.includes("ApprovalSubjectMismatch"), "the page does not show the controller's refusal: " + text.slice(0, 400));
    check(text.includes("bound to another subject"), "the page does not say the approval names another subject");
    await shot(page, "restore-approval-subject-mismatch");
  } finally {
    await page.close();
  }
  record("a forged subject binding is refused by the controller and shown as such", {
    restore: { name: restoreName, uid: restore.metadata.uid },
    approval: { name: approvalName, uid: approval.metadata.uid, subjectRef: approval.spec.subjectRef },
    status: { phase: refused.phase, reason: refused.reason },
    jobsForThatRestore: 0,
  });
}

/** PLAT-11.1: a visit with no identity offers a real, searchable selector and
 *  no plan at all -- and every row carries the identity a click will send. */
async function selectorOffersEveryPointAndNoPlan(browser, base) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1200 } });
  const posts = collectPosts(page);
  try {
    await page.goto(base + "#/restore?ns=" + namespace);
    await page.waitForSelector("#step-select-point");
    check(await page.locator("#create-restore").count() === 0,
      "a visit with no chosen point offered a submit");
    check(await page.locator("#plan-bytes").count() === 0,
      "…and rendered a plan for a point nobody chose");

    const offered = await page.locator("tr[data-point]").evaluateAll(
      (rows) => rows.map((r) => r.getAttribute("data-point")));
    check(offered.includes(points.old.uid), "the completed point is offered: " + offered.join(", "));
    check(offered.includes(points.doomed.uid), "…and so is the other one");
    check(!offered.includes(points.notAPoint.uid),
      "a Backup the controller refused (phase " + points.notAPoint.phase + ") is not a recovery " +
      "point and must not be offered");
    check(await page.locator("a[href*=\"uid=" + points.old.uid + "\"]").count() >= 1,
      "and the row links to that point BY UID");

    // The disclosed coverage is on the row, in RFC 3339 and not as an integer.
    // The section holds two tables -- the points on offer, and the catalog of
    // everything the namespace holds. This is the first.
    const table = await page.locator("#step-select-point table").first().innerText();
    check(table.includes(rfc(OLD_FROM_MS)) && table.includes(rfc(OLD_TO_MS)),
      "the row discloses the covered window: " + table.slice(0, 400));
    check(table.includes("manifest recorded"), "and what is known about its archive");
    await shot(page, "restore-point-selector");

    // THE SEARCH FILTERS IN PLACE: the input keeps its caret and the rows it
    // hides are hidden, not re-rendered.
    await page.fill("#point-search", "20261009-060000");
    await page.waitForFunction(
      (uid) => {
        const rows = Array.from(document.querySelectorAll("tr[data-point]"));
        const shown = rows.filter((r) => r.style.display !== "none");
        return shown.length === 1 && shown[0].getAttribute("data-point") === uid;
      },
      points.old.uid,
      { timeout: 5000 },
    );
    check(await page.evaluate(() => document.activeElement.id) === "point-search",
      "the search kept focus, so a filter can be typed into");
    await shot(page, "restore-point-selector-search");

    await page.fill("#point-search", "nothing-matches-this");
    await page.waitForFunction(() => {
      const rows = Array.from(document.querySelectorAll("tr[data-point]"));
      const empty = document.querySelector("#no-match");
      return rows.every((r) => r.style.display === "none") && empty !== null && !empty.hidden;
    }, null, { timeout: 5000 });

    check(posts.length === 0, "the selector wrote something: " + posts.join(", "));
    record("a visit with no recovery point offers a searchable selector and no plan", {
      offered: offered,
      notOffered: { refusedRun: points.notAPoint },
      posts: posts.length,
    });
  } finally {
    await page.close();
  }
}

/** PLAT-11.1: the link on a history row opens the wizard ON that point. */
async function historyRowPreselectsThatPoint(browser, base) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1200 } });
  try {
    await page.goto(base + "#/history?ns=" + namespace);
    await page.waitForSelector("#view-slot table");
    const link = page.locator("a[href*=\"uid=" + points.old.uid + "\"]");
    check(await link.count() === 1, "the completed Backup's row carries one Restore-this-point link");
    check((await link.innerText()).trim() === "Restore this point", await link.innerText());
    check(await page.locator("a[href*=\"uid=" + points.notAPoint.uid + "\"]").count() === 0,
      "and a Backup the controller refused (phase " + points.notAPoint.phase + ") carries none");
    await shot(page, "history-restore-this-point");

    await link.click();
    await page.waitForSelector("#create-restore");
    const hash = await page.evaluate(() => location.hash);
    check(hash.includes("uid=" + points.old.uid), "the click carried the uid: " + hash);
    check(hash.includes("backup=" + points.old.name), "…and the name beside it: " + hash);
    const uid = (await page.locator("#point-uid").innerText()).trim();
    const name = (await page.locator("#point-name").innerText()).trim();
    check(uid === points.old.uid && name === points.old.name,
      "the wizard bound to another point than the row named: " + name + " / " + uid);
    const plan = await page.locator("#plan-bytes").innerText();
    check(plan.includes("backup: \"set-old-" + suffix + "\""),
      "and the plan names THAT point's backup set: " + plan.slice(0, 400));
    check(plan.includes("window_start: \"" + rfc(OLD_FROM_MS) + "\""),
      "…and its covered window");
    await shot(page, "restore-preselected-from-history");
    record("a link from a history row pre-selects that point", {
      point: points.old,
      route: hash,
      planNamesTheSet: "set-old-" + suffix,
    });
    return (await page.locator("#plan-hash-value").innerText()).trim();
  } finally {
    await page.close();
  }
}

/** PLAT-11.1's acceptance sentence: an older selected backup remains selected
 *  throughout review, with a newer one completing underneath it. */
async function olderPointStaysSelectedWhenANewerArrives(browser, base, reviewedHash) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1200 } });
  try {
    await page.goto(base + pointRoute(points.old));
    await page.waitForSelector("#create-restore");
    const before = (await page.locator("#plan-hash-value").innerText()).trim();
    check(before === reviewedHash, "the same point rebuilt the same plan: " + before);

    // A NEWER RUN COMPLETES while the wizard is open. It completes after every
    // other point, so from now on it IS "the newest Succeeded Backup".
    const newer = seedNewerPoint();
    const listed = kubeJson(["-n", namespace, "get", "backups"]).items
      .filter((b) => (b.status || {}).phase === "Succeeded")
      .map((b) => ({ name: b.metadata.name, at: (b.status.conditions || [])[0].lastTransitionTime }))
      .sort((a, b) => (a.at < b.at ? 1 : -1));
    check(listed[0].name === newer.name,
      "the new run is the newest completion in the cluster (control): " + JSON.stringify(listed));

    // TWO RE-READS, both of which see the new run. First a route change and
    // back -- the page's module state survives it, so this is the wizard
    // deciding again rather than starting again...
    await page.evaluate((ns) => { location.hash = "#/history?ns=" + encodeURIComponent(ns); }, namespace);
    await page.waitForSelector("#view-slot table");
    await page.evaluate((route) => { location.hash = route; }, pointRoute(points.old));
    await page.waitForSelector("#create-restore");
    const afterNavigation = {
      uid: (await page.locator("#point-uid").innerText()).trim(),
      hash: (await page.locator("#plan-hash-value").innerText()).trim(),
    };
    check(afterNavigation.uid === points.old.uid,
      "a route change moved the selection to " + afterNavigation.uid);
    check(afterNavigation.hash === before,
      "a route change changed the plan: " + before + " -> " + afterNavigation.hash);

    // ...and then a RELOAD, which starts the page afresh and has only the
    // address to go on. That is what makes the identity durable.
    await page.reload({ waitUntil: "load" });
    await page.waitForSelector("#create-restore");
    const uid = (await page.locator("#point-uid").innerText()).trim();
    check(uid === points.old.uid, "the newer backup moved the selection to " + uid);
    const after = (await page.locator("#plan-hash-value").innerText()).trim();
    check(after === before, "the plan under review changed: " + before + " -> " + after);
    const plan = await page.locator("#plan-bytes").innerText();
    check(plan.includes("backup: \"set-old-" + suffix + "\""), "and it still names the old set");
    check(!plan.includes("set-new-" + suffix), "and never the new one: " + plan.slice(0, 400));

    // AND THE PAGE DID SEE THE NEW RUN: it is in the catalog table beside the
    // chosen point, which is what makes this a re-read and not a stale render.
    const step = await page.locator("#step-backup-set").innerText();
    check(step.includes(newer.name),
      "the re-read did not list the run that completed since: " + step.slice(0, 600));
    await shot(page, "restore-older-point-stays-selected");
    record("an older point stays selected when a newer Backup completes mid-wizard", {
      chosen: points.old,
      arrivedMeanwhile: newer,
      planHashBefore: before,
      planHashAfterRouteChange: afterNavigation.hash,
      planHashAfterReload: after,
      newestCompletionInCluster: listed[0].name,
    });
  } finally {
    await page.close();
  }
}

/** PLAT-11.1: a selected point that is gone is a refusal naming it, with no
 *  plan and nothing to submit -- and never a substitution. */
async function missingPointIsRefused(browser, base) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  const posts = collectPosts(page);
  try {
    // It resolves first, so the refusal afterwards is about this object going
    // away and not about a link that never worked.
    await page.goto(base + pointRoute(points.doomed));
    await page.waitForSelector("#create-restore");
    check((await page.locator("#point-uid").innerText()).trim() === points.doomed.uid,
      "the doomed point resolves while it exists (control)");

    kube(["-n", namespace, "delete", "backup", points.doomed.name, "--wait=true"], { timeout: 60000 });
    check(kube(["-n", namespace, "get", "backup", points.doomed.name, "--ignore-not-found=true",
      "-o", "name"]).stdout.trim() === "", "the fixture Backup is gone");

    await page.reload({ waitUntil: "load" });
    await page.waitForSelector("#point-refusal");
    const said = await page.locator("#point-refusal").innerText();
    check(said.includes(points.doomed.uid), "the refusal does not name the uid: " + said);
    check(said.includes(points.doomed.name), "…nor the name: " + said);
    check(await page.locator("#create-restore").count() === 0, "a refused point offered a submit");
    check(await page.locator("#plan-bytes").count() === 0, "…and rendered a plan");
    check(!said.includes(points.old.uid) && !said.includes("set-old-" + suffix),
      "and nothing was substituted for it: " + said);
    await shot(page, "restore-missing-point-refused");

    // A uid that never existed at all is the same refusal.
    await page.goto(base + "#/restore?ns=" + namespace + "&backup=" + points.old.name +
      "&uid=00000000-0000-4000-8000-000000000000");
    await page.waitForSelector("#point-refusal");
    const invented = await page.locator("#point-refusal").innerText();
    check(invented.includes("00000000-0000-4000-8000-000000000000"), invented);
    check(invented.includes("A DIFFERENT object now answers to that name"),
      "the page does not say the name is held by another object: " + invented);
    check(await page.locator("#create-restore").count() === 0, "still nothing to submit");
    check(posts.length === 0, "a refused point wrote something: " + posts.join(", "));
    record("a selected point that is gone is refused, with no plan and no substitution", {
      deleted: points.doomed,
      posts: posts.length,
      refusal: said.replace(/\s+/g, " ").slice(0, 240),
    });
  } finally {
    await page.close();
  }
}

/** D2 section 13.2 row W13a / defect UI-HTTPDOWNGRADE: path-style addressing
 *  never enables plaintext transport, and the proof is the bytes the API
 *  server ends up holding. */
async function pathStyleDoesNotEnableHttp(browser, base) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1200 } });
  try {
    await page.goto(base + pointRoute(points.old));
    await page.waitForSelector("#create-restore");
    const flag = "allow_" + "http";
    const planText = () => page.locator("#plan-bytes").innerText();

    check(await page.isChecked("#store-allow-insecure") === false,
      "the insecure-transport box does not default on");
    check((await planText()).includes(flag + ": false"), "and the plan starts with it clear");

    // TICKING PATH-STYLE CHANGES ADDRESSING AND NOTHING ELSE.
    await page.check("#store-pathStyle");
    await page.waitForFunction(
      () => document.querySelector("#plan-bytes").innerText.includes("path_style: true"),
      null, { timeout: 5000 });
    const withPathStyle = await planText();
    check(!withPathStyle.includes(flag + ": true"),
      "PATH-STYLE ENABLED PLAINTEXT TRANSPORT: " + withPathStyle.slice(0, 600));
    check(await page.isChecked("#store-allow-insecure") === false,
      "and it did not tick the other box either");
    check((withPathStyle.match(new RegExp(flag + ": false", "g")) || []).length === 2,
      "both storage blocks still refuse plaintext transport");
    await shot(page, "restore-path-style-without-http");

    // ONLY THE EXPLICIT BOX SETS IT.
    await page.check("#store-allow-insecure");
    await page.waitForFunction(
      (f) => document.querySelector("#plan-bytes").innerText.includes(f + ": true"),
      flag, { timeout: 5000 });
    await shot(page, "restore-explicit-insecure-http");

    // AND WHAT REACHES THE API SERVER IS THOSE BYTES. Untick it again and
    // submit: the stored Restore must carry path-style addressing and refuse
    // plaintext transport.
    await page.uncheck("#store-allow-insecure");
    await page.waitForFunction(
      (f) => document.querySelector("#plan-bytes").innerText.includes(f + ": false"),
      flag, { timeout: 5000 });
    const shown = await planText();
    const planHash = (await page.locator("#plan-hash-value").innerText()).trim();
    const restoreName = "restore-" + planHash.replace("sha256:", "").slice(0, 8);
    await page.click("#create-restore");
    await page.waitForFunction(() => location.hash.startsWith("#/approvals?subject="), null,
      { timeout: 20000 });
    const stored = kubeJson(["-n", namespace, "get", "restore", restoreName]);
    result.created.push({ kind: "Restore", name: restoreName, uid: stored.metadata.uid });
    check(stored.spec.planBytes === shown, "the bytes on screen are not the bytes stored");
    check(stored.spec.planBytes.includes("path_style: true"), "path-style addressing was sent");
    check(!stored.spec.planBytes.includes(flag + ": true"),
      "THE STORED PLAN ALLOWS PLAINTEXT TRANSPORT: " + stored.spec.planBytes.slice(0, 600));
    record("path-style addressing never enables plaintext transport", {
      restore: { name: restoreName, uid: stored.metadata.uid, planHash: planHash },
      storedFlags: {
        path_style: true,
        insecureTransport: stored.spec.planBytes.includes(flag + ": true"),
      },
    });
  } finally {
    await page.close();
  }
}

// ------------------------------------------------------------------ driver

let proxy = null;
let browser = null;
let failure = null;
assertSafeNamespace(namespace);
mkdirSync(ARTIFACTS, { recursive: true });

try {
  const existing = kube(["get", "namespace", namespace, "--ignore-not-found=true", "-o", "name"]).stdout.trim();
  check(existing === "", "namespace " + namespace + " already exists; this harness creates its own");
  kube(["create", "namespace", namespace]);
  kube(["label", "namespace", namespace, OWNER_LABEL]);
  result.namespaceUid = kubeJson(["get", "namespace", namespace]).metadata.uid;
  seedFixtures();

  const port = Number(process.env.UI_E2E_PORT || (await freePort()));
  const url = "http://127.0.0.1:" + String(port) + "/ui/";
  result.baseUrl = url;
  proxy = spawn(KUBECTL, [
    "--context", KUBE_CONTEXT, "proxy",
    "--www=" + UI_DIR, "--www-prefix=/ui/", "--address=127.0.0.1", "--port", String(port),
  ], { stdio: ["ignore", "pipe", "pipe"] });
  proxy.stdout.on("data", (d) => process.stderr.write("proxy: " + d));
  proxy.stderr.on("data", (d) => process.stderr.write("proxy: " + d));
  await until("the proxy serves the page", async () => {
    try {
      const answer = await fetch(url, { method: "GET" });
      return answer.status;
    } catch (unreachable) {
      return 0;
    }
  }, (status) => status === 200, 30);

  browser = await chromium.launch({ headless: true });
  await formErrorThenRetryKeepsDraft(browser, url);
  await doubleClickCreatesOneObject(browser, url);
  await lostResponseRetryResolvesToTheSameObject(browser, url);
  await navigationNeitherCancelsNorLeaks(browser, url);
  await selectorOffersEveryPointAndNoPlan(browser, url);
  const reviewedHash = await historyRowPreselectsThatPoint(browser, url);
  await olderPointStaysSelectedWhenANewerArrives(browser, url, reviewedHash);
  await pathStyleDoesNotEnableHttp(browser, url);
  const subject = await restoreSubmitRoutesToAwaitingApproval(browser, url);
  await standaloneApprovalsPage(browser, url, subject);
  await forgedAndMismatchedSubjects(browser, url, subject);
  await privateKeyIsRefusedAndCleared(browser, url, subject);
  await approvalRecordedThenRefusedByTheController(browser, url, subject);
  // Before the forged-binding journey, because it deletes a fixture the
  // selector journey counted and nothing after it reads that fixture.
  await missingPointIsRefused(browser, url);
  await serverRefusesAForgedSubjectBinding(browser, url);
} catch (error) {
  failure = error;
  result.failure = errorText(error);
} finally {
  if (browser !== null) {
    try {
      await browser.close();
    } catch (error) {
      failure = failure || error;
      result.browserCloseFailure = errorText(error);
    }
  }
  if (proxy !== null) {
    proxy.kill("SIGTERM");
  }
  try {
    if (process.env.UI_E2E_KEEP === "1") {
      result.cleanup.push({ namespace: namespace, state: "kept on request (UI_E2E_KEEP=1)" });
    } else {
      assertSafeNamespace(namespace);
      const owner = kubeJson(["get", "namespace", namespace]).metadata;
      check(owner.labels["logweir.dev/test-owner"] === OWNER, "refusing to delete a namespace this harness does not own");
      check(owner.uid === result.namespaceUid, "the namespace's UID changed under this harness");
      kube(["delete", "namespace", namespace, "--wait=false"], { timeout: 60000 });
      result.cleanup.push({ namespace: namespace, uid: owner.uid, state: "delete requested; every object in it is owned by this run" });
    }
  } catch (error) {
    failure = failure || error;
    result.cleanupFailure = errorText(error);
  }
  result.finishedAt = new Date().toISOString();
  result.ok = failure === null;
  const path = join(ARTIFACTS, "live-result-" + stamp + ".json");
  writeFileSync(path, JSON.stringify(result, null, 2) + "\n");
  process.stdout.write(JSON.stringify(result, null, 2) + "\n");
  process.stderr.write((result.ok ? "PASSED" : "FAILED") + ": " + path + "\n");
  if (failure !== null) {
    process.exitCode = 1;
  }
}
