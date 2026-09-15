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
//   UI_E2E_NAMESPACE   the namespace to create and delete; default
//                      lw-ui-correct-<utc>. It must start with lw-ui-correct-.
//   UI_E2E_UI_DIR      the directory to serve; default this worktree's ui/.
//   UI_E2E_PORT        the proxy port; default a free one this process picks.
//   UI_E2E_ARTIFACTS   where screenshots and the result go; default
//                      /tmp/logweir-roadmap-run/claude/artifacts/ui-correct.
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
const ARTIFACTS = process.env.UI_E2E_ARTIFACTS || "/tmp/logweir-roadmap-run/claude/artifacts/ui-correct";
const NAMESPACE_PREFIX = "lw-ui-correct-";
const OWNER_LABEL = "logweir.dev/test-owner=ui-correct";
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z");
const namespace = process.env.UI_E2E_NAMESPACE || (NAMESPACE_PREFIX + stamp.toLowerCase());
const suffix = Math.random().toString(36).slice(2, 7);

const result = {
  harness: "scripts/plat12-13-ui-e2e.mjs",
  kubeContext: KUBE_CONTEXT,
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

// A Backup name LONGER THAN 63 CHARACTERS. `weirkeeper`'s backup reconciler
// refuses such a name terminally BEFORE it creates anything (the pod label
// could not carry it), so this fixture never produces a runner Job, and its
// status is then ours to set: phase Succeeded with a backup set and a covered
// window, which is what the restore wizard builds a plan from.
const backupName = "lw-ui-correct-fixture-backup-deliberately-longer-than-sixty-three-chars-" + suffix;
const sourceCluster = "lw-ui-correct-source-" + suffix;
const targetCluster = "lw-ui-correct-target-" + suffix;
const archiveUrl = "s3://lw-ui-correct-fixture/" + namespace;

function seedFixtures() {
  for (const [name, role] of [[sourceCluster, "source"], [targetCluster, "target"]]) {
    const created = apply({
      apiVersion: "logweir.dev/v1alpha1",
      kind: "KafkaCluster",
      metadata: { name: name, namespace: namespace, labels: { "logweir.dev/test-owner": "ui-correct" } },
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
  const backup = apply({
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Backup",
    metadata: { name: backupName, namespace: namespace, labels: { "logweir.dev/test-owner": "ui-correct" } },
    spec: {
      archive: { url: archiveUrl },
      deadlineSeconds: 3600,
      sourceRef: { name: sourceCluster },
      topics: ["orders", "payments"],
      triggeredBy: "manual",
    },
  });
  result.created.push({ kind: "Backup", name: backupName, uid: backup.metadata.uid });

  // The controller may write its own terminal refusal (NameTooLong) at any
  // moment in the next second; a terminal status stops it looking again, so
  // the patch is repeated until it sticks.
  const wanted = {
    status: {
      phase: "Succeeded",
      backupId: "set-" + suffix,
      records: 200,
      exitCode: 0,
      exitReason: "ok",
      reason: "Ok",
      windowCovered: { fromMs: 1760000000000, toMs: 1760000060000 },
      conditions: [{
        type: "Complete", status: "True", reason: "Ok", message: "fixture",
        lastTransitionTime: "2026-09-15T12:00:00Z",
      }],
    },
  };
  for (let i = 0; i < 10; i += 1) {
    kube(["-n", namespace, "patch", "backup", backupName, "--subresource=status", "--type=merge", "-p", JSON.stringify(wanted)]);
    const seen = kubeJson(["-n", namespace, "get", "backup", backupName]).status || {};
    if (seen.phase === "Succeeded" && seen.backupId === wanted.status.backupId) {
      const jobs = kubeJson(["-n", namespace, "get", "jobs"]).items || [];
      check(jobs.every((j) => !j.metadata.name.startsWith(backupName.slice(0, 40))),
        "the fixture Backup must never produce a runner Job");
      return;
    }
    spawnSync("sleep", ["1"]);
  }
  throw new Error("the fixture Backup's status did not stay Succeeded");
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
  const name = "lw-ui-correct-retry-" + suffix;
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
  const name = "lw-ui-correct-double-" + suffix;
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
  const name = "lw-ui-correct-lost-" + suffix;
  const posts = collectPosts(page);
  try {
    await page.goto(base + "#/clusters?ns=" + namespace);
    await page.waitForSelector("#cluster-form");
    await page.fill("#cluster-name", name);
    await page.fill("#cluster-servers", "fixture.invalid:9092");
    await page.fill("#cluster-secret", "lw-ui-correct-archive");
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
    check(await page.inputValue("#cluster-secret") === "lw-ui-correct-archive", "the draft was discarded after a lost response");
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
  const name = "lw-ui-correct-durable-" + suffix;
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
    await page.goto(base + "#/restore?ns=" + namespace);
    await page.waitForSelector("#create-restore");
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
    await page.goto(base + "#/restore?ns=" + namespace);
    await page.waitForSelector("#create-restore");
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
  const restoreName = "lw-ui-correct-forged-" + suffix;
  const approvalName = "lw-ui-correct-forged-approval-" + suffix;
  const restore = apply({
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Restore",
    metadata: { name: restoreName, namespace: namespace, labels: { "logweir.dev/test-owner": "ui-correct" } },
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
    metadata: { name: approvalName, namespace: namespace, labels: { "logweir.dev/test-owner": "ui-correct" } },
    spec: {
      subjectRef: { kind: "Restore", name: "lw-ui-correct-somebody-else-" + suffix },
      planHash: "sha256:" + "0".repeat(64),
      approvalBytes: "{\"plan_hash\": \"sha256:" + "0".repeat(64) + "\", \"subject_kind\": \"Restore\"}",
      sidecarBytes: "{\"payloadType\": \"application/vnd.logweir.drill-approval+json;version=1.0.0\", \"signatures\": []}",
    },
  });
  result.created.push({ kind: "Approval", name: approvalName, uid: approval.metadata.uid });
  const refused = await until(
    "weirkeeper refuses the forged subject binding",
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
  const subject = await restoreSubmitRoutesToAwaitingApproval(browser, url);
  await standaloneApprovalsPage(browser, url, subject);
  await forgedAndMismatchedSubjects(browser, url, subject);
  await privateKeyIsRefusedAndCleared(browser, url, subject);
  await approvalRecordedThenRefusedByTheController(browser, url, subject);
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
      check(owner.labels["logweir.dev/test-owner"] === "ui-correct", "refusing to delete a namespace this harness does not own");
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
