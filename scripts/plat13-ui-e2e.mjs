// PLAT-13.1 live UI/proxy acceptance harness.
//
// Positive cases always use real Kubernetes objects and the deployed proxy.
// The deliberately mocked responses are labeled `fault injection` in the
// result and prove that assertions reject stale content and a non-2xx create.
// Test-owned CRs are seeded here and removed in `finally`.
//
// Dependencies: Node.js, kubectl, and Playwright with Chromium installed.
// Resolve Playwright without a machine-specific path, for example:
//   NODE_PATH="$(npm root -g)" node scripts/plat13-ui-e2e.mjs
//
// Required environment:
//   PLAT13_BASE_URL=http://127.0.0.1:PORT/ui/
//   PLAT13_PRIMARY_NAMESPACE=plat13-ui-e2e
//   PLAT13_UI_SERVICE_ACCOUNT=plat13-ui-e2e/plat13-ui-e2e-ui
// Optional multi-namespace stage:
//   PLAT13_STAGE=multi
//   PLAT13_SECOND_NAMESPACE=plat13-ui-e2e-second
//   PLAT13_MISSING_NAMESPACE=plat13-ui-e2e-missing

import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = process.env.PLAT13_KUBECTL || "kubectl";
const cleanupNegativeControl = process.env.PLAT13_CLEANUP_NEGATIVE_CONTROL === "forbidden";
const base = required("PLAT13_BASE_URL").replace(/\/?$/, "/");
const primary = required("PLAT13_PRIMARY_NAMESPACE");
const serviceAccount = required("PLAT13_UI_SERVICE_ACCOUNT");
const second = process.env.PLAT13_SECOND_NAMESPACE || "";
const missing = process.env.PLAT13_MISSING_NAMESPACE || "";
const stage = process.env.PLAT13_STAGE || "single";
const suffix = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
const runId = `plat13-${suffix}`;
const fixtureA = `plat13-a-${suffix}`;
const fixtureB = second ? `plat13-b-${suffix}` : "";
const targetName = `plat13-target-${suffix}`;
const durableName = `plat13-post-${suffix}`;
const rejectedName = `plat13-reject-${suffix}`;
const owned = [];
const result = {
  source: {
    harness: "scripts/plat13-ui-e2e.mjs",
    kubeContext: KUBE_CONTEXT,
    cleanupNegativeControl,
  },
  base,
  stage,
  namespaces: { primary, second, missing },
  runId,
  created: [],
  positiveRealApiCases: [],
  faultInjectionControls: [],
  rbacProof: [],
  cleanup: [],
};

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function check(condition, message) {
  if (!condition) throw new Error(message);
}

function errorText(error) {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

function emit(event) {
  process.stderr.write(`${JSON.stringify({ plat13: event })}\n`);
}

function apiPath(namespace, plural, name = "") {
  const collection = `/apis/logweir.dev/v1alpha1/namespaces/${namespace}/${plural}`;
  return name ? `${collection}/${name}` : collection;
}

function apiUrl(path) {
  return new URL(path.replace(/^\//, ""), new URL(base).origin + "/").href;
}

function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((yes, no) => {
    resolve = yes;
    reject = no;
  });
  promise.catch(() => {});
  return { promise, resolve, reject, settled: false };
}

async function bounded(promise, ms, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function kube(args, options = {}) {
  const completed = spawnSync(KUBECTL, ["--context", KUBE_CONTEXT, ...args], {
    encoding: "utf8",
    input: options.input,
    timeout: options.timeout || 15_000,
    maxBuffer: 1024 * 1024,
  });
  const expected = options.expected || [0];
  if (!expected.includes(completed.status)) {
    const stderr = (completed.stderr || "").trim().slice(0, 1200);
    throw new Error(
      `${KUBECTL} --context ${KUBE_CONTEXT} ${args.join(" ")} exited ${completed.status}: ${stderr}`,
    );
  }
  return completed;
}

function assertSafeNamespace(namespace) {
  check(namespace !== "default", "the PLAT-13 harness refuses to operate in default");
  check(!namespace.startsWith("kube-"), `the PLAT-13 harness refuses system namespace ${namespace}`);
  check(namespace === primary || namespace === second,
    `namespace ${namespace} is outside this harness's explicit primary/second scope`);
}

function own(kind, plural, namespace, name, ownership) {
  assertSafeNamespace(namespace);
  check(name.startsWith("plat13-"), `refusing to track non-PLAT-13 object ${kind}/${name}`);
  const target = { kind, plural, namespace, name, uid: "", ownership };
  owned.push(target);
  emit({ phase: "planned-object", kind, namespace, name });
  return target;
}

function recordCreated(target, object) {
  check(object?.metadata?.name === target.name, `created ${target.kind} returned the wrong name`);
  check(object?.metadata?.namespace === target.namespace,
    `created ${target.kind}/${target.name} returned the wrong namespace`);
  check(typeof object?.metadata?.uid === "string" && object.metadata.uid.length > 0,
    `created ${target.kind}/${target.name} returned no UID`);
  target.uid = object.metadata.uid;
  const identity = {
    kind: target.kind,
    namespace: target.namespace,
    name: target.name,
    uid: target.uid,
  };
  result.created.push(identity);
  emit({ phase: "created-object", ...identity });
  return object;
}

function seedObject(target, object) {
  const completed = kube(["-n", target.namespace, "create", "-f", "-", "-o", "json"], {
    input: JSON.stringify(object),
  });
  return recordCreated(target, JSON.parse(completed.stdout));
}

function backupObject(namespace, name, sourceName) {
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Backup",
    metadata: {
      namespace,
      name,
      labels: { "plat13.logweir.dev/run": runId },
    },
    spec: {
      archive: { url: `s3://plat13-fixture/${namespace}/${name}` },
      deadlineSeconds: 3600,
      sourceRef: { name: sourceName },
      topics: [`topic-${name}`],
      triggeredBy: "manual",
    },
  };
}

function seedFixtures() {
  const target = own("KafkaCluster", "kafkaclusters", primary, targetName, "run-label");
  seedObject(target, {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "KafkaCluster",
    metadata: {
      namespace: primary,
      name: targetName,
      labels: { "plat13.logweir.dev/run": runId },
    },
    spec: {
      bootstrapServers: ["fixture.invalid:9092"],
      auth: { mode: "plaintext", tls: false },
      role: "target",
      markerTopic: "logweir.scratch",
    },
  });

  const backupA = own("Backup", "backups", primary, fixtureA, "run-label");
  seedObject(backupA, backupObject(primary, fixtureA, targetName));
  const patched = kube([
    "-n", primary, "patch", "backup", fixtureA,
    "--subresource=status", "--type=merge",
    "-p", JSON.stringify({
      status: {
        phase: "Succeeded",
        backupId: `set-${fixtureA}`,
        windowCovered: { fromMs: 1760000000000, toMs: 1760000060000 },
      },
    }),
    "-o", "json",
  ]);
  const patchedObject = JSON.parse(patched.stdout);
  check(patchedObject.metadata.uid === backupA.uid, "status patch changed the primary fixture identity");

  if (second) {
    const backupB = own("Backup", "backups", second, fixtureB, "run-label");
    seedObject(backupB, backupObject(second, fixtureB, `plat13-unused-${suffix}`));
  }
}

function isKubernetesApiRequest(url) {
  const path = new URL(url).pathname;
  return path.startsWith("/apis/") || path.startsWith("/api/");
}

function collectApiRequests(page) {
  const requests = [];
  page.on("request", (request) => {
    if (isKubernetesApiRequest(request.url())) {
      requests.push({ method: request.method(), path: new URL(request.url()).pathname });
    }
  });
  return requests;
}

function assertNoNamespaceList(requests, label) {
  const found = requests.filter((request) => request.path === "/api/v1/namespaces");
  check(found.length === 0, `${label} listed core Namespaces: ${JSON.stringify(found)}`);
}

function assertListIdentity(collection, expectedName, expectedUid, absentName, label) {
  const items = Array.isArray(collection?.items) ? collection.items : [];
  const expected = items.find((item) => item?.metadata?.name === expectedName);
  check(expected, `${label} did not contain Backup ${expectedName}`);
  check(expected?.metadata?.uid === expectedUid,
    `${label} returned the wrong UID for Backup ${expectedName}`);
  if (absentName) {
    check(!items.some((item) => item?.metadata?.name === absentName),
      `${label} contained stale Backup ${absentName}`);
  }
}

async function assertRenderedBackup(page, expectedName, absentName, label) {
  const links = await page.locator("#view-slot a").allTextContents();
  check(links.includes(expectedName), `${label} did not render Backup ${expectedName}: ${links.join(", ")}`);
  if (absentName) {
    check(!links.includes(absentName), `${label} rendered stale Backup ${absentName}: ${links.join(", ")}`);
  }
}

async function browserRead(page, path) {
  return page.evaluate(async (requestPath) => {
    const response = await fetch(requestPath, { method: "GET", cache: "no-store" });
    const text = await response.text();
    let body = null;
    try { body = text ? JSON.parse(text) : null; } catch { body = text; }
    return { status: response.status, ok: response.ok, body };
  }, path);
}

function createdIdentity(name) {
  const identity = result.created.find((item) => item.name === name);
  check(identity, `missing recorded identity for ${name}`);
  return identity;
}

async function oneNamespace(browser) {
  const page = await browser.newPage();
  const requests = collectApiRequests(page);
  try {
    await page.goto(`${base}#/clusters`);
    await page.waitForSelector("#cluster-form");
    check(await page.locator("#ns-input").inputValue() === primary,
      "single-namespace runtime context did not auto-select its sole namespace");
    check(requests.some((request) => request.path === apiPath(primary, "kafkaclusters")),
      "single-namespace page did not read its selected namespace through the live proxy");

    const oldForm = await page.locator("#cluster-form").evaluate((form) => {
      window.__plat13OldForm = form;
      return true;
    });
    check(oldForm, "could not retain the old form for listener disposal check");
    await page.evaluate((ns) => { location.hash = `#/backups?ns=${encodeURIComponent(ns)}`; }, primary);
    await page.waitForSelector("#view-slot h2");
    await assertRenderedBackup(page, fixtureA, "", "single-namespace real Backup list");
    const before = requests.length;
    await page.evaluate(() => {
      window.__plat13OldForm.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });
    await pause(250);
    check(requests.length === before, "disposed route listener issued an API request after navigation");
    assertNoNamespaceList(requests, "single-namespace browser");
    result.positiveRealApiCases.push("single namespace auto-select, real Backup identity, and listener disposal");
  } finally {
    await page.close();
  }
}

async function multipleNamespaces(browser) {
  if (!second) return;
  const page = await browser.newPage();
  const requests = collectApiRequests(page);
  try {
    await page.goto(`${base}#/backups`);
    await page.waitForSelector("#ns-input");
    check(await page.locator("#ns-input").evaluate((node) => node.tagName) === "SELECT",
      "multi-namespace context did not render a constrained selector");
    const choices = ["Choose a namespace", primary, second];
    if (missing) choices.push(missing);
    check((await page.locator("#ns-input option").allTextContents()).join("|") === choices.join("|"),
      "namespace selector differs from the Helm-configured namespaces");
    await pause(100);
    check(requests.length === 0, `unselected multi-namespace route made an API request: ${JSON.stringify(requests)}`);

    const primaryHandled = deferred();
    await page.route(`**${apiPath(primary, "backups")}`, async (route) => {
      try {
        const response = await route.fetch();
        const body = await response.body();
        const json = JSON.parse(body.toString("utf8"));
        assertListIdentity(json, fixtureA, createdIdentity(fixtureA).uid, fixtureB,
          "delayed real primary response");
        await pause(500);
        await route.fulfill({ status: response.status(), headers: response.headers(), body });
        primaryHandled.settled = true;
        primaryHandled.resolve({ status: response.status() });
      } catch (error) {
        primaryHandled.settled = true;
        primaryHandled.reject(error);
        throw error;
      }
    });

    const primaryRequest = page.waitForRequest((request) =>
      request.method() === "GET" && new URL(request.url()).pathname === apiPath(primary, "backups"));
    await page.evaluate((ns) => { location.hash = `#/backups?ns=${encodeURIComponent(ns)}`; }, primary);
    await primaryRequest;
    const secondResponsePromise = page.waitForResponse((response) =>
      response.request().method() === "GET" &&
      new URL(response.url()).pathname === apiPath(second, "backups"));
    await page.evaluate((ns) => { location.hash = `#/backups?ns=${encodeURIComponent(ns)}`; }, second);
    const secondResponse = await secondResponsePromise;
    check(secondResponse.status() === 200, `real second namespace Backup GET returned ${secondResponse.status()}`);
    const secondBody = await secondResponse.json();
    assertListIdentity(secondBody, fixtureB, createdIdentity(fixtureB).uid, fixtureA,
      "real second response");
    await bounded(primaryHandled.promise, 3000, "delayed primary route fulfillment");
    await assertRenderedBackup(page, fixtureB, fixtureA, "post-switch DOM");
    assertNoNamespaceList(requests, "multi-namespace browser");
    result.positiveRealApiCases.push(
      "delayed real namespace A GET left namespace B's distinctive Backup name/UID rendered and A absent",
    );
  } finally {
    await page.close();
  }
}

async function staleContentNegativeControl(browser) {
  if (!second) return;
  const page = await browser.newPage();
  try {
    await page.goto(base);
    const primaryResponse = await page.request.get(apiUrl(apiPath(primary, "backups")));
    check(primaryResponse.status() === 200, `fault setup primary GET returned ${primaryResponse.status()}`);
    const body = await primaryResponse.body();
    assertListIdentity(JSON.parse(body.toString("utf8")), fixtureA, createdIdentity(fixtureA).uid, fixtureB,
      "fault setup primary response");
    await page.route(`**${apiPath(second, "backups")}`, async (route) => {
      await route.fulfill({ status: 200, headers: { "content-type": "application/json" }, body });
    });
    await page.goto(`${base}#/backups?ns=${encodeURIComponent(second)}`);
    await page.waitForSelector("#view-slot h2");
    let rejected = "";
    try {
      await assertRenderedBackup(page, fixtureB, fixtureA, "fault-injected stale namespace A content");
    } catch (error) {
      rejected = errorText(error);
    }
    check(rejected.includes(fixtureB) || rejected.includes(fixtureA),
      `stale-content assertion did not reject injected namespace A body: ${rejected}`);
    result.faultInjectionControls.push({
      control: "stale namespace A list body fulfilled for namespace B",
      outcome: "rejected by distinctive B-present/A-absent DOM assertion",
      diagnostic: rejected,
    });
  } finally {
    await page.close();
  }
}

function assertSuccessfulPost(record, expectedName) {
  check(record.backendStatus === 201, `backend POST expected 201, got ${record.backendStatus}`);
  check(record.originalStatus === 201, `original page POST expected 201, got ${record.originalStatus}`);
  check(record.finishedError === null, `original page POST did not finish cleanly: ${record.finishedError}`);
  check(record.requestFailure === null, `original page POST was aborted: ${JSON.stringify(record.requestFailure)}`);
  for (const [label, body] of [["backend", record.backendBody], ["original", record.originalBody]]) {
    check(body?.kind === "KafkaCluster", `${label} POST body kind was not KafkaCluster`);
    check(body?.metadata?.name === expectedName, `${label} POST body returned wrong name`);
    check(typeof body?.metadata?.uid === "string" && body.metadata.uid.length > 0,
      `${label} POST body returned no UID`);
  }
  check(record.backendBody.metadata.uid === record.originalBody.metadata.uid,
    "backend and original page POST bodies returned different UIDs");
}

async function durableClusterPost(browser) {
  const target = own("KafkaCluster", "kafkaclusters", primary, durableName, "ui-post");
  const page = await browser.newPage();
  const backendFetched = deferred();
  const routeHandled = deferred();
  const record = {};
  try {
    await page.goto(`${base}#/clusters?ns=${encodeURIComponent(primary)}`);
    await page.waitForSelector("#cluster-form");
    await page.locator("#cluster-name").fill(durableName);
    await page.locator("#cluster-servers").fill("fixture.invalid:9092");
    await page.route(`**${apiPath(primary, "kafkaclusters")}?fieldManager=logweir-ui`, async (route) => {
      if (route.request().method() !== "POST") {
        await route.continue();
        return;
      }
      try {
        const response = await route.fetch();
        const body = await response.body();
        record.backendStatus = response.status();
        record.backendBody = JSON.parse(body.toString("utf8"));
        if (record.backendStatus === 201 && record.backendBody?.metadata?.name === durableName) {
          recordCreated(target, record.backendBody);
        }
        backendFetched.settled = true;
        backendFetched.resolve();
        await pause(350);
        await route.fulfill({ status: response.status(), headers: response.headers(), body });
        routeHandled.settled = true;
        routeHandled.resolve();
      } catch (error) {
        if (!backendFetched.settled) {
          backendFetched.settled = true;
          backendFetched.reject(error);
        }
        routeHandled.settled = true;
        routeHandled.reject(error);
        throw error;
      }
    });
    const requestPromise = page.waitForRequest((request) =>
      request.method() === "POST" && request.url().includes(apiPath(primary, "kafkaclusters")));
    await page.locator("#cluster-form button[type=submit]").click();
    const postRequest = await requestPromise;
    await bounded(backendFetched.promise, 5000, "live backend POST");
    await page.evaluate((ns) => { location.hash = `#/backups?ns=${encodeURIComponent(ns)}`; }, primary);
    await page.waitForSelector("#view-slot h2");
    await bounded(routeHandled.promise, 5000, "original POST fulfillment after navigation");
    const originalResponse = await postRequest.response();
    check(originalResponse !== null, "original page POST produced no response");
    record.originalStatus = originalResponse.status();
    record.finishedError = errorText(await originalResponse.finished());
    if (record.finishedError === "null") record.finishedError = null;
    record.requestFailure = postRequest.failure();
    record.originalBody = await originalResponse.json();
    assertSuccessfulPost(record, durableName);
    await assertRenderedBackup(page, fixtureA, "", "post-navigation route content");

    const readBack = await browserRead(page, apiPath(primary, "kafkaclusters", durableName));
    check(readBack.status === 200, `independent live read-back returned ${readBack.status}`);
    check(readBack.body?.metadata?.name === durableName, "independent read-back returned wrong name");
    check(readBack.body?.metadata?.uid === record.originalBody.metadata.uid,
      "independent read-back returned wrong UID");
    result.positiveRealApiCases.push({
      case: "delayed live POST finished after navigation and persisted",
      backendResponseStatus: record.backendStatus,
      originalResponseStatus: record.originalStatus,
      originalResponseFinished: record.finishedError === null,
      originalRequestFailure: record.requestFailure,
      returnedBodyIdentity: {
        kind: record.originalBody.kind,
        name: record.originalBody.metadata.name,
        uid: record.originalBody.metadata.uid,
      },
      readBackStatus: readBack.status,
      readBackIdentity: {
        name: readBack.body.metadata.name,
        uid: readBack.body.metadata.uid,
      },
    });
  } finally {
    await page.close();
  }
}

async function non2xxPostNegativeControl(browser) {
  const page = await browser.newPage();
  try {
    await page.goto(`${base}#/clusters?ns=${encodeURIComponent(primary)}`);
    await page.waitForSelector("#cluster-form");
    await page.locator("#cluster-name").fill(rejectedName);
    await page.locator("#cluster-servers").fill("fixture.invalid:9092");
    await page.route(`**${apiPath(primary, "kafkaclusters")}?fieldManager=logweir-ui`, async (route) => {
      if (route.request().method() !== "POST") {
        await route.continue();
        return;
      }
      await route.fulfill({
        status: 403,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "v1", kind: "Status", status: "Failure", reason: "Forbidden",
          message: "PLAT-13 controlled non-2xx POST fault", code: 403,
        }),
      });
    });
    const responsePromise = page.waitForResponse((response) =>
      response.request().method() === "POST" && response.url().includes(apiPath(primary, "kafkaclusters")));
    await page.locator("#cluster-form button[type=submit]").click();
    const response = await responsePromise;
    const body = await response.json();
    let rejected = "";
    try {
      assertSuccessfulPost({
        backendStatus: response.status(), originalStatus: response.status(),
        backendBody: body, originalBody: body,
        finishedError: errorText(await response.finished()),
        requestFailure: response.request().failure(),
      }, rejectedName);
    } catch (error) {
      rejected = errorText(error);
    }
    check(rejected.includes("expected 201"), `non-2xx assertion did not reject injected 403: ${rejected}`);
    await page.waitForSelector("#view-slot .error");
    check((await page.locator("#view-slot .error").innerText()).includes("403"),
      "UI did not render the injected 403 as an error");
    const readBack = await browserRead(page, apiPath(primary, "kafkaclusters", rejectedName));
    check(readBack.status === 404, `fault-injected rejected object unexpectedly persisted: ${readBack.status}`);
    result.faultInjectionControls.push({
      control: "KafkaCluster POST fulfilled with controlled Kubernetes 403 Status",
      outcome: "rejected by 201/body assertion; independent real GET remained 404",
      diagnostic: rejected,
    });
  } finally {
    await page.close();
  }
}

async function staleRestorePreparation(browser) {
  const page = await browser.newPage();
  try {
    await page.addInitScript(() => {
      const digest = crypto.subtle.digest.bind(crypto.subtle);
      crypto.subtle.digest = (...args) => new Promise((resolve, reject) => {
        setTimeout(() => digest(...args).then(resolve, reject), 300);
      });
    });
    const posts = [];
    page.on("request", (request) => {
      if (request.method() === "POST" && request.url().includes("/restores")) posts.push(request.url());
    });
    // THE WIZARD IS ENTERED ON A RECOVERY POINT (PLAT-11.1). `#/restore?ns=`
    // alone is the point SELECTOR and renders no plan and no submit: the page
    // no longer picks a Backup for the operator. `fixtureA` is patched to
    // Succeeded with a backupId and a covered window above, so it IS one, and
    // its uid is the identity the route resolves by.
    await page.goto(
      `${base}#/restore?ns=${encodeURIComponent(primary)}` +
        `&backup=${encodeURIComponent(fixtureA)}` +
        `&uid=${encodeURIComponent(createdIdentity(fixtureA).uid)}`,
    );
    await page.waitForSelector("#create-restore");
    await page.locator("#create-restore").click();
    await page.evaluate((ns) => { location.hash = `#/backups?ns=${encodeURIComponent(ns)}`; }, second || primary);
    await pause(750);
    check(posts.length === 0, `stale restore preparation posted after navigation: ${posts.join(", ")}`);
    await assertRenderedBackup(page, second ? fixtureB : fixtureA, second ? fixtureA : "",
      "post-restore-navigation route content");
    result.positiveRealApiCases.push(
      "real restore fixtures with delayed digest produced no stale Restore POST and kept destination content",
    );
  } finally {
    await page.close();
  }
}

function canI(args, expectedAllowed, label) {
  const [saNamespace, saName, extra] = serviceAccount.split("/");
  check(saNamespace && saName && !extra, "PLAT13_UI_SERVICE_ACCOUNT must be namespace/name");
  const completed = kube([
    "auth", "can-i", `--as=system:serviceaccount:${saNamespace}:${saName}`,
    ...args, "--quiet",
  ], { expected: [0, 1] });
  const allowed = completed.status === 0;
  check(allowed === expectedAllowed,
    `${label}: expected kubectl auth can-i=${expectedAllowed}, exit=${completed.status}`);
  result.rbacProof.push({ label, allowed, exitCode: completed.status });
}

async function denialAndCoreDetectorProof(browser) {
  const page = await browser.newPage();
  try {
    const requests = collectApiRequests(page);
    await page.goto(base);
    const unbound = await browserRead(page, apiPath("plat13-unbound", "kafkaclusters"));
    check(unbound.status === 403, `unbound namespace expected live 403, got ${unbound.status}`);
    const core = await browserRead(page, "/api/v1/namespaces");
    check(core.status === 403, `core Namespace list expected proxy/API 403, got ${core.status}`);
    check(requests.some((request) => request.path === "/api/v1/namespaces"),
      "core /api/v1/namespaces request was not collected");
    result.faultInjectionControls.push({
      control: "explicit forbidden core /api/v1/namespaces browser request",
      outcome: "collector observed /api/ path and live proxy denied it with 403",
    });
    result.positiveRealApiCases.push("unbound namespace direct live proxy request returned 403");
  } finally {
    await page.close();
  }

  canI(["list", "backups.logweir.dev", "-n", primary], true,
    "UI ServiceAccount may list Backups in primary namespace");
  if (second) {
    canI(["list", "backups.logweir.dev", "-n", second], true,
      "UI ServiceAccount may list Backups in configured second namespace");
  }
  canI(["list", "backups.logweir.dev", "-n", "plat13-unbound"], false,
    "UI ServiceAccount may not list Backups in unbound namespace");
  canI(["list", "namespaces"], false, "UI ServiceAccount may not list core Namespaces");
}

async function missingNamespaceCase(browser) {
  if (!missing) return;
  const page = await browser.newPage();
  try {
    await page.goto(`${base}#/backups?ns=${encodeURIComponent(missing)}`);
    await page.waitForSelector("#view-slot .error");
    const text = await page.locator("#view-slot").innerText();
    check(text.includes("403") || text.includes("404"),
      `configured unavailable namespace did not render live API error: ${text}`);
    result.positiveRealApiCases.push(
      `configured deleted namespace rendered live ${text.includes("404") ? "404" : "403"}`,
    );
  } finally {
    await page.close();
  }
}

function ownershipMatches(target, object) {
  if (target.uid) return object?.metadata?.uid === target.uid;
  if (target.ownership === "run-label") {
    return object?.metadata?.labels?.["plat13.logweir.dev/run"] === runId;
  }
  if (target.ownership === "ui-post") {
    const managers = Array.isArray(object?.metadata?.managedFields)
      ? object.metadata.managedFields.map((field) => field.manager)
      : [];
    return managers.includes("logweir-ui") && object?.metadata?.name === durableName &&
      object?.spec?.bootstrapServers?.[0] === "fixture.invalid:9092";
  }
  return false;
}

async function cleanupOwned() {
  const errors = [];
  for (const target of [...owned].reverse()) {
    try {
      assertSafeNamespace(target.namespace);
      check(target.name.startsWith("plat13-"), `unsafe cleanup name ${target.name}`);
      const found = kube(
        [
          "-n", target.namespace, "get", target.plural, target.name,
          "--ignore-not-found=true", "-o", "json",
        ],
        { expected: [0], timeout: 5000 },
      );
      if (found.stdout.trim() === "") {
        result.cleanup.push({ ...target, state: "already absent" });
        continue;
      }
      const object = JSON.parse(found.stdout);
      check(object?.metadata?.namespace === target.namespace && object?.metadata?.name === target.name,
        `cleanup lookup identity mismatch for ${target.kind}/${target.name}`);
      check(ownershipMatches(target, object),
        `refusing to delete ${target.kind}/${target.name}: UID/ownership signature does not match`);
      const deleted = kube([
        "-n", target.namespace, "delete", target.plural, target.name,
        "--wait=true", "--timeout=10s", "--ignore-not-found=true",
      ], { expected: [0], timeout: 12_000 });
      const verify = kube(
        [
          "-n", target.namespace, "get", target.plural, target.name,
          "--ignore-not-found=true", "-o", "name",
        ],
        { expected: [0], timeout: 5000 },
      );
      check(verify.stdout.trim() === "", `${target.kind}/${target.name} remains after bounded delete`);
      result.cleanup.push({
        kind: target.kind, namespace: target.namespace, name: target.name,
        uid: object.metadata.uid, state: "deleted and verified absent", commandExitCode: deleted.status,
      });
      emit({ phase: "cleaned-object", kind: target.kind, namespace: target.namespace, name: target.name });
    } catch (error) {
      const diagnostic = errorText(error);
      errors.push(diagnostic);
      result.cleanup.push({
        kind: target.kind, namespace: target.namespace, name: target.name,
        state: "cleanup failed", diagnostic,
      });
    }
  }
  if (errors.length) throw new Error(`cleanup failures: ${errors.join("; ")}`);
}

check(stage === "single" || stage === "multi", `PLAT13_STAGE must be single or multi, got ${stage}`);
check(stage !== "multi" || second, "PLAT13_SECOND_NAMESPACE is required for the multi stage");
assertSafeNamespace(primary);
if (second) assertSafeNamespace(second);

let browser = null;
let failure = null;
try {
  if (cleanupNegativeControl) {
    own("Backup", "backups", primary, `plat13-cleanup-negative-${suffix}`, "run-label");
  } else {
    seedFixtures();
    browser = await chromium.launch({ headless: true });
    if (stage === "single") await oneNamespace(browser);
    await multipleNamespaces(browser);
    await staleContentNegativeControl(browser);
    await durableClusterPost(browser);
    await non2xxPostNegativeControl(browser);
    await staleRestorePreparation(browser);
    await denialAndCoreDetectorProof(browser);
    await missingNamespaceCase(browser);
  }
} catch (error) {
  failure = error;
  result.failure = errorText(error);
} finally {
  if (browser !== null) {
    try {
      await browser.close();
    } catch (error) {
      failure ||= error;
      result.browserCloseFailure = errorText(error);
    }
  }
  try {
    await cleanupOwned();
  } catch (error) {
    failure ||= error;
    result.cleanupFailure = errorText(error);
  }
  result.ok = failure === null;
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  if (failure !== null) process.exitCode = 1;
}
