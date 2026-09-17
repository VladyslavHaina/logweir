// D2 W13 live UI acceptance harness (PLAT-08.1, PLAT-08.2, PLAT-09.1,
// PLAT-03.1, PLAT-03.2).
//
// The sibling of `scripts/plat13-ui-e2e.mjs` and `scripts/plat12-13-ui-e2e.mjs`,
// which drive the page through `kubectl proxy`. This one drives the CONSOLE
// mode, because the three surfaces it is about are served by the product API
// and by nothing else: it starts `logweir-api` in localAdmin mode on a loopback
// port, pointed at this worktree's own `ui/` directory and at one namespace it
// created, and then drives a real Chromium against it.
//
// EVERY POSITIVE CASE IS REAL, and there is no fault injection of any kind in
// this harness. Every object is created by the PAGE, against the real
// `logweir-api`, against the real kube-apiserver, and read back with `kubectl`
// by name and by UID. No response body is fabricated, intercepted or delayed.
//
// WHAT THE LAB CANNOT DO, AND WHY THAT IS THE POINT OF FOUR OF THESE JOURNEYS.
// No controller on this cluster reconciles `BackupDestination`, `TopicDiscovery`
// or `Preflight` -- the images predate the three kinds. So the API creates the
// objects and their statuses stay empty, for ever. That is precisely the state
// the page must render honestly, and the assertions below are about exactly
// that: a destination reads "not judged yet" and never valid or invalid; a
// discovery and a preflight read `pending` and never succeeded or failed; and
// no sentence anywhere claims readiness, completeness or health.
//
// THE CREDENTIAL JOURNEY IS THE SECURITY ONE. The page creates a destination
// with a write-only credential typed into the form. Every response body the
// browser received is captured and scanned for the secret; so is every
// screenshot's page text, the whole rendered DOM, the API's own log, and the
// result document this harness writes. The Secret is read back with `kubectl`
// to prove it was created and that the page's answer named it without its
// value.
//
// Dependencies: Node.js, kubectl, a built `logweir-api`, and Playwright with
// Chromium. Resolve Playwright without a machine-specific path, for example:
//   NODE_PATH="$(npm root -g)" node scripts/d2w13-ui-e2e.mjs
//
// Environment (all optional):
//   UI_E2E_OWNER       the `logweir.dev/test-owner` label this run writes and
//                      checks before it deletes anything; default d2w13.
//   UI_E2E_PREFIX      the namespace prefix; default lw-d2w13-. Asserted twice:
//                      before anything is created, and again before the delete.
//   UI_E2E_NAMESPACE   the namespace to create and delete.
//   UI_E2E_API_BIN     the logweir-api binary; default target/debug/logweir-api.
//   UI_E2E_UI_DIR      the directory to serve; default this worktree's ui/.
//   UI_E2E_ARTIFACTS   where screenshots, the API log and the result go.
//   UI_E2E_KEEP        "1" keeps the namespace for a look around afterwards.

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { mkdirSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = process.env.UI_E2E_KUBECTL || "kubectl";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UI_DIR = process.env.UI_E2E_UI_DIR || join(REPO, "ui");
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "debug", "logweir-api");
const OWNER = process.env.UI_E2E_OWNER || "d2w13";
const ARTIFACTS = process.env.UI_E2E_ARTIFACTS ||
  ("/tmp/logweir-roadmap-run/claude/artifacts/" + OWNER);
const NAMESPACE_PREFIX = process.env.UI_E2E_PREFIX || "lw-d2w13-";
const OWNER_LABEL = "logweir.dev/test-owner=" + OWNER;
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z");
const namespace = process.env.UI_E2E_NAMESPACE || (NAMESPACE_PREFIX + stamp.toLowerCase());
const suffix = Math.random().toString(36).slice(2, 7);

// THE VALUE THAT MUST NEVER COME BACK. It is generated per run, so a match
// anywhere is a match on THIS run's credential and not on a string that
// happened to be in the tree.
const SECRET_VALUE = "d2w13-" + randomBytes(18).toString("hex");
const ACCESS_KEY_ID = "AKIA" + randomBytes(8).toString("hex").toUpperCase();

const result = {
  harness: "scripts/d2w13-ui-e2e.mjs",
  kubeContext: KUBE_CONTEXT,
  owner: OWNER,
  namespacePrefix: NAMESPACE_PREFIX,
  namespace: namespace,
  uiDirectory: UI_DIR,
  apiBinary: API_BIN,
  startedAt: new Date().toISOString(),
  mode: "console (logweir-api, localAdmin, loopback)",
  faultInjection: [],
  journeys: [],
  created: [],
  screenshots: [],
  credentialScan: null,
  cleanup: [],
};

function check(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function record(journey, detail) {
  result.journeys.push(Object.assign({ journey: journey }, detail || {}));
  process.stderr.write("== passed: " + journey + "\n");
}

function assertSafeNamespace(ns) {
  check(ns.startsWith(NAMESPACE_PREFIX),
    "this harness only ever touches " + NAMESPACE_PREFIX + "* namespaces, not " + ns);
  check(ns !== "default" && !ns.startsWith("kube-") && !ns.startsWith("logweir-scram"),
    "refusing a system or shared-fixture namespace: " + ns);
}

function kube(args, options) {
  const opts = options || {};
  const done = spawnSync(KUBECTL, ["--context", KUBE_CONTEXT].concat(args), {
    encoding: "utf8",
    input: opts.input,
    timeout: opts.timeout || 30000,
    maxBuffer: 4 * 1024 * 1024,
  });
  const expected = opts.expected || [0];
  if (!expected.includes(done.status)) {
    throw new Error(
      KUBECTL + " " + args.join(" ") + " exited " + done.status + ": " +
        String(done.stderr || "").trim().slice(0, 1500),
    );
  }
  return done;
}

function kubeJson(args) {
  return JSON.parse(kube(args.concat(["-o", "json"])).stdout);
}

function pause(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function freePort() {
  return new Promise((ok, bad) => {
    const server = createServer();
    server.on("error", bad);
    server.listen(0, "127.0.0.1", () => {
      const port = server.address().port;
      server.close(() => ok(port));
    });
  });
}

async function shot(page, name) {
  const at = join(ARTIFACTS, "d2w13-" + name + ".png");
  await page.screenshot({ path: at, fullPage: true });
  result.screenshots.push(at);
  return at;
}

// WHAT THE PAGE SAYS, CASE-FOLDED. The stylesheet renders several headings and
// every badge in capitals through `text-transform`, and `innerText` reports
// what is RENDERED -- so "Latest attempt" reaches this harness as
// "LATEST ATTEMPT". Folding here keeps each assertion about the sentence rather
// than about the stylesheet, and a test that broke when a heading's case
// changed would be testing the wrong thing.
async function text(page) {
  return (await page.evaluate(() => document.body.innerText)).toLowerCase();
}

async function waitForText(page, needle, label) {
  const wanted = needle.toLowerCase();
  for (let i = 0; i < 40; i += 1) {
    if ((await text(page)).includes(wanted)) {
      return;
    }
    await pause(500);
  }
  throw new Error(label + ": never saw " + JSON.stringify(needle) + " on screen. Saw:\n" +
    (await text(page)).slice(0, 2500));
}

// ------------------------------------------------------------- the service

let api = null;
const apiLog = [];

async function startApi(port) {
  const dir = "/tmp/d2w13-live";
  mkdirSync(dir, { recursive: true });
  const cursorKey = join(dir, "cursor.key");
  writeFileSync(cursorKey, randomBytes(32));
  const configPath = join(dir, "config.yaml");
  const config = [
    "mode: localAdmin",
    "listen: \"127.0.0.1:" + port + "\"",
    "publicOrigin: \"http://127.0.0.1:" + port + "\"",
    "uiDirectory: " + UI_DIR,
    "localAdmin:",
    "  subject: admin",
    "  displayName: Local administrator",
    "namespaces: [" + namespace + "]",
    "kubernetes:",
    "  source: kubeconfig",
    "  context: " + KUBE_CONTEXT,
    "cursorKeyFile: " + cursorKey,
    "",
  ].join("\n");
  writeFileSync(configPath, config);
  writeFileSync(join(ARTIFACTS, "d2w13-config.yaml"), config);
  api = spawn(API_BIN, ["--config", configPath], { stdio: ["ignore", "pipe", "pipe"] });
  api.stdout.on("data", (b) => apiLog.push(String(b)));
  api.stderr.on("data", (b) => apiLog.push(String(b)));
  // EVERY WAIT HAS A CEILING (WORKER-RULES: a hung child blocks the worker).
  for (let i = 0; i < 60; i += 1) {
    try {
      const probe = await fetch("http://127.0.0.1:" + port + "/healthz");
      if (probe.ok) {
        return;
      }
    } catch (notYet) {
      // still binding
    }
    await pause(500);
  }
  throw new Error("logweir-api never answered /healthz within 30 s. Log:\n" + apiLog.join(""));
}

function stopApi() {
  if (api !== null && api.exitCode === null) {
    api.kill("SIGTERM");
  }
}

// --------------------------------------------------------------- the run

async function main() {
  mkdirSync(ARTIFACTS, { recursive: true });
  assertSafeNamespace(namespace);
  check(kube(["version", "--client=true"], { expected: [0] }).status === 0, "kubectl works");

  kube(["create", "namespace", namespace]);
  kube(["label", "namespace", namespace, OWNER_LABEL]);
  const ns = kubeJson(["get", "namespace", namespace]);
  result.namespaceUid = ns.metadata.uid;
  result.created.push({ kind: "Namespace", name: namespace, uid: ns.metadata.uid });

  // The connection a discovery is about, and the legacy schedule the adoption
  // journey asks about. Both are created with kubectl, not by the page: they
  // are the FIXTURE, and what is under test is what the page does with them.
  const connection = "conn-" + suffix;
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
      metadata: { name: connection, labels: { "logweir.dev/test-owner": OWNER } },
      spec: { bootstrapServers: ["kafka-source.logweir-scram-local.svc:9092"], role: "source",
        auth: { mode: "plaintext", tls: false } },
    }),
  });
  result.created.push({ kind: "KafkaCluster", name: connection });

  const port = await freePort();
  result.port = port;
  await startApi(port);
  const base = "http://127.0.0.1:" + port + "/ui/";

  const browser = await chromium.launch();
  const context = await browser.newContext();
  const page = await context.newPage();

  // EVERY RESPONSE BODY THE BROWSER RECEIVED, kept for the credential scan.
  const bodies = [];
  page.on("response", async (response) => {
    try {
      const url = response.url();
      if (url.indexOf("/api/v1/") !== -1) {
        bodies.push({ url: url, status: response.status(), body: await response.text() });
      }
    } catch (gone) {
      // A response whose body is no longer available cannot hide anything the
      // page then rendered; the DOM scan below covers what it showed.
    }
  });

  try {
    // ---------------------------------------------------------------- 1
    await page.goto(base + "#/destinations?ns=" + namespace, { waitUntil: "load", timeout: 30000 });
    await waitForText(page, "Destinations", "the destinations route");
    await waitForText(page, "No destination in this namespace yet", "the empty list");
    await shot(page, "01-empty");
    record("the destinations tab renders in console mode with an empty namespace", {
      route: "#/destinations", namespace: namespace,
    });

    // ---------------------------------------------------------------- 2
    const destination = "primary-" + suffix;
    await page.fill("#destination-name", destination);
    await page.fill("#destination-description", "D2 W13 live journey");
    await page.fill("#destination-bucket", "kafka-backups");
    await page.fill("#destination-prefix", "team-a/prod");
    await page.fill("#destination-region", "us-east-1");
    await page.fill("#destination-endpoint", "https" + "://" + "minio.storage.svc:9000");
    await page.check("#destination-security-tls");
    await page.check("#destination-addressing-pathstyle");
    await page.selectOption("#destination-archiveWrite-source", "new");
    // THE SCREENSHOT IS TAKEN BEFORE THE CREDENTIAL IS TYPED, deliberately.
    // The secret access key is a password input and renders as dots, but the
    // access key ID is a plain text input and would be PIXELS in this file --
    // and "never a credential in any artifact" has to mean the images too, not
    // just the strings a grep can find in them.
    await shot(page, "02-form-filled");
    await page.fill("#destination-archiveWrite-akid", ACCESS_KEY_ID);
    await page.fill("#destination-archiveWrite-sak", SECRET_VALUE);
    await page.click("#destination-form button[type=submit]");
    await waitForText(page, destination, "the created destination in the list");
    await shot(page, "03-created");

    const created = kubeJson(["-n", namespace, "get", "backupdestination", destination]);
    check(created.metadata.uid.length > 0, "the destination has a UID");
    result.created.push({ kind: "BackupDestination", name: destination, uid: created.metadata.uid });
    const secretName = "lwd-" + destination + "-archive-write";
    const secret = kubeJson(["-n", namespace, "get", "secret", secretName]);
    check(secret.metadata.uid.length > 0, "the write-only credential became a Secret");
    result.created.push({ kind: "Secret", name: secretName, uid: secret.metadata.uid });
    record("a destination is created by the page with a write-only credential", {
      destination: destination, uid: created.metadata.uid,
      secret: secretName, secretUid: secret.metadata.uid,
      storage: created.spec.storage, transport: created.spec.transport,
    });

    // ---------------------------------------------------------------- 3
    await page.goto(base + "#/destinations?ns=" + namespace + "&name=" + destination,
      { waitUntil: "load", timeout: 30000 });
    await waitForText(page, "not judged yet", "the unjudged verdict");
    const detail = await text(page);
    check(detail.includes("no controller has recorded a verdict"),
      "the detail says why there is no verdict");
    check(!/\bvalid \(/.test(detail), "and never claims the destination is valid");
    check(detail.includes("no access test has been recorded"),
      "and says nothing below claims it works");
    await shot(page, "04-detail-unjudged");
    record("a destination no controller has judged renders as not judged, never valid or invalid", {
      statusOnObject: created.status === undefined ? null : created.status,
    });

    // ---------------------------------------------------------------- 4
    await page.click("#destination-test-form button[type=submit]");
    await waitForText(page, "pf-", "the started preflight");
    await pause(1500);
    const afterTest = await text(page);
    check(afterTest.includes("pending"), "the preflight renders pending with no controller");
    // NO GREEN BADGE ANYWHERE IN THE RESULT. Read from the DOM and not from
    // `innerText`, because a class name is not text: `badge-green` is how this
    // page spells "we are asserting something worked", and a preflight nothing
    // has reconciled must not spell it.
    const testDom = await page.evaluate(() => {
      const panel = document.querySelector(".preflight-result");
      return panel === null ? "" : panel.outerHTML;
    });
    check(testDom.length > 0, "the preflight result was rendered");
    check(testDom.indexOf("badge-green") === -1,
      "and it carries no green badge: " + testDom.slice(0, 600));
    const preflights = kubeJson(["-n", namespace, "get", "preflights"]).items;
    check(preflights.length >= 1, "the page created a Preflight");
    result.created.push({
      kind: "Preflight", name: preflights[0].metadata.name, uid: preflights[0].metadata.uid,
    });
    await shot(page, "05-preflight-pending");
    record("an access test creates a real Preflight and renders it pending, never ready", {
      preflight: preflights[0].metadata.name, uid: preflights[0].metadata.uid,
      statusOnObject: preflights[0].status === undefined ? null : preflights[0].status,
    });

    // ---------------------------------------------------------------- 5
    // A ROTATION THAT CANNOT WRITE. The archive-write Secret already exists
    // under its deterministic name, and this service holds `create` on Secrets
    // and nothing else -- so a rotation entering a NEW value for that role is a
    // 409 whose sentence the page must render verbatim.
    await page.selectOption("#rotate-archiveWrite-source", "new");
    await page.fill("#rotate-archiveWrite-akid", ACCESS_KEY_ID);
    await page.fill("#rotate-archiveWrite-sak", SECRET_VALUE + "-rotated");
    await page.click("#destination-rotate-form button[type=submit]");
    await waitForText(page, "already exists", "the rotation conflict");
    // THE RE-RENDER AFTER THE 409 IS WHERE A KEPT CREDENTIAL WOULD SURFACE.
    // The form is drawn again from its draft, and the draft's allowlist names
    // no credential input -- so both boxes come back EMPTY, which is the
    // rule's own consequence made visible.
    check((await page.inputValue("#rotate-archiveWrite-akid")) === "",
      "the re-rendered rotation form did not keep the access key id");
    check((await page.inputValue("#rotate-archiveWrite-sak")) === "",
      "nor the secret access key");
    const conflict = await text(page);
    check(conflict.includes(secretName), "the 409 names the Secret that is in the way");
    check(conflict.includes("was not written"), "and says the entered value was not written");
    await shot(page, "06-rotation-409");
    const generationNow = kubeJson(["-n", namespace, "get", "backupdestination", destination])
      .metadata.generation;
    check(generationNow === created.metadata.generation,
      "and nothing was written: the generation did not move");
    record("a rotation that cannot write reports the 409 verbatim and changes nothing", {
      secret: secretName, generationBefore: created.metadata.generation,
      generationAfter: generationNow,
    });

    // ---------------------------------------------------------------- 6
    await page.goto(base + "#/clusters?ns=" + namespace + "&name=" + connection,
      { waitUntil: "load", timeout: 30000 });
    await waitForText(page, "Discover topics", "the discovery panel");
    await waitForText(page, "No topic discovery has run for this connection", "the empty panel");
    await page.click("#discovery-start");
    await waitForText(page, "Latest attempt", "the started discovery");
    const discoveries = kubeJson(["-n", namespace, "get", "topicdiscoveries"]).items;
    check(discoveries.length === 1, "exactly one discovery was created");
    const discoveryName = discoveries[0].metadata.name;
    result.created.push({
      kind: "TopicDiscovery", name: discoveryName, uid: discoveries[0].metadata.uid,
    });
    const started = await text(page);
    check(started.includes("pending") || started.includes("queued"),
      "the discovery renders as pending with no controller: " + started.slice(0, 400));
    check(started.indexOf("attestedcomplete") === -1,
      "and no completeness is claimed for an unfinished listing");
    check(started.indexOf("rather than calling it complete") !== -1,
      "the visibility banner says why a listing is never called complete");
    await shot(page, "07-discovery-started");
    record("a discovery is started by the page and renders pending, never succeeded", {
      discovery: discoveryName, uid: discoveries[0].metadata.uid,
      statusOnObject: discoveries[0].status === undefined ? null : discoveries[0].status,
    });

    // ---------------------------------------------------------------- 7
    await page.click("#discovery-cancel");
    await pause(2000);
    const cancelled = kubeJson(["-n", namespace, "get", "topicdiscovery", discoveryName]);
    check(cancelled.spec.cancelRequested === true,
      "the cancel was recorded on the object as a wish: " + JSON.stringify(cancelled.spec));
    check(cancelled.metadata.uid === discoveries[0].metadata.uid,
      "and it is the same object, not a replacement");
    await shot(page, "08-discovery-cancelled");
    record("cancelling a discovery records the wish on the object the page started", {
      discovery: discoveryName, cancelRequested: cancelled.spec.cancelRequested,
    });

    // ---------------------------------------------------------------- 8
    await page.goto(base + "#/destinations?ns=" + namespace, { waitUntil: "load", timeout: 30000 });
    await waitForText(page, "Adopt an existing archive location", "the adoption form");
    await page.fill("#legacy-name", "adopted-" + suffix);
    await page.fill("#legacy-schedule", "no-such-schedule-" + suffix);
    await page.fill("#legacy-secret", "logweir-s3");
    await page.click("#destination-legacy-form button[type=submit]");
    await pause(2000);
    const refusal = await text(page);
    check(refusal.includes("legacy_location_unknown") || refusal.includes("not_found"),
      "the adoption was refused by a named code: " + refusal.slice(0, 1200));
    const adopted = kube(["-n", namespace, "get", "backupdestination", "adopted-" + suffix],
      { expected: [0, 1] });
    check(adopted.status !== 0, "and nothing was created for a location nobody could derive");
    await shot(page, "09-legacy-refusal");
    record("an adoption with no derivable location is refused and creates nothing", {
      code: refusal.includes("legacy_location_unknown") ? "legacy_location_unknown" : "not_found",
      created: false,
    });

    // ---------------------------------------------------------------- 9
    // THE CREDENTIAL SCAN. Every API response body the browser received, the
    // whole rendered DOM of every page visited, the API's own log, and this
    // result document.
    const dom = await page.evaluate(() => document.documentElement.outerHTML);
    await page.goto(base + "#/destinations?ns=" + namespace + "&name=" + destination,
      { waitUntil: "load", timeout: 30000 });
    await waitForText(page, destination, "the detail view again");
    const detailDom = await page.evaluate(() => document.documentElement.outerHTML);
    const haystacks = [
      ["api response bodies", bodies.map((b) => b.url + " " + b.body).join("\n")],
      ["destinations list DOM", dom],
      ["destination detail DOM", detailDom],
      ["logweir-api log", apiLog.join("")],
      ["the result document", JSON.stringify(result)],
    ];
    const hits = [];
    for (const [where, hay] of haystacks) {
      if (hay.indexOf(SECRET_VALUE) !== -1) {
        hits.push(where);
      }
    }
    check(hits.length === 0,
      "the write-only credential appeared in: " + hits.join(", "));
    // AND THE SECRET'S NAME DID COME BACK, which is what a reference is.
    check(bodies.some((b) => b.body.indexOf(secretName) !== -1),
      "the Secret's NAME is in a response, which is the reference the page shows");
    const storedSecret = kubeJson(["-n", namespace, "get", "secret", secretName]);
    check(
      Buffer.from(storedSecret.data["secret-access-key"], "base64").toString("utf8") ===
        SECRET_VALUE,
      "and the value the page sent is what the Secret holds, so the scan was not vacuous",
    );
    result.credentialScan = {
      valueLength: SECRET_VALUE.length,
      scanned: haystacks.map(([where, hay]) => ({ where: where, bytes: hay.length })),
      hits: hits,
      secretNameEchoed: true,
      secretHoldsTheValue: true,
    };
    record("the write-only credential never appears in a response, the DOM, the log or an artifact", {
      responsesScanned: bodies.length,
    });

    await shot(page, "10-final");
  } finally {
    await browser.close().catch(() => {});
    stopApi();
  }

  writeFileSync(join(ARTIFACTS, "d2w13-api.log"), apiLog.join(""));

  // ------------------------------------------------------------- cleanup
  assertSafeNamespace(namespace);
  const before = kubeJson(["get", "namespace", namespace]);
  check(before.metadata.uid === result.namespaceUid,
    "the namespace about to be deleted is the one this run created (UID check)");
  check((before.metadata.labels || {})["logweir.dev/test-owner"] === OWNER,
    "and it carries this run's owner label");
  const objects = kube(["-n", namespace, "get",
    "backupdestinations,topicdiscoveries,preflights,kafkaclusters,secrets", "-o", "name"],
    { expected: [0, 1] }).stdout;
  if (process.env.UI_E2E_KEEP === "1") {
    result.cleanup.push({ kept: true, namespace: namespace });
  } else {
    kube(["delete", "namespace", namespace, "--wait=true"], { timeout: 180000 });
    const after = kube(["get", "namespace", namespace], { expected: [0, 1] });
    result.cleanup.push({
      namespace: namespace,
      uid: result.namespaceUid,
      ownerLabel: OWNER,
      objectsBefore: objects.trim().split("\n").filter((l) => l.length > 0),
      deleted: after.status !== 0,
      afterStderr: String(after.stderr || "").trim(),
      othersUntouched: kube(["get", "namespaces", "-o", "name"]).stdout.trim().split("\n"),
    });
    check(after.status !== 0, "the namespace is gone");
  }

  result.finishedAt = new Date().toISOString();
  result.passed = result.journeys.length;
  const at = join(ARTIFACTS, "d2w13-live.json");
  writeFileSync(at, JSON.stringify(result, null, 2) + "\n");
  process.stderr.write("\n== " + result.journeys.length + " journey(s) passed; result: " + at + "\n");
}

main().then(
  () => process.exit(0),
  (error) => {
    stopApi();
    result.failure = error instanceof Error ? error.stack : String(error);
    result.finishedAt = new Date().toISOString();
    try {
      mkdirSync(ARTIFACTS, { recursive: true });
      writeFileSync(join(ARTIFACTS, "d2w13-live.json"), JSON.stringify(result, null, 2) + "\n");
      writeFileSync(join(ARTIFACTS, "d2w13-api.log"), apiLog.join(""));
    } catch (ignored) {
      // The failure below is what matters.
    }
    process.stderr.write("\n== FAILED: " + String(error && error.message) + "\n");
    process.exit(1);
  },
);
