// PLAT-10.1 / PLAT-10.2 live UI acceptance harness.
//
// The sibling of `scripts/d2w13-ui-e2e.mjs`, whose launcher this reuses
// verbatim in shape: `logweir-api` in localAdmin mode on a loopback port,
// pointed at this worktree's own `ui/` and at one namespace this run created,
// with a real Chromium driven against it. Console mode, because the guided
// form's cadence preview, its policy replace and its readiness check are all
// product-API routes and `kubectl proxy` serves none of them.
//
// EVERY RUN IN EVERY HISTORY IS REAL. Page-created objects are real
// API/Kubernetes writes, read back by name and UID, and every Backup, Preflight,
// catalog sync and Restore admission below is reconciled by the ONE controller
// on this cluster: the shared lab's `weirkeeper` in `logweir-scram-local`,
// which watches every namespace and is only ever READ here (its pod, image and
// its log lines about this namespace are recorded). No run status is written
// by this harness and no second controller is deployed; the harness refuses to
// start if any other weirkeeper Deployment exists.
//
// WHAT THIS RUN PROVISIONS, all inside one owner-labelled namespace: the
// no-verb `logweir-runner` account; copies (never printed) of the lab's
// source/target SCRAM, object-store and signer Secrets -- the signer's public
// half is the cluster TrustRoster `default`'s, which is what lets the catalog
// answer `Verified`; saved connections; a BackupDestination on the lab's MinIO
// in a bucket OF ITS OWN (`mc mb`, never --ignore-existing, removed with
// `mc rb --force` before the namespace is deleted); and a RecoveryCatalog on it.
//
// EVERY JOURNEY HAS A NEGATIVE CONTROL, and each control is an assertion that
// the product REFUSED something -- a create with no compiled expression, a
// dynamic selection with no incompleteDiscovery answer, a cadence the API
// rejects, a readiness check that must end `notReady`, a failed run that must
// offer no restore, an unavailable or incomplete catalog row that must not be
// green, a Restore that must be held for its Approval -- together with a
// `kubectl` read proving what did or did not reach the cluster.
//
// WHAT IT CANNOT DO ON THIS HOST, recorded rather than claimed: admit the
// Restore. That needs an Approval the TrustRoster's approver key verifies, and
// the private half of approver@scram-local.invalid's key is not on this host.
//
// Dependencies: Node.js, kubectl, a built `logweir-api`, Playwright/Chromium:
//   NODE_PATH="$(npm root -g)" node scripts/plat10-ui-e2e.mjs
//
// Environment (all optional):
//   UI_E2E_OWNER       the `logweir.dev/test-owner` label; default plat10.
//   UI_E2E_PREFIX      the namespace prefix; default lw-p10-.
//   UI_E2E_NAMESPACE   the namespace to create and delete.
//   UI_E2E_API_BIN     the logweir-api binary; default target/release/logweir-api.
//   UI_E2E_UI_DIR      the directory to serve; default this worktree's ui/.
//   UI_E2E_ARTIFACTS   where screenshots, the API log and the result go.
//   UI_E2E_KEEP        "1" keeps the namespace for a look around afterwards.
//   UI_E2E_BUCKET      the bucket this run creates and removes; default the
//                       namespace name. It must not exist beforehand.

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { existsSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = process.env.UI_E2E_KUBECTL || "kubectl";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UI_DIR = process.env.UI_E2E_UI_DIR || join(REPO, "ui");
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "release", "logweir-api");
const OWNER = process.env.UI_E2E_OWNER || "plat10";
const ARTIFACTS_ROOT = process.env.UI_E2E_ARTIFACTS ||
  ("/tmp/logweir-roadmap-run/claude/artifacts/plat10-ui");
const NAMESPACE_PREFIX = process.env.UI_E2E_PREFIX || "lw-p10-";
const OWNER_LABEL = "logweir.dev/test-owner=" + OWNER;
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z");
const namespace = process.env.UI_E2E_NAMESPACE || (NAMESPACE_PREFIX + stamp.toLowerCase());
const suffix = Math.random().toString(36).slice(2, 7);

// The shared lab's Kafka and MinIO, read-only, from this namespace. Named here
// so the result document records exactly which shared fixture was addressed.
const LAB = "logweir-scram-local";
const LAB_KAFKA = "kafka-source." + LAB + ".svc.cluster.local:9096";
const LAB_MINIO = "minio." + LAB + ".svc:9000";
const LAB_TARGET = "kafka-target." + LAB + ".svc.cluster.local:9096";
// THIS RUN'S OWN BUCKET, named after its namespace, created with `mc mb` and
// removed with `mc rb --force` in the cleanup: no object this run reads or
// deletes can belong to anybody else.
const BUCKET = process.env.UI_E2E_BUCKET || namespace;
const STORE_SECRET = "p10-object-store";
const CATALOG = "p10-catalog";
const NOT_IN_CATALOG = "not in the catalog";
const UNREACHABLE_KAFKA = "kafka-nowhere." + "lw-p10-void" + ".svc:9092";

const ARTIFACTS = join(ARTIFACTS_ROOT, namespace);
const WORK_DIR = join("/tmp", "plat10-live-" + namespace);

const result = {
  harness: "scripts/plat10-ui-e2e.mjs",
  tasks: ["PLAT-10.1", "PLAT-10.2"],
  kubeContext: KUBE_CONTEXT,
  owner: OWNER,
  namespacePrefix: NAMESPACE_PREFIX,
  namespace: namespace,
  lab: { release: LAB, kafka: LAB_KAFKA, target: LAB_TARGET, minio: LAB_MINIO,
    usedReadOnly: true, bucket: BUCKET },
  uiDirectory: UI_DIR,
  apiBinary: API_BIN,
  revision: null,
  apiBinarySha256: null,
  startedAt: new Date().toISOString(),
  mode: "console (logweir-api, localAdmin, loopback)",
  faultInjection: [],
  journeys: [],
  controls: [],
  fixtures: [],
  created: [],
  screenshots: [],
  requests: [],
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

function control(about, detail) {
  result.controls.push(Object.assign({ control: about }, detail || {}));
  process.stderr.write("   -- control: " + about + "\n");
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

function commandText(command, args) {
  const done = spawnSync(command, args, { encoding: "utf8" });
  if (done.status !== 0) {
    return null;
  }
  return String(done.stdout || "").trim();
}

function schedules() {
  return kubeJson(["-n", namespace, "get", "backupschedules"]).items;
}

function backups() {
  return kubeJson(["-n", namespace, "get", "backups"]).items;
}

async function waitForTerminalBackup(name, label) {
  let object = null;
  for (let attempt = 0; attempt < 240; attempt += 1) {
    object = kubeJson(["-n", namespace, "get", "backup", name]);
    const phase = String((object.status || {}).phase || "");
    if (["Succeeded", "Failed", "Cancelled"].includes(phase)) {
      return object;
    }
    await pause(1000);
  }
  throw new Error(label + ": Backup " + name + " never reached a terminal phase");
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
  const at = join(ARTIFACTS, name + ".png");
  await page.screenshot({ path: at, fullPage: true });
  result.screenshots.push(at);
  return at;
}

async function text(page) {
  return (await page.evaluate(() => document.body.innerText)).toLowerCase();
}

async function waitForText(page, needle, label) {
  const wanted = needle.toLowerCase();
  for (let i = 0; i < 60; i += 1) {
    if ((await text(page)).includes(wanted)) {
      return;
    }
    await pause(500);
  }
  throw new Error(label + ": never saw " + JSON.stringify(needle) + " on screen. Saw:\n" +
    (await text(page)).slice(0, 3000));
}

async function waitForSelector(page, selector, label) {
  try {
    await page.waitForSelector(selector, { timeout: 30000 });
  } catch (never) {
    throw new Error(label + ": " + selector + " never appeared. Saw:\n" +
      (await text(page)).slice(0, 2000));
  }
}

/** Reaches an element ONLY by Tab. This is deliberately not `locator.focus()`:
 *  the keyboard row must prove the browser's focus order, rather than merely
 *  put a caret in a field that a keyboard user could not reach. The schedule
 *  view can repaint while a selection changes the fields that exist, so a
 *  missed element is reported with the focus trail instead of silently using a
 *  pointer or programmatic focus fallback. */
async function tabTo(page, selector, label) {
  const seen = [];
  for (let i = 0; i < 160; i += 1) {
    await page.keyboard.press("Tab");
    const landed = await page.evaluate(() => {
      const node = document.activeElement;
      if (node === null) {
        return "(none)";
      }
      return node.id || node.getAttribute("name") || node.tagName.toLowerCase();
    });
    seen.push(landed);
    const matched = await page.evaluate((target) => {
      const node = document.querySelector(target);
      return node !== null && document.activeElement === node;
    }, selector);
    if (matched) {
      return seen;
    }
  }
  throw new Error(label + ": Tab did not reach " + selector + ". Focus trail: " +
    JSON.stringify(seen) + ". Saw:\n" + (await text(page)).slice(0, 1200));
}

/** Opens a route and waits for the control that says it rendered, RELOADING
 *  ONCE if it did not.
 *
 *  WHY A RETRY IS HONEST HERE AND NOT A PAPERED-OVER FAILURE. `ui/client.js`
 *  decides the mode from ONE `GET /api/v1/session` per page load; a load whose
 *  probe is answered `503 kubernetes_unavailable` -- a transient from the API's
 *  own Kubernetes client -- lands in legacy mode for the life of that load and
 *  then refuses every product route by name. That is the page behaving exactly
 *  as designed; what it is not is a statement about the view under test. The
 *  reload is a new page load and therefore a new decision, and the result
 *  document records every retry so a run that needed several is visible. */
async function openRoute(page, url, selector, label) {
  await page.goto(url, { waitUntil: "load", timeout: 30000 });
  try {
    await page.waitForSelector(selector, { timeout: 20000 });
    return 0;
  } catch (notYet) {
    const saw = (await text(page)).slice(0, 400);
    await page.reload({ waitUntil: "load", timeout: 30000 });
    try {
      await page.waitForSelector(selector, { timeout: 20000 });
      return 1;
    } catch (stillNot) {
      throw new Error(label + ": " + selector + " never appeared, over two page loads. " +
        "First load saw:\n" + saw + "\nSecond load saw:\n" +
        (await text(page)).slice(0, 2000));
    }
  }
}

// ------------------------------------------------------------- the service

let api = null;
const apiLog = [];

async function startApi(port) {
  mkdirSync(WORK_DIR, { recursive: true, mode: 0o700 });
  const cursorKey = join(WORK_DIR, "cursor.key");
  writeFileSync(cursorKey, randomBytes(32), { mode: 0o600 });
  const configPath = join(WORK_DIR, "config.yaml");
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
  writeFileSync(join(ARTIFACTS, "config.yaml"), config);
  api = spawn(API_BIN, ["--config", configPath], { stdio: ["ignore", "pipe", "pipe"] });
  api.stdout.on("data", (b) => apiLog.push(String(b)));
  api.stderr.on("data", (b) => apiLog.push(String(b)));
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

// ------------------------------------------------------------- the fixtures

/** Copies one of the lab's Secrets into this run's namespace under `ownName`.
 *
 *  THE VALUE NEVER CROSSES THIS PROCESS AS TEXT IT PRINTS. It is read as JSON,
 *  stripped of every field that names the source object, and handed straight
 *  back to `kubectl create`; nothing is echoed, logged or written to an
 *  artifact, and the copy dies with the namespace. */
function copyLabSecret(labName, ownName) {
  const source = kubeJson(["-n", LAB, "get", "secret", labName]);
  const copy = {
    apiVersion: "v1", kind: "Secret", type: source.type || "Opaque",
    metadata: { name: ownName, namespace: namespace,
      labels: { "logweir.dev/test-owner": OWNER } },
    data: source.data,
  };
  kube(["-n", namespace, "create", "-f", "-"], { input: JSON.stringify(copy) });
  result.fixtures.push({ kind: "Secret", name: ownName, copiedFrom: LAB + "/" + labName,
    note: "value never printed, read or logged" });
  return ownName;
}

function seedConnection(name, servers, role, labSecret) {
  const scram = labSecret !== undefined;
  if (scram) {
    copyLabSecret(labSecret, name + "-scram");
  }
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
      metadata: { name: name, labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        bootstrapServers: [servers], role: role,
        auth: scram
          ? { mode: "scramSha512", tls: false, username: "scram-user",
            secretRef: { name: name + "-scram" } }
          : { mode: "plaintext", tls: false },
      },
    }),
  });
  result.fixtures.push({ kind: "KafkaCluster", name: name, role: role, createdBy: "kubectl",
    bootstrapServers: servers });
  return name;
}

/** One `minio/mc` Job in THIS namespace, against THIS run's bucket only, with
 *  the owned copy of the lab's object-store credential. Its log is the
 *  evidence; it never prints a credential (the alias line is silenced). */
let mcJobs = 0;
function mcJob(step, script) {
  mcJobs += 1;
  const name = "p10-mc-" + String(mcJobs) + "-" + step;
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "batch/v1", kind: "Job",
      metadata: { name: name, labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        backoffLimit: 0,
        template: {
          metadata: { labels: { "logweir.dev/test-owner": OWNER } },
          spec: {
            restartPolicy: "Never",
            containers: [{
              name: "mc", image: "minio/mc:latest", imagePullPolicy: "IfNotPresent",
              command: ["/bin/sh", "-ec"],
              args: ["mc alias set p10 \"$S3_ENDPOINT\" \"$AWS_ACCESS_KEY_ID\" " +
                "\"$AWS_SECRET_ACCESS_KEY\" >/dev/null; " + script],
              env: [
                { name: "S3_ENDPOINT", value: "http" + "://" + LAB_MINIO },
                { name: "S3_BUCKET", value: BUCKET },
                { name: "AWS_ACCESS_KEY_ID", valueFrom: { secretKeyRef:
                  { name: STORE_SECRET, key: "access-key-id" } } },
                { name: "AWS_SECRET_ACCESS_KEY", valueFrom: { secretKeyRef:
                  { name: STORE_SECRET, key: "secret-access-key" } } },
              ],
            }],
          },
        },
      },
    }),
  });
  const waited = kube(["-n", namespace, "wait", "--for=condition=complete", "job/" + name,
    "--timeout=150s"], { expected: [0, 1], timeout: 160000 });
  const logs = kube(["-n", namespace, "logs", "job/" + name], { expected: [0, 1] }).stdout;
  writeFileSync(join(ARTIFACTS, "mc-" + name + ".log"), logs);
  check(waited.status === 0, "the object-store step " + name + " did not complete:\n" + logs);
  return logs.trim();
}

function seedDestination(name) {
  copyLabSecret("logweir-s3", STORE_SECRET);
  // A BUCKET OF ITS OWN. `mc mb` without --ignore-existing: a name that
  // already exists is a refusal, so nothing below can read or delete an
  // object this run did not write.
  const made = mcJob("make-bucket", "mc mb \"p10/$S3_BUCKET\"; mc ls p10 | " +
    "while read -r line; do case \"$line\" in *\" $S3_BUCKET/\") echo \"bucket present: " +
    "$S3_BUCKET\";; esac; done");
  check(made.indexOf("bucket present: " + BUCKET) !== -1, "the owned bucket was not created");
  result.bucket = { name: BUCKET, endpoint: LAB_MINIO, createdBy: "this run", log: made };
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
      metadata: { name: name, labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        description: "PLAT-10 live journey: this run's own bucket on the lab's MinIO",
        storage: { provider: "S3", bucket: BUCKET, prefix: namespace,
          addressing: "PathStyle", endpoint: "http" + "://" + LAB_MINIO },
        transport: { security: "InsecureHTTP" },
        access: {
          archiveWrite: { mode: "SecretKeys", secret: { name: STORE_SECRET } },
          // The catalog sync Job reads with this explicit read grant.
          archiveRead: { mode: "SecretKeys", secret: { name: STORE_SECRET } },
          // The one evidence-read mode this build's controller performs. The
          // shared lab's installation policy allowlists no location, so the
          // lab controller answers NotAttempted/ControllerIdentityNotAllowlisted
          // for each run -- recorded below as it is, never as verified. The
          // VERIFIED verdict this harness asserts is the durable catalog's.
          evidenceRead: { mode: "ControllerIdentity" },
        },
      },
    }),
  });
  result.fixtures.push({ kind: "BackupDestination", name: name, createdBy: "kubectl",
    bucket: BUCKET, prefix: namespace, endpoint: LAB_MINIO });
  return name;
}

/** Asks the lab controller for one catalog sync and waits for ITS answer. */
async function syncCatalog(token) {
  kube(["-n", namespace, "patch", "recoverycatalog", CATALOG, "--type=merge", "-p",
    JSON.stringify({ spec: { syncRequest: token } })]);
  let catalog = null;
  for (let attempt = 0; attempt < 150; attempt += 1) {
    catalog = kubeJson(["-n", namespace, "get", "recoverycatalog", CATALOG]);
    const st = catalog.status || {};
    const synced = (st.conditions || []).find((c) => c.type === "Synced");
    if (st.observedSyncRequest === token && synced !== undefined && synced.status === "True" &&
      ((st.lastSyncJob || {}).finishedAt || "").length > 0) {
      writeFileSync(join(ARTIFACTS, "catalog-" + token + ".json"),
        JSON.stringify({ spec: catalog.spec, status: catalog.status }, null, 2) + "\n");
      return catalog;
    }
    await pause(1000);
  }
  throw new Error("the lab controller did not complete catalog sync " + token + ": " +
    JSON.stringify((catalog || {}).status || {}).slice(0, 1500));
}

// --------------------------------------------------------------- the run

async function main() {
  mkdirSync(ARTIFACTS, { recursive: true });
  assertSafeNamespace(namespace);
  check(kube(["version", "--client=true"], { expected: [0] }).status === 0, "kubectl works");
  result.revision = commandText("git", ["-C", REPO, "rev-parse", "HEAD"]);
  result.apiBinarySha256 = commandText("shasum", ["-a", "256", API_BIN]);
  check(result.revision !== null, "the harness could not record its source revision");
  check(result.apiBinarySha256 !== null, "the harness could not hash its API binary");

  // THE ONE RECONCILER. The shared lab's controller watches every namespace;
  // no other Logweir controller may exist, or "the controller reconciled it"
  // would not name one.
  const labPods = kubeJson(["-n", LAB, "get", "pods", "-l",
    "app.kubernetes.io/component=control-plane"]).items;
  check(labPods.length === 1, "expected exactly one lab controller pod");
  const others = kubeJson(["get", "deployments", "-A"]).items.filter((d) =>
    String(d.metadata.name).indexOf("weirkeeper") !== -1 && d.metadata.namespace !== LAB);
  check(others.length === 0, "another weirkeeper deployment exists: " +
    JSON.stringify(others.map((d) => d.metadata.namespace + "/" + d.metadata.name)));
  result.controller = {
    namespace: LAB, pod: labPods[0].metadata.name,
    image: labPods[0].spec.containers[0].image,
    imageID: (labPods[0].status.containerStatuses || [{}])[0].imageID,
    otherWeirkeeperDeployments: 0,
    readOnly: "observed only; nothing in " + LAB + " was changed",
  };

  const existingNamespace = kube(["get", "namespace", namespace, "-o", "json"],
    { expected: [0, 1] });
  check(existingNamespace.status !== 0,
    "refusing to reuse an existing namespace: the bucket and every run must be this run's");
  kube(["create", "namespace", namespace]);
  kube(["label", "namespace", namespace, OWNER_LABEL]);
  const ns = kubeJson(["get", "namespace", namespace]);
  result.namespaceUid = ns.metadata.uid;
  result.created.push({ kind: "Namespace", name: namespace, uid: ns.metadata.uid });

  // THE RUNNER NAMESPACE PREREQUISITES (docs/install.md section 4, low-level
  // path): the no-verb runner account, and the lab's already-established
  // signer, whose public half the cluster TrustRoster `default` carries.
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "v1", kind: "ServiceAccount",
      metadata: { name: "logweir-runner", labels: { "logweir.dev/test-owner": OWNER } },
      automountServiceAccountToken: false,
    }),
  });
  copyLabSecret("logweir-signing-key", "logweir-signing-key");

  const source = seedConnection("orders-" + suffix, LAB_KAFKA, "source", "source-scram");
  const broken = seedConnection("nowhere-" + suffix, UNREACHABLE_KAFKA, "source");
  const target = seedConnection("target-" + suffix, LAB_TARGET, "target", "target-scram");
  const destination = seedDestination("primary-" + suffix);

  // THE DURABLE CATALOG, reconciled by the lab controller from this run's own
  // bucket. Synced once while the bucket is empty, so the first history below
  // reads a real, empty view rather than a catalog that has never answered.
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "RecoveryCatalog",
      metadata: { name: CATALOG, labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        destinationRef: { name: destination },
        sync: { deepCheck: "ManifestDigest", intervalSeconds: 0, maxObjectsPerRun: 1000,
          mode: "Full", viewLimit: 100 },
        syncRequest: "empty",
      },
    }),
  });
  const emptyCatalog = await syncCatalog("empty");
  result.catalog = { name: CATALOG, uid: emptyCatalog.metadata.uid,
    initialCounts: (emptyCatalog.status || {}).counts };

  const port = await freePort();
  result.port = port;
  await startApi(port);
  const base = "http://127.0.0.1:" + port + "/ui/";
  const apiBase = "http://127.0.0.1:" + port + "/api/v1/namespaces/" +
    encodeURIComponent(namespace);
  async function apiGet(path) {
    const response = await fetch(apiBase + path);
    const body = await response.text();
    check(response.ok, "GET " + path + " answered " + response.status + ": " + body.slice(0, 600));
    return JSON.parse(body);
  }
  async function catalogPoints(label) {
    const page = await apiGet("/catalogs/" + CATALOG + "/points?limit=200");
    writeFileSync(join(ARTIFACTS, "catalog-points-" + label + ".json"),
      JSON.stringify(page, null, 2) + "\n");
    return page;
  }

  const browser = await chromium.launch();
  const context = await browser.newContext();
  const page = await context.newPage();

  const bodies = [];
  page.on("response", async (response) => {
    try {
      const url = response.url();
      if (url.indexOf("/api/v1/") !== -1) {
        bodies.push({ url: url, status: response.status(), body: await response.text() });
      }
    } catch (gone) {
      // covered by the DOM assertions
    }
  });
  page.on("request", (request) => {
    const url = request.url();
    if (url.indexOf("/api/v1/") !== -1 && request.method() !== "GET") {
      result.requests.push({ method: request.method(), url: url,
        body: String(request.postData() || "").slice(0, 20000) });
    }
  });

  const listRoute = base + "#/schedules?ns=" + namespace;
  const detailOf = (name) => base + "#/schedules?ns=" + namespace + "&name=" + name;

  /** The history row for one run, read from the rendered table. */
  async function historyRow(runName, section) {
    return page.evaluate(([name, sel]) => {
      const root = document.querySelector(sel);
      if (root === null) {
        return null;
      }
      const row = Array.from(root.querySelectorAll("tbody tr")).find((tr) =>
        tr.innerText.indexOf(name) !== -1);
      if (row === undefined) {
        return null;
      }
      const cells = Array.from(row.querySelectorAll("td"));
      const cellText = (i) => (cells[i] === undefined ? "" : cells[i].innerText.trim());
      const greens = (i) => (cells[i] === undefined ? 0
        : cells[i].querySelectorAll(".badge-green").length);
      const restore = row.querySelector("a[href^=\"#/restore\"]");
      return {
        text: row.innerText, phase: cellText(2), backupSet: cellText(4),
        availability: cellText(7), verification: cellText(8),
        availabilityGreens: greens(7), verificationGreens: greens(8),
        greens: row.querySelectorAll(".badge-green").length,
        restoreHref: restore === null ? null : restore.getAttribute("href"),
      };
    }, [runName, section || "#schedule-history"]);
  }

  /** Clicks Back up now on the open detail and returns the ONE run it made. */
  async function backUpNow(scheduleName) {
    const before = backups().map((b) => b.metadata.name);
    const again = await page.$("button[data-run-again=\"" + scheduleName + "\"]");
    if (again !== null) {
      await again.click();
      await pause(500);
    }
    await waitForSelector(page, "form.run-now-form", "the manual-run panel");
    await page.click("form.run-now-form button[type=submit]");
    let made = [];
    for (let i = 0; i < 40 && made.length === 0; i += 1) {
      await pause(250);
      made = backups().filter((b) => before.indexOf(b.metadata.name) === -1);
    }
    check(made.length === 1, "one click produced " + made.length + " run(s)");
    result.created.push({ kind: "Backup", name: made[0].metadata.name,
      uid: made[0].metadata.uid, createdBy: "the page (Back up now)" });
    return made[0];
  }

  /** Moves a focused `<select>` to the wanted value WITH THE KEYBOARD ALONE.
   *
   *  Two keyboard idioms, tried in order, because a `<select>` answers to both
   *  and a headless engine does not always answer to the first: type-ahead (a
   *  letter jumps to the next option whose label starts with it) and the arrow
   *  keys. Which one moved it is recorded, so the journey says what a person
   *  would actually have pressed. No pointer event is sent by either. */
  async function keyboardSelect(selector, wanted, letter) {
    const read = () => page.evaluate((sel) => document.querySelector(sel).value, selector);
    const how = [];
    for (let i = 0; i < 8 && (await read()) !== wanted; i += 1) {
      await page.keyboard.press(letter);
      how.push(letter);
    }
    if ((await read()) !== wanted) {
      await page.keyboard.press("Home");
      how.push("Home");
      for (let i = 0; i < 16 && (await read()) !== wanted; i += 1) {
        await page.keyboard.press("ArrowDown");
        how.push("ArrowDown");
      }
    }
    const landed = await read();
    check(landed === wanted, "the keyboard did not reach " + wanted + " on " + selector +
      "; landed on " + landed + " after " + JSON.stringify(how) + ". Options: " +
      JSON.stringify(await page.evaluate((sel) =>
        Array.from(document.querySelector(sel).options).map((o) => [o.value, o.text]),
      selector)));
    return how;
  }

  /** Chooses the saved connection by its UID, which is what the selector's
   *  option values are. */
  async function chooseSource(name) {
    const object = kubeJson(["-n", namespace, "get", "kafkacluster", name]);
    await page.selectOption("#schedule-source", object.metadata.uid);
    return object.metadata.uid;
  }

  /** The advanced-cron create path: the form's own fields, one submit. */
  async function createAdvanced(name, cron, topics) {
    result.reloads = (result.reloads || 0) +
      await openRoute(page, listRoute, "#schedule-form", "the create form for " + name);
    await page.fill("#schedule-name", name);
    await chooseSource(source);
    await page.selectOption("#policy-create-mode", "advanced");
    await waitForSelector(page, "#policy-create-cron", "the advanced cron input");
    await page.fill("#policy-create-cron", cron);
    await page.fill("#policy-create-topics", topics);
    await page.selectOption("#policy-create-destination", destination);
    await page.click("#schedule-form button[type=submit]");
    await waitForSelector(page, "#schedule-detail", "the redirect after creating " + name);
    const made = schedules().filter((s) => s.spec.schedule === cron);
    check(made.length === 1, "exactly one schedule with " + cron + " exists");
    result.created.push({ kind: "BackupSchedule", name: made[0].metadata.name,
      uid: made[0].metadata.uid, createdBy: "the page" });
    return made[0];
  }

  try {
    // =================================================================== 1
    // PLAT-10.1 selected-topic creation + first-run redirect.
    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the schedules route");
    await shot(page, "01-form");

    // NEGATIVE CONTROL 1a: the submit is refused before the API has compiled
    // the preset, and NOTHING reaches the cluster.
    const beforeAny = schedules().length;
    await page.fill("#schedule-name", "nightly-" + suffix);
    await chooseSource(source);
    await page.selectOption("#policy-create-mode", "daily");
    await waitForSelector(page, "#policy-create-hour", "the preset's parameters");
    await page.fill("#policy-create-hour", "2");
    await page.fill("#policy-create-minute", "30");
    await page.fill("#policy-create-timeZone", "Europe/Berlin");
    await page.fill("#policy-create-topics", "orders, payments");
    await page.selectOption("#policy-create-destination", destination);
    const submitDisabled = await page.isDisabled("#schedule-form button[type=submit]");
    check(submitDisabled,
      "the Create button is enabled before the cadence has been compiled by the API");
    await page.evaluate(() => {
      document.querySelector("#schedule-form").dispatchEvent(
        new Event("submit", { bubbles: true, cancelable: true }));
    });
    await pause(1000);
    check(schedules().length === beforeAny,
      "a submit with no compiled expression created a schedule");
    control("a preset cannot be created before the API has compiled it", {
      submitDisabled: submitDisabled, schedulesBefore: beforeAny,
      schedulesAfter: schedules().length,
    });

    await page.click("button[data-preview=\"" + "create" + "\"]");
    await waitForText(page, "This cadence compiles to", "the cadence preview");
    const canonical = await page.evaluate(() => {
      const node = document.querySelector("[data-canonical]");
      return node === null ? "" : node.getAttribute("data-canonical");
    });
    check(canonical.length > 0, "the preview published no canonical expression");
    await shot(page, "02-previewed");

    await page.click("#schedule-form button[type=submit]");
    await waitForSelector(page, "#schedule-detail", "the first-run redirect");
    const created = schedules();
    check(created.length === beforeAny + 1, "exactly one schedule was created");
    const selected = created[created.length - 1];
    check(selected.spec.schedule === canonical,
      "the stored expression is not the one the API compiled: " + selected.spec.schedule +
        " vs " + canonical);
    check(selected.spec.timeZone === "Europe/Berlin", "the zone was not stored");
    check(JSON.stringify(selected.spec.destinationRef) === JSON.stringify({ name: destination }),
      "the saved destination was not sent: " + JSON.stringify(selected.spec));
    check(selected.spec.archive.url === "logweir-destination://" + destination,
      "the sentinel URL was not built by the API: " + selected.spec.archive.url);
    check(selected.spec.archive.secretRef === undefined,
      "a destination-backed schedule carries no inline credential");
    check(JSON.stringify(selected.spec.topics) === JSON.stringify(["orders", "payments"]),
      "the named allowlist was not stored");
    const detailHash = await page.evaluate(() => window.location.hash);
    check(detailHash.indexOf("name=" + selected.metadata.name) !== -1,
      "the redirect did not land on the created schedule: " + detailHash);
    await shot(page, "03-detail-after-create");
    result.created.push({ kind: "BackupSchedule", name: selected.metadata.name,
      uid: selected.metadata.uid, createdBy: "the page" });
    record("PLAT-10.1 selected-topic creation through the guided form, and the first-run redirect", {
      schedule: selected.metadata.name, uid: selected.metadata.uid,
      canonicalExpression: canonical, storedExpression: selected.spec.schedule,
      timeZone: selected.spec.timeZone, destinationRef: selected.spec.destinationRef,
      sentinelUrl: selected.spec.archive.url, topics: selected.spec.topics,
      landedOn: detailHash,
    });

    // =================================================================== 2
    // PLAT-10.1 all-user-topic creation, with exclusions.
    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the create form again");
    await page.fill("#schedule-name", "dynamic-" + suffix);
    await chooseSource(source);
    await page.selectOption("#policy-create-mode", "hourly");
    await waitForSelector(page, "#policy-create-minute", "the hourly preset");
    await page.fill("#policy-create-minute", "5");
    await page.selectOption("#policy-create-selection", "dynamic");
    await waitForSelector(page, "#policy-create-incompleteDiscovery", "the dynamic block");
    await page.selectOption("#policy-create-destination", destination);

    const beforeDynamic = schedules().length;
    await page.click("button[data-preview=\"" + "create" + "\"]");
    await waitForText(page, "This cadence compiles to", "the hourly preview");
    await page.click("#schedule-form button[type=submit]");
    await pause(1000);
    check(schedules().length === beforeDynamic,
      "a dynamic selection with no incompleteDiscovery created a schedule");
    await waitForText(page, "There is no default", "the incompleteDiscovery refusal");
    await shot(page, "04-dynamic-refused");
    control("a dynamic selection with no incompleteDiscovery answer is refused", {
      schedulesBefore: beforeDynamic, schedulesAfter: schedules().length,
    });

    await page.selectOption("#policy-create-incompleteDiscovery", "Refuse");
    await page.fill("#policy-create-excludePrefixes", "dev-");
    await page.click("button[data-preview=\"" + "create" + "\"]");
    await waitForText(page, "This cadence compiles to", "the hourly preview again");
    await page.click("#schedule-form button[type=submit]");
    await waitForSelector(page, "#schedule-detail", "the redirect after the dynamic create");
    const dynamic = schedules().filter((s) => s.spec.allUserTopics !== undefined);
    check(dynamic.length === 1, "exactly one dynamic schedule exists");
    check(JSON.stringify(dynamic[0].spec.topics) === JSON.stringify([]),
      "a dynamic schedule named topics: " + JSON.stringify(dynamic[0].spec.topics));
    check(dynamic[0].spec.allUserTopics.incompleteDiscovery === "Refuse",
      "the incompleteDiscovery answer was not stored");
    check(JSON.stringify(dynamic[0].spec.allUserTopics.exclude.prefixes) ===
      JSON.stringify(["dev-"]), "the exclusion prefix was not stored");
    await shot(page, "05-dynamic-created");
    result.created.push({ kind: "BackupSchedule", name: dynamic[0].metadata.name,
      uid: dynamic[0].metadata.uid, createdBy: "the page" });
    record("PLAT-10.1 all-user-topic creation with exclusions", {
      schedule: dynamic[0].metadata.name,
      allUserTopics: dynamic[0].spec.allUserTopics,
      topics: dynamic[0].spec.topics,
    });

    // =================================================================== 3
    // PLAT-10.1 invalid cron: the API's refusal, and the draft survives it.
    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the create form for the cron journey");
    const beforeCron = schedules().length;
    const cronPost = result.requests.length;
    await page.fill("#schedule-name", "badcron-" + suffix);
    await chooseSource(source);
    await page.selectOption("#policy-create-mode", "advanced");
    await waitForSelector(page, "#policy-create-cron", "the advanced cron input");
    await page.fill("#policy-create-cron", "61 * * * *");
    await page.fill("#policy-create-topics", "orders, payments");
    await page.selectOption("#policy-create-destination", destination);
    await page.click("#schedule-form button[type=submit]");
    await pause(1500);
    check(schedules().length === beforeCron, "an invalid cron created a schedule");
    const refusedResponses = bodies.filter((b) => b.status === 422 &&
      (b.url.indexOf("/schedules") !== -1 || b.url.indexOf("/cadence-previews") !== -1));
    check(refusedResponses.length >= 1,
      "the API was never asked, so the refusal is not the API's own words");
    const apiWords = refusedResponses[refusedResponses.length - 1].body;
    const shown = await text(page);
    const apiDetail = (() => {
      try {
        const problem = JSON.parse(apiWords);
        return String(((problem.errors || [])[0] || {}).message || problem.detail || "");
      } catch (notJson) {
        return "";
      }
    })();
    check(apiDetail.length > 0 && shown.indexOf(apiDetail.toLowerCase()) !== -1,
      "the API's own refusal words " + JSON.stringify(apiDetail) + " are not on screen. Saw:\n" +
        shown.slice(0, 1500));
    const keptCron = await page.inputValue("#policy-create-cron");
    const keptTopics = await page.inputValue("#policy-create-topics");
    const keptName = await page.inputValue("#schedule-name");
    check(keptCron === "61 * * * *", "the refused cadence was lost from the draft");
    check(keptTopics === "orders, payments", "the topic list was lost from the draft");
    check(keptName === "badcron-" + suffix, "the name was lost from the draft");
    const invalidMarked = await page.evaluate(() => {
      const node = document.querySelector("#policy-create-cron");
      return node === null ? null : node.getAttribute("aria-invalid");
    });
    await shot(page, "06-invalid-cron");
    record("PLAT-10.1 an invalid cron is refused by the API's own words and the draft is retained", {
      typed: "61 * * * *", keptCron: keptCron, keptTopics: keptTopics, keptName: keptName,
      ariaInvalid: invalidMarked, schedulesUnchanged: schedules().length === beforeCron,
      apiRefusalOnScreen: apiDetail,
      requestsDuring: result.requests.slice(cronPost).map((r) => r.method + " " + r.url),
    });

    // NEGATIVE CONTROL 3a: the same form with a VALID advanced cron creates.
    await page.fill("#policy-create-cron", "7 4 * * *");
    await page.click("#schedule-form button[type=submit]");
    await waitForSelector(page, "#schedule-detail", "the redirect after a valid advanced cron");
    check(schedules().length === beforeCron + 1, "a valid advanced cron did not create");
    const advanced = schedules().filter((s) => s.spec.schedule === "7 4 * * *");
    check(advanced.length === 1, "the valid expression was stored verbatim");
    control("the same form with a valid advanced cron creates", {
      expression: "7 4 * * *", schedule: advanced[0].metadata.name,
    });
    result.created.push({ kind: "BackupSchedule", name: advanced[0].metadata.name,
      uid: advanced[0].metadata.uid, createdBy: "the page" });

    // =================================================================== 4
    // PLAT-10.1 readiness failure: a real Preflight, reconciled by the lab
    // controller to a TERMINAL notReady, and that verdict on screen.
    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the create form for the readiness journey");
    const unchecked = await page.evaluate(() =>
      document.querySelector("[data-readiness=\"unchecked\"]") !== null);
    check(unchecked, "the form did not say readiness was unchecked before any check");
    control("readiness is unchecked, and never ready, before a check is started", {
      sawUncheckedMarker: unchecked,
    });
    await page.fill("#schedule-name", "unreachable-" + suffix);
    await chooseSource(broken);
    await page.fill("#policy-create-topics", "orders");
    await page.selectOption("#policy-create-destination", destination);
    await page.click("#schedule-check-readiness");
    await waitForSelector(page, "#schedule-readiness-verdict .preflight-result",
      "the readiness verdict");
    const badgeSelector = "#schedule-readiness-verdict .preflight-head .badge";
    const initialBadge = (await page.textContent(badgeSelector)).trim();
    let preflights = [];
    let badgeText = initialBadge;
    // The form re-reads the check itself (20 x 2 s); wait for the CONTROLLER's
    // terminal phase AND for the page's own re-read to paint it.
    for (let attempt = 0; attempt < 90; attempt += 1) {
      preflights = kubeJson(["-n", namespace, "get", "preflights"]).items;
      badgeText = (await page.textContent(badgeSelector)).trim();
      const terminal = preflights.some((item) =>
        ["Completed", "Failed", "Cancelled"].includes(String((item.status || {}).phase || "")));
      if (terminal && badgeText !== "running" && badgeText !== "pending" && badgeText !== "queued") {
        break;
      }
      await pause(1000);
    }
    check(preflights.length === 1, "the readiness button created " + preflights.length +
      " Preflight object(s)");
    const preflight = preflights[0];
    const phase = String((preflight.status || {}).phase || "");
    check(["Completed", "Failed", "Cancelled"].includes(phase),
      "the lab controller did not record a terminal Preflight phase: " + phase);
    const terminalResponse = await apiGet("/preflights/" +
      encodeURIComponent(preflight.metadata.name));
    const answered = terminalResponse.item;
    writeFileSync(join(ARTIFACTS, "preflight-terminal-response.json"),
      JSON.stringify(terminalResponse, null, 2) + "\n");
    check(answered.id === preflight.metadata.name,
      "the readiness answer is about a different Preflight than the one in the cluster");
    // THE REFUSAL THIS ROW REQUIRES: the terminal verdict is notReady, and the
    // page shows exactly that word -- not `running`, not `ready`, not green.
    check(answered.state === "notReady",
      "the terminal route state for an unreachable source is " + answered.state +
        ", not notReady");
    check(badgeText === "not ready",
      "the page shows " + JSON.stringify(badgeText) + " for a terminal notReady check");
    const greenVerdict = await page.evaluate(() =>
      document.querySelectorAll("#schedule-readiness-verdict .preflight-head .badge-green")
        .length);
    check(greenVerdict === 0, "a green readiness badge was rendered for an unreachable source");
    await shot(page, "07-readiness-not-ready");
    record("PLAT-10.1 readiness failure: a real Preflight reconciled terminal by the lab controller, rendered as not ready", {
      preflight: preflight.metadata.name, uid: preflight.metadata.uid,
      source: broken, bootstrapServers: UNREACHABLE_KAFKA,
      terminalPhase: phase, routeState: answered.state, routeTerminal: answered.terminal,
      completeCondition: ((preflight.status || {}).conditions || [])
        .filter((c) => c.type === "Complete").map((c) => c.reason + ": " + c.message),
      badgeInitially: initialBadge, badgeOnScreen: badgeText, greenBadges: greenVerdict,
      schedulesCreated: 0,
    });

    // =================================================================== 5
    // PLAT-10.2 empty history, then a real run: running, then Succeeded.
    const detailRoute = detailOf(selected.metadata.name);
    result.reloads = (result.reloads || 0) +
      await openRoute(page, detailRoute, "#schedule-detail", "the schedule detail route");
    await waitForText(page, "runs and recovery points", "the history section");
    await waitForText(page, "this schedule has produced no run yet", "the empty history");
    const noRestore = await page.evaluate(() =>
      document.querySelector("#schedule-restore-latest") === null);
    check(noRestore, "an empty history offered a page-level Restore");
    await shot(page, "08-detail-empty-history");
    record("PLAT-10.2 empty history says what empty means and offers no Restore", {
      route: "#/schedules?ns=&name=", schedule: selected.metadata.name,
      pageLevelRestoreOffered: !noRestore,
    });

    const runA = await backUpNow(selected.metadata.name);
    check(runA.spec.scheduleRef.name === selected.metadata.name &&
      runA.spec.scheduleRef.uid === selected.metadata.uid,
      "the run does not carry the schedule's identity");
    // RUNNING: the history re-read while the lab controller's Job is still
    // going. The row must say so, and offer no restore yet.
    let runningRow = null;
    let runningPhaseInCluster = null;
    for (let i = 0; i < 20; i += 1) {
      await page.goto(detailRoute, { waitUntil: "load", timeout: 30000 });
      await waitForSelector(page, "#schedule-history", "the history while the run is live");
      const row = await historyRow(runA.metadata.name);
      if (row !== null && row.phase.length > 0 && row.phase !== "-") {
        runningRow = row;
        runningPhaseInCluster = String(((kubeJson(["-n", namespace, "get", "backup",
          runA.metadata.name]).status) || {}).phase || "");
        break;
      }
      await pause(300);
    }
    check(runningRow !== null, "the new run never appeared in the history");
    await shot(page, "09-history-run-running");
    check(["Succeeded", "Failed", "Cancelled"].indexOf(runningRow.phase) === -1,
      "the first rendered phase was already terminal (" + runningRow.phase +
        "); this row did not observe a running run");
    check(runningRow.restoreHref === null, "a run still going offered a Restore");
    check(runningRow.greens === 0, "a run still going was badged green");
    record("PLAT-10.2 a running run: the history shows its live phase and offers no restore", {
      run: runA.metadata.name, phaseOnScreen: runningRow.phase,
      phaseInClusterJustAfter: runningPhaseInCluster, greens: runningRow.greens,
      restoreOffered: runningRow.restoreHref !== null,
    });

    const terminalA = await waitForTerminalBackup(runA.metadata.name, "run A");
    check(terminalA.status.phase === "Succeeded",
      "run A did not succeed: " + JSON.stringify(terminalA.status).slice(0, 1500));
    await page.goto(detailRoute, { waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "#schedule-history", "the history after run A");
    const rowA0 = await historyRow(runA.metadata.name);
    check(rowA0 !== null && rowA0.phase === "Succeeded", "run A's row is not Succeeded");
    check(rowA0.availability === NOT_IN_CATALOG && rowA0.verification === NOT_IN_CATALOG,
      "a run the synced catalog has not seen did not say so: " + JSON.stringify(rowA0));
    check(rowA0.greens === 0, "a run the catalog has never seen was badged green");
    await shot(page, "10-history-not-in-catalog");
    const ownVerification = ((terminalA.status.evidence || {}).verification) || {};
    record("PLAT-10.2 a succeeded run the durable catalog has not yet seen is neither available nor unavailable", {
      run: runA.metadata.name, uid: runA.metadata.uid, backupId: terminalA.status.backupId,
      phase: terminalA.status.phase, catalogSyncedBefore: "empty",
      availabilityOnScreen: rowA0.availability, verificationOnScreen: rowA0.verification,
      greens: rowA0.greens,
      runOwnVerification: { result: ownVerification.result,
        detail: String(ownVerification.detail || "").slice(0, 300) },
    });

    // =================================================================== 6
    // PLAT-10.1 edit from the detail; a later REAL run carries the revision.
    result.reloads = (result.reloads || 0) +
      await openRoute(page, detailRoute, "form.policy-form", "the policy form on the detail");
    const beforeEdit = kubeJson(["-n", namespace, "get", "backupschedule",
      selected.metadata.name]);
    const panel = "form.policy-form[data-name=\"" + selected.metadata.name + "\"]";
    await page.fill(panel + " [name=\"topics\"]", "orders");
    await page.click("button[data-preview=\"" + selected.metadata.name + "\"]");
    await waitForText(page, "this cadence compiles to", "the edit preview");
    await page.click(panel + " button[type=submit]");
    let afterEdit = beforeEdit;
    for (let i = 0; i < 20 && afterEdit.metadata.generation === beforeEdit.metadata.generation; i += 1) {
      await pause(500);
      afterEdit = kubeJson(["-n", namespace, "get", "backupschedule", selected.metadata.name]);
    }
    check(afterEdit.metadata.generation > beforeEdit.metadata.generation,
      "the edit did not move the revision");
    check(JSON.stringify(afterEdit.spec.topics) === JSON.stringify(["orders"]),
      "the edited allowlist was not stored: " + JSON.stringify(afterEdit.spec.topics));
    check(JSON.stringify(afterEdit.spec.destinationRef) === JSON.stringify({ name: destination }),
      "the whole-policy replace dropped the destination");
    check(afterEdit.spec.timeZone === "Europe/Berlin", "the whole-policy replace dropped the zone");
    await shot(page, "11-policy-edited");
    record("PLAT-10.1 editing the future policy from the detail makes a new revision", {
      schedule: selected.metadata.name,
      fromGeneration: beforeEdit.metadata.generation,
      toGeneration: afterEdit.metadata.generation,
      topics: afterEdit.spec.topics, destinationRef: afterEdit.spec.destinationRef,
    });

    await page.goto(detailRoute, { waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "form.run-now-form", "the run-now panel after the edit");
    const runB = await backUpNow(selected.metadata.name);
    check(runB.spec.scheduleRef.generation === afterEdit.metadata.generation,
      "the later run froze g" + runB.spec.scheduleRef.generation + ", not the new g" +
        afterEdit.metadata.generation);
    const frozenA = kubeJson(["-n", namespace, "get", "backup", runA.metadata.name]);
    check(frozenA.spec.scheduleRef.generation === beforeEdit.metadata.generation,
      "the edit reached a run that already existed");
    control("the run taken before the edit still carries the revision it froze", {
      run: runA.metadata.name, generation: frozenA.spec.scheduleRef.generation,
      editedTo: afterEdit.metadata.generation,
    });
    const terminalB = await waitForTerminalBackup(runB.metadata.name, "run B");
    check(terminalB.status.phase === "Succeeded",
      "run B did not succeed: " + JSON.stringify(terminalB.status).slice(0, 1500));
    record("PLAT-10.1 a real run taken after the edit carries the new revision", {
      run: runB.metadata.name, uid: runB.metadata.uid,
      frozenGeneration: runB.spec.scheduleRef.generation, phase: terminalB.status.phase,
      topics: terminalB.spec.topics,
    });

    // =================================================================== 7
    // PLAT-10.2 a FAILED run, from the lab controller: a schedule naming a
    // topic the lab source does not hold.
    const failing = await createAdvanced("failing-" + suffix, "11 1 * * *",
      "p10-absent-" + suffix);
    await page.goto(detailOf(failing.metadata.name), { waitUntil: "load", timeout: 30000 });
    const runF = await backUpNow(failing.metadata.name);
    const terminalF = await waitForTerminalBackup(runF.metadata.name, "the failing run");
    check(terminalF.status.phase === "Failed",
      "a run naming an absent topic did not fail: " + terminalF.status.phase);
    await page.goto(detailOf(failing.metadata.name), { waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "#schedule-history", "the failing schedule's history");
    const rowF = await historyRow(runF.metadata.name);
    check(rowF !== null && rowF.phase === "Failed", "the failed run's row does not say Failed");
    // THE REFUSAL: a failed run is not a recovery point.
    check(rowF.restoreHref === null, "a failed run offered a Restore");
    check(rowF.greens === 0, "a failed run was badged green");
    const noLatest = await page.evaluate(() =>
      document.querySelector("#schedule-restore-latest") === null);
    check(noLatest, "a schedule whose only run failed offered a page-level Restore");
    await shot(page, "12-history-failed-run");
    record("PLAT-10.2 a failed run from the lab controller: Failed, never green, no restore", {
      schedule: failing.metadata.name, run: runF.metadata.name, uid: runF.metadata.uid,
      phase: terminalF.status.phase, exitReason: terminalF.status.exitReason,
      failedCondition: ((terminalF.status.conditions || []).find((c) => c.type === "Failed") || {}),
      phaseOnScreen: rowF.phase, restoreOffered: rowF.restoreHref !== null,
      pageLevelRestoreOffered: !noLatest,
    });

    // =================================================================== 8
    // PLAT-10.2 VERIFIED runs: the lab controller's catalog sync over this
    // run's bucket, and both points healthy on the detail.
    const synced = await syncCatalog("after-two-runs");
    const healthyPage = await catalogPoints("healthy");
    check(healthyPage.incomplete === undefined, "a complete view carried `incomplete`");
    const pointOf = (pageBody, run) => (pageBody.items || []).filter((p) =>
      p.backupId === run.status.backupId);
    const pA = pointOf(healthyPage, terminalA);
    const pB = pointOf(healthyPage, terminalB);
    check(pA.length >= 1 && pB.length >= 1, "the catalog does not list both runs' points: " +
      JSON.stringify((healthyPage.items || []).map((p) => p.backupId)));
    for (const p of pA.concat(pB)) {
      check(p.availability === "Available", "a fresh point is " + p.availability);
      check(p.verification === "Verified" || p.verification === "VerifiedHistorical",
        "a fresh point's catalog verification is " + p.verification);
      check(p.selectable === true, "a fresh verified point is not selectable");
    }
    await page.goto(detailRoute, { waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "#schedule-history", "the history after the sync");
    const rowA1 = await historyRow(runA.metadata.name);
    const rowB1 = await historyRow(runB.metadata.name);
    for (const [row, p] of [[rowA1, pA[0]], [rowB1, pB[0]]]) {
      check(row.availability === p.availability && row.availabilityGreens >= 1,
        "a healthy point's availability is not the catalog's green word: " + JSON.stringify(row));
      check(row.verification === p.verification && row.verificationGreens >= 1,
        "a verified point's verification is not the catalog's green word: " + JSON.stringify(row));
      check(row.restoreHref !== null, "a healthy point offers no Restore");
    }
    await shot(page, "13-history-healthy-verified");
    record("PLAT-10.2 verified runs: the lab controller's catalog says Available/Verified and the detail renders exactly that", {
      catalog: CATALOG, lastSyncJob: synced.status.lastSyncJob, counts: synced.status.counts,
      signers: synced.status.signers,
      points: pA.concat(pB).map((p) => ({ backupId: p.backupId, pointId: p.pointId,
        availability: p.availability, verification: p.verification,
        selectable: p.selectable, signerKeyId: p.signerKeyId })),
      onScreen: [rowA1, rowB1].map((r) => ({ availability: r.availability,
        verification: r.verification, greens: r.greens })),
    });

    // =================================================================== 9
    // PLAT-10.2 navigation to an OLDER backup: A's own link opens the wizard
    // bound to A, while B is the newest.
    const latestHref = await page.getAttribute("#schedule-restore-latest", "href");
    check(latestHref.indexOf("uid=" + runB.metadata.uid) !== -1,
      "the page-level Restore is not bound to the newest real point: " + latestHref);
    check(rowA1.restoreHref.indexOf("uid=" + runA.metadata.uid) !== -1,
      "run A's row Restore is not bound to run A: " + rowA1.restoreHref);
    result.reloads = (result.reloads || 0) +
      await openRoute(page, base + rowA1.restoreHref, "#point-uid", "the wizard on the older point");
    const boundName = (await page.textContent("#point-name")).trim();
    const boundUid = (await page.textContent("#point-uid")).trim();
    check(boundUid === runA.metadata.uid, "the wizard is bound to " + boundUid);
    check(boundName === runA.metadata.name, "the wizard named " + boundName);
    check(boundUid !== runB.metadata.uid,
      "the wizard substituted the newest point for the one the link named");
    await shot(page, "14-wizard-on-older-point");
    record("PLAT-10.2 navigation to an older real backup opens the wizard bound to it, not the newest", {
      followed: rowA1.restoreHref, boundName: boundName, boundUid: boundUid,
      newestUid: runB.metadata.uid, pageLevelRestore: latestHref,
    });

    // ================================================================== 10
    // PLAT-10.2 an UNAVAILABLE archive beside a healthy one: remove only A's
    // manifest from this run's bucket; the lab controller's next sync says so.
    const manifestKey = namespace + "/" + terminalA.status.backupId + "/manifest.json";
    const removed = mcJob("remove-manifest-a", "mc stat \"p10/$S3_BUCKET/" + manifestKey +
      "\" >/dev/null && echo present; mc rm \"p10/$S3_BUCKET/" + manifestKey + "\"; " +
      "if mc stat \"p10/$S3_BUCKET/" + manifestKey + "\" >/dev/null 2>&1; then echo still-there; " +
      "else echo gone; fi");
    check(removed.indexOf("present") !== -1 && /gone\s*$/.test(removed),
      "the owned manifest was not present-then-gone: " + removed);
    const afterDelete = await syncCatalog("after-manifest-delete");
    const missingPage = await catalogPoints("after-manifest-delete");
    const pA2 = pointOf(missingPage, terminalA);
    const pB2 = pointOf(missingPage, terminalB);
    check(pA2.length >= 1 && pA2.every((p) => p.availability !== "Available" &&
      p.selectable === false), "the catalog still calls A available: " + JSON.stringify(pA2));
    check(pB2.length >= 1 && pB2.every((p) => p.availability === "Available" &&
      p.selectable === true), "the catalog no longer calls B available: " + JSON.stringify(pB2));
    await page.goto(detailRoute, { waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "#schedule-history", "the history after the manifest removal");
    const rowA2 = await historyRow(runA.metadata.name);
    const rowB2 = await historyRow(runB.metadata.name);
    check(rowA2.availability === pA2[0].availability,
      "A's availability on screen is not the catalog's word: " + JSON.stringify(rowA2));
    check(rowA2.availabilityGreens === 0 && rowA2.verificationGreens === 0,
      "an unavailable archive was badged green: " + JSON.stringify(rowA2));
    check(rowB2.availability === "Available" && rowB2.availabilityGreens >= 1 &&
      rowB2.verificationGreens >= 1, "the healthy point beside it lost its green: " +
      JSON.stringify(rowB2));
    await shot(page, "15-history-unavailable-beside-healthy");
    record("PLAT-10.2 an unavailable archive is distinguishable from a healthy point in one history", {
      removedKey: BUCKET + "/" + manifestKey, mcLog: removed,
      counts: afterDelete.status.counts,
      catalogA: pA2.map((p) => [p.availability, p.verification, p.selectable]),
      catalogB: pB2.map((p) => [p.availability, p.verification, p.selectable]),
      onScreenA: [rowA2.availability, rowA2.verification, rowA2.greens],
      onScreenB: [rowB2.availability, rowB2.verification, rowB2.greens],
    });

    // ================================================================== 11
    // DONE EVIDENCE: create -> backup -> schedule detail -> restore, with no
    // configuration reconstructed. B's row opens the wizard bound to B; the
    // harness picks ONLY a target cluster and presses Create.
    result.reloads = (result.reloads || 0) +
      await openRoute(page, base + rowB2.restoreHref, "#point-uid", "the wizard on the healthy point");
    check((await page.textContent("#point-uid")).trim() === runB.metadata.uid,
      "the wizard is not bound to run B");
    await waitForSelector(page, "#target-cluster", "the wizard's target step");
    const targetUid = kubeJson(["-n", namespace, "get", "kafkacluster", target]).metadata.uid;
    await page.selectOption("#target-cluster", targetUid);
    await waitForSelector(page, "#plan-bytes", "the plan preview");
    await pause(1000);
    const previewed = await page.evaluate(() => ({
      bytes: (document.querySelector("#plan-bytes") || {}).textContent || "",
      hash: (document.querySelector("#plan-hash-value") || {}).textContent || "",
    }));
    writeFileSync(join(ARTIFACTS, "restore-previewed-plan.txt"), previewed.bytes);
    await shot(page, "16-wizard-before-create");
    const restoreMarker = result.requests.length;
    await page.click("#create-restore");
    let restorePosts = [];
    for (let i = 0; i < 40 && restorePosts.length === 0; i += 1) {
      await pause(500);
      restorePosts = result.requests.slice(restoreMarker).filter((r) =>
        r.method === "POST" && /\/restores$/.test(r.url));
    }
    check(restorePosts.length === 1, "the wizard sent " + restorePosts.length +
      " restore create(s). Page said:\n" + (await text(page)).slice(0, 1500));
    const restoreBody = JSON.parse(restorePosts[0].body);
    writeFileSync(join(ARTIFACTS, "restore-create-request.json"),
      JSON.stringify(restoreBody, null, 2) + "\n");
    check((restoreBody.sourceDestinationRef || {}).name === destination,
      "the restore request does not carry the schedule's saved destination: " +
        JSON.stringify(restoreBody.sourceDestinationRef));
    check(((restoreBody.sourceArchive || {}).url) === "logweir-destination://" + destination &&
      (restoreBody.sourceArchive || {}).credentialRef === undefined,
      "the restore request reconstructed an archive location or carried a credential: " +
        JSON.stringify(restoreBody.sourceArchive));
    check(restoreBody.planBytes === previewed.bytes, "the submitted plan is not the preview");
    check(previewed.bytes.indexOf(terminalB.status.backupId) !== -1,
      "the plan does not name run B's backup set");
    check(previewed.bytes.indexOf(terminalA.status.backupId) === -1,
      "the plan names run A's backup set");
    let restore = null;
    for (let i = 0; i < 40 && restore === null; i += 1) {
      restore = (kubeJson(["-n", namespace, "get", "restores"]).items || []).find((r) =>
        ((r.spec || {}).approvalRef || {}).name === (restoreBody.approvalRef || {}).name) || null;
      if (restore === null) {
        await pause(500);
      }
    }
    check(restore !== null, "no Restore object carries the wizard's approval reference");
    result.created.push({ kind: "Restore", name: restore.metadata.name,
      uid: restore.metadata.uid, createdBy: "the page (restore wizard)" });
    await shot(page, "17-restore-submitted");
    // ADMISSION: the lab controller holds the Restore until an Approval that
    // the TrustRoster's approver key verifies exists. The approver's private
    // key is not on this host, so no Approval can be minted here.
    let held = null;
    for (let i = 0; i < 60; i += 1) {
      const current = kubeJson(["-n", namespace, "get", "restore", restore.metadata.name]);
      const admitted = ((current.status || {}).conditions || []).find((c) => c.type === "Admitted");
      if (admitted !== undefined) {
        held = current;
        break;
      }
      await pause(1000);
    }
    check(held !== null, "the lab controller never admitted or held the Restore");
    const admitted = held.status.conditions.find((c) => c.type === "Admitted");
    // THE REFUSAL THIS ROW REQUIRES: held, not running, for the approval.
    check(admitted.status === "False" && admitted.reason === "ApprovalNotVerified",
      "the Restore was not held for its approval: " + JSON.stringify(admitted));
    check(["Running", "Succeeded"].indexOf(String(held.status.phase || "")) === -1,
      "a Restore with no verified Approval is " + held.status.phase);
    const approval = kube(["-n", namespace, "get", "approval", restoreBody.approvalRef.name],
      { expected: [0, 1] });
    check(approval.status !== 0, "an Approval exists that this run did not mint");
    writeFileSync(join(ARTIFACTS, "restore-held.json"),
      JSON.stringify({ metadata: { name: held.metadata.name, uid: held.metadata.uid },
        spec: { approvalRef: held.spec.approvalRef, sourceDestinationRef:
          held.spec.sourceDestinationRef, sourceArchive: held.spec.sourceArchive },
        status: held.status }, null, 2) + "\n");
    record("DONE EVIDENCE create -> backup -> detail -> restore: the wizard submits with the schedule's destination and point carried; admission BLOCKED ON HOST KEY MATERIAL", {
      schedule: selected.metadata.name, point: { name: runB.metadata.name,
        uid: runB.metadata.uid, backupId: terminalB.status.backupId },
      harnessTyped: "only the target cluster selection",
      request: { sourceDestinationRef: restoreBody.sourceDestinationRef,
        evidenceDestinationRef: restoreBody.evidenceDestinationRef,
        sourceArchive: restoreBody.sourceArchive, approvalRef: restoreBody.approvalRef },
      restore: { name: held.metadata.name, uid: held.metadata.uid, phase: held.status.phase,
        admitted: admitted },
      approvalObject: "absent (NotFound)",
      blocked: "BLOCKED ON HOST KEY MATERIAL: the approver private key for " +
        "approver@scram-local.invalid is not on this host, so no verifiable Approval can be minted",
    });

    // ================================================================== 12
    // PLAT-10.2 INCOMPLETE evidence: the materialised page the view names
    // disappears; the detail says catalog incomplete, never absent or healthy.
    const pageCm = afterDelete.status.lastSyncJob.name + "-p0";
    const cm = kubeJson(["-n", namespace, "get", "configmap", pageCm]);
    check(String(cm.metadata.name) === pageCm, "the named page ConfigMap does not exist");
    kube(["-n", namespace, "delete", "configmap", pageCm, "--wait=true"]);
    const incompletePage = await catalogPoints("incomplete");
    check(incompletePage.incomplete === true,
      "the API did not name the incomplete view: " + JSON.stringify(incompletePage).slice(0, 400));
    await page.goto(detailRoute, { waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "#schedule-catalog-unreadable", "the incomplete-catalog note");
    const rowA3 = await historyRow(runA.metadata.name);
    const rowB3 = await historyRow(runB.metadata.name);
    const historyText = await page.evaluate(() =>
      document.querySelector("#schedule-history").innerText);
    for (const row of [rowA3, rowB3]) {
      check(row.availability === "catalog incomplete" && row.verification === "catalog incomplete",
        "a row in an incomplete view is not labelled catalog incomplete: " + JSON.stringify(row));
      check(row.greens === 0, "a row in an incomplete view was badged green");
    }
    check(historyText.toLowerCase().indexOf(NOT_IN_CATALOG) === -1,
      "an incomplete view claimed a run is not in the catalog");
    await shot(page, "18-history-catalog-incomplete");
    record("PLAT-10.2 incomplete catalog evidence is labelled catalog incomplete, never absent or healthy", {
      deletedPage: pageCm, apiIncomplete: incompletePage.incomplete,
      apiItems: (incompletePage.items || []).length,
      onScreen: [rowA3, rowB3].map((r) => [r.availability, r.verification, r.greens]),
      sawNotInCatalog: false,
    });

    // ================================================================== 13
    // PLAT-10.2 pause and resume, from the detail.
    result.reloads = (result.reloads || 0) +
      await openRoute(page, detailRoute, "form.suspend", "the suspend toggle on the detail");
    await page.click("form.suspend button[type=submit]");
    await pause(2000);
    const paused = kubeJson(["-n", namespace, "get", "backupschedule", selected.metadata.name]);
    check(paused.spec.suspend === true, "the pause did not suspend the schedule");
    await page.reload({ waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "#schedule-history", "the history while paused");
    const pausedText = await text(page);
    check(pausedText.indexOf("restore this point") !== -1,
      "a paused schedule stopped offering its recovery points");
    await shot(page, "19-paused");
    await waitForSelector(page, "form.suspend", "the resume toggle");
    await page.click("form.suspend button[type=submit]");
    await pause(2000);
    const resumed = kubeJson(["-n", namespace, "get", "backupschedule", selected.metadata.name]);
    check(resumed.spec.suspend === false, "the resume did not un-suspend the schedule");
    await shot(page, "20-resumed");
    record("PLAT-10.2 pause and resume from the detail, with the history still offered while paused", {
      schedule: selected.metadata.name, pausedSpecSuspend: paused.spec.suspend,
      resumedSpecSuspend: resumed.spec.suspend,
      pointsOfferedWhilePaused: pausedText.indexOf("restore this point") !== -1,
    });

    // ================================================================== 14
    // PLAT-10.2 the archived state: a REAL run, then the schedule deleted.
    const doomed = advanced[0].metadata.name;
    await page.goto(detailOf(doomed), { waitUntil: "load", timeout: 30000 });
    const runV = await backUpNow(doomed);
    const terminalV = await waitForTerminalBackup(runV.metadata.name, "the archived schedule's run");
    check(terminalV.status.phase === "Succeeded", "the archived schedule's run did not succeed");
    kube(["-n", namespace, "delete", "backupschedule", doomed, "--wait=true"]);
    const gone = kube(["-n", namespace, "get", "backupschedule", doomed], { expected: [0, 1] });
    check(gone.status !== 0, "the schedule was not deleted");
    check(kube(["-n", namespace, "get", "backup", runV.metadata.name], { expected: [0, 1] })
      .status === 0, "deleting the schedule took its run with it");
    result.reloads = (result.reloads || 0) + await openRoute(page, detailOf(doomed),
      "[data-archived=\"1\"]", "the archived schedule state");
    await waitForText(page, "no longer exists in this namespace", "the archived sentence");
    const archivedRow = await page.evaluate((name) => {
      const row = Array.from(document.querySelectorAll("tbody tr")).find((tr) =>
        tr.innerText.indexOf(name) !== -1);
      return row === undefined ? null : {
        text: row.innerText,
        restore: (row.querySelector("a[href^=\"#/restore\"]") || { getAttribute: () => null })
          .getAttribute("href"),
      };
    }, runV.metadata.name);
    check(archivedRow !== null && archivedRow.restore !== null &&
      archivedRow.restore.indexOf("uid=" + runV.metadata.uid) !== -1,
      "the archived schedule's real run is not restorable by its UID: " +
        JSON.stringify(archivedRow));
    const controls = await page.evaluate(() => ({
      suspend: document.querySelectorAll("form.suspend").length,
      policy: document.querySelectorAll("form.policy-form").length,
      runNow: document.querySelectorAll("form.run-now-form").length,
    }));
    check(controls.suspend === 0 && controls.policy === 0 && controls.runNow === 0,
      "a deleted schedule still offers controls that need one: " + JSON.stringify(controls));
    await shot(page, "21-archived-schedule");
    record("PLAT-10.2 a deleted schedule keeps its real history and offers no schedule controls", {
      schedule: doomed, run: runV.metadata.name, runUid: runV.metadata.uid,
      restoreLink: archivedRow.restore, controlsOffered: controls,
    });

    // ================================================================== 15
    // PLAT-10.1 keyboard-only creation: Tab, arrows, Space and Enter.
    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the create form for the keyboard journey");
    const beforeKeyboard = schedules().length;
    await tabTo(page, "#schedule-name", "the keyboard journey's first field");
    await page.evaluate(() => {
      window.__plat10PointerEvents = 0;
      document.addEventListener("pointerdown", () => { window.__plat10PointerEvents += 1; },
        { capture: true });
    });
    const typedName = "kbd-" + suffix;
    await page.keyboard.type(typedName);
    const sourceUid = kubeJson(["-n", namespace, "get", "kafkacluster", source]).metadata.uid;
    await tabTo(page, "#schedule-source", "the keyboard journey's source selector");
    const sourceKeys = await keyboardSelect("#schedule-source", sourceUid, source[0]);
    await tabTo(page, "#policy-create-mode", "the keyboard journey's cadence mode");
    const modeKeys = await keyboardSelect("#policy-create-mode", "advanced", "a");
    await waitForSelector(page, "#policy-create-cron", "the keyboard journey's cron field");
    await tabTo(page, "#policy-create-cron", "the keyboard journey's cron field");
    await page.keyboard.type("9 3 * * *");
    await tabTo(page, "#policy-create-topics", "the keyboard journey's topic field");
    await page.keyboard.type("orders");
    await tabTo(page, "#policy-create-destination", "the keyboard journey's destination");
    const destinationKeys = await keyboardSelect("#policy-create-destination", destination,
      destination[0]);
    const focusVisible = await page.evaluate(() => {
      const node = document.getElementById("policy-create-cron");
      const style = window.getComputedStyle(node, ":focus-visible");
      return { outline: style.outlineStyle, width: style.outlineWidth };
    });
    await shot(page, "22-keyboard-filled");
    await tabTo(page, "#schedule-form button[type=submit]", "the keyboard journey's Create button");
    await page.keyboard.press("Space");
    await waitForSelector(page, "#schedule-detail", "the redirect after a keyboard-only create");
    const keyboardMade = schedules().filter((s) => s.spec.schedule === "9 3 * * *");
    check(keyboardMade.length === 1,
      "the keyboard-only route did not create a schedule; count " + keyboardMade.length);
    check(keyboardMade[0].spec.sourceRef.name === source,
      "the keyboard-chosen connection was not the one stored");
    check(JSON.stringify(keyboardMade[0].spec.destinationRef) ===
      JSON.stringify({ name: destination }),
      "the keyboard-chosen destination was not stored");
    check(schedules().length === beforeKeyboard + 1, "more than one schedule was created");
    const pointerEvents = await page.evaluate(() => window.__plat10PointerEvents);
    check(pointerEvents === 0, "the keyboard journey sent " + pointerEvents + " pointer event(s)");
    await shot(page, "23-keyboard-created");
    result.created.push({ kind: "BackupSchedule", name: keyboardMade[0].metadata.name,
      uid: keyboardMade[0].metadata.uid, createdBy: "the page (keyboard only)" });
    record("PLAT-10.1 the standard route completes with the keyboard alone: no pointer event was sent", {
      schedule: keyboardMade[0].metadata.name,
      expression: keyboardMade[0].spec.schedule,
      sourceRef: keyboardMade[0].spec.sourceRef,
      destinationRef: keyboardMade[0].spec.destinationRef,
      pointerEvents: pointerEvents,
      keysForTheConnection: sourceKeys,
      keysForTheDestination: destinationKeys,
      keysForTheCadenceMode: modeKeys,
      focusVisibleOutline: focusVisible,
    });

    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the create form for the keyboard control");
    const beforeEmpty = schedules().length;
    await tabTo(page, "#schedule-name", "the keyboard control's first field");
    await page.keyboard.press("Enter");
    await pause(1200);
    check(schedules().length === beforeEmpty,
      "an empty keyboard submit created a schedule");
    control("an empty form submitted with the keyboard creates nothing", {
      schedulesBefore: beforeEmpty, schedulesAfter: schedules().length,
    });

    // ================================================================== 16
    // Migration: the old list deep link still reaches the list.
    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the old deep link still reaches the list");
    const listText = await text(page);
    check(listText.indexOf("create a backupschedule") !== -1,
      "the old `#/schedules?ns=` link no longer renders the list");
    const detailOnList = await page.evaluate(() =>
      document.querySelector("#schedule-detail") !== null);
    check(!detailOnList, "the list route rendered a detail");
    await shot(page, "24-old-deep-link");
    record("migration: the pre-PLAT-10.2 deep link `#/schedules?ns=` still reaches the list", {
      route: "#/schedules?ns=" + namespace, renderedDetail: detailOnList,
    });
  } finally {
    writeFileSync(join(ARTIFACTS, "api.log"), apiLog.join(""));
    writeFileSync(join(ARTIFACTS, "responses.json"),
      JSON.stringify(bodies.map((b) => ({ url: b.url, status: b.status,
        body: b.body.slice(0, 20000) })), null, 2) + "\n");
    // THE CLUSTER STATE THIS RUN LEFT, dumped before anything is deleted.
    for (const kind of ["backupschedules", "backups", "preflights", "restores",
      "recoverycatalogs", "backupdestinations", "kafkaclusters"]) {
      writeFileSync(join(ARTIFACTS, "kubectl-" + kind + ".json"),
        kube(["-n", namespace, "get", kind, "-o", "json"], { expected: [0, 1] }).stdout);
    }
    // What the ONE reconciler said about this namespace.
    const labLog = kube(["-n", LAB, "logs", "deploy/weirkeeper", "--since=2h"],
      { expected: [0, 1], timeout: 60000 }).stdout;
    writeFileSync(join(ARTIFACTS, "lab-controller-log-this-namespace.txt"),
      labLog.split("\n").filter((line) => line.indexOf(namespace) !== -1).join("\n") + "\n");
    await browser.close();
  }
}

// -------------------------------------------------------------- the cleanup

function cleanUp(how) {
  try {
    const before = kube(["get", "namespace", namespace, "-o", "json"], { expected: [0, 1] });
    if (before.status !== 0) {
      result.cleanup.push({ namespace: namespace, how: how, alreadyGone: true });
      return;
    }
    const object = JSON.parse(before.stdout);
    if (object.metadata.uid !== result.namespaceUid) {
      result.cleanup.push({
        namespace: namespace, how: how, refused: true,
        why: "the namespace under this name is not the one this run created",
        expectedUid: result.namespaceUid, foundUid: object.metadata.uid,
      });
      return;
    }
    if ((object.metadata.labels || {})["logweir.dev/test-owner"] !== OWNER) {
      result.cleanup.push({
        namespace: namespace, how: how, refused: true,
        why: "the namespace does not carry this run's owner label",
      });
      return;
    }
    const objects = kube(["-n", namespace, "get",
      "backupschedules,backups,kafkaclusters,backupdestinations,preflights,restores," +
        "recoverycatalogs,secrets", "-o", "name"],
      { expected: [0, 1] }).stdout;
    if (process.env.UI_E2E_KEEP === "1") {
      result.cleanup.push({ namespace: namespace, how: how, kept: true });
      return;
    }
    // THE BUCKET FIRST, while this namespace's credential copy still exists.
    // It is this run's own bucket (created with `mc mb`, never --ignore-existing).
    let bucket = null;
    if (result.bucket !== undefined) {
      try {
        bucket = mcJob("remove-bucket", "mc rb --force \"p10/$S3_BUCKET\"; " +
          "if mc ls \"p10/$S3_BUCKET\" >/dev/null 2>&1; then echo bucket-still-there; " +
          "else echo bucket-gone; fi");
      } catch (failed) {
        bucket = "FAILED: " + (failed instanceof Error ? failed.message : String(failed));
      }
    }
    kube(["delete", "namespace", namespace, "--wait=true"], { timeout: 180000 });
    const after = kube(["get", "namespace", namespace], { expected: [0, 1] });
    result.cleanup.push({
      namespace: namespace,
      how: how,
      uid: result.namespaceUid,
      ownerLabel: OWNER,
      objectsBefore: objects.trim().split("\n").filter((l) => l.length > 0),
      bucket: bucket,
      deleted: after.status !== 0,
      afterStderr: String(after.stderr || "").trim(),
      othersUntouched: kube(["get", "namespaces", "-o", "name"]).stdout.trim().split("\n"),
    });
    check(after.status !== 0, "the namespace is gone");
  } catch (failed) {
    result.cleanup.push({
      namespace: namespace, how: how,
      error: failed instanceof Error ? failed.message : String(failed),
    });
  }
}

function writeResult() {
  result.finishedAt = new Date().toISOString();
  result.passed = result.journeys.length;
  result.controlsRun = result.controls.length;
  result.artifacts = ARTIFACTS;
  mkdirSync(ARTIFACTS, { recursive: true });
  const at = join(ARTIFACTS, "live.json");
  if (existsSync(at)) {
    throw new Error(
      "a result document already exists at " + at + "; this harness never overwrites one.",
    );
  }
  writeFileSync(at, JSON.stringify(result, null, 2) + "\n");
  return at;
}

function shutDown() {
  stopApi();
  // A retained namespace is deliberately inspectable after the browser closes.
  // Keep its localAdmin config and cursor key with it so a follow-up can query
  // the exact same owned API surface; ordinary runs still remove all local
  // material immediately.
  if (process.env.UI_E2E_KEEP === "1") {
    return;
  }
  try {
    rmSync(WORK_DIR, { recursive: true, force: true });
  } catch (ignored) {
    // recorded by path in the result either way
  }
}

main().then(
  () => {
    shutDown();
    cleanUp("passed");
    const at = writeResult();
    process.stderr.write(
      "\n== " + result.journeys.length + " journey(s) and " + result.controls.length +
        " control(s) passed; result: " + at + "\n",
    );
    process.exit(0);
  },
  (error) => {
    shutDown();
    result.failure = error instanceof Error ? error.stack : String(error);
    cleanUp("failed");
    try {
      const at = writeResult();
      process.stderr.write("== result: " + at + "\n");
    } catch (ignored) {
      // the failure below is what matters
    }
    process.stderr.write("\n== FAILED: " + String(error && error.message) + "\n");
    process.exit(1);
  },
);
