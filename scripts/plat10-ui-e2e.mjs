// PLAT-10.1 / PLAT-10.2 live UI acceptance harness.
//
// The sibling of `scripts/d2w13-ui-e2e.mjs`, whose launcher this reuses
// verbatim in shape: `logweir-api` in localAdmin mode on a loopback port,
// pointed at this worktree's own `ui/` and at one namespace this run created,
// with a real Chromium driven against it. Console mode, because the guided
// form's cadence preview, its policy replace and its readiness check are all
// product-API routes and `kubectl proxy` serves none of them.
//
// EVERY POSITIVE CASE IS REAL AND THERE IS NO FAULT INJECTION. Every object is
// created by the PAGE against the real `logweir-api` and the real
// kube-apiserver, and read back with `kubectl` by name and by UID. Two objects
// are created by `kubectl` and are FIXTURES rather than results: the saved
// connection and the saved destination the form chooses between, and (for the
// history journeys) one `Backup` whose status is written with `kubectl` so the
// console's rendering of a finished run is exercised deterministically. Both
// are labelled as fixtures in the result document. What is under test is what
// the PAGE does with them.
//
// EVERY JOURNEY HAS A NEGATIVE CONTROL, and each control is an assertion that
// the page REFUSED something -- a create with no compiled expression, a
// dynamic selection with no incompleteDiscovery answer, a cadence the API
// rejects -- together with a `kubectl` count proving nothing reached the
// cluster. A control that merely observes a different screen proves nothing.
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
const LAB_KAFKA = "kafka-source." + LAB + ".svc:9092";
const LAB_MINIO = "minio." + LAB + ".svc:9000";
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
  lab: { release: LAB, kafka: LAB_KAFKA, minio: LAB_MINIO, usedReadOnly: true },
  uiDirectory: UI_DIR,
  apiBinary: API_BIN,
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

function schedules() {
  return kubeJson(["-n", namespace, "get", "backupschedules"]).items;
}

function backups() {
  return kubeJson(["-n", namespace, "get", "backups"]).items;
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

function seedConnection(name, servers) {
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
      metadata: { name: name, labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        bootstrapServers: [servers], role: "source",
        auth: { mode: "plaintext", tls: false },
      },
    }),
  });
  result.fixtures.push({ kind: "KafkaCluster", name: name, createdBy: "kubectl",
    bootstrapServers: servers });
  return name;
}

function seedDestination(name) {
  kube(["-n", namespace, "create", "secret", "generic", name + "-access",
    "--from-literal=access-key-id=minioadmin",
    "--from-literal=secret-access-key=minioadmin"]);
  kube(["-n", namespace, "label", "secret", name + "-access", OWNER_LABEL]);
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
      metadata: { name: name, labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        description: "PLAT-10 live journey, the lab's MinIO, read-only use",
        storage: { provider: "S3", bucket: "kafka-backups", prefix: namespace,
          addressing: "PathStyle", endpoint: "http" + "://" + LAB_MINIO },
        transport: { security: "InsecureHTTP" },
        access: {
          archiveWrite: {
            mode: "SecretKeys",
            secret: { name: name + "-access" },
          },
        },
      },
    }),
  });
  result.fixtures.push({ kind: "BackupDestination", name: name, createdBy: "kubectl",
    endpoint: LAB_MINIO });
  return name;
}

/** A FINISHED RUN, AS A FIXTURE. PLAT-10.2's history rows are about what the
 *  console renders for a run that produced a recovery point; producing one for
 *  real is D1's territory and is already proved live there. This writes the
 *  `Backup` and then its STATUS through the status subresource, which is the
 *  same shape the controller writes, so `isRecoveryPoint` and the restore link
 *  see exactly what they would see in production. */
function seedFinishedRun(name, schedule, scheduleUid, generation, at, setId) {
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "Backup",
      metadata: {
        name: name,
        labels: {
          "logweir.dev/test-owner": OWNER,
          "logweir.dev/schedule": schedule,
          "logweir.dev/schedule-uid": scheduleUid,
        },
      },
      spec: {
        sourceRef: { name: "unused-fixture-source" },
        topics: ["orders"],
        archive: { url: "s3://kafka-backups/" + namespace },
        scheduleRef: { name: schedule, uid: scheduleUid, generation: generation },
        slot: at.replace(/[-:TZ]/g, "").slice(0, 15),
        trigger: { kind: "Scheduled", attempt: 0 },
        triggeredBy: "schedule",
        deadlineSeconds: 3600,
      },
    }),
  });
  const status = {
    status: {
      phase: "Succeeded",
      backupId: setId,
      records: 42,
      windowCovered: { fromMs: Date.parse(at) - 3600000, toMs: Date.parse(at) },
      conditions: [{
        type: "Complete", status: "True", reason: "Ok",
        lastTransitionTime: at, message: "fixture",
      }],
    },
  };
  kube(["-n", namespace, "patch", "backup", name, "--subresource=status",
    "--type=merge", "-p", JSON.stringify(status)]);
  const stored = kubeJson(["-n", namespace, "get", "backup", name]);
  check(stored.status.phase === "Succeeded", name + " is a finished run");
  result.fixtures.push({ kind: "Backup", name: name, createdBy: "kubectl",
    uid: stored.metadata.uid, backupId: setId, note: "a finished run, written through the " +
      "status subresource in the controller's own shape" });
  return stored;
}

// --------------------------------------------------------------- the run

async function main() {
  mkdirSync(ARTIFACTS, { recursive: true });
  assertSafeNamespace(namespace);
  check(kube(["version", "--client=true"], { expected: [0] }).status === 0, "kubectl works");

  // THE SHARED LAB IS READ-ONLY AND ITS ROLLOUT IS WAITED FOR, never taken
  // under a lock this harness does not hold.
  const rollout = kube(["-n", LAB, "rollout", "status", "deploy/weirkeeper",
    "--timeout=300s"], { timeout: 310000 });
  result.lab.rollout = String(rollout.stdout || "").trim();

  kube(["create", "namespace", namespace]);
  kube(["label", "namespace", namespace, OWNER_LABEL]);
  const ns = kubeJson(["get", "namespace", namespace]);
  result.namespaceUid = ns.metadata.uid;
  result.created.push({ kind: "Namespace", name: namespace, uid: ns.metadata.uid });

  const source = seedConnection("orders-" + suffix, LAB_KAFKA);
  const broken = seedConnection("nowhere-" + suffix, UNREACHABLE_KAFKA);
  const destination = seedDestination("primary-" + suffix);

  const port = await freePort();
  result.port = port;
  await startApi(port);
  const base = "http://127.0.0.1:" + port + "/ui/";

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
      result.requests.push({ method: request.method(), url: url });
    }
  });

  const listRoute = base + "#/schedules?ns=" + namespace;

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

  try {
    // =================================================================== 1
    // The standard route: a preset, a coverage, a saved destination.
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

    // THE PREVIEW IS THE API'S. The canonical expression it returns is what
    // gets stored, and this harness compares the two afterwards.
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
    record("selected-topic creation through the guided form, and the first-run redirect", {
      schedule: selected.metadata.name, uid: selected.metadata.uid,
      canonicalExpression: canonical, storedExpression: selected.spec.schedule,
      timeZone: selected.spec.timeZone, destinationRef: selected.spec.destinationRef,
      sentinelUrl: selected.spec.archive.url, topics: selected.spec.topics,
      landedOn: detailHash,
    });

    // =================================================================== 2
    // All-user-topic creation, with exclusions.
    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the create form again");
    await page.fill("#schedule-name", "dynamic-" + suffix);
    await chooseSource(source);
    await page.selectOption("#policy-create-mode", "hourly");
    await waitForSelector(page, "#policy-create-minute", "the hourly preset");
    await page.fill("#policy-create-minute", "5");
    await page.selectOption("#policy-create-selection", "dynamic");
    await waitForSelector(page, "#policy-create-incompleteDiscovery", "the dynamic block");
    await page.selectOption("#policy-create-destination", destination);

    // NEGATIVE CONTROL 2a: a dynamic selection with no answer to "what if
    // discovery cannot prove it saw everything" is refused, and nothing is
    // created -- both defaults are wrong in a way nobody would notice.
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
    record("all-user-topic creation with exclusions", {
      schedule: dynamic[0].metadata.name,
      allUserTopics: dynamic[0].spec.allUserTopics,
      topics: dynamic[0].spec.topics,
    });

    // =================================================================== 3
    // An invalid cron is the API's refusal, and the draft survives it.
    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the create form for the cron journey");
    const beforeCron = schedules().length;
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
    const shown = await text(page);
    check(shown.indexOf("61") !== -1 || shown.indexOf("minute") !== -1 ||
      shown.indexOf("cron") !== -1,
      "the API's own words about the cadence are not on screen. Saw:\n" + shown.slice(0, 1500));
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
    record("an invalid cron is refused by the API and the draft is retained", {
      typed: "61 * * * *", keptCron: keptCron, keptTopics: keptTopics, keptName: keptName,
      ariaInvalid: invalidMarked, schedulesUnchanged: schedules().length === beforeCron,
      apiRefusal: (result.requests.filter((r) => r.url.indexOf("/schedules") !== -1).length > 0),
    });

    // NEGATIVE CONTROL 3a: the same form with a VALID advanced cron creates,
    // so the refusal above is about the expression and not about a form that
    // never submits.
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
    // Readiness against an unreachable source: the check's own verdict.
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
    await pause(8000);
    const preflights = kubeJson(["-n", namespace, "get", "preflights"]).items;
    check(preflights.length >= 1, "no Preflight object was created by the readiness button");
    const rawState = ((preflights[0].status || {}).state) || "";
    // THE VERDICT ON SCREEN IS THE ROUTE'S OWN WORD, ASSERTED AGAINST THE
    // RESPONSE THE BROWSER ACTUALLY RECEIVED. The state is the product API's
    // NORMALIZED one (D2's `CheckOperationResponse`), not the raw custom
    // resource's -- a `Preflight` no controller has touched has no
    // `status.state` at all and the route projects `running` for it -- so the
    // thing to compare the badge with is the body, byte for byte.
    const preflightBodies = bodies.filter((b) => b.url.indexOf("/preflights/") !== -1);
    check(preflightBodies.length >= 1, "the browser received no preflight read");
    const answered = JSON.parse(preflightBodies[preflightBodies.length - 1].body).item;
    const verdictState = String(answered.state || "");
    const badgeText = (await page.textContent(
      "#schedule-readiness-verdict .preflight-head .badge")).trim();
    const expected = { ready: "ready", notReady: "not ready", failed: "failed: no result",
      cancelled: "cancelled: no result", pending: "pending", queued: "queued",
      running: "running" }[verdictState] || "unknown";
    check(badgeText === expected,
      "the badge says " + JSON.stringify(badgeText) + " while the route answered state " +
        JSON.stringify(verdictState) + " (which renders as " + JSON.stringify(expected) + ")");
    check(answered.id === preflights[0].metadata.name,
      "the readiness answer is about a different Preflight than the one in the cluster");
    // AND IT IS NOT READY. The source cannot be resolved at all, so a green
    // `ready` here would be the fabricated verdict UI-FAKEPREFLIGHT forbids.
    check(badgeText !== "ready",
      "the page rendered a READY verdict for a source that does not resolve");
    const greenVerdict = await page.evaluate(() =>
      document.querySelectorAll("#schedule-readiness-verdict .preflight-head .badge-green")
        .length);
    check(greenVerdict === 0, "a green readiness badge was rendered for an unreachable source");
    await shot(page, "07-readiness");
    record("the readiness check starts a real Preflight and renders its own recorded verdict", {
      preflight: preflights[0].metadata.name, uid: preflights[0].metadata.uid,
      source: broken, bootstrapServers: UNREACHABLE_KAFKA,
      routeState: verdictState,
      rawObjectState: rawState === "" ? "(none recorded)" : rawState,
      badgeOnScreen: badgeText,
      greenBadges: greenVerdict,
      note: verdictState === "ready" || verdictState === "notReady"
        ? "the controller recorded a terminal verdict and the page rendered exactly it"
        : "no controller on this lab reconciles Preflight, so the route projects a " +
          "non-terminal state and the page renders exactly that -- never ready, which is " +
          "what this row asserts",
    });

    // =================================================================== 5
    // The schedule detail: Back up now, and the run it produced.
    const detailRoute = base + "#/schedules?ns=" + namespace + "&name=" +
      selected.metadata.name;
    result.reloads = (result.reloads || 0) +
      await openRoute(page, detailRoute, "#schedule-detail", "the schedule detail route");
    await waitForText(page, "runs and recovery points", "the history section");
    await waitForText(page, "this schedule has produced no run yet", "the empty history");
    await shot(page, "08-detail-empty-history");
    record("PLAT-10.2: the detail's empty history says what empty means", {
      route: "#/schedules?ns=&name=", schedule: selected.metadata.name,
    });

    const beforeRun = backups().length;
    await waitForSelector(page, "form.run-now-form", "the manual-run panel");
    await page.click("form.run-now-form button[type=submit]");
    await waitForText(page, "follow this run", "the run this click produced");
    await pause(1500);
    const runs = backups();
    check(runs.length === beforeRun + 1, "one click produced " + (runs.length - beforeRun) +
      " run(s)");
    const manual = runs[runs.length - 1];
    check(manual.spec.scheduleRef.name === selected.metadata.name,
      "the run does not name the schedule it was taken from");
    check(manual.spec.scheduleRef.uid === selected.metadata.uid,
      "the run does not carry the schedule's identity");
    await shot(page, "09-backed-up-now");
    result.created.push({ kind: "Backup", name: manual.metadata.name,
      uid: manual.metadata.uid, createdBy: "the page" });
    record("Back up now from the detail creates one run bound to this schedule", {
      schedule: selected.metadata.name, run: manual.metadata.name, uid: manual.metadata.uid,
      scheduleRef: manual.spec.scheduleRef,
    });

    await page.reload({ waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "#schedule-history", "the history after the run");
    await waitForText(page, manual.metadata.name, "the run in the history");
    const historyText = await text(page);
    check(historyText.indexOf("not in the catalog") !== -1,
      "a run with no catalog entry did not say so. Saw:\n" + historyText.slice(0, 1500));
    const greens = await page.evaluate(() =>
      document.querySelectorAll("#schedule-history .badge-green").length);
    check(greens === 0,
      "a run the catalog has never seen was badged green " + greens + " time(s)");
    await shot(page, "10-history-not-in-catalog");
    record("a run the durable catalog has not seen is neither available nor unavailable", {
      run: manual.metadata.name, greenBadgesInHistory: greens,
      sawNotInCatalog: historyText.indexOf("not in the catalog") !== -1,
    });

    // =================================================================== 6
    // A finished run, its recovery point, and the restore bound to THAT point.
    const older = seedFinishedRun("fixture-older-" + suffix, selected.metadata.name,
      selected.metadata.uid, selected.metadata.generation, "2026-09-18T02:30:00Z",
      "set-older-" + suffix);
    const newest = seedFinishedRun("fixture-newest-" + suffix, selected.metadata.name,
      selected.metadata.uid, selected.metadata.generation, "2026-09-20T02:30:00Z",
      "set-newest-" + suffix);
    await page.reload({ waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "#schedule-history", "the history with points");
    await waitForSelector(page, "#schedule-restore-latest", "the page-level restore");
    const latestHref = await page.getAttribute("#schedule-restore-latest", "href");
    check(latestHref.indexOf("uid=" + newest.metadata.uid) !== -1,
      "the page-level Restore is not bound to the newest point: " + latestHref);
    const rowHrefs = await page.evaluate(() =>
      Array.from(document.querySelectorAll("#schedule-history a"))
        .map((a) => a.getAttribute("href"))
        .filter((h) => h !== null && h.indexOf("#/restore") === 0));
    check(rowHrefs.some((h) => h.indexOf("uid=" + older.metadata.uid) !== -1),
      "the older point has no restore link of its own: " + JSON.stringify(rowHrefs));
    check(rowHrefs.some((h) => h.indexOf("uid=" + newest.metadata.uid) !== -1),
      "the newest point has no restore link of its own");
    await shot(page, "11-history-with-points");
    record("PLAT-10.2: each recovery point carries a restore bound to that point", {
      latestAction: latestHref, rowLinks: rowHrefs,
      newest: newest.metadata.name, older: older.metadata.name,
    });

    // NAVIGATION TO AN OLDER BACKUP: following the OLDER point's link opens the
    // wizard on that point, and the harness stops at the wizard's first step --
    // the wizard itself is another task's.
    const olderLink = rowHrefs.filter((h) => h.indexOf("uid=" + older.metadata.uid) !== -1)[0];
    result.reloads = (result.reloads || 0) +
      await openRoute(page, base + olderLink, "#point-uid", "the wizard's bound recovery point");
    // THE BINDING IS THE UID, and the wizard publishes both halves of it.
    // (The step also lists what the namespace holds, newest included, with the
    // chosen one marked -- so the assertion is on the binding and not on the
    // absence of a name from the page.)
    const boundName = (await page.textContent("#point-name")).trim();
    const boundUid = (await page.textContent("#point-uid")).trim();
    check(boundUid === older.metadata.uid,
      "the wizard is bound to " + boundUid + " and the link named " + older.metadata.uid);
    check(boundName === older.metadata.name,
      "the wizard named " + boundName + " and the link named " + older.metadata.name);
    check(boundUid !== newest.metadata.uid,
      "the wizard substituted the newest point for the one the link named");
    const chosenMark = await text(page);
    check(chosenMark.indexOf(older.metadata.name.toLowerCase() + " (chosen)") !== -1,
      "the namespace table does not mark the older point as the chosen one");
    await shot(page, "12-wizard-on-older-point");
    record("navigation to an older backup opens the wizard bound to it, not to the newest", {
      followed: olderLink, boundName: boundName, boundUid: boundUid,
      newestUid: newest.metadata.uid, substituted: boundUid === newest.metadata.uid,
    });

    // =================================================================== 7
    // Editing the future policy from the detail, and the revision it makes.
    result.reloads = (result.reloads || 0) +
      await openRoute(page, detailRoute, "form.policy-form", "the policy form on the detail");
    const beforeEdit = kubeJson(["-n", namespace, "get", "backupschedule",
      selected.metadata.name]);
    const panel = "form.policy-form[data-name=\"" + selected.metadata.name + "\"]";
    await page.fill(panel + " [name=\"topics\"]", "orders, payments, shipments");
    await page.click("button[data-preview=\"" + selected.metadata.name + "\"]");
    await waitForText(page, "this cadence compiles to", "the edit preview");
    await page.click(panel + " button[type=submit]");
    await pause(2000);
    const afterEdit = kubeJson(["-n", namespace, "get", "backupschedule",
      selected.metadata.name]);
    check(afterEdit.metadata.generation > beforeEdit.metadata.generation,
      "the edit did not move the revision: " + beforeEdit.metadata.generation + " -> " +
        afterEdit.metadata.generation);
    check(JSON.stringify(afterEdit.spec.topics) ===
      JSON.stringify(["orders", "payments", "shipments"]),
      "the edited allowlist was not stored: " + JSON.stringify(afterEdit.spec.topics));
    check(JSON.stringify(afterEdit.spec.destinationRef) ===
      JSON.stringify({ name: destination }),
      "the whole-policy replace dropped the destination");
    await shot(page, "13-policy-edited");
    record("editing the future policy from the detail makes a new revision", {
      schedule: selected.metadata.name,
      fromGeneration: beforeEdit.metadata.generation,
      toGeneration: afterEdit.metadata.generation,
      topics: afterEdit.spec.topics, destinationRef: afterEdit.spec.destinationRef,
    });

    // A LATER RUN CARRIES THE NEW REVISION.
    await page.reload({ waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "form.run-now-form", "the run-now panel after the edit");
    const again = await page.$("button[data-run-again=\"" + selected.metadata.name + "\"]");
    if (again !== null) {
      await again.click();
      await pause(500);
    }
    const beforeSecond = backups().length;
    await page.click("form.run-now-form button[type=submit]");
    await pause(2500);
    const second = backups().filter((b) =>
      b.spec.scheduleRef && b.spec.scheduleRef.name === selected.metadata.name &&
      b.spec.trigger && b.spec.trigger.kind === "Manual" &&
      b.spec.scheduleRef.generation === afterEdit.metadata.generation);
    check(second.length >= 1,
      "no later run carries the new revision g" + afterEdit.metadata.generation +
        "; runs now: " + JSON.stringify(backups().map((b) =>
          [b.metadata.name, (b.spec.scheduleRef || {}).generation])));
    await shot(page, "14-run-with-new-revision");
    record("a run taken after the edit carries the new revision", {
      revision: afterEdit.metadata.generation,
      run: second[0].metadata.name,
      frozenGeneration: second[0].spec.scheduleRef.generation,
      runsBefore: beforeSecond, runsAfter: backups().length,
    });
    result.created.push({ kind: "Backup", name: second[0].metadata.name,
      uid: second[0].metadata.uid, createdBy: "the page" });

    // NEGATIVE CONTROL 7a: the run created BEFORE the edit still carries the
    // revision it froze. An edit that reached an existing run would be the
    // defect PLAT-05.1 exists to prevent.
    const frozen = kubeJson(["-n", namespace, "get", "backup", manual.metadata.name]);
    check(frozen.spec.scheduleRef.generation === beforeEdit.metadata.generation,
      "the edit reached a run that already existed: " +
        frozen.spec.scheduleRef.generation);
    control("the run taken before the edit still carries the revision it froze", {
      run: manual.metadata.name, generation: frozen.spec.scheduleRef.generation,
      editedTo: afterEdit.metadata.generation,
    });

    // =================================================================== 8
    // Pause and resume, from the detail.
    result.reloads = (result.reloads || 0) +
      await openRoute(page, detailRoute, "form.suspend", "the suspend toggle on the detail");
    await page.click("form.suspend button[type=submit]");
    await pause(2000);
    const paused = kubeJson(["-n", namespace, "get", "backupschedule", selected.metadata.name]);
    check(paused.spec.suspend === true, "the pause did not suspend the schedule");
    await shot(page, "15-paused");
    await page.reload({ waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, "#schedule-history", "the history while paused");
    const pausedText = await text(page);
    check(pausedText.indexOf("restore this point") !== -1,
      "a paused schedule stopped offering its recovery points");
    await waitForSelector(page, "form.suspend", "the resume toggle");
    await page.click("form.suspend button[type=submit]");
    await pause(2000);
    const resumed = kubeJson(["-n", namespace, "get", "backupschedule", selected.metadata.name]);
    check(resumed.spec.suspend === false, "the resume did not un-suspend the schedule");
    await shot(page, "16-resumed");
    record("pause and resume from the detail, with the history still offered while paused", {
      schedule: selected.metadata.name, pausedSpecSuspend: paused.spec.suspend,
      resumedSpecSuspend: resumed.spec.suspend,
      pointsOfferedWhilePaused: pausedText.indexOf("restore this point") !== -1,
    });

    // =================================================================== 9
    // The archived state: delete the schedule, keep the history.
    const doomed = advanced[0].metadata.name;
    seedFinishedRun("fixture-archived-" + suffix, doomed,
      advanced[0].metadata.uid, advanced[0].metadata.generation, "2026-09-19T04:00:00Z",
      "set-archived-" + suffix);
    kube(["-n", namespace, "delete", "backupschedule", doomed, "--wait=true"]);
    const gone = kube(["-n", namespace, "get", "backupschedule", doomed], { expected: [0, 1] });
    check(gone.status !== 0, "the schedule was not deleted");
    const survivors = backups().filter((b) =>
      (b.spec.scheduleRef || {}).name === doomed);
    check(survivors.length >= 1, "deleting the schedule took its history with it");
    result.reloads = (result.reloads || 0) + await openRoute(page,
      base + "#/schedules?ns=" + namespace + "&name=" + doomed,
      "[data-archived=\"1\"]", "the archived schedule state");
    await waitForText(page, "no longer exists in this namespace", "the archived sentence");
    const archivedText = await text(page);
    check(archivedText.indexOf("restore this point") !== -1,
      "the archived schedule's points are not restorable");
    const controls = await page.evaluate(() => ({
      suspend: document.querySelectorAll("form.suspend").length,
      policy: document.querySelectorAll("form.policy-form").length,
      runNow: document.querySelectorAll("form.run-now-form").length,
    }));
    check(controls.suspend === 0 && controls.policy === 0 && controls.runNow === 0,
      "a deleted schedule still offers controls that need one: " + JSON.stringify(controls));
    await shot(page, "17-archived-schedule");
    record("PLAT-10.2: a deleted schedule keeps its history and offers no schedule controls", {
      schedule: doomed, retainedRuns: survivors.map((b) => b.metadata.name),
      controlsOffered: controls,
    });

    // =================================================================== 10
    // Keyboard-only creation: Tab, arrows, Space and Enter, and no click.
    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the create form for the keyboard journey");
    const beforeKeyboard = schedules().length;
    // The FIRST field, every selection, and submit are reached by Tab. This
    // starts at the document's natural focus position; there is intentionally
    // no `focus()`, locator click or `selectOption()` on this path.
    await tabTo(page, "#schedule-name", "the keyboard journey's first field");
    await page.evaluate(() => {
      window.__plat10PointerEvents = 0;
      document.addEventListener("pointerdown", () => { window.__plat10PointerEvents += 1; },
        { capture: true });
    });
    const typedName = "kbd-" + suffix;
    await page.keyboard.type(typedName);
    const sourceUid = kubeJson(["-n", namespace, "get", "kafkacluster", source]).metadata.uid;
    // Tab to the source selector and choose with the keyboard.
    await tabTo(page, "#schedule-source", "the keyboard journey's source selector");
    const sourceKeys = await keyboardSelect("#schedule-source", sourceUid, source[0]);
    // Advanced cron is selected with the keyboard, which repaints the preset
    // parameters into the cron field. `a` is recorded like the other select
    // keys, and the next Tab traversal begins at the repainted document.
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
    await shot(page, "18-keyboard-filled");
    // The native submit button is reached and activated with Space, so the
    // final action is also an explicit keyboard action rather than a synthetic
    // form submit.
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
    await shot(page, "19-keyboard-created");
    result.created.push({ kind: "BackupSchedule", name: keyboardMade[0].metadata.name,
      uid: keyboardMade[0].metadata.uid, createdBy: "the page (keyboard only)" });
    record("the standard route completes with the keyboard alone: no pointer event was sent", {
      schedule: keyboardMade[0].metadata.name,
      expression: keyboardMade[0].spec.schedule,
      sourceRef: keyboardMade[0].spec.sourceRef,
      destinationRef: keyboardMade[0].spec.destinationRef,
      pointerEvents: await page.evaluate(() => window.__plat10PointerEvents),
      keysForTheConnection: sourceKeys,
      keysForTheDestination: destinationKeys,
      keysForTheCadenceMode: modeKeys,
      focusVisibleOutline: focusVisible,
    });

    // NEGATIVE CONTROL 10a: the same keyboard route with NOTHING chosen is
    // refused and creates nothing, so the row above is about what was typed.
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

    // ================================================================== 11
    // THE DEEP LINK MIGRATION, live: the old list link still reaches the list.
    result.reloads = (result.reloads || 0) + await openRoute(page, listRoute, "#schedule-form", "the old deep link still reaches the list");
    const listText = await text(page);
    check(listText.indexOf("create a backupschedule") !== -1,
      "the old `#/schedules?ns=` link no longer renders the list");
    const detailOnList = await page.evaluate(() =>
      document.querySelector("#schedule-detail") !== null);
    check(!detailOnList, "the list route rendered a detail");
    await shot(page, "20-old-deep-link");
    record("the pre-PLAT-10.2 deep link `#/schedules?ns=` still reaches the list", {
      route: "#/schedules?ns=" + namespace, renderedDetail: detailOnList,
    });
  } finally {
    writeFileSync(join(ARTIFACTS, "api.log"), apiLog.join(""));
    writeFileSync(join(ARTIFACTS, "responses.json"),
      JSON.stringify(bodies.map((b) => ({ url: b.url, status: b.status,
        body: b.body.slice(0, 20000) })), null, 2) + "\n");
    // THE CLUSTER STATE THIS RUN LEFT, dumped before anything is deleted.
    writeFileSync(join(ARTIFACTS, "kubectl-schedules.json"),
      kube(["-n", namespace, "get", "backupschedules", "-o", "json"],
        { expected: [0, 1] }).stdout);
    writeFileSync(join(ARTIFACTS, "kubectl-backups.json"),
      kube(["-n", namespace, "get", "backups", "-o", "json"], { expected: [0, 1] }).stdout);
    writeFileSync(join(ARTIFACTS, "kubectl-preflights.json"),
      kube(["-n", namespace, "get", "preflights", "-o", "json"], { expected: [0, 1] }).stdout);
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
      "backupschedules,backups,kafkaclusters,backupdestinations,preflights,secrets", "-o", "name"],
      { expected: [0, 1] }).stdout;
    if (process.env.UI_E2E_KEEP === "1") {
      result.cleanup.push({ namespace: namespace, how: how, kept: true });
      return;
    }
    kube(["delete", "namespace", namespace, "--wait=true"], { timeout: 180000 });
    const after = kube(["get", "namespace", namespace], { expected: [0, 1] });
    result.cleanup.push({
      namespace: namespace,
      how: how,
      uid: result.namespaceUid,
      ownerLabel: OWNER,
      objectsBefore: objects.trim().split("\n").filter((l) => l.length > 0),
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
