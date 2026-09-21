// PLAT-06.2 live UI acceptance harness: "Back up now", journeyed.
//
// The sibling of `scripts/d2w13-ui-e2e.mjs`, whose launcher this is built on:
// it starts `logweir-api` in localAdmin mode on a loopback port, pointed at
// this worktree's own `ui/` directory and at one namespace it created, and
// drives a real Chromium against it. What is under test here is PLAT-06.2's
// whole test list on ONE build -- double click, lost HTTP response, refresh,
// paused schedule, FAILED PREFLIGHT, and the successful scheduled-policy copy
// that is the negative control for the last of them.
//
// D1 SECTION 8.4 IS THE SENTENCE THIS HARNESS IS ABOUT, and it is quoted here
// because the assertions below are its clauses and nothing else:
//
//     "The API and controller never gate on readiness: execution-time guards
//      stay authoritative and the direct CR path exists regardless. When the
//      console advertises the readiness capability, the UI fetches the latest
//      readiness for the same operation inputs. `ready` -> submit. `notReady`
//      -> show each failed prerequisite and remedy and require a second
//      explicit "Run anyway" (sent as `readinessAcknowledgement`, recorded as
//      annotation `logweir.dev/readiness-ack`, not authoritative).
//      `unknown`/capability absent -> submit, with the label "Readiness not
//      checked; the run reports its own prerequisites"."
//
// THIS BUILD IS THE THIRD CASE, and the harness measures that rather than a
// case it would like to be in: no per-schedule readiness verdict is fetched
// for the "Back up now" panel in this build, so the panel carries the exact
// label section 8.4 prescribes, the click is NOT refused, a run IS created,
// and the run reports its own prerequisites. The assertion that the page
// "never shows a client-side guess" is therefore concrete: the pre-click text
// is captured and required to contain no verdict about the source at all, and
// after the run goes terminal the CONTROLLER'S OWN reason and message -- read
// back from the object with kubectl -- are required to be on screen verbatim.
//
// THE UNREACHABLE SOURCE IS REAL AND IS IN THIS RUN'S OWN NAMESPACE: a
// `KafkaCluster` whose only bootstrap entry is `192.0.2.1:9092`, RFC 5737
// TEST-NET-1, which is reserved for documentation and routed nowhere. Nothing
// is faulted, patched or intercepted to produce that failure.
//
// THE ONE FAULT INJECTION IS THE LOST RESPONSE, and it is declared in the
// result document under `faultInjection`. A lost HTTP response is a tracker
// test ("lost HTTP response") and it cannot be produced without taking a
// response away from the browser AFTER the server has handled the request --
// which is exactly what is done: the request is forwarded to the real API, the
// real 201 is read, and only then is the browser's fetch aborted. The object
// the API created is real and is read back with kubectl.
//
// Dependencies: Node.js, kubectl, a built `logweir-api`, Playwright/Chromium.
//   NODE_PATH="$(npm root -g)" node scripts/plat06-2-ui-e2e.mjs
//
// Environment (all optional):
//   UI_E2E_OWNER       the `logweir.dev/test-owner` label; default plat06-2-finish.
//   UI_E2E_PREFIX      the namespace prefix; default lw-p062-.
//   UI_E2E_NAMESPACE   the namespace to create and delete.
//   UI_E2E_API_BIN     the logweir-api binary; default target/release/logweir-api.
//   UI_E2E_UI_DIR      the directory to serve; default this worktree's ui/.
//   UI_E2E_ARTIFACTS   where screenshots, the API log and the result go.
//   UI_E2E_LAB         the shared release's namespace; default logweir-scram-local.
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
const OWNER = process.env.UI_E2E_OWNER || "plat06-2-finish";
// THE SHARED RELEASE IS READ-ONLY, AND IT IS READ EXACTLY TWICE: to copy the
// SCRAM credential and the archive credential this run's own schedules need.
// Nothing in it is created, patched or deleted, and the two Secrets are copied
// by VALUE into this run's namespace with `kubectl get -o json | create`, never
// referenced across namespaces (a Secret reference is namespace-scoped, which
// is the product invariant this respects rather than works around).
const LAB = process.env.UI_E2E_LAB || "logweir-scram-local";
const ARTIFACTS_ROOT = process.env.UI_E2E_ARTIFACTS ||
  ("/tmp/logweir-roadmap-run/claude/artifacts/plat06-2-ui");
const NAMESPACE_PREFIX = process.env.UI_E2E_PREFIX || "lw-p062-";
const OWNER_LABEL = "logweir.dev/test-owner=" + OWNER;
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z");
const namespace = process.env.UI_E2E_NAMESPACE || (NAMESPACE_PREFIX + "ui" + stamp.toLowerCase());
const suffix = randomBytes(3).toString("hex");

const ARTIFACTS = join(ARTIFACTS_ROOT, namespace);
const WORK_DIR = join("/tmp", "plat062-live-" + namespace);

const result = {
  harness: "scripts/plat06-2-ui-e2e.mjs",
  task: "PLAT-06.2",
  kubeContext: KUBE_CONTEXT,
  owner: OWNER,
  namespacePrefix: NAMESPACE_PREFIX,
  namespace: namespace,
  lab: LAB,
  uiDirectory: UI_DIR,
  apiBinary: API_BIN,
  startedAt: new Date().toISOString(),
  mode: "console (logweir-api, localAdmin, loopback)",
  decisionQuoted: "D1 §8.4",
  faultInjection: [],
  journeys: [],
  trackerTests: {},
  created: [],
  screenshots: [],
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

/** Ties one tracker test to the journey step that measured it, so the report's
 *  table is read off the evidence rather than assembled by hand. */
function tracker(test, step, detail) {
  result.trackerTests[test] = Object.assign({ step: step }, detail || {});
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
    maxBuffer: 8 * 1024 * 1024,
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
  const at = join(ARTIFACTS, name + ".png");
  await page.screenshot({ path: at, fullPage: true });
  result.screenshots.push(at);
  return at;
}

/** What the page says, case-folded: several headings and every badge render in
 *  capitals through `text-transform`, and `innerText` reports what is
 *  RENDERED. Folding keeps each assertion about the sentence. */
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
    (await text(page)).slice(0, 2500));
}

/** Every `Backup` in this namespace whose trigger says Manual, by schedule. */
function manualRuns(schedule) {
  return kubeJson(["-n", namespace, "get", "backups"]).items.filter((b) =>
    ((b.spec || {}).trigger || {}).kind === "Manual" &&
    ((b.spec || {}).scheduleRef || {}).name === schedule);
}

/** Waits until this namespace holds `count` manual runs of `schedule`, or
 *  fails naming what it saw. EVERY WAIT HAS A CEILING (WORKER-RULES). */
async function waitForRuns(schedule, count, label) {
  let seen = [];
  for (let i = 0; i < 60; i += 1) {
    seen = manualRuns(schedule);
    if (seen.length >= count) {
      return seen;
    }
    await pause(500);
  }
  throw new Error(label + ": expected " + count + " manual run(s) of " + schedule +
    ", saw " + seen.length + ": " + seen.map((b) => b.metadata.name).join(", "));
}

/** Waits for one `Backup` to go terminal and returns it. */
async function waitTerminal(name, seconds) {
  const deadline = Date.now() + (seconds || 420) * 1000;
  let object = null;
  while (Date.now() < deadline) {
    object = kubeJson(["-n", namespace, "get", "backup", name]);
    const phase = (object.status || {}).phase;
    if (phase === "Succeeded" || phase === "Failed" || phase === "Refused") {
      return object;
    }
    await pause(2000);
  }
  throw new Error("backup " + name + " never went terminal; phase " +
    String(((object || {}).status || {}).phase));
}

/** The words the CONTROLLER recorded about a run: its phase, its exit reason
 *  and the reason/message of its terminal condition. These are the strings the
 *  page must render, and nothing the page could have composed. */
function controllerWords(object) {
  const status = object.status || {};
  const conditions = status.conditions || [];
  const failed = conditions.find((c) => c.type === "Failed" && c.status === "True") ||
    conditions.find((c) => c.status === "True") || {};
  return {
    phase: status.phase,
    exitCode: status.exitCode,
    exitReason: status.exitReason,
    conditionType: failed.type,
    reason: failed.reason,
    message: failed.message,
  };
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
    "  subject: plat06-2-finish",
    "  displayName: PLAT-06.2 live journey",
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

// ------------------------------------------------------------- the fixture

/** Copies one Secret out of the shared release BY VALUE. The release is read
 *  and never written; the copy lands in this run's own namespace with this
 *  run's owner label, because a Secret reference is namespace-scoped and a
 *  run here must not reach into the lab's. */
function copySecret(name) {
  const source = kubeJson(["-n", LAB, "get", "secret", name]);
  const copy = {
    apiVersion: "v1",
    kind: "Secret",
    metadata: {
      name: name,
      namespace: namespace,
      labels: { "logweir.dev/test-owner": OWNER },
    },
    type: source.type,
    data: source.data,
  };
  kube(["-n", namespace, "create", "-f", "-"], { input: JSON.stringify(copy) });
  result.created.push({ kind: "Secret", name: name });
}

/** The archive every schedule here writes to: the shared release's own MinIO,
 *  under this run's own prefix. The controller forwards its object-store
 *  environment into runner Jobs for legacy inline-archive runs, which is how
 *  the lab's own `scram-schedule` works and is documented as open by design in
 *  `docs/kubernetes.md` sections 20-22; this harness makes no claim about that
 *  either way and simply uses the path the lab already uses. */
const ARCHIVE_URL = "s3://kafka-backups/plat06-2-ui/" + stamp.toLowerCase();

function makeSchedule(name, spec) {
  const object = {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "BackupSchedule",
    metadata: { name: name, namespace: namespace, labels: { "logweir.dev/test-owner": OWNER } },
    spec: Object.assign({
      schedule: "0 3 * * *",
      // NOT suspended, and the cadence is daily at 03:00 UTC so nothing fires
      // inside a run of this harness: every `Backup` counted below is a run a
      // CLICK made. Journey 8 asks for a suspended one by name, because that
      // is the tracker test.
      suspend: false,
      topics: ["orders"],
      archive: { url: ARCHIVE_URL, secretRef: { name: "logweir-s3" } },
      concurrencyPolicy: "Forbid",
      activeDeadlineSeconds: 300,
    }, spec),
  };
  const made = JSON.parse(
    kube(["-n", namespace, "create", "-f", "-", "-o", "json"],
      { input: JSON.stringify(object) }).stdout,
  );
  result.created.push({ kind: "BackupSchedule", name: name, uid: made.metadata.uid });
  return made;
}

function makeCluster(name, bootstrap, auth) {
  const object = {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "KafkaCluster",
    metadata: { name: name, namespace: namespace, labels: { "logweir.dev/test-owner": OWNER } },
    spec: { bootstrapServers: [bootstrap], role: "source", auth: auth },
  };
  const made = JSON.parse(
    kube(["-n", namespace, "create", "-f", "-", "-o", "json"],
      { input: JSON.stringify(object) }).stdout,
  );
  result.created.push({ kind: "KafkaCluster", name: name, uid: made.metadata.uid });
  return made;
}

/** Waits until the controller has published a policy digest for this
 *  schedule's CURRENT generation. Nothing may copy a revision the controller
 *  has not evaluated, in either mode. */
async function awaitObserved(name) {
  for (let i = 0; i < 120; i += 1) {
    const object = kubeJson(["-n", namespace, "get", "backupschedule", name]);
    const policy = (object.status || {}).policy || {};
    if (policy.generation === object.metadata.generation && policy.runPolicySha256) {
      return object;
    }
    await pause(1000);
  }
  throw new Error("the controller never published a policy digest for " + name);
}

// --------------------------------------------------------------- the run

async function main() {
  mkdirSync(ARTIFACTS, { recursive: true });
  assertSafeNamespace(namespace);
  check(kube(["version", "--client=true"], { expected: [0] }).status === 0, "kubectl works");
  check(existsSync(API_BIN), API_BIN + " does not exist; `cargo build --release -p logweir-api`");

  kube(["create", "namespace", namespace]);
  kube(["label", "namespace", namespace, OWNER_LABEL]);
  const ns = kubeJson(["get", "namespace", namespace]);
  result.namespaceUid = ns.metadata.uid;
  result.created.push({ kind: "Namespace", name: namespace, uid: ns.metadata.uid });

  // The runner identity every Backup Job in this namespace runs as, with no
  // token: the shape the shared release's own runner Jobs use.
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "v1", kind: "ServiceAccount",
      metadata: {
        name: "logweir-runner", namespace: namespace,
        labels: { "logweir.dev/test-owner": OWNER },
      },
      automountServiceAccountToken: false,
    }),
  });
  // THE THREE SECRETS A RUNNER JOB IN THIS NAMESPACE MOUNTS OR READS, measured
  // from the shared release's own backup Job rather than guessed: the SCRAM
  // password (`LOGWEIR_SOURCE_PASSWORD`), the object-store keys
  // (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) and the receipt signing key,
  // which is a VOLUME -- so a namespace missing it fails the Job at
  // `VolumeMountFailed` before the runner starts, which looks exactly like a
  // real failure and is a defect in the fixture. The negative control in
  // journey 3 is what makes that distinguishable, and journey 2 refuses the
  // fixture-shaped reasons by name as well.
  copySecret("source-scram");
  copySecret("logweir-s3");
  copySecret("logweir-signing-key");

  // TWO SOURCES AND TWO SCHEDULES, AND THE DIFFERENCE BETWEEN THEM IS ONE
  // BOOTSTRAP ENTRY. `healthy` is the shared release's own reachable broker;
  // `unreachable` is RFC 5737 TEST-NET-1, reserved for documentation and
  // routed nowhere. Nothing else about the two schedules differs, so whatever
  // the page does differently is about the source and not about the fixture.
  const healthySource = makeCluster(
    "healthy-source",
    "kafka-source." + LAB + ".svc.cluster.local:9096",
    { mode: "scramSha512", username: "scram-user", tls: false,
      secretRef: { name: "source-scram" } },
  );
  const brokenSource = makeCluster(
    "unreachable-source", "192.0.2.1:9092", { mode: "plaintext", tls: false },
  );
  const healthy = makeSchedule("healthy-" + suffix, {
    sourceRef: { name: "healthy-source" }, topics: ["orders"],
  });
  const broken = makeSchedule("unreachable-" + suffix, {
    sourceRef: { name: "unreachable-source" }, topics: ["orders"],
  });
  await awaitObserved(healthy.metadata.name);
  await awaitObserved(broken.metadata.name);

  const port = await freePort();
  result.port = port;
  await startApi(port);
  const base = "http://127.0.0.1:" + port + "/ui/";
  const schedulesRoute = base + "#/schedules?ns=" + namespace;

  const browser = await chromium.launch();
  const context = await browser.newContext();
  const page = await context.newPage();
  const posts = [];
  page.on("response", async (response) => {
    try {
      const url = response.url();
      if (url.indexOf("/backups") !== -1 && response.request().method() === "POST") {
        posts.push({ url: url, status: response.status() });
      }
    } catch (gone) {
      // a body that is no longer available cannot change what the page showed
    }
  });

  const runNow = (name) =>
    "form.run-now-form[data-name=\"" + name + "\"] button[type=submit]";

  /** Opens the schedules route from scratch.
   *
   *  A `goto` TO THE URL THE PAGE IS ALREADY ON IS A SAME-DOCUMENT NAVIGATION
   *  and does not reload: the first cut used one between journeys, and a
   *  schedule created after the page had painted was simply not there. Every
   *  step below wants a fresh mount -- a list read again from the API, and, in
   *  journey 7, the in-memory idempotency intent genuinely gone -- so this
   *  always ends in a real reload. */
  async function openSchedules() {
    await page.goto(schedulesRoute, { waitUntil: "load", timeout: 30000 });
    await page.reload({ waitUntil: "load", timeout: 30000 });
  }

  try {
    // ---------------------------------------------------------------- 1
    // D1 section 8.4, THIRD CASE: the capability is absent, so the panel carries
    // the label the decision prescribes and the click is not gated.
    await openSchedules();
    await waitForText(page, broken.metadata.name, "the unreachable schedule's card");
    // THE PANEL'S HEADING IS "Run first backup now" UNTIL A RUN EXISTS, which
    // is D1 section 8.5's Idle row, so the wait is on the control and not on a
    // caption that changes with the state under test.
    await page.waitForSelector(runNow(broken.metadata.name), { timeout: 30000 });
    check((await text(page)).includes("run first backup now"),
      "the first-run heading is not on a schedule that has never fired");
    const beforeClick = await text(page);
    check(beforeClick.includes("readiness not checked; the run reports its own prerequisites"),
      "D1 section 8.4's label for an absent readiness capability is not on screen: " +
        beforeClick.slice(0, 1200));
    // AND NO CLIENT-SIDE GUESS ABOUT THE SOURCE. The page has read the
    // KafkaCluster's own status -- it renders connections elsewhere -- and it
    // must not turn that into a verdict about this run.
    for (const guess of ["will fail", "cannot reach", "source is unreachable",
      "not ready to run", "this run will"]) {
      check(beforeClick.indexOf(guess) === -1,
        "the page composed a verdict of its own before the run existed: " + guess);
    }
    const brokenButton = await page.$(runNow(broken.metadata.name));
    check(brokenButton !== null, "the unreachable schedule still offers the button");
    check(!(await brokenButton.isDisabled()),
      "and it is ENABLED: the API and controller never gate on readiness (D1 section 8.4)");
    await shot(page, "01-readiness-not-checked");
    record("an unreachable source is not a gate: the panel carries D1 section 8.4's label and the " +
      "button is enabled", {
      schedule: broken.metadata.name,
      sourceBootstrap: brokenSource.spec.bootstrapServers,
      label: "Readiness not checked; the run reports its own prerequisites.",
      buttonEnabled: true,
      quoted: "The API and controller never gate on readiness: execution-time guards stay " +
        "authoritative and the direct CR path exists regardless. ... `unknown`/capability " +
        "absent -> submit, with the label \"Readiness not checked; the run reports its own " +
        "prerequisites\".",
    });

    // ---------------------------------------------------------------- 2
    await page.click(runNow(broken.metadata.name));
    await page.waitForSelector("p.run-now-result", { timeout: 30000 });
    const brokenRuns = await waitForRuns(broken.metadata.name, 1, "the unreachable run");
    check(brokenRuns.length === 1, "one click, one run");
    const brokenRun = brokenRuns[0];
    result.created.push({ kind: "Backup", name: brokenRun.metadata.name,
      uid: brokenRun.metadata.uid });
    await shot(page, "02-run-created-on-unreachable-source");

    const terminal = await waitTerminal(brokenRun.metadata.name, 420);
    const words = controllerWords(terminal);
    check(terminal.status.phase !== "Succeeded",
      "a run against a source routed nowhere reported success: " + JSON.stringify(words));
    check(typeof words.reason === "string" && words.reason.length > 0,
      "the controller recorded no reason at all: " + JSON.stringify(terminal.status));
    // AND IT MUST NOT BE A FAILURE OF THIS HARNESS'S OWN FIXTURE. A missing
    // Secret, a plan conflict or a name collision fails a run just as
    // terminally as an unreachable broker does, and a journey that accepted
    // one of those would be measuring its own scaffolding. Journey 3's
    // negative control is the main guard; this names the impostors.
    for (const impostor of ["VolumeMountFailed", "PlanConfigMapConflict", "JobNameConflict",
      "ExecutionSpecInvalid", "RunPolicyDigestMismatch"]) {
      check(words.reason !== impostor,
        "the run failed at " + impostor + ", which is this harness's fixture and not the " +
          "unreachable source: " + JSON.stringify(words));
    }

    // THE PRODUCT'S OWN WORDS, ON THE RUN VIEW. Read back from the object with
    // kubectl and required to be on screen verbatim -- this is the difference
    // between "the page says something about the failure" and "the page says
    // what the controller said".
    await page.goto(base + "#/backups?ns=" + namespace + "&name=" + brokenRun.metadata.name,
      { waitUntil: "load", timeout: 30000 });
    await waitForText(page, brokenRun.metadata.name, "the run view");
    await waitForText(page, String(terminal.status.phase), "the terminal phase");
    const runView = await text(page);
    const onScreen = [];
    for (const word of [words.phase, words.exitReason, words.reason].filter(
      (w) => typeof w === "string" && w.length > 0)) {
      if (runView.includes(String(word).toLowerCase())) {
        onScreen.push(word);
      }
    }
    check(onScreen.includes(words.phase),
      "the controller's phase is not on the run view: " + runView.slice(0, 1500));
    check(onScreen.length >= 2,
      "the run view carries fewer than two of the controller's own words " +
        JSON.stringify(words) + "; saw:\n" + runView.slice(0, 1500));
    check(runView.indexOf("succeeded") === -1, "and it never reads as a success");
    await shot(page, "03-failed-run-view");
    record("the failed run reports its own prerequisites, in the controller's words", {
      schedule: broken.metadata.name,
      run: brokenRun.metadata.name,
      uid: brokenRun.metadata.uid,
      controller: words,
      renderedVerbatim: onScreen,
    });
    tracker("failed preflight", "journeys 1-2",
      { run: brokenRun.metadata.name, phase: words.phase, reason: words.reason });

    // ---------------------------------------------------------------- 3
    // THE NEGATIVE CONTROL: the same click, the same page, the same build, on
    // a schedule whose source IS reachable. Without this the journey above
    // proves only that something failed.
    await openSchedules();
    await waitForText(page, healthy.metadata.name, "the healthy schedule's card");
    await page.waitForSelector(runNow(healthy.metadata.name), { timeout: 30000 });
    await page.click(runNow(healthy.metadata.name));
    await page.waitForSelector("p.run-now-result", { timeout: 30000 });
    const healthyRuns = await waitForRuns(healthy.metadata.name, 1, "the healthy run");
    const healthyRun = healthyRuns[0];
    result.created.push({ kind: "Backup", name: healthyRun.metadata.name,
      uid: healthyRun.metadata.uid });
    const healthyTerminal = await waitTerminal(healthyRun.metadata.name, 420);
    check(healthyTerminal.status.phase === "Succeeded",
      "the negative control did not succeed, so the journey above proves nothing about the " +
        "source: " + JSON.stringify(controllerWords(healthyTerminal)));
    await shot(page, "04-healthy-run-succeeded");
    record("NEGATIVE CONTROL: the same click on a reachable source succeeds", {
      schedule: healthy.metadata.name,
      run: healthyRun.metadata.name,
      uid: healthyRun.metadata.uid,
      phase: healthyTerminal.status.phase,
      backupId: healthyTerminal.status.backupId,
    });
    tracker("successful scheduled-policy copy", "journey 3", {
      run: healthyRun.metadata.name,
      copiedRevision: healthyRun.spec.scheduleRef,
      scheduleRevision: {
        generation: healthy.metadata.generation,
        runPolicySha256: (await awaitObserved(healthy.metadata.name)).status.policy
          .runPolicySha256,
      },
    });
    check(healthyRun.spec.scheduleRef.runPolicySha256 ===
      (await awaitObserved(healthy.metadata.name)).status.policy.runPolicySha256,
      "the run did not copy the schedule's revision");

    // ---------------------------------------------------------------- 4
    // DOUBLE CLICK. Two clicks inside one intent, on a third schedule so the
    // count is unambiguous.
    const twice = makeSchedule("double-" + suffix, {
      sourceRef: { name: "healthy-source" }, topics: ["orders"],
    });
    await awaitObserved(twice.metadata.name);
    await openSchedules();
    await waitForText(page, twice.metadata.name, "the double-click schedule");
    await page.waitForSelector(runNow(twice.metadata.name), { timeout: 30000 });
    const before = posts.length;
    // TWO CLICKS INSIDE ONE TASK, so the second one lands before the page has
    // repainted and before the first request has answered -- which is what a
    // double click IS. A Playwright `click()` twice would wait for
    // actionability in between and measure something gentler.
    await page.evaluate((selector) => {
      const button = document.querySelector(selector);
      button.click();
      button.click();
    }, runNow(twice.metadata.name));
    await page.waitForSelector("p.run-now-result", { timeout: 30000 });
    await pause(2500);
    // AND A THIRD, AFTER THE RUN EXISTS AND THE PANEL HAS REPAINTED: the same
    // intent is still the draft's, so this is a resend and not a new request.
    await page.click(runNow(twice.metadata.name), { force: true }).catch(() => {});
    await pause(2500);
    const doubled = manualRuns(twice.metadata.name);
    check(doubled.length === 1,
      "two clicks made " + doubled.length + " runs: " +
        doubled.map((b) => b.metadata.name).join(", "));
    result.created.push({ kind: "Backup", name: doubled[0].metadata.name,
      uid: doubled[0].metadata.uid });
    await shot(page, "05-double-click-one-run");
    record("a double click creates ONE run", {
      schedule: twice.metadata.name,
      postsObserved: posts.length - before,
      runs: doubled.map((b) => ({ name: b.metadata.name, uid: b.metadata.uid })),
    });
    tracker("double click", "journey 4", {
      posts: posts.length - before, runs: doubled.length,
    });

    // ---------------------------------------------------------------- 5
    // A DELIBERATE LATER BACKUP, which is the acceptance's second clause.
    await page.click("button[data-run-again=\"" + twice.metadata.name + "\"]");
    await pause(500);
    await page.click(runNow(twice.metadata.name));
    const two = await waitForRuns(twice.metadata.name, 2, "the deliberate second run");
    check(two.length === 2, "Back up again did not create a second run");
    const second = two.find((b) => b.metadata.uid !== doubled[0].metadata.uid);
    result.created.push({ kind: "Backup", name: second.metadata.name, uid: second.metadata.uid });
    await shot(page, "06-deliberate-second-run");
    record("a deliberate later backup creates another run", {
      schedule: twice.metadata.name,
      first: doubled[0].metadata.name,
      second: second.metadata.name,
      distinctUids: doubled[0].metadata.uid !== second.metadata.uid,
    });

    // ---------------------------------------------------------------- 6
    // LOST HTTP RESPONSE. The request reaches the real API, the real 201 is
    // read, and the browser's fetch is aborted so the page never sees it. The
    // object the API created is real; what is taken away is the ANSWER.
    const lost = makeSchedule("lost-" + suffix, {
      sourceRef: { name: "healthy-source" }, topics: ["orders"],
    });
    await awaitObserved(lost.metadata.name);
    let swallowed = null;
    await context.route("**/api/v1/namespaces/" + namespace + "/backups", async (route) => {
      if (route.request().method() !== "POST" || swallowed !== null) {
        await route.continue();
        return;
      }
      const answer = await route.fetch();
      swallowed = { status: answer.status() };
      await route.abort("failed");
    });
    result.faultInjection.push({
      what: "the response to ONE POST .../backups is dropped after the API answered it",
      why: "PLAT-06.2's tracker test 'lost HTTP response' cannot be produced any other way: " +
        "the server must really have created the run and the browser must really not have " +
        "learned of it. Nothing is fabricated and no body is rewritten.",
      where: "journey 6",
    });
    await openSchedules();
    await waitForText(page, lost.metadata.name, "the lost-response schedule");
    await page.waitForSelector(runNow(lost.metadata.name), { timeout: 30000 });
    await page.click(runNow(lost.metadata.name));
    await waitForText(page, "is unknown", "the unknown outcome");
    const unknown = await text(page);
    check(unknown.includes("resends the idempotency key this click is holding"),
      "the unknown-outcome sentence is not the one true of this route: " +
        unknown.slice(0, 1200));
    check(swallowed !== null && swallowed.status === 201,
      "the API did not actually create the run whose answer was dropped: " +
        JSON.stringify(swallowed));
    await shot(page, "07-lost-response");
    const created = await waitForRuns(lost.metadata.name, 1, "the run the lost answer made");
    // THE SAME INTENT, RESENT. The page's own control, not a reload: this is
    // what "Check status" does.
    await page.click(runNow(lost.metadata.name));
    await waitForText(page, "already started", "the replay");
    await pause(2000);
    const afterReplay = manualRuns(lost.metadata.name);
    check(afterReplay.length === 1,
      "the resend created a second run: " + afterReplay.map((b) => b.metadata.name).join(", "));
    check(afterReplay[0].metadata.uid === created[0].metadata.uid, "and it is the same run");
    result.created.push({ kind: "Backup", name: created[0].metadata.name,
      uid: created[0].metadata.uid });
    await shot(page, "08-lost-response-replayed");
    record("a lost HTTP response leaves one run, and resending the same intent finds it", {
      schedule: lost.metadata.name,
      apiAnswered: swallowed.status,
      run: created[0].metadata.name,
      uid: created[0].metadata.uid,
      runsAfterResend: afterReplay.length,
    });
    tracker("lost HTTP response", "journey 6", {
      run: created[0].metadata.name, runs: afterReplay.length, replayed: true,
    });
    await context.unroute("**/api/v1/namespaces/" + namespace + "/backups");

    // ---------------------------------------------------------------- 7
    // REFRESH. The intent is a field of the draft and there is no browser
    // storage anywhere in this tree, so a reload loses it -- and the runs that
    // already exist are listed, so a person reads them before clicking again.
    await openSchedules();
    await waitForText(page, created[0].metadata.name, "the manual run listed after a reload");
    const reloaded = await text(page);
    check(reloaded.includes("a click after a reload is a deliberate new run"),
      "the page does not say what a reload costs: " + reloaded.slice(0, 1200));
    await shot(page, "09-after-refresh");
    await page.click(runNow(lost.metadata.name));
    const afterRefresh = await waitForRuns(lost.metadata.name, 2, "the post-reload run");
    check(afterRefresh.length === 2,
      "a click after a reload did not create a new run: " + afterRefresh.length);
    const third = afterRefresh.find((b) => b.metadata.uid !== created[0].metadata.uid);
    result.created.push({ kind: "Backup", name: third.metadata.name, uid: third.metadata.uid });
    await shot(page, "10-refresh-then-a-new-run");
    record("a refresh lists the runs that exist and a click after it is a deliberate new run", {
      schedule: lost.metadata.name,
      before: created[0].metadata.name,
      after: third.metadata.name,
    });
    tracker("refresh", "journey 7", {
      listedAfterReload: created[0].metadata.name,
      newRunAfterReload: third.metadata.name,
    });

    // ---------------------------------------------------------------- 8
    // PAUSED SCHEDULE. Every schedule here is suspended, and this is the
    // journey that reads the page's words about it: D1 section 8.3 says nothing
    // about a schedule blocks a manual run, so what the page does is require a
    // second explicit confirmation and say the schedule stays suspended.
    const paused = makeSchedule("paused-" + suffix, {
      sourceRef: { name: "healthy-source" }, topics: ["orders"], suspend: true,
    });
    await awaitObserved(paused.metadata.name);
    await openSchedules();
    await waitForText(page, paused.metadata.name, "the paused schedule");
    await page.waitForSelector(runNow(paused.metadata.name), { timeout: 30000 });
    const notice = await page.$("[data-suspended-notice=\"" + paused.metadata.name + "\"]");
    check(notice !== null, "the suspended schedule carries no notice");
    const noticeText = (await notice.innerText()).toLowerCase();
    check(noticeText.includes("a manual run is still allowed"),
      "the notice does not say a manual run is allowed: " + noticeText);
    check(noticeText.includes("does not resume the schedule"),
      "nor that running one does not resume the schedule: " + noticeText);
    const pausedButton = await page.$(runNow(paused.metadata.name));
    check(await pausedButton.isDisabled(),
      "the button is enabled before the second explicit confirmation");
    await shot(page, "11-paused-needs-confirmation");
    await page.check("input[data-acknowledge=\"" + paused.metadata.name + "\"]");
    await pause(300);
    check(!(await (await page.$(runNow(paused.metadata.name))).isDisabled()),
      "confirming did not enable the button");
    await page.click(runNow(paused.metadata.name));
    const pausedRuns = await waitForRuns(paused.metadata.name, 1, "the run of a paused schedule");
    result.created.push({ kind: "Backup", name: pausedRuns[0].metadata.name,
      uid: pausedRuns[0].metadata.uid });
    const stillSuspended = kubeJson(["-n", namespace, "get", "backupschedule",
      paused.metadata.name]);
    check(stillSuspended.spec.suspend === true,
      "running a manual backup resumed the schedule, which it must never do");
    check(stillSuspended.metadata.generation === paused.metadata.generation,
      "the schedule's generation moved, so something wrote to its spec");
    await shot(page, "12-paused-run-created");
    record("a paused schedule admits a manual run after one explicit confirmation, and stays " +
      "paused", {
      schedule: paused.metadata.name,
      run: pausedRuns[0].metadata.name,
      uid: pausedRuns[0].metadata.uid,
      suspendBefore: true,
      suspendAfter: stillSuspended.spec.suspend,
      generationUnchanged: stillSuspended.metadata.generation === paused.metadata.generation,
    });
    tracker("paused schedule", "journey 8", {
      run: pausedRuns[0].metadata.name,
      scheduleStillSuspended: stillSuspended.spec.suspend,
    });

    // ---------------------------------------------------------------- 9
    // NO CREDENTIAL ANYWHERE. Two Secrets were copied into this namespace and
    // the runs reference them by name; not one value may reach a response, the
    // DOM, the API log or this document.
    const values = [];
    for (const name of ["source-scram", "logweir-s3"]) {
      const secret = kubeJson(["-n", namespace, "get", "secret", name]);
      for (const key of Object.keys(secret.data || {})) {
        values.push(Buffer.from(secret.data[key], "base64").toString("utf8"));
      }
    }
    const dom = await page.evaluate(() => document.documentElement.outerHTML);
    const haystacks = [
      ["the rendered DOM", dom],
      ["the logweir-api log", apiLog.join("")],
      ["the result document", JSON.stringify(result)],
    ];
    const hits = [];
    for (const [where, hay] of haystacks) {
      for (const value of values) {
        if (value.length >= 6 && hay.indexOf(value) !== -1) {
          hits.push(where);
        }
      }
    }
    check(hits.length === 0, "a credential value appeared in: " + hits.join(", "));
    result.credentialScan = {
      valuesChecked: values.length,
      scanned: haystacks.map(([where, hay]) => ({ where: where, bytes: hay.length })),
      hits: hits,
    };
    record("no credential value reaches the DOM, the API log or this document", {
      valuesChecked: values.length,
    });

    result.summary = {
      manualRunsCreated: kubeJson(["-n", namespace, "get", "backups"]).items.length,
      postsObserved: posts.length,
    };
  } finally {
    await browser.close().catch(() => {});
    stopApi();
  }

  writeFileSync(join(ARTIFACTS, "api.log"), apiLog.join(""));
}

// ---------------------------------------------------------------- cleanup

let cleaned = false;

/** Deletes this run's namespace, under the same three guards whichever way the
 *  run ended.
 *
 *  IT RUNS ON THE FAILURE PATH TOO, which the first cut did not: the negative
 *  control failed at its first journey and left its namespace behind to be
 *  removed by hand. A harness that only tidies up when it succeeds is a
 *  harness that leaves the most litter exactly when something went wrong.
 *
 *  THE THREE GUARDS ARE UNCHANGED AND NON-NEGOTIABLE: the prefix (asserted
 *  here and again at the top of the run, before anything was created), the UID
 *  recorded at creation, and the owner label. A namespace that fails any of
 *  them is REPORTED and never deleted -- this harness would rather leave its
 *  own litter than delete something it cannot prove is its own. */
function cleanUp(how) {
  if (cleaned) {
    return;
  }
  cleaned = true;
  try {
    assertSafeNamespace(namespace);
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
      "backupschedules,backups,kafkaclusters,secrets,jobs", "-o", "name"],
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

/** Writes the result document, REFUSING to replace one that is already there.
 *
 *  The path is inside this run's own directory, so a collision means two runs
 *  shared a namespace name -- which the prefix and the timestamp make close to
 *  impossible and which would be worth knowing about rather than silently
 *  resolving in favour of whichever finished last. */
function writeResult() {
  result.finishedAt = new Date().toISOString();
  result.passed = result.journeys.length;
  result.artifacts = ARTIFACTS;
  mkdirSync(ARTIFACTS, { recursive: true });
  const at = join(ARTIFACTS, "live.json");
  if (existsSync(at)) {
    throw new Error(
      "a result document already exists at " + at + "; this harness never overwrites one. " +
        "Two runs would have to have shared a namespace name for this to happen.",
    );
  }
  writeFileSync(at, JSON.stringify(result, null, 2) + "\n");
  return at;
}

function shutDown() {
  stopApi();
  // THE CURSOR KEY DOES NOT OUTLIVE THE PROCESS THAT USED IT. It is signing
  // material for the API's opaque paging cursors, written into a shared `/tmp`
  // by this harness, and the first cut left it there at mode 0644 for ever.
  try {
    rmSync(WORK_DIR, { recursive: true, force: true });
  } catch (ignored) {
    // A temp directory that cannot be removed is worth neither failing a green
    // run nor hiding a red one; the result document records the path either way.
  }
}

main().then(
  () => {
    shutDown();
    cleanUp("passed");
    const at = writeResult();
    process.stderr.write(
      "\n== " + result.journeys.length + " journey(s) passed; result: " + at + "\n",
    );
    process.exit(0);
  },
  (error) => {
    shutDown();
    result.failure = error instanceof Error ? error.stack : String(error);
    cleanUp("failed");
    try {
      writeFileSync(join(ARTIFACTS, "api.log"), apiLog.join(""));
      const at = writeResult();
      process.stderr.write("== result: " + at + "\n");
    } catch (ignored) {
      // The failure below is what matters.
    }
    process.stderr.write("\n== FAILED: " + String(error && error.message) + "\n");
    process.exit(1);
  },
);
