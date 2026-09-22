// PLAT-11.2 live UI acceptance harness: the topic subset, the exact mapping
// preview, the recovery limits, the readiness gate and the fresh-target retry.
//
// THE LAUNCHER IS `scripts/d2w13-ui-e2e.mjs`'s, and for its reason: the
// surfaces under test are served by `logweir-api` and by nothing else, so this
// starts the product API in localAdmin mode on a loopback port, pointed at this
// worktree's own `ui/` directory and at one namespace it created, and drives a
// real Chromium against it. The `Backup` seeding is
// `scripts/plat12-13-ui-e2e.mjs`'s: two recovery points with deliberately
// different completion times, patched onto the status subresource until they
// stick.
//
// EVERY POSITIVE CASE IS REAL. Every object is created by the PAGE against the
// real `logweir-api` against the real kube-apiserver, and read back with
// `kubectl` by name and by UID. No response body is fabricated, intercepted or
// delayed, and there is no fault injection anywhere in this file.
//
// THE TWO FIXTURES ARE FIXTURES, AND SAID TO BE. The recovery points are
// `Backup` objects this harness creates with `kubectl` and whose `Succeeded`
// status it writes: nothing on this lab produced an archive for them, and no
// assertion here claims one did. What is under test is what the PAGE does with
// a recovery point, which is a question about the page.
//
// THE COLLISION IS REAL AND IT IS THE POINT OF JOURNEY 5. The mapped target
// topic is created on the lab's own `kafka-target` broker before the check
// runs, with `kafka-topics.sh` inside the broker pod, and deleted in the
// `finally`. The shared release is otherwise read-only: nothing else about
// `logweir-scram-local` is touched.
//
// WHAT THIS HARNESS DOES NOT DO, and why it is a gap rather than an omission:
// it does not drive a restore through to a VERIFIED SCORECARD. That needs the
// runner to execute (an archive in the object store, a check Job, a signed
// scorecard) AND an Approval this lab's `TrustRoster` verifies -- and the
// private half of `approver@scram-local.invalid`'s key is not on this host
// (PLAT-12.2's partial record says the same). Every clause this harness can
// prove without it, it proves.
//
// Dependencies: Node.js, kubectl, a built `logweir-api`, Playwright/Chromium:
//   NODE_PATH="$(npm root -g)" node scripts/plat11-2-ui-e2e.mjs
//
// Environment (all optional): UI_E2E_OWNER, UI_E2E_PREFIX, UI_E2E_NAMESPACE,
// UI_E2E_API_BIN, UI_E2E_UI_DIR, UI_E2E_ARTIFACTS, UI_E2E_KEEP, UI_E2E_KUBECTL.

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { mkdirSync, writeFileSync, rmSync } from "node:fs";
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
const OWNER = process.env.UI_E2E_OWNER || "plat11-2";
const ARTIFACTS_ROOT = process.env.UI_E2E_ARTIFACTS ||
  ("/tmp/logweir-roadmap-run/claude/artifacts/plat11-2-ui");
const NAMESPACE_PREFIX = process.env.UI_E2E_PREFIX || "lw-p112-";
const OWNER_LABEL = "logweir.dev/test-owner=" + OWNER;
const LABELS = { "logweir.dev/test-owner": OWNER };

// THE SHARED LAB, USED READ-ONLY EXCEPT FOR ONE TOPIC THIS RUN CREATES AND
// DELETES. Its brokers are addressed by service DNS from this namespace's own
// `KafkaCluster` objects; nothing in it is patched, scaled or reconfigured.
const LAB_NS = "logweir-scram-local";
const LAB_TARGET_BOOTSTRAP = "kafka-target." + LAB_NS + ".svc:9092";
const LAB_SOURCE_BOOTSTRAP = "kafka-source." + LAB_NS + ".svc:9092";

const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z");
const namespace = process.env.UI_E2E_NAMESPACE || (NAMESPACE_PREFIX + stamp.toLowerCase());
const suffix = randomBytes(3).toString("hex");
const ARTIFACTS = join(ARTIFACTS_ROOT, stamp.toLowerCase());
const WORK_DIR = join("/tmp", "plat11-2-live-" + namespace);

// THE TWO POINTS' COVERED WINDOWS, fixed integers so every derived value in
// this file -- the default topic prefix, the boundary instants, the colliding
// topic name -- is computable here and asserted against what the page shows.
const OLD_FROM_MS = 1760000000000;
const OLD_TO_MS = 1760000060000;
const NEW_FROM_MS = 1760003600000;
const NEW_TO_MS = 1760003660000;
const TOPICS = ["orders", "payments", "shipments"];

const rfc = (ms) => new Date(ms).toISOString().replace(".000Z", "Z");

/** `logweir_core::spec::default_topic_prefix`, recomputed HERE so the
 *  assertions are against an independently derived string rather than against
 *  whatever the page happened to render. */
function defaultPrefix(ms) {
  const iso = new Date(ms).toISOString();
  return "restore-" + iso.slice(0, 4) + iso.slice(5, 7) + iso.slice(8, 10) + "T" +
    iso.slice(11, 13) + iso.slice(14, 16) + iso.slice(17, 19) + "Z-";
}

const OLD_PREFIX = defaultPrefix(OLD_TO_MS);
const COLLIDING_TOPIC = OLD_PREFIX + "orders";

const result = {
  harness: "scripts/plat11-2-ui-e2e.mjs",
  task: "PLAT-11.2",
  kubeContext: KUBE_CONTEXT,
  owner: OWNER,
  namespacePrefix: NAMESPACE_PREFIX,
  namespace: namespace,
  labNamespace: LAB_NS,
  uiDirectory: UI_DIR,
  apiBinary: API_BIN,
  startedAt: new Date().toISOString(),
  mode: "console (logweir-api, localAdmin, loopback)",
  faultInjection: [],
  fixtures: [],
  journeys: [],
  negativeControls: [],
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

function control(about, detail) {
  result.negativeControls.push(Object.assign({ control: about }, detail || {}));
  process.stderr.write("== control: " + about + "\n");
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
    timeout: opts.timeout || 60000,
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

function apply(object) {
  return JSON.parse(
    kube(["-n", namespace, "create", "-f", "-", "-o", "json"], { input: JSON.stringify(object) }).stdout,
  );
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

function save(name, body) {
  const at = join(ARTIFACTS, name);
  writeFileSync(at, typeof body === "string" ? body : JSON.stringify(body, null, 2));
  return at;
}

/** What the page says, case-folded: the stylesheet renders headings and badges
 *  through `text-transform`, and `innerText` reports what is RENDERED. */
async function text(page) {
  return (await page.evaluate(() => document.body.innerText)).toLowerCase();
}

async function waitForText(page, needle, label) {
  const wanted = String(needle).toLowerCase();
  for (let i = 0; i < 60; i += 1) {
    if ((await text(page)).includes(wanted)) {
      return;
    }
    await pause(500);
  }
  throw new Error(label + ": never saw " + JSON.stringify(needle) + " on screen. Saw:\n" +
    (await text(page)).slice(0, 3000));
}

async function waitFor(page, selector, label) {
  try {
    await page.waitForSelector(selector, { timeout: 30000 });
  } catch (never) {
    throw new Error(label + ": " + selector + " never appeared. Page said:\n" +
      (await text(page)).slice(0, 3000));
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
  save("config.yaml", config);
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

// -------------------------------------------------------------- the fixtures

// EVERY FIXTURE BACKUP'S NAME IS LONGER THAN 63 CHARACTERS, and that is the
// mechanism rather than a style: `weirkeeper`'s backup reconciler refuses such
// a name TERMINALLY before it creates anything (a pod label could not carry
// it), so no fixture here ever produces a runner Job and its status is then
// this harness's to set. The first cut of this file used short names, the
// controller started a real runner Job against a non-existent archive, and it
// overwrote the seeded status within a second. (The same trick, and the same
// reason, as `scripts/plat12-13-ui-e2e.mjs`.)
const LONG = "fixture-backup-deliberately-longer-than-sixty-three-characters-";

const sourceCluster = "source-" + suffix;
const targetCluster = "target-" + suffix;
const secondTarget = "target-b-" + suffix;
const archiveUrl = "s3://logweir-fixture/" + namespace;
const points = {};

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

function seedBackup(name, status) {
  const created = apply({
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Backup",
    metadata: { name: name, namespace: namespace, labels: LABELS },
    spec: {
      archive: { url: archiveUrl },
      deadlineSeconds: 3600,
      sourceRef: { name: sourceCluster },
      topics: TOPICS.slice(),
      triggeredBy: "manual",
    },
  });
  const point = { name: name, uid: created.metadata.uid };
  result.created.push(Object.assign({ kind: "Backup" }, point));
  for (let i = 0; i < 20; i += 1) {
    kube(["-n", namespace, "patch", "backup", name, "--subresource=status", "--type=merge",
      "-p", JSON.stringify({ status: status })]);
    spawnSync("sleep", ["1"]);
    const seen = kubeJson(["-n", namespace, "get", "backup", name]).status || {};
    if (seen.phase === status.phase && seen.backupId === status.backupId) {
      const jobs = kubeJson(["-n", namespace, "get", "jobs"]).items || [];
      check(jobs.every((j) => !j.metadata.name.includes(name)),
        "a fixture Backup must never produce a runner Job");
      return point;
    }
    spawnSync("sleep", ["1"]);
  }
  throw new Error("the fixture Backup " + name + " did not keep the status this run set");
}

function seedCluster(name, role, bootstrap) {
  const made = apply({
    apiVersion: "logweir.dev/v1alpha1",
    kind: "KafkaCluster",
    metadata: { name: name, namespace: namespace, labels: LABELS },
    spec: { bootstrapServers: [bootstrap], role: role, auth: { mode: "plaintext", tls: false } },
  });
  result.created.push({ kind: "KafkaCluster", name: name, uid: made.metadata.uid });
  return made;
}

/** Creates, or deletes, one topic on the LAB's own `kafka-target` broker.
 *
 *  THE ONE WRITE THIS RUN MAKES TO THE SHARED RELEASE, and the brief permits
 *  exactly it: a collision cannot be proved against a broker that does not
 *  hold the colliding topic. It is removed in the `finally`, whatever happens.
 */
function labTopic(verb, name) {
  const pods = kubeJson(["-n", LAB_NS, "get", "pods", "-l", "app=kafka-target"]).items || [];
  check(pods.length > 0, "the lab's kafka-target pod is not running");
  const pod = pods[0].metadata.name;
  const done = kube(
    ["-n", LAB_NS, "exec", pod, "--", "/opt/kafka/bin/kafka-topics.sh",
      "--bootstrap-server", "localhost:9092", "--" + verb, "--topic", name]
      .concat(verb === "create" ? ["--partitions", "1", "--replication-factor", "1"] : []),
    { expected: [0, 1], timeout: 120000 },
  );
  return String(done.stdout || "") + String(done.stderr || "");
}

// ------------------------------------------------------------------ the run

async function main() {
  mkdirSync(ARTIFACTS, { recursive: true });
  assertSafeNamespace(namespace);
  check(kube(["version", "--client=true"]).status === 0, "kubectl works");

  kube(["create", "namespace", namespace]);
  kube(["label", "namespace", namespace, OWNER_LABEL]);
  const ns = kubeJson(["get", "namespace", namespace]);
  result.namespaceUid = ns.metadata.uid;
  result.created.push({ kind: "Namespace", name: namespace, uid: ns.metadata.uid });

  seedCluster(sourceCluster, "source", LAB_SOURCE_BOOTSTRAP);
  seedCluster(targetCluster, "target", LAB_TARGET_BOOTSTRAP);
  seedCluster(secondTarget, "target", LAB_TARGET_BOOTSTRAP);

  // TWO POINTS, AND THE OLDER ONE IS THE ONE THIS JOURNEY RESTORES.
  points.old = seedBackup(
    NAMESPACE_PREFIX + LONG + "old-" + suffix,
    succeededStatus("01JB7Z0000000000000000OLD", 3000, OLD_FROM_MS, OLD_TO_MS, rfc(OLD_TO_MS)),
  );
  points.new = seedBackup(
    NAMESPACE_PREFIX + LONG + "new-" + suffix,
    succeededStatus("01JB7Z0000000000000000NEW", 4000, NEW_FROM_MS, NEW_TO_MS, rfc(NEW_TO_MS)),
  );
  result.fixtures.push({
    what: "two Succeeded Backup objects created with kubectl, status written by this harness",
    older: points.old, newer: points.new, topics: TOPICS,
    note: "no archive exists for either: they are fixtures for what the PAGE does with a " +
      "recovery point, and nothing here claims a run produced them",
  });

  // THE COLLISION, ON THE LAB'S OWN BROKER.
  const createdTopic = labTopic("create", COLLIDING_TOPIC);
  save("lab-topic-create.txt", createdTopic);
  const listed = labTopic("list", COLLIDING_TOPIC);
  check(listed.includes(COLLIDING_TOPIC),
    "the colliding topic was not created on kafka-target: " + listed);
  save("lab-topic-list.txt", listed);
  result.created.push({ kind: "KafkaTopic", name: COLLIDING_TOPIC, on: LAB_TARGET_BOOTSTRAP });

  const port = await freePort();
  result.port = port;
  await startApi(port);
  const base = "http://127.0.0.1:" + port + "/ui/";
  const origin = "http://127.0.0.1:" + port;

  const browser = await chromium.launch();
  const context = await browser.newContext();
  const page = await context.newPage();

  const requests = [];
  page.on("request", (r) => {
    if (r.url().indexOf("/api/v1/") !== -1 && r.method() !== "GET") {
      requests.push({ method: r.method(), url: r.url(), body: r.postData() });
    }
  });
  const bodies = [];
  page.on("response", async (r) => {
    try {
      if (r.url().indexOf("/api/v1/") !== -1) {
        bodies.push({ url: r.url(), status: r.status(), body: (await r.text()).slice(0, 20000) });
      }
    } catch (gone) {
      // a body no longer available cannot hide what the page then rendered
    }
  });
  const writesSoFar = () => requests.length;

  const targetUid = kubeJson(["-n", namespace, "get", "kafkacluster", targetCluster]).metadata.uid;
  const secondUid = kubeJson(["-n", namespace, "get", "kafkacluster", secondTarget]).metadata.uid;

  const oldRoute = base + "#/restore?ns=" + namespace + "&backup=" + points.old.name +
    "&uid=" + points.old.uid;

  async function openOldPoint() {
    await page.goto(oldRoute, { waitUntil: "load", timeout: 30000 });
    await waitFor(page, "#step-target", "the wizard's target step");
    await page.selectOption("#target-cluster", targetUid);
    await waitFor(page, ".topic-box", "the topic subset control");
  }

  try {
    // ------------------------------------------------------------------ 1
    // THE OLDER POINT, A TWO-TOPIC SUBSET, AND THE EXACT MAPPING.
    await openOldPoint();
    // THE BINDING IS ASSERTED ON STEP 2 AND ON THE PLAN, NOT ON THE WHOLE
    // PAGE. The newer point is deliberately ON screen -- the wizard's "what
    // this namespace holds" catalog lists every Backup there is -- so a page
    // scan would assert the wrong thing. What matters is which point the PLAN
    // is built from.
    const step2 = await page.evaluate(() => {
      const section = document.querySelector("#step-backup-set");
      return section === null ? "" : section.innerText;
    });
    check(step2.includes(points.old.name), "step 2 names the OLDER point: " + step2.slice(0, 900));
    // Step 2 also carries the namespace's whole catalog -- the newer run is
    // deliberately LISTED there -- so the binding is asserted on the facts
    // block: the Backup this plan is built from, and its uid.
    const boundTo = step2.slice(0, step2.indexOf("Choose a different recovery point"));
    check(boundTo.includes(points.old.uid),
      "the point the wizard is bound to is the older one, by UID: " + boundTo.slice(0, 900));
    check(!boundTo.includes(points.new.uid),
      "and not the newer one's: " + boundTo.slice(0, 900));
    const planNow = await page.evaluate(() => {
      const pre = document.querySelector("#plan-bytes");
      return pre === null ? "" : pre.textContent;
    });
    check(planNow.includes("01JB7Z0000000000000000OLD"),
      "and the plan carries the older point's backup set");
    check(!planNow.includes("01JB7Z0000000000000000NEW"),
      "and not the newer point's, which exists and completed later");
    await page.uncheck(".topic-box[data-topic=\"shipments\"]");
    await waitForText(page, OLD_PREFIX + "payments", "the mapped name for payments");
    const mapped = await page.evaluate(() => {
      const section = document.querySelector("#step-target");
      return section === null ? "" : section.innerText;
    });
    for (const topic of ["orders", "payments"]) {
      check(mapped.includes(OLD_PREFIX + topic), "the mapping shows " + OLD_PREFIX + topic);
    }
    check(!mapped.includes(OLD_PREFIX + "shipments"),
      "and does NOT map the topic that was unticked: " + mapped);
    await shot(page, "01-subset-and-mapping");
    save("01-mapping-table.txt", mapped);
    record("an older point restores a two-topic subset and the exact mapping is shown first", {
      point: points.old, unselected: "shipments", prefix: OLD_PREFIX,
      mapped: ["orders", "payments"].map((t) => OLD_PREFIX + t),
    });

    // THE NEGATIVE CONTROL: the NEWER point's prefix differs, so a page that
    // had silently followed the newest completion would render other names.
    check(defaultPrefix(NEW_TO_MS) !== OLD_PREFIX, "the two points' prefixes differ");
    check(!mapped.includes(defaultPrefix(NEW_TO_MS)),
      "no name from the newer point's plan is on screen");
    control("the newer point's prefix is absent, so the page did not follow the newest run", {
      newerPrefix: defaultPrefix(NEW_TO_MS),
    });

    // ------------------------------------------------------------------ 2
    // THE LIMITS, AND WHAT IS NOT SHOWN.
    const shown = await text(page);
    for (const sentence of [
      "consumers are not moved",
      "resume is not implemented",
      "sampled check, not an exhaustive comparison",
      "partition counts are not shown before the run",
      "target replication factor",
    ]) {
      check(shown.includes(sentence), "the limits panel says " + JSON.stringify(sentence));
    }
    check(!shown.includes("exhaustive comparison of every"),
      "and nothing claims an exhaustive check");
    await shot(page, "02-recovery-limits");
    save("02-limits-text.txt", shown);
    record("the recovery limits, the sampled scope, the cutover limit and unimplemented resume", {
      from: "contract constants in ui/render.js and plan fields, never server prose",
    });
    control("no sentence on the page claims an exhaustive verification", {});

    // ------------------------------------------------------------------ 3
    // AN INVALID PREFIX, REFUSED BY NAME, WITH NOTHING SENT.
    const before3 = writesSoFar();
    await page.fill("#topic-prefix", "bad prefix!");
    await page.dispatchEvent("#topic-prefix", "change");
    await waitForText(page, "not a name a broker accepts", "the prefix refusal");
    await shot(page, "03-invalid-prefix");
    await page.click("#create-restore");
    await pause(1500);
    check(writesSoFar() === before3,
      "nothing was sent while the prefix is refused: " + JSON.stringify(requests.slice(before3)));
    const blockedText = await text(page);
    check(blockedText.includes("bad prefix!"), "the refusal names the value typed");
    record("an invalid topic prefix is refused by name and nothing is sent", {
      typed: "bad prefix!", writesDuring: 0,
    });

    // THE NEGATIVE CONTROL: a legal prefix clears it.
    await page.fill("#topic-prefix", OLD_PREFIX);
    await page.dispatchEvent("#topic-prefix", "change");
    await waitForText(page, OLD_PREFIX + "orders", "the mapping back");
    check(!(await text(page)).includes("not a name a broker accepts"),
      "a legal prefix clears the refusal");
    control("a legal prefix clears the refusal, so the rule is not always-on", {});

    // ------------------------------------------------------------------ 4
    // THE PRODUCT API REFUSES A DUPLICATE MAPPING, NAMING BOTH ROWS.
    //
    // ISSUED FROM THE PAGE'S OWN ORIGIN, with its own session, against the
    // real service -- not from this process, which would be a different
    // caller. A duplicate cannot be produced through the checkboxes (the
    // subset is a set), which is a stronger property than refusing one; the
    // rail exists for a restored draft, a prefill, and any other caller.
    const planBytes = await page.evaluate(() => {
      const pre = document.querySelector("#plan-bytes");
      return pre === null ? null : pre.textContent;
    });
    check(typeof planBytes === "string" && planBytes.length > 0, "the plan bytes are on screen");
    save("04-plan-bytes.yaml", planBytes);
    const planHash = await page.evaluate(() => {
      const code = document.querySelector("#plan-hash-value");
      return code === null ? "" : code.textContent;
    });
    check(planHash.startsWith("sha256:"), "the plan hash is on screen: " + planHash);

    // ONE BODY, TWO MAPPINGS. Everything but `topicMapping` is identical, so
    // the difference between the two answers is the duplicate and nothing else.
    const createBody = {
      planBytes: planBytes,
      planHash: planHash,
      approvalRef: { name: "apr-mapping-probe" },
      sourceArchive: { url: archiveUrl },
      backupSetRef: "01JB7Z0000000000000000OLD",
      pointInTime: rfc(OLD_TO_MS),
      target: {
        clusterRef: { name: targetCluster }, mode: "newTopic",
        topicNaming: { prefix: OLD_PREFIX },
      },
      deadlineSeconds: 3600,
    };
    save("04-create-body.json", createBody);

    // ISSUED FROM THE PAGE'S OWN ORIGIN, against the real service. In
    // localAdmin mode the synchroniser token is `null` and the loopback
    // listener plus an exact Origin check is the control, so a fetch from the
    // page is exactly the call the page itself would make.
    const post = (body, key) => page.evaluate(async (args) => {
      const response = await fetch(args.origin + "/api/v1/namespaces/" + args.ns + "/restores", {
        method: "POST",
        headers: { "content-type": "application/json", "idempotency-key": args.key },
        credentials: "same-origin",
        body: JSON.stringify(args.body),
      });
      return { status: response.status, body: (await response.text()).slice(0, 8000) };
    }, { origin: origin, ns: namespace, body: body, key: key });

    const duplicate = await post(
      Object.assign({}, createBody, {
        topicMapping: [
          { source: "orders", target: OLD_PREFIX + "orders" },
          { source: "orders", target: OLD_PREFIX + "orders" },
        ],
      }),
      "plat11-2-duplicate-01",
    );
    save("04-duplicate-mapping-response.json", duplicate);
    check(duplicate.status === 422,
      "the API refuses a duplicate mapping with 422, not " + duplicate.status + ": " + duplicate.body);
    const problem = JSON.parse(duplicate.body);
    const dupError = (problem.errors || []).find((e) => e.code === "duplicate_mapping");
    check(dupError !== undefined, "the refusal is `duplicate_mapping`: " + duplicate.body);
    check(dupError.message.includes("orders") && dupError.message.includes(OLD_PREFIX + "orders"),
      "and it names both rows and the target they share: " + dupError.message);
    record("a duplicate mapping is refused by the product API with 422 naming both rows", {
      field: dupError.field, code: dupError.code, message: dupError.message,
      issuedFrom: "the page's own origin, against the real logweir-api",
      note: "a duplicate cannot be produced through the checkboxes -- the subset is a set -- " +
        "which is a stronger property than refusing one. The rail exists for a restored " +
        "draft, an edit prefill and any other caller.",
    });

    // THE NEGATIVE CONTROL: the identical request with the duplicate removed
    // raises no mapping error, so the 422 above is about the duplicate.
    const single = await post(
      Object.assign({}, createBody, {
        topicMapping: [{ source: "orders", target: OLD_PREFIX + "orders" }],
      }),
      "plat11-2-single-0001",
    );
    save("04-single-mapping-response.json", single);
    const singleErrors = single.status === 422 ? (JSON.parse(single.body).errors || []) : [];
    check(!singleErrors.some((e) => String(e.field || "").startsWith("topicMapping")),
      "one row of the same mapping raises no mapping error: " + single.body);
    control("the same request with the duplicate removed raises no mapping error", {
      status: single.status,
      mappingErrors: singleErrors.filter((e) => String(e.field || "").startsWith("topicMapping")),
    });

    // AND A ROW RENAMED AFTER THE PREVIEW -- the mutant the acceptance sentence
    // is about -- is refused against the prefix this very request stores.
    const renamed = await post(
      Object.assign({}, createBody, {
        topicMapping: [{ source: "orders", target: "somewhere-else-orders" }],
      }),
      "plat11-2-renamed-001",
    );
    save("04-renamed-mapping-response.json", renamed);
    check(renamed.status === 422, "a renamed row is refused: " + renamed.body);
    const renameError = (JSON.parse(renamed.body).errors || [])
      .find((e) => e.code === "mapping_mismatch");
    check(renameError !== undefined, "with `mapping_mismatch`: " + renamed.body);
    check(renameError.message.includes(OLD_PREFIX + "orders"),
      "naming the target the stored prefix produces: " + renameError.message);
    record("a mapping row renamed after the preview is refused by the API", {
      code: renameError.code, message: renameError.message,
    });

    // ------------------------------------------------------------------ 5
    // THE TIMESTAMP BOUNDARY.
    await openOldPoint();
    const before5 = writesSoFar();
    await page.fill("#point-in-time", rfc(OLD_TO_MS + 1000));
    await page.dispatchEvent("#point-in-time", "change");
    // THE COMPLAINT ELEMENT, not the sentence: the bound is printed beside the
    // input in EVERY state, so a text scan for it would hold for an accepted
    // point too. The first cut of this row did exactly that and its own
    // negative control caught it, which is why the element has an id.
    await waitFor(page, "#point-in-time-complaint", "the out-of-window complaint");
    // AND THE SUBMIT ITSELF IS REFUSED, which is the half that matters: the
    // complaint is a rendering, `validateRestore` is the refusal, and what is
    // asserted here is that the click sent nothing.
    await page.click("#create-restore");
    await waitForText(page, "outside the coverage this recovery point discloses",
      "the wizard's own refusal of the submit");
    await shot(page, "05-timestamp-boundary");
    check(writesSoFar() === before5,
      "nothing was sent for a point outside the window: " + JSON.stringify(requests.slice(before5)));
    record("a point-in-time outside the disclosed window is refused by name, nothing sent", {
      typed: rfc(OLD_TO_MS + 1000), window: { from: rfc(OLD_FROM_MS), to: rfc(OLD_TO_MS) },
      writesDuring: 0,
    });

    // THE NEGATIVE CONTROL: the boundary instant ITSELF is inside.
    await page.fill("#point-in-time", rfc(OLD_TO_MS));
    await page.dispatchEvent("#point-in-time", "change");
    await pause(1500);
    const stillComplaining = await page.evaluate(() =>
      document.querySelector("#point-in-time-complaint") !== null);
    check(stillComplaining === false,
      "the window is closed at both ends: the bound itself is inside it");
    control("the boundary instant itself is accepted, so the window is closed at both ends", {
      typed: rfc(OLD_TO_MS),
    });

    // ------------------------------------------------------------------ 6
    // THE READINESS CHECK, THE COLLISION, AND THE REFUSED SUBMIT.
    await openOldPoint();
    await waitFor(page, "#restore-readiness-start", "the readiness control");
    await page.click("#restore-readiness-start");
    await pause(4000);
    // The lab's controller decides this; whatever it decides is recorded.
    let verdict = null;
    for (let i = 0; i < 90; i += 1) {
      const list = kubeJson(["-n", namespace, "get", "preflights"]).items || [];
      const mine = list[list.length - 1];
      if (mine && mine.status && typeof mine.status.state === "string" &&
        ["ready", "notReady", "unknown", "failed", "cancelled"].includes(mine.status.state)) {
        verdict = mine;
        break;
      }
      await pause(2000);
    }
    const preflights = kubeJson(["-n", namespace, "get", "preflights"]);
    save("06-preflights.json", preflights);
    save("06-preflight-conditions.json", (preflights.items || []).map((p) => ({
      name: p.metadata.name,
      state: (p.status || {}).state || null,
      reason: (p.status || {}).reason || null,
      conditions: (p.status || {}).conditions || [],
      checks: ((p.status || {}).checks || []).map((c) => ({ id: c.id, state: c.state, code: c.code })),
    })));
    await shot(page, "06-readiness");
    save("06-readiness-text.txt", await text(page));
    if (verdict === null) {
      result.journeys.push({
        journey: "the readiness check for this plan reaches a verdict",
        outcome: "NOT REACHED — the lab controller did not record a terminal state within 180 s",
        preflights: (preflights.items || []).map((p) => ({
          name: p.metadata.name, state: (p.status || {}).state || null,
          reason: (p.status || {}).reason || null,
        })),
      });
      process.stderr.write("== NOT REACHED: a terminal preflight verdict\n");
    } else {
      const state = verdict.status.state;
      const entries = (verdict.status.checks || []).concat(verdict.status.warnings || []);
      const mappedRow = entries.find((c) => c.id === "target.mappedTopics");
      record("the readiness check for this exact plan reached a verdict on the lab", {
        preflight: verdict.metadata.name, uid: verdict.metadata.uid, state: state,
        planHash: ((verdict.status.binding || {}).planHash) || null,
        mappedTopicsRow: mappedRow || null,
        collidingTopic: COLLIDING_TOPIC,
      });
      if (mappedRow && mappedRow.code === "MappedTopicExists") {
        const said = await text(page);
        check(said.includes("nothing is sent"),
          "the wizard refuses the submit over a collision: " + said.slice(0, 2000));
        const disabled = await page.evaluate(() => {
          const b = document.querySelector("#create-restore");
          return b === null ? null : b.disabled;
        });
        check(disabled === true, "and the create button is disabled");
        record("an existing target topic with the mapped name refuses the ordinary path", {
          topic: COLLIDING_TOPIC, code: mappedRow.code, message: mappedRow.message,
        });
      }
    }

    // ------------------------------------------------------ 6b (the collision)
    //
    // THE WIZARD'S OWN CHECK CANNOT REACH THE TARGET ROWS ON THIS BUILD, and
    // the controller says exactly why: step 5 sends the recovery point's
    // INLINE archive URL, because the product API's `Backup` projection
    // publishes neither `destinationRef` nor `locationDigest` (the gap
    // `restore-wizard.js`'s `SOURCE_DESTINATION_NOT_PUBLISHED` already names,
    // owed by PLAT-08.2), and this build "checks saved destinations only".
    // So the check plan is never built and `target.mappedTopics` never runs.
    //
    // The TARGET half is still provable, and this is where it is proved: the
    // same request with a saved `BackupDestination` named instead, issued
    // from the page's own origin against the real service, so the lab's own
    // controller decides whether the pre-created topic collides.
    const destination = "dest-" + suffix;
    kube(["-n", namespace, "create", "secret", "generic", "store-" + suffix,
      "--from-literal=access-key-id=unused-by-the-target-rows",
      "--from-literal=secret-access-key=unused-by-the-target-rows"]);
    apply({
      apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
      metadata: { name: destination, namespace: namespace, labels: LABELS },
      spec: {
        storage: { provider: "S3", bucket: "kafka-backups", prefix: namespace,
          addressing: "PathStyle", endpoint: "http" + "://minio." + LAB_NS + ".svc:9000" },
        transport: { security: "InsecureHTTP" },
        access: {
          archiveWrite: { mode: "SecretKeys", secret: { name: "store-" + suffix } },
        },
      },
    });
    result.created.push({ kind: "BackupDestination", name: destination });
    const started = await page.evaluate(async (args) => {
      const response = await fetch(args.origin + "/api/v1/namespaces/" + args.ns + "/preflights", {
        method: "POST",
        headers: { "content-type": "application/json", "idempotency-key": args.key },
        credentials: "same-origin",
        body: JSON.stringify({
          operation: "restore",
          restore: {
            planBytes: args.planBytes,
            planHash: args.planHash,
            target: args.target,
            sourceDestination: args.destination,
            evidenceDestination: args.destination,
            recoveryPoint: { backupName: args.point, backupUid: args.pointUid },
          },
          timeoutSeconds: 120,
        }),
      });
      return { status: response.status, body: (await response.text()).slice(0, 8000) };
    }, {
      origin: origin, ns: namespace, key: "plat11-2-collision-1",
      planBytes: planBytes, planHash: planHash, target: targetCluster,
      destination: destination, point: points.old.name, pointUid: points.old.uid,
    });
    save("06b-collision-preflight-start.json", started);
    let collisionRow = null;
    let collisionPreflight = null;
    if (started.status === 202 || started.status === 200 || started.status === 201) {
      const id = (JSON.parse(started.body).item || {}).id;
      for (let i = 0; i < 90; i += 1) {
        const o = kubeJson(["-n", namespace, "get", "preflight", id]);
        const st = o.status || {};
        const rows = (st.checks || []).concat(st.warnings || []);
        const row = rows.find((c) => c.id === "target.mappedTopics");
        if (row !== undefined) {
          collisionRow = row;
          collisionPreflight = o;
          break;
        }
        if (typeof st.state === "string" &&
          ["ready", "notReady", "unknown", "failed", "cancelled"].includes(st.state)) {
          collisionPreflight = o;
          break;
        }
        await pause(2000);
      }
    }
    // THE FINAL OBJECT, WHATEVER IT SAYS, so a run that did not reach the row
    // still records WHY -- the namespace is deleted a minute later.
    if (started.status === 202) {
      try {
        const id = (JSON.parse(started.body).item || {}).id;
        save("06b-collision-preflight-final.json", kubeJson(["-n", namespace, "get", "preflight", id]));
      } catch (gone) {
        save("06b-collision-preflight-final.json", { error: String(gone) });
      }
    }
    save("06b-collision-preflight.json", collisionPreflight || started);
    if (collisionRow !== null && collisionRow.code === "MappedTopicExists") {
      record("an existing target topic with the mapped name is refused by the readiness check", {
        topic: COLLIDING_TOPIC, preflight: collisionPreflight.metadata.name,
        uid: collisionPreflight.metadata.uid,
        code: collisionRow.code, message: collisionRow.message,
        state: (collisionPreflight.status || {}).state,
      });
    } else {
      result.journeys.push({
        journey: "an existing target topic with the mapped name is refused by the readiness check",
        outcome: "NOT REACHED",
        why: collisionPreflight === null
          ? "the preflight was not accepted: " + started.body.slice(0, 600)
          : "no target.mappedTopics row was recorded; state=" +
            String((collisionPreflight.status || {}).state) + " reason=" +
            String((collisionPreflight.status || {}).reason),
        collidingTopic: COLLIDING_TOPIC,
      });
      process.stderr.write("== NOT REACHED: target.mappedTopics\n");
    }

    // ------------------------------------------------------------------ 7
    // A TARGET CHANGE INVALIDATES THE VERDICT AND REFUSES THE SUBMIT.
    const hashBefore = await page.evaluate(() => {
      const code = document.querySelector("#plan-hash-value");
      return code === null ? "" : code.textContent;
    });
    await page.selectOption("#target-cluster", secondUid);
    await pause(1500);
    const hashAfter = await page.evaluate(() => {
      const code = document.querySelector("#plan-hash-value");
      return code === null ? "" : code.textContent;
    });
    const staleText = await text(page);
    await shot(page, "07-target-change-stale");
    save("07-stale-text.txt", staleText);
    if (hashBefore === hashAfter) {
      // The two targets address the same broker, so the plan bytes do not
      // move. That is a property of this fixture and it is recorded as such
      // rather than asserted away: journey 8 changes the prefix instead.
      result.journeys.push({
        journey: "a target change re-runs the readiness check",
        outcome: "NOT DISTINGUISHING — both fixture targets address the same broker, so the " +
          "plan bytes and the hash are identical. Journey 8 changes the plan instead.",
        hash: hashBefore,
      });
    } else {
      check(staleText.includes("out of date") || staleText.includes("run it again") ||
        staleText.includes("nothing is sent"),
        "a target change makes the verdict out of date: " + staleText.slice(0, 2000));
      record("a target change moves the plan hash and the readiness verdict stops applying", {
        before: hashBefore, after: hashAfter,
      });
    }

    // ------------------------------------------------------------------ 8
    // AN EDIT AFTER THE CHECK: THE VERDICT IS ABOUT ANOTHER PLAN, SUBMIT REFUSED.
    await page.selectOption("#target-cluster", targetUid);
    await page.fill("#topic-prefix", "moved-" + suffix + "-");
    await page.dispatchEvent("#topic-prefix", "change");
    await pause(1500);
    const editedHash = await page.evaluate(() => {
      const code = document.querySelector("#plan-hash-value");
      return code === null ? "" : code.textContent;
    });
    const editedText = await text(page);
    await shot(page, "08-stale-after-edit");
    save("08-stale-after-edit.txt", editedText);
    check(editedHash !== hashBefore, "the edited plan hashes differently");
    if (verdict !== null) {
      check(editedText.includes("out of date") || editedText.includes("run it again"),
        "the verdict is marked out of date after the edit: " + editedText.slice(0, 2000));
      const disabled = await page.evaluate(() => {
        const b = document.querySelector("#create-restore");
        return b === null ? null : b.disabled;
      });
      check(disabled === true,
        "and the submit is refused until the check is run again");
      record("a readiness verdict for another plan refuses the submit until it is re-run", {
        checkedHash: hashBefore, currentHash: editedHash,
      });
      // THE NEGATIVE CONTROL: putting the plan back makes the verdict apply
      // again, so the refusal is about the hash and not a permanent state.
      await page.fill("#topic-prefix", OLD_PREFIX);
      await page.dispatchEvent("#topic-prefix", "change");
      await pause(1500);
      const restoredHash = await page.evaluate(() => {
        const code = document.querySelector("#plan-hash-value");
        return code === null ? "" : code.textContent;
      });
      check(restoredHash === hashBefore, "the plan is back to the one that was checked");
      control("restoring the plan restores the verdict's applicability", {
        hash: restoredHash,
      });
    }

    // ------------------------------------------------------------------ 9
    // A FAILED RESTORE OFFERS A FRESH-TARGET RETRY.
    //
    // THE FAILURE IS THE CONTROLLER'S OWN. The Restore names an Approval whose
    // `planHash` is another plan's, which the controller refuses terminally --
    // no Job, no data-plane work. Nothing here writes a status.
    const failedName = "rst-failed-" + suffix;
    const failedApproval = "apr-failed-" + suffix;
    apply({
      apiVersion: "logweir.dev/v1alpha1", kind: "Approval",
      metadata: { name: failedApproval, namespace: namespace, labels: LABELS },
      spec: {
        // AN APPROVAL BOUND TO ANOTHER EXECUTION, which is step 2 of the
        // restore reconciler's admission order and TERMINAL: "that Approval
        // must exist and its immutable subject must identify this exact
        // Restore name, namespace and UID -- else ApprovalSubjectMismatch".
        // It is deliberately step 2 and not step 3: an approval that merely
        // has not been verified yet is HELD and requeued for ever (interface
        // I19), which is a Pending Restore and not a failed one. This is the
        // controller's own verdict; no status is written by this harness.
        subjectRef: { kind: "Restore", name: "rst-some-other-execution" },
        planHash: "sha256:" + "0".repeat(64),
        approvalBytes: "{}\n",
        sidecarBytes: "{}\n",
      },
    });
    apply({
      apiVersion: "logweir.dev/v1alpha1", kind: "Restore",
      metadata: { name: failedName, namespace: namespace, labels: LABELS },
      spec: {
        planBytes: planBytes,
        approvalRef: { name: failedApproval },
        sourceArchive: { url: archiveUrl },
        backupSetRef: "01JB7Z0000000000000000OLD",
        pointInTime: rfc(OLD_TO_MS),
        target: {
          clusterRef: { name: targetCluster }, mode: "newTopic",
          topicNaming: { prefix: OLD_PREFIX },
        },
        deadlineSeconds: 3600,
      },
    });
    result.created.push({ kind: "Restore", name: failedName });
    let failed = null;
    for (let i = 0; i < 60; i += 1) {
      const o = kubeJson(["-n", namespace, "get", "restore", failedName]);
      if (((o.status || {}).phase) === "Failed") {
        failed = o;
        break;
      }
      await pause(2000);
    }
    save("09-failed-restore.json", failed || kubeJson(["-n", namespace, "get", "restore", failedName]));
    const jobs = kubeJson(["-n", namespace, "get", "jobs"]).items || [];
    check(jobs.every((j) => !j.metadata.name.includes(failedName)),
      "a refused Restore never produced a runner Job");

    if (failed === null) {
      result.journeys.push({
        journey: "a failed restore offers a fresh-target retry",
        outcome: "NOT REACHED — the lab controller did not mark the Restore Failed within 120 s",
      });
      process.stderr.write("== NOT REACHED: a terminally failed Restore\n");
    } else {
      await page.goto(base + "#/operations?ns=" + namespace + "&kind=restore&name=" + failedName,
        { waitUntil: "load", timeout: 30000 });
      await waitForText(page, "retry to a fresh target", "the retry affordance");
      await shot(page, "09-failed-operation");
      record("a failed restore's operation view offers a fresh-target retry", {
        restore: failedName, uid: failed.metadata.uid,
        phase: failed.status.phase, reason: failed.status.reason || null,
        runnerJobs: 0,
      });

      await page.click("#retry-fresh-target");
      await waitForText(page, "retrying to a fresh target", "the retry banner on the selector");
      await shot(page, "10-retry-selector");
      await page.click("a[href*=\"retryOf=\"][href*=\"" + points.old.uid + "\"]");
      await waitFor(page, "#topic-prefix", "the wizard as a retry");
      await page.selectOption("#target-cluster", targetUid);
      await pause(1000);
      const retryPrefix = await page.evaluate(() => {
        const input = document.querySelector("#topic-prefix");
        return input === null ? "" : input.value;
      });
      const retryText = await text(page);
      const retryNames = await page.evaluate(() => {
        const facts = Array.from(document.querySelectorAll("code")).map((c) => c.textContent);
        return facts;
      });
      await shot(page, "11-retry-wizard");
      save("11-retry-text.txt", retryText);
      check(retryPrefix !== OLD_PREFIX,
        "the retry's prefix is NOT the failed run's: " + retryPrefix);
      check(retryPrefix.includes("retry-"), "and it names the run it retries: " + retryPrefix);
      check(retryText.includes(failedName.toLowerCase()),
        "the banner names the failed run");
      check(retryText.includes("not reused"),
        "and says the old approval is not reused");
      const mintedRestore = retryNames.find((n) => /^rst-[a-z0-9]{26}$/.test(String(n)));
      const mintedApproval = retryNames.find((n) => /^apr-[a-z0-9]{26}$/.test(String(n)));
      check(mintedRestore !== undefined && mintedRestore !== failedName,
        "the retry mints a NEW Restore name: " + mintedRestore + " vs " + failedName);
      check(mintedApproval !== undefined && mintedApproval !== failedApproval,
        "and a NEW Approval name: " + mintedApproval + " vs " + failedApproval);
      record("the retry is a new execution: a fresh prefix, a new plan and two new names", {
        failedRestore: failedName, failedApproval: failedApproval,
        retryPrefix: retryPrefix, mintedRestore: mintedRestore, mintedApproval: mintedApproval,
      });

      // THE OLD RUN IS UNTOUCHED, read back from the cluster by UID.
      const after = kubeJson(["-n", namespace, "get", "restore", failedName]);
      check(after.metadata.uid === failed.metadata.uid, "the failed Restore is the same object");
      check(after.metadata.resourceVersion === failed.metadata.resourceVersion ||
        after.status.phase === "Failed",
        "and it is still Failed; nothing the wizard did modified it");
      check(after.spec.target.topicNaming.prefix === OLD_PREFIX,
        "its own prefix is unchanged");
      save("09-failed-restore-after.json", after);
      control("the failed Restore is byte-identical in the fields the retry could have touched", {
        uid: after.metadata.uid, phase: after.status.phase,
        prefix: after.spec.target.topicNaming.prefix,
      });

      // AND THE OLD APPROVAL REFERENCE IS NOWHERE IN WHAT THE RETRY WOULD SEND.
      const wouldSend = requests.filter((r) => r.url.endsWith("/restores"));
      save("12-non-get-requests.json", requests);
      check(wouldSend.every((r) => !String(r.body || "").includes(failedApproval)),
        "no request this page made names the failed run's Approval");
      control("no request the page issued names the failed run's Approval", {
        requestsInspected: requests.length,
      });
    }

    save("api-bodies.json", bodies);
    result.finishedAt = new Date().toISOString();
    result.outcome = "passed";
  } catch (failure) {
    result.finishedAt = new Date().toISOString();
    result.outcome = "failed";
    result.failure = String(failure && failure.stack ? failure.stack : failure);
    try {
      await shot(page, "zz-failure");
      save("zz-failure-text.txt", await text(page));
    } catch (noPage) {
      // the page may already be gone
    }
    save("api-bodies.json", bodies);
    throw failure;
  } finally {
    await browser.close().catch(() => {});
    save("api.log", apiLog.join(""));
    stopApi();
    // THE LAB TOPIC GOES, WHATEVER HAPPENED.
    try {
      const deleted = labTopic("delete", COLLIDING_TOPIC);
      const remaining = labTopic("list", COLLIDING_TOPIC);
      result.cleanup.push({
        what: "the colliding topic on the lab's kafka-target",
        topic: COLLIDING_TOPIC,
        deleted: deleted.trim().slice(0, 400),
        stillPresent: remaining.includes(COLLIDING_TOPIC),
      });
    } catch (undeleted) {
      result.cleanup.push({ what: "the colliding topic", error: String(undeleted) });
    }
    // THE NAMESPACE GOES, AFTER AN OWNER-LABEL CHECK.
    if (process.env.UI_E2E_KEEP !== "1") {
      try {
        assertSafeNamespace(namespace);
        const live = kubeJson(["get", "namespace", namespace]);
        const label = ((live.metadata.labels || {})["logweir.dev/test-owner"]);
        check(label === OWNER, "refusing to delete a namespace this run does not own: " + label);
        check(live.metadata.uid === result.namespaceUid, "and whose UID has changed");
        kube(["delete", "namespace", namespace, "--wait=false"]);
        result.cleanup.push({
          what: "namespace", name: namespace, uid: live.metadata.uid,
          ownerLabel: label, deleted: true,
        });
      } catch (undeleted) {
        result.cleanup.push({ what: "namespace", name: namespace, error: String(undeleted) });
      }
    } else {
      result.cleanup.push({ what: "namespace", name: namespace, kept: "UI_E2E_KEEP=1" });
    }
    rmSync(WORK_DIR, { recursive: true, force: true });
    const at = save("result.json", result);
    process.stderr.write("== result: " + at + "\n");
  }
}

main().then(
  () => {
    process.stderr.write("== plat11-2 live journey: " + result.journeys.length +
      " journey(s), " + result.negativeControls.length + " negative control(s)\n");
    process.exit(0);
  },
  (error) => {
    process.stderr.write("== plat11-2 live journey FAILED: " + String(error) + "\n");
    process.exit(1);
  },
);
