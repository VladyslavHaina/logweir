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
// THE LAB'S OWN ADDRESSES AND ITS OWN AUTH. Both brokers expose ONE port,
// 9096, and it is a SASL/SCRAM listener: a plaintext 9092 exists inside the
// pod but no Service publishes it, so a `KafkaCluster` naming 9092 answers
// `BrokerUnreachable` and every target row after it reads
// `BlockedByPrerequisite`. A previous run of this harness recorded exactly
// that. The SCRAM password is the lab's own Secret, COPIED into this run's
// namespace without ever being printed, read or logged -- the lab's Kafka is
// usable from an owned namespace, and its credential is how it is used.
const LAB_TARGET_BOOTSTRAP = "kafka-target." + LAB_NS + ".svc.cluster.local:9096";
const LAB_SOURCE_BOOTSTRAP = "kafka-source." + LAB_NS + ".svc.cluster.local:9096";
const LAB_SCRAM_USER = "scram-user";
const LAB_TARGET_SECRET = "target-scram";
const LAB_SOURCE_SECRET = "source-scram";

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
const destinationName = "dest-" + suffix;
const archiveUrl = "s3://logweir-fixture/" + namespace;
const points = {};

function succeededStatus(set, records, fromMs, toMs, completedAt, destination) {
  return {
    phase: "Succeeded",
    backupId: set,
    records: records,
    exitCode: 0,
    exitReason: "ok",
    reason: "Ok",
    manifestKey: namespace + "/" + set + "/manifest.json",
    destination: destination,
    windowCovered: { fromMs: fromMs, toMs: toMs },
    conditions: [{
      type: "Complete", status: "True", reason: "Ok", message: "fixture",
      lastTransitionTime: completedAt,
    }],
  };
}

function seedBackup(name, status, destination) {
  const created = apply({
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Backup",
    metadata: { name: name, namespace: namespace, labels: LABELS },
    spec: {
      archive: { url: "logweir-destination://" + destination.name },
      destinationRef: { name: destination.name },
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

/** A saved destination whose resolved identity can be frozen onto the two
 * recovery-point fixtures. The controller writes the digest; this harness
 * copies that fact and never computes one. */
async function seedDestination() {
  kube(["-n", namespace, "create", "secret", "generic", "store-" + suffix,
    "--from-literal=access-key-id=unused-by-the-target-rows",
    "--from-literal=secret-access-key=unused-by-the-target-rows"]);
  result.created.push({ kind: "Secret", name: "store-" + suffix });
  const made = apply({
    apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
    metadata: { name: destinationName, namespace: namespace, labels: LABELS },
    spec: {
      storage: { provider: "S3", bucket: "kafka-backups", prefix: namespace,
        addressing: "PathStyle", endpoint: "http" + "://minio." + LAB_NS + ".svc:9000" },
      transport: { security: "InsecureHTTP" },
      access: {
        archiveWrite: { mode: "SecretKeys", secret: { name: "store-" + suffix } },
      },
    },
  });
  result.created.push({ kind: "BackupDestination", name: destinationName, uid: made.metadata.uid });
  for (let i = 0; i < 60; i += 1) {
    const seen = kubeJson(["-n", namespace, "get", "backupdestination", destinationName]);
    const status = seen.status || {};
    if (typeof status.locationDigest === "string" && status.locationDigest.startsWith("sha256:")) {
      return {
        name: destinationName,
        uid: seen.metadata.uid,
        generation: seen.metadata.generation,
        locationDigest: status.locationDigest,
      };
    }
    await pause(1000);
  }
  throw new Error("BackupDestination " + destinationName + " never published locationDigest");
}

function seedCluster(name, role, bootstrap, secret) {
  const made = apply({
    apiVersion: "logweir.dev/v1alpha1",
    kind: "KafkaCluster",
    metadata: { name: name, namespace: namespace, labels: LABELS },
    spec: {
      bootstrapServers: [bootstrap],
      role: role,
      auth: { mode: "scramSha512", tls: false, username: LAB_SCRAM_USER,
        secretRef: { name: secret } },
    },
  });
  result.created.push({ kind: "KafkaCluster", name: name, uid: made.metadata.uid });
  return made;
}

/** Copies one of the lab's credential Secrets into this run's namespace.
 *
 *  THE VALUE NEVER CROSSES THIS PROCESS AS TEXT IT PRINTS. It is read as
 *  JSON, stripped of every field that names the source object, and handed
 *  straight back to `kubectl create`; nothing is echoed, logged or written to
 *  an artifact, and the copy dies with the namespace. */
function copyLabSecret(name) {
  const source = kubeJson(["-n", LAB_NS, "get", "secret", name]);
  const copy = {
    apiVersion: "v1", kind: "Secret", type: source.type || "Opaque",
    metadata: { name: name, namespace: namespace, labels: LABELS },
    data: source.data,
  };
  kube(["-n", namespace, "create", "-f", "-"], { input: JSON.stringify(copy) });
  result.created.push({ kind: "Secret", name: name, note: "copied from the lab, value never printed" });
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

  // THE SERVICE ACCOUNT EVERY RUNNER JOB THIS NAMESPACE PRODUCES RUNS AS.
  // `weirkeeper` builds every Job -- a connection probe and a readiness check
  // alike -- with `serviceAccountName: logweir-runner`, and the chart creates
  // it per installation namespace. A namespace this harness made itself has
  // none, and the Job controller then answers `FailedCreate ... serviceaccount
  // "logweir-runner" not found`, no pod is created, and the check reports
  // `NotReady` about the POD rather than about the target. (The first run of
  // this harness recorded exactly that, which is why the line is here.) It
  // holds no RBAC: a check pod dials a broker and an object store and reads no
  // API object.
  const account = apply({
    apiVersion: "v1", kind: "ServiceAccount",
    metadata: { name: "logweir-runner", namespace: namespace, labels: LABELS },
  });
  result.created.push({ kind: "ServiceAccount", name: "logweir-runner", uid: account.metadata.uid });

  // THE SIGNING KEY THE CHECK POD MOUNTS, generated for this run and this run
  // only. `weirkeeper` gives every runner Job a `signing` volume from the
  // Secret `logweir-signing-key`, and a namespace without it never starts the
  // pod ("MountVolume.SetUp failed ... secret not found") -- so the check
  // reports NotReady about the VOLUME instead of about the target, which a
  // previous run of this harness recorded. The key is ed25519, generated here,
  // never printed, and deleted with the namespace. It signs nothing this run
  // asserts on: the clause under test is `target.mappedTopics`, which is a
  // question about the broker.
  const keyPath = join(WORK_DIR, "signing.pem");
  mkdirSync(WORK_DIR, { recursive: true, mode: 0o700 });
  const minted = spawnSync("openssl", ["genpkey", "-algorithm", "ed25519", "-out", keyPath],
    { encoding: "utf8", timeout: 30000 });
  check(minted.status === 0, "openssl could not mint an ed25519 key: " + String(minted.stderr));
  kube(["-n", namespace, "create", "secret", "generic", "logweir-signing-key",
    "--from-file=signing.pem=" + keyPath]);
  result.created.push({ kind: "Secret", name: "logweir-signing-key", note: "ed25519, per-run, never printed" });

  copyLabSecret(LAB_TARGET_SECRET);
  const sourceSecret = kube(["-n", LAB_NS, "get", "secret", LAB_SOURCE_SECRET],
    { expected: [0, 1] }).status === 0 ? LAB_SOURCE_SECRET : LAB_TARGET_SECRET;
  if (sourceSecret === LAB_SOURCE_SECRET) {
    copyLabSecret(LAB_SOURCE_SECRET);
  }
  seedCluster(sourceCluster, "source", LAB_SOURCE_BOOTSTRAP, sourceSecret);
  seedCluster(targetCluster, "target", LAB_TARGET_BOOTSTRAP, LAB_TARGET_SECRET);
  seedCluster(secondTarget, "target", LAB_TARGET_BOOTSTRAP, LAB_TARGET_SECRET);

  const frozenDestination = await seedDestination();

  // TWO DESTINATION-BACKED POINTS, AND THE OLDER ONE IS THE ONE THIS JOURNEY
  // restores. The status block is the destination identity the controller
  // resolved above, copied rather than recomputed.
  points.old = seedBackup(
    NAMESPACE_PREFIX + LONG + "old-" + suffix,
    succeededStatus("01JB7Z0000000000000000OLD", 3000, OLD_FROM_MS, OLD_TO_MS,
      rfc(OLD_TO_MS), frozenDestination),
    frozenDestination,
  );
  points.new = seedBackup(
    NAMESPACE_PREFIX + LONG + "new-" + suffix,
    succeededStatus("01JB7Z0000000000000000NEW", 4000, NEW_FROM_MS, NEW_TO_MS,
      rfc(NEW_TO_MS), frozenDestination),
    frozenDestination,
  );
  result.fixtures.push({
    what: "two Succeeded Backup objects created with kubectl, status written by this harness",
    older: points.old, newer: points.new, topics: TOPICS, frozenDestination: frozenDestination,
    note: "no archive exists for either: they are fixtures for what the PAGE does with a " +
      "recovery point, and nothing here claims a run produced them",
  });

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
  // THE WIZARD'S OWN WRITES, and not this harness's probes. Both go through
  // the page (the probes are `fetch` inside `page.evaluate`, which is the
  // point: same origin, same session), so they are told apart by WHEN they
  // happened -- everything after a marker taken immediately before a click.
  const writesSince = (marker) => requests.slice(marker);
  const restoreCreatesSince = (marker) =>
    writesSince(marker).filter((r) => r.method === "POST" && r.url.endsWith("/restores"));
  /** The Restore this namespace holds for THESE plan bytes AND THIS approval
   *  reference.
   *
   *  BOTH HALVES, BECAUSE THE BYTES ARE NOT AN IDENTITY. More than one object
   *  in this namespace can carry the same plan document -- journey 4's accepted
   *  single-row probe and journey 5b's wizard create both do -- and a lookup on
   *  the bytes alone returned whichever `kubectl get restores` happened to list
   *  first. In the branch's own recorded run that was the PROBE, so four
   *  assertions about "the object the wizard created" were being made against
   *  an object this harness had created from the same bytes, and could not have
   *  failed on the wizard's account. The second review found it.
   *
   *  The approval reference IS an identity here: it is minted from the plan
   *  bytes by the page and named by the request, so `approval-<hash>` belongs
   *  to the wizard's create and `apr-*-probe` to a harness one. */
  const restoreFor = (bytes, approvalRef) => {
    const all = kubeJson(["-n", namespace, "get", "restores"]).items || [];
    const matching = all.filter((o) => ((o.spec || {}).planBytes) === bytes &&
      (((o.spec || {}).approvalRef || {}).name) === approvalRef);
    check(matching.length <= 1,
      "two Restores share these bytes AND this approval reference, so neither is an " +
      "identity: " + JSON.stringify(matching.map((o) => o.metadata.name)));
    return matching[0] || null;
  };

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
    // THE WORD OCCURS ONLY INSIDE THE NEGATION, which is the property D3
    // section 3.5 asks for. The earlier form scanned for a phrase the page
    // never contained -- a strawman that no value could fail.
    const sentence = "that is a sampled check, not an exhaustive comparison";
    check(shown.includes(sentence), "the sampled clause is present verbatim");
    check(
      shown.split("exhaustive").length - 1 === shown.split(sentence).length - 1,
      "and `exhaustive` occurs nowhere else on the page: " +
        String(shown.split("exhaustive").length - 1) + " occurrence(s) vs " +
        String(shown.split(sentence).length - 1) + " of the clause",
    );
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
    // BOTH ROWS, COUNTED. A duplicate under a prefix map is the SAME source
    // twice, so `includes("orders")` holds for a message naming one side only
    // -- the Rust row counts occurrences for that reason and so does this.
    check(dupError.message.split("`orders`").length - 1 === 2,
      "the refusal names BOTH rows, not one: " + dupError.message);
    check(dupError.message.includes(OLD_PREFIX + "orders"),
      "and the target they share: " + dupError.message);
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

    // --------------------------------------------------- 5b (the wizard SUBMITS)
    //
    // THE SEAM BETWEEN THE PREVIEW AND THE WIRE, WHICH NOTHING LIVE HAD
    // TOUCHED (the independent review's F2): every earlier `POST .../restores`
    // in this file is a harness-authored probe, so `ui/client.js`'s delivery of
    // the declaration -- the only code that puts it on the request -- had never
    // run against a real service, and no Restore had ever been created by the
    // wizard's own Create button. This journey clicks it and then reads the
    // created object back with `kubectl` to compare its spec with what was on
    // screen.
    //
    // The submit is allowed because no readiness check has run for this plan,
    // which is a WARNING and not a refusal (D2 section 6.6 is about a verdict
    // that has stopped applying, not about the absence of one).
    await openOldPoint();
    await page.uncheck(".topic-box[data-topic=\"shipments\"]");
    await waitForText(page, OLD_PREFIX + "payments", "the mapping before the submit");
    const previewed = await page.evaluate(() => ({
      bytes: (document.querySelector("#plan-bytes") || {}).textContent,
      hash: (document.querySelector("#plan-hash-value") || {}).textContent,
      rows: Array.from(document.querySelectorAll("#step-target tbody tr"))
        .map((tr) => Array.from(tr.querySelectorAll("td")).map((td) => td.innerText.trim()))
        .filter((cells) => cells.length === 2),
    }));
    save("5b-previewed.json", previewed);
    check(previewed.hash.startsWith("sha256:"), "the previewed hash is on screen");
    check(previewed.rows.length === 2,
      "two mapped rows are previewed: " + JSON.stringify(previewed.rows));

    const marker = writesSoFar();
    await page.click("#create-restore");

    // THE REQUEST FIRST, BECAUSE IT CARRIES THE IDENTITY THE READBACK NEEDS.
    let wizardCreates = [];
    for (let i = 0; i < 40; i += 1) {
      wizardCreates = restoreCreatesSince(marker);
      if (wizardCreates.length > 0) {
        break;
      }
      await pause(500);
    }
    check(wizardCreates.length === 1,
      "exactly one create, by the page: " + JSON.stringify(wizardCreates.map((r) => r.url)) +
      "; page said:\n" + (await text(page)).slice(0, 1200));
    const wizardBody = JSON.parse(wizardCreates[0].body);
    save("5b-wizard-request.json", wizardBody);

    let created = null;
    for (let i = 0; i < 40; i += 1) {
      created = restoreFor(previewed.bytes, wizardBody.approvalRef.name);
      if (created !== null) {
        break;
      }
      await pause(500);
    }
    await shot(page, "5b-submitted");
    check(created !== null,
      "the wizard's Create button created a Restore with the previewed bytes under its own " +
      "approval reference " + wizardBody.approvalRef.name + ". Page said:\n" +
      (await text(page)).slice(0, 1500));
    save("5b-created-restore.json", created);

    // THE CONTROL FOR THE READBACK ITSELF. On the branch's own earlier run this
    // assertion fails: the object found by plan bytes alone was the harness's
    // probe, whose approvalRef is `apr-mapping-probe`. It is what makes the
    // four stored-object assertions below about the WIZARD's object.
    check(created.spec.approvalRef.name === wizardBody.approvalRef.name,
      "the object read back is the one the wizard's request named: " +
      created.spec.approvalRef.name + " vs " + wizardBody.approvalRef.name);
    check(created.spec.approvalRef.name.startsWith("approval-"),
      "a name minted from the plan bytes by the page, not a harness probe's: " +
      created.spec.approvalRef.name);
    const probes = (kubeJson(["-n", namespace, "get", "restores"]).items || [])
      .filter((o) => ((o.spec || {}).planBytes) === previewed.bytes);
    check(probes.length > 1,
      "and more than one object carries these bytes, which is why the lookup needs an " +
      "identity: " + JSON.stringify(probes.map((o) => o.metadata.name)));

    // THE REQUEST IS THE PREVIEW, ROW FOR ROW AND BYTE FOR BYTE.
    check(wizardBody.planBytes === previewed.bytes,
      "the submitted plan bytes are the bytes that were on screen");
    check(wizardBody.planHash === previewed.hash,
      "under the hash that was shown beside them");
    check((wizardBody.sourceDestinationRef || {}).name === frozenDestination.name,
      "the wizard create request carries the recovery point's saved source destination");
    check((wizardBody.evidenceDestinationRef || {}).name === frozenDestination.name,
      "the wizard create request carries the recovery point's saved evidence destination");
    check(wizardBody.sourceArchive.url === "logweir-destination://" + frozenDestination.name &&
      wizardBody.sourceArchive.credentialRef === undefined,
    "the create request uses the destination sentinel and no inline credential");
    check(Array.isArray(wizardBody.topicMapping) && wizardBody.topicMapping.length === 2,
      "the declaration reached the request: " + JSON.stringify(wizardBody.topicMapping));
    for (const [i, row] of wizardBody.topicMapping.entries()) {
      check(row.source === previewed.rows[i][0] && row.target === previewed.rows[i][1],
        "row " + i + " equals the previewed row: " + JSON.stringify(row) + " vs " +
        JSON.stringify(previewed.rows[i]));
      check(row.target === OLD_PREFIX + row.source, "and is `prefix + source`");
    }
    check(wizardBody.topicMapping.some((r) => r.source === "payments"),
      "including the second topic, which no earlier request in this run ever carried");

    // AND THE STORED OBJECT IS THE PREVIEW TOO -- with the declaration absent,
    // because `Restore.spec` has no field for it.
    check(created.spec.planBytes === previewed.bytes,
      "the stored plan bytes are the previewed bytes, byte for byte");
    check(created.spec.target.topicNaming.prefix === OLD_PREFIX,
      "the stored prefix is the previewed prefix");
    check((created.spec.sourceDestinationRef || {}).name === frozenDestination.name &&
      (created.spec.evidenceDestinationRef || {}).name === frozenDestination.name,
    "the stored Restore keeps both saved destination references");
    check(created.spec.sourceArchive.url === "logweir-destination://" + frozenDestination.name &&
      created.spec.sourceArchive.secretRef === undefined,
    "the stored Restore keeps the sentinel and no inline credential");
    check(created.spec.topicMapping === undefined,
      "the declaration is a rail, never a stored field: " + JSON.stringify(created.spec));
    // THE `topics:` BLOCK, not every list entry in the document: the target's
    // `bootstrap_servers` list is spelled identically and would otherwise be
    // read as a topic. The block is the lines after `  topics:` up to the next
    // line that is not a list entry.
    const planLines = created.spec.planBytes.split("\n");
    const topicsAt = planLines.indexOf("  topics:");
    check(topicsAt !== -1, "the stored plan carries a topics block");
    const storedTopics = [];
    for (let i = topicsAt + 1; i < planLines.length; i += 1) {
      if (!planLines[i].startsWith("    - \"")) {
        break;
      }
      storedTopics.push(planLines[i].slice(7, -1));
    }
    check(JSON.stringify(storedTopics) === JSON.stringify(["orders", "payments"]),
      "and the stored plan carries exactly the previewed subset: " + JSON.stringify(storedTopics));
    record("the wizard submits, and the created Restore is the preview byte for byte", {
      restore: created.metadata.name, uid: created.metadata.uid,
      planHash: previewed.hash,
      declared: wizardBody.topicMapping,
      storedTopics: storedTopics,
      storedPrefix: created.spec.target.topicNaming.prefix,
      sourceDestinationRef: created.spec.sourceDestinationRef,
      evidenceDestinationRef: created.spec.evidenceDestinationRef,
    });

    // THE NEGATIVE CONTROL: the topic that was unticked is in NEITHER the
    // declaration nor the stored plan, so the subset is a real narrowing.
    check(!wizardBody.topicMapping.some((r) => r.source === "shipments"), "declared");
    check(created.spec.planBytes.indexOf("\"shipments\"") === -1,
      "and the stored plan does not name it");
    control("the unticked topic reaches neither the request nor the stored plan", {
      unticked: "shipments",
    });

    // ------------------------------------------------------------------ 6
    // THE READINESS CHECK WITHOUT A COLLISION. This is deliberately separate
    // from the negative control below: a MappedTopicExists row must never hide
    // a PlanDestinationMismatch in the same aggregate verdict.
    await openOldPoint();
    await waitFor(page, "#restore-readiness-start", "the readiness control");
    const beforeClean = new Set(
      (kubeJson(["-n", namespace, "get", "preflights"]).items || [])
        .map((p) => p.metadata.uid),
    );
    await page.click("#restore-readiness-start");
    await pause(4000);
    let cleanVerdict = null;
    for (let i = 0; i < 90; i += 1) {
      const list = kubeJson(["-n", namespace, "get", "preflights"]).items || [];
      const mine = list.find((p) => !beforeClean.has(p.metadata.uid));
      const phase = ((mine || {}).status || {}).phase;
      if (phase === "Completed" || phase === "Failed" || phase === "Cancelled") {
        cleanVerdict = mine;
        break;
      }
      await pause(2000);
    }
    const cleanPreflights = kubeJson(["-n", namespace, "get", "preflights"]);
    save("06a-no-collision-preflights.json", cleanPreflights);
    save("06a-no-collision-conditions.json", (cleanPreflights.items || []).map((p) => ({
      name: p.metadata.name,
      state: (p.status || {}).state || null,
      reason: (p.status || {}).reason || null,
      conditions: (p.status || {}).conditions || [],
      checks: ((((p.status || {}).result || {}).checks) || [])
        .map((c) => ({ id: c.id, state: c.state, code: c.code })),
    })));
    await shot(page, "06a-no-collision-readiness");
    save("06a-no-collision-readiness-text.txt", await text(page));
    check(cleanVerdict !== null,
      "the lab controller did not record a terminal no-collision preflight within 180 s: " +
        JSON.stringify((cleanPreflights.items || []).map((p) => ({
          name: p.metadata.name, phase: (p.status || {}).phase,
          reason: (p.status || {}).reason,
        }))));
    const cleanResult = cleanVerdict.status.result || {};
    const cleanEntries = (cleanResult.checks || []).concat(cleanResult.warnings || []);
    const cleanBinding = cleanEntries.find((c) => c.id === "plan.bindings");
    check(cleanBinding !== undefined && cleanBinding.code === "PlanMatchesReferences",
      "the no-collision control requires the signed plan to match both saved references: " +
        JSON.stringify(cleanBinding || cleanResult));
    const cleanMapped = cleanEntries.find((c) => c.id === "target.mappedTopics");
    check(cleanMapped === undefined || cleanMapped.code !== "MappedTopicExists",
      "the no-collision control unexpectedly found the topic before this harness created it: " +
        JSON.stringify(cleanMapped));
    record("the ordinary wizard readiness path matches its saved destination references", {
      preflight: cleanVerdict.metadata.name, uid: cleanVerdict.metadata.uid,
      state: cleanVerdict.status.reason || cleanVerdict.status.phase,
      planHash: ((cleanVerdict.status.binding || {}).planHash) || null,
      planBindingsRow: cleanBinding,
    });
    control("no collision is present while plan.bindings is PlanMatchesReferences", {
      planBindingsCode: cleanBinding.code,
      mappedTopicsCode: cleanMapped === undefined ? null : cleanMapped.code,
    });

    // NOW create the collision on the lab's broker, and run a distinct check.
    const createdTopic = labTopic("create", COLLIDING_TOPIC);
    save("lab-topic-create.txt", createdTopic);
    const listed = labTopic("list", COLLIDING_TOPIC);
    check(listed.includes(COLLIDING_TOPIC),
      "the colliding topic was not created on kafka-target: " + listed);
    save("lab-topic-list.txt", listed);
    result.created.push({ kind: "KafkaTopic", name: COLLIDING_TOPIC, on: LAB_TARGET_BOOTSTRAP });

    const beforeCollision = new Set(
      (kubeJson(["-n", namespace, "get", "preflights"]).items || [])
        .map((p) => p.metadata.uid),
    );
    await waitFor(page, "#restore-readiness-start", "the second readiness control");
    await page.click("#restore-readiness-start");
    await pause(4000);
    let verdict = null;
    for (let i = 0; i < 90; i += 1) {
      const list = kubeJson(["-n", namespace, "get", "preflights"]).items || [];
      const mine = list.find((p) => !beforeCollision.has(p.metadata.uid));
      const phase = ((mine || {}).status || {}).phase;
      if (phase === "Completed" || phase === "Failed" || phase === "Cancelled") {
        verdict = mine;
        break;
      }
      await pause(2000);
    }
    const preflights = kubeJson(["-n", namespace, "get", "preflights"]);
    save("06b-collision-preflights.json", preflights);
    save("06b-collision-conditions.json", (preflights.items || []).map((p) => ({
      name: p.metadata.name,
      state: (p.status || {}).state || null,
      reason: (p.status || {}).reason || null,
      conditions: (p.status || {}).conditions || [],
      checks: ((((p.status || {}).result || {}).checks) || [])
        .map((c) => ({ id: c.id, state: c.state, code: c.code })),
    })));
    await shot(page, "06b-collision-readiness");
    save("06b-collision-readiness-text.txt", await text(page));
    check(verdict !== null, "the collision preflight did not become terminal within 180 s");
    const state = verdict.status.reason || verdict.status.phase;
    const vres = verdict.status.result || {};
    const entries = (vres.checks || []).concat(vres.warnings || []);
    const bindingRow = entries.find((c) => c.id === "plan.bindings");
    check(bindingRow !== undefined && bindingRow.code === "PlanMatchesReferences",
      "the collision control must still match the saved references: " + JSON.stringify(bindingRow));
    const mappedRow = entries.find((c) => c.id === "target.mappedTopics");
    check(mappedRow !== undefined,
      "the wizard's own readiness request never reached target.mappedTopics: " +
        JSON.stringify({ state: state, result: vres }));
    check(mappedRow.code === "MappedTopicExists",
      "the pre-created collision did not produce MappedTopicExists: " + JSON.stringify(mappedRow));
    record("the wizard's own readiness check reached target.mappedTopics on the lab", {
      preflight: verdict.metadata.name, uid: verdict.metadata.uid, state: state,
      planHash: ((verdict.status.binding || {}).planHash) || null,
      planBindingsRow: bindingRow,
      mappedTopicsRow: mappedRow,
      collidingTopic: COLLIDING_TOPIC,
    });

    const said = await text(page);
    check(said.includes("nothing is sent"),
      "the wizard refuses the submit over a collision: " + said.slice(0, 2000));
    const disabled = await page.evaluate(() => {
      const b = document.querySelector("#create-restore");
      return b === null ? null : b.disabled;
    });
    check(disabled === true, "the create button is disabled after the collision verdict");
    record("an existing target topic with the mapped name refuses the ordinary path", {
      topic: COLLIDING_TOPIC, code: mappedRow.code, message: mappedRow.message,
    });

    // ------------------------------------------------------------------ 7
    // AN EDIT AFTER THE CHECK: THE VERDICT IS ABOUT ANOTHER PLAN, SUBMIT REFUSED.
    //
    // THIS COMES BEFORE THE TARGET SWAP ON PURPOSE. The swap now DROPS the
    // verdict (journey 8), so an edit performed after one would have nothing
    // left to be stale about.
    const hashBefore = await page.evaluate(() => {
      const code = document.querySelector("#plan-hash-value");
      return code === null ? "" : code.textContent;
    });
    await page.fill("#topic-prefix", "moved-" + suffix + "-");
    await page.dispatchEvent("#topic-prefix", "change");
    await pause(1500);
    const editedHash = await page.evaluate(() => {
      const code = document.querySelector("#plan-hash-value");
      return code === null ? "" : code.textContent;
    });
    const editedText = await text(page);
    await shot(page, "07-stale-after-edit");
    save("07-stale-after-edit.txt", editedText);
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
      // THE CONTROL IS THAT THE *STALE* REFUSAL IS GONE, not that every
      // refusal is: this lab's verdict is `notReady` for its own reasons
      // (journey 6), so the gate still refuses -- with a different sentence.
      // Asserting "no refusal at all" would be asserting something false about
      // a correct page, which is how a control becomes noise.
      const refusalNow = await page.evaluate(() => {
        const p = document.querySelector("#readiness-blocked");
        return p === null ? null : p.textContent;
      });
      check(refusalNow === null || !refusalNow.includes(editedHash),
        "the refusal no longer names the edited plan: " + String(refusalNow).slice(0, 400));
      check(refusalNow === null || !refusalNow.includes("Run it again"),
        "and it is no longer the stale-plan refusal: " + String(refusalNow).slice(0, 400));
      control("restoring the plan removes the stale-plan refusal, and only that one", {
        hash: restoredHash,
        refusalStillShown: refusalNow === null ? null : refusalNow.slice(0, 200),
      });
    }

    // ------------------------------------------------------------------ 8
    // A TARGET CHANGE REFUSES THE SUBMIT EVEN WHEN THE PLAN BYTES DO NOT MOVE.
    //
    // D2 SECTION 6.6's SECOND INVALIDATION CAUSE, AND THE ONE THE HASH CANNOT
    // SEE. Both fixture targets address the same broker with the same auth, so
    // they render IDENTICAL plan bytes -- which is exactly the configuration
    // the earlier version of this journey recorded as "NOT DISTINGUISHING" and
    // walked past. `selectTarget` now drops the cached verdict on a UID change,
    // so the page falls back to "nothing has run" and the operator re-runs the
    // check against the cluster they actually chose.
    if (verdict !== null) {
      const hadVerdict = await page.evaluate(() =>
        document.querySelector("#readiness-not-run") === null);
      check(hadVerdict === true,
        "a verdict is on screen before the swap; otherwise this journey proves nothing");
      const hashAtSwap = await page.evaluate(() => {
        const code = document.querySelector("#plan-hash-value");
        return code === null ? "" : code.textContent;
      });
      await page.selectOption("#target-cluster", secondUid);
      await pause(1500);
      const hashAfterSwap = await page.evaluate(() => {
        const code = document.querySelector("#plan-hash-value");
        return code === null ? "" : code.textContent;
      });
      const droppedText = await text(page);
      await shot(page, "08-target-change-refuses-submit");
      save("08-target-change.txt", droppedText);
      check(hashAfterSwap === hashAtSwap,
        "the two targets render the SAME plan bytes -- which is what makes the hash arm blind " +
        "here, and what this journey is about: " + hashAtSwap + " vs " + hashAfterSwap);
      // AND THE SUBMIT IS REFUSED, with the server's own reason named. An
      // earlier version DELETED the verdict, which left the page on the
      // "nothing has run" warning and PERMITTED the submit -- so a prefix edit
      // refused and a target swap only warned, for one rule. The second review
      // named the asymmetry.
      const blockedText = await page.evaluate(() => {
        const p = document.querySelector("#readiness-blocked");
        return p === null ? null : p.textContent;
      });
      check(typeof blockedText === "string",
        "the submit is refused after the swap: " + droppedText.slice(0, 1500));
      check(blockedText.includes("referentChanged"),
        "with D2 6.6's own reason named: " + blockedText.slice(0, 400));
      check(blockedText.includes(secondTarget),
        "and the cluster it is about: " + blockedText.slice(0, 400));
      const swapDisabled = await page.evaluate(() => {
        const b = document.querySelector("#create-restore");
        return b === null ? null : b.disabled;
      });
      check(swapDisabled === true, "and the create button is disabled");
      record("a target change refuses the submit though the plan bytes do not move", {
        planHash: hashAtSwap,
        from: targetCluster, to: secondTarget,
        reason: blockedText.slice(0, 300),
        rule: "D2 6.6 referentChanged: choosing another target changes a referent UID, so the " +
          "result is stale",
      });
      // THE NEGATIVE CONTROL: re-selecting the SAME target is not a change, so
      // the state stays where it is rather than being marked unconditionally.
      const beforeSame = await page.evaluate(() => {
        const p = document.querySelector("#readiness-blocked");
        return p === null ? null : p.textContent;
      });
      await page.selectOption("#target-cluster", secondUid);
      await pause(1000);
      const afterSame = await page.evaluate(() => {
        const p = document.querySelector("#readiness-blocked");
        return p === null ? null : p.textContent;
      });
      check(beforeSame === afterSame,
        "re-selecting the same target changes nothing about the readiness state");
      control("re-selecting the same target is not a target change", {
        refusalUnchanged: true,
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
      // THE BASELINE FOR "UNTOUCHED", taken immediately before the retry
      // journey starts rather than when the controller first marked the run
      // Failed -- so the comparison at the end is about what the RETRY did and
      // not about the controller settling.
      const beforeRetry = kubeJson(["-n", namespace, "get", "restore", failedName]);
      save("09-failed-restore-before-retry.json", beforeRetry);
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
      const retryPlanBytes = await page.evaluate(() => {
        const pre = document.querySelector("#plan-bytes");
        return pre === null ? null : pre.textContent;
      });
      check(typeof retryPlanBytes === "string" && retryPlanBytes.length > 0,
        "the retry's plan bytes are on screen");
      await shot(page, "11-retry-wizard");
      save("11-retry-text.txt", retryText);
      check(retryPrefix !== OLD_PREFIX,
        "the retry's prefix is NOT the failed run's: " + retryPrefix);
      check(retryPrefix.includes("retry-"), "and it names the run it retries: " + retryPrefix);
      check(retryText.includes(failedName.toLowerCase()),
        "the banner names the failed run");
      check(retryText.includes("not reused"),
        "and says the old approval is not reused");
      // THE MINTED NAMES ARE `restore-<8 hex>` AND `approval-<8 hex>`, taken
      // from the plan hash -- `ui/plan.js`'s `mintNames`. (A previous run
      // looked for the product API's `rst-`/`apr-` prefixes, which are what
      // the SERVER mints when it names an object for a caller that supplied
      // none; the wizard supplies one.)
      const mintedRestore = retryNames.find((n) => /^restore-[0-9a-f]{8}$/.test(String(n)));
      const mintedApproval = retryNames.find((n) => /^approval-[0-9a-f]{8}$/.test(String(n)));
      check(mintedRestore !== undefined && mintedRestore !== failedName,
        "the retry mints a NEW Restore name: " + mintedRestore + " vs " + failedName);
      check(mintedApproval !== undefined && mintedApproval !== failedApproval,
        "and a NEW Approval name: " + mintedApproval + " vs " + failedApproval);
      record("the retry is a new execution: a fresh prefix, a new plan and two new names", {
        failedRestore: failedName, failedApproval: failedApproval,
        retryPrefix: retryPrefix, mintedRestore: mintedRestore, mintedApproval: mintedApproval,
      });

      // AND THE RETRY IS SUBMITTED, so "the old approval ref is never sent" is
      // a statement about a request that EXISTS.
      //
      // THE EARLIER CONTROL COULD NOT FAIL and the review said so: it filtered
      // every `/restores` write in the run and asserted none named the failed
      // Approval -- but no retry create was ever issued, so the set held only
      // this harness's own probes, whose approvalRef is `apr-mapping-probe`.
      // No value made it fail. Now the button is clicked, the set is non-empty
      // by assertion, and what is checked is the body the WIZARD sent.
      const retryMarker = writesSoFar();
      await page.click("#create-restore");
      let retryCreates = [];
      for (let i = 0; i < 40; i += 1) {
        retryCreates = restoreCreatesSince(retryMarker);
        if (retryCreates.length > 0) {
          break;
        }
        await pause(500);
      }
      await shot(page, "12-retry-submitted");
      save("12-retry-requests.json", retryCreates);
      save("12-non-get-requests.json", requests);
      check(retryCreates.length === 1,
        "the retry sent exactly one create: " + JSON.stringify(retryCreates.map((r) => r.url)) +
        "; page said:\n" + (await text(page)).slice(0, 1200));
      const retryBody = JSON.parse(retryCreates[0].body);
      // THE SAME IDENTITY LOOKUP AS 5b. These bytes happen to be unique to the
      // retry, so the bytes alone would answer -- but a readback that is only
      // correct by accident is the trap the second review named, and this file
      // does not keep two shapes for one question.
      let retryCreated = null;
      for (let i = 0; i < 40; i += 1) {
        retryCreated = restoreFor(retryPlanBytes, retryBody.approvalRef.name);
        if (retryCreated !== null) {
          break;
        }
        await pause(500);
      }
      check(retryBody.approvalRef.name === mintedApproval,
        "and it names the NEWLY minted Approval " + mintedApproval + ", not " +
        retryBody.approvalRef.name);
      check(retryBody.approvalRef.name !== failedApproval,
        "which is not the failed run's");
      check(!JSON.stringify(retryBody).includes(failedApproval),
        "the failed run's Approval is named nowhere in the body the wizard sent: " +
        JSON.stringify(retryBody).slice(0, 800));
      check(!JSON.stringify(retryBody).includes(failedName),
        "and neither is the failed Restore");
      check(retryBody.target.topicNaming.prefix === retryPrefix,
        "under the fresh prefix");
      check(retryCreated !== null, "and the object exists with the retry's own plan bytes");
      save("12-retry-created.json", retryCreated);
      check(retryCreated.metadata.name !== failedName, "as a different object");
      record("the retry is submitted and names its own newly minted Approval", {
        created: retryCreated.metadata.name, uid: retryCreated.metadata.uid,
        approvalRef: retryBody.approvalRef.name, failedApproval: failedApproval,
        prefix: retryBody.target.topicNaming.prefix,
      });
      control("the failed run's Approval is absent from a NON-EMPTY set of wizard-sent bodies", {
        wizardCreates: retryCreates.length,
        approvalRefSent: retryBody.approvalRef.name,
      });

      // AND THE OLD RUN IS UNTOUCHED BY ALL OF IT, whole-object. The earlier
      // form was `resourceVersion unchanged OR still Failed`, and the `||` let
      // it pass on a changed revision -- the review named it. Comparing the
      // whole object catches a status write, a label, an annotation or a spec
      // edge anywhere.
      const after = kubeJson(["-n", namespace, "get", "restore", failedName]);
      save("09-failed-restore-after.json", after);
      check(JSON.stringify(after) === JSON.stringify(beforeRetry),
        "the failed Restore is byte-identical after the whole retry, submit included. " +
        "resourceVersion " + beforeRetry.metadata.resourceVersion + " -> " +
        after.metadata.resourceVersion);
      control("the failed Restore is byte-identical after the retry was submitted, whole-object", {
        uid: after.metadata.uid, phase: after.status.phase,
        resourceVersion: after.metadata.resourceVersion,
        prefix: after.spec.target.topicNaming.prefix,
      });
    }

    // ------------------------------------------------ destination TOCTOU control
    // Render and review an exact saved-destination plan first. Only then
    // delete and recreate the Destination under the same name. The submit's
    // final public read must see the replacement UID, refuse, and leave both
    // the reviewed bytes and the cluster's Restore set untouched.
    await openOldPoint();
    const racePlan = await page.evaluate(() => {
      const pre = document.querySelector("#plan-bytes");
      return pre === null ? null : pre.textContent;
    });
    check(typeof racePlan === "string" && racePlan.includes("bucket: \"kafka-backups\""),
      "the TOCTOU control first reviewed the original saved location");
    const beforeReplacement = kubeJson([
      "-n", namespace, "get", "backupdestination", frozenDestination.name,
    ]);
    check(beforeReplacement.metadata.uid === frozenDestination.uid,
      "the object about to be replaced is the destination frozen onto the point");
    kube(["-n", namespace, "delete", "backupdestination", frozenDestination.name,
      "--wait=true"]);
    const replacementSpec = JSON.parse(JSON.stringify(beforeReplacement.spec));
    replacementSpec.storage.bucket = "replacement-" + suffix;
    const replacementMade = apply({
      apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
      metadata: { name: frozenDestination.name, namespace: namespace, labels: LABELS },
      spec: replacementSpec,
    });
    check(replacementMade.metadata.uid !== frozenDestination.uid,
      "the same-name replacement has a different UID");
    let replacement = null;
    for (let i = 0; i < 60; i += 1) {
      const seen = kubeJson([
        "-n", namespace, "get", "backupdestination", frozenDestination.name,
      ]);
      if (typeof ((seen.status || {}).locationDigest) === "string") {
        replacement = seen;
        break;
      }
      await pause(1000);
    }
    check(replacement !== null, "the replacement Destination never published a location digest");

    const beforeRaceSubmit = writesSoFar();
    await page.click("#create-restore");
    await waitForText(page, "was recreated", "the post-review destination replacement refusal");
    await shot(page, "13-destination-race-refused");
    const racePlanAfter = await page.evaluate(() => {
      const pre = document.querySelector("#plan-bytes");
      return pre === null ? null : pre.textContent;
    });
    check(writesSoFar() === beforeRaceSubmit,
      "the same-name destination replacement sent no write: " +
        JSON.stringify(requests.slice(beforeRaceSubmit)));
    check(racePlanAfter === racePlan,
      "the confirming read did not replace or mutate the reviewed plan bytes");
    check(!(await text(page)).includes(("replacement-" + suffix).toLowerCase()),
      "the replacement bucket was neither rendered nor signed");
    control("a saved destination recreated after review is refused before submit", {
      name: frozenDestination.name,
      frozenUid: frozenDestination.uid,
      replacementUid: replacement.metadata.uid,
      frozenLocationDigest: frozenDestination.locationDigest,
      replacementLocationDigest: replacement.status.locationDigest,
      writesDuring: 0,
      reviewedPlanUnchanged: true,
    });

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
