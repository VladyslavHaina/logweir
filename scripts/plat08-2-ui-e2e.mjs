// PLAT-08.2 live UI acceptance harness: destination defaults are inherited,
// storage choices are validated, archive and evidence are separate stores, and
// transport security and path-style addressing are independent controls.
//
// THE LAUNCHER IS `scripts/plat10-ui-e2e.mjs`'s: `logweir-api` in localAdmin
// mode on a loopback port, pointed at this worktree's own `ui/` and at one
// namespace this run created, with a real Chromium driven against it.
//
// THE ONE RECONCILER IS THE SHARED LAB'S. Every BackupDestination, Backup,
// Preflight, Approval and Restore below is reconciled by `weirkeeper` in
// `logweir-scram-local`, which watches every namespace and is only READ here.
// The lab's MinIO is used the way `plat10-ui-e2e.mjs` uses it -- from a
// `minio/mc` Job in this namespace -- but through TWO BUCKETS AND TWO USERS OF
// THIS RUN'S OWN, created here and removed in the cleanup, so "nothing was
// written to the other bucket" is a statement about stores nobody else uses,
// and each user's policy reaches its own bucket only. The restore target is a
// single-node Kafka this run deploys in its own namespace, so no topic is ever
// created on the shared brokers; the lab's `kafka-source` is only read.
//
// WHAT IS A FIXTURE, AND SAID TO BE. One object: the recovery point the wizard
// is opened on. A destination-backed run on this lab never becomes a recovery
// point by itself -- `status.windowCovered` is written only after the
// controller reads the receipt through the destination's evidence grant, and
// this build does that only for an allowlisted ControllerIdentity location,
// which the lab policy has none of (plat10's `blocked` record). So the harness
// creates ONE Backup object (a name over 63 characters, which the controller
// refuses before it creates anything) and writes onto its status the REAL
// run's facts: its backupId and records, its frozen `status.destination`, and
// the `windowCovered` the controller's own `window_covered()` would have
// derived from the REAL receipt, read from this run's bucket. Every other
// object is real, and the restore the wizard submits reads the REAL archive.
//
// EVERY ROW HAS A NEGATIVE CONTROL, and each is a refusal the product must
// make: a page refusal that sends nothing, an API refusal, or a submit that
// creates no object -- with a `kubectl` read proving what did not happen.
//
// SECRETS. The two MinIO users' secret keys are minted here, held in memory,
// typed into the destination form's write-only inputs and into one owned
// Secret the mc Job reads; they are never printed, and the run fails if any
// artifact, response body, page text or API log line contains one.
//
// Dependencies: Node.js, kubectl, a built `logweir-api` and `logweir`,
// Playwright/Chromium, openssl:
//   NODE_PATH="$(npm root -g)" node scripts/plat08-2-ui-e2e.mjs
//
// Environment (all optional): UI_E2E_OWNER (plat08-2), UI_E2E_PREFIX
// (lw-p082-), UI_E2E_API_BIN, UI_E2E_CLI_BIN, UI_E2E_UI_DIR, UI_E2E_ARTIFACTS,
// UI_E2E_KEEP ("1" keeps the namespace), UI_E2E_APPROVER_KEY (the lab approver
// PRIVATE key; its path is passed to `logweir drill approve` and nothing here
// reads it).

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, randomBytes } from "node:crypto";
import { openDestinationCreate, wizardAt, wizardStep } from "./console-steps.mjs";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = process.env.UI_E2E_KUBECTL || "kubectl";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UI_DIR = process.env.UI_E2E_UI_DIR || join(REPO, "ui");
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "release", "logweir-api");
const CLI_BIN = process.env.UI_E2E_CLI_BIN || join(REPO, "target", "release", "logweir");
const APPROVER_KEY = process.env.UI_E2E_APPROVER_KEY ||
  join(homedir(), ".logweir-lab", "scram-e2e", "approver.pem");
const OWNER = process.env.UI_E2E_OWNER || "plat08-2";
const ARTIFACTS_ROOT = process.env.UI_E2E_ARTIFACTS ||
  "/tmp/logweir-roadmap-run/claude/artifacts/plat08-2";
const NAMESPACE_PREFIX = process.env.UI_E2E_PREFIX || "lw-p082-";
const OWNER_LABEL = "logweir.dev/test-owner=" + OWNER;
const LABELS = { "logweir.dev/test-owner": OWNER };

const LAB = "logweir-scram-local";
const LAB_SOURCE = "kafka-source." + LAB + ".svc.cluster.local:9096";
// The lab MinIO, in the two spellings this run needs: the plaintext endpoint it
// actually serves, and an https origin on the same host -- the latter only for
// the destination whose TRANSPORT this run examines, never for a run.
const MINIO_HOST = "minio." + LAB + ".svc:9000";
const MINIO_HTTP = "http" + "://" + MINIO_HOST;
const MINIO_HTTPS = "https" + "://" + MINIO_HOST;

const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z").toLowerCase();
const namespace = NAMESPACE_PREFIX + stamp;
const suffix = randomBytes(3).toString("hex");
const ARTIFACTS = join(ARTIFACTS_ROOT, namespace);
const WORK_DIR = join("/tmp", "plat08-2-live-" + namespace);

// THIS RUN'S OWN STORES. Bucket names are the namespace plus a letter.
const BUCKET_A = namespace + "-a";
const BUCKET_B = namespace + "-b";
const PREFIX_A = "p082";
const PREFIX_B = "p082";
const USER_A = "p082a" + suffix;
const USER_B = "p082b" + suffix;
const SECRET_A = randomBytes(18).toString("hex");
const SECRET_B = randomBytes(18).toString("hex");
const SECRETS = [SECRET_A, SECRET_B];

const DEST_A = "dest-a";
const DEST_B = "dest-b";
const DEST_TLS = "dest-tls";
const DEST_C = "dest-c";
const TOPIC = "orders";

const result = {
  harness: "scripts/plat08-2-ui-e2e.mjs",
  task: "PLAT-08.2",
  kubeContext: KUBE_CONTEXT,
  owner: OWNER,
  namespace: namespace,
  lab: { release: LAB, source: LAB_SOURCE, minio: MINIO_HOST, usedReadOnly: true },
  buckets: [BUCKET_A, BUCKET_B],
  minioUsers: [USER_A, USER_B],
  uiDirectory: UI_DIR,
  apiBinary: API_BIN,
  revision: null,
  apiBinarySha256: null,
  startedAt: new Date().toISOString(),
  mode: "console (logweir-api, localAdmin, loopback)",
  fixtures: [],
  rows: [],
  controls: [],
  blocked: [],
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

function row(id, title, detail) {
  result.rows.push(Object.assign({ row: id, title: title, verdict: "PASS" }, detail || {}));
  process.stderr.write("== PASS " + id + ": " + title + "\n");
}

function control(id, about, detail) {
  result.controls.push(Object.assign({ row: id, control: about }, detail || {}));
  process.stderr.write("   -- control " + id + ": " + about + "\n");
}

function blocked(id, title, detail) {
  result.blocked.push(Object.assign({ row: id, title: title, verdict: "BLOCKED" }, detail || {}));
  process.stderr.write("== BLOCKED " + id + ": " + title + "\n");
}

function save(name, value) {
  const at = join(ARTIFACTS, name);
  writeFileSync(at, typeof value === "string" ? value : JSON.stringify(value, null, 2) + "\n");
  return at;
}

function assertSafeNamespace(ns) {
  check(ns.startsWith(NAMESPACE_PREFIX), "this harness only touches " + NAMESPACE_PREFIX + "*");
  check(ns !== "default" && !ns.startsWith("kube-") && !ns.startsWith("logweir-scram"),
    "refusing a system or shared-fixture namespace: " + ns);
}

function kube(args, options) {
  const opts = options || {};
  const done = spawnSync(KUBECTL, ["--context", KUBE_CONTEXT].concat(args), {
    encoding: "utf8",
    input: opts.input,
    timeout: opts.timeout || 60000,
    maxBuffer: 16 * 1024 * 1024,
  });
  const expected = opts.expected || [0];
  if (!expected.includes(done.status)) {
    throw new Error(KUBECTL + " " + args.filter((a) => !String(a).startsWith("{")).join(" ") +
      " exited " + done.status + ": " + String(done.stderr || "").trim().slice(0, 1500));
  }
  return done;
}

function kubeJson(args) {
  return JSON.parse(kube(args.concat(["-o", "json"])).stdout);
}

function create(object) {
  return JSON.parse(kube(["-n", namespace, "create", "-f", "-", "-o", "json"],
    { input: JSON.stringify(object) }).stdout);
}

function commandText(command, args) {
  const done = spawnSync(command, args, { encoding: "utf8", timeout: 30000 });
  return done.status === 0 ? String(done.stdout || "").trim() : null;
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

async function until(label, read, predicate, seconds) {
  let last = null;
  for (let i = 0; i < (seconds || 120); i += 1) {
    last = read();
    if (predicate(last)) {
      return last;
    }
    await pause(1000);
  }
  throw new Error(label + ": never happened within " + (seconds || 120) + " s; last seen: " +
    JSON.stringify(last === null ? null : (last.status || last)).slice(0, 1500));
}

// --------------------------------------------------------------- the browser

let page = null;

async function text() {
  return (await page.evaluate(() => document.body.innerText));
}

async function shot(name) {
  const at = join(ARTIFACTS, name + ".png");
  await page.screenshot({ path: at, fullPage: true });
  result.screenshots.push(at);
  return at;
}

async function waitFor(selector, label, timeout) {
  try {
    await page.waitForSelector(selector, { timeout: timeout || 30000 });
  } catch (never) {
    throw new Error(label + ": " + selector + " never appeared. Saw:\n" + (await text()).slice(0, 2500));
  }
}

async function waitForText(needle, label, seconds) {
  for (let i = 0; i < (seconds || 60) * 2; i += 1) {
    if ((await text()).indexOf(needle) !== -1) {
      return;
    }
    await pause(500);
  }
  throw new Error(label + ": never saw " + JSON.stringify(needle) + ". Saw:\n" +
    (await text()).slice(0, 3000));
}

/** Opens a route with a FRESH page load (a hash change to the same route
 *  re-renders nothing), reloading once if the mode probe landed in legacy
 *  mode -- the reason `plat10-ui-e2e.mjs` records. */
async function open(url, selector, label) {
  await page.goto(url, { waitUntil: "load", timeout: 30000 });
  await page.reload({ waitUntil: "load", timeout: 30000 });
  try {
    await page.waitForSelector(selector, { timeout: 20000 });
  } catch (notYet) {
    await page.reload({ waitUntil: "load", timeout: 30000 });
    await waitFor(selector, label);
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
  throw new Error("logweir-api never answered /healthz. Log:\n" + apiLog.join("").slice(-3000));
}

function stopApi() {
  if (api !== null && api.exitCode === null) {
    api.kill("SIGTERM");
  }
}

// ------------------------------------------------------------ the fixtures

function copyLabSecret(labName, ownName) {
  const source = kubeJson(["-n", LAB, "get", "secret", labName]);
  create({
    apiVersion: "v1", kind: "Secret", type: source.type || "Opaque",
    metadata: { name: ownName, labels: LABELS }, data: source.data,
  });
  result.fixtures.push({ kind: "Secret", name: ownName, copiedFrom: LAB + "/" + labName,
    note: "value never printed, read or logged" });
}

/** One `minio/mc` Job in THIS namespace. `root` jobs use the owned copy of the
 *  lab's MinIO root credential (bucket and user administration, and reads
 *  for evidence); the alias line is silenced so no credential reaches a log. */
let mcJobs = 0;
function mcJob(step, script, extraEnv) {
  mcJobs += 1;
  const name = "p082-mc-" + String(mcJobs) + "-" + step;
  create({
    apiVersion: "batch/v1", kind: "Job",
    metadata: { name: name, labels: LABELS },
    spec: {
      backoffLimit: 0,
      ttlSecondsAfterFinished: 3600,
      template: {
        metadata: { labels: LABELS },
        spec: {
          restartPolicy: "Never",
          containers: [{
            name: "mc", image: "docker.io/vladyslavhaina/mc-mirror@sha256:9c7cbc3f47b092d52b73124fb9ab12f3266534c23c283b2e984d07408c9ff381", imagePullPolicy: "IfNotPresent",
            command: ["/bin/sh", "-ec"],
            args: ["mc alias set p082 \"$S3_ENDPOINT\" \"$ROOT_USER\" \"$ROOT_PASSWORD\" >/dev/null; " +
              script],
            env: [
              { name: "S3_ENDPOINT", value: MINIO_HTTP },
              { name: "BUCKET_A", value: BUCKET_A },
              { name: "BUCKET_B", value: BUCKET_B },
              { name: "ROOT_USER", valueFrom: { secretKeyRef: { name: "p082-minio-root", key: "user" } } },
              { name: "ROOT_PASSWORD", valueFrom: { secretKeyRef: { name: "p082-minio-root", key: "password" } } },
            ].concat(extraEnv || []),
          }],
        },
      },
    },
  });
  const waited = kube(["-n", namespace, "wait", "--for=condition=complete", "job/" + name,
    "--timeout=150s"], { expected: [0, 1], timeout: 160000 });
  const logs = kube(["-n", namespace, "logs", "job/" + name], { expected: [0, 1] }).stdout;
  save("mc-" + name + ".log", logs);
  check(waited.status === 0, "the object-store step " + name + " did not complete:\n" + logs);
  return logs.trim();
}

function userEnv() {
  const ref = (key) => ({ valueFrom: { secretKeyRef: { name: "p082-users", key: key } } });
  return [
    Object.assign({ name: "USER_A" }, ref("user-a")),
    Object.assign({ name: "SECRET_A" }, ref("secret-a")),
    Object.assign({ name: "USER_B" }, ref("user-b")),
    Object.assign({ name: "SECRET_B" }, ref("secret-b")),
  ];
}

let storesMade = false;
function makeStores() {
  copyLabSecret("minio-root", "p082-minio-root");
  const b64 = (v) => Buffer.from(v).toString("base64");
  create({
    apiVersion: "v1", kind: "Secret", type: "Opaque",
    metadata: { name: "p082-users", labels: LABELS },
    data: { "user-a": b64(USER_A), "secret-a": b64(SECRET_A), "user-b": b64(USER_B),
      "secret-b": b64(SECRET_B) },
  });
  const policy = (bucket) => JSON.stringify({ Version: "2012-10-17", Statement: [{
    Effect: "Allow", Action: ["s3:*"],
    Resource: ["arn:aws:s3:::" + bucket, "arn:aws:s3:::" + bucket + "/*"],
  }] });
  // MARKED BEFORE THE JOB, so a job that fails half way still gets the
  // (idempotent) cleanup below.
  storesMade = true;
  const made = mcJob("make-stores",
    // A BUCKET OF ITS OWN, twice: `mc mb` without --ignore-existing, so a name
    // that already exists is a refusal.
    "mc mb \"p082/$BUCKET_A\"; mc mb \"p082/$BUCKET_B\"; " +
    "printf '%s' '" + policy(BUCKET_A) + "' > /tmp/pa.json; " +
    "printf '%s' '" + policy(BUCKET_B) + "' > /tmp/pb.json; " +
    "mc admin policy create p082 \"$USER_A-rw\" /tmp/pa.json >/dev/null; " +
    "mc admin policy create p082 \"$USER_B-rw\" /tmp/pb.json >/dev/null; " +
    "mc admin user add p082 \"$USER_A\" \"$SECRET_A\" >/dev/null; " +
    "mc admin user add p082 \"$USER_B\" \"$SECRET_B\" >/dev/null; " +
    "mc admin policy attach p082 \"$USER_A-rw\" --user \"$USER_A\" >/dev/null; " +
    "mc admin policy attach p082 \"$USER_B-rw\" --user \"$USER_B\" >/dev/null; " +
    // The mc image has no grep or awk: presence is asked of `mc stat`.
    "mc stat \"p082/$BUCKET_A\" >/dev/null && mc stat \"p082/$BUCKET_B\" >/dev/null && echo stores-made",
    userEnv());
  check(made.indexOf("stores-made") !== -1,
    "the two owned buckets and users were not made: " + made);
  result.stores = { buckets: [BUCKET_A, BUCKET_B], users: [USER_A, USER_B],
    userPolicies: [USER_A + "-rw: s3:* on " + BUCKET_A + " only", USER_B + "-rw: s3:* on " + BUCKET_B + " only"],
    log: made };
}

/** A recursive listing of one owned bucket, object keys only. */
function listBucket(step, bucket) {
  const out = mcJob(step, "mc ls --recursive --json \"p082/" + bucket + "\"; echo listed");
  return out.split("\n").filter((l) => l.startsWith("{")).map((l) => JSON.parse(l))
    .filter((o) => typeof o.key === "string").map((o) => o.key);
}

function catObject(step, bucket, key) {
  return mcJob(step, "mc cat \"p082/" + bucket + "/" + key + "\"");
}

function deployTargetKafka() {
  const clusterId = randomBytes(16).toString("base64url").slice(0, 22);
  const host = "kafka-p082." + namespace + ".svc.cluster.local";
  create({
    apiVersion: "apps/v1", kind: "Deployment",
    metadata: { name: "kafka-p082", labels: LABELS },
    spec: {
      replicas: 1,
      selector: { matchLabels: { app: "kafka-p082" } },
      template: {
        metadata: { labels: Object.assign({ app: "kafka-p082" }, LABELS) },
        spec: {
          containers: [{
            name: "kafka", image: "apache/kafka:3.7.1", imagePullPolicy: "IfNotPresent",
            resources: { requests: { cpu: "100m", memory: "512Mi" }, limits: { memory: "1Gi" } },
            readinessProbe: { tcpSocket: { port: 9094 }, periodSeconds: 3, initialDelaySeconds: 5 },
            env: [
              ["CLUSTER_ID", clusterId], ["KAFKA_NODE_ID", "1"],
              ["KAFKA_PROCESS_ROLES", "broker,controller"],
              ["KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXT://0.0.0.0:9094"],
              ["KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:9092,EXT://" + host + ":9094"],
              ["KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093"],
              ["KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"],
              ["KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT"],
              ["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXT:PLAINTEXT"],
              ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"],
              ["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"],
              ["KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"],
              ["KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"],
              ["KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"],
              ["KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs"],
              ["KAFKA_HEAP_OPTS", "-Xmx512M -Xms256M"],
            ].map(([n, v]) => ({ name: n, value: v })),
          }],
        },
      },
    },
  });
  create({
    apiVersion: "v1", kind: "Service",
    metadata: { name: "kafka-p082", labels: LABELS },
    spec: { selector: { app: "kafka-p082" }, ports: [{ name: "ext", port: 9094, targetPort: 9094 }] },
  });
  result.fixtures.push({ kind: "Deployment", name: "kafka-p082",
    note: "the restore target: a single-node Kafka in this namespace, so no shared broker is written" });
  return host + ":9094";
}

function connection(name, servers, role, scramSecret) {
  create({
    apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
    metadata: { name: name, labels: LABELS },
    spec: {
      bootstrapServers: [servers], role: role,
      auth: scramSecret === undefined
        ? { mode: "plaintext", tls: false }
        : { mode: "scramSha512", tls: false, username: "scram-user", secretRef: { name: scramSecret } },
    },
  });
  result.fixtures.push({ kind: "KafkaCluster", name: name, role: role, bootstrapServers: servers });
}

// --------------------------------------------------------------- the run

async function main() {
  mkdirSync(ARTIFACTS, { recursive: true });
  assertSafeNamespace(namespace);
  result.revision = commandText("git", ["-C", REPO, "rev-parse", "HEAD"]);
  result.apiBinarySha256 = commandText("shasum", ["-a", "256", API_BIN]);
  check(result.revision !== null && result.apiBinarySha256 !== null, "revision and binary recorded");
  check(existsSync(CLI_BIN), "the logweir CLI is built: " + CLI_BIN);
  check(existsSync(APPROVER_KEY) && (statSync(APPROVER_KEY).mode & 0o077) === 0,
    "the lab approver key is present and private (0600): " + APPROVER_KEY);

  const labPods = kubeJson(["-n", LAB, "get", "pods", "-l", "app.kubernetes.io/component=control-plane"]).items;
  check(labPods.length === 1, "exactly one lab controller pod");
  const others = kubeJson(["get", "deployments", "-A"]).items.filter((d) =>
    String(d.metadata.name).indexOf("weirkeeper") !== -1 && d.metadata.namespace !== LAB);
  check(others.length === 0, "no second weirkeeper deployment exists");
  result.controller = { namespace: LAB, pod: labPods[0].metadata.name,
    image: labPods[0].spec.containers[0].image,
    imageID: (labPods[0].status.containerStatuses || [{}])[0].imageID,
    readOnly: "observed only; nothing in " + LAB + " was changed" };

  check(kube(["get", "namespace", namespace], { expected: [0, 1] }).status !== 0,
    "refusing to reuse an existing namespace");
  kube(["create", "namespace", namespace]);
  kube(["label", "namespace", namespace, OWNER_LABEL]);
  result.namespaceUid = kubeJson(["get", "namespace", namespace]).metadata.uid;
  result.created.push({ kind: "Namespace", name: namespace, uid: result.namespaceUid });

  create({ apiVersion: "v1", kind: "ServiceAccount",
    metadata: { name: "logweir-runner", labels: LABELS }, automountServiceAccountToken: false });
  copyLabSecret("logweir-signing-key", "logweir-signing-key");
  copyLabSecret("source-scram", "source-scram");
  makeStores();
  const targetServers = deployTargetKafka();
  const source = "orders-" + suffix;
  const target = "target-" + suffix;
  connection(source, LAB_SOURCE, "source", "source-scram");
  // THE TARGET IS DECLARED ONCE ITS BROKER ANSWERS, so the controller's first
  // probe is of a broker that exists (a failed first probe waits a full probe
  // interval before the next, which run 4 of this harness measured).
  kube(["-n", namespace, "rollout", "status", "deploy/kafka-p082", "--timeout=300s"], { timeout: 320000 });
  connection(target, targetServers, "target");
  await until("the lab controller probes both connections",
    () => kubeJson(["-n", namespace, "get", "kafkaclusters"]).items,
    (items) => items.length === 2 && items.every((c) => typeof ((c.status || {}).clusterId) === "string" &&
      c.status.clusterId.length > 0), 600);

  const port = await freePort();
  await startApi(port);
  const base = "http://127.0.0.1:" + port + "/ui/";
  const apiBase = "http://127.0.0.1:" + port + "/api/v1/namespaces/" + namespace;
  const browser = await chromium.launch();
  const context = await browser.newContext();
  page = await context.newPage();
  const bodies = [];
  context.on("response", async (response) => {
    try {
      if (response.url().indexOf("/api/v1/") !== -1) {
        bodies.push({ url: response.url(), status: response.status(), body: await response.text() });
      }
    } catch (gone) {
      // the DOM assertions cover it
    }
  });
  context.on("request", (request) => {
    if (request.url().indexOf("/api/v1/") !== -1 && request.method() !== "GET") {
      // THE BROWSER MUST SEND A TYPED CREDENTIAL ONCE -- that is the create
      // request -- and THIS HARNESS must not keep it: the captured body is
      // redacted before it is stored, and the fact that it carried one is kept.
      let body = String(request.postData() || "");
      let carried = false;
      for (const secret of SECRETS) {
        if (body.indexOf(secret) !== -1) {
          carried = true;
          body = body.split(secret).join("[typed secret access key, redacted by the harness]");
        }
      }
      result.requests.push({ method: request.method(), url: request.url(), body: body,
        carriedTypedCredential: carried });
    }
  });
  const postsTo = (suffixPath) => result.requests.filter((r) => r.method === "POST" &&
    r.url.split("?")[0].endsWith(suffixPath));
  const destinations = () => kubeJson(["-n", namespace, "get", "backupdestinations"]).items;
  const destination = (name) => kubeJson(["-n", namespace, "get", "backupdestination", name]);

  const destinationsRoute = base + "#/destinations?ns=" + namespace;
  const schedulesRoute = base + "#/schedules?ns=" + namespace;

  /** Fills the destination form; the credential, when given, goes only into
   *  the write-only inputs and nowhere else. */
  async function fillDestination(d) {
    await openDestinationCreate(page);
    await page.fill("#destination-name", d.name);
    await page.fill("#destination-bucket", d.bucket);
    await page.fill("#destination-prefix", d.prefix);
    await page.fill("#destination-region", "us-east-1");
    await page.fill("#destination-endpoint", d.endpoint);
    await page.check(d.addressing === "virtualHosted" ? "#destination-addressing-virtual"
      : "#destination-addressing-pathstyle");
    await page.check(d.security === "insecureHttp" ? "#destination-security-http"
      : "#destination-security-tls");
    if (d.accessKeyId !== undefined) {
      await page.selectOption("#destination-archiveWrite-source", "new");
      await page.fill("#destination-archiveWrite-akid", d.accessKeyId);
      await page.fill("#destination-archiveWrite-sak", d.secretAccessKey);
    } else {
      await page.selectOption("#destination-archiveWrite-source", "existing");
      await page.fill("#destination-archiveWrite-secret", d.existingSecret);
    }
    if (d.isDefault === true) {
      await page.check("#destination-default");
    }
  }

  async function radios() {
    return page.evaluate(() => ({
      pathStyle: document.querySelector("#destination-addressing-pathstyle").checked,
      virtualHosted: document.querySelector("#destination-addressing-virtual").checked,
      tls: document.querySelector("#destination-security-tls").checked,
      insecureHttp: document.querySelector("#destination-security-http").checked,
    }));
  }

  async function submitDestinationForm(name, expectPost) {
    const before = postsTo("/destinations").length;
    // A refused submit keeps the form open (its draft is in flight); required again here.
    await openDestinationCreate(page);
    await page.click("#destination-form button[type=submit]");
    if (!expectPost) {
      await pause(1500);
      return postsTo("/destinations").slice(before);
    }
    await until("the page POSTs destination " + name, () => postsTo("/destinations").slice(before),
      (posts) => posts.length === 1, 30);
    await until("destination " + name + " exists", () => destinations(),
      (items) => items.some((d) => d.metadata.name === name), 30);
    return postsTo("/destinations").slice(before);
  }

  try {
    // ================================================================= T2 + A2
    // EXPLICITLY CONFIGURED LOCAL HTTP, AND THE TWO CONTROLS ARE INDEPENDENT.
    await open(destinationsRoute, "#destination-create-disclosure", "the destinations page");
    // The form sits behind "Create destination" since console-ux-1 (MCP-10): opened by a click.
    await openDestinationCreate(page);
    await fillDestination({ name: DEST_A, bucket: BUCKET_A, prefix: PREFIX_A, endpoint: MINIO_HTTP,
      addressing: "pathStyle", security: "tls", accessKeyId: USER_A, secretAccessKey: SECRET_A,
      isDefault: true });
    // CONTROL: an http endpoint with TLS is refused by the page, and NOTHING is sent.
    const refusedTls = await submitDestinationForm(DEST_A, false);
    check(refusedTls.length === 0, "a TLS destination with an http endpoint was POSTed");
    await waitForText("the endpoint is plaintext http and the transport says TLS",
      "the page's scheme/transport refusal");
    check(!destinations().some((d) => d.metadata.name === DEST_A), "dest-a exists after a refusal");
    control("T2", "TLS with an http endpoint is refused by the page; no POST, no object", {
      posts: refusedTls.length });
    await shot("t2-01-tls-with-http-refused");

    // INDEPENDENCE, BOTH DIRECTIONS, IN THE BROWSER.
    let state = await radios();
    check(state.pathStyle && state.tls, "starting pair pathStyle + tls: " + JSON.stringify(state));
    await page.check("#destination-security-http");
    const afterTransport = await radios();
    check(afterTransport.pathStyle === true && afterTransport.virtualHosted === false,
      "choosing insecureHttp moved addressing: " + JSON.stringify(afterTransport));
    await page.check("#destination-addressing-virtual");
    const afterAddressing = await radios();
    check(afterAddressing.insecureHttp === true && afterAddressing.tls === false,
      "choosing virtualHosted moved transport: " + JSON.stringify(afterAddressing));
    // CONTROL (T4): virtualHosted with a custom endpoint is refused before sending.
    const refusedVh = await submitDestinationForm(DEST_A, false);
    check(refusedVh.length === 0, "a virtualHosted destination with a custom endpoint was POSTed");
    await waitForText("virtualHosted addressing with a custom endpoint is refused",
      "the page's custom-endpoint addressing refusal");
    control("T4", "virtualHosted with a custom endpoint is refused by the page; no POST", {});
    await page.check("#destination-addressing-pathstyle");
    const back = await radios();
    check(back.pathStyle && back.insecureHttp, "returning to pathStyle moved transport: " + JSON.stringify(back));
    // The refusal re-rendered the form from its draft, which never holds a
    // credential, so the write-only inputs are empty again: type them again.
    await page.selectOption("#destination-archiveWrite-source", "new");
    await page.fill("#destination-archiveWrite-akid", USER_A);
    await page.fill("#destination-archiveWrite-sak", SECRET_A);
    const madeA = await submitDestinationForm(DEST_A, true);
    const bodyA = JSON.parse(madeA[0].body);
    check(bodyA.storage.addressing === "pathStyle" && bodyA.transport.security === "insecureHttp",
      "dest-a body pair: " + JSON.stringify([bodyA.storage, bodyA.transport]));
    const objA = await until("dest-a is judged Valid", () => destination(DEST_A),
      (o) => ((o.status || {}).conditions || []).some((c) => c.type === "Valid" && c.status === "True"), 120);
    check(objA.spec.storage.addressing === "PathStyle" && objA.spec.transport.security === "InsecureHTTP",
      "stored dest-a: " + JSON.stringify([objA.spec.storage, objA.spec.transport]));
    await shot("t2-02-dest-a-created");
    row("T2", "explicitly configured local HTTP: the page creates an insecureHttp + pathStyle destination only when both are chosen, and changing one control never moved the other", {
      transitions: { start: state, afterTransport: afterTransport, afterAddressing: afterAddressing, back: back },
      requestPair: [bodyA.storage.addressing, bodyA.transport.security],
      stored: { addressing: objA.spec.storage.addressing, security: objA.spec.transport.security,
        endpoint: objA.spec.storage.endpoint },
      controller: (objA.status.conditions || []).find((c) => c.type === "Valid"),
      destination: { name: DEST_A, uid: objA.metadata.uid, locationDigest: objA.status.locationDigest },
    });
    result.created.push({ kind: "BackupDestination", name: DEST_A, uid: objA.metadata.uid, createdBy: "the page" });
    row("A2", "addressing and transport are independent explicit controls in the browser, in both directions", {
      transitions: { start: state, afterTransport: afterTransport, afterAddressing: afterAddressing, back: back },
    });

    await open(destinationsRoute, "#destination-create-disclosure", "the destinations page again");
    // The form sits behind "Create destination" since console-ux-1 (MCP-10): opened by a click.
    await openDestinationCreate(page);
    await fillDestination({ name: DEST_B, bucket: BUCKET_B, prefix: PREFIX_B, endpoint: MINIO_HTTP,
      addressing: "pathStyle", security: "insecureHttp", accessKeyId: USER_B, secretAccessKey: SECRET_B });
    await submitDestinationForm(DEST_B, true);
    const objB = await until("dest-b is judged Valid", () => destination(DEST_B),
      (o) => ((o.status || {}).conditions || []).some((c) => c.type === "Valid" && c.status === "True"), 120);
    result.created.push({ kind: "BackupDestination", name: DEST_B, uid: objB.metadata.uid, createdBy: "the page" });
    const grantA = objA.spec.access.archiveWrite.secret.name;
    const grantB = objB.spec.access.archiveWrite.secret.name;

    // ======================================================================= T1
    // HTTPS WITH PATH-STYLE: a TLS + pathStyle destination on an https origin.
    await open(destinationsRoute, "#destination-create-disclosure", "the destinations page (TLS)");
    // The form sits behind "Create destination" since console-ux-1 (MCP-10): opened by a click.
    await openDestinationCreate(page);
    await fillDestination({ name: DEST_TLS, bucket: BUCKET_A, prefix: "p082-tls", endpoint: MINIO_HTTPS,
      addressing: "pathStyle", security: "insecureHttp", existingSecret: grantA });
    const refusedHttps = await submitDestinationForm(DEST_TLS, false);
    check(refusedHttps.length === 0, "insecureHttp with an https endpoint was POSTed");
    await waitForText("the endpoint is https and the transport says insecureHttp", "the https refusal");
    control("T1", "insecureHttp with an https endpoint is refused by the page; no POST", {});
    await page.check("#destination-security-tls");
    check((await radios()).pathStyle === true, "choosing TLS moved addressing");
    const madeTls = await submitDestinationForm(DEST_TLS, true);
    const bodyTls = JSON.parse(madeTls[0].body);
    check(bodyTls.storage.addressing === "pathStyle" && bodyTls.transport.security === "tls",
      "dest-tls body pair: " + JSON.stringify([bodyTls.storage, bodyTls.transport]));
    const objTls = await until("dest-tls is judged Valid", () => destination(DEST_TLS),
      (o) => ((o.status || {}).conditions || []).some((c) => c.type === "Valid" && c.status === "True"), 120);
    result.created.push({ kind: "BackupDestination", name: DEST_TLS, uid: objTls.metadata.uid, createdBy: "the page" });
    await shot("t1-01-dest-tls-created");

    // ======================================================================= T4
    // CUSTOM ENDPOINT, THE API HALF: the page is not the gate.
    const direct = await fetch(apiBase + "/destinations", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Idempotency-Key": "p082-vh-" + suffix,
        "Origin": "http://127.0.0.1:" + port },
      body: JSON.stringify({ name: "dest-vh", storage: { provider: "s3", bucket: BUCKET_A,
        prefix: "vh", endpoint: MINIO_HTTP, addressing: "virtualHosted" },
      transport: { security: "insecureHttp" },
      access: { archiveWrite: { mode: "secretKeys", secret: { existing: { name: grantA } } } } }),
    });
    const directBody = await direct.text();
    save("t4-api-virtualhosted-refusal.json", { status: direct.status, body: JSON.parse(directBody) });
    check(direct.status === 422 && directBody.indexOf("addressing_unsupported_by_engine") !== -1,
      "the API accepted virtualHosted with a custom endpoint: " + direct.status + " " + directBody.slice(0, 400));
    check(!destinations().some((d) => d.metadata.name === "dest-vh"), "dest-vh exists");
    control("T4", "the product API refuses virtualHosted with a custom endpoint (422 addressing_unsupported_by_engine); no object", {
      status: direct.status });
    row("T4", "custom endpoint: pathStyle is accepted on a custom endpoint (dest-a, dest-b, dest-tls); virtualHosted there is refused by the page and by the API", {
      accepted: [DEST_A, DEST_B, DEST_TLS].map((n) => destination(n).spec.storage.endpoint),
      apiRefusal: { status: direct.status },
    });

    // ================================================================== S1 / T3
    // THE SCHEDULE INHERITS THE DEFAULT DESTINATION, AND SHOWS WHAT IT INHERITS.
    await open(schedulesRoute, "#schedule-form", "the schedules page");
    const preselected = await page.evaluate(() => ({
      value: document.querySelector("#policy-create-destination").value,
      uid: document.querySelector("#policy-create-destination-uid").value,
      note: (document.querySelector("#policy-create-destination-default") || {}).innerText || "",
      inherited: (document.querySelector("#policy-create-destination-inherited") || {}).innerText || "",
    }));
    check(preselected.value === DEST_A && preselected.uid === objA.metadata.uid,
      "the namespace default was not preselected by identity: " + JSON.stringify(preselected));
    check(preselected.inherited.indexOf(MINIO_HTTP) !== -1 &&
      preselected.inherited.indexOf("pathStyle") !== -1 &&
      preselected.inherited.indexOf("insecureHttp") !== -1,
    "the inherited settings are not shown: " + preselected.inherited);
    const inputsInInherited = await page.evaluate(() =>
      document.querySelectorAll("#policy-create-destination-inherited input").length);
    check(inputsInInherited === 0, "the inherited settings are editable inputs");
    await shot("s1-01-schedule-inherits-default");

    // CONSOLE MODE HAS NO SCHEDULE NAME FIELD (poc-fixes-2 review L5): the
    // product API names the schedule sch-<26 base32>, so the typed name is
    // filled only where the form still offers one.
    async function fillScheduleName(name) {
      if (await page.locator("#schedule-name").count() > 0) {
        await page.fill("#schedule-name", name);
      }
    }
    async function chooseSource() {
      const uid = kubeJson(["-n", namespace, "get", "kafkacluster", source]).metadata.uid;
      await page.selectOption("#schedule-source", uid);
    }
    async function createSchedule(name, destinationName) {
      await fillScheduleName(name);
      await chooseSource();
      await page.selectOption("#policy-create-mode", "advanced");
      await waitFor("#policy-create-cron", "the advanced cron input");
      await page.fill("#policy-create-cron", "0 0 1 1 *");
      await page.fill("#policy-create-topics", TOPIC);
      if (destinationName !== undefined) {
        await page.selectOption("#policy-create-destination", destinationName);
      }
      const before = kubeJson(["-n", namespace, "get", "backupschedules"]).items.length;
      await page.click("#schedule-form button[type=submit]");
      return before;
    }
    const schedules = () => kubeJson(["-n", namespace, "get", "backupschedules"]).items;
    const before1 = await createSchedule("nightly-a");
    await waitFor("#schedule-detail", "the redirect after creating the dest-a schedule");
    const schedA = await until("the dest-a schedule exists", schedules, (s) => s.length === before1 + 1, 30)
      .then((s) => s[s.length - 1]);
    check(JSON.stringify(schedA.spec.destinationRef) === JSON.stringify({ name: DEST_A }),
      "the inherited destination was not sent: " + JSON.stringify(schedA.spec));
    result.created.push({ kind: "BackupSchedule", name: schedA.metadata.name, uid: schedA.metadata.uid,
      createdBy: "the page" });
    row("S1", "a new schedule inherits the namespace default destination by identity, shows what it inherits as facts, and sends destinationRef", {
      preselected: preselected, stored: { name: schedA.metadata.name, destinationRef: schedA.spec.destinationRef,
        archiveUrl: schedA.spec.archive.url },
    });

    // T3 (schedule): a destination DELETED AND RECREATED during the draft is
    // refused; one EDITED during the draft is not.
    const cSpec = { storage: { provider: "S3", bucket: BUCKET_B, prefix: "p082-c", endpoint: MINIO_HTTP,
      addressing: "PathStyle" }, transport: { security: "InsecureHTTP" },
    access: { archiveWrite: { mode: "SecretKeys", secret: { name: grantB } } } };
    const c1 = create({ apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
      metadata: { name: DEST_C, labels: LABELS }, spec: cSpec });
    result.created.push({ kind: "BackupDestination", name: DEST_C, uid: c1.metadata.uid, createdBy: "kubectl" });
    await open(schedulesRoute, "#schedule-form", "the schedules page (draft C)");
    await fillScheduleName("draft-c");
    await chooseSource();
    await page.selectOption("#policy-create-mode", "advanced");
    await waitFor("#policy-create-cron", "the cron input");
    await page.fill("#policy-create-cron", "0 0 1 1 *");
    await page.fill("#policy-create-topics", TOPIC);
    await page.selectOption("#policy-create-destination", DEST_C);
    const pinnedC = await page.evaluate(() => document.querySelector("#policy-create-destination-uid").value);
    check(pinnedC === c1.metadata.uid, "choosing dest-c did not pin its uid: " + pinnedC);
    kube(["-n", namespace, "delete", "backupdestination", DEST_C, "--wait=true"]);
    const c2 = create({ apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
      metadata: { name: DEST_C, labels: LABELS }, spec: cSpec });
    result.created.push({ kind: "BackupDestination", name: DEST_C, uid: c2.metadata.uid,
      createdBy: "kubectl (recreated under the same name)" });
    const beforeC = schedules().length;
    await page.click("#schedule-form button[type=submit]");
    await waitForText("different object now answers to that name", "the recreated-destination refusal");
    await pause(1000);
    check(schedules().length === beforeC, "a schedule was created against a recreated destination");
    await shot("t3-01-schedule-recreated-destination-refused");
    control("T3", "a destination deleted and recreated under the same name while the schedule form was open is refused; no schedule is created", {
      pinnedUid: c1.metadata.uid, liveUid: c2.metadata.uid, schedulesBefore: beforeC, schedulesAfter: schedules().length });

    // AN EDIT DURING THE DRAFT: dest-b's access is rotated on its own page
    // while the schedule form has it chosen. Same uid, same location.
    await open(schedulesRoute, "#schedule-form", "the schedules page (draft B)");
    await fillScheduleName("nightly-b");
    await chooseSource();
    await page.selectOption("#policy-create-mode", "advanced");
    await waitFor("#policy-create-cron", "the cron input");
    await page.fill("#policy-create-cron", "0 0 1 1 *");
    await page.fill("#policy-create-topics", TOPIC);
    await page.selectOption("#policy-create-destination", DEST_B);
    const genBefore = destination(DEST_B).metadata.generation;
    const rotator = await context.newPage();
    await rotator.goto(base + "#/destinations?ns=" + namespace + "&name=" + DEST_B, { waitUntil: "load" });
    await rotator.reload({ waitUntil: "load" });
    await rotator.waitForSelector("#destination-rotate-form", { timeout: 30000 });
    await rotator.selectOption("#rotate-archiveWrite-source", "existing");
    await rotator.fill("#rotate-archiveWrite-secret", grantB);
    await rotator.selectOption("#rotate-archiveRead-source", "existing");
    await rotator.fill("#rotate-archiveRead-secret", grantB);
    await rotator.click("#destination-rotate-form button[type=submit]");
    const rotated = await until("dest-b's generation moves", () => destination(DEST_B),
      (o) => o.metadata.generation > genBefore, 30);
    await rotator.close();
    check(rotated.metadata.uid === objB.metadata.uid && rotated.status.locationDigest === objB.status.locationDigest,
      "the rotation moved the destination's identity or location");
    const beforeB = schedules().length;
    await page.click("#schedule-form button[type=submit]");
    await waitFor("#schedule-detail", "the redirect after creating the dest-b schedule");
    const schedB = await until("the dest-b schedule exists", schedules, (s) => s.length === beforeB + 1, 30)
      .then((s) => s.find((x) => (x.spec.destinationRef || {}).name === DEST_B));
    check(schedB !== undefined, "no schedule names dest-b");
    result.created.push({ kind: "BackupSchedule", name: schedB.metadata.name, uid: schedB.metadata.uid, createdBy: "the page" });
    row("T3-schedule", "destination edit during a schedule draft: an access rotation (same uid, same location, generation " +
      genBefore + " -> " + rotated.metadata.generation + ") is not a refusal; a recreated destination is", {
      rotation: { generationBefore: genBefore, generationAfter: rotated.metadata.generation,
        uid: rotated.metadata.uid, locationDigest: rotated.status.locationDigest },
      schedule: { name: schedB.metadata.name, destinationRef: schedB.spec.destinationRef },
    });

    // ======================================================================= M3
    // TWO DESTINATIONS, TWO RUNS, TWO BUCKETS, AND NOTHING CROSSES.
    async function backUpNow(schedule) {
      await open(base + "#/schedules?ns=" + namespace + "&name=" + schedule.metadata.name,
        "form.run-now-form", "the " + schedule.metadata.name + " detail");
      const beforeRuns = kubeJson(["-n", namespace, "get", "backups"]).items.map((b) => b.metadata.name);
      await page.click("form.run-now-form button[type=submit]");
      const made = await until("one run of " + schedule.metadata.name,
        () => kubeJson(["-n", namespace, "get", "backups"]).items.filter((b) => beforeRuns.indexOf(b.metadata.name) === -1),
        (runs) => runs.length === 1, 30);
      result.created.push({ kind: "Backup", name: made[0].metadata.name, uid: made[0].metadata.uid,
        createdBy: "the page (Back up now)" });
      return until("run " + made[0].metadata.name + " is terminal",
        () => kubeJson(["-n", namespace, "get", "backup", made[0].metadata.name]),
        (b) => ["Succeeded", "Failed", "Cancelled"].indexOf(String((b.status || {}).phase)) !== -1, 600);
    }
    const runA = await backUpNow(schedA);
    save("m3-backup-a.json", { metadata: runA.metadata, spec: runA.spec, status: runA.status });
    check(runA.status.phase === "Succeeded", "the dest-a run did not succeed: " + JSON.stringify(runA.status).slice(0, 800));
    const runB = await backUpNow(schedB);
    save("m3-backup-b.json", { metadata: runB.metadata, spec: runB.spec, status: runB.status });
    check(runB.status.phase === "Succeeded", "the dest-b run did not succeed: " + JSON.stringify(runB.status).slice(0, 800));
    const jobEnv = (name) => {
      const job = kubeJson(["-n", namespace, "get", "job", name]);
      const env = job.spec.template.spec.containers[0].env || [];
      const out = {};
      for (const e of env) {
        out[e.name] = e.value !== undefined ? e.value
          : (((e.valueFrom || {}).secretKeyRef) ? "secret:" + e.valueFrom.secretKeyRef.name : "(ref)");
      }
      return out;
    };
    const envA = jobEnv(runA.metadata.name);
    const envB = jobEnv(runB.metadata.name);
    save("m3-job-env.json", { a: envA, b: envB });
    const listingA = listBucket("list-a-after-backups", BUCKET_A);
    const listingB = listBucket("list-b-after-backups", BUCKET_B);
    save("m3-bucket-listings.json", { [BUCKET_A]: listingA, [BUCKET_B]: listingB });
    const idA = runA.status.backupId;
    const idB = runB.status.backupId;
    check(listingA.some((k) => k.indexOf(idA) !== -1) && !listingA.some((k) => k.indexOf(idB) !== -1),
      "bucket A holds run A and not run B");
    check(listingB.some((k) => k.indexOf(idB) !== -1) && !listingB.some((k) => k.indexOf(idA) !== -1),
      "bucket B holds run B and not run A");
    check(envA.AWS_ACCESS_KEY_ID === "secret:" + grantA && envB.AWS_ACCESS_KEY_ID === "secret:" + grantB,
      "each run's credential is its own destination's: " + JSON.stringify([envA.AWS_ACCESS_KEY_ID, envB.AWS_ACCESS_KEY_ID]));
    check(envA.AWS_ENDPOINT_URL === undefined && envB.AWS_ENDPOINT_URL === undefined,
      "a destination-backed Job carries the controller's AWS_ENDPOINT_URL");
    check(envA.AWS_ALLOW_HTTP === "true" && envB.AWS_ALLOW_HTTP === "true",
      "the explicit insecureHttp transport did not reach the Jobs");
    row("M3-backup", "two destinations, two page-started runs: each run is in its own bucket only, with its own credential and its own explicit transport", {
      runs: { a: { name: runA.metadata.name, backupId: idA, destination: runA.status.destination },
        b: { name: runB.metadata.name, backupId: idB, destination: runB.status.destination } },
      jobCredentials: { a: envA.AWS_ACCESS_KEY_ID, b: envB.AWS_ACCESS_KEY_ID },
      allowHttp: { a: envA.AWS_ALLOW_HTTP, b: envB.AWS_ALLOW_HTTP },
    });

    // ======================================================================= M1
    // CONVERTING AN INLINE SCHEDULE: the location is kept, or the move is said.
    await open(schedulesRoute, "#schedule-form", "the schedules page (inline)");
    await fillScheduleName("inline-a");
    await chooseSource();
    await page.selectOption("#policy-create-mode", "advanced");
    await waitFor("#policy-create-cron", "the cron input");
    await page.fill("#policy-create-cron", "0 0 1 1 *");
    await page.fill("#policy-create-topics", TOPIC);
    await page.selectOption("#policy-create-destination", "");
    await page.click("#policy-create-inline-archive summary");
    await page.fill("#policy-create-archive", "s3://" + BUCKET_A + "/" + PREFIX_A);
    await page.fill("#policy-create-archiveSecret", grantA);
    const beforeInline = schedules().length;
    await page.click("#schedule-form button[type=submit]");
    await waitFor("#schedule-detail", "the redirect after creating the inline schedule");
    const inline = await until("the inline schedule exists", schedules, (s) => s.length === beforeInline + 1, 30)
      .then((s) => s.find((x) => x.spec.destinationRef === undefined));
    check(inline !== undefined && inline.spec.archive.url === "s3://" + BUCKET_A + "/" + PREFIX_A,
      "the inline schedule was not stored as typed");
    result.created.push({ kind: "BackupSchedule", name: inline.metadata.name, uid: inline.metadata.uid, createdBy: "the page" });
    const inlineRun = await backUpNow(inline);
    save("m1-inline-run-before.json", { metadata: inlineRun.metadata, spec: inlineRun.spec, status: inlineRun.status });
    const detail = base + "#/schedules?ns=" + namespace + "&name=" + inline.metadata.name;
    await open(detail, "form.policy-form", "the inline schedule's policy panel");
    const policySelect = "#policy-" + inline.metadata.name + "-destination";
    await page.selectOption(policySelect, DEST_B);
    await waitFor("#policy-" + inline.metadata.name + "-location-move", "the location-move warning");
    const genInline = inline.metadata.generation;
    await page.click("form.policy-form[data-name=\"" + inline.metadata.name + "\"] button[type=submit]");
    await waitForText("this edit moves the archive location and the box that says so is not ticked",
      "the unacknowledged move refusal");
    await pause(1000);
    check(kubeJson(["-n", namespace, "get", "backupschedule", inline.metadata.name]).metadata.generation === genInline,
      "an unacknowledged location move was saved");
    await shot("m1-01-move-refused");
    control("M1", "pointing an inline schedule at a destination in ANOTHER bucket is refused until the move is acknowledged; the schedule's generation does not move", {
      from: inline.spec.archive.url, to: destination(DEST_B).status.canonicalUrl, generation: genInline });
    await page.selectOption(policySelect, DEST_A);
    await waitFor("#policy-" + inline.metadata.name + "-location-same", "the location-kept note");
    await page.click("form.policy-form[data-name=\"" + inline.metadata.name + "\"] button[type=submit]");
    const converted = await until("the conversion is saved",
      () => kubeJson(["-n", namespace, "get", "backupschedule", inline.metadata.name]),
      (o) => o.metadata.generation > genInline, 30);
    check(JSON.stringify(converted.spec.destinationRef) === JSON.stringify({ name: DEST_A }) &&
      converted.spec.archive.url === "logweir-destination://" + DEST_A,
    "the conversion did not store dest-a: " + JSON.stringify(converted.spec));
    const inlineAfter = kubeJson(["-n", namespace, "get", "backup", inlineRun.metadata.name]);
    check(JSON.stringify(inlineAfter.spec) === JSON.stringify(inlineRun.spec),
      "the run created before the conversion changed");
    const convertedRun = await backUpNow(converted);
    save("m1-runs.json", { before: { metadata: inlineRun.metadata, spec: inlineRun.spec, status: inlineRun.status },
      beforeReadAgain: { spec: inlineAfter.spec, status: inlineAfter.status },
      after: { metadata: convertedRun.metadata, spec: convertedRun.spec, status: convertedRun.status } });
    const listingConv = listBucket("list-a-after-conversion", BUCKET_A);
    save("m1-bucket-a-after-conversion.json", listingConv);
    const underPrefix = (id) => listingConv.some((k) => k.indexOf(PREFIX_A + "/" + id + "/") === 0 ||
      k.indexOf(PREFIX_A + "/" + id) === 0);
    check(convertedRun.status.phase === "Succeeded", "the converted schedule's run did not succeed: " +
      JSON.stringify(convertedRun.status).slice(0, 600));
    check(underPrefix(convertedRun.status.backupId), "the converted run is not under " + BUCKET_A + "/" + PREFIX_A);
    const inlineInPrefix = inlineRun.status.phase === "Succeeded" ? underPrefix(inlineRun.status.backupId) : null;
    row("M1", "inline -> destination conversion keeps the archive location: the panel says the location is kept, the earlier run's spec is byte-identical after the save, and the next run lands under the same prefix", {
      schedule: inline.metadata.name, generation: [genInline, converted.metadata.generation],
      inlineUrl: inline.spec.archive.url, destinationCanonical: destination(DEST_A).status.canonicalUrl,
      earlierRun: { name: inlineRun.metadata.name, phase: inlineRun.status.phase, backupId: inlineRun.status.backupId,
        specUnchanged: true, inPrefix: inlineInPrefix },
      laterRun: { name: convertedRun.metadata.name, backupId: convertedRun.status.backupId,
        destination: convertedRun.status.destination, inPrefix: true },
    });

    // ================================================================== A1 / T5
    // THE RECOVERY POINT, THE WIZARD, AND TWO STORES.
    const receiptKey = (((runA.status.evidence || {}).receiptKey) || "");
    check(receiptKey.length > 0, "run A recorded no receipt key");
    const receipt = JSON.parse(catObject("cat-receipt-a", BUCKET_A, receiptKey));
    const covered = receipt.covered || {};
    check(typeof covered.from_ms === "number" && typeof covered.to_ms === "number",
      "the real receipt has no covered window: " + JSON.stringify(covered));
    save("a1-receipt-covered.json", { receiptKey: receiptKey, covered: covered, backupId: receipt.backup_id });
    const LONG = "fixture-point-deliberately-longer-than-sixty-three-characters-";
    const pointName = LONG + suffix;
    const point = create({
      apiVersion: "logweir.dev/v1alpha1", kind: "Backup",
      metadata: { name: pointName, labels: LABELS },
      spec: { archive: { url: "logweir-destination://" + DEST_A }, destinationRef: { name: DEST_A },
        deadlineSeconds: 3600, sourceRef: { name: source }, topics: [TOPIC], triggeredBy: "manual" },
    });
    const seeded = {
      phase: "Succeeded", backupId: runA.status.backupId, records: runA.status.records,
      exitCode: 0, exitReason: "ok", reason: "Ok",
      destination: runA.status.destination,
      windowCovered: { fromMs: covered.from_ms, toMs: covered.to_ms },
      conditions: [{ type: "Complete", status: "True", reason: "Ok",
        message: "fixture carrying the facts of " + runA.metadata.name,
        lastTransitionTime: new Date().toISOString().replace(/\.\d+Z$/, "Z") }],
    };
    for (let i = 0; i < 30; i += 1) {
      kube(["-n", namespace, "patch", "backup", pointName, "--subresource=status", "--type=merge",
        "-p", JSON.stringify({ status: seeded })]);
      await pause(1000);
      const seen = kubeJson(["-n", namespace, "get", "backup", pointName]).status || {};
      if (seen.phase === "Succeeded" && seen.backupId === seeded.backupId) {
        break;
      }
    }
    check(kubeJson(["-n", namespace, "get", "jobs"]).items.every((j) => j.metadata.name.indexOf(pointName) === -1),
      "the fixture point produced a Job");
    result.fixtures.push({ kind: "Backup", name: pointName, uid: point.metadata.uid,
      what: "the recovery point: status written by this harness from the REAL run " + runA.metadata.name +
        " (backupId, records, status.destination) and the windowCovered its REAL receipt gives",
      seeded: seeded });

    const route = base + "#/restore?ns=" + namespace + "&backup=" + encodeURIComponent(pointName) +
      "&uid=" + point.metadata.uid;
    // ONE STEP AT A TIME (console-ux-1, MCP-29): the wizard opens on step 1, and each control
    // below is reached on its own step with Next / Back (scripts/console-steps.mjs). The storage
    // inputs are looked for in the whole document, every step's page included.
    await open(route, "#wizard-position", "the wizard on the recovery point");
    await wizardAt(page, 1, 60);
    const inputs = await page.evaluate(() => ["#store-endpoint", "#store-region", "#store-pathStyle",
      "#store-allow-insecure", "#evidence-bucket", "#archive-secret"].filter((s) => document.querySelector(s) !== null));
    check(inputs.length === 0, "the saved point offers storage inputs to re-enter: " + inputs.join(", "));
    const targetUid = kubeJson(["-n", namespace, "get", "kafkacluster", target]).metadata.uid;
    await wizardStep(page, 4);
    await page.selectOption("#target-cluster", targetUid);
    await pause(1000);
    // THE POINT IN TIME, CHOSEN BY HAND AND SAID TO BE. The wizard defaults to
    // `windowCovered.toMs`, which the controller documents as EXCLUSIVE, and the
    // runner's own coverage check refuses it (run 6 of this harness:
    // `archive.coverage` PointInTimeAfterCoverage, "newest covered timestamp"
    // one millisecond earlier). That default is recorded as a defect in the
    // result; this journey is about storage, so it picks the last covered ms.
    const lastCovered = new Date(covered.to_ms - 1).toISOString();
    await wizardStep(page, 3);
    await page.fill("#point-in-time", lastCovered);
    await page.press("#point-in-time", "Tab");
    await pause(1000);
    result.pointInTimeChosen = { value: lastCovered, defaultWas: new Date(covered.to_ms).toISOString(),
      reason: "the default (toMs) is the exclusive end of the covered window" };
    const planOf = () => page.evaluate(() => ({
      bytes: (document.querySelector("#plan-bytes") || {}).textContent || "",
      hash: ((document.querySelector("#plan-hash-value") || {}).textContent || "").trim(),
      evidence: (document.querySelector("#evidence-destination") || {}).value || "",
    }));
    const flags = (bytes) => {
      const pick = (key) => bytes.split("\n").filter((l) => l.trim().startsWith(key + ":"))
        .map((l) => l.trim().slice(key.length + 1).trim());
      return { bucket: pick("bucket"), prefix: pick("prefix"), endpoint: pick("endpoint"),
        pathStyle: pick("path_style"), allowHttp: pick("allow_" + "http") };
    };
    const inherited = await planOf();
    const fInherited = flags(inherited.bytes);
    save("a1-plan-inherited.txt", inherited.bytes);
    check(inherited.evidence === objA.metadata.uid, "the evidence destination did not start on the point's own");
    check(fInherited.bucket.every((b) => b === "\"" + BUCKET_A + "\"") && fInherited.bucket.length === 2,
      "the inherited plan's two blocks are not dest-a's bucket: " + JSON.stringify(fInherited));
    check(JSON.stringify(fInherited.allowHttp) === JSON.stringify(["true", "true"]) &&
      JSON.stringify(fInherited.pathStyle) === JSON.stringify(["true", "true"]),
    "the inherited transport/addressing is not dest-a's: " + JSON.stringify(fInherited));
    await shot("a1-01-wizard-inherits");
    row("A1", "selecting the recovery point restores its saved settings with nothing re-entered: no storage input exists, and both plan blocks carry dest-a's bucket, endpoint, addressing and transport", {
      point: { name: pointName, uid: point.metadata.uid }, planHash: inherited.hash, blocks: fInherited,
      storageInputsOnPage: inputs,
    });

    // T1 in the plan: the TLS destination as the evidence store (step 1's control).
    await wizardStep(page, 1);
    await page.selectOption("#evidence-destination", objTls.metadata.uid);
    await pause(1000);
    const tlsPlan = await planOf();
    const fTls = flags(tlsPlan.bytes);
    save("t1-plan-evidence-tls.txt", tlsPlan.bytes);
    check(JSON.stringify(fTls.allowHttp) === JSON.stringify(["true", "false"]) &&
      JSON.stringify(fTls.pathStyle) === JSON.stringify(["true", "true"]),
    "the TLS evidence store is not path-style over TLS in the plan: " + JSON.stringify(fTls));
    check(fTls.endpoint.length === 2 && fTls.endpoint[1] === "\"" + MINIO_HTTPS + "\"",
      "the evidence endpoint is not the https origin: " + JSON.stringify(fTls.endpoint));
    row("T1", "HTTPS with path-style: the page creates a TLS + pathStyle destination on an https origin (insecureHttp refused there), the controller judges it Valid, and the wizard signs it as path_style: true with allow_http: false", {
      destination: { name: DEST_TLS, uid: objTls.metadata.uid, stored: [objTls.spec.storage.addressing,
        objTls.spec.transport.security], valid: (objTls.status.conditions || []).find((c) => c.type === "Valid") },
      requestPair: [bodyTls.storage.addressing, bodyTls.transport.security],
      planEvidenceBlock: { endpoint: fTls.endpoint[1], pathStyle: fTls.pathStyle[1], allowHttp: fTls.allowHttp[1] },
      note: "the lab MinIO serves no TLS, so no run executes against this destination; the engine's TLS path is D2 U1's",
    });

    // T5: the evidence goes to dest-b; the archive does not move.
    await page.selectOption("#evidence-destination", objB.metadata.uid);
    await pause(1000);
    const split = await planOf();
    const fSplit = flags(split.bytes);
    save("t5-plan-split.txt", split.bytes);
    check(fSplit.bucket[0] === "\"" + BUCKET_A + "\"" && fSplit.bucket[1] === "\"" + BUCKET_B + "\"",
      "archive/evidence buckets are not A/B: " + JSON.stringify(fSplit.bucket));
    // `prefix:` also names the target's topic prefix, between the two stores.
    const evidencePrefix = fSplit.prefix[fSplit.prefix.length - 1];
    check(fSplit.prefix[0] === "\"" + PREFIX_A + "\"" && evidencePrefix === "\"logweir/\"",
      "prefixes: " + JSON.stringify(fSplit.prefix));
    await shot("t5-01-wizard-evidence-dest-b");

    // READINESS, reconciled by the lab controller, followed by the page.
    async function readiness(label) {
      await wizardStep(page, 5);
      const beforePf = new Set(kubeJson(["-n", namespace, "get", "preflights"]).items.map((p) => p.metadata.uid));
      await page.click("#restore-readiness-start");
      const pf = await until(label + ": the controller records a terminal readiness check",
        () => kubeJson(["-n", namespace, "get", "preflights"]).items.find((p) => !beforePf.has(p.metadata.uid)) || null,
        (p) => p !== null && ["Completed", "Failed", "Cancelled"].indexOf(String((p.status || {}).phase)) !== -1, 300);
      // AND THE PAGE LEARNS IT BY ITSELF: no reload, no click. Before
      // PLAT-08.2 the wizard showed the create answer (pending) for ever.
      const id = pf.metadata.name;
      const learned = await page.waitForFunction((pfid) => {
        const node = document.querySelector("#preflight-" + pfid + " .preflight-head");
        return node !== null && !/\b(pending|queued|running)\b/.test(node.innerText);
      }, id, { timeout: 60000 }).then(() => true, () => false);
      const head = await page.evaluate((pfid) =>
        ((document.querySelector("#preflight-" + pfid + " .preflight-head") || {}).innerText || ""), id);
      check(learned, label + ": the page never showed the check's terminal verdict by itself; it shows: " + head);
      pf.pageHead = head;
      return pf;
    }
    const pf1 = await readiness("the first check");
    save("t5-preflight-1.json", { metadata: pf1.metadata, spec: pf1.spec, status: pf1.status });
    const res1 = pf1.status.result || {};
    const entries1 = (res1.checks || []).concat(res1.warnings || []);
    const bindings1 = entries1.find((c) => c.id === "plan.bindings");
    check(bindings1 !== undefined && bindings1.code === "PlanMatchesReferences",
      "the controller did not match the plan to BOTH saved references: " + JSON.stringify(bindings1 || res1));
    const pfRestore = ((pf1.spec.request || {}).restore) || {};
    check(pfRestore.sourceDestinationRef.name === DEST_A && pfRestore.evidenceDestinationRef.name === DEST_B,
      "the readiness check does not name dest-a/dest-b: " + JSON.stringify(pfRestore));
    await shot("t5-02-readiness");
    const verdict1 = pf1.status.reason || pf1.status.phase;
    const readyState = String((res1.state || verdict1 || "")).toLowerCase();
    row("T5", "archive/evidence separation: choosing dest-b as the evidence destination moves only the evidence block (bucket B under logweir/), and the lab controller's own plan.bindings row matches the signed plan to BOTH references", {
      plan: { archive: [fSplit.bucket[0], fSplit.prefix[0]], evidence: [fSplit.bucket[1], evidencePrefix],
        hash: split.hash },
      preflight: { name: pf1.metadata.name, uid: pf1.metadata.uid, state: res1.state || null, pageHead: pf1.pageHead,
        planBindings: bindings1, source: pfRestore.sourceDestinationRef,
        evidence: pfRestore.evidenceDestinationRef,
        blocking: (res1.checks || []).map((c) => [c.id, c.state, c.code]) },
    });

    // ===================================================================== T3
    // DESTINATION EDIT DURING THE DRAFT: dest-b is edited on its own page while
    // the wizard holds a verdict. The plan does not move; the verdict does.
    //
    // WHAT THE PAGE SHOWS, AND THE CLICK (lab-refresh-9). Until 779b7e1 every
    // draft restore check aggregated `unknown` (`approval.state` is a blocking
    // row that is `skipped`/SubjectNotCreated for any draft) and PLAT-11.2's
    // gate refused anything but `ready`, so Create was disabled after ANY
    // check and the submit-time re-read this task added could not be clicked;
    // that row was BLOCKED. The gate now admits a draft whose only non-ready
    // blocking row is the draft approval row, so below the page is CLICKED
    // after the edit and must refuse on its own re-read, sending nothing.
    // Also proved: the API's answer to the page's own question
    // (`GET .../preflights/{id}?planHash=<the hash on screen>`) before and after
    // the edit, and the plan hash on screen across the edit.
    const heldHash = (await planOf()).hash;
    const gateBefore = await page.evaluate(() => ({
      disabled: (document.querySelector("#create-restore") || {}).disabled === true,
      blocked: ((document.querySelector("#readiness-blocked") || {}).innerText || ""),
    }));
    save("t3-gate-before-edit.json", gateBefore);
    const askAbout = async (label) => {
      const response = await fetch(apiBase + "/preflights/" + encodeURIComponent(pf1.metadata.name) +
        "?planHash=" + encodeURIComponent(heldHash));
      const body = await response.text();
      check(response.ok, label + ": GET preflight answered " + response.status + ": " + body.slice(0, 300));
      const item = JSON.parse(body).item || {};
      return { stale: item.stale, applicable: item.applicable, staleReasons: item.staleReasons,
        state: item.state, planHash: (item.binding || {}).planHash };
    };
    // THE CONTROL IS ABOUT dest-b, NOT ABOUT `stale` AS A WHOLE. TrustRoster
    // is `unverifiable` to this service (it holds no verb on the kind), so the
    // held check is never `stale: false` here. Run 8 also recorded a
    // referentChanged for the recovery-point Backup on EVERY read: the
    // controller binds a Backup by uid alone (no generation) and the API
    // compared the live generation against that absence. This branch fixes the
    // API; the assertion below is that fix, live: before the edit, NO
    // referentChanged at all.
    const namesDestB = (answer) => (answer.staleReasons || []).some((r) =>
      r.reason === "referentChanged" && r.kind === "BackupDestination" && r.name === DEST_B);
    const beforeEdit = await askAbout("before the edit");
    check(!namesDestB(beforeEdit),
      "CONTROL: before the edit no stale reason names dest-b: " + JSON.stringify(beforeEdit));
    const changedBefore = (beforeEdit.staleReasons || []).filter((r) => r.reason === "referentChanged");
    check(changedBefore.length === 0,
      "before any edit the API reports a referent change (the Backup-referent defect): " +
        JSON.stringify(changedBefore));
    control("API-referent", "a recovery-point Backup bound by uid alone is not reported changed on re-read (routes/preflights.rs fix); only TrustRoster's unverifiable remains", {
      beforeEdit: beforeEdit });
    const genB = destination(DEST_B).metadata.generation;
    const editor = await context.newPage();
    await editor.goto(base + "#/destinations?ns=" + namespace + "&name=" + DEST_B, { waitUntil: "load" });
    await editor.reload({ waitUntil: "load" });
    await editor.waitForSelector("#destination-rotate-form", { timeout: 30000 });
    await editor.selectOption("#rotate-archiveWrite-source", "existing");
    await editor.fill("#rotate-archiveWrite-secret", grantB);
    await editor.selectOption("#rotate-archiveRead-source", "absent");
    await editor.click("#destination-rotate-form button[type=submit]");
    const editedB = await until("dest-b's generation moves again", () => destination(DEST_B),
      (o) => o.metadata.generation > genB, 30);
    await editor.close();
    const afterEdit = await askAbout("after the edit");
    check(afterEdit.stale === true && namesDestB(afterEdit),
      "the product API did not answer referentChanged for dest-b: " + JSON.stringify(afterEdit));
    const hashAfterEdit = (await planOf()).hash;
    check(hashAfterEdit === heldHash, "the plan hash moved on a destination edit");
    check(editedB.metadata.uid === objB.metadata.uid &&
      editedB.status.locationDigest === objB.status.locationDigest, "the edit moved dest-b's identity or location");
    save("t3-api-before-after.json", { planHash: heldHash, beforeEdit: beforeEdit, afterEdit: afterEdit,
      generation: [genB, editedB.metadata.generation] });
    await shot("t3-02-wizard-after-edit");
    control("T3", "before the edit no stale reason in the product API's answer to the page's question (held check, plan on screen) names dest-b; after dest-b is edited, referentChanged BackupDestination/dest-b appears, with the plan hash unchanged", {
      beforeEdit: beforeEdit, afterEdit: afterEdit });

    // T3-SUBMIT, AS A REAL CLICK (lab-refresh-9). Since 779b7e1 a draft whose
    // only non-ready blocking row is the draft approval row is submittable, so
    // Create is ENABLED on the held check (asserted: otherwise the refusal
    // below would be the gate's, not the re-read's). dest-b has just moved
    // under that held check; the page cannot know until it asks. The click
    // must make it ask (`confirmReadiness`), refuse on the answer naming
    // dest-b, and send NOTHING: no POST to .../restores, no Restore object.
    check(gateBefore.disabled === false,
      "T3-submit precondition: Create was not enabled on the held check before the edit, so a " +
        "refusal after it would prove nothing about the re-read: " + JSON.stringify(gateBefore));
    const restoresBeforeClick = kubeJson(["-n", namespace, "get", "restores"]).items.length;
    const postsBeforeClick = postsTo("/restores").length;
    await wizardStep(page, 6);
    await page.click("#create-restore");
    const refusalText = () => page.evaluate(() => ["#readiness-blocked", "#restore-submit-status"]
      .map((sel) => ((document.querySelector(sel) || {}).innerText || "")).join(" | "));
    const namesTheEdit = (t) => t.indexOf("no longer applies") !== -1 &&
      t.indexOf("BackupDestination/" + DEST_B) !== -1;
    let refusedWith = await refusalText();
    for (let i = 0; i < 30 && !namesTheEdit(refusedWith); i += 1) {
      await pause(1000);
      refusedWith = await refusalText();
    }
    check(namesTheEdit(refusedWith),
      "the page did not refuse the submit naming BackupDestination/" + DEST_B + " within 30 s: " +
        JSON.stringify(refusedWith).slice(0, 800));
    await pause(3000);
    const postsAfterClick = postsTo("/restores").slice(postsBeforeClick);
    const restoresAfterClick = kubeJson(["-n", namespace, "get", "restores"]).items.length;
    save("t3-submit-refused.json", { gateBeforeEdit: gateBefore, refusal: refusedWith,
      postsToRestores: postsAfterClick, restores: [restoresBeforeClick, restoresAfterClick] });
    await shot("t3-03-submit-refused");
    check(postsAfterClick.length === 0,
      "the page SENT a Restore after dest-b moved under its held check: " + JSON.stringify(postsAfterClick));
    check(restoresAfterClick === restoresBeforeClick,
      "a Restore exists after the refused submit: " + restoresBeforeClick + " -> " + restoresAfterClick);

    // THE CHECK RUN AGAIN is about the same plan and is current again.
    const pf2 = await readiness("the check run again");
    save("t3-preflight-2.json", { metadata: pf2.metadata, spec: pf2.spec, status: pf2.status });
    check(pf2.status.binding.planHash === heldHash, "the second check is about another plan");
    const res2 = pf2.status.result || {};
    const bindings2 = ((res2.checks || []).concat(res2.warnings || [])).find((c) => c.id === "plan.bindings");
    const approvalRow = (res2.checks || []).find((c) => c.id === "approval.state") || null;
    const gateAfter = await page.evaluate(() => ({
      disabled: (document.querySelector("#create-restore") || {}).disabled === true,
      blocked: ((document.querySelector("#readiness-blocked") || {}).innerText || ""),
    }));
    save("t3-gate-after-recheck.json", { gate: gateAfter, aggregate: res2.state, approvalRow: approvalRow });
    row("T3", "destination edit during a restore draft: dest-b edited on its own page (generation " + genB + " -> " +
      editedB.metadata.generation + ", same uid and location digest) moved no plan byte; the product API answered the page's held check stale naming dest-b, and a check run again is bound to the same plan hash", {
      planHash: { before: heldHash, afterEdit: hashAfterEdit, secondCheck: pf2.status.binding.planHash },
      api: { beforeEdit: beforeEdit, afterEdit: afterEdit },
      preflights: [pf1.metadata.name, pf2.metadata.name], secondBindings: bindings2 || null,
    });
    check(gateAfter.disabled === false && gateAfter.blocked === "",
      "after the check ran again Create is still refused: " + JSON.stringify(gateAfter));
    row("T3-submit", "the submit-time re-read, clicked: with Create enabled on the held check, dest-b edited under it, the click re-read the check, refused naming BackupDestination/" + DEST_B + " and sent nothing (0 POSTs to restores, no Restore); after the check ran again Create is enabled", {
      gateBeforeEdit: gateBefore, refusal: refusedWith, postsToRestores: postsAfterClick.length,
      restores: [restoresBeforeClick, restoresAfterClick], gateAfterRecheck: gateAfter,
      aggregateAfterRecheck: res2.state, approvalRow: approvalRow,
    });

    // THE RESTORE IS CREATED FROM A RE-MOUNT OF THE SAME DRAFT: leaving the
    // route and coming back (in-page navigation; a draft lives in page memory)
    // drops the held verdict -- a verdict is not a draft field -- and the kept
    // draft brings the evidence destination back BY UID, which the mount reads
    // in full before applying it. So what is created is the plan on screen,
    // with both saved references, and no check held.
    const hashRoute = route.slice(route.indexOf("#"));
    await page.evaluate((h) => { window.location.hash = h; }, "#/schedules?ns=" + namespace);
    await waitFor("#schedule-form", "the schedules route in between");
    await page.evaluate((h) => { window.location.hash = h; }, hashRoute);
    // The route carries no step, so the re-mounted wizard opens on step 1.
    await waitFor("#wizard-position", "the wizard, re-mounted on the same point");
    await wizardAt(page, 1, 60);
    await waitForText("Your unsubmitted edits to this plan", "the draft coming back");
    await pause(1500);
    const reloaded = await planOf();
    check(reloaded.evidence === objB.metadata.uid,
      "the kept draft did not bring the evidence destination back by uid: " + reloaded.evidence);
    check(reloaded.hash === heldHash, "the reloaded draft renders another plan: " + reloaded.hash + " vs " + heldHash);
    const restoresBefore = kubeJson(["-n", namespace, "get", "restores"]).items.length;
    const shown = reloaded;
    await wizardStep(page, 6);
    await page.click("#create-restore");
    const restore = await until("the wizard creates the Restore",
      () => kubeJson(["-n", namespace, "get", "restores"]).items, (items) => items.length === restoresBefore + 1, 60)
      .then((items) => items[items.length - 1]);
    result.created.push({ kind: "Restore", name: restore.metadata.name, uid: restore.metadata.uid, createdBy: "the page" });
    check(restore.spec.planBytes === shown.bytes, "the submitted plan is not the one on screen");
    check(restore.spec.sourceDestinationRef.name === DEST_A && restore.spec.evidenceDestinationRef.name === DEST_B,
      "the Restore does not name dest-a/dest-b: " + JSON.stringify([restore.spec.sourceDestinationRef, restore.spec.evidenceDestinationRef]));
    check(restore.spec.sourceArchive.url === "logweir-destination://" + DEST_A &&
      restore.spec.sourceArchive.secretRef === undefined, "the Restore carries an inline archive or credential");
    save("t5-restore.json", { metadata: restore.metadata, spec: restore.spec });
    await shot("t5-03-restore-created");
    row("T5-submit", "the wizard creates the Restore with sourceDestinationRef dest-a and evidenceDestinationRef dest-b, the sentinel archive URL and no inline credential; the plan sent is the plan shown, and a reload of the draft kept the evidence destination by uid", {
      restore: { name: restore.metadata.name, uid: restore.metadata.uid,
        sourceDestinationRef: restore.spec.sourceDestinationRef, evidenceDestinationRef: restore.spec.evidenceDestinationRef,
        sourceArchive: restore.spec.sourceArchive }, planHash: heldHash,
    });
    // ===================================================================== M3
    // THE RESTORE RUNS: archive read from bucket A, evidence written to bucket B.
    const approvalName = restore.spec.approvalRef.name;
    const work = join(WORK_DIR, "approval");
    mkdirSync(work, { recursive: true, mode: 0o700 });
    writeFileSync(join(work, "plan.json"), restore.spec.planBytes);
    const minted = spawnSync(CLI_BIN, ["drill", "approve", "--spec", join(work, "plan.json"),
      "--key", APPROVER_KEY, "--approver", "plat08-2", "--ticket", "PLAT-08.2",
      "--subject-kind", "Restore", "--out", join(work, "approval.json")],
    { encoding: "utf8", timeout: 60000 });
    check(minted.status === 0, "logweir drill approve failed: " + String(minted.stderr).slice(0, 800));
    const planHash = "sha256:" + createHash("sha256").update(restore.spec.planBytes).digest("hex");
    create({ apiVersion: "logweir.dev/v1alpha1", kind: "Approval",
      metadata: { name: approvalName, labels: LABELS },
      spec: { approvalBytes: readFileSync(join(work, "approval.json"), "utf8"),
        sidecarBytes: readFileSync(join(work, "approval.sig"), "utf8"),
        planHash: planHash, subjectRef: { kind: "Restore", name: restore.metadata.name } } });
    result.created.push({ kind: "Approval", name: approvalName, createdBy: "kubectl, signed by logweir drill approve" });
    const approval = await until("the Approval verifies", () => kubeJson(["-n", namespace, "get", "approval", approvalName]),
      (o) => (o.status || {}).verified !== undefined && (o.status || {}).verified !== null, 180);
    save("m3-approval.json", { metadata: approval.metadata, status: approval.status });
    if (approval.status.verified !== true) {
      blocked("M3-restore", "the Approval did not verify", { status: approval.status });
    } else {
      const done = await until("the Restore is terminal", () => kubeJson(["-n", namespace, "get", "restore", restore.metadata.name]),
        (o) => ["Succeeded", "Failed", "Cancelled"].indexOf(String((o.status || {}).phase)) !== -1, 900);
      save("m3-restore-terminal.json", { metadata: done.metadata, status: done.status });
      const env = done.status.jobRef ? jobEnv(done.status.jobRef.name) : {};
      save("m3-restore-job-env.json", env);
      const evidenceA = listBucket("list-a-after-restore", BUCKET_A).filter((k) => k.indexOf("logweir/drills/") === 0);
      const evidenceB = listBucket("list-b-after-restore", BUCKET_B).filter((k) => k.indexOf("logweir/drills/") === 0);
      save("m3-evidence-listings.json", { [BUCKET_A]: evidenceA, [BUCKET_B]: evidenceB });
      const scorecard = String(((done.status.evidence || {}).scorecardKey) || "");
      if (done.status.exitCode === 0 && scorecard.length > 0) {
        check(evidenceB.some((k) => k === scorecard), "the scorecard is not in bucket B: " + scorecard);
        check(evidenceA.length === 0, "drill evidence landed in the ARCHIVE bucket: " + JSON.stringify(evidenceA));
        check(env.AWS_ACCESS_KEY_ID === "secret:" + grantA, "the archive credential is not dest-a's: " + env.AWS_ACCESS_KEY_ID);
        check(env.LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID === "secret:" + grantB,
          "the evidence credential is not dest-b's: " + env.LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID);
        row("M3-restore", "two destinations in one restore: the wizard's Restore read the archive from bucket A with dest-a's credential and wrote its signed scorecard to bucket B with dest-b's, and nothing under bucket A's logweir/drills/", {
          restore: { name: done.metadata.name, phase: done.status.phase, exitCode: done.status.exitCode,
            evidence: done.status.evidence }, jobCredentials: { archive: env.AWS_ACCESS_KEY_ID,
            evidence: env.LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID }, evidenceListings: { a: evidenceA, b: evidenceB },
        });
      } else {
        blocked("M3-restore", "the Restore was admitted but did not finish with exit 0", {
          phase: done.status.phase, exitCode: done.status.exitCode, exitReason: done.status.exitReason,
          reason: done.status.reason, message: String(done.status.message || "").slice(0, 600),
          conditions: done.status.conditions, jobCredentials: { archive: env.AWS_ACCESS_KEY_ID,
            evidence: env.LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID }, evidenceListings: { a: evidenceA, b: evidenceB },
        });
      }
    }
  } finally {
    try {
      save("responses.json", bodies.map((b) => ({ url: b.url, status: b.status, body: b.body.slice(0, 20000) })));
      save("requests.json", result.requests);
      save("api.log", apiLog.join(""));
      for (const kind of ["backupdestinations", "backupschedules", "backups", "preflights", "restores", "approvals"]) {
        const out = kube(["-n", namespace, "get", kind, "-o", "json"], { expected: [0, 1] });
        if (out.status === 0) {
          save("kubectl-" + kind + ".json", out.stdout);
        }
      }
      const labLog = kube(["-n", LAB, "logs", result.controller ? result.controller.pod : "x", "--since=2h"],
        { expected: [0, 1], timeout: 60000 }).stdout;
      save("lab-controller-log-this-namespace.txt",
        labLog.split("\n").filter((l) => l.indexOf(namespace) !== -1).join("\n"));
    } catch (unsaved) {
      process.stderr.write("could not save everything: " + unsaved.message + "\n");
    }
    try {
      if (page !== null) {
        await page.context().browser().close();
      }
    } catch (closed) {
      // already gone
    }
    stopApi();
  }
}

/** No credential value anywhere this run wrote. */
function redactionSweep() {
  const hits = [];
  const walk = (dir) => {
    for (const name of readdirSync(dir)) {
      const at = join(dir, name);
      if (statSync(at).isDirectory()) {
        walk(at);
      } else if (!name.endsWith(".png")) {
        const body = readFileSync(at, "utf8");
        for (const secret of SECRETS) {
          if (body.indexOf(secret) !== -1) {
            hits.push(at);
          }
        }
      }
    }
  };
  walk(ARTIFACTS);
  result.redaction = { swept: ARTIFACTS, secretValues: SECRETS.length, hits: hits };
  return hits;
}

async function cleanup() {
  if (process.env.UI_E2E_KEEP === "1") {
    result.cleanup.push({ kept: namespace });
    return;
  }
  if (storesMade) {
    try {
      const log = mcJob("remove-stores",
        "mc rb --force \"p082/$BUCKET_A\" || true; mc rb --force \"p082/$BUCKET_B\" || true; " +
        "mc admin user rm p082 \"$USER_A\" >/dev/null || true; mc admin user rm p082 \"$USER_B\" >/dev/null || true; " +
        "mc admin policy rm p082 \"$USER_A-rw\" >/dev/null || true; mc admin policy rm p082 \"$USER_B-rw\" >/dev/null || true; " +
        "if mc stat \"p082/$BUCKET_A\" >/dev/null 2>&1 || mc stat \"p082/$BUCKET_B\" >/dev/null 2>&1; " +
        "then echo buckets-still-there; else echo buckets-gone; fi; " +
        "if mc admin user info p082 \"$USER_A\" >/dev/null 2>&1 || mc admin user info p082 \"$USER_B\" >/dev/null 2>&1; " +
        "then echo users-still-there; else echo users-gone; fi",
        userEnv());
      result.cleanup.push({ stores: log });
    } catch (failed) {
      result.cleanup.push({ storesError: failed.message });
    }
  }
  const ns = kube(["get", "namespace", namespace, "-o", "json"], { expected: [0, 1] });
  if (ns.status !== 0) {
    result.cleanup.push({ namespace: namespace, state: "absent" });
    return;
  }
  const object = JSON.parse(ns.stdout);
  if (object.metadata.uid !== result.namespaceUid ||
    (object.metadata.labels || {})["logweir.dev/test-owner"] !== OWNER) {
    result.cleanup.push({ namespace: namespace, refused: "uid or owner label does not match; not deleted" });
    return;
  }
  kube(["delete", "namespace", namespace, "--wait=true", "--timeout=300s"], { timeout: 320000, expected: [0, 1] });
  const gone = kube(["get", "namespace", namespace], { expected: [0, 1] }).status !== 0;
  result.cleanup.push({ namespace: namespace, uid: result.namespaceUid, deleted: gone });
  rmSync(WORK_DIR, { recursive: true, force: true });
}

let failure = null;
try {
  await main();
} catch (error) {
  failure = error;
  result.error = String(error && error.stack || error);
}
try {
  await cleanup();
} catch (error) {
  result.cleanup.push({ error: String(error.message) });
}
result.finishedAt = new Date().toISOString();
const hits = existsSync(ARTIFACTS) ? redactionSweep() : [];
save("live.json", result);
process.stderr.write("\n== " + result.rows.length + " row(s) PASS, " + result.controls.length +
  " control(s), " + result.blocked.length + " blocked; redaction hits: " + hits.length +
  "; result: " + join(ARTIFACTS, "live.json") + "\n");
if (failure !== null) {
  process.stderr.write(String(failure.stack || failure) + "\n");
}
process.exit(failure === null && hits.length === 0 ? (result.blocked.length === 0 ? 0 : 2) : 1);
