// PLAT-15.2 live acceptance harness: a disaster restore from a connected
// archive, with no Backup object anywhere in the namespace that restores it,
// and CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW on a schedule's detail.
//
// The sibling of `scripts/plat10-ui-e2e.mjs`, whose launcher and fixtures this
// reuses in shape: `logweir-api` in localAdmin mode on a loopback port, pointed
// at this worktree's own `ui/` and at the two namespaces this run created, with
// a real Chromium driven against it. Every Backup, Preflight, catalog sync and
// Restore below is reconciled by the ONE controller on this cluster -- the
// shared lab's `weirkeeper` in `logweir-scram-local`, observed and never changed.
// THIS HARNESS WRITES NO STATUS (`kube()` refuses `--subresource`).
//
// TWO NAMESPACES, AND THE SECOND ONE IS THE POINT.
//
//   * `<prefix>src-<stamp>` is the installation that WROTE the archive: a saved
//     source connection, a destination on this run's own MinIO bucket, a
//     schedule, and one run made with the console's "Back up now". The lab
//     controller verifies nothing for a destination-backed run whose evidence
//     grant is `ArchiveReadGrant` (this build has no evidence-fetch Job), so
//     the run is `Succeeded` with verdict `NotAttempted` and NO
//     `windowCovered` -- which is exactly CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW.
//     Its schedule detail offers no restore until a catalog lists the receipt,
//     and then offers one from the catalog's window (journey 2).
//   * `<prefix>dr-<stamp>` is the DISASTER: a namespace that never held a
//     Backup, a BackupSchedule or a source KafkaCluster. Its operator creates a
//     read-only MinIO user's Secret with kubectl, a destination on the console
//     that names that Secret (and an evidence-writer Secret) by NAME, connects
//     the archive on the console, chooses a catalog-verified point, checks
//     readiness, creates the Restore, records the Approval on the console, and
//     the restored records are compared with the source's (journeys 3-7).
//
// NEGATIVE CONTROLS, each an assertion that the product REFUSED something:
// the schedule detail offers no restore before a catalog lists the run; a
// point id the catalog does not list is refused by the wizard with no plan; the
// product API refuses a readiness check naming both a Backup and a catalog
// point; and a Restore whose signed plan binds a receipt digest the archive
// does not hold is refused by the RUNNER before any data moves (exit 3,
// PointBindingMismatch) -- its mapped topic is proved absent from the target.
//
// THE APPROVER KEY. `TrustRoster/default.spec.approverKeys[0]` is the lab's
// rotated key; its private half is `$HOME/.logweir-lab/scram-e2e/approver.pem`
// (0600). It is passed to `logweir drill approve` by PATH and never read,
// printed or copied by this process; the two documents that command writes are
// public and are pasted into the console's approval form, which is the product
// path an approver uses.
//
// Dependencies: Node.js, kubectl, built `logweir-api` and `logweir`, Playwright:
//   NODE_PATH="$(npm root -g)" node scripts/plat15-2-ui-e2e.mjs
//
// Environment (all optional): UI_E2E_OWNER (plat15-2), UI_E2E_PREFIX
// (lw-p152-), UI_E2E_API_BIN, UI_E2E_LOGWEIR_BIN, UI_E2E_UI_DIR,
// UI_E2E_ARTIFACTS, UI_E2E_KEEP ("1" keeps both namespaces), UI_E2E_KUBECTL,
// UI_E2E_APPROVER_KEY.

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes, createHash } from "node:crypto";
import { homedir } from "node:os";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = process.env.UI_E2E_KUBECTL || "kubectl";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UI_DIR = process.env.UI_E2E_UI_DIR || join(REPO, "ui");
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "release", "logweir-api");
const LOGWEIR_BIN = process.env.UI_E2E_LOGWEIR_BIN || join(REPO, "target", "release", "logweir");
const APPROVER_KEY = process.env.UI_E2E_APPROVER_KEY ||
  join(homedir(), ".logweir-lab", "scram-e2e", "approver.pem");
const OWNER = process.env.UI_E2E_OWNER || "plat15-2";
const NAMESPACE_PREFIX = process.env.UI_E2E_PREFIX || "lw-p152-";
const OWNER_LABEL = "logweir.dev/test-owner=" + OWNER;
const LABELS = { "logweir.dev/test-owner": OWNER };
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z").toLowerCase();
const SRC = NAMESPACE_PREFIX + "src-" + stamp;
const DR = NAMESPACE_PREFIX + "dr-" + stamp;
const suffix = randomBytes(3).toString("hex");
const ARTIFACTS_ROOT = process.env.UI_E2E_ARTIFACTS ||
  "/tmp/logweir-roadmap-run/claude/artifacts/plat15-2";
const ARTIFACTS = join(ARTIFACTS_ROOT, stamp);
const WORK_DIR = join("/tmp", "plat15-2-live-" + stamp);

const LAB = "logweir-scram-local";
const LAB_KAFKA = "kafka-source." + LAB + ".svc.cluster.local:9096";
const LAB_TARGET = "kafka-target." + LAB + ".svc.cluster.local:9096";
const LAB_MINIO = "minio." + LAB + ".svc:9000";
const BUCKET = "lw-p152-" + stamp.replace(/[^a-z0-9]/g, "");
const ARCHIVE_PREFIX = "archive";
const SOURCE_TOPIC = "orders";
const STORE_SECRET = "p152-object-store";
const READER_SECRET = "p152-archive-reader";
const WRITER_SECRET = "p152-evidence-writer";
const READER_USER = "p152-reader-" + suffix;
const READER_POLICY = "p152-readonly-" + suffix;
const DESTINATION = "archive";
const VERIFIED_DESTINATION = "archive-verified";
const VERIFIED_PREFIX = "verified";
const CATALOG = "archive";
const RESTORE_PREFIX = "p152-" + suffix + "-";
const TAMPERED_PREFIX = "p152-" + suffix + "-forged-";
// lab-refresh-9's rows (the PLAT-15.2 review's "rows the next lab refresh must run").
const UNSIGNED_PREFIX = "p152-" + suffix + "-unsigned-";
const REVOKED_PREFIX = "p152-" + suffix + "-revoked-";
const TRUST_POLICY = "p152-" + suffix;

const result = {
  harness: "scripts/plat15-2-ui-e2e.mjs",
  tasks: ["PLAT-15.2", "CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW"],
  kubeContext: KUBE_CONTEXT,
  owner: OWNER,
  namespaces: { writer: SRC, disaster: DR },
  lab: { release: LAB, kafka: LAB_KAFKA, target: LAB_TARGET, minio: LAB_MINIO,
    usedReadOnly: "the lab's brokers and MinIO are addressed; nothing in " + LAB + " is changed",
    bucket: BUCKET },
  revision: null,
  apiBinarySha256: null,
  logweirBinarySha256: null,
  startedAt: new Date().toISOString(),
  mode: "console (logweir-api, localAdmin, loopback)",
  statusWrites: 0,
  blocked: [],
  journeys: [],
  controls: [],
  fixtures: [],
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

function blocked(journey, detail) {
  result.blocked.push(Object.assign({ journey: journey }, detail || {}));
  process.stderr.write("== BLOCKED: " + journey + "\n");
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
  check(!args.some((a) => String(a).indexOf("--subresource") !== -1),
    "this harness never writes a status subresource: " + args.join(" "));
  const opts = options || {};
  const done = spawnSync(KUBECTL, ["--context", KUBE_CONTEXT].concat(args), {
    encoding: "utf8",
    input: opts.input,
    timeout: opts.timeout || 60000,
    maxBuffer: 16 * 1024 * 1024,
  });
  const expected = opts.expected || [0];
  if (!expected.includes(done.status)) {
    throw new Error(KUBECTL + " " + args.join(" ") + " exited " + done.status + ": " +
      String(done.stderr || "").trim().slice(0, 1500));
  }
  return done;
}

function kubeJson(args) {
  return JSON.parse(kube(args.concat(["-o", "json"])).stdout);
}

function commandText(command, args) {
  const done = spawnSync(command, args, { encoding: "utf8", timeout: 60000 });
  return done.status === 0 ? String(done.stdout || "").trim() : null;
}

function pause(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function artifact(name, value) {
  const at = join(ARTIFACTS, name);
  mkdirSync(dirname(at), { recursive: true });
  writeFileSync(at, typeof value === "string" ? value : JSON.stringify(value, null, 2) + "\n");
  return at;
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

function owned(name, ns) {
  return { name: name, namespace: ns, labels: LABELS };
}

function create(ns, object) {
  kube(["-n", ns, "create", "-f", "-"], { input: JSON.stringify(object) });
}

// ------------------------------------------------------------- the browser

async function shot(page, name) {
  const at = join(ARTIFACTS, name + ".png");
  await page.screenshot({ path: at, fullPage: true });
  result.screenshots.push(at);
  return at;
}

async function text(page) {
  return (await page.evaluate(() => document.body.innerText)).toLowerCase();
}

async function waitForSelector(page, selector, label, timeout) {
  try {
    await page.waitForSelector(selector, { timeout: timeout || 30000 });
  } catch (never) {
    throw new Error(label + ": " + selector + " never appeared. Saw:\n" +
      (await text(page)).slice(0, 2500));
  }
}

/** Opens `url` as a FRESH page load -- by way of about:blank, because a goto to
 *  the hash the page is already on is a same-document navigation that
 *  re-reads nothing, and a journey that re-opens a route to see what changed
 *  must see the server's answer now and not the render it already had. */
async function openRoute(page, url, selector, label) {
  await page.goto("about:blank", { waitUntil: "load", timeout: 30000 });
  await page.goto(url, { waitUntil: "load", timeout: 30000 });
  try {
    await page.waitForSelector(selector, { timeout: 20000 });
    return 0;
  } catch (notYet) {
    await page.reload({ waitUntil: "load", timeout: 30000 });
    await waitForSelector(page, selector, label);
    return 1;
  }
}

// ------------------------------------------------------------- the service

let api = null;
const apiLog = [];

async function startApi(port) {
  mkdirSync(WORK_DIR, { recursive: true, mode: 0o700 });
  const cursorKey = join(WORK_DIR, "cursor.key");
  writeFileSync(cursorKey, randomBytes(32), { mode: 0o600 });
  const config = [
    "mode: localAdmin",
    "listen: \"127.0.0.1:" + port + "\"",
    "publicOrigin: \"http://127.0.0.1:" + port + "\"",
    "uiDirectory: " + UI_DIR,
    "localAdmin:",
    "  subject: admin",
    "  displayName: Local administrator",
    "namespaces: [" + SRC + ", " + DR + "]",
    "kubernetes:",
    "  source: kubeconfig",
    "  context: " + KUBE_CONTEXT,
    "cursorKeyFile: " + cursorKey,
    "",
  ].join("\n");
  const configPath = join(WORK_DIR, "config.yaml");
  writeFileSync(configPath, config);
  artifact("config.yaml", config);
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
  throw new Error("logweir-api never answered /healthz. Log:\n" + apiLog.join(""));
}

function stopApi() {
  if (api !== null && api.exitCode === null) {
    api.kill("SIGTERM");
  }
}

// ------------------------------------------------------------- the fixtures

/** Copies one of the lab's Secrets into `ns` under `ownName`; the value is
 *  never printed, logged or written to an artifact. */
function copyLabSecret(labName, ns, ownName) {
  const source = kubeJson(["-n", LAB, "get", "secret", labName]);
  create(ns, { apiVersion: "v1", kind: "Secret", type: source.type || "Opaque",
    metadata: owned(ownName, ns), data: source.data });
  result.fixtures.push({ namespace: ns, kind: "Secret", name: ownName,
    copiedFrom: LAB + "/" + labName, note: "value never printed, read or logged" });
}

function seedConnection(ns, name, servers, role, labSecret) {
  copyLabSecret(labSecret, ns, name + "-scram");
  create(ns, {
    apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster", metadata: owned(name, ns),
    spec: { bootstrapServers: [servers], role: role,
      auth: { mode: "scramSha512", tls: false, username: "scram-user",
        secretRef: { name: name + "-scram" } } },
  });
  result.fixtures.push({ namespace: ns, kind: "KafkaCluster", name: name, role: role,
    bootstrapServers: servers, createdBy: "kubectl" });
}

/** The installation prerequisites a namespace needs to RUN anything
 *  (docs/install.md section 4): the no-verb runner account and the signer. These
 *  are the installation's, not the lost backup configuration's. */
function namespacePrerequisites(ns) {
  create(ns, { apiVersion: "v1", kind: "ServiceAccount", metadata: owned("logweir-runner", ns),
    automountServiceAccountToken: false });
  copyLabSecret("logweir-signing-key", ns, "logweir-signing-key");
}

let mcJobs = 0;
/** One `minio/mc` Job in `ns` with the owned copy of the lab's object-store
 *  credential (and, when given, the reader Secret's two values as env). Its log
 *  is the evidence; the alias line is silenced. */
function mcJob(ns, step, script, extraEnv) {
  mcJobs += 1;
  const name = "p152-mc-" + String(mcJobs) + "-" + step;
  create(ns, {
    apiVersion: "batch/v1", kind: "Job", metadata: owned(name, ns),
    spec: {
      backoffLimit: 0, ttlSecondsAfterFinished: 3600,
      template: {
        metadata: { labels: LABELS },
        spec: {
          restartPolicy: "Never",
          containers: [{
            name: "mc", image: "minio/mc:latest", imagePullPolicy: "IfNotPresent",
            command: ["/bin/sh", "-ec"],
            args: ["mc alias set adm \"$S3_ENDPOINT\" \"$AWS_ACCESS_KEY_ID\" " +
              "\"$AWS_SECRET_ACCESS_KEY\" >/dev/null; " + script],
            env: [
              { name: "S3_ENDPOINT", value: "http" + "://" + LAB_MINIO },
              { name: "S3_BUCKET", value: BUCKET },
              { name: "AWS_ACCESS_KEY_ID", valueFrom: { secretKeyRef:
                { name: STORE_SECRET, key: "access-key-id" } } },
              { name: "AWS_SECRET_ACCESS_KEY", valueFrom: { secretKeyRef:
                { name: STORE_SECRET, key: "secret-access-key" } } },
            ].concat(extraEnv || []),
          }],
        },
      },
    },
  });
  const waited = kube(["-n", ns, "wait", "--for=condition=complete", "job/" + name,
    "--timeout=170s"], { expected: [0, 1], timeout: 180000 });
  const logs = kube(["-n", ns, "logs", "job/" + name], { expected: [0, 1] }).stdout;
  artifact("mc/" + name + ".log", logs);
  check(waited.status === 0, "the object-store step " + name + " did not complete:\n" + logs);
  return logs.trim();
}

/** A destination on this run's bucket. `evidenceRead` absent is the
 *  deterministic way to a run the controller CANNOT verify -- `NotAttempted`,
 *  no window -- whatever the lab's build; `ArchiveReadGrant` lets a build with
 *  the evidence-fetch Job verify it (`Valid`, with `windowCovered`). */
function destinationObject(ns, name, prefix, readSecret, writeSecret, evidenceRead) {
  const access = {
    archiveWrite: { mode: "SecretKeys", secret: { name: writeSecret } },
    archiveRead: { mode: "SecretKeys", secret: { name: readSecret } },
  };
  if (evidenceRead) {
    access.evidenceRead = { mode: "ArchiveReadGrant" };
  }
  return {
    apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
    metadata: owned(name, ns),
    spec: {
      description: "PLAT-15.2 live journey: this run's own bucket on the lab's MinIO",
      storage: { provider: "S3", bucket: BUCKET, prefix: prefix, addressing: "PathStyle",
        endpoint: "http" + "://" + LAB_MINIO },
      transport: { security: "InsecureHTTP" },
      access: access,
    },
  };
}

async function waitFor(label, attempts, intervalMs, probe) {
  let last = null;
  for (let i = 0; i < attempts; i += 1) {
    last = await probe();
    if (last !== null && last !== undefined && last !== false) {
      return last;
    }
    await pause(intervalMs);
  }
  throw new Error(label + " did not happen in time");
}

async function waitForCatalogPoints(ns, minimum) {
  return waitFor("catalog " + ns + "/" + CATALOG + " publishing a view with points", 240, 2000,
    () => {
      const catalog = kubeJson(["-n", ns, "get", "recoverycatalog", CATALOG]);
      const st = catalog.status || {};
      const synced = (st.conditions || []).find((c) => c.type === "Synced");
      const pages = Array.isArray(st.pages) ? st.pages : [];
      const count = pages.reduce((n, p) => n + (p.count || 0), 0);
      if (synced !== undefined && synced.status === "True" && count >= minimum) {
        return catalog;
      }
      return null;
    });
}

function brokerPod(app) {
  const pods = kubeJson(["-n", LAB, "get", "pods", "-l", "app=" + app]).items
    .filter((p) => p.status.phase === "Running");
  check(pods.length === 1, "exactly one running " + app + " pod");
  return pods[0].metadata.name;
}

/** Every record VALUE in a topic, in offset order, read inside the broker pod
 *  over its own plaintext listener. Read-only. */
function topicValues(app, topic, expected) {
  const pod = brokerPod(app);
  const done = kube(["-n", LAB, "exec", pod, "--", "/opt/kafka/bin/kafka-console-consumer.sh",
    "--bootstrap-server", "localhost:9092", "--topic", topic, "--from-beginning",
    "--max-messages", String(expected), "--timeout-ms", "30000"],
  { expected: [0, 1], timeout: 90000 });
  return String(done.stdout || "").split("\n").filter((line) => line.length > 0);
}

function topicExists(app, topic) {
  const pod = brokerPod(app);
  const listed = kube(["-n", LAB, "exec", pod, "--", "/opt/kafka/bin/kafka-topics.sh",
    "--bootstrap-server", "localhost:9092", "--list"], { timeout: 60000 }).stdout;
  return listed.split("\n").map((t) => t.trim()).includes(topic);
}

function endOffset(app, topic) {
  const pod = brokerPod(app);
  const done = kube(["-n", LAB, "exec", pod, "--", "/opt/kafka/bin/kafka-get-offsets.sh",
    "--bootstrap-server", "localhost:9092", "--topic", topic], { timeout: 60000 });
  return String(done.stdout || "").trim();
}

function sha256(text) {
  return createHash("sha256").update(text).digest("hex");
}

// ----------------------------------------------------------------- the run

async function main() {
  mkdirSync(ARTIFACTS, { recursive: true });
  assertSafeNamespace(SRC);
  assertSafeNamespace(DR);
  result.revision = commandText("git", ["-C", REPO, "rev-parse", "HEAD"]);
  result.apiBinarySha256 = commandText("shasum", ["-a", "256", API_BIN]);
  result.logweirBinarySha256 = commandText("shasum", ["-a", "256", LOGWEIR_BIN]);
  check(result.revision !== null && result.apiBinarySha256 !== null &&
    result.logweirBinarySha256 !== null, "the harness could not record what it runs");
  check(existsSync(APPROVER_KEY), "the lab approver key is at " + APPROVER_KEY);
  check((statSync(APPROVER_KEY).mode & 0o077) === 0, "the approver key must be 0600");

  const labPods = kubeJson(["-n", LAB, "get", "pods", "-l",
    "app.kubernetes.io/component=control-plane"]).items;
  check(labPods.length === 1, "expected exactly one lab controller pod");
  const others = kubeJson(["get", "deployments", "-A"]).items.filter((d) =>
    String(d.metadata.name).indexOf("weirkeeper") !== -1 && d.metadata.namespace !== LAB);
  check(others.length === 0, "another weirkeeper deployment exists");
  result.controller = { namespace: LAB, pod: labPods[0].metadata.name,
    image: labPods[0].spec.containers[0].image,
    imageID: (labPods[0].status.containerStatuses || [{}])[0].imageID };
  const roster = kubeJson(["get", "trustroster", "default"]);
  result.trust = {
    source: "TrustRoster/default (no TrustPolicy exists: legacy-roster-v1)",
    trustPolicies: kubeJson(["get", "trustpolicies"]).items.length,
    approverKeyIds: (roster.spec.approverKeys || []).map((k) => k.keyId),
    signingKeyIds: (roster.spec.signingKeys || []).map((k) => k.keyId),
  };

  for (const ns of [SRC, DR]) {
    check(kube(["get", "namespace", ns], { expected: [0, 1] }).status !== 0,
      "refusing to reuse an existing namespace " + ns);
    kube(["create", "namespace", ns]);
    kube(["label", "namespace", ns, OWNER_LABEL]);
    const made = kubeJson(["get", "namespace", ns]);
    result.created.push({ kind: "Namespace", name: ns, uid: made.metadata.uid });
  }
  result.sourceTopic = { topic: SOURCE_TOPIC, endOffsets: endOffset("kafka-source", SOURCE_TOPIC) };

  // ---- the writer installation (SRC) ---------------------------------------
  namespacePrerequisites(SRC);
  seedConnection(SRC, "orders-source", LAB_KAFKA, "source", "source-scram");
  copyLabSecret("logweir-s3", SRC, STORE_SECRET);
  const made = mcJob(SRC, "make-bucket", "mc mb \"adm/$S3_BUCKET\"; mc ls adm | while read -r " +
    "line; do case \"$line\" in *\" $S3_BUCKET/\") echo \"bucket present: $S3_BUCKET\";; esac; done");
  check(made.indexOf("bucket present: " + BUCKET) !== -1, "the owned bucket was not created");
  result.bucket = { name: BUCKET, endpoint: LAB_MINIO, createdBy: "this run (mc mb)", log: made };
  create(SRC, destinationObject(SRC, DESTINATION, ARCHIVE_PREFIX, STORE_SECRET, STORE_SECRET,
    false));
  result.fixtures.push({ namespace: SRC, kind: "BackupDestination", name: DESTINATION,
    bucket: BUCKET, prefix: ARCHIVE_PREFIX, evidenceRead: "absent", createdBy: "kubectl" });
  create(SRC, destinationObject(SRC, VERIFIED_DESTINATION, VERIFIED_PREFIX, STORE_SECRET,
    STORE_SECRET, true));
  result.fixtures.push({ namespace: SRC, kind: "BackupDestination", name: VERIFIED_DESTINATION,
    bucket: BUCKET, prefix: VERIFIED_PREFIX, evidenceRead: "ArchiveReadGrant", createdBy: "kubectl" });
  seedConnection(SRC, "restore-target", LAB_TARGET, "target", "target-scram");

  // ---- the disaster namespace's credentials, created by an operator --------
  // A READ-ONLY MinIO user of this run's own, scoped to this bucket. Its secret
  // half is minted here, handed to the mc Job and the Secret through stdin
  // only, and never printed.
  //
  // HEX, NEVER BASE64URL. A base64url value starts with `-` one time in 32,
  // and `mc admin user add adm <user> <secret>` then parses the secret as an
  // unknown FLAG: the step fails, and mc's usage error prints the value into
  // the Job log this harness saves (seen live, 2026-09-23). Hex has no `-`.
  const readerSecret = randomBytes(24).toString("hex");
  const policy = JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow",
    Action: ["s3:GetObject", "s3:ListBucket"],
    Resource: ["arn:aws:s3:::" + BUCKET, "arn:aws:s3:::" + BUCKET + "/*"] }] });
  create(SRC, { apiVersion: "v1", kind: "Secret", metadata: owned("p152-reader-mint", SRC),
    stringData: { user: READER_USER, secret: readerSecret, policy: policy } });
  const userLog = mcJob(SRC, "reader-user",
    "printf '%s' \"$POLICY\" > /tmp/ro.json; " +
    "mc admin policy create adm \"$POLICY_NAME\" /tmp/ro.json >/dev/null && " +
    "mc admin user add adm \"$READER_USER\" \"$READER_SECRET\" >/dev/null && " +
    "mc admin policy attach adm \"$POLICY_NAME\" --user \"$READER_USER\" >/dev/null && " +
    "echo reader-created; " +
    "mc alias set ro \"$S3_ENDPOINT\" \"$READER_USER\" \"$READER_SECRET\" >/dev/null; " +
    "mc ls ro/\"$S3_BUCKET\" >/dev/null && echo reader-can-list; " +
    "if echo x | mc pipe ro/\"$S3_BUCKET\"/p152-write-probe >/dev/null 2>&1; then " +
    "echo reader-CAN-WRITE; else echo reader-cannot-write; fi",
  [
    { name: "POLICY_NAME", value: READER_POLICY },
    { name: "READER_USER", valueFrom: { secretKeyRef: { name: "p152-reader-mint", key: "user" } } },
    { name: "READER_SECRET", valueFrom: { secretKeyRef: { name: "p152-reader-mint", key: "secret" } } },
    { name: "POLICY", valueFrom: { secretKeyRef: { name: "p152-reader-mint", key: "policy" } } },
  ]);
  check(userLog.indexOf("reader-created") !== -1 && userLog.indexOf("reader-can-list") !== -1,
    "the read-only MinIO user was not created: " + userLog);
  check(userLog.indexOf("reader-cannot-write") !== -1, "the reader user can WRITE: " + userLog);
  result.readerUser = { user: READER_USER, policy: READER_POLICY, log: userLog,
    note: "secret half minted in-process, never printed or written to an artifact" };
  control("the archive reader credential cannot write the bucket", { log: userLog });

  namespacePrerequisites(DR);
  seedConnection(DR, "restore-target", LAB_TARGET, "target", "target-scram");
  create(DR, { apiVersion: "v1", kind: "Secret", metadata: owned(READER_SECRET, DR),
    stringData: { "access-key-id": READER_USER, "secret-access-key": readerSecret } });
  copyLabSecret("logweir-s3", DR, WRITER_SECRET);
  result.fixtures.push({ namespace: DR, kind: "Secret", name: READER_SECRET,
    createdBy: "kubectl (an operator's step)", note: "read-only MinIO user of this run" });

  const port = await freePort();
  result.port = port;
  await startApi(port);
  const base = "http://127.0.0.1:" + port + "/ui/";
  const apiRoot = "http://127.0.0.1:" + port + "/api/v1/namespaces/";
  async function apiCall(method, ns, path, body, key) {
    const headers = { "Content-Type": "application/json", Origin: "http://127.0.0.1:" + port };
    if (key !== undefined) {
      headers["Idempotency-Key"] = key;
    }
    const response = await fetch(apiRoot + encodeURIComponent(ns) + path, {
      method: method, headers: headers, body: body === undefined ? undefined : JSON.stringify(body),
    });
    const textBody = await response.text();
    let parsed = null;
    try {
      parsed = JSON.parse(textBody);
    } catch (notJson) {
      parsed = { raw: textBody.slice(0, 2000) };
    }
    return { status: response.status, body: parsed };
  }

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1360, height: 1000 } });
  const page = await context.newPage();
  const consoleErrors = [];
  page.on("console", (m) => { if (m.type() === "error") { consoleErrors.push(m.text()); } });
  const requests = [];
  page.on("request", (r) => {
    if (r.url().indexOf("/api/") !== -1 && r.method() !== "GET") {
      requests.push({ method: r.method(), url: r.url(), body: r.postData() });
    }
  });

  try {
    // ==== journey 1: the writer's schedule makes one run ====================
    const schedule = await apiCall("POST", SRC, "/schedules", {
      schedule: "0 0 1 1 *", sourceRef: { name: "orders-source" }, topics: [SOURCE_TOPIC],
      destinationRef: { name: DESTINATION }, suspended: false,
    }, "p152-schedule-" + suffix);
    check(schedule.status === 201 || schedule.status === 200,
      "the schedule create answered " + schedule.status + ": " + JSON.stringify(schedule.body));
    const scheduleName = schedule.body.item.name;
    result.created.push({ namespace: SRC, kind: "BackupSchedule", name: scheduleName,
      createdBy: "product API" });
    const detailUrl = base + "#/schedules?ns=" + encodeURIComponent(SRC) + "&name=" +
      encodeURIComponent(scheduleName);
    await openRoute(page, detailUrl, "#schedule-detail", "the schedule detail");
    await waitForSelector(page, "form.run-now-form", "the manual-run panel");
    await page.click("form.run-now-form button[type=submit]");
    const run = await waitFor("the Back up now run to appear", 60, 500, () => {
      const items = kubeJson(["-n", SRC, "get", "backups"]).items;
      return items.length === 1 ? items[0] : null;
    });
    result.created.push({ namespace: SRC, kind: "Backup", name: run.metadata.name,
      uid: run.metadata.uid, createdBy: "the console (Back up now)" });
    const done = await waitFor("the run to finish", 300, 2000, () => {
      const b = kubeJson(["-n", SRC, "get", "backup", run.metadata.name]);
      const phase = String((b.status || {}).phase || "");
      return ["Succeeded", "Failed", "Cancelled"].includes(phase) ? b : null;
    });
    artifact("src/backup.json", { metadata: done.metadata, spec: done.spec, status: done.status });
    check(done.status.phase === "Succeeded", "the run " + done.status.phase);
    const verdict = (((done.status.evidence || {}).verification) || {}).result || null;
    record("1. the writer installation's schedule makes one run through the console", {
      backup: done.metadata.name, uid: done.metadata.uid, phase: done.status.phase,
      backupId: done.status.backupId, verdict: verdict,
      verificationDetail: (((done.status.evidence || {}).verification) || {}).detail || null,
      windowCovered: done.status.windowCovered || null,
      receiptSha256: (done.status.evidence || {}).receiptSha256 || null,
    });

    // ==== journey 2: CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW ===================
    const windowless = done.status.windowCovered === undefined ||
      done.status.windowCovered === null;
    await openRoute(page, detailUrl, "#schedule-history", "the schedule history");
    const before = await page.$$eval("#schedule-history a[href^=\"#/restore\"]",
      (links) => links.map((a) => a.getAttribute("href")));
    await shot(page, "02a-detail-before-catalog");
    if (windowless) {
      check(before.length === 0, "a windowless run is offered a restore with no catalog: " +
        JSON.stringify(before));
      control("no catalog yet: the windowless NotAttempted run offers no restore", {
        restoreLinks: before });
    }
    const connectSrc = await apiCall("POST", SRC, "/catalogs", {
      name: CATALOG, destinationRef: { name: DESTINATION }, syncMode: "full",
      intervalSeconds: 3600,
    }, "p152-src-catalog-" + suffix);
    check(connectSrc.status === 201, "the SRC catalog create: " + connectSrc.status);
    const srcCatalog = await waitForCatalogPoints(SRC, 1);
    artifact("src/catalog.json", { spec: srcCatalog.spec, status: srcCatalog.status });
    await openRoute(page, detailUrl, "#schedule-history", "the schedule history, re-read");
    const after = await page.$$eval("#schedule-history a[data-restore-from=\"catalog\"]",
      (links) => links.map((a) => a.getAttribute("href")));
    await shot(page, "02b-detail-after-catalog");
    if (windowless) {
      check(after.length >= 1, "the synced catalog did not make the run restorable");
      check(after[0].indexOf("catalog=" + CATALOG) !== -1 &&
        after[0].indexOf("uid=" + done.metadata.uid) !== -1, "the link names the point: " + after[0]);
      await page.click("#schedule-history a[data-restore-from=\"catalog\"]");
      await waitForSelector(page, "#catalog-topics", "the wizard on the catalog point");
      await shot(page, "02c-wizard-from-catalog-window");
      const offered = await text(page);
      check(offered.indexOf("whose own verdict is absent or notattempted") !== -1,
        "the wizard names the run it was offered from");
      record("2. a windowless NotAttempted run is restorable from its catalog row, bound to it", {
        verdict: verdict, before: before, after: after,
        catalogCounts: (srcCatalog.status || {}).counts,
      });
    } else {
      blocked("2. CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW", {
        reason: "the lab controller wrote a window for this run (verdict " + verdict + "), so " +
          "the defect's precondition did not occur",
      });
    }

    // ==== journey 2b: WIZARD-DEFAULT-PIT-EXCLUSIVE on a VERIFIED Backup ======
    // A run the controller verified (evidence fetch -> Valid) has its own
    // `windowCovered`, and the wizard opens on the BACKUP. Its default point in
    // time must be the last instant the runner accepts, `toMs - 1`, and the
    // runner's own archive.coverage row must say so.
    const verifiedSchedule = await apiCall("POST", SRC, "/schedules", {
      schedule: "0 0 1 1 *", sourceRef: { name: "orders-source" }, topics: [SOURCE_TOPIC],
      destinationRef: { name: VERIFIED_DESTINATION }, suspended: false,
    }, "p152-schedule-verified-" + suffix);
    check(verifiedSchedule.status === 201 || verifiedSchedule.status === 200,
      "the verified schedule create answered " + verifiedSchedule.status);
    const verifiedRunCreate = await apiCall("POST", SRC, "/backups", {
      scheduleRef: { name: verifiedSchedule.body.item.name },
    }, "p152-run-verified-" + suffix);
    check(verifiedRunCreate.status === 201, "the verified run create: " + verifiedRunCreate.status +
      " " + JSON.stringify(verifiedRunCreate.body).slice(0, 400));
    const verifiedName = verifiedRunCreate.body.item.name;
    const verified = await waitFor("the verified run to finish and settle its verdict", 300, 2000,
      () => {
        const b = kubeJson(["-n", SRC, "get", "backup", verifiedName]);
        const st = b.status || {};
        const v = ((st.evidence || {}).verification || {}).result;
        if (["Failed", "Cancelled"].includes(st.phase)) {
          return b;
        }
        return st.phase === "Succeeded" && v !== undefined && v !== "Pending" ? b : null;
      });
    artifact("src/backup-verified.json", { metadata: verified.metadata, status: verified.status });
    const vWindow = verified.status.windowCovered || null;
    const vVerdict = (((verified.status.evidence || {}).verification) || {}).result || null;
    if (verified.status.phase !== "Succeeded" || vWindow === null) {
      blocked("2b. WIZARD-DEFAULT-PIT-EXCLUSIVE on a verified Backup", {
        reason: "the lab controller wrote no window for the ArchiveReadGrant run (phase " +
          verified.status.phase + ", verdict " + vVerdict + ")",
      });
    } else {
      const wizardAt = base + "#/restore?ns=" + encodeURIComponent(SRC) + "&backup=" +
        encodeURIComponent(verifiedName) + "&uid=" + encodeURIComponent(verified.metadata.uid);
      await openRoute(page, wizardAt, "#point-in-time", "the wizard on the verified Backup");
      const expected = new Date(vWindow.toMs - 1).toISOString();
      const shown = await page.$eval("#point-in-time", (n) => n.value);
      check(Date.parse(shown) === vWindow.toMs - 1,
        "the default point in time is windowCovered.toMs - 1 ms: " + shown + " vs " + expected);
      check(Date.parse(shown) !== vWindow.toMs, "never the exclusive end");
      await page.fill("#topic-prefix", "p152-" + suffix + "-v-");
      await page.dispatchEvent("#topic-prefix", "change");
      await pause(800);
      await page.click("#restore-readiness-start");
      const vPf = await waitFor("the verified-Backup Preflight", 60, 1000, () => {
        const items = kubeJson(["-n", SRC, "get", "preflights"]).items;
        return items.length >= 1 ? items[items.length - 1].metadata.name : null;
      });
      const vDone = await waitFor("the verified-Backup Preflight to finish", 240, 2000, () => {
        const pf = kubeJson(["-n", SRC, "get", "preflight", vPf]);
        return ["Completed", "Failed", "Cancelled"].includes(String((pf.status || {}).phase || ""))
          ? pf : null;
      });
      artifact("src/preflight-verified.json", { spec: vDone.spec, status: vDone.status });
      const vRows = (((vDone.status || {}).result || {}).checks || []);
      const vCoverage = vRows.find((c) => c.id === "archive.coverage") || {};
      check(vCoverage.state === "ready" && vCoverage.code === "PointInTimeCovered",
        "the runner accepts the wizard's default point in time: " + JSON.stringify(vCoverage));
      await shot(page, "02d-verified-backup-default-pit");
      // CONTROL: the exclusive end itself is refused on the field.
      await page.fill("#point-in-time", new Date(vWindow.toMs).toISOString());
      await page.dispatchEvent("#point-in-time", "change");
      await waitForSelector(page, "#point-in-time-complaint", "the exclusive-end refusal");
      control("WIZARD-DEFAULT-PIT-EXCLUSIVE: a verified Backup's exclusive windowCovered.toMs is " +
        "refused on the field", { toMs: vWindow.toMs });
      record("2b. a verified Backup's default point in time is the last instant the runner accepts", {
        backup: verifiedName, verdict: vVerdict, windowCovered: vWindow, defaultShown: shown,
        preflight: vPf, archiveCoverage: vCoverage,
      });
    }

    // ==== journey 3: the disaster namespace connects the archive ============
    const drBefore = {
      backups: kubeJson(["-n", DR, "get", "backups"]).items.length,
      backupSchedules: kubeJson(["-n", DR, "get", "backupschedules"]).items.length,
      sourceConnections: kubeJson(["-n", DR, "get", "kafkaclusters"]).items
        .filter((k) => (k.spec || {}).role === "source").length,
    };
    check(drBefore.backups === 0 && drBefore.backupSchedules === 0 &&
      drBefore.sourceConnections === 0, "the disaster namespace must hold no Backup, schedule " +
      "or source connection: " + JSON.stringify(drBefore));
    await openRoute(page, base + "#/destinations?ns=" + encodeURIComponent(DR),
      "#destination-form", "the destinations page");
    await page.fill("#destination-name", DESTINATION);
    await page.fill("#destination-bucket", BUCKET);
    await page.fill("#destination-prefix", ARCHIVE_PREFIX);
    await page.fill("#destination-endpoint", "http" + "://" + LAB_MINIO);
    await page.check("#destination-addressing-pathstyle");
    await page.check("#destination-security-http");
    await page.selectOption("#destination-archiveWrite-source", "existing");
    await page.fill("#destination-archiveWrite-secret", WRITER_SECRET);
    await page.selectOption("#destination-archiveRead-source", "existing");
    await page.fill("#destination-archiveRead-secret", READER_SECRET);
    await page.selectOption("#destination-evidenceRead-source", "archiveReadGrant");
    await page.selectOption("#destination-write-probe", "disabled");
    await shot(page, "03a-destination-form");
    await page.click("#destination-form button[type=submit]");
    const destination = await waitFor("the console-created destination", 60, 1000, () => {
      const got = kube(["-n", DR, "get", "backupdestination", DESTINATION, "-o", "json"],
        { expected: [0, 1] });
      return got.status === 0 ? JSON.parse(got.stdout) : null;
    });
    artifact("dr/destination.json", { metadata: destination.metadata, spec: destination.spec,
      status: destination.status });
    const destBody = requests.filter((r) => r.url.indexOf("/destinations") !== -1)
      .map((r) => r.body).join("\n");
    check(destBody.indexOf(READER_SECRET) !== -1 && destBody.indexOf(readerSecret) === -1,
      "the destination request names the Secret and carries no key");
    result.created.push({ namespace: DR, kind: "BackupDestination", name: DESTINATION,
      uid: destination.metadata.uid, createdBy: "the console (existing Secret names only)" });

    await openRoute(page, base + "#/catalog?ns=" + encodeURIComponent(DR),
      "form[data-connect-archive]", "the catalog page");
    await page.fill("#catalog-name", CATALOG);
    await page.fill("#catalog-destination", DESTINATION);
    await page.selectOption("#catalog-mode", "full");
    await shot(page, "03b-connect-form");
    await page.click("form[data-connect-archive] button[type=submit]");
    await waitFor("the catalog to exist", 60, 1000, () =>
      kube(["-n", DR, "get", "recoverycatalog", CATALOG], { expected: [0, 1] }).status === 0);
    const drCatalog = await waitForCatalogPoints(DR, 1);
    artifact("dr/catalog.json", { metadata: drCatalog.metadata, spec: drCatalog.spec,
      status: drCatalog.status });
    const points = await apiCall("GET", DR, "/catalogs/" + CATALOG + "/points?limit=200");
    artifact("dr/points.json", points.body);
    check(points.status === 200 && points.body.items.length >= 1, "the points route");
    const point = points.body.items.find((p) => p.backupId === done.status.backupId);
    check(point !== undefined, "the disaster namespace's catalog lists the writer's point");
    check(point.selectable === true, "the point is selectable: " + JSON.stringify(point));
    check(point.receiptKey.indexOf("[redacted]") === -1, "the receipt key is whole");
    check(point.backupVerdict === undefined, "no Backup exists here to refuse it");
    check(points.body.backupVerdictsIncomplete === undefined,
      "the verdict join over a namespace with no Backup is complete");
    result.created.push({ namespace: DR, kind: "RecoveryCatalog", name: CATALOG,
      uid: drCatalog.metadata.uid, createdBy: "the console (Connect an existing archive)" });
    record("3. the disaster namespace connects the archive and sees the synced, verified point", {
      namespaceHeld: drBefore, pointId: point.pointId, availability: point.availability,
      verification: point.verification, signerKeyId: point.signerKeyId,
      receiptKey: point.receiptKey, trustedSigners: (drCatalog.status.signers || []),
      conditions: (drCatalog.status.conditions || []).map((c) => c.type + "=" + c.status + "/" +
        c.reason),
    });

    // ==== control: a point the catalog does not list =========================
    const bogus = "lwp1-" + "0".repeat(32);
    await openRoute(page, base + "#/restore?ns=" + encodeURIComponent(DR) + "&catalog=" + CATALOG +
      "&point=" + bogus, "#catalog-point-refusal", "the wizard's refusal");
    const refusal = await text(page);
    check(refusal.indexOf("does not list this point") !== -1, "the refusal names why");
    check((await page.$("#create-restore")) === null && (await page.$("#plan-bytes")) === null,
      "a refused point renders no plan and no submit");
    await shot(page, "03c-refused-unknown-point");
    control("a point id the catalog does not list is refused with no plan", { pointId: bogus });

    // ==== journey 4: choose the point and build the bound plan ==============
    await openRoute(page, base + "#/restore?ns=" + encodeURIComponent(DR),
      "#step-catalog-points", "the selector's catalog section");
    await shot(page, "04a-selector-connected-archives");
    await page.click("#step-catalog-points a[href*=\"point=" + point.pointId + "\"]");
    await waitForSelector(page, "#catalog-topics", "the wizard on the catalog point");
    await page.fill("#catalog-topics", SOURCE_TOPIC);
    await page.dispatchEvent("#catalog-topics", "change");
    await waitForSelector(page, ".topic-box[data-topic=\"" + SOURCE_TOPIC + "\"]",
      "the named topic in the subset");
    await page.fill("#topic-prefix", RESTORE_PREFIX);
    await page.dispatchEvent("#topic-prefix", "change");
    await pause(800);
    const planBytes = await page.$eval("#plan-bytes", (n) => n.textContent);
    check(planBytes.indexOf("point_id: \"" + point.pointId + "\"") !== -1,
      "the plan is bound to the point");
    check(planBytes.indexOf("receipt_sha256: \"" + point.receiptSha256 + "\"") !== -1,
      "the plan carries the receipt digest");
    check(planBytes.indexOf("backup: \"" + point.backupId + "\"") !== -1,
      "source.backup is pinned to the point's set");
    check(planBytes.indexOf("prefix: \"" + RESTORE_PREFIX + "\"") !== -1, "the prefix");
    // WIZARD-DEFAULT-PIT-EXCLUSIVE: the default point in time is the LAST
    // instant the runner accepts, one millisecond before the exclusive
    // `coveredTo` -- never `coveredTo` itself.
    const lastAccepted = new Date(Date.parse(point.coveredTo) - 1).toISOString();
    check(planBytes.indexOf("point_in_time: \"" + lastAccepted + "\"") !== -1,
      "the default point in time is coveredTo - 1 ms (" + lastAccepted + ")");
    // CONTROL: the exclusive end itself is refused on the field, and nothing
    // about the plan moves until it is corrected.
    await page.fill("#point-in-time", new Date(Date.parse(point.coveredTo)).toISOString());
    await page.dispatchEvent("#point-in-time", "change");
    await waitForSelector(page, "#point-in-time-complaint", "the exclusive-end refusal");
    control("WIZARD-DEFAULT-PIT-EXCLUSIVE: the catalog window's exclusive end is refused on the " +
      "field", { refused: point.coveredTo, defaultAccepted: lastAccepted });
    await page.fill("#point-in-time", lastAccepted);
    await page.dispatchEvent("#point-in-time", "change");
    await pause(800);
    check(await page.$eval("#plan-bytes", (n) => n.textContent) === planBytes,
      "restoring the default restores the same plan bytes");
    artifact("dr/plan-on-screen.yaml", planBytes);
    await shot(page, "04b-wizard-bound-plan");
    record("4. the wizard builds a plan bound to the catalog point, with no Backup object", {
      planSha256: "sha256:" + sha256(planBytes), prefix: RESTORE_PREFIX,
    });

    // ==== journey 5: the normal readiness path ===============================
    await page.click("#restore-readiness-start");
    const pfName = await waitFor("the readiness Preflight", 60, 1000, () => {
      const items = kubeJson(["-n", DR, "get", "preflights"]).items;
      return items.length >= 1 ? items[items.length - 1].metadata.name : null;
    });
    const preflight = await waitFor("the Preflight to finish", 240, 2000, () => {
      const pf = kubeJson(["-n", DR, "get", "preflight", pfName]);
      const phase = String((pf.status || {}).phase || "");
      return ["Completed", "Failed", "Cancelled"].includes(phase) ? pf : null;
    });
    artifact("dr/preflight.json", { metadata: preflight.metadata, spec: preflight.spec,
      status: preflight.status });
    await pause(3000);
    await shot(page, "05-readiness");
    const rows = (((preflight.status || {}).result || {}).checks || []);
    const rpRow = rows.find((c) => c.id === "recoveryPoint.state") || null;
    const storedRef = ((preflight.spec.request || {}).restore || {}).catalogPointRef || null;
    const coverageRow = rows.find((c) => c.id === "archive.coverage") || {};
    check(coverageRow.state === "ready" && coverageRow.code === "PointInTimeCovered",
      "the runner's archive.coverage accepts the default point in time: " +
        JSON.stringify(coverageRow));
    const pfRequest = requests.filter((r) => r.url.indexOf("/preflights") !== -1)
      .map((r) => r.body).join("\n");
    check(pfRequest.indexOf("\"catalogPoint\"") !== -1, "the console named the catalog point");
    // THE REFRESHED LAB'S BRANCH, REQUIRED (lab-refresh-9): the served Preflight
    // CRD carries catalogPointRef, so the stored object keeps it and the
    // controller answers the recovery point's row about the catalog point.
    check(storedRef !== null && (storedRef.catalogRef || {}).name === CATALOG && storedRef.pointId === point.pointId,
      "the stored Preflight does not carry the catalog point (a pruned catalogPointRef): " +
        JSON.stringify(storedRef));
    check(rpRow !== null && rpRow.state === "ready" && rpRow.code === "CatalogPointSelectable" &&
      (rpRow.scope || {}).kind === "RecoveryCatalog" && (rpRow.scope || {}).name === CATALOG,
      "recoveryPoint.state is not ready/CatalogPointSelectable: " + JSON.stringify(rpRow));
    record("5. readiness runs through the normal Preflight path for the catalog point", {
      preflight: pfName, phase: preflight.status.phase,
      state: ((preflight.status || {}).result || {}).state,
      rows: rows.map((c) => c.id + "=" + c.state + "/" + c.code),
      recoveryPointRow: rpRow,
      catalogPointRefStored: storedRef,
      note: "the stored object carries the reference and the controller answered its row",
    });

    // ==== control: the API refuses a readiness check naming two points ======
    const both = await apiCall("POST", DR, "/preflights", {
      operation: "restore",
      restore: { planBytes: planBytes, planHash: "sha256:" + sha256(planBytes),
        target: "restore-target", sourceDestination: DESTINATION,
        evidenceDestination: DESTINATION,
        recoveryPoint: { backupName: "nothing" },
        catalogPoint: { catalog: CATALOG, pointId: point.pointId } },
    }, "p152-both-" + suffix);
    check(both.status === 422, "two recovery points in one check must be refused: " + both.status);
    control("a readiness check naming a Backup and a catalog point is refused (422)",
      { status: both.status, body: both.body });

    // ==== a finding, not a pass: the wizard's readiness gate =================
    // A DRAFT's readiness verdict is always `unknown` -- `approval.state` is
    // `skipped/SubjectNotCreated` because no Restore exists yet
    // (docs/kubernetes.md section 21.7) -- and `readinessRefusal` refuses every
    // verdict that is not `ready`. So once a draft has been checked, the guided
    // submit is disabled for that plan. That gate is PLAT-11.2's (outside this
    // task); it is recorded here with its evidence, and the journey continues the
    // way an operator can: a fresh load of the same point, the same two edits,
    // the same bytes -- asserted by hash -- submitted with no check on screen.
    const checkedHash = await page.$eval("#plan-hash-value", (n) => n.textContent);
    const gate = await page.evaluate(() => {
      const b = document.querySelector("#create-restore");
      return { disabled: b === null ? null : b.disabled,
        said: document.body.innerText.slice(0, 20000) };
    });
    result.findings = (result.findings || []).concat([{
      finding: "WIZARD-READINESS-GATE-REFUSES-EVERY-CHECKED-DRAFT",
      detail: "after a readiness check on a draft plan the create button is disabled, because " +
        "a draft's aggregate is unknown (approval.state skipped/SubjectNotCreated) and the " +
        "gate refuses anything but ready",
      createDisabled: gate.disabled, aggregate: ((preflight.status || {}).result || {}).state,
      planHash: checkedHash,
    }]);
    artifact("dr/gate-after-check.txt", gate.said);
    const wizardUrl = page.url();
    await openRoute(page, wizardUrl, "#catalog-topics", "the wizard, freshly loaded");
    await page.fill("#catalog-topics", SOURCE_TOPIC);
    await page.dispatchEvent("#catalog-topics", "change");
    await waitForSelector(page, ".topic-box[data-topic=\"" + SOURCE_TOPIC + "\"]",
      "the named topic in the subset, again");
    await page.fill("#topic-prefix", RESTORE_PREFIX);
    await page.dispatchEvent("#topic-prefix", "change");
    await pause(800);
    const reloadedBytes = await page.$eval("#plan-bytes", (n) => n.textContent);
    check(reloadedBytes === planBytes, "the fresh load renders the same plan bytes");
    check(await page.$eval("#plan-hash-value", (n) => n.textContent) === checkedHash,
      "and the same hash the readiness check was bound to");

    // ==== journey 6: create the Restore, approve it on the console ==========
    await page.click("#create-restore");
    await waitFor("the approvals page", 60, 1000, async () =>
      (await page.url()).indexOf("#/approvals") !== -1 ? true : null);
    const restore = await waitFor("the Restore", 60, 1000, () => {
      const items = kubeJson(["-n", DR, "get", "restores"]).items;
      return items.length === 1 ? items[0] : null;
    });
    check(restore.spec.planBytes === planBytes, "the Restore carries the reviewed plan bytes");
    check(restore.spec.backupSetRef === point.backupId, "backupSetRef is the point's set");
    check((restore.spec.sourceDestinationRef || {}).name === DESTINATION,
      "the Restore reads the catalog's destination");
    result.created.push({ namespace: DR, kind: "Restore", name: restore.metadata.name,
      uid: restore.metadata.uid, createdBy: "the console (guided submit)" });
    mkdirSync(join(WORK_DIR, "approval"), { recursive: true, mode: 0o700 });
    const planPath = join(WORK_DIR, "approval", "plan.yaml");
    writeFileSync(planPath, restore.spec.planBytes);
    const approve = spawnSync(LOGWEIR_BIN, ["drill", "approve", "--spec", planPath,
      "--key", APPROVER_KEY, "--approver", "plat15-2-live", "--ticket", "PLAT-15.2",
      "--subject-kind", "Restore", "--out", join(WORK_DIR, "approval", "approval.json")],
    { encoding: "utf8", timeout: 120000 });
    check(approve.status === 0, "logweir drill approve exited " + approve.status + ": " +
      String(approve.stderr || "").slice(0, 1000));
    const approvalBytes = readFileSync(join(WORK_DIR, "approval", "approval.json"), "utf8");
    const sidecarBytes = readFileSync(join(WORK_DIR, "approval", "approval.sig"), "utf8");
    artifact("dr/approval.json", approvalBytes);
    artifact("dr/approval.sig", sidecarBytes);
    // A SECOND FINDING. In console mode the product API mints the Restore's
    // name (`rst-...`), but the guided submit routes to the approval page of the
    // name the PAGE minted from the plan bytes (`restore-<8 hex>`), which does not
    // exist -- so that page offers no form. The submit and its routing are
    // PLAT-11.2/12.1's (claude/plat19-2's lane); recorded here, and the journey
    // continues the way an approver can: the approvals page's own list of
    // Restores waiting for an approval.
    const landed = page.url();
    if ((await page.$("#approval-form")) === null) {
      result.findings = (result.findings || []).concat([{
        finding: "GUIDED-SUBMIT-ROUTES-TO-THE-PAGE-MINTED-RESTORE-NAME",
        detail: "console mode: the create answered " + restore.metadata.name + " and the " +
          "submit navigated to " + landed,
        landedOn: landed, created: restore.metadata.name,
      }]);
      await openRoute(page, base + "#/approvals?ns=" + encodeURIComponent(DR),
        "#awaiting-approval", "the approvals page");
      await page.click("#awaiting-approval a:has-text(\"" + restore.metadata.name + "\")");
    }
    await waitForSelector(page, "#approval-form", "the approval form");
    await page.fill("#approval-json", approvalBytes);
    await page.fill("#approval-sig", sidecarBytes);
    await shot(page, "06a-approval-form");
    await page.click("#approval-form button[type=submit]");
    await pause(4000);
    const approvalStatus = await page.evaluate(() => {
      const n = document.querySelector("#approval-form-status");
      return n === null ? "(no status slot)" : n.innerText;
    });
    await shot(page, "06b-approval-submitted");
    artifact("dr/approval-form-status.txt", approvalStatus);
    // THE CONSOLE HAS NO APPROVAL CREATE ROUTE IN THIS BUILD (PLAT-19.2 owns
    // governed approval submission), and the page says so and points at kubectl
    // or the CLI. So the approver records it the way the page tells them to:
    // the same two documents, by kubectl, into an Approval naming the Restore.
    let approvalCreatedBy = "the console (approval form)";
    if (approvalStatus.indexOf("no create route for Approval") !== -1) {
      create(DR, { apiVersion: "logweir.dev/v1alpha1", kind: "Approval",
        metadata: owned(restore.spec.approvalRef.name, DR),
        spec: { approvalBytes: approvalBytes, sidecarBytes: sidecarBytes,
          planHash: "sha256:" + sha256(restore.spec.planBytes),
          subjectRef: { kind: "Restore", name: restore.metadata.name } } });
      approvalCreatedBy = "kubectl, as the console's approval page directs (no console create route)";
      result.findings = (result.findings || []).concat([{
        finding: "CONSOLE-HAS-NO-APPROVAL-CREATE-ROUTE",
        detail: approvalStatus.slice(0, 400),
      }]);
    }
    const approval = await waitFor("the Approval to verify (form said: " +
      approvalStatus.slice(0, 600) + ")", 90, 2000, () => {
      const got = kube(["-n", DR, "get", "approval", restore.spec.approvalRef.name, "-o", "json"],
        { expected: [0, 1] });
      if (got.status !== 0) {
        return null;
      }
      const a = JSON.parse(got.stdout);
      return (a.status || {}).verified === true ? a : null;
    });
    artifact("dr/approval-object.json", { metadata: approval.metadata, status: approval.status });
    record("6. the Restore is created by the guided submit and its Approval verifies", {
      restore: restore.metadata.name, approval: approval.metadata.name,
      approvalCreatedBy: approvalCreatedBy,
      matchedKeyId: (approval.status || {}).matchedKeyId,
    });

    // ==== journey 7: the runner re-verifies, restores, and the records match ==
    const finished = await waitFor("the Restore to finish", 360, 2000, () => {
      const r = kubeJson(["-n", DR, "get", "restore", restore.metadata.name]);
      const phase = String((r.status || {}).phase || "");
      return ["Succeeded", "Failed", "Cancelled"].includes(phase) ? r : null;
    });
    artifact("dr/restore.json", { metadata: finished.metadata, spec: finished.spec,
      status: finished.status });
    const jobName = ((finished.status || {}).jobRef || {}).name;
    const runnerLog = jobName
      ? kube(["-n", DR, "logs", "job/" + jobName], { expected: [0, 1], timeout: 60000 }).stdout
      : "";
    artifact("dr/runner.log", runnerLog);
    await openRoute(page, base + "#/history?ns=" + encodeURIComponent(DR) + "&name=" +
      encodeURIComponent(restore.metadata.name), "body", "the operation view");
    await pause(2000);
    await shot(page, "07a-operation");
    check(finished.status.phase === "Succeeded", "the restore " + finished.status.phase + ": " +
      JSON.stringify(finished.status).slice(0, 1500));
    check(runnerLog.indexOf("recovery point binding verified") !== -1 &&
      runnerLog.indexOf(point.pointId) !== -1, "the runner re-verified the bound point");
    const sourceValues = topicValues("kafka-source", SOURCE_TOPIC, 100);
    const restoredTopic = RESTORE_PREFIX + SOURCE_TOPIC;
    const restoredValues = topicValues("kafka-target", restoredTopic, sourceValues.length);
    artifact("dr/records-compare.json", {
      sourceTopic: SOURCE_TOPIC, restoredTopic: restoredTopic,
      sourceCount: sourceValues.length, restoredCount: restoredValues.length,
      sourceSha256: sha256(sourceValues.join("\n")),
      restoredSha256: sha256(restoredValues.join("\n")),
      sourceEndOffsets: endOffset("kafka-source", SOURCE_TOPIC),
      restoredEndOffsets: endOffset("kafka-target", restoredTopic),
    });
    check(sourceValues.length > 0, "the source topic has records");
    check(restoredValues.length === sourceValues.length &&
      restoredValues.every((v, i) => v === sourceValues[i]),
    "the restored records are the source's, in order: " + restoredValues.length + " vs " +
      sourceValues.length);
    record("7. the runner re-verifies the point and the restored records equal the source's", {
      phase: finished.status.phase, outcome: finished.status.outcome,
      exitCode: finished.status.exitCode, job: jobName,
      bindingLine: runnerLog.split("\n").filter((l) => l.indexOf("binding verified") !== -1),
      records: sourceValues.length, sameValuesInOrder: true, restoredTopic: restoredTopic,
    });

    // ==== control: a plan bound to a receipt the archive does not hold =====
    const forgedDigest = "sha256:" + "0".repeat(64);
    const forgedPlan = planBytes
      .split("receipt_sha256: \"" + point.receiptSha256 + "\"")
      .join("receipt_sha256: \"" + forgedDigest + "\"")
      .split("prefix: \"" + RESTORE_PREFIX + "\"").join("prefix: \"" + TAMPERED_PREFIX + "\"")
      .split("topic_mapping_prefix: \"" + RESTORE_PREFIX + "\"")
      .join("topic_mapping_prefix: \"" + TAMPERED_PREFIX + "\"");
    check(forgedPlan !== planBytes && forgedPlan.indexOf(forgedDigest) !== -1, "a forged plan");
    const forgedName = "rst-forged-" + suffix;
    const forgedApprovalName = "apr-forged-" + suffix;
    const forgedPath = join(WORK_DIR, "approval", "forged.yaml");
    writeFileSync(forgedPath, forgedPlan);
    const approveForged = spawnSync(LOGWEIR_BIN, ["drill", "approve", "--spec", forgedPath,
      "--key", APPROVER_KEY, "--approver", "plat15-2-live", "--ticket", "PLAT-15.2-control",
      "--subject-kind", "Restore", "--out", join(WORK_DIR, "approval", "forged.json")],
    { encoding: "utf8", timeout: 120000 });
    check(approveForged.status === 0, "drill approve (forged) exited " + approveForged.status);
    const forgedHash = "sha256:" + sha256(forgedPlan);
    const forgedCreate = await apiCall("POST", DR, "/restores", {
      planBytes: forgedPlan, planHash: forgedHash,
      approvalRef: { name: forgedApprovalName },
      sourceArchive: { url: "logweir-destination://" + DESTINATION },
      sourceDestinationRef: { name: DESTINATION }, evidenceDestinationRef: { name: DESTINATION },
      backupSetRef: point.backupId, pointInTime: restore.spec.pointInTime,
      target: { clusterRef: { name: "restore-target" }, mode: "newTopic",
        topicNaming: { prefix: TAMPERED_PREFIX } },
      deadlineSeconds: 1800,
    }, "p152-forged-" + suffix);
    check(forgedCreate.status === 201, "the forged Restore create: " + forgedCreate.status + " " +
      JSON.stringify(forgedCreate.body).slice(0, 600));
    const forgedRestoreName = forgedCreate.body.item.name;
    const forgedObject = kubeJson(["-n", DR, "get", "restore", forgedRestoreName]);
    create(DR, { apiVersion: "logweir.dev/v1alpha1", kind: "Approval",
      metadata: owned(forgedApprovalName, DR),
      spec: { approvalBytes: readFileSync(join(WORK_DIR, "approval", "forged.json"), "utf8"),
        sidecarBytes: readFileSync(join(WORK_DIR, "approval", "forged.sig"), "utf8"),
        planHash: forgedHash,
        subjectRef: { kind: "Restore", name: forgedRestoreName } } });
    result.created.push({ namespace: DR, kind: "Restore", name: forgedRestoreName,
      uid: forgedObject.metadata.uid, createdBy: "product API (negative control)" });
    const refused = await waitFor("the forged Restore to finish", 240, 2000, () => {
      const r = kubeJson(["-n", DR, "get", "restore", forgedRestoreName]);
      const phase = String((r.status || {}).phase || "");
      return ["Succeeded", "Failed", "Cancelled"].includes(phase) ? r : null;
    });
    artifact("dr/forged-restore.json", { metadata: refused.metadata, status: refused.status });
    const forgedJob = ((refused.status || {}).jobRef || {}).name;
    const forgedLog = forgedJob
      ? kube(["-n", DR, "logs", "job/" + forgedJob], { expected: [0, 1], timeout: 60000 }).stdout
      : "";
    artifact("dr/forged-runner.log", forgedLog);
    check(refused.status.phase === "Failed", "a forged binding must fail: " + refused.status.phase);
    check(refused.status.exitCode === 3, "exit 3, a refusal of the plan: " + refused.status.exitCode);
    check(forgedLog.indexOf("PointBindingMismatch") !== -1, "the runner names PointBindingMismatch");
    check(!topicExists("kafka-target", TAMPERED_PREFIX + SOURCE_TOPIC),
      "no data moved: the forged plan's mapped topic does not exist");
    control("a signed plan bound to a receipt digest the archive does not hold is refused by the " +
      "runner before any data moves", { restore: forgedRestoreName, phase: refused.status.phase,
      exitCode: refused.status.exitCode, reason: refused.status.exitReason || refused.status.reason,
      mappedTopicAbsent: TAMPERED_PREFIX + SOURCE_TOPIC });

    // ==== journey 8: repeated import yields the same point ids =============
    kube(["-n", DR, "patch", "recoverycatalog", CATALOG, "--type=merge", "-p",
      JSON.stringify({ spec: { syncRequest: "again-" + suffix } })]);
    await waitFor("the second sync", 180, 2000, () => {
      const c = kubeJson(["-n", DR, "get", "recoverycatalog", CATALOG]);
      const st = c.status || {};
      const synced = (st.conditions || []).find((x) => x.type === "Synced");
      return st.observedSyncRequest === "again-" + suffix && synced && synced.status === "True"
        ? c : null;
    });
    const again = await apiCall("GET", DR, "/catalogs/" + CATALOG + "/points?limit=200");
    artifact("dr/points-after-resync.json", again.body);
    const idsBefore = points.body.items.map((p) => p.pointId).sort();
    const idsAfter = again.body.items.map((p) => p.pointId).sort();
    check(JSON.stringify(idsBefore) === JSON.stringify(idsAfter), "the same point ids");
    record("8. a repeated import of the same archive yields the same point ids", {
      before: idsBefore, after: idsAfter });


    // ==== lab-refresh-9 rows: P10, an unsigned receipt, a revoked signer ====
    // The PLAT-15.2 review's rows for a lab whose controller, CRDs and runner
    // carry the branch. Each needs the real API server, the real controller or
    // the real runner image; none can be proved by a fixture. The revoked-key
    // rows create a cluster-scoped TrustPolicy over DR only (owner-labelled,
    // deleted in `cleanup`): run this harness under the cluster lock.
    const catalogCheckBody = () => ({
      operation: "restore",
      restore: { planBytes: planBytes, planHash: "sha256:" + sha256(planBytes),
        target: "restore-target", sourceDestination: DESTINATION, evidenceDestination: DESTINATION,
        catalogPoint: { catalog: CATALOG, pointId: point.pointId } },
    });
    const catalogCheck = async (label) => {
      const made = await apiCall("POST", DR, "/preflights", catalogCheckBody(), "p152-" + label + "-" + suffix);
      check(made.status === 201 || made.status === 200 || made.status === 202,
        label + ": the readiness create answered " + made.status + " " + JSON.stringify(made.body).slice(0, 400));
      const name = ((made.body || {}).item || {}).name || ((made.body || {}).item || {}).id;
      check(typeof name === "string" && name.length > 0, label + ": no Preflight name in " +
        JSON.stringify(made.body).slice(0, 400));
      const pf = await waitFor(label + ": the Preflight to finish", 240, 2000, () => {
        const got = kubeJson(["-n", DR, "get", "preflight", name]);
        return ["Completed", "Failed", "Cancelled"].includes(String((got.status || {}).phase || "")) ? got : null;
      });
      artifact("dr/lr9-" + label + "-preflight.json", { metadata: pf.metadata, spec: pf.spec, status: pf.status });
      return { name: name, row: ((((pf.status || {}).result || {}).checks) || [])
        .find((c) => c.id === "recoveryPoint.state") || null };
    };
    const signedRestore = async (label, prefix) => {
      const plan = planBytes.split("prefix: \"" + RESTORE_PREFIX + "\"").join("prefix: \"" + prefix + "\"");
      check(plan !== planBytes && plan.indexOf(point.receiptSha256) !== -1,
        label + ": the plan is the reviewed one, bound to the same receipt, under a new prefix");
      const planPath = join(WORK_DIR, "approval", label + ".yaml");
      writeFileSync(planPath, plan);
      const approved = spawnSync(LOGWEIR_BIN, ["drill", "approve", "--spec", planPath,
        "--key", APPROVER_KEY, "--approver", "plat15-2-live", "--ticket", "PLAT-15.2-" + label,
        "--subject-kind", "Restore", "--out", join(WORK_DIR, "approval", label + ".json")],
      { encoding: "utf8", timeout: 120000 });
      check(approved.status === 0, label + ": drill approve exited " + approved.status);
      const hash = "sha256:" + sha256(plan);
      const approvalName = "apr-" + label + "-" + suffix;
      const made = await apiCall("POST", DR, "/restores", {
        planBytes: plan, planHash: hash, approvalRef: { name: approvalName },
        sourceArchive: { url: "logweir-destination://" + DESTINATION },
        sourceDestinationRef: { name: DESTINATION }, evidenceDestinationRef: { name: DESTINATION },
        backupSetRef: point.backupId, pointInTime: restore.spec.pointInTime,
        target: { clusterRef: { name: "restore-target" }, mode: "newTopic", topicNaming: { prefix: prefix } },
        deadlineSeconds: 1800,
      }, "p152-" + label + "-" + suffix);
      check(made.status === 201, label + ": the Restore create answered " + made.status + " " +
        JSON.stringify(made.body).slice(0, 400));
      const name = made.body.item.name;
      const obj = kubeJson(["-n", DR, "get", "restore", name]);
      result.created.push({ namespace: DR, kind: "Restore", name: name, uid: obj.metadata.uid,
        createdBy: "product API (lab-refresh-9 row " + label + ")" });
      create(DR, { apiVersion: "logweir.dev/v1alpha1", kind: "Approval", metadata: owned(approvalName, DR),
        spec: { approvalBytes: readFileSync(join(WORK_DIR, "approval", label + ".json"), "utf8"),
          sidecarBytes: readFileSync(join(WORK_DIR, "approval", label + ".sig"), "utf8"),
          planHash: hash, subjectRef: { kind: "Restore", name: name } } });
      const done = await waitFor(label + ": the Restore to finish", 240, 2000, () => {
        const r = kubeJson(["-n", DR, "get", "restore", name]);
        return ["Succeeded", "Failed", "Cancelled"].includes(String((r.status || {}).phase || "")) ? r : null;
      });
      const jobName = ((done.status || {}).jobRef || {}).name || null;
      const job = jobName ? kubeJson(["-n", DR, "get", "job", jobName]) : null;
      const log = jobName ? kube(["-n", DR, "logs", "job/" + jobName], { expected: [0, 1], timeout: 60000 }).stdout : "";
      artifact("dr/lr9-" + label + "-restore.json", { metadata: done.metadata, status: done.status });
      artifact("dr/lr9-" + label + "-runner.log", log);
      return { name: name, uid: obj.metadata.uid, done: done, job: job, log: log };
    };
    const refusedBeforeData = (outcome, prefix, needle) => ({
      "the Restore failed": outcome.done.status.phase === "Failed",
      "exit 3, a refusal": outcome.done.status.exitCode === 3,
      ["the runner names " + needle]: outcome.log.indexOf(needle) !== -1,
      "no data phase began (no progress-phase=0:admit)": outcome.log.indexOf("progress-phase=0:admit") === -1,
      "the mapped topic was never created": !topicExists("kafka-target", prefix + SOURCE_TOPIC),
    });
    const lr9 = { rows: {} };

    // P10, EVALUATED BY A REAL API SERVER: journey 5's stored Preflight with a
    // Backup reference added is refused at admission with the CRD's message;
    // the same object with only its catalog reference is accepted.
    const p10Spec = JSON.parse(JSON.stringify(preflight.spec));
    p10Spec.request.restore.recoveryPointRef = { name: "nothing" };
    const p10 = kube(["-n", DR, "create", "-f", "-"], { expected: [0, 1],
      input: JSON.stringify({ apiVersion: "logweir.dev/v1alpha1", kind: "Preflight",
        metadata: owned("p152-p10-both-" + suffix, DR), spec: p10Spec }) });
    const only = kube(["-n", DR, "create", "-f", "-"], { expected: [0, 1],
      input: JSON.stringify({ apiVersion: "logweir.dev/v1alpha1", kind: "Preflight",
        metadata: owned("p152-p10-catalog-" + suffix, DR), spec: preflight.spec }) });
    artifact("dr/lr9-p10.json", { both: { rc: p10.status, stderr: p10.stderr.slice(0, 1200) },
      catalogOnly: { rc: only.status, stdout: only.stdout.slice(0, 400), stderr: only.stderr.slice(0, 400) } });
    check(p10.status === 1 && p10.stderr.indexOf(
      "set at most one of recoveryPointRef (a Backup) or catalogPointRef (a catalog point)") !== -1,
    "P10: the API server did not refuse both refs with the P10 message: " + p10.stderr.slice(0, 600));
    check(only.status === 0, "P10 control: a Preflight with only catalogPointRef was refused: " + only.stderr.slice(0, 600));
    kube(["-n", DR, "delete", "preflight", "p152-p10-catalog-" + suffix, "--wait=false"], { expected: [0, 1] });
    record("lr9-a. P10: a real API server refuses a Preflight naming a Backup and a catalog point, and admits one naming the catalog point alone", {
      refusal: p10.stderr.trim().slice(0, 400) });
    control("P10: the same stored spec with only catalogPointRef is admitted", { rc: only.status });

    // AN UNSIGNED RECEIPT: the point's sidecar is moved aside AFTER approval
    // is minted and before the Job runs; the runner must refuse before any data.
    const sidecarKey = point.receiptKey.replace(/\.json$/, ".sig");
    check(sidecarKey !== point.receiptKey && sidecarKey.endsWith(".receipt.sig"), "the sidecar key of " + point.receiptKey);
    const aside = "lr9-aside/" + sidecarKey;
    mcJob(SRC, "sig-aside", "mc mv \"adm/$S3_BUCKET/$SIG\" \"adm/$S3_BUCKET/$ASIDE\" && " +
      "! mc stat \"adm/$S3_BUCKET/$SIG\" >/dev/null 2>&1 && echo moved-aside",
    [{ name: "SIG", value: sidecarKey }, { name: "ASIDE", value: aside }]);
    let unsigned;
    try {
      unsigned = await signedRestore("unsigned", UNSIGNED_PREFIX);
    } finally {
      mcJob(SRC, "sig-back", "mc mv \"adm/$S3_BUCKET/$ASIDE\" \"adm/$S3_BUCKET/$SIG\" && " +
        "mc stat \"adm/$S3_BUCKET/$SIG\" >/dev/null && echo restored",
      [{ name: "SIG", value: sidecarKey }, { name: "ASIDE", value: aside }]);
    }
    const unsignedClauses = Object.assign(refusedBeforeData(unsigned, UNSIGNED_PREFIX, "PointUntrusted"), {
      "the refusal says the receipt carries no signature": unsigned.log.indexOf("carries no signature") !== -1 });
    lr9.rows.unsigned = unsignedClauses;
    check(Object.values(unsignedClauses).every(Boolean), "unsigned receipt: " + JSON.stringify(unsignedClauses));
    record("lr9-b. an unsigned receipt: the runner refuses the bound point (exit 3, PointUntrusted, carries no signature) before any data moves", {
      restore: unsigned.name, sidecarMovedAside: sidecarKey, clauses: unsignedClauses });

    // A REVOKED SIGNER. DR gets a TrustPolicy naming the roster's signing key
    // (the point's signer) Active and the lab approver key for approvals; the
    // catalog re-syncs under it (the control: ready/CatalogPointSelectable).
    // The signer is then revoked for compromise WITHOUT a re-sync.
    const roster = kubeJson(["get", "trustroster", "default"]);
    const signer = (roster.spec.signingKeys || []).find((k) => k.keyId === point.signerKeyId);
    const approver = (roster.spec.approverKeys || [])[0];
    check(signer !== undefined && approver !== undefined, "the point's signer and the approver key are the roster's");
    const policyKey = (k, usage, state, extra) => Object.assign({ keyId: k.keyId, spkiPem: k.spkiPem,
      algorithm: "p256", principal: { id: k.subject || "lab@scram-local.invalid", display: "lab " + usage },
      usages: [usage], state: state, notBefore: "2026-01-01T00:00:00Z", notAfter: "2027-01-01T00:00:00Z" }, extra || {});
    const trustPolicy = (signerState, extra) => ({ apiVersion: "logweir.dev/v1alpha1", kind: "TrustPolicy",
      metadata: { name: TRUST_POLICY, labels: LABELS },
      spec: { namespaces: [DR], keys: [policyKey(signer, "EvidenceSigning", signerState, extra),
        policyKey(approver, "GovernedApproval", "Active")] } });
    kube(["apply", "-f", "-"], { input: JSON.stringify(trustPolicy("Active")) });
    result.created.push({ kind: "TrustPolicy", name: TRUST_POLICY, createdBy: "kubectl (lab-refresh-9 rows)" });
    kube(["-n", DR, "patch", "recoverycatalog", CATALOG, "--type=merge", "-p",
      JSON.stringify({ spec: { syncRequest: "policy-" + suffix } })]);
    const underPolicy = await waitFor("the sync under the TrustPolicy", 180, 2000, () => {
      const c = kubeJson(["-n", DR, "get", "recoverycatalog", CATALOG]);
      const st = c.status || {};
      const synced = (st.conditions || []).find((x) => x.type === "Synced");
      return st.observedSyncRequest === "policy-" + suffix && synced && synced.status === "True" ? c : null;
    });
    const trustCondition = ((underPolicy.status || {}).conditions || []).find((x) => x.type === "TrustAvailable") || null;
    artifact("dr/lr9-catalog-under-policy.json", { status: underPolicy.status });
    const beforeRevoke = await catalogCheck("revoke-control");
    check(beforeRevoke.row !== null && beforeRevoke.row.state === "ready" && beforeRevoke.row.code === "CatalogPointSelectable",
      "revoked-key control: before the revocation the point is not ready/CatalogPointSelectable: " +
        JSON.stringify(beforeRevoke.row));
    control("revoked-key: before the revocation the catalog point is ready/CatalogPointSelectable under the TrustPolicy",
      { preflight: beforeRevoke.name, row: beforeRevoke.row, trustAvailable: trustCondition });
    const at = new Date().toISOString().replace(/\.\d+Z$/, "Z");
    kube(["apply", "-f", "-"], { input: JSON.stringify(trustPolicy("Revoked", { retiredAt: at, revokedAt: at,
      revocationEffectiveFrom: "2026-09-01T00:00:00Z", revocationReason: "KeyCompromise" })) });
    await pause(5000);
    const afterRevoke = await catalogCheck("revoked");
    const revokedPreflight = {
      "recoveryPoint.state is notReady/CatalogPointSignerUntrusted":
        afterRevoke.row !== null && afterRevoke.row.state === "notReady" && afterRevoke.row.code === "CatalogPointSignerUntrusted",
      "it names the key": afterRevoke.row !== null && JSON.stringify(afterRevoke.row).indexOf(signer.keyId.slice(0, 12)) !== -1,
      "it names Revoked": afterRevoke.row !== null && JSON.stringify(afterRevoke.row).indexOf("Revoked") !== -1,
    };
    lr9.rows.revokedPreflight = revokedPreflight;
    check(Object.values(revokedPreflight).every(Boolean), "revoked-key Preflight: " + JSON.stringify(afterRevoke.row));
    record("lr9-c. a revoked signer's catalog point is refused by the Preflight (CatalogPointSignerUntrusted, naming the key and Revoked), with no re-sync", {
      preflight: afterRevoke.name, row: afterRevoke.row, clauses: revokedPreflight });
    const revoked = await signedRestore("revoked", REVOKED_PREFIX);
    let bundle = null;
    let keysDigestEnv = null;
    if (revoked.job !== null) {
      const podSpec = revoked.job.spec.template.spec;
      keysDigestEnv = [].concat(...(podSpec.containers || []).map((c) => c.env || []))
        .find((e) => e.name === "LOGWEIR_EXECUTION_EVIDENCE_KEYS_SHA256") || null;
      for (const volume of (podSpec.volumes || [])) {
        const cmName = (volume.configMap || {}).name || null;
        const projected = ((volume.projected || {}).sources || []).map((src) => (src.configMap || {}).name).filter(Boolean);
        for (const name of [cmName].concat(projected).filter(Boolean)) {
          const cm = kube(["-n", DR, "get", "configmap", name, "-o", "json"], { expected: [0, 1] });
          if (cm.status === 0 && (JSON.parse(cm.stdout).data || {})["evidence-keys.json"]) {
            bundle = { configMap: name, evidenceKeys: JSON.parse(JSON.parse(cm.stdout).data["evidence-keys.json"]) };
          }
        }
      }
    }
    artifact("dr/lr9-revoked-bundle.json", { bundle: bundle, keysDigestEnv: keysDigestEnv });
    const listed = JSON.stringify((bundle || {}).evidenceKeys || {});
    const revokedRunner = Object.assign(refusedBeforeData(revoked, REVOKED_PREFIX, "PointUntrusted"), {
      "a Job was rendered after the revocation": revoked.job !== null,
      "its bundle's evidence-keys.json lists the signer as Revoked":
        bundle !== null && listed.indexOf(signer.keyId) !== -1 && listed.indexOf("Revoked") !== -1,
      "its env carries LOGWEIR_EXECUTION_EVIDENCE_KEYS_SHA256": keysDigestEnv !== null,
      "the refusal names Revoked": revoked.log.indexOf("Revoked") !== -1,
    });
    lr9.rows.revokedRunner = revokedRunner;
    check(Object.values(revokedRunner).every(Boolean), "revoked-key runner: " + JSON.stringify(revokedRunner));
    record("lr9-d. a revoked signer's catalog point is refused by the runner (exit 3, PointUntrusted, Revoked) before any data moves", {
      restore: revoked.name, job: revoked.job ? revoked.job.metadata.name : null, clauses: revokedRunner,
      control: "journey 7: the same point with its signer trusted restored exit 0 and logged 'recovery point binding verified'" });
    result.labRefresh9 = lr9;

    result.consoleErrors = consoleErrors;
    result.requests = requests.map((r) => ({ method: r.method, url: r.url,
      bodySha256: r.body ? sha256(r.body) : null }));
  } finally {
    await browser.close();
  }
}

// ------------------------------------------------------------------ cleanup

function cleanup() {
  stopApi();
  artifact("api.log", apiLog.join(""));
  for (const ns of [SRC, DR]) {
    try {
      const dump = {};
      for (const kind of ["backups", "backupschedules", "restores", "approvals", "preflights",
        "recoverycatalogs", "backupdestinations", "kafkaclusters"]) {
        const got = kube(["-n", ns, "get", kind, "-o", "json"], { expected: [0, 1] });
        dump[kind] = got.status === 0
          ? JSON.parse(got.stdout).items.map((o) => ({ name: o.metadata.name, uid: o.metadata.uid,
            phase: (o.status || {}).phase }))
          : "unreadable";
      }
      artifact("dump-" + ns + ".json", dump);
    } catch (dumpFailed) {
      result.cleanup.push({ namespace: ns, dump: String(dumpFailed.message) });
    }
  }
  try {
    const tp = kube(["get", "trustpolicy", TRUST_POLICY, "-o", "json"], { expected: [0, 1] });
    if (tp.status === 0) {
      const live = JSON.parse(tp.stdout);
      check((live.metadata.labels || {})["logweir.dev/test-owner"] === OWNER,
        "refusing to delete TrustPolicy " + TRUST_POLICY + ": not this run's");
      kube(["delete", "trustpolicy", TRUST_POLICY, "--wait=true"]);
      result.cleanup.push({ trustPolicy: TRUST_POLICY, uid: live.metadata.uid,
        deleted: kube(["get", "trustpolicy", TRUST_POLICY], { expected: [0, 1] }).status !== 0 });
    }
  } catch (tpFailed) {
    result.cleanup.push({ trustPolicy: TRUST_POLICY, error: String(tpFailed.message).slice(0, 400) });
  }
  for (const topic of [RESTORE_PREFIX + SOURCE_TOPIC, TAMPERED_PREFIX + SOURCE_TOPIC,
    UNSIGNED_PREFIX + SOURCE_TOPIC, REVOKED_PREFIX + SOURCE_TOPIC]) {
    try {
      check(topic.startsWith("p152-" + suffix + "-"), "only this run's topics are deleted");
      if (topicExists("kafka-target", topic)) {
        kube(["-n", LAB, "exec", brokerPod("kafka-target"), "--", "/opt/kafka/bin/kafka-topics.sh",
          "--bootstrap-server", "localhost:9092", "--delete", "--topic", topic], { timeout: 60000 });
        result.cleanup.push({ topic: topic, deleted: true, stillExists: topicExists("kafka-target", topic) });
      } else {
        result.cleanup.push({ topic: topic, deleted: false, reason: "never created" });
      }
    } catch (topicFailed) {
      result.cleanup.push({ topic: topic, error: String(topicFailed.message).slice(0, 400) });
    }
  }
  try {
    const removed = mcJob(SRC, "cleanup",
      "mc rb --force \"adm/$S3_BUCKET\" >/dev/null && echo bucket-removed; " +
      "mc admin user remove adm \"$READER_USER\" >/dev/null && echo user-removed; " +
      "mc admin policy remove adm \"$POLICY_NAME\" >/dev/null && echo policy-removed",
      [{ name: "READER_USER", value: READER_USER }, { name: "POLICY_NAME", value: READER_POLICY }]);
    result.cleanup.push({ objectStore: removed });
  } catch (storeFailed) {
    result.cleanup.push({ objectStore: String(storeFailed.message).slice(0, 400) });
  }
  if (process.env.UI_E2E_KEEP === "1") {
    result.cleanup.push({ kept: [SRC, DR] });
    return;
  }
  for (const made of result.created.filter((c) => c.kind === "Namespace")) {
    try {
      const live = kubeJson(["get", "namespace", made.name]);
      check(live.metadata.uid === made.uid &&
        (live.metadata.labels || {})["logweir.dev/test-owner"] === OWNER,
      "refusing to delete a namespace this run did not create: " + made.name);
      kube(["delete", "namespace", made.name, "--wait=true", "--timeout=180s"], { timeout: 200000 });
      const gone = kube(["get", "namespace", made.name], { expected: [0, 1] });
      result.cleanup.push({ namespace: made.name, uid: made.uid, deleted: gone.status !== 0 });
    } catch (nsFailed) {
      result.cleanup.push({ namespace: made.name, error: String(nsFailed.message).slice(0, 400) });
    }
  }
  rmSync(WORK_DIR, { recursive: true, force: true });
  result.cleanup.push({ workDir: WORK_DIR, removed: !existsSync(WORK_DIR) });
}

let exitCode = 0;
try {
  await main();
} catch (error) {
  exitCode = 1;
  result.error = String(error && error.stack ? error.stack : error).slice(0, 6000);
  process.stderr.write("FAILED: " + result.error + "\n");
} finally {
  try {
    cleanup();
  } catch (cleanupFailed) {
    result.cleanup.push({ error: String(cleanupFailed.message) });
  }
  result.finishedAt = new Date().toISOString();
  if (result.blocked.length > 0) {
    exitCode = 1;
  }
  result.exitCode = exitCode;
  artifact("live.json", result);
  process.stderr.write("result: " + join(ARTIFACTS, "live.json") + "\n");
  process.exit(exitCode);
}
