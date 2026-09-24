// PLAT-18.2 live acceptance harness: the Clarity console, driven in Chromium
// against a host-run `logweir-api` (localAdmin mode) over docker-desktop.
//
// WHAT THIS PROVES, AND HOW.
//   1. KEYBOARD-ONLY JOURNEYS. Configure (a saved connection, a schedule,
//      Back up now) and restore (history -> a real recovery point -> the
//      wizard -> a Restore created and held for its Approval) are driven with
//      `page.keyboard` only. A capture-phase listener counts every TRUSTED
//      pointer event on the page; a journey with one is a failure.
//   2. FOCUS SURVIVES A RE-RENDER. After the schedule detail's Back up now
//      (the PLAT-10 review's LOW) and after a topic box is toggled in the
//      wizard (which re-renders the whole wizard), `document.activeElement`
//      must be the control that was used, not the body. Run against main's
//      `ui/` (UI_E2E_UI_DIR) the same probes record the defect: that is the
//      negative control.
//   3. EVERY PRIMARY ROUTE, IN LIGHT AND DARK AND AT A PHONE WIDTH. A
//      full-page screenshot per route per theme per width; axe-core (vendored
//      offline by the host's Lighthouse CI install, version recorded) over
//      WCAG 2.0/2.1 A and AA; and the harness's own checks -- no horizontal
//      scroll at 390 px, every control named, a visible focus ring on
//      keyboard focus, no inline style.
//   4. STATES. Slow network (a delayed read shows the role=status loading
//      line), an error (a refused read renders role=alert), and an empty
//      namespace (the list's own empty sentence) on the primary lists.
//   5. LARGE DATASETS. The history and backups reads are answered with the
//      real response inflated to 1,000 rows, and the recovery point's topic
//      list to 2,000 topics (Playwright `route.fetch` + synthesis: every
//      synthesized item is a clone of a real one with a new name and uid);
//      the harness times first paint of the list, a filter keystroke, a page
//      change and a wizard checkbox toggle, and counts the rows in the DOM.
//
// WHAT IT CREATES, all inside one owner-labelled namespace `lw-p182-<utc>`:
// the no-verb runner account, copies (never printed) of the lab's SCRAM,
// object-store and signer Secrets, a target connection, a BackupDestination
// on a bucket of its own on the lab's MinIO, and a RecoveryCatalog; the
// journeys then create a source connection, a schedule, a Backup and a
// Restore through the page. The lab's controller in `logweir-scram-local` is
// the only reconciler and is only read. The Restore is left held for its
// Approval: no restore runs and nothing is written to the target broker.
//
// STAGES (UI_E2E_STAGE): `provision` creates the namespace and fixtures and
// writes `state.json`; `pass` starts the API and runs everything above
// against an existing namespace (UI_E2E_NAMESPACE); `cleanup` deletes the
// bucket and the namespace after checking its UID and owner label; `all`
// (the default) runs the three in order. UI_E2E_JOURNEYS=0 skips the two
// creating journeys (the baseline pass over main's `ui/` uses it).
//
//   NODE_PATH="$(npm root -g)" node scripts/plat18-2-ui-e2e.mjs
//
// Environment (all optional): UI_E2E_STAGE, UI_E2E_NAMESPACE, UI_E2E_UI_DIR,
// UI_E2E_LABEL (the artifact subdirectory of a pass), UI_E2E_API_BIN,
// UI_E2E_ARTIFACTS, UI_E2E_JOURNEYS, UI_E2E_AXE.

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = process.env.UI_E2E_KUBECTL || "kubectl";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UI_DIR = resolve(process.env.UI_E2E_UI_DIR || join(REPO, "ui"));
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "debug", "logweir-api");
const AXE = process.env.UI_E2E_AXE ||
  "/opt/homebrew/lib/node_modules/@lhci/cli/node_modules/axe-core/axe.min.js";
const OWNER = "plat18-2";
const OWNER_LABEL = "logweir.dev/test-owner=" + OWNER;
const PREFIX = "lw-p182-";
const STAGE = process.env.UI_E2E_STAGE || "all";
const JOURNEYS = process.env.UI_E2E_JOURNEYS !== "0";
const LABEL = process.env.UI_E2E_LABEL || "branch";
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z");
const namespace = process.env.UI_E2E_NAMESPACE || (PREFIX + stamp.toLowerCase());
const ARTIFACTS_ROOT = process.env.UI_E2E_ARTIFACTS ||
  "/tmp/logweir-roadmap-run/claude/artifacts/plat18-2";
const NS_DIR = join(ARTIFACTS_ROOT, namespace);
const ARTIFACTS = join(NS_DIR, LABEL);
const WORK_DIR = join("/tmp", "plat182-live-" + namespace);
const STATE_FILE = join(NS_DIR, "state.json");

const LAB = "logweir-scram-local";
const LAB_KAFKA = "kafka-source." + LAB + ".svc.cluster.local:9096";
const LAB_TARGET = "kafka-target." + LAB + ".svc.cluster.local:9096";
const LAB_MINIO = "minio." + LAB + ".svc:9000";
const STORE_SECRET = "p182-object-store";
const CATALOG = "p182-catalog";

// ------------------------------------------------------------------ helpers

function check(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function pause(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function assertSafeNamespace(ns) {
  check(ns.startsWith(PREFIX), "this harness only ever touches " + PREFIX + "* namespaces, not " + ns);
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
    maxBuffer: opts.maxBuffer || 16 * 1024 * 1024,
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
  const done = spawnSync(command, args, { encoding: "utf8", timeout: 30000 });
  return done.status === 0 ? String(done.stdout || "").trim() : null;
}

function readState() {
  check(existsSync(STATE_FILE), "no provisioned state at " + STATE_FILE +
    "; run UI_E2E_STAGE=provision first (or UI_E2E_STAGE=all)");
  return JSON.parse(readFileSync(STATE_FILE, "utf8"));
}

function writeState(state) {
  mkdirSync(NS_DIR, { recursive: true });
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2) + "\n");
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

// --------------------------------------------------------------- provision

function copyLabSecret(state, labName, ownName) {
  const source = kubeJson(["-n", LAB, "get", "secret", labName]);
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "v1", kind: "Secret", type: source.type || "Opaque",
      metadata: { name: ownName, namespace: namespace, labels: { "logweir.dev/test-owner": OWNER } },
      data: source.data,
    }),
  });
  state.fixtures.push({ kind: "Secret", name: ownName, copiedFrom: LAB + "/" + labName,
    note: "value never printed, read or logged" });
}

let mcJobs = 0;
function mcJob(state, step, script) {
  mcJobs += 1;
  const name = "p182-mc-" + String(Date.now()).slice(-6) + "-" + String(mcJobs) + "-" + step;
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
              name: "mc", image: "docker.io/vladyslavhaina/mc-mirror@sha256:9c7cbc3f47b092d52b73124fb9ab12f3266534c23c283b2e984d07408c9ff381", imagePullPolicy: "IfNotPresent",
              command: ["/bin/sh", "-ec"],
              args: ["mc alias set p182 \"$S3_ENDPOINT\" \"$AWS_ACCESS_KEY_ID\" " +
                "\"$AWS_SECRET_ACCESS_KEY\" >/dev/null; " + script],
              env: [
                { name: "S3_ENDPOINT", value: "http" + "://" + LAB_MINIO },
                { name: "S3_BUCKET", value: state.bucket },
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
    "--timeout=150s"], { expected: [0, 1], timeout: 170000 });
  const logs = kube(["-n", namespace, "logs", "job/" + name], { expected: [0, 1] }).stdout;
  mkdirSync(NS_DIR, { recursive: true });
  writeFileSync(join(NS_DIR, "mc-" + name + ".log"), logs);
  check(waited.status === 0, "the object-store step " + name + " did not complete:\n" + logs);
  return logs.trim();
}

async function syncCatalog(token) {
  kube(["-n", namespace, "patch", "recoverycatalog", CATALOG, "--type=merge", "-p",
    JSON.stringify({ spec: { syncRequest: token } })]);
  for (let attempt = 0; attempt < 150; attempt += 1) {
    const catalog = kubeJson(["-n", namespace, "get", "recoverycatalog", CATALOG]);
    const st = catalog.status || {};
    const synced = (st.conditions || []).find((c) => c.type === "Synced");
    if (st.observedSyncRequest === token && synced !== undefined && synced.status === "True" &&
      ((st.lastSyncJob || {}).finishedAt || "").length > 0) {
      return catalog;
    }
    await pause(1000);
  }
  throw new Error("the lab controller did not complete catalog sync " + token);
}

async function provision() {
  assertSafeNamespace(namespace);
  mkdirSync(NS_DIR, { recursive: true });
  const labPods = kubeJson(["-n", LAB, "get", "pods", "-l",
    "app.kubernetes.io/component=control-plane"]).items;
  check(labPods.length === 1, "expected exactly one lab controller pod");
  const others = kubeJson(["get", "deployments", "-A"]).items.filter((d) =>
    String(d.metadata.name).indexOf("weirkeeper") !== -1 && d.metadata.namespace !== LAB);
  check(others.length === 0, "another weirkeeper deployment exists");
  check(kube(["get", "namespace", namespace], { expected: [0, 1] }).status !== 0,
    "refusing to reuse an existing namespace");
  kube(["create", "namespace", namespace]);
  kube(["label", "namespace", namespace, OWNER_LABEL]);
  const ns = kubeJson(["get", "namespace", namespace]);
  const state = {
    harness: "scripts/plat18-2-ui-e2e.mjs",
    kubeContext: KUBE_CONTEXT,
    namespace: namespace,
    namespaceUid: ns.metadata.uid,
    owner: OWNER,
    createdAt: new Date().toISOString(),
    revision: commandText("git", ["-C", REPO, "rev-parse", "HEAD"]),
    controller: {
      namespace: LAB, pod: labPods[0].metadata.name, image: labPods[0].spec.containers[0].image,
      imageID: (labPods[0].status.containerStatuses || [{}])[0].imageID,
      readOnly: "observed only; nothing in " + LAB + " was changed",
    },
    lab: { kafka: LAB_KAFKA, target: LAB_TARGET, minio: LAB_MINIO },
    bucket: namespace,
    fixtures: [],
    suffix: Math.random().toString(36).slice(2, 7),
  };
  writeState(state);
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({ apiVersion: "v1", kind: "ServiceAccount",
      metadata: { name: "logweir-runner", labels: { "logweir.dev/test-owner": OWNER } },
      automountServiceAccountToken: false }),
  });
  copyLabSecret(state, "logweir-signing-key", "logweir-signing-key");
  copyLabSecret(state, "source-scram", "orders-scram");
  copyLabSecret(state, "target-scram", "target-scram");
  copyLabSecret(state, "logweir-s3", STORE_SECRET);
  state.target = "restore-target";
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
      metadata: { name: state.target, labels: { "logweir.dev/test-owner": OWNER } },
      spec: { bootstrapServers: [LAB_TARGET], role: "target",
        auth: { mode: "scramSha512", tls: false, username: "scram-user",
          secretRef: { name: "target-scram" } } },
    }),
  });
  state.fixtures.push({ kind: "KafkaCluster", name: state.target, role: "target" });
  const made = mcJob(state, "make-bucket", "mc mb \"p182/$S3_BUCKET\"; mc ls p182 | " +
    "while read -r line; do case \"$line\" in *\" $S3_BUCKET/\") echo \"bucket present: " +
    "$S3_BUCKET\";; esac; done");
  check(made.indexOf("bucket present: " + state.bucket) !== -1, "the owned bucket was not created");
  state.bucketCreated = true;
  state.destination = "primary";
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
      metadata: { name: state.destination, labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        description: "PLAT-18.2 live pass: this run's own bucket on the lab's MinIO",
        storage: { provider: "S3", bucket: state.bucket, prefix: namespace,
          addressing: "PathStyle", endpoint: "http" + "://" + LAB_MINIO },
        transport: { security: "InsecureHTTP" },
        access: {
          archiveWrite: { mode: "SecretKeys", secret: { name: STORE_SECRET } },
          archiveRead: { mode: "SecretKeys", secret: { name: STORE_SECRET } },
          evidenceRead: { mode: "ArchiveReadGrant" },
        },
      },
    }),
  });
  state.fixtures.push({ kind: "BackupDestination", name: state.destination, bucket: state.bucket });
  kube(["-n", namespace, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "RecoveryCatalog",
      metadata: { name: CATALOG, labels: { "logweir.dev/test-owner": OWNER } },
      spec: { destinationRef: { name: state.destination },
        sync: { deepCheck: "ManifestDigest", intervalSeconds: 0, maxObjectsPerRun: 1000,
          mode: "Full", viewLimit: 100 },
        syncRequest: "empty" },
    }),
  });
  await syncCatalog("empty");
  state.catalog = CATALOG;
  state.fixtures.push({ kind: "RecoveryCatalog", name: CATALOG });
  writeState(state);
  process.stderr.write("== provisioned " + namespace + " (uid " + state.namespaceUid + ")\n");
  return state;
}

// ----------------------------------------------------------------- cleanup

function cleanup() {
  const state = readState();
  assertSafeNamespace(state.namespace);
  const record = { at: new Date().toISOString(), namespace: state.namespace };
  const before = kube(["get", "namespace", state.namespace, "-o", "json"], { expected: [0, 1] });
  if (before.status !== 0) {
    record.alreadyGone = true;
  } else {
    const object = JSON.parse(before.stdout);
    check(object.metadata.uid === state.namespaceUid,
      "refusing: the namespace under this name is not the one provisioned (uid differs)");
    check((object.metadata.labels || {})["logweir.dev/test-owner"] === OWNER,
      "refusing: the namespace does not carry this harness's owner label");
    record.uid = object.metadata.uid;
    record.objectsBefore = kube(["-n", state.namespace, "get",
      "backupschedules,backups,kafkaclusters,backupdestinations,preflights,restores," +
        "recoverycatalogs,approvals,secrets,jobs", "-o", "name"], { expected: [0, 1] })
      .stdout.trim().split("\n").filter((l) => l.length > 0);
    if (state.bucketCreated) {
      try {
        record.bucket = mcJob(state, "remove-bucket", "mc rb --force \"p182/$S3_BUCKET\"; " +
          "if mc ls \"p182/$S3_BUCKET\" >/dev/null 2>&1; then echo bucket-still-there; " +
          "else echo bucket-gone; fi");
      } catch (failed) {
        record.bucket = "FAILED: " + (failed instanceof Error ? failed.message : String(failed));
      }
    }
    kube(["delete", "namespace", state.namespace, "--wait=true"], { timeout: 300000 });
    const after = kube(["get", "namespace", state.namespace], { expected: [0, 1] });
    record.deleted = after.status !== 0;
    record.afterStderr = String(after.stderr || "").trim();
  }
  rmSync(WORK_DIR, { recursive: true, force: true });
  writeFileSync(join(NS_DIR, "cleanup.json"), JSON.stringify(record, null, 2) + "\n");
  process.stderr.write("== cleanup: " + JSON.stringify(record) + "\n");
  check(record.alreadyGone === true || record.deleted === true, "the namespace is still there");
}

// ------------------------------------------------------------------ export

export { provision, cleanup, readState, kube, kubeJson, syncCatalog, namespace, ARTIFACTS,
  NS_DIR, WORK_DIR, UI_DIR, API_BIN, AXE, JOURNEYS, LABEL, freePort, pause, check,
  commandText, randomBytes, spawn, chromium, writeState };

async function main() {
  if (STAGE === "provision" || STAGE === "all") {
    await provision();
  }
  if (STAGE === "pass" || STAGE === "all") {
    const { runPass } = await import("./plat18-2-ui-pass.mjs");
    await runPass();
  }
  if (STAGE === "cleanup" || STAGE === "all") {
    cleanup();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(String((error && error.stack) || error) + "\n");
    process.exit(1);
  });
}
