// PLAT-19.2 (and PLAT-12.1's policy routing) live console harness: a Restore
// submitted through the real console goes where the namespace's FROZEN
// approval policy sends it.
//
// THE LAUNCHER AND THE FIXTURES ARE `scripts/plat11-2-ui-e2e.mjs`'s. This
// starts the source-built `logweir-api` in localAdmin mode on a loopback port
// against docker-desktop, with an approval-policy document and a per-run
// ConsoleConfirmation key, and drives a real Chromium against three namespaces
// this run creates:
//
//   <ns>-o   bound to an ORDINARY policy: Create the Restore signs the console's
//            confirmation, stores it as the Approval the Restore names, and the
//            page routes straight to the operation view.
//   <ns>-v   bound to a GOVERNED policy: Create the Restore stores the console's
//            confirmation as `<approvalRef>-confirmation`, routes to Awaiting
//            approval, and the requester's own countersignature is refused.
//   <ns>-g   UNBOUND (`legacy-governed-v1`): routes to Awaiting approval; an
//            Approval minted with the lab's own approver key (`d2_live.py`
//            `mint_approval`'s procedure) is recorded and the lab controller
//            verifies it, and the page then shows it Verified.
//
// WHAT THE LAB CONTROLLER IS, AND WHAT THAT MEANS HERE. The shared lab runs the
// `weirkeeper` image of the last lab refresh, which predates PLAT-19.2, so it
// cannot verify an authorization document v2. That is not hidden: journey 1
// records the lab controller's verdict on the console's ordinary confirmation,
// which is D0's rollback rule observed live -- an old controller refuses every
// v2 document (`PayloadTypeMismatch`) and the Restore holds with no Job. The
// new controller's verdicts are proved by kube-mock rows in the branch and at
// the next batch lab refresh (see the result file).
//
// EVERY OBJECT IS CREATED BY THE PAGE against the real service against the real
// API server, except the fixtures (named as such) and the minted Approval of
// journey 3, which an approver records out of band exactly as today. No
// response is fabricated, intercepted or delayed.
//
// THE APPROVER KEY NEVER LEAVES ITS FILE. `$HOME/.logweir-lab/scram-e2e/
// approver.pem` is passed by PATH to `logweir drill approve`; its contents are
// never read, printed or copied by this process. The console key and the
// throwaway countersigning key are generated per run into a 0700 work
// directory, never printed, and deleted in the `finally`.
//
//   NODE_PATH="$(npm root -g)" node scripts/plat19-2-ui-e2e.mjs
//
// Environment (all optional): UI_E2E_OWNER, UI_E2E_PREFIX, UI_E2E_API_BIN,
// UI_E2E_LOGWEIR_BIN, UI_E2E_UI_DIR, UI_E2E_ARTIFACTS, UI_E2E_KEEP,
// UI_E2E_KUBECTL, UI_E2E_APPROVER_KEY.

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { createHash, createPublicKey, randomBytes, verify as verifySig } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = process.env.UI_E2E_KUBECTL || "kubectl";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UI_DIR = process.env.UI_E2E_UI_DIR || join(REPO, "ui");
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "debug", "logweir-api");
const LOGWEIR_BIN = process.env.UI_E2E_LOGWEIR_BIN || join(REPO, "target", "debug", "logweir");
const APPROVER_KEY = process.env.UI_E2E_APPROVER_KEY ||
  join(process.env.HOME || "", ".logweir-lab", "scram-e2e", "approver.pem");
const OWNER = process.env.UI_E2E_OWNER || "plat19-2";
const NAMESPACE_PREFIX = process.env.UI_E2E_PREFIX || "lw-p192-";
const OWNER_LABEL = "logweir.dev/test-owner=" + OWNER;
const LABELS = { "logweir.dev/test-owner": OWNER };
const LAB_NS = "logweir-scram-local";
const LAB_TARGET_BOOTSTRAP = "kafka-target." + LAB_NS + ".svc.cluster.local:9096";
const LAB_SOURCE_BOOTSTRAP = "kafka-source." + LAB_NS + ".svc.cluster.local:9096";
const LAB_SCRAM_USER = "scram-user";
const LAB_TARGET_SECRET = "target-scram";
const LAB_SOURCE_SECRET = "source-scram";
const V2_PAYLOAD = "application/vnd.logweir.restore-authorization+json;version=2.0.0";

const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z").toLowerCase();
const base = NAMESPACE_PREFIX + stamp;
const NS = { ordinary: base + "-o", governed: base + "-v", legacy: base + "-g", readiness: base + "-r" };
const ARTIFACTS = join(process.env.UI_E2E_ARTIFACTS ||
  "/tmp/logweir-roadmap-run/claude/artifacts/plat19-2", stamp);
const WORK_DIR = join("/tmp", "plat19-2-live-" + stamp);
const suffix = randomBytes(3).toString("hex");
const TOPICS = ["orders", "payments"];
const FROM_MS = 1760000000000;
const TO_MS = 1760000060000;
const BASE_REV = process.env.UI_E2E_BASE_REV || "f49849d";

const result = {
  harness: "scripts/plat19-2-ui-e2e.mjs",
  task: "PLAT-19.2 + PLAT-12.1 policy routing",
  kubeContext: KUBE_CONTEXT,
  owner: OWNER,
  namespaces: NS,
  labNamespace: LAB_NS,
  uiDirectory: UI_DIR,
  apiBinary: API_BIN,
  logweirBinary: LOGWEIR_BIN,
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
    throw new Error(KUBECTL + " " + args.join(" ") + " exited " + done.status + ": " +
      String(done.stderr || "").trim().slice(0, 1500));
  }
  return done;
}

function kubeJson(args) {
  return JSON.parse(kube(args.concat(["-o", "json"])).stdout);
}

function apply(ns, object) {
  return JSON.parse(kube(["-n", ns, "create", "-f", "-", "-o", "json"],
    { input: JSON.stringify(object) }).stdout);
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
  throw new Error(label + ": never saw " + JSON.stringify(needle) + ". Saw:\n" +
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

async function waitForHash(page, prefix, label) {
  for (let i = 0; i < 60; i += 1) {
    const hash = await page.evaluate(() => window.location.hash);
    if (hash.startsWith(prefix)) {
      return hash;
    }
    await pause(500);
  }
  throw new Error(label + ": the page never reached " + prefix + "; it is at " +
    (await page.evaluate(() => window.location.hash)) + "\n" + (await text(page)).slice(0, 1500));
}

function runCli(args, timeoutMs) {
  const done = spawnSync(LOGWEIR_BIN, args, { encoding: "utf8", timeout: timeoutMs || 120000 });
  return { status: done.status, out: String(done.stdout || "") + String(done.stderr || "") };
}

/** DSSE PAE, exactly as `logweir-verify` computes it. */
function pae(type, body) {
  return Buffer.concat([
    Buffer.from("DSSEv1 " + Buffer.byteLength(type) + " " + type + " " + body.length + " "),
    body,
  ]);
}

// ------------------------------------------------------------- the service

let api = null;
const apiLog = [];
let consolePublicPem = "";

async function startApi(port) {
  mkdirSync(WORK_DIR, { recursive: true, mode: 0o700 });
  const cursorKey = join(WORK_DIR, "cursor.key");
  writeFileSync(cursorKey, randomBytes(32), { mode: 0o600 });
  const consoleKey = join(WORK_DIR, "confirmation.key");
  const minted = spawnSync("openssl", ["genpkey", "-algorithm", "ed25519", "-out", consoleKey],
    { encoding: "utf8", timeout: 30000 });
  check(minted.status === 0, "openssl could not mint the console key");
  const pub = spawnSync("openssl", ["pkey", "-in", consoleKey, "-pubout"],
    { encoding: "utf8", timeout: 30000 });
  check(pub.status === 0, "openssl could not derive the console public key");
  consolePublicPem = pub.stdout;
  save("console-confirmation.pub.pem", consolePublicPem);
  const policyPath = join(WORK_DIR, "approval-policy.yaml");
  const policy = [
    "allowOrdinaryConfirmation: true",
    "policies:",
    "  - name: p192-ordinary",
    "    mode: Ordinary",
    "    maxAgeSeconds: 900",
    "  - name: p192-governed",
    "    mode: Governed",
    "    maxAgeSeconds: 86400",
    "namespaces:",
    "  " + NS.ordinary + ": p192-ordinary",
    "  " + NS.governed + ": p192-governed",
    "  " + NS.readiness + ": p192-ordinary",
    "",
  ].join("\n");
  writeFileSync(policyPath, policy);
  save("approval-policy.yaml", policy);
  const configPath = join(WORK_DIR, "config.yaml");
  const config = [
    "mode: localAdmin",
    "listen: \"127.0.0.1:" + port + "\"",
    "publicOrigin: \"http://127.0.0.1:" + port + "\"",
    "uiDirectory: " + UI_DIR,
    "localAdmin:",
    "  subject: admin",
    "  displayName: Local administrator",
    "namespaces: [" + Object.values(NS).join(", ") + "]",
    "kubernetes:",
    "  source: kubeconfig",
    "  context: " + KUBE_CONTEXT,
    "cursorKeyFile: " + cursorKey,
    "approvalPolicyFile: " + policyPath,
    "confirmationKeyFile: " + consoleKey,
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
  throw new Error("logweir-api never answered /healthz within 30 s. Log:\n" + apiLog.join(""));
}

function stopApi() {
  if (api !== null && api.exitCode === null) {
    api.kill("SIGTERM");
  }
}

// -------------------------------------------------------------- the fixtures

const LONG = "fixture-backup-deliberately-longer-than-sixty-three-characters-";
const rfc = (ms) => new Date(ms).toISOString().replace(".000Z", "Z");

function copyLabSecret(ns, name) {
  const source = kubeJson(["-n", LAB_NS, "get", "secret", name]);
  kube(["-n", ns, "create", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "v1", kind: "Secret", type: source.type || "Opaque",
      metadata: { name: name, namespace: ns, labels: LABELS }, data: source.data,
    }),
  });
  result.created.push({ kind: "Secret", namespace: ns, name: name, note: "copied from the lab, value never printed" });
}

/** The newest Succeeded lab Backup: its backupId and covered window are the
 *  REAL archive the readiness namespace's fixture point names. Read-only. */
function labRecoveryPoint() {
  const items = (kubeJson(["-n", LAB_NS, "get", "backups"]).items || [])
    .filter((b) => (b.status || {}).phase === "Succeeded" && (b.status || {}).backupId &&
      ((b.status || {}).windowCovered || {}).toMs)
    .sort((a, b) => b.status.windowCovered.toMs - a.status.windowCovered.toMs);
  check(items.length > 0, "the lab has no Succeeded Backup to read an archive from");
  const b = items[0];
  const url = String(((b.spec || {}).archive || {}).url || "");
  const m = /^s3:\/\/([^/]+)\/(.*)$/.exec(url);
  check(m !== null, "the lab Backup's archive url is not s3://bucket/prefix: " + url);
  return { name: b.metadata.name, backupId: b.status.backupId, windowCovered: b.status.windowCovered,
    topics: b.spec.topics, bucket: m[1], prefix: m[2],
    secret: ((((b.spec || {}).archive || {}).secretRef) || {}).name };
}

async function seedNamespace(ns, lab) {
  assertSafeNamespace(ns);
  kube(["create", "namespace", ns]);
  kube(["label", "namespace", ns, OWNER_LABEL]);
  const made = kubeJson(["get", "namespace", ns]);
  result.created.push({ kind: "Namespace", name: ns, uid: made.metadata.uid });
  apply(ns, { apiVersion: "v1", kind: "ServiceAccount",
    metadata: { name: "logweir-runner", namespace: ns, labels: LABELS } });
  const keyPath = join(WORK_DIR, "signing-" + ns + ".pem");
  const minted = spawnSync("openssl", ["genpkey", "-algorithm", "ed25519", "-out", keyPath],
    { encoding: "utf8", timeout: 30000 });
  check(minted.status === 0, "openssl could not mint a signing key");
  kube(["-n", ns, "create", "secret", "generic", "logweir-signing-key",
    "--from-file=signing.pem=" + keyPath]);
  copyLabSecret(ns, LAB_TARGET_SECRET);
  const sourceSecret = kube(["-n", LAB_NS, "get", "secret", LAB_SOURCE_SECRET],
    { expected: [0, 1] }).status === 0 ? LAB_SOURCE_SECRET : LAB_TARGET_SECRET;
  if (sourceSecret === LAB_SOURCE_SECRET) {
    copyLabSecret(ns, LAB_SOURCE_SECRET);
  }
  const cluster = (name, role, bootstrap, secret) => {
    const c = apply(ns, {
      apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
      metadata: { name: name, namespace: ns, labels: LABELS },
      spec: { bootstrapServers: [bootstrap], role: role,
        auth: { mode: "scramSha512", tls: false, username: LAB_SCRAM_USER, secretRef: { name: secret } } },
    });
    result.created.push({ kind: "KafkaCluster", namespace: ns, name: name, uid: c.metadata.uid });
    return c;
  };
  cluster("source-" + suffix, "source", LAB_SOURCE_BOOTSTRAP, sourceSecret);
  const target = cluster("target-" + suffix, "target", LAB_TARGET_BOOTSTRAP, LAB_TARGET_SECRET);

  if (lab) {
    // THE LAB'S OWN ARCHIVE, READ ONLY: the store Secret is copied (value never
    // printed) so the readiness check can READ the manifest and segments the
    // lab's real run wrote. Nothing in this journey writes to the store.
    const source = kubeJson(["-n", LAB_NS, "get", "secret", lab.secret]);
    kube(["-n", ns, "create", "-f", "-"], {
      input: JSON.stringify({
        apiVersion: "v1", kind: "Secret", type: source.type || "Opaque",
        metadata: { name: "store-" + suffix, namespace: ns, labels: LABELS }, data: source.data,
      }),
    });
    result.created.push({ kind: "Secret", namespace: ns, name: "store-" + suffix,
      note: "copied from the lab's " + lab.secret + ", value never printed" });
  } else {
    kube(["-n", ns, "create", "secret", "generic", "store-" + suffix,
      "--from-literal=access-key-id=unused", "--from-literal=secret-access-key=unused"]);
  }
  const dest = apply(ns, {
    apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
    metadata: { name: "dest-" + suffix, namespace: ns, labels: LABELS },
    spec: {
      storage: { provider: "S3", bucket: lab ? lab.bucket : "kafka-backups",
        prefix: lab ? lab.prefix : ns, addressing: "PathStyle",
        endpoint: "http" + "://minio." + LAB_NS + ".svc:9000" },
      transport: { security: "InsecureHTTP" },
      access: { archiveWrite: { mode: "SecretKeys", secret: { name: "store-" + suffix } } },
    },
  });
  let frozen = null;
  for (let i = 0; i < 60 && frozen === null; i += 1) {
    const seen = kubeJson(["-n", ns, "get", "backupdestination", "dest-" + suffix]);
    const status = seen.status || {};
    if (typeof status.locationDigest === "string" && status.locationDigest.startsWith("sha256:")) {
      frozen = { name: "dest-" + suffix, uid: seen.metadata.uid,
        generation: seen.metadata.generation, locationDigest: status.locationDigest };
    } else {
      await pause(1000);
    }
  }
  check(frozen !== null, "BackupDestination never published locationDigest in " + ns);
  result.created.push({ kind: "BackupDestination", namespace: ns, name: dest.metadata.name, uid: dest.metadata.uid });

  const name = NAMESPACE_PREFIX + LONG + suffix;
  const backup = apply(ns, {
    apiVersion: "logweir.dev/v1alpha1", kind: "Backup",
    metadata: { name: name, namespace: ns, labels: LABELS },
    spec: { archive: { url: "logweir-destination://" + frozen.name }, destinationRef: { name: frozen.name },
      deadlineSeconds: 3600, sourceRef: { name: "source-" + suffix },
      topics: lab ? lab.topics.slice() : TOPICS.slice(),
      triggeredBy: "manual" },
  });
  const status = {
    phase: "Succeeded", backupId: lab ? lab.backupId : "01JB7Z0000000000000000P192", records: 1000,
    exitCode: 0, exitReason: "ok", reason: "Ok",
    manifestKey: lab ? lab.prefix + "/" + lab.backupId + "/manifest.json" : ns + "/set/manifest.json",
    destination: frozen,
    windowCovered: lab ? lab.windowCovered : { fromMs: FROM_MS, toMs: TO_MS },
    conditions: [{ type: "Complete", status: "True", reason: "Ok", message: "fixture",
      lastTransitionTime: rfc(TO_MS) }],
  };
  let kept = false;
  for (let i = 0; i < 20 && !kept; i += 1) {
    kube(["-n", ns, "patch", "backup", name, "--subresource=status", "--type=merge",
      "-p", JSON.stringify({ status: status })]);
    await pause(1000);
    const seen = kubeJson(["-n", ns, "get", "backup", name]).status || {};
    kept = seen.phase === "Succeeded" && seen.backupId === status.backupId;
  }
  check(kept, "the fixture Backup did not keep its status in " + ns);
  result.created.push({ kind: "Backup", namespace: ns, name: name, uid: backup.metadata.uid });
  result.fixtures.push({ namespace: ns, backup: name, note: lab
    ? "a Succeeded fixture Backup whose backupId and covered window are the lab run " + lab.name +
      "'s, frozen to a destination over the lab's own bucket/prefix: the archive it names is REAL"
    : "a Succeeded fixture Backup; no archive exists for it" });
  return { point: { name: name, uid: backup.metadata.uid }, targetUid: target.metadata.uid,
    targetName: target.metadata.name, window: status.windowCovered };
}

// ------------------------------------------------------------------ the run

async function main() {
  mkdirSync(ARTIFACTS, { recursive: true });
  mkdirSync(WORK_DIR, { recursive: true, mode: 0o700 });
  for (const ns of Object.values(NS)) {
    assertSafeNamespace(ns);
  }
  const labController = kubeJson(["-n", LAB_NS, "get", "deploy", "weirkeeper"]);
  result.labControllerImage = labController.spec.template.spec.containers[0].image;
  result.labControllerRevision = (labController.spec.template.metadata.labels || {});
  result.labControllerPods = (kubeJson(["-n", LAB_NS, "get", "pods"]).items || [])
    .filter((p) => p.metadata.name.startsWith("weirkeeper"))
    .map((p) => ({ name: p.metadata.name, phase: (p.status || {}).phase,
      imageIDs: ((p.status || {}).containerStatuses || []).map((c) => c.imageID) }));
  const lab = labRecoveryPoint();
  result.labRecoveryPoint = { name: lab.name, backupId: lab.backupId, windowCovered: lab.windowCovered,
    bucket: lab.bucket, prefix: lab.prefix };
  const seeded = {};
  for (const [key, ns] of Object.entries(NS)) {
    seeded[key] = await seedNamespace(ns, key === "readiness" ? lab : null);
  }

  const port = await freePort();
  await startApi(port);
  const origin = "http://127.0.0.1:" + port;
  const ui = origin + "/ui/";
  result.port = port;
  result.apiStartLine = apiLog.join("").split("\n").find((l) => l.includes("logweir-api started")) || "";

  const browser = await chromium.launch();
  const page = await (await browser.newContext()).newPage();
  const bodies = [];
  page.on("response", async (r) => {
    try {
      if (r.url().indexOf("/api/v1/") !== -1) {
        bodies.push({ url: r.url(), method: r.request().method(), status: r.status(),
          body: (await r.text()).slice(0, 20000) });
      }
    } catch (gone) {
      // body no longer available
    }
  });
  const createAnswer = (ns) => bodies.filter((b) => b.method === "POST" &&
    b.url.endsWith("/namespaces/" + ns + "/restores")).pop();

  async function submitIn(key) {
    const ns = NS[key];
    const route = ui + "#/restore?ns=" + ns + "&backup=" + seeded[key].point.name +
      "&uid=" + seeded[key].point.uid;
    await page.goto(route, { waitUntil: "load", timeout: 30000 });
    await waitFor(page, "#step-target", "the wizard in " + ns);
    await page.selectOption("#target-cluster", seeded[key].targetUid);
    await waitFor(page, "#plan-bytes", "the plan in " + ns);
    const planBytes = await page.evaluate(() => document.querySelector("#plan-bytes").textContent);
    const planStep = await page.evaluate(() => document.querySelector("#step-plan").innerText);
    await page.click("#create-restore");
    let answer = null;
    for (let i = 0; i < 60 && answer === null; i += 1) {
      answer = createAnswer(ns) || null;
      if (answer === null) {
        await pause(500);
      }
    }
    check(answer !== null && (answer.status === 201 || answer.status === 200),
      "the page's create in " + ns + " answered " + JSON.stringify(answer));
    const body = JSON.parse(answer.body);
    return { ns: ns, planBytes: planBytes, planStep: planStep, answer: body,
      restore: body.item.name, approvalName: body.item.approvalRef.name };
  }

  try {
    // ---------------------------------------------------------------- 0
    // The page must be ON the service's origin before it can fetch from it.
    await page.goto(ui, { waitUntil: "load", timeout: 30000 });
    const policies = {};
    for (const [key, ns] of Object.entries(NS)) {
      const read = await page.evaluate(async (u) => {
        const r = await fetch(u);
        return { status: r.status, body: await r.json() };
      }, origin + "/api/v1/namespaces/" + ns + "/approval-policy");
      check(read.status === 200, "the policy route answered " + read.status);
      policies[key] = read.body.item;
    }
    save("00-approval-policies.json", policies);
    check(policies.ordinary.mode === "ordinary" && policies.ordinary.legacy === false, "o is Ordinary");
    check(policies.governed.mode === "governed" && policies.governed.requireDistinctPrincipal === true,
      "v is Governed with distinct principals");
    check(policies.legacy.name === "legacy-governed-v1" && policies.legacy.legacy === true, "g is unbound");
    check(policies.ordinary.installationDigest === policies.governed.installationDigest,
      "one installation document");
    record("GET .../approval-policy publishes each namespace's effective policy", {
      policies: Object.fromEntries(Object.entries(policies).map(([k, v]) => [k, v.name + "/" + v.mode])),
      installationDigest: policies.ordinary.installationDigest,
      confirmationKeyId: policies.ordinary.confirmationKeyId,
    });

    // ---------------------------------------------------------------- 1
    // ORDINARY: the submission routes to EXECUTION.
    const o = await submitIn("ordinary");
    check(o.planStep.toLowerCase().includes("ordinary confirmation"),
      "the submit step said the policy before the click: " + o.planStep.slice(0, 800));
    check(!o.planStep.includes("logweir drill approve"), "and offered no out-of-band approval");
    check(o.answer.authorization && o.answer.authorization.state === "confirmed",
      "the product API answered confirmed: " + JSON.stringify(o.answer.authorization));
    const oHash = await waitForHash(page, "#/history?ns=" + o.ns + "&name=" + o.restore,
      "the ordinary submission's destination");
    await shot(page, "01-ordinary-operation-view");
    const oApproval = kubeJson(["-n", o.ns, "get", "approval", o.approvalName]);
    const oRestore = kubeJson(["-n", o.ns, "get", "restore", o.restore]);
    save("01-ordinary-approval.json", oApproval);
    save("01-ordinary-restore.json", oRestore);
    const doc = JSON.parse(oApproval.spec.approvalBytes);
    const sidecar = JSON.parse(oApproval.spec.sidecarBytes);
    check(sidecar.payloadType === V2_PAYLOAD, "the sidecar is authorization document v2");
    check(doc.subject.uid === oRestore.metadata.uid, "bound to the Restore's UID");
    check(doc.planHash === "sha256:" + createHash("sha256").update(oRestore.spec.planBytes).digest("hex"),
      "bound to the Restore's plan hash");
    check(doc.requester.issuer === "urn:logweir:local-admin" && doc.requester.subject === "admin",
      "the console attested the local administrator");
    check(doc.policy.name === "p192-ordinary" && doc.policy.digest === policies.ordinary.digest,
      "and the bound policy's snapshot digest");
    const signed = verifySig(null, pae(V2_PAYLOAD, Buffer.from(oApproval.spec.approvalBytes)),
      createPublicKey(consolePublicPem), Buffer.from(sidecar.signatures[0].sig, "base64"));
    check(signed, "the console's signature verifies over the exact stored bytes");
    // The lab controller predates PLAT-19.2: record its verdict, whatever it is.
    let labVerdict = null;
    for (let i = 0; i < 30 && labVerdict === null; i += 1) {
      const seen = kubeJson(["-n", o.ns, "get", "approval", o.approvalName]);
      const c = ((seen.status || {}).conditions || []).find((x) => x.type === "Verified");
      labVerdict = c ? { status: c.status, reason: c.reason, message: (c.message || "").slice(0, 300) } : null;
      if (labVerdict === null) {
        await pause(1000);
      }
    }
    const oRestoreNow = kubeJson(["-n", o.ns, "get", "restore", o.restore]);
    const oJobs = (kubeJson(["-n", o.ns, "get", "jobs"]).items || []).map((j) => j.metadata.name);
    save("01-lab-controller-verdict.json", { approval: labVerdict, restoreStatus: oRestoreNow.status || null, jobs: oJobs });
    check(!oJobs.includes(o.restore), "the pre-19.2 lab controller created no Job for a v2 document");
    record("ordinary confirmation: one click, the console signs, the page routes to execution", {
      namespace: o.ns, restore: o.restore, restoreUid: oRestore.metadata.uid, approval: o.approvalName,
      routedTo: oHash, requester: "urn:logweir:local-admin#admin", policy: doc.policy,
      labControllerVerdict: labVerdict,
      labControllerNote: "the lab image predates PLAT-19.2: an old controller refuses every v2 " +
        "document and creates no Job -- D0's rollback rule, observed. Admission by the new " +
        "controller is the lab-refresh row.",
    });

    // ---------------------------------------------------------------- 2
    // GOVERNED (explicit): Awaiting approval, and the requester cannot approve.
    const v = await submitIn("governed");
    check(v.answer.authorization && v.answer.authorization.state === "awaitingApproval" &&
      v.answer.authorization.mode === "governed" && v.answer.authorization.legacy === false,
      "governed answered awaitingApproval: " + JSON.stringify(v.answer.authorization));
    const vHash = await waitForHash(page, "#/approvals?subject=" + v.restore, "the governed destination");
    await waitFor(page, "#countersign-form", "the governed countersign panel");
    await shot(page, "02-governed-awaiting-approval");
    const confirmationName = v.approvalName + "-confirmation";
    const confirmation = kubeJson(["-n", v.ns, "get", "approval", confirmationName]);
    save("02-governed-confirmation.json", confirmation);
    check(kube(["-n", v.ns, "get", "approval", v.approvalName], { expected: [0, 1] }).status === 1,
      "the referenced Approval does not exist until an approver submits");
    // The requester countersigns with a throwaway key and submits through the PAGE.
    const doc2 = join(WORK_DIR, "confirmation.json");
    const conf2 = join(WORK_DIR, "confirmation.sig");
    writeFileSync(doc2, confirmation.spec.approvalBytes);
    writeFileSync(conf2, confirmation.spec.sidecarBytes);
    const throwaway = join(WORK_DIR, "self-approver.pem");
    check(spawnSync("openssl", ["genpkey", "-algorithm", "ed25519", "-out", throwaway], { timeout: 30000 }).status === 0,
      "a throwaway approver key");
    const counter = runCli(["drill", "countersign", "--document", doc2, "--confirmation", conf2,
      "--key", throwaway, "--out", join(WORK_DIR, "self.sig")]);
    check(counter.status === 0, "logweir drill countersign: " + counter.out);
    save("02-countersign-summary.txt", counter.out.replace(/key_id\s+\S+/, "key_id <redacted>"));
    await page.fill("#countersigned-sidecar", readFileSync(join(WORK_DIR, "self.sig"), "utf8"));
    await page.click("#submit-countersignature");
    await waitForText(page, "requested this restore", "the self-approval refusal");
    await shot(page, "02-governed-self-approval-refused");
    const refusal = bodies.filter((b) => b.url.endsWith("/restores/" + v.restore + "/approval")).pop();
    save("02-self-approval-response.json", refusal);
    check(refusal && refusal.status === 403, "the product API refused the requester: " + JSON.stringify(refusal));
    check(kube(["-n", v.ns, "get", "approval", v.approvalName], { expected: [0, 1] }).status === 1,
      "and no Approval was created");
    record("governed: Awaiting approval, and the requester's own countersignature is refused", {
      namespace: v.ns, restore: v.restore, routedTo: vHash, confirmation: confirmationName,
      selfApproval: { status: refusal.status, code: JSON.parse(refusal.body).code },
    });
    control("the requester is refused whatever it holds: localAdmin holds every action, including approval.submit", {});

    // ---------------------------------------------------------------- 3
    // LEGACY (unbound): Awaiting approval, then Verified with a minted Approval.
    const g = await submitIn("legacy");
    check(g.answer.authorization && g.answer.authorization.state === "awaitingApproval" &&
      g.answer.authorization.legacy === true, "unbound answered legacy awaitingApproval");
    check(g.planStep.includes("logweir drill approve"), "the submit step offered today's out-of-band approval");
    const gHash = await waitForHash(page, "#/approvals?subject=" + g.restore, "the legacy destination");
    await waitForText(page, "awaiting approval", "Awaiting approval");
    await shot(page, "03-legacy-awaiting-approval");
    const gRestore = kubeJson(["-n", g.ns, "get", "restore", g.restore]);
    const work = join(WORK_DIR, "mint");
    mkdirSync(work, { recursive: true, mode: 0o700 });
    writeFileSync(join(work, "plan.yaml"), gRestore.spec.planBytes);
    const approve = runCli(["drill", "approve", "--spec", join(work, "plan.yaml"), "--key", APPROVER_KEY,
      "--approver", "plat19-2-live", "--ticket", "P192", "--subject-kind", "Restore",
      "--out", join(work, "approval.json")]);
    check(approve.status === 0, "logweir drill approve failed: " + approve.out.slice(0, 400));
    apply(g.ns, {
      apiVersion: "logweir.dev/v1alpha1", kind: "Approval",
      metadata: { name: g.approvalName, namespace: g.ns, labels: LABELS },
      spec: { subjectRef: { kind: "Restore", name: g.restore },
        planHash: "sha256:" + createHash("sha256").update(gRestore.spec.planBytes).digest("hex"),
        approvalBytes: readFileSync(join(work, "approval.json"), "utf8"),
        sidecarBytes: readFileSync(join(work, "approval.sig"), "utf8") },
    });
    let verified = null;
    for (let i = 0; i < 90 && verified === null; i += 1) {
      const seen = kubeJson(["-n", g.ns, "get", "approval", g.approvalName]);
      if ((seen.status || {}).verified === true) {
        verified = seen;
      } else {
        await pause(1000);
      }
    }
    check(verified !== null, "the lab controller verified the minted Approval");
    save("03-legacy-approval-verified.json", { status: verified.status });
    await page.goto(ui + gHash, { waitUntil: "load" });
    await waitForText(page, "approved: verified by weirkeeper", "the Verified state on the page");
    await shot(page, "03-legacy-verified");
    record("unbound (legacy-governed-v1): Awaiting approval, then Verified with an Approval", {
      namespace: g.ns, restore: g.restore, routedTo: gHash, approval: g.approvalName,
      matchedKeyId: verified.status.matchedKeyId,
    });

    // ---------------------------------------------------------------- 4
    // DRAFT-PREFLIGHT-NEVER-READY: the wizard's own readiness check, run by
    // the lab controller against a REAL archive, answers `approval.state`
    // skipped/SubjectNotCreated for the draft and keeps the aggregate
    // `unknown` -- and the shipped gate lets exactly that verdict submit.
    const r = NS.readiness;
    const rs = seeded.readiness;
    await page.goto(ui + "#/restore?ns=" + r + "&backup=" + rs.point.name + "&uid=" + rs.point.uid,
      { waitUntil: "load", timeout: 30000 });
    await waitFor(page, "#step-target", "the wizard in " + r);
    await page.selectOption("#target-cluster", rs.targetUid);
    await pause(500);
    const prefix = "p192" + suffix + "-";
    await page.fill("#topic-prefix", prefix);
    await page.press("#topic-prefix", "Tab");
    // `windowCovered.toMs` is EXCLUSIVE (WIZ-PIT-EXCLUSIVE-DEFAULT, owned by
    // plat15-2): the last covered millisecond is chosen by hand, and said to be.
    const lastCovered = new Date(rs.window.toMs - 1).toISOString();
    await page.fill("#point-in-time", lastCovered);
    await page.press("#point-in-time", "Tab");
    await pause(1000);
    await waitFor(page, "#plan-bytes", "the plan in " + r);
    const rPlan = await page.evaluate(() => ({
      bytes: document.querySelector("#plan-bytes").textContent,
      hash: (document.querySelector("#plan-hash-value") || {}).textContent.trim(),
    }));
    save("04-draft-plan.txt", rPlan.bytes);
    const before = new Set((kubeJson(["-n", r, "get", "preflights"]).items || []).map((x) => x.metadata.uid));
    await page.click("#restore-readiness-start");
    let pf = null;
    for (let i = 0; i < 150 && pf === null; i += 1) {
      const mine = (kubeJson(["-n", r, "get", "preflights"]).items || [])
        .find((x) => !before.has(x.metadata.uid));
      if (mine && ["Completed", "Failed", "Cancelled"].includes(String((mine.status || {}).phase))) {
        pf = mine;
      } else {
        await pause(2000);
      }
    }
    check(pf !== null, "the lab controller recorded no terminal readiness check within 300 s");
    await shot(page, "04-draft-readiness-started");
    // WHAT THE PAGE ITSELF HOLDS. At this base the wizard keeps the create
    // answer (non-terminal) and never follows the check to its verdict; that
    // follow is PLAT-08.2's (`followRestoreReadiness`, claude/plat08-2, not on
    // main). Recorded, not asserted: the gate under test is fed below with the
    // exact item that follow would hold.
    const pageGate = await page.evaluate(() => {
      const p = document.querySelector("#readiness-blocked");
      return p === null ? null : p.innerText;
    });
    result.pageHeldReadiness = { blockedSentence: pageGate,
      note: "the page's own follow-to-verdict is PLAT-08.2's; see the result file" };
    // THE VERDICT AS THE PRODUCT API SERVES IT TO THE PAGE, bound to this plan.
    const served = await page.evaluate(async (u) => {
      const res = await fetch(u);
      return { status: res.status, body: await res.json() };
    }, origin + "/api/v1/namespaces/" + r + "/preflights/" + pf.metadata.name +
      "?planHash=" + encodeURIComponent(rPlan.hash));
    save("04-draft-preflight-served.json", served);
    save("04-draft-preflight-object.json", { status: pf.status });
    check(served.status === 200, "the product API answered " + served.status);
    const item = served.body.item;
    const rows = (item.checks || []).map((c) => ({ id: c.id, state: c.state, gating: c.gating, code: c.code }));
    const approvalRow = rows.find((c) => c.id === "approval.state");
    check(approvalRow && approvalRow.state === "skipped" && approvalRow.code === "SubjectNotCreated",
      "the controller answered the draft's approval row " + JSON.stringify(approvalRow));
    check(item.state === "unknown", "and kept the aggregate unknown: " + item.state);
    const otherBlocking = rows.filter((c) => c.gating === "blocking" && c.id !== "approval.state" &&
      c.state !== "ready");
    // THE SHIPPED GATE, loaded from the service this run serves, on the served
    // verdict; and the gate at the rebase base, on the same verdict.
    const gate = (moduleUrl, verdict, hash) => page.evaluate(async ([m, v, h]) => {
      const mod = await import(m);
      return mod.readinessRefusal({ readiness: { boundHash: h, preflight: v } }, { hash: h });
    }, [moduleUrl, verdict, hash]);
    const shipped = await gate(origin + "/ui/pages/restore-wizard.js", item, rPlan.hash);
    const flipped = JSON.parse(JSON.stringify(item));
    flipped.state = "notReady";
    flipped.checks.find((c) => c.id === "approval.state").state = "notReady";
    flipped.checks.find((c) => c.id === "approval.state").code = "ApprovalNotVerified";
    const refusedNotReady = await gate(origin + "/ui/pages/restore-wizard.js", flipped, rPlan.hash);
    const other = JSON.parse(JSON.stringify(item));
    const victim = other.checks.find((c) => c.gating === "blocking" && c.id !== "approval.state" &&
      c.state === "ready");
    let refusedOther = null;
    if (victim) {
      victim.state = "unknown";
      victim.code = "BlockedByPrerequisite";
      refusedOther = await gate(origin + "/ui/pages/restore-wizard.js", other, rPlan.hash);
    }
    const baseDir = join(WORK_DIR, "base-ui");
    mkdirSync(baseDir, { recursive: true });
    const archived = spawnSync("bash", ["-c", "git -C " + JSON.stringify(REPO) + " archive " +
      BASE_REV + " ui | tar -x -C " + JSON.stringify(baseDir)], { encoding: "utf8", timeout: 60000 });
    check(archived.status === 0, "git archive of the base ui failed: " + archived.stderr);
    const baseModule = await import(join(baseDir, "ui", "pages", "restore-wizard.js"));
    const atBase = baseModule.readinessRefusal({ readiness: { boundHash: rPlan.hash, preflight: item } },
      { hash: rPlan.hash });
    const gateRecord = { shipped: shipped, atBase: atBase, approvalNotReady: refusedNotReady,
      otherRowUnknown: { row: victim ? victim.id : null, refusal: refusedOther }, baseRev: BASE_REV };
    save("04-draft-gate.json", gateRecord);
    if (otherBlocking.length === 0) {
      check(shipped === null, "the shipped gate refused the draft-shaped live verdict: " + shipped);
      check(typeof atBase === "string" && atBase.includes("approval.state (SubjectNotCreated)"),
        "the base gate refused it for the draft row (the defect, reproduced): " + atBase);
      record("DRAFT-PREFLIGHT-NEVER-READY: the lab controller's draft verdict (every blocking row ready but " +
        "approval.state skipped/SubjectNotCreated, aggregate unknown) passes the shipped gate and was refused at " +
        BASE_REV, {
        namespace: r, preflight: pf.metadata.name, planHash: rPlan.hash, aggregate: item.state,
        approvalRow: approvalRow, blockingReady: rows.filter((c) => c.gating === "blocking" && c.state === "ready").length,
        atBase: atBase,
      });
    } else {
      record("DRAFT-PREFLIGHT-NEVER-READY (partial): the live verdict carries other non-ready blocking rows, " +
        "so the gate refuses naming THEM", { namespace: r, preflight: pf.metadata.name, others: otherBlocking,
        shipped: shipped });
      check(typeof shipped === "string" && otherBlocking.every((c) => shipped.includes(c.id + " (" + c.code + ")")) &&
        !shipped.includes("approval.state"), "the refusal names the other rows and not the draft row: " + shipped);
    }
    check(typeof refusedNotReady === "string" && refusedNotReady.includes("approval.state (ApprovalNotVerified)"),
      "approval.state notReady refuses: " + refusedNotReady);
    control("the same live verdict with approval.state notReady/ApprovalNotVerified refuses by id and code",
      { refusal: refusedNotReady });
    check(victim === undefined || (typeof refusedOther === "string" &&
      refusedOther.includes(victim.id + " (BlockedByPrerequisite)") && !refusedOther.includes("approval.state")),
    "another blocking row unknown refuses naming it: " + refusedOther);
    control("the same live verdict with one other blocking row unknown refuses naming that row only",
      { row: victim ? victim.id : null, refusal: refusedOther });
  } finally {
    save("api-log.txt", apiLog.join("").replace(/-----BEGIN[\s\S]*?-----END[^\n]*\n/g, "<pem redacted>\n"));
    await browser.close();
    stopApi();
  }
}

async function cleanup() {
  if (process.env.UI_E2E_KEEP === "1") {
    result.cleanup.push("kept on request");
    return;
  }
  for (const ns of Object.values(NS)) {
    try {
      assertSafeNamespace(ns);
      const seen = kube(["get", "namespace", ns, "-o", "json"], { expected: [0, 1] });
      if (seen.status !== 0) {
        continue;
      }
      const object = JSON.parse(seen.stdout);
      check(((object.metadata.labels || {})["logweir.dev/test-owner"]) === OWNER,
        "refusing to delete " + ns + ": not labelled " + OWNER_LABEL);
      kube(["delete", "namespace", ns, "--wait=true", "--timeout=180s"], { timeout: 200000 });
      const gone = kube(["get", "namespace", ns], { expected: [0, 1] }).status === 1;
      result.cleanup.push({ namespace: ns, uid: object.metadata.uid, deleted: gone });
    } catch (error) {
      result.cleanup.push({ namespace: ns, error: String(error && error.message) });
    }
  }
  rmSync(WORK_DIR, { recursive: true, force: true });
  result.cleanup.push({ workDir: WORK_DIR, removed: true });
}

let failed = null;
try {
  await main();
} catch (error) {
  failed = error;
  result.error = String(error && error.stack || error);
} finally {
  await cleanup();
  result.finishedAt = new Date().toISOString();
  result.passed = failed === null;
  mkdirSync(ARTIFACTS, { recursive: true });
  save("result.json", result);
  process.stderr.write("result: " + join(ARTIFACTS, "result.json") + "\n");
}
if (failed !== null) {
  process.stderr.write(String(failed && failed.stack || failed) + "\n");
  process.exit(1);
}
