// PREFLIGHT-TRUSTROSTER-STALE live harness: a restore readiness verdict the
// lab controller records with a `TrustRoster/default` referent is served
// FRESH by the product API, and the wizard's Create is then allowed for a
// draft -- and when the API's principal may NOT read the roster, the same
// verdict is served stale (`unverifiable`), which is the fail-closed half.
//
// THE FIXTURES ARE `scripts/plat19-2-ui-e2e.mjs`'s journey 4. One namespace
// this run creates (`lw-ts-<stamp>`, label `logweir.dev/test-owner=trust-stale`)
// holds a KafkaCluster pair over the lab's brokers, a BackupDestination over
// the lab's OWN bucket/prefix (read only; its Secret copied, never printed) and
// ONE fixture Backup (a name over 63 characters, which the controller refuses
// before it creates anything) whose status names the newest Succeeded lab run's
// backupId and covered window -- so the archive the readiness check reads is
// REAL. Every other object is created by the page or reconciled by the lab's
// `weirkeeper` (read only).
//
// TWO `logweir-api` PROCESSES, both source-built from this worktree, both in
// localAdmin mode on loopback:
//
//   * ADMIN: the kubeconfig's own identity (docker-desktop). The browser
//     journey runs against it: readiness check, re-read, Create.
//   * SA: the kubeconfig is a ServiceAccount token in this run's namespace,
//     bound to COPIES of the chart's rendered API roles (`logweir-api`,
//     `logweir-api-trustpolicies`, `logweir-api-trustroster`, renamed
//     `<ns>-api*` and labelled). This is the rendered grant, live: the
//     `kubectl auth can-i` matrix is asked as that account, it reads the
//     verdict fresh, and after its roster ClusterRoleBinding is deleted it
//     reads the SAME verdict `unverifiable` -> stale.
//
// Creating the three ClusterRoles and two ClusterRoleBindings is a cluster-
// scoped change, so it runs under `/tmp/logweir-roadmap-run/claude/k8s-lock.sh`
// and every object is deleted (owner label checked) before the lock is released.
//
//   NODE_PATH="$(npm root -g)" node scripts/trust-stale-ui-e2e.mjs
//
// Environment (all optional): UI_E2E_OWNER (trust-stale), UI_E2E_PREFIX (lw-ts-),
// UI_E2E_API_BIN, UI_E2E_UI_DIR, UI_E2E_ARTIFACTS, UI_E2E_KEEP ("1" keeps the
// namespace; the cluster RBAC is always removed).

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = "kubectl";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UI_DIR = process.env.UI_E2E_UI_DIR || join(REPO, "ui");
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "debug", "logweir-api");
const LOCK = "/tmp/logweir-roadmap-run/claude/k8s-lock.sh";
// The owner label (also the cluster-lock owner) and the namespace prefix, set
// the way the sibling harnesses let a second worker set them, so a run is
// provably that worker's own.
const OWNER = process.env.UI_E2E_OWNER || "trust-stale";
const TASK = OWNER;
const NAMESPACE_PREFIX = process.env.UI_E2E_PREFIX || "lw-ts-";
if (!/^lw-[a-z0-9-]*-$/.test(NAMESPACE_PREFIX)) {
  throw new Error("UI_E2E_PREFIX must be an lw-*- test prefix, not " + JSON.stringify(NAMESPACE_PREFIX));
}
const OWNER_LABEL = "logweir.dev/test-owner=" + OWNER;
const LABELS = { "logweir.dev/test-owner": OWNER };
const LAB_NS = "logweir-scram-local";
const LAB_TARGET_BOOTSTRAP = "kafka-target." + LAB_NS + ".svc.cluster.local:9096";
const LAB_SOURCE_BOOTSTRAP = "kafka-source." + LAB_NS + ".svc.cluster.local:9096";
const LAB_SCRAM_USER = "scram-user";
const LAB_TARGET_SECRET = "target-scram";
const LAB_SOURCE_SECRET = "source-scram";
const RENDER = join(REPO, "charts", "logweir", "rendered", "demo.yaml");

const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z").toLowerCase();
const NS = NAMESPACE_PREFIX + stamp;
const ARTIFACTS = join(process.env.UI_E2E_ARTIFACTS ||
  "/tmp/logweir-roadmap-run/claude/artifacts/trust-stale", NS);
const WORK_DIR = join("/tmp", "trust-stale-live-" + stamp);
const suffix = randomBytes(3).toString("hex");
const SA = "api";
const SA_USER = "system:serviceaccount:" + NS + ":" + SA;
// The rendered role name -> this run's copy.
const ROLE_COPIES = {
  "logweir-api": NS + "-api",
  "logweir-api-trustpolicies": NS + "-api-trustpolicies",
  "logweir-api-trustroster": NS + "-api-trustroster",
};

const result = {
  harness: "scripts/trust-stale-ui-e2e.mjs",
  task: "PREFLIGHT-TRUSTROSTER-STALE",
  kubeContext: KUBE_CONTEXT,
  owner: OWNER,
  namespace: NS,
  labNamespace: LAB_NS,
  apiBinary: API_BIN,
  revision: spawnSync("git", ["-C", REPO, "rev-parse", "HEAD"], { encoding: "utf8", timeout: 10000 }).stdout.trim(),
  startedAt: new Date().toISOString(),
  rows: [],
  controls: [],
  created: [],
  screenshots: [],
  lock: [],
  cleanup: [],
};

function check(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function row(name, detail) {
  result.rows.push(Object.assign({ row: name, result: "PASS" }, detail || {}));
  process.stderr.write("== PASS: " + name + "\n");
}

function control(name, detail) {
  result.controls.push(Object.assign({ control: name, result: "PASS" }, detail || {}));
  process.stderr.write("== CONTROL: " + name + "\n");
}

function kube(args, options) {
  const opts = options || {};
  const done = spawnSync(KUBECTL, ["--context", KUBE_CONTEXT].concat(args), {
    encoding: "utf8", input: opts.input, timeout: opts.timeout || 60000, maxBuffer: 16 * 1024 * 1024,
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

function create(object, ns) {
  const args = ns ? ["-n", ns, "create", "-f", "-", "-o", "json"] : ["create", "-f", "-", "-o", "json"];
  return JSON.parse(kube(args, { input: JSON.stringify(object) }).stdout);
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

function save(name, body) {
  const at = join(ARTIFACTS, name);
  writeFileSync(at, typeof body === "string" ? body : JSON.stringify(body, null, 2));
  return at;
}

async function shot(page, name) {
  const at = join(ARTIFACTS, name + ".png");
  await page.screenshot({ path: at, fullPage: true });
  result.screenshots.push(at);
}

async function waitFor(page, selector, label) {
  try {
    await page.waitForSelector(selector, { timeout: 30000 });
  } catch (never) {
    throw new Error(label + ": " + selector + " never appeared");
  }
}

function lock(verb) {
  const done = spawnSync(LOCK, [verb, TASK], { encoding: "utf8", timeout: 1800000 });
  result.lock.push({ verb: verb, status: done.status, at: new Date().toISOString(),
    out: String(done.stdout || "").trim() + String(done.stderr || "").trim() });
  check(done.status === 0, "k8s-lock " + verb + " failed: " + done.stderr);
}

// ------------------------------------------------------------- fixtures

const LONG = "fixture-backup-deliberately-longer-than-sixty-three-characters-";
const rfc = (ms) => new Date(ms).toISOString().replace(".000Z", "Z");

function copyLabSecret(name, as) {
  const source = kubeJson(["-n", LAB_NS, "get", "secret", name]);
  create({ apiVersion: "v1", kind: "Secret", type: source.type || "Opaque",
    metadata: { name: as || name, namespace: NS, labels: LABELS }, data: source.data }, NS);
  result.created.push({ kind: "Secret", namespace: NS, name: as || name,
    note: "copied from the lab's " + name + ", value never printed" });
}

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

async function seedNamespace(lab) {
  kube(["create", "namespace", NS]);
  kube(["label", "namespace", NS, OWNER_LABEL]);
  const made = kubeJson(["get", "namespace", NS]);
  result.created.push({ kind: "Namespace", name: NS, uid: made.metadata.uid });
  create({ apiVersion: "v1", kind: "ServiceAccount",
    metadata: { name: "logweir-runner", namespace: NS, labels: LABELS } }, NS);
  const keyPath = join(WORK_DIR, "signing.pem");
  const minted = spawnSync("openssl", ["genpkey", "-algorithm", "ed25519", "-out", keyPath],
    { encoding: "utf8", timeout: 30000 });
  check(minted.status === 0, "openssl could not mint a signing key");
  kube(["-n", NS, "create", "secret", "generic", "logweir-signing-key", "--from-file=signing.pem=" + keyPath]);
  copyLabSecret(LAB_TARGET_SECRET);
  const sourceSecret = kube(["-n", LAB_NS, "get", "secret", LAB_SOURCE_SECRET], { expected: [0, 1] }).status === 0
    ? LAB_SOURCE_SECRET : LAB_TARGET_SECRET;
  if (sourceSecret === LAB_SOURCE_SECRET) {
    copyLabSecret(LAB_SOURCE_SECRET);
  }
  const cluster = (name, role, bootstrap, secret) => {
    const c = create({ apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
      metadata: { name: name, namespace: NS, labels: LABELS },
      spec: { bootstrapServers: [bootstrap], role: role,
        auth: { mode: "scramSha512", tls: false, username: LAB_SCRAM_USER, secretRef: { name: secret } } } }, NS);
    result.created.push({ kind: "KafkaCluster", namespace: NS, name: name, uid: c.metadata.uid });
    return c;
  };
  cluster("source-" + suffix, "source", LAB_SOURCE_BOOTSTRAP, sourceSecret);
  const target = cluster("target-" + suffix, "target", LAB_TARGET_BOOTSTRAP, LAB_TARGET_SECRET);
  copyLabSecret(lab.secret, "store-" + suffix);
  const dest = create({ apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
    metadata: { name: "dest-" + suffix, namespace: NS, labels: LABELS },
    spec: {
      storage: { provider: "S3", bucket: lab.bucket, prefix: lab.prefix, addressing: "PathStyle",
        endpoint: "http" + "://minio." + LAB_NS + ".svc:9000" },
      transport: { security: "InsecureHTTP" },
      access: { archiveWrite: { mode: "SecretKeys", secret: { name: "store-" + suffix } } },
    } }, NS);
  let frozen = null;
  for (let i = 0; i < 60 && frozen === null; i += 1) {
    const seen = kubeJson(["-n", NS, "get", "backupdestination", "dest-" + suffix]);
    const status = seen.status || {};
    if (typeof status.locationDigest === "string" && status.locationDigest.startsWith("sha256:")) {
      frozen = { name: "dest-" + suffix, uid: seen.metadata.uid,
        generation: seen.metadata.generation, locationDigest: status.locationDigest };
    } else {
      await pause(1000);
    }
  }
  check(frozen !== null, "BackupDestination never published locationDigest");
  result.created.push({ kind: "BackupDestination", namespace: NS, name: dest.metadata.name, uid: dest.metadata.uid });
  const name = NAMESPACE_PREFIX + LONG + suffix;
  const backup = create({ apiVersion: "logweir.dev/v1alpha1", kind: "Backup",
    metadata: { name: name, namespace: NS, labels: LABELS },
    spec: { archive: { url: "logweir-destination://" + frozen.name }, destinationRef: { name: frozen.name },
      deadlineSeconds: 3600, sourceRef: { name: "source-" + suffix }, topics: lab.topics.slice(),
      triggeredBy: "manual" } }, NS);
  const status = {
    phase: "Succeeded", backupId: lab.backupId, records: 1000, exitCode: 0, exitReason: "ok", reason: "Ok",
    manifestKey: lab.prefix + "/" + lab.backupId + "/manifest.json", destination: frozen,
    windowCovered: lab.windowCovered,
    conditions: [{ type: "Complete", status: "True", reason: "Ok", message: "fixture",
      lastTransitionTime: rfc(lab.windowCovered.toMs) }],
  };
  let kept = false;
  for (let i = 0; i < 20 && !kept; i += 1) {
    kube(["-n", NS, "patch", "backup", name, "--subresource=status", "--type=merge",
      "-p", JSON.stringify({ status: status })]);
    await pause(1000);
    const seen = kubeJson(["-n", NS, "get", "backup", name]).status || {};
    kept = seen.phase === "Succeeded" && seen.backupId === status.backupId;
  }
  check(kept, "the fixture Backup did not keep its status");
  result.created.push({ kind: "Backup", namespace: NS, name: name, uid: backup.metadata.uid,
    note: "fixture: status names lab run " + lab.name + "'s backupId and window; the archive is REAL" });
  return { point: { name: name, uid: backup.metadata.uid }, targetUid: target.metadata.uid, window: lab.windowCovered };
}

// ------------------------------------------------------------- the APIs

const children = [];

async function startApi(label, port, kubeconfig) {
  const cursorKey = join(WORK_DIR, "cursor-" + label + ".key");
  writeFileSync(cursorKey, randomBytes(32), { mode: 0o600 });
  const configPath = join(WORK_DIR, "config-" + label + ".yaml");
  const config = [
    "mode: localAdmin",
    "listen: \"127.0.0.1:" + port + "\"",
    "publicOrigin: \"http://127.0.0.1:" + port + "\"",
    "uiDirectory: " + UI_DIR,
    "localAdmin:",
    "  subject: admin",
    "  displayName: Local administrator",
    "namespaces: [" + NS + "]",
    "kubernetes:",
    "  source: kubeconfig",
    "  context: " + KUBE_CONTEXT,
  ].concat(kubeconfig ? ["  kubeconfig: " + kubeconfig] : []).concat([
    "cursorKeyFile: " + cursorKey,
    "",
  ]).join("\n");
  writeFileSync(configPath, config);
  save("config-" + label + ".yaml", config);
  const log = [];
  const child = spawn(API_BIN, ["--config", configPath], { stdio: ["ignore", "pipe", "pipe"] });
  child.stdout.on("data", (b) => log.push(String(b)));
  child.stderr.on("data", (b) => log.push(String(b)));
  children.push({ label: label, child: child, log: log });
  const url = "http://127.0.0.1:" + port;
  for (let i = 0; i < 60; i += 1) {
    try {
      if ((await fetch(url + "/healthz")).ok) {
        return url;
      }
    } catch (notYet) {
      // binding
    }
    await pause(500);
  }
  throw new Error(label + " API never answered /healthz. Log:\n" + log.join(""));
}

async function getJson(url) {
  const r = await fetch(url, { signal: AbortSignal.timeout(30000) });
  return { status: r.status, body: await r.json() };
}

// ------------------------------------------------------ the rendered RBAC

/** The three rendered API ClusterRoles from the checked-in demo render, as
 *  JSON (through `kubectl create --dry-run=client`), renamed and labelled. */
function renderedRoles() {
  const docs = readFileSync(RENDER, "utf8").split(/^---$/m);
  const out = [];
  for (const [rendered, copy] of Object.entries(ROLE_COPIES)) {
    const doc = docs.find((d) => /^kind: ClusterRole$/m.test(d) &&
      new RegExp("^  name: " + rendered + "$", "m").test(d));
    check(doc !== undefined, "the render carries no ClusterRole " + rendered);
    const json = JSON.parse(kube(["create", "--dry-run=client", "-o", "json", "-f", "-"], { input: doc }).stdout);
    json.metadata = { name: copy, labels: Object.assign({}, LABELS, { "logweir.dev/rendered-as": rendered }) };
    out.push(json);
  }
  save("rbac-rendered-copies.json", out);
  return out;
}

function saKubeconfig() {
  const raw = kubeJson(["config", "view", "--raw", "--minify"]);
  const cluster = raw.clusters[0].cluster;
  const token = kube(["-n", NS, "create", "token", SA, "--duration=1h"]).stdout.trim();
  const path = join(WORK_DIR, "sa-kubeconfig.json");
  writeFileSync(path, JSON.stringify({
    apiVersion: "v1", kind: "Config",
    clusters: [{ name: "c", cluster: { server: cluster.server,
      "certificate-authority-data": cluster["certificate-authority-data"] } }],
    users: [{ name: "sa", user: { token: token } }],
    contexts: [{ name: KUBE_CONTEXT, context: { cluster: "c", user: "sa" } }],
    "current-context": KUBE_CONTEXT,
  }), { mode: 0o600 });
  return path;
}

function canI(verb, resource) {
  const done = kube(["auth", "can-i", verb, resource, "--as", SA_USER].concat(
    resource.startsWith("trust") ? [] : ["-n", NS]), { expected: [0, 1] });
  return done.stdout.trim();
}

const MATRIX = [
  ["get", "trustrosters.logweir.dev/default", "yes"],
  ["get", "trustrosters.logweir.dev/other", "no"],
  ["get", "trustrosters.logweir.dev", "no"],
  ["list", "trustrosters.logweir.dev", "no"],
  ["watch", "trustrosters.logweir.dev", "no"],
  ["update", "trustrosters.logweir.dev/default", "no"],
  ["patch", "trustrosters.logweir.dev/default", "no"],
  ["delete", "trustrosters.logweir.dev/default", "no"],
  ["get", "trustpolicies.logweir.dev/any-name", "yes"],
  ["list", "trustpolicies.logweir.dev", "yes"],
  ["create", "trustpolicies.logweir.dev", "no"],
  ["patch", "trustpolicies.logweir.dev/any-name", "no"],
  ["get", "preflights.logweir.dev", "yes"],
  ["get", "secrets", "no"],
  ["get", "configmaps", "yes"],
  ["list", "configmaps", "no"],
];

// ---------------------------------------------------------------- run

let rbacCreated = false;
let lockHeld = false;

async function main() {
  mkdirSync(ARTIFACTS, { recursive: true });
  mkdirSync(WORK_DIR, { recursive: true, mode: 0o700 });
  const labController = kubeJson(["-n", LAB_NS, "get", "deploy", "weirkeeper"]);
  result.labController = { image: labController.spec.template.spec.containers[0].image,
    pods: (kubeJson(["-n", LAB_NS, "get", "pods"]).items || [])
      .filter((p) => p.metadata.name.startsWith("weirkeeper"))
      .map((p) => ({ name: p.metadata.name, imageIDs: ((p.status || {}).containerStatuses || []).map((c) => c.imageID) })) };
  const roster = kubeJson(["get", "trustroster", "default"]);
  result.roster = { name: "default", uid: roster.metadata.uid, generation: roster.metadata.generation };
  result.trustPolicies = (kubeJson(["get", "trustpolicies"]).items || []).map((p) => p.metadata.name);
  const lab = labRecoveryPoint();
  result.labRecoveryPoint = { name: lab.name, backupId: lab.backupId, windowCovered: lab.windowCovered,
    bucket: lab.bucket, prefix: lab.prefix };
  const seeded = await seedNamespace(lab);

  const adminPort = await freePort();
  const admin = await startApi("admin", adminPort);
  const browser = await chromium.launch();
  const bodies = [];
  const page = await (await browser.newContext()).newPage();
  page.on("response", async (r) => {
    try {
      if (r.url().indexOf("/api/v1/") !== -1) {
        bodies.push({ url: r.url(), method: r.request().method(), status: r.status(),
          body: (await r.text()).slice(0, 20000) });
      }
    } catch (gone) {
      // body gone
    }
  });
  try {
    // ------------------------------------------------ 1. the readiness check
    await page.goto(admin + "/ui/#/restore?ns=" + NS + "&backup=" + seeded.point.name + "&uid=" + seeded.point.uid,
      { waitUntil: "load", timeout: 30000 });
    await waitFor(page, "#step-target", "the wizard");
    await page.selectOption("#target-cluster", seeded.targetUid);
    await pause(500);
    await page.fill("#topic-prefix", "ts" + suffix + "-");
    await page.press("#topic-prefix", "Tab");
    await page.fill("#point-in-time", new Date(seeded.window.toMs - 1).toISOString());
    await page.press("#point-in-time", "Tab");
    await pause(1000);
    await waitFor(page, "#plan-bytes", "the plan");
    const planHash = await page.evaluate(() => document.querySelector("#plan-hash-value").textContent.trim());
    result.planHash = planHash;
    await page.click("#restore-readiness-start");
    let pf = null;
    for (let i = 0; i < 150 && pf === null; i += 1) {
      const mine = (kubeJson(["-n", NS, "get", "preflights"]).items || [])[0];
      if (mine && ["Completed", "Failed", "Cancelled"].includes(String((mine.status || {}).phase))) {
        pf = mine;
      } else {
        await pause(2000);
      }
    }
    check(pf !== null, "the lab controller recorded no terminal readiness check within 300 s");
    save("01-preflight-object.json", pf);
    const referents = ((pf.status || {}).binding || {}).referents || [];
    const rosterRef = referents.find((r) => r.kind === "TrustRoster");
    check(rosterRef && rosterRef.name === "default" && rosterRef.uid === result.roster.uid &&
      rosterRef.generation === result.roster.generation,
    "the lab controller recorded TrustRoster/default at the live uid/generation: " + JSON.stringify(referents));
    row("the lab controller (f49849d) recorded TrustRoster/default as a referent of the wizard's readiness check",
      { preflight: pf.metadata.name, uid: pf.metadata.uid, referents: referents });

    // ------------------------------------------ 2. served fresh, on re-read
    const url = admin + "/api/v1/namespaces/" + NS + "/preflights/" + pf.metadata.name +
      "?planHash=" + encodeURIComponent(planHash);
    const first = await getJson(url);
    await pause(3000);
    const second = await getJson(url);
    save("02-served-first.json", first);
    save("02-served-reread.json", second);
    for (const [label, read] of [["first", first], ["re-read", second]]) {
      const item = read.body.item;
      check(read.status === 200, label + ": " + read.status);
      check(item.stale === false && item.applicable === true && item.staleReasons.length === 0,
        label + ": the verdict was not served fresh: " + JSON.stringify({ stale: item.stale,
          applicable: item.applicable, staleReasons: item.staleReasons }));
      check(item.staleBasis.includes("referents:" + referents.length),
        label + ": every referent (the roster included) was compared: " + JSON.stringify(item.staleBasis));
    }
    row("the product API serves the verdict FRESH on read and on re-read (stale=false, applicable=true, " +
      "staleReasons=[], every referent compared)", { state: second.body.item.state,
      staleBasis: second.body.item.staleBasis });

    // ------------------------------------------ 3. the wizard's Create
    let gate = null;
    for (let i = 0; i < 60; i += 1) {
      gate = await page.evaluate(() => ({
        blocked: (document.querySelector("#readiness-blocked") || {}).innerText || null,
        createDisabled: (document.querySelector("#create-restore") || {}).disabled,
      }));
      if (gate.blocked === null && gate.createDisabled === false) {
        break;
      }
      await pause(1000);
    }
    await shot(page, "03-wizard-after-readiness");
    const shipped = await page.evaluate(async ([m, v, h]) => {
      const mod = await import(m);
      return mod.readinessRefusal({ readiness: { boundHash: h, preflight: v } }, { hash: h });
    }, [admin + "/ui/pages/restore-wizard.js", second.body.item, planHash]);
    const approvalRow = (second.body.item.checks || []).find((c) => c.id === "approval.state");
    save("03-gate.json", { page: gate, shippedGateOnServedItem: shipped, aggregate: second.body.item.state,
      approvalRow: approvalRow,
      nonReadyBlocking: (second.body.item.checks || []).filter((c) => c.gating === "blocking" && c.state !== "ready") });
    check(shipped === null, "the shipped gate refused the served verdict: " + shipped);
    check(gate.blocked === null && gate.createDisabled === false,
      "the page did not enable Create after the check: " + JSON.stringify(gate));
    const since = bodies.length;
    await page.click("#create-restore");
    let answer = null;
    for (let i = 0; i < 60 && answer === null; i += 1) {
      answer = bodies.slice(since).filter((b) => b.method === "POST" && b.url.endsWith("/namespaces/" + NS + "/restores")).pop() || null;
      if (answer === null) {
        await pause(500);
      }
    }
    check(answer !== null && (answer.status === 201 || answer.status === 200), "Create answered " + JSON.stringify(answer));
    const created = JSON.parse(answer.body).item;
    await pause(2000);
    await shot(page, "03-after-create");
    const restore = kubeJson(["-n", NS, "get", "restore", created.name]);
    save("03-restore.json", restore);
    result.created.push({ kind: "Restore", namespace: NS, name: created.name, uid: restore.metadata.uid });
    row("the wizard's Create is enabled after the check (draft: approval.state " +
      (approvalRow ? approvalRow.state + "/" + approvalRow.code : "absent") + ", aggregate " +
      second.body.item.state + ") and the page's POST creates the Restore",
    { restore: created.name, uid: restore.metadata.uid, status: answer.status,
      pageHash: await page.evaluate(() => window.location.hash) });

    // ------------------------------------------ 4. the rendered RBAC, live
    lock("acquire");
    lockHeld = true;
    create({ apiVersion: "v1", kind: "ServiceAccount", metadata: { name: SA, namespace: NS, labels: LABELS } }, NS);
    rbacCreated = true;
    for (const role of renderedRoles()) {
      const made = create(role);
      result.created.push({ kind: "ClusterRole", name: made.metadata.name, uid: made.metadata.uid });
    }
    const rb = create({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "RoleBinding",
      metadata: { name: "api", namespace: NS, labels: LABELS },
      roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "ClusterRole", name: ROLE_COPIES["logweir-api"] },
      subjects: [{ kind: "ServiceAccount", name: SA, namespace: NS }] }, NS);
    result.created.push({ kind: "RoleBinding", namespace: NS, name: "api", uid: rb.metadata.uid });
    for (const rendered of ["logweir-api-trustpolicies", "logweir-api-trustroster"]) {
      const crb = create({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRoleBinding",
        metadata: { name: ROLE_COPIES[rendered], labels: LABELS },
        roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "ClusterRole", name: ROLE_COPIES[rendered] },
        subjects: [{ kind: "ServiceAccount", name: SA, namespace: NS }] });
      result.created.push({ kind: "ClusterRoleBinding", name: crb.metadata.name, uid: crb.metadata.uid });
    }
    await pause(2000);
    const matrix = MATRIX.map(([verb, resource, want]) => ({ verb: verb, resource: resource, want: want,
      got: canI(verb, resource) }));
    save("04-can-i.json", { as: SA_USER, matrix: matrix });
    const wrong = matrix.filter((m) => m.got !== m.want);
    check(wrong.length === 0, "can-i disagreed with the rendered grant: " + JSON.stringify(wrong));
    row("kubectl auth can-i as a ServiceAccount bound to the RENDERED API roles: get trustrosters/default " +
      "yes; any other roster, list, watch, write: no", { as: SA_USER, questions: matrix.length });

    const saPort = await freePort();
    const saApi = await startApi("sa", saPort, saKubeconfig());
    const saUrl = saApi + "/api/v1/namespaces/" + NS + "/preflights/" + pf.metadata.name +
      "?planHash=" + encodeURIComponent(planHash);
    const saFresh = await getJson(saUrl);
    save("04-sa-served-with-roster-grant.json", saFresh);
    check(saFresh.status === 200 && saFresh.body.item.stale === false && saFresh.body.item.applicable === true,
      "the API as the rendered principal did not serve the verdict fresh: " +
      JSON.stringify({ status: saFresh.status, stale: (saFresh.body.item || {}).stale,
        staleReasons: (saFresh.body.item || {}).staleReasons }));
    row("the API running AS the rendered principal serves the same verdict fresh", {
      stale: saFresh.body.item.stale, applicable: saFresh.body.item.applicable });

    // ------------------------------------------ 5. fail-closed control
    kube(["delete", "clusterrolebinding", ROLE_COPIES["logweir-api-trustroster"]]);
    await pause(3000);
    const deniedCanI = canI("get", "trustrosters.logweir.dev/default");
    const saDenied = await getJson(saUrl);
    save("05-sa-served-without-roster-grant.json", { canIGetDefault: deniedCanI, served: saDenied });
    const reasons = saDenied.body.item.staleReasons || [];
    check(deniedCanI === "no", "can-i still answers " + deniedCanI + " after the binding was removed");
    check(saDenied.body.item.stale === true && saDenied.body.item.applicable === false &&
      reasons.length === 1 && reasons[0].reason === "unverifiable" && reasons[0].kind === "TrustRoster" &&
      reasons[0].name === "default" && String(reasons[0].basis).includes("could not be read"),
    "a refused roster read did not stay unverifiable: " + JSON.stringify(reasons));
    const refusal = await page.evaluate(async ([m, v, h]) => {
      const mod = await import(m);
      return mod.readinessRefusal({ readiness: { boundHash: h, preflight: v } }, { hash: h });
    }, [admin + "/ui/pages/restore-wizard.js", saDenied.body.item, planHash]);
    check(typeof refusal === "string" && refusal.length > 0, "the gate accepted a stale verdict: " + refusal);
    control("with the roster ClusterRoleBinding removed, the SAME verdict is served stale: unverifiable " +
      "TrustRoster/default (read refused), and the shipped wizard gate refuses it", {
      canIGetDefault: deniedCanI, staleReasons: reasons, gateRefusal: refusal });
  } finally {
    for (const c of children) {
      save("api-" + c.label + ".log", c.log.join(""));
      if (c.child.exitCode === null) {
        c.child.kill("SIGTERM");
      }
    }
    await browser.close();
  }
}

async function cleanup() {
  // The cluster-scoped copies: always removed, owner label checked first.
  if (rbacCreated || lockHeld) {
    for (const kind of ["clusterrolebinding", "clusterrole"]) {
      for (const name of Object.values(ROLE_COPIES)) {
        const seen = kube(["get", kind, name, "-o", "json"], { expected: [0, 1] });
        if (seen.status !== 0) {
          result.cleanup.push({ kind: kind, name: name, present: false });
          continue;
        }
        const obj = JSON.parse(seen.stdout);
        if (((obj.metadata.labels || {})["logweir.dev/test-owner"]) !== OWNER) {
          result.cleanup.push({ kind: kind, name: name, refused: "not labelled " + OWNER_LABEL });
          continue;
        }
        kube(["delete", kind, name]);
        const gone = kube(["get", kind, name], { expected: [0, 1] }).status === 1;
        result.cleanup.push({ kind: kind, name: name, uid: obj.metadata.uid, deleted: gone });
      }
    }
  }
  if (lockHeld) {
    lock("release");
    lockHeld = false;
  }
  if (process.env.UI_E2E_KEEP === "1") {
    result.cleanup.push("namespace kept on request");
  } else {
    const seen = kube(["get", "namespace", NS, "-o", "json"], { expected: [0, 1] });
    if (seen.status === 0) {
      const object = JSON.parse(seen.stdout);
      check(((object.metadata.labels || {})["logweir.dev/test-owner"]) === OWNER,
        "refusing to delete " + NS + ": not labelled " + OWNER_LABEL);
      kube(["delete", "namespace", NS, "--wait=true", "--timeout=180s"], { timeout: 200000 });
      const gone = kube(["get", "namespace", NS], { expected: [0, 1] }).status === 1;
      result.cleanup.push({ namespace: NS, uid: object.metadata.uid, deleted: gone });
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
  try {
    await cleanup();
  } catch (error) {
    result.cleanupError = String(error && error.stack || error);
  }
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
