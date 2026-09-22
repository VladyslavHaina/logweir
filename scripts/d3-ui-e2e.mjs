// D3 console live journey harness (PLAT-14.1, PLAT-15.1, PLAT-16.1, PLAT-19.1).
//
// The sibling of `scripts/d2w13-ui-e2e.mjs`, whose launcher this reuses: it
// starts `logweir-api` in localAdmin mode on a loopback port, pointed at this
// worktree's own `ui/` directory and at ONE namespace this run creates, and
// then drives a real Chromium against it. Nothing is intercepted, fabricated
// or delayed: every object is created against the real `logweir-api` and the
// real kube-apiserver, and read back with `kubectl` by name and by UID.
//
// Adopted into the repository at lab-refresh-7 from the uncommitted
// `claude/d3w12-finish-live.mjs`, which proved these journeys live but lived
// outside the tree because `scripts/` was not that worker's to write. The
// journeys and their assertions are unchanged; what changed is the launcher
// (this worktree's `ui/` and binary rather than a hard-coded worktree path),
// the `--namespace` flag, and the cleanup guards d2w13 carries.
//
// Kubernetes: `kubectl --context docker-desktop` only, and no other context.
// The lab release `logweir-scram-local` is READ (its Kafka, its MinIO and its
// Secrets are copied into this run's own namespace); the only cluster-scoped
// object this run creates is one TrustPolicy, taken under the cluster lock and
// deleted at the end after an owner-label check.
//
// Dependencies: Node.js, kubectl, a built `logweir-api`, and Playwright with
// Chromium. Resolve Playwright without a machine-specific path, for example:
//   NODE_PATH="$(npm root -g)" node scripts/d3-ui-e2e.mjs
//
// Usage:
//   node scripts/d3-ui-e2e.mjs [--namespace <ns>]
//
// `--namespace` names the namespace to create and delete. It is checked by
// `assertSafeNamespace` BEFORE anything is created and again before the
// delete, so an operator-supplied name can never point this harness at a
// system namespace or at the shared lab.
//
// Environment (all optional):
//   UI_E2E_OWNER       the `logweir.dev/test-owner` label this run writes and
//                      checks before it deletes anything; default d3.
//   UI_E2E_PREFIX      the namespace prefix; default lw-d3-. Asserted twice:
//                      before anything is created, and again before the delete.
//   UI_E2E_NAMESPACE   the namespace to create and delete; `--namespace` wins.
//   UI_E2E_API_BIN     the logweir-api binary; default target/debug/logweir-api.
//   UI_E2E_UI_DIR      the directory to serve; default this worktree's ui/.
//   UI_E2E_ARTIFACTS   where screenshots, the API log and the result go.
//   UI_E2E_KEEP        "1" keeps the namespace for a look around afterwards.
//   UI_E2E_KUBECTL     the kubectl binary.

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { existsSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const USAGE = "usage: node scripts/d3-ui-e2e.mjs [--namespace <ns>]\n";

// THE FLAG IS MATCHED EXACTLY AND ONLY BY NAME, and a flag with no value is a
// usage error rather than a silently-ignored argument that would leave the
// caller believing it had named a namespace.
const argv = process.argv.slice(2);
let argNamespace = null;
{
  const at = argv.indexOf("--namespace");
  if (at !== -1) {
    if (at + 1 >= argv.length) {
      process.stderr.write(USAGE);
      process.exit(2);
    }
    argNamespace = argv[at + 1];
    argv.splice(at, 2);
  }
  if (argv.length) {
    process.stderr.write("unexpected argument " + JSON.stringify(argv[0]) + "\n" + USAGE);
    process.exit(2);
  }
}

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = process.env.UI_E2E_KUBECTL || "kubectl";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UI_DIR = process.env.UI_E2E_UI_DIR || join(REPO, "ui");
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "debug", "logweir-api");
const OWNER = process.env.UI_E2E_OWNER || "d3";
const LAB = "logweir-scram-local";
const ARTIFACTS_ROOT = process.env.UI_E2E_ARTIFACTS ||
  ("/tmp/logweir-roadmap-run/claude/artifacts/" + OWNER);
const NAMESPACE_PREFIX = process.env.UI_E2E_PREFIX || "lw-d3-";

const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z");
const namespace = argNamespace || process.env.UI_E2E_NAMESPACE ||
  (NAMESPACE_PREFIX + stamp.toLowerCase());

// EVERY RUN WRITES INTO ITS OWN DIRECTORY, NAMED AFTER ITS NAMESPACE, AND
// NEVER OVERWRITES ANOTHER'S -- d2w13's rule, adopted here for the same
// reason: keying the directory on the stamp rather than the namespace lets a
// second run started in the same minute replace the first run's evidence.
const ARTIFACTS = join(ARTIFACTS_ROOT, namespace);
// The temp directory is per-run too, so two runs cannot collide on the config
// or the cursor key, and it is REMOVED in the `finally` below: the key is
// signing material and it must not outlive the process that used it.
const WORK_DIR = join("/tmp", "d3-ui-e2e-" + namespace);
const suffix = Math.random().toString(36).slice(2, 7);
const BUCKET = "d3ui-" + stamp.toLowerCase();
const TRUST_POLICY = "d3ui-" + suffix;

const result = {
  harness: "scripts/d3-ui-e2e.mjs (adopted from claude/d3w12-finish-live.mjs, launcher from scripts/d2w13-ui-e2e.mjs)",
  kubeContext: KUBE_CONTEXT,
  owner: OWNER,
  namespace: namespace,
  namespacePrefix: NAMESPACE_PREFIX,
  bucket: BUCKET,
  trustPolicy: TRUST_POLICY,
  uiDirectory: UI_DIR,
  apiBinary: API_BIN,
  artifacts: ARTIFACTS,
  startedAt: new Date().toISOString(),
  mode: "console (logweir-api, localAdmin, loopback)",
  faultInjection: [],
  journeys: [],
  created: [],
  screenshots: [],
  cleanup: [],
  lock: [],
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

function assertSafeNamespace(ns) {
  check(ns.startsWith(NAMESPACE_PREFIX),
    "this harness only ever touches " + NAMESPACE_PREFIX + "* namespaces, not " + ns);
  check(ns !== "default" && !ns.startsWith("kube-") && !ns.startsWith(LAB),
    "refusing a system or shared-fixture namespace: " + ns);
}

function kube(args, options) {
  const opts = options || {};
  const done = spawnSync(KUBECTL, ["--context", KUBE_CONTEXT].concat(args), {
    encoding: "utf8", input: opts.input,
    timeout: opts.timeout || 60000, maxBuffer: 16 * 1024 * 1024,
  });
  const expected = opts.expected || [0];
  if (!expected.includes(done.status)) {
    throw new Error(KUBECTL + " " + args.join(" ") + " exited " + done.status + ": " +
      String(done.stderr || "").trim().slice(0, 1500));
  }
  return done;
}

const kubeJson = (args) => JSON.parse(kube(args.concat(["-o", "json"])).stdout);
const pause = (ms) => new Promise((r) => setTimeout(r, ms));

/** A TrustPolicy, with every PEM body and the `last-applied-configuration`
 *  annotation removed.
 *
 *  A PUBLIC key is not a credential -- it is the thing an operator compares out
 *  of band, and `docs/keys.md` prints one. But an evidence directory should
 *  carry the key IDs it is about and not the bodies behind them, for the same
 *  reason the roster dump already carries IDs only: a file a person skims
 *  should not be full of base64 nobody is going to read. */
function withoutKeyBodies(policy) {
  const p = policy || {};
  const meta = Object.assign({}, p.metadata || {});
  const annotations = Object.assign({}, meta.annotations || {});
  delete annotations["kubectl.kubernetes.io/last-applied-configuration"];
  meta.annotations = annotations;
  const spec = Object.assign({}, p.spec || {});
  spec.keys = (spec.keys || []).map((k) => {
    const copy = Object.assign({}, k || {});
    delete copy.spkiPem;
    return copy;
  });
  return { metadata: meta, spec: spec, status: p.status };
}

function dump(name, value) {
  const at = join(ARTIFACTS, name);
  mkdirSync(join(at, ".."), { recursive: true });
  writeFileSync(at, typeof value === "string" ? value : JSON.stringify(value, null, 2) + "\n");
  return at;
}

/** Copies one Secret from the lab into this run's namespace WITHOUT reading a
 *  value into this process's own output: the JSON goes straight back to
 *  `kubectl apply` on stdin and is never logged, dumped or asserted on. */
function copySecret(name) {
  const raw = kube(["-n", LAB, "get", "secret", name, "-o", "json"]).stdout;
  const object = JSON.parse(raw);
  const stripped = {
    apiVersion: "v1", kind: "Secret", type: object.type,
    metadata: {
      name: name, namespace: namespace,
      labels: { "logweir.dev/test-owner": OWNER },
    },
    data: object.data,
  };
  kube(["-n", namespace, "apply", "-f", "-"], { input: JSON.stringify(stripped) });
  result.created.push({ kind: "Secret", name: name, note: "copied from " + LAB + ", value never printed" });
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

/** Reads the raw `text/event-stream` bytes of one operation, in the page.
 *
 *  WHY RAW, AND WHY NOT THROUGH THE CONSOLE'S OWN WATCH. The watch CLOSES the
 *  stream the moment a document it decodes is settled, which is correct
 *  behaviour and means the client usually wins the race against the server's
 *  own `end` frame. To see what the wire carries, something has to keep
 *  reading; this does, for a fixed budget, with the same session cookie the
 *  page already holds and no interception of any kind.
 *
 *  Returns `{text, frames, endReason}` -- the literal bytes, the event names
 *  in order, and the `end` frame's reason if one arrived. */
function rawStream(target, url, budgetMs) {
  return target.evaluate(async (args) => {
    const answer = await fetch(args.url, { credentials: "same-origin" });
    const reader = answer.body.getReader();
    const decode = new TextDecoder();
    let text = "";
    const deadline = Date.now() + args.budgetMs;
    while (Date.now() < deadline) {
      const left = deadline - Date.now();
      const chunk = await Promise.race([
        reader.read(),
        new Promise((r) => setTimeout(() => r({ timedOut: true }), left)),
      ]);
      if (chunk.timedOut === true || chunk.done === true) { break; }
      text += decode.decode(chunk.value, { stream: true });
      if (text.indexOf("event: end") !== -1) { break; }
    }
    try { await reader.cancel(); } catch (ignored) { /* the budget is up */ }
    const frames = [];
    let endReason = null;
    for (const block of text.split("\n\n")) {
      const name = /^event: (.+)$/m.exec(block);
      if (name === null) { continue; }
      frames.push(name[1]);
      if (name[1] === "end") {
        const data = /^data: (.+)$/m.exec(block);
        if (data !== null) { endReason = JSON.parse(data[1]).reason; }
      }
    }
    return { status: answer.status, contentType: answer.headers.get("content-type"),
      text: text.slice(0, 40000), frames: frames, endReason: endReason };
  }, { url: url, budgetMs: budgetMs });
}

let page = null;
async function shot(name) {
  const at = join(ARTIFACTS, "shots", name + ".png");
  mkdirSync(join(ARTIFACTS, "shots"), { recursive: true });
  await page.screenshot({ path: at, fullPage: true });
  result.screenshots.push(at);
  return at;
}

async function text() {
  return (await page.evaluate(() => document.body.innerText)).toLowerCase();
}

async function waitForText(needle, label, tries) {
  const wanted = needle.toLowerCase();
  const n = tries || 60;
  for (let i = 0; i < n; i += 1) {
    if ((await text()).includes(wanted)) {
      return;
    }
    await pause(500);
  }
  throw new Error(label + ": never saw " + JSON.stringify(needle) + " at " +
    page.url() + ". Saw:\n" + (await text()).slice(0, 3000));
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
  dump("config.yaml", config);
  api = spawn(API_BIN, ["--config", configPath], { stdio: ["ignore", "pipe", "pipe"] });
  api.stdout.on("data", (b) => apiLog.push(String(b)));
  api.stderr.on("data", (b) => apiLog.push(String(b)));
  for (let i = 0; i < 60; i += 1) {
    try {
      const probe = await fetch("http://127.0.0.1:" + port + "/healthz");
      if (probe.ok) {
        return;
      }
    } catch (notYet) { /* still binding */ }
    await pause(500);
  }
  throw new Error("logweir-api never answered /healthz within 30 s. Log:\n" + apiLog.join(""));
}

function stopApi() {
  if (api !== null && api.exitCode === null) {
    api.kill("SIGTERM");
  }
}

function lock(action, waitMinutes) {
  const argv = ["/tmp/logweir-roadmap-run/claude/k8s-lock.sh", action, OWNER];
  if (waitMinutes !== undefined) {
    argv.push(String(waitMinutes));
  }
  const done = spawnSync("bash", argv, { encoding: "utf8", timeout: 120000 });
  return { ok: done.status === 0, rc: done.status,
    out: String(done.stdout || done.stderr || "").trim().slice(0, 300) };
}

/** Takes the cluster lock WITHOUT BLOCKING NODE'S EVENT LOOP.
 *
 *  `k8s-lock.sh acquire <owner>` waits in a shell `sleep` loop, and `spawnSync`
 *  makes that the whole process's wait: a Chromium connection and a 330-second
 *  raw stream are both in flight by the time this runs, and neither survives
 *  the event loop stopping for minutes. `acquire <owner> 0` is ONE attempt, so
 *  the waiting happens here, in `await`, with everything else still running. */
async function acquireLock(minutes) {
  const tries = Math.max(1, Math.round((minutes * 60) / 15));
  for (let i = 0; i < tries; i += 1) {
    const got = lock("acquire", 0);
    if (got.ok) {
      result.lock.push({ action: "acquire", attempts: i + 1, out: got.out });
      return true;
    }
    if (i === 0) {
      process.stderr.write("== waiting for the cluster lock: " + got.out + "\n");
    }
    await pause(15000);
  }
  result.lock.push({ action: "acquire", attempts: tries, failed: true });
  return false;
}
let holdsLock = false;

// --------------------------------------------------------------- the setup

function labFacts() {
  const roster = kubeJson(["get", "trustroster", "default"]);
  // THE PEM BODIES ARE NOT WRITTEN INTO AN ARTIFACT. They are public key
  // material and harmless, and the rule this run is proving is that no key
  // body reaches a file a person reads -- so the dump carries key IDs only.
  dump("setup/trustroster-before.json", {
    metadata: { name: roster.metadata.name, uid: roster.metadata.uid },
    spec: {
      allowedClusterIds: roster.spec.allowedClusterIds,
      approverKeyIds: (roster.spec.approverKeys || []).map((k) => k.keyId),
      signingKeyIds: (roster.spec.signingKeys || []).map((k) => k.keyId),
    },
    status: roster.status,
  });
  return {
    rosterSuperseded: (roster.status.conditions || []).find((c) => c.type === "Superseded"),
    signingKeyId: roster.spec.signingKeys[0].keyId,
    approverKeyId: roster.spec.approverKeys[0].keyId,
    signingSpki: roster.spec.signingKeys[0].spkiPem,
    approverSpki: roster.spec.approverKeys[0].spkiPem,
    algorithm: "p256",
  };
}

function setUp() {
  assertSafeNamespace(namespace);
  kube(["create", "namespace", namespace]);
  kube(["label", "namespace", namespace, "logweir.dev/test-owner=" + OWNER]);
  const ns = kubeJson(["get", "namespace", namespace]);
  result.namespaceUid = ns.metadata.uid;
  result.created.push({ kind: "Namespace", name: namespace, uid: ns.metadata.uid });

  for (const name of ["logweir-s3", "source-scram", "target-scram", "logweir-signing-key",
    "minio-root"]) {
    copySecret(name);
  }

  // THE RUNNER SERVICE ACCOUNT. The chart installs one per release namespace
  // and the controller names it on every runner Job; a namespace without one
  // has its Jobs refused by the Job controller before a pod exists. It mounts
  // no token, exactly as the lab's own does.
  kube(["-n", namespace, "apply", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "v1", kind: "ServiceAccount",
      metadata: { name: "logweir-runner", namespace: namespace,
        labels: { "logweir.dev/test-owner": OWNER } },
      automountServiceAccountToken: false,
    }),
  });
  result.created.push({ kind: "ServiceAccount", name: "logweir-runner" });

  // The bucket this run writes into, made with the lab's own `mc` image.
  kube(["-n", namespace, "apply", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "v1", kind: "Pod",
      metadata: { name: "d3ui-mc", labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        restartPolicy: "Never",
        containers: [{
          name: "mc", image: "minio/mc:latest",
          // THE IMAGE IS ALREADY ON THIS NODE and Docker Hub refuses an
          // anonymous pull of it here; `IfNotPresent` uses the copy the lab
          // itself runs from rather than dialling out for one.
          imagePullPolicy: "IfNotPresent",
          command: ["/bin/sh", "-c"],
          args: ["mc alias set local http://minio." + LAB + ".svc.cluster.local:9000 " +
            "\"$AWS_ACCESS_KEY_ID\" \"$AWS_SECRET_ACCESS_KEY\" >/dev/null && " +
            "touch /tmp/ready && sleep 5400"],
          env: [
            { name: "AWS_ACCESS_KEY_ID", valueFrom: { secretKeyRef: { name: "logweir-s3", key: "access-key-id" } } },
            { name: "AWS_SECRET_ACCESS_KEY", valueFrom: { secretKeyRef: { name: "logweir-s3", key: "secret-access-key" } } },
          ],
        }],
      },
    }),
  });
  kube(["-n", namespace, "wait", "--for=condition=Ready", "pod/d3ui-mc", "--timeout=180s"],
    { timeout: 200000 });
  kube(["-n", namespace, "exec", "d3ui-mc", "--", "mc", "mb", "--ignore-existing", "local/" + BUCKET],
    { timeout: 60000 });
  result.created.push({ kind: "Bucket(MinIO)", name: BUCKET });

  // The source connection and the destination: the FIXTURE, created with
  // kubectl. What is under test is what the console does with them.
  kube(["-n", namespace, "apply", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
      metadata: { name: "source", labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        bootstrapServers: ["kafka-source." + LAB + ".svc.cluster.local:9096"],
        role: "source",
        auth: { mode: "scramSha512", tls: false, username: "scram-user",
          secretRef: { name: "source-scram" } },
      },
    }),
  });
  result.created.push({ kind: "KafkaCluster", name: "source" });

  const secretKeys = {
    mode: "SecretKeys",
    secret: { name: "logweir-s3", accessKeyIdKey: "access-key-id", secretAccessKeyKey: "secret-access-key" },
  };
  kube(["-n", namespace, "apply", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
      metadata: { name: "primary", labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        description: "d3 console live journey, bucket " + BUCKET,
        storage: { provider: "S3", bucket: BUCKET, prefix: "archive",
          endpoint: "http://minio." + LAB + ".svc.cluster.local:9000",
          region: "us-east-1", addressing: "PathStyle" },
        transport: { security: "InsecureHTTP" },
        readiness: { writeProbe: "Disabled" },
        access: { archiveRead: secretKeys, archiveWrite: secretKeys,
          evidenceRead: secretKeys, evidenceWrite: secretKeys },
      },
    }),
  });
  const dest = kubeJson(["-n", namespace, "get", "backupdestination", "primary"]);
  result.created.push({ kind: "BackupDestination", name: "primary", uid: dest.metadata.uid });
}

function createBackup(name, topics) {
  kube(["-n", namespace, "apply", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "logweir.dev/v1alpha1", kind: "Backup",
      metadata: { name: name, labels: { "logweir.dev/test-owner": OWNER } },
      spec: {
        sourceRef: { name: "source" },
        archive: { url: "s3://" + BUCKET + "/archive", secretRef: { name: "logweir-s3" } },
        topics: topics, triggeredBy: "manual", deadlineSeconds: 900,
      },
    }),
  });
  const object = kubeJson(["-n", namespace, "get", "backup", name]);
  result.created.push({ kind: "Backup", name: name, uid: object.metadata.uid });
  return object;
}

async function waitForBackup(name, predicate, label, seconds) {
  const deadline = Date.now() + (seconds || 600) * 1000;
  let last = null;
  while (Date.now() < deadline) {
    last = kubeJson(["-n", namespace, "get", "backup", name]);
    if (predicate(last)) {
      return last;
    }
    await pause(3000);
  }
  throw new Error(label + ": timed out. Last status:\n" +
    JSON.stringify((last || {}).status, null, 2).slice(0, 2000));
}

// --------------------------------------------------------------- the run

async function main() {
  // BEFORE ANYTHING IS CREATED. `--namespace` puts the name in an operator's
  // hands, so the guard that keeps this harness off `default`, `kube-*` and
  // the shared lab has to run before the first create, not only inside
  // `setUp()`. It runs again at the top of `cleanUp`, which is the call that
  // deletes.
  assertSafeNamespace(namespace);
  mkdirSync(ARTIFACTS, { recursive: true });
  check(existsSync(API_BIN), "the logweir-api binary exists at " + API_BIN +
    " (build it, or point UI_E2E_API_BIN at one)");
  const lab = labFacts();
  result.labRosterSupersededBefore = lab.rosterSuperseded;

  setUp();

  const port = await freePort();
  result.port = port;
  await startApi(port);
  const base = "http://127.0.0.1:" + port + "/ui/";

  const browser = await chromium.launch();
  const context = await browser.newContext({ viewport: { width: 1400, height: 1100 } });
  // EVERY SSE FRAME EVERY TAB RECEIVES, captured in the browser itself.
  //
  // ON THE CONTEXT, NOT ON ONE PAGE. `page.addInitScript` installs this for
  // that page alone, and this run opens three: the journey tab, the raw
  // reader's tab and the tab held on the never-settling Restore. The last one
  // is where the reconnect has to be counted, and with a per-page script its
  // counter did not exist -- which read as "the console did not reconnect"
  // when what had happened was that nothing was counting.
  await context.addInitScript(() => {
    window.__d3uiFrames = [];
    window.__d3uiOpens = 0;
    const Native = window.EventSource;
    window.EventSource = function (url, init) {
      window.__d3uiOpens += 1;
      const source = new Native(url, init);
      const add = source.addEventListener.bind(source);
      source.addEventListener = (type, handler, options) => {
        add(type, (event) => {
          try {
            window.__d3uiFrames.push({ type: type, data: String(event.data).slice(0, 8000),
              at: new Date().toISOString() });
          } catch (ignored) { /* never break the page to observe it */ }
          handler(event);
        }, options);
      };
      return source;
    };
    window.EventSource.prototype = Native.prototype;
  });

  page = await context.newPage();

  const bodies = [];
  page.on("response", async (response) => {
    try {
      const url = response.url();
      // NOT THE STREAM. `response.text()` on a `text/event-stream` resolves
      // only when the body ends, which is 300 s away; awaiting it here would
      // stall this handler for the whole connection.
      if (url.indexOf("/api/v1/") !== -1 && url.indexOf("/events") === -1) {
        bodies.push({ url: url, status: response.status(),
          body: (await response.text()).slice(0, 200000) });
      }
    } catch (gone) { /* a body that is gone cannot hide what was rendered */ }
  });
  const frames = [];

  try {
    // =====================================================================
    // 1. THE OPERATION VIEW OF A REAL BACKUP
    // =====================================================================
    const backupName = "d3ui-backup-" + suffix;
    const backup = createBackup(backupName, ["orders", "payments"]);
    const route = "#/operations?ns=" + namespace + "&kind=backup&name=" + backupName +
      "&uid=" + backup.metadata.uid;

    await page.goto(base + route, { waitUntil: "load", timeout: 30000 });
    await waitForText("Operation", "the operation route");
    await waitForText(backupName, "the operation's own name");
    await shot("01-operation-first-read");
    const firstText = await text();
    check(firstText.includes("following this operation as a stream"),
      "console mode follows this operation as a STREAM, not by re-reading. Saw:\n" +
        firstText.slice(0, 1200));
    record("the operation view opens in console mode and follows the run as a stream", {
      route: route, namespace: namespace, name: backupName, uid: backup.metadata.uid,
      evidence: "shots/01-operation-first-read.png",
    });

    // ---- A REFRESH MID-RUN KEEPS ONE OPERATION, and creates none.
    const beforeRefresh = kubeJson(["-n", namespace, "get", "backup", backupName]);
    check(!["Succeeded", "Failed", "Refused"].includes((beforeRefresh.status || {}).phase || ""),
      "the refresh happens while the run is still going");
    await shot("02-operation-mid-run");
    const before = kube(["-n", namespace, "get", "backups", "-o", "name"]).stdout.trim();
    await page.reload({ waitUntil: "load", timeout: 30000 });
    await waitForText(backupName, "the same operation after a refresh");
    const after = kube(["-n", namespace, "get", "backups", "-o", "name"]).stdout.trim();
    check(before === after,
      "a refresh mid-run creates no second operation: before=" + before + " after=" + after);
    check(after.split("\n").length === 1, "exactly one Backup exists: " + after);
    await shot("03-operation-after-refresh");
    record("a refresh mid-run keeps ONE operation and creates no second run", {
      phaseAtRefresh: (beforeRefresh.status || {}).phase || "(none yet)",
      backupsBefore: before, backupsAfter: after,
      evidence: "shots/03-operation-after-refresh.png",
    });

    // ---- the states the run actually passes through, as the PAGE shows them
    const statesSeen = [];
    const deadline = Date.now() + 900000;
    while (Date.now() < deadline) {
      const shown = await page.evaluate(() => {
        const node = document.querySelector("p.state");
        return node === null ? null : node.textContent.trim();
      });
      if (shown !== null && shown.length > 0 && statesSeen.indexOf(shown) === -1) {
        statesSeen.push(shown);
      }
      const object = kubeJson(["-n", namespace, "get", "backup", backupName]);
      const status = object.status || {};
      // THE API DECIDES WHEN A RUN HAS SETTLED, NOT THIS HARNESS. A terminal
      // phase is what this loop waits for; whether the verification has
      // settled beside it is `is_settled`'s question and the `end` frame
      // below is its answer.
      if (["Succeeded", "Failed", "Refused"].includes(status.phase || "")) {
        break;
      }
      await pause(2000);
    }

    const finished = await waitForBackup(backupName,
      (o) => ["Succeeded", "Failed", "Refused"].includes(((o.status || {}).phase) || ""),
      "the backup reaching a terminal phase", 900);
    dump("operation/backup-final.json", finished);

    // ---- what the console DID with the stream, and what the WIRE carried
    //
    // THE CONSOLE WINS THE RACE AGAINST `end: settled`, AND THAT IS CORRECT.
    // `watchOperation` closes the connection the moment a document it decoded
    // is settled -- terminal, with the verification verdict in -- so the
    // server's own `end` frame usually arrives at a socket the browser has
    // already closed. The console's evidence is therefore the `operation`
    // frames and the absence of a decode error; the `end` frame's evidence
    // comes from the wire, read below by something that does not close early.
    let captured = [];
    let opens = 0;
    for (let i = 0; i < 180; i += 1) {
      const state = await page.evaluate(() => ({
        frames: window.__d3uiFrames || [], opens: window.__d3uiOpens || 0,
      }));
      captured = state.frames;
      opens = state.opens;
      dump("operation/sse-frames.json", captured);
      if (captured.some((f) => {
        try { return JSON.parse(f.data).terminal === true; } catch (x) { return false; }
      })) { break; }
      await pause(1000);
    }
    check(captured.length > 0, "the stream delivered frames to the page");
    await shot("04-operation-settled");
    frames.push.apply(frames, captured);
    dump("operation/sse-frames.json", captured);

    const operationFrames = captured.filter((f) => f.type === "operation" || f.type === "reset");
    check(operationFrames.length > 0, "the stream delivered at least one operation frame");
    const firstFrame = JSON.parse(operationFrames[0].data);
    check(firstFrame.item === undefined && firstFrame.requestId === undefined,
      "THE RECONCILIATION: an `operation` frame is the BARE view, with no `item` and no " +
        "`requestId`. Got keys: " + Object.keys(firstFrame).join(", "));
    check(typeof firstFrame.name === "string" && firstFrame.name === backupName,
      "and the bare view carries the operation's own fields at the top level");
    const lastFrame = JSON.parse(operationFrames[operationFrames.length - 1].data);
    const settledText = await text();
    check(settledText.indexOf("this page could not read") === -1 &&
      settledText.indexOf("contract failure") === -1,
      "and no frame produced a decode error, which is what the envelope bug produced for EVERY " +
        "frame. Saw:\n" + settledText.slice(0, 1500));
    check(settledText.indexOf(String(lastFrame.state)) !== -1,
      "the page renders the state the last frame carried (" + lastFrame.state + ")");

    // ---- THE WIRE, read without closing early.
    const wire = await rawStream(page, "/api/v1/namespaces/" + namespace +
      "/operations/backup/" + backupName + "/events", 30000);
    dump("operation/wire-settled.json", wire);
    check(wire.status === 200 && String(wire.contentType).indexOf("text/event-stream") === 0,
      "the stream answers text/event-stream. Got " + wire.status + " " + wire.contentType);
    check(wire.frames.indexOf("end") !== -1,
      "the server closes a settled operation's stream with an `end` frame. Frames: " +
        JSON.stringify(wire.frames));
    check(wire.endReason === "settled",
      "and its reason is `settled`. Got: " + JSON.stringify(wire.endReason));
    check(wire.text.indexOf("event: end\ndata: {\"reason\":\"settled\"}") !== -1,
      "THE RECONCILIATION: the `end` frame's whole payload is {\"reason\":\"settled\"} -- no " +
        "document, no envelope. Raw tail:\n" + wire.text.slice(-400));

    record("the operation view renders the run from BARE stream frames, and the wire's `end` " +
      "frame is a reason and not a document", {
      statesRendered: operationFrames.map((f) => { try { return JSON.parse(f.data).state; }
        catch (x) { return "?"; } }),
      operationFrames: operationFrames.length,
      streamOpensInThePage: opens,
      bareViewKeys: Object.keys(firstFrame),
      consoleClosedBeforeTheEndFrame: captured.filter((f) => f.type === "end").length === 0,
      wireFrames: wire.frames,
      wireEndReason: wire.endReason,
      finalPhase: (finished.status || {}).phase,
      verification: lastFrame.verification,
      trust: lastFrame.trust,
      evidence: "operation/sse-frames.json, operation/wire-settled.json, " +
        "shots/04-operation-settled.png",
    });

    // =====================================================================
    // 2. A RESTORE, LABELLED BY ITS TARGET MODE
    // =====================================================================
    // The target cluster is the lab's own `kafka-target`, read-only from this
    // run's namespace, with its SCRAM secret copied in.
    kube(["-n", namespace, "apply", "-f", "-"], {
      input: JSON.stringify({
        apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
        metadata: { name: "target", labels: { "logweir.dev/test-owner": OWNER } },
        spec: {
          bootstrapServers: ["kafka-target." + LAB + ".svc.cluster.local:9096"],
          role: "target",
          auth: { mode: "scramSha512", tls: false, username: "scram-user",
            secretRef: { name: "target-scram" } },
        },
      }),
    });
    result.created.push({ kind: "KafkaCluster", name: "target" });

    // THE PLAN IS NOT A VALID SIGNED PLAN AND THAT IS STATED, NOT HIDDEN.
    // A verified plan needs the whole approval flow (PLAT-15.2's wizard, which
    // is still open); what this row is about is the LABEL, which the published
    // view carries at the top level from the moment the object exists. The
    // controller refuses these runs and the console labels them anyway, which
    // is exactly the case review item 5 is about.
    const planBytes = Buffer.from(JSON.stringify({
      note: "d3-ui-e2e: not a signed plan; the controller is expected to refuse it",
    })).toString("base64");
    const restores = [];
    for (const [name, mode] of [["d3ui-restore-scratch-" + suffix, "scratch"],
      ["d3ui-restore-newtopic-" + suffix, "newTopic"]]) {
      const made = kube(["-n", namespace, "apply", "-f", "-"], {
        input: JSON.stringify({
          apiVersion: "logweir.dev/v1alpha1", kind: "Restore",
          metadata: { name: name, labels: { "logweir.dev/test-owner": OWNER } },
          spec: {
            backupSetRef: backup.metadata.uid,
            deadlineSeconds: 600,
            planBytes: planBytes,
            pointInTime: new Date().toISOString().replace(/\.\d+/, ""),
            sourceArchive: { url: "s3://" + BUCKET + "/archive",
              secretRef: { name: "logweir-s3" } },
            // A RESTORE IS NEVER UNAUTHORIZED -- the CRD refuses one with
            // neither `approvalRef` nor `authorization`. This names an
            // Approval that does not exist, so the controller holds the run
            // at `Pending` with `ApprovalNotVerified` and never starts a Job:
            // a real object, a real refusal, and an operation that never
            // settles, which is exactly what the 300-second ceiling needs.
            approvalRef: { name: "d3ui-no-such-approval" },
            target: { mode: mode, clusterRef: { name: "target" },
              topicNaming: { prefix: "d3ui-restored-" } },
          },
        }),
        expected: [0, 1],
      });
      if (made.status !== 0) {
        throw new Error("the " + mode + " Restore could not be created: " +
          String(made.stderr || "").trim().slice(0, 800));
      }
      const object = kubeJson(["-n", namespace, "get", "restore", name]);
      result.created.push({ kind: "Restore", name: name, uid: object.metadata.uid, mode: mode });
      restores.push({ name: name, mode: mode, uid: object.metadata.uid });
    }

    // ---- THE 300-SECOND CEILING, STARTED HERE AND COLLECTED AT THE END.
    //
    // A Restore with no verified approval never leaves `Pending`, so its
    // stream is never settled and reaches `max_connection` instead. Two things
    // run against it for the next five minutes, in parallel with the rest of
    // this journey: a raw reader, which sees what the wire sends at the
    // ceiling; and a real console tab, whose watch must RE-OPEN the stream
    // rather than stop -- which is the defect this pass fixed.
    let ceiling = null;
    let ceilingPage = null;
    if (restores.length > 0) {
      const held = restores[0];
      const rawPage = await context.newPage();
      await rawPage.goto(base, { waitUntil: "load", timeout: 30000 });
      ceiling = {
        held: held,
        // `.catch` AT THE SITE, NOT AT THE AWAIT. This promise is started here
        // and awaited five minutes later; a rejection in between -- the
        // browser closing because some LATER journey failed -- is an unhandled
        // rejection, which in node kills the process outright and takes
        // `cleanUp` with it. It leaked a namespace, a cluster-scoped
        // TrustPolicy and the cluster lock exactly once, on 2026-09-21.
        reader: rawStream(rawPage, "/api/v1/namespaces/" + namespace +
          "/operations/restore/" + held.name + "/events", 330000)
          .catch((gone) => ({ status: null, contentType: null, text: "",
            frames: [], endReason: null,
            failed: gone instanceof Error ? gone.message : String(gone) })),
        rawPage: rawPage,
      };
      ceilingPage = await context.newPage();
      await ceilingPage.goto(base + "#/operations?ns=" + namespace + "&kind=restore&name=" +
        held.name + "&uid=" + held.uid, { waitUntil: "load", timeout: 30000 });
      result.ceilingStartedAt = new Date().toISOString();
    }

    for (const r of restores) {
      const at = "#/operations?ns=" + namespace + "&kind=restore&name=" + r.name + "&uid=" + r.uid;
      await page.goto(base + at, { waitUntil: "load", timeout: 30000 });
      await waitForText(r.name, "the restore's operation view");
      await pause(3000);
      const shown = await page.evaluate(() => {
        const node = document.querySelector("[data-target-mode]");
        const completion = document.querySelector("section.completion");
        return {
          targetMode: node === null ? null : node.getAttribute("data-target-mode"),
          label: node === null ? null : node.textContent,
          completionPanel: completion === null ? null : completion.textContent,
          body: document.body.innerText,
        };
      });
      dump("restore/" + r.mode + ".json", shown);
      await shot("05-restore-" + r.mode);
      const object = kubeJson(["-n", namespace, "get", "restore", r.name]);
      dump("restore/" + r.mode + "-object.json", object);
      check(shown.targetMode === r.mode,
        "THE RECONCILIATION: the console labels this run `" + r.mode + "` from the top-level " +
          "`targetMode`, with no scorecard in sight. Got: " + JSON.stringify(shown.targetMode));
      if (r.mode === "scratch") {
        check(/rehearsal/i.test(String(shown.label)),
          "and a scratch run is called a rehearsal BEFORE it finishes. Got: " + shown.label);
      }
      record("the restore operation view labels an unfinished run by its target mode: " + r.mode, {
        route: at, name: r.name, uid: r.uid, mode: r.mode,
        targetModeRendered: shown.targetMode,
        label: String(shown.label || "").trim(),
        completionPanelPresent: shown.completionPanel !== null,
        controllerPhase: (object.status || {}).phase || null,
        controllerReason: (object.status || {}).reason || null,
        evidence: "restore/" + r.mode + ".json, restore/" + r.mode + "-object.json, " +
          "shots/05-restore-" + r.mode + ".png",
      });
    }

    // =====================================================================
    // 3. THE PROTECTION PAGE
    // =====================================================================
    const policyName = "d3ui-protect-" + suffix;
    kube(["-n", namespace, "apply", "-f", "-"], {
      input: JSON.stringify({
        apiVersion: "logweir.dev/v1alpha1", kind: "ProtectionPolicy",
        metadata: { name: policyName, labels: { "logweir.dev/test-owner": OWNER } },
        spec: {
          protects: { sourceRef: { name: "source" }, topics: ["orders", "payments"],
            destinationRef: { name: "primary" } },
          objectives: { maxRecoveryPointAgeSeconds: 86400, maxConsecutiveFailedRuns: 2,
            requireVerifiedEvidence: true, requireCatalogAvailability: false },
          evaluationIntervalSeconds: 300,
        },
      }),
    });
    const protectionObject = kubeJson(["-n", namespace, "get", "protectionpolicy", policyName]);
    result.created.push({ kind: "ProtectionPolicy", name: policyName, uid: protectionObject.metadata.uid });

    // (a) THE UNKNOWN CASE, read the moment it exists: nothing has evaluated it.
    await page.goto(base + "#/protection?ns=" + namespace, { waitUntil: "load", timeout: 30000 });
    await waitForText(policyName, "the protection list");
    const unknownText = await text();
    await shot("06-protection-unknown");
    dump("protection/list-unknown.txt", unknownText);
    const statusAtFirstRead = kubeJson(["-n", namespace, "get", "protectionpolicy", policyName]);
    dump("protection/policy-first-read.json", statusAtFirstRead);
    const unevaluatedYet = (statusAtFirstRead.status || {}).evaluatedAt === undefined;
    if (unevaluatedYet) {
      check(unknownText.includes("unknown"),
        "a policy nothing has evaluated reads `unknown`. Saw:\n" + unknownText.slice(0, 1500));
    }
    record("the protection page reads a policy the controller has not evaluated yet", {
      policy: policyName, uid: protectionObject.metadata.uid,
      evaluatedAtFirstRead: !unevaluatedYet,
      sawUnknown: unknownText.includes("unknown"),
      evidence: "protection/list-unknown.txt, shots/06-protection-unknown.png",
    });

    // (b) AFTER THE CONTROLLER EVALUATES.
    let evaluated = null;
    for (let i = 0; i < 80; i += 1) {
      const object = kubeJson(["-n", namespace, "get", "protectionpolicy", policyName]);
      if ((object.status || {}).evaluatedAt !== undefined) {
        evaluated = object;
        break;
      }
      await pause(5000);
    }
    if (evaluated !== null) {
      dump("protection/policy-evaluated.json", evaluated);
      await page.goto(base + "#/protection?ns=" + namespace + "&name=" + policyName,
        { waitUntil: "load", timeout: 30000 });
      await waitForText(policyName, "the protection detail");
      await pause(1500);
      const detail = await text();
      dump("protection/detail-evaluated.txt", detail);
      await shot("07-protection-evaluated");
      record("the protection detail renders the controller's own health and objectives", {
        policy: policyName, health: (evaluated.status || {}).health,
        availabilityBasis: (evaluated.status || {}).availabilityBasis,
        evidence: "protection/detail-evaluated.txt, shots/07-protection-evaluated.png",
      });
    } else {
      result.journeys.push({ journey: "the protection policy was never evaluated by the lab controller",
        policy: policyName, skipped: true });
    }

    // =====================================================================
    // 4. THE CATALOG PAGE
    // =====================================================================
    await page.goto(base + "#/catalog?ns=" + namespace, { waitUntil: "load", timeout: 30000 });
    await waitForText("Connect an existing archive", "the connect form");
    await shot("08-catalog-empty");

    // (a) ONE REFUSAL, RENDERED VERBATIM. A name the API refuses.
    await page.fill("#catalog-name", "Not A DNS Name");
    await page.fill("#catalog-destination", "primary");
    await page.selectOption("#catalog-mode", "full");
    await page.click("form[data-connect-archive] button[type=submit]");
    await pause(2500);
    const refusedText = await text();
    dump("catalog/refusal.txt", refusedText);
    await shot("09-catalog-refusal");
    const refusalBodies = bodies.filter((b) => b.url.includes("/catalogs") && b.status >= 400);
    dump("catalog/refusal-bodies.json", refusalBodies);
    check(refusalBodies.length > 0, "the API refused the submission with a Problem body");
    const problem = JSON.parse(refusalBodies[refusalBodies.length - 1].body);
    const verbatim = String(problem.detail || problem.title || "");
    check(verbatim.length > 0, "the Problem carries a detail this page can render verbatim");
    check(refusedText.includes(String(problem.code || "").toLowerCase()) ||
      refusedText.includes(verbatim.toLowerCase().slice(0, 40)),
      "and the page renders the API's own words, not a sentence of its own. Problem: " +
        JSON.stringify(problem).slice(0, 600) + "\nPage:\n" + refusedText.slice(0, 1500));
    // AND THE PER-FIELD SENTENCE, WHICH IS THE PART THAT WAS MISSING. The form
    // says "fix the fields marked below"; this is the check that something is.
    const fieldMessages = (problem.errors || []).map((e) => String(e.message || ""));
    for (const m of fieldMessages) {
      check(m.length > 0 && refusedText.indexOf(m.toLowerCase()) !== -1,
        "the server's own sentence for the field it names is on screen: " + JSON.stringify(m) +
          "\nPage:\n" + refusedText.slice(0, 2000));
    }
    record("the connect form renders the API's own refusal verbatim, including the sentence for " +
      "the field it names", {
      submitted: "Not A DNS Name",
      problem: problem,
      perFieldMessagesOnScreen: fieldMessages,
      evidence: "catalog/refusal.txt, catalog/refusal-bodies.json, shots/09-catalog-refusal.png",
    });

    // (b) THE HAPPY PATH.
    const catalogName = "d3ui-catalog-" + suffix;
    await page.fill("#catalog-name", catalogName);
    await page.fill("#catalog-destination", "primary");
    await page.selectOption("#catalog-mode", "full");
    await page.click("form[data-connect-archive] button[type=submit]");
    await waitForText(catalogName, "the created catalog");
    await shot("10-catalog-created");
    const catalogObject = kubeJson(["-n", namespace, "get", "recoverycatalog", catalogName]);
    result.created.push({ kind: "RecoveryCatalog", name: catalogName, uid: catalogObject.metadata.uid,
      createdBy: "the console's connect form" });
    dump("catalog/catalog-created.json", catalogObject);
    record("the connect form creates a RecoveryCatalog through the product API", {
      catalog: catalogName, uid: catalogObject.metadata.uid,
      syncMode: (catalogObject.spec || {}).syncMode || (catalogObject.spec || {}).mode,
      destinationRef: (catalogObject.spec || {}).destinationRef,
      evidence: "catalog/catalog-created.json, shots/10-catalog-created.png",
    });

    // (c) THE SYNCED CATALOG: points and signers.
    let synced = null;
    for (let i = 0; i < 90; i += 1) {
      const object = kubeJson(["-n", namespace, "get", "recoverycatalog", catalogName]);
      const status = object.status || {};
      if (status.syncedAt !== undefined && (status.counts || {}).total !== undefined) {
        synced = object;
        break;
      }
      await pause(5000);
    }
    if (synced !== null) {
      dump("catalog/catalog-synced.json", synced);
      await page.goto(base + "#/catalog?ns=" + namespace + "&name=" + catalogName,
        { waitUntil: "load", timeout: 30000 });
      await waitForText(catalogName, "the catalog detail");
      await pause(2500);
      const detail = await text();
      dump("catalog/detail.txt", detail);
      await shot("11-catalog-detail");
      const pointBodies = bodies.filter((b) => b.url.includes("/points"));
      const signerBodies = bodies.filter((b) => b.url.includes("/signers"));
      dump("catalog/points-body.json", pointBodies.slice(-1));
      dump("catalog/signers-body.json", signerBodies.slice(-1));
      // WHAT THE POINT ROW ACTUALLY CARRIES, recorded rather than asserted:
      // the four plan-binding keys are what "Restore this point" is built from,
      // and whether the archive key survives the product's own path redactor
      // is a fact about `crates/logweir`, not about this console.
      let firstPoint = null;
      if (pointBodies.length > 0) {
        try { firstPoint = (JSON.parse(pointBodies[pointBodies.length - 1].body).items || [])[0]; }
        catch (notJson) { firstPoint = null; }
      }
      record("the catalog page renders the synced view: counts, points and signers", {
        catalog: catalogName,
        counts: (synced.status || {}).counts,
        pointsRequests: pointBodies.length, signersRequests: signerBodies.length,
        firstPoint: firstPoint === null ? null : {
          pointId: firstPoint.pointId, availability: firstPoint.availability,
          verification: firstPoint.verification, selectable: firstPoint.selectable,
          receiptKey: firstPoint.receiptKey, manifestKey: firstPoint.manifestKey,
          signerKeyId: firstPoint.signerKeyId,
        },
        receiptKeyWasRedactedByTheProduct: firstPoint !== null &&
          String(firstPoint.receiptKey || "").indexOf("[redacted]") !== -1,
        evidence: "catalog/detail.txt, catalog/points-body.json, catalog/signers-body.json, " +
          "shots/11-catalog-detail.png",
      });
    } else {
      result.journeys.push({ journey: "the catalog never synced within the window",
        catalog: catalogName, skipped: true });
    }

    // =====================================================================
    // 5. THE RETENTION PANEL
    // =====================================================================
    const retentionName = "d3ui-retention-" + suffix;
    kube(["-n", namespace, "apply", "-f", "-"], {
      input: JSON.stringify({
        apiVersion: "logweir.dev/v1alpha1", kind: "RetentionPolicy",
        metadata: { name: retentionName, labels: { "logweir.dev/test-owner": OWNER } },
        spec: {
          destinationRef: { name: "primary" }, catalogRef: { name: catalogName },
          scope: { prefix: "archive" }, mode: "Report",
          rules: { keepLast: 7, minUsablePoints: 1 },
        },
      }),
    });
    const retentionObject = kubeJson(["-n", namespace, "get", "retentionpolicy", retentionName]);
    result.created.push({ kind: "RetentionPolicy", name: retentionName, uid: retentionObject.metadata.uid });
    await page.goto(base + "#/schedules?ns=" + namespace, { waitUntil: "load", timeout: 30000 });
    await waitForText("Schedules", "the schedules route");
    await pause(2500);
    const retentionText = await text();
    dump("retention/schedules.txt", retentionText);
    await shot("12-retention-panel");
    record("the retention panel renders on the schedules page for a Report-mode policy", {
      policy: retentionName, mode: "Report",
      neverDeletesSentence: retentionText.includes("deletes nothing") ||
        retentionText.includes("never deletes"),
      evidence: "retention/schedules.txt, shots/12-retention-panel.png",
    });

    // =====================================================================
    // 6. THE KEYS PAGE — cluster-scoped, under the cluster lock
    // =====================================================================
    // (a) THE LEGACY ROSTER HALF, before any TrustPolicy exists. This is the
    //     KEYSVIEW-ABSENT-VALID surface, live.
    await page.goto(base + "#/keys", { waitUntil: "load", timeout: 30000 });
    await waitForText("Keys", "the keys route");
    await pause(2000);
    const rosterText = await text();
    dump("keys/roster-half.txt", rosterText);
    await shot("13-keys-roster");
    record("the keys page reads the cluster's trust, with no TrustPolicy in the cluster", {
      rosterSupersededBefore: lab.rosterSuperseded,
      evidence: "keys/roster-half.txt, shots/13-keys-roster.png",
    });

    // THE ONE CLUSTER-SCOPED OBJECT, UNDER THE LOCK. Every other object this
    // run creates is in its own namespace and needs no lock.
    check(await acquireLock(40), "the cluster lock was acquired within 40 minutes");
    holdsLock = true;
    kube(["apply", "-f", "-"], {
      input: JSON.stringify({
        apiVersion: "logweir.dev/v1alpha1", kind: "TrustPolicy",
        metadata: { name: TRUST_POLICY, labels: { "logweir.dev/test-owner": OWNER } },
        spec: {
          default: false,
          // SCOPED TO THIS RUN'S OWN NAMESPACE ONLY. Every other namespace in
          // the cluster still resolves to the legacy roster, so no other
          // worker's trust resolution moves (`weirkeeper::trust::resolve_in`).
          namespaces: [namespace],
          keys: [
            { keyId: lab.signingKeyId, spkiPem: lab.signingSpki, algorithm: lab.algorithm,
              state: "Active", usages: ["EvidenceSigning"],
              notBefore: "2026-01-01T00:00:00Z", notAfter: "2027-01-01T00:00:00Z",
              principal: { id: "signing@scram-local.invalid" } },
            // THE RETIRED KEY. Its state is what the keys page must print, and
            // a retirement is a PASS for everything it signed before that
            // instant -- not a downgrade.
            { keyId: lab.approverKeyId, spkiPem: lab.approverSpki, algorithm: lab.algorithm,
              state: "Retired", retiredAt: "2026-09-01T00:00:00Z",
              usages: ["GovernedApproval"],
              notBefore: "2026-01-01T00:00:00Z", notAfter: "2027-01-01T00:00:00Z",
              principal: { id: "approver@scram-local.invalid" } },
          ],
        },
      }),
    });
    const policy = kubeJson(["get", "trustpolicy", TRUST_POLICY]);
    result.created.push({ kind: "TrustPolicy", name: TRUST_POLICY, uid: policy.metadata.uid,
      clusterScoped: true, underLock: true });

    // (b) THE UNKNOWN CASE, read BEFORE the controller writes a status.
    await page.goto(base + "#/keys", { waitUntil: "load", timeout: 30000 });
    // A goto TO THE URL THE PAGE IS ALREADY AT IS A SAME-DOCUMENT NAVIGATION:
    // no `hashchange`, so `ui/app.js` never re-mounts and the previous read
    // stays on screen. The reload is what makes this a fresh read.
    await page.reload({ waitUntil: "load", timeout: 30000 });
    await waitForText(TRUST_POLICY, "the new policy on the keys page", 120);
    const keysUnknown = await text();
    dump("keys/policy-unknown.txt", keysUnknown);
    // SCOPED TO THE KEY TABLE, NOT THE PAGE (review M-2). The keys page carries
    // the sentence "an evaluation this page could not confirm is fresh reads
    // `unknown`, never `valid`" whether or not any row IS unknown, so a
    // page-wide search for the word answers `true` for every policy ever
    // rendered. What the row is about is the EVALUATION column.
    const unknownInTheTable = await page.evaluate(() => {
      const cells = Array.from(document.querySelectorAll("table td"));
      return cells.some((c) => /(^|\s)unknown(\s|$)/i.test(c.textContent.trim()));
    });
    await shot("14-keys-policy-unknown");
    const unevaluated = kubeJson(["get", "trustpolicy", TRUST_POLICY]);
    dump("keys/policy-object-first-read.json", withoutKeyBodies(unevaluated));
    record("the keys page reads the new TrustPolicy", {
      policy: TRUST_POLICY, uid: policy.metadata.uid,
      statusAtFirstRead: unevaluated.status === undefined ? "absent" : "present",
      evaluationAtFirstRead: ((unevaluated.status || {}).evaluatedAt) === undefined
        ? "absent" : "present",
      sawUnknownInTheKeyTable: unknownInTheTable,
      evidence: "keys/policy-unknown.txt, shots/14-keys-policy-unknown.png",
    });

    // (c) ONCE EVALUATED: the retired key reads its state.
    let policyEvaluated = null;
    for (let i = 0; i < 60; i += 1) {
      const object = kubeJson(["get", "trustpolicy", TRUST_POLICY]);
      if ((object.status || {}).evaluatedAt !== undefined) {
        policyEvaluated = object;
        break;
      }
      await pause(5000);
    }
    if (policyEvaluated !== null) {
      dump("keys/policy-evaluated.json", withoutKeyBodies(policyEvaluated));
      await page.goto(base + "#/keys", { waitUntil: "load", timeout: 30000 });
    // A goto TO THE URL THE PAGE IS ALREADY AT IS A SAME-DOCUMENT NAVIGATION:
    // no `hashchange`, so `ui/app.js` never re-mounts and the previous read
    // stays on screen. The reload is what makes this a fresh read.
    await page.reload({ waitUntil: "load", timeout: 30000 });
      await waitForText(TRUST_POLICY, "the evaluated policy", 120);
      await pause(2000);
      const keysText = await text();
      dump("keys/policy-evaluated.txt", keysText);
      await shot("15-keys-policy-evaluated");
      const keyBodies = bodies.filter((b) => b.url.includes("/trust-policies"));
      dump("keys/trust-policies-body.json", keyBodies.slice(-1));
      record("the keys page renders the API's own verdict and the retired key's state", {
        policy: TRUST_POLICY,
        evaluation: (keyBodies.slice(-1)[0] === undefined ? null
          : (JSON.parse(keyBodies.slice(-1)[0].body).items || [])
            .map((p) => ({ name: p.name, evaluation: p.evaluation,
              keys: (p.keys || []).map((k) => ({ keyId: k.keyId, state: k.state,
                effectiveState: k.effectiveState })) }))),
        sawRetired: keysText.includes("retired"),
        evidence: "keys/policy-evaluated.txt, keys/trust-policies-body.json, " +
          "shots/15-keys-policy-evaluated.png",
      });
    } else {
      result.journeys.push({ journey: "the TrustPolicy was never evaluated by the lab controller",
        policy: TRUST_POLICY, skipped: true });
    }

    // (d) THE API'S OWN REFUSAL AND ITS OWN FILTERING, BOTH BY NAME.
    //
    // `localAdmin` HAS NO ROLE SELECTOR — `LocalAdminFile` is
    // `deny_unknown_fields` over `{subject, displayName}` and
    // `LocalAdminAuthorizer` grants every implemented action in every
    // configured namespace — so a viewer or operator identity cannot be minted
    // on this host without an OIDC issuer. What CAN be produced live, and is,
    // are the two branches the keys page actually takes: the cluster route's
    // namespace FILTER (`authorize_cluster`, which serves only policies that
    // govern a namespace this actor administers), and a namespaced refusal
    // rendered from the API's own Problem.
    const otherNs = namespace + "-elsewhere";
    const otherPort = await freePort();
    const otherConfig = join(WORK_DIR, "other.yaml");
    writeFileSync(otherConfig, [
      "mode: localAdmin",
      "listen: \"127.0.0.1:" + otherPort + "\"",
      "publicOrigin: \"http://127.0.0.1:" + otherPort + "\"",
      "uiDirectory: " + UI_DIR,
      "localAdmin:",
      "  subject: elsewhere",
      "  displayName: An administrator of another namespace",
      "namespaces: [" + otherNs + "]",
      "kubernetes:",
      "  source: kubeconfig",
      "  context: " + KUBE_CONTEXT,
      "cursorKeyFile: " + join(WORK_DIR, "cursor.key"),
      "",
    ].join("\n"));
    const other = spawn(API_BIN, ["--config", otherConfig], { stdio: ["ignore", "pipe", "pipe"] });
    const otherLog = [];
    other.stdout.on("data", (b) => otherLog.push(String(b)));
    other.stderr.on("data", (b) => otherLog.push(String(b)));
    let otherUp = false;
    for (let i = 0; i < 40; i += 1) {
      try {
        if ((await fetch("http://127.0.0.1:" + otherPort + "/healthz")).ok) { otherUp = true; break; }
      } catch (notYet) { /* binding */ }
      await pause(500);
    }
    if (otherUp) {
      const otherPage = await context.newPage();
      const otherBodies = [];
      otherPage.on("response", async (r) => {
        try {
          if (r.url().includes("/api/v1/")) {
            otherBodies.push({ url: r.url(), status: r.status(), body: await r.text() });
          }
        } catch (gone) { /* nothing */ }
      });

      // (d1) THE KEYS PAGE, FILTERED. This actor administers another namespace,
      // so the policy that governs THIS one is not served to it at all.
      await otherPage.goto("http://127.0.0.1:" + otherPort + "/ui/#/keys",
        { waitUntil: "load", timeout: 30000 });
      await pause(3500);
      const filteredText = await otherPage.evaluate(() => document.body.innerText);
      dump("keys/filtered.txt", filteredText);
      const trustBodies = otherBodies.filter((b) => b.url.includes("/trust-policies"));
      dump("keys/filtered-bodies.json", trustBodies);
      let served = null;
      if (trustBodies.length > 0) {
        try { served = JSON.parse(trustBodies[trustBodies.length - 1].body); }
        catch (notJson) { served = trustBodies[trustBodies.length - 1].body.slice(0, 600); }
      }
      let at = join(ARTIFACTS, "shots", "16-keys-filtered.png");
      await otherPage.screenshot({ path: at, fullPage: true });
      result.screenshots.push(at);
      record("the keys page is served only the policies this actor administers, and says the " +
        "list is partial", {
        actorNamespaces: [otherNs], port: otherPort,
        statuses: trustBodies.map((b) => b.status),
        policiesServed: served === null || served.items === undefined
          ? served
          : served.items.map((p) => ({ name: p.name, namespacesFiltered: p.namespacesFiltered })),
        thisRunsPolicyServed: served !== null && served.items !== undefined &&
          served.items.some((p) => p.name === TRUST_POLICY),
        evidence: "keys/filtered.txt, keys/filtered-bodies.json, shots/16-keys-filtered.png",
      });

      // (d2) A NAMESPACED D3 ROUTE FOR A NAMESPACE THIS ACTOR IS NOT BOUND TO:
      //      the API's own Problem, rendered by the page, by its own name.
      await otherPage.goto("http://127.0.0.1:" + otherPort + "/ui/#/protection?ns=" + namespace,
        { waitUntil: "load", timeout: 30000 });
      await pause(3500);
      const refusedNs = await otherPage.evaluate(() => document.body.innerText);
      dump("keys/namespace-refusal.txt", refusedNs);
      const refusedBodies = otherBodies.filter((b) =>
        b.url.includes("/protection-policies") && b.status >= 400);
      dump("keys/namespace-refusal-bodies.json", refusedBodies);
      at = join(ARTIFACTS, "shots", "17-namespace-refusal.png");
      await otherPage.screenshot({ path: at, fullPage: true });
      result.screenshots.push(at);
      let refusal = null;
      if (refusedBodies.length > 0) {
        try { refusal = JSON.parse(refusedBodies[refusedBodies.length - 1].body); }
        catch (notJson) { refusal = refusedBodies[refusedBodies.length - 1].body.slice(0, 600); }
      }
      const renderedByName = refusal !== null && (
        refusedNs.toLowerCase().includes(String((refusal || {}).code || "zzz").toLowerCase()) ||
        refusedNs.includes(String((refusal || {}).status || "zzz")));
      record("a namespace this actor is not bound to: what the API answered and what the page " +
        "put on screen", {
        actorNamespaces: [otherNs], askedFor: namespace,
        statuses: refusedBodies.map((b) => b.status), problem: refusal,
        pageRendersTheApisOwnWords: renderedByName,
        evidence: "keys/namespace-refusal.txt, keys/namespace-refusal-bodies.json, " +
          "shots/17-namespace-refusal.png",
      });
      await otherPage.close();
    } else {
      result.journeys.push({ journey: "the second API process never started", skipped: true,
        log: otherLog.join("").slice(-2000) });
    }
    if (other.exitCode === null) {
      other.kill("SIGTERM");
    }
    result.otherActorLog = otherLog.join("").slice(-4000);
    // =====================================================================
    // 7. THE 300-SECOND CEILING, COLLECTED
    // =====================================================================
    if (ceiling !== null) {
      const wire = await ceiling.reader;
      dump("operation/wire-maxduration.json", wire);
      // THE RECONNECT IS NOT INSTANT, AND THE PRODUCT SAYS SO. `ui/operation-watch.js`
      // exports `BACKOFF_MS = [1000, 2000, 5000, 30000]` and reconnects after
      // `backoffFor(0, jitter)` -- at least a second, deliberately jittered so
      // two tabs do not reconnect in lockstep. This read used to happen in the
      // same tick as the raw stream's `end` frame, so it could only ever
      // observe the FIRST open and the row below could only ever fail: on
      // 2026-09-22 it failed twice running with `Opens: 1` against a console
      // whose reconnect works. THE ASSERTION IS UNCHANGED -- still `>= 2`, a
      // re-open or a failure -- and only the WAIT is added; polling stops the
      // moment the second open lands, so a console that never reconnects still
      // fails, and takes the full window to do it.
      const reopenDeadline = Date.now() + 30000;
      let opensNow = await ceilingPage.evaluate(() => window.__d3uiOpens || 0);
      while (opensNow < 2 && Date.now() < reopenDeadline) {
        await pause(500);
        opensNow = await ceilingPage.evaluate(() => window.__d3uiOpens || 0);
      }
      const ceilingText = await ceilingPage.evaluate(() => document.body.innerText);
      dump("operation/ceiling-page.txt", ceilingText);
      const at = join(ARTIFACTS, "shots", "18-ceiling-page.png");
      await ceilingPage.screenshot({ path: at, fullPage: true });
      result.screenshots.push(at);
      result.ceiling = {
        restore: ceiling.held.name, uid: ceiling.held.uid,
        startedAt: result.ceilingStartedAt,
        wireFrames: wire.frames, wireEndReason: wire.endReason,
        streamOpensInTheConsoleTab: opensNow,
      };
      check(wire.frames.indexOf("heartbeat") !== -1,
        "a quiet stream is kept alive by heartbeats. Frames: " + JSON.stringify(wire.frames));
      check(wire.endReason === "maxDuration",
        "an operation that never settles reaches the CONNECTION's ceiling and the server says " +
          "so. Got: " + JSON.stringify(wire.endReason) + " after " +
          JSON.stringify(wire.frames));
      check(opensNow >= 2,
        "THE FIX, LIVE: the console RE-OPENS the stream after a `maxDuration` close instead of " +
          "stopping. Opens: " + opensNow);
      record("a `maxDuration` close is the connection's ceiling, not the end of the run: the " +
        "wire says so and the console reconnects", {
        restore: ceiling.held.name, uid: ceiling.held.uid,
        startedAt: result.ceilingStartedAt,
        wireFrames: wire.frames, wireEndReason: wire.endReason,
        endPayload: (/event: end\ndata: (.+)/.exec(wire.text) || [])[1] || null,
        streamOpensInTheConsoleTab: opensNow,
        evidence: "operation/wire-maxduration.json, operation/ceiling-page.txt, " +
          "shots/18-ceiling-page.png",
      });
      await ceilingPage.close();
      await ceiling.rawPage.close();
    }
  } finally {
    try {
      dump("api.log", apiLog.join(""));
      dump("api-bodies.json", bodies.map((b) => ({ url: b.url, status: b.status,
        bytes: b.body.length })));
      dump("api-bodies-full.json", bodies);
      dump("sse-frames-all.json", frames);
    } catch (ignored) { /* the failure below matters more */ }
    await page.context().browser().close().catch(() => {});
  }
}

// --------------------------------------------------------------- cleanup

let cleaned = false;

function cleanUp(how) {
  // ONCE. Both the success and the failure arm below call this, and a throw
  // inside the success arm would otherwise re-run every delete against a
  // namespace that is already terminating.
  if (cleaned) {
    return;
  }
  cleaned = true;
  // THE SECOND ASSERTION, and the one that matters: this is the call that
  // deletes. `--namespace` and UI_E2E_NAMESPACE both land here.
  //
  // IT REFUSES, IT DOES NOT THROW. `cleanUp` runs from the rejection arm too,
  // and the one run that reaches it with an unsafe name is the run that failed
  // BECAUSE the name was unsafe -- so throwing here would replace that error
  // with a second copy of itself, raised where nothing can catch it. A
  // namespace this harness must not touch is a namespace it must not delete:
  // the refusal is the correct outcome and it is recorded.
  try {
    assertSafeNamespace(namespace);
  } catch (unsafe) {
    result.cleanup.push({ namespace: namespace, how: how, refused: true,
      why: String(unsafe && unsafe.message) });
    return;
  }
  if (process.env.UI_E2E_KEEP === "1") {
    result.cleanup.push({ namespace: namespace, how: how, kept: true,
      why: "UI_E2E_KEEP=1; delete it by hand after looking around" });
    return;
  }
  try {
    const found = kube(["get", "namespace", namespace, "-o", "json"], { expected: [0, 1] });
    if (found.status !== 0) {
      result.cleanup.push({ namespace: namespace, how: how, missing: true });
    } else {
      const object = JSON.parse(found.stdout);
      if (object.metadata.uid !== result.namespaceUid) {
        result.cleanup.push({ namespace: namespace, how: how, refused: true,
          why: "the namespace under this name is not the one this run created" });
      } else if ((object.metadata.labels || {})["logweir.dev/test-owner"] !== OWNER) {
        result.cleanup.push({ namespace: namespace, how: how, refused: true,
          why: "the namespace does not carry this run's owner label" });
      } else {
        const before = kube(["-n", namespace, "get",
          "backups,restores,recoverycatalogs,protectionpolicies,retentionpolicies," +
          "backupdestinations,kafkaclusters,secrets,pods", "-o", "name"],
          { expected: [0, 1] }).stdout;
        try {
          kube(["-n", namespace, "exec", "d3ui-mc", "--", "mc", "rb", "--force",
            "local/" + BUCKET], { timeout: 120000, expected: [0, 1] });
        } catch (bucket) { /* recorded below by the listing */ }
        kube(["delete", "namespace", namespace, "--wait=true"], { timeout: 300000 });
        const after = kube(["get", "namespace", namespace], { expected: [0, 1] });
        result.cleanup.push({ namespace: namespace, how: how, uid: result.namespaceUid,
          ownerLabel: OWNER,
          objectsBefore: before.trim().split("\n").filter((l) => l.length > 0),
          deleted: after.status !== 0,
          othersUntouched: kube(["get", "namespaces", "-o", "name"]).stdout.trim().split("\n") });
      }
    }
  } catch (failed) {
    result.cleanup.push({ namespace: namespace, how: how,
      error: failed instanceof Error ? failed.message : String(failed) });
  }
  // The one cluster-scoped object, by UID and owner label.
  try {
    const found = kube(["get", "trustpolicy", TRUST_POLICY, "-o", "json"], { expected: [0, 1] });
    if (found.status === 0) {
      const object = JSON.parse(found.stdout);
      if ((object.metadata.labels || {})["logweir.dev/test-owner"] === OWNER) {
        kube(["delete", "trustpolicy", TRUST_POLICY, "--wait=true"], { timeout: 120000 });
        result.cleanup.push({ trustPolicy: TRUST_POLICY, uid: object.metadata.uid, deleted: true });
      } else {
        result.cleanup.push({ trustPolicy: TRUST_POLICY, refused: true,
          why: "no owner label" });
      }
    } else {
      result.cleanup.push({ trustPolicy: TRUST_POLICY, missing: true });
    }
    const policiesAfter = kube(["get", "trustpolicies", "-o", "name"], { expected: [0, 1] }).stdout;
    result.cleanup.push({ trustPoliciesAfter: policiesAfter.trim() });
    const roster = kube(["get", "trustroster", "default", "-o", "json"], { expected: [0, 1] });
    if (roster.status === 0) {
      const object = JSON.parse(roster.stdout);
      result.labRosterSupersededAfter =
        (object.status.conditions || []).find((c) => c.type === "Superseded");
      dump("setup/trustroster-after.json", object);
    }
  } catch (failed) {
    result.cleanup.push({ trustPolicy: TRUST_POLICY,
      error: failed instanceof Error ? failed.message : String(failed) });
  }
  if (holdsLock) {
    result.lock.push(Object.assign({ action: "release" }, lock("release")));
    holdsLock = false;
  }
}

function writeResult() {
  result.finishedAt = new Date().toISOString();
  result.passed = result.journeys.filter((j) => j.skipped !== true).length;
  mkdirSync(ARTIFACTS, { recursive: true });
  const at = join(ARTIFACTS, "live.json");
  if (existsSync(at)) {
    throw new Error("a result document already exists at " + at + "; this harness never " +
      "overwrites one.");
  }
  writeFileSync(at, JSON.stringify(result, null, 2) + "\n");
  return at;
}

function shutDown() {
  stopApi();
  try {
    rmSync(WORK_DIR, { recursive: true, force: true });
  } catch (ignored) { /* recorded by the result document's path */ }
}

main().then(
  () => {
    shutDown();
    cleanUp("passed");
    const at = writeResult();
    process.stderr.write("\n== " + result.passed + " journey(s) passed; result: " + at + "\n");
    process.exit(0);
  },
  (error) => {
    shutDown();
    result.failure = error instanceof Error ? error.stack : String(error);
    // THE API LOG IS EVIDENCE PRECISELY WHEN THE RUN FAILED. `main()`'s own
    // `finally` dumps it, but a throw raised outside that `try` -- in
    // `labFacts()`, in `assertSafeNamespace`, in the launcher -- would lose
    // it, which is the run whose log is most worth having.
    try {
      dump("api.log", apiLog.join(""));
    } catch (ignored) { /* the failure below is what matters */ }
    cleanUp("failed");
    try {
      const at = writeResult();
      process.stderr.write("== result: " + at + "\n");
    } catch (ignored) { /* the failure below is what matters */ }
    process.stderr.write("\n== FAILED: " + String(error && error.message) + "\n");
    process.exit(1);
  },
);
