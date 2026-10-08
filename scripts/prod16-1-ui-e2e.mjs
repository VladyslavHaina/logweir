// PROD-16.1 live host proof: a first restore confirmed in the console with no
// key file handled by any person.
//
// WHAT RUNS FOR REAL, AND WHAT STANDS IN FOR THE CLUSTER'S PARTS:
//
//   * `logweir identity bootstrap` — the REAL binary of this tree, run from
//     the workstation under the token of a ServiceAccount bound to EXACTLY the
//     Role the chart renders for the hook (`helm template … --show-only
//     templates/identity.yaml`), over the empty retained placeholders the chart
//     renders. It generates the installation identity and the console's
//     ConsoleConfirmation key and publishes both public halves. It is NOT given
//     `--installation-trust-policy`: the fresh install's default TrustPolicy is
//     cluster-wide, and this shared cluster already has trust of its own (the
//     PoC's), so the trust step and therefore the fresh-install MARKER are not
//     exercised here — their rows are the hook's, the console's and the
//     controller's unit and mock-cluster rows, and the PoC refresh.
//   * `logweir-api` — the REAL console of this tree, on loopback, in
//     `localAdmin` mode, against docker-desktop through the kubeconfig. Its
//     confirmation key file is the path the chart mounts the managed Secret at;
//     this harness plays the kubelet: it writes the Secret's `confirmation.key`
//     into that path AFTER the console started (the hook is a post-install
//     hook), exactly once, and deletes it at the end.
//   * Chromium (Playwright) clicks through the wizard.
//   * `logweir restore run` — the REAL runner, on the host, over the bundle the
//     console's Approval and the published public halves make, against compose
//     slot 3's brokers and MinIO.
//
// Journeys (one namespace, `lw-prod-16-1-<stamp>`, labelled
// `logweir.dev/test-owner=prod-16-1`, deleted at the end):
//
//   0  bootstrap twice: generated, then existing with the SAME key ids.
//   1  UNMARKED (an upgraded install, or a fresh one whose trust step did not
//      run): the unbound namespace is `legacy-governed-v1`; a marker patched
//      into the identity ConfigMap — bare, or as a claim naming a policy that
//      does not exist — changes nothing (WARN); Create awaits an out-of-band
//      approval and nothing is signed.
//   2  THE DOCUMENTED OPT-IN (`allowOrdinaryConfirmation` + `defaultMode:
//      confirm`), console key not there yet: the step says so, Create is
//      withheld, nothing is sent.
//   3  the "kubelet" projects the hook's key; the SAME console now confirms in
//      one click: an Approval carrying authorization document v2 under
//      `default-confirm-v1`, requester `urn:logweir:local-admin#admin`, signed
//      by the key the hook generated (verified here with node crypto against
//      the PUBLIC half the hook published).
//   4  the real runner accepts that bundle (no authorization refusal) and fails
//      later for a reason that is not the authorization; control: the same
//      bundle beside another policy's snapshot is refused exit 3.
//   5  key loss: the console Secret emptied, the hook refuses to regenerate.
//   6  NO STANDING GRANT ON A FAILED HOOK (fix round, review H1): a real
//      ClusterRoleBinding `<ns>-identity-trust`, bound to the hook's account
//      through a ClusterRole whose ONLY rule is `delete` on that one binding
//      (no TrustPolicy power is ever granted here), is deleted by the hook
//      when a step fails, when its store cannot be opened, and after a usage
//      error; without `--revoke-trust-binding` it stays (control). Both
//      cluster-scoped objects carry the test-owner label and are deleted in
//      cleanup.
//
//   NODE_PATH="$(npm root -g)" node scripts/prod16-1-ui-e2e.mjs
//
// Requires: `eval "$(e2e/compose/stack-env.sh --slot 3 --profiles cluster2)"`
// and `just e2e-up` in this shell's environment; target/debug/{logweir,logweir-api}
// built from this tree.

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer, connect } from "node:net";
import { createHash, createPublicKey, randomBytes, verify as verifySig } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { wizardAt, wizardStep } from "./console-steps.mjs";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "debug", "logweir-api");
const LOGWEIR_BIN = process.env.UI_E2E_LOGWEIR_BIN || join(REPO, "target", "debug", "logweir");
const OWNER = "prod-16-1";
const PREFIX = "lw-prod-16-1-";
const LABELS = { "logweir.dev/test-owner": OWNER };
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z").toLowerCase();
const NS = PREFIX + stamp;
const ARTIFACTS = join(process.env.UI_E2E_ARTIFACTS ||
  "/tmp/logweir-roadmap-run/claude/artifacts/prod-16-1", stamp);
const WORK = process.env.UI_E2E_WORK || join(tmpdir(), "prod16-1-live-" + stamp);
const V2_PAYLOAD = "application/vnd.logweir.restore-authorization+json;version=2.0.0";
const LOCAL_ADMIN = "urn:logweir:local-admin#admin";
const DEFAULT_CONFIRM_SNAPSHOT =
  "{\"formatVersion\":\"1\",\"kind\":\"ApprovalPolicySnapshot\",\"name\":\"default-confirm-v1\"," +
  "\"mode\":\"Ordinary\",\"maxAgeSeconds\":900,\"requireDistinctPrincipal\":false}";
const SOURCE_PORT = process.env.LOGWEIR_E2E_KAFKA_PORT;
const TARGET_PORT = process.env.LOGWEIR_E2E_CLUSTER2_PORT;
const S3_PORT = process.env.LOGWEIR_E2E_S3_PORT;
const TARGET_CLUSTER_ID = (() => {
  try {
    const env = require("node:fs").readFileSync(join(REPO, "e2e", "compose", "slots",
      String(process.env.COMPOSE_PROJECT_NAME), "kafka-cluster2.env"), "utf8");
    return (/^CLUSTER_ID=(.+)$/m.exec(env) || [])[1] || "";
  } catch (absent) {
    return "";
  }
})();

const result = {
  harness: "scripts/prod16-1-ui-e2e.mjs", stamp: stamp, namespace: NS, context: KUBE_CONTEXT,
  composeProject: process.env.COMPOSE_PROJECT_NAME || null, journeys: [], negativeControls: [],
  created: [], screenshots: [], passed: false,
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
function pause(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function save(name, body) {
  const at = join(ARTIFACTS, name);
  writeFileSync(at, typeof body === "string" ? body : JSON.stringify(body, null, 2));
  return at;
}
function kube(args, options) {
  const opts = options || {};
  const done = spawnSync("kubectl", ["--context", KUBE_CONTEXT].concat(args), {
    encoding: "utf8", input: opts.input, timeout: opts.timeout || 60000, maxBuffer: 16 * 1024 * 1024,
  });
  const expected = opts.expected || [0];
  if (!expected.includes(done.status)) {
    throw new Error("kubectl " + args.join(" ") + " exited " + done.status + ": " +
      String(done.stderr || "").trim().slice(0, 1500));
  }
  return done;
}
const kubeJson = (args) => JSON.parse(kube(args.concat(["-o", "json"])).stdout);
function create(object) {
  return JSON.parse(kube(["-n", NS, "create", "-f", "-", "-o", "json"],
    { input: JSON.stringify(object) }).stdout);
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
function reachable(port) {
  return new Promise((ok) => {
    const socket = connect({ host: "127.0.0.1", port: Number(port) });
    const done = (v) => {
      socket.destroy();
      ok(v);
    };
    socket.setTimeout(3000, () => done(false));
    socket.on("connect", () => done(true));
    socket.on("error", () => done(false));
  });
}
function pae(type, body) {
  return Buffer.concat([
    Buffer.from("DSSEv1 " + Buffer.byteLength(type) + " " + type + " " + body.length + " "),
    body,
  ]);
}
const sha256 = (bytes) => "sha256:" + createHash("sha256").update(bytes).digest("hex");

// ------------------------------------------------------------ the hook

const PLACEHOLDERS = [
  ["Secret", "logweir-signing-key"],
  ["ConfigMap", "logweir-signing-trust"],
  ["Secret", "logweir-console-confirmation"],
  ["ConfigMap", "logweir-console-trust"],
];

/** The chart's own render of the hook's objects for this namespace, as JSON. */
function chartIdentityRender() {
  const helm = spawnSync("helm", ["template", "lw", join(REPO, "charts", "logweir"), "-n", NS,
    "--set", "api.enabled=true", "--set", "api.console.enabled=true",
    "--set", "api.console.mode=localAdmin", "--set", "api.console.keySecret=unused",
    "--set", "identity.bootstrapFeatures.consoleKey=true",
    "--show-only", "templates/identity.yaml"], { encoding: "utf8", timeout: 60000 });
  check(helm.status === 0, "helm template failed: " + helm.stderr);
  const asJson = kube(["create", "--dry-run=client", "-o", "json", "-f", "-"], { input: helm.stdout });
  // One JSON object per document, concatenated: split on the top-level braces.
  const objects = [];
  let depth = 0;
  let start = -1;
  let inString = false;
  let escaped = false;
  const text = asJson.stdout;
  for (let i = 0; i < text.length; i += 1) {
    const c = text[i];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (c === "\\") {
        escaped = true;
      } else if (c === "\"") {
        inString = false;
      }
      continue;
    }
    if (c === "\"") {
      inString = true;
    } else if (c === "{") {
      if (depth === 0) {
        start = i;
      }
      depth += 1;
    } else if (c === "}") {
      depth -= 1;
      if (depth === 0) {
        objects.push(JSON.parse(text.slice(start, i + 1)));
      }
    }
  }
  return objects.flatMap((o) => o.items || [o]);
}

function bootstrapEnv(dir) {
  const server = kube(["config", "view", "--minify", "-o",
    "jsonpath={.clusters[0].cluster.server}"]).stdout.trim();
  const url = new URL(server);
  return {
    KUBERNETES_SERVICE_HOST: url.hostname,
    KUBERNETES_SERVICE_PORT_HTTPS: url.port || "443",
    LOGWEIR_SERVICE_ACCOUNT_DIR: dir,
  };
}

function runBootstrap(env, extra) {
  const args = ["identity", "bootstrap", "--namespace", NS,
    "--console-secret-name", "logweir-console-confirmation",
    "--console-public-configmap-name", "logweir-console-trust",
    "--revoke-trust-binding", "lw-identity-trust"].concat(extra || []);
  const done = spawnSync(LOGWEIR_BIN, args, {
    encoding: "utf8", timeout: 120000, env: Object.assign({}, process.env, env),
  });
  return { status: done.status, out: String(done.stdout || ""), err: String(done.stderr || ""), args: args };
}

// ------------------------------------------------------------ the console

let api = null;
const apiLog = [];
async function startApi(port, policyFile) {
  const config = [
    "mode: localAdmin",
    "listen: \"127.0.0.1:" + port + "\"",
    "publicOrigin: \"http://127.0.0.1:" + port + "\"",
    "uiDirectory: " + join(REPO, "ui"),
    "localAdmin:",
    "  subject: admin",
    "  displayName: Local administrator",
    "namespaces: [" + NS + "]",
    "kubernetes:",
    "  source: kubeconfig",
    "  context: " + KUBE_CONTEXT,
    "cursorKeyFile: " + join(WORK, "cursor.key"),
    "confirmationKeyFile: " + join(WORK, "confirmation", "confirmation.key"),
    "confirmationKeyManaged: true",
    "installationIdentity:",
    "  namespace: " + NS,
    "  publicConfigMap: logweir-signing-trust",
  ].concat(policyFile ? ["approvalPolicyFile: " + policyFile] : []).concat([""]).join("\n");
  const configPath = join(WORK, "config-" + port + ".yaml");
  writeFileSync(configPath, config);
  save("config-" + (policyFile ? "opt-in" : "unmarked") + ".yaml", config);
  api = spawn(API_BIN, ["--config", configPath], { stdio: ["ignore", "pipe", "pipe"] });
  api.stdout.on("data", (b) => apiLog.push(String(b)));
  api.stderr.on("data", (b) => apiLog.push(String(b)));
  for (let i = 0; i < 60; i += 1) {
    try {
      if ((await fetch("http://127.0.0.1:" + port + "/healthz")).ok) {
        return;
      }
    } catch (notYet) {
      // binding
    }
    await pause(500);
  }
  throw new Error("the console never answered /healthz:\n" + apiLog.join(""));
}
async function stopApi() {
  if (api !== null && api.exitCode === null) {
    api.kill("SIGTERM");
    for (let i = 0; i < 20 && api.exitCode === null; i += 1) {
      await pause(250);
    }
    if (api.exitCode === null) {
      api.kill("SIGKILL");
    }
  }
  api = null;
}
async function policyView(base) {
  const r = await fetch(base + "/api/v1/namespaces/" + NS + "/approval-policy");
  return { status: r.status, body: await r.json() };
}

// ------------------------------------------------------------ the fixtures

async function seed() {
  kube(["create", "namespace", NS]);
  kube(["label", "namespace", NS, "logweir.dev/test-owner=" + OWNER]);
  result.created.push({ kind: "Namespace", name: NS, uid: kubeJson(["get", "namespace", NS]).metadata.uid });
  const cluster = (name, role, port) => create({
    apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
    metadata: { name: name, namespace: NS, labels: LABELS },
    spec: { bootstrapServers: ["127.0.0.1:" + port], role: role, auth: { mode: "plaintext", tls: false } },
  });
  cluster("slot3-source", "source", SOURCE_PORT);
  const target = cluster("slot3-target", "target", TARGET_PORT);
  kube(["-n", NS, "create", "secret", "generic", "slot3-store",
    "--from-literal=access-key-id=minioadmin", "--from-literal=secret-access-key=minioadmin"]);
  // TWO points, so the unmarked journey and the confirm journey create two
  // different Restores (a Restore's name is minted from its plan bytes, and the
  // same plan submitted again is a replay of the same object).
  const point = (suffix, backupId, fromMs) => {
    const name = "slot3-backup-" + suffix + "-" + stamp;
    const backup = create({
      apiVersion: "logweir.dev/v1alpha1", kind: "Backup",
      metadata: { name: name, namespace: NS, labels: LABELS },
      spec: { archive: { url: "s3://kafka-backups/" + NS, secretRef: { name: "slot3-store" } },
        deadlineSeconds: 3600, sourceRef: { name: "slot3-source" }, topics: ["orders"],
        triggeredBy: "manual" },
    });
    const status = {
      phase: "Succeeded", backupId: backupId, records: 10, exitCode: 0,
      exitReason: "ok", reason: "Ok", manifestKey: NS + "/" + backupId + "/manifest.json",
      windowCovered: { fromMs: fromMs, toMs: fromMs + 60000 },
      conditions: [{ type: "Complete", status: "True", reason: "Ok", message: "fixture",
        lastTransitionTime: new Date(fromMs + 60000).toISOString().replace(".000Z", "Z") }],
    };
    kube(["-n", NS, "patch", "backup", name, "--subresource=status", "--type=merge",
      "-p", JSON.stringify({ status: status })]);
    return { name: name, uid: backup.metadata.uid };
  };
  const legacyPoint = point("a", "01JB7Z000000000000000P161A", 1760000000000);
  const confirmPoint = point("b", "01JB7Z000000000000000P161B", 1760000600000);
  result.fixtures = { backups: [legacyPoint.name, confirmPoint.name],
    note: "two Succeeded fixture Backups over slot 3's MinIO; no archive was written for them",
    source: "127.0.0.1:" + SOURCE_PORT, target: "127.0.0.1:" + TARGET_PORT };
  return { legacyPoint: legacyPoint, confirmPoint: confirmPoint, targetUid: target.metadata.uid };
}

// ------------------------------------------------------------ the run

async function main() {
  check(NS.startsWith(PREFIX), "namespace prefix");
  check(result.composeProject === "logweir-e2e-s3" && SOURCE_PORT && TARGET_PORT && S3_PORT,
    "run under `eval \"$(e2e/compose/stack-env.sh --slot 3 --profiles cluster2)\"`");
  for (const port of [SOURCE_PORT, TARGET_PORT, S3_PORT]) {
    check(await reachable(port), "compose slot 3 does not answer on 127.0.0.1:" + port + " (just e2e-up)");
  }
  mkdirSync(ARTIFACTS, { recursive: true });
  mkdirSync(WORK, { recursive: true, mode: 0o700 });
  mkdirSync(join(WORK, "account"), { recursive: true, mode: 0o700 });
  mkdirSync(join(WORK, "confirmation"), { recursive: true, mode: 0o700 });
  writeFileSync(join(WORK, "cursor.key"), randomBytes(32), { mode: 0o600 });
  const seeded = await seed();

  // ---------------------------------------------------------------- 0
  const render = chartIdentityRender();
  const pick = (kind, name) => render.find((o) => o.kind === kind && o.metadata.name === name);
  for (const [kind, name] of PLACEHOLDERS) {
    const object = pick(kind, name);
    check(object, "the chart renders no placeholder " + kind + "/" + name);
    object.metadata.namespace = NS;
    object.metadata.labels = Object.assign({}, object.metadata.labels, LABELS);
    create(object);
  }
  const role = pick("Role", "lw-identity-bootstrap");
  const sa = pick("ServiceAccount", "lw-identity-bootstrap");
  const binding = pick("RoleBinding", "lw-identity-bootstrap");
  check(role && sa && binding, "the chart renders the hook's account, Role and RoleBinding");
  for (const object of [sa, role, binding]) {
    object.metadata.namespace = NS;
    create(object);
  }
  save("00-hook-role.json", role.rules);
  check(!JSON.stringify(role.rules).includes("\"list\""), "the hook's Role lists nothing");
  const token = kube(["-n", NS, "create", "token", "lw-identity-bootstrap", "--duration=10m"]).stdout.trim();
  writeFileSync(join(WORK, "account", "token"), token, { mode: 0o600 });
  const caData = kube(["config", "view", "--raw", "--minify", "-o",
    "jsonpath={.clusters[0].cluster.certificate-authority-data}"]).stdout.trim();
  writeFileSync(join(WORK, "account", "ca.crt"), Buffer.from(caData, "base64"));
  const env = bootstrapEnv(join(WORK, "account"));
  const first = runBootstrap(env);
  save("00-bootstrap-1.txt", first.out + first.err);
  check(first.status === 0, "bootstrap #1 failed: " + first.err);
  check(/identity-ready key-id=[0-9a-f]{64} source=generated/.test(first.out), "identity generated: " + first.out);
  check(/console-confirmation-ready key-id=[0-9a-f]{64} source=generated/.test(first.out),
    "console key generated: " + first.out);
  check(first.out.includes("installation-trust-grant lw-identity-trust:not-held"),
    "the revocation of a grant this account never held answers not-held: " + first.out);
  const consoleTrust = kubeJson(["-n", NS, "get", "configmap", "logweir-console-trust"]);
  const signingTrust = kubeJson(["-n", NS, "get", "configmap", "logweir-signing-trust"]);
  save("00-console-trust.json", consoleTrust);
  check(Object.keys(consoleTrust.data).sort().join(",") === "algorithm,confirmation.pub.pem,key-id,trust-usage",
    "the console's public record is exactly its four keys");
  check(consoleTrust.data["trust-usage"] === "ConsoleConfirmation" && consoleTrust.data.algorithm === "ed25519",
    "Ed25519, usage ConsoleConfirmation");
  check(!JSON.stringify(consoleTrust).includes("PRIVATE") && !JSON.stringify(signingTrust).includes("PRIVATE"),
    "no private material in a public record");
  const consoleKeyId = consoleTrust.data["key-id"];
  const secretBefore = kubeJson(["-n", NS, "get", "secret", "logweir-console-confirmation"]).metadata.resourceVersion;
  const second = runBootstrap(env);
  save("00-bootstrap-2.txt", second.out + second.err);
  check(second.status === 0 && /console-confirmation-ready key-id=([0-9a-f]{64}) source=existing/.test(second.out),
    "bootstrap #2: existing: " + second.out + second.err);
  check(second.out.includes("key-id=" + consoleKeyId), "the SAME console key id on the second run");
  check(kubeJson(["-n", NS, "get", "secret", "logweir-console-confirmation"]).metadata.resourceVersion ===
    secretBefore, "the second run did not write the console Secret");
  record("the real hook, under the chart's own Role, generates the console key once and publishes only its public half", {
    consoleKeyId: consoleKeyId, signingKeyId: signingTrust.data["key-id"],
    identityMarker: (signingTrust.metadata.annotations || {})["logweir.dev/approval-default"] || null,
  });

  // ---------------------------------------------------------------- 1
  // UI_E2E_CHROMIUM: an explicit Chromium when the global Playwright's own
  // download is missing (this host carries a newer headless shell).
  const browser = await chromium.launch(process.env.UI_E2E_CHROMIUM
    ? { executablePath: process.env.UI_E2E_CHROMIUM } : {});
  result.browser = { executable: process.env.UI_E2E_CHROMIUM || "playwright default", version: browser.version() };
  const page = await (await browser.newContext()).newPage();
  const bodies = [];
  page.on("response", async (r) => {
    try {
      if (r.url().indexOf("/api/v1/") !== -1) {
        bodies.push({ url: r.url(), method: r.request().method(), status: r.status(),
          body: (await r.text()).slice(0, 200000) });
      }
    } catch (gone) {
      // body gone
    }
  });
  async function toPlanStep(base, point) {
    await page.goto(base + "/ui/#/restore?ns=" + NS + "&backup=" + point.name + "&uid=" +
      point.uid, { waitUntil: "load", timeout: 30000 });
    await page.waitForSelector("#wizard-position", { timeout: 30000 });
    await wizardAt(page, 1, 60);
    await wizardStep(page, 4);
    await page.waitForSelector("#step-target", { timeout: 30000 });
    await page.selectOption("#target-cluster", seeded.targetUid);
    await wizardStep(page, 6);
    await page.waitForSelector("#plan-bytes", { timeout: 30000 });
    return page.evaluate(() => document.querySelector("#step-plan").innerText);
  }
  const shot = async (name) => {
    const at = join(ARTIFACTS, name + ".png");
    await page.screenshot({ path: at, fullPage: true });
    result.screenshots.push(at);
  };
  let port = await freePort();
  let base = "http://127.0.0.1:" + port;
  await startApi(port, null);
  let view = await policyView(base);
  save("01-policy-unmarked.json", view.body);
  check(view.body.item.name === "legacy-governed-v1" && view.body.item.basis === "legacy" &&
    view.body.item.operatorMode === "strict", "unmarked: legacy, strict: " + JSON.stringify(view.body.item));
  // THE ATTACKS: one object edit each, on the ConfigMap the console reads.
  for (const [what, value] of [
    ["bare confirm", "confirm"],
    ["a claim naming a policy that does not exist", "confirm;policy=logweir-installation;uid=" +
      "00000000-0000-4000-8000-000000000bad;signing=" + signingTrust.data["key-id"] + ";console=" + consoleKeyId],
  ]) {
    kube(["-n", NS, "annotate", "configmap", "logweir-signing-trust", "--overwrite",
      "logweir.dev/approval-default=" + value]);
    const attacked = (await policyView(base)).body.item;
    check(attacked.name === "legacy-governed-v1" && attacked.basis === "legacy",
      what + ": the console must stay legacy: " + JSON.stringify(attacked));
    control("a marker patched into the identity ConfigMap (" + what + ") is not honoured", {
      name: attacked.name, basis: attacked.basis });
  }
  check(apiLog.join("").includes("NOT honoured"), "the console logs the refused marker as a WARN");
  save("01-console-warn.txt", apiLog.join("").split("\n").filter((l) => l.includes("NOT honoured")).join("\n"));
  kube(["-n", NS, "annotate", "configmap", "logweir-signing-trust", "logweir.dev/approval-default-"]);
  const legacyStep = await toPlanStep(base, seeded.legacyPoint);
  check(legacyStep.toLowerCase().includes("approve it out of band"), "the strict step: " + legacyStep.slice(0, 600));
  await shot("01-unmarked-strict-step");
  const since = bodies.length;
  await page.click("#create-restore");
  let answer = null;
  for (let i = 0; i < 60 && answer === null; i += 1) {
    answer = bodies.slice(since).filter((b) => b.method === "POST" && b.url.endsWith("/restores")).pop() || null;
    if (answer === null) {
      await pause(500);
    }
  }
  check(answer && (answer.status === 201 || answer.status === 200), "the legacy create: " + JSON.stringify(answer));
  const legacyAnswer = JSON.parse(answer.body);
  save("01-legacy-create.json", legacyAnswer);
  check(legacyAnswer.authorization.state === "awaitingApproval" && legacyAnswer.authorization.legacy === true,
    "the unmarked install awaits a v1 approval");
  check((kubeJson(["-n", NS, "get", "approvals"]).items || []).length === 0, "nothing was signed");
  record("an unmarked install keeps legacy-governed-v1: Create awaits an out-of-band approval, nothing is signed", {
    restore: legacyAnswer.item.name });
  await stopApi();

  // ---------------------------------------------------------------- 2
  const policyFile = join(WORK, "approval-policy.yaml");
  writeFileSync(policyFile, "allowOrdinaryConfirmation: true\ndefaultMode: confirm\n");
  save("02-approval-policy.yaml", "allowOrdinaryConfirmation: true\ndefaultMode: confirm\n");
  port = await freePort();
  base = "http://127.0.0.1:" + port;
  await startApi(port, policyFile);
  view = await policyView(base);
  save("02-policy-key-pending.json", view.body);
  check(view.body.item.name === "default-confirm-v1" && view.body.item.operatorMode === "confirm" &&
    view.body.item.basis === "configured" && view.body.item.ordinaryConfirmationAvailable === false,
  "the opt-in before the key exists: " + JSON.stringify(view.body.item));
  const pendingStep = await toPlanStep(base, seeded.confirmPoint);
  check(pendingStep.toLowerCase().includes("not available yet"), "the key-pending step: " + pendingStep.slice(0, 600));
  check(await page.evaluate(() => document.querySelector("#create-restore").disabled), "Create is withheld");
  await shot("02-confirm-key-pending");
  control("a confirm namespace whose console key is not there yet withholds Create and sends nothing", {
    ordinaryConfirmationAvailable: false });

  // ---------------------------------------------------------------- 3
  // THE KUBELET'S PART: project the Secret the hook filled into the mount path.
  const secret = kubeJson(["-n", NS, "get", "secret", "logweir-console-confirmation"]);
  writeFileSync(join(WORK, "confirmation", "confirmation.key"),
    Buffer.from(secret.data["confirmation.key"], "base64"), { mode: 0o600 });
  view = await policyView(base);
  save("03-policy-key-loaded.json", view.body);
  check(view.body.item.ordinaryConfirmationAvailable === true && view.body.item.confirmationKeyId === consoleKeyId,
    "the console read the hook's key on first use: " + JSON.stringify(view.body.item));
  check(view.body.item.digest === sha256(Buffer.from(DEFAULT_CONFIRM_SNAPSHOT)),
    "the policy digest is default-confirm-v1's snapshot");
  const confirmStep = await toPlanStep(base, seeded.confirmPoint);
  check(confirmStep.toLowerCase().includes("no key needed"), "the confirm step: " + confirmStep.slice(0, 600));
  check(!confirmStep.includes("logweir drill"), "and no command for a key holder");
  await shot("03-confirm-step");
  const since3 = bodies.length;
  await page.click("#create-restore");
  answer = null;
  for (let i = 0; i < 60 && answer === null; i += 1) {
    answer = bodies.slice(since3).filter((b) => b.method === "POST" && b.url.endsWith("/restores")).pop() || null;
    if (answer === null) {
      await pause(500);
    }
  }
  check(answer && answer.status === 201, "the confirm create: " + JSON.stringify(answer));
  const created = JSON.parse(answer.body);
  save("03-confirm-create.json", created);
  check(created.authorization.state === "confirmed" && created.authorization.operatorMode === "confirm" &&
    created.authorization.policy === "default-confirm-v1" && created.authorization.requester === LOCAL_ADMIN,
  "confirmed, as the local administrator, under default-confirm-v1: " + JSON.stringify(created.authorization));
  for (let i = 0; i < 40; i += 1) {
    const hash = await page.evaluate(() => window.location.hash);
    if (hash.startsWith("#/history")) {
      break;
    }
    await pause(500);
  }
  await shot("03-confirmed-operation-view");
  const approval = kubeJson(["-n", NS, "get", "approval", created.item.approvalRef.name]);
  const restore = kubeJson(["-n", NS, "get", "restore", created.item.name]);
  save("03-approval.json", approval);
  save("03-restore.json", restore);
  const doc = JSON.parse(approval.spec.approvalBytes);
  const sidecar = JSON.parse(approval.spec.sidecarBytes);
  check(sidecar.payloadType === V2_PAYLOAD && sidecar.signatures.length === 1, "one console signature, v2");
  check(sidecar.signatures[0].keyid === consoleKeyId, "signed by the key the hook generated");
  check(doc.policy.name === "default-confirm-v1" && doc.policy.digest === view.body.item.digest,
    "under default-confirm-v1");
  check(doc.requester.issuer === "urn:logweir:local-admin" && doc.requester.subject === "admin",
    "requester: the local administrator");
  check(doc.subject.uid === restore.metadata.uid && doc.planHash === sha256(Buffer.from(restore.spec.planBytes)),
    "bound to the Restore's UID and plan hash");
  const verified = verifySig(null, pae(V2_PAYLOAD, Buffer.from(approval.spec.approvalBytes)),
    createPublicKey(consoleTrust.data["confirmation.pub.pem"]), Buffer.from(sidecar.signatures[0].sig, "base64"));
  check(verified, "the signature verifies over the exact stored bytes against the hook's PUBLISHED half");
  record("ONE CLICK in the localAdmin console confirms the first restore: v2 under default-confirm-v1, " +
    "attested to the local administrator, signed by the hook's key, verified against its published half", {
    restore: created.item.name, approval: approval.metadata.name, keyId: consoleKeyId,
    policyDigest: doc.policy.digest });

  // ---------------------------------------------------------------- 4
  const bundle = join(WORK, "bundle");
  mkdirSync(bundle, { recursive: true, mode: 0o700 });
  const write = (name, bytes) => {
    writeFileSync(join(bundle, name), bytes, { mode: 0o600 });
    return join(bundle, name);
  };
  const files = {
    plan: write("restore.yaml", restore.spec.planBytes),
    approval: write("approval.json", approval.spec.approvalBytes),
    sidecar: write("approval.sig", approval.spec.sidecarBytes),
    approverKey: write("approver.pub.pem", consoleTrust.data["confirmation.pub.pem"]),
    confirmationKey: write("confirmation.pub.pem", consoleTrust.data["confirmation.pub.pem"]),
    snapshot: write("approval-policy.json", DEFAULT_CONFIRM_SNAPSHOT),
    // The slot's cluster2 broker, the plan's target: the id stack-env.sh gave it.
    allowed: write("allowed-clusters.json", JSON.stringify({ allowed_cluster_ids: [TARGET_CLUSTER_ID] })),
  };
  // The runner Job mounts the installation identity; the "kubelet" again.
  const signing = kubeJson(["-n", NS, "get", "secret", "logweir-signing-key"]);
  const signingPath = write("signing.pem", Buffer.from(signing.data["signing.pem"], "base64"));
  const digest = (p) => sha256(require("node:fs").readFileSync(p));
  const contract = {
    LOGWEIR_EXECUTION_CONTRACT_VERSION: "2",
    LOGWEIR_EXECUTION_SUBJECT_API_VERSION: "logweir.dev/v1alpha1",
    LOGWEIR_EXECUTION_SUBJECT_KIND: "Restore",
    LOGWEIR_EXECUTION_SUBJECT_NAME: restore.metadata.name,
    LOGWEIR_EXECUTION_SUBJECT_NAMESPACE: NS,
    LOGWEIR_EXECUTION_SUBJECT_UID: restore.metadata.uid,
    LOGWEIR_EXECUTION_APPROVAL_NAME: approval.metadata.name,
    LOGWEIR_EXECUTION_APPROVAL_UID: approval.metadata.uid,
    LOGWEIR_EXECUTION_PLAN_SHA256: digest(files.plan),
    LOGWEIR_EXECUTION_APPROVAL_SHA256: digest(files.approval),
    LOGWEIR_EXECUTION_APPROVAL_SIDECAR_SHA256: digest(files.sidecar),
    LOGWEIR_EXECUTION_APPROVER_KEY_SHA256: digest(files.approverKey),
    LOGWEIR_EXECUTION_ALLOWED_CLUSTERS_SHA256: digest(files.allowed),
    LOGWEIR_EXECUTION_POLICY_SNAPSHOT_SHA256: digest(files.snapshot),
    LOGWEIR_EXECUTION_CONFIRMATION_KEY_SHA256: digest(files.confirmationKey),
    AWS_ACCESS_KEY_ID: "minioadmin", AWS_SECRET_ACCESS_KEY: "minioadmin",
    AWS_ENDPOINT_URL: "http://127.0.0.1:" + S3_PORT, AWS_ALLOW_HTTP: "true", AWS_REGION: "us-east-1",
    // The runner Job's image names its engine; a host run says what it is.
    LOGWEIR_ENGINE_VERSION: "host-proof:prod-16-1",
    LOGWEIR_ENGINE_DIGEST: "sha256:" + "0".repeat(64),
    LOGWEIR_ENGINE_BIN: join(REPO, ".engine", "kafka-backup"),
  };
  const runRunner = (snapshotPath, env) => {
    const done = spawnSync(LOGWEIR_BIN, ["restore", "run", "--spec", files.plan, "--approval", files.approval,
      "--approver-key", files.approverKey, "--allowed-clusters", files.allowed, "--signing-key", signingPath,
      "--triggered-by", "approval/" + approval.metadata.name, "--execution-contract-version", "2",
      "--policy-snapshot", snapshotPath, "--confirmation-key", files.confirmationKey],
    { encoding: "utf8", timeout: 180000, env: Object.assign({}, process.env, env) });
    return { status: done.status, transcript: String(done.stdout || "") + String(done.stderr || "") };
  };
  const run = runRunner(files.snapshot, contract);
  save("04-runner.txt", "exit " + run.status + "\n" + run.transcript);
  // The authorization refusals `crates/logweir/tests/authorization_v2.rs`
  // names for the real binary (AUTHORIZATION_REFUSALS), plus the payload type.
  for (const refusal of ["console confirmation signature", "governed approver signature", "is Governed",
    "is Ordinary", "payload_type mismatch"]) {
    check(!run.transcript.includes(refusal),
      "the runner refused the console's bundle on " + refusal + " (exit " + run.status + "):\n" +
        run.transcript.slice(0, 3000));
  }
  check(run.transcript.includes("progress-phase=1:approval") && !run.transcript.includes("no data operation"),
    "the runner admitted the plan against slot 3's target (phase 0) and passed the approval (phase 1)");
  check(run.status !== 0, "no archive was written for the fixture point, so the run must fail later");
  const failure = (/"error":"([^"]*)"/.exec(run.transcript) || [])[1] || "";
  check(failure.startsWith("engine:"), "the run fails in the engine (no archive), not before: " + failure);
  record("the real runner admits the console-confirmed default-confirm-v1 bundle against slot 3 (phase 0), " +
    "verifies it (phase 1), and fails only in the engine, for which no archive exists", { exit: run.status,
    phases: run.transcript.split("\n").filter((l) => l.startsWith("progress-phase=")), failure: failure });
  // CONTROL: the same bundle beside ANOTHER policy's snapshot.
  const other = write("other-policy.json", DEFAULT_CONFIRM_SNAPSHOT.replace("default-confirm-v1", "other-v1"));
  const otherEnv = Object.assign({}, contract, { LOGWEIR_EXECUTION_POLICY_SNAPSHOT_SHA256: digest(other) });
  const refused = runRunner(other, otherEnv);
  save("04-runner-other-policy.txt", "exit " + refused.status + "\n" + refused.transcript);
  check(refused.status === 3, "another policy's snapshot is refused exit 3: " + refused.transcript.slice(0, 2000));
  control("the same bundle beside another policy's snapshot is refused by the runner, exit 3", {
    exit: refused.status });

  // ---------------------------------------------------------------- 5
  kube(["-n", NS, "patch", "secret", "logweir-console-confirmation", "--type=json",
    "-p", "[{\"op\":\"remove\",\"path\":\"/data\"}]"]);
  const lost = runBootstrap(env);
  save("05-bootstrap-key-loss.txt", lost.out + lost.err);
  check(lost.status !== 0 && lost.err.includes("restore logweir-console-confirmation from backup"),
    "key loss is refused, never regenerated: " + lost.err);
  check(!kubeJson(["-n", NS, "get", "secret", "logweir-console-confirmation"]).data,
    "and nothing was written into the emptied Secret");
  control("a published console key whose private half is gone stops the hook (key loss), nothing regenerated", {
    exit: lost.status });

  // ---------------------------------------------------------------- 6
  const grant = NS + "-identity-trust";
  const grantLabels = Object.assign({}, LABELS);
  const bindGrant = () => {
    kube(["create", "-f", "-"], { input: JSON.stringify({
      apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRoleBinding",
      metadata: { name: grant, labels: grantLabels },
      roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "ClusterRole", name: grant },
      subjects: [{ kind: "ServiceAccount", name: "lw-identity-bootstrap", namespace: NS }],
    }) });
  };
  const grantExists = () => kube(["get", "clusterrolebinding", grant, "--ignore-not-found", "-o", "name"])
    .stdout.trim() !== "";
  kube(["create", "-f", "-"], { input: JSON.stringify({
    apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRole",
    metadata: { name: grant, labels: grantLabels },
    rules: [{ apiGroups: ["rbac.authorization.k8s.io"], resources: ["clusterrolebindings"],
      resourceNames: [grant], verbs: ["delete"] }],
  }) });
  result.created.push({ kind: "ClusterRole", name: grant });
  const hook = (args) => {
    const done = spawnSync(LOGWEIR_BIN, args, {
      encoding: "utf8", timeout: 120000, env: Object.assign({}, process.env, env),
    });
    return { status: done.status, text: String(done.stdout || "") + String(done.stderr || "") };
  };
  const consoleArgs = ["--console-secret-name", "logweir-console-confirmation",
    "--console-public-configmap-name", "logweir-console-trust"];
  const paths = [
    ["a failed step (the console key loss above)",
      ["identity", "bootstrap", "--namespace", NS].concat(consoleArgs, ["--revoke-trust-binding", grant]),
      grant + ":revoked"],
    ["a store that cannot be opened (an argument the hook refuses)",
      ["identity", "bootstrap", "--namespace", NS, "--secret-name", "Not_A_Name", "--revoke-trust-binding", grant],
      grant + ":revoked"],
    ["a usage error (a flag from a newer chart)",
      ["identity", "bootstrap", "--namespace", NS, "--a-flag-from-a-newer-chart", "--revoke-trust-binding", grant],
      grant + ":revoked (after a usage error)"],
  ];
  const revocations = [];
  paths.forEach(([what, args, expected], i) => {
    bindGrant();
    check(grantExists(), "the grant is bound before " + what);
    const run = hook(args);
    save("06-revocation-" + (i + 1) + ".txt", "$ logweir " + args.join(" ") + "\nexit " + run.status + "\n" + run.text);
    check(run.status !== 0, what + ": the hook fails: " + run.text);
    check(run.text.includes("installation-trust-grant " + expected), what + ": revoked: " + run.text);
    check(!grantExists(), what + ": the ClusterRoleBinding is gone");
    revocations.push({ path: what, exit: run.status });
  });
  record("NO STANDING GRANT: a real ClusterRoleBinding granting the hook is deleted by the hook itself when a step " +
    "fails, when its store cannot be opened, and after a usage error", { binding: grant, paths: revocations });
  // CONTROL: the same failing hook WITHOUT the flag leaves the binding.
  bindGrant();
  const kept = hook(["identity", "bootstrap", "--namespace", NS].concat(consoleArgs));
  save("06-revocation-control.txt", "exit " + kept.status + "\n" + kept.text);
  check(kept.status !== 0 && grantExists(), "without --revoke-trust-binding the binding stays: " + kept.text);
  kube(["delete", "clusterrolebinding", grant]);
  control("without --revoke-trust-binding a failed hook leaves the binding (so the deletions above are the hook's)", {
    exit: kept.status });

  // ------------------------------------------------- no key file handled
  const tree = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const p = join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(p);
      } else {
        tree.push(p.slice(WORK.length + 1));
      }
    }
  };
  walk(WORK);
  result.workFiles = tree.sort();
  result.keyFilesNote = "account/{token,ca.crt}: the hook's projected token and the cluster CA (the pod's " +
    "/var/run/secrets); confirmation/confirmation.key and bundle/signing.pem: the kubelet's projections of the " +
    "two Secrets the hook generated; cursor.key: the console's cursor MAC key (the chart's keySecret). No " +
    "approver key exists anywhere and no person generated, copied or pasted a key.";
  await browser.close();
  result.passed = true;
}

async function cleanup() {
  await stopApi();
  // Journey 6's cluster-scoped pair, by name and only when labelled ours.
  const grant = NS + "-identity-trust";
  for (const kind of ["clusterrolebinding", "clusterrole"]) {
    try {
      const found = spawnSync("kubectl", ["--context", KUBE_CONTEXT, "get", kind, grant, "-o", "json",
        "--ignore-not-found"], { encoding: "utf8", timeout: 60000 });
      const object = found.stdout.trim() ? JSON.parse(found.stdout) : null;
      if (object && (object.metadata.labels || {})["logweir.dev/test-owner"] === OWNER) {
        kube(["delete", kind, grant]);
      }
      result.cleanup_cluster = (result.cleanup_cluster || []).concat([{ kind: kind, name: grant,
        present: kube(["get", kind, grant, "--ignore-not-found", "-o", "name"]).stdout.trim() !== "" }]);
    } catch (error) {
      result.cleanup_cluster = (result.cleanup_cluster || []).concat([{ kind: kind, error: String(error.message) }]);
    }
  }
  try {
    const ns = kubeJson(["get", "namespace", NS]);
    if ((ns.metadata.labels || {})["logweir.dev/test-owner"] === OWNER &&
      ns.metadata.uid === (result.created.find((c) => c.kind === "Namespace") || {}).uid) {
      kube(["delete", "namespace", NS, "--wait=true", "--timeout=180s"], { timeout: 200000 });
      result.cleanup = { namespace: NS, deleted: true };
    } else {
      result.cleanup = { namespace: NS, deleted: false, reason: "ownership did not match" };
    }
  } catch (error) {
    result.cleanup = { namespace: NS, error: String(error.message).slice(0, 300) };
  }
  rmSync(WORK, { recursive: true, force: true });
  result.workRemoved = !existsSync(WORK);
  result.apiLogTail = apiLog.join("").split("\n").slice(-40);
}

main()
  .catch((error) => {
    result.error = String(error && error.stack || error);
    process.stderr.write(result.error + "\n");
  })
  .finally(async () => {
    await cleanup();
    mkdirSync(ARTIFACTS, { recursive: true });
    save("result.json", result);
    process.stderr.write("result: " + join(ARTIFACTS, "result.json") + " passed=" + result.passed + "\n");
    process.exit(result.passed ? 0 : 1);
  });
