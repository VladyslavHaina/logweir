// PLAT-19.2 (and PLAT-12.1's policy routing) live console harness: a Restore
// submitted through the real console goes where the namespace's FROZEN
// approval policy sends it.
//
// THE LAUNCHER AND THE FIXTURES ARE `scripts/plat11-2-ui-e2e.mjs`'s. This
// starts TWO source-built `logweir-api` consoles over ONE approval-policy
// document and ONE per-run ConsoleConfirmation key, against docker-desktop,
// and drives real Chromium browsers:
//
//   * the SHARED console (review H1: D0's administrator mode "does not expose
//     Ordinary"), behind a TLS terminator that overwrites the forwarded
//     headers like an ingress, signing people in through a local ES256 OpenID
//     provider (PKCE S256, client_secret_basic) -- alice (operator) and bob
//     (approver), each in their own browser context;
//   * the ADMINISTRATOR (`localAdmin`) console on loopback.
//
// Four namespaces this run creates:
//
//   <ns>-o   bound ORDINARY. Shared: alice's Create signs the console's
//            confirmation of ALICE and routes to the operation view.
//            Administrator console: the page disables Create and the product
//            API refuses the same request `policy_mismatch`, creating nothing.
//   <ns>-v   bound GOVERNED. Shared: alice's Create (with the ticket D0
//            requires) stores the confirmation and routes to Awaiting
//            approval; alice's own countersignature is refused 403; bob's,
//            from his own browser, is recorded (201) with both signatures.
//   <ns>-g   UNBOUND (`legacy-governed-v1`), administrator console: Awaiting
//            approval; the approver's `logweir drill approve` files (the lab
//            key, by path) are recorded THROUGH THE PAGE, the lab controller
//            verifies them, and the page shows Verified.
//   <ns>-r   UNBOUND, over the lab's real archive: DRAFT-PREFLIGHT-NEVER-READY.
//
// WHAT THE LAB CONTROLLER MUST BE (FLIPPED AT lab-refresh-9). Until then the
// lab ran a controller that predated PLAT-19.2 and journey 1 recorded its
// refusal of every v2 document (`PayloadTypeMismatch`, no Job). The lab now
// runs a PLAT-19.2 controller, so journey 1 REQUIRES the new controller to
// ACCEPT the console's ordinary confirmation: Approval `Verified=True` with
// `status.authorization` {mode Ordinary, the bound policy, alice, the console
// key}, then the Restore's Job, carrying `--policy-snapshot` and
// `--confirmation-key`. That needs two things only an installation admin can
// give, both prepared OUTSIDE this process under the cluster lock:
//
//   1. the controller loads THIS run's approval-policy document, which binds
//      the run's namespaces -- run once with `UI_E2E_POLICY_ONLY=1` and a fixed
//      `UI_E2E_STAMP` to write `<artifacts>/<stamp>/approval-policy.yaml`,
//      mount it into the lab controller as the chart does
//      (`LOGWEIR_APPROVAL_POLICY_FILE`), and restore the controller after;
//   2. a `TrustPolicy` over the two bound namespaces naming the console's
//      `ConsoleConfirmation` key and bob's `GovernedApproval` key (principal
//      `<issuer>#bob`) -- this harness creates it (owner-labelled, deleted in
//      `cleanup`) once it has minted both keys. Journey 2 then REQUIRES the
//      confirmation alone to be `GovernedApprovalRequired` with no Job, and
//      the two-signature Approval to be Verified (Governed provenance) and
//      admitted with the frozen policy.
//
// The harness REFUSES to start when the controller has not bound the run's
// namespaces: a journey-1 "pass" against an unbound controller would be the
// old refusal again.
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
import { createServer as createHttpServer, request as httpRequest } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import {
  createHash, createPublicKey, generateKeyPairSync, randomBytes, sign as cryptoSign,
  verify as verifySig,
} from "node:crypto";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { wizardAt, wizardStep } from "./console-steps.mjs";

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

const stamp = process.env.UI_E2E_STAMP ||
  new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z").toLowerCase();
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
let sharedApi = null;
const apiLog = [];
const sharedLog = [];
let consolePublicPem = "";
let consoleKeyPath = "";
let policyPath = "";

/** THE approval-policy document of this run: the consoles mount it, and the
 *  lab controller must load the same one (see the header). */
function policyDocument() {
  return [
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
    "",
  ].join("\n");
}

/** Does the lab controller enforce THIS run's bindings? Read from its own
 *  startup line, which names every bound namespace. */
function controllerBinding() {
  const pods = (kubeJson(["-n", LAB_NS, "get", "pods"]).items || [])
    .filter((p) => p.metadata.name.startsWith("weirkeeper") && (p.status || {}).phase === "Running");
  const lines = pods.map((p) => kube(["-n", LAB_NS, "logs", p.metadata.name], { expected: [0, 1] }).stdout || "")
    .join("\n").split("\n").filter((l) => l.includes("the approval policies this controller enforces"));
  const last = lines.length > 0 ? JSON.parse(lines[lines.length - 1]) : null;
  const fields = (last || {}).fields || {};
  return { pods: pods.map((p) => p.metadata.name), line: fields,
    bound: String(fields.bound_namespaces || "") };
}

/** The bound namespaces' TrustPolicy: the console key (ConsoleConfirmation),
 *  bob's approver key (GovernedApproval, principal `<issuer>#bob`, minted here
 *  so the countersignature below uses it) and each namespace's own evidence
 *  key, so their runs stay attributable. Needs IDP_ISSUER (after startIdp). */
function consoleTrustPolicy() {
  const bobKey = join(WORK_DIR, "bob-approver.pem");
  check(spawnSync("openssl", ["genpkey", "-algorithm", "ed25519", "-out", bobKey], { timeout: 30000 }).status === 0,
    "bob's approver key");
  const bobPem = spawnSync("openssl", ["pkey", "-in", bobKey, "-pubout"], { encoding: "utf8", timeout: 30000 }).stdout;
  const spkiOf = (pem) => createPublicKey(pem).export({ type: "spki", format: "der" });
  const keyIdOf = (pem) => createHash("sha256").update(spkiOf(pem)).digest("hex");
  const evidenceOf = (ns) => {
    const pem = spawnSync("openssl", ["pkey", "-in", join(WORK_DIR, "signing-" + ns + ".pem"), "-pubout"],
      { encoding: "utf8", timeout: 30000 }).stdout;
    check(pem.includes("BEGIN PUBLIC KEY"), "the evidence public key of " + ns);
    return pem;
  };
  const notBefore = new Date(Date.now() - 3600 * 1000).toISOString().replace(/\.\d+Z$/, "Z");
  const notAfter = new Date(Date.now() + 86400 * 1000).toISOString().replace(/\.\d+Z$/, "Z");
  const key = (pem, usage, id, display) => ({ keyId: keyIdOf(pem), spkiPem: pem, algorithm: "ed25519",
    principal: { id: id, display: display }, usages: [usage], state: "Active",
    notBefore: notBefore, notAfter: notAfter });
  return {
    apiVersion: "logweir.dev/v1alpha1", kind: "TrustPolicy",
    metadata: { name: base + "-console", labels: LABELS },
    spec: { namespaces: [NS.ordinary, NS.governed], keys: [
      key(consolePublicPem, "ConsoleConfirmation", "urn:logweir:console:" + base, "the run's console key"),
      key(bobPem, "GovernedApproval", IDP_ISSUER + "#bob", "bob, the governed approver"),
      key(evidenceOf(NS.ordinary), "EvidenceSigning", "signing@" + NS.ordinary + ".invalid", "o's evidence key"),
      key(evidenceOf(NS.governed), "EvidenceSigning", "signing@" + NS.governed + ".invalid", "v's evidence key"),
    ] },
  };
}

/** The approval-policy document and the per-run console key, shared by both
 *  consoles (the chart mounts ONE document and ONE key Secret). */
function writePolicyAndKey() {
  mkdirSync(WORK_DIR, { recursive: true, mode: 0o700 });
  consoleKeyPath = join(WORK_DIR, "confirmation.key");
  const minted = spawnSync("openssl", ["genpkey", "-algorithm", "ed25519", "-out", consoleKeyPath],
    { encoding: "utf8", timeout: 30000 });
  check(minted.status === 0, "openssl could not mint the console key");
  const pub = spawnSync("openssl", ["pkey", "-in", consoleKeyPath, "-pubout"],
    { encoding: "utf8", timeout: 30000 });
  check(pub.status === 0, "openssl could not derive the console public key");
  consolePublicPem = pub.stdout;
  save("console-confirmation.pub.pem", consolePublicPem);
  policyPath = join(WORK_DIR, "approval-policy.yaml");
  const policy = policyDocument();
  writeFileSync(policyPath, policy);
  save("approval-policy.yaml", policy);
}

async function waitHealthy(url, log, what) {
  for (let i = 0; i < 60; i += 1) {
    try {
      const probe = await fetch(url);
      if (probe.ok) {
        return;
      }
    } catch (notYet) {
      // still binding
    }
    await pause(500);
  }
  throw new Error(what + " never answered " + url + " within 30 s. Log:\n" + log.join(""));
}

/** THE ADMINISTRATOR (`localAdmin`) CONSOLE, on loopback, serving every run
 *  namespace. D0: it does not expose Ordinary. */
async function startApi(port) {
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
    "namespaces: [" + Object.values(NS).join(", ") + "]",
    "kubernetes:",
    "  source: kubeconfig",
    "  context: " + KUBE_CONTEXT,
    "cursorKeyFile: " + cursorKey,
    "approvalPolicyFile: " + policyPath,
    "confirmationKeyFile: " + consoleKeyPath,
    "",
  ].join("\n");
  writeFileSync(configPath, config);
  save("config.yaml", config);
  api = spawn(API_BIN, ["--config", configPath], { stdio: ["ignore", "pipe", "pipe"] });
  api.stdout.on("data", (b) => apiLog.push(String(b)));
  api.stderr.on("data", (b) => apiLog.push(String(b)));
  await waitHealthy("http://127.0.0.1:" + port + "/healthz", apiLog, "the localAdmin console");
}

// ------------------------------------------------ the shared console (H1)
//
// REVIEW H1: ordinary confirmation is proved in the SHARED console with real
// signed-in identities. Three loopback processes stand in for what a cluster
// provides: a local OpenID provider (ES256, PKCE S256, client_secret_basic),
// a TLS terminator that OVERWRITES X-Forwarded-For/Proto like an ingress
// controller, and the source-built `logweir-api` in `mode: shared` behind it.
// The approach is PLAT-17.2's live harness (claude/artifacts/plat17-2/harness),
// ported to this process so the browser drives the real login redirects.

const CLIENT_ID = "logweir-console";
const CLIENT_SECRET = "p192-client-" + randomBytes(8).toString("hex");
const idpKey = generateKeyPairSync("ec", { namedCurve: "P-256" });
const idpNext = { sub: "alice", groups: [] };
const idpCodes = new Map();
let idpServer = null;
let proxyServer = null;
let IDP_ISSUER = "";

const b64u = (buf) => Buffer.from(buf).toString("base64").replace(/=+$/, "")
  .replace(/\+/g, "-").replace(/\//g, "_");

function mintIdToken(claims) {
  const header = b64u(JSON.stringify({ alg: "ES256", kid: "p192-es256", typ: "JWT" }));
  const payload = b64u(JSON.stringify(claims));
  const sig = cryptoSign("sha256", Buffer.from(header + "." + payload),
    { key: idpKey.privateKey, dsaEncoding: "ieee-p1363" });
  return header + "." + payload + "." + b64u(sig);
}

async function startIdp(port) {
  IDP_ISSUER = "http://127.0.0.1:" + port;
  const jwk = Object.assign(idpKey.publicKey.export({ format: "jwk" }),
    { kid: "p192-es256", alg: "ES256", use: "sig" });
  idpServer = createHttpServer((req, res) => {
    const u = new URL(req.url, IDP_ISSUER);
    const send = (status, body, headers) => {
      const data = Buffer.from(JSON.stringify(body));
      res.writeHead(status, Object.assign({ "content-type": "application/json",
        "content-length": data.length }, headers || {}));
      res.end(data);
    };
    if (req.method === "GET" && u.pathname === "/.well-known/openid-configuration") {
      return send(200, { issuer: IDP_ISSUER, authorization_endpoint: IDP_ISSUER + "/authorize",
        token_endpoint: IDP_ISSUER + "/token", jwks_uri: IDP_ISSUER + "/jwks",
        response_types_supported: ["code"], subject_types_supported: ["public"],
        id_token_signing_alg_values_supported: ["ES256"] });
    }
    if (req.method === "GET" && u.pathname === "/jwks") {
      return send(200, { keys: [jwk] });
    }
    if (req.method === "GET" && u.pathname === "/authorize") {
      const q = Object.fromEntries(u.searchParams);
      if (q.client_id !== CLIENT_ID || q.code_challenge_method !== "S256") {
        return send(400, { error: "invalid_request" });
      }
      const code = randomBytes(18).toString("hex");
      idpCodes.set(code, { user: Object.assign({}, idpNext), nonce: q.nonce,
        challenge: q.code_challenge, redirect: q.redirect_uri });
      res.writeHead(302, { location: q.redirect_uri + "?" +
        new URLSearchParams({ code: code, state: q.state }).toString(), "content-length": 0 });
      return res.end();
    }
    if (req.method === "POST" && u.pathname === "/token") {
      let raw = "";
      req.on("data", (c) => { raw += c; });
      req.on("end", () => {
        const basic = "Basic " + Buffer.from(encodeURIComponent(CLIENT_ID) + ":" +
          encodeURIComponent(CLIENT_SECRET)).toString("base64");
        if (req.headers.authorization !== basic) {
          return send(401, { error: "invalid_client" });
        }
        const form = Object.fromEntries(new URLSearchParams(raw));
        const grant = idpCodes.get(form.code);
        idpCodes.delete(form.code);
        if (!grant || form.redirect_uri !== grant.redirect) {
          return send(400, { error: "invalid_grant" });
        }
        const challenge = b64u(createHash("sha256").update(form.code_verifier || "").digest());
        if (challenge !== grant.challenge) {
          return send(400, { error: "invalid_grant" });
        }
        const now = Math.floor(Date.now() / 1000);
        return send(200, {
          id_token: mintIdToken({ iss: IDP_ISSUER, aud: CLIENT_ID, sub: grant.user.sub, iat: now,
            exp: now + 600, auth_time: now, nonce: grant.nonce, groups: grant.user.groups,
            name: grant.user.sub }),
          access_token: "at-" + randomBytes(8).toString("hex"), token_type: "Bearer",
          expires_in: 600,
        });
      });
      return undefined;
    }
    return send(404, { error: "not_found" });
  });
  await new Promise((ok) => idpServer.listen(port, "127.0.0.1", ok));
}

/** The stand-in ingress: TLS on loopback, forwarding over HTTP, OVERWRITING
 *  the forwarded headers rather than trusting the client's. */
async function startProxy(port, upstream) {
  const certDir = join(WORK_DIR, "tls");
  mkdirSync(certDir, { recursive: true, mode: 0o700 });
  const made = spawnSync("openssl", ["req", "-x509", "-newkey", "ec", "-pkeyopt",
    "ec_paramgen_curve:prime256v1", "-nodes", "-days", "1", "-subj", "/CN=localhost",
    "-addext", "subjectAltName=DNS:localhost", "-keyout", join(certDir, "tls.key"),
    "-out", join(certDir, "tls.crt")], { encoding: "utf8", timeout: 30000 });
  check(made.status === 0, "openssl could not mint the proxy certificate");
  const hop = new Set(["connection", "keep-alive", "transfer-encoding", "te", "upgrade",
    "x-forwarded-for", "x-forwarded-proto", "x-forwarded-host", "forwarded"]);
  proxyServer = createHttpsServer({
    key: readFileSync(join(certDir, "tls.key")), cert: readFileSync(join(certDir, "tls.crt")),
  }, (req, res) => {
    const headers = {};
    for (const [k, v] of Object.entries(req.headers)) {
      if (!hop.has(k.toLowerCase())) {
        headers[k] = v;
      }
    }
    headers["x-forwarded-for"] = req.socket.remoteAddress.replace("::ffff:", "");
    headers["x-forwarded-proto"] = "https";
    const up = httpRequest({ host: "127.0.0.1", port: upstream, method: req.method, path: req.url,
      headers: headers }, (answer) => {
      const out = {};
      for (const [k, v] of Object.entries(answer.headers)) {
        if (!hop.has(k.toLowerCase())) {
          out[k] = v;
        }
      }
      res.writeHead(answer.statusCode, out);
      answer.pipe(res);
    });
    up.on("error", () => { res.writeHead(502); res.end(); });
    req.pipe(up);
  });
  await new Promise((ok) => proxyServer.listen(port, "127.0.0.1", ok));
}

/** THE SHARED CONSOLE, serving the Ordinary and the Governed namespaces:
 *  alice operates in both, bob approves in the Governed one. */
async function startSharedApi(port, proxyPort) {
  const secretPath = join(WORK_DIR, "client-secret");
  writeFileSync(secretPath, CLIENT_SECRET, { mode: 0o600 });
  const sessionKey = join(WORK_DIR, "session.key");
  writeFileSync(sessionKey, "version: 1\nkey: \"" + randomBytes(32).toString("base64") + "\"\n",
    { mode: 0o600 });
  const cursorKey = join(WORK_DIR, "cursor-shared.key");
  writeFileSync(cursorKey, "version: 1\nkey: \"" + randomBytes(32).toString("base64") + "\"\n",
    { mode: 0o600 });
  const configPath = join(WORK_DIR, "shared.yaml");
  const config = [
    "mode: shared",
    "listen: \"127.0.0.1:" + port + "\"",
    "publicBaseUrl: \"https://localhost:" + proxyPort + "\"",
    "uiDirectory: " + UI_DIR,
    "oidc:",
    "  issuer: " + IDP_ISSUER,
    "  clientId: " + CLIENT_ID,
    "  clientSecretFile: " + secretPath,
    "  allowedAlgorithms: [ES256]",
    "  scopes: [openid, profile, groups]",
    "  groupsClaim: groups",
    "  displayNameClaim: name",
    "  insecureLoopbackIssuer: true",
    "roles:",
    "  revision: p192-live-1",
    "  bindings:",
    "    - {role: operator, namespace: " + NS.ordinary + ", groups: [p192-ops]}",
    "    - {role: operator, namespace: " + NS.governed + ", groups: [p192-ops]}",
    "    - {role: approver, namespace: " + NS.governed + ", groups: [p192-approvers]}",
    "sessionKey: {file: " + sessionKey + ", expectedVersion: 1}",
    "cursorKey: {file: " + cursorKey + ", expectedVersion: 1}",
    "sessionMaxAgeSeconds: 900",
    "trustedProxyCidrs: [\"127.0.0.1/32\"]",
    "namespaces: [" + NS.ordinary + ", " + NS.governed + "]",
    "kubernetes:",
    "  source: kubeconfig",
    "  context: " + KUBE_CONTEXT,
    "approvalPolicyFile: " + policyPath,
    "confirmationKeyFile: " + consoleKeyPath,
    "",
  ].join("\n");
  writeFileSync(configPath, config);
  save("shared.yaml", config.replace(CLIENT_SECRET, "<redacted>"));
  sharedApi = spawn(API_BIN, ["--config", configPath], { stdio: ["ignore", "pipe", "pipe"] });
  sharedApi.stdout.on("data", (b) => sharedLog.push(String(b)));
  sharedApi.stderr.on("data", (b) => sharedLog.push(String(b)));
  await waitHealthy("http://127.0.0.1:" + port + "/healthz", sharedLog, "the shared console");
}

/** Sign `sub` (in `groups`) in through the real redirect flow, in `context`. */
async function signIn(context, base, sub, groups) {
  idpNext.sub = sub;
  idpNext.groups = groups;
  const page = await context.newPage();
  await page.goto(base + "/auth/login", { waitUntil: "load", timeout: 30000 });
  const session = await page.evaluate(async () => {
    const r = await fetch("/api/v1/session");
    return { status: r.status, body: await r.json() };
  });
  check(session.status === 200, sub + " did not sign in: " + JSON.stringify(session));
  return { page: page, session: session.body };
}

function stopApi() {
  for (const child of [api, sharedApi]) {
    if (child !== null && child.exitCode === null) {
      child.kill("SIGTERM");
    }
  }
  for (const server of [idpServer, proxyServer]) {
    if (server !== null) {
      server.close();
    }
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
  const binding = controllerBinding();
  result.labControllerBinding = binding;
  check(binding.bound.includes(NS.ordinary) && binding.bound.includes(NS.governed),
    "the lab controller does not enforce this run's approval policy (bound_namespaces " +
      JSON.stringify(binding.bound) + "); write it with UI_E2E_POLICY_ONLY=1 UI_E2E_STAMP=" + stamp +
      ", mount it into the controller as the chart does, then run again with the same stamp");
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

  writePolicyAndKey();
  const port = await freePort();
  await startApi(port);
  const origin = "http://127.0.0.1:" + port;
  const ui = origin + "/ui/";
  result.port = port;
  result.apiStartLine = apiLog.join("").split("\n").find((l) => l.includes("logweir-api started")) || "";
  const idpPort = await freePort();
  await startIdp(idpPort);
  const trustPolicy = consoleTrustPolicy();
  kube(["apply", "-f", "-"], { input: JSON.stringify(trustPolicy) });
  result.created.push({ kind: "TrustPolicy", name: trustPolicy.metadata.name,
    uid: kubeJson(["get", "trustpolicy", trustPolicy.metadata.name]).metadata.uid,
    keyIds: trustPolicy.spec.keys.map((k) => k.keyId), usages: trustPolicy.spec.keys.map((k) => k.usages[0]) });
  const sharedPort = await freePort();
  const proxyPort = await freePort();
  await startProxy(proxyPort, sharedPort);
  await startSharedApi(sharedPort, proxyPort);
  const sharedBase = "https://localhost:" + proxyPort;
  const sharedUi = sharedBase + "/ui/";
  result.sharedConsole = { base: sharedBase, idp: IDP_ISSUER, listen: "127.0.0.1:" + sharedPort,
    startLine: sharedLog.join("").split("\n").find((l) => l.includes("logweir-api started")) || "" };

  const browser = await chromium.launch();
  const bodies = [];
  const capture = (p) => p.on("response", async (r) => {
    try {
      if (r.url().indexOf("/api/v1/") !== -1) {
        bodies.push({ url: r.url(), method: r.request().method(), status: r.status(),
          request: (r.request().postData() || "").slice(0, 400000),
          body: (await r.text()).slice(0, 20000) });
      }
    } catch (gone) {
      // body no longer available
    }
  });
  const page = await (await browser.newContext()).newPage();
  capture(page);
  // Two people, two browsers: alice operates, bob approves.
  // alice holds BOTH roles in the Governed namespace (D0's role union), so
  // the refusal of her own approval is the separation-of-duties rule and not
  // a missing grant.
  const alice = await signIn(await browser.newContext({ ignoreHTTPSErrors: true }), sharedBase,
    "alice", ["p192-ops", "p192-approvers"]);
  capture(alice.page);
  const bob = await signIn(await browser.newContext({ ignoreHTTPSErrors: true }), sharedBase,
    "bob", ["p192-approvers"]);
  capture(bob.page);
  const ALICE = IDP_ISSUER + "#alice";
  check(alice.session.authenticationMode === "oidc" && ((alice.session.actor || {}).id) === ALICE,
    "alice signed in through the provider: " + JSON.stringify(alice.session));
  save("00-sessions.json", { alice: { mode: alice.session.authenticationMode,
    actor: (alice.session.actor || {}).id }, bob: { mode: bob.session.authenticationMode,
    actor: (bob.session.actor || {}).id } });
  const createAnswer = (ns, since) => bodies.slice(since || 0).filter((b) => b.method === "POST" &&
    b.url.endsWith("/namespaces/" + ns + "/restores")).pop();

  async function submitIn(key, onPage, base, ticket) {
    const at = onPage || page;
    const ns = NS[key];
    const route = (base || ui) + "#/restore?ns=" + ns + "&backup=" + seeded[key].point.name +
      "&uid=" + seeded[key].point.uid;
    const since = bodies.length;
    await at.goto(route, { waitUntil: "load", timeout: 30000 });
    // ONE STEP AT A TIME (console-ux-1, MCP-29): step 1 on arrival, the target on step 4, the
    // plan, the ticket and Create on step 6, each reached with Next (scripts/console-steps.mjs).
    await waitFor(at, "#wizard-position", "the wizard in " + ns);
    await wizardAt(at, 1, 60);
    await wizardStep(at, 4);
    await waitFor(at, "#step-target", "the wizard's target step in " + ns);
    await at.selectOption("#target-cluster", seeded[key].targetUid);
    await wizardStep(at, 6);
    await waitFor(at, "#plan-bytes", "the plan in " + ns);
    if (typeof ticket === "string") {
      await waitFor(at, "#change-ticket", "the Governed ticket field in " + ns);
      await at.fill("#change-ticket", ticket);
    }
    const planBytes = await at.evaluate(() => document.querySelector("#plan-bytes").textContent);
    const planStep = await at.evaluate(() => document.querySelector("#step-plan").innerText);
    await at.click("#create-restore");
    let answer = null;
    for (let i = 0; i < 60 && answer === null; i += 1) {
      answer = createAnswer(ns, since) || null;
      if (answer === null) {
        await pause(500);
      }
    }
    check(answer !== null && (answer.status === 201 || answer.status === 200),
      "the page's create in " + ns + " answered " + JSON.stringify(answer));
    const body = JSON.parse(answer.body);
    return { ns: ns, planBytes: planBytes, planStep: planStep, answer: body, request: answer.request,
      restore: body.item.name, approvalName: body.item.approvalRef.name };
  }

  try {
    // ---------------------------------------------------------------- 0
    // The page must be ON the service's origin before it can fetch from it.
    await page.goto(ui, { waitUntil: "load", timeout: 30000 });
    const readPolicy = (p, base, ns) => p.evaluate(async (u) => {
      const r = await fetch(u);
      return { status: r.status, body: await r.json() };
    }, base + "/api/v1/namespaces/" + ns + "/approval-policy");
    const policies = {};
    for (const [key, ns] of Object.entries(NS)) {
      const read = await readPolicy(page, origin, ns);
      check(read.status === 200, "the policy route answered " + read.status);
      policies[key] = read.body.item;
    }
    const sharedOrdinary = (await readPolicy(alice.page, sharedBase, NS.ordinary)).body.item;
    save("00-approval-policies.json", { localAdmin: policies, sharedOrdinary: sharedOrdinary });
    check(policies.ordinary.mode === "ordinary" && policies.ordinary.legacy === false, "o is Ordinary");
    check(policies.ordinary.ordinaryConfirmationAvailable === false,
      "the localAdmin console does not offer Ordinary");
    check(sharedOrdinary.ordinaryConfirmationAvailable === true,
      "the shared console offers it: " + JSON.stringify(sharedOrdinary));
    check(policies.governed.mode === "governed" && policies.governed.requireDistinctPrincipal === true &&
      policies.governed.ticketRequired === true, "v is Governed, distinct principals, ticket required");
    check(policies.legacy.name === "legacy-governed-v1" && policies.legacy.legacy === true, "g is unbound");
    check(policies.ordinary.installationDigest === sharedOrdinary.installationDigest,
      "both consoles read one installation document");
    record("GET .../approval-policy publishes each namespace's effective policy, per console mode", {
      policies: Object.fromEntries(Object.entries(policies).map(([k, v]) => [k, v.name + "/" + v.mode])),
      ordinaryAvailable: { localAdmin: false, shared: true },
      installationDigest: policies.ordinary.installationDigest,
      confirmationKeyId: policies.ordinary.confirmationKeyId,
    });

    // ---------------------------------------------------------------- 1
    // ORDINARY, IN THE SHARED CONSOLE (review H1): alice, signed in through
    // the provider, submits; the console attests HER; the page routes to
    // execution.
    const o = await submitIn("ordinary", alice.page, sharedUi);
    check(o.planStep.toLowerCase().includes("ordinary confirmation"),
      "the submit step said the policy before the click: " + o.planStep.slice(0, 800));
    check(!o.planStep.includes("logweir drill approve"), "and offered no out-of-band approval");
    check(o.answer.authorization && o.answer.authorization.state === "confirmed",
      "the product API answered confirmed: " + JSON.stringify(o.answer.authorization));
    const oHash = await waitForHash(alice.page, "#/history?ns=" + o.ns + "&name=" + o.restore,
      "the ordinary submission's destination");
    await shot(alice.page, "01-ordinary-operation-view");
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
    check(doc.requester.issuer === IDP_ISSUER && doc.requester.subject === "alice",
      "the console attested alice, the signed-in person: " + JSON.stringify(doc.requester));
    check(doc.policy.name === "p192-ordinary" && doc.policy.digest === policies.ordinary.digest,
      "and the bound policy's snapshot digest");
    const signed = verifySig(null, pae(V2_PAYLOAD, Buffer.from(oApproval.spec.approvalBytes)),
      createPublicKey(consolePublicPem), Buffer.from(sidecar.signatures[0].sig, "base64"));
    check(signed, "the console's signature verifies over the exact stored bytes");
    const annotations = oRestore.metadata.annotations || {};
    // THE LAB CONTROLLER MUST ACCEPT IT (lab-refresh-9 flip): Verified=True
    // on the v2 document, with the provenance it verified, and then the
    // Restore's Job carrying the frozen policy and the console key.
    let labVerdict = null;
    let oApprovalSeen = null;
    for (let i = 0; i < 120 && (labVerdict === null || labVerdict.status !== "True"); i += 1) {
      oApprovalSeen = kubeJson(["-n", o.ns, "get", "approval", o.approvalName]);
      const c = ((oApprovalSeen.status || {}).conditions || []).find((x) => x.type === "Verified");
      labVerdict = c ? { status: c.status, reason: c.reason, message: (c.message || "").slice(0, 400) } : null;
      if (labVerdict === null || labVerdict.status !== "True") {
        await pause(1000);
      }
    }
    const provenance = (oApprovalSeen.status || {}).authorization || null;
    save("01-controller-verdict.json", { approval: labVerdict, status: oApprovalSeen.status || null });
    check(labVerdict !== null && labVerdict.status === "True",
      "the PLAT-19.2 controller did not verify the console's v2 ordinary confirmation: " +
        JSON.stringify(labVerdict));
    check(provenance !== null && provenance.mode === "Ordinary" && provenance.policyName === "p192-ordinary" &&
      provenance.policyDigest === policies.ordinary.digest &&
      provenance.requester === IDP_ISSUER + "#alice" &&
      provenance.confirmationKeyId === sidecar.signatures[0].keyid,
    "the verdict's provenance is not the bound Ordinary policy, alice and the console key: " +
      JSON.stringify(provenance));
    let oJob = null;
    for (let i = 0; i < 180 && oJob === null; i += 1) {
      oJob = (kubeJson(["-n", o.ns, "get", "jobs"]).items || []).find((j) =>
        (j.metadata.ownerReferences || []).some((r) => r.uid === oRestore.metadata.uid)) || null;
      if (oJob === null) {
        await pause(1000);
      }
    }
    const oRestoreNow = kubeJson(["-n", o.ns, "get", "restore", o.restore]);
    const admitted = ((oRestoreNow.status || {}).conditions || []).find((x) => x.type === "Admitted") || null;
    check(oJob !== null, "the verified ordinary Restore got no Job: " + JSON.stringify(oRestoreNow.status || null).slice(0, 800));
    const argv = [].concat(...(oJob.spec.template.spec.containers || []).map((c) =>
      (c.command || []).concat(c.args || [])));
    check(argv.includes("--policy-snapshot") && argv.includes("--confirmation-key"),
      "the Job does not carry the frozen policy and the console key: " + JSON.stringify(argv));
    let runnerLog = "";
    for (let i = 0; i < 90; i += 1) {
      const pods = (kubeJson(["-n", o.ns, "get", "pods", "-l", "job-name=" + oJob.metadata.name]).items || []);
      const done = pods.find((p) => ((p.status || {}).containerStatuses || []).some((c) => c.state && c.state.terminated));
      if (done) {
        runnerLog = kube(["-n", o.ns, "logs", done.metadata.name], { expected: [0, 1] }).stdout || "";
        break;
      }
      await pause(2000);
    }
    save("01-controller-admission.json", { job: oJob.metadata.name, jobUid: oJob.metadata.uid, argv: argv,
      admitted: admitted, restoreStatus: oRestoreNow.status || null,
      runnerLogTail: runnerLog.replace(/-----BEGIN[\s\S]*?-----END[^\n]*\n/g, "<pem redacted>\n").slice(-4000) });
    record("ordinary confirmation in the SHARED console: alice signs in, one click, the console attests alice, the page routes to execution, and the PLAT-19.2 controller verifies and admits it", {
      namespace: o.ns, restore: o.restore, restoreUid: oRestore.metadata.uid, approval: o.approvalName,
      routedTo: oHash, requester: doc.requester, policy: doc.policy,
      restoreActorAnnotation: annotations["api.logweir.dev/actor"] || null,
      controllerVerdict: labVerdict, provenance: provenance, admitted: admitted,
      job: { name: oJob.metadata.name, uid: oJob.metadata.uid, carriesPolicySnapshot: true,
        carriesConfirmationKey: true },
      runnerAuthorizationLine: (runnerLog.split("\n").find((l) => /authoriz/i.test(l)) || "").slice(0, 400),
    });

    // ---------------------------------------------------------------- 1b
    // THE ADMINISTRATOR CONSOLE DOES NOT OFFER ORDINARY (review H1). The page
    // says so and disables Create; and the product API itself refuses the
    // very request alice's page sent, replayed from the administrator page,
    // before anything exists.
    await page.goto(ui + "#/restore?ns=" + NS.ordinary + "&backup=" + seeded.ordinary.point.name +
      "&uid=" + seeded.ordinary.point.uid, { waitUntil: "load", timeout: 30000 });
    await waitFor(page, "#wizard-position", "the wizard in the administrator console");
    await wizardAt(page, 1, 60);
    await wizardStep(page, 4);
    await waitFor(page, "#step-target", "the wizard's target step in the administrator console");
    await page.selectOption("#target-cluster", seeded.ordinary.targetUid);
    // The policy's sentence and Create are step 6's.
    await wizardStep(page, 6);
    await waitFor(page, "#approval-policy-ordinary-unavailable", "the unavailable sentence");
    const disabled = await page.evaluate(() => document.querySelector("#create-restore").disabled);
    check(disabled === true, "Create is disabled in the administrator console");
    await shot(page, "01b-local-admin-ordinary-unavailable");
    const restoresBefore = (kubeJson(["-n", NS.ordinary, "get", "restores"]).items || []).length;
    const direct = await page.evaluate(async ([u, body]) => {
      const r = await fetch(u, { method: "POST", body: body, headers: {
        "Content-Type": "application/json", "Idempotency-Key": "p192-local-ordinary-0001" } });
      return { status: r.status, body: await r.json() };
    }, [origin + "/api/v1/namespaces/" + NS.ordinary + "/restores", o.request]);
    save("01b-local-admin-ordinary-refusal.json", direct);
    const restoresAfter = (kubeJson(["-n", NS.ordinary, "get", "restores"]).items || []).length;
    check(direct.status === 409 && direct.body.code === "policy_mismatch",
      "the localAdmin console refused Ordinary: " + JSON.stringify(direct));
    check(restoresAfter === restoresBefore, "and created nothing (" + restoresBefore + " → " +
      restoresAfter + ")");
    record("the administrator (localAdmin) console never signs an ordinary confirmation: the page " +
      "disables Create and the product API refuses the same request with nothing created", {
      namespace: NS.ordinary, status: direct.status, code: direct.body.code,
      restoresBefore: restoresBefore, restoresAfter: restoresAfter,
    });

    // ---------------------------------------------------------------- 2
    // GOVERNED, IN THE SHARED CONSOLE: alice asks (with a ticket), cannot
    // approve her own request, and bob -- a different signed-in person --
    // approves through his own browser.
    const TICKET = "CHG-P192-" + stamp;
    const v = await submitIn("governed", alice.page, sharedUi, TICKET);
    check(v.answer.authorization && v.answer.authorization.state === "awaitingApproval" &&
      v.answer.authorization.mode === "governed" && v.answer.authorization.legacy === false,
      "governed answered awaitingApproval: " + JSON.stringify(v.answer.authorization));
    const vHash = await waitForHash(alice.page, "#/approvals?subject=" + v.restore, "the governed destination");
    await waitFor(alice.page, "#countersign-form", "the governed countersign panel");
    await shot(alice.page, "02-governed-awaiting-approval");
    const confirmationName = v.approvalName + "-confirmation";
    const confirmation = kubeJson(["-n", v.ns, "get", "approval", confirmationName]);
    save("02-governed-confirmation.json", confirmation);
    const vDoc = JSON.parse(confirmation.spec.approvalBytes);
    check(vDoc.ticket === TICKET, "the ticket alice typed is signed into the confirmation");
    check(vDoc.requester.subject === "alice", "the confirmation attests alice");
    check(kube(["-n", v.ns, "get", "approval", v.approvalName], { expected: [0, 1] }).status === 1,
      "the referenced Approval does not exist until an approver submits");
    const doc2 = join(WORK_DIR, "confirmation.json");
    const conf2 = join(WORK_DIR, "confirmation.sig");
    writeFileSync(doc2, confirmation.spec.approvalBytes);
    writeFileSync(conf2, confirmation.spec.sidecarBytes);
    const countersignWith = (who) => {
      const key = join(WORK_DIR, who + "-approver.pem");
      if (!existsSync(key)) {
        check(spawnSync("openssl", ["genpkey", "-algorithm", "ed25519", "-out", key], { timeout: 30000 }).status === 0,
          "a throwaway approver key for " + who);
      }
      const out = join(WORK_DIR, who + ".sig");
      const counter = runCli(["drill", "countersign", "--document", doc2, "--confirmation", conf2,
        "--key", key, "--out", out]);
      check(counter.status === 0, "logweir drill countersign: " + counter.out);
      save("02-countersign-summary-" + who + ".txt", counter.out.replace(/key_id\s+\S+/g, "key_id <redacted>"));
      return readFileSync(out, "utf8");
    };
    await alice.page.fill("#countersigned-sidecar", countersignWith("alice"));
    await alice.page.click("#submit-countersignature");
    await waitForText(alice.page, "requested this restore", "the self-approval refusal");
    await shot(alice.page, "02-governed-self-approval-refused");
    const refusal = bodies.filter((b) => b.url.endsWith("/restores/" + v.restore + "/approval")).pop();
    save("02-self-approval-response.json", refusal);
    check(refusal && refusal.status === 403, "the product API refused the requester: " + JSON.stringify(refusal));
    check(kube(["-n", v.ns, "get", "approval", v.approvalName], { expected: [0, 1] }).status === 1,
      "and no Approval was created");
    control("alice, the requester, holds the Approver role too and is still refused approving her own request (403 forbidden)", {});
    // THE CONFIRMATION ALONE AUTHORISES NOTHING (lab-refresh-9, the
    // controller's half): the console-signed confirmation is judged
    // GovernedApprovalRequired, and the Restore creates no Job.
    let pending = null;
    for (let i = 0; i < 60; i += 1) {
      const seen = kubeJson(["-n", v.ns, "get", "approval", confirmationName]);
      pending = ((seen.status || {}).conditions || []).find((x) => x.type === "Verified") || null;
      if (pending !== null && pending.reason === "GovernedApprovalRequired") {
        break;
      }
      await pause(1000);
    }
    const jobsBeforeBob = (kubeJson(["-n", v.ns, "get", "jobs"]).items || []).filter((j) =>
      (j.metadata.ownerReferences || []).some((r) => r.name === v.restore));
    save("02-confirmation-verdict.json", { verified: pending, jobsForTheRestore: jobsBeforeBob.map((j) => j.metadata.name) });
    check(pending !== null && pending.status === "False" && pending.reason === "GovernedApprovalRequired",
      "the controller did not hold the confirmation alone as GovernedApprovalRequired: " + JSON.stringify(pending));
    check(jobsBeforeBob.length === 0, "a Job exists for the Restore before any approver signed");
    control("the console's confirmation alone is GovernedApprovalRequired at the controller, and no Job exists", {
      verified: pending });

    await bob.page.goto(sharedUi + vHash, { waitUntil: "load", timeout: 30000 });
    await waitFor(bob.page, "#countersign-form", "bob's countersign panel");
    await bob.page.fill("#countersigned-sidecar", countersignWith("bob"));
    await bob.page.click("#submit-countersignature");
    let bobAnswer = null;
    for (let i = 0; i < 60 && bobAnswer === null; i += 1) {
      bobAnswer = bodies.filter((b) => b.url.endsWith("/restores/" + v.restore + "/approval") &&
        b.status !== 403).pop() || null;
      if (bobAnswer === null) {
        await pause(500);
      }
    }
    save("02-bob-approval-response.json", bobAnswer);
    check(bobAnswer !== null && bobAnswer.status === 201, "bob's approval was recorded: " +
      JSON.stringify(bobAnswer));
    await shot(bob.page, "02-governed-approved-by-bob");
    const vApproval = kubeJson(["-n", v.ns, "get", "approval", v.approvalName]);
    save("02-governed-approval.json", vApproval);
    const vSidecar = JSON.parse(vApproval.spec.sidecarBytes);
    check(vApproval.spec.approvalBytes === confirmation.spec.approvalBytes,
      "the Approval carries the confirmation's exact bytes");
    check(vSidecar.signatures.length === 2, "the console's signature and bob's");
    check(((vApproval.metadata.annotations || {})["logweir.dev/approver"]) === IDP_ISSUER + "#bob",
      "bob is recorded as the approver");
    // TWO SEPARATE APPROVALS, AND THE CONTROLLER ADMITS ON THEM (lab-refresh-9).
    let vVerdict = null;
    let vSeen = null;
    for (let i = 0; i < 120 && (vVerdict === null || vVerdict.status !== "True"); i += 1) {
      vSeen = kubeJson(["-n", v.ns, "get", "approval", v.approvalName]);
      vVerdict = ((vSeen.status || {}).conditions || []).find((x) => x.type === "Verified") || null;
      if (vVerdict === null || vVerdict.status !== "True") {
        await pause(1000);
      }
    }
    const vProvenance = (vSeen.status || {}).authorization || null;
    check(vVerdict !== null && vVerdict.status === "True",
      "the controller did not verify the two-signature governed document: " + JSON.stringify(vVerdict));
    check(vProvenance !== null && vProvenance.mode === "Governed" && vProvenance.policyName === "p192-governed" &&
      vProvenance.requester === IDP_ISSUER + "#alice", "the governed provenance: " + JSON.stringify(vProvenance));
    const vRestoreObj = kubeJson(["-n", v.ns, "get", "restore", v.restore]);
    let vJob = null;
    for (let i = 0; i < 180 && vJob === null; i += 1) {
      vJob = (kubeJson(["-n", v.ns, "get", "jobs"]).items || []).find((j) =>
        (j.metadata.ownerReferences || []).some((r) => r.uid === vRestoreObj.metadata.uid)) || null;
      if (vJob === null) {
        await pause(1000);
      }
    }
    check(vJob !== null, "the governed Restore got no Job after both approvals");
    const vArgv = [].concat(...(vJob.spec.template.spec.containers || []).map((c) => (c.command || []).concat(c.args || [])));
    check(vArgv.includes("--policy-snapshot") && vArgv.includes("--confirmation-key"),
      "the governed Job does not carry the frozen policy and the console key");
    save("02-governed-controller-admission.json", { verified: vVerdict, provenance: vProvenance,
      job: vJob.metadata.name, argv: vArgv });
    record("governed in the SHARED console: alice asks with a ticket, is refused approving her own request, bob approves from his own browser, and the controller admits only on both signatures", {
      namespace: v.ns, restore: v.restore, routedTo: vHash, confirmation: confirmationName,
      ticket: TICKET, selfApproval: { status: refusal.status, code: JSON.parse(refusal.body).code },
      bobApproval: { status: bobAnswer.status, signatures: vSidecar.signatures.length },
      confirmationAlone: pending, controllerVerdict: vVerdict, provenance: vProvenance,
      job: vJob.metadata.name,
    });

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
    // RECORDED THROUGH THE PAGE (CONSOLE-HAS-NO-APPROVAL-CREATE-ROUTE): the two
    // files, pasted into the approvals page's form, sent by the page to the
    // product API's `POST .../restores/{name}/approval`. No kubectl write.
    await waitFor(page, "#approval-form", "the v1 approval form in console mode");
    await page.fill("#approval-json", readFileSync(join(work, "approval.json"), "utf8"));
    await page.fill("#approval-sig", readFileSync(join(work, "approval.sig"), "utf8"));
    await page.click("#approval-form button[type=submit]");
    let recordedAnswer = null;
    for (let i = 0; i < 60 && recordedAnswer === null; i += 1) {
      recordedAnswer = bodies.filter((b) => b.method === "POST" &&
        b.url.endsWith("/restores/" + g.restore + "/approval")).pop() || null;
      if (recordedAnswer === null) {
        await pause(500);
      }
    }
    save("03-legacy-record-response.json", recordedAnswer);
    check(recordedAnswer !== null && recordedAnswer.status === 201,
      "the page recorded the approval through the product API: " + JSON.stringify(recordedAnswer));
    const recordedObject = kubeJson(["-n", g.ns, "get", "approval", g.approvalName]);
    check(recordedObject.spec.approvalBytes === readFileSync(join(work, "approval.json"), "utf8") &&
      recordedObject.spec.sidecarBytes === readFileSync(join(work, "approval.sig"), "utf8"),
    "the stored Approval carries the two files byte-for-byte");
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
    // A goto to the SAME hash is a same-document no-op; reload reads again.
    await page.reload({ waitUntil: "load" });
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
    // Walked 1 -> 4 (target, prefix) -> 3 (point in time) -> 6 (the plan) -> 5 (readiness).
    await waitFor(page, "#wizard-position", "the wizard in " + r);
    await wizardAt(page, 1, 60);
    await wizardStep(page, 4);
    await waitFor(page, "#step-target", "the wizard's target step in " + r);
    await page.selectOption("#target-cluster", rs.targetUid);
    await pause(500);
    const prefix = "p192" + suffix + "-";
    await page.fill("#topic-prefix", prefix);
    await page.press("#topic-prefix", "Tab");
    // `windowCovered.toMs` is EXCLUSIVE (WIZ-PIT-EXCLUSIVE-DEFAULT, owned by
    // plat15-2): the last covered millisecond is chosen by hand, and said to be.
    const lastCovered = new Date(rs.window.toMs - 1).toISOString();
    await wizardStep(page, 3);
    await page.fill("#point-in-time", lastCovered);
    await page.press("#point-in-time", "Tab");
    await pause(1000);
    await wizardStep(page, 6);
    await waitFor(page, "#plan-bytes", "the plan in " + r);
    const rPlan = await page.evaluate(() => ({
      bytes: document.querySelector("#plan-bytes").textContent,
      hash: (document.querySelector("#plan-hash-value") || {}).textContent.trim(),
    }));
    save("04-draft-plan.txt", rPlan.bytes);
    const before = new Set((kubeJson(["-n", r, "get", "preflights"]).items || []).map((x) => x.metadata.uid));
    await wizardStep(page, 5);
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
    const servedItem = served.body.item;
    // THE SERVED VERDICT'S FRESHNESS IS NOT THIS TASK'S. At this base the
    // product API reports every restore readiness verdict stale in console
    // mode for two reasons outside PLAT-19.2's lane: `Backup` referentChanged
    // on every re-read (fixed on claude/plat08-2, ede475f, not on main) and
    // the controller's `TrustRoster` referent, which `routes/preflights.rs`
    // has no verb for (`unverifiable`). The gate is evaluated on the served
    // item AS SERVED (a staleness refusal, not the draft row), and on the
    // same item with only its freshness fields set as a fresh verdict --
    // DERIVED, and labelled so -- to show the draft rule on the controller's
    // real rows.
    const servedGate = await page.evaluate(async ([m, v, h]) => {
      const mod = await import(m);
      return mod.readinessRefusal({ readiness: { boundHash: h, preflight: v } }, { hash: h });
    }, [origin + "/ui/pages/restore-wizard.js", servedItem, rPlan.hash]);
    result.draftServedFreshness = { stale: servedItem.stale, applicable: servedItem.applicable,
      staleReasons: servedItem.staleReasons, shippedGateOnServed: servedGate };
    const item = servedItem.stale === true || servedItem.applicable !== true
      ? Object.assign(JSON.parse(JSON.stringify(servedItem)),
        { stale: false, staleReasons: [], applicable: true })
      : servedItem;
    result.draftGateInput = item === servedItem ? "served" : "derived: served item with stale=false, " +
      "staleReasons=[], applicable=true; every check row, the aggregate and the binding as served";
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
    save("api-shared-log.txt", sharedLog.join("").split(CLIENT_SECRET).join("<redacted>")
      .replace(/-----BEGIN[\s\S]*?-----END[^\n]*\n/g, "<pem redacted>\n"));
    await browser.close();
    stopApi();
  }
}

// Whether a live object is one THIS run created: the owner label AND the UID
// recorded at creation (`result.created`), as `plat19-2-admission-e2e.mjs`
// checks. A same-named, same-labelled object with another UID (an earlier
// run's leftover, or somebody else's) is refused (plat20-1.review.md L-3).
function ownedByThisRun(live, kind, name, created = result.created) {
  const meta = (live && live.metadata) || {};
  const labelled = (meta.labels || {})["logweir.dev/test-owner"] === OWNER;
  const made = created.find((c) => c.kind === kind && c.name === name && c.uid);
  return labelled && made !== undefined && typeof meta.uid === "string" && made.uid === meta.uid;
}

// The guard's planted twins, run before any cluster change and on request
// (UI_E2E_OWNERSHIP_SELFTEST=1, offline): the guard must accept this run's own
// object and refuse each wrong one, or the harness does not start.
function ownershipSelftest() {
  const mine = { metadata: { name: "x", uid: "uid-mine", labels: { ...LABELS } } };
  const created = [{ kind: "Namespace", name: "x", uid: "uid-mine" }];
  const twins = {
    "same label, another UID": { metadata: { name: "x", uid: "uid-other", labels: { ...LABELS } } },
    "this UID, no owner label": { metadata: { name: "x", uid: "uid-mine", labels: {} } },
    "this UID, another owner": { metadata: { name: "x", uid: "uid-mine",
      labels: { "logweir.dev/test-owner": OWNER + "-other" } } },
    "no UID at all": { metadata: { name: "x", labels: { ...LABELS } } },
  };
  const refused = Object.fromEntries(Object.entries(twins).map(([k, o]) => [k, !ownedByThisRun(o, "Namespace", "x", created)]));
  refused["labelled, never recorded as created"] = !ownedByThisRun(mine, "Namespace", "x", []);
  refused["recorded as another kind"] = !ownedByThisRun(mine, "TrustPolicy", "x", created);
  const accepted = ownedByThisRun(mine, "Namespace", "x", created);
  return { accepted, refused, killed: accepted && Object.values(refused).every(Boolean) };
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
      check(ownedByThisRun(object, "Namespace", ns),
        "refusing to delete " + ns + ": not labelled " + OWNER_LABEL + " with the UID this run created");
      kube(["delete", "namespace", ns, "--wait=true", "--timeout=180s"], { timeout: 200000 });
      const gone = kube(["get", "namespace", ns], { expected: [0, 1] }).status === 1;
      result.cleanup.push({ namespace: ns, uid: object.metadata.uid, deleted: gone });
    } catch (error) {
      result.cleanup.push({ namespace: ns, error: String(error && error.message) });
    }
  }
  try {
    const tp = kube(["get", "trustpolicy", base + "-console", "-o", "json"], { expected: [0, 1] });
    if (tp.status === 0) {
      const object = JSON.parse(tp.stdout);
      check(ownedByThisRun(object, "TrustPolicy", base + "-console"),
        "refusing to delete TrustPolicy " + base + "-console: not labelled " + OWNER_LABEL +
        " with the UID this run created");
      kube(["delete", "trustpolicy", base + "-console", "--wait=true"]);
      result.cleanup.push({ trustPolicy: base + "-console", uid: object.metadata.uid,
        deleted: kube(["get", "trustpolicy", base + "-console"], { expected: [0, 1] }).status === 1 });
    }
  } catch (error) {
    result.cleanup.push({ trustPolicy: base + "-console", error: String(error && error.message) });
  }
  rmSync(WORK_DIR, { recursive: true, force: true });
  result.cleanup.push({ workDir: WORK_DIR, removed: true });
}

if (process.env.UI_E2E_OWNERSHIP_SELFTEST === "1") {
  const own = ownershipSelftest();
  process.stdout.write(JSON.stringify(own) + "\n");
  process.exit(own.killed ? 0 : 1);
}

if (process.env.UI_E2E_POLICY_ONLY === "1") {
  mkdirSync(ARTIFACTS, { recursive: true });
  writeFileSync(join(ARTIFACTS, "approval-policy.yaml"), policyDocument());
  process.stderr.write("wrote " + join(ARTIFACTS, "approval-policy.yaml") + " for stamp " + stamp + "\n");
  process.exit(0);
}

let failed = null;
try {
  result.ownershipSelftest = ownershipSelftest();
  check(result.ownershipSelftest.killed, "the cleanup ownership guard cannot refuse: " +
    JSON.stringify(result.ownershipSelftest));
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
