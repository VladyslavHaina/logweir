// PLAT-19.2's four remaining live rows — the companion of
// `scripts/plat19-2-ui-e2e.mjs`, run against the lab controller with THIS
// run's approval-policy document mounted into it (the chart's way), under the
// cluster lock, and restored exactly afterwards.
//
//   row 3  a policy EDIT between verification and admission: the Approval is
//          Verified=True under policy A and the Restore is held on a missing
//          evidence destination; the installation document is edited (one
//          rollout, A -> A', never unbound in between) and the Restore ends
//          `ApprovalPolicyMismatch` naming A'. Releasing the hold afterwards
//          creates nothing.
//   row 4  authorization DOCUMENT expiry before admission: a 60 s policy, the
//          document verified and held; past `expiresAt` the Restore ends
//          `AuthorizationExpired`, and the released hold creates nothing.
//   row 6  DIRECT-CR writes of each failure mode: an Approval and a Restore
//          created by `kubectl`, never by the console — a v1 document under a
//          bound policy, the wrong policy digest, an expired document, another
//          plan, another UID, an unknown console key, a v2 document in an
//          unbound namespace, an Ordinary confirmation in a Governed
//          namespace, a console-only Governed document, a self-approval and a
//          countersignature by a key without the approver usage. Each is
//          refused by name on the Approval, the Restore never gets a Job, and
//          no ConfigMap exists for it.
//   row 7  console-key RETIREMENT after confirmation: verified and held, then
//          the TrustPolicy retires the console key; the released Restore gets
//          no Job and no bundle, and the refusal names the key.
//
// THE COUNTERFACTUAL IS MEASURED, NOT ASSUMED (control C). Rows 3, 4 and 7 end
// in "no Job"; so would a controller that never releases a held Restore. C is
// the same shape — a valid document, verified, held on a missing evidence
// destination — whose destination is created under the unedited policy with
// an Active key, and it MUST get its Job. Row 6's direct writes are judged
// against a direct write of a VALID document (control D), which must be
// Verified=True and admitted: the refusals are about the documents, not about
// who wrote them.
//
// WHY DIRECT WRITES FOR 3/4/7 TOO. The console signs with the key its config
// names; this harness holds the same kind of key (minted per run, listed in
// the run's TrustPolicy as `ConsoleConfirmation`) and signs the exact v2
// document the console would, because the console always pairs a Restore with
// its own valid destination and so can never produce the held state these
// rows need. The controller cannot tell the difference, which is the point of
// row 6: the controller, not the console, is the gate.
//
// Every kubectl names --context docker-desktop; every namespace is
// `<prefix><stamp>-{a,x,v,u}` labelled with the owner, deleted after the label
// and UID are read back; the TrustPolicy likewise. The shared controller is
// changed only by `scripts/live/approval_policy_swap.py` (on / edit / off),
// which refuses without the lock and fails unless the restore is exact.
//
//   LOGWEIR_PYTHON=… NODE_PATH="$(npm root -g)" node scripts/plat19-2-admission-e2e.mjs
//
// Environment (optional): UI_E2E_OWNER, UI_E2E_PREFIX, UI_E2E_API_BIN,
// UI_E2E_LOGWEIR_BIN, UI_E2E_ARTIFACTS, UI_E2E_APPROVER_KEY, UI_E2E_KEEP,
// UI_E2E_STAMP, LOGWEIR_PYTHON, LOGWEIR_K8S_LOCK.

import { spawn, spawnSync } from "node:child_process";
import { createServer } from "node:net";
import { createHash, generateKeyPairSync, randomBytes, sign as cryptoSign } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const KUBE_CONTEXT = "docker-desktop";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "debug", "logweir-api");
const LOGWEIR_BIN = process.env.UI_E2E_LOGWEIR_BIN || join(REPO, "target", "debug", "logweir");
const PYTHON = process.env.LOGWEIR_PYTHON || "python3";
const SWAP = join(REPO, "scripts", "live", "approval_policy_swap.py");
const LOCK = process.env.LOGWEIR_K8S_LOCK || "/tmp/logweir-roadmap-run/claude/k8s-lock.sh";
const APPROVER_KEY = process.env.UI_E2E_APPROVER_KEY ||
  join(process.env.HOME || "", ".logweir-lab", "scram-e2e", "approver.pem");
const OWNER = process.env.UI_E2E_OWNER || "plat19-2-admission";
const PREFIX = process.env.UI_E2E_PREFIX || "lw-p192a-";
const LABELS = { "logweir.dev/test-owner": OWNER };
const LAB_NS = "logweir-scram-local";
const LAB_TARGET = "kafka-target." + LAB_NS + ".svc.cluster.local:9096";
const LAB_SOURCE = "kafka-source." + LAB_NS + ".svc.cluster.local:9096";
const V2 = "application/vnd.logweir.restore-authorization+json;version=2.0.0";
const ISSUER = "https://idp.p192a.invalid";
const BACKUP_ID = "01JB7Z0000000000000000P19A";
const POINT = "2025-10-09T08:54:19.999Z";

const stamp = process.env.UI_E2E_STAMP ||
  new Date().toISOString().replace(/[-:]/g, "").replace(/\..*/, "Z").toLowerCase();
const base = PREFIX + stamp;
const NS = { a: base + "-a", x: base + "-x", v: base + "-v", u: base + "-u" };
const ARTIFACTS = join(process.env.UI_E2E_ARTIFACTS ||
  "/tmp/logweir-roadmap-run/claude/artifacts/plat19-2-admission", stamp);
const WORK = join("/tmp", "p192a-live-" + stamp);
const suffix = randomBytes(3).toString("hex");
const TRUST = base + "-trust";

const result = {
  harness: "scripts/plat19-2-admission-e2e.mjs", task: "PLAT-19.2 lab-refresh rows 3, 4, 6, 7",
  kubeContext: KUBE_CONTEXT, owner: OWNER, namespaces: NS, startedAt: new Date().toISOString(),
  apiBinary: API_BIN, logweirBinary: LOGWEIR_BIN,
  journeys: [], negativeControls: [], created: [], swaps: [], cleanup: [],
};

function check(ok, message) {
  if (!ok) {
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
function save(name, body) {
  writeFileSync(join(ARTIFACTS, name), typeof body === "string" ? body : JSON.stringify(body, null, 2));
}
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
const iso = (ms) => new Date(ms).toISOString();

function kube(args, opts) {
  const o = opts || {};
  const done = spawnSync("kubectl", ["--context", KUBE_CONTEXT].concat(args),
    { encoding: "utf8", input: o.input, timeout: o.timeout || 60000, maxBuffer: 16 * 1024 * 1024 });
  if (!(o.expected || [0]).includes(done.status)) {
    throw new Error("kubectl " + args.join(" ") + " exited " + done.status + ": " +
      String(done.stderr || "").trim().slice(0, 1200));
  }
  return done;
}
const kubeJson = (args) => JSON.parse(kube(args.concat(["-o", "json"])).stdout);
const getOpt = (ns, kind, name) => {
  const d = kube(["-n", ns, "get", kind, name, "-o", "json"], { expected: [0, 1] });
  return d.status === 0 ? JSON.parse(d.stdout) : null;
};
function create(ns, object) {
  return JSON.parse(kube(["-n", ns, "create", "-f", "-", "-o", "json"], { input: JSON.stringify(object) }).stdout);
}
function assertOwn(ns) {
  check(ns.startsWith(PREFIX) && !ns.startsWith("logweir-scram") && !ns.startsWith("kube-"),
    "this harness touches only " + PREFIX + "* namespaces, not " + ns);
}

// ------------------------------------------------------------- keys & signing

const spkiDer = (pub) => pub.export({ type: "spki", format: "der" });
const keyIdOf = (pub) => createHash("sha256").update(spkiDer(pub)).digest("hex");
const pemOf = (pub) => pub.export({ type: "spki", format: "pem" });
function mint() {
  const k = generateKeyPairSync("ed25519");
  return { pub: k.publicKey, priv: k.privateKey, id: keyIdOf(k.publicKey), pem: pemOf(k.publicKey) };
}
function pae(type, body) {
  return Buffer.concat([Buffer.from("DSSEv1 " + Buffer.byteLength(type) + " " + type + " " + body.length + " "), body]);
}
function signature(key, bytes, payloadType) {
  return { keyid: key.id, sig: cryptoSign(null, pae(payloadType || V2, Buffer.from(bytes)), key.priv).toString("base64") };
}
const KEYS = { console: mint(), bob: mint(), alice: mint(), carol: mint(), stranger: mint() };

/** The exact v2 document the console signs (`RestoreAuthorization`). */
function authDoc(o) {
  const doc = {
    formatVersion: "2.0.0", kind: "RestoreAuthorization", authorizationMode: o.mode,
    subject: { apiVersion: "logweir.dev/v1alpha1", kind: "Restore", namespace: o.ns, name: o.name, uid: o.uid },
    planHash: o.planHash, requester: { issuer: ISSUER, subject: o.requester || "alice" },
    policy: { name: o.policy.name, digest: o.policy.digest },
    issuedAt: iso(o.issuedAt), expiresAt: iso(o.expiresAt),
  };
  if (o.ticket) {
    doc.ticket = o.ticket;
  }
  return JSON.stringify(doc);
}
function sidecar(bytes, signers) {
  return JSON.stringify({ payloadType: V2, signatures: signers.map((k) => signature(k, bytes)) });
}

// ---------------------------------------------------------------- the policy

const POLICY = { ordinary: "p192a-ordinary", short: "p192a-short", governed: "p192a-governed" };
function policyDocument(ordinaryMaxAge) {
  return ["allowOrdinaryConfirmation: true", "policies:",
    "  - name: " + POLICY.ordinary, "    mode: Ordinary", "    maxAgeSeconds: " + ordinaryMaxAge,
    "  - name: " + POLICY.short, "    mode: Ordinary", "    maxAgeSeconds: 60",
    "  - name: " + POLICY.governed, "    mode: Governed", "    maxAgeSeconds: 86400",
    "namespaces:",
    "  " + NS.a + ": " + POLICY.ordinary, "  " + NS.x + ": " + POLICY.short, "  " + NS.v + ": " + POLICY.governed,
    ""].join("\n");
}

function freePort() {
  return new Promise((ok, bad) => {
    const s = createServer();
    s.on("error", bad);
    s.listen(0, "127.0.0.1", () => {
      const p = s.address().port;
      s.close(() => ok(p));
    });
  });
}

/** THE PRODUCT'S OWN DIGESTS of a policy document: a source-built console
 *  (localAdmin, loopback) is started over it and `GET .../approval-policy` is
 *  read for every namespace. Nothing re-implements the canonical snapshot. */
async function productDigests(policyPath, label) {
  const port = await freePort();
  const cursor = join(WORK, "cursor-" + label + ".key");
  writeFileSync(cursor, randomBytes(32), { mode: 0o600 });
  const config = join(WORK, "api-" + label + ".yaml");
  writeFileSync(config, ["mode: localAdmin", "listen: \"127.0.0.1:" + port + "\"",
    "publicOrigin: \"http://127.0.0.1:" + port + "\"", "uiDirectory: " + join(REPO, "ui"),
    "localAdmin:", "  subject: admin", "namespaces: [" + Object.values(NS).join(", ") + "]",
    "kubernetes:", "  source: kubeconfig", "  context: " + KUBE_CONTEXT, "cursorKeyFile: " + cursor,
    "approvalPolicyFile: " + policyPath, "confirmationKeyFile: " + join(WORK, "console.key"), ""].join("\n"));
  const log = [];
  const api = spawn(API_BIN, ["--config", config], { stdio: ["ignore", "pipe", "pipe"] });
  api.stdout.on("data", (b) => log.push(String(b)));
  api.stderr.on("data", (b) => log.push(String(b)));
  try {
    let up = false;
    for (let i = 0; i < 60 && !up; i += 1) {
      try {
        up = (await fetch("http://127.0.0.1:" + port + "/healthz")).ok;
      } catch (notYet) {
        await pause(500);
      }
    }
    check(up, "the console over " + label + " never answered: " + log.join("").slice(-1500));
    const out = {};
    for (const [key, ns] of Object.entries(NS)) {
      const r = await fetch("http://127.0.0.1:" + port + "/api/v1/namespaces/" + ns + "/approval-policy");
      check(r.status === 200, "approval-policy for " + ns + " answered " + r.status);
      out[key] = (await r.json()).item;
    }
    return out;
  } finally {
    api.kill("SIGTERM");
  }
}

function swap(verb, policyPath) {
  const args = [SWAP, verb, "--owner", OWNER, "--record", join(ARTIFACTS, "swap")];
  if (policyPath) {
    args.push("--policy", policyPath);
  }
  const done = spawnSync(PYTHON, args, { encoding: "utf8", timeout: 600000 });
  const entry = { verb: verb, rc: done.status, stderr: String(done.stderr || "").slice(-1500) };
  try {
    entry.facts = JSON.parse(done.stdout);
  } catch (unparsed) {
    entry.stdout = String(done.stdout || "").slice(-1500);
  }
  result.swaps.push(entry);
  check(done.status === 0, "approval_policy_swap.py " + verb + " exited " + done.status + ": " + entry.stderr);
  return entry.facts;
}

function lockHolder() {
  const out = spawnSync(LOCK, ["status"], { encoding: "utf8", timeout: 30000 }).stdout || "";
  return out.startsWith("held:") ? out.split(/\s+/)[1] : "";
}

// ------------------------------------------------------------- the fixtures

function copyLabSecret(ns, name) {
  const s = kubeJson(["-n", LAB_NS, "get", "secret", name]);
  create(ns, { apiVersion: "v1", kind: "Secret", type: s.type || "Opaque",
    metadata: { name: name, namespace: ns, labels: LABELS }, data: s.data });
}

function destination(ns, name) {
  return create(ns, { apiVersion: "logweir.dev/v1alpha1", kind: "BackupDestination",
    metadata: { name: name, namespace: ns, labels: LABELS },
    spec: { storage: { provider: "S3", bucket: "kafka-backups", prefix: ns, addressing: "PathStyle",
      endpoint: "http" + "://minio." + LAB_NS + ".svc:9000" },
    transport: { security: "InsecureHTTP" },
    access: { archiveWrite: { mode: "SecretKeys", secret: { name: "store-" + suffix } } } } });
}

async function waitFor(what, seconds, probe) {
  const until = Date.now() + seconds * 1000;
  let last = null;
  while (Date.now() < until) {
    last = probe();
    if (last && last.done) {
      return last;
    }
    await pause(2000);
  }
  throw new Error(what + " within " + seconds + " s; last: " + JSON.stringify(last).slice(0, 1500));
}

async function seed(ns) {
  assertOwn(ns);
  kube(["create", "namespace", ns]);
  kube(["label", "namespace", ns, "logweir.dev/test-owner=" + OWNER]);
  result.created.push({ kind: "Namespace", name: ns, uid: kubeJson(["get", "namespace", ns]).metadata.uid });
  create(ns, { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: "logweir-runner", namespace: ns, labels: LABELS } });
  const evidence = mint();
  kube(["-n", ns, "create", "secret", "generic", "logweir-signing-key", "--from-file=signing.pem=/dev/stdin"],
    { input: evidence.priv.export({ type: "pkcs8", format: "pem" }) });
  copyLabSecret(ns, "target-scram");
  const sourceSecret = kube(["-n", LAB_NS, "get", "secret", "source-scram"], { expected: [0, 1] }).status === 0
    ? "source-scram" : "target-scram";
  if (sourceSecret === "source-scram") {
    copyLabSecret(ns, "source-scram");
  }
  for (const [role, boot, secret] of [["source", LAB_SOURCE, sourceSecret], ["target", LAB_TARGET, "target-scram"]]) {
    create(ns, { apiVersion: "logweir.dev/v1alpha1", kind: "KafkaCluster",
      metadata: { name: role + "-" + suffix, namespace: ns, labels: LABELS },
      spec: { bootstrapServers: [boot], role: role,
        auth: { mode: "scramSha512", tls: false, username: "scram-user", secretRef: { name: secret } } } });
  }
  kube(["-n", ns, "create", "secret", "generic", "store-" + suffix,
    "--from-literal=access-key-id=unused", "--from-literal=secret-access-key=unused"]);
  destination(ns, "dest-" + suffix);
  return { evidence: evidence };
}

async function destinationReady(ns, name) {
  return waitFor("BackupDestination " + ns + "/" + name + " Valid=True", 120, () => {
    const d = getOpt(ns, "backupdestination", name) || {};
    const valid = ((d.status || {}).conditions || []).find((c) => c.type === "Valid");
    return { done: !!valid && valid.status === "True" && !!(d.status || {}).locationDigest, valid: valid };
  });
}

function planText(ns, prefix) {
  return ["# The restore plan document (rendered by scripts/plat19-2-admission-e2e.mjs in the",
    "# console's exact shape). The sha256 of these bytes is the plan hash an approval binds.",
    "source:", "  storage:", "    backend: \"s3\"", "    bucket: \"kafka-backups\"", "    prefix: \"" + ns + "\"",
    "    endpoint: \"http" + "://minio." + LAB_NS + ".svc:9000\"", "    path_style: true", "    allow_http: true",
    "  backup: \"" + BACKUP_ID + "\"", "  topics:", "    - \"orders\"",
    "target:", "  bootstrap_servers:", "    - \"" + LAB_TARGET + "\"", "  auth:", "    mode: \"scramSha512\"",
    "    username: \"scram-user\"", "    tls: false", "  mode: \"newTopic\"", "  topic_naming:",
    "    prefix: \"" + prefix + "\"", "  topic_mapping_prefix: \"" + prefix + "\"",
    "  marker_topic: \"logweir.scratch\"", "  default_replication_factor: 1", "  teardown: \"delete\"",
    "restore:", "  point_in_time: \"" + POINT + "\"",
    "sample:", "  window_start: \"2025-10-09T08:53:20Z\"", "  window_end: \"" + POINT + "\"",
    "  records_per_partition: 25", "  anchor: \"head\"", "objectives: {}",
    "evidence:", "  backend: \"s3\"", "  bucket: \"kafka-backups\"", "  prefix: \"logweir/\"",
    "  endpoint: \"http" + "://minio." + LAB_NS + ".svc:9000\"", "  path_style: true", "  allow_http: true",
    "notifications:", "  webhooks: []", ""].join("\n");
}
const sha = (s) => "sha256:" + createHash("sha256").update(s).digest("hex");

/** A Restore written directly (kubectl), held on `evidence` if that
 *  destination does not exist. */
function restore(ns, tag, evidence) {
  const name = "hr-" + tag + "-" + suffix;
  const prefix = "restore-p192a-" + tag + "-";
  const plan = planText(ns, prefix);
  const made = create(ns, { apiVersion: "logweir.dev/v1alpha1", kind: "Restore",
    metadata: { name: name, namespace: ns, labels: LABELS },
    spec: { approvalRef: { name: "ap-" + tag + "-" + suffix }, backupSetRef: BACKUP_ID, deadlineSeconds: 3600,
      evidenceDestinationRef: { name: evidence }, planBytes: plan, pointInTime: POINT,
      sourceArchive: { url: "logweir-destination://dest-" + suffix }, sourceDestinationRef: { name: "dest-" + suffix },
      target: { clusterRef: { name: "target-" + suffix }, mode: "newTopic", topicNaming: { prefix: prefix } } } });
  return { ns: ns, name: name, uid: made.metadata.uid, plan: plan, planHash: sha(plan), approval: "ap-" + tag + "-" + suffix };
}

function approval(r, bytes, side, planHash) {
  return create(r.ns, { apiVersion: "logweir.dev/v1alpha1", kind: "Approval",
    metadata: { name: r.approval, namespace: r.ns, labels: LABELS },
    spec: { approvalBytes: bytes, sidecarBytes: side, planHash: planHash || r.planHash,
      subjectRef: { kind: "Restore", name: r.name } } });
}

const condition = (o, type) => (((o || {}).status || {}).conditions || []).find((c) => c.type === type) || null;

/** Everything this Restore owns or would own: its Job, and the plan and
 *  bundle ConfigMaps by owner UID or by name. */
function materialised(r) {
  const owned = (items) => items.filter((i) => (i.metadata.ownerReferences || []).some((o) => o.uid === r.uid) ||
    i.metadata.name === r.name || i.metadata.name.startsWith(r.name + "-"));
  return {
    jobs: owned(kubeJson(["-n", r.ns, "get", "jobs"]).items || []).map((j) => j.metadata.name),
    configMaps: owned(kubeJson(["-n", r.ns, "get", "configmaps"]).items || []).map((c) => c.metadata.name),
  };
}

async function approvalVerdict(r, want, seconds) {
  return waitFor("Approval " + r.approval + " " + JSON.stringify(want), seconds || 90, () => {
    const a = getOpt(r.ns, "approval", r.approval);
    const v = condition(a, "Verified");
    return { done: !!v && v.status === want.status && (!want.reason || v.reason === want.reason),
      verified: v, authorization: ((a || {}).status || {}).authorization || null, status: (a || {}).status || null };
  });
}

async function restoreState(r, test, seconds, what) {
  return waitFor("Restore " + r.name + " " + what, seconds || 120, () => {
    const o = getOpt(r.ns, "restore", r.name);
    const s = (o || {}).status || {};
    const conds = s.conditions || [];
    return { done: test(s, conds), phase: s.phase, reason: s.reason, conditions: conds.map((c) =>
      ({ type: c.type, status: c.status, reason: c.reason, message: (c.message || "").slice(0, 700) })) };
  });
}
// The two shapes the Restore controller writes: a HOLD is `phase: Pending`
// with one `Admitted=False` condition carrying the reason; a TERMINAL refusal
// is `phase: Failed`, `status.reason` and a `Failed=True` condition, both the
// same reason (`refused_status_patch`).
const heldAs = (reason) => (s, conds) => s.phase === "Pending" && s.reason === reason &&
  conds.some((c) => c.type === "Admitted" && c.status === "False" && c.reason === reason);
const heldOnDestination = heldAs("DestinationNotFound");
const terminalAs = (reason) => (s, conds) => s.phase === "Failed" && s.reason === reason &&
  conds.some((c) => c.type === "Failed" && c.status === "True" && c.reason === reason);

async function settledNothing(r, seconds) {
  // Longer than the 30 s admission requeue: a Job the controller were going
  // to create would exist by now.
  await pause((seconds || 40) * 1000);
  return materialised(r);
}

// ------------------------------------------------------------------ the run

async function main() {
  mkdirSync(ARTIFACTS, { recursive: true });
  mkdirSync(join(ARTIFACTS, "swap"), { recursive: true });
  mkdirSync(WORK, { recursive: true, mode: 0o700 });
  for (const ns of Object.values(NS)) {
    assertOwn(ns);
  }
  check(lockHolder() === OWNER, "the cluster lock must be held by " + OWNER + " (it is held by " +
    (lockHolder() || "nobody") + "): this run changes the shared controller");
  writeFileSync(join(WORK, "console.key"), KEYS.console.priv.export({ type: "pkcs8", format: "pem" }), { mode: 0o600 });
  const policyA = join(ARTIFACTS, "approval-policy-A.yaml");
  const policyA2 = join(ARTIFACTS, "approval-policy-A-edited.yaml");
  writeFileSync(policyA, policyDocument(900));
  writeFileSync(policyA2, policyDocument(901));

  const seeded = {};
  for (const [key, ns] of Object.entries(NS)) {
    seeded[key] = await seed(ns);
  }
  const digestsA = await productDigests(policyA, "A");
  const digestsA2 = await productDigests(policyA2, "A-edited");
  save("00-policy-digests.json", { A: digestsA, edited: digestsA2 });
  check(digestsA.a.digest !== digestsA2.a.digest, "the edit changes the ordinary policy's digest");
  check(digestsA.x.digest === digestsA2.x.digest && digestsA.v.digest === digestsA2.v.digest,
    "and only that policy's");
  check(digestsA.u.legacy === true, "u is unbound");
  const pol = (d, key) => ({ name: d[key].name, digest: d[key].digest });

  // THE RUN'S TRUST: the console key, two governed approvers (bob; alice, who
  // is also the requester), carol's evidence-only key, and each namespace's
  // own evidence key. `stranger` is in no policy.
  const window = { notBefore: iso(Date.now() - 3600e3).replace(/\.\d+Z$/, "Z"),
    notAfter: iso(Date.now() + 86400e3).replace(/\.\d+Z$/, "Z") };
  const tkey = (k, usage, id, display) => Object.assign({ keyId: k.id, spkiPem: k.pem, algorithm: "ed25519",
    principal: { id: id, display: display }, usages: [usage], state: "Active" }, window);
  const trust = { apiVersion: "logweir.dev/v1alpha1", kind: "TrustPolicy",
    metadata: { name: TRUST, labels: LABELS },
    spec: { namespaces: Object.values(NS), keys: [
      tkey(KEYS.console, "ConsoleConfirmation", "urn:logweir:console:" + base, "the run's console key"),
      tkey(KEYS.bob, "GovernedApproval", ISSUER + "#bob", "bob"),
      tkey(KEYS.alice, "GovernedApproval", ISSUER + "#alice", "alice, also the requester"),
      tkey(KEYS.carol, "EvidenceSigning", ISSUER + "#carol", "carol, evidence only"),
    ].concat(Object.entries(seeded).map(([k, s]) => tkey(s.evidence, "EvidenceSigning",
      "signing@" + NS[k] + ".invalid", k + "'s evidence key"))) } };
  kube(["create", "-f", "-"], { input: JSON.stringify(trust) });
  result.created.push({ kind: "TrustPolicy", name: TRUST, uid: kubeJson(["get", "trustpolicy", TRUST]).metadata.uid,
    keys: { console: KEYS.console.id, bob: KEYS.bob.id, alice: KEYS.alice.id, carol: KEYS.carol.id,
      stranger: KEYS.stranger.id } });

  // THE SHARED CONTROLLER NOW ENFORCES POLICY A.
  const on = swap("on", policyA);
  save("01-swap-on.json", on);
  check(on.bound && on.bound.approval_policy_digest === digestsA.a.installationDigest,
    "the controller loaded policy A (installation digest " + digestsA.a.installationDigest + "): " + JSON.stringify(on.bound));
  for (const key of ["a", "x", "v"]) {
    check(String(on.bound.bound_namespaces || "").includes(NS[key]), "the controller binds " + NS[key]);
  }
  check(!String(on.bound.bound_namespaces || "").includes(NS.u), "and not " + NS.u);
  result.controller = { image: on.imageID, pod: on.pod };

  for (const ns of Object.values(NS)) {
    await waitFor("KafkaCluster target in " + ns + " reachable", 180, () => {
      const t = getOpt(ns, "kafkacluster", "target-" + suffix) || {};
      return { done: ((t.status || {}).reachable) === true, status: t.status || null };
    });
    await destinationReady(ns, "dest-" + suffix);
  }

  const now = () => Date.now();
  const valid = (r, policy, extra) => authDoc(Object.assign({ mode: "Ordinary", ns: r.ns, name: r.name, uid: r.uid,
    planHash: r.planHash, policy: policy, issuedAt: now() - 5000, expiresAt: now() + 600e3 }, extra || {}));
  const verifiedAndHeld = async (r, label) => {
    const v = await approvalVerdict(r, { status: "True" });
    const held = await restoreState(r, heldOnDestination, 120, "held on DestinationNotFound");
    const nothing = materialised(r);
    check(nothing.jobs.length === 0, label + ": a Job exists while held: " + nothing.jobs);
    save(label + "-verified-and-held.json", { verified: v, restore: held, materialised: nothing });
    return { verified: v, held: held };
  };

  /** Write the Approval for `r` over ONE byte string, signed by `signers`. */
  const authorise = (r, bytes, signers, planHash) => {
    approval(r, bytes, sidecar(bytes, signers), planHash);
    return bytes;
  };
  const release = (r) => destination(r.ns, "held-" + r.name.split("-")[1] + "-" + suffix);

  // ------------------------------------------------------------------ C
  // THE COUNTERFACTUAL: verified, held, released under policy A with the key
  // Active -> a Job. Without it, rows 3/4/7's "no Job" would be unfalsifiable.
  const c = restore(NS.a, "c", "held-c-" + suffix);
  authorise(c, valid(c, pol(digestsA, "a")), [KEYS.console]);
  const cHeld = await verifiedAndHeld(c, "C");
  check(cHeld.verified.authorization && cHeld.verified.authorization.policyDigest === digestsA.a.digest &&
    cHeld.verified.authorization.confirmationKeyId === KEYS.console.id,
  "C was verified under policy A and the console key: " + JSON.stringify(cHeld.verified.authorization));
  release(c);
  const cJob = await waitFor("C's Job after its destination exists", 150, () => {
    const m = materialised(c);
    return { done: m.jobs.length === 1, materialised: m };
  });
  save("C-released.json", { materialised: cJob.materialised, restore: getOpt(c.ns, "restore", c.name).status });
  control("C: a verified Restore held on a missing evidence destination gets its Job once the destination exists " +
    "(policy A, console key Active) — the release path rows 3, 4 and 7 must NOT take", {
    restore: c.name, uid: c.uid, provenance: cHeld.verified.authorization, job: cJob.materialised.jobs[0],
    configMaps: cJob.materialised.configMaps });

  // ------------------------------------------------------------------ 4 (start)
  // A 60 s policy: verified now, held, judged after expiresAt.
  const x = restore(NS.x, "exp", "held-exp-" + suffix);
  const xIssued = now() - 2000;
  const xBytes = authorise(x, authDoc({ mode: "Ordinary", ns: x.ns, name: x.name, uid: x.uid, planHash: x.planHash,
    policy: pol(digestsA, "x"), issuedAt: xIssued, expiresAt: xIssued + 60e3 }), [KEYS.console]);
  const xHeld = await verifiedAndHeld(x, "row4");

  // ------------------------------------------------------------------ 3 (start)
  const p = restore(NS.a, "edit", "held-edit-" + suffix);
  authorise(p, valid(p, pol(digestsA, "a")), [KEYS.console]);
  const pHeld = await verifiedAndHeld(p, "row3");
  check(pHeld.verified.authorization.policyDigest === digestsA.a.digest, "row 3 was verified under A");

  // ------------------------------------------------------------------ 6
  // DIRECT-CR WRITES. D first: a VALID document written the same way.
  const d = restore(NS.a, "d", "dest-" + suffix);
  authorise(d, valid(d, pol(digestsA, "a")), [KEYS.console]);
  const dVerdict = await approvalVerdict(d, { status: "True" });
  const dJob = await waitFor("D's Job", 150, () => {
    const m = materialised(d);
    return { done: m.jobs.length === 1, materialised: m };
  });
  control("D: a VALID v2 document and its Restore written directly with kubectl are Verified=True and admitted " +
    "(a Job exists) — row 6's refusals are about the documents, not about who wrote them", {
    restore: d.name, verified: dVerdict.verified, job: dJob.materialised.jobs[0] });

  const governed = (r, extra) => authDoc(Object.assign({ mode: "Governed", ns: r.ns, name: r.name, uid: r.uid,
    planHash: r.planHash, policy: pol(digestsA, "v"), issuedAt: now() - 5000, expiresAt: now() + 3600e3,
    ticket: "CHG-P192A-" + stamp }, extra || {}));
  const other = planText(NS.a, "restore-p192a-other-");
  const cases = [];
  const addCase = (tag, ns, about, want, write) => cases.push({ tag: tag, ns: ns, about: about, want: want, write: write });
  addCase("v1", NS.a, "a v1 approval document (the lab approver key, `logweir drill approve`) under a bound Ordinary policy",
    { approval: "ApprovalPolicyMismatch", restore: "ApprovalPolicyMismatch", terminal: true }, (r) => {
      const work = join(WORK, "v1");
      mkdirSync(work, { recursive: true, mode: 0o700 });
      writeFileSync(join(work, "plan.yaml"), r.plan);
      const done = spawnSync(LOGWEIR_BIN, ["drill", "approve", "--spec", join(work, "plan.yaml"), "--key", APPROVER_KEY,
        "--approver", "p192a-direct", "--ticket", "P192A", "--subject-kind", "Restore", "--out", join(work, "approval.json")],
      { encoding: "utf8", timeout: 120000 });
      check(done.status === 0, "logweir drill approve: " + String(done.stderr).slice(0, 400));
      approval(r, readFileSync(join(work, "approval.json"), "utf8"), readFileSync(join(work, "approval.sig"), "utf8"));
    });
  addCase("digest", NS.a, "a v2 document naming the bound policy's name with another digest (the Governed policy's)",
    { approval: "ApprovalPolicyMismatch", restore: "ApprovalPolicyMismatch", terminal: true },
    (r) => authorise(r, valid(r, { name: POLICY.ordinary, digest: digestsA.v.digest }), [KEYS.console]));
  addCase("expired", NS.a, "a v2 document whose expiresAt passed before it was written",
    { approval: "AuthorizationExpired", restore: "AuthorizationExpired", terminal: true },
    (r) => authorise(r, valid(r, pol(digestsA, "a"), { issuedAt: now() - 1200e3, expiresAt: now() - 400e3 }),
      [KEYS.console]));
  addCase("plan", NS.a, "a v2 document binding another plan's hash",
    { approval: "PlanHashMismatch", restore: "ApprovalNotVerified", terminal: false },
    (r) => authorise(r, valid(r, pol(digestsA, "a"), { planHash: sha(other) }), [KEYS.console]));
  addCase("uid", NS.a, "a v2 document binding the right name and another UID",
    { approval: "AuthorizationSubjectMismatch", restore: "ApprovalNotVerified", terminal: false },
    (r) => authorise(r, valid(r, pol(digestsA, "a"), { uid: "5f0c1a2b-0000-4000-8000-00000000192a" }),
      [KEYS.console]));
  addCase("stranger", NS.a, "a v2 document signed by a key in no TrustPolicy",
    { approval: "KeyIdNotInRoster", restore: "ApprovalNotVerified", terminal: false },
    (r) => authorise(r, valid(r, pol(digestsA, "a")), [KEYS.stranger]));
  addCase("unbound", NS.u, "a v2 document in an UNBOUND namespace (legacy-governed-v1 accepts v1 only)",
    { approval: "ApprovalPolicyMismatch", restore: "ApprovalPolicyMismatch", terminal: true },
    (r) => authorise(r, valid(r, pol(digestsA, "a")), [KEYS.console]));
  addCase("downgrade", NS.v, "an ORDINARY confirmation presented in a Governed namespace (the downgrade)",
    { approval: "ApprovalPolicyMismatch", restore: "ApprovalPolicyMismatch", terminal: true },
    (r) => authorise(r, valid(r, pol(digestsA, "a")), [KEYS.console]));
  addCase("alone", NS.v, "a Governed document carrying the console's confirmation and no approver",
    { approval: "GovernedApprovalRequired", restore: "ApprovalNotVerified", terminal: false },
    (r) => authorise(r, governed(r), [KEYS.console]));
  addCase("self", NS.v, "a Governed document countersigned by the REQUESTER's own approver key (alice)",
    { approval: "SelfApprovalRefused", restore: "ApprovalNotVerified", terminal: false },
    (r) => authorise(r, governed(r), [KEYS.console, KEYS.alice]));
  addCase("usage", NS.v, "a Governed document countersigned by a key without the GovernedApproval usage (carol, EvidenceSigning)",
    { approval: "KeyIdNotInRoster", restore: "ApprovalNotVerified", terminal: false },
    (r) => authorise(r, governed(r), [KEYS.console, KEYS.carol]));
  for (const k of cases) {
    // The evidence destination EXISTS: nothing but the authorization holds these.
    k.restore = restore(k.ns, k.tag, "dest-" + suffix);
    k.write(k.restore);
  }
  for (const k of cases) {
    const r = k.restore;
    const v = await approvalVerdict(r, { status: "False", reason: k.want.approval });
    const s = await restoreState(r, k.want.terminal ? terminalAs(k.want.restore) : heldAs(k.want.restore),
      120, (k.want.terminal ? "terminal " : "held ") + k.want.restore);
    k.verdict = v.verified;
    k.restoreSeen = s;
  }
  await pause(40000);
  for (const k of cases) {
    const r = k.restore;
    const m = materialised(r);
    const s = (getOpt(r.ns, "restore", r.name) || {}).status || {};
    save("row6-" + k.tag + ".json", { about: k.about, want: k.want, approval: getOpt(r.ns, "approval", r.approval),
      restoreStatus: s, materialised: m });
    check(m.jobs.length === 0 && m.configMaps.length === 0,
      "row 6 " + k.tag + ": the refused Restore materialised " + JSON.stringify(m));
    check(!(s.conditions || []).some((c2) => c2.type === "Admitted" && c2.status === "True"),
      "row 6 " + k.tag + ": the Restore reads Admitted=True");
    record("PLAT-19.2 row 6 direct-CR " + k.tag + ": " + k.about + " is refused " + k.want.approval +
      " on the Approval, the Restore is " + k.want.restore + ", and no Job or ConfigMap exists", {
      namespace: r.ns, restore: r.name, uid: r.uid, approvalVerified: k.verdict,
      restoreConditions: (s.conditions || []).map((c2) => ({ type: c2.type, status: c2.status, reason: c2.reason })),
      jobs: m.jobs, configMaps: m.configMaps });
  }

  // ------------------------------------------------------------------ 4 (judge)
  const xExpires = xIssued + 60e3;
  if (now() < xExpires + 3000) {
    await pause(xExpires + 3000 - now());
  }
  const xEnd = await restoreState(x, terminalAs("AuthorizationExpired"), 150, "terminal AuthorizationExpired");
  const xApproval = getOpt(x.ns, "approval", x.approval);
  release(x);
  const xAfter = await settledNothing(x, 45);
  save("row4-expired.json", { document: JSON.parse(xBytes), heldBefore: xHeld, restore: xEnd,
    approvalStatus: (xApproval || {}).status || null, afterRelease: xAfter });
  check(xAfter.jobs.length === 0 && xAfter.configMaps.length === 0,
    "row 4: the expired authorization materialised after release: " + JSON.stringify(xAfter));
  record("PLAT-19.2 row 4: an authorization document verified under a 60 s policy and held past its expiresAt " +
    "ends the Restore AuthorizationExpired; releasing the hold creates no Job and no ConfigMap", {
    namespace: x.ns, restore: x.name, uid: x.uid, expiresAt: iso(xExpires),
    verifiedBefore: xHeld.verified.verified, heldBefore: xHeld.held.conditions,
    restoreConditions: xEnd.conditions, approvalVerified: condition(xApproval, "Verified"), afterRelease: xAfter });

  // ------------------------------------------------------------------ 3 (judge)
  const edited = swap("edit", policyA2);
  save("row3-swap-edit.json", edited);
  check(edited.bound && edited.bound.approval_policy_digest === digestsA2.a.installationDigest,
    "the controller loaded the EDITED document: " + JSON.stringify(edited.bound));
  const pEnd = await restoreState(p, terminalAs("ApprovalPolicyMismatch"), 180, "terminal ApprovalPolicyMismatch");
  const refusal = (pEnd.conditions || []).find((c2) => c2.reason === "ApprovalPolicyMismatch") || {};
  check(String(refusal.message || "").includes(digestsA2.a.digest),
    "row 3: the refusal names the edited policy digest " + digestsA2.a.digest + ": " + refusal.message);
  const pApproval = getOpt(p.ns, "approval", p.approval);
  release(p);
  const pAfter = await settledNothing(p, 45);
  save("row3-policy-edit.json", { heldBefore: pHeld, restore: pEnd, approvalStatus: (pApproval || {}).status || null,
    afterRelease: pAfter, digests: { before: digestsA.a.digest, after: digestsA2.a.digest } });
  check(pAfter.jobs.length === 0 && pAfter.configMaps.length === 0,
    "row 3: the Restore materialised after the policy edit: " + JSON.stringify(pAfter));
  record("PLAT-19.2 row 3: a policy edit between verification and admission (A -> A', one rollout) ends the held " +
    "Restore ApprovalPolicyMismatch naming the edited digest; releasing the hold creates no Job and no ConfigMap", {
    namespace: p.ns, restore: p.name, uid: p.uid, verifiedUnder: pHeld.verified.authorization,
    editedDigest: digestsA2.a.digest, controllerAfterEdit: { pod: edited.pod, digest: edited.bound.approval_policy_digest },
    refusal: refusal, approvalVerified: condition(pApproval, "Verified"), afterRelease: pAfter });

  // ------------------------------------------------------------------ 7
  // Under A' now: verified, held, then the console key is RETIRED.
  const q = restore(NS.a, "ret", "held-ret-" + suffix);
  authorise(q, valid(q, pol(digestsA2, "a")), [KEYS.console]);
  const qHeld = await verifiedAndHeld(q, "row7");
  const tp = kubeJson(["get", "trustpolicy", TRUST]);
  const i = tp.spec.keys.findIndex((k2) => k2.keyId === KEYS.console.id);
  check(i >= 0, "the console key is in the run's TrustPolicy");
  const retiredAt = iso(now() - 1000).replace(/\.\d+Z$/, "Z");
  kube(["patch", "trustpolicy", TRUST, "--type=json", "-p", JSON.stringify([
    { op: "replace", path: "/spec/keys/" + i + "/state", value: "Retired" },
    { op: "add", path: "/spec/keys/" + i + "/retiredAt", value: retiredAt }])]);
  release(q);
  const named = await waitFor("row 7's refusal naming the console key", 150, () => {
    const a = getOpt(q.ns, "approval", q.approval);
    const v = condition(a, "Verified");
    const s = (getOpt(q.ns, "restore", q.name) || {}).status || {};
    const bundle = (s.conditions || []).find((c2) => String(c2.message || "").includes(KEYS.console.id) &&
      /no longer accepts/.test(String(c2.message || "")));
    const approvalSays = v && v.status === "False" && ["KeyRetired", "KeyIdNotInRoster"].includes(v.reason) &&
      String(v.message || "").includes(KEYS.console.id);
    return { done: !!bundle || approvalSays, where: bundle ? "restore-bundle" : (approvalSays ? "approval" : null),
      approval: v, restoreCondition: bundle || null, restoreConditions: (s.conditions || []).map((c2) =>
        ({ type: c2.type, status: c2.status, reason: c2.reason, message: (c2.message || "").slice(0, 500) })) };
  });
  const qAfter = await settledNothing(q, 45);
  save("row7-console-key-retired.json", { heldBefore: qHeld, retiredAt: retiredAt, refusal: named, afterRelease: qAfter,
    trustPolicyKey: kubeJson(["get", "trustpolicy", TRUST]).spec.keys[i] });
  check(qAfter.jobs.length === 0 && !qAfter.configMaps.some((n) => n.endsWith("-approval") || n.includes("bundle")),
    "row 7: a Job or an approval bundle exists after the console key was retired: " + JSON.stringify(qAfter));
  check(qAfter.jobs.length === 0 && qAfter.configMaps.length === 0,
    "row 7: the Restore materialised after the console key was retired: " + JSON.stringify(qAfter));
  record("PLAT-19.2 row 7: the console key retired after the confirmation was verified: the released Restore gets " +
    "no Job and no bundle, and the refusal names the key", {
    namespace: q.ns, restore: q.name, uid: q.uid, consoleKeyId: KEYS.console.id, retiredAt: retiredAt,
    verifiedBefore: qHeld.verified.authorization, refusedAt: named.where, approvalVerified: named.approval,
    restoreCondition: named.restoreCondition, afterRelease: qAfter });
}

async function cleanup() {
  if (process.env.UI_E2E_KEEP === "1") {
    result.cleanup.push("kept on request");
    return;
  }
  for (const ns of Object.values(NS)) {
    try {
      assertOwn(ns);
      const seen = kube(["get", "namespace", ns, "-o", "json"], { expected: [0, 1] });
      if (seen.status !== 0) {
        continue;
      }
      const o = JSON.parse(seen.stdout);
      check((o.metadata.labels || {})["logweir.dev/test-owner"] === OWNER, "refusing " + ns + ": not labelled " + OWNER);
      const uid = (result.created.find((c) => c.kind === "Namespace" && c.name === ns) || {}).uid;
      check(uid === o.metadata.uid, "refusing " + ns + ": its UID is not the one this run created");
      kube(["delete", "namespace", ns, "--wait=true", "--timeout=180s"], { timeout: 200000 });
      result.cleanup.push({ namespace: ns, uid: o.metadata.uid,
        deleted: kube(["get", "namespace", ns], { expected: [0, 1] }).status === 1 });
    } catch (error) {
      result.cleanup.push({ namespace: ns, error: String(error && error.message) });
    }
  }
  try {
    const tp = kube(["get", "trustpolicy", TRUST, "-o", "json"], { expected: [0, 1] });
    if (tp.status === 0) {
      const o = JSON.parse(tp.stdout);
      check((o.metadata.labels || {})["logweir.dev/test-owner"] === OWNER, "refusing TrustPolicy " + TRUST);
      kube(["delete", "trustpolicy", TRUST, "--wait=true"]);
      result.cleanup.push({ trustPolicy: TRUST, uid: o.metadata.uid,
        deleted: kube(["get", "trustpolicy", TRUST], { expected: [0, 1] }).status === 1 });
    }
  } catch (error) {
    result.cleanup.push({ trustPolicy: TRUST, error: String(error && error.message) });
  }
  // THE SHARED CONTROLLER, BACK TO ITS BASELINE — whatever happened above.
  if (result.swaps.some((s) => s.verb === "on" && s.rc === 0)) {
    try {
      const off = swap("off");
      result.cleanup.push({ controller: "restored", facts: off });
      check(off.restored === true, "the controller is not back at its baseline");
    } catch (error) {
      result.cleanup.push({ controller: "NOT RESTORED", error: String(error && error.message) });
      result.controllerNotRestored = true;
    }
  }
  rmSync(WORK, { recursive: true, force: true });
  result.cleanup.push({ workDir: WORK, removed: true });
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
  result.passed = failed === null && !result.controllerNotRestored;
  mkdirSync(ARTIFACTS, { recursive: true });
  save("result.json", result);
  process.stderr.write("result: " + join(ARTIFACTS, "result.json") + "\n");
}
if (failed !== null || result.controllerNotRestored) {
  process.stderr.write(String(failed && failed.stack || "the shared controller was not restored") + "\n");
  process.exit(1);
}
