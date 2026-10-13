// The approvals mock: each namespace's approval policy, its Approval records,
// two-person requests and the three submissions, with the API's refusals
// (docs/api.md "Approval policy" and "Two-person approval in the console").
//
// team-a and team-b are confirm (OD-8's default): every Restore there carries the
// console's signed confirmation, derived here from the shared restore list. The
// three demo namespaces (approvalNamespaces.ts) show the other modes, and their
// restores are served from this file:
//   payments  two-person: one request by Bob waits for Alice; one is Alice's own
//             (she may not approve it); one Alice approved yesterday.
//   ledger    strict: a request by Bob waits for a countersignature.
//   sandbox   unbound: a restore waits for the two `logweir drill approve` files.
import type { components } from "../api/schema";
import type { OperationView, Restore } from "../api/types";
import { sha256 } from "../components/hash";
import { DAY, HOUR, MINUTE, NOW, iso } from "./clock";
import { hex, requestId, restores as sharedRestores, session } from "./data";

type S = components["schemas"];
type Approval = S["Approval"];
type ApprovalPolicy = S["ApprovalPolicyView"];
type ApprovalRequest = S["ApprovalRequestView"];
type ApprovalScope = S["ApprovalScopeView"];

const ALICE = session.actor.id;
const BOB = "https://dex.logweir.localtest.me#bob";
const CONSOLE_KEY = "85551a95543b7e54ffd8a7ce17560f6f850c993b7a35e5ab23987f04c946ebd8";
const APPROVER_KEY = "24f4f5a02694bdf2e800a471545287d2c5d4b4b361212bb270ab4a4722921cdd";
const V1_APPROVAL_PAYLOAD = "application/vnd.logweir.drill-approval+json;version=1.0.0";
/** How long the mock controller takes to verify a new Approval. */
const VERIFY_MS = 3000;

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" },
  });
}

function problem(status: number, code: string, detail: string, errors?: S["FieldError"][]): Response {
  return json(status, { status, code, title: code, type: `https://logweir.dev/problems/${code}`, detail, requestId: requestId(), retryable: false, ...(errors ? { errors } : {}) });
}

function page<T>(items: T[]) {
  return { items, page: { limit: 200, snapshot: String(64000 + items.length) }, requestId: requestId() };
}

function uid(seed: number): string {
  const h = hex(seed, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-9${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

// ---------------------------------------------------------------- policies

const DIGEST = (seed: number) => "sha256:" + hex(seed, 64);

function policy(ns: string, over: Partial<ApprovalPolicy>): ApprovalPolicy {
  return {
    namespace: ns,
    name: "default-confirm-v1",
    mode: "ordinary",
    operatorMode: "confirm",
    legacy: false,
    basis: "freshInstall",
    requireDistinctPrincipal: false,
    installationDigest: DIGEST(500),
    ordinaryConfirmationAvailable: true,
    consoleApprovalAvailable: false,
    ticketRequired: false,
    maxAgeSeconds: 900,
    digest: DIGEST(501),
    confirmationKeyId: CONSOLE_KEY,
    ...over,
  };
}

function policyOf(ns: string): ApprovalPolicy {
  switch (ns) {
    case "payments":
      return policy(ns, {
        name: "prod-pair",
        mode: "governed",
        operatorMode: "two-person",
        basis: "binding",
        requireDistinctPrincipal: true,
        ordinaryConfirmationAvailable: false,
        consoleApprovalAvailable: true,
        ticketRequired: true,
        maxAgeSeconds: 3600,
        digest: DIGEST(502),
      });
    case "ledger":
      return policy(ns, {
        name: "prod-strict",
        mode: "governed",
        operatorMode: "strict",
        basis: "binding",
        requireDistinctPrincipal: true,
        ordinaryConfirmationAvailable: false,
        ticketRequired: true,
        maxAgeSeconds: 3600,
        digest: DIGEST(503),
      });
    case "sandbox":
      return policy(ns, {
        name: "legacy-governed-v1",
        mode: "governed",
        operatorMode: "strict",
        legacy: true,
        basis: "legacy",
        requireDistinctPrincipal: true,
        ordinaryConfirmationAvailable: false,
        ticketRequired: false,
        maxAgeSeconds: null,
        digest: null,
      });
    default:
      return policy(ns, {});
  }
}

// ---------------------------------------------------------------- the demo restores

interface Demo {
  restore: Omit<Restore, "planHash">;
  plan: string;
  requester: string;
  ticket?: string;
  issuedAt: number;
  scope: ApprovalScope;
  /** Who approved, when the demo starts approved. */
  approvedBy?: { who: string; at: number };
}

const PAYMENT_SERVERS = ["kafka-0.payments-dr.svc:9093", "kafka-1.payments-dr.svc:9093", "kafka-2.payments-dr.svc:9093"];
const ARCHIVE = { backend: "s3", location: "s3://kafka-backups/payments", region: "eu-west-1", plaintextHttp: false };
const EVIDENCE = { backend: "s3", location: "s3://logweir-evidence/payments/", region: "eu-west-1", plaintextHttp: false };

function planText(name: string, topics: string[], prefix: string, point: number, extra = ""): string {
  return [
    `name: ${name}`,
    "source:",
    "  storage: {backend: s3, bucket: kafka-backups, prefix: payments, region: eu-west-1}",
    `  backup: 01JB${hex(name.length * 7, 22).toUpperCase()}`,
    `  topics: [${topics.join(", ")}]`,
    "target:",
    `  bootstrap_servers: [${PAYMENT_SERVERS.map((s) => `"${s}"`).join(", ")}]`,
    "  mode: newTopic",
    `  topic_mapping_prefix: "${prefix}"`,
    extra,
    "restore:",
    `  point_in_time: ${iso(point)}`,
    "sample:",
    `  window_start: ${iso(point - 2 * HOUR)}`,
    `  window_end: ${iso(point)}`,
    "  records_per_partition: 25",
    "evidence: {backend: s3, bucket: logweir-evidence, prefix: payments/, region: eu-west-1}",
    "",
  ]
    .filter((line) => line !== "")
    .join("\n")
    .concat("\n");
}

function baseRestore(ns: string, name: string, seed: number, over: Partial<Restore>): Omit<Restore, "planHash"> {
  return {
    name,
    namespace: ns,
    uid: uid(seed),
    resourceVersion: String(64100 + seed),
    createdAt: iso(NOW - 12 * MINUTE),
    approvalRef: { name },
    approvalSubject: "ordinary",
    backupSetRef: `01JB${hex(seed * 3, 22).toUpperCase()}`,
    coverage: { requested: "sampled" },
    deadlineSeconds: 3600,
    newTopics: [],
    operation: { state: "pending", stateReason: "ApprovalNotVerified", terminal: false, verificationState: "pending", verifiedSuccess: false },
    planBytesLength: 0,
    pointInTime: iso(NOW - 3 * HOUR),
    sourceArchive: { url: "logweir-destination://primary" },
    target: { clusterRef: { name: "payments-dr" }, mode: "newTopic", originalName: false, topicPrefix: "" },
    ...over,
  };
}

function scopeOf(name: string, topics: { source: string; target: string; originalName?: boolean; partitions?: number[] }[], prefix: string, point: number, over: Partial<ApprovalScope["target"]> = {}): ApprovalScope {
  return {
    planName: name,
    source: { storage: ARCHIVE, backup: `01JB${hex(name.length * 7, 22).toUpperCase()}`, pointId: `pt-${hex(name.length * 11, 26)}`, receiptSha256: DIGEST(name.length * 13) },
    // To the millisecond, as the plan states it: the run applies both ends that exactly.
    recovery: { pointInTime: new Date(point).toISOString(), pointInTimeStated: true },
    target: {
      bootstrapServers: PAYMENT_SERVERS,
      authMode: "scramSha512",
      authUsername: "logweir-restorer",
      mode: "newTopic",
      topicPrefix: prefix,
      replicationFactor: 3,
      teardown: "keep",
      ...over,
    },
    topics: topics.map((t) => ({ originalName: false, ...t })),
    topicsCount: topics.length,
    verification: { coverage: "sampled", windowStart: iso(point - 2 * HOUR), windowEnd: iso(point) },
    evidence: EVIDENCE,
  };
}

const PITR_POINT = NOW - 3 * HOUR - 17 * MINUTE + 999;
const ORIG_POINT = NOW - 26 * HOUR;
const DRILL_POINT = NOW - 30 * HOUR;

const demos: Demo[] = [
  {
    restore: baseRestore("payments", "payments-pitr-r7q2c", 610, {
      newTopics: ["pitr-1012-payments", "pitr-1012-payments-ledger", "pitr-1012-refunds"],
      target: { clusterRef: { name: "payments-dr" }, mode: "newTopic", originalName: false, topicPrefix: "pitr-1012-" },
      pointInTime: iso(PITR_POINT),
    }),
    plan: planText("payments-pitr-r7q2c", ["payments", "payments-ledger", "refunds"], "pitr-1012-", PITR_POINT),
    requester: BOB,
    ticket: "CHG-20417",
    issuedAt: NOW - 12 * MINUTE,
    scope: scopeOf(
      "payments-pitr-r7q2c",
      [
        { source: "payments", target: "pitr-1012-payments" },
        { source: "payments-ledger", target: "pitr-1012-payments-ledger" },
        { source: "refunds", target: "pitr-1012-refunds", partitions: [0, 2, 5] },
      ],
      "pitr-1012-",
      PITR_POINT,
    ),
  },
  {
    restore: baseRestore("payments", "payments-orig-m3k8t", 611, {
      createdAt: iso(NOW - 25 * MINUTE),
      approvalSubject: "originalName",
      coverage: { requested: "complete" },
      newTopics: ["payments", "refunds"],
      target: { clusterRef: { name: "payments-dr" }, mode: "newTopic", originalName: true, topicPrefix: "" },
      pointInTime: iso(ORIG_POINT),
    }),
    plan: planText("payments-orig-m3k8t", ["payments", "refunds"], "", ORIG_POINT, "  topic_naming: {prefix: \"\", original_name: {owners: []}}"),
    requester: ALICE,
    ticket: "CHG-20431",
    issuedAt: NOW - 25 * MINUTE,
    scope: {
      ...scopeOf(
        "payments-orig-m3k8t",
        [
          { source: "payments", target: "payments", originalName: true },
          { source: "refunds", target: "refunds", originalName: true },
        ],
        "",
        ORIG_POINT,
        { ownerStatement: { owners: [], ownerPath: false } },
      ),
      verification: { coverage: "complete", windowStart: iso(ORIG_POINT - 2 * HOUR), windowEnd: iso(ORIG_POINT) },
    },
  },
  {
    restore: baseRestore("payments", "payments-drill-x9v4n", 612, {
      createdAt: iso(NOW - DAY - 2 * HOUR),
      newTopics: ["drill-payments", "drill-refunds"],
      target: { clusterRef: { name: "payments-dr" }, mode: "newTopic", originalName: false, topicPrefix: "drill-" },
      pointInTime: iso(DRILL_POINT),
      operation: { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "valid", verifiedSuccess: true, exitCode: 0, outcome: "pass" },
    }),
    plan: planText("payments-drill-x9v4n", ["payments", "refunds"], "drill-", DRILL_POINT),
    requester: BOB,
    ticket: "CHG-20388",
    issuedAt: NOW - DAY - 2 * HOUR,
    scope: scopeOf(
      "payments-drill-x9v4n",
      [
        { source: "payments", target: "drill-payments" },
        { source: "refunds", target: "drill-refunds" },
      ],
      "drill-",
      DRILL_POINT,
    ),
    approvedBy: { who: ALICE, at: NOW - DAY - 110 * MINUTE },
  },
  {
    restore: baseRestore("ledger", "ledger-restore-h2w6p", 620, {
      createdAt: iso(NOW - 40 * MINUTE),
      newTopics: ["restore-ledger-entries", "restore-ledger-balances"],
      target: { clusterRef: { name: "ledger-dr" }, mode: "newTopic", originalName: false, topicPrefix: "restore-" },
      pointInTime: iso(NOW - 5 * HOUR),
    }),
    plan: planText("ledger-restore-h2w6p", ["ledger-entries", "ledger-balances"], "restore-", NOW - 5 * HOUR),
    requester: BOB,
    ticket: "CHG-20402",
    issuedAt: NOW - 40 * MINUTE,
    scope: scopeOf("ledger-restore-h2w6p", [{ source: "ledger-entries", target: "restore-ledger-entries" }], "restore-", NOW - 5 * HOUR),
  },
  {
    restore: baseRestore("sandbox", "sandbox-drill-b8n1z", 630, {
      createdAt: iso(NOW - 2 * HOUR),
      newTopics: ["drill-orders"],
      target: { clusterRef: { name: "sandbox-kafka" }, mode: "scratch", originalName: false, topicPrefix: "drill-" },
      pointInTime: iso(NOW - 4 * HOUR),
    }),
    plan: planText("sandbox-drill-b8n1z", ["orders"], "drill-", NOW - 4 * HOUR),
    requester: BOB,
    issuedAt: NOW - 2 * HOUR,
    scope: scopeOf("sandbox-drill-b8n1z", [{ source: "orders", target: "drill-orders" }], "drill-", NOW - 4 * HOUR),
  },
];

const hashes = new Map<string, string>();

async function planHashOf(d: Demo): Promise<string> {
  let h = hashes.get(d.restore.name);
  if (!h) {
    h = await sha256(d.plan);
    hashes.set(d.restore.name, h);
  }
  return h;
}

/** Approvals recorded through this mock: by namespace, then by name. */
const recorded = new Map<string, Map<string, { approval: Approval; at: number; body: string }>>();

function stored(ns: string): Map<string, { approval: Approval; at: number; body: string }> {
  let m = recorded.get(ns);
  if (!m) {
    m = new Map();
    recorded.set(ns, m);
  }
  return m;
}

/** An Approval as the controller leaves it `ms` after it was recorded. */
function settled(entry: { approval: Approval; at: number }, restore: { name: string; uid: string; namespace: string }): Approval {
  if (Date.now() - entry.at < VERIFY_MS) return entry.approval;
  return {
    ...entry.approval,
    verified: true,
    verifiedSubject: { kind: "Restore", name: restore.name, namespace: restore.namespace, uid: restore.uid },
    conditions: [
      { type: "Verified", status: "True", reason: "Verified", message: `verified against key ${entry.approval.matchedKeyId}`, lastTransitionTime: iso(entry.at + VERIFY_MS) },
      { type: "Consumed", status: "True", reason: "RestoreAdmitted", message: `Restore ${restore.namespace}/${restore.name} (UID ${restore.uid}) was admitted under this verdict.`, lastTransitionTime: iso(entry.at + VERIFY_MS) },
    ],
  };
}

function approvalOf(ns: string, name: string, over: Partial<Approval>): Approval {
  return {
    name,
    namespace: ns,
    uid: uid(name.length * 97 + ns.length),
    resourceVersion: String(65000 + name.length),
    createdAt: iso(Date.now()),
    approvalBytesLength: 540,
    sidecarBytesLength: 180,
    approvalSubject: "ordinary",
    conditions: [],
    planHash: "",
    subjectRef: { kind: "Restore", name },
    selfAttestedRisk: false,
    ...over,
  };
}

/** The demo restore with its live state: pending until its approval verifies, then it runs. */
async function restoreOf(d: Demo, withPlan: boolean): Promise<Restore> {
  const planHash = await planHashOf(d);
  const r: Restore = { ...d.restore, planHash, planBytesLength: d.plan.length, ...(withPlan ? { planBytes: d.plan } : {}) };
  const entry = stored(r.namespace).get(r.approvalRef.name);
  if (entry && Date.now() - entry.at >= VERIFY_MS) {
    const running = Date.now() - entry.at < VERIFY_MS + 12_000;
    return {
      ...r,
      operation: running
        ? { state: "running", terminal: false, verificationState: "pending", verifiedSuccess: false }
        : { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "valid", verifiedSuccess: true, exitCode: 0, outcome: "pass" },
    };
  }
  return r;
}

function demoOf(ns: string, name: string): Demo | undefined {
  return demos.find((d) => d.restore.namespace === ns && d.restore.name === name);
}

function ownsNamespace(ns: string): boolean {
  return demos.some((d) => d.restore.namespace === ns);
}

// ---------------------------------------------------------------- confirm-mode records

/** A one-person confirmation's signed document; OD-10's typed names for an original-name restore. */
function confirmationDocument(r: Restore): string {
  const [issuer, subject] = ALICE.split("#");
  return JSON.stringify({
    formatVersion: r.approvalSubject === "originalName" ? "2.1.0" : "2.0.0",
    kind: "RestoreAuthorization",
    authorizationMode: "Ordinary",
    subject: { apiVersion: "logweir.dev/v1alpha1", kind: "Restore", namespace: r.namespace, name: r.name, uid: r.uid },
    planHash: r.planHash,
    requester: { issuer, subject },
    policy: { name: "default-confirm-v1", digest: DIGEST(501) },
    issuedAt: r.createdAt,
    expiresAt: iso(Date.parse(String(r.createdAt)) + 15 * MINUTE),
    ...(r.approvalSubject === "originalName" ? { approvalSubject: "originalName", originalNameConfirmation: { typedTopics: r.newTopics } } : {}),
  });
}

function confirmedApproval(r: Restore): Approval {
  const at = Date.parse(String(r.createdAt));
  return settled(
    {
      at,
      approval: approvalOf(r.namespace, r.approvalRef.name, {
        createdAt: r.createdAt,
        planHash: r.planHash,
        subjectRef: { kind: "Restore", name: r.name },
        approvalSubject: r.approvalSubject,
        approver: ALICE,
        matchedKeyId: CONSOLE_KEY,
        authorization: { confirmationKeyId: CONSOLE_KEY, mode: "Ordinary", policyDigest: DIGEST(501), policyName: "default-confirm-v1", requester: ALICE },
      }),
    },
    r,
  );
}

// ---------------------------------------------------------------- two-person requests

async function requestOf(d: Demo): Promise<ApprovalRequest> {
  const r = await restoreOf(d, false);
  const entry = stored(r.namespace).get(r.approvalRef.name);
  const approved = d.approvedBy ?? (entry ? { who: String(entry.approval.approver), at: entry.at } : undefined);
  const base = {
    approvalName: r.approvalRef.name,
    approvalSubject: r.approvalSubject,
    confirmationName: `${r.approvalRef.name}-confirmation`,
    confirmationSha256: DIGEST(d.restore.name.length * 17),
    issuedAt: iso(d.issuedAt),
    expiresAt: iso(d.issuedAt + HOUR),
    namespace: r.namespace,
    planHash: r.planHash,
    policy: "prod-pair",
    policyDigest: DIGEST(502),
    requester: d.requester,
    restore: r.name,
    restoreUid: r.uid,
    ticket: d.ticket ?? null,
    scopeComplete: true,
    scope: d.scope,
  };
  if (approved) {
    return {
      ...base,
      state: "approved",
      stateSentence: "A second person approved this request in the console.",
      approver: approved.who,
      approvedAt: iso(approved.at),
      approve: { offered: false, refusal: "notPending", sentence: "This request is already approved." },
    };
  }
  const own = d.requester === ALICE;
  return {
    ...base,
    state: "pending",
    stateSentence: "The console confirmed this request and it is waiting for a second person to approve it.",
    approve: own
      ? { offered: false, refusal: "requester", sentence: "You requested this restore. A two-person approval needs a second person, and no role changes that." }
      : {
          offered: true,
          sentence: "You are signed in as an approver and you are not the requester: approving records your identity and the time in the signed approval. No key is needed.",
        },
  };
}

// ---------------------------------------------------------------- strict confirmations

function governedDocument(d: Demo, planHash: string): string {
  const [issuer, subject] = d.requester.split("#");
  return JSON.stringify(
    {
      formatVersion: "2.0.0",
      kind: "RestoreAuthorization",
      authorizationMode: "Governed",
      subject: { apiVersion: "logweir.dev/v1alpha1", kind: "Restore", namespace: d.restore.namespace, name: d.restore.name, uid: d.restore.uid },
      planHash,
      requester: { issuer, subject },
      policy: { name: "prod-strict", digest: DIGEST(503) },
      issuedAt: iso(d.issuedAt),
      expiresAt: iso(d.issuedAt + HOUR),
      ticket: d.ticket,
    },
    null,
    0,
  );
}

const CONSOLE_SIDECAR = JSON.stringify({
  payloadType: "application/vnd.logweir.restore-authorization+json;version=2.0.0",
  signatures: [{ keyid: CONSOLE_KEY, sig: "rjYAdluGmt/dReYbvsBkzPFXtQEHeeO01CCjJdOWTmBOBq2GR7Mj5p/2cNBEiAOTRRI0Yy1pgfTnJFNDpNGbAQ==" }],
});

// ---------------------------------------------------------------- operations

function operationOf(r: Restore): OperationView {
  const waiting = r.operation.state === "pending";
  return {
    kind: "restore",
    name: r.name,
    namespace: r.namespace,
    uid: r.uid,
    resourceVersion: r.resourceVersion,
    createdAt: r.createdAt,
    lastUpdatedAt: iso(Date.now()),
    state: r.operation.state,
    stateReason: r.operation.stateReason,
    stage: waiting ? "admission" : r.operation.terminal ? "finished" : "running",
    terminal: r.operation.terminal,
    stale: false,
    awaitingApproval: waiting,
    verifiedSuccess: r.operation.verifiedSuccess,
    conditions: waiting
      ? [{ type: "Admitted", status: "False", reason: "ApprovalNotVerified", message: `Waiting for Approval ${r.approvalRef.name} to be verified.`, lastTransitionTime: r.createdAt }]
      : [{ type: "Admitted", status: "True", reason: "Approved", message: `Approval ${r.approvalRef.name} verified.`, lastTransitionTime: r.createdAt }],
    progress: { stage: waiting ? "admission" : "running", message: waiting ? "Waiting for approval" : "" },
    readiness: { state: "ready", basis: "preflight" },
    result: r.operation.terminal ? { status: "pass", exitCode: 0, outcome: "pass", lastPhaseCompleted: 7 } : { status: "pending" },
    verification: r.operation.terminal ? { state: "valid", matchedKeyId: CONSOLE_KEY, payloadType: "scorecard" } : { state: "pending" },
    verificationScope: { level: "sampled", coverage: "sampled" },
    trust: r.operation.terminal ? { state: "verified", basis: "Current" } : { state: "pending", basis: "none" },
    evidence: {},
  } as OperationView;
}

// ---------------------------------------------------------------- routing

async function body(request: Request): Promise<Record<string, unknown>> {
  try {
    const value: unknown = await request.json();
    return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

function carriesPrivateKey(text: unknown): boolean {
  return typeof text === "string" && /private[\s_-]*key/i.test(text);
}

async function listApprovals(ns: string): Promise<Approval[]> {
  if (!ownsNamespace(ns)) {
    return sharedRestores.filter((r) => r.namespace === ns).map(confirmedApproval);
  }
  const out: Approval[] = [];
  for (const d of demos.filter((x) => x.restore.namespace === ns)) {
    const one = await approvalNamed(ns, d.restore.approvalRef.name);
    if (one) out.push(one);
  }
  return out;
}

async function approvalNamed(ns: string, name: string): Promise<Approval | null> {
  if (!ownsNamespace(ns)) {
    const r = sharedRestores.find((x) => x.namespace === ns && x.approvalRef.name === name);
    return r ? confirmedApproval(r) : null;
  }
  const d = demos.find((x) => x.restore.namespace === ns && x.restore.approvalRef.name === name);
  if (!d) return null;
  const r = await restoreOf(d, false);
  if (d.approvedBy) {
    return settled(
      {
        at: d.approvedBy.at,
        approval: approvalOf(ns, name, {
          createdAt: iso(d.approvedBy.at),
          planHash: r.planHash,
          approver: d.approvedBy.who,
          matchedKeyId: CONSOLE_KEY,
          ticket: d.ticket ?? null,
          authorization: { confirmationKeyId: CONSOLE_KEY, mode: "Governed", policyDigest: DIGEST(502), policyName: "prod-pair", requester: d.requester },
        }),
      },
      r,
    );
  }
  const entry = stored(ns).get(name);
  return entry ? settled(entry, r) : null;
}

/** The handler fetch.ts consults first; null for a request it does not own. */
export async function approvalsMock(request: Request, method: string, path: string): Promise<Response | null> {
  let p: RegExpMatchArray | null;

  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/approval-policy$/))) {
    return json(200, { item: policyOf(p[1]!), requestId: requestId() });
  }
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/approvals$/))) {
    return json(200, page(await listApprovals(p[1]!)));
  }
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/approvals\/([^/]+)\/packet$/))) {
    const [ns, name] = [p[1]!, p[2]!];
    const ledger = demos.find((d) => d.restore.namespace === ns && `${d.restore.approvalRef.name}-confirmation` === name && ns === "ledger");
    if (ledger) {
      const r = await restoreOf(ledger, false);
      return json(200, {
        item: { name, namespace: ns, uid: uid(name.length * 31), planHash: r.planHash, subjectRef: { kind: "Restore", name: r.name }, approvalBytes: governedDocument(ledger, r.planHash), sidecarBytes: CONSOLE_SIDECAR },
        requestId: requestId(),
      });
    }
    const shared = !ownsNamespace(ns) ? sharedRestores.find((x) => x.namespace === ns && x.approvalRef.name === name) : undefined;
    if (shared) {
      return json(200, {
        item: { name, namespace: ns, uid: uid(name.length * 97 + ns.length), planHash: shared.planHash, subjectRef: { kind: "Restore", name: shared.name }, approvalBytes: confirmationDocument(shared), sidecarBytes: CONSOLE_SIDECAR },
        requestId: requestId(),
      });
    }
    const entry = stored(ns).get(name);
    if (entry) {
      const sent = JSON.parse(entry.body) as { approvalBytes?: string; sidecarBytes?: string };
      return json(200, {
        item: { name, namespace: ns, uid: entry.approval.uid, planHash: entry.approval.planHash, subjectRef: entry.approval.subjectRef, approvalBytes: sent.approvalBytes ?? "", sidecarBytes: sent.sidecarBytes ?? "" },
        requestId: requestId(),
      });
    }
    return problem(404, "not_found", `No Approval named ${name} in ${ns}.`);
  }
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/approvals\/([^/]+)$/))) {
    const found = await approvalNamed(p[1]!, p[2]!);
    return found ? json(200, { item: found, requestId: requestId() }) : problem(404, "not_found", `No Approval named ${p[2]} in ${p[1]}.`);
  }

  // The demo namespaces' restores: listed, read with their plan, and followed.
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/restores$/)) && ownsNamespace(p[1]!)) {
    const ns = p[1]!;
    return json(200, page(await Promise.all(demos.filter((d) => d.restore.namespace === ns).map((d) => restoreOf(d, false)))));
  }
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/restores\/([^/]+)$/)) && ownsNamespace(p[1]!)) {
    const d = demoOf(p[1]!, p[2]!);
    return d ? json(200, { item: await restoreOf(d, true), requestId: requestId() }) : problem(404, "not_found", `No Restore named ${p[2]} in ${p[1]}.`);
  }
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/operations\/restore\/([^/]+)$/)) && ownsNamespace(p[1]!)) {
    const d = demoOf(p[1]!, p[2]!);
    return d ? json(200, { item: operationOf(await restoreOf(d, false)), requestId: requestId() }) : problem(404, "not_found", `No restore named ${p[2]}.`);
  }

  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/restores\/([^/]+)\/approval-request$/))) {
    const [ns, name] = [p[1]!, p[2]!];
    if (policyOf(ns).operatorMode !== "two-person") {
      return problem(409, "policy_mismatch", `Namespace ${ns} does not take its approval from the console (not_a_console_approval_policy).`);
    }
    const d = demoOf(ns, name);
    return d ? json(200, { item: await requestOf(d), requestId: requestId() }) : problem(404, "not_found", `No Restore named ${name} in ${ns}.`);
  }

  if (method === "POST" && (p = path.match(/^\/namespaces\/([^/]+)\/restores\/([^/]+)\/console-approval$/))) {
    const [ns, name] = [p[1]!, p[2]!];
    const sent = await body(request);
    const d = demoOf(ns, name);
    if (policyOf(ns).operatorMode !== "two-person") return problem(409, "policy_mismatch", `Namespace ${ns} does not take its approval from the console.`);
    if (!d) return problem(404, "not_found", `No Restore named ${name} in ${ns}.`);
    if (Object.keys(sent).some((k) => k !== "confirmationSha256")) {
      return problem(422, "validation_failed", "The body names the request that was shown and nothing else.", [{ field: Object.keys(sent).find((k) => k !== "confirmationSha256")!, code: "unknown_field", message: "unknown field" }]);
    }
    const view = await requestOf(d);
    if (sent.confirmationSha256 !== view.confirmationSha256) {
      return problem(409, "state_conflict", "The request changed since it was shown (request_changed). Nothing was approved; read it again.");
    }
    if (d.requester === ALICE) {
      return problem(403, "forbidden", `Policy prod-pair needs a second person: you requested this restore (${ALICE}). Nothing was approved.`);
    }
    const existing = stored(ns).get(d.restore.approvalRef.name);
    if (existing || d.approvedBy) {
      if (existing && existing.approval.approver === ALICE) {
        return json(200, { item: await approvalNamed(ns, d.restore.approvalRef.name), replayed: true, requestId: requestId() });
      }
      return problem(409, "state_conflict", "Someone else already approved this request (already_approved).");
    }
    const r = await restoreOf(d, false);
    const approval = approvalOf(ns, r.approvalRef.name, {
      planHash: r.planHash,
      approver: ALICE,
      matchedKeyId: CONSOLE_KEY,
      ticket: d.ticket ?? null,
      authorization: { confirmationKeyId: CONSOLE_KEY, mode: "Governed", policyDigest: DIGEST(502), policyName: "prod-pair", requester: d.requester },
    });
    stored(ns).set(r.approvalRef.name, { approval, at: Date.now(), body: JSON.stringify(sent) });
    return json(201, { item: approval, requestId: requestId() });
  }

  if (method === "POST" && (p = path.match(/^\/namespaces\/([^/]+)\/restores\/([^/]+)\/approval$/))) {
    const [ns, name] = [p[1]!, p[2]!];
    const sent = await body(request);
    const keyed = (["sidecarBytes", "approvalBytes"] as const).filter((f) => carriesPrivateKey(sent[f]));
    if (keyed.length > 0) {
      return problem(422, "validation_failed", "A field carries private-key text.", keyed.map((field) => ({ field, code: "private_key", message: "this carries private-key text; paste the signed files `logweir drill` wrote, never a key. Nothing was stored." })));
    }
    let sidecar: { payloadType?: unknown; signatures?: unknown } | null = null;
    try {
      sidecar = JSON.parse(String(sent.sidecarBytes ?? ""));
    } catch {
      sidecar = null;
    }
    if (!sidecar || typeof sidecar !== "object") {
      return problem(422, "validation_failed", "The sidecar is not a DSSE sidecar.", [{ field: "sidecarBytes", code: "not_a_sidecar", message: "must be the DSSE sidecar `logweir drill countersign` wrote" }]);
    }
    const mode = policyOf(ns);
    const d = demoOf(ns, name) ?? undefined;
    if (!d && ownsNamespace(ns)) return problem(404, "not_found", `No Restore named ${name} in ${ns}.`);
    if (mode.legacy) {
      if (typeof sent.approvalBytes !== "string" || sent.approvalBytes.length === 0) {
        return problem(422, "validation_failed", "Both documents are needed.", [{ field: "approvalBytes", code: "required", message: "an unbound namespace records the approval.json `logweir drill approve` wrote, beside its sidecar" }]);
      }
      if (sidecar.payloadType !== V1_APPROVAL_PAYLOAD) {
        return problem(409, "policy_mismatch", `Namespace ${ns} is bound to no approval policy (legacy-governed-v1), which records the approval logweir drill approve signs; this sidecar's payload type is ${String(sidecar.payloadType ?? "absent")}.`);
      }
      const r = await restoreOf(d!, false);
      let signed: unknown;
      try {
        signed = (JSON.parse(sent.approvalBytes) as { plan_hash?: unknown }).plan_hash;
      } catch {
        signed = undefined;
      }
      if (signed !== r.planHash) {
        return problem(422, "validation_failed", "The approval names another plan.", [{ field: "approvalBytes", code: "plan_mismatch", message: `the approval document names plan hash ${String(signed ?? "none")} and Restore ${r.name} hashes to ${r.planHash}; sign this Restore's plan` }]);
      }
      return record(ns, r, sent, APPROVER_KEY, "incident-commander");
    }
    if (mode.mode !== "governed" || mode.operatorMode === "two-person") {
      return problem(409, "policy_mismatch", `Namespace ${ns} is bound to ${mode.name} (${mode.operatorMode}), not a strict policy, so there is no countersignature to submit here.`);
    }
    if (sent.approvalBytes !== undefined) {
      return problem(422, "validation_failed", "Only the sidecar is accepted.", [{ field: "approvalBytes", code: "not_accepted", message: "under an explicit approval policy the console's confirmation document is the one signed; submit only the countersigned sidecar" }]);
    }
    if (d!.requester === ALICE) {
      return problem(403, "forbidden", `You requested this restore (${ALICE}); policy prod-strict requires the approver to be a different principal from the requester, and no role changes that.`);
    }
    if (!Array.isArray(sidecar.signatures) || sidecar.signatures.length < 2) {
      return problem(422, "validation_failed", "The sidecar adds no signature.", [{ field: "sidecarBytes", code: "no_countersignature", message: "the sidecar carries no signature beside the console's: run logweir drill countersign over the confirmation" }]);
    }
    return record(ns, await restoreOf(d!, false), sent, APPROVER_KEY, ALICE, {
      ticket: d!.ticket ?? null,
      authorization: { confirmationKeyId: CONSOLE_KEY, mode: "Governed", policyDigest: DIGEST(503), policyName: "prod-strict", requester: d!.requester },
    });
  }

  return null;
}

function record(ns: string, r: Restore, sent: Record<string, unknown>, key: string, approver: string, over: Partial<Approval> = {}): Response {
  const text = JSON.stringify(sent);
  const existing = stored(ns).get(r.approvalRef.name);
  if (existing) {
    if (existing.body === text) return json(200, { item: settled(existing, r), replayed: true, requestId: requestId() });
    return problem(409, "state_conflict", `Another Approval already holds the name ${r.approvalRef.name}; it is never overwritten.`);
  }
  const approval = approvalOf(ns, r.approvalRef.name, { planHash: r.planHash, approver, matchedKeyId: key, approvalSubject: r.approvalSubject, ...over });
  stored(ns).set(r.approvalRef.name, { approval, at: Date.now(), body: text });
  return json(201, { item: approval, requestId: requestId() });
}
