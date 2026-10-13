// Mock documents for the prototype, typed by the generated product-API schema, so
// every field name and state here is one the real API publishes. Values imitate
// the PoC and the console fixtures (ui/tests/fixtures/console/).
import type {
  Backup,
  Capabilities,
  Connection,
  PointView,
  ProtectionPolicy,
  Restore,
  Schedule,
  Session,
} from "../api/types";
import { DAY, HOUR, MINUTE, NOW, iso, slot, slotName } from "./clock";

let counter = 0;
export function requestId(): string {
  counter += 1;
  return "01M2" + (NOW.toString(36) + counter.toString(36)).toUpperCase().padStart(22, "0").slice(-22);
}

/** Deterministic hex from a seed (mulberry32), so ids look real and stay stable. */
export function hex(seed: number, length: number): string {
  let a = Math.floor(seed) >>> 0;
  let out = "";
  while (out.length < length) {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    out += ((t ^ (t >>> 14)) >>> 0).toString(16).padStart(8, "0");
  }
  return out.slice(0, length);
}

function uid(seed: number): string {
  const h = hex(seed, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-9${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const ALL: Capabilities = {
  approvalPacketRead: true,
  approvalSubmit: true,
  approvalsRead: true,
  backupsRead: true,
  catalogConnect: true,
  catalogWindowQuery: true,
  catalogs: true,
  connectionCreate: true,
  connectionTest: true,
  connectionsRead: true,
  credentialWrite: true,
  destinations: true,
  manualBackupCreate: true,
  operationEvents: true,
  operationsRead: true,
  preflight: true,
  protection: true,
  restoreCreate: true,
  restoresRead: true,
  retention: true,
  scheduleCreate: true,
  scheduleSetSuspension: true,
  schedulesRead: true,
  topicDiscovery: true,
  trustAdministration: false,
  trustPoliciesRead: true,
};

export const NAMESPACES = ["team-a", "team-b"] as const;

export const session: Session = {
  actor: {
    displayName: "Alice Martin",
    id: "https://dex.logweir.localtest.me#alice",
    issuer: "https://dex.logweir.localtest.me",
    subject: "alice",
  },
  authenticationMode: "oidc",
  bindingRevision: "7",
  capabilities: ALL,
  csrfToken: "prototype-only",
  expiresAt: iso(NOW + 8 * HOUR),
  namespaces: [
    { name: "team-a", roles: ["operator", "approver"], capabilities: ALL },
    { name: "team-b", roles: ["operator"], capabilities: ALL },
  ],
  requestId: requestId(),
};

// ---------------------------------------------------------------- connections

export const connections: Connection[] = [
  {
    name: "orders-prod",
    namespace: "team-a",
    uid: uid(1),
    role: "source",
    resourceVersion: "60110",
    createdAt: iso(NOW - 34 * DAY),
    bootstrapServers: ["kafka-0.orders.svc:9093", "kafka-1.orders.svc:9093", "kafka-2.orders.svc:9093"],
    auth: { mode: "scramSha512", tls: true, username: "logweir-reader", credentialRef: { name: "orders-scram" } },
    markerTopic: "logweir.scratch",
    reachability: { state: "reachable", reason: "Reachable", clusterId: "MkU3NEVCTTlSM0FCQVRMQQ", observedAt: iso(NOW - 3 * MINUTE) },
  },
  {
    name: "orders-dr",
    namespace: "team-a",
    uid: uid(2),
    role: "target",
    resourceVersion: "60111",
    createdAt: iso(NOW - 34 * DAY),
    bootstrapServers: ["kafka-0.orders-dr.svc:9093", "kafka-1.orders-dr.svc:9093", "kafka-2.orders-dr.svc:9093"],
    auth: { mode: "scramSha512", tls: true, username: "logweir-restorer", credentialRef: { name: "orders-dr-scram" } },
    markerTopic: "logweir.scratch",
    reachability: { state: "reachable", reason: "Reachable", clusterId: "Q0xVU1RFUi1EUi0wMDAx", observedAt: iso(NOW - 3 * MINUTE) },
  },
  {
    name: "analytics-prod",
    namespace: "team-a",
    uid: uid(3),
    role: "source",
    resourceVersion: "60140",
    createdAt: iso(NOW - 20 * DAY),
    bootstrapServers: ["kafka-0.analytics.svc:9093"],
    auth: { mode: "mtls", tls: true },
    markerTopic: "logweir.scratch",
    reachability: { state: "unreachable", reason: "ProbeReportedUnreachable", observedAt: iso(NOW - 6 * MINUTE) },
  },
];

// --------------------------------------------------------------- destinations

/** Destinations are not drawn in the prototype; the overview counts them. */
export const DESTINATION_NAMES = ["primary"];

// ------------------------------------------------------------------ schedules

const ORDERS_TOPICS = ["orders", "orders-dlq", "payments"];

export const schedules: Schedule[] = [
  {
    name: "orders-hourly",
    namespace: "team-a",
    uid: uid(5),
    resourceVersion: "61002",
    schedule: "0 * * * *",
    timeZone: "UTC",
    suspended: false,
    concurrencyPolicy: "Forbid",
    sourceRef: { name: "orders-prod" },
    destinationRef: { name: "primary" },
    topics: ORDERS_TOPICS,
    archive: { url: "logweir-destination://primary" },
    retention: { keepDays: 14 },
    status: { nextFireTime: iso(slot(-1)), lastFireTime: iso(slot(0)) },
  },
  {
    name: "analytics-daily",
    namespace: "team-a",
    uid: uid(6),
    resourceVersion: "61009",
    schedule: "0 2 * * *",
    timeZone: "UTC",
    suspended: false,
    concurrencyPolicy: "Forbid",
    sourceRef: { name: "analytics-prod" },
    destinationRef: { name: "primary" },
    topics: ["clickstream", "sessions"],
    archive: { url: "logweir-destination://primary" },
    retention: { keepDays: 30 },
    status: { nextFireTime: iso(Math.floor(NOW / DAY) * DAY + DAY + 2 * HOUR) },
  },
];

// -------------------------------------------------------------------- backups

function verifiedBackup(name: string, at: number, records: number, extra: Partial<Backup> = {}): Backup {
  const seed = at / MINUTE;
  return {
    name,
    namespace: "team-a",
    uid: uid(seed),
    resourceVersion: String(60000 + (seed % 9000)),
    backupId: "01JB" + hex(seed, 22).toUpperCase(),
    createdAt: iso(at),
    deadlineSeconds: 1800,
    archive: { url: "logweir-destination://primary" },
    destinationRef: { name: "primary" },
    manifestKey: `team-a/prod/${hex(seed, 26).toUpperCase()}/manifest.json`,
    observedAuth: { mode: "scramSha512", username: "logweir-reader" },
    operation: { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "valid", verifiedSuccess: true, exitCode: 0 },
    records,
    schedule: "orders-hourly",
    scheduleRef: { name: "orders-hourly" },
    slot: slotName(at),
    sourceRef: { name: "orders-prod" },
    topics: ORDERS_TOPICS,
    trigger: { kind: "Scheduled", attempt: 1 },
    triggeredBy: "schedule",
    windowCovered: { fromMs: at - HOUR, toMs: at + 4 * MINUTE },
    ...extra,
  };
}

const hourly: Backup[] = [];
const currentSlotDone = NOW - slot(0) > 8 * MINUTE;
for (let h = currentSlotDone ? 0 : 1; h <= 30; h += 1) {
  const at = slot(h);
  const records = 104_000 + ((h * 7919) % 23_000);
  const name = `orders-hourly-${slotName(at)}`;
  if (h === 9) {
    hourly.push(
      verifiedBackup(`${name}-r2`, at + 6 * MINUTE, records, {
        trigger: { kind: "Retry", attempt: 2, retryOf: { name } },
      }),
    );
    hourly.push(
      verifiedBackup(name, at, 0, {
        operation: {
          state: "failed",
          stateReason: "DeadlineExceeded",
          terminal: true,
          verificationState: "notAttempted",
          verifiedSuccess: false,
          exitCode: 1,
          outcome: "the engine did not finish within deadlineSeconds (1800)",
        },
        records: undefined,
      }),
    );
  } else {
    hourly.push(verifiedBackup(name, at, records));
  }
}

const analytics: Backup[] = [1, 2, 3, 4].map((d) => {
  const at = Math.floor(NOW / DAY) * DAY - (d - 1) * DAY + 2 * HOUR - (NOW % DAY < 2 * HOUR ? DAY : 0);
  const failed = d <= 2;
  return verifiedBackup(`analytics-daily-${slotName(at)}`, at, failed ? 0 : 2_840_112 + d * 1201, {
    schedule: "analytics-daily",
    scheduleRef: { name: "analytics-daily" },
    sourceRef: { name: "analytics-prod" },
    topics: ["clickstream", "sessions"],
    observedAuth: { mode: "mtls" },
    windowCovered: { fromMs: at - DAY, toMs: at + 20 * MINUTE },
    ...(failed
      ? {
          records: undefined,
          operation: {
            state: "failed",
            stateReason: "SourceUnreachable",
            terminal: true,
            verificationState: "notAttempted",
            verifiedSuccess: false,
            exitCode: 2,
            outcome: "the source cluster analytics-prod did not answer: BrokerTransportFailure",
          },
        }
      : {}),
  });
});

/** The one run that moves while the prototype is open (see mock/fetch.ts). */
export const LIVE_BACKUP = `orders-manual-${slotName(NOW)}`;

const manual: Backup = verifiedBackup(LIVE_BACKUP, NOW - 2 * MINUTE, 0, {
  trigger: { kind: "Manual", attempt: 1 },
  triggeredBy: "manual",
  schedule: undefined,
  scheduleRef: undefined,
  slot: undefined,
  records: undefined,
  operation: { state: "running", terminal: false, verificationState: "pending", verifiedSuccess: false },
  windowCovered: undefined,
});

export const backups: Backup[] = [manual, ...hourly, ...analytics].sort((a, b) =>
  String(b.createdAt).localeCompare(String(a.createdAt)),
);

// ------------------------------------------------------------- recovery points

export const CONSUMER_GROUPS = [
  { groupId: "orders-billing", groupType: "classic", outcome: "captured", active: false, related: 4, notRelated: 0 },
  { groupId: "orders-shipping", groupType: "consumer", outcome: "captured", active: true, related: 3, notRelated: 0 },
  { groupId: "orders-reporting", groupType: "classic", outcome: "captured", active: false, related: 2, notRelated: 0 },
  { groupId: "orders-fraud", groupType: "classic", outcome: "captured", active: false, related: 2, notRelated: 1 },
  { groupId: "orders-audit-stream", groupType: "other", outcome: "excluded", reason: "GroupTypeNotCaptured" },
] as const;

export function pointOf(backup: Backup): PointView {
  const seed = Date.parse(String(backup.createdAt)) / MINUTE;
  return {
    pointId: "lwp1-" + hex(seed, 32),
    backupId: String(backup.backupId),
    runId: "01M2" + hex(seed + 1, 22).toUpperCase(),
    availability: "Available",
    verification: "Verified",
    selectable: true,
    formatVersion: "1.7.0",
    receiptKey: `logweir/receipts/${hex(seed, 12)}.receipt.json`,
    receiptSha256: "sha256:" + hex(seed + 2, 64),
    manifestKey: backup.manifestKey,
    manifestSha256: "sha256:" + hex(seed + 3, 64),
    signerKeyId: "2c76e22ff89969dc0337e64756c85f18edb3e51ae2950ea18d81021d7176d7fe",
    coveredFrom: iso(backup.windowCovered?.fromMs ?? seed * MINUTE),
    coveredTo: iso(backup.windowCovered?.toMs ?? seed * MINUTE),
    recoveryPointAt: iso((backup.windowCovered?.toMs ?? seed * MINUTE) - MINUTE),
    topics: [
      { name: "orders", partitions: 3, replicationFactor: 3, applyRoute: "createTopic", configCoverage: "captured" },
      { name: "orders-dlq", partitions: 1, replicationFactor: 3, applyRoute: "createTopic", configCoverage: "captured" },
      { name: "payments", partitions: 6, replicationFactor: 3, applyRoute: "createTopic", configCoverage: "captured" },
    ],
    consumerPositions: {
      listing: "complete",
      observedFrom: iso((backup.windowCovered?.toMs ?? 0) - 40_000),
      observedTo: iso((backup.windowCovered?.toMs ?? 0) - 38_000),
      groups: CONSUMER_GROUPS.map((g) =>
        g.outcome === "captured"
          ? {
              groupId: g.groupId,
              groupType: g.groupType,
              outcome: g.outcome,
              active: g.active,
              positions: { related: g.related, beyondEnd: 0, failed: 0, neverCommitted: 0, notObserved: 0, notRelated: g.notRelated },
            }
          : { groupId: g.groupId, groupType: g.groupType, outcome: g.outcome, reason: g.reason },
      ),
    },
  };
}

// ------------------------------------------------------------------- restores

export const restores: Restore[] = [
  {
    name: "orders-clone-k3v9d",
    namespace: "team-a",
    uid: uid(78),
    resourceVersion: "62010",
    createdAt: iso(NOW - HOUR),
    approvalRef: { name: "orders-clone-k3v9d" },
    approvalSubject: "ordinary",
    backupSetRef: "01JB" + hex(905, 22).toUpperCase(),
    coverage: { requested: "sampled", recorded: "sampled", covered: true },
    deadlineSeconds: 3600,
    newTopics: ["clone-orders", "clone-orders-dlq", "clone-payments"],
    operation: { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "valid", verifiedSuccess: true, exitCode: 0, outcome: "pass" },
    planBytesLength: 2266,
    planHash: "sha256:" + hex(906, 64),
    pointInTime: iso(slot(1) + 3 * MINUTE),
    sourceArchive: { url: "logweir-destination://primary" },
    target: { clusterRef: { name: "orders-dr" }, mode: "scratch", originalName: false, topicPrefix: "clone-" },
  },
  {
    // Theme A's demo: restored up to 12:57, just before a bad deploy, under the
    // original names, with a complete verification so positions can be mapped.
    name: "orders-recovery-q4m8x",
    namespace: "team-a",
    uid: uid(76),
    resourceVersion: "61800",
    createdAt: iso(NOW - 2 * HOUR - 14 * MINUTE),
    approvalRef: { name: "orders-recovery-q4m8x" },
    approvalSubject: "originalName",
    backupSetRef: "01JB" + hex(903, 22).toUpperCase(),
    coverage: { requested: "complete", recorded: "complete", covered: true },
    deadlineSeconds: 3600,
    newTopics: ["orders", "orders-dlq", "payments"],
    operation: { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "valid", verifiedSuccess: true, exitCode: 0, outcome: "pass" },
    planBytesLength: 2301,
    planHash: "sha256:" + hex(904, 64),
    pointInTime: iso(slot(3) - 3 * MINUTE),
    sourceArchive: { url: "logweir-destination://primary" },
    target: { clusterRef: { name: "orders-dr" }, mode: "newTopic", originalName: true, topicPrefix: "" },
  },
  {
    name: "orders-drill-weekly-a7k2p",
    namespace: "team-a",
    uid: uid(77),
    resourceVersion: "59001",
    createdAt: iso(NOW - 3 * DAY - 5 * HOUR),
    approvalRef: { name: "orders-drill-weekly-a7k2p" },
    approvalSubject: "ordinary",
    backupSetRef: "01JB" + hex(901, 22).toUpperCase(),
    coverage: { requested: "sampled", recorded: "sampled", covered: true },
    deadlineSeconds: 3600,
    newTopics: ["drill-orders", "drill-orders-dlq", "drill-payments"],
    operation: { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "valid", verifiedSuccess: true, exitCode: 0, outcome: "pass" },
    planBytesLength: 2214,
    planHash: "sha256:" + hex(902, 64),
    pointInTime: iso(NOW - 3 * DAY - 6 * HOUR),
    sourceArchive: { url: "logweir-destination://primary" },
    target: { clusterRef: { name: "orders-dr" }, mode: "newTopic", originalName: false, topicPrefix: "drill-" },
  },
];

export const protectionPolicies: ProtectionPolicy[] = [
  {
    name: "orders",
    namespace: "team-a",
    uid: uid(10),
    resourceVersion: "70001",
    evaluationIntervalSeconds: 60,
    evaluatedAt: iso(NOW - MINUTE),
    health: "Healthy",
    objectives: { maxRecoveryPointAgeSeconds: 2 * 3600, maxConsecutiveFailedRuns: 2, requireCatalogAvailability: true, requireVerifiedEvidence: true },
    protects: { sourceRef: { name: "orders-prod" }, destinationRef: { name: "primary" }, scheduleRefs: [{ name: "orders-hourly" }], topics: ORDERS_TOPICS },
    consecutiveFailedRuns: 0,
    schedules: [{ name: "orders-hourly", ready: "True", suspended: false, nextFireTime: iso(slot(-1)) }],
  },
  {
    name: "analytics",
    namespace: "team-a",
    uid: uid(11),
    resourceVersion: "70002",
    evaluationIntervalSeconds: 60,
    evaluatedAt: iso(NOW - MINUTE),
    health: "Unprotected",
    objectives: { maxRecoveryPointAgeSeconds: 26 * 3600, maxConsecutiveFailedRuns: 1, requireCatalogAvailability: true, requireVerifiedEvidence: true },
    protects: { sourceRef: { name: "analytics-prod" }, destinationRef: { name: "primary" }, scheduleRefs: [{ name: "analytics-daily" }], topics: ["clickstream", "sessions"] },
    consecutiveFailedRuns: 2,
    alerts: [{ key: "consecutiveFailedRuns", kind: "ConsecutiveFailedRuns", state: "open", openedAt: iso(NOW - 22 * HOUR) }],
    schedules: [{ name: "analytics-daily", ready: "True", suspended: false }],
  },
];

export function withPoint(policy: ProtectionPolicy, newest: Backup | undefined): ProtectionPolicy {
  if (!newest) return policy;
  const at = newest.windowCovered?.toMs ?? Date.parse(String(newest.createdAt));
  return {
    ...policy,
    lastAvailablePoint: {
      backupRef: { name: newest.name },
      recoveryPointAt: iso(at - MINUTE),
      ageSeconds: Math.round((Date.now() - at) / 1000),
      topics: newest.topics,
      topicsTruncated: false,
      evidence: "Verified",
    },
  };
}
