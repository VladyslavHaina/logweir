// The Restores page group's mock (PROD-18.1 R5): restores and their operations, the
// restore Preflight, the approval policy and a two-person request, answered the way
// logweir-api answers them (the hash and mapping rails, OD-10's typed names, the ticket,
// idempotent creates). `restoresMock` answers the routes this group owns;
// `restoresFallback` answers the reads it needs from other groups' routes (destinations,
// catalogs, a backup's receipt digest, the target's discovery) only when nothing else did.
import type { components } from "../api/schema";
import { DAY, HOUR, MINUTE, NOW, iso } from "./clock";
import { backups, connections, hex, pointOf, requestId, restores as baseRestores } from "./data";
import { renderPlanBytes, type PlanFields } from "../pages/restores/plan";
import { planBlockValue } from "../pages/restores/words";

type S = components["schemas"];
type Restore = S["Restore"];
type OperationView = S["OperationView"];
type Preflight = S["Preflight"];

// -------------------------------------------------------------- responses

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" },
  });
}

function problem(status: number, code: string, detail: string, errors?: { field: string; code: string; message: string }[]): Response {
  return json(status, { status, code, title: code, detail, requestId: requestId(), retryable: false, type: `https://logweir.dev/problems/${code}`, ...(errors ? { errors } : {}) });
}

function page<T>(items: T[]) {
  return { items, page: { limit: 200, snapshot: String(62000 + items.length) }, requestId: requestId() };
}

async function sha256(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return `sha256:${Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("")}`;
}

// ------------------------------------------------------- the approval policy

export type MockMode = "confirm" | "two-person" | "strict" | "legacy";

/** The mode each namespace is under. The walk switches it through `__lwMockApprovalMode`
 *  (mock builds only) to draw the two-person and strict review steps. */
const modes: Record<string, MockMode> = { "team-a": "confirm", "team-b": "two-person" };
(globalThis as { __lwMockApprovalMode?: Record<string, MockMode> }).__lwMockApprovalMode = modes;

const POLICY_DIGEST = `sha256:${hex(501, 64)}`;
export function policyFor(ns: string): S["ApprovalPolicyView"] {
  const mode = modes[ns] ?? "confirm";
  const governed = mode === "two-person" || mode === "strict";
  return {
    namespace: ns,
    name: mode === "legacy" ? "legacy-governed-v1" : mode === "confirm" ? "default-confirm-v1" : mode === "two-person" ? "prod-pair" : "prod-strict",
    mode: mode === "confirm" ? "ordinary" : "governed",
    operatorMode: mode === "legacy" ? "strict" : mode,
    legacy: mode === "legacy",
    basis: mode === "legacy" ? "legacy" : mode === "confirm" ? "freshInstall" : "binding",
    requireDistinctPrincipal: governed,
    ordinaryConfirmationAvailable: mode === "confirm",
    consoleApprovalAvailable: mode === "two-person",
    ticketRequired: governed,
    installationDigest: `sha256:${hex(502, 64)}`,
    ...(mode === "legacy" ? {} : { digest: POLICY_DIGEST, maxAgeSeconds: 3600 }),
  };
}

// ------------------------------------------------- frozen destination facts

const PRIMARY_UID = "5f0c2a1e-7b3d-4c9a-8e21-6d4b9f0a1c37";
const PRIMARY_DIGEST = `sha256:${hex(510, 64)}`;
const VAULT_UID = "a81d7e44-02c9-4b6f-9d3a-1f5e8c7b2a90";

// The mock's runs were frozen against `primary`: the uid and location digest every
// destination-backed point carries.
for (const b of backups) {
  if (b.destinationRef?.name === "primary") {
    b.destinationRef.uid = PRIMARY_UID;
    b.locationDigest = PRIMARY_DIGEST;
  }
}

function destination(name: string, ns: string): S["Destination"] | null {
  const base = {
    namespace: ns,
    resourceVersion: "61500",
    generation: 3,
    createdAt: iso(NOW - 34 * DAY),
    writeProbe: "disabled",
    status: { state: "valid", reason: "Valid", observedGeneration: 3 },
    access: {},
  } as unknown as S["Destination"];
  if (name === "primary") {
    return {
      ...base,
      name,
      uid: PRIMARY_UID,
      default: true,
      description: "The team's backup bucket",
      canonicalUrl: "s3://kafka-backups/team-a/prod",
      locationDigest: PRIMARY_DIGEST,
      storage: { provider: "s3", bucket: "kafka-backups", prefix: "team-a/prod", region: "eu-west-1", endpoint: "https://minio.storage.svc:9000", addressing: "pathStyle" },
      transport: { security: "tls" },
    } as S["Destination"];
  }
  if (name === "evidence-vault") {
    return {
      ...base,
      name,
      uid: VAULT_UID,
      default: false,
      description: "Write-once evidence store",
      canonicalUrl: "s3://logweir-evidence",
      locationDigest: `sha256:${hex(511, 64)}`,
      storage: { provider: "s3", bucket: "logweir-evidence", prefix: "", region: "eu-west-1", addressing: "virtualHosted" },
      transport: { security: "tls" },
    } as S["Destination"];
  }
  return null;
}

// ------------------------------------------------------------------ restores

/** A plan for a mock restore, in the real grammar, so its hash and plan view are true. */
function mockPlan(r: Restore, topics: string[], extra: Partial<PlanFields["sample"]> = {}): string {
  const original = r.target.originalName;
  return renderPlanBytes({
    backupSetRef: r.backupSetRef,
    topics,
    pointInTime: r.pointInTime,
    timeBasis: "",
    source: { bucket: "kafka-backups", prefix: "team-a/prod", region: "eu-west-1", endpoint: "https://minio.storage.svc:9000", pathStyle: true, allowHttp: false },
    target: {
      bootstrapServers: ["kafka-0.orders-dr.svc:9093", "kafka-1.orders-dr.svc:9093", "kafka-2.orders-dr.svc:9093"],
      auth: { mode: "scramSha512", username: "logweir-restorer", tls: true },
      mode: r.target.mode,
      topicPrefix: r.target.topicPrefix,
      topicMappingPrefix: original ? "restore-20261001T000000Z-" : r.target.topicPrefix,
      markerTopic: "logweir.scratch",
      replicationFactor: 3,
      teardown: "delete",
      ...(original ? { originalName: true, originalNameNoOwner: true } : {}),
    },
    sample: {
      windowStart: iso(Date.parse(r.pointInTime) - HOUR),
      windowEnd: r.pointInTime,
      recordsPerPartition: 25,
      anchor: "head",
      ...(r.coverage.requested === "complete" ? { coverage: "complete" } : {}),
      ...extra,
    },
    objectives: {},
    evidence: { bucket: "kafka-backups", prefix: "logweir/", region: "eu-west-1", endpoint: "https://minio.storage.svc:9000", pathStyle: true, allowHttp: false },
  });
}

const ORDERS = ["orders", "orders-dlq", "payments"];

/** The extra demo restores this group draws: a run that stopped while creating topics,
 *  a complete verification that did not cover, and a partition subset. */
const failed: Restore = {
  ...baseRestores[2]!,
  name: "orders-drill-retry-x7c2m",
  uid: "0b7e5d2c-9a14-4f3e-8c61-2d9a7b4e1f05",
  createdAt: iso(NOW - 26 * HOUR),
  newTopics: ["drill2-orders", "drill2-orders-dlq", "drill2-payments"],
  coverage: { requested: "sampled" },
  operation: { state: "failed", stateReason: "TargetTopicAppeared", terminal: true, verificationState: "noEvidence", verifiedSuccess: false, exitCode: 1, outcome: undefined },
  target: { clusterRef: { name: "orders-dr" }, mode: "newTopic", originalName: false, topicPrefix: "drill2-" },
  pointInTime: iso(NOW - 27 * HOUR),
  targetTopicsAppeared: {
    appeared: ["drill2-orders"],
    appearedCount: 1,
    left: ["drill2-orders-dlq"],
    leftCount: 1,
    leftInstruction: "created by this restore and left empty; remove it yourself once you have checked nothing writes to it",
    unconfirmed: [],
    unconfirmedCount: 0,
  },
};
const uncovered: Restore = {
  ...baseRestores[2]!,
  name: "orders-full-check-h4b8r",
  uid: "6c3f1e2a-8b4d-4a7e-9f10-3e5d7c9b1a24",
  createdAt: iso(NOW - 2 * DAY),
  newTopics: ["check-orders", "check-orders-dlq", "check-payments"],
  coverage: { requested: "complete", recorded: "complete", covered: false, completeMaxRecords: 2_000_000, incompleteReason: "the record bound (2,000,000) was reached" },
  operation: { state: "failed", stateReason: "DrillNotPass", terminal: true, verificationState: "valid", verifiedSuccess: false, exitCode: 2, outcome: "fail-integrity" },
  target: { clusterRef: { name: "orders-dr" }, mode: "newTopic", originalName: false, topicPrefix: "check-" },
  pointInTime: iso(NOW - 2 * DAY - HOUR),
};
const subset: Restore = {
  ...baseRestores[2]!,
  name: "orders-partition-fix-p2k9w",
  uid: "9e2d4b6a-1c3f-4e8b-a7d5-0f6c2e9b4a13",
  createdAt: iso(NOW - 5 * DAY),
  newTopics: ["fix-orders"],
  coverage: { requested: "complete", recorded: "complete", covered: true },
  selection: { scope: "partial", narrowedTopics: 1, partitions: [{ topic: "orders", partitions: [1] }], engineRuns: 1 },
  target: { clusterRef: { name: "orders-dr" }, mode: "newTopic", originalName: false, topicPrefix: "fix-" },
  pointInTime: iso(NOW - 5 * DAY - 2 * HOUR),
};

interface Kept {
  item: Restore;
  topics: string[];
  key?: string;
  createdMs: number;
  awaiting?: boolean;
  requester?: string;
  ticket?: string;
}

const store: Kept[] = [];
const ready: Promise<void> = (async () => {
  for (const r of [...baseRestores, failed, uncovered, subset]) {
    const topics = r === subset ? ["orders"] : ORDERS;
    const planBytes = mockPlan(r, topics, r === uncovered ? { completeMaxRecords: 2_000_000 } : {});
    store.push({
      item: { ...r, planBytes, planBytesLength: new TextEncoder().encode(planBytes).length, planHash: await sha256(planBytes), sourceDestinationRef: { name: "primary" }, evidenceDestinationRef: { name: "primary" } },
      topics,
      createdMs: Date.parse(String(r.createdAt)),
    });
  }
})();

const STAGES = [
  { until: 2, state: "pending", stage: "admission" },
  { until: 6, state: "preparing", stage: "preparing" },
  { until: 20, state: "running", stage: "running" },
  { until: 27, state: "verifying", stage: "verifying" },
] as const;
const RECORDS = 1_182_410;

/** A new restore moves through its stages once admitted; one waiting for approval does not. */
function live(k: Kept): Restore {
  if (k.key === undefined) return k.item;
  if (k.awaiting) return { ...k.item, operation: { state: "pending", stateReason: "ApprovalNotVerified", terminal: false, verificationState: "pending", verifiedSuccess: false } };
  const e = (Date.now() - k.createdMs) / 1000;
  const stage = STAGES.find((s) => e < s.until);
  if (stage) return { ...k.item, operation: { state: stage.state, terminal: false, verificationState: "pending", verifiedSuccess: false } };
  return {
    ...k.item,
    coverage: { ...k.item.coverage, recorded: k.item.coverage.requested, ...(k.item.coverage.requested === "complete" ? { covered: true } : {}) },
    operation: { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "valid", verifiedSuccess: true, exitCode: 0, outcome: "pass" },
  };
}

const list = (ns: string) => store.filter((k) => k.item.namespace === ns).sort((a, b) => b.createdMs - a.createdMs);
const SIGNER = "2c76e22ff89969dc0337e64756c85f18edb3e51ae2950ea18d81021d7176d7fe";

function partitionRows(topics: string[], parts: Record<string, number>, matching: (t: string, p: number) => number, expected: (t: string, p: number) => number) {
  return topics.flatMap((topic) =>
    Array.from({ length: parts[topic] ?? 1 }, (_, partition) => {
      const exp = expected(topic, partition);
      const got = matching(topic, partition);
      return {
        topic,
        partition,
        compared: got >= 0,
        replay: { expected: exp, restored: Math.max(got, 0), matching: Math.max(got, 0), missing: got >= 0 ? exp - got : exp, unexpected: 0, duplicates: 0, outOfOrder: 0, mismatched: 0 },
      };
    }),
  );
}

const PARTS: Record<string, number> = { orders: 3, "orders-dlq": 1, payments: 6 };

function operationOf(k: Kept): OperationView {
  const r = live(k);
  const since = k.createdMs;
  const e = (Date.now() - since) / 1000;
  const done = r.operation.terminal;
  const succeeded = r.operation.state === "succeeded";
  const complete = r.coverage.requested === "complete";
  const stage = done ? "finished" : k.awaiting ? "admission" : (STAGES.find((s) => e < s.until)?.stage ?? "finished");
  const conditions: S["ConditionView"][] = [];
  if (k.awaiting) {
    conditions.push({ type: "Admitted", status: "False", reason: "ApprovalNotVerified", message: "Waiting for an approval of this plan.", lastTransitionTime: iso(since) });
  } else {
    conditions.push({ type: "Admitted", status: "True", reason: "ApprovalVerified", message: `Plan ${r.planHash.slice(0, 19)}... approved; the approval verified.`, lastTransitionTime: iso(since) });
    if (e >= 2 || done) conditions.push({ type: "Preparing", status: "True", reason: "CreatingTopics", message: `Creating ${r.newTopics.length} topics on ${r.target.clusterRef.name}.`, lastTransitionTime: iso(since + 2000) });
    if (r === failed || r.name === failed.name) {
      conditions.push({ type: "Failed", status: "True", reason: "TargetTopicAppeared", message: "drill2-orders appeared on the target after the restore was admitted; the restore stopped before writing any record.", lastTransitionTime: iso(since + 4000) });
    } else {
      if (e >= 6 || done) conditions.push({ type: "Running", status: "True", reason: "Replaying", message: "Replaying records from the archive.", lastTransitionTime: iso(since + 6000) });
      if (e >= 20 || done) conditions.push({ type: "Verifying", status: "True", reason: complete ? "Complete" : "Sampling", message: complete ? "Comparing every restored record with the archive." : "Comparing a sample with the archive.", lastTransitionTime: iso(since + 20000) });
      if (done) conditions.push({ type: succeeded ? "Succeeded" : "Failed", status: "True", reason: r.operation.stateReason ?? "Ok", message: succeeded ? "Restore verified; the scorecard is signed and the controller verified it." : "The scorecard records a failed integrity check.", lastTransitionTime: iso(since + 27000) });
    }
  }
  const replayed = done ? RECORDS : Math.max(0, Math.min(1, (e - 6) / 14)) * RECORDS;
  const evidence = done && r.name !== failed.name
    ? { payloadKey: `logweir/drills/${r.uid.slice(0, 8)}.scorecard.json`, payloadSha256: `sha256:${hex(r.uid.charCodeAt(0) + 7, 64)}`, sidecarKey: `logweir/drills/${r.uid.slice(0, 8)}.scorecard.sig`, offsetReportKey: `logweir/drills/${r.uid.slice(0, 8)}.offsets.json` }
    : {};
  const verified = done && r.operation.verificationState === "valid";
  let completeBlock: S["CompleteVerificationView"] | undefined;
  if (done && complete && r.name !== failed.name) {
    const topics = k.topics;
    if (r.name === uncovered.name) {
      const rows = partitionRows(topics, PARTS, (t, p) => (t === "payments" && p >= 3 ? -1 : 40_000), () => 40_000);
      completeBlock = { covered: false, incompleteReason: "the record bound (2,000,000) was reached", maxRecords: 2_000_000, partitionCount: rows.length, partitions: rows, recordsDecoded: 2_000_000, offsetHoles: 0, segments: 120, segmentsVerified: 84, segmentsFailed: 0, segmentsUnverified: 36, replay: { expected: 400_000, restored: 280_000, matching: 280_000, missing: 120_000, unexpected: 0, duplicates: 0, outOfOrder: 0, mismatched: 0 } };
    } else {
      const sel = r.selection?.partitions;
      const rows = sel
        ? sel.flatMap((s) => s.partitions.map((p) => ({ topic: s.topic, partition: p, compared: true, replay: { expected: 39_870, restored: 39_870, matching: 39_870, missing: 0, unexpected: 0, duplicates: 0, outOfOrder: 0, mismatched: 0 } })))
        : partitionRows(topics, PARTS, () => 118_241, () => 118_241);
      const total = rows.reduce((a, b) => a + b.replay.expected, 0);
      completeBlock = { covered: true, partitionCount: rows.length, partitions: rows, recordsDecoded: total, offsetHoles: 0, segments: rows.length * 4, segmentsVerified: rows.length * 4, segmentsFailed: 0, segmentsUnverified: 0, replay: { expected: total, restored: total, matching: total, missing: 0, unexpected: 0, duplicates: 0, outOfOrder: 0, mismatched: 0 } };
    }
  }
  const diagnostics = r.name === failed.name
    ? [{ code: "TargetTopicAppeared", severity: "Error", message: "A mapped target topic was created by someone else after admission: drill2-orders.", count: 1, firstSeen: iso(since + 4000), lastSeen: iso(since + 4000), object: { kind: "Restore", name: r.name } }]
    : [];
  return {
    kind: "restore",
    name: r.name,
    namespace: r.namespace,
    uid: r.uid,
    resourceVersion: String(70000 + Math.floor(e)),
    createdAt: r.createdAt,
    lastUpdatedAt: iso(Math.min(Date.now(), since + 27000)),
    state: r.operation.state,
    stateReason: r.operation.stateReason,
    message: conditions[conditions.length - 1]?.message,
    stage,
    terminal: done,
    stale: false,
    awaitingApproval: !!k.awaiting,
    verifiedSuccess: r.operation.verifiedSuccess,
    targetMode: r.target.mode,
    conditions,
    diagnostics,
    progress: {
      stage,
      reason: r.operation.stateReason,
      message: done ? "Finished" : stage === "running" ? `Replayed ${Math.round(replayed).toLocaleString("en-US")} of ${RECORDS.toLocaleString("en-US")} records` : "",
      ...(k.awaiting ? {} : { runnerPhase: { name: done ? "report" : "replay", number: done ? 9 : 4 } }),
      lastTransitionTime: conditions[conditions.length - 1]?.lastTransitionTime,
    },
    readiness: { state: "unknown", basis: "notImplemented" },
    result: done
      ? r.name === failed.name
        ? { status: "notPass", exitCode: 1, exitReason: "TargetTopicAppeared", lastPhaseCompleted: 5 }
        : { status: succeeded ? "pass" : "notPass", exitCode: r.operation.exitCode ?? 0, exitReason: r.operation.outcome ?? "ok", outcome: r.operation.outcome ?? "pass", lastPhaseCompleted: 9 }
      : { status: "pending" },
    verification: verified ? { state: "valid", verifiedAt: iso(since + 27000), matchedKeyId: SIGNER, payloadType: "application/vnd.logweir.scorecard+json" } : { state: done ? "noEvidence" : "pending" },
    trust: verified ? { state: "verified", basis: "Current", signedAt: iso(since + 26000), keyState: "Active", policy: { name: "team-a", generation: 2 } } : { state: done ? "notApplicable" : "pending", basis: "None" },
    evidence,
    verificationScope: complete
      ? { level: "sampled", coverage: done && r.name !== failed.name ? "complete" : undefined, ...(completeBlock ? { complete: completeBlock } : {}), ...(r.selection ? { selection: r.selection } : {}) }
      : r.name === failed.name
        ? { level: "none" }
        : { level: "sampled", coverage: done ? "sampled" : undefined, recordsExpected: 10_000, recordsSampled: done ? 10_000 : undefined, recordsSampledMatching: done ? 10_000 : undefined },
    completion:
      done && succeeded
        ? {
            recordsExpected: complete ? undefined : 10_000,
            recordsRestored: complete ? undefined : 10_000,
            recordsSampled: complete ? completeBlock?.replay.expected : 10_000,
            recordsSampledMatching: complete ? completeBlock?.replay.matching : 10_000,
            integrityLevel: "byte-fingerprint",
            newTopics: r.newTopics.map((name, i) => ({ name, partitions: PARTS[k.topics[i] ?? "orders"] ?? 3 })),
          }
        : undefined,
    ...(r.target.mode === "scratch" && done ? { teardown: { deleted: r.newTopics, deletedTruncated: false, attestationKey: `logweir/drills/${r.uid.slice(0, 8)}.teardown.json` } } : {}),
  } as OperationView;
}

// --------------------------------------------------------------- the create

let serial = 0;
/** One request per key at a time, as the API's idempotency record serialises them. */
const inflight = new Map<string, Promise<unknown>>();

async function create(ns: string, request: Request): Promise<Response> {
  const key = request.headers.get("Idempotency-Key") ?? "";
  if (key.length < 8) return problem(400, "idempotency_key_required", "A durable create carries an Idempotency-Key.");
  const full = `${ns}/${key}`;
  while (inflight.has(full)) await inflight.get(full);
  const run = createOnce(ns, full, request);
  inflight.set(full, run.catch(() => null));
  try {
    return await run;
  } finally {
    inflight.delete(full);
  }
}

async function createOnce(ns: string, full: string, request: Request): Promise<Response> {
  await ready;
  const body = (await request.json()) as S["CreateRestoreRequest"];
  const seen = store.find((k) => k.key === full);
  if (seen) return json(200, { item: live(seen), replayed: true, authorization: routing(ns, seen), requestId: requestId() });
  const errors: { field: string; code: string; message: string }[] = [];
  if ((await sha256(body.planBytes)) !== body.planHash) errors.push({ field: "planHash", code: "hash_mismatch", message: "is not the sha256 of planBytes" });
  const naming = body.target.topicNaming;
  const prefix = naming.originalName ? "" : naming.prefix;
  (body.topicMapping ?? []).forEach((row, i) => {
    if (row.target !== prefix + row.source) errors.push({ field: `topicMapping[${i}].target`, code: "mapping_mismatch", message: `is not ${prefix + row.source}, the prefix and the source` });
  });
  const mode = modes[ns] ?? "confirm";
  const sources = (body.topicMapping ?? []).map((r) => r.source);
  if (naming.originalName && mode === "confirm") {
    const typed = body.originalNameConfirmation?.typedTopics;
    if (!typed) errors.push({ field: "originalNameConfirmation", code: "typed_topics_required", message: "a restore under the original names confirmed by one person re-types every topic name" });
    else if ([...typed].sort().join("\n") !== [...sources].sort().join("\n")) errors.push({ field: "originalNameConfirmation.typedTopics", code: "typed_topics_mismatch", message: "is not exactly the plan's topics" });
  } else if (body.originalNameConfirmation) {
    errors.push({ field: "originalNameConfirmation", code: "not_accepted", message: "is accepted only for a restore under the original names confirmed by one person" });
  }
  if ((mode === "two-person" || mode === "strict") && !body.ticket) errors.push({ field: "ticket", code: "required", message: "a governed namespace requires a change ticket" });
  if (errors.length) return problem(422, "validation_failed", "One or more fields are invalid.", errors);

  serial += 1;
  const name = `rst-${(NOW / MINUTE + serial).toString(36).slice(-5)}${hex(serial + 900, 4)}`;
  const connection = body.target.clusterRef.name;
  const item: Restore = {
    name,
    namespace: ns,
    uid: crypto.randomUUID(),
    resourceVersion: String(63000 + serial),
    createdAt: iso(Date.now()),
    approvalRef: body.approvalRef,
    approvalSubject: naming.originalName ? "originalName" : "ordinary",
    backupSetRef: body.backupSetRef,
    coverage: { requested: body.coverage ?? "sampled", ...(body.completeMaxRecords ? { completeMaxRecords: body.completeMaxRecords } : {}) },
    deadlineSeconds: body.deadlineSeconds,
    newTopics: (body.topicMapping ?? []).map((r) => r.target),
    operation: { state: "pending", terminal: false, verificationState: "pending", verifiedSuccess: false },
    planBytes: body.planBytes,
    planBytesLength: new TextEncoder().encode(body.planBytes).length,
    planHash: body.planHash,
    pointInTime: body.pointInTime,
    sourceArchive: { url: body.sourceArchive.url, ...(body.sourceArchive.credentialRef ? { credentialRef: body.sourceArchive.credentialRef } : {}) },
    ...(body.sourceDestinationRef ? { sourceDestinationRef: body.sourceDestinationRef } : {}),
    ...(body.evidenceDestinationRef ? { evidenceDestinationRef: body.evidenceDestinationRef } : {}),
    target: { clusterRef: { name: connection }, mode: body.target.mode, originalName: naming.originalName === true, topicPrefix: naming.prefix },
  };
  if (body.target.mode === "scratch") item.newTopics = sources.map((s) => (planBlockValue(body.planBytes, "target", "topic_mapping_prefix") ?? "") + s);
  const kept: Kept = { item, topics: sources, key: full, createdMs: Date.now(), awaiting: mode !== "confirm", requester: "https://dex.logweir.localtest.me#alice", ticket: body.ticket ?? undefined };
  store.push(kept);
  return json(201, { item: live(kept), replayed: false, authorization: routing(ns, kept), requestId: requestId() });
}

function routing(ns: string, k: Kept): S["RestoreRoutingView"] {
  const p = policyFor(ns);
  return {
    state: k.awaiting ? "awaitingApproval" : "confirmed",
    mode: p.mode,
    operatorMode: p.operatorMode,
    legacy: p.legacy,
    policy: p.name,
    approvalName: k.item.approvalRef.name,
    ...(p.legacy ? {} : { requester: k.requester, policyDigest: POLICY_DIGEST, expiresAt: iso(k.createdMs + HOUR) }),
    ...(p.mode === "governed" && !p.legacy ? { confirmationName: `${k.item.approvalRef.name}-confirmation` } : {}),
  };
}

function approvalRequest(ns: string, k: Kept): S["ApprovalRequestView"] {
  const p = policyFor(ns);
  return {
    namespace: ns,
    restore: k.item.name,
    restoreUid: k.item.uid,
    approvalName: k.item.approvalRef.name,
    confirmationName: `${k.item.approvalRef.name}-confirmation`,
    approvalSubject: k.item.approvalSubject,
    planHash: k.item.planHash,
    policy: p.name,
    policyDigest: POLICY_DIGEST,
    requester: k.requester,
    ticket: k.ticket,
    issuedAt: iso(k.createdMs),
    expiresAt: iso(k.createdMs + HOUR),
    state: "pending",
    stateSentence: "The console confirmed this request and it is waiting for a second person to approve it.",
    approve: { offered: false, sentence: "You requested this restore, so a different person approves it." },
    scopeComplete: true,
  } as S["ApprovalRequestView"];
}

// -------------------------------------------------------------- preflights

interface KeptCheck {
  item: Preflight;
  createdMs: number;
  checks: S["CheckEntryView"][];
  aggregate: S["Preflight"]["state"];
}
const checks = new Map<string, KeptCheck>();

function existingTopics(cluster: string): Set<string> {
  const names = store.filter((k) => k.item.target.clusterRef.name === cluster).flatMap((k) => k.item.newTopics);
  return new Set(names);
}

function restoreChecks(ns: string, body: S["RestorePreflightRequest"]): { checks: S["CheckEntryView"][]; aggregate: S["Preflight"]["state"] } {
  const now = iso(Date.now());
  const row = (id: string, code: string, state: S["CheckEntryView"]["state"], extra: Partial<S["CheckEntryView"]> = {}): S["CheckEntryView"] => ({
    id,
    code,
    state,
    gating: "blocking",
    authority: id.startsWith("target") || id.startsWith("archive") ? "checkJob" : "controller",
    observedAt: now,
    ...extra,
  });
  const plan = body.planBytes ?? "";
  const target = body.target ?? "";
  const prefix = planBlockValue(plan, "target", "topic_mapping_prefix") ?? "";
  const original = /original_name:/.test(plan);
  const naming = original ? "" : (/topic_naming:\n {4}prefix: "([^"]*)"/.exec(plan)?.[1] ?? prefix);
  const mode = planBlockValue(plan, "target", "mode");
  const topics = [...plan.matchAll(/^ {4}- "([^"]+)"$/gm)].map((m) => m[1]!).filter((t) => ORDERS.includes(t) || !t.includes(":"));
  const mapped = topics.map((t) => (mode === "scratch" ? prefix : naming) + t);
  const taken = mapped.filter((t) => existingTopics(target).has(t));
  const rf = Number(planBlockValue(plan, "target", "default_replication_factor") ?? "1");
  const brokers = target === "orders-dr" ? 3 : 1;
  const out = [
    row("plan.parse", "PlanParsed", "ready"),
    row("plan.bindings", "PlanBound", "ready"),
    row("destination.resolved", "DestinationValid", "ready", { scope: { kind: "BackupDestination", name: body.sourceDestination ?? "primary" } }),
    row("archive.coverage", "PointInWindow", "ready"),
    row("target.resolved", "ConnectionResolved", connections.some((c) => c.namespace === ns && c.name === target) ? "ready" : "notReady"),
    row("target.authenticated", "Authenticated", "ready"),
    taken.length
      ? row("target.mappedTopics", "MappedTopicExists", "notReady", { message: `${taken.join(", ")} already exist on ${target}`, remedy: "Choose another prefix, or remove the topics if nothing uses them." })
      : row("target.mappedTopics", "MappedTopicsAbsent", "ready"),
    rf > brokers
      ? row("target.topicCreate", "ReplicationFactorExceedsBrokers", "notReady", { message: `${target} has ${brokers} brokers and the plan asks for ${rf} replicas`, remedy: `Set the replication factor to ${brokers} or fewer.` })
      : row("target.topicCreate", "TopicCreateAllowed", "ready"),
    row("approval.state", "SubjectNotCreated", "skipped", { message: "No Restore exists for this plan yet, so no approval can be checked." }),
  ];
  const blocking = out.filter((c) => c.state !== "ready");
  const aggregate = blocking.some((c) => c.state === "notReady") ? "notReady" : blocking.length ? "unknown" : "ready";
  return { checks: out, aggregate };
}

function preflightNow(k: KeptCheck, planHash: string | null): Preflight {
  const e = (Date.now() - k.createdMs) / 1000;
  const cancelled = k.item.state === "cancelled";
  const terminal = cancelled || e >= 3.5;
  const state = cancelled ? "cancelled" : e < 1.2 ? "pending" : e < 3.5 ? "running" : k.aggregate;
  const changed = planHash !== null && planHash !== "" && planHash !== k.item.binding.planHash;
  return {
    ...k.item,
    state,
    terminal,
    reason: terminal ? (state === "ready" ? "Ready" : state === "notReady" ? "NotReady" : "Unknown") : "Running",
    observedAt: terminal ? iso(k.createdMs + 3500) : undefined,
    checks: terminal && !cancelled ? k.checks : [],
    stale: changed,
    staleReasons: changed ? [{ reason: "planHashChanged" }] : [],
    applicable: terminal && !changed && !cancelled,
  };
}

let checkSerial = 0;

async function startCheck(ns: string, request: Request): Promise<Response | null> {
  const body = (await request.json()) as S["CreatePreflightRequest"];
  if (body.operation !== "restore" || !body.restore) return null;
  const r = body.restore;
  if (typeof r.planBytes === "string" && (await sha256(r.planBytes)) !== r.planHash) {
    return problem(422, "validation_failed", "One or more fields are invalid.", [{ field: "restore.planHash", code: "hash_mismatch", message: "is not the sha256 of planBytes" }]);
  }
  checkSerial += 1;
  const id = `pf-r5${(NOW / MINUTE + checkSerial).toString(36).slice(-4)}${hex(checkSerial + 700, 6)}`;
  const { checks: rows, aggregate } = restoreChecks(ns, r);
  const item: Preflight = {
    id,
    namespace: ns,
    uid: crypto.randomUUID(),
    resourceVersion: String(64000 + checkSerial),
    createdAt: iso(Date.now()),
    operation: "restore",
    state: "pending",
    terminal: false,
    applicable: false,
    stale: false,
    staleBasis: ["expiry", "planHash", `referent:KafkaCluster/${r.target ?? ""}`],
    staleReasons: [],
    binding: { planHash: r.planHash ?? undefined, inputsDigest: `sha256:${hex(checkSerial + 800, 64)}`, referents: [{ kind: "KafkaCluster", name: r.target ?? "" } as S["ReferentView"]] },
    checks: [],
    warnings: [{ id: "destination.evidenceReadable", code: "EvidenceReadGrantConfigured", state: "ready", gating: "advisory", authority: "controller" }],
    executionOnly: [{ id: "target.engineProtocol", note: "Whether the target accepts every record the archive holds is only proven by the run itself." }],
    conditions: [],
    detailsAvailable: false,
  };
  checks.set(`${ns}/${id}`, { item, createdMs: Date.now(), checks: rows, aggregate });
  return json(201, { item, replayed: false, requestId: requestId() });
}

// ----------------------------------------------------------------- routing

const at = (path: string, pattern: RegExp) => path.match(pattern);

/** The routes this group owns. */
export async function restoresMock(request: Request): Promise<Response | null> {
  const url = new URL(request.url);
  const path = url.pathname.replace(/^.*?\/api\/v1/, "");
  const method = request.method;
  let m: RegExpMatchArray | null;
  if (method === "GET" && (m = at(path, /^\/namespaces\/([^/]+)\/approval-policy$/))) return json(200, { item: policyFor(m[1]!), requestId: requestId() });
  if ((m = at(path, /^\/namespaces\/([^/]+)\/restores$/))) {
    await ready;
    if (method === "GET") return json(200, page(list(m[1]!).map(live).map(({ planBytes: _omit, ...rest }) => rest)));
    if (method === "POST") return create(m[1]!, request);
  }
  if (method === "GET" && (m = at(path, /^\/namespaces\/([^/]+)\/restores\/([^/:]+)$/))) {
    await ready;
    const k = list(m[1]!).find((x) => x.item.name === m![2]);
    return k ? json(200, { item: live(k), requestId: requestId() }) : problem(404, "not_found", `No Restore named ${m[2]} in ${m[1]}.`);
  }
  if (method === "GET" && (m = at(path, /^\/namespaces\/([^/]+)\/restores\/([^/]+)\/approval-request$/))) {
    await ready;
    const k = list(m[1]!).find((x) => x.item.name === m![2]);
    if (!k || !k.awaiting || modes[m[1]!] !== "two-person") return problem(404, "not_found", "No two-person request exists for this restore.");
    return json(200, { item: approvalRequest(m[1]!, k), requestId: requestId() });
  }
  if (method === "GET" && (m = at(path, /^\/namespaces\/([^/]+)\/operations\/restore\/([^/]+)$/))) {
    await ready;
    const k = list(m[1]!).find((x) => x.item.name === m![2]);
    return k ? json(200, { item: operationOf(k), requestId: requestId() }) : problem(404, "not_found", `No restore named ${m[2]}.`);
  }
  if (method === "POST" && (m = at(path, /^\/namespaces\/([^/]+)\/preflights$/))) {
    return startCheck(m[1]!, request.clone());
  }
  if ((m = at(path, /^\/namespaces\/([^/]+)\/preflights\/([^/:]+)(:cancel)?$/))) {
    const k = checks.get(`${m[1]}/${m[2]}`);
    if (!k) return null;
    if (method === "POST" && m[3]) {
      if (!preflightNow(k, null).terminal) k.item = { ...k.item, state: "cancelled" };
      return json(200, { item: preflightNow(k, null), requestId: requestId() });
    }
    if (method === "GET") return json(200, { item: preflightNow(k, url.searchParams.get("planHash")), requestId: requestId() });
  }
  return null;
}

/** Reads this group needs from routes other groups own, answered only when nothing else did. */
export async function restoresFallback(request: Request): Promise<Response | null> {
  const url = new URL(request.url);
  const path = url.pathname.replace(/^.*?\/api\/v1/, "");
  if (request.method !== "GET") return null;
  let m: RegExpMatchArray | null;
  if ((m = at(path, /^\/namespaces\/([^/]+)\/destinations$/))) {
    const ns = m[1]!;
    return json(200, page(ns === "team-a" ? [destination("primary", ns), destination("evidence-vault", ns)] : []));
  }
  if ((m = at(path, /^\/namespaces\/([^/]+)\/destinations\/([^/]+)$/))) {
    const d = m[1] === "team-a" ? destination(m[2]!, m[1]!) : null;
    return d ? json(200, { item: d, requestId: requestId() }) : problem(404, "not_found", `No destination named ${m[2]}.`);
  }
  if ((m = at(path, /^\/namespaces\/([^/]+)\/catalogs$/))) {
    return json(200, page(m[1] === "team-a" ? [catalog(m[1]!)] : []));
  }
  if ((m = at(path, /^\/namespaces\/([^/]+)\/catalogs\/primary$/))) return json(200, { item: catalog(m[1]!), requestId: requestId() });
  if ((m = at(path, /^\/namespaces\/([^/]+)\/operations\/backup\/([^/]+)$/))) {
    const b = backups.find((x) => x.namespace === m![1] && x.name === m![2]);
    if (!b) return problem(404, "not_found", `No backup named ${m[2]}.`);
    // The receipt digest a finished run reports; the base mock's live run finishes as the page watches.
    const signed = b.operation.verifiedSuccess || b.trigger?.kind === "Manual";
    return json(200, {
      item: { kind: "backup", name: b.name, namespace: b.namespace, uid: b.uid, state: b.operation.state, terminal: b.operation.terminal, evidence: signed ? { payloadKey: `logweir/receipts/${b.name}.receipt.json`, payloadSha256: pointOf(b).receiptSha256 } : {} },
      requestId: requestId(),
    });
  }
  if ((m = at(path, /^\/namespaces\/([^/]+)\/connections\/([^/]+)\/topic-discoveries$/)) && url.searchParams.get("latest") === "true") {
    const c = connections.find((x) => x.namespace === m![1] && x.name === m![2]);
    if (!c || c.role !== "target") return json(200, { latestAttempt: null, lastSuccessful: null, requestId: requestId() });
    const discovery = {
      id: `td-${c.name}-1`,
      namespace: c.namespace,
      uid: crypto.randomUUID(),
      resourceVersion: "62001",
      state: "succeeded",
      terminal: true,
      stale: false,
      staleReasons: [],
      truncated: false,
      chunkCount: 1,
      conditions: [],
      brokerCount: 3,
      observedAt: iso(NOW - 40 * MINUTE),
      connection: { name: c.name, uid: c.uid, generation: 1 },
    };
    return json(200, { latestAttempt: discovery, lastSuccessful: discovery, requestId: requestId() });
  }
  return null;
}

function catalog(ns: string): S["CatalogView"] {
  return {
    name: "primary",
    namespace: ns,
    uid: "c47a1f20-3d5e-4b8c-9a61-7e2f0d4b8c15",
    resourceVersion: "61900",
    destinationRef: { name: "primary" },
    mode: "Index",
    deepCheck: "None",
    intervalSeconds: 3600,
    truncated: false,
    viewExpired: false,
    viewLimit: 500,
    viewPoints: 30,
    counts: { points: 30 } as S["CatalogView"]["counts"],
    syncedAt: iso(NOW - 20 * MINUTE),
  };
}
