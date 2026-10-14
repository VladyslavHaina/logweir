// The R4 mock (Backups, the Recovery catalog, Trust): the backup operation
// view, the catalogs with their points and signers, connecting an archive, and
// the trust policies. Registered with one line in mock/fetch.ts; it answers
// only the routes it owns and hands everything else back (null).
import type { components } from "../api/schema";
import type { Backup } from "../api/types";
import { DAY, HOUR, MINUTE, NOW, iso } from "./clock";
import { backups, hex, pointOf, requestId } from "./data";

type S = components["schemas"];
type OperationView = S["OperationView"];
type Catalog = S["CatalogView"];
type PointView = S["PointView"];
type TrustPolicy = S["TrustPolicyView"];

/** The key every mock receipt verifies under, and the one retired before it. */
export const ACTIVE_KEY = "2c76e22ff89969dc0337e64756c85f18edb3e51ae2950ea18d81021d7176d7fe";
export const RETIRED_KEY = "1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f809";
const COMPROMISED_KEY = "3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b";
const APPROVAL_KEY = "8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6";
const STRANGER_KEY = "b51c9f0e2a3d47f6890ab1c2d3e4f5061728394a5b6c7d8e9f0a1b2c3d4e5f60";

type Route = (request: Request) => Promise<Response>;

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" },
  });
}

function problem(status: number, code: string, detail: string, errors?: S["FieldError"][]): Response {
  return json(status, { status, code, title: code, detail, requestId: requestId(), retryable: false, type: `https://logweir.dev/problems/${code}`, ...(errors ? { errors } : {}) });
}

function seedOf(text: string): number {
  let h = 2166136261;
  for (const ch of text) h = Math.imul(h ^ ch.charCodeAt(0), 16777619) >>> 0;
  return h;
}

// --------------------------------------------------------------- backups

/** A run an older installation wrote to an inline archive; the controller could not read its receipt. */
const LEGACY: Backup = {
  ...backups[backups.length - 1]!,
  name: "orders-import-20261007",
  uid: "6b1f0c3e-4d2a-4e8f-9b7c-1a2b3c4d5e6f",
  backupId: "01JA" + hex(4401, 22).toUpperCase(),
  createdAt: iso(NOW - 5 * DAY - 3 * HOUR),
  archive: { url: "s3://kafka-backups/drill-demo", credentialRef: { name: "kafka-backups-reader" } },
  destinationRef: undefined,
  locationDigest: undefined,
  schedule: undefined,
  scheduleRef: undefined,
  slot: undefined,
  trigger: undefined,
  triggeredBy: "manual",
  sourceRef: { name: "orders-prod" },
  topics: ["orders", "orders-dlq", "payments"],
  records: 1_204_118,
  observedAuth: { mode: "scramSha512", username: "logweir-reader" },
  windowCovered: { fromMs: NOW - 6 * DAY, toMs: NOW - 5 * DAY - 3 * HOUR + 9 * MINUTE },
  manifestKey: `drill-demo/${hex(4402, 26).toUpperCase()}/manifest.json`,
  operation: { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "notAttempted", verifiedSuccess: false, exitCode: 0 },
};
backups.push(LEGACY);

/** Runs older than this verified under the key that has since been retired. */
const HISTORICAL_BEFORE = NOW - 28 * HOUR;

function conditionsOf(b: Backup, at: number): S["ConditionView"][] {
  const list: S["ConditionView"][] = [
    { type: "Admitted", status: "True", reason: b.trigger?.kind === "Manual" ? "Manual" : "Scheduled", message: "admitted; the plan and its inputs are frozen", lastTransitionTime: iso(at) },
  ];
  if (b.operation.terminal) {
    list.push(
      b.operation.state === "succeeded"
        ? { type: "Complete", status: "True", reason: "Ok", message: "the runner exited 0 (ok)", lastTransitionTime: iso(at + 4 * MINUTE) }
        : { type: "Complete", status: "False", reason: b.operation.stateReason ?? "Failed", message: b.operation.outcome ?? "the run failed", lastTransitionTime: iso(at + 4 * MINUTE) },
    );
  }
  return list;
}

/** One run's operation view, from its current summary (the base mock moves the live run). */
export function operationOf(b: Backup): OperationView {
  const at = Date.parse(String(b.createdAt));
  const seed = seedOf(b.name);
  const op = b.operation;
  const keyBase = `logweir/backups/${b.backupId ?? "pending"}/${hex(seed, 12)}`;
  const common = {
    kind: "backup" as const,
    name: b.name,
    namespace: b.namespace,
    uid: b.uid,
    resourceVersion: String(70000 + (seed % 9000)),
    createdAt: b.createdAt,
    awaitingApproval: false,
    readiness: { state: "unknown", basis: "notImplemented" },
    stale: false,
    state: op.state,
    stateReason: op.stateReason,
    terminal: op.terminal,
    verifiedSuccess: op.verifiedSuccess,
    verificationScope: { level: "none" as const },
    conditions: conditionsOf(b, at),
    ...(op.terminal
      ? {
          stage: "finished" as const,
          progress: {
            stage: "finished" as const,
            reason: op.stateReason ?? "Finished",
            message: op.state === "succeeded" ? "the runner exited 0 (ok)" : (op.outcome ?? "the run ended"),
            lastTransitionTime: iso(at + 4 * MINUTE),
            runnerPhase: { name: op.state === "succeeded" ? "report" : "copy", number: op.state === "succeeded" ? 6 : 3 },
          },
        }
      : {}),
  };
  if (!op.terminal) {
    const stage = op.state === "verifying" ? "verifying" : op.state === "running" ? "running" : op.state === "queued" ? "queued" : "admission";
    return {
      ...common,
      stage,
      lastUpdatedAt: iso(Date.now()),
      message: stage === "verifying" ? "the controller is fetching and verifying the signed receipt" : "copying the topics into the archive",
      progress: {
        stage,
        reason: stage === "verifying" ? "Verifying" : "Copying",
        message: stage === "verifying" ? "receipt written; verification in progress" : "reading partitions and writing segments",
        lastTransitionTime: iso(at + 30_000),
        lastObservedTime: iso(Date.now() - 5_000),
        runnerPhase: { name: stage === "verifying" ? "sign" : "copy", number: stage === "verifying" ? 5 : 3 },
      },
      result: { status: "pending" },
      evidence: {},
      verification: { state: "pending" },
      trust: { state: "pending", basis: "None" },
    } as OperationView;
  }
  if (op.state === "failed") {
    const unreachable = op.stateReason === "SourceUnreachable";
    return {
      ...common,
      lastUpdatedAt: iso(at + 4 * MINUTE),
      message: op.outcome ?? "the run failed",
      result: { status: "notPass", exitCode: op.exitCode ?? 1, exitReason: op.stateReason ?? "failed" },
      evidence: {},
      verification: { state: "noEvidence", detail: "the run wrote no receipt" },
      trust: { state: "notApplicable", basis: "None" },
      diagnostics: [
        unreachable
          ? { code: "BrokerTransportFailure", severity: "Error", message: "connection to kafka-0.analytics.svc:9093 timed out after 30 s", object: { kind: "Pod", name: `${b.name}-runner` }, count: 3, firstSeen: iso(at + MINUTE), lastSeen: iso(at + 3 * MINUTE) }
          : { code: "DeadlineExceeded", severity: "Error", message: "the Job ran past activeDeadlineSeconds (1800) and was stopped", object: { kind: "Job", name: `${b.name}-runner` }, count: 1, firstSeen: iso(at + 30 * MINUTE), lastSeen: iso(at + 30 * MINUTE) },
      ],
    } as OperationView;
  }
  // The receipt the catalog lists for this run (mock/data.ts `pointOf`), so the run and its point agree.
  const point = pointOf(b);
  const evidence = b.destinationRef
    ? { payloadKey: point.receiptKey, payloadSha256: point.receiptSha256, sidecarKey: point.receiptKey.replace(/\.json$/, ".sig") }
    : { payloadKey: `${keyBase}.receipt.json`, payloadSha256: "sha256:" + hex(seed + 2, 64), sidecarKey: `${keyBase}.receipt.sig` };
  if (op.verificationState !== "valid") {
    return {
      ...common,
      lastUpdatedAt: iso(at + 9 * MINUTE),
      message: "the runner exited 0 (ok)",
      result: { status: "pass", exitCode: 0, exitReason: "ok" },
      evidence,
      verification: {
        state: "notAttempted",
        detail: "the controller could not read the receipt: the Secret kafka-backups-reader that this inline archive names does not exist in team-a",
      },
      trust: { state: "notAttempted", basis: "None" },
      diagnostics: [
        { code: "CredentialSecretNotFound", severity: "Warning", message: 'secret "kafka-backups-reader" not found', object: { kind: "Job", name: `${b.name}-verify` }, count: 2, firstSeen: iso(at + 9 * MINUTE), lastSeen: iso(at + 12 * MINUTE) },
      ],
    } as OperationView;
  }
  const historical = at < HISTORICAL_BEFORE;
  return {
    ...common,
    lastUpdatedAt: iso(at + 4 * MINUTE + 11_000),
    message: "the runner exited 0 (ok)",
    result: { status: "pass", exitCode: 0, exitReason: "ok" },
    evidence,
    verification: {
      state: "valid",
      matchedKeyId: historical ? RETIRED_KEY : ACTIVE_KEY,
      verifiedAt: iso(at + 4 * MINUTE + 11_000),
      payloadType: "application/vnd.logweir.backup-receipt+json",
    },
    trust: {
      state: historical ? "verifiedHistorical" : "verified",
      basis: historical ? "Historical" : "Current",
      keyState: historical ? "Retired" : "Active",
      policy: { name: "org-default", generation: 3 },
      signedAt: iso(at + 4 * MINUTE + 2_000),
    },
  } as OperationView;
}

// --------------------------------------------------------------- catalogs

function view(name: string, extra: Partial<Catalog>): Catalog {
  return {
    name,
    namespace: "team-a",
    uid: `0f24d5a2-2d21-4a46-9d63-${hex(seedOf(name), 12)}`,
    resourceVersion: String(80000 + (seedOf(name) % 9000)),
    createdAt: iso(NOW - 30 * DAY),
    generation: 1,
    observedGeneration: 1,
    mode: "Index",
    deepCheck: "ManifestDigest",
    intervalSeconds: 300,
    viewLimit: 500,
    viewPoints: 0,
    truncated: false,
    viewExpired: false,
    counts: { total: 0, available: 0, missing: 0, unreadable: 0, unverified: 0, untrustedSigner: 0, invalid: 0, conflict: 0, deleted: 0, unsupportedFormat: 0 },
    ...extra,
  };
}

const READY: S["ConditionView"][] = [
  { type: "Ready", status: "True", reason: "ViewReady", message: "the bounded view is published and has not aged out", lastTransitionTime: iso(NOW - 4 * MINUTE) },
  { type: "Synced", status: "True", reason: "Succeeded", message: "the walk completed", lastTransitionTime: iso(NOW - 4 * MINUTE) },
  { type: "Stale", status: "False", reason: "ViewFresh", message: "the view is within two sync intervals of its last refresh", lastTransitionTime: iso(NOW - 4 * MINUTE) },
  { type: "TrustAvailable", status: "True", reason: "TrustMaterialPresent", message: "2 signing keys from TrustPolicy/org-default are available to verify with", lastTransitionTime: iso(NOW - 4 * MINUTE) },
];

function histogramOf(points: PointView[]): S["HistogramBucketView"][] {
  const byDay = new Map<string, number>();
  for (const p of points) {
    const day = String(p.recoveryPointAt ?? "").slice(0, 10);
    if (day) byDay.set(day, (byDay.get(day) ?? 0) + 1);
  }
  return [...byDay.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([day, n]) => ({ day, points: n }));
}

/** The connected archive of another installation: every state the catalog can report. */
function archivePoint(i: number, extra: Partial<PointView>): PointView {
  const at = NOW - (9 + i) * DAY;
  return {
    pointId: "lwp1-" + hex(5100 + i, 32),
    backupId: `89b585c5-5498-48dc-ae32-090809457ec8-${new Date(at).toISOString().slice(0, 10).replace(/-/g, "")}-020000`,
    runId: "01M2" + hex(5200 + i, 22).toUpperCase(),
    availability: "Available",
    verification: "Verified",
    selectable: true,
    formatVersion: "1.7.0",
    receiptKey: `logweir/backups/89b585c5/${hex(5300 + i, 12)}.receipt.json`,
    receiptSha256: "sha256:" + hex(5400 + i, 64),
    manifestKey: `dr/${hex(5500 + i, 26).toUpperCase()}/manifest.json`,
    manifestSha256: "sha256:" + hex(5600 + i, 64),
    signerKeyId: ACTIVE_KEY,
    coveredFrom: iso(at - DAY),
    coveredTo: iso(at + 12 * MINUTE),
    recoveryPointAt: iso(at + 11 * MINUTE),
    locations: [{ locationId: "s3://orders-dr-archive/logweir", availability: "Available" }],
    ...extra,
  };
}

const ARCHIVE_POINTS: PointView[] = [
  archivePoint(0, {
    topics: [
      { name: "orders", partitions: 3, replicationFactor: 3, applyRoute: "adminApi", configCoverage: "captured", schemaDependency: { verdict: "schemaDependent", sides: ["value"], schemaIds: [12, 14], basis: "sampled" } },
      { name: "payments", partitions: 6, replicationFactor: 3, applyRoute: "adminApi", configCoverage: "captured", schemaDependency: { verdict: "notDetected", basis: "sampled" } },
    ],
    consumerPositions: {
      listing: "complete",
      observedBeforeRecoveryPointMs: 2_400,
      groups: [
        { groupId: "orders-billing", groupType: "classic", outcome: "captured", active: false, positions: { related: 3, beyondEnd: 0, failed: 0, neverCommitted: 0, notObserved: 0, notRelated: 0 } },
        { groupId: "orders-audit-stream", groupType: "other", outcome: "excluded", reason: "GroupTypeNotCaptured" },
      ],
    },
  }),
  archivePoint(1, { verification: "VerifiedHistorical", signerKeyId: RETIRED_KEY }),
  archivePoint(2, {
    verification: "UntrustedSigner",
    selectable: false,
    signerKeyId: STRANGER_KEY,
    remedy: "the receipt verifies under a key this namespace's TrustPolicy does not list; compare its fingerprint out of band and have an administrator add it",
  }),
  archivePoint(3, {
    availability: "Missing",
    selectable: false,
    remedy: "The archive does not hold the objects this recovery point names. Restore from another point, or recover the objects from your own backup of the bucket.",
    locations: [{ locationId: "s3://orders-dr-archive/logweir", availability: "Missing" }],
  }),
  archivePoint(4, { availability: "Conflict", verification: "Invalid", selectable: false, remedy: "two records disagree about this identity, so the catalog cannot say which copy is the record" }),
  archivePoint(5, { availability: "Deleted", verification: "NotAttempted", selectable: false, remedy: "a completed tombstone exists for this point; its evidence under logweir/ is untouched" }),
];

/** The second page of the archive's view, so "read the next 200" has something to read. */
const ARCHIVE_PAGE_2: PointView[] = [archivePoint(6, { receiptKey: "[redacted].receipt.json" })];

const catalogs = new Map<string, Catalog>([
  [
    "dr-archive",
    view("dr-archive", {
      createdAt: iso(NOW - 12 * DAY),
      destinationRef: { name: "dr-archive" },
      mode: "Full",
      deepCheck: "SegmentSample",
      intervalSeconds: 0,
      viewLimit: 7,
      viewPoints: 7,
      truncated: true,
      conditions: READY.map((c) => (c.type === "TrustAvailable" ? { ...c, message: "1 of the 2 keys that signed these points is listed in TrustPolicy/org-default" } : c)),
      counts: { total: 9, available: 4, missing: 1, unreadable: 0, unverified: 0, untrustedSigner: 1, invalid: 0, conflict: 1, deleted: 1, unsupportedFormat: 0 },
      syncedAt: iso(NOW - 2 * HOUR),
      viewExpiresAt: iso(NOW + 22 * HOUR),
      lastSync: { startedAt: iso(NOW - 2 * HOUR - 40_000), finishedAt: iso(NOW - 2 * HOUR), exitCode: 0 },
      cursor: { complete: true },
      histogram: histogramOf([...ARCHIVE_POINTS, ...ARCHIVE_PAGE_2]),
      signers: [
        { keyId: ACTIVE_KEY, points: 5, trusted: true },
        { keyId: STRANGER_KEY, points: 1, principalHint: "install:another", trusted: false },
      ],
    }),
  ],
]);

/** The namespace's own catalog, built from the base mock's point view of destination primary. */
async function primary(route: Route, ns: string): Promise<Catalog> {
  const answer = await route(new Request(`https://logweir.invalid/api/v1/namespaces/${ns}/catalogs/primary/points`));
  const points = ((await answer.json()) as { items?: PointView[] }).items ?? [];
  return view("primary", {
    destinationRef: { name: "primary" },
    viewPoints: points.length,
    conditions: READY,
    counts: { total: points.length + 2, available: points.length, missing: 0, unreadable: 0, unverified: 0, untrustedSigner: 0, invalid: 0, conflict: 0, deleted: 2, unsupportedFormat: 0 },
    syncedAt: iso(NOW - 4 * MINUTE),
    viewExpiresAt: iso(NOW + 56 * MINUTE),
    lastSync: { startedAt: iso(NOW - 5 * MINUTE), finishedAt: iso(NOW - 4 * MINUTE), exitCode: 0 },
    cursor: { complete: true, indexShard: iso(NOW).slice(0, 10) },
    histogram: histogramOf(points),
    signers: [{ keyId: ACTIVE_KEY, points: points.length, trusted: true }],
  });
}

async function allCatalogs(route: Route, ns: string): Promise<Catalog[]> {
  if (ns !== "team-a") return [...catalogs.values()].filter((c) => c.namespace === ns);
  return [await primary(route, ns), ...[...catalogs.values()].filter((c) => c.namespace === ns)];
}

/** What a key was spent on, so a replay answers 200 and a different body under it 409. */
const spent = new Map<string, { body: string; name: string }>();

async function connect(request: Request, ns: string, route: Route): Promise<Response> {
  const key = request.headers.get("Idempotency-Key") ?? "";
  if (key.length < 8 || key.length > 128) return problem(400, "idempotency_key_required", "Durable creates need an Idempotency-Key of 8 to 128 visible characters.");
  const body = (await request.json()) as S["ConnectArchiveRequest"];
  const text = JSON.stringify(body);
  const before = spent.get(`${ns}/${key}`);
  if (before) {
    if (before.body !== text) return problem(409, "idempotency_conflict", "This Idempotency-Key was already used for a different request.");
    return json(200, { item: catalogs.get(before.name), replayed: true, requestId: requestId() });
  }
  const errors: S["FieldError"][] = [];
  if (!/^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$/.test(body.name ?? "")) {
    errors.push({ field: "name", code: "invalid_name", message: "must be a DNS-1123 subdomain" });
  }
  if (!body.destinationRef?.name) errors.push({ field: "destinationRef.name", code: "required", message: "name the saved destination to index" });
  if (errors.length > 0) return problem(422, "validation_failed", "One or more fields are invalid.", errors);
  const taken = (await allCatalogs(route, ns)).some((c) => c.name === body.name);
  if (taken) return problem(409, "state_conflict", `A RecoveryCatalog named ${body.name} already exists and this request did not create it.`);
  const made = view(body.name, {
    namespace: ns,
    createdAt: iso(Date.now()),
    destinationRef: body.destinationRef ?? undefined,
    mode: body.syncMode === "index" ? "Index" : "Full",
    conditions: [],
  });
  catalogs.set(made.name, made);
  spent.set(`${ns}/${key}`, { body: text, name: made.name });
  return json(201, { item: made, replayed: false, requestId: requestId() });
}

function page<T>(items: T[], extra: Record<string, unknown> = {}) {
  return { items, page: { limit: 200, snapshot: String(81000 + items.length), nextCursor: null as string | null }, requestId: requestId(), ...extra };
}

// ------------------------------------------------------------------ trust

function trustKey(keyId: string, extra: Partial<S["TrustKeyView"]>): S["TrustKeyView"] {
  return {
    keyId,
    algorithm: "p256",
    effectiveState: "Active",
    notBefore: iso(NOW - 200 * DAY),
    notAfter: iso(NOW + 165 * DAY),
    principal: { id: "install:logweir-team-a", display: "the team-a signing key" },
    state: "Active",
    usableForNewSignatures: true,
    usableForVerification: "Full",
    usages: ["EvidenceSigning"],
    ...extra,
  };
}

const TRUST: TrustPolicy[] = [
  {
    name: "org-default",
    uid: "5ddde3f8-9e79-4321-b1a1-02b29e1c9cbd",
    resourceVersion: "2677816",
    createdAt: iso(NOW - 200 * DAY),
    default: false,
    generation: 3,
    observedGeneration: 3,
    loaded: true,
    namespaces: ["team-a", "team-b"],
    boundNamespaces: ["team-a", "team-b"],
    namespacesFiltered: false,
    namespacesTruncated: false,
    allowedTargetClusterIds: 2,
    conflicts: [],
    keyCount: 4,
    keysTruncated: false,
    evaluation: { state: "fresh", reason: "evaluated", evaluatedAt: iso(NOW - MINUTE), serverTime: iso(NOW), ageSeconds: 60, freshWithinSeconds: 900 },
    conditions: [
      { type: "Loaded", status: "True", reason: "Loaded", message: "every spkiPem on this policy parsed and hashes to its own keyId: 4 keys", lastTransitionTime: iso(NOW - 30 * DAY) },
      { type: "Bound", status: "True", reason: "Bound", message: "this policy governs 2 namespaces", lastTransitionTime: iso(NOW - 30 * DAY) },
      { type: "ExpiringSoon", status: "False", reason: "NotExpiring", message: "no Active key reaches its notAfter within 30 days", lastTransitionTime: iso(NOW - 30 * DAY) },
      { type: "CompromiseGuard", status: "True", reason: "CompromiseRecorded", message: "a KeyCompromise revocation is recorded on this policy; the finalizer keeps a deletion from re-trusting the key", lastTransitionTime: iso(NOW - 40 * DAY) },
    ],
    keys: [
      trustKey(ACTIVE_KEY, {}),
      trustKey(RETIRED_KEY, {
        principal: { id: "install:logweir-team-a-2025", display: "the signing key this one replaced" },
        state: "Retired",
        effectiveState: "Retired",
        retiredAt: iso(NOW - 28 * HOUR),
        usableForNewSignatures: false,
        usableForVerification: "Historical",
      }),
      trustKey(COMPROMISED_KEY, {
        principal: { id: "install:ci-runner", display: "a key withdrawn after an incident" },
        state: "Revoked",
        effectiveState: "Revoked",
        revocationReason: "KeyCompromise",
        revocationEffectiveFrom: iso(NOW - 45 * DAY),
        revokedAt: iso(NOW - 40 * DAY),
        usableForNewSignatures: false,
        usableForVerification: "None",
      }),
      trustKey(APPROVAL_KEY, {
        principal: { id: "console:logweir-api", display: "the console's confirmation key" },
        usages: ["ConsoleConfirmation"],
      }),
    ],
  },
  {
    name: "installation-default",
    uid: "7a8b9c0d-1e2f-4a3b-8c4d-5e6f7a8b9c0d",
    resourceVersion: "2677001",
    createdAt: iso(NOW - 400 * DAY),
    default: true,
    generation: 5,
    observedGeneration: 5,
    loaded: true,
    namespaces: [],
    boundNamespaces: [],
    namespacesFiltered: true,
    namespacesTruncated: false,
    allowedTargetClusterIds: 0,
    conflicts: [],
    keyCount: 1,
    keysTruncated: false,
    evaluation: { state: "unknown", reason: "stale", evaluatedAt: iso(NOW - 2 * HOUR), serverTime: iso(NOW), ageSeconds: 7200, freshWithinSeconds: 900 },
    conditions: [{ type: "Loaded", status: "True", reason: "Loaded", message: "every spkiPem on this policy parsed: 1 key", lastTransitionTime: iso(NOW - 90 * DAY) }],
    keys: [
      trustKey("9f8e7d6c5b4a39281706f5e4d3c2b1a09f8e7d6c5b4a39281706f5e4d3c2b1a0", {
        principal: { id: "install:legacy-roster", display: "migrated from the legacy roster" },
        effectiveState: "unknown",
        usableForNewSignatures: undefined,
        usableForVerification: undefined,
      }),
    ],
  },
];

// ------------------------------------------------------------------ routing

/** The R4 routes; null for anything else. */
export async function backupsMock(request: Request, route: Route): Promise<Response | null> {
  const path = new URL(request.url).pathname.replace(/^.*?\/api\/v1/, "");
  const method = request.method;
  let p: RegExpMatchArray | null;

  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/operations\/backup\/([^/]+)$/))) {
    const answer = await route(new Request(`https://logweir.invalid/api/v1/namespaces/${p[1]}/backups/${p[2]}`));
    if (!answer.ok) return problem(404, "not_found", `No backup named ${p[2]}.`);
    const { item } = (await answer.json()) as { item: Backup };
    return json(200, { item: operationOf(item), requestId: requestId() });
  }

  if ((p = path.match(/^\/namespaces\/([^/]+)\/catalogs$/))) {
    const ns = p[1]!;
    if (method === "POST") return connect(request, ns, route);
    if (method === "GET") return json(200, page(await allCatalogs(route, ns)));
    return null;
  }
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/catalogs\/([^/]+)$/))) {
    const found = (await allCatalogs(route, p[1]!)).find((c) => c.name === p![2]);
    return found ? json(200, { item: found, requestId: requestId() }) : problem(404, "not_found", `No RecoveryCatalog named ${p[2]} in ${p[1]}.`);
  }
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/catalogs\/([^/]+)\/points$/))) {
    const name = p[2]!;
    if (name === "primary") return null; // the base mock's view, the one the restore wizard reads too
    const c = catalogs.get(name);
    if (!c || c.namespace !== p[1]) return problem(404, "not_found", `No RecoveryCatalog named ${name}.`);
    if (name !== "dr-archive") return json(200, { ...page([]), truncated: false, viewExpired: false });
    const query = new URL(request.url).searchParams;
    const selectable = query.get("selectable") === "true";
    const second = query.get("cursor") === "page-2";
    const items = (second ? ARCHIVE_PAGE_2 : ARCHIVE_POINTS).filter((x) => !selectable || x.selectable);
    const body = { ...page(items), truncated: true, viewExpired: false, viewExpiresAt: c.viewExpiresAt };
    if (!second) body.page = { ...body.page, nextCursor: "page-2" };
    return json(200, body);
  }
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/catalogs\/([^/]+)\/signers$/))) {
    const found = (await allCatalogs(route, p[1]!)).find((c) => c.name === p![2]);
    if (!found) return problem(404, "not_found", `No RecoveryCatalog named ${p[2]}.`);
    const signers = found.signers ?? [];
    return json(200, {
      items: signers,
      untrustedPoints: signers.filter((s) => s.trusted === false).reduce((n, s) => n + (s.points ?? 0), 0),
      fingerprintCommand: "openssl pkey -pubin -outform DER -in <key>.pub.pem | openssl dgst -sha256",
      requestId: requestId(),
    });
  }

  if (method === "GET" && path === "/trust-policies") return json(200, page(TRUST));
  if (method === "GET" && (p = path.match(/^\/trust-policies\/([^/]+)$/))) {
    const found = TRUST.find((t) => t.name === p![1]);
    return found ? json(200, { item: found, requestId: requestId() }) : problem(404, "not_found", `No TrustPolicy named ${p[1]}.`);
  }
  return null;
}
