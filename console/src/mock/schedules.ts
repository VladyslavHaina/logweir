// The mock API for Schedules and Protection (PROD-18.1 R3): the schedule
// routes, the cadence preview, retention policies and one protection policy,
// with the refusals the real routes give (422 with field paths, 409 on a reused
// key, 412 on a stale revision). mock/fetch.ts asks this module first; for a
// request it does not own it answers null and the shared mock handles it.
//
// Destinations, topic discoveries and backup readiness checks belong to other
// pages. This module answers them only when no other mock does, so the schedule
// form works on its own.
import type { components } from "../api/schema";
import type { Backup, Schedule } from "../api/types";
import { CadenceError, checkZone, matchPreset, nextRuns, compilePreset, type PresetKind } from "./cadence";
import { DAY, HOUR, MINUTE, NOW, iso } from "./clock";
import { backups, connections, hex, protectionPolicies, requestId, schedules } from "./data";
import { mockFetch } from "./fetch";

type S = components["schemas"];
type Problem = S["Problem"];
type FieldError = S["FieldError"];

const PASS = "x-lw-mock-passthrough";
const TZDB = "chrono-tz 0.10.4";

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" },
  });
}

function problem(status: number, code: Problem["code"], detail: string, extra: Partial<Problem> = {}): Response {
  return json(status, {
    type: `https://logweir.dev/problems/${code.replace(/_/g, "-")}`,
    title: code,
    status,
    code,
    detail,
    requestId: requestId(),
    retryable: false,
    ...extra,
  } satisfies Problem);
}

function invalid(errors: FieldError[]): Response {
  return problem(422, "validation_failed", "One or more fields are invalid.", { errors });
}

/** Ask the shared mock, marked so this module does not answer itself. */
async function passthrough(request: Request, body?: string): Promise<Response> {
  const headers = new Headers(request.headers);
  headers.set(PASS, "1");
  return mockFetch(new Request(request.url, { method: request.method, headers, ...(body === undefined ? {} : { body }) }));
}

async function unanswered(response: Response): Promise<boolean> {
  if (response.status !== 404) return false;
  const body = (await response.clone().json()) as { detail?: string };
  return String(body.detail).startsWith("The prototype has no mock");
}

/** A name the route derives from the request key: `sch-` and 26 base32 characters. */
function minted(prefix: string, key: string): string {
  let h = 2166136261;
  for (const c of key) h = Math.imul(h ^ c.charCodeAt(0), 16777619) >>> 0;
  const alphabet = "abcdefghijklmnopqrstuvwxyz234567";
  return prefix + Array.from(hex(h, 26), (c) => alphabet[parseInt(c, 16) * 2]).join("");
}

// ------------------------------------------------------------ the schedules

let resourceVersion = 64_000;
const nextVersion = () => String((resourceVersion += 1));

function digest(s: Schedule): string {
  return "sha256:" + hex(JSON.stringify([s.schedule, s.timeZone, s.topics, s.destinationRef, s.archive, s.retry]).length * 7919 + s.name.length, 64);
}

/** What the controller would have written: revision, preset match, next runs, active runs. */
function observe(s: Schedule, now = Date.now()): Schedule {
  const zone = s.timeZone ?? undefined;
  let runs: S["NextRunView"][] = [];
  let ready: S["ConditionView"] = { type: "Ready", status: "True", reason: "Scheduled", message: "the schedule is admitting slots", lastTransitionTime: s.createdAt };
  if (s.suspended) {
    ready = { type: "Ready", status: "True", reason: "Suspended", message: "spec.suspend is true; no slot is admitted", lastTransitionTime: s.createdAt };
  } else {
    try {
      runs = nextRuns(s.schedule, zone, now, 5);
    } catch (e) {
      ready = { type: "Ready", status: "False", reason: e instanceof CadenceError && e.field === "timeZone" ? "UnknownTimeZone" : "InvalidSchedule", message: (e as Error).message };
    }
  }
  const active = backups
    .filter((b) => b.scheduleRef?.name === s.name && !b.operation.terminal && b.trigger?.kind !== "Manual")
    .map((b) => ({ name: b.name, kind: b.trigger?.kind ?? "Scheduled", attempt: b.trigger?.attempt ?? 0 }));
  return {
    ...s,
    preset: matchPreset(s.schedule) as Schedule["preset"],
    status: {
      ...s.status,
      nextRuns: runs,
      nextFireTime: runs[0]?.at ?? null,
      activeRuns: active,
      ready,
      observedGeneration: s.generation,
      policy: {
        generation: s.generation ?? 1,
        effectiveSince: s.status.policy?.effectiveSince ?? String(s.createdAt),
        evaluatedAt: iso(Math.floor(now / MINUTE) * MINUTE),
        runPolicySha256: digest(s),
        timeZone: s.timeZone ?? "UTC",
        tzdb: TZDB,
      },
    },
  };
}

// The shared mock's schedules gain what the API publishes and the prototype left out.
for (const s of schedules) {
  s.generation ??= 1;
  s.createdAt ??= iso(NOW - 34 * DAY);
  const runs = backups.filter((b) => b.scheduleRef?.name === s.name && b.trigger?.kind !== "Manual");
  const last = runs[0];
  if (last) {
    s.status.lastSlot = {
      slot: String(last.slot ?? ""),
      dueAt: String(last.createdAt),
      attempt: last.trigger?.attempt ?? 0,
      disposition: last.trigger?.kind === "Retry" ? "Retried" : "Admitted",
      reason: "Scheduled",
      decidedAt: String(last.createdAt),
      backupRef: { name: last.name },
    };
  }
  s.status.missedSlots = { count: 0, countCapped: false };
  s.status.lastFireTime ??= last ? String(last.createdAt) : null;
  for (const b of runs) {
    if (b.scheduleRef) b.scheduleRef = { ...b.scheduleRef, uid: s.uid, generation: 1, runPolicySha256: digest(s) };
  }
}
for (const p of protectionPolicies) {
  p.generation ??= 1;
  p.observedGeneration ??= 1;
  p.availabilityBasis ??= "Catalog";
  // The controller's spellings (AlertKind, Open/Resolved), which the prototype's data predates.
  for (const a of p.alerts ?? []) {
    if (a.state === "open") a.state = "Open";
    if (a.kind === "ConsecutiveFailedRuns") a.kind = "BackupFailure";
  }
  const healthy = p.health === "Healthy";
  p.conditions ??= [
    { type: "Ready", status: "True", reason: "Evaluated", message: "the objective was evaluated", lastTransitionTime: iso(NOW - MINUTE) },
    healthy
      ? { type: "Protected", status: "True", reason: "WithinObjective", message: "the newest available recovery point is inside the objective", lastTransitionTime: iso(NOW - 3 * DAY) }
      : { type: "Protected", status: "False", reason: "ConsecutiveFailedRuns", message: "2 runs in a row failed; the objective allows 1", lastTransitionTime: iso(NOW - 22 * HOUR) },
  ];
  const last = backups.find((b) => b.sourceRef.name === p.protects.sourceRef.name && b.trigger?.kind !== "Manual");
  if (last && !p.lastAttempt) {
    p.lastAttempt = { at: String(last.createdAt), backupRef: { name: last.name }, phase: last.operation.state === "failed" ? "Failed" : "Succeeded", reason: last.operation.stateReason ?? null };
  }
}

const scheduleKeys = new Map<string, { name: string; body: string }>();
const manualKeys = new Map<string, { name: string; body: string }>();

function forNamespace<T extends { namespace: string }>(ns: string, items: T[]): T[] {
  return items.filter((i) => i.namespace === ns);
}

function find(ns: string, name: string): Schedule | undefined {
  return schedules.find((s) => s.namespace === ns && s.name === name);
}

type Policy = S["CreateScheduleRequest"] & { topicSelection?: S["TopicSelectionRequest"] };

/** The checks both schedule routes make, in the API's field paths. */
function refusals(ns: string, body: Policy, selectionPath: "" | "topicSelection."): FieldError[] {
  const errors: FieldError[] = [];
  const topics = body.topicSelection ? (body.topicSelection.topics ?? []) : (body.topics ?? []);
  const all = body.topicSelection ? body.topicSelection.allUserTopics : body.allUserTopics;
  try {
    nextRuns(body.schedule, body.timeZone ?? undefined, Date.now(), 1);
  } catch (e) {
    if (e instanceof CadenceError) errors.push({ field: e.field, code: e.code, message: e.message });
  }
  if (!all && topics.length === 0) errors.push({ field: `${selectionPath}topics`, code: "selection_invalid", message: "name at least one topic, or all user topics" });
  if (all && topics.length > 0) errors.push({ field: `${selectionPath}topics`, code: "selection_invalid", message: "named topics and all user topics are exclusive" });
  topics.forEach((t, i) => {
    if (/[*?[\]{}]/.test(t)) errors.push({ field: `${selectionPath}topics[${i}]`, code: "pattern_not_allowed", message: "a topic name, never a pattern" });
  });
  if (body.destinationRef && !DESTINATIONS.some((d) => d.name === body.destinationRef!.name)) {
    errors.push({ field: "destinationRef.name", code: "not_found", message: `no BackupDestination named ${body.destinationRef.name} in ${ns}` });
  }
  if (!body.destinationRef && !body.archive) errors.push({ field: "archive", code: "required", message: "exactly one of archive or destinationRef" });
  if (body.archive && !/^(s3|gs|az):\/\/[^@]+$|^file:\/\/\//.test(body.archive.url)) {
    errors.push({ field: "archive.url", code: "invalid_url", message: "s3://, gs://, az:// or file:/// with no userinfo" });
  }
  return errors;
}

function applyPolicy(s: Schedule, body: Policy): Schedule {
  const all = body.topicSelection ? body.topicSelection.allUserTopics : body.allUserTopics;
  return {
    ...s,
    schedule: body.schedule,
    timeZone: body.timeZone ?? undefined,
    topics: all ? [] : (body.topicSelection?.topics ?? body.topics ?? []),
    allUserTopics: all ?? undefined,
    destinationRef: body.destinationRef ?? undefined,
    archive: body.destinationRef ? { url: `logweir-destination://${body.destinationRef.name}` } : { url: body.archive!.url, ...(body.archive!.credentialRef ? { credentialRef: body.archive!.credentialRef } : {}) },
    concurrencyPolicy: body.concurrencyPolicy ?? "Forbid",
    startingDeadlineSeconds: body.startingDeadlineSeconds ?? undefined,
    catchUpPolicy: body.catchUpPolicy ?? undefined,
    retry: body.retry ?? undefined,
    activeDeadlineSeconds: body.activeDeadlineSeconds ?? undefined,
    // Omitted is removed: an edit clears the withdrawn spec.retention.
    retention: undefined,
    suspended: body.suspended,
  };
}

async function createSchedule(ns: string, request: Request): Promise<Response> {
  const key = request.headers.get("Idempotency-Key") ?? "";
  if (key.length < 8) return problem(400, "idempotency_key_required", "Idempotency-Key is required (8 to 128 visible characters).");
  const text = await request.text();
  const body = JSON.parse(text) as Policy;
  const held = scheduleKeys.get(key);
  if (held) {
    if (held.body !== text) return problem(409, "idempotency_conflict", "This idempotency key was already used with a different request body.");
    return json(200, { item: observe(find(ns, held.name)!), replayed: true, requestId: requestId() });
  }
  const errors = refusals(ns, body, "");
  if (!connections.some((c) => c.namespace === ns && c.name === body.sourceRef.name)) {
    errors.push({ field: "sourceRef.name", code: "not_found", message: `no connection named ${body.sourceRef.name} in ${ns}` });
  }
  if (errors.length) return invalid(errors);
  const name = minted("sch-", key);
  const created = applyPolicy(
    {
      name,
      namespace: ns,
      uid: crypto.randomUUID(),
      resourceVersion: nextVersion(),
      generation: 1,
      createdAt: iso(Date.now()),
      sourceRef: body.sourceRef,
      status: { activeRuns: [] },
    } as unknown as Schedule,
    body,
  );
  created.status.policy = { generation: 1, effectiveSince: String(created.createdAt), evaluatedAt: String(created.createdAt), runPolicySha256: "", timeZone: "UTC", tzdb: TZDB };
  schedules.push(created);
  scheduleKeys.set(key, { name, body: text });
  return json(201, { item: observe(created), replayed: false, requestId: requestId() });
}

async function editSchedule(ns: string, name: string, request: Request): Promise<Response> {
  const s = find(ns, name);
  if (!s) return problem(404, "not_found", `No BackupSchedule named ${name} in ${ns}.`);
  const body = (await request.json()) as Policy & { expectedGeneration: number; sourceRef?: { name: string } };
  if (body.sourceRef) return invalid([{ field: "sourceRef", code: "field_immutable", message: "a different cluster is a different schedule" }]);
  if (body.expectedGeneration !== s.generation) {
    return problem(412, "precondition_failed", `The schedule is at generation ${s.generation}, not ${body.expectedGeneration}. Read it again.`);
  }
  const errors = refusals(ns, body, "topicSelection.");
  if (errors.length) return invalid(errors);
  const next = applyPolicy(s, body);
  next.generation = (s.generation ?? 1) + 1;
  next.resourceVersion = nextVersion();
  next.status = { ...s.status, policy: { ...s.status.policy!, effectiveSince: iso(Date.now()) } };
  schedules.splice(schedules.indexOf(s), 1, next);
  return json(200, { item: observe(next), requestId: requestId() });
}

async function setSuspension(ns: string, name: string, request: Request): Promise<Response> {
  const s = find(ns, name);
  if (!s) return problem(404, "not_found", `No BackupSchedule named ${name} in ${ns}.`);
  const body = (await request.json()) as S["SetSuspensionRequest"];
  if (body.expectedResourceVersion !== s.resourceVersion) {
    return problem(412, "precondition_failed", "The schedule changed since it was read (resourceVersion). Read it again and resend.");
  }
  // Suspension moves the generation and leaves the run-policy digest alone.
  const next = { ...s, suspended: body.suspended, generation: (s.generation ?? 1) + 1, resourceVersion: nextVersion() };
  schedules.splice(schedules.indexOf(s), 1, next);
  return json(200, { item: observe(next), requestId: requestId() });
}

async function backUpNow(ns: string, request: Request): Promise<Response | null> {
  const text = await request.clone().text();
  const body = JSON.parse(text) as S["CreateBackupRequest"];
  if (!body.scheduleRef) return null;
  const key = request.headers.get("Idempotency-Key") ?? "";
  const held = manualKeys.get(key);
  const s = find(ns, body.scheduleRef.name);
  const context = s && { name: s.name, uid: s.uid, generation: s.generation ?? 1, suspended: s.suspended, activeRuns: observe(s).status.activeRuns ?? [], runPolicySha256: digest(s) };
  if (held) {
    if (held.body !== text) return problem(409, "idempotency_conflict", "This idempotency key was already used with a different request body.");
    const current = (await (await passthrough(new Request(request.url.replace(/\/backups$/, `/backups/${held.name}`)))).json()) as { item: Backup };
    return json(200, { item: current.item, replayed: true, schedule: context, requestId: requestId() });
  }
  if (s && typeof body.scheduleRef.expectedGeneration === "number" && body.scheduleRef.expectedGeneration !== s.generation) {
    return problem(409, "policy_changed", "The schedule's policy changed after the revision this request named. Read the schedule again; confirming runs the new revision under a new Idempotency-Key.", {
      policy: { currentGeneration: s.generation ?? 1, currentRunPolicySha256: digest(s) },
    });
  }
  const made = await passthrough(request, JSON.stringify({ scheduleRef: { name: body.scheduleRef.name } }));
  if (!made.ok) return made;
  const answer = (await made.json()) as { item: Backup };
  const stored = backups.find((b) => b.name === answer.item.name);
  const ref = s ? { name: s.name, uid: s.uid, generation: s.generation ?? 1, runPolicySha256: digest(s) } : undefined;
  if (stored && ref) Object.assign(stored, { scheduleRef: ref, schedule: s!.name, topics: s!.topics });
  manualKeys.set(key, { name: answer.item.name, body: text });
  return json(201, { item: { ...answer.item, scheduleRef: ref, schedule: s?.name, topics: s?.topics ?? answer.item.topics }, replayed: false, schedule: context, requestId: requestId() });
}

function preview(url: URL): Response {
  const q = url.searchParams;
  const schedule = q.get("schedule");
  const kind = q.get("preset") as PresetKind | null;
  if ((schedule === null) === (kind === null)) {
    return invalid([{ field: "schedule", code: "required", message: "send exactly one of schedule=<cron> or preset=<kind> with its parameters" }]);
  }
  try {
    const zone = q.get("timeZone") ?? undefined;
    checkZone(zone);
    let line = schedule ?? "";
    if (kind) {
      const n = (name: string) => (q.has(name) ? Number(q.get(name)) : undefined);
      line = compilePreset({ kind, minute: n("minute") as number, hour: n("hour"), dayOfWeek: n("dayOfWeek"), dayOfMonth: n("dayOfMonth"), n: n("n") });
    }
    const after = Date.now();
    const runs = nextRuns(line, zone, after, Number(q.get("count") ?? 10));
    const preset = matchPreset(line);
    return json(200, { after: iso(after), schedule: line, timeZone: zone ?? "UTC", tzdb: TZDB, runs, ...(preset ? { preset } : {}), requestId: requestId() });
  } catch (e) {
    if (e instanceof CadenceError) return invalid([{ field: e.field, code: e.code, message: e.message }]);
    throw e;
  }
}

// ---------------------------------------------------- retention, protection

const retentionPolicies: S["RetentionPolicyView"][] = [
  {
    name: "primary-retention",
    namespace: "team-a",
    uid: "5c1d0d63-2e3b-4f1a-9a55-7f2d3c0b8a11",
    resourceVersion: "70310",
    generation: 2,
    observedGeneration: 2,
    createdAt: iso(NOW - 30 * DAY),
    destinationRef: { name: "primary" },
    catalogRef: { name: "primary" },
    scopePrefix: "team-a",
    mode: "Report",
    keepDays: 14,
    keepLast: 48,
    minUsablePoints: 3,
    holds: 0,
    leasedPoints: 0,
    enforcement: "RecommendationOnly",
    enforcementDegraded: false,
    approvedPlanState: "notApplicable",
    guarantees: { ageExpiry: "NotEnforced", minUsablePoints: "LogweirEnforced", activeRestoreProtection: "LogweirEnforced", legalHold: "ProviderEnforcedUnverified" },
    lastEvaluation: {
      at: iso(NOW - 40 * MINUTE),
      accounting: "Recorded",
      pointsEvaluated: 61,
      keptCount: 48,
      candidateCount: 13,
      truncatedByCap: 0,
      maxDeletionsPerRun: 50,
      viewIncomplete: false,
      truncated: false,
    },
    conditions: [
      { type: "Ready", status: "True", reason: "PolicyReady", message: "the destination and the catalog resolve", lastTransitionTime: iso(NOW - 30 * DAY) },
      { type: "Enforced", status: "False", reason: "RecommendationOnly", message: "this policy reports what would be removed and removes nothing", lastTransitionTime: iso(NOW - 30 * DAY) },
    ],
  },
];

// ------------------------------------- answered only when no other mock does

const DESTINATIONS: S["DestinationSummary"][] = [
  {
    name: "primary",
    uid: "0c9b4c1e-58a2-4a77-9b0b-3d1f7d41c2aa",
    generation: 3,
    canonicalUrl: "s3://logweir-team-a/archive",
    endpoint: "https://minio.storage.svc:9000",
    addressing: "pathStyle",
    transport: "tls",
    default: true,
    description: "The team's archive bucket",
    status: { valid: true, reason: "AccessVerified", observedAt: iso(NOW - 20 * MINUTE) },
  },
  {
    name: "dr-archive",
    uid: "8e0e7f1a-1b44-4f0b-9d0c-62b9d2f0a7c3",
    generation: 1,
    canonicalUrl: "s3://logweir-team-a-dr/archive",
    addressing: "virtualHosted",
    transport: "tls",
    default: false,
    status: { valid: true, reason: "AccessVerified", observedAt: iso(NOW - 2 * HOUR) },
  },
];

const DISCOVERED: Record<string, string[]> = {
  "orders-prod": ["orders", "orders-dlq", "payments", "payments-dlq", "refunds", "inventory", "shipments", "__consumer_offsets"],
};

function discovery(ns: string, connection: string): S["TopicDiscovery"] {
  const names = DISCOVERED[connection] ?? [];
  return {
    id: `disc-${connection}`,
    namespace: ns,
    uid: hex(connection.length * 31, 8) + "-0000-4000-8000-" + hex(connection.length, 12),
    resourceVersion: "70400",
    state: "succeeded",
    terminal: true,
    stale: false,
    staleReasons: [],
    truncated: false,
    chunkCount: 1,
    conditions: [],
    connection: { name: connection },
    observedAt: iso(NOW - 3 * HOUR),
    freshUntil: iso(NOW + 21 * HOUR),
    counts: { listed: names.length, returned: names.length, internalExcluded: 0, errored: 0 },
    visibility: { state: "unknown", basis: ["DescribeClusterAllowed"] },
  };
}

const checks = new Map<string, { at: number; request: S["BackupPreflightRequest"] }>();

function readiness(ns: string, id: string): S["Preflight"] | undefined {
  const held = checks.get(id);
  if (!held) return undefined;
  const done = Date.now() - held.at > 2500;
  const unreachable = connections.find((c) => c.name === held.request.sourceConnection)?.reachability.state === "unreachable";
  const entry = (cid: string, ok: boolean, message: string, remedy?: string): S["CheckEntryView"] => ({
    id: cid,
    state: ok ? "ready" : "notReady",
    gating: "blocking",
    message,
    ...(remedy && !ok ? { remedy } : {}),
    observedAt: iso(held.at + 2000),
  });
  return {
    id,
    namespace: ns,
    uid: hex(held.at, 8) + "-1111-4000-8000-" + hex(held.at, 12),
    resourceVersion: "70500",
    operation: "backup",
    applicable: done,
    stale: false,
    staleBasis: [],
    staleReasons: [],
    terminal: done,
    state: done ? (unreachable ? "notReady" : "ready") : "running",
    detailsAvailable: false,
    binding: { referents: [{ kind: "KafkaCluster", name: held.request.sourceConnection }] },
    conditions: [],
    warnings: [],
    executionOnly: [{ id: "archive.write", note: "Writing the first object is only proven by the run itself." }],
    observedAt: done ? iso(held.at + 2000) : null,
    checks: done
      ? [
          entry("source.reachable", !unreachable, `${held.request.sourceConnection} answers as the backup principal`, "Check the connection on the Connections page."),
          entry("source.topicsDescribed", !unreachable, `${held.request.topics.length} named topics are described`),
          entry("destination.listable", true, `${held.request.destination ?? held.request.legacyArchive?.url ?? "the archive"} can be listed`),
        ]
      : [],
  };
}

// ------------------------------------------------------------------ routing

/** The R3 routes; null for a request this module leaves to the shared mock. */
export async function schedulesMock(request: Request): Promise<Response | null> {
  if (request.headers.get(PASS)) return null;
  const url = new URL(request.url);
  const path = url.pathname.replace(/^.*?\/api\/v1/, "");
  const method = request.method;
  let p: RegExpMatchArray | null;

  if (method === "GET" && path === "/cadence-previews") return preview(url);
  if ((p = path.match(/^\/namespaces\/([^/]+)\/schedules$/))) {
    const ns = p[1]!;
    if (method === "GET") {
      const items = forNamespace(ns, schedules).map((s) => observe(s));
      return json(200, { items, page: { limit: 50, snapshot: nextVersion() }, requestId: requestId() });
    }
    if (method === "POST") return createSchedule(ns, request);
  }
  if ((p = path.match(/^\/namespaces\/([^/]+)\/schedules\/([^/:]+)$/))) {
    const [, ns, name] = p as unknown as [string, string, string];
    if (method === "GET") {
      const s = find(ns, name);
      return s ? json(200, { item: observe(s), requestId: requestId() }) : problem(404, "not_found", `No BackupSchedule named ${name} in ${ns}.`);
    }
    if (method === "PUT") return editSchedule(ns, name, request);
  }
  if (method === "POST" && (p = path.match(/^\/namespaces\/([^/]+)\/schedules\/([^/:]+):set-suspension$/))) {
    return setSuspension(p[1]!, p[2]!, request);
  }
  if (method === "POST" && (p = path.match(/^\/namespaces\/([^/]+)\/backups$/))) return backUpNow(p[1]!, request);
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/retention-policies$/))) {
    return json(200, { items: forNamespace(p[1]!, retentionPolicies), page: { limit: 50, snapshot: "70310" }, requestId: requestId() });
  }
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/protection-policies\/([^/]+)$/))) {
    const listUrl = request.url.replace(/\/[^/]+$/, "");
    const list = (await (await passthrough(new Request(listUrl))).json()) as { items: S["ProtectionPolicyView"][] };
    const found = list.items.find((x) => x.name === p![2]);
    return found ? json(200, { item: found, requestId: requestId() }) : problem(404, "not_found", `No ProtectionPolicy named ${p[2]} in ${p[1]}.`);
  }

  // Readiness checks this module started, and the backup ones it is asked to start.
  if (method === "POST" && (p = path.match(/^\/namespaces\/([^/]+)\/preflights$/))) {
    const body = (await request.clone().json()) as S["CreatePreflightRequest"];
    if (body.operation !== "backup" || !body.backup) return null;
    const id = minted("pf-backup-", request.headers.get("Idempotency-Key") ?? String(Date.now()));
    if (!checks.has(id)) checks.set(id, { at: Date.now(), request: body.backup });
    return json(201, { item: readiness(p[1]!, id), replayed: false, requestId: requestId() });
  }
  if (method === "GET" && (p = path.match(/^\/namespaces\/([^/]+)\/preflights\/(pf-backup-[^/]+)$/))) {
    const item = readiness(p[1]!, p[2]!);
    return item ? json(200, { item, requestId: requestId() }) : problem(404, "not_found", `No preflight ${p[2]}.`);
  }

  // Another page's routes: the shared mock first, these only when it has none.
  const fallback =
    method !== "GET"
      ? null
      : (p = path.match(/^\/namespaces\/([^/]+)\/destinations$/))
        ? () => json(200, { items: DESTINATIONS, page: { limit: 50, snapshot: "1" }, requestId: requestId() })
        : (p = path.match(/^\/namespaces\/([^/]+)\/connections\/([^/]+)\/topic-discoveries$/)) && url.searchParams.get("latest") === "true"
          ? () => {
              const known = DISCOVERED[p![2]!];
              const d = known ? discovery(p![1]!, p![2]!) : undefined;
              return json(200, { ...(d ? { lastSuccessful: d, latestAttempt: d } : {}), requestId: requestId() });
            }
          : (p = path.match(/^\/namespaces\/([^/]+)\/topic-discoveries\/disc-([^/]+)\/topics$/))
            ? () => {
                const names = DISCOVERED[p![2]!] ?? [];
                return json(200, {
                  items: names.map((name, i) => ({ name, partitions: [3, 1, 6, 1, 3, 12, 3, 50][i] ?? 3, internal: name.startsWith("__"), expected: false })),
                  page: { limit: 200, snapshot: "disc" },
                  scan: { chunksScanned: 1, complete: true },
                  requestId: requestId(),
                });
              }
            : null;
  if (fallback) {
    const shared = await passthrough(request);
    return (await unanswered(shared)) ? fallback() : shared;
  }
  return null;
}
