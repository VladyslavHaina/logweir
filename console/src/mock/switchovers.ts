// Theme A's cutover on mock data (PROD-18.1 R7): the switchover review (a `Preflight`
// with `operation: switchover` and its details lines), Switchovers with the controller's
// phases (Pending, Running, then every group's outcome at once, as the controller lifts
// them from the Job's lines), the approval routing by namespace mode, and a two-person
// request. Shapes are the API's (schema.d.ts, from claude/v2a-integrate at 10426a92);
// the run's decisions follow `logweir switchover run` (§3.5): a live group is refused
// GroupActive, a moved one TargetPositionsChanged, an applied one reads alreadyApplied.
//
// The demo story, on team-a's `orders-recovery-q4m8x` (complete verification):
//   orders-billing    absent on the target: ready, created by the switchover
//   orders-shipping   2 consumers connected: stop them, then check again; they rejoin
//                     once during the first run that includes them (partial application)
//   orders-reporting  applied by an earlier switchover: already switched
//   orders-fraud      one partition unavailable (BeforeArchive): offered, not preselected
//   orders-audit      positions moved since: blocked (TargetPositionsChanged)
import type { components } from "../api/schema";
import { HOUR, MINUTE, NOW, iso } from "./clock";
import { hex, requestId, restores } from "./data";

type S = components["schemas"];
type Preflight = S["Preflight"];
type Switchover = S["SwitchoverView"];

// ------------------------------------------------------------------ responses

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" },
  });
}

function problem(status: number, code: string, detail: string, errors?: S["FieldError"][]): Response {
  return json(status, { status, code, title: code, detail, requestId: requestId(), retryable: false, type: `https://logweir.dev/problems/${code}`, ...(errors ? { errors } : {}) });
}

async function sha256(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return `sha256:${Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("")}`;
}

// ------------------------------------------------------- approval modes (shared)

/** The namespace modes. The same global the restores mock reads, so both agree; the walk
 *  flips a namespace to two-person or strict through it (mock builds only). */
type Mode = "confirm" | "two-person" | "strict" | "legacy";
const g = globalThis as { __lwMockApprovalMode?: Record<string, Mode> };
g.__lwMockApprovalMode ??= { "team-a": "confirm", "team-b": "two-person" };
const modeOf = (ns: string): Mode => g.__lwMockApprovalMode?.[ns] ?? "confirm";

function policyFor(ns: string): S["ApprovalPolicyView"] {
  const mode = modeOf(ns);
  const governed = mode !== "confirm";
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
    ticketRequired: mode === "two-person" || mode === "strict",
    installationDigest: `sha256:${hex(502, 64)}`,
    ...(mode === "legacy" ? {} : { digest: `sha256:${hex(501, 64)}`, maxAgeSeconds: 3600 }),
  };
}

// ------------------------------------------------- the mapping and the target

type Cls = "exact" | "approximate" | "unavailable";
interface Entry {
  topic: number; // index into the restore's new topics
  partition: number;
  cls: Cls;
  offset?: number;
  reason?: string;
}

/** The positions mapping the demo restore's complete verification wrote (§2.4). */
const MAPPING: Record<string, { type: string; entries: Entry[] }> = {
  "orders-audit": { type: "classic", entries: [{ topic: 0, partition: 0, cls: "exact", offset: 1_197_004 }] },
  "orders-billing": {
    type: "classic",
    entries: [
      { topic: 0, partition: 0, cls: "exact", offset: 1_198_870 },
      { topic: 0, partition: 1, cls: "exact", offset: 1_181_455 },
      { topic: 0, partition: 2, cls: "exact", offset: 1_205_101 },
      { topic: 1, partition: 0, cls: "approximate", offset: 3_790, reason: "selectionExcluded" },
    ],
  },
  "orders-fraud": {
    type: "classic",
    entries: [
      { topic: 0, partition: 0, cls: "exact", offset: 1_194_412 },
      { topic: 0, partition: 1, cls: "unavailable", reason: "BeforeArchive" },
      { topic: 0, partition: 2, cls: "exact", offset: 1_200_925 },
    ],
  },
  "orders-reporting": {
    type: "classic",
    entries: [
      { topic: 2, partition: 0, cls: "exact", offset: 88_098 },
      { topic: 2, partition: 1, cls: "exact", offset: 87_880 },
    ],
  },
  "orders-shipping": {
    type: "consumer",
    entries: [
      { topic: 0, partition: 0, cls: "exact", offset: 1_198_529 },
      { topic: 0, partition: 1, cls: "exact", offset: 1_181_330 },
      { topic: 0, partition: 2, cls: "approximate", offset: 1_205_388, reason: "selectionExcluded" },
    ],
  },
};

/** Restores the mock can review: complete, verified, not scratch, with a mapping. */
const MAPPED = new Set(["orders-recovery-q4m8x"]);

interface TargetGroup {
  exists: boolean;
  type: string;
  state: string;
  members: number;
  /** "topic/partition" -> committed offset. */
  committed: Map<string, number>;
}

const targets = new Map<string, Map<string, TargetGroup>>();

function restoreOf(ns: string, name: string) {
  return restores.find((r) => r.namespace === ns && r.name === name);
}

function topicsOf(ns: string, restore: string): string[] {
  return restoreOf(ns, restore)?.newTopics ?? ["orders", "orders-dlq", "payments"];
}

function targetOf(ns: string, restore: string): Map<string, TargetGroup> {
  const key = `${ns}/${restore}`;
  let t = targets.get(key);
  if (!t) {
    const topics = topicsOf(ns, restore);
    const mapped = (group: string) => new Map((MAPPING[group]?.entries ?? []).filter((e) => e.offset !== undefined).map((e) => [`${topics[e.topic]}/${e.partition}`, e.offset!]));
    t = new Map<string, TargetGroup>([
      ["orders-audit", { exists: true, type: "classic", state: "Empty", members: 0, committed: new Map([[`${topics[0]}/0`, 1_199_950]]) }],
      ["orders-billing", { exists: false, type: "classic", state: "Dead", members: 0, committed: new Map() }],
      ["orders-fraud", { exists: false, type: "classic", state: "Dead", members: 0, committed: new Map() }],
      ["orders-reporting", { exists: true, type: "classic", state: "Empty", members: 0, committed: mapped("orders-reporting") }],
      ["orders-shipping", { exists: true, type: "consumer", state: "Stable", members: 2, committed: new Map() }],
    ]);
    targets.set(key, t);
  }
  return t;
}

/** The shipping consumers stop when the operator says so (a second review finds them gone),
 *  and one of them rejoins during the first run that includes the group. */
let shippingStopped = false;
let shippingRejoins = true;

// ------------------------------------------------------------- the decision

type Decision = { admissible: boolean; already: boolean; reason?: string; message?: string };

/** `decide_group`, read-only (§3.5): admission, then the compare against `expected: none`. */
function decide(t: TargetGroup | undefined, planned: [string, number][]): Decision {
  if (!t || !t.exists) return { admissible: true, already: false };
  if (t.type !== "classic" && t.type !== "consumer") return { admissible: false, already: false, reason: "TargetGroupTypeUnsupported", message: `the target group is a ${t.type} group` };
  if (t.members > 0 || t.state === "Unknown") return { admissible: false, already: false, reason: "GroupActive", message: `the group has ${t.members} member(s) (${t.state})` };
  if (planned.every(([k, o]) => t.committed.get(k) === o)) return { admissible: true, already: true };
  const moved = planned.find(([k]) => t.committed.has(k));
  if (moved) return { admissible: false, already: false, reason: "TargetPositionsChanged", message: `${moved[0].replace("/", ":")} holds ${t.committed.get(moved[0])}, the plan expects none` };
  return { admissible: true, already: false };
}

function plannedOf(ns: string, restore: string, group: string): [string, number][] {
  const topics = topicsOf(ns, restore);
  return (MAPPING[group]?.entries ?? []).filter((e) => e.offset !== undefined).map((e) => [`${topics[e.topic]}/${e.partition}`, e.offset!]);
}

/** The details lines `detail_lines` writes: one per group and restored topic. */
function detailLines(ns: string, restore: string): Record<string, unknown>[] {
  const topics = topicsOf(ns, restore);
  const target = targetOf(ns, restore);
  if (shippingStopped) Object.assign(target.get("orders-shipping")!, { members: 0, state: "Empty" });
  const out: Record<string, unknown>[] = [];
  for (const [group, m] of Object.entries(MAPPING)) {
    const t = target.get(group);
    const d = decide(t, plannedOf(ns, restore, group));
    const byTopic = new Map<string, Entry[]>();
    for (const e of m.entries) byTopic.set(topics[e.topic]!, [...(byTopic.get(topics[e.topic]!) ?? []), e]);
    for (const [topic, entries] of byTopic) {
      out.push({
        group,
        target: group,
        admissible: d.admissible,
        absent: !t?.exists,
        alreadyApplied: d.already,
        ...(t?.exists ? { type: t.type, state: t.state, members: t.members } : {}),
        ...(d.reason ? { reason: d.reason, message: d.message } : {}),
        topic,
        positions: entries.map((e) => ({
          partition: e.partition,
          class: e.cls,
          committed: t?.committed.get(`${topic}/${e.partition}`) ?? null,
          ...(e.offset !== undefined ? { offset: e.offset } : {}),
          ...(e.reason ? { reason: e.reason } : {}),
        })),
      });
    }
  }
  return out;
}

// ------------------------------------------------------------------ reviews

interface Review {
  item: Preflight;
  createdMs: number;
  lines: Record<string, unknown>[];
  verdict: Preflight["state"];
  checks: S["CheckEntryView"][];
}

const reviews = new Map<string, Review>();
let reviewSerial = 0;

const SCORECARD = `sha256:${hex(911, 64)}`;
const MAPPING_SHA = `sha256:${hex(912, 64)}`;

function row(id: string, state: S["CheckVerdict"], code: string, message: string, remedy?: string): S["CheckEntryView"] {
  return { id, category: "switchover", state, gating: "blocking", authority: id === "switchover.restore" ? "controller" : "checkJob", code, message, ...(remedy ? { remedy } : {}), observedAt: iso(Date.now()) };
}

function startReview(ns: string, body: S["SwitchoverPreflightRequest"]): Review {
  reviewSerial += 1;
  const id = `pf-sw${(NOW / MINUTE + reviewSerial).toString(36).slice(-4)}${hex(reviewSerial + 930, 6)}`;
  const r = restoreOf(ns, body.restoreName);
  let checks: S["CheckEntryView"][];
  let lines: Record<string, unknown>[] = [];
  let verdict: Preflight["state"] = "ready";
  const restoreRow = (code: string, message: string) => row("switchover.restore", "notReady", code, message, "Name the verified Restore whose consumer positions a switchover would apply.");
  if (!r) checks = [restoreRow("RestoreNotFound", `no Restore named \`${body.restoreName}\` exists in this namespace`)];
  else if (body.restoreUid && body.restoreUid !== r.uid) checks = [restoreRow("RestoreReplaced", `Restore ${r.name} has another UID than the one reviewed: it was deleted and created again`)];
  else if (r.target.mode === "scratch") checks = [restoreRow("RestoreIsScratch", `Restore ${r.name} is a scratch drill: its topics are deleted, so there is nothing to switch over to`)];
  else if (!r.operation.terminal || !r.operation.verifiedSuccess) checks = [restoreRow("RestoreNotSucceeded", `Restore ${r.name} is not Succeeded with a Valid verdict`)];
  else if (!MAPPED.has(r.name)) {
    checks = [
      row("switchover.restore", "ready", "RestoreAdmissible", `Restore ${r.name} succeeded with a Valid verdict`),
      row("switchover.evidence", "notReady", "RestoreEvidenceRefused", `VerificationNotComplete: integrity.verification.coverage is ${r.coverage.recorded ?? "not recorded"}, not complete; no positions mapping is written`, "Restore again with `coverage: complete` from a point that captured these consumer groups, or fix the namespace's evidence trust; the message names the switchover's own reason."),
      row("switchover.target", "unknown", "BlockedByPrerequisite", "the restore's evidence did not verify, so there is no mapping to read"),
      row("switchover.groups", "unknown", "BlockedByPrerequisite", "the restore's evidence did not verify, so there is no mapping to read"),
    ];
  } else {
    lines = detailLines(ns, r.name);
    const groups = new Map(lines.map((l) => [l.group as string, l]));
    const refused = [...groups.values()].filter((l) => !l.admissible);
    const positions = lines.reduce((n, l) => n + (l.positions as unknown[]).length, 0);
    const count = (c: string) => lines.reduce((n, l) => n + (l.positions as { class: string }[]).filter((p) => p.class === c).length, 0);
    const summary = `${groups.size} group(s), ${positions} position(s): ${count("exact")} exact, ${count("approximate")} approximate, ${count("unavailable")} unavailable`;
    verdict = refused.length ? "notReady" : "ready";
    checks = [
      row("runner.contract", "ready", "ContractVersionAccepted", "the check runner speaks this controller's contract"),
      row("switchover.restore", "ready", "RestoreAdmissible", `Restore ${r.name} succeeded with a Valid verdict and writes evidence to a saved destination`),
      row("switchover.evidence", "ready", "RestoreEvidenceVerified", `the restore's scorecard verifies, its verification is complete and the positions mapping it binds verifies (${Object.keys(MAPPING).length} group(s)) [mappingSha256=${MAPPING_SHA}; scorecardSha256=${SCORECARD}]`),
      row("switchover.target", "ready", "TargetIsTheRestores", `the target answers the restore's cluster id and ${topicsOf(ns, r.name).length} restored topic(s) carry the IDs recorded at creation [clusterId=${hex(913, 8)}-${hex(914, 4)}-${hex(915, 4)}-${hex(916, 4)}-${hex(917, 12)}]`),
      refused.length
        ? row("switchover.groups", "notReady", "GroupNotAdmissible", `${refused[0]!.reason}: ${refused[0]!.target} (${refused[0]!.message}); ${refused.length} of ${groups.size} group(s) cannot be applied now. ${summary}`, "Stop every member of the named group (or choose another target group), give the target principal DESCRIBE and READ on the group, and review again; the details list every group and position.")
        : row("switchover.groups", "ready", "GroupsAdmissible", `${summary}; every group can be applied now`),
    ];
  }
  const failedEarly = !lines.length;
  if (failedEarly) verdict = "notReady";
  const item: Preflight = {
    id,
    namespace: ns,
    uid: crypto.randomUUID(),
    resourceVersion: String(66000 + reviewSerial),
    createdAt: iso(Date.now()),
    operation: "switchover",
    state: "pending",
    terminal: false,
    applicable: false,
    stale: false,
    staleBasis: ["expiry", `referent:Restore/${body.restoreName}`],
    staleReasons: [],
    binding: { referents: [{ kind: "Restore", name: body.restoreName, ...(r ? { uid: r.uid } : {}) }] },
    checks: [],
    warnings: [],
    executionOnly: [],
    conditions: [],
    detailsAvailable: false,
  };
  const review = { item, createdMs: Date.now(), lines, verdict, checks };
  reviews.set(`${ns}/${id}`, review);
  return review;
}

function reviewNow(r: Review): Preflight {
  const e = (Date.now() - r.createdMs) / 1000;
  if (e < 0.8) return r.item;
  if (e < 2.4) return { ...r.item, state: "running" };
  return {
    ...r.item,
    state: r.verdict,
    terminal: true,
    applicable: true,
    observedAt: iso(r.createdMs + 2400),
    expiresAt: iso(r.createdMs + 15 * MINUTE),
    checks: r.checks,
    detailsAvailable: r.lines.length > 0,
    reason: r.verdict === "ready" ? "Ready" : "NotReady",
  };
}

// --------------------------------------------------------------- switchovers

interface Kept {
  item: Switchover;
  ns: string;
  createdMs: number;
  /** Under a governed mode: when the approval arrived (null: still waiting). */
  approvedMs: number | null;
  requester: string;
  ticket?: string;
  /** The groups and partitions the plan commits, in plan order. */
  plan: { source: string; target: string; positions: [string, number][] }[];
  settled: boolean;
}

const switchovers: Kept[] = [];
let swoSerial = 0;

async function seed() {
  const ns = "team-a";
  const restore = restoreOf(ns, "orders-recovery-q4m8x");
  if (!restore || switchovers.length) return;
  const at = NOW - 50 * MINUTE;
  switchovers.push({
    ns,
    createdMs: at,
    approvedMs: at,
    requester: "alice",
    settled: true,
    plan: [],
    item: {
      name: "swo-3k9d2f7a",
      namespace: ns,
      uid: "4b1f6c2e-9a7d-4e3b-8c15-2d6f0a9e7b41",
      restore: { name: restore.name, uid: restore.uid },
      approvalName: "swo-approval-6e1a90c4",
      planHash: "",
      // Written by `logweir switchover plan` (serde_yaml's block layout), not by the console.
      planBytes: [
        "format_version: 1.0.0",
        "restore:",
        `  name: ${restore.name}`,
        `  uid: ${restore.uid}`,
        `  plan_hash: ${restore.planHash}`,
        `  scorecard_sha256: ${SCORECARD}`,
        `  mapping_sha256: ${MAPPING_SHA}`,
        "groups:",
        "- source: orders-reporting",
        "  target: orders-reporting",
        "  topics:",
        "    payments:",
        "      offsets:",
        "        0: 88098",
        "        1: 87880",
        "      expected: none",
        "- source: orders-shipping",
        "  target: orders-shipping",
        "  topics:",
        "    orders:",
        "      offsets:",
        "        0: 1198529",
        "        1: 1181330",
        "        2: 1205388",
        "      approximate:",
        "      - 2",
        "      expected: none",
        "",
      ].join("\n"),
      phase: "PartiallyApplied",
      reason: "PartiallyApplied",
      exitCode: 2,
      recordKey: "logweir/switchovers/01M3K9D2F7A8Q4M8X0C1V7B2N5.record.json",
      groups: [
        { source: "orders-reporting", target: "orders-reporting", outcome: "applied", created: false, partitions: 2 },
        { source: "orders-shipping", target: "orders-shipping", outcome: "refused", reason: "GroupActive", created: false, partitions: 3 },
      ],
    },
  });
  // The API's planHash is the sha256 of the stored bytes.
  switchovers[0]!.item.planHash = await sha256(switchovers[0]!.item.planBytes!);
  // An attempt on a restore deleted since: refused before any commit.
  switchovers.push({
    ns,
    createdMs: NOW - 26 * HOUR,
    approvedMs: NOW - 26 * HOUR,
    requester: "alice",
    settled: true,
    plan: [],
    item: {
      name: "swo-8h1c5e0b",
      namespace: ns,
      uid: "c7e2a9d1-3f6b-4a8e-9b2c-5d1e7f0a3c64",
      restore: { name: "orders-recovery-p2z6w", uid: "e1d3c5b7-a9f1-4e2d-8c6b-0a2f4e6d8b1c" },
      approvalName: "swo-approval-2b7d4e19",
      planHash: `sha256:${hex(922, 64)}`,
      phase: "Refused",
      reason: "TargetGenerationChanged",
      exitCode: 3,
      groups: [],
    },
  });
}

function planOf(bytes: string): Kept["plan"] {
  // The console's own layout (api/switchovers.ts `planDocument`); the mock reads nothing else.
  const out: Kept["plan"] = [];
  let topic = "";
  for (const line of bytes.split("\n")) {
    let m: RegExpMatchArray | null;
    if ((m = line.match(/^ {2}- source: "(.*)"$/))) out.push({ source: m[1]!, target: m[1]!, positions: [] });
    else if ((m = line.match(/^ {4}target: "(.*)"$/))) out[out.length - 1]!.target = m[1]!;
    else if ((m = line.match(/^ {6}"(.*)":$/))) topic = m[1]!;
    else if ((m = line.match(/^ {8}offsets: \{(.*)\}$/))) {
      for (const pair of m[1]!.split(", ")) {
        const [p, o] = pair.split(": ");
        out[out.length - 1]!.positions.push([`${topic}/${p}`, Number(o)]);
      }
    }
  }
  return out;
}

/** The controller's view of one Switchover now: Pending (and why), Running, then the outcome. */
function switchoverNow(k: Kept): Switchover {
  if (k.settled) return k.item;
  if (k.approvedMs === null) return { ...k.item, phase: "Pending", reason: "ApprovalNotVerified" };
  const e = (Date.now() - k.approvedMs) / 1000;
  if (e < 1.5) return { ...k.item, phase: "Pending", reason: null };
  if (e < 5) return { ...k.item, phase: "Running", reason: "JobCreated" };
  // The run: each group in plan order, against the target as it is now.
  const target = targetOf(k.ns, k.item.restore.name);
  const groups: S["SwitchoverGroupView"][] = k.plan.map((p) => {
    const t = target.get(p.target);
    if (p.target === "orders-shipping" && shippingRejoins && t) {
      shippingRejoins = false;
      Object.assign(t, { members: 1, state: "PreparingRebalance" });
      window.setTimeout(() => Object.assign(t, { members: 0, state: "Empty" }), 8000);
      return { source: p.source, target: p.target, outcome: "refused", reason: "GroupActive", created: false, partitions: p.positions.length };
    }
    const d = decide(t, p.positions);
    if (!d.admissible) return { source: p.source, target: p.target, outcome: "refused", reason: d.reason, created: false, partitions: p.positions.length };
    if (d.already) return { source: p.source, target: p.target, outcome: "alreadyApplied", created: false, partitions: p.positions.length };
    const created = !t || !t.exists;
    const next = t ?? { exists: true, type: "classic", state: "Empty", members: 0, committed: new Map<string, number>() };
    Object.assign(next, { exists: true, state: "Empty" });
    for (const [key, offset] of p.positions) next.committed.set(key, offset);
    target.set(p.target, next);
    return { source: p.source, target: p.target, outcome: "applied", created, partitions: p.positions.length };
  });
  const applied = groups.filter((x) => x.outcome === "applied" || x.outcome === "alreadyApplied").length;
  const phase = applied === groups.length ? "Succeeded" : applied > 0 ? "PartiallyApplied" : "Failed";
  k.item = {
    ...k.item,
    phase,
    reason: phase,
    exitCode: phase === "Succeeded" ? 0 : 2,
    groups,
    recordKey: `logweir/switchovers/01M3${hex(940 + swoSerial, 22).toUpperCase()}.record.json`,
  };
  k.settled = true;
  return k.item;
}

async function create(ns: string, request: Request): Promise<Response> {
  const body = (await request.json()) as S["CreateSwitchoverRequest"];
  const key = request.headers.get("Idempotency-Key") ?? "";
  if (key.length < 8) return problem(400, "idempotency_key_required", "An Idempotency-Key header of 8 to 128 characters is required.");
  const errors: S["FieldError"][] = [];
  if ((await sha256(body.planBytes)) !== body.planHash) errors.push({ field: "planHash", code: "plan_hash_mismatch", message: "is not the SHA-256 of planBytes exactly as submitted" });
  if (!/^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/.test(body.approvalRef.name)) errors.push({ field: "approvalRef.name", code: "invalid_name", message: "must be a Kubernetes object name" });
  const policy = policyFor(ns);
  if (policy.ticketRequired && !body.ticket?.trim()) errors.push({ field: "ticket", code: "required", message: "a change ticket is required by this namespace's policy" });
  if (policy.legacy && body.ticket) errors.push({ field: "ticket", code: "not_accepted", message: "an unbound namespace signs nothing; `logweir drill approve --ticket` carries it" });
  if (!body.planBytes.includes(`uid: "${body.restoreRef.uid}"`)) errors.push({ field: "restoreRef", code: "plan_mismatch", message: "is not the plan's own restore block" });
  if (errors.length) return problem(422, "validation_failed", "One or more fields are invalid.", errors);
  const replay = switchovers.find((k) => k.ns === ns && (k.item as { __key?: string }).__key === key);
  if (replay) return json(200, { item: switchoverNow(replay), replayed: true, requestId: requestId() });
  const running = switchovers.find((k) => k.ns === ns && k.item.restore.name === body.restoreRef.name && !["Succeeded", "PartiallyApplied", "Failed", "Refused"].includes(switchoverNow(k).phase ?? ""));
  swoSerial += 1;
  const name = `swo-${(await sha256(key)).slice(7, 15)}`;
  const mode = policy.operatorMode;
  const kept: Kept = {
    ns,
    createdMs: Date.now(),
    approvedMs: mode === "confirm" ? Date.now() : null,
    requester: "alice",
    ticket: body.ticket ?? undefined,
    plan: planOf(body.planBytes),
    settled: false,
    item: { name, namespace: ns, uid: crypto.randomUUID(), restore: body.restoreRef, approvalName: body.approvalRef.name, planHash: body.planHash, planBytes: body.planBytes, groups: [], phase: null },
  };
  Object.defineProperty(kept.item, "__key", { value: key, enumerable: false });
  if (running) {
    kept.settled = true;
    kept.item = { ...kept.item, phase: "Refused", reason: "SwitchoverInProgress", exitCode: null };
  }
  switchovers.unshift(kept);
  return json(201, {
    item: switchoverNow(kept),
    replayed: false,
    authorization: {
      state: mode === "confirm" ? "confirmed" : "awaitingApproval",
      mode: policy.mode,
      operatorMode: mode,
      legacy: policy.legacy,
      policy: policy.name,
      approvalName: body.approvalRef.name,
      ...(mode === "two-person" || mode === "strict" ? { confirmationName: `${body.approvalRef.name}-confirmation`, expiresAt: iso(Date.now() + HOUR) } : {}),
      ...(policy.legacy ? {} : { policyDigest: policy.digest, requester: "https://dex.logweir.local#alice" }),
    },
    requestId: requestId(),
  });
}

function scopeOf(k: Kept): S["SwitchoverScopeView"] {
  const restore = restoreOf(k.ns, k.item.restore.name);
  const parsed = k.item.planBytes ? planOf(k.item.planBytes) : [];
  return {
    restore: { name: k.item.restore.name, uid: k.item.restore.uid, planHash: restore?.planHash ?? `sha256:${hex(904, 64)}`, scorecardSha256: SCORECARD, mappingSha256: MAPPING_SHA },
    groupsCount: parsed.length,
    positionsCount: parsed.reduce((n, p) => n + p.positions.length, 0),
    groups: parsed.map((p) => {
      const topics = [...new Set(p.positions.map(([key]) => key.split("/")[0]!))];
      return {
        source: p.source,
        target: p.target,
        topics: topics.map((topic) => ({
          topic,
          positions: p.positions
            .filter(([key]) => key.split("/")[0] === topic)
            .map(([key, offset]) => {
              const partition = Number(key.split("/")[1]);
              const entry = MAPPING[p.source]?.entries.find((e) => topicsOf(k.ns, k.item.restore.name)[e.topic] === topic && e.partition === partition);
              return { partition, offset, approximate: entry?.cls === "approximate", expected: null };
            }),
        })),
      };
    }),
  };
}

async function approvalRequest(k: Kept): Promise<S["SwitchoverApprovalRequestView"]> {
  const approved = k.approvedMs !== null;
  const scope = scopeOf(k);
  return {
    switchover: k.item.name,
    switchoverUid: k.item.uid,
    namespace: k.ns,
    approvalName: k.item.approvalName,
    confirmationName: `${k.item.approvalName}-confirmation`,
    confirmationSha256: await sha256(`${k.item.uid}:${k.item.planHash}`),
    policy: policyFor(k.ns).name,
    policyDigest: policyFor(k.ns).digest ?? "",
    planHash: k.item.planHash,
    requester: "https://dex.logweir.local#alice",
    issuedAt: iso(k.createdMs),
    expiresAt: iso(k.createdMs + HOUR),
    ticket: k.ticket ?? null,
    state: approved ? "approved" : "pending",
    stateSentence: approved ? "Approved: the Approval exists, and the controller runs the switchover once it verifies it." : "Waiting for a second person to approve.",
    scopeComplete: true,
    scope,
    scopeSentence: `${scope.groupsCount} ${scope.groupsCount === 1 ? "group" : "groups"} and ${scope.positionsCount} ${scope.positionsCount === 1 ? "position" : "positions"} on restore ${k.item.restore.name}'s target.`,
    approve: approved
      ? { offered: false, sentence: "Already approved.", refusal: "notPending" }
      : { offered: true, sentence: "Approving lets the controller run this switchover: it commits every position listed, to groups with no members." },
    ...(approved ? { approver: "https://dex.logweir.local#bob", approvedAt: iso(k.approvedMs!) } : {}),
  };
}

// ------------------------------------------------------------------- routing

const at = (path: string, pattern: RegExp) => path.match(pattern);

/** The routes this group owns: the switchover review, Switchovers and their approval. */
let seeded: Promise<void> | null = null;

export async function switchoversMock(request: Request): Promise<Response | null> {
  await (seeded ??= seed());
  const url = new URL(request.url);
  const path = url.pathname.replace(/^.*?\/api\/v1/, "");
  const method = request.method;
  let m: RegExpMatchArray | null;
  if (method === "GET" && (m = at(path, /^\/namespaces\/([^/]+)\/approval-policy$/))) return json(200, { item: policyFor(m[1]!), requestId: requestId() });
  if (method === "POST" && (m = at(path, /^\/namespaces\/([^/]+)\/preflights$/))) {
    const body = (await request.clone().json()) as S["CreatePreflightRequest"];
    if (body.operation !== "switchover") return null;
    if (!body.switchover?.restoreName) return problem(422, "validation_failed", "One or more fields are invalid.", [{ field: "switchover.restoreName", code: "required", message: "name the restore to review" }]);
    // A second review after the operator stopped the shipping consumers finds them gone.
    if (reviews.size > 0) shippingStopped = true;
    return json(202, { item: reviewNow(startReview(m[1]!, body.switchover)), replayed: false, requestId: requestId() });
  }
  if (method === "GET" && (m = at(path, /^\/namespaces\/([^/]+)\/preflights\/([^/:]+)$/))) {
    const r = reviews.get(`${m[1]}/${m[2]}`);
    return r ? json(200, { item: reviewNow(r), requestId: requestId() }) : null;
  }
  if (method === "GET" && (m = at(path, /^\/namespaces\/([^/]+)\/preflights\/([^/:]+)\/details$/))) {
    const r = reviews.get(`${m[1]}/${m[2]}`);
    if (!r) return null;
    if (!reviewNow(r).detailsAvailable) return problem(404, "not_found", "This preflight has no details.");
    return json(200, { items: r.lines.map((entry) => ({ entry })), page: { limit: 200, snapshot: `sha256:${hex(950, 64)}` }, requestId: requestId() });
  }
  if ((m = at(path, /^\/namespaces\/([^/]+)\/switchovers$/))) {
    const ns = m[1]!;
    if (method === "GET") {
      const items = switchovers.filter((k) => k.ns === ns).map(switchoverNow).map((s) => ({ ...s, planBytes: undefined }));
      return json(200, { items, page: { limit: 50, snapshot: String(67000 + items.length) }, requestId: requestId() });
    }
    if (method === "POST") return create(ns, request);
  }
  if (method === "GET" && (m = at(path, /^\/namespaces\/([^/]+)\/switchovers\/([^/]+)$/))) {
    const k = switchovers.find((x) => x.ns === m![1] && x.item.name === m![2]);
    return k ? json(200, { item: switchoverNow(k), requestId: requestId() }) : problem(404, "not_found", `No Switchover named ${m[2]} in ${m[1]}.`);
  }
  if ((m = at(path, /^\/namespaces\/([^/]+)\/switchovers\/([^/]+)\/(approval-request|console-approval|approval)$/))) {
    const k = switchovers.find((x) => x.ns === m![1] && x.item.name === m![2]);
    if (!k) return problem(404, "not_found", `No Switchover named ${m[2]} in ${m[1]}.`);
    const mode = modeOf(k.ns);
    if (m[3] === "approval-request" && method === "GET") {
      if (mode !== "two-person") return problem(409, "policy_mismatch", `${k.ns} is not a two-person namespace; no request is shown here.`);
      return json(200, { item: await approvalRequest(k), requestId: requestId() });
    }
    if (m[3] === "console-approval" && method === "POST") {
      if (mode !== "two-person") return problem(409, "policy_mismatch", `${k.ns} is not a two-person namespace.`);
      const body = (await request.json()) as S["ConsoleApprovalRequest"];
      const shown = await approvalRequest(k);
      if (body.confirmationSha256 !== shown.confirmationSha256) return problem(409, "state_conflict", "The request changed since it was shown; read it again.");
      if (k.approvedMs === null) k.approvedMs = Date.now();
      return json(201, { item: { name: k.item.approvalName, namespace: k.ns }, replayed: false, requestId: requestId() });
    }
    if (m[3] === "approval" && method === "POST") {
      if (mode === "confirm" || mode === "two-person") return problem(409, "policy_mismatch", "This namespace approves in the console, not with files.");
      return problem(422, "validation_failed", "One or more fields are invalid.", [{ field: "sidecarBytes", code: "invalid", message: "the mock verifies no signature" }]);
    }
  }
  return null;
}
