// The prototype's stand-in for the network. The typed client (api/client.ts)
// hands every request here instead of to window.fetch, so the pages, the query
// cache and the client run exactly as they will against logweir-api, and the
// page makes no network request at all.
import type { Backup, CreateRestoreRequest, OperationView, Restore } from "../api/types";
import { LOADED_AT, MINUTE, NOW, iso, slotName } from "./clock";
import {
  LIVE_BACKUP,
  backups,
  connections,
  pointOf,
  protectionPolicies,
  requestId,
  restores,
  schedules,
  session,
  withPoint,
} from "./data";
import { declareRestore } from "./themeA";
import { connectionsMock } from "./connections";
import { destinationsMock } from "./destinations";
import { schedulesMock } from "./schedules";

const LATENCY_MS = 350;

/** When each moving run started, in page time. */
const liveSince = new Map<string, number>([[LIVE_BACKUP, LOADED_AT]]);

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" },
  });
}

function problem(status: number, code: string, detail: string): Response {
  return json(status, { status, code, title: code, detail, requestId: requestId() });
}

function page<T>(items: T[]) {
  return { items, page: { limit: 200, snapshot: String(61000 + items.length) }, requestId: requestId() };
}

// ------------------------------------------------------------- moving runs

function backupNow(b: Backup): Backup {
  const since = liveSince.get(b.name);
  if (since === undefined) return b;
  const e = (Date.now() - since) / 1000;
  if (e < 14) return { ...b, operation: { state: "running", terminal: false, verificationState: "pending", verifiedSuccess: false } };
  if (e < 20) return { ...b, operation: { state: "verifying", terminal: false, verificationState: "pending", verifiedSuccess: false } };
  return {
    ...b,
    records: 118_734,
    windowCovered: { fromMs: Date.parse(String(b.createdAt)) - 60 * MINUTE, toMs: since + 20_000 },
    operation: { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "valid", verifiedSuccess: true, exitCode: 0 },
  };
}

/** A restore's stages, in seconds since it was created. */
const RESTORE_STAGES = [
  { until: 2, state: "pending", stage: "admission" },
  { until: 6, state: "preparing", stage: "preparing" },
  { until: 20, state: "running", stage: "running" },
  { until: 27, state: "verifying", stage: "verifying" },
] as const;

const RESTORE_RECORDS = 1_182_410;

function restoreNow(r: Restore): Restore {
  const since = liveSince.get(r.name);
  if (since === undefined) return r;
  const e = (Date.now() - since) / 1000;
  const stage = RESTORE_STAGES.find((s) => e < s.until);
  if (stage) {
    return { ...r, operation: { state: stage.state, terminal: false, verificationState: "pending", verifiedSuccess: false } };
  }
  return {
    ...r,
    operation: { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "valid", verifiedSuccess: true, exitCode: 0, outcome: "pass" },
  };
}

function operationOf(r: Restore): OperationView {
  const now = restoreNow(r);
  const since = liveSince.get(r.name) ?? Date.parse(String(r.createdAt));
  const e = (Date.now() - since) / 1000;
  const done = now.operation.terminal;
  const stage = done ? "finished" : (RESTORE_STAGES.find((s) => e < s.until)?.stage ?? "finished");
  const replayed = done ? RESTORE_RECORDS : Math.max(0, Math.min(1, (e - 6) / 14)) * RESTORE_RECORDS;
  const conditions = [
    { type: "Admitted", status: "True", reason: "Confirmed", message: `Plan ${r.planHash.slice(0, 19)}... confirmed by Alice Martin (one-person confirmation).`, lastTransitionTime: iso(since) },
  ];
  if (e >= 2 || done) conditions.push({ type: "Preparing", status: "True", reason: "CreatingTopics", message: `Creating ${r.newTopics.length} topics on ${r.target.clusterRef.name} with the source partition counts.`, lastTransitionTime: iso(since + 2000) });
  if (e >= 6 || done) conditions.push({ type: "Running", status: "True", reason: "Replaying", message: "Replaying records from the archive (runner phase 4 of 7).", lastTransitionTime: iso(since + 6000) });
  const complete = r.coverage?.requested === "complete";
  if (e >= 20 || done) {
    conditions.push({
      type: "Verifying",
      status: "True",
      reason: complete ? "Complete" : "Sampling",
      message: complete
        ? "Reading every restored record and comparing it with the archive, then mapping each captured consumer position."
        : "Comparing a 10,000-record sample with the archive, partition by partition.",
      lastTransitionTime: iso(since + 20000),
    });
  }
  if (done) conditions.push({ type: "Succeeded", status: "True", reason: "Verified", message: "Restore verified. The scorecard is signed and the controller verified it.", lastTransitionTime: iso(since + 27000) });
  return {
    kind: "restore",
    name: r.name,
    namespace: r.namespace,
    uid: r.uid,
    resourceVersion: String(70000 + Math.floor(e)),
    createdAt: r.createdAt,
    lastUpdatedAt: iso(Date.now()),
    state: now.operation.state,
    stage,
    terminal: done,
    stale: false,
    awaitingApproval: false,
    verifiedSuccess: now.operation.verifiedSuccess,
    conditions,
    progress: {
      stage,
      message: done ? "Finished" : stage === "running" ? `Replayed ${Math.round(replayed).toLocaleString("en-US")} of ${RESTORE_RECORDS.toLocaleString("en-US")} records` : "",
      runnerPhase: { name: done ? "report" : "replay", number: done ? 7 : 4 },
    },
    readiness: { state: "ready", basis: "preflight" },
    result: done ? { status: "pass", exitCode: 0, outcome: "pass", lastPhaseCompleted: 7 } : { status: "pending" },
    verification: done
      ? { state: "valid", verifiedAt: iso(since + 27000), matchedKeyId: "2c76e22ff89969dc0337e64756c85f18edb3e51ae2950ea18d81021d7176d7fe", payloadType: "scorecard" }
      : { state: "pending" },
    verificationScope: complete
      ? { level: "sampled", coverage: "complete", recordsExpected: RESTORE_RECORDS, recordsSampled: RESTORE_RECORDS, recordsSampledMatching: done ? RESTORE_RECORDS : 0 }
      : { level: "sampled", coverage: "sampled", recordsExpected: RESTORE_RECORDS, recordsSampled: 10_000, recordsSampledMatching: done ? 10_000 : 0 },
    trust: done ? { state: "verified", basis: "Current", signedAt: iso(since + 26000) } : { state: "pending", basis: "none" },
    evidence: done ? { payloadKey: `logweir/scorecards/${r.name}.json`, payloadSha256: "sha256:" + "4be1".repeat(16) } : {},
    completion: done
      ? {
          recordsExpected: RESTORE_RECORDS,
          recordsRestored: RESTORE_RECORDS,
          recordsSampled: complete ? RESTORE_RECORDS : 10_000,
          recordsSampledMatching: complete ? RESTORE_RECORDS : 10_000,
          integrityLevel: "byte-fingerprint",
          newTopics: r.newTopics.map((name, i) => ({ name, partitions: [3, 1, 6][i] ?? 3 })),
        }
      : undefined,
  } as OperationView;
}

// ----------------------------------------------------------------- routing

function forNamespace<T extends { namespace: string }>(ns: string, items: T[]): T[] {
  return items.filter((i) => i.namespace === ns);
}

function newestVerified(ns: string, source: string): Backup | undefined {
  return forNamespace(ns, backups)
    .map(backupNow)
    .filter((b) => b.sourceRef.name === source && b.operation.verifiedSuccess)
    .sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)))[0];
}

let restoreSerial = 0;

async function route(request: Request): Promise<Response> {
  const owned = await schedulesMock(request); if (owned) return owned;
  const url = new URL(request.url);
  const path = url.pathname.replace(/^.*?\/api\/v1/, "");
  const method = request.method;
  const m = (pattern: RegExp) => path.match(pattern);
  let p: RegExpMatchArray | null;

  if (method === "GET" && path === "/session") return json(200, session);
  { const settings = (await connectionsMock(request, method, path)) ?? (await destinationsMock(request, method, path)); if (settings) return settings; }

  if (method === "GET" && (p = m(/^\/namespaces\/([^/]+)\/backups$/))) {
    return json(200, page(forNamespace(p[1]!, backups).map(backupNow)));
  }
  if (method === "POST" && (p = m(/^\/namespaces\/([^/]+)\/backups$/))) {
    const ns = p[1]!;
    const body = (await request.json()) as { scheduleRef?: { name?: string } };
    const schedule = schedules.find((s) => s.namespace === ns && s.name === body.scheduleRef?.name);
    if (!schedule) return problem(422, "validation_failed", "Name a schedule to back up now.");
    const name = `${schedule.name}-manual-${slotName(Date.now())}`.replace("-hourly", "");
    const base = backups.find((b) => b.schedule === schedule.name) ?? backups[0]!;
    const created: Backup = {
      ...base,
      name,
      uid: crypto.randomUUID(),
      createdAt: iso(Date.now()),
      trigger: { kind: "Manual", attempt: 1 },
      triggeredBy: "manual",
      slot: undefined,
      operation: { state: "pending", terminal: false, verificationState: "pending", verifiedSuccess: false },
    };
    backups.unshift(created);
    liveSince.set(name, Date.now());
    return json(201, { item: backupNow(created), requestId: requestId() });
  }
  if (method === "GET" && (p = m(/^\/namespaces\/([^/]+)\/backups\/([^/]+)$/))) {
    const found = forNamespace(p[1]!, backups).find((b) => b.name === p![2]);
    return found ? json(200, { item: backupNow(found), requestId: requestId() }) : problem(404, "not_found", `No Backup named ${p[2]} in ${p[1]}.`);
  }

  if (method === "GET" && (p = m(/^\/namespaces\/([^/]+)\/restores$/))) {
    return json(200, page(forNamespace(p[1]!, restores).map(restoreNow)));
  }
  if (method === "POST" && (p = m(/^\/namespaces\/([^/]+)\/restores$/))) {
    const ns = p[1]!;
    const body = (await request.json()) as CreateRestoreRequest;
    restoreSerial += 1;
    const suffix = (NOW / MINUTE + restoreSerial).toString(36).slice(-5);
    const name = body.approvalRef.name || `orders-restore-${suffix}`;
    const naming = body.target.topicNaming;
    const created: Restore = {
      ...restores[0]!,
      name,
      namespace: ns,
      uid: crypto.randomUUID(),
      createdAt: iso(Date.now()),
      approvalRef: { name },
      approvalSubject: naming.originalName ? "originalName" : "ordinary",
      backupSetRef: body.backupSetRef,
      coverage: { requested: body.coverage ?? "sampled", recorded: body.coverage ?? "sampled", covered: true },
      newTopics: (body.topicMapping ?? []).map((row) => row.target),
      planBytesLength: body.planBytes.length,
      planHash: body.planHash,
      pointInTime: body.pointInTime,
      target: {
        clusterRef: body.target.clusterRef,
        mode: body.target.mode,
        originalName: naming.originalName === true,
        topicPrefix: naming.prefix,
      },
      operation: { state: "pending", terminal: false, verificationState: "pending", verifiedSuccess: false },
    };
    restores.unshift(created);
    declareRestore(name, body.planBytes);
    liveSince.set(name, Date.now());
    return json(201, {
      item: created,
      authorization: { state: "confirmed", mode: "ordinary", operatorMode: "oneperson", legacy: false, policy: "team-a", approvalName: name, requester: "alice" },
      requestId: requestId(),
    });
  }
  if (method === "GET" && (p = m(/^\/namespaces\/([^/]+)\/restores\/([^/]+)$/))) {
    const found = forNamespace(p[1]!, restores).find((r) => r.name === p![2]);
    return found ? json(200, { item: restoreNow(found), requestId: requestId() }) : problem(404, "not_found", `No Restore named ${p[2]} in ${p[1]}.`);
  }
  if (method === "GET" && (p = m(/^\/namespaces\/([^/]+)\/operations\/restore\/([^/]+)$/))) {
    const found = forNamespace(p[1]!, restores).find((r) => r.name === p![2]);
    return found ? json(200, { item: operationOf(found), requestId: requestId() }) : problem(404, "not_found", `No restore named ${p[2]}.`);
  }

  if (method === "GET" && (p = m(/^\/namespaces\/([^/]+)\/connections$/))) return json(200, page(forNamespace(p[1]!, connections)));
  if (method === "GET" && (p = m(/^\/namespaces\/([^/]+)\/schedules$/))) return json(200, page(forNamespace(p[1]!, schedules)));
  if (method === "GET" && (p = m(/^\/namespaces\/([^/]+)\/protection-policies$/))) {
    const ns = p[1]!;
    return json(200, page(forNamespace(ns, protectionPolicies).map((pp) => withPoint(pp, newestVerified(ns, pp.protects.sourceRef.name)))));
  }
  if (method === "GET" && (p = m(/^\/namespaces\/([^/]+)\/catalogs\/([^/]+)\/points$/))) {
    const points = forNamespace(p[1]!, backups)
      .map(backupNow)
      .filter((b) => b.operation.verifiedSuccess && b.sourceRef.name === "orders-prod")
      .map(pointOf);
    return json(200, { ...page(points), truncated: false, viewExpired: false });
  }

  return problem(404, "not_found", `The prototype has no mock for ${method} ${path}.`);
}

/** The `fetch` the typed client is given in the prototype. */
export async function mockFetch(request: Request): Promise<Response> {
  await new Promise((resolve) => setTimeout(resolve, LATENCY_MS));
  return route(request);
}
