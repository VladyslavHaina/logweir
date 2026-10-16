// The mock of the Destinations routes. A typed credential becomes a Secret
// NAME and is dropped, as the service does. Shapes follow the console
// fixtures (destination.json, destinations-list.json, destination-usage.json).
import type { components } from "../api/schema";
import { backups, hex, requestId, schedules } from "./data";
import { HOUR, MINUTE, NOW, iso } from "./clock";
import { createdSecrets, json, lastTestFor, problem, startCheck, uuid } from "./connections";

type S = components["schemas"];
type Destination = S["Destination"];
type Role = "archiveWrite" | "archiveRead" | "evidenceWrite" | "evidenceRead";
const ROLES: Role[] = ["archiveWrite", "archiveRead", "evidenceWrite", "evidenceRead"];
const KEBAB: Record<Role, string> = { archiveWrite: "archive-write", archiveRead: "archive-read", evidenceWrite: "evidence-write", evidenceRead: "evidence-read" };
const SECRET_KEYS = ["access-key-id", "secret-access-key"];

const destinations: Destination[] = [
  {
    name: "primary",
    namespace: "team-a",
    uid: "17bc54c4-2e52-4607-b4ac-c31517a6e568",
    resourceVersion: "2428991",
    generation: 2,
    createdAt: iso(NOW - 34 * 24 * HOUR),
    default: true,
    description: "the team-a production archive",
    canonicalUrl: "s3://kafka-backups/team-a/prod",
    locationDigest: "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
    storage: { provider: "s3", bucket: "kafka-backups", prefix: "team-a/prod", region: "us-east-1", endpoint: "https://minio.storage.svc:9000", addressing: "pathStyle" },
    transport: { security: "tls", caBundle: { configMapName: "minio-ca", key: "ca.crt", sha256: "sha256:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae" } },
    writeProbe: "createOnlyMarker",
    access: {
      archiveWrite: { mode: "secretKeys", secretName: "logweir-s3", keys: SECRET_KEYS },
      archiveRead: { mode: "secretKeys", secretName: "lwd-primary-archive-read", keys: SECRET_KEYS },
      evidenceWrite: { mode: "inheritsArchiveWrite", keys: [] },
      evidenceRead: { mode: "archiveReadGrant", keys: [] },
    },
    status: { valid: true, reason: "Valid", observedGeneration: 2, observedAt: iso(NOW - 4 * MINUTE) },
    lastTest: { preflightId: "pf-c5ovcfrjvxwk7axdpexnma4w6l", state: "ready", stale: false, truncated: false, observedAt: iso(NOW - 20 * MINUTE) },
  },
  {
    name: "lab",
    namespace: "team-a",
    uid: "6f1e0f1a-1111-4a2b-9c3d-000000000002",
    resourceVersion: "2429022",
    generation: 1,
    createdAt: iso(NOW - 2 * HOUR),
    default: false,
    canonicalUrl: "s3://kafka-backups/lab",
    storage: { provider: "s3", bucket: "kafka-backups", prefix: "lab", endpoint: "http://minio.storage.svc:9000", addressing: "pathStyle" },
    transport: { security: "insecureHttp" },
    writeProbe: "createOnlyMarker",
    access: {
      archiveWrite: { mode: "secretKeys", secretName: "lwd-lab-archive-write", keys: SECRET_KEYS },
      archiveRead: { mode: "inheritsArchiveWrite", keys: [] },
      evidenceWrite: { mode: "inheritsArchiveWrite", keys: [] },
      evidenceRead: { mode: "notConfigured", keys: [] },
    },
    status: {},
  },
];

/** When a destination was written here; the controller judges it a few seconds later. */
const judgedAfter = new Map<string, number>();
const JUDGE_MS = 4000;

function now(d: Destination): Destination {
  const at = judgedAfter.get(d.name);
  const judged = at !== undefined && Date.now() >= at;
  const status = judged ? { valid: true, reason: "Valid", observedGeneration: d.generation, observedAt: iso(at) } : d.status;
  const lastTest = lastTestFor(d.namespace, "BackupDestination", d.name) ?? d.lastTest;
  return { ...d, status, ...(lastTest ? { lastTest } : {}) };
}

function summary(d: Destination): S["DestinationSummary"] {
  return {
    name: d.name,
    uid: d.uid,
    default: d.default,
    description: d.description ?? null,
    canonicalUrl: d.canonicalUrl,
    endpoint: d.storage.endpoint ?? null,
    addressing: d.storage.addressing,
    transport: d.transport.security,
    generation: d.generation,
    status: d.status,
  };
}

function grantView(name: string, role: Role, g: S["AccessGrantRequest"] | undefined, suffix: string): S["AccessGrantView"] {
  if (!g) return { mode: role === "evidenceRead" ? "notConfigured" : "inheritsArchiveWrite", keys: [] };
  if (g.mode === "workloadIdentity") return { mode: "workloadIdentity", keys: [], serviceAccountName: g.workloadIdentity?.serviceAccountName ?? "logweir-runner" };
  if (g.mode !== "secretKeys") return { mode: g.mode, keys: [] };
  if (g.secret?.existing) return { mode: "secretKeys", secretName: g.secret.existing.name, keys: SECRET_KEYS };
  const secretName = `lwd-${name}-${KEBAB[role]}${suffix}`;
  createdSecrets.push(secretName);
  return { mode: "secretKeys", secretName, keys: g.secret?.new?.sessionToken ? [...SECRET_KEYS, "session-token"] : SECRET_KEYS };
}

const HTTPS = "https" + "://";
const HTTP = "http" + "://";

function locationErrors(body: S["CreateDestinationRequest"]): S["FieldError"][] {
  const e: S["FieldError"][] = [];
  const s = body.storage;
  if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(s.bucket)) e.push({ field: "storage.bucket", code: "bucket_invalid", message: "is not an S3 bucket name" });
  const prefix = s.prefix ?? "";
  if (prefix === "logweir" || prefix.startsWith("logweir/")) e.push({ field: "storage.prefix", code: "prefix_reserved", message: "logweir/ is reserved for evidence" });
  const endpoint = s.endpoint ?? "";
  if (endpoint && !/^https?:\/\/[^/?#@]+$/.test(endpoint)) e.push({ field: "storage.endpoint", code: "endpoint_not_origin", message: "must be an origin: scheme, host and optional port" });
  if (endpoint.startsWith(HTTPS) && body.transport.security === "insecureHttp") e.push({ field: "transport.security", code: "transport_scheme_mismatch", message: "an https endpoint with insecureHttp" });
  if (endpoint.startsWith(HTTP) && body.transport.security === "tls") e.push({ field: "transport.security", code: "transport_scheme_mismatch", message: "an http endpoint with tls" });
  if (!endpoint && body.transport.security === "insecureHttp") e.push({ field: "storage.endpoint", code: "insecure_http_requires_http_endpoint", message: "insecureHttp needs an http:// endpoint" });
  if (body.transport.caBundle && body.transport.security !== "tls") e.push({ field: "transport.caBundle", code: "ca_bundle_requires_tls", message: "a CA bundle needs tls" });
  if (endpoint && s.addressing === "virtualHosted") e.push({ field: "storage.addressing", code: "addressing_unsupported_by_engine", message: "this engine addresses a custom endpoint path-style" });
  return e;
}

function accessErrors(access: S["AccessRequest"], allowExisting: readonly string[]): S["FieldError"][] {
  const e: S["FieldError"][] = [];
  for (const role of ROLES) {
    const g = access[role];
    if (!g) continue;
    if (g.mode === "secretKeys" && !g.secret) e.push({ field: `access.${role}.secret`, code: "grant_mode_fields", message: "secretKeys needs a secret" });
    const existing = g.secret?.existing?.name;
    if (existing !== undefined && !allowExisting.includes(existing)) {
      e.push({ field: `access.${role}.secret.existing`, code: "existing_credential_refused", message: "names a Secret this destination does not already name; enter the credential instead" });
    }
    const fresh = g.secret?.new;
    if (fresh && (!fresh.accessKeyId || !fresh.secretAccessKey)) e.push({ field: `access.${role}.secret.new`, code: "required", message: "needs an access key id and a secret access key" });
  }
  return e;
}

const createKeys = new Map<string, { name: string; digest: string }>();

function writeOnly(access: S["AccessRequest"]): string {
  return JSON.stringify(access, (k, v) => (k === "new" ? "<write-only>" : v));
}

function create(ws: string, key: string, body: S["CreateDestinationRequest"]): Response {
  const digest = JSON.stringify({ ...body, access: writeOnly(body.access) });
  const held = createKeys.get(`${ws}/${key}`);
  if (held) {
    if (held.digest !== digest) return problem(409, "idempotency_conflict", "This idempotency key was already used with a different request body.");
    return json(200, { item: now(destinations.find((d) => d.namespace === ws && d.name === held.name)!), replayed: true, requestId: requestId() });
  }
  const errors = [...locationErrors(body), ...accessErrors(body.access, [])];
  if (!/^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/.test(body.name)) errors.unshift({ field: "name", code: "invalid_name", message: "is not a Kubernetes object name" });
  if (errors.length > 0) return problem(422, "destination_invalid", "The destination's location or access is invalid.", errors);
  if (destinations.some((d) => d.namespace === ws && d.name === body.name)) {
    return problem(409, "state_conflict", `A destination named ${body.name} already exists in ${ws}. Nothing was written.`);
  }
  if (body.default && destinations.some((d) => d.namespace === ws && d.default)) {
    const holder = destinations.find((d) => d.namespace === ws && d.default)!;
    return problem(409, "state_conflict", `${holder.name} is already the default destination in ${ws}. Clear it there first; nothing was written.`);
  }
  const prefix = body.storage.prefix ?? "";
  const item: Destination = {
    name: body.name,
    namespace: ws,
    uid: uuid(`${ws}/destination/${body.name}`),
    resourceVersion: String(2429100 + destinations.length),
    generation: 1,
    createdAt: iso(Date.now()),
    default: body.default === true,
    description: body.description ?? null,
    canonicalUrl: `s3://${body.storage.bucket}${prefix ? `/${prefix}` : ""}`,
    locationDigest: `sha256:${hex(body.name.length + body.storage.bucket.length, 64)}`,
    storage: { provider: "s3", bucket: body.storage.bucket, prefix, region: body.storage.region ?? null, endpoint: body.storage.endpoint ?? null, addressing: body.storage.addressing },
    transport: {
      security: body.transport.security,
      caBundle: body.transport.caBundle ? { configMapName: body.transport.caBundle.configMapName, key: body.transport.caBundle.key ?? "ca.crt", sha256: `sha256:${hex(41, 64)}` } : undefined,
    },
    writeProbe: body.readiness?.writeProbe ?? "createOnlyMarker",
    access: Object.fromEntries(ROLES.map((r) => [r, grantView(body.name, r, body.access[r], "")])) as unknown as S["AccessView"],
    status: {},
  };
  destinations.push(item);
  judgedAfter.set(item.name, Date.now() + JUDGE_MS);
  createKeys.set(`${ws}/${key}`, { name: item.name, digest });
  return json(201, { item: now(item), replayed: false, requestId: requestId() });
}

function rotate(d: Destination, body: S["UpdateDestinationAccessRequest"]): Response {
  if (body.expectedGeneration !== d.generation) {
    return problem(412, "precondition_failed", `The destination is at generation ${d.generation}, not ${body.expectedGeneration}. Read it again and resend.`);
  }
  const named = ROLES.map((r) => d.access[r].secretName).filter((n): n is string => Boolean(n));
  const errors = accessErrors(body.access, named);
  if (!body.access.archiveWrite) errors.push({ field: "access.archiveWrite", code: "required", message: "archiveWrite is required" });
  if (body.transport?.caBundle && d.transport.security !== "tls") {
    return problem(409, "transport_downgrade_forbidden", "A CA bundle on a plaintext destination is refused. Nothing was changed.");
  }
  if (errors.length > 0) return problem(422, "validation_failed", "One or more fields are invalid.", errors);
  const suffix = `-${hex(d.generation * 977 + Date.now() % 1000, 8)}`;
  d.access = Object.fromEntries(ROLES.map((r) => [r, grantView(d.name, r, body.access[r], suffix)])) as unknown as S["AccessView"];
  if (body.transport) {
    d.transport = {
      ...d.transport,
      caBundle: body.transport.caBundle ? { configMapName: body.transport.caBundle.configMapName, key: body.transport.caBundle.key ?? "ca.crt", sha256: `sha256:${hex(d.generation + 51, 64)}` } : undefined,
    };
  }
  d.generation += 1;
  d.resourceVersion = String(Number(d.resourceVersion) + 1);
  judgedAfter.set(d.name, Date.now() + JUDGE_MS);
  return json(200, { item: now(d), requestId: requestId() });
}

function accessCheck(d: Destination, roles: Role[]): Parameters<typeof startCheck>[4] {
  const at = Date.now() + 4500;
  const scope = { kind: "BackupDestination", name: d.name, uid: d.uid };
  const exercise = (r: Role) => roles.length === 0 || roles.includes(r);
  const plaintextLab = d.transport.security === "insecureHttp";
  const entry = (id: string, state: S["CheckEntryView"]["state"], code: string, extra: Partial<S["CheckEntryView"]> = {}): S["CheckEntryView"] => ({
    id,
    state,
    code,
    gating: "blocking",
    authority: "checkJob",
    category: "destination",
    observedAt: iso(at),
    expiresAt: iso(at + 15 * MINUTE),
    ...extra,
  });
  const checks: S["CheckEntryView"][] = [
    entry("destination.resolved", "ready", "DestinationValid", { authority: "controller", scope }),
    entry("destination.credentialBound", "ready", "CredentialBound", { scope }),
  ];
  if (exercise("archiveWrite") || exercise("archiveRead")) {
    checks.push(
      plaintextLab
        ? entry("destination.archiveListable", "notReady", "AccessDenied", {
            scope,
            message: "the archive credential was refused: AccessDenied on ListObjectsV2",
            remedy: "Check that the access key in this destination's Secret may list the bucket and prefix.",
          })
        : entry("destination.archiveListable", "ready", "ArchiveListable", { scope }),
    );
  }
  const warnings: S["CheckEntryView"][] = [];
  if (exercise("evidenceWrite")) {
    if (d.writeProbe === "createOnlyMarker") checks.push(entry("destination.evidenceWritable", "ready", "MarkerWritten", { scope, message: `wrote logweir/readiness/${d.uid}.json` }));
    else warnings.push(entry("destination.evidenceWritable", "skipped", "WriteNotProbed", { gating: "advisory", scope, message: "the write probe is disabled" }));
  }
  if (exercise("evidenceRead")) {
    if (d.access.evidenceRead.mode === "notConfigured") {
      warnings.push(entry("destination.evidenceReadable", "unknown", "EvidenceReadNotConfigured", { gating: "advisory", authority: "controller", message: "no evidenceRead grant is configured", remedy: "Configure an evidenceRead grant if verification should read evidence back." }));
    } else checks.push(entry("destination.evidenceReadable", "ready", "EvidenceReadable", { scope }));
  }
  const ready = checks.every((c) => c.state === "ready");
  return {
    state: ready ? "ready" : "notReady",
    reason: ready ? "Ready" : "NotReady",
    checks,
    warnings,
    executionOnly: [{ id: "destination.archivePrefixWritable", note: "A write under the archive prefix is only proven by the run itself; a check never writes there." }],
  };
}

/** The Destinations routes, or null for any other request. */
export async function destinationsMock(request: Request, method: string, path: string): Promise<Response | null> {
  let p: RegExpMatchArray | null;
  const ws = path.match(/^\/workspaces\/([^/]+)\//)?.[1] ?? "";
  const key = request.headers.get("Idempotency-Key") ?? "";
  const find = (name: string) => destinations.find((d) => d.namespace === ws && d.name === name);
  const missing = (name: string) => problem(404, "not_found", `No destination named ${name} in ${ws}.`);

  if (method === "GET" && /^\/workspaces\/[^/]+\/destinations$/.test(path)) {
    const items = destinations.filter((d) => d.namespace === ws).map((d) => summary(now(d)));
    return json(200, { items, page: { limit: 200, snapshot: String(2429022 + items.length) }, requestId: requestId() });
  }
  if (method === "POST" && /^\/workspaces\/[^/]+\/destinations$/.test(path)) {
    if (key.length < 8) return problem(400, "idempotency_key_required", "This route requires an Idempotency-Key header.");
    return create(ws, key, (await request.json()) as S["CreateDestinationRequest"]);
  }
  if (method === "GET" && (p = path.match(/^\/workspaces\/[^/]+\/destinations\/([^/:]+)$/))) {
    const d = find(p[1]!);
    return d ? json(200, { item: now(d), requestId: requestId() }) : missing(p[1]!);
  }
  if (method === "GET" && (p = path.match(/^\/workspaces\/[^/]+\/destinations\/([^/:]+)\/usage$/))) {
    const d = find(p[1]!);
    if (!d) return missing(p[1]!);
    const use = (kind: string) => (x: { name: string; createdAt?: string | null }) => ({ kind, name: x.name, createdAt: x.createdAt ?? null });
    return json(200, {
      name: d.name,
      schedules: schedules.filter((s) => s.namespace === ws && s.destinationRef?.name === d.name).map(use("BackupSchedule")),
      backups: backups.filter((b) => b.namespace === ws && b.destinationRef?.name === d.name).slice(0, 5).map(use("Backup")),
      truncated: false,
      basis: "schedules and backups in this workspace whose spec.destinationRef names this destination, from at most 1600 of each",
      requestId: requestId(),
    });
  }
  if (method === "POST" && (p = path.match(/^\/workspaces\/[^/]+\/destinations\/([^/:]+):update-access$/))) {
    const d = find(p[1]!);
    if (!d) return missing(p[1]!);
    if (key) return problem(400, "malformed_request", "This route takes no Idempotency-Key: its guard is expectedGeneration.");
    return rotate(d, (await request.json()) as S["UpdateDestinationAccessRequest"]);
  }
  if (method === "POST" && (p = path.match(/^\/workspaces\/[^/]+\/destinations\/([^/:]+):test$/))) {
    const d = find(p[1]!);
    if (!d) return missing(p[1]!);
    if (key.length < 8) return problem(400, "idempotency_key_required", "This route requires an Idempotency-Key header.");
    const body = (await request.json()) as S["TestDestinationRequest"];
    const made = startCheck(ws, key, "destinationAccess", { kind: "BackupDestination", name: d.name, uid: d.uid }, accessCheck(now(d), (body.roles ?? []) as Role[]));
    return json(made.replayed ? 200 : 202, { item: made.item, replayed: made.replayed, requestId: requestId() });
  }
  return null;
}

/** Test seam: the names the mock holds, so a test can prove no value is among them. */
export function destinationStore(): readonly Destination[] {
  return destinations;
}
