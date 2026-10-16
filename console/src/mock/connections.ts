// The mock of the Connections routes, readiness checks and topic discovery.
// Like the real service, it keeps a typed credential nowhere: a create turns
// it into a Secret NAME and drops the value. Shapes follow the console
// fixtures (console/fixtures/console/).
import type { components } from "../api/schema";
import { connections, hex, requestId } from "./data";
import { DAY, HOUR, MINUTE, NOW, iso } from "./clock";

type S = components["schemas"];
type Connection = S["Connection"];
type Preflight = S["Preflight"];
type CheckEntry = S["CheckEntryView"];
type TopicDiscovery = S["TopicDiscovery"];
type TopicEntry = S["TopicEntryView"];

export function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" },
  });
}

export function problem(status: number, code: string, detail: string, errors?: S["FieldError"][]): Response {
  return json(status, {
    type: `https://logweir.dev/problems/${code.replace(/_/g, "-")}`,
    title: code,
    status,
    code,
    detail,
    requestId: requestId(),
    retryable: false,
    ...(errors ? { errors } : {}),
  });
}

/** A stable 26-character base32 name part from any text (the service derives it from the idempotency scope). */
export function base32(text: string): string {
  let seed = 0;
  for (const ch of text) seed = (Math.imul(seed, 31) + ch.charCodeAt(0)) >>> 0;
  const alphabet = "abcdefghijklmnopqrstuvwxyz234567";
  return Array.from(hex(seed, 26), (h, i) => alphabet[(parseInt(h, 16) * 2 + (i % 2)) % 32]).join("");
}

export function uuid(seed: string): string {
  const h = hex(Number.parseInt(base32(seed).slice(0, 6), 36) || 1, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-9${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

/** The Secret NAMES the mock created. Never a value: there is nothing here to read one from. */
export const createdSecrets: string[] = [];

// --------------------------------------------------------------- the probe

/** Connections made in this page: their first probe runs for a few seconds. */
const probeFrom = new Map<string, number>();
const PROBE_RUN_MS = 5000;

function reachNow(c: Connection): Connection {
  const from = probeFrom.get(c.name);
  if (from === undefined) return c;
  const down = c.bootstrapServers.some((s) => s.includes("down"));
  if (Date.now() - from < PROBE_RUN_MS) return { ...c, reachability: { state: "unknown", reason: "ProbeRunning" } };
  return {
    ...c,
    reachability: down
      ? { state: "unreachable", reason: "BrokerTransportFailure", observedAt: iso(from + PROBE_RUN_MS) }
      : { state: "reachable", reason: "Reachable", clusterId: `Q0x${base32(c.name).slice(0, 16).toUpperCase()}`, observedAt: iso(from + PROBE_RUN_MS) },
  };
}

// --------------------------------------------------------- readiness checks

interface HeldCheck {
  item: Preflight;
  since: number;
  runMs: number;
  final: Pick<Preflight, "state" | "reason" | "checks" | "warnings" | "executionOnly">;
}

const checks = new Map<string, HeldCheck>();
const checkKeys = new Map<string, string>();

function checkNow(held: HeldCheck): Preflight {
  const e = Date.now() - held.since;
  if (e < 800) return { ...held.item, state: "pending", terminal: false };
  if (e < held.runMs) return { ...held.item, state: "running", terminal: false };
  const done = held.since + held.runMs;
  return {
    ...held.item,
    ...held.final,
    terminal: true,
    applicable: true,
    observedAt: iso(done),
    expiresAt: iso(done + 15 * MINUTE),
    staleBasis: ["expiry", ...held.item.binding.referents.map((r) => `referent:${r.kind}/${r.name}`)],
  };
}

/** Starts a mock Preflight under an idempotency key; the same key replays the same check. */
export function startCheck(
  ws: string,
  key: string,
  operation: Preflight["operation"],
  referent: { kind: string; name: string; uid: string },
  final: HeldCheck["final"],
): { item: Preflight; replayed: boolean } {
  const known = checkKeys.get(`${ws}/${key}`);
  if (known && checks.has(known)) return { item: checkNow(checks.get(known)!), replayed: true };
  const id = `pf-${base32(`${ws}/${key}`)}`;
  const item: Preflight = {
    id,
    namespace: ws,
    uid: uuid(id),
    resourceVersion: String(70000 + checks.size),
    createdAt: iso(Date.now()),
    operation,
    state: "pending",
    terminal: false,
    applicable: false,
    stale: false,
    staleBasis: [],
    staleReasons: [],
    binding: { referents: [{ ...referent, generation: 1 }], inputsDigest: `sha256:${hex(checks.size + 7, 64)}` },
    checks: [],
    warnings: [],
    executionOnly: [],
    conditions: [],
    detailsAvailable: false,
  };
  checks.set(id, { item, since: Date.now(), runMs: 4500, final });
  checkKeys.set(`${ws}/${key}`, id);
  return { item, replayed: false };
}

/** The newest check about one object, as a `lastTest` summary. */
export function lastTestFor(ws: string, kind: string, name: string): S["LastTestView"] | undefined {
  const mine = [...checks.values()].filter((h) => h.item.namespace === ws && h.item.binding.referents.some((r) => r.kind === kind && r.name === name));
  const newest = mine.sort((a, b) => b.since - a.since)[0];
  if (!newest) return undefined;
  const now = checkNow(newest);
  return { preflightId: now.id, state: now.state, stale: false, truncated: false, observedAt: now.observedAt ?? null };
}

function entry(id: string, state: CheckEntry["state"], code: string, at: number, extra: Partial<CheckEntry> = {}): CheckEntry {
  return { id, state, code, gating: "blocking", authority: "checkJob", category: id.split(".")[0], observedAt: iso(at), expiresAt: iso(at + 15 * MINUTE), ...extra };
}

/** FX-46: the three advisory capability rows a Test connection carries. */
function capabilityRows(at: number, scope: CheckEntry["scope"], authenticated: boolean): CheckEntry[] {
  const advisory = { gating: "advisory" as const, scope };
  if (!authenticated) {
    return ["connection.engineProtocol", "target.engineProtocol", "connection.groupTypes"].map((id) =>
      entry(id, "unknown", "BlockedByPrerequisite", at, { ...advisory, message: "the connection did not authenticate, so this check did not run" }),
    );
  }
  return [
    entry("connection.engineProtocol", "ready", "EngineProtocolServed", at, { ...advisory, message: "the brokers serve every request version the engine sends to back up (Fetch v4 and up)" }),
    entry("target.engineProtocol", "ready", "EngineProtocolServed", at, { ...advisory, message: "the brokers serve every request version the engine sends to restore (Produce v8, served v0-v13)" }),
    entry("connection.groupTypes", "ready", "GroupTypesListed", at, { ...advisory, message: "the brokers list consumer groups with their types" }),
  ];
}

function connectionCheckResult(c: Connection, at = Date.now() + 4500): HeldCheck["final"] {
  const scope = { kind: "KafkaCluster", name: c.name, uid: c.uid };
  const reachable = reachNow(c).reachability.state !== "unreachable";
  const principal = c.auth.username ? `User:${c.auth.username}` : c.auth.mode === "mtls" ? "the client certificate's subject" : "ANONYMOUS";
  return {
    state: reachable ? "ready" : "notReady",
    reason: reachable ? "Ready" : "NotReady",
    checks: [
      entry("connection.resolved", "ready", "ConnectionResolved", at, { authority: "controller", scope }),
      entry("connection.credentialProjected", "ready", "CredentialProjected", at, { scope, message: c.auth.mode === "plaintext" ? "this connection presents no credential" : "the credential Secret was projected into the check Job" }),
      reachable
        ? entry("connection.authenticated", "ready", "Authenticated", at, { scope, message: `authenticated as ${principal}` })
        : entry("connection.authenticated", "notReady", "BrokerTransportFailure", at, {
            scope,
            message: "no broker answered within the connect budget",
            remedy: "Check the bootstrap servers, and that the brokers are reachable from pods in this workspace.",
          }),
      reachable
        ? entry("connection.clusterIdentity", "ready", "ClusterIdentityMatched", at, { scope, message: `cluster id ${reachNow(c).reachability.clusterId ?? "recorded"}` })
        : entry("connection.clusterIdentity", "unknown", "NotAttempted", at, { scope, message: "not attempted: the connection did not authenticate" }),
      // The words a real check pod records: one long unbroken token, which a table must wrap (DUI-7).
      entry("runner.image", "ready", "ImageAvailable", at, {
        scope: { kind: "Pod", name: `lwc-sc-${c.uid.slice(0, 8)}` },
        message:
          "the runner image was present and the container started [imageID=docker-pullable://registry.example/logweir@sha256:6aad79ee4625aed9555fd0de794ec7671f9247f5be590ed1e4b1c0d2f3a4b5c6]",
      }),
      ...capabilityRows(at, scope, reachable),
    ],
    warnings: [],
    executionOnly: [],
  };
}

// ------------------------------------------------------------------ topics

const ORDERS_TOPICS: TopicEntry[] = [
  ...["orders", "orders-dlq", "orders-retry", "payments", "payments-dlq", "refunds", "invoices", "customers", "customer-addresses", "carts"].map((name, i) => ({
    name,
    partitions: [12, 3, 3, 6, 3, 6, 6, 4, 4, 6][i]!,
    internal: false,
    expected: false,
  })),
  ...["inventory", "inventory-reservations", "pricing", "promotions", "catalog", "catalog-images", "search-events", "notifications", "emails", "sms"].map((name, i) => ({
    name,
    partitions: [6, 6, 3, 3, 6, 3, 12, 3, 3, 3][i]!,
    internal: false,
    expected: false,
  })),
  ...["audit-log", "fraud-signals", "fraud-decisions", "loyalty-points", "returns", "shipments", "warehouse-events", "connect-configs", "connect-offsets", "connect-status"].map((name, i) => ({
    name,
    partitions: [3, 6, 6, 3, 3, 0, 6, 1, 25, 5][i]!,
    internal: false,
    expected: false,
    ...(name === "shipments" ? { errorCode: "UnknownTopicOrPartition" } : {}),
  })),
  { name: "__consumer_offsets", partitions: 50, internal: true, expected: false },
  { name: "__transaction_state", partitions: 50, internal: true, expected: false },
].sort((a, b) => (a.name < b.name ? -1 : 1));

interface HeldDiscovery {
  item: TopicDiscovery;
  since: number;
  connection: Connection;
  request: { includeInternal: boolean; expected: string[] };
}

const discoveries = new Map<string, HeldDiscovery>();
const discoveryKeys = new Map<string, string>();
const slots = new Map<string, { latest: string | null; success: string | null }>();
const DISCOVERY_RUN_MS = 5000;

function inventoryOf(d: HeldDiscovery): TopicEntry[] {
  if (d.connection.name !== "orders-prod" && !probeFrom.has(d.connection.name)) return [];
  const expected = new Set(d.request.expected);
  return ORDERS_TOPICS.filter((t) => d.request.includeInternal || !t.internal).map((t) => ({ ...t, expected: expected.has(t.name) }));
}

function discoveryNow(d: HeldDiscovery): TopicDiscovery {
  const base = d.item;
  if (base.terminal) return { ...base, stale: base.freshUntil ? Date.parse(base.freshUntil) < Date.now() : false, staleReasons: base.freshUntil && Date.parse(base.freshUntil) < Date.now() ? ["expired"] : [] };
  const e = Date.now() - d.since;
  if (e < 800) return { ...base, state: "pending", terminal: false, reason: "Pending" };
  if (e < DISCOVERY_RUN_MS) return { ...base, state: "running", terminal: false, reason: "Running" };
  const done = d.since + DISCOVERY_RUN_MS;
  const reachable = reachNow(d.connection).reachability.state !== "unreachable";
  if (!reachable) {
    return { ...base, state: "failed", terminal: true, reason: "BrokerTransportFailure", observedAt: iso(done), error: { code: "BrokerTransportFailure", message: "no broker answered within the metadata budget" } };
  }
  const inventory = inventoryOf(d);
  const names = new Set(ORDERS_TOPICS.map((t) => t.name));
  const restricted = d.request.expected.filter((n) => n.startsWith("restricted"));
  const visible = d.request.expected.filter((n) => names.has(n));
  const all = ORDERS_TOPICS.length;
  return {
    ...base,
    state: "succeeded",
    terminal: true,
    reason: "Succeeded",
    observedAt: iso(done),
    freshUntil: iso(done + HOUR),
    clusterId: reachNow(d.connection).reachability.clusterId ?? null,
    chunkCount: 1,
    counts: { listed: all, returned: inventory.length, internalExcluded: all - inventory.length, errored: inventory.filter((t) => t.errorCode).length },
    expected: d.request.expected.length
      ? { requested: d.request.expected.length, visible: visible.length, notAuthorized: restricted.length, notFound: d.request.expected.length - visible.length - restricted.length, unknown: 0 }
      : undefined,
    visibility: restricted.length ? { state: "limited", basis: ["listingOnly", "expectedTopicNotAuthorized"] } : { state: "unknown", basis: ["listingOnly"] },
    topicsSha256: `sha256:${hex(inventory.length + 3, 64)}`,
  };
}

function settleSlots(name: string): void {
  const slot = slots.get(name);
  if (!slot?.latest) return;
  const latest = discoveries.get(slot.latest);
  if (latest && discoveryNow(latest).state === "succeeded") slot.success = slot.latest;
}

function newDiscovery(c: Connection, id: string, request: HeldDiscovery["request"], since: number): HeldDiscovery {
  return {
    since,
    connection: c,
    request,
    item: {
      id,
      namespace: c.namespace,
      uid: uuid(id),
      resourceVersion: String(2429000 + discoveries.size),
      createdAt: iso(since),
      state: "pending",
      terminal: false,
      stale: false,
      staleReasons: [],
      truncated: false,
      chunkCount: 0,
      conditions: [],
      connection: { name: c.name, uid: c.uid, generation: 1, authMode: c.auth.mode, principal: c.auth.username ? `User:${c.auth.username}` : null },
    },
  };
}

// Seeds: orders-prod has an inventory past its freshness; analytics-prod's last attempt failed.
function seed(): void {
  const orders = connections.find((c) => c.name === "orders-prod");
  if (orders) {
    const since = NOW - 3 * HOUR;
    const held = newDiscovery(orders, "td-akkwhbx3ek33inhuajcdtth7q5", { includeInternal: false, expected: ["orders", "payments"] }, since);
    held.item = { ...discoveryNow({ ...held, since: since - DISCOVERY_RUN_MS - 1 }), observedAt: iso(since), freshUntil: iso(since + HOUR) };
    discoveries.set(held.item.id, held);
    slots.set("orders-prod", { latest: held.item.id, success: held.item.id });
  }
  const analytics = connections.find((c) => c.name === "analytics-prod");
  if (analytics) {
    const since = NOW - 50 * MINUTE;
    const held = newDiscovery(analytics, "td-newest", { includeInternal: false, expected: [] }, since);
    held.item = {
      ...held.item,
      state: "failed",
      terminal: true,
      reason: "BrokerTransportFailure",
      observedAt: iso(since + 20_000),
      error: { code: "BrokerTransportFailure", message: "no broker answered within the metadata budget" },
    };
    discoveries.set(held.item.id, held);
    slots.set("analytics-prod", { latest: held.item.id, success: null });
  }
}
seed();

/** Seeded recorded tests, so a connection's page shows what a returning operator sees. */
const SEEDED_TESTS: Record<string, S["LastTestView"]> = {
  "orders-prod": { preflightId: "pf-c5ovcfrjvxwk7axdpexnma4w6l", state: "ready", stale: true, truncated: false, observedAt: iso(NOW - 26 * HOUR) },
  "analytics-prod": { preflightId: "pf-7mz2b4qkx6cv3ahd5ryuse2wtn", state: "notReady", stale: false, truncated: false, observedAt: iso(NOW - DAY + 40 * MINUTE) },
};

// The seeded tests' own checks, so the page can read their rows (FX-46). Kept apart from
// `checks`, so a connection's recorded test stays the seeded summary above.
const seededChecks = new Map<string, HeldCheck>();
for (const [name, t] of Object.entries(SEEDED_TESTS)) {
  const c = connections.find((x) => x.name === name);
  if (!c) continue;
  const done = Date.parse(t.observedAt ?? "");
  const made = startCheck(c.namespace, `seed-${name}`, "sourceConnection", { kind: "KafkaCluster", name: c.name, uid: c.uid }, connectionCheckResult(c, done));
  const held = checks.get(made.item.id)!;
  checks.delete(made.item.id);
  checkKeys.delete(`${c.namespace}/seed-${name}`);
  held.item = { ...held.item, id: t.preflightId, createdAt: iso(done - held.runMs) };
  held.since = done - held.runMs;
  seededChecks.set(t.preflightId, held);
}

// ------------------------------------------------------------------ creates

const HOST_PORT = /^[^\s:/@]+:\d{1,5}$/;
const SASL = ["scramSha512", "scramSha256", "plain"];
const connectionKeys = new Map<string, { name: string; digest: string }>();

function createConnection(ws: string, key: string, body: S["CreateConnectionRequest"]): Response {
  // Write-only fields hash as <write-only>: a value is not part of the request's identity.
  const digest = JSON.stringify({ ...body, auth: { ...body.auth, credential: body.auth.credential ? "<write-only>" : undefined } });
  const held = connectionKeys.get(`${ws}/${key}`);
  if (held) {
    if (held.digest !== digest) return problem(409, "idempotency_conflict", "This idempotency key was already used with a different request body.");
    const existing = connections.find((c) => c.namespace === ws && c.name === held.name)!;
    return json(200, { item: reachNow(existing), replayed: true, requestId: requestId() });
  }
  const errors: S["FieldError"][] = [];
  body.bootstrapServers.forEach((s, i) => {
    if (!HOST_PORT.test(s)) errors.push({ field: `bootstrapServers[${i}]`, code: "invalid_port", message: "must be host:port with no scheme or userinfo" });
  });
  const a = body.auth;
  const credential = a.credential ?? {};
  if (SASL.includes(a.mode) && !a.username) errors.push({ field: "auth.username", code: "required", message: `${a.mode} requires a username` });
  if (SASL.includes(a.mode) && !credential.password) errors.push({ field: "auth.credential.password", code: "required", message: `${a.mode} requires a password` });
  if (a.mode === "plain" && !a.tls) errors.push({ field: "auth.tls", code: "plain_requires_tls", message: "plain sends the password itself and is accepted only with tls: true" });
  if (a.mode === "mtls") {
    if (!a.tls) errors.push({ field: "auth.tls", code: "mtls_requires_tls", message: "mtls requires tls: true" });
    if (!/-----BEGIN CERTIFICATE-----/.test(credential.certificatePem ?? "")) {
      errors.push({ field: "auth.credential.certificatePem", code: "invalid_pem", message: "is not a PEM certificate" });
    }
    if (!/-----BEGIN [A-Z ]*PRIVATE KEY-----/.test(credential.privateKeyPem ?? "")) {
      errors.push({ field: "auth.credential.privateKeyPem", code: "invalid_pem", message: "is not an unencrypted PEM private key" });
    } else if (/ENCRYPTED/.test(credential.privateKeyPem ?? "")) {
      errors.push({ field: "auth.credential.privateKeyPem", code: "encrypted_key", message: "is encrypted; paste the unencrypted key" });
    }
  }
  if (a.mode === "plaintext" && a.credential) errors.push({ field: "auth.credential", code: "not_allowed", message: "plaintext takes no credential" });
  if (errors.length > 0) return problem(422, "validation_failed", "One or more fields are invalid.", errors);

  const name = `conn-${base32(`${ws}/${key}`)}`;
  const secret = a.mode === "plaintext" ? undefined : `${name}-credential`;
  if (secret) createdSecrets.push(secret);
  const item: Connection = {
    name,
    namespace: ws,
    uid: uuid(name),
    role: body.role,
    resourceVersion: String(61000 + connections.length),
    createdAt: iso(Date.now()),
    bootstrapServers: body.bootstrapServers,
    markerTopic: body.markerTopic ?? null,
    auth: {
      mode: a.mode,
      tls: a.tls,
      username: SASL.includes(a.mode) ? (a.username ?? null) : null,
      credentialRef: secret && a.mode !== "mtls" ? { name: secret } : undefined,
      clientCertificateRef: secret && a.mode === "mtls" ? { name: secret } : undefined,
      tlsCa: a.tlsCa ? { kind: "configMap", name: a.tlsCa.configMapKeyRef.name, key: a.tlsCa.configMapKeyRef.key } : undefined,
    },
    reachability: { state: "unknown", reason: "ProbeRunning" },
  };
  connections.unshift(item);
  probeFrom.set(name, Date.now());
  connectionKeys.set(`${ws}/${key}`, { name, digest });
  return json(201, { item, replayed: false, requestId: requestId() });
}

// ------------------------------------------------------------------ routing

function listPage<T>(items: T[]) {
  return { items, page: { limit: 200, snapshot: String(60219 + items.length) }, requestId: requestId() };
}

/** The Connections, readiness-check and discovery routes, or null for any other request. */
export async function connectionsMock(request: Request, method: string, path: string): Promise<Response | null> {
  let p: RegExpMatchArray | null;
  const ws = path.match(/^\/workspaces\/([^/]+)\//)?.[1] ?? "";
  const key = request.headers.get("Idempotency-Key") ?? "";
  const find = (name: string) => connections.find((c) => c.namespace === ws && c.name === name);

  if (method === "GET" && /^\/workspaces\/[^/]+\/connections$/.test(path)) {
    return json(200, listPage(connections.filter((c) => c.namespace === ws).map(reachNow)));
  }
  if (method === "POST" && /^\/workspaces\/[^/]+\/connections$/.test(path)) {
    if (key.length < 8) return problem(400, "idempotency_key_required", "This route requires an Idempotency-Key header.");
    return createConnection(ws, key, (await request.json()) as S["CreateConnectionRequest"]);
  }
  if (method === "GET" && (p = path.match(/^\/workspaces\/[^/]+\/connections\/([^/]+)$/))) {
    const c = find(p[1]!);
    if (!c) return problem(404, "not_found", `No connection named ${p[1]} in ${ws}.`);
    const lastTest = lastTestFor(ws, "KafkaCluster", c.name) ?? SEEDED_TESTS[c.name];
    return json(200, { item: { ...reachNow(c), ...(lastTest ? { lastTest } : {}) }, requestId: requestId() });
  }

  // --- readiness checks
  if (method === "POST" && /^\/workspaces\/[^/]+\/preflights$/.test(path)) {
    const body = (await request.json()) as S["CreatePreflightRequest"];
    if (body.operation !== "sourceConnection" || !body.sourceConnection?.connectionRef) {
      return problem(422, "validation_failed", "The prototype only mocks the sourceConnection check here.", [{ field: "operation", code: "unsupported", message: "only sourceConnection is mocked" }]);
    }
    const c = find(body.sourceConnection.connectionRef);
    if (!c) return problem(404, "not_found", `No connection named ${body.sourceConnection.connectionRef} in ${ws}.`);
    const made = startCheck(ws, key, "sourceConnection", { kind: "KafkaCluster", name: c.name, uid: c.uid }, connectionCheckResult(c));
    return json(made.replayed ? 200 : 202, { item: made.item, replayed: made.replayed, requestId: requestId() });
  }
  if (method === "GET" && (p = path.match(/^\/workspaces\/[^/]+\/preflights\/([^/:]+)$/))) {
    const held = checks.get(p[1]!) ?? seededChecks.get(p[1]!);
    return held ? json(200, { item: checkNow(held), requestId: requestId() }) : problem(404, "not_found", `No readiness check named ${p[1]} in ${ws}.`);
  }

  // --- topic discovery
  if (method === "GET" && (p = path.match(/^\/workspaces\/[^/]+\/connections\/([^/]+)\/topic-discoveries$/))) {
    const name = p[1]!;
    if (!find(name)) return problem(404, "not_found", `No connection named ${name} in ${ws}.`);
    settleSlots(name);
    const slot = slots.get(name) ?? { latest: null, success: null };
    const read = (id: string | null) => (id && discoveries.has(id) ? discoveryNow(discoveries.get(id)!) : null);
    return json(200, { latestAttempt: read(slot.latest), lastSuccessful: read(slot.success), requestId: requestId() });
  }
  if (method === "POST" && (p = path.match(/^\/workspaces\/[^/]+\/connections\/([^/]+)\/topic-discoveries$/))) {
    const c = find(p[1]!);
    if (!c) return problem(404, "not_found", `No connection named ${p[1]} in ${ws}.`);
    const body = (await request.json()) as S["CreateTopicDiscoveryRequest"];
    const req = { includeInternal: body.includeInternal === true, expected: [...(body.expectedTopics ?? [])].sort() };
    const replay = discoveryKeys.get(`${ws}/${key}`);
    if (replay) return json(200, { item: discoveryNow(discoveries.get(replay)!), replayed: true, requestId: requestId() });
    settleSlots(c.name);
    const slot = slots.get(c.name) ?? { latest: null, success: null };
    const success = slot.success ? discoveries.get(slot.success) : undefined;
    if (body.reuseFresh !== false && success && JSON.stringify(success.request) === JSON.stringify(req) && !discoveryNow(success).stale) {
      return json(200, { item: discoveryNow(success), reused: true, requestId: requestId() });
    }
    const id = `td-${base32(`${ws}/${key}`)}`;
    discoveries.set(id, newDiscovery(c, id, req, Date.now()));
    discoveryKeys.set(`${ws}/${key}`, id);
    slots.set(c.name, { latest: id, success: slot.success });
    return json(202, { item: discoveryNow(discoveries.get(id)!), replayed: false, reused: false, requestId: requestId() });
  }
  if (method === "POST" && (p = path.match(/^\/workspaces\/[^/]+\/topic-discoveries\/([^/:]+):cancel$/))) {
    const held = discoveries.get(p[1]!);
    if (!held) return problem(404, "not_found", `No topic discovery named ${p[1]} in ${ws}.`);
    const now = discoveryNow(held);
    if (now.terminal) return json(200, { id: now.id, state: now.state, alreadyTerminal: true, requestId: requestId() });
    held.item = { ...now, state: "cancelled", terminal: true, reason: "Cancelled", observedAt: iso(Date.now()) };
    return json(200, { id: now.id, state: "cancelled", alreadyTerminal: false, requestId: requestId() });
  }
  if (method === "GET" && (p = path.match(/^\/workspaces\/[^/]+\/topic-discoveries\/([^/:]+)\/topics$/))) {
    const held = discoveries.get(p[1]!);
    if (!held || discoveryNow(held).state !== "succeeded") return problem(404, "not_found", `No stored inventory for ${p[1]}.`);
    const url = new URL(request.url);
    const q = (url.searchParams.get("q") ?? "").toLowerCase();
    const prefix = url.searchParams.get("prefix") ?? "";
    const internal = url.searchParams.get("internal") ?? "exclude";
    const errored = url.searchParams.get("errored") ?? "include";
    const limit = Number(url.searchParams.get("limit") ?? 50);
    const offset = Number(atob(url.searchParams.get("cursor") ?? "") || 0);
    const rows = inventoryOf(held)
      .filter((t) => !q || t.name.toLowerCase().includes(q))
      .filter((t) => t.name.startsWith(prefix))
      .filter((t) => internal === "include" || !t.internal)
      .filter((t) => (errored === "exclude" ? !t.errorCode : errored === "only" ? Boolean(t.errorCode) : true));
    const pageRows = rows.slice(offset, offset + limit);
    const next = offset + limit < rows.length ? btoa(String(offset + limit)) : undefined;
    const now = discoveryNow(held);
    return json(200, {
      items: pageRows,
      page: { limit, ...(next ? { nextCursor: next } : {}), snapshot: `${now.uid}@${now.topicsSha256 ?? ""}` },
      scan: { chunksScanned: 1, complete: true },
      requestId: requestId(),
    });
  }
  return null;
}
