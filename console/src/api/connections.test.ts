// Connections: the probe's words, the create form's rules and request, field
// errors placed from the API's paths, write outcomes, keys, the private-key
// guard and who may operate. Every row has its accepted (or refused) twin.
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "./client";
import {
  CONNECTION_DEFAULTS,
  KEY_IN_PLAIN_FIELD,
  LocalRefusal,
  PROBE_WORDS,
  ProblemError,
  carriesKeyMaterial,
  connectionBody,
  connectionField,
  connectionProblems,
  discoveryBody,
  followGap,
  forgetConnectionCredential,
  grantFor,
  mayAddConnection,
  mayStart,
  mintKey,
  outcomeOf,
  placeProblems,
  preferredConnection,
  probeState,
  resolveSelection,
  savedConnections,
  stoppedFor,
  type Connection,
  type ConnectionValues,
} from "./connections";
import type { components } from "./schema";

type S = components["schemas"];
const fixtures = import.meta.glob("../../../ui/tests/fixtures/console/*.json", { eager: true, import: "default" });
function fixture<T>(name: string): T {
  const found = fixtures[`../../../ui/tests/fixtures/console/${name}`];
  if (found === undefined) throw new Error(`no fixture ${name}`);
  return structuredClone(found) as T;
}

const SECRET = "typed-once-9f2c";
const NOW = Date.parse("2026-09-12T09:20:00Z");

function values(over: Partial<ConnectionValues>): ConnectionValues {
  return { ...CONNECTION_DEFAULTS, servers: "kafka-0:9093", username: "app", password: SECRET, ...over };
}

describe("the connection probe", () => {
  it("reads the fixtures' observations in the probe's own words", () => {
    const list = fixture<S["ConnectionList"]>("connections-list.json");
    const [prod, scratch] = list.items as [Connection, Connection];
    expect(probeState(prod.reachability, NOW).verdict).toBe("reachable");
    expect(probeState(prod.reachability, NOW).stale).toBe(false);
    // Unset with no reason: the controller has written nothing yet.
    expect(probeState(scratch.reachability, NOW).verdict).toBe("never");
  });

  it("calls an observation older than 630 s stale, and a fresh one not", () => {
    const at = "2026-09-12T09:00:00Z";
    expect(probeState({ state: "reachable", observedAt: at }, Date.parse(at) + 631_000).stale).toBe(true);
    expect(probeState({ state: "reachable", observedAt: at }, Date.parse(at) + 629_000).stale).toBe(false);
    // More than a minute in the future: the arithmetic cannot be trusted either way.
    expect(probeState({ state: "reachable", observedAt: at }, Date.parse(at) - 61_000).stale).toBe(true);
    expect(probeState({ state: "reachable", observedAt: at }, Date.parse(at) - 59_000).stale).toBe(false);
  });

  it("tells a refusal, a probe in flight and an unknown reason apart, and never calls a probe in flight stale", () => {
    expect(probeState({ state: "unknown", reason: "ConnectionConfigInvalid" }).verdict).toBe("refused");
    expect(probeState({ state: "unknown", reason: "PlainWithoutTls" }).verdict).toBe("refused");
    expect(probeState({ state: "unknown", reason: "ProbeRunning", observedAt: "2020-01-01T00:00:00Z" }, NOW)).toMatchObject({ verdict: "probing", stale: false });
    expect(probeState({ state: "unknown", reason: "SomethingNew" }).verdict).toBe("unknown");
    expect(probeState({ state: "unknown" }).verdict).toBe("never");
    // A refused connection has no observation, so it is never "stale".
    expect(probeState({ state: "unknown", reason: "CredentialNotRenderable" }, NOW)).toMatchObject({ observed: false, stale: false });
  });

  it("marks a later probe's reason beside an earlier reading, and not the reading's own reason", () => {
    expect(probeState({ state: "reachable", reason: "NoExitCode" }).laterReason).toBe("NoExitCode");
    expect(probeState({ state: "reachable", reason: "Reachable" }).laterReason).toBe("");
    expect(probeState({ state: "unreachable", reason: "ProbeReportedUnreachable" }).laterReason).toBe("");
  });

  it("never says ready about a probe", () => {
    const sayReady = (words: string[]) => words.some((w) => /\bready\b/i.test(w));
    expect(sayReady(Object.values(PROBE_WORDS))).toBe(false);
    // Control: the same test catches the word when it is there.
    expect(sayReady([...Object.values(PROBE_WORDS), "Ready"])).toBe(true);
  });
});

describe("the create form's rules", () => {
  it("refuses SASL/PLAIN without TLS by name and accepts it over TLS", () => {
    const refused = connectionProblems(values({ mode: "plain", tls: false }));
    expect(refused.tls).toMatch(/^PlainWithoutTls: /);
    expect(JSON.stringify(refused)).not.toContain(SECRET);
    expect(connectionProblems(values({ mode: "plain", tls: true }))).toEqual({});
  });

  it("refuses no authentication with TLS on, and accepts a plaintext listener", () => {
    expect(connectionProblems(values({ mode: "plaintext", tls: true })).tls).toMatch(/refused rather than dialled without TLS/);
    expect(connectionProblems(values({ mode: "plaintext", tls: false }))).toEqual({});
  });

  it("asks a SASL mode for its username and its password, and mtls for TLS, the certificate and the key", () => {
    const sasl = connectionProblems(values({ mode: "scramSha256", username: "", password: "" }));
    expect(sasl.username).toMatch(/SCRAM-SHA-256 needs the SASL username/);
    expect(sasl.password).toMatch(/entered once here/);
    const mtls = connectionProblems(values({ mode: "mtls", tls: false, certificatePem: "", privateKeyPem: "" }));
    expect(mtls.tls).toMatch(/requires TLS on/);
    expect(mtls.certificatePem).toBeDefined();
    expect(mtls.privateKeyPem).toBeDefined();
    expect(connectionProblems(values({ mode: "mtls", certificatePem: "-----BEGIN CERTIFICATE-----", privateKeyPem: SECRET }))).toEqual({});
  });

  it("takes bootstrap servers as host:port only", () => {
    expect(connectionProblems(values({ servers: "" })).servers).toMatch(/at least one/);
    expect(connectionProblems(values({ servers: "kafka-0" })).servers).toMatch(/Not host:port: kafka-0/);
    expect(connectionProblems(values({ servers: "kafka-0:70000" })).servers).toMatch(/Not host:port/);
    expect(connectionProblems(values({ servers: "https://kafka-0:9093" })).servers).toMatch(/Not host:port/);
    expect(connectionProblems(values({ servers: "kafka-0:9093, kafka-1:9093\n[::1]:9092" })).servers).toBeUndefined();
  });

  it("takes a private CA from a ConfigMap only with TLS on", () => {
    expect(connectionProblems(values({ tls: false, mode: "scramSha512", caName: "kafka-ca", caKey: "ca.crt" })).caName).toMatch(/requires TLS on/);
    expect(connectionProblems(values({ caName: "kafka-ca", caKey: "" })).caKey).toBeDefined();
    expect(connectionProblems(values({ caName: "kafka-ca", caKey: "ca.crt" }))).toEqual({});
  });
});

describe("the private-key guard", () => {
  const keys = [
    "-----BEGIN PRIVATE KEY-----\nMIG\n-----END PRIVATE KEY-----\n",
    "-----BEGIN RSA PRIVATE KEY-----\nMII\n",
    "-----BEGIN EC PRIVATE KEY-----\nMHc\n",
    "-----BEGIN DSA PRIVATE KEY-----\nMII\n",
    "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMII\n",
    "-----BEGIN OPENSSH PRIVATE KEY-----\nb3Bl\n",
    "-----begin private key-----\nmig\n",
    "-----Begin Rsa Private Key-----\n",
    "-----BEGIN   PRIVATE   KEY-----\n",
    "-----BEGIN PRIVATE\nKEY-----\n",
    "-----BEGIN RSA PRIVATE-KEY-----\n",
    "-----begin openssh private_key-----\n",
  ];

  it("reads every PEM spelling of a private key, in any case and spacing", () => {
    for (const text of keys) expect(carriesKeyMaterial(text), text).toBe(true);
    // Controls: a certificate and a legal object name that merely contains the words.
    expect(carriesKeyMaterial("-----BEGIN CERTIFICATE-----\nMII\n")).toBe(false);
    expect(carriesKeyMaterial("minio-private-key")).toBe(false);
  });

  it("refuses private-key text in a field that is stored and shown back, and keeps the key field for keys", () => {
    const key = keys[0]!;
    expect(connectionProblems(values({ username: key })).username).toBe(KEY_IN_PLAIN_FIELD);
    expect(connectionProblems(values({ servers: key })).servers).toBe(KEY_IN_PLAIN_FIELD);
    expect(connectionProblems(values({ caName: key, caKey: "ca.crt" })).caName).toBe(KEY_IN_PLAIN_FIELD);
    // The one field a private key belongs in takes it.
    expect(connectionProblems(values({ mode: "mtls", certificatePem: "-----BEGIN CERTIFICATE-----", privateKeyPem: key }))).toEqual({});
    // And a CA ConfigMap honestly named for its contents is not refused.
    expect(connectionProblems(values({ caName: "minio-private-key", caKey: "ca.crt" }))).toEqual({});
  });
});

describe("the create request", () => {
  it("carries the credential typed, never named, and exactly as typed", () => {
    const body = connectionBody(values({ username: "  app  ", password: ` ${SECRET} ` }));
    expect(body.auth.username).toBe("app");
    expect(body.auth.credential).toEqual({ password: ` ${SECRET} ` });
    expect(body.auth.credentialRef).toBeUndefined();
    const mtls = connectionBody(values({ mode: "mtls", certificatePem: "C", privateKeyPem: SECRET }));
    expect(mtls.auth.credential).toEqual({ certificatePem: "C", privateKeyPem: SECRET });
    expect(mtls.auth.username).toBeUndefined();
    expect(connectionBody(values({ mode: "plaintext", tls: false })).auth.credential).toBeUndefined();
  });

  it("names a private CA by ConfigMap key only when one is given", () => {
    expect(connectionBody(values({ caName: "kafka-ca", caKey: "ca.crt" })).auth.tlsCa).toEqual({ configMapKeyRef: { name: "kafka-ca", key: "ca.crt" } });
    expect(connectionBody(values({})).auth.tlsCa).toBeUndefined();
  });

  it("drops the credential from the request once it is sent", () => {
    const body = connectionBody(values({}));
    expect(JSON.stringify(body)).toContain(SECRET);
    forgetConnectionCredential(body);
    expect(JSON.stringify(body)).not.toContain(SECRET);
    expect(body.bootstrapServers).toEqual(["kafka-0:9093"]);
  });
});

describe("field errors and outcomes", () => {
  it("places the API's field errors beside their fields and lists what no field claims", () => {
    const problem = fixture<S["Problem"]>("problem-validation.json");
    problem.errors = [...(problem.errors ?? []), { field: "markerTopic", code: "invalid", message: "is not a topic name" }];
    const placed = placeProblems(new ProblemError(422, problem), (p) => connectionField(p, "scramSha512"));
    expect(placed.fields).toEqual({ servers: "must be host:port with no scheme or userinfo", username: "scramSha512 requires a username" });
    expect(placed.unmatched).toEqual(["markerTopic: is not a topic name"]);
  });

  it("places a bare credential error on the field the mode uses", () => {
    expect(connectionField("auth.credential", "scramSha512")).toBe("password");
    expect(connectionField("auth.credential", "mtls")).toBe("certificatePem");
    expect(connectionField("auth.tlsCa.configMapKeyRef.key", "plain")).toBe("caKey");
  });

  it("calls a write with no answer unknown, and a refusal a refusal", () => {
    expect(outcomeOf(new TypeError("Failed to fetch"))).toBe("unknown");
    expect(outcomeOf(new ApiError(504, { code: "upstream_timeout" }))).toBe("unknown");
    expect(outcomeOf(new ProblemError(422, fixture("problem-validation.json")))).toBe("invalid");
    expect(outcomeOf(new ApiError(409, fixture("problem-conflict.json")))).toBe("refused");
    expect(outcomeOf(new LocalRefusal("no random source"))).toBe("local");
  });
});

describe("idempotency keys", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("mints a key the API accepts, a different one each time", () => {
    const a = mintKey("connection");
    expect(a).toMatch(/^logweir-console\.connection\.[0-9a-f]{32}$/);
    expect(a.length).toBeGreaterThanOrEqual(8);
    expect(a.length).toBeLessThanOrEqual(128);
    expect(mintKey("connection")).not.toBe(a);
  });

  it("refuses rather than fall back to a counter when there is no random source", () => {
    vi.stubGlobal("crypto", undefined);
    expect(() => mintKey("connection")).toThrow(LocalRefusal);
  });
});

describe("who may operate", () => {
  it("gives operator actions to the operator and administrator roles, and to localAdmin", () => {
    const operator = fixture<S["SessionResponse"]>("session.json");
    expect(grantFor(operator, "team-a").mayOperate).toBe(true);
    const viewer = fixture<S["SessionResponse"]>("session-viewer.json");
    expect(grantFor(viewer, "team-a")).toMatchObject({ known: true, mayOperate: false });
    // A flag is a read floor: the viewer may read connections and still not add one.
    expect(grantFor(viewer, "team-a").can?.connectionsRead).toBe(true);
    expect(grantFor(viewer, "team-b")).toEqual({ known: true, can: null, mayOperate: false });
    expect(grantFor(undefined, "team-a").known).toBe(false);
    const oidcOperator: S["SessionResponse"] = { ...viewer, namespaces: viewer.namespaces.map((g) => ({ ...g, roles: ["operator"] })) };
    expect(grantFor(oidcOperator, "team-a").mayOperate).toBe(true);
  });

  it("offers Add, Test and Discover by flag and role together", () => {
    const viewer = grantFor(fixture<S["SessionResponse"]>("session-viewer.json"), "team-a");
    const admin = grantFor(fixture<S["SessionResponse"]>("session.json"), "team-a");
    expect([mayAddConnection(viewer), mayStart(viewer, "preflight"), mayStart(viewer, "destinations")]).toEqual([false, false, false]);
    expect([mayAddConnection(admin), mayStart(admin, "preflight"), mayStart(admin, "destinations")]).toEqual([true, true, true]);
    // A role that may operate still needs the domain's flag (a domain this build does not serve is false).
    const unserved = { ...admin, can: { ...admin.can!, credentialWrite: false, topicDiscovery: false } };
    expect(mayAddConnection(unserved)).toBe(false);
    expect(mayStart(unserved, "topicDiscovery")).toBe(false);
  });
});

describe("following a check", () => {
  it("reads every 2 s for 20 s, then backs off to 10 s", () => {
    expect([0, 9, 10, 11, 12, 13, 30].map(followGap)).toEqual([2000, 2000, 3000, 4500, 6750, 10_000, 10_000]);
  });

  it("stops only an unfinished check, and says why", () => {
    expect(stoppedFor(false, true, false)).toBe("deadline");
    expect(stoppedFor(false, false, true)).toBe("unreadable");
    expect(stoppedFor(false, false, false)).toBe("");
    // A finished check is an answer, whatever the clock says.
    expect(stoppedFor(true, true, true)).toBe("");
  });

  it("asks a discovery about named topics only when some are given", () => {
    expect(discoveryBody({ includeInternal: false, expectedTopics: "" })).toEqual({});
    expect(discoveryBody({ includeInternal: true, expectedTopics: "orders, payments\nrefunds" })).toEqual({ includeInternal: true, expectedTopics: ["orders", "payments", "refunds"] });
  });
});

describe("choosing a saved connection", () => {
  const list = () => fixture<S["ConnectionList"]>("connections-list.json").items as Connection[];

  it("offers every connection by name, and prefers a role without filtering by it", () => {
    expect(savedConnections(list()).map((c) => c.name)).toEqual(["orders-prod", "orders-scratch"]);
    expect(preferredConnection(list(), "target")?.name).toBe("orders-scratch");
    expect(preferredConnection(list(), "nobody")?.name).toBe("orders-prod");
    expect(preferredConnection([], "target")).toBeNull();
  });

  it("resolves by UID, pins a name-only reference, and follows a rename", () => {
    const [prod] = list() as [Connection];
    expect(resolveSelection(list(), null)).toEqual({ state: "none" });
    expect(resolveSelection(list(), { uid: prod.uid, name: prod.name })).toMatchObject({ state: "selected", pinned: false, renamedFrom: "" });
    expect(resolveSelection(list(), { uid: "", name: "orders-prod" })).toMatchObject({ state: "selected", pinned: true });
    expect(resolveSelection(list(), { uid: prod.uid, name: "orders-old" })).toMatchObject({ state: "selected", renamedFrom: "orders-old" });
  });

  it("refuses a recreated connection and a missing one, and never moves the choice onto another object", () => {
    const recreated = list();
    recreated[0] = { ...recreated[0]!, uid: "ffffffff-0000-4000-9000-000000000000" };
    const held = { uid: "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9", name: "orders-prod" };
    expect(resolveSelection(recreated, held)).toEqual({ state: "recreated", ...held, recreatedUid: "ffffffff-0000-4000-9000-000000000000" });
    expect(resolveSelection(recreated.slice(1), held)).toEqual({ state: "missing", ...held });
    // Control: with the original object in place the same choice resolves.
    expect(resolveSelection(list(), held).state).toBe("selected");
  });
});
