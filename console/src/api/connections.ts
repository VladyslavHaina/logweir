// Connections: the reads, the write-once create, the connection check (a
// `sourceConnection` Preflight) and topic discovery, over the typed client.
// Also the pieces the destinations module shares: write outcomes, field
// errors placed from the API's `errors[].field`, idempotency keys, the
// private-key guard, a role's grant, and following a check until it answers.
import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, LocalRefusal, api, call, createHeaders, mintKey, outcomeUnknown } from "./client";
import { keys, useConnections } from "./queries";
import { useCan, useSessionDoc } from "./session";
import type { components, operations } from "./schema";

type S = components["schemas"];
export type Connection = S["Connection"];
export type CreateConnectionRequest = S["CreateConnectionRequest"];
export type ConnectionAuthMode = S["ConnectionAuthMode"];
export type Reachability = S["ReachabilityView"];
export type LastTest = S["LastTestView"];
export type Preflight = S["Preflight"];
export type TopicDiscovery = S["TopicDiscovery"];
export type TopicPage = S["TopicPageResponse"];
export type FieldError = S["FieldError"];
type Session = S["SessionResponse"];
type Capabilities = S["Capabilities"];

// ------------------------------------------------------------------ grants

/** The two product roles that may start, cancel and manage (`authz.rs`). */
export const OPERATING_ROLES: readonly string[] = ["operator", "administrator"];

export interface Grant {
  /** False until the session has answered. */
  known: boolean;
  /** The namespace grant's capability flags (`implemented && allowed`), or null without a grant. */
  can: Capabilities | null;
  /**
   * A role, not a flag: the domain flags (`destinations`, `topicDiscovery`,
   * `preflight`) are read floors and are true for a viewer too. localAdmin has
   * no roles and holds every action, so the server is the gate there.
   */
  mayOperate: boolean;
}

export function grantFor(session: Session | undefined, ns: string): Grant {
  if (!session) return { known: false, can: null, mayOperate: false };
  const grant = session.namespaces.find((g) => g.name === ns);
  if (!grant) return { known: true, can: null, mayOperate: false };
  const mayOperate =
    session.authenticationMode === "localAdmin" || grant.roles.some((r) => OPERATING_ROLES.includes(r));
  return { known: true, can: grant.capabilities, mayOperate };
}

/** Add a connection: the create flag, the write-only credential flag, and an operating role. */
export function mayAddConnection(g: Grant): boolean {
  return g.known && Boolean(g.can?.connectionCreate) && Boolean(g.can?.credentialWrite) && g.mayOperate;
}

/** Start a check or a discovery: the domain is served and readable here, and the role may operate. */
export function mayStart(g: Grant, domain: "preflight" | "topicDiscovery" | "destinations"): boolean {
  return Boolean(g.can?.[domain]) && g.mayOperate;
}

export function useGrant(ns: string): Grant {
  return grantFor(useSessionDoc(), ns);
}

// ------------------------------------------------------- writes and errors

export { LocalRefusal, mintKey };

/**
 * What became of a write.
 * - `unknown`: no answer reached the page (or a gateway gave up), so the write
 *   may or may not have landed; repeating it with the same key is safe.
 * - `invalid`: the API refused fields; the field errors say which.
 * - `local`: nothing was sent.
 * - `refused`: the API answered with a refusal.
 */
export type Outcome = "unknown" | "invalid" | "local" | "refused";

export function outcomeOf(error: unknown): Outcome {
  if (error instanceof LocalRefusal) return "local";
  if (outcomeUnknown(error) || (error instanceof ApiError && error.code === "upstream_timeout")) return "unknown";
  if (error instanceof ApiError && error.fieldErrors.length > 0) return "invalid";
  return "refused";
}

/** Each API field error placed beside the form field it is about, or listed when no field claims it. */
export function placeProblems<F extends string>(
  error: unknown,
  place: (path: string) => F | undefined,
): { fields: Partial<Record<F, string>>; unmatched: string[] } {
  const fields: Partial<Record<F, string>> = {};
  const unmatched: string[] = [];
  const list = error instanceof ApiError ? error.fieldErrors : [];
  for (const e of list) {
    const field = place(e.field);
    if (field === undefined) unmatched.push(`${e.field}: ${e.message}`);
    else fields[field] = fields[field] ? `${fields[field]} ${e.message}` : e.message;
  }
  return { fields, unmatched };
}

/**
 * One key per intent. `take()` mints it at the first submit and returns the
 * same key until `settle()`, so a double click, a lost answer and a resend all
 * carry one key and make one object.
 */
export function useIntentKey(kind: string) {
  const held = useRef<string | null>(null);
  return {
    take: (): string => (held.current ??= mintKey(kind)),
    settle: (): void => {
      held.current = null;
    },
  };
}

// The words that open a private-key PEM, in any case and spacing, or a PEM
// BEGIN line whose label joins the words with a dash or underscore (the old
// console's `carriesKeyMaterial`, ui/lifecycle.js).
const KEY_MARKER = /private\s+key|-{3,}\s*begin[^\n]{0,40}?private[\s_-]*key/i;

export function carriesKeyMaterial(text: unknown): boolean {
  return typeof text === "string" && KEY_MARKER.test(text);
}

/** What a field that is stored and shown back says when private-key text is typed into it. */
export const KEY_IN_PLAIN_FIELD =
  "This looks like a private key. This field is stored and shown to everyone who can read this namespace, so this page never takes a private key here.";

// ------------------------------------------------------------- the probe

/** How old an observation may be before it is labelled stale: twice the controller's 315 s re-probe cadence. */
export const PROBE_FRESH_SECONDS = 630;
/** Clock skew tolerated before an observation in the future is called stale. */
export const PROBE_SKEW_SECONDS = 60;

/**
 * The `status.reason` values the resolver writes when it refuses a saved
 * connection before any probe Job exists (weirkeeper `PROBE_CONDITION_REASONS`).
 * A check would fail to render a plan for any of these, so Test connection is
 * not offered.
 */
export const CONNECTION_REFUSAL_REASONS: readonly string[] = [
  "ConnectionConfigInvalid",
  "ConnectionReferenceInvalid",
  "ConnectionFieldUnsupported",
  "CredentialNotRenderable",
  "PlainWithoutTls",
];

/** Refusals of the probe itself: nothing was dialled, so the verdict is Refused. */
const PROBE_REFUSAL_REASONS: readonly string[] = [...CONNECTION_REFUSAL_REASONS, "CredentialBindingMismatch", "PodCreationForbidden"];
export const PROBE_RUNNING_REASON = "ProbeRunning";

/**
 * FX-46: the probe's own verdict when the bootstrap address answered and the
 * brokers the cluster advertises did not. It is this probe's reading, never a
 * later probe's reason beside an earlier one.
 */
export const ADVERTISED_BROKERS_UNREACHABLE_WORDS =
  "Not reachable: the bootstrap address answered, but the brokers this cluster advertises cannot be reached from the cluster. Check advertised.listeners for this listener, or use the bootstrap address of a listener meant for this network.";

/** A gloss beside the controller's own reason, never a replacement for it. */
export const REFUSAL_GLOSS: Record<string, string> = {
  ConnectionConfigInvalid:
    "the saved settings contradict each other (plaintext with TLS on, a CA with TLS off, or two CA sources), so the controller refused to dial rather than dial something else",
  ConnectionReferenceInvalid:
    "a reference on this object is not a legal Kubernetes name or data key, so nothing could be resolved in this namespace",
  ConnectionFieldUnsupported: "this object sets a field this controller does not implement; it is refused rather than resolved without that field",
  CredentialNotRenderable:
    "the credential this connection needs cannot be rendered (a missing SASL username or Secret reference), so no run could present an identity",
  PlainWithoutTls: "SASL/PLAIN sends the password itself, so it is refused without TLS",
  CredentialBindingMismatch: "the credential Secret is not bound to this connection, so the probe refused to present it",
  PodCreationForbidden: "the probe pod was refused at creation (a quota or an admission policy), so nothing was dialled",
  ProbeStale: "the last reading is too old to vouch for; the next probe that answers replaces it",
  ProbeOutputUnreadable: "the probe's output could not be read, so nothing about the cluster is known either way",
  AdvertisedBrokersUnreachable: ADVERTISED_BROKERS_UNREACHABLE_WORDS,
};

/** A later probe's reason beside an earlier reading (an older controller left `reachable` alone). */
export const LATER_PROBE_GLOSS: Record<string, string> = {
  NoExitCode: "the latest probe Job ended without an exit code, so it recorded no reading",
  ProbeOutputUnreadable: "the latest probe's output could not be read",
};
const VERDICT_OWN_REASONS = ["Reachable", "ProbeReportedUnreachable", "AdvertisedBrokersUnreachable"];

export type ProbeVerdict = "reachable" | "unreachable" | "refused" | "probing" | "unknown" | "never";

/** The probe's words. A probe is never a readiness verdict, so none of these says "ready". */
export const PROBE_WORDS: Record<ProbeVerdict, string> = {
  reachable: "Reachable",
  unreachable: "Not reachable",
  refused: "Refused",
  probing: "Probing",
  unknown: "Unknown",
  never: "Never probed",
};

export const PROBE_SENTENCE =
  "The connection probe is a pod the controller runs on its own cadence: it dialled these brokers once and recorded what happened. It is not a readiness verdict and not a promise about right now. An observation older than 10 minutes is labelled stale, and a connection the controller refused to dial is labelled with its own reason.";

export interface ProbeState {
  verdict: ProbeVerdict;
  reason: string;
  observedAt: string;
  observed: boolean;
  ageSeconds: number | null;
  stale: boolean;
  clusterId: string;
  /** The newest probe's own reason when it differs from the reading beside it, with its gloss. */
  laterReason: string;
  laterGloss: string;
}

export function probeState(r: Reachability | undefined, now = Date.now(), freshSeconds = PROBE_FRESH_SECONDS): ProbeState {
  const reason = r?.reason ?? "";
  const observedAt = r?.observedAt ?? "";
  let verdict: ProbeVerdict = "never";
  if (r?.state === "reachable") verdict = "reachable";
  else if (r?.state === "unreachable") verdict = "unreachable";
  else if (PROBE_REFUSAL_REASONS.includes(reason)) verdict = "refused";
  else if (reason === PROBE_RUNNING_REASON) verdict = "probing";
  else if (reason.length > 0) verdict = "unknown";
  const parsed = observedAt ? Date.parse(observedAt) : NaN;
  const ageMs = Number.isNaN(parsed) ? null : now - parsed;
  // Staleness is about an observation, so it needs one; a probe in flight has none yet.
  const stale = verdict !== "probing" && ageMs !== null && (ageMs > freshSeconds * 1000 || ageMs < -PROBE_SKEW_SECONDS * 1000);
  const later =
    (verdict === "reachable" || verdict === "unreachable") &&
    reason.length > 0 &&
    !VERDICT_OWN_REASONS.includes(reason) &&
    reason !== PROBE_RUNNING_REASON;
  return {
    verdict,
    reason,
    observedAt,
    observed: ageMs !== null,
    ageSeconds: ageMs === null ? null : Math.floor(ageMs / 1000),
    stale,
    clusterId: r?.clusterId ?? "",
    laterReason: later ? reason : "",
    laterGloss: later
      ? `${LATER_PROBE_GLOSS[reason] ?? "the latest probe recorded this reason"}; the ${verdict === "reachable" ? "reachable" : "not reachable"} reading is from an earlier probe`
      : "",
  };
}

// ------------------------------------------- what the endpoint is usable for

/**
 * FX-46: a Test connection's three advisory capability rows (PROD-01.2), read by
 * id from `checks` and `warnings` (the API puts an advisory `notReady` row in
 * `warnings`). They never change the check's verdict.
 */
export const USABILITY_ROWS = [
  { id: "connection.engineProtocol", label: "Usable as a backup source" },
  { id: "target.engineProtocol", label: "Usable as a restore target" },
  { id: "connection.groupTypes", label: "Consumer group positions can be recorded" },
] as const;

export type Usable = "yes" | "no" | "undetermined";

export interface UsabilityLine {
  id: string;
  label: string;
  state: Usable;
  /** The row's own message; shown beside a no or a not determined. */
  message: string;
}

/**
 * The three lines and one summary sentence from the two engine rows, or null when
 * the check carries none of the rows (an older controller asked for none).
 */
export function usability(p: Pick<Preflight, "checks" | "warnings">): { lines: UsabilityLine[]; summary: string } | null {
  const rows = [...p.checks, ...p.warnings];
  const found = USABILITY_ROWS.map((u) => ({ u, row: rows.find((r) => r.id === u.id) }));
  if (found.every((f) => f.row === undefined)) return null;
  const lines: UsabilityLine[] = found.map(({ u, row }) => ({
    id: u.id,
    label: u.label,
    state: row?.state === "ready" ? "yes" : row?.state === "notReady" ? "no" : "undetermined",
    message: row ? (row.message ?? "") : "this check recorded no answer for it",
  }));
  const [source, target] = [lines[0]!.state, lines[1]!.state];
  let summary = "What it can be used for was not determined";
  if (source === "yes" && target === "yes") summary = "Usable as a source and as a target";
  else if (source === "yes" && target === "no") summary = "Usable as a source, not as a target";
  else if (source === "no" && target === "yes") summary = "Usable as a target, not as a source";
  else if (source === "no" && target === "no") summary = "Usable neither as a source nor as a target";
  return { lines, summary };
}

// ------------------------------------------------------------ the create form

export const AUTH_MODES = ["plaintext", "scramSha512", "scramSha256", "plain", "mtls"] as const satisfies readonly ConnectionAuthMode[];
export const SASL_MODES: readonly ConnectionAuthMode[] = ["scramSha512", "scramSha256", "plain"];

export const AUTH_WORDS: Record<ConnectionAuthMode, string> = {
  plaintext: "No authentication",
  scramSha512: "SCRAM-SHA-512",
  scramSha256: "SCRAM-SHA-256",
  plain: "SASL/PLAIN",
  mtls: "TLS client certificate",
};

export interface ConnectionValues {
  role: "source" | "target";
  servers: string;
  mode: ConnectionAuthMode;
  username: string;
  /** Write-only: sent once, never kept, shown or stored. */
  password: string;
  /** Write-only (mtls). */
  certificatePem: string;
  /** Write-only (mtls). */
  privateKeyPem: string;
  tls: boolean;
  caName: string;
  caKey: string;
}

export type ConnectionField = keyof ConnectionValues;

/** The fields that carry a credential: read for the one request, then cleared. */
export const CONNECTION_SECRET_FIELDS = ["password", "certificatePem", "privateKeyPem"] as const satisfies readonly ConnectionField[];

export const CONNECTION_DEFAULTS: ConnectionValues = {
  role: "source",
  servers: "",
  mode: "scramSha512",
  username: "",
  password: "",
  certificatePem: "",
  privateKeyPem: "",
  tls: true,
  caName: "",
  caKey: "",
};

const HOST_PORT = /^(\[[0-9A-Fa-f:.]+\]|[^\s:[\]/@]+):([0-9]{1,5})$/;
const DNS_SUBDOMAIN = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$/;
const DATA_KEY = /^[-._a-zA-Z0-9]+$/;

export function isObjectName(v: string): boolean {
  return v.length > 0 && v.length <= 253 && DNS_SUBDOMAIN.test(v);
}

export function isDataKey(v: string): boolean {
  return v.length > 0 && v.length <= 253 && DATA_KEY.test(v);
}

export function serverList(text: string): string[] {
  return text
    .split(/[\s,]+/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

/**
 * The page's own checks, by field. A convenience that puts the message beside
 * the field: the API's schema and the controller are the gate. Messages name
 * the rule and never echo a value.
 */
export function connectionProblems(v: ConnectionValues): Partial<Record<ConnectionField, string>> {
  const p: Partial<Record<ConnectionField, string>> = {};
  const servers = serverList(v.servers);
  if (servers.length === 0) p.servers = "Name at least one bootstrap server, as host:port.";
  else if (servers.length > 16) p.servers = "At most 16 bootstrap servers.";
  else {
    const bad = servers.filter((s) => {
      const m = HOST_PORT.exec(s);
      return m === null || Number(m[2]) < 1 || Number(m[2]) > 65535;
    });
    if (bad.length > 0) p.servers = `Not host:port: ${bad.join(", ")}.`;
  }
  const sasl = SASL_MODES.includes(v.mode);
  if (sasl && v.username.trim().length === 0) p.username = `${AUTH_WORDS[v.mode]} needs the SASL username.`;
  if (sasl && v.password.length === 0) p.password = `${AUTH_WORDS[v.mode]} needs the SASL password, entered once here.`;
  if (v.mode === "mtls") {
    if (v.certificatePem.trim().length === 0) p.certificatePem = "A TLS client certificate needs the certificate, pasted once here as PEM.";
    if (v.privateKeyPem.trim().length === 0) p.privateKeyPem = "A TLS client certificate needs its unencrypted private key, pasted once here as PEM.";
  }
  if (v.mode === "plaintext" && v.tls) {
    p.tls =
      "No authentication with TLS on (TLS without SASL) is not supported by saved connections and is refused rather than dialled without TLS. Choose SCRAM over TLS, a TLS client certificate, or turn TLS off for a plaintext listener.";
  }
  if (v.mode === "plain" && !v.tls) p.tls = "PlainWithoutTls: SASL/PLAIN sends the password itself, so it is accepted only with TLS on.";
  if (v.mode === "mtls" && !v.tls) p.tls = "A TLS client certificate requires TLS on.";
  const caName = v.caName.trim();
  const caKey = v.caKey.trim();
  if (caName.length > 0 || caKey.length > 0) {
    if (!v.tls) p.caName = "A CA verifies a TLS transport, so naming one requires TLS on.";
    else if (!isObjectName(caName)) p.caName = "The name of a ConfigMap in this namespace that holds the PEM CA certificate(s).";
    if (!isDataKey(caKey)) p.caKey = "The data key inside it: letters, digits, '-', '_' and '.'.";
  }
  for (const f of ["servers", "username", "caName", "caKey"] as const) {
    if (carriesKeyMaterial(v[f])) p[f] = KEY_IN_PLAIN_FIELD;
  }
  return p;
}

/** The request a filled-in form makes. The password is sent exactly as typed. */
export function connectionBody(v: ConnectionValues): CreateConnectionRequest {
  const auth: CreateConnectionRequest["auth"] = { mode: v.mode, tls: v.tls };
  if (SASL_MODES.includes(v.mode)) {
    auth.username = v.username.trim();
    auth.credential = { password: v.password };
  } else if (v.mode === "mtls") {
    auth.credential = { certificatePem: v.certificatePem, privateKeyPem: v.privateKeyPem };
  }
  if (v.caName.trim().length > 0) {
    auth.tlsCa = { configMapKeyRef: { name: v.caName.trim(), key: v.caKey.trim() } };
  }
  return { role: v.role, bootstrapServers: serverList(v.servers), auth };
}

/** The form field an API field path is about, so a 422 lands beside it. */
export function connectionField(path: string, mode: ConnectionAuthMode): ConnectionField | undefined {
  if (/^bootstrapServers(\[\d+\])?$/.test(path)) return "servers";
  if (path === "role") return "role";
  if (path === "auth.mode") return "mode";
  if (path === "auth.username") return "username";
  if (path === "auth.tls") return "tls";
  if (path === "auth.credential.password") return "password";
  if (path === "auth.credential.certificatePem") return "certificatePem";
  if (path === "auth.credential.privateKeyPem") return "privateKeyPem";
  if (path === "auth.credential" || path === "auth.credentialRef") return mode === "mtls" ? "certificatePem" : "password";
  if (path === "auth.tlsCa.configMapKeyRef.key") return "caKey";
  if (path.startsWith("auth.tlsCa")) return "caName";
  return undefined;
}

/** Drops the credential from a request once it has been sent, so nothing that holds the request keeps it. */
export function forgetConnectionCredential(body: CreateConnectionRequest): void {
  delete body.auth.credential;
}

// ----------------------------------------------------------------- reads

/** While a probe is in flight the page re-reads every 4 s, for at most the probe Job's two-minute deadline. */
const PROBE_RE_READ_MS = 4000;
const PROBE_RE_READS = 30;

function probing(c: Connection | undefined): boolean {
  return c?.reachability.reason === PROBE_RUNNING_REASON;
}

/** The namespace's connections (the Overview's list, one cache), re-read while a probe is in flight. */
export function useConnectionList(ns: string) {
  return useConnections(ns, (items, reads) => (items.some(probing) && reads < PROBE_RE_READS ? PROBE_RE_READ_MS : false));
}

export function useConnection(ns: string, name: string, enabled = true) {
  const readable = useCan(ns, "connectionsRead");
  return useQuery({
    queryKey: keys.item("connections", ns, name),
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/connections/{name}", { params: { path: { ns, name } }, signal }))).item,
    enabled: enabled && readable,
    refetchInterval: (q) => (probing(q.state.data) && q.state.dataUpdateCount < PROBE_RE_READS ? PROBE_RE_READ_MS : false),
    retry: (count, error) => !(error instanceof ApiError && (error.status === 404 || error.status === 403)) && count < 2,
  });
}

// ---------------------------------------------------------------- writes

export function useCreateConnection(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ key, body }: { key: string; body: CreateConnectionRequest }) => {
      try {
        return await call(() =>
          api.POST("/api/v1/namespaces/{ns}/connections", { params: { path: { ns }, header: createHeaders(key) }, body }),
        );
      } finally {
        // The request is serialized before it is sent; the copy the mutation
        // cache holds as its variables loses the credential here.
        forgetConnectionCredential(body);
      }
    },
    onSuccess: () => client.invalidateQueries({ queryKey: keys.list("connections", ns) }),
  });
}

// ------------------------------------------------------- following a check

/** The largest preflight `timeoutSeconds` (600) plus the Job's start margin (90) and a settle grace (30). */
export const PREFLIGHT_FOLLOW_MS = (600 + 90 + 30) * 1000;
/** The largest discovery `timeoutSeconds` (300) plus the same margins. */
export const DISCOVERY_FOLLOW_MS = (300 + 90 + 30) * 1000;

/** The wait before read `n`: 2 s for the first 20 s, then growing by half to at most 10 s. */
export function followGap(n: number): number {
  if (n < 10) return 2000;
  return Math.min(10_000, Math.round(2000 * Math.pow(1.5, n - 9)));
}

export type Stopped = "" | "deadline" | "unreadable";

export const CHECKING_SENTENCE =
  "The check has not finished. Whether its result applies is decided when it has one; this page reads it again until then.";
export const CHECK_UNFINISHED_SENTENCE =
  "The check did not finish in the longest time a check may take, so this page stopped reading it. It was not cancelled, and nothing it records later is shown here. Run the check again to start a new one.";
export const CHECK_UNREADABLE_SENTENCE =
  "This page could not read the check again, so it stopped following it. The check was not cancelled. Run the check again to start a new one.";

/**
 * Whether the page has stopped following an unfinished check, and why: the
 * longest time a check may take has passed (on this page's clock, from when it
 * began to follow), or a read of it failed.
 */
export function stoppedFor(terminal: boolean | undefined, deadlinePassed: boolean, readFailed: boolean): Stopped {
  if (terminal !== false) return "";
  if (readFailed) return "unreadable";
  return deadlinePassed ? "deadline" : "";
}

/** Re-renders once when `deadline` (a `performance.now()` value) passes. */
function useDeadline(deadline: number | null): boolean {
  const [passed, setPassed] = useState(false);
  useEffect(() => {
    setPassed(false);
    if (deadline === null) return undefined;
    const t = window.setTimeout(() => setPassed(true), Math.max(0, deadline - performance.now()));
    return () => window.clearTimeout(t);
  }, [deadline]);
  return passed;
}

/** When this page began to follow `id`, on its own monotonic clock. */
function useFollowingSince(id: string | null): number {
  const since = useRef<{ id: string | null; at: number }>({ id: null, at: 0 });
  if (since.current.id !== id) since.current = { id, at: performance.now() };
  return since.current.at;
}

/** Reads one preflight and follows it until a read answers terminal, the deadline passes, or a read fails. */
export function useFollowedPreflight(ns: string, id: string | null) {
  const at = useFollowingSince(id);
  const readable = useCan(ns, "preflight");
  const query = useQuery({
    queryKey: ["preflights", ns, id],
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/preflights/{id}", { params: { path: { ns, id: id ?? "" } }, signal }))).item,
    enabled: id !== null && readable,
    retry: 3,
    refetchInterval: (q) =>
      !q.state.data || q.state.data.terminal || q.state.status === "error" || performance.now() - at > PREFLIGHT_FOLLOW_MS
        ? false
        : followGap(q.state.dataUpdateCount),
  });
  const terminal = query.data?.terminal;
  const passed = useDeadline(id !== null && terminal === false ? at + PREFLIGHT_FOLLOW_MS : null);
  return { query, stopped: stoppedFor(terminal, passed, query.isRefetchError) };
}

/** "Test connection": one `sourceConnection` Preflight per click. */
export function useStartConnectionCheck(ns: string, name: string) {
  return useMutation({
    mutationFn: async (key: string) =>
      (
        await call(() =>
          api.POST("/api/v1/namespaces/{ns}/preflights", {
            params: { path: { ns }, header: createHeaders(key) },
            body: { operation: "sourceConnection", sourceConnection: { connectionRef: name } },
          }),
        )
      ).item,
  });
}

// ---------------------------------------------------------- topic discovery

export interface DiscoverySlots {
  latestAttempt: TopicDiscovery | null;
  lastSuccessful: TopicDiscovery | null;
}

/** `?latest=true`: the two slots, so a failed attempt never hides the last inventory and the reverse. */
export function useDiscoverySlots(ns: string, name: string, enabled = true) {
  const [followFrom, setFollowFrom] = useState<string | null>(null);
  const at = useFollowingSince(followFrom);
  const readable = useCan(ns, "topicDiscovery");
  const query = useQuery({
    queryKey: ["topic-discoveries", ns, "slots", name],
    queryFn: async ({ signal }): Promise<DiscoverySlots> => {
      const answer = (await call(() =>
        api.GET("/api/v1/namespaces/{ns}/connections/{name}/topic-discoveries", {
          params: { path: { ns, name }, query: { latest: true } },
          signal,
        }),
      )) as unknown as Partial<DiscoverySlots>;
      return { latestAttempt: answer.latestAttempt ?? null, lastSuccessful: answer.lastSuccessful ?? null };
    },
    enabled: enabled && readable,
    refetchInterval: (q) => {
      const latest = q.state.data?.latestAttempt;
      if (!latest || latest.terminal || q.state.status === "error") return false;
      return performance.now() - at > DISCOVERY_FOLLOW_MS ? false : followGap(q.state.dataUpdateCount);
    },
  });
  // The follow of an unfinished attempt is measured from when this page first saw it.
  const latest = query.data?.latestAttempt ?? null;
  const unfinished = latest !== null && !latest.terminal ? latest.id : null;
  useEffect(() => {
    if (unfinished !== null) setFollowFrom(unfinished);
  }, [unfinished]);
  const passed = useDeadline(unfinished !== null && followFrom === unfinished ? at + DISCOVERY_FOLLOW_MS : null);
  return { query, stopped: stoppedFor(latest?.terminal, passed, query.isRefetchError) };
}

export interface DiscoveryRequest {
  includeInternal: boolean;
  expectedTopics: string;
}

export function discoveryBody(v: DiscoveryRequest): S["CreateTopicDiscoveryRequest"] {
  const body: S["CreateTopicDiscoveryRequest"] = {};
  if (v.includeInternal) body.includeInternal = true;
  const expected = v.expectedTopics
    .split(/[\s,]+/)
    .map((t) => t.trim())
    .filter((t) => t.length > 0);
  if (expected.length > 0) body.expectedTopics = expected;
  return body;
}

export function useStartDiscovery(ns: string, name: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ key, request }: { key: string; request: DiscoveryRequest }) =>
      call(() =>
        api.POST("/api/v1/namespaces/{ns}/connections/{name}/topic-discoveries", {
          params: { path: { ns, name }, header: createHeaders(key) },
          body: discoveryBody(request),
        }),
      ),
    // Every discovery read of the namespace: these slots and a schedule form's topic offer.
    onSuccess: () => client.invalidateQueries({ queryKey: ["topic-discoveries", ns] }),
  });
}

export function useCancelDiscovery(ns: string, name: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) =>
      call(() =>
        api.POST("/api/v1/namespaces/{ns}/topic-discoveries/{id}:cancel", {
          params: { path: { ns, id }, header: { Origin: window.location.origin } },
        }),
      ),
    onSettled: () => client.invalidateQueries({ queryKey: ["topic-discoveries", ns, "slots", name] }),
  });
}

export interface TopicFilters {
  q: string;
  prefix: string;
  internal: "exclude" | "include";
  errored: "include" | "exclude" | "only";
}

export const TOPIC_PAGE_LIMIT = 25;

type TopicQuery = NonNullable<
  operations["listDiscoveredTopics"]["parameters"]["query"]
>;

export function useTopicPage(ns: string, id: string | null, filters: TopicFilters, cursor: string | null) {
  const readable = useCan(ns, "topicDiscovery");
  return useQuery({
    queryKey: ["topic-discoveries", ns, "topics", id, filters, cursor],
    queryFn: async ({ signal }) => {
      const query: TopicQuery = { limit: TOPIC_PAGE_LIMIT };
      if (filters.q.trim()) query.q = filters.q.trim();
      if (filters.prefix.trim()) query.prefix = filters.prefix.trim();
      if (filters.internal !== "exclude") query.internal = filters.internal;
      if (filters.errored !== "include") query.errored = filters.errored;
      if (cursor) query.cursor = cursor;
      return call(() =>
        api.GET("/api/v1/namespaces/{ns}/topic-discoveries/{id}/topics", {
          params: { path: { ns, id: id ?? "" }, query },
          signal,
        }),
      );
    },
    enabled: id !== null && readable,
    placeholderData: (previous) => previous,
  });
}

// ------------------------------------------------- choosing a saved connection

/**
 * A saved connection as a form holds it. The UID decides; the name is for
 * reading and for refusals. A connection deleted and recreated under the same
 * name is a different set of brokers with a different credential.
 */
export interface Selection {
  uid: string;
  name: string;
}

export type Resolved =
  | { state: "none" }
  | { state: "selected"; connection: Connection; pinned: boolean; renamedFrom: string }
  | { state: "recreated"; uid: string; name: string; recreatedUid: string }
  | { state: "missing"; uid: string; name: string };

/** Every saved connection, by name. No role filter, ever: the role travels with each option and only decides the default. */
export function savedConnections(list: readonly Connection[]): Connection[] {
  return [...list].filter((c) => c.name.length > 0).sort((a, b) => a.name.localeCompare(b.name));
}

/** The connection a fresh form starts on: the first with the preferred role, else the first. A default, not a decision. */
export function preferredConnection(list: readonly Connection[], prefer?: string): Connection | null {
  const all = savedConnections(list);
  return all.find((c) => c.role === prefer) ?? all[0] ?? null;
}

/**
 * What a held selection resolves to. A name-only selection (one written
 * before UIDs were held) resolves and is pinned to the UID that answered it;
 * a UID that is gone is refused, never moved onto whatever now has the name.
 */
export function resolveSelection(list: readonly Connection[], selection: Partial<Selection> | null | undefined): Resolved {
  const uid = selection?.uid?.trim() ?? "";
  const name = selection?.name?.trim() ?? "";
  if (!uid && !name) return { state: "none" };
  const all = savedConnections(list);
  const found = all.find((c) => (uid ? c.uid === uid : c.name === name));
  if (!found) {
    const impostor = uid && name ? all.find((c) => c.name === name) : undefined;
    return impostor ? { state: "recreated", uid, name, recreatedUid: impostor.uid } : { state: "missing", uid, name };
  }
  return { state: "selected", connection: found, pinned: !uid, renamedFrom: uid && name && name !== found.name ? name : "" };
}
