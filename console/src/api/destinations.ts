// Destinations: the list, one destination, what uses it, the write-once
// create, the access rotation and the access test, over the typed client.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ApiError, createHeaders, unwrap } from "./client";
import { KEY_IN_PLAIN_FIELD, carriesKeyMaterial, isDataKey, isObjectName, unwrapWrite } from "./connections";
import type { components } from "./schema";

type S = components["schemas"];
export type Destination = S["Destination"];
export type DestinationSummary = S["DestinationSummary"];
export type DestinationUsage = S["DestinationUsageResponse"];
export type AccessGrantView = S["AccessGrantView"];
export type AccessView = S["AccessView"];
export type CreateDestinationRequest = S["CreateDestinationRequest"];
export type UpdateDestinationAccessRequest = S["UpdateDestinationAccessRequest"];
type AccessRequest = S["AccessRequest"];
type AccessGrantRequest = S["AccessGrantRequest"];

// ------------------------------------------------------------------ grants

/** The four grants, in the order the form and the detail list them. Only archiveWrite is required. */
export const GRANT_ROLES = ["archiveWrite", "archiveRead", "evidenceWrite", "evidenceRead"] as const;
export type GrantRole = (typeof GRANT_ROLES)[number];

export const ROLE_WORDS: Record<GrantRole, string> = {
  archiveWrite: "Archive write",
  archiveRead: "Archive read",
  evidenceWrite: "Evidence write",
  evidenceRead: "Evidence read",
};

/** What an absent grant means: `AccessRequest`'s own documented absences. */
export const ABSENT_GRANT_MEANING: Record<GrantRole, string> = {
  archiveWrite: "Required: every backup Job writes the archive with it.",
  archiveRead: "Absent means restores read the archive with the archive-write credential.",
  evidenceWrite: "Absent means evidence is written with the archive-write credential.",
  evidenceRead: "Absent means verification is not attempted, and says so rather than leaving a blank.",
};

/** What choosing absent on the rotation removes, for a grant the destination holds. */
export const ABSENT_REMOVES: Partial<Record<GrantRole, string>> = {
  archiveRead: "Absent removes this grant: restores then read the archive with the archive-write credential.",
  evidenceWrite: "Absent removes this grant: evidence is then written with the archive-write credential.",
  evidenceRead: "Absent removes this grant: evidence verification stops, and is recorded as NotAttempted, until a grant is set again.",
};

export type GrantSource =
  | "keep"
  | "absent"
  | "existing"
  | "new"
  | "workloadIdentity"
  | "controllerIdentity"
  | "archiveReadGrant";

export const SOURCE_WORDS: Record<GrantSource, string> = {
  keep: "Keep as it is",
  absent: "None (absent)",
  existing: "A Secret this destination already names",
  new: "Enter a new credential",
  workloadIdentity: "Workload identity (a ServiceAccount)",
  controllerIdentity: "The controller's own identity",
  archiveReadGrant: "The archive-read grant",
};

/** The evidenceRead-only sources, refused for the other three roles (as the API does). */
export const EVIDENCE_READ_ONLY: readonly GrantSource[] = ["controllerIdentity", "archiveReadGrant"];

/**
 * The sources a form offers a role. A new destination never names an existing
 * Secret (the API refuses it: `existing_credential_refused`); the rotation
 * starts every grant on `keep`, because it sends the complete block and a
 * grant left out is removed.
 */
export function sourcesFor(role: GrantRole, form: "create" | "rotate"): GrantSource[] {
  const base: GrantSource[] = form === "rotate" ? ["keep", "absent", "existing", "new", "workloadIdentity"] : ["absent", "new", "workloadIdentity"];
  const all = role === "evidenceRead" ? [...base, ...EVIDENCE_READ_ONLY] : base;
  return role === "archiveWrite" ? all.filter((s) => s !== "absent") : all;
}

export interface GrantValues {
  source: GrantSource;
  /** Write-only. */
  accessKeyId: string;
  /** Write-only. */
  secretAccessKey: string;
  serviceAccount: string;
  /** For `existing`: a Secret NAME. */
  secret: string;
}

export type Grants = Record<GrantRole, GrantValues>;

export function emptyGrant(source: GrantSource): GrantValues {
  return { source, accessKeyId: "", secretAccessKey: "", serviceAccount: "", secret: "" };
}

const ABSENT_MODES = ["inheritsArchiveWrite", "notConfigured"];

/** Whether the destination holds this grant itself (not absent). */
export function holdsGrant(view: AccessGrantView | undefined): boolean {
  return view !== undefined && !ABSENT_MODES.includes(view.mode);
}

const DEFAULT_SECRET_KEYS = ["access-key-id", "secret-access-key"];

/**
 * The grant a destination has now, as a request: what `keep` sends. `null`
 * keeps an absent grant absent. `undefined` when this page cannot spell it: a
 * Secret whose keys are not the API's defaults is published as a sorted list
 * that does not say which key holds the key id, and a guess could point the
 * grant at the wrong value.
 */
export function keptGrant(view: AccessGrantView | undefined): AccessGrantRequest | null | undefined {
  if (view === undefined) return undefined;
  if (!holdsGrant(view)) return null;
  if (view.mode === "controllerIdentity" || view.mode === "archiveReadGrant") return { mode: view.mode };
  if (view.mode === "workloadIdentity") {
    return view.serviceAccountName
      ? { mode: "workloadIdentity", workloadIdentity: { serviceAccountName: view.serviceAccountName } }
      : { mode: "workloadIdentity" };
  }
  if (view.mode !== "secretKeys" || !view.secretName) return undefined;
  const others = view.keys.filter((k) => !DEFAULT_SECRET_KEYS.includes(k));
  if (others.length > 1 || DEFAULT_SECRET_KEYS.some((k) => !view.keys.includes(k))) return undefined;
  return {
    mode: "secretKeys",
    secret: { existing: others.length === 1 ? { name: view.secretName, sessionTokenKey: others[0] } : { name: view.secretName } },
  };
}

/** Why `keep` cannot be sent for this grant. */
export function keepRefusal(view: AccessGrantView | undefined): string {
  if (view === undefined) return "Keep cannot be sent: this page read no current grant for it. Enter a new credential.";
  return `Keep cannot be sent: the API publishes this Secret's data keys (${view.keys.join(", ")}) without saying which holds the key id, so this page cannot name them again. Enter a new credential, or rotate it with kubectl.`;
}

/** One grant as the API's `AccessGrantRequest`, or `null` for absent (never `inheritsArchiveWrite` on a request). */
export function grantBody(g: GrantValues, role: GrantRole, current?: AccessView): AccessGrantRequest | null {
  switch (g.source) {
    case "absent":
      return null;
    case "keep":
      return keptGrant(current?.[role]) ?? null;
    case "workloadIdentity":
      return g.serviceAccount.trim()
        ? { mode: "workloadIdentity", workloadIdentity: { serviceAccountName: g.serviceAccount.trim() } }
        : { mode: "workloadIdentity" };
    case "controllerIdentity":
    case "archiveReadGrant":
      return { mode: g.source };
    case "existing":
      return { mode: "secretKeys", secret: { existing: { name: g.secret.trim() } } };
    case "new":
      // The only place a credential value enters a request; the request is
      // sent once and the value is dropped from it afterwards.
      return { mode: "secretKeys", secret: { new: { accessKeyId: g.accessKeyId, secretAccessKey: g.secretAccessKey } } };
  }
}

/** The complete four-grant block, for the create and the rotation alike (a grant left out of a rotation is removed). */
export function accessBody(grants: Grants, current?: AccessView): AccessRequest {
  const access: Partial<AccessRequest> = {};
  for (const role of GRANT_ROLES) {
    const grant = grantBody(grants[role], role, current);
    if (grant !== null) access[role] = grant;
  }
  return access as AccessRequest;
}

/** Drops every typed credential from a request once it has been sent. */
export function forgetAccessCredentials(access: Partial<AccessRequest>): void {
  for (const role of GRANT_ROLES) {
    const secret = access[role]?.secret;
    if (secret?.new) delete secret.new;
  }
}

type GrantField = `grants.${GrantRole}.${keyof GrantValues}`;

/** The grants' own checks, for the create and the rotation. */
export function grantProblems(grants: Grants, current?: AccessView, secretNames: readonly string[] = []): Partial<Record<GrantField, string>> {
  const p: Partial<Record<GrantField, string>> = {};
  for (const role of GRANT_ROLES) {
    const g = grants[role];
    if (g.source === "keep" && keptGrant(current?.[role]) === undefined) p[`grants.${role}.source`] = keepRefusal(current?.[role]);
    if (role === "archiveWrite" && g.source === "absent") p[`grants.${role}.source`] = "Archive write is required: it is the credential every backup Job writes the archive with.";
    if (EVIDENCE_READ_ONLY.includes(g.source) && role !== "evidenceRead") p[`grants.${role}.source`] = `${SOURCE_WORDS[g.source]} is an evidence-read answer only.`;
    if (g.source === "existing" && !secretNames.includes(g.secret.trim())) {
      p[`grants.${role}.secret`] = "Choose a Secret this destination already names. To change a credential, enter the new value.";
    }
    if (g.source === "new") {
      if (g.accessKeyId.length === 0) p[`grants.${role}.accessKeyId`] = "A new credential needs the access key id.";
      if (g.secretAccessKey.length === 0) p[`grants.${role}.secretAccessKey`] = "A new credential needs the secret access key.";
    }
    if (g.source === "workloadIdentity") {
      const sa = g.serviceAccount.trim();
      if (carriesKeyMaterial(sa)) p[`grants.${role}.serviceAccount`] = KEY_IN_PLAIN_FIELD;
      else if (sa.length > 0 && !isObjectName(sa)) p[`grants.${role}.serviceAccount`] = "A ServiceAccount name, or blank for logweir-runner.";
    }
  }
  return p;
}

// ------------------------------------------------------------ the create form

export interface DestinationValues {
  name: string;
  description: string;
  bucket: string;
  prefix: string;
  region: string;
  endpoint: string;
  addressing: "pathStyle" | "virtualHosted";
  security: "tls" | "insecureHttp";
  caName: string;
  caKey: string;
  writeProbe: "createOnlyMarker" | "disabled";
  isDefault: boolean;
  grants: Grants;
}

export type DestinationField = Exclude<keyof DestinationValues, "grants"> | GrantField;

export const DESTINATION_DEFAULTS: DestinationValues = {
  name: "",
  description: "",
  bucket: "",
  prefix: "",
  region: "",
  endpoint: "",
  addressing: "pathStyle",
  security: "tls",
  caName: "",
  caKey: "",
  writeProbe: "createOnlyMarker",
  isDefault: false,
  grants: {
    archiveWrite: emptyGrant("new"),
    archiveRead: emptyGrant("absent"),
    evidenceWrite: emptyGrant("absent"),
    evidenceRead: emptyGrant("absent"),
  },
};

// The schemes, as words a reader types, not addresses this page fetches.
const HTTPS = "https" + "://";
const HTTP = "http" + "://";

export const WRITE_ONLY_SENTENCE =
  "Typed once and written once. The value goes into a Secret this namespace owns and is never read back: no response, status, log line or audit record carries it, and this page does not keep it either. A refusal, a reload or leaving this form loses it, on purpose. What comes back is the Secret's name.";

export const DEFAULT_DESTINATION_SENTENCE =
  "At most one destination per namespace is the default, and new schedules start from it. Naming a second one is refused rather than silently moving the flag: clear it on the destination that holds it first.";

export const DEFAULT_ANNOTATION_RESIDUAL =
  "A default set by hand with only the logweir.dev/default-destination annotation carries no label, so the conflict check cannot see it; it still reads as the default everywhere else.";

export const WRITE_PROBE_SENTENCE =
  "Create-only marker lets an access test create one marker object under logweir/readiness/. It is never deleted, and nothing else is ever written by a test. A destination created with kubectl and no opinion gets disabled instead.";

export const NOT_JUDGED_SENTENCE =
  "No controller has recorded a verdict for this destination yet. That is what a new object looks like, and also what an installation whose controller predates destinations looks like. It is not a claim that the destination is wrong, nor that it works.";

export const DESTINATION_TEST_SENTENCE =
  "A test starts a readiness check: a real Job, with this destination's own credentials, against this destination's own endpoint. The verdict is that check's recorded result, read back from its status; this page decides nothing.";

/** The page's own checks over the create form. The API's `destination_invalid` and the CRD are the gate. */
export function destinationProblems(v: DestinationValues): Partial<Record<DestinationField, string>> {
  const p: Partial<Record<DestinationField, string>> = {};
  if (!isObjectName(v.name.trim())) {
    p.name = "A destination name is a Kubernetes object name: lowercase letters, digits, '-' and '.', starting and ending with a letter or digit.";
  }
  if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(v.bucket.trim())) {
    p.bucket = "An S3 bucket is 3 to 63 lowercase letters, digits, '.' and '-', starting and ending with a letter or digit.";
  }
  const prefix = v.prefix.trim();
  if (prefix.startsWith("/") || prefix.includes("..")) p.prefix = "A prefix is relative and has no parent-directory step.";
  else if (prefix === "logweir" || prefix.startsWith("logweir/")) {
    p.prefix = "logweir and everything under it is reserved for evidence; an archive there would write into the evidence tree.";
  }
  const region = v.region.trim();
  if (region.length > 0 && !/^[a-z0-9-]{1,32}$/.test(region)) p.region = "A region is lowercase letters, digits and '-'.";
  const endpoint = v.endpoint.trim();
  const origin = "An endpoint is an origin: a scheme, a host and an optional port, with no path, query or credentials.";
  if (endpoint.length > 0) {
    if (endpoint.startsWith(HTTPS)) {
      if (v.security === "insecureHttp") p.endpoint = `The endpoint is https and the transport says plaintext. Choose TLS, or type an ${HTTP} endpoint: this page will not change one to suit the other.`;
    } else if (endpoint.startsWith(HTTP)) {
      if (v.security === "tls") p.endpoint = "The endpoint is plaintext http and the transport says TLS. Plaintext is allowed only when it is chosen explicitly.";
    } else p.endpoint = origin;
    if (/[?#@]/.test(endpoint) || endpoint.replace(/^https?:\/\//, "").includes("/")) p.endpoint = origin;
  } else if (v.security === "insecureHttp") {
    p.endpoint = `Plaintext needs an explicit ${HTTP} endpoint. There is no way to reach AWS S3 in the clear.`;
  }
  if (endpoint.length > 0 && v.addressing === "virtualHosted" && p.endpoint === undefined) {
    p.addressing =
      "Virtual-hosted addressing with a custom endpoint is refused: this engine addresses a custom endpoint path-style, so the location would not be the one its runs use. Choose path-style, or clear the endpoint for AWS S3.";
  }
  const caName = v.caName.trim();
  if (caName.length > 0) {
    if (v.security !== "tls") p.caName = "A CA bundle verifies a TLS transport; a plaintext destination has nothing to verify.";
    else if (!isObjectName(caName)) p.caName = "The name of a ConfigMap in this namespace holding the PEM bundle.";
    if (v.caKey.trim().length > 0 && !isDataKey(v.caKey.trim())) p.caKey = "A ConfigMap data key is letters, digits, '-', '_' and '.'.";
  }
  for (const f of ["name", "description", "bucket", "prefix", "region", "endpoint", "caName", "caKey"] as const) {
    if (carriesKeyMaterial(v[f])) p[f] = KEY_IN_PLAIN_FIELD;
  }
  return { ...p, ...grantProblems(v.grants) };
}

/** The `CreateDestinationRequest` a filled-in form makes. Addressing and transport are read apart: neither implies the other. */
export function destinationBody(v: DestinationValues): CreateDestinationRequest {
  const storage: CreateDestinationRequest["storage"] = {
    provider: "s3",
    bucket: v.bucket.trim(),
    addressing: v.addressing === "virtualHosted" ? "virtualHosted" : "pathStyle",
  };
  if (v.prefix.trim()) storage.prefix = v.prefix.trim();
  if (v.region.trim()) storage.region = v.region.trim();
  if (v.endpoint.trim()) storage.endpoint = v.endpoint.trim();
  const transport: CreateDestinationRequest["transport"] = { security: v.security === "insecureHttp" ? "insecureHttp" : "tls" };
  if (v.caName.trim() && transport.security === "tls") {
    transport.caBundle = v.caKey.trim() ? { configMapName: v.caName.trim(), key: v.caKey.trim() } : { configMapName: v.caName.trim() };
  }
  const body: CreateDestinationRequest = {
    name: v.name.trim(),
    storage,
    transport,
    access: accessBody(v.grants),
    readiness: { writeProbe: v.writeProbe },
  };
  if (v.description.trim()) body.description = v.description.trim();
  if (v.isDefault) body.default = true;
  return body;
}

/** The form field an API field path is about. */
export function destinationField(path: string): DestinationField | undefined {
  const direct: Record<string, DestinationField> = {
    name: "name",
    description: "description",
    "storage.bucket": "bucket",
    "storage.prefix": "prefix",
    "storage.region": "region",
    "storage.endpoint": "endpoint",
    "storage.addressing": "addressing",
    "transport.security": "security",
    "transport.caBundle": "caName",
    "transport.caBundle.configMapName": "caName",
    "transport.caBundle.key": "caKey",
    "readiness.writeProbe": "writeProbe",
    default: "isDefault",
  };
  if (direct[path]) return direct[path];
  const m = /^access\.(archiveWrite|archiveRead|evidenceWrite|evidenceRead)(?:\.(.*))?$/.exec(path);
  if (!m) return undefined;
  const role = m[1] as GrantRole;
  const rest = m[2] ?? "";
  if (rest.startsWith("secret.new.accessKeyId")) return `grants.${role}.accessKeyId`;
  if (rest.startsWith("secret.new.secretAccessKey")) return `grants.${role}.secretAccessKey`;
  if (rest.startsWith("secret.existing")) return `grants.${role}.secret`;
  if (rest.startsWith("workloadIdentity")) return `grants.${role}.serviceAccount`;
  return `grants.${role}.source`;
}

// ------------------------------------------------------------- the rotation

export interface RotateValues {
  grants: Grants;
  caChange: "keep" | "set" | "clear";
  caName: string;
  caKey: string;
}

export function rotateDefaults(): RotateValues {
  return {
    grants: {
      archiveWrite: emptyGrant("keep"),
      archiveRead: emptyGrant("keep"),
      evidenceWrite: emptyGrant("keep"),
      evidenceRead: emptyGrant("keep"),
    },
    caChange: "keep",
    caName: "",
    caKey: "",
  };
}

/** The Secret names a destination's grants already carry: the only ones `existing` may name on a rotation. */
export function namedSecrets(access: AccessView | undefined): string[] {
  const names = GRANT_ROLES.map((r) => access?.[r]?.secretName).filter((n): n is string => typeof n === "string" && n.length > 0);
  return [...new Set(names)].sort();
}

export function rotationProblems(v: RotateValues, d: Pick<Destination, "access" | "transport">): Partial<Record<"caName" | "caKey" | GrantField, string>> {
  const p: Partial<Record<"caName" | "caKey" | GrantField, string>> = grantProblems(v.grants, d.access, namedSecrets(d.access));
  if (v.caChange === "set") {
    if (d.transport.security !== "tls") p.caName = "A CA bundle verifies a TLS transport; this destination is plaintext.";
    else if (!isObjectName(v.caName.trim())) p.caName = "The name of a ConfigMap in this namespace holding the PEM bundle.";
    if (v.caKey.trim() && !isDataKey(v.caKey.trim())) p.caKey = "A ConfigMap data key is letters, digits, '-', '_' and '.'.";
    for (const f of ["caName", "caKey"] as const) if (carriesKeyMaterial(v[f])) p[f] = KEY_IN_PLAIN_FIELD;
  }
  return p;
}

/** The `UpdateDestinationAccessRequest`: the generation this page read, the complete access block, and the CA change. */
export function rotationBody(v: RotateValues, d: Pick<Destination, "access" | "generation">): UpdateDestinationAccessRequest {
  const body: UpdateDestinationAccessRequest = { expectedGeneration: d.generation, access: accessBody(v.grants, d.access) };
  if (v.caChange === "set" && v.caName.trim()) {
    body.transport = { caBundle: v.caKey.trim() ? { configMapName: v.caName.trim(), key: v.caKey.trim() } : { configMapName: v.caName.trim() } };
  } else if (v.caChange === "clear") {
    // Present with no caBundle clears it; absent leaves it alone.
    body.transport = {};
  }
  return body;
}

export function rotationField(path: string): "caName" | "caKey" | GrantField | undefined {
  if (path === "transport.caBundle.key") return "caKey";
  if (path.startsWith("transport")) return "caName";
  const f = destinationField(path);
  return f && f.startsWith("grants.") ? (f as GrantField) : undefined;
}

// ----------------------------------------------------------------- reads

const LIST_LIMIT = 200;

export function useDestinationList(ns: string, enabled = true) {
  return useQuery({
    queryKey: ["destinations", ns, "list"],
    queryFn: async ({ signal }) => {
      const page = unwrap(
        await api.GET("/api/v1/namespaces/{ns}/destinations", { params: { path: { ns }, query: { limit: LIST_LIMIT } }, signal }),
      );
      return { items: page.items, more: Boolean(page.page.nextCursor) };
    },
    enabled,
  });
}

export function useDestination(ns: string, name: string, enabled = true) {
  return useQuery({
    queryKey: ["destinations", ns, "item", name],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/destinations/{name}", { params: { path: { ns, name } }, signal })).item,
    enabled,
    retry: (count, error) => !(error instanceof ApiError && (error.status === 404 || error.status === 403)) && count < 2,
  });
}

/** What names this destination. Its failure is not the page's: the detail still renders. */
export function useDestinationUsage(ns: string, name: string, enabled = true) {
  return useQuery({
    queryKey: ["destinations", ns, "usage", name],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/destinations/{name}/usage", { params: { path: { ns, name } }, signal })),
    enabled,
    retry: false,
  });
}

// ---------------------------------------------------------------- writes

export function useCreateDestination(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ key, body }: { key: string; body: CreateDestinationRequest }) => {
      try {
        return unwrapWrite(
          await api.POST("/api/v1/namespaces/{ns}/destinations", { params: { path: { ns }, header: createHeaders(key) }, body }),
        );
      } finally {
        forgetAccessCredentials(body.access);
      }
    },
    onSuccess: () => client.invalidateQueries({ queryKey: ["destinations", ns] }),
  });
}

/**
 * The rotation carries no `Idempotency-Key`: its replay guard is
 * `expectedGeneration`, so a second send after the first landed is a 412,
 * which is the right answer (the object moved).
 */
export function useRotateAccess(ns: string, name: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body: UpdateDestinationAccessRequest) => {
      try {
        return unwrapWrite(
          await api.POST("/api/v1/namespaces/{ns}/destinations/{name}:update-access", {
            params: { path: { ns, name }, header: { Origin: window.location.origin } },
            body,
          }),
        ).item;
      } finally {
        forgetAccessCredentials(body.access);
      }
    },
    // A test's applicability is recomputed on read; the grants it was about just changed.
    onSettled: () => Promise.all([client.invalidateQueries({ queryKey: ["destinations", ns] }), client.invalidateQueries({ queryKey: ["preflights", ns] })]),
  });
}

/** "Test access": one DestinationAccess Preflight per click; no roles means every configured role. */
export function useTestDestination(ns: string, name: string) {
  return useMutation({
    mutationFn: async ({ key, roles }: { key: string; roles: GrantRole[] }) =>
      unwrapWrite(
        await api.POST("/api/v1/namespaces/{ns}/destinations/{name}:test", {
          params: { path: { ns, name }, header: createHeaders(key) },
          body: roles.length > 0 ? { roles } : {},
        }),
      ).item,
  });
}
