// The restore plan document: its bytes, their sha256 and the two names minted from
// them. A port of ui/plan.js, byte for byte: the plan hash is what an approver signs,
// so the console that replaces ui/ must emit the same bytes for the same fields.
//
// The grammar is the runner's restore.yaml (logweir_core::spec::RestoreSpec,
// docs/mvp/03-spec.md section 6.1). The controller writes Restore.spec.planBytes
// verbatim into the runner's plan ConfigMap and the runner parses it, so an invented
// shape would pass every approval check and fail only inside the runner pod. The
// goldens under console/fixtures/plan*.golden.yaml are deserialised into RestoreSpec
// by crates/logweir/tests/console_plan.rs; plan.test.ts holds this module to the same files.
//
// Nothing here parses the document it emits, reads a clock or makes a request:
// crypto.subtle is the only platform API it touches.

export const TIME_BASIS_PRODUCER_TIME = "producerTime";
export const COVERAGE_COMPLETE = "complete";
/** `target.teardown: {ttl_seconds}`'s bounds (`CloneTtlOutOfRange`). */
export const CLONE_TTL_SECONDS = { min: 300, max: 2_592_000 } as const;

/** Why a clone's lifetime is refused, or null (null itself: delete when the run ends). */
export function cloneTtlError(seconds: number | null): string | null {
  if (seconds === null) return null;
  if (!Number.isInteger(seconds) || seconds < CLONE_TTL_SECONDS.min || seconds > CLONE_TTL_SECONDS.max) return "A clone is kept 5 minutes to 30 days (CloneTtlOutOfRange).";
  return null;
}

/** A topic's declarative owner as the plan states it (`logweir_core::topic_configuration::DeclaredOwner`). */
export interface DeclaredOwner {
  topic: string;
  kind: "strimzi" | "external";
  reference: string;
}

/** `OWNER_KINDS` and `MAX_OWNER_REFERENCE_CHARS` in logweir_core::topic_configuration. */
export const OWNER_KINDS: readonly string[] = ["strimzi", "external"];
export const MAX_OWNER_REFERENCE_CHARS = 256;

/** The topic configuration choice (theme A, PROD-05.2), as the wizard holds it. */
export interface TopicConfigurationValue {
  /** `null`: no block; the topics are created with the target's defaults, as always. */
  transition: "afterVerification" | "none" | null;
  /**
   * DUI-17: the requester's statement about the topics' declarative owners. "none" signs
   * `owners: []`; "named" signs the owners in `ownerLines`; null states nothing, which
   * the runner accepts only when the point's backup looked for owners itself.
   */
  owners: "none" | "named" | null;
  /** "topic kind reference", one owner per line, for "named". */
  ownerLines: string;
}

export const NO_TOPIC_CONFIGURATION: TopicConfigurationValue = { transition: null, owners: null, ownerLines: "" };

/** Why one owner is refused, by the runner's own rules (`refuse_declared`), or null. */
export function ownerProblem(o: DeclaredOwner, topics: readonly string[] | null): string | null {
  if (topics && !topics.includes(o.topic)) return `${o.topic} is not one of the topics this restore restores`;
  if (!OWNER_KINDS.includes(o.kind)) return `${o.topic}: the kind is strimzi or external, not ${o.kind}`;
  const chars = [...o.reference];
  if (chars.length === 0 || chars.length > MAX_OWNER_REFERENCE_CHARS) return `${o.topic}: where it is defined is 1 to ${MAX_OWNER_REFERENCE_CHARS} characters`;
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(o.reference)) return `${o.topic}: where it is defined holds a control character`;
  return null;
}

/** "topic kind reference" lines to owners, with the first problem found (a topic named twice is one). */
export function parseOwnerLines(text: string, topics: readonly string[] | null): { owners: DeclaredOwner[]; problem: string | null } {
  const owners: DeclaredOwner[] = [];
  let problem: string | null = null;
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line) continue;
    const m = /^(\S+)\s+(\S+)\s+(.+)$/.exec(line);
    if (!m) {
      problem ??= `"${line}": write the topic, strimzi or external, and where it is defined`;
      continue;
    }
    const owner = { topic: m[1]!, kind: m[2]! as DeclaredOwner["kind"], reference: m[3]!.trim() };
    const why = ownerProblem(owner, topics) ?? (owners.some((o) => o.topic === owner.topic) ? `${owner.topic} is named twice` : null);
    if (why) problem ??= why;
    else owners.push(owner);
  }
  return { owners, problem };
}

/** The plan's `target.topic_configuration` value, or `null` for none. */
export function topicConfigurationPlan(
  v: TopicConfigurationValue,
  topics: readonly string[] | null = null,
): { transition: "afterVerification" | "none"; owners?: DeclaredOwner[] } | null {
  if (!v.transition) return null;
  if (v.owners === "none") return { transition: v.transition, owners: [] };
  if (v.owners === "named") return { transition: v.transition, owners: parseOwnerLines(v.ownerLines, topics).owners };
  return { transition: v.transition };
}

/** `target.default_replication_factor` is an i16 in logweir_core::spec. */
export const MAX_REPLICATION_FACTOR = 32767;
/** The runner's TargetMode and the Restore CRD's enum, byte for byte. */
export const TARGET_MODES = ["scratch", "newTopic"] as const;
export type TargetMode = (typeof TARGET_MODES)[number];

export interface PlanStorage {
  bucket: string;
  prefix?: string;
  region?: string;
  endpoint?: string;
  pathStyle?: boolean;
  allowHttp?: boolean;
}

export interface PlanPointBinding {
  pointId: string;
  receiptKey: string;
  receiptSha256: string;
  manifestSha256: string;
}

export interface PlanAuth {
  mode: string;
  username?: string;
  tls?: boolean;
}

/** What the wizard accumulates: the argument to renderPlanBytes (console/fixtures/plan-fields.json). */
export interface PlanFields {
  name?: string;
  backupSetRef: string;
  topics: string[];
  pointInTime: string;
  timeBasis?: string | null;
  /** PROD-00.3h: `restore.rate_limit_bytes_per_sec`, for the whole restore; absent is no limit. */
  rateLimitBytesPerSec?: number | null;
  point?: PlanPointBinding;
  source: PlanStorage;
  target: {
    bootstrapServers: string[];
    auth?: PlanAuth;
    mode: string;
    topicPrefix: string;
    topicMappingPrefix: string;
    markerTopic: string;
    replicationFactor: number;
    teardown: string;
    /** A clone (theme A, PROD-11.1a): `teardown: {ttl_seconds: N}`, scratch mode only. */
    teardownTtlSeconds?: number | null;
    /** Theme A, PROD-05.2: `topic_configuration`, created from the point's model. */
    topicConfiguration?: { transition: "afterVerification" | "none"; owners?: DeclaredOwner[] } | null;
    originalName?: boolean;
    originalNameNoOwner?: boolean;
  };
  sample: {
    windowStart: string;
    windowEnd: string;
    recordsPerPartition: number;
    anchor: string;
    coverage?: string | null;
    completeMaxRecords?: number | null;
  };
  objectives?: { rtoSeconds?: number | null; rpoSeconds?: number | null; passRate?: number | null };
  evidence: PlanStorage;
}

/** The prefix `logweir_core::spec::default_topic_prefix` produces for an instant:
 *  `restore-<YYYYmmddTHHMMSSZ>-`. For Kafka topic names only. */
export function defaultTopicPrefix(pointInTime: string | number): string {
  const at = new Date(pointInTime);
  if (Number.isNaN(at.getTime())) {
    throw new TypeError(`defaultTopicPrefix(): ${String(pointInTime)} is not an instant this page can read`);
  }
  const iso = at.toISOString();
  return `restore-${iso.slice(0, 4)}${iso.slice(5, 7)}${iso.slice(8, 10)}T${iso.slice(11, 13)}${iso.slice(14, 16)}${iso.slice(17, 19)}Z-`;
}

function timeBasisOf(value: unknown): string | null {
  if (value === undefined || value === null || value === "") return null;
  if (value !== TIME_BASIS_PRODUCER_TIME) {
    throw new TypeError(`timeBasis must be "${TIME_BASIS_PRODUCER_TIME}" or absent, not ${String(value)}`);
  }
  return value;
}

function coverageOf(value: unknown): string | null {
  if (value === undefined || value === null || value === "" || value === "sampled") return null;
  if (value !== COVERAGE_COMPLETE) {
    throw new TypeError(`sample.coverage must be "${COVERAGE_COMPLETE}", "sampled" or absent, not ${String(value)}`);
  }
  return value;
}

/** The restore plan document as a UTF-8 string, in the runner's restore.yaml grammar.
 *  Byte for byte what goes into Restore.spec.planBytes; never re-serialised anywhere. */
export function renderPlanBytes(fields: PlanFields): string {
  const f = (fields ?? {}) as Partial<PlanFields>;
  const source = (f.source ?? {}) as Partial<PlanStorage>;
  const target = (f.target ?? {}) as Partial<PlanFields["target"]>;
  const sample = (f.sample ?? {}) as Partial<PlanFields["sample"]>;
  const objectives = f.objectives ?? {};
  const evidence = (f.evidence ?? {}) as Partial<PlanStorage>;

  const out: string[] = [];
  out.push("# The restore plan document. Its grammar is the runner's restore document:");
  out.push("# logweir_core::spec::RestoreSpec, stated once in docs/mvp/03-spec.md section 6.1,");
  out.push("# \"The restore plan document has ONE grammar, and it is the runner's");
  out.push("# restore.yaml (amendment 4)\". These bytes are what Restore.spec.planBytes");
  out.push("# carries, what the controller writes into the runner's plan ConfigMap verbatim,");
  out.push("# and what logweir restore run --spec parses. Rendered by the Logweir UI; the");
  out.push("# sha256 of exactly these bytes is the plan hash an approval binds.");
  if (typeof f.name === "string" && f.name.length > 0) out.push(`name: ${quote(f.name)}`);

  out.push("source:");
  pushStorage(out, "  ", source);
  out.push(`  backup: ${quote(needed(f.backupSetRef, "backupSetRef"))}`);
  pushList(out, "  ", "topics", requireList(f.topics, "topics"));
  pushPointBinding(out, f.point);

  out.push("target:");
  pushList(out, "  ", "bootstrap_servers", requireList(target.bootstrapServers, "target.bootstrapServers"));
  const auth = target.auth;
  if (auth && auth.mode !== "plaintext") {
    // The runner's AuthSpec arms. SASL carries a username; mTLS none (the identity is
    // the client certificate). PLAIN and mTLS without TLS are refused as the runner does.
    const sasl = ["scramSha512", "scramSha256", "plain"];
    if (!sasl.includes(auth.mode) && auth.mode !== "mtls") {
      throw new TypeError("target.auth.mode must be plaintext, scramSha512, scramSha256, plain or mtls");
    }
    if ((auth.mode === "plain" || auth.mode === "mtls") && auth.tls !== true) {
      throw new TypeError(
        auth.mode === "plain"
          ? "PlainWithoutTls: target.auth.mode plain is accepted only with tls: true"
          : "target.auth.mode mtls requires tls: true",
      );
    }
    out.push("  auth:");
    out.push(`    mode: ${quote(auth.mode)}`);
    if (auth.mode !== "mtls") out.push(`    username: ${quote(needed(auth.username, "target.auth.username"))}`);
    out.push(`    tls: ${auth.tls === true ? "true" : "false"}`);
  }
  out.push(`  mode: ${quote(requireMode(target.mode))}`);
  out.push("  topic_naming:");
  if (target.originalName === true) {
    // Under the ORIGINAL names: the empty prefix beside the block that opts in, newTopic
    // only, with the owner statement (`owners: []`) and complete verification. The
    // runner refuses each missing piece at phase 0, so such a plan is never rendered.
    if (target.mode !== "newTopic") {
      throw new TypeError(
        "OriginalNameNotNewTopic: a restore under the original topic names is a newTopic " +
          "restore; scratch never restores under the original names",
      );
    }
    if (target.originalNameNoOwner !== true) {
      throw new TypeError(
        "OriginalNameOwnerNotChecked: a restore under the original topic names states that no " +
          "declarative owner manages the names (target.originalNameNoOwner)",
      );
    }
    if (coverageOf(sample.coverage) !== COVERAGE_COMPLETE) {
      throw new TypeError(
        "OriginalNameNeedsCompleteCoverage: a restore under the original topic names requires " +
          `complete verification (sample.coverage "${COVERAGE_COMPLETE}"), where every ` +
          "restored record is compared with the archive; a sampled check is refused",
      );
    }
    out.push('    prefix: ""');
    out.push("    original_name:");
    out.push("      owners: []");
  } else {
    out.push(`    prefix: ${quote(needed(target.topicPrefix, "target.topicPrefix"))}`);
  }
  // Unread in newTopic mode and still required by the grammar (no serde default).
  out.push(`  topic_mapping_prefix: ${quote(needed(target.topicMappingPrefix, "target.topicMappingPrefix"))}`);
  out.push(`  marker_topic: ${quote(needed(target.markerTopic, "target.markerTopic"))}`);
  out.push(`  default_replication_factor: ${replicationFactor(target.replicationFactor, "target.replicationFactor")}`);
  const ttl = target.teardownTtlSeconds;
  if (ttl !== undefined && ttl !== null) {
    // The clone form, which every runner before theme A refuses at parse rather than keep
    // the topics forever (decisions/PROD-04.2-cutover.md section 5).
    if (target.mode !== "scratch") throw new TypeError("CloneTtlNeedsScratch: a clone's lifetime is set only in scratch mode");
    const n = Number(integer(ttl, "target.teardownTtlSeconds"));
    if (n < CLONE_TTL_SECONDS.min || n > CLONE_TTL_SECONDS.max) {
      throw new TypeError(`CloneTtlOutOfRange: a clone is kept ${CLONE_TTL_SECONDS.min} to ${CLONE_TTL_SECONDS.max} seconds, not ${n}`);
    }
    out.push("  teardown:");
    out.push(`    ttl_seconds: ${n}`);
  } else {
    out.push(`  teardown: ${quote(needed(target.teardown, "target.teardown"))}`);
  }
  const tc = target.topicConfiguration;
  if (tc) {
    if (tc.transition !== "afterVerification" && tc.transition !== "none") {
      throw new TypeError(`target.topicConfiguration.transition must be afterVerification or none, not ${String(tc.transition)}`);
    }
    out.push("  topic_configuration:");
    out.push(`    transition: ${quote(tc.transition)}`);
    if (tc.owners && tc.owners.length === 0) out.push("    owners: []");
    else if (tc.owners) {
      // Each owner by the runner's own rules: a plan it would refuse is never rendered.
      const seen = new Set<string>();
      out.push("    owners:");
      for (const o of tc.owners) {
        const why = ownerProblem(o, f.topics ?? null) ?? (seen.has(o.topic) ? `${o.topic} is named twice` : null);
        if (why) throw new TypeError(`TopicOwnerDeclarationRefused: ${why}`);
        seen.add(o.topic);
        out.push(`      - topic: ${quote(o.topic)}`);
        out.push(`        kind: ${quote(o.kind)}`);
        out.push(`        reference: ${quote(o.reference)}`);
      }
    }
  }

  // The recovery point: the window's END only. Its start is the archive's own floor.
  out.push("restore:");
  out.push(`  point_in_time: ${quote(needed(f.pointInTime, "pointInTime"))}`);
  const timeBasis = timeBasisOf(f.timeBasis);
  if (timeBasis !== null) out.push(`  time_basis: ${quote(timeBasis)}`);
  const rate = f.rateLimitBytesPerSec;
  if (rate !== undefined && rate !== null) {
    const n = integer(rate, "rateLimitBytesPerSec");
    if (Number(n) < 1) throw new TypeError(`rateLimitBytesPerSec must be at least 1, not ${String(rate)}`);
    out.push(`  rate_limit_bytes_per_sec: ${n}`);
  }

  out.push("sample:");
  out.push(`  window_start: ${quote(needed(sample.windowStart, "sample.windowStart"))}`);
  out.push(`  window_end: ${quote(needed(sample.windowEnd, "sample.windowEnd"))}`);
  out.push(`  records_per_partition: ${integer(sample.recordsPerPartition, "sample.recordsPerPartition")}`);
  out.push(`  anchor: ${quote(needed(sample.anchor, "sample.anchor"))}`);
  const coverage = coverageOf(sample.coverage);
  const bound = sample.completeMaxRecords;
  const bounded = bound !== undefined && bound !== null && (bound as unknown) !== "";
  if (coverage === COVERAGE_COMPLETE) {
    out.push(`  coverage: ${quote(coverage)}`);
    if (bounded) {
      const n = integer(bound, "sample.completeMaxRecords");
      if (Number(n) < 1) throw new TypeError(`sample.completeMaxRecords must be at least 1, not ${String(bound)}`);
      out.push(`  complete_max_records: ${n}`);
    }
  } else if (bounded) {
    throw new TypeError(
      "sample.completeMaxRecords bounds a complete verification and is set only with " +
        `sample.coverage "${COVERAGE_COMPLETE}"`,
    );
  }

  // Each objective is optional in the grammar: absent is "not asked for".
  const stated: string[] = [];
  if (objectives.rtoSeconds !== undefined && objectives.rtoSeconds !== null) {
    stated.push(`  rto_seconds: ${integer(objectives.rtoSeconds, "objectives.rtoSeconds")}`);
  }
  if (objectives.rpoSeconds !== undefined && objectives.rpoSeconds !== null) {
    stated.push(`  rpo_seconds: ${integer(objectives.rpoSeconds, "objectives.rpoSeconds")}`);
  }
  if (objectives.passRate !== undefined && objectives.passRate !== null) {
    stated.push(`  pass_rate: ${decimal(objectives.passRate, "objectives.passRate")}`);
  }
  if (stated.length === 0) {
    out.push("objectives: {}");
  } else {
    out.push("objectives:");
    out.push(...stated);
  }

  out.push("evidence:");
  pushStorageFields(out, "  ", evidence);
  out.push("notifications:");
  out.push("  webhooks: []");
  return `${out.join("\n")}\n`;
}

/** Whether this page can hash a plan at all. crypto.subtle is absent outside a secure
 *  context (plain transport through an ingress, a LAN address). */
export function planHashAvailable(): boolean {
  return Boolean(globalThis.crypto?.subtle);
}

export const INSECURE_CONTEXT_SENTENCE =
  "This page must be served from a secure context: loopback (127.0.0.1 or localhost) over plain " +
  "transport, or any TLS origin. SubtleCrypto is unavailable here and the plan hash cannot be computed.";

async function sha256Hex(bytes: string): Promise<string> {
  if (typeof bytes !== "string") {
    throw new TypeError(`the plan bytes are a string; got ${bytes === null ? "null" : typeof bytes}`);
  }
  if (!planHashAvailable()) throw new Error(INSECURE_CONTEXT_SENTENCE);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(bytes));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

/** `sha256:<lowercase hex>` over exactly these bytes, the form the CLI prints. */
export async function planHash(bytes: string): Promise<string> {
  return `sha256:${await sha256Hex(bytes)}`;
}

/** The two names minted from the plan bytes before either object exists:
 *  `restore-<8 hex>` and `approval-<8 hex>` (spec section 7). */
export async function mintNames(bytes: string): Promise<{ restoreName: string; approvalName: string }> {
  const suffix = (await sha256Hex(bytes)).slice(0, 8);
  return { restoreName: `restore-${suffix}`, approvalName: `approval-${suffix}` };
}

export interface PreparedPlan {
  readonly bytes: string;
  readonly hash: string;
  readonly restoreName: string;
  readonly approvalName: string;
}

/** How many prepared documents are remembered; every field change makes a new one. */
export const PREPARED_KEPT = 16;
const prepared = new Map<string, PreparedPlan>();

/** THE ONE PREPARED PLAN per distinct document: the same frozen object for the same
 *  bytes, so the plan reviewed and the plan submitted are one object, not two equal ones. */
export async function preparePlanDocument(fields: PlanFields, options?: { bytes?: string }): Promise<PreparedPlan> {
  const bytes = typeof options?.bytes === "string" ? options.bytes : renderPlanBytes(fields);
  const already = prepared.get(bytes);
  if (already !== undefined) return already;
  const hex = await sha256Hex(bytes);
  const suffix = hex.slice(0, 8);
  const document: PreparedPlan = Object.freeze({
    bytes,
    hash: `sha256:${hex}`,
    restoreName: `restore-${suffix}`,
    approvalName: `approval-${suffix}`,
  });
  prepared.set(bytes, document);
  while (prepared.size > PREPARED_KEPT) {
    const oldest = prepared.keys().next();
    if (oldest.done) break;
    prepared.delete(oldest.value);
  }
  return document;
}

/** The prepared document for exactly these bytes, or null: the create sends only a
 *  hash that was on screen. */
export function preparedFor(bytes: string): PreparedPlan | null {
  return prepared.get(bytes) ?? null;
}

// A storage block as the runner's StorageUrl: `backend` first. Only s3 is reachable here.
function pushStorage(out: string[], indent: string, block: Partial<PlanStorage>) {
  out.push(`${indent}storage:`);
  pushStorageFields(out, `${indent}  `, block);
}

function pushStorageFields(out: string[], indent: string, block: Partial<PlanStorage>) {
  const b = block ?? {};
  out.push(`${indent}backend: ${quote("s3")}`);
  out.push(`${indent}bucket: ${quote(needed(b.bucket, "storage.bucket"))}`);
  out.push(`${indent}prefix: ${quote(b.prefix === undefined ? "" : b.prefix)}`);
  if (typeof b.region === "string" && b.region.length > 0) out.push(`${indent}region: ${quote(b.region)}`);
  if (typeof b.endpoint === "string" && b.endpoint.length > 0) out.push(`${indent}endpoint: ${quote(b.endpoint)}`);
  out.push(`${indent}path_style: ${b.pathStyle === true ? "true" : "false"}`);
  out.push(`${indent}allow_http: ${b.allowHttp === true ? "true" : "false"}`);
}

/** `source.point`: absent is the Backup-bound document byte for byte; present, it is
 *  whole or refused, in the shapes the runner checks (PointBindingMismatch). */
function pushPointBinding(out: string[], point: PlanPointBinding | undefined | null) {
  if (point === undefined || point === null) return;
  const id = needed(point.pointId, "point.pointId");
  if (!/^lwp1-[0-9a-f]{32}$/.test(id)) {
    throw new RangeError(
      `point.pointId is \`${id}\`, not \`lwp1-\` plus 32 lowercase hex characters; the runner refuses such a binding with PointBindingMismatch`,
    );
  }
  const receiptKey = needed(point.receiptKey, "point.receiptKey");
  if (receiptKey.trim().length === 0) throw new RangeError("point.receiptKey is blank; the runner refuses such a binding");
  for (const field of ["receiptSha256", "manifestSha256"] as const) {
    const value = needed(point[field], `point.${field}`);
    if (!/^sha256:[0-9a-f]{64}$/.test(value)) {
      throw new RangeError(
        `point.${field} is \`${value}\`, not \`sha256:\` plus 64 lowercase hex characters; the runner refuses such a binding with PointBindingMismatch`,
      );
    }
  }
  out.push("  point:");
  out.push(`    point_id: ${quote(id)}`);
  out.push(`    receipt_key: ${quote(receiptKey)}`);
  out.push(`    receipt_sha256: ${quote(point.receiptSha256)}`);
  out.push(`    manifest_sha256: ${quote(point.manifestSha256)}`);
}

function pushList(out: string[], indent: string, key: string, values: unknown[]) {
  out.push(`${indent}${key}:`);
  for (const value of values) out.push(`${indent}  - ${quote(value)}`);
}

/** THE ONE SCALAR WRITER: every string double-quoted, so a topic name with a colon, a
 *  `#` or a quote cannot change the document's shape. Also the console's one writer of a
 *  value into any YAML it shows (the catalog's TrustPolicy snippet). */
export function quote(value: unknown): string {
  let out = '"';
  for (const ch of String(value)) {
    const code = ch.codePointAt(0)!;
    if (ch === '"') out += '\\"';
    else if (ch === "\\") out += "\\\\";
    else if (ch === "\n") out += "\\n";
    else if (ch === "\r") out += "\\r";
    else if (ch === "\t") out += "\\t";
    else if (code < 0x20 || code === 0x7f) out += `\\u${code.toString(16).padStart(4, "0")}`;
    else out += ch;
  }
  return `${out}"`;
}

function integer(value: unknown, field: string): string {
  if (typeof value !== "number" || !Number.isFinite(value) || Math.floor(value) !== value) {
    throw new TypeError(`${field} must be a whole number; got ${String(value)}`);
  }
  return String(value);
}

function replicationFactor(value: unknown, field: string): string {
  const text = integer(value, field);
  if ((value as number) < 1 || (value as number) > MAX_REPLICATION_FACTOR) {
    throw new RangeError(`${field} must be a whole number from 1 to ${MAX_REPLICATION_FACTOR}; got ${text}`);
  }
  return text;
}

// Always with a decimal point, so `pass_rate: 1` never reads as an integer.
function decimal(value: unknown, field: string): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new TypeError(`${field} must be a number; got ${String(value)}`);
  }
  const text = String(value);
  return !text.includes(".") && !text.includes("e") ? `${text}.0` : text;
}

function needed(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${field} is required by the runner's grammar; a document without it does not parse`);
  }
  return value;
}

function requireList(value: unknown, field: string): unknown[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new TypeError(`${field} is required by the runner's grammar and must not be empty`);
  }
  return value;
}

function requireMode(value: unknown): string {
  if (!(TARGET_MODES as readonly unknown[]).includes(value)) {
    throw new RangeError(
      `target.mode is one of ${TARGET_MODES.join(", ")}; got ${String(value)}. These two strings are the runner's TargetMode and the CRD's enum, byte for byte.`,
    );
  }
  return value as string;
}
