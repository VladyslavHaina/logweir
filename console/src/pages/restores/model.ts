// The wizard's logic, pure: from the chosen point, the values typed and what the API
// said about the target, the plan fields (and so the plan bytes), the mapping and every
// refusal made before anything is sent, and the create request. A port of
// ui/pages/restore-wizard.js's state rules, so the same inputs make the same plan.
import type { components } from "../../api/schema";
import {
  COVERAGE_COMPLETE,
  MAX_REPLICATION_FACTOR,
  cloneTtlError,
  topicConfigurationPlan,
  type TopicConfigurationValue,
  TIME_BASIS_PRODUCER_TIME,
  defaultTopicPrefix,
  type PlanFields,
  type PreparedPlan,
} from "./plan";
import {
  EVIDENCE_PREFIX,
  bucketOf,
  destinationStore,
  epochMs,
  evidenceStoreOf,
  pointBindingOf,
  prefixOf,
  restorableWindow,
  rfc3339,
  windowMessage,
  type Destination,
  type RecoveryPoint,
} from "./points";


type S = components["schemas"];
export type Connection = S["Connection"];
export type TopicDiscovery = S["TopicDiscovery"];
export type CreateRestoreRequest = S["CreateRestoreRequest"];

/** Everything the operator types or ticks, as react-hook-form holds it. */
export interface WizardValues {
  pointKey: string;
  /** The chosen subset of the point's frozen topics. */
  topics: string[];
  /** A catalog point publishes no frozen list: the topics are named here. */
  topicsText: string;
  timeMode: "end" | "earlier";
  /** The instant typed for an earlier moment (RFC 3339, UTC). */
  pointInTime: string;
  timeBasis: boolean;
  coverage: "complete" | "sampled";
  completeMaxRecords: string;
  target: string;
  naming: "prefix" | "original";
  /** As typed; empty means the default for the point in time. */
  prefix: string;
  noOwner: boolean;
  scratch: boolean;
  /** A scratch restore kept as a clone for this many seconds; null deletes it when the run ends. */
  cloneTtl: number | null;
  /** The topic configuration choice (theme A, PROD-05.2). */
  topicConfiguration: TopicConfigurationValue;
  /** As typed; empty means the default. */
  replication: string;
  evidenceDestination: string;
  // An inline (legacy) archive's access, typed: never a credential, only a Secret NAME.
  archiveSecret: string;
  storeEndpoint: string;
  storeRegion: string;
  storePathStyle: boolean;
  storeAllowHttp: boolean;
  evidenceBucket: string;
  ticket: string;
  typedNames: string;
}

/** What the wizard knows besides the values: read from the API, not typed. */
export interface WizardContext {
  point: RecoveryPoint;
  retryOf: string;
  /** The live saved destination the point was frozen against (checked at mount). */
  destination: Destination | null;
  /** The saved destination the evidence goes to, read in full. */
  evidence: Destination | null;
  connection: Connection | null;
  brokers: BrokerFact;
}

export const MARKER_TOPIC = "logweir.scratch";
export const RECORDS_PER_PARTITION = 25;
export const SAMPLE_ANCHOR = "head";
export const DEFAULT_DEADLINE_SECONDS = 3600;
/** `logweir_core::guard::MAX_TOPIC_NAME_CHARS`, the API's and the CRD's bound. */
export const MAX_TOPIC_NAME_CHARS = 249;

// -------------------------------------------------------------------- time

/** The point in time the plan names: the last instant the window restores (its
 *  exclusive end less 1 ms) or the instant typed. */
export function pointInTimeOf(p: RecoveryPoint, v: Pick<WizardValues, "timeMode" | "pointInTime">): string {
  if (v.timeMode === "earlier") return v.pointInTime;
  const w = restorableWindow(p.window);
  return w ? rfc3339(w.toMs) : "";
}

export function endInstant(p: RecoveryPoint): string {
  const w = restorableWindow(p.window);
  return w ? rfc3339(w.toMs) : "";
}

/** Refused before anything is sent when not an instant or outside the window the point
 *  discloses (both bounds inside it). Guard G-WIN in the runner remains the gate. */
export function pointInTimeProblem(p: RecoveryPoint, instant: string): string | null {
  const at = epochMs(instant);
  if (at === null) return "an RFC 3339 instant in UTC, such as 2026-09-07T14:05:00Z";
  const w = restorableWindow(p.window);
  if (!w || at < w.fromMs || at > w.toMs) {
    return `outside the coverage this recovery point discloses: ${w ? windowMessage(w) : "it discloses none"}, and both bounds are inside it`;
  }
  return null;
}

// ------------------------------------------------------------------ prefix

export const RETRY_TAIL_CHARS = 8;

/** A fresh-target retry's prefix: the point's default plus a tail of the failed run's
 *  name, so every target name, the plan hash and both minted names are new and the
 *  failed run's approval cannot authorise it. Deterministic; reads no clock. */
export function freshTargetPrefix(pointInTime: string, retryOf: string): string {
  if (epochMs(pointInTime) === null) return "";
  let tail = "";
  for (const c of retryOf.slice(-RETRY_TAIL_CHARS).toLowerCase()) {
    if ((c >= "a" && c <= "z") || (c >= "0" && c <= "9")) tail += c;
  }
  return `${defaultTopicPrefix(pointInTime)}retry-${tail}-`;
}

export function defaultPrefixFor(pointInTime: string, retryOf: string): string {
  if (epochMs(pointInTime) === null) return "";
  return retryOf ? freshTargetPrefix(pointInTime, retryOf) : defaultTopicPrefix(pointInTime);
}

/** The prefix the plan maps through: as typed, or the default when emptied. */
export function prefixOfValues(ctx: Pick<WizardContext, "point" | "retryOf">, v: WizardValues): string {
  return v.prefix || defaultPrefixFor(pointInTimeOf(ctx.point, v), ctx.retryOf);
}

/** Whether this plan restores under the ORIGINAL names: newTopic with the choice made. */
export function originalNameChosen(v: Pick<WizardValues, "naming" | "scratch">): boolean {
  return v.naming === "original" && !v.scratch;
}

// ------------------------------------------------------------------ topics

/** The point's frozen list, or the names typed for a catalog point. */
export function frozenTopicsOf(p: RecoveryPoint, v: Pick<WizardValues, "topicsText">): string[] {
  return p.topics ?? parseTopicList(v.topicsText);
}

/** Names separated by commas or whitespace, in the order typed. Duplicates are KEPT,
 *  so the mapping check refuses them by name. */
export function parseTopicList(text: string): string[] {
  return text.split(/[\s,]+/).filter(Boolean);
}

/** The subset, in the FROZEN LIST's order (a click order would change the plan hash
 *  for no change of meaning). A name the list does not hold is kept, to be refused. */
export function selectedTopics(p: RecoveryPoint, v: Pick<WizardValues, "topics" | "topicsText">): string[] {
  if (p.topics === null) return parseTopicList(v.topicsText);
  const wanted = new Set(v.topics);
  const frozen = new Set(p.topics);
  return [...p.topics.filter((t) => wanted.has(t)), ...v.topics.filter((t) => !frozen.has(t))];
}

/** The ONE mapping rule: concatenation. Nothing in the grammar renames a topic. */
export function mappedTopicName(prefix: string, topic: string): string {
  return prefix + topic;
}

/** The prefix the RUN maps through: `topic_naming.prefix` for newTopic, the scratch
 *  prefix for scratch (`logweir_core::spec::target_topic_prefix`). */
export function effectivePrefix(f: PlanFields): string {
  return f.target.mode === "scratch" ? f.target.topicMappingPrefix : f.target.originalName ? "" : f.target.topicPrefix;
}

export function topicMapping(f: PlanFields): { source: string; target: string }[] {
  const prefix = effectivePrefix(f);
  return f.topics.map((source) => ({ source, target: mappedTopicName(prefix, source) }));
}

/** A name a Kafka broker accepts: ASCII letters, digits, `.`, `_`, `-`, at most 249. */
export function isKafkaTopicName(name: string): boolean {
  return name.length > 0 && name.length <= MAX_TOPIC_NAME_CHARS && /^[A-Za-z0-9._-]+$/.test(name);
}

/** The five mapping refusals the product API makes (routes/restores.rs
 *  validate_topic_mapping), made first, naming the value to fix and the input it is about. */
export function mappingProblems(p: RecoveryPoint, v: WizardValues, f: PlanFields): { field: "topics" | "topicsText" | "prefix"; message: string } | null {
  const prefix = effectivePrefix(f);
  const listField = p.topics === null ? "topicsText" : "topics";
  const raw = p.topics === null ? parseTopicList(v.topicsText) : v.topics;
  const seen = new Set<string>();
  for (const t of raw) {
    if (seen.has(t)) {
      return { field: listField, message: `\`${t}\` and \`${t}\` both map to the target topic \`${mappedTopicName(prefix, t)}\`; one restore cannot write two source topics into one target` };
    }
    seen.add(t);
  }
  if (f.topics.length === 0) return { field: listField, message: "choose at least one topic to restore" };
  if (p.topics !== null) {
    const frozen = new Set(p.topics);
    const outside = f.topics.filter((t) => !frozen.has(t));
    if (outside.length > 0) {
      return { field: listField, message: `${outside.map((t) => `\`${t}\``).join(", ")} ${outside.length === 1 ? "is" : "are"} not in this recovery point (PlanTopicsNotInRecoveryPoint)` };
    }
  }
  if (!f.target.originalName && prefix === "") {
    return { field: "prefix", message: "the prefix every restored topic's name starts with: an empty prefix maps every topic onto itself" };
  }
  if (prefix !== "" && !/^[A-Za-z0-9._-]*$/.test(prefix)) {
    return { field: "prefix", message: `the prefix \`${prefix}\` may hold only letters, digits, '.', '_' and '-'` };
  }
  for (const row of topicMapping(f)) {
    if (!isKafkaTopicName(row.target)) {
      return {
        field: isKafkaTopicName(row.source) ? "prefix" : listField,
        message: `\`${row.source}\` would be restored as \`${row.target}\`, which a broker refuses: letters, digits, '.', '_' and '-' only, at most ${MAX_TOPIC_NAME_CHARS} characters`,
      };
    }
  }
  return null;
}

// ------------------------------------------------------- replication factor

export const GRAMMAR_REPLICATION_FACTOR = 1;
/** Apache Kafka's guidance tops out at 3; a broker count above it is capped for a default. */
export const DEFAULT_REPLICATION_CEILING = 3;

export interface BrokerFact {
  count: number | null;
  fresh: boolean;
  discovery: string;
  observedAt: string;
  why: string;
}

export const BROKERS_NOT_READ: BrokerFact = { count: null, fresh: false, discovery: "", observedAt: "", why: "the target's topic discoveries have not been read" };

/** The target's broker count from its newest successful topic discovery, or why none.
 *  A count past its freshness alone still sets the default and refuses nothing. */
export function targetBrokerFact(
  answer: { lastSuccessful?: TopicDiscovery | null } | { unread: string } | null,
  connection: Connection | null,
): BrokerFact {
  const name = connection?.name ?? "";
  if (!name) return { ...BROKERS_NOT_READ, why: "no target connection is chosen" };
  if (answer === null) return BROKERS_NOT_READ;
  if ("unread" in answer) return { ...BROKERS_NOT_READ, why: `the target's topic discoveries could not be read: ${answer.unread}` };
  const last = answer.lastSuccessful ?? null;
  if (!last) return { ...BROKERS_NOT_READ, why: `no topic discovery of ${name} has succeeded; run Discover topics on it` };
  const said = `the newest successful topic discovery of ${name} (${last.id})`;
  const base = { count: null, fresh: false, discovery: last.id, observedAt: last.observedAt ?? "" };
  const reasons = last.staleReasons ?? [];
  const expiredAlone = reasons.length === 1 && reasons[0] === "expired";
  if (last.stale && !expiredAlone) return { ...base, why: `${said} is stale (${reasons.join(", ")}), so its broker count is not used` };
  if (last.connection.uid && connection?.uid && last.connection.uid !== connection.uid) {
    return { ...base, why: `${said} was taken of another connection under that name (uid ${last.connection.uid}), so its broker count is not used` };
  }
  if (typeof last.brokerCount !== "number" || last.brokerCount < 1) return { ...base, why: `${said} recorded no broker count` };
  return { ...base, count: last.brokerCount, fresh: !last.stale, why: "" };
}

/** Each selected topic's SOURCE replication factor, as the bound catalog row records it. */
export function sourceFactors(p: RecoveryPoint, topics: string[]): (number | null)[] | null {
  const listed = p.catalog?.entry.topics;
  if (!listed || listed.length === 0) return null;
  const byName = new Map(listed.map((t) => [t.name, t]));
  return topics.map((t) => {
    const rf = byName.get(t)?.replicationFactor;
    return typeof rf === "number" && Number.isInteger(rf) && rf >= 1 ? rf : null;
  });
}

export type ReplicationBasis = "source" | "capped" | "brokers" | "ceiling" | "grammar" | "chosen";
export interface ReplicationChoice {
  value: number;
  basis: ReplicationBasis;
  source: number | null;
  brokers: number | null;
}

/** THE DEFAULT, from two facts: the largest selected source factor capped by the
 *  target's brokers; else the brokers capped at 3; else the grammar's 1, said as such. */
export function replicationDefault(factors: (number | null)[] | null, brokers: number | null): ReplicationChoice {
  const recorded = (factors ?? []).filter((n): n is number => typeof n === "number" && n >= 1 && n <= MAX_REPLICATION_FACTOR);
  const count = typeof brokers === "number" && brokers >= 1 ? brokers : null;
  if (recorded.length > 0) {
    const source = Math.max(...recorded);
    return count !== null && count < source ? { value: count, basis: "capped", source, brokers: count } : { value: source, basis: "source", source, brokers: count };
  }
  if (count !== null) {
    return count > DEFAULT_REPLICATION_CEILING
      ? { value: DEFAULT_REPLICATION_CEILING, basis: "ceiling", source: null, brokers: count }
      : { value: count, basis: "brokers", source: null, brokers: count };
  }
  return { value: GRAMMAR_REPLICATION_FACTOR, basis: "grammar", source: null, brokers: null };
}

/** The factor on the plan: the operator's when typed (kept as typed, so a non-number
 *  is refused rather than guessed), the default otherwise. */
export function replicationChoice(ctx: WizardContext, v: WizardValues): ReplicationChoice {
  const typed = v.replication.trim();
  if (typed) return { value: Number(typed), basis: "chosen", source: null, brokers: ctx.brokers.count };
  return replicationDefault(sourceFactors(ctx.point, selectedTopics(ctx.point, v)), ctx.brokers.count);
}

export function replicationBasisText(c: ReplicationChoice, brokers: BrokerFact): string {
  const n = (k: number) => `${k} ${k === 1 ? "broker" : "brokers"}`;
  const asOf = brokers.count !== null && !brokers.fresh && brokers.observedAt ? ` as of ${brokers.observedAt}` : "";
  switch (c.basis) {
    case "source":
      return `the source's, as the recovery catalog records it${c.brokers === null ? "; the target's broker count is not known, so it is not capped" : ""}`;
    case "capped":
      return `capped at the target's ${n(c.brokers!)}${asOf}; the source's is ${c.source}`;
    case "brokers":
      return `the target's ${n(c.brokers!)}${asOf}; the source's factor is not known for this point`;
    case "ceiling":
      return `at most ${DEFAULT_REPLICATION_CEILING} by default, of the target's ${n(c.brokers!)}${asOf}; the source's factor is not known for this point`;
    case "chosen":
      return `set by you; ${c.brokers === null ? "the target's broker count is not known" : `the target has ${n(c.brokers)}${asOf}`}`;
    default:
      return "the plan grammar's default: neither the source's factor nor the target's broker count is known";
  }
}

/** A factor no broker places, or one above a FRESH broker count (the readiness check's
 *  ReplicationFactorExceedsBrokers, refused here first). */
export function replicationProblem(c: ReplicationChoice, brokers: BrokerFact): string | null {
  if (!Number.isInteger(c.value) || c.value < 1 || c.value > MAX_REPLICATION_FACTOR) {
    return `a replication factor is a whole number from 1 to ${MAX_REPLICATION_FACTOR}`;
  }
  if (brokers.count !== null && brokers.fresh && c.value > brokers.count) {
    return `ReplicationFactorExceedsBrokers: the target's topic discovery ${brokers.discovery} read ${brokers.count} ${
      brokers.count === 1 ? "broker" : "brokers"
    }, and a factor of ${c.value} needs ${c.value}; set ${brokers.count} or fewer`;
  }
  return null;
}

/** Whether the factor may differ from the source's (its storage follows it). */
export function replicationMayDiffer(c: ReplicationChoice): boolean {
  return !(c.source !== null && c.value === c.source);
}

// ----------------------------------------------------------------- coverage

/** The record bound as typed: a whole number becomes one; anything else stays text, so
 *  the plan refuses to render and names the field rather than guessing a number. */
function boundOf(text: string): number | string | undefined {
  const t = text.trim();
  if (!t) return undefined;
  return /^[0-9]+$/.test(t) && Number.isSafeInteger(Number(t)) ? Number(t) : t;
}

/** Complete coverage: chosen, or required by a restore under the original names. */
export function completeChosen(v: WizardValues): boolean {
  return v.coverage === "complete" || originalNameChosen(v);
}

// ------------------------------------------------------------------- fields

function targetAuth(c: Connection | null): PlanFields["target"]["auth"] {
  const a = c?.auth;
  if (!a || a.mode === "plaintext") return undefined;
  return { mode: a.mode, username: a.username ?? undefined, tls: a.tls === true };
}

/** THE PLAN FIELDS, the argument to renderPlanBytes: the old wizard's initialState plus
 *  every edit it applies, from the same inputs. */
export function planFields(ctx: WizardContext, v: WizardValues): PlanFields {
  const p = ctx.point;
  const pit = pointInTimeOf(p, v);
  const prefix = prefixOfValues(ctx, v);
  const original = originalNameChosen(v);
  const saved = p.destinationName.length > 0;
  const store = saved
    ? (destinationStore(ctx.destination) ?? { bucket: "", prefix: "", region: "", endpoint: "", pathStyle: false, allowHttp: false })
    : {
        bucket: bucketOf(p.archiveUrl),
        prefix: prefixOf(p.archiveUrl),
        region: v.storeRegion,
        endpoint: v.storeEndpoint,
        pathStyle: v.storePathStyle,
        allowHttp: v.storeAllowHttp,
      };
  const evidence = saved
    ? (evidenceStoreOf(ctx.evidence) ?? { bucket: "", prefix: EVIDENCE_PREFIX })
    : { bucket: v.evidenceBucket, prefix: EVIDENCE_PREFIX, region: v.storeRegion, endpoint: v.storeEndpoint, pathStyle: v.storePathStyle, allowHttp: v.storeAllowHttp };
  const complete = completeChosen(v);
  const bound = complete ? boundOf(v.completeMaxRecords) : undefined;
  const choice = replicationChoice(ctx, v);
  const binding = pointBindingOf(p);
  return {
    backupSetRef: p.backupSetRef,
    topics: selectedTopics(p, v),
    ...(binding ? { point: binding } : {}),
    pointInTime: pit,
    timeBasis: v.timeBasis ? TIME_BASIS_PRODUCER_TIME : "",
    source: store,
    target: {
      bootstrapServers: ctx.connection?.bootstrapServers.slice() ?? [],
      auth: targetAuth(ctx.connection),
      mode: v.scratch ? "scratch" : "newTopic",
      // Both keys the same string unless restoring under the original names, where the
      // naming prefix is "" and the scratch prefix keeps a real value.
      topicPrefix: original ? "" : prefix,
      topicMappingPrefix: prefix,
      markerTopic: MARKER_TOPIC,
      replicationFactor: choice.value,
      teardown: "delete",
      ...(v.scratch && v.cloneTtl !== null ? { teardownTtlSeconds: v.cloneTtl } : {}),
      ...(topicConfigurationPlan(v.topicConfiguration) ? { topicConfiguration: topicConfigurationPlan(v.topicConfiguration)! } : {}),
      ...(original ? { originalName: true, originalNameNoOwner: v.noOwner } : {}),
    },
    sample: {
      windowStart: rfc3339(p.window.fromMs),
      windowEnd: endInstant(p),
      recordsPerPartition: RECORDS_PER_PARTITION,
      anchor: SAMPLE_ANCHOR,
      ...(complete ? { coverage: COVERAGE_COMPLETE } : {}),
      ...(bound !== undefined ? { completeMaxRecords: bound as number } : {}),
    },
    objectives: {},
    evidence,
  };
}

// --------------------------------------------------------------- validation

export type Problems = Partial<Record<keyof WizardValues | "archive" | "point", string>>;

/** The page's own refusals, by input, before anything is sent. */
export function validate(ctx: WizardContext, v: WizardValues, f: PlanFields, destinationProblem: string | null): Problems {
  const out: Problems = {};
  if (destinationProblem) out.archive = destinationProblem;
  const pit = pointInTimeOf(ctx.point, v);
  const when = pointInTimeProblem(ctx.point, pit);
  if (when) out.pointInTime = when;
  const mapping = mappingProblems(ctx.point, v, f);
  if (mapping) out[mapping.field] = mapping.message;
  const rf = replicationProblem(replicationChoice(ctx, v), ctx.brokers);
  if (rf) out.replication = rf;
  if (!ctx.connection) out.target = "choose the target connection this restore writes to";
  const ttl = v.scratch ? cloneTtlError(v.cloneTtl) : null;
  if (ttl) out.cloneTtl = ttl;
  if (originalNameChosen(v) && !v.noOwner) out.noOwner = "state that no declarative owner manages these names, or restore under a prefix";
  if (ctx.point.destinationName) {
    if (!evidenceStoreOf(ctx.evidence)) out.evidenceDestination = "choose the saved destination the evidence is written to";
  } else {
    if (!v.evidenceBucket.trim()) out.evidenceBucket = "the bucket the signed evidence is written to";
    if (v.archiveSecret && !/^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/.test(v.archiveSecret)) {
      out.archiveSecret = "a Secret name is lowercase letters, digits, '-' and '.'";
    }
    if (!ctx.point.archiveUrl) out.archive = "no archive URL was recorded for this recovery point";
  }
  return out;
}

// -------------------------------------------------------------- the request

/** The create body. `planBytes` and `planHash` are the PREPARED plan's, unchanged;
 *  `approvalRef` is the name minted from those bytes, which does not exist yet. */
export function restoreBody(
  ctx: WizardContext,
  v: WizardValues,
  f: PlanFields,
  prepared: PreparedPlan,
  policy: ApprovalPolicy | null,
): CreateRestoreRequest {
  const p = ctx.point;
  const original = originalNameChosen(v);
  const body: CreateRestoreRequest = {
    planBytes: prepared.bytes,
    planHash: prepared.hash,
    approvalRef: { name: prepared.approvalName },
    sourceArchive: p.destinationName
      ? { url: p.archiveUrl }
      : { url: p.archiveUrl, ...(v.archiveSecret.trim() ? { credentialRef: { name: v.archiveSecret.trim() } } : {}) },
    backupSetRef: f.backupSetRef,
    pointInTime: f.pointInTime,
    target: {
      clusterRef: { name: ctx.connection?.name ?? v.target },
      mode: f.target.mode as CreateRestoreRequest["target"]["mode"],
      topicNaming: original ? { prefix: "", originalName: true } : { prefix: f.target.topicPrefix },
    },
    deadlineSeconds: DEFAULT_DEADLINE_SECONDS,
  };
  // Declared beside the plan: the controller refuses a Restore whose declaration and
  // plan disagree, so both come from the same field. Absent for a sampled plan.
  if (f.sample.coverage === COVERAGE_COMPLETE) {
    body.coverage = "complete";
    if (typeof f.sample.completeMaxRecords === "number") body.completeMaxRecords = f.sample.completeMaxRecords;
  }
  if (p.destinationName) {
    body.sourceDestinationRef = { name: p.destinationName };
    body.evidenceDestinationRef = { name: ctx.evidence?.name ?? p.destinationName };
  }
  // The mapping the preview showed, declared on the REQUEST; not in scratch, where the
  // API cannot check one and refuses it by name.
  if (f.target.mode !== "scratch") body.topicMapping = topicMapping(f);
  const ticket = ticketFor(policy, v.ticket);
  if (ticket !== undefined) body.ticket = ticket;
  if (typedConfirmationRequired(policy, v)) body.originalNameConfirmation = { typedTopics: parseTypedTopics(v.typedNames) };
  return body;
}

/** The Idempotency-Key: the old console's own, from the name minted from the bytes, so
 *  a second click, a lost answer or a reload of the same plan never makes a second Restore. */
export function restoreIdempotencyKey(ns: string, prepared: PreparedPlan): string {
  return `logweir-ui.restores.${ns}.${prepared.restoreName}`;
}

// ---------------------------------------------------------- approval routing

export type ApprovalPolicy = S["ApprovalPolicyView"];

/** The mode a submission meets, in the operator's words, from the namespace's EFFECTIVE
 *  policy. An unknown word never reads as "no key needed": it is strict. */
export type ApprovalRoute =
  | "confirm"
  | "confirm-unavailable"
  | "two-person"
  | "two-person-unavailable"
  | "strict"
  | "legacy"
  | "unknown";

export function approvalRoute(p: ApprovalPolicy | null): ApprovalRoute {
  if (p === null) return "unknown";
  if (p.legacy) return "legacy";
  if (p.mode === "ordinary") return p.ordinaryConfirmationAvailable ? "confirm" : "confirm-unavailable";
  if (p.mode === "governed" && p.operatorMode === "two-person") return p.consoleApprovalAvailable ? "two-person" : "two-person-unavailable";
  return "strict";
}

/** A refusal the policy makes before anything is sent (the API refuses the same). */
export function policyRefusal(p: ApprovalPolicy | null): string | null {
  const route = approvalRoute(p);
  if (route === "confirm-unavailable") {
    return `Namespace policy ${p!.name} is confirm, and this console cannot sign a confirmation yet: its confirmation key is not there. Try again in a minute.`;
  }
  if (route === "two-person-unavailable") {
    return `Namespace policy ${p!.name} is two-person, and this console cannot take part in it: request and approve this restore in the shared console.`;
  }
  return null;
}

/** The ticket sent: only under an explicit (non-legacy) binding; an unbound namespace signs nothing. */
export function ticketFor(p: ApprovalPolicy | null, ticket: string): string | undefined {
  const t = ticket.trim();
  return p !== null && !p.legacy && t ? t : undefined;
}

/** OD-10: a one-person confirmation of a restore under the original names re-types every name. */
export function typedConfirmationRequired(p: ApprovalPolicy | null, v: Pick<WizardValues, "naming" | "scratch">): boolean {
  return originalNameChosen(v) && p !== null && !p.legacy && p.mode === "ordinary";
}

/** One per line (a comma also separates), trimmed, blanks dropped; never case-folded. */
export function parseTypedTopics(text: string): string[] {
  return text
    .split(/[\n,]/)
    .map((t) => t.trim())
    .filter(Boolean);
}

/** What the typed names get wrong against the topics restored, or null when they are
 *  exactly them, each once (the API answers typed_topics_mismatch for the same). */
export function typedTopicsProblem(p: ApprovalPolicy | null, v: WizardValues, topics: string[]): string | null {
  if (!typedConfirmationRequired(p, v)) return null;
  const typed = parseTypedTopics(v.typedNames);
  if (typed.length === 0) return "re-type every original topic name, exactly, to confirm this restore under the original names alone";
  const wanted = new Set(topics);
  const seen = new Set<string>();
  const repeated = new Set<string>();
  for (const t of typed) {
    if (seen.has(t)) repeated.add(t);
    seen.add(t);
  }
  const missing = [...wanted].filter((t) => !seen.has(t));
  const extra = [...seen].filter((t) => !wanted.has(t));
  const parts = [
    ...(missing.length ? [`not typed: ${missing.join(", ")}`] : []),
    ...(extra.length ? [`typed but not restored by this plan: ${extra.join(", ")}`] : []),
    ...(repeated.size ? [`typed more than once: ${[...repeated].join(", ")}`] : []),
  ];
  return parts.length ? `the typed names are not exactly the topics this plan restores (${parts.join("; ")})` : null;
}

// -------------------------------------------------------------- readiness

export type Preflight = S["Preflight"];
export type CheckEntry = S["CheckEntryView"];

/** Advisory and execution-only rows never gate; anything else (unknown included) does. */
export function isBlockingRow(c: CheckEntry): boolean {
  return c.gating !== "advisory" && c.gating !== "executionOnly";
}

/** The one row a draft cannot make ready: no Restore exists yet for an approver to sign. */
export function isDraftApprovalRow(c: CheckEntry): boolean {
  return c.id === "approval.state" && c.state === "skipped" && c.code === "SubjectNotCreated";
}

/** `unknown` only because of the draft's approval row, every other blocking row ready
 *  (and at least one), and no execution-only row said no. */
export function readyButForDraftApproval(p: Preflight): boolean {
  if (p.state !== "unknown" || !p.terminal) return false;
  const blocking = p.checks.filter(isBlockingRow);
  const others = blocking.filter((c) => !isDraftApprovalRow(c));
  const refusedAtRun = p.checks.some((c) => c.gating === "executionOnly" && c.state === "notReady");
  return blocking.some(isDraftApprovalRow) && others.length > 0 && others.every((c) => c.state === "ready") && !refusedAtRun;
}

export function staleReasonLine(r: S["StaleReasonView"]): string {
  const subject = r.kind ? ` (${r.kind}${r.name ? `/${r.name}` : ""})` : "";
  if (r.reason === "unverifiable") return `could not be checked${subject}${r.basis ? `: ${r.basis}` : ""}`;
  return `${r.reason}${subject}`;
}

/** Why the readiness check on screen does not let this plan through, or null. `boundHash`
 *  is the hash the check was started for; the server's binding is compared too. */
export function readinessRefusal(result: Preflight | null, boundHash: string, hash: string): string | null {
  if (result === null) return "No readiness check has run for this plan.";
  const serverHash = result.binding.planHash ?? "";
  if (hash && ((boundHash && boundHash !== hash) || (serverHash && serverHash !== hash))) {
    return `The readiness check was produced for plan ${boundHash || serverHash}, and this plan hashes to ${hash}: something that changes the document changed since it ran. Run it again.`;
  }
  if (result.stale) {
    const reasons = result.staleReasons.map(staleReasonLine).join("; ");
    return `The readiness check no longer applies to the inputs it was run against${reasons ? ` (${reasons})` : ""}. Run it again.`;
  }
  if (!result.terminal) return "The readiness check has not finished.";
  if (!result.applicable) return "The readiness check finished without becoming applicable to its inputs. Run it again.";
  if (result.state !== "ready" && !readyButForDraftApproval(result)) {
    const blocking = result.checks.filter(isBlockingRow).filter((c) => c.state !== "ready");
    const others = blocking.filter((c) => !isDraftApprovalRow(c));
    const failing = (others.length ? others : blocking).map((c) => `${c.id} (${c.code ?? c.state})`).join(", ");
    const said: Record<string, string> = { notReady: "not ready", unknown: "inconclusive", failed: "a failure", cancelled: "cancelled" };
    return `The readiness check says ${said[result.state] ?? result.state}${failing ? `: ${failing}` : ""}. Nothing is sent while a blocking check refuses this plan.`;
  }
  return null;
}

/** The preflight request for exactly the prepared bytes. */
export function readinessRequest(ctx: WizardContext, v: WizardValues, prepared: PreparedPlan): S["CreatePreflightRequest"] {
  const p = ctx.point;
  const restore: S["RestorePreflightRequest"] = {
    planBytes: prepared.bytes,
    planHash: prepared.hash,
    target: ctx.connection?.name ?? v.target,
  };
  // A point bound to its catalog row is checked as that row (the controller re-reads its
  // availability and verification); an unbound Backup as the Backup, by uid.
  if (p.catalog) restore.catalogPoint = { catalog: p.catalog.name, pointId: p.catalog.entry.pointId };
  else if (p.backup) restore.recoveryPoint = { backupName: p.backup.name, backupUid: p.backup.uid };
  if (p.destinationName) {
    restore.sourceDestination = p.destinationName;
    restore.evidenceDestination = ctx.evidence?.name ?? p.destinationName;
  } else if (p.archiveUrl) {
    restore.legacySourceArchive = { url: p.archiveUrl, ...(v.archiveSecret.trim() ? { credentialRef: { name: v.archiveSecret.trim() } } : {}) };
  }
  return { operation: "restore", restore };
}
