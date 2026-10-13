// Theme A on mock data: cutover (PROD-04.2), target topic configuration (PROD-05.2)
// and safe clones (PROD-11.1a). The product API does not publish these yet, so the
// pages read this module directly. Every shape and name follows the shared design,
// decisions/PROD-04.2-cutover.md: the positions-mapping document (§2.4), the
// Switchover preflight (§3.8), plan (§3.2), status and per-group outcomes (§3.3, §3.5),
// `Restore.status.topicConfiguration` (§4.4, §6) and `Restore.status.clone` (§5, §6).
// Fields the design does not name are marked "console's reading".
import type { Restore } from "../api/types";
import { HOUR, MINUTE, NOW, iso } from "./clock";
import { CONSUMER_GROUPS, hex } from "./data";

// ------------------------------------------------- positions mapping (§2.2, §2.4)

export type MappingClass = "exact" | "approximate" | "unavailable";

/** One entry of the positions-mapping document (format 1.0.0). */
export interface MappingEntry {
  topic: string;
  partition: number;
  /** The captured committed position on the source (the next offset to read). */
  position: number;
  target_topic: string;
  class: MappingClass;
  target_offset?: number;
  /** `approximate` only: unread records the restore's own selection left out. */
  skipped?: number;
  /** `unavailable` only: ArchiveGap, NoArchivedData, BeforeLogStart, BeforeArchive, BeyondArchive, PositionBeyondEnd, NotCaptured: <reason>, PartitionNotRestored, TransactionalArchive. */
  reason?: string;
}

export interface MappingGroup {
  group_type: "classic" | "consumer" | "streams" | "share";
  active_at_capture: boolean;
  entries: MappingEntry[];
}

/** What each unavailable reason means, in the operator's words. */
export const UNAVAILABLE_WORDS: Record<string, string> = {
  ArchiveGap: "Records between the position and the next restored record are missing from the archive.",
  NoArchivedData: "The archive holds no records for this partition.",
  BeforeLogStart: "The position was before the source partition's first offset when the backup ran.",
  BeforeArchive: "The position is older than the first archived record, so no restored record corresponds to it.",
  BeyondArchive: "The position is after the last archived record.",
  PositionBeyondEnd: "The position was past the partition's end when it was captured.",
  PartitionNotRestored: "This partition was not part of the restore.",
  TransactionalArchive: "The archive holds transaction markers; a position could land on one, so none is mapped.",
};

/** The rule the mapping document names (§2.4). */
export const MAPPING_RULE = "firstTargetRecordAtOrAboveLineage";

/** The restore that every demo cutover starts from: up to 12:57, just before a bad deploy. */
export const CUTOVER_DEMO = "orders-recovery-q4m8x";

type Row = [topic: string, partition: number, position: number, cls: MappingClass, target?: number, extra?: number | string];

function group(type: MappingGroup["group_type"], active: boolean, rows: Row[], targetOf: (topic: string) => string): MappingGroup {
  return {
    group_type: type,
    active_at_capture: active,
    entries: rows.map(([topic, partition, position, cls, target, extra]) => ({
      topic,
      partition,
      position,
      target_topic: targetOf(topic),
      class: cls,
      ...(target !== undefined ? { target_offset: target } : {}),
      ...(cls === "approximate" ? { skipped: Number(extra) } : {}),
      ...(cls === "unavailable" ? { reason: String(extra) } : {}),
    })),
  };
}

/** The mapping the restore's complete verification wrote (captured groups only). */
export function mappingFor(restore: Restore): Record<string, MappingGroup> {
  const sources = ["orders", "orders-dlq", "payments"];
  const targetOf = (topic: string) => restore.newTopics[sources.indexOf(topic)] ?? topic;
  return {
    "orders-billing": group("classic", false, [
      ["orders", 0, 1_204_331, "exact", 1_198_870],
      ["orders", 1, 1_187_002, "exact", 1_181_455],
      ["orders", 2, 1_210_948, "exact", 1_205_101],
      ["orders-dlq", 0, 3_812, "approximate", 3_790, 9],
    ], targetOf),
    "orders-shipping": group("consumer", true, [
      ["orders", 0, 1_203_990, "exact", 1_198_529],
      ["orders", 1, 1_186_877, "exact", 1_181_330],
      ["orders", 2, 1_211_406, "approximate", 1_205_388, 37],
    ], targetOf),
    "orders-reporting": group("classic", false, [
      ["payments", 0, 88_120, "exact", 88_098],
      ["payments", 1, 87_904, "exact", 87_880],
    ], targetOf),
    "orders-fraud": group("classic", false, [
      ["orders", 0, 1_199_870, "exact", 1_194_412],
      ["orders", 1, 402_118, "unavailable", undefined, "BeforeArchive"],
      ["orders", 2, 1_206_771, "exact", 1_200_925],
    ], targetOf),
  };
}

// ------------------------------------------- the target groups now (§3.5, §3.8)

/** One target group, as the Switchover preflight read it (field names: console's reading). */
export interface TargetGroupState {
  group: string;
  visible: boolean;
  exists: boolean;
  group_type?: MappingGroup["group_type"];
  state?: "Empty" | "Stable" | "Unknown";
  members: number;
  /** Committed positions on the planned partitions, "topic/partition" to offset. */
  committed: Record<string, number>;
}

/** The preflight's answer. `round` 0 is the first read; on later reads the shipping consumers have stopped. */
export function preflightFor(restore: Restore, round: number): Record<string, TargetGroupState> {
  const mapping = mappingFor(restore);
  const planned = (g: string) =>
    Object.fromEntries((mapping[g]?.entries ?? []).map((e) => [`${e.target_topic}/${e.partition}`, e.target_offset ?? 0]));
  return {
    "orders-billing": { group: "orders-billing", visible: true, exists: false, members: 0, committed: {} },
    "orders-shipping":
      round === 0
        ? { group: "orders-shipping", visible: true, exists: true, group_type: "consumer", state: "Stable", members: 2, committed: {} }
        : { group: "orders-shipping", visible: true, exists: true, group_type: "consumer", state: "Empty", members: 0, committed: {} },
    "orders-reporting": {
      group: "orders-reporting",
      visible: true,
      exists: true,
      group_type: "classic",
      state: "Empty",
      members: 0,
      committed: planned("orders-reporting"),
    },
    "orders-fraud": { group: "orders-fraud", visible: true, exists: false, members: 0, committed: {} },
  };
}

// ------------------------------------------------------- the review per group

export type Verdict = "ready" | "alreadyApplied" | "stopConsumers" | "blocked";

export interface ReviewGroup {
  source: string;
  target: string;
  groupType: MappingGroup["group_type"];
  activeAtCapture: boolean;
  /** Empty for a group the backup did not capture. */
  entries: MappingEntry[];
  /** The capture reason for a group not captured (from the recovery point). */
  notCaptured?: string;
  state?: TargetGroupState;
  verdict: Verdict;
  /** The refusal or class name behind a verdict that is not Ready. */
  reason?: string;
}

function decide(g: Omit<ReviewGroup, "verdict" | "reason">): Pick<ReviewGroup, "verdict" | "reason"> {
  if (g.notCaptured) return { verdict: "blocked", reason: `NotCaptured: ${g.notCaptured}` };
  const gap = g.entries.find((e) => e.class === "unavailable");
  if (gap) return { verdict: "blocked", reason: gap.reason };
  const s = g.state;
  if (!s) return { verdict: "blocked", reason: "NoPositionsMapping" };
  if (!s.visible) return { verdict: "blocked", reason: "TargetNotVisible" };
  if (s.group_type === "streams" || s.group_type === "share") return { verdict: "blocked", reason: "TargetGroupTypeUnsupported" };
  if (s.members > 0 || s.state === "Unknown") return { verdict: "stopConsumers", reason: "GroupActive" };
  const keys = Object.keys(s.committed);
  if (keys.length > 0) {
    const same = g.entries.every((e) => s.committed[`${e.target_topic}/${e.partition}`] === e.target_offset);
    return same ? { verdict: "alreadyApplied", reason: "alreadyApplied" } : { verdict: "blocked", reason: "TargetPositionsChanged" };
  }
  return { verdict: "ready" };
}

export function reviewFor(restore: Restore, round: number): ReviewGroup[] {
  const mapping = mappingFor(restore);
  const states = preflightFor(restore, round);
  const captured: ReviewGroup[] = Object.entries(mapping).map(([source, m]) => {
    const g = { source, target: source, groupType: m.group_type, activeAtCapture: m.active_at_capture, entries: m.entries, state: states[source] };
    return { ...g, ...decide(g) };
  });
  const missed: ReviewGroup[] = CONSUMER_GROUPS.filter((g) => g.outcome !== "captured").map((g) => {
    const base = { source: g.groupId, target: g.groupId, groupType: "streams" as const, activeAtCapture: false, entries: [], notCaptured: g.reason };
    return { ...base, ...decide(base) };
  });
  return [...captured, ...missed];
}

// ------------------------------------------------- the plan (§3.2) and status (§3.3)

export function planYaml(restore: Restore, groups: ReviewGroup[], scorecard: string, mapping: string): string {
  const lines = [
    'format_version: "1.0.0"',
    `restore: {name: ${restore.name}, uid: ${restore.uid}, plan_hash: "${restore.planHash}", scorecard_sha256: "${scorecard}", mapping_sha256: "${mapping}"}`,
    "groups:",
  ];
  for (const g of groups) {
    lines.push(`  - source: ${g.source}`, `    target: ${g.target}`, "    topics:");
    const topics = [...new Set(g.entries.map((e) => e.target_topic))];
    for (const t of topics) {
      const es = g.entries.filter((e) => e.target_topic === t);
      const approx = es.filter((e) => e.class === "approximate").map((e) => e.partition);
      lines.push(`      ${t}:`, `        offsets: {${es.map((e) => `${e.partition}: ${e.target_offset}`).join(", ")}}`);
      if (approx.length) lines.push(`        approximate: [${approx.join(", ")}]`);
      lines.push("        expected: none");
    }
  }
  return lines.join("\n") + "\n";
}

export type SwitchoverPhase = "Pending" | "Running" | "Succeeded" | "PartiallyApplied" | "Failed" | "Refused";
export type GroupOutcome = "applied" | "alreadyApplied" | "refused" | "failed";

export interface SwitchoverGroupStatus {
  source: string;
  target: string;
  outcome: GroupOutcome | null;
  reason?: string;
  created: boolean;
  partitions: number;
}

export interface SwitchoverStatus {
  phase: SwitchoverPhase;
  groups: SwitchoverGroupStatus[];
  counts: { applied: number; alreadyApplied: number; refused: number; failed: number; untouched: number };
  evidence?: { recordKey: string; verification: "Pending" | "Valid" };
}

/** The Switchover's status after `done` of its groups were processed, in plan order. */
export function switchoverStatus(groups: ReviewGroup[], done: number, runId: string): SwitchoverStatus {
  const rows: SwitchoverGroupStatus[] = groups.map((g, i) => ({
    source: g.source,
    target: g.target,
    outcome: i < done ? (g.verdict === "alreadyApplied" ? "alreadyApplied" : "applied") : null,
    created: i < done && g.verdict === "ready" && g.state?.exists === false,
    partitions: g.entries.length,
  }));
  const count = (o: GroupOutcome) => rows.filter((r) => r.outcome === o).length;
  const finished = done >= groups.length;
  return {
    phase: done === 0 ? "Pending" : finished ? "Succeeded" : "Running",
    groups: rows,
    counts: {
      applied: count("applied"),
      alreadyApplied: count("alreadyApplied"),
      refused: count("refused"),
      failed: count("failed"),
      untouched: rows.filter((r) => r.outcome === null).length,
    },
    evidence: { recordKey: `logweir/switchovers/${runId}.record.json`, verification: finished ? "Valid" : "Pending" },
  };
}

// ------------------------------------------ target topic configuration (§4)

/** Logweir's keys while records are written (§4.2, "during restore"). */
export const DURING_RESTORE: Record<string, string> = {
  "cleanup.policy": "delete",
  "retention.ms": "-1",
  "retention.bytes": "-1",
  "message.timestamp.type": "CreateTime",
  "message.timestamp.before.max.ms": "9223372036854775807",
  "message.timestamp.after.max.ms": "9223372036854775807",
};

export type ApplyRoute = "adminApi" | "desiredStateExport" | "unknown";

/** One topic of the restore preflight's configuration diff (§4.3). */
export interface TopicConfigPreview {
  source: string;
  partitions: number;
  replication_factor: { source: number; target: number; reason?: string };
  route: ApplyRoute;
  /** Who owns the topic's configuration when it is not the admin API (console's reading). */
  owner?: string;
  at_creation: Record<string, string>;
  deferred: Record<string, string>;
  not_applied: { key: string; value: string; class: "inherited" | "clusterBound" | "providerOnly" | "secret" }[];
}

export const TOPIC_CONFIG: TopicConfigPreview[] = [
  {
    source: "orders",
    partitions: 3,
    replication_factor: { source: 3, target: 3 },
    route: "adminApi",
    at_creation: { "max.message.bytes": "2097152", "min.insync.replicas": "2" },
    deferred: { "retention.ms": "604800000" },
    not_applied: [{ key: "leader.replication.throttled.replicas", value: "*", class: "clusterBound" }],
  },
  {
    source: "orders-dlq",
    partitions: 1,
    replication_factor: { source: 3, target: 3 },
    route: "adminApi",
    at_creation: { "min.insync.replicas": "2" },
    deferred: { "retention.ms": "2592000000" },
    not_applied: [{ key: "segment.bytes", value: "536870912 (broker default)", class: "inherited" }],
  },
  {
    source: "payments",
    partitions: 6,
    replication_factor: { source: 3, target: 3 },
    route: "desiredStateExport",
    owner: "Strimzi KafkaTopic payments (namespace kafka)",
    at_creation: { "min.insync.replicas": "2", "min.compaction.lag.ms": "3600000" },
    deferred: { "cleanup.policy": "compact", "retention.ms": "-1" },
    not_applied: [],
  },
];

/** `Restore.status.topicConfiguration` (§4.4, §6). */
export interface TopicConfigurationStatus {
  transition: "afterVerification" | "none";
  applied: string[];
  failed: { topic: string; error: string; desired: Record<string, string> }[];
  exported: string[];
  desiredStateKey?: string;
}

// ------------------------------------------------------------ clones (§5)

/** `Restore.status.clone` (§5, §6). Phases other than RemovalFailed are the console's reading. */
export interface CloneStatus {
  ttlSeconds: number;
  expiresAt: string;
  phase: "Kept" | "Removing" | "Removed" | "RemovalFailed";
  attempts: number;
}

export const TTL_CHOICES = [
  { seconds: 4 * 3600, words: "4 hours" },
  { seconds: 24 * 3600, words: "24 hours" },
  { seconds: 7 * 86400, words: "7 days" },
];

// --------------------------------------------------- theme A state per restore

export interface RestoreThemeA {
  /** Whether a positions mapping exists, and if not, why (§2.1). */
  mapping: "written" | "sampled" | "scratch" | "pending";
  topicConfiguration?: TopicConfigurationStatus;
  clone?: CloneStatus;
  scorecardSha256: string;
  mappingSha256: string;
}

const declared = new Map<string, { ttlSeconds?: number; transition?: "afterVerification" | "none"; endedAt?: number }>([
  [CUTOVER_DEMO, { transition: "afterVerification" }],
  ["orders-clone-k3v9d", { ttlSeconds: 24 * 3600, transition: "afterVerification", endedAt: NOW - HOUR + 6 * MINUTE }],
]);

/** The mock POST records what a new restore's plan asked for. */
export function declareRestore(name: string, plan: string): void {
  const ttl = plan.match(/ttl_seconds: (\d+)/);
  const transition = plan.match(/transition: (afterVerification|none)/);
  declared.set(name, {
    ...(ttl ? { ttlSeconds: Number(ttl[1]) } : {}),
    ...(transition ? { transition: transition[1] as "afterVerification" | "none" } : {}),
  });
}

export function themeAOf(r: Restore): RestoreThemeA {
  const d = declared.get(r.name) ?? {};
  const seed = r.uid.charCodeAt(0) * 31 + r.name.length;
  const verified = r.operation.terminal && r.operation.verifiedSuccess;
  const complete = r.coverage?.recorded === "complete" || (!r.coverage?.recorded && r.coverage?.requested === "complete");
  const topics = r.newTopics;
  const owned = TOPIC_CONFIG.filter((t) => t.route === "desiredStateExport").map((t) => topics[["orders", "orders-dlq", "payments"].indexOf(t.source)] ?? t.source);
  const endedAt = d.endedAt ?? Date.parse(String(r.createdAt)) + 27_000;
  return {
    mapping: r.target.mode === "scratch" ? "scratch" : !verified ? "pending" : complete ? "written" : "sampled",
    scorecardSha256: "sha256:" + hex(seed + 11, 64),
    mappingSha256: "sha256:" + hex(seed + 12, 64),
    ...(d.transition && verified
      ? {
          topicConfiguration: {
            transition: d.transition,
            applied: d.transition === "afterVerification" ? topics.filter((t) => !owned.includes(t)) : [],
            failed: [],
            exported: d.transition === "afterVerification" ? topics.filter((t) => owned.includes(t)) : [],
            ...(d.transition === "afterVerification" ? { desiredStateKey: `logweir/drills/${r.uid.slice(0, 8)}.desired-state.yaml` } : {}),
          },
        }
      : {}),
    ...(d.ttlSeconds
      ? {
          clone: {
            ttlSeconds: d.ttlSeconds,
            expiresAt: iso(endedAt + d.ttlSeconds * 1000),
            phase: endedAt + d.ttlSeconds * 1000 < Date.now() ? "Removed" : "Kept",
            attempts: 0,
          },
        }
      : {}),
  };
}

