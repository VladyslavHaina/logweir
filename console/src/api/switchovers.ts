// Theme A's cutover on the product API (PROD-04.2; decisions/PROD-04.2-cutover.md):
// the Switchover preflight (§3.8) and its details, the plan the approver signs (§3.2),
// the Switchover kind (§3.3) with its per-group outcomes (§3.5), the rollback a record
// allows (§3.6), and the approval a two-person namespace shows (§6, `switchover_scope`).
// Every read and write goes through the one typed client (`api`), as the other groups do.
//
// THE PREFLIGHT'S WORD DECIDES. A group is offered only when the review's own details
// line says `admissible`; the console never derives "ready" from anything else, and the
// Switchover Job repeats every check under its approval anyway.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { components } from "./schema";
import { ApiError, LIST_PAGE_SIZE, api, call, createHeaders, listAll, mintKey } from "./client";
import { keys } from "./queries";
import { useCan } from "./session";
import { hasShape, shellArg } from "../lib/shell";

export { outcomeUnknown } from "./client";
export { useApprovalPolicy } from "./approvals";

type S = components["schemas"];

export type Switchover = S["SwitchoverView"];
export type SwitchoverGroup = S["SwitchoverGroupView"];
export type SwitchoverScope = S["SwitchoverScopeView"];
export type SwitchoverApprovalRequest = S["SwitchoverApprovalRequestView"];
export type Preflight = S["Preflight"];
export type CheckEntry = S["CheckEntryView"];
export type DetailEntry = S["DetailEntryView"];
export type Authorization = S["RestoreRoutingView"];
export type TopicConfiguration = S["TopicConfigurationView"];
export type Clone = S["CloneView"];
export type Restore = S["Restore"];

const LIVE_MS = 1500;

// ------------------------------------------------------------- the words

/** The phases a Switchover ends in (§3.3). Anything else, absent included, is still moving. */
export const TERMINAL_PHASES = ["Succeeded", "PartiallyApplied", "Failed", "Refused"] as const;

export function isTerminal(s: Pick<Switchover, "phase">): boolean {
  return (TERMINAL_PHASES as readonly string[]).includes(s.phase ?? "");
}

/** What each unavailable class means (§2.2), in the operator's words. */
export const UNAVAILABLE_WORDS: Record<string, string> = {
  ArchiveGap: "Records between the position and the next restored record are missing from the archive.",
  NoArchivedData: "The archive holds no records for this partition.",
  BeforeLogStart: "The position was before the source partition's first offset when the backup ran.",
  BeforeArchive: "The position is older than the first archived record, so no restored record corresponds to it.",
  BeyondArchive: "The position is after the last archived record.",
  PositionBeyondEnd: "The position was past the partition's end when it was captured.",
  PartitionNotRestored: "This partition was not part of the restore.",
  TransactionalArchive: "The archive holds transaction markers; a position could land on one, so none is mapped.",
  NotCaptured: "The backup did not capture this group's positions.",
};

/** Why a group, or a whole switchover, is refused (§3.4, §3.5, §6), in the operator's words. */
export const REFUSAL_WORDS: Record<string, string> = {
  GroupActive: "The group has members on the target. The broker takes a position only while a group has none: stop its consumers.",
  TargetGroupTypeUnsupported: "The target group is a share or streams group, or another protocol, which a switchover does not set.",
  TargetGroupTypeUnproven: "The target's brokers cannot say what type the group is, so it is not set.",
  TargetNotVisible: "Logweir's principal on the target cannot see this group. Grant it DESCRIBE and READ on the group.",
  TargetPositionsChanged: "The group's committed positions on the target are not what the plan expects, so nothing is committed for it.",
  BelowTargetLogStart: "A planned offset is below the target partition's first offset now.",
  AboveTargetEnd: "A planned offset is past the target partition's end now.",
  TargetMarksUnreadable: "A planned partition's first and last offsets could not be read on the target.",
  NotAuthorized: "The broker refused the commit for this principal.",
  CommitFailed: "The commit failed; the record says which partitions were applied.",
  NotInMapping: "The restore's mapping names no such group.",
  NothingMapped: "No position of this group maps onto a restored partition.",
  SwitchoverPlanInvalid: "The plan does not parse or breaks a bound.",
  TargetGenerationChanged: "A restored topic was deleted and created again since the restore created it, so no position is applied.",
  TargetIdentityUnknown: "A restored topic's ID could not be read, so its generation cannot be proven.",
  TargetClusterChanged: "The target answers with another cluster ID than the one the restore wrote to.",
  RestoreNotSucceeded: "The restore did not succeed with a verified scorecard.",
  RestoreIsScratch: "The restore is a clone (scratch mode); its topics are deleted when it expires.",
  SwitchoverInProgress: "Another switchover of this restore is still running.",
  NoPositionsMapping: "The restore wrote no positions mapping.",
  VerificationNotComplete: "The restore was not verified completely (coverage complete, every partition covered); positions are mapped only then.",
  MappingDigestMismatch: "The positions mapping does not match the digest the signed scorecard binds.",
  PlanDisagreesWithMapping: "A planned position is not the one the restore's mapping maps.",
  SubjectKindMismatch: "The approval was minted for another kind of object.",
};

/** The leading reason word of a reason that may carry a sentence (`Word: why`). */
export function reasonWord(reason: string | null | undefined): string {
  return (reason ?? "").split(":")[0]!.trim();
}

export function reasonWords(reason: string | null | undefined): string | undefined {
  const word = reasonWord(reason);
  return REFUSAL_WORDS[word] ?? UNAVAILABLE_WORDS[word];
}

// ------------------------------------------------ the review (§3.8 details)

export type MappingClass = "exact" | "approximate" | "unavailable";

/** One partition of a group's review line. */
export interface ReviewPosition {
  topic: string;
  partition: number;
  class: MappingClass;
  /** The target offset the mapping maps to; absent for `unavailable`. */
  offset?: number;
  /** An `unavailable` entry's reason, or `selectionExcluded` for an approximate one. */
  reason?: string;
  /** Records the restore's own selection left out (approximate). The review line does not
   *  carry it today (`crates/logweir/src/check/kinds/switchover.rs` `detail_lines`); shown
   *  when it does. */
  skipped?: number;
  /** The committed position the review read on the target now; null: none. */
  committed: number | null;
}

/** One group as the review read it: the details lines of the group, merged over its topics. */
export interface ReviewGroup {
  source: string;
  target: string;
  /** The run would commit this group, or find it already applied: the preflight's word. */
  admissible: boolean;
  /** The target group does not exist: a switchover creates it. */
  absent: boolean;
  /** Every planned partition already holds its mapped offset: nothing to commit. */
  alreadyApplied: boolean;
  type?: string;
  state?: string;
  members?: number;
  /** The run's own refusal word (GroupActive, TargetPositionsChanged, …), when not admissible. */
  reason?: string;
  message?: string;
  positions: ReviewPosition[];
}

const CLASSES: readonly string[] = ["exact", "approximate", "unavailable"];

function obj(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

const str = (v: unknown): string | undefined => (typeof v === "string" ? v : undefined);
const int = (v: unknown): number | undefined => (typeof v === "number" && Number.isInteger(v) ? v : undefined);

/**
 * The details stream of a `switchover` preflight, as groups: one line per group and
 * restored topic (`detail_lines`). A line that is not a group line (the truncation note)
 * is reported, never guessed into a group. A group whose lines disagree about its
 * verdict is not admissible.
 */
export function reviewGroups(entries: DetailEntry[]): { groups: ReviewGroup[]; truncated: boolean; unread: number } {
  const groups = new Map<string, ReviewGroup>();
  let truncated = false;
  let unread = 0;
  for (const e of entries) {
    const line = obj(e.entry);
    if (!line) {
      unread += 1;
      continue;
    }
    if (line.truncated === true) {
      truncated = true;
      continue;
    }
    const source = str(line.group);
    const target = str(line.target);
    if (!source || !target || typeof line.admissible !== "boolean") {
      unread += 1;
      continue;
    }
    const key = `${source}\u0000${target}`;
    const seen = groups.get(key);
    const g: ReviewGroup = seen ?? {
      source,
      target,
      admissible: line.admissible,
      absent: line.absent === true,
      alreadyApplied: line.alreadyApplied === true,
      positions: [],
    };
    if (seen && (seen.admissible !== line.admissible || seen.alreadyApplied !== (line.alreadyApplied === true))) {
      g.admissible = false;
      g.reason ??= "LinesDisagree";
    }
    if (str(line.type)) g.type = str(line.type);
    if (str(line.state)) g.state = str(line.state);
    if (int(line.members) !== undefined) g.members = int(line.members);
    if (str(line.reason)) g.reason = str(line.reason);
    if (str(line.message)) g.message = str(line.message);
    const topic = str(line.topic);
    const positions = Array.isArray(line.positions) ? line.positions : [];
    for (const raw of positions) {
      const p = obj(raw);
      const partition = int(p?.partition);
      const cls = str(p?.class);
      if (!p || !topic || partition === undefined || !cls || !CLASSES.includes(cls)) {
        unread += 1;
        // A partition the console cannot read is never planned: the group is not offered.
        g.admissible = false;
        g.reason ??= "LineUnreadable";
        continue;
      }
      g.positions.push({
        topic,
        partition,
        class: cls as MappingClass,
        ...(int(p.offset) !== undefined ? { offset: int(p.offset) } : {}),
        ...(str(p.reason) ? { reason: str(p.reason) } : {}),
        ...(int(p.skipped) !== undefined ? { skipped: int(p.skipped) } : {}),
        committed: int(p.committed) ?? null,
      });
    }
    groups.set(key, g);
  }
  return { groups: [...groups.values()], truncated, unread };
}

export type Verdict = "ready" | "alreadyApplied" | "stopConsumers" | "blocked";

/** The review's verdict for one group, from the preflight's own word and nothing else. */
export function verdictOf(g: ReviewGroup): { verdict: Verdict; reason?: string } {
  if (g.admissible && g.alreadyApplied) return { verdict: "alreadyApplied" };
  if (g.admissible) return { verdict: "ready" };
  const word = reasonWord(g.reason) || "NotAdmissible";
  return word === "GroupActive" ? { verdict: "stopConsumers", reason: word } : { verdict: "blocked", reason: word };
}

/** The positions a plan commits for this group: every mapped one (§3.2), never an unavailable one. */
export function plannedPositions(g: ReviewGroup): ReviewPosition[] {
  return g.positions.filter((p) => p.class !== "unavailable" && p.offset !== undefined);
}

export function unavailablePositions(g: ReviewGroup): ReviewPosition[] {
  return g.positions.filter((p) => p.class === "unavailable" || p.offset === undefined);
}

/** A group the review offers: admissible, not already applied, and with something to commit. */
export function offered(g: ReviewGroup): boolean {
  return verdictOf(g).verdict === "ready" && plannedPositions(g).length > 0;
}

/** Offered and chosen by default: every partition maps. A group with an unavailable
 *  partition is offered, but only on an explicit choice. */
export function chosenByDefault(g: ReviewGroup): boolean {
  return offered(g) && unavailablePositions(g).length === 0;
}

export function classCounts(positions: ReviewPosition[]) {
  const n = (c: MappingClass) => positions.filter((p) => p.class === c).length;
  return { exact: n("exact"), approximate: n("approximate"), unavailable: n("unavailable") };
}

/** Whether a preflight answered at all, and whether its answer still holds. */
export type ReviewState =
  | { kind: "running"; state: string }
  | { kind: "refused"; checks: CheckEntry[] }
  | { kind: "undecided"; why: string }
  | { kind: "decided" };

const SWITCHOVER_ROWS = ["switchover.restore", "switchover.evidence", "switchover.target"] as const;

/**
 * Where a switchover review stands. Groups are read only from a review that finished
 * with a verdict (`ready` or `notReady`), still applies, and whose restore, evidence and
 * target rows are `ready`: anything else offers nothing.
 */
export function reviewState(pf: Preflight): ReviewState {
  if (!pf.terminal) return { kind: "running", state: pf.state };
  if (pf.state === "cancelled") return { kind: "undecided", why: "The review was cancelled before it answered." };
  if (pf.state === "failed") return { kind: "undecided", why: pf.reason ? `The review produced no result (${pf.reason}).` : "The review produced no result." };
  const refused = pf.checks.filter((c) => (SWITCHOVER_ROWS as readonly string[]).includes(c.id) && c.state === "notReady");
  if (refused.length > 0) return { kind: "refused", checks: refused };
  if (pf.stale || !pf.applicable) {
    return { kind: "undecided", why: `The review no longer describes the restore${pf.staleReasons.length ? ` (${pf.staleReasons.map((r) => r.reason).join(", ")})` : ""}. Check again.` };
  }
  const missing = SWITCHOVER_ROWS.filter((id) => id !== "switchover.restore" && !pf.checks.some((c) => c.id === id && c.state === "ready"));
  if (missing.length > 0 || (pf.state !== "ready" && pf.state !== "notReady")) {
    return { kind: "undecided", why: `The review could not decide${missing.length ? ` ${missing.join(" and ")}` : ""} (${pf.state}).` };
  }
  return { kind: "decided" };
}

/**
 * The digests the plan binds (§3.2): the restore's signed scorecard and the positions
 * mapping it binds, as the review's `switchover.evidence` row states them. The shipped
 * Preflight carries no facts map, so the controller folds them into the row's message as
 * `[mappingSha256=sha256:…; scorecardSha256=sha256:…]` (`entry_of`).
 */
export function evidenceDigests(pf: Preflight): { scorecardSha256: string; mappingSha256: string } | null {
  const row = pf.checks.find((c) => c.id === "switchover.evidence" && c.state === "ready");
  const message = row?.message ?? "";
  const fact = (key: string) => message.match(new RegExp(`(?:^|[\\[; ])${key}=(sha256:[0-9a-f]{64})(?=[;\\]])`))?.[1];
  const scorecardSha256 = fact("scorecardSha256");
  const mappingSha256 = fact("mappingSha256");
  return scorecardSha256 && mappingSha256 ? { scorecardSha256, mappingSha256 } : null;
}

// ----------------------------------------------------------- the plan (§3.2)

/** The plan grammar's bounds (`logweir_core::switchover`). */
export const PLAN_BOUNDS = { groups: 100, positions: 8192, bytes: 256 * 1024, groupIdBytes: 255 } as const;

export interface PlanRestore {
  name: string;
  uid: string;
  /** The restore plan's hash (the scorecard's `approval.plan_hash`). */
  planHash: string;
  scorecardSha256: string;
  mappingSha256: string;
}

/**
 * A YAML double-quoted scalar: every string the plan names is quoted, so no topic or
 * group name can read as a number, a boolean, a null or YAML syntax. Everything outside
 * printable ASCII is escaped (`\uXXXX`, `\UXXXXXXXX`), so no line break or byte-order
 * mark inside a name is folded by the parser.
 */
export function yamlString(s: string): string {
  let out = '"';
  for (const ch of s) {
    const cp = ch.codePointAt(0)!;
    if (ch === '"') out += '\\"';
    else if (ch === "\\") out += "\\\\";
    else if (cp >= 0x20 && cp < 0x7f) out += ch;
    else if (cp <= 0xffff) out += `\\u${cp.toString(16).padStart(4, "0")}`;
    else out += `\\U${cp.toString(16).padStart(8, "0")}`;
  }
  return out + '"';
}

const SHA256 = /^sha256:[0-9a-f]{64}$/;

function idFault(what: string, id: string): string | null {
  // eslint-disable-next-line no-control-regex
  if (!id.trim() || /[\u0000-\u001f\u007f-\u009f]/.test(id) || new TextEncoder().encode(id).length > PLAN_BOUNDS.groupIdBytes) {
    return `${what} "${id}" is blank, carries a control character or is over ${PLAN_BOUNDS.groupIdBytes} bytes`;
  }
  return null;
}

/**
 * **The forward plan** for the chosen groups, as `logweir switchover plan` plans them:
 * every mapped position of each group, `approximate` naming the approximate partitions,
 * `expected: none`; unavailable positions are never planned. Groups in review order,
 * topics and partitions in order. Refuses what the plan grammar refuses (`validate`).
 */
export function planDocument(restore: PlanRestore, groups: ReviewGroup[]): { bytes: string } | { refused: string } {
  for (const [what, v] of [["restore.plan_hash", restore.planHash], ["restore.scorecard_sha256", restore.scorecardSha256], ["restore.mapping_sha256", restore.mappingSha256]] as const) {
    if (!SHA256.test(v)) return { refused: `${what} is not sha256:<64 lowercase hex>` };
  }
  if (!restore.name.trim() || !restore.uid.trim()) return { refused: "the restore's name or UID is blank" };
  if (groups.length < 1 || groups.length > PLAN_BOUNDS.groups) return { refused: `the plan names ${groups.length} groups; it names 1 to ${PLAN_BOUNDS.groups}` };
  const targets = new Set<string>();
  let positions = 0;
  const lines = [
    'format_version: "1.0.0"',
    "restore:",
    `  name: ${yamlString(restore.name)}`,
    `  uid: ${yamlString(restore.uid)}`,
    `  plan_hash: ${yamlString(restore.planHash)}`,
    `  scorecard_sha256: ${yamlString(restore.scorecardSha256)}`,
    `  mapping_sha256: ${yamlString(restore.mappingSha256)}`,
    "groups:",
  ];
  for (const g of groups) {
    const fault = idFault("source group", g.source) ?? idFault("target group", g.target);
    if (fault) return { refused: fault };
    if (targets.has(g.target)) return { refused: `target group "${g.target}" is named twice; one group is applied once per plan` };
    targets.add(g.target);
    const planned = plannedPositions(g);
    if (planned.length === 0) return { refused: `group "${g.source}" has no mapped position to apply` };
    lines.push(`  - source: ${yamlString(g.source)}`, `    target: ${yamlString(g.target)}`, "    topics:");
    const topics = [...new Set(planned.map((p) => p.topic))].sort();
    for (const t of topics) {
      const ps = planned.filter((p) => p.topic === t).sort((a, b) => a.partition - b.partition);
      if (ps.some((p, i) => i > 0 && ps[i - 1]!.partition === p.partition)) return { refused: `group "${g.source}" ${t} names a partition twice` };
      if (ps.some((p) => (p.offset ?? -1) < 0)) return { refused: `group "${g.source}" ${t} plans a negative offset` };
      positions += ps.length;
      const approx = ps.filter((p) => p.class === "approximate").map((p) => p.partition);
      lines.push(`      ${yamlString(t)}:`, `        offsets: {${ps.map((p) => `${p.partition}: ${p.offset}`).join(", ")}}`);
      if (approx.length) lines.push(`        approximate: [${approx.join(", ")}]`);
      lines.push("        expected: none");
    }
  }
  if (positions > PLAN_BOUNDS.positions) return { refused: `the plan commits ${positions} positions; it commits 1 to ${PLAN_BOUNDS.positions}, which is what one approval scope shows` };
  const bytes = lines.join("\n") + "\n";
  if (new TextEncoder().encode(bytes).length > PLAN_BOUNDS.bytes) return { refused: `the plan is over its bound of ${PLAN_BOUNDS.bytes} bytes` };
  return { bytes };
}

/** One partition of a plan the console wrote. */
export interface PlannedPartition {
  topic: string;
  partition: number;
  offset: number;
  approximate: boolean;
  /** The committed position the group must hold first; null: none. */
  expected: number | null;
}

export interface ParsedPlan {
  restore: Record<string, string>;
  groups: { source: string; target: string; partitions: PlannedPartition[] }[];
}

function unquote(s: string): string | null {
  const t = s.trim();
  if (!t.startsWith('"')) return /^[A-Za-z0-9._:-]+$/.test(t) ? t : null;
  try {
    // The console's own scalars: JSON-compatible escapes plus \UXXXXXXXX.
    return JSON.parse(t.replace(/\\U([0-9a-fA-F]{8})/g, (_m, h: string) => {
      const cp = parseInt(h, 16);
      const hi = Math.floor((cp - 0x10000) / 0x400) + 0xd800;
      const lo = ((cp - 0x10000) % 0x400) + 0xdc00;
      return `\\u${hi.toString(16)}\\u${lo.toString(16)}`;
    })) as string;
  } catch {
    return null;
  }
}

function intMap(s: string): Map<number, number> | null {
  const m = s.trim().match(/^\{(.*)\}$/);
  if (!m) return null;
  const out = new Map<number, number>();
  for (const pair of m[1]!.split(",").map((x) => x.trim()).filter(Boolean)) {
    const kv = pair.match(/^(\d+): (\d+)$/);
    if (!kv) return null;
    out.set(Number(kv[1]), Number(kv[2]));
  }
  return out;
}

/**
 * Reads back a plan in exactly the layout `planDocument` writes, for the Switchover's
 * page (its partitions and the rollback). Any other layout (a plan written by
 * `logweir switchover plan`, or by hand) is `null`: the page then shows the plan's bytes
 * and the CLI, and draws no table it could misread.
 */
export function parseOwnPlan(bytes: string): ParsedPlan | null {
  const lines = bytes.split("\n");
  if (lines[0] !== 'format_version: "1.0.0"' || lines[1] !== "restore:") return null;
  const restore: Record<string, string> = {};
  let i = 2;
  for (; i < lines.length && lines[i]!.startsWith("  ") && !lines[i]!.startsWith("  -"); i += 1) {
    const kv = lines[i]!.match(/^ {2}([a-z_0-9]+): (.*)$/);
    const v = kv ? unquote(kv[2]!) : null;
    if (!kv || v === null) return null;
    restore[kv[1]!] = v;
  }
  if (lines[i] !== "groups:") return null;
  i += 1;
  const groups: ParsedPlan["groups"] = [];
  while (i < lines.length && lines[i] !== "") {
    const src = lines[i]!.match(/^ {2}- source: (.*)$/);
    const tgt = lines[i + 1]?.match(/^ {4}target: (.*)$/);
    if (!src || !tgt || lines[i + 2] !== "    topics:") return null;
    const source = unquote(src[1]!);
    const target = unquote(tgt[1]!);
    if (source === null || target === null) return null;
    i += 3;
    const partitions: PlannedPartition[] = [];
    while (i < lines.length && lines[i]!.startsWith("      ") && !lines[i]!.startsWith("        ")) {
      const head = lines[i]!.match(/^ {6}(.*):$/);
      const topic = head ? unquote(head[1]!) : null;
      const offsets = lines[i + 1]?.match(/^ {8}offsets: (.*)$/);
      if (topic === null || !offsets) return null;
      const om = intMap(offsets[1]!);
      if (!om) return null;
      i += 2;
      let approximate: number[] = [];
      const ap = lines[i]?.match(/^ {8}approximate: \[([\d, ]*)\]$/);
      if (ap) {
        approximate = ap[1]!.split(",").map((x) => x.trim()).filter(Boolean).map(Number);
        i += 1;
      }
      const ex = lines[i]?.match(/^ {8}expected: (.*)$/);
      if (!ex) return null;
      const expected = ex[1] === "none" ? null : intMap(ex[1]!);
      if (ex[1] !== "none" && !expected) return null;
      i += 1;
      for (const [partition, offset] of om) {
        partitions.push({ topic, partition, offset, approximate: approximate.includes(partition), expected: expected?.get(partition) ?? null });
      }
    }
    groups.push({ source, target, partitions });
  }
  return groups.length ? { restore, groups } : null;
}

// ------------------------------------------------------------- rollback (§3.6)

export interface RollbackPlan {
  /** Partitions a rollback Switchover commits back: the prior position, expecting the applied one. */
  restorable: { target: string; topic: string; partition: number; prior: number; applied: number }[];
  /** Partitions that had no position before: a commit cannot remove one. */
  notRestorable: { target: string; topic: string; partition: number }[];
  /** Groups the switchover left `failed: CommitFailed`: neither re-run nor rolled back. */
  stuck: string[];
  text: string;
}

/**
 * **What a rollback would do**, for display and download only: a rollback is a new
 * Switchover with its own approval, and its signed plan comes from the record
 * (`logweir switchover rollback-plan --record`). Only groups the switchover APPLIED are
 * reversed; a prior that was absent is listed with the broker's own command.
 *
 * The downloaded text carries no server-supplied string in a comment, and every one in
 * a command goes through `shellArg`: a name it does not allow becomes a placeholder.
 */
export function rollbackPlan(sw: Switchover, plan: ParsedPlan | null): RollbackPlan {
  const restorable: RollbackPlan["restorable"] = [];
  const notRestorable: RollbackPlan["notRestorable"] = [];
  const stuck = sw.groups.filter((g) => g.outcome === "failed" && reasonWord(g.reason) === "CommitFailed").map((g) => g.target);
  for (const g of sw.groups.filter((x) => x.outcome === "applied")) {
    const planned = plan?.groups.find((p) => p.target === g.target);
    for (const p of planned?.partitions ?? []) {
      if (p.expected === null) notRestorable.push({ target: g.target, topic: p.topic, partition: p.partition });
      else restorable.push({ target: g.target, topic: p.topic, partition: p.partition, prior: p.expected, applied: p.offset });
    }
  }
  const text = [
    "# The rollback of a Logweir switchover, for review only.",
    "# A rollback is a NEW Switchover, approved like this one. Its signed plan is built from the switchover's record:",
    "#   logweir switchover rollback-plan --record <a local copy of the switchover record> --out rollback.yaml",
    "# It is refused for a group whose consumers committed since (TargetPositionsChanged, naming the records read).",
    ...(restorable.length ? ["#", `# ${restorable.length} position(s) a rollback commits back to the prior one; the console's table lists them.`] : []),
    ...(notRestorable.length
      ? [
          "#",
          "# These had no committed position before the switchover. A commit cannot remove one; with the group",
          "# stopped, remove each with the broker's own tool (a <placeholder> marks a name to fill in by hand):",
          ...notRestorable.map(
            (r) =>
              `kafka-consumer-groups.sh --bootstrap-server <bootstrap> --group ${shellArg(r.target, "kafka", "<group>")} --topic ${shellArg(topicPartition(r.topic, r.partition), "kafka", "<topic:partition>")} --delete-offsets`,
          ),
        ]
      : []),
    ...(stuck.length
      ? ["#", `# ${stuck.length} group(s) failed part-way (CommitFailed): neither re-run nor rolled back. The record's per-partition`, "# \"applied\" says which positions to remove before a new Switchover with the same plan."]
      : []),
    ...(!plan && sw.groups.some((g) => g.outcome === "applied") ? ["#", "# The plan was not written by the console: the record names each applied partition and its prior position."] : []),
    "",
  ].join("\n");
  return { restorable, notRestorable, stuck, text };
}

function topicPartition(topic: string, partition: number): string {
  return Number.isInteger(partition) && partition >= 0 ? `${topic}:${partition}` : "";
}

/** A file name for a download: the server-supplied stem only when it is a plain name. */
export function fileName(stem: string, suffix: string, fallback: string): string {
  return hasShape(stem, "name") ? `${stem}${suffix}` : fallback;
}

// ------------------------------------------------------- errors and outcomes

/** The sentence an unknown outcome reads as (one wording everywhere). */
export const UNKNOWN_OUTCOME = "The service did not confirm the switchover. It may or may not exist; submitting again is safe, because it carries the same request key.";

// --------------------------------------------------------------- queries
// R1's keys and session: a list is [kind, ns], one object [kind, ns, name]; a Switchover is
// read under `restoresRead` and created under `restoreCreate` (the API authorizes it as a
// restore), and a read the session does not grant stays idle.

const moving = (items: Switchover[] | undefined) => (items?.some((s) => !isTerminal(s)) ? LIVE_MS : false);

export function useSwitchovers(ns: string) {
  return useQuery({
    queryKey: keys.list("switchovers", ns),
    enabled: useCan(ns, "restoresRead"),
    queryFn: ({ signal }) =>
      listAll("switchovers", (cursor) =>
        call(() => api.GET("/api/v1/namespaces/{ns}/switchovers", { params: { path: { ns }, query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
    refetchInterval: (q) => moving(q.state.data),
  });
}

export function useSwitchover(ns: string, name: string | undefined) {
  const readable = useCan(ns, "restoresRead");
  return useQuery({
    queryKey: keys.item("switchovers", ns, name ?? ""),
    enabled: readable && !!name,
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/switchovers/{name}", { params: { path: { ns, name: name! } }, signal }))).item,
    refetchInterval: (q) => (q.state.data && !isTerminal(q.state.data) ? LIVE_MS : false),
    retry: (n, error) => !(error instanceof ApiError && error.status === 404) && n < 2,
  });
}

/** Start a switchover review of one restore (every group its mapping maps). Each click is a
 *  new review, so each gets a fresh key. */
export function useStartReview(ns: string) {
  return useMutation({
    mutationFn: async (restore: { name: string; uid: string }): Promise<Preflight> =>
      (
        await call(() =>
          api.POST("/api/v1/namespaces/{ns}/preflights", {
            params: { path: { ns }, header: createHeaders(mintKey("switchover-review")) },
            body: { operation: "switchover", switchover: { restoreName: restore.name, restoreUid: restore.uid } },
          }),
        )
      ).item,
  });
}

export function usePreflight(ns: string, id: string | undefined) {
  const readable = useCan(ns, "preflight");
  return useQuery({
    queryKey: keys.item("preflights", ns, id ?? ""),
    enabled: readable && !!id,
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/preflights/{id}", { params: { path: { ns, id: id! } }, signal }))).item,
    refetchInterval: (q) => (q.state.data && !q.state.data.terminal ? 1000 : false),
  });
}

/** Every page of a preflight's details (at most 64 pages of 200). */
export async function readDetails(ns: string, id: string, check?: string, signal?: AbortSignal): Promise<DetailEntry[]> {
  const out: DetailEntry[] = [];
  let cursor: string | undefined;
  for (let page = 0; page < 64; page += 1) {
    const res = await call(() =>
      api.GET("/api/v1/namespaces/{ns}/preflights/{id}/details", {
        params: { path: { ns, id }, query: { limit: LIST_PAGE_SIZE, ...(cursor ? { cursor } : {}), ...(check ? { check } : {}) } },
        signal,
      }),
    );
    out.push(...res.items);
    cursor = res.page.nextCursor ?? undefined;
    if (!cursor) break;
  }
  return out;
}

export function usePreflightDetails(ns: string, pf: Preflight | undefined, check?: string) {
  return useQuery({
    queryKey: ["preflight-details", ns, pf?.id, check ?? ""],
    enabled: !!pf && pf.terminal && pf.detailsAvailable,
    queryFn: ({ signal }) => readDetails(ns, pf!.id, check, signal),
    staleTime: Infinity,
  });
}

export interface CreateSwitchover {
  restore: { name: string; uid: string };
  planBytes: string;
  planHash: string;
  approvalName: string;
  ticket?: string;
  /** The request key: the same click, retried, carries the same one. */
  key: string;
}

/** The Job's deadline: 15 minutes, as the design's example (60 to 86400 s). */
export const DEADLINE_SECONDS = 900;

export function useCreateSwitchover(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (c: CreateSwitchover) =>
      call(() =>
        api.POST("/api/v1/namespaces/{ns}/switchovers", {
          params: { path: { ns }, header: createHeaders(c.key) },
          body: {
            planBytes: c.planBytes,
            planHash: c.planHash,
            approvalRef: { name: c.approvalName },
            restoreRef: c.restore,
            deadlineSeconds: DEADLINE_SECONDS,
            ...(c.ticket ? { ticket: c.ticket } : {}),
          },
        }),
      ),
    onSuccess: () => client.invalidateQueries({ queryKey: keys.list("switchovers", ns) }),
  });
}

/** The request key and Approval name of one plan: the same plan and attempt give the same
 *  pair (a retried click replays), a new attempt (a re-run) gives a new pair. */
export function mintNames(planHash: string, attempt: string): { key: string; approvalName: string } {
  const hex = planHash.replace(/^sha256:/, "");
  const tag = attempt.replace(/[^a-z0-9-]/g, "").slice(0, 40);
  return {
    key: `logweir-console.switchover.${hex.slice(0, 32)}.${tag || "first"}`,
    approvalName: `swo-approval-${hex.slice(0, 8)}${tag && tag !== "first" ? `-${tag.slice(-12)}` : ""}`.replace(/-+$/, ""),
  };
}

// --------------------------------------------------- approvals (R6's pages)

/** A two-person switchover request, as the second person is shown it. */
export function useSwitchoverApprovalRequest(ns: string, name: string, enabled = true) {
  const readable = useCan(ns, "approvalsRead");
  return useQuery({
    queryKey: ["switchover-approval-requests", ns, name],
    enabled: enabled && readable,
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/switchovers/{name}/approval-request", { params: { path: { ns, name } }, signal }))).item,
    retry: false,
  });
}

/** The second person's click: names only the request they were shown. */
export function useApproveSwitchover(ns: string, name: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (confirmationSha256: string) =>
      call(() =>
        api.POST("/api/v1/namespaces/{ns}/switchovers/{name}/console-approval", {
          params: { path: { ns, name }, header: { Origin: window.location.origin } },
          body: { confirmationSha256 },
        }),
      ),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ["switchover-approval-requests", ns, name] });
      void client.invalidateQueries({ queryKey: keys.list("switchovers", ns) });
      void client.invalidateQueries({ queryKey: keys.list("approvals", ns) });
    },
  });
}
