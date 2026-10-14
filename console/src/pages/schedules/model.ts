// The schedule form and the facts the schedule pages derive, as plain functions
// with no React in them, so each rule is tested once (model.test.ts).
//
// One form for create and edit: the edit route (PUT .../schedules/{name})
// replaces the WHOLE future policy, so it shows and sends every field the
// create sends, minus the source, which never changes.
import { z } from "zod";
import type { components } from "../../api/schema";
import type { Backup } from "../../api/types";
import { ApiError, outcomeUnknown } from "../../api/client";
import {
  type CadenceQuery,
  type CreateScheduleRequest,
  type DestinationSummary,
  type RetentionPolicy,
  type Schedule,
  type UpdateSchedulePolicyRequest,
} from "../../api/schedules";

// ---------------------------------------------------------------- cadence

export type PresetKind = "hourly" | "everyNHours" | "daily" | "weekly" | "monthly";
export type CadenceMode = PresetKind | "custom";
type Param = "minute" | "hour" | "dayOfWeek" | "dayOfMonth" | "n";

/** The five presets as display data. The API compiles a preset to its cron
 *  line (the console holds no template), and that line is what is saved. */
export const PRESETS: { kind: PresetKind; label: string; params: Param[] }[] = [
  { kind: "hourly", label: "Every hour", params: ["minute"] },
  { kind: "everyNHours", label: "Every few hours", params: ["n", "minute"] },
  { kind: "daily", label: "Every day", params: ["hour", "minute"] },
  { kind: "weekly", label: "Every week", params: ["dayOfWeek", "hour", "minute"] },
  { kind: "monthly", label: "Every month", params: ["dayOfMonth", "hour", "minute"] },
];

/** The parameter bounds `console/fixtures/cadence-presets.json` (Rust's catalogue) states. */
export const BOUNDS: Record<Param, { min: number; max: number; values?: number[] }> = {
  minute: { min: 0, max: 59 },
  hour: { min: 0, max: 23 },
  dayOfWeek: { min: 0, max: 6 },
  dayOfMonth: { min: 1, max: 28 },
  n: { min: 2, max: 12, values: [2, 3, 4, 6, 8, 12] },
};

export const DAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

/** How many firings a draft preview asks for: what the controller keeps in status.nextRuns. */
export const PREVIEW_COUNT = 5;

// ------------------------------------------------------------------ values

export const NUMBER_FIELDS = ["startingDeadlineSeconds", "maxRetries", "retryDelaySeconds", "activeDeadlineSeconds"] as const;

const optionalWhole = (min: number, max: number) =>
  z.string().refine((v) => v.trim() === "" || (/^\d+$/.test(v.trim()) && Number(v) >= min && Number(v) <= max), {
    message: `A whole number from ${min} to ${max}, or blank for the default.`,
  });

const GLOB = /[*?[\]{}]/;
const CRON_SHAPE = /^([0-9*,/-]+\s+){4}[0-9*,/-]+$/;

export const policySchema = z
  .object({
    source: z.string(),
    mode: z.enum(["hourly", "everyNHours", "daily", "weekly", "monthly", "custom"]),
    cron: z.string(),
    minute: z.string(),
    hour: z.string(),
    dayOfWeek: z.string(),
    dayOfMonth: z.string(),
    n: z.string(),
    timeZone: z.string(),
    selection: z.enum(["named", "all"]),
    topics: z.string(),
    incompleteDiscovery: z.enum(["", "Refuse", "BackUpVisibleTopics"]),
    excludeTopics: z.string(),
    excludePrefixes: z.string(),
    consumerGroups: z.string(),
    where: z.enum(["destination", "archive"]),
    destination: z.string(),
    archiveUrl: z.string(),
    archiveSecret: z.string(),
    suspended: z.boolean(),
    concurrencyPolicy: z.enum(["", "Forbid", "Allow"]),
    startingDeadlineSeconds: optionalWhole(60, 604_800),
    catchUpPolicy: z.enum(["", "None", "Latest"]),
    maxRetries: optionalWhole(0, 3),
    retryDelaySeconds: optionalWhole(60, 21_600),
    activeDeadlineSeconds: optionalWhole(60, 86_400),
    confirmMove: z.boolean(),
  })
  .superRefine((v, ctx) => {
    const issue = (path: string, message: string) => ctx.addIssue({ code: "custom", path: [path], message });
    if (v.source.trim() === "") issue("source", "Choose the connection this schedule reads from.");
    if (v.mode === "custom") {
      const cron = v.cron.trim();
      if (!["@hourly", "@daily", "@weekly"].includes(cron) && !CRON_SHAPE.test(cron)) {
        issue("cron", "Five cron fields (minute hour day-of-month month day-of-week), or @hourly, @daily or @weekly.");
      }
    } else {
      for (const p of PRESETS.find((x) => x.kind === v.mode)!.params) {
        const raw = v[p].trim();
        const b = BOUNDS[p];
        if (!/^\d+$/.test(raw) || Number(raw) < b.min || Number(raw) > b.max) issue(p, `From ${b.min} to ${b.max}.`);
        else if (b.values && !b.values.includes(Number(raw))) issue(p, `One of ${b.values.join(", ")}.`);
      }
    }
    if (v.selection === "named") {
      const topics = list(v.topics);
      if (topics.length === 0) issue("topics", "Name at least one topic; an empty list backs up nothing.");
      else if (topics.length > 256) issue("topics", "At most 256 named topics.");
      else {
        const globbed = topics.filter((t) => GLOB.test(t));
        if (globbed.length) issue("topics", `Names, never patterns: ${globbed.join(", ")}.`);
      }
    } else if (v.incompleteDiscovery === "") {
      issue("incompleteDiscovery", "Choose what a run does when discovery cannot prove it saw every topic.");
    }
    const groupsWhy = groupsRefusal(groupList(v.consumerGroups));
    if (groupsWhy) issue("consumerGroups", groupsWhy);
    if (v.where === "destination" && v.destination === "") issue("destination", "Choose a saved destination.");
    if (v.where === "archive" && !/^[a-z0-9]+:\/\/./.test(v.archiveUrl.trim())) {
      issue("archiveUrl", "An object-store URL with its scheme, such as s3://bucket/prefix.");
    }
  });

export type PolicyValues = z.infer<typeof policySchema>;

/** A guided draft: a preset at 02:00 every day, everything optional left blank,
 *  because a blank field means the documented default and a prefilled one
 *  would write that default into the object. */
export function draftValues(destinations: DestinationSummary[] | undefined): PolicyValues {
  const defaults = (destinations ?? []).filter((d) => d.default);
  return {
    source: "",
    mode: "daily",
    cron: "",
    minute: "0",
    hour: "2",
    dayOfWeek: "1",
    dayOfMonth: "1",
    n: "6",
    timeZone: "",
    selection: "named",
    topics: "",
    incompleteDiscovery: "",
    excludeTopics: "",
    excludePrefixes: "",
    consumerGroups: "",
    where: "destination",
    // Two defaults are no default: nothing is preselected then.
    destination: defaults.length === 1 ? defaults[0]!.name : "",
    archiveUrl: "",
    archiveSecret: "",
    suspended: false,
    concurrencyPolicy: "",
    startingDeadlineSeconds: "",
    catchUpPolicy: "",
    maxRetries: "",
    retryDelaySeconds: "",
    activeDeadlineSeconds: "",
    confirmMove: false,
  };
}

const num = (v: number | null | undefined) => (typeof v === "number" ? String(v) : "");

/** The form a saved schedule opens on. The preset is the server's match, never ours. */
export function valuesOf(s: Schedule): PolicyValues {
  const p = s.preset;
  const exclude = s.allUserTopics?.exclude;
  return {
    source: s.sourceRef.name,
    mode: p ? p.kind : "custom",
    cron: s.schedule,
    minute: p ? String(p.minute) : "0",
    hour: p && "hour" in p ? String(p.hour) : "2",
    dayOfWeek: p && "dayOfWeek" in p ? String(p.dayOfWeek) : "1",
    dayOfMonth: p && "dayOfMonth" in p ? String(p.dayOfMonth) : "1",
    n: p && "n" in p ? String(p.n) : "6",
    timeZone: s.timeZone ?? "",
    selection: s.allUserTopics ? "all" : "named",
    topics: s.topics.join(", "),
    incompleteDiscovery: s.allUserTopics?.incompleteDiscovery ?? "",
    excludeTopics: (exclude?.topics ?? []).join(", "),
    excludePrefixes: (exclude?.prefixes ?? []).join(", "),
    consumerGroups: (s.consumerGroups ?? []).join("\n"),
    where: s.destinationRef ? "destination" : "archive",
    destination: s.destinationRef?.name ?? "",
    archiveUrl: s.destinationRef ? "" : s.archive.url,
    archiveSecret: s.destinationRef ? "" : (s.archive.credentialRef?.name ?? ""),
    suspended: s.suspended,
    concurrencyPolicy: s.concurrencyPolicy,
    startingDeadlineSeconds: num(s.startingDeadlineSeconds),
    catchUpPolicy: s.catchUpPolicy ?? "",
    maxRetries: num(s.retry?.maxRetries),
    retryDelaySeconds: num(s.retry?.delaySeconds),
    activeDeadlineSeconds: num(s.activeDeadlineSeconds),
    confirmMove: false,
  };
}

// ---------------------------------------------------------- consumer groups

/** The runner's bounds on `spec.consumerGroups` (consumer_positions.rs). */
export const GROUP_BOUNDS = { count: 100, idBytes: 255 } as const;

/** One group id per line: a group id may hold a comma or a space, never a line break
 *  (a control character, which the runner refuses). Blank lines are dropped. */
export function groupList(text: string): string[] {
  return text
    .split(/\r?\n/)
    .map((g) => g.trim())
    .filter(Boolean);
}

/** Why the runner would refuse this selection before any Job, or null (DUI-25). The API
 *  checks it again, the receipt's 80 KiB cap included. */
export function groupsRefusal(groups: string[]): string | null {
  if (groups.length > GROUP_BOUNDS.count) return `At most ${GROUP_BOUNDS.count} consumer groups; this names ${groups.length}.`;
  const seen = new Set<string>();
  for (const g of groups) {
    // eslint-disable-next-line no-control-regex
    if (/[\u0000-\u001f\u007f-\u009f]/.test(g)) return `A group id carries a control character: ${JSON.stringify(g.slice(0, 64))}.`;
    if (new TextEncoder().encode(g).length > GROUP_BOUNDS.idBytes) return `A group id is longer than ${GROUP_BOUNDS.idBytes} bytes: ${g.slice(0, 64)}…`;
    if (seen.has(g)) return `${g} is listed twice; name each group once.`;
    seen.add(g);
  }
  return null;
}

export function list(text: string): string[] {
  return text
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);
}

/** The preview a draft asks for: exactly one of `schedule` or `preset`, and
 *  only the named preset's own parameters (the API refuses any other). */
export function previewQuery(v: Pick<PolicyValues, "mode" | "cron" | Param | "timeZone">): CadenceQuery | null {
  const query: CadenceQuery = { count: PREVIEW_COUNT };
  if (v.timeZone.trim()) query.timeZone = v.timeZone.trim();
  if (v.mode === "custom") {
    if (!v.cron.trim()) return null;
    return { ...query, schedule: v.cron.trim() };
  }
  const preset = PRESETS.find((p) => p.kind === v.mode)!;
  const params: Partial<Record<Param, number>> = {};
  for (const name of preset.params) {
    const raw = v[name].trim();
    if (!/^\d+$/.test(raw)) return null;
    params[name] = Number(raw);
  }
  // An out-of-range value is still sent: the API's refusal names the parameter.
  return { ...query, ...(params as Omit<CadenceQuery, "n"> & { n?: 2 | 3 | 4 | 6 | 8 | 12 }), preset: v.mode };
}

const whole = (v: string) => (v.trim() === "" ? undefined : Number(v));

/** The policy half both routes share. `schedule` is the line the API compiled a
 *  preset to, or the typed line for a custom cadence. Blank optional fields are
 *  left out: absent means the documented default. Retention is withdrawn and
 *  never sent. */
function policy(v: PolicyValues, schedule: string) {
  const exclude = { topics: list(v.excludeTopics), prefixes: list(v.excludePrefixes) };
  const allUserTopics =
    v.selection === "all"
      ? {
          incompleteDiscovery: v.incompleteDiscovery as "Refuse" | "BackUpVisibleTopics",
          ...(exclude.topics.length || exclude.prefixes.length
            ? {
                exclude: {
                  ...(exclude.topics.length ? { topics: exclude.topics } : {}),
                  ...(exclude.prefixes.length ? { prefixes: exclude.prefixes } : {}),
                },
              }
            : {}),
        }
      : undefined;
  const maxRetries = whole(v.maxRetries);
  const delay = whole(v.retryDelaySeconds);
  return {
    schedule,
    suspended: v.suspended,
    allUserTopics,
    topics: v.selection === "named" ? list(v.topics) : [],
    ...(v.timeZone.trim() ? { timeZone: v.timeZone.trim() } : {}),
    // One location, never both: a destination is sent by name and the API
    // writes the CRD's sentinel URL itself.
    ...(v.where === "destination"
      ? { destinationRef: { name: v.destination } }
      : {
          archive: {
            url: v.archiveUrl.trim(),
            ...(v.archiveSecret.trim() ? { credentialRef: { name: v.archiveSecret.trim() } } : {}),
          },
        }),
    ...(v.concurrencyPolicy ? { concurrencyPolicy: v.concurrencyPolicy } : {}),
    ...(whole(v.startingDeadlineSeconds) !== undefined ? { startingDeadlineSeconds: whole(v.startingDeadlineSeconds) } : {}),
    ...(v.catchUpPolicy ? { catchUpPolicy: v.catchUpPolicy } : {}),
    ...(maxRetries !== undefined ? { retry: { maxRetries, ...(delay !== undefined ? { delaySeconds: delay } : {}) } } : {}),
    ...(whole(v.activeDeadlineSeconds) !== undefined ? { activeDeadlineSeconds: whole(v.activeDeadlineSeconds) } : {}),
  };
}

export function createBody(v: PolicyValues, schedule: string): CreateScheduleRequest {
  const { allUserTopics, ...rest } = policy(v, schedule);
  // The groups are set on create only (DUI-25): the edit route keeps the stored ones.
  const consumerGroups = groupList(v.consumerGroups);
  return { ...rest, sourceRef: { name: v.source }, ...(allUserTopics ? { allUserTopics } : {}), ...(consumerGroups.length ? { consumerGroups } : {}) };
}

/** The whole future policy under the revision the form was opened at. `sourceRef` is never sent. */
export function editBody(v: PolicyValues, schedule: string, expectedGeneration: number): UpdateSchedulePolicyRequest {
  const { allUserTopics, topics, ...rest } = policy(v, schedule);
  return { ...rest, expectedGeneration, topicSelection: allUserTopics ? { topics: [], allUserTopics } : { topics } };
}

/** Where a set of values writes, for the move check: by destination name, or by URL. */
export function locationOf(v: Pick<PolicyValues, "where" | "destination" | "archiveUrl">): string {
  return v.where === "destination" ? `destination ${v.destination}` : v.archiveUrl.trim();
}

/** Whether an edit moves the archive, which needs its own explicit box. */
export function movesLocation(saved: Schedule, v: PolicyValues): boolean {
  return locationOf(valuesOf(saved)) !== locationOf(v);
}

// ------------------------------------------------------------- refusals

/** The API's field paths to this form's inputs. The edit route nests the
 *  selection under `topicSelection`; the create route spells it flat. */
const FIELD_PATHS: [string, keyof PolicyValues][] = [
  ["", "topics"],
  ["schedule", "cron"],
  ["timeZone", "timeZone"],
  ["sourceRef", "source"],
  ["topics", "topics"],
  ["allUserTopics", "incompleteDiscovery"],
  ["allUserTopics.exclude.topics", "excludeTopics"],
  ["allUserTopics.exclude.prefixes", "excludePrefixes"],
  ["consumerGroups", "consumerGroups"],
  ["archive", "archiveUrl"],
  ["archive.credentialRef", "archiveSecret"],
  ["destinationRef", "destination"],
  ["concurrencyPolicy", "concurrencyPolicy"],
  ["startingDeadlineSeconds", "startingDeadlineSeconds"],
  ["catchUpPolicy", "catchUpPolicy"],
  ["retry", "maxRetries"],
  ["retry.delaySeconds", "retryDelaySeconds"],
  ["activeDeadlineSeconds", "activeDeadlineSeconds"],
  ["minute", "minute"],
  ["hour", "hour"],
  ["dayOfWeek", "dayOfWeek"],
  ["dayOfMonth", "dayOfMonth"],
  ["n", "n"],
];

/** The input a field path names: the path itself or a path under it
 *  (`topics[2]`, `topicSelection.allUserTopics.exclude.prefixes`); the longest match wins. */
export function fieldOf(path: string): keyof PolicyValues | undefined {
  const normal = path.replace(/\[\d+\]/g, "").replace(/^topicSelection\.?/, "");
  let best: [string, keyof PolicyValues] | undefined;
  for (const entry of FIELD_PATHS) {
    const [prefix] = entry;
    const hit = prefix === "" ? normal === "" : normal === prefix || normal.startsWith(`${prefix}.`);
    if (hit && (!best || prefix.length > best[0].length)) best = entry;
  }
  return best?.[1];
}

/** A refusal's field errors, by input; what no input claims is listed apart. */
export function placeFieldErrors(error: unknown): { fields: Partial<Record<keyof PolicyValues, string>>; unmatched: string[] } {
  const fields: Partial<Record<keyof PolicyValues, string>> = {};
  const unmatched: string[] = [];
  if (error instanceof ApiError) {
    for (const e of error.fieldErrors) {
      const field = fieldOf(e.field);
      if (field && !fields[field]) fields[field] = e.message;
      else if (!field) unmatched.push(`${e.field}: ${e.message}`);
    }
  }
  return { fields, unmatched };
}

export type Outcome = "unknown" | "invalid" | "conflict" | "precondition" | "refused";

/** What a failed write means. No answer, a timeout or a 5xx is UNKNOWN: the
 *  object may or may not exist, and only resending the same request can tell. */
export function outcomeOf(error: unknown): Outcome {
  if (outcomeUnknown(error)) return "unknown";
  // Not sent at all (a LocalRefusal): known, and the page says why.
  if (!(error instanceof ApiError)) return "refused";
  if (error.status === 412) return "precondition";
  if (error.status === 409) return "conflict";
  if (error.status === 400 || error.status === 422) return "invalid";
  return "refused";
}

// ---------------------------------------------------------------- words

const two = (n: number) => String(n).padStart(2, "0");

/** A cadence in words, from the server's preset match or the expression itself. */
export function cadenceWords(preset: Schedule["preset"] | undefined | null, expression: string): string {
  if (!preset) return `Custom: ${expression}`;
  switch (preset.kind) {
    case "hourly":
      return `Every hour at minute ${preset.minute}`;
    case "everyNHours":
      return `Every ${preset.n} hours at minute ${preset.minute}`;
    case "daily":
      return `Every day at ${two(preset.hour)}:${two(preset.minute)}`;
    case "weekly":
      return `Every ${DAYS[preset.dayOfWeek] ?? `day ${preset.dayOfWeek}`} at ${two(preset.hour)}:${two(preset.minute)}`;
    case "monthly":
      return `Day ${preset.dayOfMonth} of every month at ${two(preset.hour)}:${two(preset.minute)}`;
  }
}

/** The zone a schedule's cron fields are read in: what the controller recorded, else the spec, else UTC. */
export function zoneOf(s: Schedule): string {
  return s.status.policy?.timeZone ?? s.timeZone ?? "UTC";
}

export type ScheduleState = { tone: "success" | "warning" | "danger" | "neutral"; text: string; title?: string };

/** One status per schedule. Suspended first (it explains everything else),
 *  then the controller's own Ready condition, then staleness. */
export function scheduleState(s: Schedule, now = Date.now()): ScheduleState {
  if (s.suspended) return { tone: "neutral", text: "Suspended", title: "spec.suspend is true: no slot is admitted" };
  const ready = s.status.ready;
  if (!ready) return { tone: "neutral", text: "Not evaluated yet" };
  const title = `Ready=${ready.status}${ready.reason ? ` ${ready.reason}` : ""}`;
  if (ready.status === "False") return { tone: "danger", text: "Not ready", title };
  if (ready.status !== "True") return { tone: "warning", text: "Unknown", title };
  if (stale(s, now)) return { tone: "warning", text: "Not being evaluated", title: "the first of status.nextRuns is in the past" };
  return { tone: "success", text: "Active", title };
}

/** The staleness signal: the first next run in the past. evaluatedAt is NOT one. */
export function stale(s: Schedule, now = Date.now()): boolean {
  const first = s.status.nextRuns?.[0];
  return Boolean(first && Date.parse(first.at) < now);
}

/** "2026-10-25T02:30:00+02:00" as "2026-10-25 02:30 +02:00": the wall time, read as written. */
export function localWords(localTime: string): string {
  const m = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})(?::\d{2})?(Z|[+-]\d{2}:\d{2})$/.exec(localTime);
  if (!m) return localTime;
  return `${m[1]} ${m[2]} ${m[3] === "Z" ? "+00:00" : m[3]}`;
}

export const ADJUSTMENT_WORDS: Record<string, string> = {
  NonexistentLocalTimeShifted: "This time does not exist that day (clocks go forward), so the run is at the end of the gap.",
  RepeatedLocalTimeFirst: "This time happens twice that day (clocks go back): the first of two runs.",
  RepeatedLocalTimeSecond: "This time happens twice that day: the second of two runs.",
};

// ------------------------------------------------------------------ runs

/** A schedule's runs, newest first, by IDENTITY: a run naming this schedule
 *  under another uid belongs to an earlier schedule of the same name, and one
 *  with no uid cannot be placed, so neither is ever this schedule's point. */
export function runsOf(name: string, uid: string | undefined, backups: Backup[]) {
  const named = backups
    .filter((b) => b.scheduleRef?.name === name || (b.scheduleRef === undefined && b.schedule === name))
    .sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
  if (!uid) return { mine: [] as Backup[], earlier: named.filter((b) => b.scheduleRef?.uid), legacy: named.filter((b) => !b.scheduleRef?.uid) };
  return {
    mine: named.filter((b) => b.scheduleRef?.uid === uid),
    earlier: named.filter((b) => b.scheduleRef?.uid && b.scheduleRef.uid !== uid),
    legacy: named.filter((b) => !b.scheduleRef?.uid),
  };
}

/**
 * When this schedule last ran (DUI-31.2): its newest run of either kind. `lastFireTime` is
 * the scheduler's last slot only, so a Back up now run newer than it is the last run.
 * Undefined when neither is recorded.
 */
export function lastRunAt(s: Pick<Schedule, "name" | "uid" | "status">, backups: Backup[] | undefined): string | undefined {
  const fired = s.status.lastFireTime ?? undefined;
  const newest = backups ? runsOf(s.name, s.uid, backups).mine[0]?.createdAt ?? undefined : undefined;
  if (!fired) return newest;
  if (!newest) return fired;
  return Date.parse(newest) > Date.parse(fired) ? newest : fired;
}

/** The newest run of this schedule the controller verified: what "Restore the latest point" opens. */
export function latestPoint(mine: Backup[]): Backup | undefined {
  return mine.find((b) => b.operation.verifiedSuccess);
}

// ------------------------------------------------------------- retention

export const NEVER_DELETES = "Logweir never deletes from your archive.";

export const ENFORCEMENT_WORDS: Record<string, string> = {
  RecommendationOnly: "Logweir reports what would be removed under this policy and removes nothing.",
  LogweirWorker:
    "An isolated Logweir retention worker deletes archive objects under this policy, with its own delete-capable credential that this controller never reads.",
  ExternalLifecycleDeclared:
    "Deletion at this destination is performed by your bucket lifecycle rule; Logweir reports and cannot protect individual points here.",
};

export type RetentionCoverage =
  | { kind: "unread" }
  | { kind: "inline" }
  | { kind: "none"; elsewhere: RetentionPolicy[] }
  | { kind: "one"; policy: RetentionPolicy; elsewhere: RetentionPolicy[] }
  | { kind: "conflict"; policies: RetentionPolicy[] };

/** Which RetentionPolicy governs a schedule's destination, by the controller's
 *  own rule (it names the destination). "Logweir never deletes" is said only
 *  when the read succeeded and no policy that deletes could reach it. */
export function retentionOf(s: Schedule, policies: RetentionPolicy[] | undefined): RetentionCoverage {
  if (!policies) return { kind: "unread" };
  const destination = s.destinationRef?.name;
  if (!destination) return { kind: "inline" };
  const named = policies.filter((p) => p.destinationRef.name === destination);
  const deleting = policies.filter((p) => p.enforcement && p.enforcement !== "RecommendationOnly" && p.destinationRef.name !== destination);
  if (named.length > 1) return { kind: "conflict", policies: named };
  if (named.length === 1) return { kind: "one", policy: named[0]!, elsewhere: deleting };
  return { kind: "none", elsewhere: deleting };
}

/** Whether a covering policy deletes: by what is happening, then by the mode asked for. */
export function deletes(p: RetentionPolicy): boolean {
  return p.enforcement === "LogweirWorker" || p.enforcement === "ExternalLifecycleDeclared" || p.mode === "Enforce" || p.mode === "ExternalLifecycle";
}

// ------------------------------------------------- a READ grant the broker denies

type CheckRow = components["schemas"]["CheckEntryView"];

/**
 * FX-45: a Backup Preflight whose `connection.topicsReadable` is blocking `notReady`
 * `TopicAuthorizationFailed`: the broker REPORTS that the principal may not READ
 * some selected topics. The count and the bounded sample come from the row's
 * ` [detail: {"count":N,"sample":[...]}]`; null for any other row, which stays in
 * the execution-only list with its message.
 */
export function missingRead(row: CheckRow): { count: number; sample: string[]; message: string; remedy: string } | null {
  if (row.id !== "connection.topicsReadable" || row.state !== "notReady" || row.code !== "TopicAuthorizationFailed" || row.gating === "executionOnly") {
    return null;
  }
  const full = row.message ?? "";
  const at = full.lastIndexOf(" [detail: ");
  let sample: string[] = [];
  let count: number | null = null;
  if (at >= 0 && full.endsWith("]")) {
    try {
      const d = JSON.parse(full.slice(at + " [detail: ".length, -1)) as { count?: unknown; sample?: unknown };
      if (typeof d.count === "number" && Number.isInteger(d.count)) count = d.count;
      if (Array.isArray(d.sample)) sample = d.sample.filter((t): t is string => typeof t === "string");
    } catch {
      // Not a detail this page can read: the message is shown whole below.
    }
  }
  const message = count === null ? full : full.slice(0, at);
  count ??= Number(/may not READ (\d+) of/.exec(full)?.[1] ?? sample.length);
  return { count, sample, message, remedy: row.remedy ?? "" };
}
