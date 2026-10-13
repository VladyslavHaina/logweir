// The mock's stand-in for weirkeeper::cadence (crates/weirkeeper/src/cadence.rs).
//
// The console itself evaluates no cron: in the live build a draft's preview is
// GET /api/v1/cadence-previews and a saved schedule's next runs are its
// status.nextRuns, both computed by the Rust module. This file only lets the
// mock build answer those the way the controller would. cadence.test.ts holds
// it to the Rust module's own worked examples (tests/cadence.rs): a repeated
// local time fires at both instants, a nonexistent one fires once at the end of
// the gap, an interval keeps its UTC cadence, and a month without the day is
// skipped.

export type Adjustment = "NonexistentLocalTimeShifted" | "RepeatedLocalTimeFirst" | "RepeatedLocalTimeSecond";

export interface NextRun {
  at: string;
  localTime: string;
  adjustment?: Adjustment;
}

export type PresetKind = "hourly" | "everyNHours" | "daily" | "weekly" | "monthly";
export interface Preset {
  kind: PresetKind;
  minute: number;
  hour?: number;
  dayOfWeek?: number;
  dayOfMonth?: number;
  n?: number;
}

/** A refusal in the API's shape: the field it names and the code. */
export class CadenceError extends Error {
  constructor(
    readonly field: string,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

interface Cron {
  minutes: boolean[];
  hours: boolean[];
  dom: boolean[];
  months: boolean[];
  dow: boolean[];
  domRestricted: boolean;
  dowRestricted: boolean;
}

const AT_FORMS: Record<string, string> = { "@hourly": "0 * * * *", "@daily": "0 0 * * *", "@weekly": "0 0 * * 0" };
const FIELDS = [
  ["minute", 0, 59],
  ["hour", 0, 23],
  ["day-of-month", 1, 31],
  ["month", 1, 12],
  ["day-of-week", 0, 6],
] as const;

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;
/** slot.rs WALK_DAYS: how far a walk looks before it says "no further firing". */
const WALK_DAYS = 3700;
/** cadence.rs MAX_OFFSET_HOURS, EDGE_DAYS and GAP_SEARCH_MINUTES. */
const MAX_OFFSET = 26 * HOUR;
const EDGE_DAYS = 4;
const GAP_SEARCH_MINUTES = 48 * 60;

function term(text: string, min: number, max: number): number[] {
  const number = (raw: string) => {
    if (!/^\d+$/.test(raw)) throw new Error("is not a number, a range or a step");
    const value = Number(raw);
    if (value < min || value > max) throw new Error(`is outside ${min}..=${max}`);
    return value;
  };
  if (text === "*") return range(min, max);
  if (text.startsWith("*/")) {
    const step = text.slice(2);
    if (!/^\d+$/.test(step)) throw new Error("is not a number, a range or a step");
    if (Number(step) === 0) throw new Error("has a zero step");
    return range(min, max).filter((v) => (v - min) % Number(step) === 0);
  }
  const dash = text.indexOf("-");
  if (dash > 0) {
    const from = number(text.slice(0, dash));
    const to = number(text.slice(dash + 1));
    if (from > to) throw new Error("is a descending range");
    return range(from, to);
  }
  return [number(text)];
}

function range(from: number, to: number): number[] {
  return Array.from({ length: to - from + 1 }, (_, i) => from + i);
}

/** slot.rs Cron::parse: five fields or one of three `@` forms, no lenient path. */
export function parseCron(expression: string): Cron {
  const text = expression.trim();
  const expanded = text.startsWith("@") ? AT_FORMS[text] : text;
  if (expanded === undefined) {
    throw new CadenceError("schedule", "schedule_invalid", `unrecognised cron form "${text}"; only @hourly, @daily and @weekly are accepted`);
  }
  const fields = expanded.split(/\s+/).filter(Boolean);
  if (fields.length !== 5) {
    throw new CadenceError(
      "schedule",
      "schedule_invalid",
      `a cron expression has five fields (minute hour day-of-month month day-of-week); got ${fields.length}`,
    );
  }
  const sets = fields.map((field, index) => {
    const [name, min, max] = FIELDS[index]!;
    const bits = new Array<boolean>(max + 1).fill(false);
    for (const part of field.split(",")) {
      try {
        if (part.includes("+")) throw new Error("is not a number, a range or a step");
        for (const v of term(part, min, max)) bits[v] = true;
      } catch (e) {
        throw new CadenceError("schedule", "schedule_invalid", `cron field ${index} (${name}): "${part}" ${(e as Error).message}`);
      }
    }
    return bits;
  });
  return {
    minutes: sets[0]!,
    hours: sets[1]!,
    dom: sets[2]!,
    months: sets[3]!,
    dow: sets[4]!,
    domRestricted: !fields[2]!.startsWith("*"),
    dowRestricted: !fields[4]!.startsWith("*"),
  };
}

/** Vixie's day rule: both day fields restricted means either may match. */
function matchesDate(cron: Cron, day: Date): boolean {
  if (!cron.months[day.getUTCMonth() + 1]) return false;
  const dom = cron.dom[day.getUTCDate()] === true;
  const dow = cron.dow[day.getUTCDay()] === true;
  return cron.domRestricted && cron.dowRestricted ? dom || dow : dom && dow;
}

// --------------------------------------------------------------- the zone

const formats = new Map<string, Intl.DateTimeFormat>();

function format(zone: string): Intl.DateTimeFormat {
  let f = formats.get(zone);
  if (!f) {
    f = new Intl.DateTimeFormat("en-US", {
      timeZone: zone,
      hourCycle: "h23",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      era: "short",
    });
    formats.set(zone, f);
  }
  return f;
}

/** Resolve `timeZone`, refusing an unknown name as the API does (never UTC by accident). */
export function checkZone(zone: string | undefined): string {
  if (zone === undefined) return "UTC";
  try {
    format(zone);
    return zone;
  } catch {
    throw new CadenceError(
      "timeZone",
      "timezone_unknown",
      `"${zone}" is not a time zone in chrono-tz 0.10.4; spec.timeZone takes an IANA name such as Europe/Berlin, and an absent field means UTC`,
    );
  }
}

/** The wall time of instant `ms` in `zone`, as milliseconds read as if UTC. */
function wall(zone: string, ms: number): number {
  const parts: Record<string, string> = {};
  for (const p of format(zone).formatToParts(new Date(ms))) parts[p.type] = p.value;
  let year = Number(parts.year);
  if (parts.era === "BC" || parts.era === "B") year = 1 - year;
  // Year, month and day in one call: setting the year alone first would roll
  // 29 February into March in a non-leap placeholder year.
  const date = new Date(0);
  date.setUTCFullYear(year, Number(parts.month) - 1, Number(parts.day));
  date.setUTCHours(Number(parts.hour), Number(parts.minute), Number(parts.second), 0);
  return date.getTime();
}

function offsetOf(zone: string, ms: number): number {
  const at = Math.floor(ms / 1000) * 1000;
  return wall(zone, at) - at;
}

/** Every instant whose wall time in `zone` is `local` (0, 1 or 2 of them). */
function instantsOf(zone: string, local: number): number[] {
  const offsets = new Set([offsetOf(zone, local - MAX_OFFSET), offsetOf(zone, local), offsetOf(zone, local + MAX_OFFSET)]);
  const found = new Set<number>();
  for (const offset of offsets) {
    const at = local - offset;
    if (offsetOf(zone, at) === offset) found.add(at);
  }
  return [...found].sort((a, b) => a - b);
}

/** cadence.rs gap_end: the first real local minute after a nonexistent one. */
function gapEnd(zone: string, local: number): number | undefined {
  for (let step = 1; step <= GAP_SEARCH_MINUTES; step += 1) {
    const at = instantsOf(zone, local + step * MINUTE)[0];
    if (at !== undefined) return at;
  }
  return undefined;
}

function rank(adjustment: Adjustment | undefined): number {
  if (adjustment === undefined) return 0;
  return adjustment === "NonexistentLocalTimeShifted" ? 2 : 1;
}

function pad(n: number, width = 2): string {
  return String(Math.abs(n)).padStart(width, "0");
}

/** Zone::render_local: the wall time with its offset, `2026-10-25T02:30:00+02:00`. */
export function renderLocal(zone: string, ms: number): string {
  const w = new Date(wall(zone, ms));
  const offset = Math.round((w.getTime() - Math.floor(ms / 1000) * 1000) / MINUTE);
  const sign = offset < 0 ? "-" : "+";
  return (
    `${pad(w.getUTCFullYear(), 4)}-${pad(w.getUTCMonth() + 1)}-${pad(w.getUTCDate())}` +
    `T${pad(w.getUTCHours())}:${pad(w.getUTCMinutes())}:${pad(w.getUTCSeconds())}` +
    `${sign}${pad(Math.floor(Math.abs(offset) / 60))}:${pad(Math.abs(offset) % 60)}`
  );
}

function iso(ms: number): string {
  return new Date(ms).toISOString().replace(/\.\d{3}Z$/, "Z");
}

/** Cadence::next_runs: up to `count` firings strictly after `after`, ascending. */
export function nextRuns(expression: string, timeZone: string | undefined, after: number, count: number): NextRun[] {
  const cron = parseCron(expression);
  const zone = checkZone(timeZone);
  const found = new Map<number, Adjustment | undefined>();
  const record = (at: number, adjustment: Adjustment | undefined) => {
    if (at <= after) return;
    if (!found.has(at) || rank(adjustment) < rank(found.get(at))) found.set(at, adjustment);
  };
  const start = Math.floor(wall(zone, after) / DAY) * DAY - EDGE_DAYS * DAY;
  const horizon = start + (WALK_DAYS + EDGE_DAYS) * DAY;
  for (let day = start; day <= horizon; day += DAY) {
    if (found.size >= count) {
      const nth = [...found.keys()].sort((a, b) => a - b)[count - 1]!;
      if (day - MAX_OFFSET > nth) break;
    }
    if (!matchesDate(cron, new Date(day))) continue;
    for (let h = 0; h < 24; h += 1) {
      if (!cron.hours[h]) continue;
      for (let m = 0; m < 60; m += 1) {
        if (!cron.minutes[m]) continue;
        const local = day + h * HOUR + m * MINUTE;
        const instants = instantsOf(zone, local);
        if (instants.length === 1) record(instants[0]!, undefined);
        else if (instants.length === 2) {
          record(instants[0]!, "RepeatedLocalTimeFirst");
          record(instants[1]!, "RepeatedLocalTimeSecond");
        } else {
          const end = gapEnd(zone, local);
          if (end !== undefined) record(end, "NonexistentLocalTimeShifted");
        }
      }
    }
  }
  return [...found.entries()]
    .sort((a, b) => a[0] - b[0])
    .slice(0, count)
    .map(([at, adjustment]) => ({ at: iso(at), localTime: renderLocal(zone, at), ...(adjustment ? { adjustment } : {}) }));
}

// ---------------------------------------------------------------- presets

const PARAMS: Record<PresetKind, [keyof Preset, number, number, number[]?][]> = {
  hourly: [["minute", 0, 59]],
  everyNHours: [
    ["n", 2, 12, [2, 3, 4, 6, 8, 12]],
    ["minute", 0, 59],
  ],
  daily: [
    ["hour", 0, 23],
    ["minute", 0, 59],
  ],
  weekly: [
    ["dayOfWeek", 0, 6],
    ["hour", 0, 23],
    ["minute", 0, 59],
  ],
  monthly: [
    ["dayOfMonth", 1, 28],
    ["hour", 0, 23],
    ["minute", 0, 59],
  ],
};

/** presets::compile: the canonical expression a preset IS, or the API's refusal. */
export function compilePreset(p: Preset): string {
  for (const [name, min, max, values] of PARAMS[p.kind]) {
    const v = p[name];
    if (typeof v !== "number") throw new CadenceError(name, "required", `preset ${p.kind} requires ${name}`);
    if (v < min || v > max || (values && !values.includes(v))) {
      throw new CadenceError(name, "out_of_range", `${name} is ${v}; accepted values are ${values ? values.join(", ") : `${min}..=${max}`}`);
    }
  }
  switch (p.kind) {
    case "hourly":
      return `${p.minute} * * * *`;
    case "everyNHours":
      return `${p.minute} */${p.n} * * *`;
    case "daily":
      return `${p.minute} ${p.hour} * * *`;
    case "weekly":
      return `${p.minute} ${p.hour} * * ${p.dayOfWeek}`;
    case "monthly":
      return `${p.minute} ${p.hour} ${p.dayOfMonth} * *`;
  }
}

/** presets::match_preset: the preset an expression IS, only for the canonical spelling. */
export function matchPreset(expression: string): Preset | undefined {
  const text = AT_FORMS[expression.trim()] ?? expression.trim();
  const n = String.raw`(0|[1-9]\d?)`;
  const tests: [RegExp, (m: number[]) => Preset][] = [
    [new RegExp(`^${n} \\* \\* \\* \\*$`), ([minute]) => ({ kind: "hourly", minute: minute! })],
    [new RegExp(`^${n} \\*/${n} \\* \\* \\*$`), ([minute, every]) => ({ kind: "everyNHours", minute: minute!, n: every! })],
    [new RegExp(`^${n} ${n} \\* \\* \\*$`), ([minute, hour]) => ({ kind: "daily", minute: minute!, hour: hour! })],
    [new RegExp(`^${n} ${n} \\* \\* ${n}$`), ([minute, hour, dow]) => ({ kind: "weekly", minute: minute!, hour: hour!, dayOfWeek: dow! })],
    [new RegExp(`^${n} ${n} ${n} \\* \\*$`), ([minute, hour, dom]) => ({ kind: "monthly", minute: minute!, hour: hour!, dayOfMonth: dom! })],
  ];
  for (const [re, build] of tests) {
    const m = re.exec(text);
    if (!m) continue;
    const preset = build(m.slice(1).map(Number));
    try {
      return compilePreset(preset) === text ? preset : undefined;
    } catch {
      return undefined;
    }
  }
  return undefined;
}
