// The mock's cadence against the controller's own worked examples. Every
// expected value below is copied from crates/weirkeeper/tests/cadence.rs (which
// runs the Rust module) or from the console fixtures the API produced
// (console/fixtures/console/cadence-preview-*.json), so the mock build previews
// the cadences the controller will actually run.
import { describe, expect, it } from "vitest";
import { compilePreset, matchPreset, nextRuns, parseCron, type Adjustment } from "./cadence";
import gap from "../../fixtures/console/cadence-preview-gap.json";
import repeated from "../../fixtures/console/cadence-preview-repeated.json";

const utc = (y: number, mo: number, d: number, h: number, mi: number) => Date.UTC(y, mo - 1, d, h, mi);
const slot = (at: string) => at.replace(/[-:]/g, "").replace("T", "-").replace("Z", "");

interface Case {
  what: string;
  zone: string;
  expr: string;
  after: number;
  slots: string[];
  markers: (Adjustment | undefined)[];
}

// tests/cadence.rs dst_cases(), the rows whose zones every tz database agrees on.
const DST: Case[] = [
  {
    what: "Berlin, fixed 02:30, the repeated hour of 2026-10-25: BOTH occurrences",
    zone: "Europe/Berlin",
    expr: "30 2 * * *",
    after: utc(2026, 10, 24, 12, 0),
    slots: ["20261025-003000", "20261025-013000", "20261026-013000"],
    markers: ["RepeatedLocalTimeFirst", "RepeatedLocalTimeSecond", undefined],
  },
  {
    what: "Berlin, fixed 02:30, the gap of 2027-03-28: ONCE, at the gap's end",
    zone: "Europe/Berlin",
    expr: "30 2 * * *",
    after: utc(2027, 3, 27, 12, 0),
    slots: ["20270328-010000", "20270329-003000"],
    markers: ["NonexistentLocalTimeShifted", undefined],
  },
  {
    what: "Berlin, */15 through the gap: a continuous UTC cadence, no burst",
    zone: "Europe/Berlin",
    expr: "*/15 * * * *",
    after: utc(2027, 3, 28, 0, 30),
    slots: ["20270328-004500", "20270328-010000", "20270328-011500", "20270328-013000"],
    markers: [undefined, undefined, undefined, undefined],
  },
  {
    what: "Berlin, */15 through the repeated hour: still a continuous UTC cadence",
    zone: "Europe/Berlin",
    expr: "*/15 * * * *",
    after: utc(2026, 10, 24, 23, 30),
    slots: ["20261024-234500", "20261025-000000", "20261025-001500", "20261025-003000", "20261025-004500", "20261025-010000"],
    markers: [
      undefined,
      "RepeatedLocalTimeFirst",
      "RepeatedLocalTimeFirst",
      "RepeatedLocalTimeFirst",
      "RepeatedLocalTimeFirst",
      "RepeatedLocalTimeSecond",
    ],
  },
  {
    what: "New York, fixed 01:30, the repeated hour of 2026-11-01",
    zone: "America/New_York",
    expr: "30 1 * * *",
    after: utc(2026, 10, 31, 12, 0),
    slots: ["20261101-053000", "20261101-063000", "20261102-063000"],
    markers: ["RepeatedLocalTimeFirst", "RepeatedLocalTimeSecond", undefined],
  },
  {
    what: "New York, fixed 02:30, the gap of 2027-03-14",
    zone: "America/New_York",
    expr: "30 2 * * *",
    after: utc(2027, 3, 13, 12, 0),
    slots: ["20270314-070000", "20270315-063000"],
    markers: ["NonexistentLocalTimeShifted", undefined],
  },
  {
    what: "Lord Howe, a THIRTY-minute gap on 2026-10-04",
    zone: "Australia/Lord_Howe",
    expr: "15 2 * * *",
    after: utc(2026, 10, 2, 12, 0),
    slots: ["20261002-154500", "20261003-153000", "20261004-151500"],
    markers: [undefined, "NonexistentLocalTimeShifted", undefined],
  },
  {
    what: "Lord Howe, the THIRTY-minute repeated window of 2027-04-04",
    zone: "Australia/Lord_Howe",
    expr: "45 1 * * *",
    after: utc(2027, 4, 3, 0, 0),
    slots: ["20270403-144500", "20270403-151500", "20270404-151500"],
    markers: ["RepeatedLocalTimeFirst", "RepeatedLocalTimeSecond", undefined],
  },
  {
    what: "Kathmandu, the fixed +05:45 offset",
    zone: "Asia/Kathmandu",
    expr: "0 9 * * *",
    after: utc(2026, 9, 14, 12, 0),
    slots: ["20260915-031500", "20260916-031500"],
    markers: [undefined, undefined],
  },
];

describe("the mock cadence agrees with weirkeeper::cadence", () => {
  it.each(DST)("$what", (c) => {
    const runs = nextRuns(c.expr, c.zone, c.after, c.slots.length);
    expect(runs.map((r) => slot(r.at))).toEqual(c.slots);
    expect(runs.map((r) => r.adjustment)).toEqual(c.markers);
  });

  it("renders the two occurrences of a repeated hour apart by their offset (previews_mark_shifted_and_repeated_instants)", () => {
    const runs = nextRuns("30 2 * * *", "Europe/Berlin", utc(2026, 10, 24, 12, 0), 20);
    const marked = runs.filter((r) => r.adjustment);
    expect(marked.map((r) => r.localTime)).toEqual(["2026-10-25T02:30:00+02:00", "2026-10-25T02:30:00+01:00"]);
    // Negative control: a UTC preview of the same line carries +00:00 and no marker at all.
    const plain = nextRuns("30 2 * * *", undefined, utc(2026, 10, 24, 12, 0), 5);
    expect(plain[0]!.localTime).toBe("2026-10-25T02:30:00+00:00");
    expect(plain.every((r) => r.adjustment === undefined)).toBe(true);
  });

  it("reproduces the API's own preview fixtures byte for byte", () => {
    for (const fixture of [gap, repeated]) {
      const runs = nextRuns(fixture.schedule, fixture.timeZone, Date.parse(fixture.after), fixture.runs.length);
      expect(runs).toEqual(fixture.runs);
    }
  });

  it("skips the months without the day, and runs out instead of hanging (leap_year_and_month_end_expressions_answer)", () => {
    const monthEnd = nextRuns("0 3 31 * *", "Europe/Berlin", utc(2026, 1, 1, 0, 0), 7).map((r) => r.localTime);
    expect(monthEnd).toEqual([
      "2026-01-31T03:00:00+01:00",
      "2026-03-31T03:00:00+02:00",
      "2026-05-31T03:00:00+02:00",
      "2026-07-31T03:00:00+02:00",
      "2026-08-31T03:00:00+02:00",
      "2026-10-31T03:00:00+01:00",
      "2026-12-31T03:00:00+01:00",
    ]);
    expect(nextRuns("0 3 29 2 *", "Europe/Berlin", utc(2026, 3, 1, 0, 0), 1)[0]!.at).toBe("2028-02-29T02:00:00Z");
    expect(nextRuns("0 3 29 2 *", "Europe/Berlin", utc(2096, 3, 1, 0, 0), 1)[0]!.at).toBe("2104-02-29T02:00:00Z");
    // A legal expression with no match is an empty answer, not an error.
    expect(nextRuns("0 0 30 2 *", "Europe/Berlin", utc(2026, 1, 1, 0, 0), 5)).toEqual([]);
    // A sparse expression is SHORTER than asked, not padded.
    const short = nextRuns("0 0 29 2 *", undefined, utc(2096, 3, 1, 0, 0), 20);
    expect(short.length).toBeGreaterThan(0);
    expect(short.length).toBeLessThan(20);
  });

  it("reads a restricted day-of-month and day-of-week as either, Vixie's rule", () => {
    // 2026-10-13 is a Tuesday. "0 0 13 * 5" fires on the 13th OR on Fridays.
    const runs = nextRuns("0 0 13 * 5", undefined, utc(2026, 10, 12, 0, 0), 2).map((r) => r.at);
    expect(runs).toEqual(["2026-10-13T00:00:00Z", "2026-10-16T00:00:00Z"]);
    // Negative control: a starred day-of-month intersects, so "*/2 ... 5" is Fridays on odd days only.
    const starred = nextRuns("0 0 */2 * 5", undefined, utc(2026, 10, 12, 0, 0), 1).map((r) => r.at);
    expect(starred).toEqual(["2026-10-23T00:00:00Z"]);
  });

  it("refuses what slot.rs refuses, naming the field", () => {
    for (const bad of ["61 * * * *", "* * * *", "1-30/5 * * * *", "+5 * * * *", "0 0 * * 7", "@monthly", "1,,2 * * * *"]) {
      expect(() => parseCron(bad), bad).toThrow(/cron/);
    }
    expect(() => parseCron("*/15 2 1-15 * 1,3")).not.toThrow();
  });
});

describe("the mock presets are presets::compile and match_preset", () => {
  it("compiles to the canonical expressions (presets_compile_to_the_canonical_expressions)", () => {
    expect(compilePreset({ kind: "hourly", minute: 17 })).toBe("17 * * * *");
    expect(compilePreset({ kind: "everyNHours", n: 6, minute: 30 })).toBe("30 */6 * * *");
    expect(compilePreset({ kind: "daily", hour: 2, minute: 0 })).toBe("0 2 * * *");
    expect(compilePreset({ kind: "weekly", dayOfWeek: 0, hour: 0, minute: 0 })).toBe("0 0 * * 0");
    expect(compilePreset({ kind: "monthly", dayOfMonth: 1, hour: 3, minute: 15 })).toBe("15 3 1 * *");
    // Negative control: the monthly preset stops at 28, and N must divide the day.
    expect(() => compilePreset({ kind: "monthly", dayOfMonth: 31, hour: 3, minute: 0 })).toThrow(/dayOfMonth/);
    expect(() => compilePreset({ kind: "everyNHours", n: 5, minute: 0 })).toThrow(/\bn\b/);
  });

  it("claims a preset only for its canonical spelling (expressions_that_are_not_presets_are_not_claimed)", () => {
    expect(matchPreset("@daily")).toEqual({ kind: "daily", hour: 0, minute: 0 });
    expect(matchPreset("0 2 * * *")).toEqual({ kind: "daily", hour: 2, minute: 0 });
    for (const advanced of ["0,30 2 * * *", "0 2 * * 1-5", "0 2 1 1 *", "0 2 31 * *", "0 */5 * * *", "*/15 * * * *", "0 03 * * *", "0 2 29 * *"]) {
      expect(matchPreset(advanced), advanced).toBeUndefined();
    }
  });
});
