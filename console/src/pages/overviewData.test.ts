// The Overview's derivations, over the API's recorded documents (ui/tests/fixtures/console/,
// the same files the current console's contract rows read) and small variants of them.
import { describe, expect, it } from "vitest";
import backupsList from "../../../ui/tests/fixtures/console/backups-list.json";
import restoresList from "../../../ui/tests/fixtures/console/restores-list.json";
import connectionsList from "../../../ui/tests/fixtures/console/connections-list.json";
import policiesList from "../../../ui/tests/fixtures/console/protection-policies-list.json";
import type { Backup, Connection, ProtectionPolicy, Restore } from "../api/types";
import { newestFirst } from "../api/queries";
import { attentionItems, failedLastDay, pointAgeSeconds } from "../app/attention";
import { activity, backupDay, freshestPoint, protectionCount, scheduleOf, strip } from "./overviewData";

const BACKUP = backupsList.items[0] as unknown as Backup;
const RESTORE = restoresList.items[0] as unknown as Restore;
const CONNECTIONS = connectionsList.items as unknown as Connection[];
const POLICIES = policiesList.items as unknown as ProtectionPolicy[];
const HEALTHY = POLICIES.find((p) => p.health === "Healthy")!;
const UNPROTECTED = POLICIES.find((p) => p.health === "Unprotected")!;

const MIN = 60_000;
const HOUR = 60 * MIN;
const NOW = Date.parse("2026-09-19T01:12:00Z");
const iso = (ms: number) => new Date(ms).toISOString();

/** A run of `schedule`, `minutesAgo` old, in `state`. */
function run(name: string, minutesAgo: number | null, state: string, extra: Partial<Backup> = {}): Backup {
  const terminal = !["pending", "queued", "preparing", "running", "verifying"].includes(state);
  return {
    ...BACKUP,
    name,
    createdAt: minutesAgo === null ? null : iso(NOW - minutesAgo * MIN),
    scheduleRef: { name: "orders-hourly" },
    trigger: { kind: "Scheduled", attempt: 0 },
    operation: {
      state,
      terminal,
      verificationState: state === "succeeded" ? "valid" : "pending",
      verifiedSuccess: state === "succeeded",
    },
    ...extra,
  } as Backup;
}

describe("protection numbers", () => {
  it("count the policies meeting their objective", () => {
    expect(protectionCount(POLICIES)).toEqual({ healthy: 1, total: 2 });
    expect(protectionCount([])).toEqual({ healthy: 0, total: 0 });
  });

  it("age a point from its capture start at the time of reading, not at evaluation", () => {
    // recoveryPointAt 01:10:11.287Z, read at 01:12:00Z: 108.7 s, not the 94 s evaluated.
    expect(pointAgeSeconds(HEALTHY, NOW)).toBeCloseTo(108.713, 3);
    expect(pointAgeSeconds(UNPROTECTED, NOW)).toBeUndefined();
    const noStart = { ...HEALTHY, lastAvailablePoint: { ...HEALTHY.lastAvailablePoint!, recoveryPointAt: null } };
    expect(pointAgeSeconds(noStart, NOW)).toBe(94);
  });

  it("pick the newest point across policies and judge it by its own policy's objective", () => {
    const freshest = freshestPoint(POLICIES, NOW)!;
    expect([freshest.policy.name, freshest.withinObjective, freshest.verified]).toEqual(["protect-healthy", true, true]);
    // Ten minutes later the same point is past its 300 s objective.
    expect(freshestPoint(POLICIES, NOW + 10 * MIN)!.withinObjective).toBe(false);
    // "Verified" only for evidence the controller verified.
    const unverified = { ...HEALTHY, lastAvailablePoint: { ...HEALTHY.lastAvailablePoint!, evidence: "NotAttempted" } };
    expect(freshestPoint([unverified], NOW)!.verified).toBe(false);
    expect(freshestPoint([UNPROTECTED], NOW)).toBeUndefined();
  });
});

describe("backups", () => {
  // The API lists in name order; the Overview reads newest first.
  const runs = newestFirst([
    run("orders-hourly-a", 30 * 60, "succeeded"),
    run("orders-hourly-b", 23 * 60, "failed"),
    run("orders-hourly-c", 22 * 60, "succeeded", { trigger: { kind: "Retry", attempt: 1, retryOf: { name: "orders-hourly-b" } } }),
    run("orders-hourly-d", 60, "failed"),
    run("orders-hourly-e", 5, "running"),
    run("orders-hourly-f", null, "succeeded"),
    run("orders-manual-g", 10, "succeeded", { scheduleRef: undefined, schedule: "orders-hourly", trigger: { kind: "Manual", attempt: 0 } }),
  ]);

  it("sort name-ordered lists newest first, with an unrecorded time last", () => {
    expect(runs.map((r) => r.name)).toEqual([
      "orders-hourly-e",
      "orders-manual-g",
      "orders-hourly-d",
      "orders-hourly-c",
      "orders-hourly-b",
      "orders-hourly-a",
      "orders-hourly-f",
    ]);
  });

  it("count the last 24 hours: verified, failed, and every run still moving", () => {
    expect(backupDay(runs, NOW)).toEqual({ verified: 2, failed: 2, running: 1 });
    expect(failedLastDay(runs, NOW).map((b) => b.name)).toEqual(["orders-hourly-d", "orders-hourly-b"]);
  });

  it("draw a schedule's strip oldest first, a retried failure once, linked to its retry", () => {
    expect(scheduleOf(runs.find((r) => r.name === "orders-manual-g")!)).toBe("orders-hourly");
    const cells = strip(runs, "orders-hourly");
    expect(cells.map((c) => [c.backup.name, c.cls, c.target.name])).toEqual([
      ["orders-hourly-f", "ok", "orders-hourly-f"],
      ["orders-hourly-a", "ok", "orders-hourly-a"],
      ["orders-hourly-b", "retried", "orders-hourly-c"],
      ["orders-hourly-d", "failed", "orders-hourly-d"],
      ["orders-manual-g", "ok", "orders-manual-g"],
      ["orders-hourly-e", "running", "orders-hourly-e"],
    ]);
    expect(cells[0]!.at).toBe("time not recorded");
    expect(strip(runs, "nightly")).toEqual([]);
  });

  it("list the newest backups and restores together", () => {
    const rows = activity(runs, [{ ...RESTORE, createdAt: iso(NOW - 2 * MIN) }], 3);
    expect(rows.map((r) => `${r.kind} ${r.name}`)).toEqual([
      "Restore orders-drill-20260911",
      "Backup orders-hourly-e",
      "Backup orders-manual-g",
    ]);
    expect(activity([BACKUP], [RESTORE]).map((r) => r.at)).toEqual([BACKUP.createdAt, RESTORE.createdAt]);
  });
});

describe("needs attention", () => {
  it("names an unreachable connection, an unhealthy policy and a failure fixed by a retry", () => {
    const unreachable = { ...CONNECTIONS[0]!, reachability: { state: "unreachable", reason: "BrokersUnreachable" } } as Connection;
    const runs = [
      run("orders-hourly-c", 50, "succeeded", { trigger: { kind: "Retry", attempt: 1, retryOf: { name: "orders-hourly-b" } } }),
      run("orders-hourly-b", 60, "failed"),
    ];
    const items = attentionItems("team-a", POLICIES, runs, [unreachable, CONNECTIONS[1]!], NOW);
    expect(items.map((i) => [i.key, i.tone, i.title, i.to])).toEqual([
      ["conn-orders-prod", "danger", "orders-prod is unreachable", "/team-a/connections"],
      ["policy-protect-a", "danger", "protect-a is not protected", "/team-a/protection"],
      ["retried", "warning", "1 backup run failed and succeeded on retry", "/team-a/backups/orders-hourly-b"],
    ]);
    expect(items[0]!.detail).toContain("BrokersUnreachable");
    expect(items[1]!.detail).toBe("No recovery point is available.");
  });

  it("is empty for the recorded namespace's healthy half and a reachable connection", () => {
    expect(attentionItems("team-a", [HEALTHY], [BACKUP], CONNECTIONS, NOW)).toEqual([]);
  });

  it("says why a policy is past its objective, and how many runs failed", () => {
    const stale = { ...HEALTHY, health: "Stale", consecutiveFailedRuns: 2 };
    const [item] = attentionItems("team-a", [stale], [], [], NOW + HOUR);
    expect([item!.title, item!.to]).toEqual(["protect-healthy is past its objective", "/team-a/backups"]);
    expect(item!.detail).toBe("The newest recovery point is 62 min old; the objective is 5 min. 2 backups in a row failed; the objective allows 1.");
    // A policy not evaluated yet is not an item; one that could not be evaluated is.
    expect(attentionItems("team-a", [{ ...HEALTHY, health: null }], [], [], NOW)).toEqual([]);
    expect(attentionItems("team-a", [{ ...HEALTHY, health: "Unknown" }], [], [], NOW)[0]!.title).toBe(
      "protect-healthy could not be evaluated",
    );
  });
});
