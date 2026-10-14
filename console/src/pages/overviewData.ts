// The Overview's numbers, strips and activity, from the lists the API publishes.
// Pure functions of the documents and `now`; overviewData.test.ts pins them.
import type { Backup, OperationSummary, PointView, ProtectionPolicy, Restore } from "../api/types";
import { lastDay, pointAgeSeconds, pointVerified } from "../app/attention";
import { utcIso } from "../components/When";

export type Freshest = { policy: ProtectionPolicy; ageSeconds: number; withinObjective: boolean; verified: boolean };

/** Policies meeting their objective (health `Healthy`) out of all policies. */
export function protectionCount(policies: ProtectionPolicy[]) {
  return { healthy: policies.filter((p) => p.health === "Healthy").length, total: policies.length };
}

/** The newest available point across the policies, judged by its own policy's objective. */
export function freshestPoint(policies: ProtectionPolicy[], now: number): Freshest | undefined {
  let best: Freshest | undefined;
  for (const policy of policies) {
    const ageSeconds = pointAgeSeconds(policy, now);
    if (ageSeconds === undefined) continue;
    if (!best || ageSeconds < best.ageSeconds) {
      best = {
        policy,
        ageSeconds,
        withinObjective: ageSeconds <= policy.objectives.maxRecoveryPointAgeSeconds,
        verified: pointVerified(policy),
      };
    }
  }
  return best;
}

/** A catalog's newest selectable point, as the Overview reads it. */
export type CatalogPoint = { catalog: string; point: PointView };

/** Where the namespace's newest recovery point was found. Only a policy's point has an objective. */
export type NewestPoint =
  | { kind: "policy"; ageSeconds: number; verified: boolean; policy: ProtectionPolicy; withinObjective: boolean }
  | { kind: "backup"; ageSeconds: number; verified: true; backup: Backup }
  | { kind: "catalog"; ageSeconds: number; verified: true; catalog: string; point: PointView };

function msOf(value: string | null | undefined): number {
  const t = value ? Date.parse(value) : NaN;
  return Number.isFinite(t) ? t : NaN;
}

/**
 * DUI-3: the newest recovery point the namespace HOLDS, not only the one a protection
 * policy names. Candidates: each policy's newest available point (judged by its own
 * objective), every Backup the controller verified (aged from its creation, which
 * precedes its capture start, so never younger than it is), and each catalog's
 * newest selectable point. A namespace with no policy but verified backups has a
 * point; "none" is said only when no source holds one.
 */
export function newestPoint(policies: ProtectionPolicy[], backups: Backup[], catalogPoints: CatalogPoint[], now: number): NewestPoint | undefined {
  const candidates: NewestPoint[] = [];
  const policy = freshestPoint(policies, now);
  if (policy) candidates.push({ kind: "policy", ...policy });
  for (const backup of backups) {
    const at = msOf(backup.createdAt);
    if (backup.operation.verifiedSuccess && Number.isFinite(at)) candidates.push({ kind: "backup", ageSeconds: Math.max(0, (now - at) / 1000), verified: true, backup });
  }
  for (const { catalog, point } of catalogPoints) {
    const at = msOf(point.recoveryPointAt);
    if (point.selectable && Number.isFinite(at)) candidates.push({ kind: "catalog", ageSeconds: Math.max(0, (now - at) / 1000), verified: true, catalog, point });
  }
  // The youngest wins; on a tie the policy's point, which carries an objective.
  let best: NewestPoint | undefined;
  for (const c of candidates) if (!best || c.ageSeconds < best.ageSeconds) best = c;
  return best;
}

/** Backups created in the last 24 hours that verified or failed, and every run still moving. */
export function backupDay(backups: Backup[], now: number) {
  const day = lastDay(backups, now);
  return {
    verified: day.filter((b) => b.operation.verifiedSuccess).length,
    failed: day.filter((b) => b.operation.state === "failed").length,
    running: backups.filter((b) => !b.operation.terminal).length,
  };
}

/** The schedule a run belongs to: the revision it copied, else the label the controller set. */
export function scheduleOf(b: Backup): string | undefined {
  return b.scheduleRef?.name ?? b.schedule ?? undefined;
}

export type Cell = { backup: Backup; target: Backup; cls: "ok" | "retried" | "running" | "failed"; word: string; at: string };

/** A schedule's last 24 runs, oldest first, from `backups` newest first. A failed run whose
 *  retry verified is one "verified on retry" cell that links to the retry. */
export function strip(backups: Backup[], schedule: string): Cell[] {
  const runs = backups.filter((b) => scheduleOf(b) === schedule);
  return runs
    .filter((r) => r.trigger?.kind !== "Retry")
    .slice(0, 24)
    .reverse()
    .map((b) => {
      const retry = runs.find((r) => r.trigger?.retryOf?.name === b.name);
      // Runs of a strip span days: each cell names its date as well as its time (DUI-12).
      const iso = utcIso(b.createdAt);
      const at = iso ? iso.slice(0, 16).replace("T", " ") + " UTC" : b.createdAt ? "not a time" : "time not recorded";
      if (b.operation.state === "failed" && retry?.operation.verifiedSuccess) {
        return { backup: b, target: retry, cls: "retried", word: "failed, then verified on retry", at };
      }
      if (!b.operation.terminal) return { backup: b, target: b, cls: "running", word: "in progress", at };
      if (b.operation.verifiedSuccess) return { backup: b, target: b, cls: "ok", word: "verified", at };
      return { backup: b, target: b, cls: "failed", word: b.operation.state === "succeeded" ? "done, not verified" : b.operation.state, at };
    });
}

export type ActivityRow = { kind: "Backup" | "Restore"; name: string; at: string | null; op: OperationSummary };

/** The newest backups and restores together, newest first. */
export function activity(backups: Backup[], restores: Restore[], limit = 6): ActivityRow[] {
  const at = (row: ActivityRow) => (row.at ? Date.parse(row.at) || 0 : 0);
  return [
    ...backups.map((b) => ({ kind: "Backup" as const, name: b.name, at: b.createdAt ?? null, op: b.operation })),
    ...restores.map((r) => ({ kind: "Restore" as const, name: r.name, at: r.createdAt ?? null, op: r.operation })),
  ]
    .sort((a, b) => at(b) - at(a))
    .slice(0, limit);
}
