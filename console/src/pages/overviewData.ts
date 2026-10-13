// The Overview's numbers, strips and activity, from the lists the API publishes.
// Pure functions of the documents and `now`; overviewData.test.ts pins them.
import type { Backup, OperationSummary, ProtectionPolicy, Restore } from "../api/types";
import { lastDay, pointAgeSeconds, pointVerified } from "../app/attention";

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
      const at = b.createdAt ? new Date(b.createdAt).toISOString().slice(11, 16) + " UTC" : "time not recorded";
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
