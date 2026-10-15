// What the Overview and the navigation's counts show, worked out once from the lists the
// API publishes (protection policies, backups, restores, connections). Pure functions
// of the documents and `now`, so the rows in attention.test.ts pin every number.
import { useApprovalsNavCount } from "../api/approvals";
import { useBackups, useConnections, useProtection } from "../api/queries";
import { useCan } from "../api/session";
import type { Backup, Connection, ProtectionPolicy } from "../api/types";
import { duration } from "../components/When";
import { LATER_PROBE_GLOSS, probeState } from "../api/connections";

export const DAY_MS = 86_400_000;

export type Attention = { key: string; tone: "danger" | "warning"; title: string; detail: string; to: string; action: string };

function ms(value: string | null | undefined): number {
  const t = value ? Date.parse(value) : NaN;
  return Number.isFinite(t) ? t : NaN;
}

/** How old a policy's newest available point is now: from its capture start when
 *  recorded, else the age the controller computed at evaluation. Undefined: no point. */
export function pointAgeSeconds(p: ProtectionPolicy, now: number): number | undefined {
  const point = p.lastAvailablePoint;
  if (!point) return undefined;
  const at = ms(point.recoveryPointAt);
  if (Number.isFinite(at)) return Math.max(0, (now - at) / 1000);
  return typeof point.ageSeconds === "number" ? point.ageSeconds : undefined;
}

/**
 * FX-38: the controller's `Backup` listing was cut at its bound (`Protected` reason
 * `BackupHistoryTooLarge`), so the newest point, the last attempt and the failure
 * streak are not known. Neither healthy nor a failure.
 */
export function historyTooLarge(p: Pick<ProtectionPolicy, "conditions">): boolean {
  return (p.conditions ?? []).some((c) => c.type === "Protected" && c.reason === "BackupHistoryTooLarge");
}

/** "Verified" only when the controller verified the point's signed evidence. */
export function pointVerified(p: ProtectionPolicy): boolean {
  const evidence = p.lastAvailablePoint?.evidence;
  return evidence === "Valid" || evidence === "ValidHistorical";
}

const HEALTH_TITLE: Record<string, { tone: Attention["tone"]; says: string }> = {
  Unprotected: { tone: "danger", says: "is not protected" },
  Stale: { tone: "danger", says: "is past its objective" },
  AtRisk: { tone: "warning", says: "is at risk" },
  Unknown: { tone: "warning", says: "could not be evaluated" },
};

function policyDetail(p: ProtectionPolicy, now: number): string {
  // FX-38: nothing about the point or the streak is known; the controller's message says why and what to do.
  if (historyTooLarge(p)) return p.conditions?.find((c) => c.type === "Protected")?.message || "Open the policy for what the controller found.";
  const failed = p.consecutiveFailedRuns ?? 0;
  const age = pointAgeSeconds(p, now);
  const objective = p.objectives.maxRecoveryPointAgeSeconds;
  const parts: string[] = [];
  if (age === undefined) parts.push("No recovery point is available.");
  else if (age > objective) parts.push(`The newest recovery point is ${duration(age)} old; the objective is ${duration(objective)}.`);
  if (failed > 0) {
    parts.push(`${failed} ${failed === 1 ? "backup" : "backups in a row"} failed; the objective allows ${p.objectives.maxConsecutiveFailedRuns}.`);
  }
  if (parts.length === 0) {
    // The controller's own words for what it found (a suspended schedule, a missed slot, an unread catalog).
    const why = p.conditions?.find((c) => c.type === "Protected")?.message;
    parts.push(why || "Open the policy for what the controller found.");
  }
  return parts.join(" ");
}

/** What an unreachable connection stops, by its role: a target's owner restores to it (DUI-6). */
export function unreachableConsequence(role: string): string {
  if (role === "source") return "Backups from it fail until it is reachable.";
  if (role === "target") return "Restores to it fail until it is reachable.";
  return "Backups from it and restores to it fail until it is reachable.";
}

/** Where a policy's attention item leads: its failed run, the runs that did not verify, or the policy. */
export function failedRunsLink(ns: string, p: Pick<ProtectionPolicy, "consecutiveFailedRuns" | "lastAttempt">): { to: string; action: string } {
  if ((p.consecutiveFailedRuns ?? 0) === 0) return { to: `/${ns}/protection`, action: "Open protection" };
  const last = p.lastAttempt?.backupRef?.name;
  return last
    ? { to: `/${ns}/backups/${encodeURIComponent(last)}`, action: "Open the last failed run" }
    : { to: `/${ns}/backups?filter=unverified`, action: "See the runs that did not verify" };
}

/**
 * DUI-49.2: why a connection reads unreachable, by `probeState` as its page words it (DUI-31.6,
 * DUI-48.3). The reading's own reason is the reason the brokers were not reached; a later
 * probe's reason (a new probe running, one that recorded no reading) is said as that, beside
 * the earlier reading, never as the cause. A stale reading says so.
 */
export function unreachableWords(c: Connection, now = Date.now()): string {
  const p = probeState(c.reachability, now);
  const stale = p.stale ? " That reading is stale." : "";
  if (p.laterReason) {
    const gloss = LATER_PROBE_GLOSS[p.laterReason];
    return `The controller's last reading could not reach its brokers; since then, ${p.laterReason}${gloss ? `: ${gloss}` : ""}.${stale}`;
  }
  return `The controller could not reach its brokers (${p.reason || "no reason given"}).${stale}`;
}

export function attentionItems(
  ns: string,
  policies: ProtectionPolicy[],
  backups: Backup[],
  connections: Connection[],
  now = Date.now(),
): Attention[] {
  const items: Attention[] = [];
  for (const c of connections.filter((x) => x.reachability.state === "unreachable")) {
    items.push({
      key: `conn-${c.name}`,
      tone: "danger",
      title: `${c.name} is unreachable`,
      detail: `${unreachableWords(c, now)} ${unreachableConsequence(c.role)}`,
      to: `/${ns}/connections/${c.name}`,
      action: "Open the connection",
    });
  }
  for (const p of policies) {
    const health = p.health ? HEALTH_TITLE[p.health] : undefined;
    if (!health) continue;
    items.push({
      key: `policy-${p.name}`,
      tone: health.tone,
      title: `${p.name} ${health.says}`,
      detail: policyDetail(p, now),
      // DUI-31.3: the policy's own last run when it names one, else the runs that did not verify.
      ...failedRunsLink(ns, p),
    });
  }
  const retried = failedLastDay(backups, now).filter((b) =>
    backups.some((r) => r.trigger?.retryOf?.name === b.name && r.operation.verifiedSuccess),
  );
  if (retried.length > 0) {
    items.push({
      key: "retried",
      tone: "warning",
      title: `${retried.length} backup ${retried.length === 1 ? "run" : "runs"} failed and succeeded on retry`,
      detail: `${retried.map((b) => b.name).join(", ")}: no point is missing, but the deadline may be too tight.`,
      to: `/${ns}/backups/${retried[0]!.name}`,
      action: "Open the failed run",
    });
  }
  return items;
}

export function lastDay(backups: Backup[], now = Date.now()): Backup[] {
  return backups.filter((b) => now - ms(b.createdAt) < DAY_MS);
}

/**
 * A run that backed up nothing: it failed, or it was refused before it ran (DUI-32).
 * Neither is a success; the words keep the two apart ("failed", "refused").
 */
export function didNotBackUp(b: Pick<Backup, "operation">): boolean {
  return b.operation.state === "failed" || b.operation.state === "refused";
}

/** The runs of the last 24 hours that backed up nothing: failed or refused. */
export function failedLastDay(backups: Backup[], now = Date.now()): Backup[] {
  return lastDay(backups, now).filter(didNotBackUp);
}

/** "1 failed", "2 refused", "1 failed, 2 refused": each state its own word; "" for none. */
export function notBackedUpWords(runs: Pick<Backup, "operation">[], join = ", "): string {
  const failed = runs.filter((b) => b.operation.state === "failed").length;
  const refused = runs.filter((b) => b.operation.state === "refused").length;
  return [failed > 0 && `${failed} failed`, refused > 0 && `${refused} refused`].filter(Boolean).join(join);
}

export type NavCount = { n: number; tone: "danger" | "warning" | "brand"; text: string };

/** Whether the session may read everything "needs attention" is worked out from. A partial
 *  answer would say "nothing needs you" about what the role cannot see. */
export function useAttentionReadable(ns: string): boolean {
  return useCan(ns, "protection") && useCan(ns, "connectionsRead") && useCan(ns, "backupsRead");
}

/** The navigation's counts: needs attention (Overview) and failed backups in 24 hours (Backups),
 *  from the same queries the Overview reads, and the requests waiting for this session's
 *  approval (Approvals, from the Approvals page's own reads). */
export function useNavCounts(ns: string): Record<string, NavCount | undefined> {
  const backups = useBackups(ns);
  const protection = useProtection(ns);
  const connections = useConnections(ns);
  const readable = useAttentionReadable(ns);
  const ready = readable && protection.data && connections.data && backups.data;
  const attention = ready ? attentionItems(ns, protection.data!, backups.data!, connections.data!) : [];
  const missed = backups.data ? failedLastDay(backups.data) : [];
  const approvals = useApprovalsNavCount(ns);
  return {
    "": attention.length
      ? {
          n: attention.length,
          tone: attention.some((a) => a.tone === "danger") ? "danger" : "warning",
          text: `${attention.length} ${attention.length === 1 ? "item needs" : "items need"} attention`,
        }
      : undefined,
    backups: missed.length ? { n: missed.length, tone: "danger", text: `${notBackedUpWords(missed)} in the last 24 hours` } : undefined,
    approvals,
  };
}
