// What needs the operator, worked out once for the Overview and the navigation's counts.
import { useBackups, useConnections, useProtection } from "../api/queries";
import type { Backup, Connection, ProtectionPolicy } from "../api/types";

export const DAY_MS = 86_400_000;

export type Attention = { key: string; tone: "danger" | "warning"; title: string; detail: string; to: string; action: string };

export function attentionItems(ns: string, policies: ProtectionPolicy[], backups: Backup[], connections: Connection[]): Attention[] {
  const items: Attention[] = [];
  for (const c of connections.filter((x) => x.reachability.state === "unreachable")) {
    items.push({
      key: `conn-${c.name}`,
      tone: "danger",
      title: `${c.name} is unreachable`,
      detail: `The controller could not reach its brokers (${c.reachability.reason ?? "no reason given"}). Backups from it fail until it is reachable.`,
      to: `/${ns}/connections`,
      action: "Open connections",
    });
  }
  for (const p of policies.filter((x) => x.health && x.health !== "Healthy")) {
    items.push({
      key: `policy-${p.name}`,
      tone: p.health === "Unprotected" ? "danger" : "warning",
      title: `${p.name} is ${p.health === "Unprotected" ? "not protected" : "at risk"}`,
      detail: `${p.consecutiveFailedRuns ?? 0} backups in a row failed; the objective allows ${p.objectives.maxConsecutiveFailedRuns}.`,
      to: `/${ns}/backups`,
      action: "See the failed runs",
    });
  }
  const retried = failedLastDay(backups).filter((b) => backups.some((r) => r.trigger?.retryOf?.name === b.name && r.operation.verifiedSuccess));
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

export function failedLastDay(backups: Backup[]): Backup[] {
  return backups.filter((b) => b.operation.state === "failed" && Date.now() - Date.parse(String(b.createdAt)) < DAY_MS);
}

export type NavCount = { n: number; tone: "danger" | "warning" | "brand"; text: string };

/** The navigation's counts: needs attention (Overview) and failed backups in 24 hours (Backups).
 *  Approvals waiting for you would join them in a two-person namespace; in a
 *  one-person namespace (team-a, OD-8's default) nothing can wait for anyone. */
export function useNavCounts(ns: string): Record<string, NavCount | undefined> {
  const backups = useBackups(ns);
  const protection = useProtection(ns);
  const connections = useConnections(ns);
  const runs = backups.data ?? [];
  const attention = attentionItems(ns, protection.data ?? [], runs, connections.data ?? []);
  const failed = failedLastDay(runs).length;
  return {
    "": attention.length
      ? {
          n: attention.length,
          tone: attention.some((a) => a.tone === "danger") ? "danger" : "warning",
          text: `${attention.length} ${attention.length === 1 ? "item needs" : "items need"} attention`,
        }
      : undefined,
    backups: failed ? { n: failed, tone: "danger", text: `${failed} failed in the last 24 hours` } : undefined,
  };
}
