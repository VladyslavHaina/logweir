// Every read and write the pages make, as TanStack Query hooks over the typed client.
//
// Query keys: [kind, ws] for a list, [kind, ws, name] for one object, and
// ["operations", ws, kind, name] for a run's operation view, so invalidating
// [kind, ws] refreshes a list and its items together. A read the session does
// not grant in `ws` is never sent: its hook stays idle (`fetchStatus: "idle"`,
// no data), and the page shows what it can.
//
// Lists read every page (client.listAll) and hand back newest first. A list
// holding a run still in progress re-reads itself; an open run's operation is
// pushed into the cache by the event stream (stream.ts).
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { LIST_PAGE_SIZE, api, call, createHeaders, listAll } from "./client";
import { useCan } from "./session";
import type { Backup, Connection } from "./types";

const LIVE_MS = 1500;

export const keys = {
  session: ["session"] as const,
  list: (kind: string, ws: string) => [kind, ws] as const,
  item: (kind: string, ws: string, name: string) => [kind, ws, name] as const,
  operation: (ws: string, kind: "backup" | "restore", name: string) => ["operations", ws, kind, name] as const,
};

function moving(items: { operation: { terminal: boolean } }[] | undefined): number | false {
  return items?.some((i) => !i.operation.terminal) ? LIVE_MS : false;
}

function createdMs(item: { createdAt?: string | null }): number {
  const ms = item.createdAt ? Date.parse(item.createdAt) : NaN;
  return Number.isFinite(ms) ? ms : 0;
}

/** Newest first, by creation time; an object with no recorded time sorts last. */
export function newestFirst<T extends { createdAt?: string | null; name: string }>(items: T[]): T[] {
  return [...items].sort((a, b) => createdMs(b) - createdMs(a) || a.name.localeCompare(b.name));
}

const path = (ws: string) => ({ path: { ws } });

export function useBackups(ws: string) {
  return useQuery({
    queryKey: keys.list("backups", ws),
    enabled: useCan(ws, "backupsRead"),
    queryFn: ({ signal }) =>
      listAll("backups", (cursor) =>
        call(() => api.GET("/api/v1/workspaces/{ws}/backups", { params: { ...path(ws), query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
    select: newestFirst,
    refetchInterval: (q) => moving(q.state.data),
  });
}

export function useBackup(ws: string, name: string) {
  return useQuery({
    queryKey: keys.item("backups", ws, name),
    enabled: useCan(ws, "backupsRead"),
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/workspaces/{ws}/backups/{name}", { params: { path: { ws, name } }, signal }))).item,
    refetchInterval: (q) => (q.state.data && !q.state.data.operation.terminal ? LIVE_MS : false),
    retry: (count, error) => !(error instanceof Error && "status" in error && error.status === 404) && count < 2,
  });
}

export function useRestores(ws: string, enabled = true) {
  const readable = useCan(ws, "restoresRead");
  return useQuery({
    queryKey: keys.list("restores", ws),
    enabled: enabled && readable,
    queryFn: ({ signal }) =>
      listAll("restores", (cursor) =>
        call(() => api.GET("/api/v1/workspaces/{ws}/restores", { params: { ...path(ws), query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
    select: newestFirst,
    refetchInterval: (q) => moving(q.state.data),
  });
}

/**
 * DUI-38: a terminal run whose verdict is still coming. The API's own rule: its event
 * stream ends `settled` only once a run is terminal AND its verification is no longer
 * pending (logweir-api `status::is_settled`). A restore whose scorecard an
 * evidence-fetch Job reads is terminal seconds before the controller verifies it.
 */
export function verdictPending(op: { terminal: boolean; verificationState?: string; verification?: { state: string } } | null | undefined): boolean {
  if (!op?.terminal) return false;
  return (op.verificationState ?? op.verification?.state) === "pending";
}

/**
 * One Restore. No name reads nothing (DUI-31.8: a page whose name comes from another
 * read asks only once it has one). `fresh` reads it again on every mount, so a page
 * that decides from it never decides from a copy cached before the run ended (DUI-28).
 * `follow` keeps reading a terminal restore while its verdict is pending (DUI-38); the
 * page bounds it.
 */
export function useRestore(ws: string, name: string, { fresh = false, follow = false }: { fresh?: boolean; follow?: boolean } = {}) {
  const readable = useCan(ws, "restoresRead");
  return useQuery({
    queryKey: keys.item("restores", ws, name),
    enabled: readable && name.length > 0,
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/workspaces/{ws}/restores/{name}", { params: { path: { ws, name } }, signal }))).item,
    refetchInterval: (q) => (q.state.data && (!q.state.data.operation.terminal || (follow && verdictPending(q.state.data.operation))) ? LIVE_MS : false),
    ...(fresh ? { refetchOnMount: "always" as const } : {}),
  });
}

/**
 * DUI-42: the Backup a restore's set id names (`spec.backupSetRef` is its `backupId`), whose
 * `consumerGroups` are the groups its point recorded: absent selects none, so no position is
 * mapped and nothing can be switched over. `undefined` while unknown (the list unread, or not
 * readable to this role); `null` when no Backup carries that id any more.
 */
export function useSourceBackup(ws: string, backupSetRef: string | undefined): Backup | null | undefined {
  const backups = useBackups(ws);
  if (!backupSetRef || !backups.data) return undefined;
  return backups.data.find((b) => b.backupId === backupSetRef) ?? null;
}

/** One run's operation view. While `streaming` (stream.ts holds the run's event stream
 *  open) the stream writes this key and the poll stands down. `follow` keeps reading a
 *  terminal run while its verdict is pending (DUI-38). */
export function useOperation(ws: string, kind: "backup" | "restore", name: string, streaming = false, follow = false) {
  return useQuery({
    queryKey: keys.operation(ws, kind, name),
    enabled: useCan(ws, "operationsRead"),
    queryFn: async ({ signal }) =>
      (
        await call(() =>
          api.GET("/api/v1/workspaces/{ws}/operations/{kind}/{name}", { params: { path: { ws, kind, name } }, signal }),
        )
      ).item,
    refetchInterval: (q) => (!streaming && q.state.data && (!q.state.data.terminal || (follow && verdictPending(q.state.data))) ? 1000 : false),
  });
}

/** `reReadMs` lets a page that watches a probe in flight re-read the list while it runs. */
export function useConnections(ws: string, reReadMs?: (items: Connection[], reads: number) => number | false) {
  return useQuery({
    queryKey: keys.list("connections", ws),
    enabled: useCan(ws, "connectionsRead"),
    queryFn: ({ signal }) =>
      listAll("connections", (cursor) =>
        call(() => api.GET("/api/v1/workspaces/{ws}/connections", { params: { ...path(ws), query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
    refetchInterval: (q) => (reReadMs && q.state.data ? reReadMs(q.state.data, q.state.dataUpdateCount) : false),
  });
}

/** The workspace's saved destinations: the Destinations page, a schedule's and a catalog's pick-lists. */
export function useDestinations(ws: string) {
  return useQuery({
    queryKey: keys.list("destinations", ws),
    enabled: useCan(ws, "destinations"),
    queryFn: ({ signal }) =>
      listAll("destinations", (cursor) =>
        call(() => api.GET("/api/v1/workspaces/{ws}/destinations", { params: { ...path(ws), query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
  });
}

export function useSchedules(ws: string) {
  return useQuery({
    queryKey: keys.list("schedules", ws),
    enabled: useCan(ws, "schedulesRead"),
    queryFn: ({ signal }) =>
      listAll("schedules", (cursor) =>
        call(() => api.GET("/api/v1/workspaces/{ws}/schedules", { params: { ...path(ws), query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
  });
}

export function useProtection(ws: string) {
  return useQuery({
    queryKey: keys.list("protection", ws),
    enabled: useCan(ws, "protection"),
    queryFn: ({ signal }) =>
      listAll("protection policies", (cursor) =>
        call(() =>
          api.GET("/api/v1/workspaces/{ws}/protection-policies", { params: { ...path(ws), query: { limit: LIST_PAGE_SIZE, cursor } }, signal }),
        ),
      ),
    refetchInterval: 15_000,
  });
}

/** Back up now from a schedule. `key` is the Idempotency-Key: the caller keeps it for a
 *  retry after an unknown outcome, so a second click names the same run. */
export function useBackUpNow(ws: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ schedule, key }: { schedule: string; key: string }): Promise<Backup> =>
      (
        await call(() =>
          api.POST("/api/v1/workspaces/{ws}/backups", {
            params: { path: { ws }, header: createHeaders(key) },
            body: { scheduleRef: { name: schedule } },
          }),
        )
      ).item,
    onSettled: () => client.invalidateQueries({ queryKey: keys.list("backups", ws) }),
  });
}

