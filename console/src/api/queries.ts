// Every read and write the pages make, as TanStack Query hooks over the typed client.
//
// Query keys: [kind, ns] for a list, [kind, ns, name] for one object, and
// ["operations", ns, kind, name] for a run's operation view, so invalidating
// [kind, ns] refreshes a list and its items together. A read the session does
// not grant in `ns` is never sent: its hook stays idle (`fetchStatus: "idle"`,
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
  list: (kind: string, ns: string) => [kind, ns] as const,
  item: (kind: string, ns: string, name: string) => [kind, ns, name] as const,
  operation: (ns: string, kind: "backup" | "restore", name: string) => ["operations", ns, kind, name] as const,
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

const path = (ns: string) => ({ path: { ns } });

export function useBackups(ns: string) {
  return useQuery({
    queryKey: keys.list("backups", ns),
    enabled: useCan(ns, "backupsRead"),
    queryFn: ({ signal }) =>
      listAll("backups", (cursor) =>
        call(() => api.GET("/api/v1/namespaces/{ns}/backups", { params: { ...path(ns), query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
    select: newestFirst,
    refetchInterval: (q) => moving(q.state.data),
  });
}

export function useBackup(ns: string, name: string) {
  return useQuery({
    queryKey: keys.item("backups", ns, name),
    enabled: useCan(ns, "backupsRead"),
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/backups/{name}", { params: { path: { ns, name } }, signal }))).item,
    refetchInterval: (q) => (q.state.data && !q.state.data.operation.terminal ? LIVE_MS : false),
    retry: (count, error) => !(error instanceof Error && "status" in error && error.status === 404) && count < 2,
  });
}

export function useRestores(ns: string, enabled = true) {
  const readable = useCan(ns, "restoresRead");
  return useQuery({
    queryKey: keys.list("restores", ns),
    enabled: enabled && readable,
    queryFn: ({ signal }) =>
      listAll("restores", (cursor) =>
        call(() => api.GET("/api/v1/namespaces/{ns}/restores", { params: { ...path(ns), query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
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
export function useRestore(ns: string, name: string, { fresh = false, follow = false }: { fresh?: boolean; follow?: boolean } = {}) {
  const readable = useCan(ns, "restoresRead");
  return useQuery({
    queryKey: keys.item("restores", ns, name),
    enabled: readable && name.length > 0,
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/restores/{name}", { params: { path: { ns, name } }, signal }))).item,
    refetchInterval: (q) => (q.state.data && (!q.state.data.operation.terminal || (follow && verdictPending(q.state.data.operation))) ? LIVE_MS : false),
    ...(fresh ? { refetchOnMount: "always" as const } : {}),
  });
}

/** One run's operation view. While `streaming` (stream.ts holds the run's event stream
 *  open) the stream writes this key and the poll stands down. `follow` keeps reading a
 *  terminal run while its verdict is pending (DUI-38). */
export function useOperation(ns: string, kind: "backup" | "restore", name: string, streaming = false, follow = false) {
  return useQuery({
    queryKey: keys.operation(ns, kind, name),
    enabled: useCan(ns, "operationsRead"),
    queryFn: async ({ signal }) =>
      (
        await call(() =>
          api.GET("/api/v1/namespaces/{ns}/operations/{kind}/{name}", { params: { path: { ns, kind, name } }, signal }),
        )
      ).item,
    refetchInterval: (q) => (!streaming && q.state.data && (!q.state.data.terminal || (follow && verdictPending(q.state.data))) ? 1000 : false),
  });
}

/** `reReadMs` lets a page that watches a probe in flight re-read the list while it runs. */
export function useConnections(ns: string, reReadMs?: (items: Connection[], reads: number) => number | false) {
  return useQuery({
    queryKey: keys.list("connections", ns),
    enabled: useCan(ns, "connectionsRead"),
    queryFn: ({ signal }) =>
      listAll("connections", (cursor) =>
        call(() => api.GET("/api/v1/namespaces/{ns}/connections", { params: { ...path(ns), query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
    refetchInterval: (q) => (reReadMs && q.state.data ? reReadMs(q.state.data, q.state.dataUpdateCount) : false),
  });
}

/** The namespace's saved destinations: the Destinations page, a schedule's and a catalog's pick-lists. */
export function useDestinations(ns: string) {
  return useQuery({
    queryKey: keys.list("destinations", ns),
    enabled: useCan(ns, "destinations"),
    queryFn: ({ signal }) =>
      listAll("destinations", (cursor) =>
        call(() => api.GET("/api/v1/namespaces/{ns}/destinations", { params: { ...path(ns), query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
  });
}

export function useSchedules(ns: string) {
  return useQuery({
    queryKey: keys.list("schedules", ns),
    enabled: useCan(ns, "schedulesRead"),
    queryFn: ({ signal }) =>
      listAll("schedules", (cursor) =>
        call(() => api.GET("/api/v1/namespaces/{ns}/schedules", { params: { ...path(ns), query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
  });
}

export function useProtection(ns: string) {
  return useQuery({
    queryKey: keys.list("protection", ns),
    enabled: useCan(ns, "protection"),
    queryFn: ({ signal }) =>
      listAll("protection policies", (cursor) =>
        call(() =>
          api.GET("/api/v1/namespaces/{ns}/protection-policies", { params: { ...path(ns), query: { limit: LIST_PAGE_SIZE, cursor } }, signal }),
        ),
      ),
    refetchInterval: 15_000,
  });
}

/** Back up now from a schedule. `key` is the Idempotency-Key: the caller keeps it for a
 *  retry after an unknown outcome, so a second click names the same run. */
export function useBackUpNow(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ schedule, key }: { schedule: string; key: string }): Promise<Backup> =>
      (
        await call(() =>
          api.POST("/api/v1/namespaces/{ns}/backups", {
            params: { path: { ns }, header: createHeaders(key) },
            body: { scheduleRef: { name: schedule } },
          }),
        )
      ).item,
    onSettled: () => client.invalidateQueries({ queryKey: keys.list("backups", ns) }),
  });
}

