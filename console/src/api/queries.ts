// Every read and write the pages make, as TanStack Query hooks over the typed
// client. A list that holds a run still in progress re-reads itself; in the
// built console the operation stream (EventSource) pushes into the same cache.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, createHeaders, unwrap } from "./client";
import type { Backup, CreateRestoreRequest, Restore } from "./types";

const LIVE_MS = 1500;

function moving(items: { operation: { terminal: boolean } }[] | undefined): number | false {
  return items?.some((i) => !i.operation.terminal) ? LIVE_MS : false;
}

export function useSession() {
  return useQuery({
    queryKey: ["session"],
    queryFn: async () => unwrap(await api.GET("/api/v1/session")),
    staleTime: Infinity,
  });
}

export function useBackups(ns: string) {
  return useQuery({
    queryKey: ["backups", ns],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/backups", { params: { path: { ns } }, signal })).items,
    refetchInterval: (q) => moving(q.state.data),
  });
}

export function useBackup(ns: string, name: string) {
  return useQuery({
    queryKey: ["backups", ns, name],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/backups/{name}", { params: { path: { ns, name } }, signal })).item,
    refetchInterval: (q) => (q.state.data && !q.state.data.operation.terminal ? LIVE_MS : false),
    retry: (count, error) => !(error instanceof Error && "status" in error && error.status === 404) && count < 2,
  });
}

export function useRestores(ns: string) {
  return useQuery({
    queryKey: ["restores", ns],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/restores", { params: { path: { ns } }, signal })).items,
    refetchInterval: (q) => moving(q.state.data),
  });
}

export function useRestore(ns: string, name: string) {
  return useQuery({
    queryKey: ["restores", ns, name],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/restores/{name}", { params: { path: { ns, name } }, signal })).item,
    refetchInterval: (q) => (q.state.data && !q.state.data.operation.terminal ? LIVE_MS : false),
  });
}

export function useRestoreOperation(ns: string, name: string) {
  return useQuery({
    queryKey: ["operations", ns, "restore", name],
    queryFn: async ({ signal }) =>
      unwrap(
        await api.GET("/api/v1/namespaces/{ns}/operations/{kind}/{name}", {
          params: { path: { ns, kind: "restore", name } },
          signal,
        }),
      ).item,
    refetchInterval: (q) => (q.state.data && !q.state.data.terminal ? 1000 : false),
  });
}

export function useConnections(ns: string) {
  return useQuery({
    queryKey: ["connections", ns],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/connections", { params: { path: { ns } }, signal })).items,
  });
}

export function useSchedules(ns: string) {
  return useQuery({
    queryKey: ["schedules", ns],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/schedules", { params: { path: { ns } }, signal })).items,
  });
}

export function useProtection(ns: string) {
  return useQuery({
    queryKey: ["protection", ns],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/protection-policies", { params: { path: { ns } }, signal })).items,
    refetchInterval: 15_000,
  });
}

export function usePoints(ns: string, catalog = "primary") {
  return useQuery({
    queryKey: ["points", ns, catalog],
    queryFn: async ({ signal }) =>
      unwrap(
        await api.GET("/api/v1/namespaces/{ns}/catalogs/{name}/points", {
          params: { path: { ns, name: catalog } },
          signal,
        }),
      ).items,
  });
}

export function useBackUpNow(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (schedule: string): Promise<Backup> =>
      unwrap(
        await api.POST("/api/v1/namespaces/{ns}/backups", {
          params: { path: { ns }, header: createHeaders(`logweir-console.backup.${crypto.randomUUID()}`) },
          body: { scheduleRef: { name: schedule } },
        }),
      ).item,
    onSuccess: () => client.invalidateQueries({ queryKey: ["backups", ns] }),
  });
}

export function useCreateRestore(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body: CreateRestoreRequest): Promise<Restore> =>
      unwrap(
        await api.POST("/api/v1/namespaces/{ns}/restores", {
          params: { path: { ns }, header: createHeaders(body.planHash) },
          body,
        }),
      ).item,
    onSuccess: () => client.invalidateQueries({ queryKey: ["restores", ns] }),
  });
}
