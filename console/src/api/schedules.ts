// Schedules and Protection (PROD-18.1 R3): every read and write those pages make,
// typed from the OpenAPI document. Lists that the overview also reads
// (schedules, protection policies, backups) stay in queries.ts; this module adds
// the rest under the same query keys, so a write here refreshes the overview too.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, api, call, createHeaders, listAll, LIST_PAGE_SIZE, mintKey } from "./client";
import { keys } from "./queries";
import { useCan } from "./session";
import type { components, operations } from "./schema";

type S = components["schemas"];

export type Schedule = S["Schedule"];
export type CadencePreview = S["CadencePreviewResponse"];
export type CadenceQuery = NonNullable<operations["previewCadence"]["parameters"]["query"]>;
export type NextRun = S["NextRunView"];
export type CreateScheduleRequest = S["CreateScheduleRequest"];
export type UpdateSchedulePolicyRequest = S["UpdateSchedulePolicyRequest"];
export type ManualBackupResponse = S["ManualBackupResponse"];
export type DestinationSummary = S["DestinationSummary"];
export type RetentionPolicy = S["RetentionPolicyView"];
export type Preflight = S["Preflight"];
export type TopicDiscovery = S["TopicDiscovery"];
export type ProtectionPolicy = S["ProtectionPolicyView"];

const LIVE_MS = 1500;

/** PUT and the suspension command carry no Idempotency-Key: a precondition makes them safe. */
function originHeader() {
  return { Origin: window.location.origin };
}

/** A fresh idempotency intent: random, never counted, and available on plain http too. */
export function mintIntent(kind: "schedule" | "manual" | "readiness"): string {
  return mintKey(kind);
}

function notFound(error: unknown): boolean {
  return error instanceof ApiError && error.status === 404;
}

export function useSchedule(ns: string, name: string) {
  return useQuery({
    queryKey: keys.item("schedules", ns, name),
    enabled: useCan(ns, "schedulesRead"),
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/schedules/{name}", { params: { path: { ns, name } }, signal }))).item,
    retry: (count, error) => !notFound(error) && count < 1,
  });
}


/** GET /cadence-previews: computed by the scheduler's own module; the browser evaluates no cron. */
export function useCadencePreview(query: CadenceQuery | null) {
  return useQuery({
    queryKey: ["cadence-preview", query],
    queryFn: async ({ signal }) => call(() => api.GET("/api/v1/cadence-previews", { params: { query: query ?? {} }, signal })),
    enabled: query !== null,
    retry: false,
    staleTime: 60_000,
  });
}


/** The topics the newest successful discovery of a connection saw: an offer, never a bound. */
export function useDiscoveredTopics(ns: string, connection: string) {
  const readable = useCan(ns, "topicDiscovery");
  return useQuery({
    queryKey: ["topic-discoveries", ns, connection, "latest-topics"],
    enabled: connection.length > 0 && readable,
    queryFn: async ({ signal }) => {
      const latest = (await call(() =>
        api.GET("/api/v1/namespaces/{ns}/connections/{name}/topic-discoveries", {
          params: { path: { ns, name: connection }, query: { latest: true } },
          signal,
        }),
      )) as unknown as S["DiscoveryLatestResponse"];
      const found = latest.lastSuccessful;
      if (!found) return { discovery: undefined, failed: latest.latestAttempt?.state === "failed", topics: [] as string[], more: false };
      const page = await call(() =>
        api.GET("/api/v1/namespaces/{ns}/topic-discoveries/{id}/topics", {
          params: { path: { ns, id: found.id }, query: { limit: 200 } },
          signal,
        }),
      );
      return {
        discovery: found,
        failed: latest.latestAttempt?.state === "failed",
        topics: page.items.filter((t) => !t.internal).map((t) => t.name),
        // One page of 200 is offered; past it the operator types the name.
        more: Boolean(page.page.nextCursor),
      };
    },
    staleTime: 60_000,
  });
}

export function useRetentionPolicies(ns: string) {
  return useQuery({
    queryKey: keys.list("retention-policies", ns),
    enabled: useCan(ns, "retention"),
    queryFn: ({ signal }) =>
      listAll("retention policies", (cursor) =>
        call(() =>
          api.GET("/api/v1/namespaces/{ns}/retention-policies", { params: { path: { ns }, query: { limit: LIST_PAGE_SIZE, cursor } }, signal }),
        ),
      ),
    retry: false,
  });
}

export function useProtectionPolicy(ns: string, name: string) {
  return useQuery({
    queryKey: keys.item("protection", ns, name),
    enabled: useCan(ns, "protection"),
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/protection-policies/{name}", { params: { path: { ns, name } }, signal }))).item,
    refetchInterval: 15_000,
    retry: (count, error) => !notFound(error) && count < 1,
  });
}

// ------------------------------------------------------------------ writes

export function useCreateSchedule(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ body, intent }: { body: CreateScheduleRequest; intent: string }) =>
      call(() =>
        api.POST("/api/v1/namespaces/{ns}/schedules", {
          params: { path: { ns }, header: createHeaders(intent) },
          body,
        }),
      ),
    onSuccess: () => client.invalidateQueries({ queryKey: keys.list("schedules", ns) }),
  });
}

export function useEditPolicy(ns: string, name: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body: UpdateSchedulePolicyRequest) =>
      (
        await call(() =>
          api.PUT("/api/v1/namespaces/{ns}/schedules/{name}", {
            params: { path: { ns, name }, header: originHeader() },
            body,
          }),
        )
      ).item,
    onSettled: () => client.invalidateQueries({ queryKey: keys.list("schedules", ns) }),
  });
}

export function useSetSuspension(ns: string, name: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body: { suspended: boolean; expectedResourceVersion: string }) =>
      (
        await call(() =>
          api.POST("/api/v1/namespaces/{ns}/schedules/{name}:set-suspension", {
            params: { path: { ns, name }, header: originHeader() },
            body,
          }),
        )
      ).item,
    // A refusal (412: somebody changed it meanwhile) re-reads as well as a success does.
    onSettled: () => client.invalidateQueries({ queryKey: keys.list("schedules", ns) }),
  });
}

/** Back up now from a schedule: its current revision, under the click's own intent. */
export function useRunFromSchedule(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ schedule, generation, intent }: { schedule: string; generation: number | null | undefined; intent: string }) =>
      (await call(() =>
        api.POST("/api/v1/namespaces/{ns}/backups", {
          params: { path: { ns }, header: createHeaders(intent) },
          body: {
            scheduleRef: typeof generation === "number" ? { name: schedule, expectedGeneration: generation } : { name: schedule },
          },
        }),
      )) as ManualBackupResponse,
    onSuccess: () => client.invalidateQueries({ queryKey: keys.list("backups", ns) }),
    // A policy_changed refusal names a revision this page has not read: re-read it.
    onSettled: () => client.invalidateQueries({ queryKey: keys.list("schedules", ns) }),
  });
}

/** A backup readiness check (a Preflight of operation `backup`) for what a form describes. */
export function useStartReadiness(ns: string) {
  return useMutation({
    mutationFn: async ({ backup, intent }: { backup: S["BackupPreflightRequest"]; intent: string }) =>
      (
        await call(() =>
          api.POST("/api/v1/namespaces/{ns}/preflights", {
            params: { path: { ns }, header: createHeaders(intent) },
            body: { operation: "backup", backup },
          }),
        )
      ).item,
  });
}

/** One readiness check, re-read until it is terminal. */
export function usePreflight(ns: string, id: string | undefined) {
  const readable = useCan(ns, "preflight");
  return useQuery({
    queryKey: ["preflights", ns, id],
    enabled: Boolean(id) && readable,
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/preflights/{id}", { params: { path: { ns, id: id ?? "" } }, signal }))).item,
    refetchInterval: (q) => (q.state.data && !q.state.data.terminal ? LIVE_MS : false),
  });
}
