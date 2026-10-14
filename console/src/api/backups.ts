// Backups, the recovery catalog and trust (PROD-18.1 R4): every read and write
// those pages make, over the typed client. The backup list and one backup share
// the overview's query keys (`["backups", ns, ...]`), so a write anywhere
// refreshes both.
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, LIST_PAGE_SIZE, LocalRefusal, api, call, createHeaders, listAll, mintKey, outcomeUnknown } from "./client";
import { keys, useBackup, useBackups, useDestinations } from "./queries";
import { useCan } from "./session";
import type { components } from "./schema";

type S = components["schemas"];

export type Backup = S["Backup"];
export type OperationView = S["OperationView"];
export type OperationSummary = S["OperationSummary"];
export type ManualBackupResponse = S["ManualBackupResponse"];
export type Destination = S["Destination"];
export type DestinationSummary = S["DestinationSummary"];
export type Catalog = S["CatalogView"];
export type PointView = S["PointView"];
export type PointPage = S["PointPageResponse"];
export type SignerPage = S["SignerPageResponse"];
export type ConnectArchiveRequest = S["ConnectArchiveRequest"];
export type CatalogResponse = S["CatalogResponse"];
export type TrustPolicy = S["TrustPolicyView"];
export type TrustKey = S["TrustKeyView"];
export type FieldError = S["FieldError"];

const LIVE_MS = 1500;

// ------------------------------------------------------------ refusals

export { LocalRefusal, mintKey };

/**
 * What became of a write (the old console's `failureKind`):
 * - `unknown`: no answer, a timeout or a 5xx, so the object may or may not exist; resending the same key is safe;
 * - `conflict`: 409; `invalid`: 400 or 422; `refused`: any other answer; `local`: nothing was sent.
 */
export type Outcome = "unknown" | "conflict" | "invalid" | "refused" | "local";

export function outcomeOf(error: unknown): Outcome {
  if (error instanceof LocalRefusal) return "local";
  if (!(error instanceof ApiError) || outcomeUnknown(error)) return "unknown";
  if (error.status === 409) return "conflict";
  if (error.status === 400 || error.status === 422) return "invalid";
  return "refused";
}

// ------------------------------------------------------------- backups

/** Every Backup in the namespace, newest first, re-read while one is moving: the Overview's list, one cache. */
export const useBackupList = useBackups;

function notFound(error: unknown): boolean {
  return error instanceof ApiError && error.status === 404;
}

export const useBackupDoc = useBackup;

/** How often a terminal run whose verdict is still pending is read again, and for how long. */
const SETTLE_MS = 5000;
const SETTLE_READS = 60;

/** The run's operation view: stage, diagnoses, result, evidence. Read until it is terminal and its
 *  verdict has settled. While `streaming` (stream.ts holds the run's event stream open) the stream
 *  writes this key and the poll of a moving run stands down. */
export function useBackupOperation(ns: string, name: string, streaming = false) {
  return useQuery({
    queryKey: keys.operation(ns, "backup", name),
    enabled: useCan(ns, "operationsRead"),
    queryFn: async ({ signal }) =>
      (
        await call(() =>
          api.GET("/api/v1/namespaces/{ns}/operations/{kind}/{name}", {
            params: { path: { ns, kind: "backup", name } },
            signal,
          }),
        )
      ).item,
    refetchInterval: (q) => {
      const op = q.state.data;
      if (!op) return false;
      if (!op.terminal) return streaming ? false : LIVE_MS;
      return op.verification?.state === "pending" && q.state.dataUpdateCount < SETTLE_READS ? SETTLE_MS : false;
    },
    retry: (count, error) => !notFound(error) && count < 2,
  });
}

/** The destination a run names, read for its bucket. A refusal is the check's caveat, not the page's error. */
export function useRunDestination(ns: string, name: string | undefined) {
  const readable = useCan(ns, "destinations");
  return useQuery({
    queryKey: keys.item("destinations", ns, name ?? ""),
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/destinations/{name}", { params: { path: { ns, name: name! } }, signal }))).item,
    enabled: Boolean(name) && readable,
    retry: false,
  });
}

export interface RunNow {
  schedule: string;
  /** The revision this page showed; a different one now is `409 policy_changed`, never a run under a policy nobody saw. */
  generation?: number | null;
  key: string;
}

/** Back up now from one schedule: one run per key, whatever happens to the answer. */
export function useBackUpNow(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ schedule, generation, key }: RunNow): Promise<ManualBackupResponse> =>
      call(() =>
        api.POST("/api/v1/namespaces/{ns}/backups", {
          params: { path: { ns }, header: createHeaders(key) },
          body: { scheduleRef: { name: schedule, ...(typeof generation === "number" ? { expectedGeneration: generation } : {}) } },
        }),
      ),
    onSettled: () => client.invalidateQueries({ queryKey: keys.list("backups", ns) }),
  });
}

// -------------------------------------------------------------- catalog

export function useCatalogs(ns: string) {
  return useQuery({
    queryKey: keys.list("catalogs", ns),
    enabled: useCan(ns, "catalogs"),
    queryFn: ({ signal }) =>
      listAll("recovery catalogs", (cursor) =>
        call(() => api.GET("/api/v1/namespaces/{ns}/catalogs", { params: { path: { ns }, query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
  });
}

export function useCatalog(ns: string, name: string) {
  return useQuery({
    queryKey: keys.item("catalogs", ns, name),
    enabled: useCan(ns, "catalogs"),
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/namespaces/{ns}/catalogs/{name}", { params: { path: { ns, name } }, signal }))).item,
    retry: (count, error) => !notFound(error) && count < 2,
  });
}

/** One bounded page of the catalog's view at a time (200 points), the next only when asked for. */
export function useCatalogPoints(ns: string, name: string, selectableOnly: boolean, enabled = true) {
  const readable = useCan(ns, "catalogs");
  return useInfiniteQuery({
    enabled: enabled && readable,
    queryKey: ["points", ns, name, selectableOnly ? "selectable" : "all"],
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam, signal }) =>
      call(() =>
        api.GET("/api/v1/namespaces/{ns}/catalogs/{name}/points", {
          params: {
            path: { ns, name },
            query: {
              limit: 200,
              ...(pageParam ? { cursor: pageParam } : {}),
              ...(selectableOnly ? { selectable: "true" as const } : {}),
            },
          },
          signal,
        }),
      ),
    getNextPageParam: (last) => last.page.nextCursor ?? undefined,
    retry: false,
  });
}

export function useCatalogSigners(ns: string, name: string) {
  return useQuery({
    queryKey: ["catalogs", ns, name, "signers"],
    enabled: useCan(ns, "catalogs"),
    queryFn: async ({ signal }) =>
      call(() => api.GET("/api/v1/namespaces/{ns}/catalogs/{name}/signers", { params: { path: { ns, name } }, signal })),
    retry: false,
  });
}

/** The namespace's saved destinations, for the connect form's pick-list. A failed read leaves a text box. */
export const useDestinationChoices = useDestinations;

/** Connect an existing archive: one RecoveryCatalog per key. */
export function useConnectArchive(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ body, key }: { body: ConnectArchiveRequest; key: string }): Promise<CatalogResponse> =>
      call(() => api.POST("/api/v1/namespaces/{ns}/catalogs", { params: { path: { ns }, header: createHeaders(key) }, body })),
    onSettled: () => client.invalidateQueries({ queryKey: keys.list("catalogs", ns) }),
  });
}

// ---------------------------------------------------------------- trust

/** The installation's trust policies this reader administers (cluster-scoped; no namespace). */
export function useTrustPolicies(enabled = true) {
  return useQuery({
    enabled,
    queryKey: ["trust-policies"],
    queryFn: ({ signal }) =>
      listAll("trust policies", (cursor) =>
        call(() => api.GET("/api/v1/trust-policies", { params: { query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
    retry: (count, error) => !(error instanceof ApiError && (error.status === 403 || error.status === 404)) && count < 2,
  });
}
