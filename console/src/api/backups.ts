// Backups, the recovery catalog and trust (PROD-18.1 R4): every read and write
// those pages make, over the typed client. The backup list and one backup share
// the overview's query keys (`["backups", ws, ...]`), so a write anywhere
// refreshes both.
import { useInfiniteQuery, useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
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

/** Every Backup in the workspace, newest first, re-read while one is moving: the Overview's list, one cache. */
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
export function useBackupOperation(ws: string, name: string, streaming = false) {
  return useQuery({
    queryKey: keys.operation(ws, "backup", name),
    enabled: useCan(ws, "operationsRead"),
    queryFn: async ({ signal }) =>
      (
        await call(() =>
          api.GET("/api/v1/workspaces/{ws}/operations/{kind}/{name}", {
            params: { path: { ws, kind: "backup", name } },
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
export function useRunDestination(ws: string, name: string | undefined) {
  const readable = useCan(ws, "destinations");
  return useQuery({
    queryKey: keys.item("destinations", ws, name ?? ""),
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/workspaces/{ws}/destinations/{name}", { params: { path: { ws, name: name! } }, signal }))).item,
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
export function useBackUpNow(ws: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ schedule, generation, key }: RunNow): Promise<ManualBackupResponse> =>
      call(() =>
        api.POST("/api/v1/workspaces/{ws}/backups", {
          params: { path: { ws }, header: createHeaders(key) },
          body: { scheduleRef: { name: schedule, ...(typeof generation === "number" ? { expectedGeneration: generation } : {}) } },
        }),
      ),
    onSettled: () => client.invalidateQueries({ queryKey: keys.list("backups", ws) }),
  });
}

// -------------------------------------------------------------- catalog

export function useCatalogs(ws: string) {
  return useQuery({
    queryKey: keys.list("catalogs", ws),
    enabled: useCan(ws, "catalogs"),
    queryFn: ({ signal }) =>
      listAll("recovery catalogs", (cursor) =>
        call(() => api.GET("/api/v1/workspaces/{ws}/catalogs", { params: { path: { ws }, query: { limit: LIST_PAGE_SIZE, cursor } }, signal })),
      ),
  });
}

export function useCatalog(ws: string, name: string) {
  return useQuery({
    queryKey: keys.item("catalogs", ws, name),
    enabled: useCan(ws, "catalogs"),
    queryFn: async ({ signal }) =>
      (await call(() => api.GET("/api/v1/workspaces/{ws}/catalogs/{name}", { params: { path: { ws, name } }, signal }))).item,
    // DUI-54.2: a view not built yet (a just-connected catalog) or aged out is re-read until a sync builds it.
    refetchInterval: (q) => (q.state.data?.viewExpired ? VIEW_MISSING_MS : false),
    retry: (count, error) => !notFound(error) && count < 2,
  });
}

/** How often a catalog whose view is missing is re-read. */
const VIEW_MISSING_MS = 5000;

/**
 * Each catalog's newest selectable point, for the Overview: one read of one point per
 * catalog that reports available points (the view lists newest first). A catalog whose
 * read fails is left out, never counted as holding nothing.
 */
export function useNewestCatalogPoints(ws: string, catalogs: Catalog[] | undefined): { catalog: string; point: PointView }[] {
  const readable = useCan(ws, "catalogs");
  const wanted = readable ? (catalogs ?? []).filter((c) => (c.counts.available ?? 0) > 0 && !c.viewExpired) : [];
  const results = useQueries({
    queries: wanted.map((c) => ({
      queryKey: ["points", ws, c.name, "newest"],
      queryFn: async ({ signal }: { signal: AbortSignal }) =>
        (
          await call(() =>
            api.GET("/api/v1/workspaces/{ws}/catalogs/{name}/points", {
              params: { path: { ws, name: c.name }, query: { limit: 1, selectable: "true" as const } },
              signal,
            }),
          )
        ).items[0] ?? null,
      retry: false,
    })),
  });
  return results.flatMap((r, i) => (r.data ? [{ catalog: wanted[i]!.name, point: r.data }] : []));
}

/** One bounded page of the catalog's view at a time (200 points), the next only when asked for. */
export function useCatalogPoints(ws: string, name: string, selectableOnly: boolean, enabled = true) {
  const readable = useCan(ws, "catalogs");
  return useInfiniteQuery({
    enabled: enabled && readable,
    queryKey: ["points", ws, name, selectableOnly ? "selectable" : "all"],
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam, signal }) =>
      call(() =>
        api.GET("/api/v1/workspaces/{ws}/catalogs/{name}/points", {
          params: {
            path: { ws, name },
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
    refetchInterval: (q) => (q.state.data?.pages.some((p) => p.viewExpired) ? VIEW_MISSING_MS : false),
    retry: false,
  });
}

export function useCatalogSigners(ws: string, name: string) {
  return useQuery({
    queryKey: ["catalogs", ws, name, "signers"],
    enabled: useCan(ws, "catalogs"),
    queryFn: async ({ signal }) =>
      call(() => api.GET("/api/v1/workspaces/{ws}/catalogs/{name}/signers", { params: { path: { ws, name } }, signal })),
    retry: false,
  });
}

/** The workspace's saved destinations, for the connect form's pick-list. A failed read leaves a text box. */
export const useDestinationChoices = useDestinations;

/** Connect an existing archive: one RecoveryCatalog per key. */
export function useConnectArchive(ws: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ body, key }: { body: ConnectArchiveRequest; key: string }): Promise<CatalogResponse> =>
      call(() => api.POST("/api/v1/workspaces/{ws}/catalogs", { params: { path: { ws }, header: createHeaders(key) }, body })),
    onSettled: () => client.invalidateQueries({ queryKey: keys.list("catalogs", ws) }),
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
