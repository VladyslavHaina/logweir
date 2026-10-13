// The Restores page group's reads and writes (PROD-18.1 R5), over the typed client.
// Lists re-read while a run in them moves; one restore's page follows its operation
// while it is not final. Every write carries the Idempotency-Key the plan's minted
// name gives it, so a second click, a lost answer or a reload never makes a second object.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, LIST_PAGE_BUDGET, LIST_PAGE_SIZE, api, call, createHeaders, listAll } from "./client";
import { keys, useBackups, useConnections, useDestinations, useOperation, useRestore, useRestores } from "./queries";
import { grantOf, useSessionDoc } from "./session";
import type { components } from "./schema";

type S = components["schemas"];
export type Restore = S["Restore"];
export type Backup = S["Backup"];
export type Connection = S["Connection"];
export type Destination = S["Destination"];
export type CatalogView = S["CatalogView"];
export type PointView = S["PointView"];
export type OperationView = S["OperationView"];
export type Preflight = S["Preflight"];
export type ApprovalPolicy = S["ApprovalPolicyView"];
export type ApprovalRequest = S["ApprovalRequestView"];
export type RestoreRouting = S["RestoreRoutingView"];
export type TopicDiscovery = S["TopicDiscovery"];
export type Capabilities = S["Capabilities"];

const PAGE = LIST_PAGE_SIZE;
const PAGE_BUDGET = LIST_PAGE_BUDGET;

/** R1's keys where the read is the shared one; the rest have their own kind. */
export const restoreKeys = {
  list: (ns: string) => keys.list("restores", ns),
  item: (ns: string, name: string) => keys.item("restores", ns, name),
  operation: (ns: string, name: string) => keys.operation(ns, "restore", name),
  /** Null when no request was made: kept apart from the Approvals page's read, which throws. */
  approvalRequest: (ns: string, name: string) => ["restore-approval-requests", ns, name] as const,
  /** The fail-safe reading (null when unreadable), kept apart from the Approvals page's. */
  policy: (ns: string) => ["approval-policy", ns, "fail-safe"] as const,
  preflight: (ns: string, id: string) => ["preflights", ns, id] as const,
};

export { outcomeUnknown } from "./client";

/** A read whose 404 is an answer (nothing there), not an error. */
async function orNull<T>(read: () => Promise<T>): Promise<T | null> {
  try {
    return await read();
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}

/** What this session may do in a namespace. */
export function useCapabilities(ns: string): Capabilities | undefined {
  const s = useSessionDoc();
  return grantOf(s, ns)?.capabilities ?? s.capabilities;
}

// ------------------------------------------------------------------- reads

/** The namespace's restores, newest first: R1's list, one cache with Approvals and the Overview. */
export const useRestoreList = useRestores;
/** One restore: R1's read. */
export const useRestoreItem = useRestore;

/** THE LIVE WATCH: the run's operation (R1's read), re-read every second while it is not
 *  final unless the run's event stream (stream.ts) is carrying it. */
export function useRestoreOperation(ns: string, name: string, streaming = false) {
  return useOperation(ns, "restore", name, streaming);
}

/** The two-person request a restore waits on, when there is one (404 when none was made). */
export function useApprovalRequest(ns: string, name: string, enabled: boolean) {
  return useQuery({
    queryKey: restoreKeys.approvalRequest(ns, name),
    enabled,
    queryFn: ({ signal }) =>
      orNull(async () => (await call(() => api.GET("/api/v1/namespaces/{ns}/restores/{name}/approval-request", { params: { path: { ns, name } }, signal }))).item),
    refetchInterval: 5000,
  });
}

/** The namespace's EFFECTIVE approval policy. A read that fails is null: the page then
 *  shows the out-of-band (strict) instructions, the fail-safe reading. */
export function useApprovalPolicy(ns: string) {
  return useQuery({
    queryKey: restoreKeys.policy(ns),
    queryFn: async ({ signal }): Promise<ApprovalPolicy | null> => {
      try {
        // The published operation names a stray `name` path parameter this route does not have;
        // openapi-fetch drops what the path does not use.
        return (await call(() => api.GET("/api/v1/namespaces/{ns}/approval-policy", { params: { path: { ns, name: "" } }, signal }))).item;
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") throw error;
        return null;
      }
    },
    // Read afresh on every visit: it decides where a submission goes.
    staleTime: 0,
  });
}

/** The shared lists (R1's keys, one cache with the pages that own them). */
export const useBackupList = useBackups;
export const useConnectionList = useConnections;
export const useDestinationList = useDestinations;

export async function readDestination(ns: string, name: string, signal?: AbortSignal): Promise<Destination | null> {
  return orNull(async () => (await call(() => api.GET("/api/v1/namespaces/{ns}/destinations/{name}", { params: { path: { ns, name } }, signal }))).item);
}

/** One destination, null when it does not exist (kept apart from R2's read, which throws on 404). */
export function useDestination(ns: string, name: string) {
  return useQuery({
    queryKey: ["destinations-or-null", ns, name],
    enabled: name.length > 0,
    queryFn: ({ signal }) => readDestination(ns, name, signal),
  });
}

export async function listCatalogs(ns: string, signal?: AbortSignal): Promise<CatalogView[]> {
  return listAll("catalogs", (cursor) =>
    call(() => api.GET("/api/v1/namespaces/{ns}/catalogs", { params: { path: { ns }, query: { limit: PAGE, cursor } }, signal })),
  );
}

export async function readCatalog(ns: string, name: string, signal?: AbortSignal): Promise<CatalogView> {
  return (await call(() => api.GET("/api/v1/namespaces/{ns}/catalogs/{name}", { params: { path: { ns, name } }, signal }))).item;
}

export interface PointSearch {
  entry: PointView | null;
  viewExpired: boolean;
  backupVerdictsIncomplete: string | null;
  incomplete: boolean;
  budgetExhausted: boolean;
}

/** Find one point in a catalog's view, page by page (the view holds at most 5000). */
export async function findCatalogPoint(ns: string, catalog: string, match: (p: PointView) => boolean, signal?: AbortSignal, selectable = false): Promise<PointSearch> {
  const out: PointSearch = { entry: null, viewExpired: false, backupVerdictsIncomplete: null, incomplete: false, budgetExhausted: false };
  let cursor: string | undefined;
  for (let read = 0; read < PAGE_BUDGET; read += 1) {
    const at = cursor;
    const page = await call(() =>
      api.GET("/api/v1/namespaces/{ns}/catalogs/{name}/points", {
        params: { path: { ns, name: catalog }, query: { limit: PAGE, ...(at ? { cursor: at } : {}), ...(selectable ? { selectable: "true" as const } : {}) } },
        signal,
      }),
    );
    if (page.viewExpired) out.viewExpired = true;
    if (page.backupVerdictsIncomplete) out.backupVerdictsIncomplete = String(page.backupVerdictsIncomplete);
    if (page.incomplete) out.incomplete = true;
    const found = page.items.find(match);
    if (found) return { ...out, entry: found };
    cursor = page.page.nextCursor ?? undefined;
    if (!cursor || out.incomplete) return out;
  }
  return { ...out, budgetExhausted: true };
}

/** The catalog points connected archives offer, page by page across catalogs. */
export async function readCatalogOffers(ns: string, signal?: AbortSignal) {
  const offers: { catalog: CatalogView; entry: PointView; page: { viewExpired: boolean; backupVerdictsIncomplete: string | null } }[] = [];
  const notes: string[] = [];
  let catalogs: CatalogView[];
  try {
    catalogs = await listCatalogs(ns, signal);
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") throw error;
    return { offers, notes: [`The recovery catalogs could not be listed (${error instanceof Error ? error.message : String(error)}).`] };
  }
  for (const catalog of catalogs) {
    try {
      let cursor: string | undefined;
      let complete = false;
      for (let read = 0; read < PAGE_BUDGET; read += 1) {
        const at = cursor;
        const page = await call(() =>
          api.GET("/api/v1/namespaces/{ns}/catalogs/{name}/points", {
            params: { path: { ns, name: catalog.name }, query: { limit: PAGE, selectable: "true", ...(at ? { cursor: at } : {}) } },
            signal,
          }),
        );
        const flags = { viewExpired: page.viewExpired, backupVerdictsIncomplete: page.backupVerdictsIncomplete ? String(page.backupVerdictsIncomplete) : null };
        for (const entry of page.items) offers.push({ catalog, entry, page: flags });
        cursor = page.page.nextCursor ?? undefined;
        if (!cursor) {
          complete = true;
          break;
        }
      }
      if (!complete) notes.push(`Catalog ${catalog.name} holds more points than this page reads; open it for the rest.`);
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") throw error;
      notes.push(`Catalog ${catalog.name}'s points could not be read (${error instanceof Error ? error.message : String(error)}).`);
    }
  }
  return { offers, notes };
}

/** A Backup's own receipt digest, from its operation's evidence (the list does not carry it). */
export async function readBackupReceiptDigest(ns: string, name: string, signal?: AbortSignal): Promise<string | null> {
  const op = (await call(() => api.GET("/api/v1/namespaces/{ns}/operations/{kind}/{name}", { params: { path: { ns, kind: "backup", name } }, signal }))).item;
  return op.evidence.payloadSha256 ?? null;
}

/** The target's newest topic discoveries (`?latest=true`), for its broker count. */
export async function readLatestDiscoveries(ns: string, connection: string, signal?: AbortSignal) {
  const r = await call(() =>
    api.GET("/api/v1/namespaces/{ns}/connections/{name}/topic-discoveries", {
      params: { path: { ns, name: connection }, query: { latest: true } },
      signal,
    }),
  );
  return r as unknown as { latestAttempt?: TopicDiscovery | null; lastSuccessful?: TopicDiscovery | null };
}

// ------------------------------------------------------------------- writes

/** Start a restore Preflight for exactly the prepared bytes. One deliberate click is one
 *  observation of mutable state, so each click mints its own key. */
export async function startPreflight(ns: string, body: S["CreatePreflightRequest"], key: string): Promise<Preflight> {
  return (await call(() => api.POST("/api/v1/namespaces/{ns}/preflights", { params: { path: { ns }, header: createHeaders(key) }, body }))).item;
}

/** Read a preflight against the hash on screen: the server recomputes `applicable` and `stale`. */
export async function readPreflight(ns: string, id: string, planHash: string, signal?: AbortSignal): Promise<Preflight> {
  return (await call(() => api.GET("/api/v1/namespaces/{ns}/preflights/{id}", { params: { path: { ns, id }, query: planHash ? { planHash } : {} }, signal }))).item;
}

export async function cancelPreflight(ns: string, id: string): Promise<void> {
  await call(() => api.POST("/api/v1/namespaces/{ns}/preflights/{id}:cancel", { params: { path: { ns, id }, header: { Origin: window.location.origin } } }));
}

export function useCreateRestore(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ body, key }: { body: S["CreateRestoreRequest"]; key: string }) =>
      call(() => api.POST("/api/v1/namespaces/{ns}/restores", { params: { path: { ns }, header: createHeaders(key) }, body })),
    onSuccess: () => client.invalidateQueries({ queryKey: restoreKeys.list(ns) }),
  });
}
